package compat

import (
	"context"
	"errors"
	policyv1 "k8s.io/api/policy/v1"
	policy "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
)

func TestEvictionPreservesUIDDryRunAnnotationsAndPDBFailure(t *testing.T) {
	for _, version := range []string{"v1beta1", "v1"} {
		t.Run(version, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			client.Resources = []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "pods/eviction", Group: "policy", Version: version}}}}
			denied := apierrors.NewTooManyRequests("PDB blocks eviction", 0)
			client.PrependReactor("create", "pods", func(a clienttesting.Action) (bool, runtime.Object, error) {
				if a.GetSubresource() != "eviction" || a.GetResource().Resource != "pods" {
					t.Fatalf("wrong request: %#v", a)
				}
				obj := a.(clienttesting.CreateAction).GetObject()
				var opts *metav1.DeleteOptions
				switch e := obj.(type) {
				case *policy.Eviction:
					if version != "v1beta1" {
						t.Fatal("wrong decoder")
					}
					opts = e.DeleteOptions
				case *policyv1.Eviction:
					if version != "v1" {
						t.Fatal("wrong decoder")
					}
					opts = e.DeleteOptions
				default:
					t.Fatalf("wrong type %T", obj)
				}
				if opts == nil || opts.Preconditions == nil || string(*opts.Preconditions.UID) != "original" || len(opts.DryRun) != 1 || *opts.GracePeriodSeconds != 3 {
					t.Fatalf("options lost: %v", opts)
				}
				data := obj.(metav1.Object)
				if data.GetAnnotations()["trigger"] != "agent" {
					t.Fatal("annotation lost")
				}
				return true, nil, denied
			})
			uid := types.UID("original")
			grace := int64(3)
			e := &policy.Eviction{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", Annotations: map[string]string{"trigger": "agent"}}, DeleteOptions: &metav1.DeleteOptions{GracePeriodSeconds: &grace, Preconditions: &metav1.Preconditions{UID: &uid}, DryRun: []string{metav1.DryRunAll}}}
			err := Evict(context.Background(), client, e)
			if !errors.Is(err, denied) {
				t.Fatalf("PDB error lost: %v", err)
			}
			actions := client.Actions()
			mutations := 0
			for _, action := range actions {
				if action.GetVerb() == "create" || action.GetVerb() == "delete" {
					mutations++
				}
			}
			if mutations != 1 {
				t.Fatalf("retried rejected mutation: %v", actions)
			}
			if e.APIVersion != "" {
				t.Fatal("caller object mutated")
			}
		})
	}
}
func TestDiscoveryErrorsDoNotDowngrade(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "resource", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "resource"}, "", errors.New("denied"))
	})
	_, err := ResourceVersion(client.Discovery(), "autoscaling", "horizontalpodautoscalers", "v2", "v2beta2")
	if !apierrors.IsForbidden(err) {
		t.Fatal(err)
	}
}
