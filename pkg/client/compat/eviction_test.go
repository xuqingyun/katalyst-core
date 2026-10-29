package compat

import (
	"context"
	"errors"
	"testing"

	policy "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEvictionUsesBetaAndPreservesRequest(t *testing.T) {
	client := fake.NewSimpleClientset()
	denied := apierrors.NewTooManyRequests("PDB blocks eviction", 0)
	client.PrependReactor("create", "pods", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" || a.GetResource().Resource != "pods" {
			t.Fatalf("wrong request: %#v", a)
		}
		e, ok := a.(clienttesting.CreateAction).GetObject().(*policy.Eviction)
		if !ok {
			t.Fatalf("wrong type %T", a.(clienttesting.CreateAction).GetObject())
		}
		if e.APIVersion != "policy/v1beta1" || e.Kind != "Eviction" {
			t.Fatalf("wrong eviction GVK: %s %s", e.APIVersion, e.Kind)
		}
		opts := e.DeleteOptions
		if opts == nil || opts.Preconditions == nil || string(*opts.Preconditions.UID) != "original" || len(opts.DryRun) != 1 || *opts.GracePeriodSeconds != 3 {
			t.Fatalf("options lost: %v", opts)
		}
		if e.Annotations["trigger"] != "agent" {
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
