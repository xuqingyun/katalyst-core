package compat

import (
	"context"
	"fmt"
	policyv1 "k8s.io/api/policy/v1"
	policybeta "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Evict uses the version advertised by the pods/eviction subresource.
// It preserves PDB admission, annotations, UID preconditions and dry-run options.
// A rejected eviction is never retried via DELETE or a different API version.
func Evict(ctx context.Context, client kubernetes.Interface, eviction *policybeta.Eviction) error {
	if eviction == nil {
		return fmt.Errorf("nil eviction")
	}
	resources, err := client.Discovery().ServerResourcesForGroupVersion("v1")
	if err != nil {
		return err
	}
	for _, r := range resources.APIResources {
		if r.Name != "pods/eviction" {
			continue
		}
		switch r.Version {
		case "v1":
			e := &policyv1.Eviction{TypeMeta: metav1.TypeMeta{APIVersion: "policy/v1", Kind: "Eviction"}, ObjectMeta: *eviction.ObjectMeta.DeepCopy(), DeleteOptions: eviction.DeleteOptions.DeepCopy()}
			return client.PolicyV1().Evictions(e.Namespace).Evict(ctx, e)
		case "v1beta1":
			e := eviction.DeepCopy()
			e.APIVersion = "policy/v1beta1"
			e.Kind = "Eviction"
			return client.PolicyV1beta1().Evictions(e.Namespace).Evict(ctx, e)
		default:
			return fmt.Errorf("unsupported pods/eviction version %q", r.Version)
		}
	}
	return fmt.Errorf("pods/eviction not served")
}
