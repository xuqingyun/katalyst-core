package compat

import (
	"context"
	"fmt"

	policybeta "k8s.io/api/policy/v1beta1"
	"k8s.io/client-go/kubernetes"
)

// Evict uses policy/v1beta1, which is served by both supported internal
// Kubernetes versions (1.18 and 1.35).
// It preserves PDB admission, annotations, UID preconditions and dry-run options.
// A rejected eviction is never retried via DELETE.
func Evict(ctx context.Context, client kubernetes.Interface, eviction *policybeta.Eviction) error {
	if eviction == nil {
		return fmt.Errorf("nil eviction")
	}
	e := eviction.DeepCopy()
	e.APIVersion = "policy/v1beta1"
	e.Kind = "Eviction"
	return client.PolicyV1beta1().Evictions(e.Namespace).Evict(ctx, e)
}
