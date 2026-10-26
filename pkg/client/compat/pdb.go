package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	policyv1 "k8s.io/api/policy/v1"
	policybeta "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// PodDisruptionBudgetClient is a read-only PDB client. A single discovery result
// selects the same served version for LIST and WATCH throughout its lifetime.
// Both methods expose policy/v1 objects with policy/v1 selector semantics;
// they are observations, not objects intended for writing back to the server.
type PodDisruptionBudgetClient struct {
	client    kubernetes.Interface
	namespace string
	version   string
}

// NewPodDisruptionBudgetClient prefers policy/v1 and uses policy/v1beta1 on old
// servers. Only discovery 404 or an absent resource allows trying another
// version. Authorization, throttling and transport errors are returned intact.
// Discovery is lazy: agents that do not read PDBs need no additional permission.
func NewPodDisruptionBudgetClient(client kubernetes.Interface, namespace string) (*PodDisruptionBudgetClient, error) {
	for _, version := range []string{"v1", "v1beta1"} {
		resources, err := client.Discovery().ServerResourcesForGroupVersion("policy/" + version)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, resource := range resources.APIResources {
			if resource.Name != "poddisruptionbudgets" {
				continue
			}
			var list, watch bool
			for _, verb := range resource.Verbs {
				list = list || verb == "list"
				watch = watch || verb == "watch"
			}
			if !list || !watch {
				return nil, fmt.Errorf("policy/%s poddisruptionbudgets must serve list and watch", version)
			}
			return &PodDisruptionBudgetClient{client: client, namespace: namespace, version: version}, nil
		}
	}
	return nil, fmt.Errorf("no served policy/v1 or policy/v1beta1 poddisruptionbudgets")
}

// List preserves list metadata, pagination, resourceVersion and all PDB fields.
// A failed LIST is never retried against a different version.
func (c *PodDisruptionBudgetClient) List(ctx context.Context, options metav1.ListOptions) (*policyv1.PodDisruptionBudgetList, error) {
	if c.version == "v1" {
		return c.client.PolicyV1().PodDisruptionBudgets(c.namespace).List(ctx, options)
	}
	list, err := c.client.PolicyV1beta1().PodDisruptionBudgets(c.namespace).List(ctx, options)
	if err != nil {
		return nil, err
	}
	result := &policyv1.PodDisruptionBudgetList{
		TypeMeta: metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudgetList"},
		ListMeta: *list.ListMeta.DeepCopy(),
		Items:    make([]policyv1.PodDisruptionBudget, len(list.Items)),
	}
	for i := range list.Items {
		pdb, err := normalizeBetaPDB(&list.Items[i])
		if err != nil {
			return nil, err
		}
		result.Items[i] = *pdb
	}
	return result, nil
}

// Watch converts added, modified, deleted and bookmark events to the same type
// as List. Error Status objects pass through unchanged, including 410 expiration.
func (c *PodDisruptionBudgetClient) Watch(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	if c.version == "v1" {
		return c.client.PolicyV1().PodDisruptionBudgets(c.namespace).Watch(ctx, options)
	}
	incoming, err := c.client.PolicyV1beta1().PodDisruptionBudgets(c.namespace).Watch(ctx, options)
	if err != nil {
		return nil, err
	}
	w := &pdbWatch{incoming: incoming, result: make(chan watch.Event), done: make(chan struct{})}
	go w.run(ctx)
	return w, nil
}

// JSON field conversion copies every shared SDK field (including status,
// conditions, disruptedPods and unhealthyPodEvictionPolicy) without aliasing.
// beta {} means no pods; representing it as v1 nil preserves that meaning.
// A v1 {} is left untouched and continues to mean all pods in the namespace.
func normalizeBetaPDB(pdb *policybeta.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error) {
	data, err := json.Marshal(pdb)
	if err != nil {
		return nil, err
	}
	result := &policyv1.PodDisruptionBudget{}
	if err := json.Unmarshal(data, result); err != nil {
		return nil, err
	}
	result.TypeMeta = metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"}
	if selector := result.Spec.Selector; selector != nil && len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0 {
		result.Spec.Selector = nil
	}
	return result, nil
}

type pdbWatch struct {
	incoming watch.Interface
	result   chan watch.Event
	done     chan struct{}
	once     sync.Once
}

func (w *pdbWatch) ResultChan() <-chan watch.Event { return w.result }

func (w *pdbWatch) Stop() {
	w.once.Do(func() {
		close(w.done)
		w.incoming.Stop()
	})
}

func (w *pdbWatch) run(ctx context.Context) {
	defer close(w.result)
	defer w.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		case event, ok := <-w.incoming.ResultChan():
			if !ok {
				return
			}
			if event.Type != watch.Error {
				pdb, ok := event.Object.(*policybeta.PodDisruptionBudget)
				var err error
				if !ok {
					err = fmt.Errorf("unexpected policy/v1beta1 PDB watch object %T", event.Object)
				} else {
					event.Object, err = normalizeBetaPDB(pdb)
				}
				if err != nil {
					event = watch.Event{Type: watch.Error, Object: &metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonInternalError, Code: 500, Message: err.Error()}}
				}
			}
			select {
			case <-w.done:
				return
			case <-ctx.Done():
				return
			case w.result <- event:
			}
		}
	}
}
