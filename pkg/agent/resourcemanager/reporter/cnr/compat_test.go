package cnr

import (
	"context"
	node "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	fake "github.com/kubewharf/katalyst-api/pkg/client/clientset/versioned/fake"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"
	"sync"
	"testing"
)

func TestRefreshDeletedCNRDoesNotRestoreStaleStatus(t *testing.T) {
	client := fake.NewSimpleClientset()
	stale := &node.CustomNodeResource{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "deleted"}, Status: node.CustomNodeResourceStatus{TopologyZone: []*node.TopologyZone{{Name: "stale"}}}}
	r := &cnrReporterImpl{cnrName: "node", client: client, emitter: metrics.DummyMetrics{}, defaultLabels: map[string]string{"owner": "agent"}}
	r.cacheCNR(stale)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = r.cachedCNR()
		}
	}()
	r.refreshLatestCNR(context.Background())
	wg.Wait()
	got, err := client.NodeV1alpha1().CustomNodeResources().Get(context.Background(), "node", metav1.GetOptions{})
	if err != nil || got.UID == stale.UID || len(got.Status.TopologyZone) != 0 || got.Labels["owner"] != "agent" {
		t.Fatalf("restored deleted state: %v %v", got, err)
	}
	if r.cachedCNR() == nil {
		t.Fatal("cache not repopulated")
	}
}
func TestRefreshCNRCreateFailureKeepsCacheEmpty(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "customnoderesources", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "customnoderesources"}, "node", nil)
	})
	r := &cnrReporterImpl{cnrName: "node", client: client, emitter: metrics.DummyMetrics{}}
	r.cacheCNR(&node.CustomNodeResource{ObjectMeta: metav1.ObjectMeta{Name: "node"}})
	r.refreshLatestCNR(context.Background())
	if r.cachedCNR() != nil {
		t.Fatal("deleted cache survived create failure")
	}
}
func TestRefreshCNRConcurrentCreatorUsesExistingObject(t *testing.T) {
	client := fake.NewSimpleClientset()
	gvr := schema.GroupVersionResource{Group: "node.katalyst.kubewharf.io", Version: "v1alpha1", Resource: "customnoderesources"}
	client.PrependReactor("create", "customnoderesources", func(a clienttesting.Action) (bool, runtime.Object, error) {
		other := &node.CustomNodeResource{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "other"}}
		if err := client.Tracker().Create(gvr, other, ""); err != nil {
			return true, nil, err
		}
		return true, nil, apierrors.NewAlreadyExists(gvr.GroupResource(), "node")
	})
	r := &cnrReporterImpl{cnrName: "node", client: client, emitter: metrics.DummyMetrics{}}
	r.refreshLatestCNR(context.Background())
	if got := r.cachedCNR(); got == nil || got.UID != "other" {
		t.Fatalf("did not use concurrent object: %v", got)
	}
}
