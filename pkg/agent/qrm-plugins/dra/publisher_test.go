package dra

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"testing"
)

func publisherTestInventory(n int) Inventory {
	i := Inventory{PoolName: "node-a"}
	for k := 0; k < n; k++ {
		i.Devices = append(i.Devices, resourcev1.Device{Name: fmt.Sprintf("channel-%d", k)})
	}
	return i
}
func TestShardsGenerationAndOwnership(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-uid"}}
	client := kubefake.NewSimpleClientset(node)
	cfg := &PublisherConfig{DriverName: "test.example.com", OwnerName: AgentName}
	cfg.NodeName = node.Name
	p := &Publisher{Config: cfg, Client: client}
	require.NoError(t, p.Reconcile(ctx, publisherTestInventory(257)))
	list, err := client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 3)
	for _, s := range list.Items {
		require.LessOrEqual(t, len(s.Spec.Devices), 128)
		require.Equal(t, int64(3), s.Spec.Pool.ResourceSliceCount)
		require.Equal(t, int64(1), s.Spec.Pool.Generation)
		require.Equal(t, node.UID, s.OwnerReferences[0].UID)
	}
	before := len(client.Actions())
	require.NoError(t, p.Reconcile(ctx, publisherTestInventory(257)))
	for _, a := range client.Actions()[before:] {
		require.NotEqual(t, "update", a.GetVerb())
	}
	require.NoError(t, p.Reconcile(ctx, publisherTestInventory(1)))
	list, err = client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.Equal(t, int64(2), list.Items[0].Spec.Pool.Generation)
}
func TestForeignSlicePreventsConflictingPublication(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-uid"}}
	foreign := BuildSlices(node, publisherTestInventory(1), 1, "test.example.com", AgentName)[0]
	foreign.Name = "nvidia-existing"
	foreign.Labels = nil
	client := kubefake.NewSimpleClientset(node, &foreign)
	cfg := &PublisherConfig{DriverName: "test.example.com", OwnerName: AgentName}
	cfg.NodeName = node.Name
	p := &Publisher{Config: cfg, Client: client}
	require.Error(t, p.Reconcile(context.Background(), publisherTestInventory(2)))
	for _, a := range client.Actions() {
		require.NotContains(t, []string{"create", "update", "delete"}, a.GetVerb())
	}
}

func TestPublicationResumesPartiallyCommittedGeneration(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-uid"}}
	client := kubefake.NewSimpleClientset(node)
	cfg := &PublisherConfig{DriverName: "test.example.com", OwnerName: AgentName}
	cfg.NodeName = node.Name
	p := &Publisher{Config: cfg, Client: client}
	count := 0
	client.PrependReactor("create", "resourceslices", func(action ktesting.Action) (bool, runtime.Object, error) {
		count++
		if count == 2 {
			return true, nil, fmt.Errorf("injected API failure")
		}
		return false, nil, nil
	})
	require.Error(t, p.Reconcile(ctx, publisherTestInventory(257)))
	require.NoError(t, p.Reconcile(ctx, publisherTestInventory(257)))
	slices, err := client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, slices.Items, 3)
	for _, s := range slices.Items {
		require.Equal(t, int64(1), s.Spec.Pool.Generation)
		require.Equal(t, int64(3), s.Spec.Pool.ResourceSliceCount)
	}
}
