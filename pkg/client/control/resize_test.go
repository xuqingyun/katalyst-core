package control

import (
	"context"
	"encoding/json"
	"errors"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
)

func TestResizeUsesSubresourceAndDoesNotFallbackOnRejection(t *testing.T) {
	client := fake.NewSimpleClientset()
	denied := apierrors.NewBadRequest("resize rejected")
	client.PrependReactor("patch", "pods", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "resize" {
			t.Fatal("ordinary pod PATCH used")
		}
		var patch map[string]interface{}
		if err := json.Unmarshal(a.(clienttesting.PatchAction).GetPatch(), &patch); err != nil {
			t.Fatal(err)
		}
		if _, ok := patch["metadata"]; ok {
			t.Fatal("unexpected metadata changes")
		}
		return true, nil, denied
	})
	old := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}, Spec: core.PodSpec{Containers: []core.Container{{Name: "main", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("1")}}}}}}
	next := old.DeepCopy()
	next.Spec.Containers[0].Resources.Requests[core.ResourceCPU] = resource.MustParse("2")
	err := NewRealPodUpdater(client).ResizePod(context.Background(), old, next)
	if !errors.Is(err, denied) || len(client.Actions()) != 1 {
		t.Fatalf("rejected resize retried: %v %v", err, client.Actions())
	}
}
