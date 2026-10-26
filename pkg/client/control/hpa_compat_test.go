package control

import (
	"context"
	v2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestHPAManagerUsesBetaForCreateAndPatch(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewHPAManagerForVersion(client, "v2beta2")
	seconds := int32(60)
	in := &v2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "hpa", Namespace: "ns", OwnerReferences: []metav1.OwnerReference{{Name: "ihpa"}}}, Spec: v2.HorizontalPodAutoscalerSpec{MaxReplicas: 10, Behavior: &v2.HorizontalPodAutoscalerBehavior{ScaleDown: &v2.HPAScalingRules{StabilizationWindowSeconds: &seconds}}}}
	old, err := manager.Create(context.Background(), in, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next := old.DeepCopy()
	next.Spec.MaxReplicas = 12
	updated, err := manager.Patch(context.Background(), old, next, metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Spec.MaxReplicas != 12 || *updated.Spec.Behavior.ScaleDown.StabilizationWindowSeconds != 60 || updated.OwnerReferences[0].Name != "ihpa" {
		t.Fatal(updated)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Group != "autoscaling" || action.GetResource().Version != "v2beta2" {
			t.Fatalf("v2 requested on beta server: %v", action)
		}
	}
}

func TestHPAManagerBetaRejectsNewContainerMetricBeforeRequest(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewHPAManagerForVersion(client, "v2beta2")
	hpa := &v2.HorizontalPodAutoscaler{Spec: v2.HorizontalPodAutoscalerSpec{Metrics: []v2.MetricSpec{{Type: v2.ContainerResourceMetricSourceType}}}}
	if _, err := manager.Create(context.Background(), hpa, metav1.CreateOptions{}); err == nil || len(client.Actions()) != 0 {
		t.Fatalf("unsupported metric submitted: %v %v", err, client.Actions())
	}
}
