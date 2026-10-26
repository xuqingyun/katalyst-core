package compat

import (
	"context"
	v2 "k8s.io/api/autoscaling/v2"
	beta "k8s.io/api/autoscaling/v2beta2"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
	"time"
)

func TestHPABetaPreservesExternalMetricsAndBehavior(t *testing.T) {
	seconds := int32(60)
	replicas := int32(2)
	quantity := resource.MustParse("10")
	h := &v2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "hpa", Namespace: "ns", UID: "id", OwnerReferences: []metav1.OwnerReference{{Name: "ihpa"}}}, Spec: v2.HorizontalPodAutoscalerSpec{MinReplicas: &replicas, MaxReplicas: 10, Metrics: []v2.MetricSpec{{Type: v2.ExternalMetricSourceType, External: &v2.ExternalMetricSource{Metric: v2.MetricIdentifier{Name: "queue", Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}}}, Target: v2.MetricTarget{Type: v2.AverageValueMetricType, AverageValue: &quantity}}}}, Behavior: &v2.HorizontalPodAutoscalerBehavior{ScaleDown: &v2.HPAScalingRules{StabilizationWindowSeconds: &seconds, Policies: []v2.HPAScalingPolicy{{Type: v2.PercentScalingPolicy, Value: 20, PeriodSeconds: 30}}}}}}
	b, err := HPAToBeta(h)
	if err != nil {
		t.Fatal(err)
	}
	if b.Spec.Metrics[0].External.Metric.Name != "queue" || *b.Spec.Behavior.ScaleDown.StabilizationWindowSeconds != 60 || b.Spec.Behavior.ScaleDown.Policies[0].Value != 20 || b.UID != "id" {
		t.Fatal(b)
	}
	back, err := HPAFromBeta(b)
	if err != nil || back.Spec.Metrics[0].External.Target.AverageValue.Cmp(quantity) != 0 {
		t.Fatalf("lost metric: %v %v", back, err)
	}
	client := fake.NewSimpleClientset(b)
	stream := watch.NewRaceFreeFake()
	client.PrependWatchReactor("horizontalpodautoscalers", clienttesting.DefaultWatchReactor(stream, nil))
	lw := HPABetaListWatch(client, "ns")
	list, err := lw.ListWithContext(context.Background(), metav1.ListOptions{})
	if err != nil || len(list.(*v2.HorizontalPodAutoscalerList).Items) != 1 {
		t.Fatalf("list conversion failed: %v %v", list, err)
	}
	w, err := lw.WatchWithContext(context.Background(), metav1.ListOptions{ResourceVersion: "7"})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	stream.Modify(b)
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Modified || event.Object.(*v2.HorizontalPodAutoscaler).Spec.Metrics[0].External.Metric.Name != "queue" {
			t.Fatal(event)
		}
	case <-time.After(time.Second):
		t.Fatal("no watch event")
	}
	failure := &metav1.Status{Code: 410, Reason: metav1.StatusReasonExpired}
	stream.Error(failure)
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Error || event.Object != failure {
			t.Fatal("watch error hidden")
		}
	case <-time.After(time.Second):
		t.Fatal("no error event")
	}
	_ = beta.HorizontalPodAutoscaler{}
	_ = core.Pod{}
	_ = runtime.Object(nil)
}
func TestHPABetaRejectsNewTolerance(t *testing.T) {
	q := resource.MustParse("0.1")
	h := &v2.HorizontalPodAutoscaler{Spec: v2.HorizontalPodAutoscalerSpec{Behavior: &v2.HorizontalPodAutoscalerBehavior{ScaleUp: &v2.HPAScalingRules{Tolerance: &q}}}}
	if _, err := HPAToBeta(h); err == nil {
		t.Fatal("unsupported field silently dropped")
	}
}

func TestHPABetaRejectsContainerResourceAbsentInInternal118(t *testing.T) {
	in := &v2.HorizontalPodAutoscaler{Spec: v2.HorizontalPodAutoscalerSpec{Metrics: []v2.MetricSpec{{Type: v2.ContainerResourceMetricSourceType, ContainerResource: &v2.ContainerResourceMetricSource{Name: core.ResourceCPU, Container: "main"}}}}}
	if _, err := HPAToBeta(in); err == nil {
		t.Fatal("new beta2 Go type hid the 1.18 server field gap")
	}
}
