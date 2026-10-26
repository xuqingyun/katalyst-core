package vpa

import (
	apis "github.com/kubewharf/katalyst-api/pkg/apis/autoscaling/v1alpha1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestNativeResizePreservesOtherResourcesAndChecksRestartPolicy(t *testing.T) {
	pod := &core.Pod{Spec: core.PodSpec{Containers: []core.Container{{Name: "main", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("1"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("128Mi"), core.ResourceName("example/gpu"): resource.MustParse("1")}}}}}}
	target := map[string]core.ResourceRequirements{"main": {Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2")}}}
	got, err := nativeResizePod(pod, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Containers[0].Resources.Requests.Cpu().Cmp(resource.MustParse("2")) != 0 || got.Spec.Containers[0].Resources.Requests.Memory().Cmp(resource.MustParse("64Mi")) != 0 || len(got.Spec.Containers[0].Resources.Limits) != 2 {
		t.Fatal(got)
	}
	if pod.Spec.Containers[0].Resources.Requests.Cpu().Cmp(resource.MustParse("1")) != 0 {
		t.Fatal("mutated cache object")
	}
	policies := map[string]apis.ContainerResourcePolicy{"main": {ResourceResizePolicy: apis.ResourceResizePolicyRestart}}
	if _, err := nativeResizePod(pod, target, policies); err == nil {
		t.Fatal("immutable restart policy ignored")
	}
	pod.Spec.Containers[0].ResizePolicy = []core.ContainerResizePolicy{{ResourceName: core.ResourceCPU, RestartPolicy: core.RestartContainer}}
	if _, err := nativeResizePod(pod, target, policies); err != nil {
		t.Fatal(err)
	}
	target["main"] = core.ResourceRequirements{Requests: core.ResourceList{core.ResourceName("example/gpu"): resource.MustParse("2")}}
	if _, err := nativeResizePod(pod, target, nil); err == nil {
		t.Fatal("unsupported resize accepted")
	}
}
