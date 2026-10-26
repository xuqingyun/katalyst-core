/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamicpolicy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestMemoryInitAndRecreateAllocationLifecycle(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	machineInfo, err := machine.GenerateDummyMachineInfo(2, 32)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, machineInfo, t.TempDir())
	require.NoError(t, err)
	ctx := context.Background()
	req := &pluginapi.ResourceRequest{
		PodUid: "old-uid", PodNamespace: "allocation-lifecycle", PodName: "same-name", ContainerName: "init",
		ContainerType: pluginapi.ContainerType_INIT, ResourceName: string(v1.ResourceMemory),
		ResourceRequests: map[string]float64{string(v1.ResourceMemory): 32 * 1024 * 1024},
	}
	before := p.state.GetPodResourceEntries()
	resp, err := p.Allocate(ctx, req)
	require.NoError(t, err)
	require.Nil(t, resp.AllocationResult)
	require.Equal(t, before, p.state.GetPodResourceEntries(), "init must not leave a persistent memory allocation")
	req.ContainerName = "load"
	req.ContainerType = pluginapi.ContainerType_MAIN
	req.ResourceRequests[string(v1.ResourceMemory)] = 64 * 1024 * 1024
	_, err = p.GetTopologyHints(ctx, req)
	require.NoError(t, err)
	resp, err = p.Allocate(ctx, req)
	require.NoError(t, err)
	original := p.state.GetAllocationInfo(v1.ResourceMemory, req.PodUid, req.ContainerName).Clone()
	require.Equal(t, apiconsts.PodAnnotationQoSLevelSharedCores, original.QoSLevel)
	require.Equal(t, topology.CPUDetails.NUMANodes(), original.NumaAllocationResult)
	repeated, err := p.Allocate(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resp.AllocationResult, repeated.AllocationResult)
	require.Equal(t, original, p.state.GetAllocationInfo(v1.ResourceMemory, req.PodUid, req.ContainerName))
	_, err = p.RemovePod(ctx, &pluginapi.RemovePodRequest{PodUid: req.PodUid})
	require.NoError(t, err)
	require.Nil(t, p.state.GetAllocationInfo(v1.ResourceMemory, "old-uid", "load"))
	req.PodUid = "new-uid"
	req.Annotations = map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSystemCores}
	p.qosConfig.SetExpandQoSLevelSelector(apiconsts.PodAnnotationQoSLevelSystemCores, map[string]string{
		apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSystemCores,
	})
	_, err = p.Allocate(ctx, req)
	require.NoError(t, err)
	require.Equal(t, apiconsts.PodAnnotationQoSLevelSystemCores, p.state.GetAllocationInfo(v1.ResourceMemory, "new-uid", "load").QoSLevel)
	require.Nil(t, p.state.GetAllocationInfo(v1.ResourceMemory, "old-uid", "load"))
}
