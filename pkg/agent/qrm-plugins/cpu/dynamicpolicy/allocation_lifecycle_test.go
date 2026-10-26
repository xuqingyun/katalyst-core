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
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func lifecycleCPURequest(uid, name string, containerType pluginapi.ContainerType) *pluginapi.ResourceRequest {
	return &pluginapi.ResourceRequest{
		PodUid: uid, PodNamespace: "allocation-lifecycle", PodName: "same-name", ContainerName: name,
		ContainerType: containerType, ResourceName: string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{string(v1.ResourceCPU): 0.25},
		Annotations:      map[string]string{apiconsts.PodAnnotationAggregatedRequestsKey: `{"cpu":"250m"}`},
	}
}

func TestNonBindingSharedHintsExcludeRestrictedPools(t *testing.T) {
	for _, pool := range []string{commonstate.PoolNameInterrupt, commonstate.PoolNamePrefixSystem + "test"} {
		t.Run(pool, func(t *testing.T) {
			topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
			require.NoError(t, err)
			p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
			require.NoError(t, err)
			available := topology.CPUDetails.CPUs().Difference(p.reservedCPUs)
			p.state.SetAllocationInfo(pool, commonstate.FakedContainerName, &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(pool),
				AllocationResult: available.Clone(), OriginalAllocationResult: available.Clone(),
			}, false)
			req := lifecycleCPURequest("restricted", "load", pluginapi.ContainerType_MAIN)
			hints, err := p.GetTopologyHints(context.Background(), req)
			require.ErrorContains(t, err, "no available cpu hints")
			require.Nil(t, hints)
			resp, err := p.Allocate(context.Background(), req)
			require.ErrorContains(t, err, "get empty pooledCPUs")
			require.Nil(t, resp)
			require.Nil(t, p.state.GetAllocationInfo(req.PodUid, req.ContainerName))

			// Restrict only part of the available set: valid user CPUs remain usable.
			keep := machine.NewCPUSet(available.ToSliceInt()[0])
			restricted := available.Difference(keep)
			p.state.SetAllocationInfo(pool, commonstate.FakedContainerName, &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(pool),
				AllocationResult: restricted, OriginalAllocationResult: restricted.Clone(),
			}, false)
			hints, err = p.GetTopologyHints(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, hints)
			resp, err = p.Allocate(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, keep.String(), resp.AllocationResult.ResourceAllocation[string(v1.ResourceCPU)].AllocationResult)
		})
	}
}

func TestCPUInitAndRecreateAllocationLifecycle(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	ctx := context.Background()
	before := p.state.GetPodEntries()
	initReq := lifecycleCPURequest("old-uid", "init", pluginapi.ContainerType_INIT)
	resp, err := p.Allocate(ctx, initReq)
	require.NoError(t, err)
	require.Nil(t, resp.AllocationResult)
	require.Equal(t, before, p.state.GetPodEntries(), "init must not leave a persistent allocation")
	req := lifecycleCPURequest("old-uid", "load", pluginapi.ContainerType_MAIN)
	_, err = p.GetTopologyHints(ctx, req)
	require.NoError(t, err)
	resp, err = p.Allocate(ctx, req)
	require.NoError(t, err)
	allocation := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	require.NotNil(t, allocation)
	require.False(t, allocation.AllocationResult.IsEmpty())
	require.True(t, allocation.AllocationResult.Intersection(p.reservedCPUs).IsEmpty())
	original := allocation.Clone()
	// Annotation changes do not migrate an existing CPU allocation on readmission.
	p.qosConfig.SetExpandQoSLevelSelector(apiconsts.PodAnnotationQoSLevelSystemCores, map[string]string{
		apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSystemCores,
	})
	req.Annotations[apiconsts.PodAnnotationQoSLevelKey] = apiconsts.PodAnnotationQoSLevelSystemCores
	req.Annotations[apiconsts.PodAnnotationCPUEnhancementKey] = `{"cpuset_pool":"reserve"}`
	repeated, err := p.Allocate(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resp.AllocationResult, repeated.AllocationResult)
	require.Equal(t, original, p.state.GetAllocationInfo(req.PodUid, req.ContainerName))
	_, err = p.RemovePod(ctx, &pluginapi.RemovePodRequest{PodUid: req.PodUid})
	require.NoError(t, err)
	require.Nil(t, p.state.GetAllocationInfo("old-uid", "load"))
	req.PodUid = "new-uid"
	// Each kubelet RPC carries fresh Pod annotations, before enhancement parsing.
	req.Annotations = map[string]string{
		apiconsts.PodAnnotationQoSLevelKey:       apiconsts.PodAnnotationQoSLevelSystemCores,
		apiconsts.PodAnnotationCPUEnhancementKey: `{"cpuset_pool":"reserve"}`,
	}
	recreated, err := p.Allocate(ctx, req)
	require.NoError(t, err)
	require.Equal(t, p.reservedCPUs.String(), recreated.AllocationResult.ResourceAllocation[string(v1.ResourceCPU)].AllocationResult)
	require.Equal(t, apiconsts.PodAnnotationQoSLevelSystemCores, p.state.GetAllocationInfo("new-uid", "load").QoSLevel)
}

func TestNonBindingSharedRejectsOccupiedNUMA(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.qosConfig.SetExpandQoSLevelSelector(apiconsts.PodAnnotationQoSLevelDedicatedCores, map[string]string{
		apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores,
	})
	bound := lifecycleCPURequest("bound", "load", pluginapi.ContainerType_MAIN)
	bound.ResourceRequests[string(v1.ResourceCPU)] = 1
	bound.Hint = &pluginapi.TopologyHint{Nodes: []uint64{0}, Preferred: true}
	bound.Annotations = map[string]string{
		apiconsts.PodAnnotationQoSLevelKey:           apiconsts.PodAnnotationQoSLevelDedicatedCores,
		apiconsts.PodAnnotationMemoryEnhancementKey:  `{"numa_binding":"true","numa_exclusive":"false"}`,
		apiconsts.PodAnnotationAggregatedRequestsKey: `{"cpu":"1"}`,
	}
	_, err = p.Allocate(context.Background(), bound)
	require.NoError(t, err)
	original := p.state.GetAllocationInfo(bound.PodUid, bound.ContainerName).Clone()
	req := lifecycleCPURequest("conflict", "load", pluginapi.ContainerType_MAIN)
	_, err = p.GetTopologyHints(context.Background(), req)
	require.ErrorContains(t, err, "no available cpu hints")
	_, err = p.Allocate(context.Background(), req)
	require.ErrorContains(t, err, "get empty pooledCPUs")
	require.Nil(t, p.state.GetAllocationInfo(req.PodUid, req.ContainerName))
	require.Equal(t, original, p.state.GetAllocationInfo(bound.PodUid, bound.ContainerName), "conflicting input must retain the bound allocation")
}
