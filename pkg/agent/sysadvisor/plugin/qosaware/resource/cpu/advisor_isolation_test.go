/*
Copyright 2022 The Katalyst Authors.

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

package cpu

import (
	"testing"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	pkgconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

// Exercise the real load isolator, region assignment, safety check and common
// provision assembler together. A NUMA-bound shared pool leaves nonBindingNumas
// empty on a single-NUMA node, but still has capacity for isolation on that NUMA.
func TestAdvisorAutoIsolationNUMABinding(t *testing.T) {
	conf := generateTestConfiguration(t, t.TempDir(), t.TempDir())
	conf.IsolationDisabled = false
	conf.IsolatedMaxResourceRatio = 0.9
	conf.IsolationLockInThreshold = 3
	conf.IsolationLockOutPeriodSecs = 0
	conf.GetDynamicConfiguration().EnableReclaim = false

	mf := metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*metric.FakeMetricsFetcher)
	pods := []*v1.Pod{}
	for _, uid := range []string{"noisy", "shared", "dedicated"} {
		request, limit := "1", "1"
		if uid == "noisy" {
			limit = "4"
		} else if uid == "dedicated" {
			request, limit = "2", "2"
		}
		pods = append(pods, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{UID: k8stypes.UID(uid), Name: uid, Namespace: "default"},
			Spec: v1.PodSpec{Containers: []v1.Container{{Name: "load", Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse(request)},
				Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse(limit)},
			}}}},
		})
	}
	advisor, cache := newTestCPUResourceAdvisor(t, pods, conf, mf, nil)
	topology, err := machine.GenerateDummyCPUTopology(32, 1, 1)
	require.NoError(t, err)
	advisor.metaServer.KatalystMachineInfo = &machine.KatalystMachineInfo{
		MachineInfo: &info.MachineInfo{NumCores: 32}, CPUTopology: topology,
	}
	pool := "share-NUMA0"
	for name, cpus := range map[string]string{commonstate.PoolNameReserve: "0-1", pool: "2-13"} {
		require.NoError(t, cache.SetPoolInfo(name, &types.PoolInfo{PoolName: name,
			TopologyAwareAssignments: types.TopologyAwareAssignment{0: machine.MustParse(cpus)}}))
	}
	annotations := map[string]string{consts.PodAnnotationMemoryEnhancementNumaBinding: consts.PodAnnotationMemoryEnhancementNumaBindingEnable}
	for _, ci := range []*types.ContainerInfo{
		makeContainerInfo("noisy", "default", "noisy", "load", consts.PodAnnotationQoSLevelSharedCores, pool,
			annotations, types.TopologyAwareAssignment{0: machine.MustParse("2-13")}, 1, 4),
		makeContainerInfo("shared", "default", "shared", "load", consts.PodAnnotationQoSLevelSharedCores, pool,
			annotations, types.TopologyAwareAssignment{0: machine.MustParse("2-13")}, 1),
		makeContainerInfo("dedicated", "default", "dedicated", "load", consts.PodAnnotationQoSLevelDedicatedCores, commonstate.PoolNameDedicated,
			annotations, types.TopologyAwareAssignment{0: machine.MustParse("14-15")}, 2),
	} {
		require.NoError(t, cache.SetContainerInfo(ci.PodUID, ci.ContainerName, ci))
	}
	setLoad := func(value float64) {
		now := time.Now()
		mf.SetContainerMetric("noisy", "load", pkgconsts.MetricCPUNrRunnableContainer, utilmetric.MetricData{Value: value, Time: &now})
		mf.SetContainerMetric("shared", "load", pkgconsts.MetricCPUNrRunnableContainer, utilmetric.MetricData{Value: 0, Time: &now})
	}
	assertIsolation := func(want bool) {
		ci, ok := cache.GetContainerInfo("noisy", "load")
		require.True(t, ok)
		require.Equal(t, want, ci.Isolated)
	}
	setLoad(0)
	_, err = advisor.update()
	require.NoError(t, err)
	require.True(t, advisor.nonBindingNumas.IsEmpty())
	for hit := 1; hit <= conf.IsolationLockInThreshold; hit++ {
		setLoad(8) // exceeds max(request=1, limit=4) * 1.5 and max+2
		result, err := advisor.update()
		require.NoError(t, err)
		assertIsolation(hit == conf.IsolationLockInThreshold)
		if hit == conf.IsolationLockInThreshold {
			ci, _ := cache.GetContainerInfo("noisy", "load")
			require.Len(t, ci.RegionNames, 1)
			for regionName := range ci.RegionNames {
				require.Equal(t, configapi.QoSRegionTypeIsolation, advisor.regionMap[regionName].Type())
				require.Equal(t, 4, result.PoolEntries[regionName][0].Size)
			}
			require.Contains(t, result.PoolEntries, pool)
		}
	}
	setLoad(0)
	result, err := advisor.update()
	require.NoError(t, err)
	assertIsolation(false)
	for _, r := range advisor.regionMap {
		require.NotEqual(t, configapi.QoSRegionTypeIsolation, r.Type())
	}
	require.Contains(t, result.PoolEntries, pool)
}

type isolationSafetyTestRegion struct {
	region.QoSRegion
	kind      configapi.QoSRegionType
	numas     machine.CPUSet
	exclusive bool
	request   float64
}

func (r *isolationSafetyTestRegion) Name() string                    { return string(r.kind) }
func (r *isolationSafetyTestRegion) Type() configapi.QoSRegionType   { return r.kind }
func (r *isolationSafetyTestRegion) GetBindingNumas() machine.CPUSet { return r.numas }
func (r *isolationSafetyTestRegion) IsNumaExclusive() bool           { return r.exclusive }
func (r *isolationSafetyTestRegion) IsNumaBinding() bool             { return !r.numas.IsEmpty() }
func (r *isolationSafetyTestRegion) GetPodsRequest() float64         { return r.request }
func (r *isolationSafetyTestRegion) GetPods() types.PodSet {
	return types.PodSet{"noisy": {"load": {}}}
}
func (r *isolationSafetyTestRegion) GetProvision() (types.ControlKnob, error) {
	return types.ControlKnob{configapi.ControlKnobNonReclaimedCPURequirement: {Value: r.request}}, nil
}

func TestIsolationSafetyNUMACapacity(t *testing.T) {
	for _, tc := range []struct {
		name             string
		shareRequirement float64
		dedicated        *isolationSafetyTestRegion
		want             bool
	}{
		{"shared NUMA binding with non-exclusive dedicated", 24,
			&isolationSafetyTestRegion{kind: configapi.QoSRegionTypeDedicated, numas: machine.NewCPUSet(0), request: 4}, true},
		{"requirements exceed total capacity", 61, nil, false},
		{"exclusive NUMA capacity excluded", 40,
			&isolationSafetyTestRegion{kind: configapi.QoSRegionTypeDedicated, numas: machine.NewCPUSet(0), exclusive: true}, false},
		{"other NUMA still available", 24,
			&isolationSafetyTestRegion{kind: configapi.QoSRegionTypeDedicated, numas: machine.NewCPUSet(0), exclusive: true}, true},
		{"all NUMAs exclusive", 24,
			&isolationSafetyTestRegion{kind: configapi.QoSRegionTypeDedicated, numas: machine.NewCPUSet(0, 1), exclusive: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := generateTestConfiguration(t, t.TempDir(), t.TempDir())
			mf := metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*metric.FakeMetricsFetcher)
			advisor, cache := newTestCPUResourceAdvisor(t, nil, conf, mf, nil)
			topology, err := machine.GenerateDummyCPUTopology(64, 2, 2)
			require.NoError(t, err)
			advisor.metaServer.KatalystMachineInfo = &machine.KatalystMachineInfo{
				MachineInfo: &info.MachineInfo{NumCores: 64}, CPUTopology: topology,
			}
			require.NoError(t, cache.SetContainerInfo("noisy", "load", &types.ContainerInfo{
				PodUID: "noisy", ContainerName: "load", CPULimit: 4,
			}))
			advisor.nonBindingNumas = machine.NewCPUSet()
			advisor.regionMap = map[string]region.QoSRegion{
				"share":     &isolationSafetyTestRegion{kind: configapi.QoSRegionTypeShare, numas: machine.NewCPUSet(1), request: tc.shareRequirement},
				"isolation": &isolationSafetyTestRegion{kind: configapi.QoSRegionTypeIsolation, numas: machine.NewCPUSet(1)},
			}
			if tc.dedicated != nil {
				advisor.regionMap["dedicated"] = tc.dedicated
			}
			require.Equal(t, tc.want, advisor.checkIsolationSafety())
		})
	}
}
