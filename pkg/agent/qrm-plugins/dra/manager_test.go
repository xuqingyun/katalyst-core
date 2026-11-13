// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync/atomic"
	"testing"
	"time"
)

func candidate(id, model string) PluginConfig {
	return PluginConfig{ID: id, Provider: id, Enabled: true, Required: true, DriverName: id + ".example.com", NodeSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "node.kubernetes.io/instance-type", Operator: metav1.LabelSelectorOpIn, Values: []string{model}}}}}
}
func runtimeFor(model string) Runtime {
	return Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{"node.kubernetes.io/instance-type": model}}}, nil
	}}
}
func managerConfig(plugins ...PluginConfig) Config {
	return Config{RegistrationDir: "/tmp/registry", StateDir: "/tmp/state", Plugins: plugins}
}
func noop() Plugin {
	return PluginFuncs{StartFunc: func(context.Context) error { return nil }, StopFunc: func(context.Context) error { return nil }}
}
func waitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("lifecycle did not finish")
		return nil
	}
}

func TestExclusiveSelectionBeforeAnyVendorConstruction(t *testing.T) {
	for _, tc := range []struct {
		name, model string
		secondSame  bool
		want        string
		conflict    bool
	}{{"unmatched", "cpu", false, "", false}, {"first", "gpu-a", false, "a", false}, {"second", "gpu-b", false, "b", false}, {"overlap", "gpu-a", true, "", true}} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := candidate("a", "gpu-a"), candidate("b", "gpu-b")
			if tc.secondSame {
				b.NodeSelector = a.NodeSelector.DeepCopy()
			}
			made := []string{}
			factory := func(p PluginConfig, _ Runtime) (Plugin, error) { made = append(made, p.ID); return noop(), nil }
			m, err := NewManagerWithProviders(managerConfig(a, b), runtimeFor(tc.model), map[string]ProviderFactory{"a": factory, "b": factory})
			require.NoError(t, err)
			err = m.Start()
			if tc.conflict {
				require.ErrorContains(t, err, "multiple plugins")
				require.Empty(t, made)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, m.SelectedPlugin())
				if tc.want == "" {
					require.Empty(t, made)
				} else {
					require.Equal(t, []string{tc.want}, made)
				}
			}
			require.NoError(t, m.Stop())
		})
	}
}
func TestSelectorValidationAndSnapshot(t *testing.T) {
	a := candidate("a", "gpu-a")
	a.Config = map[string]json.RawMessage{"flag": json.RawMessage(`"old"`)}
	factory := func(p PluginConfig, _ Runtime) (Plugin, error) {
		require.Equal(t, `"old"`, string(p.Config["flag"]))
		p.Config["flag"][1] = 'Y'
		p.NodeSelector.MatchExpressions[0].Values[0] = "factory-mutated"
		return noop(), nil
	}
	m, err := NewManagerWithProviders(managerConfig(a), runtimeFor("gpu-a"), map[string]ProviderFactory{"a": factory})
	require.NoError(t, err)
	a.NodeSelector.MatchExpressions[0].Values[0] = "mutated"
	a.Config["flag"][1] = 'X'
	require.NoError(t, m.Start())
	require.Equal(t, "a", m.SelectedPlugin())
	require.NoError(t, m.Stop())
	require.NoError(t, m.Start())
	require.Equal(t, "a", m.SelectedPlugin())
	require.NoError(t, m.Stop())
	for _, selector := range []*metav1.LabelSelector{nil, {}, {MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "machine", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"cpu"}}}}, {MatchLabels: map[string]string{"invalid/key/again": "gpu"}}} {
		p := candidate("a", "gpu-a")
		p.NodeSelector = selector
		_, err := NewManagerWithProviders(managerConfig(p), runtimeFor("gpu-a"), map[string]ProviderFactory{"a": factory})
		require.Error(t, err)
	}
}
func TestSelectionDoesNotHideNodeReadFailureOrCanceledLookup(t *testing.T) {
	failure := errors.New("node lookup failed")
	calls := 0
	runtime := Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) { return nil, failure }}
	m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtime, map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { calls++; return noop(), nil }})
	require.NoError(t, err)
	require.ErrorIs(t, m.Start(), failure)
	require.Zero(t, calls)
	entered := make(chan struct{})
	runtime.GetNode = func(ctx context.Context) (*corev1.Node, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() }
	m, err = NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtime, map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { calls++; return noop(), nil }})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { result <- m.Start() }()
	<-entered
	require.NoError(t, m.Stop())
	require.ErrorIs(t, waitError(t, result), context.Canceled)
	require.Zero(t, calls)
}
func TestNoEnabledPluginNeedsNoNodeRead(t *testing.T) {
	m, err := NewManagerWithProviders(managerConfig(), Runtime{}, nil)
	require.NoError(t, err)
	require.NoError(t, m.Start())
	require.NoError(t, m.Stop())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, m.StartWithContext(ctx), context.Canceled)
}
func TestSelectedUnsupportedProviderNeverFallsBack(t *testing.T) {
	a, b := candidate("a", "gpu-a"), candidate("b", "gpu-b")
	a.Required = false
	a.AutoDetect = true
	var calls atomic.Int32
	m, err := NewManagerWithProviders(managerConfig(a, b), runtimeFor("gpu-a"), map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { calls.Add(1); return nil, ErrUnsupportedNode }, "b": func(PluginConfig, Runtime) (Plugin, error) { t.Fatal("fallback constructed"); return nil, nil }})
	require.NoError(t, err)
	require.NoError(t, m.Start())
	require.EqualValues(t, 1, calls.Load())
	select {
	case <-m.Done():
		t.Fatal("skipped plugin must leave Manager idle until Stop")
	default:
	}
	require.NoError(t, m.Stop())
}
func TestFactoryAndReadyStartupCancellation(t *testing.T) {
	for _, phase := range []string{"factory", "start", "ready"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			var stopped atomic.Int32
			block := func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }
			p := &readyVendor{Plugin: PluginFuncs{StartFunc: func(context.Context) error { return nil }, StopFunc: func(context.Context) error { stopped.Add(1); return nil }}, ready: func(context.Context) error { return nil }}
			if phase == "start" {
				p.Plugin = PluginFuncs{StartFunc: block, StopFunc: func(context.Context) error { stopped.Add(1); return nil }}
			}
			if phase == "ready" {
				p.ready = block
			}
			m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtimeFor("gpu"), map[string]ProviderFactory{"a": func(_ PluginConfig, r Runtime) (Plugin, error) {
				if phase == "factory" {
					return p, block(r.Context)
				}
				return p, nil
			}})
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() { result <- m.Start() }()
			<-entered
			require.NoError(t, m.Stop())
			require.ErrorIs(t, waitError(t, result), context.Canceled)
			require.EqualValues(t, 1, stopped.Load())
		})
	}
}

type readyVendor struct {
	Plugin
	ready func(context.Context) error
}

func (p *readyVendor) Ready(ctx context.Context) error { return p.ready(ctx) }
func TestGracefulStopAndRetryKeepRunningContextAlive(t *testing.T) {
	failure := errors.New("drain incomplete")
	var running context.Context
	stops := 0
	p := PluginFuncs{StartFunc: func(ctx context.Context) error { running = ctx; return nil }, StopFunc: func(ctx context.Context) error {
		require.NoError(t, running.Err(), "cancel must follow vendor drain")
		require.NoError(t, ctx.Err())
		stops++
		if stops == 1 {
			return failure
		}
		return nil
	}}
	m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtimeFor("gpu"), map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { return p, nil }})
	require.NoError(t, err)
	require.NoError(t, m.Start())
	require.ErrorIs(t, m.Stop(), failure)
	require.NoError(t, running.Err())
	require.ErrorContains(t, m.Start(), "shutdown is incomplete")
	require.NoError(t, m.Stop())
	require.ErrorIs(t, running.Err(), context.Canceled)
	require.Equal(t, 2, stops)
	require.NoError(t, m.Start())
	require.NoError(t, running.Err())
	require.NoError(t, m.Stop())
}
func TestParentCancellationDrainsBeforeRunningContextCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var running context.Context
	drained := make(chan struct{})
	p := PluginFuncs{StartFunc: func(ctx context.Context) error { running = ctx; return nil }, StopFunc: func(ctx context.Context) error {
		require.NoError(t, running.Err())
		require.NoError(t, ctx.Err())
		close(drained)
		return nil
	}}
	m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtimeFor("gpu"), map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { return p, nil }})
	require.NoError(t, err)
	require.NoError(t, m.StartWithContext(parent))
	cancel()
	select {
	case <-drained:
	case <-time.After(3 * time.Second):
		t.Fatal("parent cancellation did not drain")
	}
	require.NoError(t, m.Stop())
	require.ErrorIs(t, running.Err(), context.Canceled)
}
func TestStopDeadlineRetainsPluginForRetry(t *testing.T) {
	var running context.Context
	stops := 0
	p := PluginFuncs{StartFunc: func(ctx context.Context) error { running = ctx; return nil }, StopFunc: func(ctx context.Context) error {
		stops++
		if stops == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		require.NoError(t, running.Err())
		return nil
	}}
	m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtimeFor("gpu"), map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { return p, nil }})
	require.NoError(t, err)
	require.NoError(t, m.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, m.StopWithContext(ctx), context.DeadlineExceeded)
	require.Error(t, m.Start())
	require.NoError(t, m.Stop())
	require.ErrorIs(t, running.Err(), context.Canceled)
}
func TestPartialFactoryFailureRetainsCleanupAndPendingStop(t *testing.T) {
	failure := errors.New("factory partially failed")
	cleanup := errors.New("cleanup failed")
	stopCalls := 0
	p := PluginFuncs{StartFunc: func(context.Context) error { t.Fatal("partial plugin started"); return nil }, StopFunc: func(context.Context) error {
		stopCalls++
		if stopCalls == 1 {
			return cleanup
		}
		return nil
	}}
	m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtimeFor("gpu"), map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { return p, failure }})
	require.NoError(t, err)
	err = m.Start()
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, err, cleanup)
	require.Error(t, m.Start())
	require.NoError(t, m.Stop())
	m.contextMu.Lock()
	m.stopRequests++
	m.contextMu.Unlock()
	require.ErrorIs(t, m.Start(), context.Canceled)
	m.contextMu.Lock()
	m.stopRequests--
	m.contextMu.Unlock()
}
func TestSelectionIsFixedUntilRestart(t *testing.T) {
	model := "gpu-a"
	runtime := runtimeFor(model)
	runtime.GetNode = func(context.Context) (*corev1.Node, error) { return runtimeFor(model).GetNode(context.Background()) }
	made := []string{}
	factory := func(p PluginConfig, _ Runtime) (Plugin, error) { made = append(made, p.ID); return noop(), nil }
	m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu-a"), candidate("b", "gpu-b")), runtime, map[string]ProviderFactory{"a": factory, "b": factory})
	require.NoError(t, err)
	require.NoError(t, m.Start())
	model = "gpu-b"
	require.NoError(t, m.Start())
	require.Equal(t, []string{"a"}, made)
	require.NoError(t, m.Stop())
	require.NoError(t, m.Start())
	require.Equal(t, []string{"a", "b"}, made)
	require.NoError(t, m.Stop())
}

type observedVendor struct {
	Plugin
	done chan struct{}
	err  error
}

func (p *observedVendor) Done() <-chan struct{} { return p.done }
func (p *observedVendor) Err() error            { return p.err }
func TestVendorFailureIsObservedAndCleanedUp(t *testing.T) {
	failure := errors.New("vendor exited")
	var stopped atomic.Int32
	p := &observedVendor{Plugin: PluginFuncs{StartFunc: func(context.Context) error { return nil }, StopFunc: func(context.Context) error { stopped.Add(1); return nil }}, done: make(chan struct{}), err: failure}
	m, err := NewManagerWithProviders(managerConfig(candidate("a", "gpu")), runtimeFor("gpu"), map[string]ProviderFactory{"a": func(PluginConfig, Runtime) (Plugin, error) { return p, nil }})
	require.NoError(t, err)
	require.NoError(t, m.Start())
	close(p.done)
	select {
	case <-m.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("vendor exit not observed")
	}
	require.ErrorIs(t, m.Err(), failure)
	require.EqualValues(t, 1, stopped.Load())
	require.NoError(t, m.Stop())
}
