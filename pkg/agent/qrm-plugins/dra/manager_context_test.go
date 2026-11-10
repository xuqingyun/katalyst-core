// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/katalyst-api/pkg/plugins/skeleton"
	"github.com/stretchr/testify/require"
)

type managerContextPlugin struct {
	name        string
	start       func(context.Context) error
	stop        func() error
	legacyCalls atomic.Int32
}

func (p *managerContextPlugin) Name() string { return p.name }
func (p *managerContextPlugin) Start() error {
	p.legacyCalls.Add(1)
	return errors.New("contextual plugin started without its lifecycle context")
}
func (p *managerContextPlugin) StartWithContext(ctx context.Context) error { return p.start(ctx) }
func (p *managerContextPlugin) Stop() error                                { return p.stop() }

func managerContextConfig(t *testing.T, plugins ...PluginConfig) Config {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "dra-ctx-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return Config{RegistrationDir: root, StateDir: filepath.Join(root, "state"), Plugins: plugins}
}

func awaitManagerContextError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("manager lifecycle operation did not finish after cancellation")
		return nil
	}
}

func awaitManagerStartupContext(t *testing.T, entered <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-entered:
		return ctx
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not enter blocking startup")
		return nil
	}
}

func TestManagerStopCancelsOptionalContextStartup(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered := make(chan context.Context, 1)
	var stopped atomic.Int32
	plugin := &managerContextPlugin{
		name: "a.example.com",
		start: func(ctx context.Context) error {
			entered <- ctx
			<-ctx.Done()
			return ctx.Err()
		},
		stop: func() error { stopped.Add(1); return nil },
	}
	var factoryContext context.Context
	manager, err := NewManagerWithProviders(managerContextConfig(t, PluginConfig{
		ID: "fake", Enabled: true, AutoDetect: true, DriverName: plugin.name,
	}), Runtime{}, map[string]ProviderFactory{"fake": func(_ PluginConfig, runtime Runtime) (skeleton.GenericPlugin, error) {
		factoryContext = runtime.Context
		return plugin, nil
	}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- manager.StartWithContext(parent) }()
	pluginContext := awaitManagerStartupContext(t, entered)
	require.Same(t, factoryContext, pluginContext)
	require.NoError(t, pluginContext.Err())
	stopping := make(chan error, 1)
	go func() { stopping <- manager.Stop() }()
	require.NoError(t, awaitManagerContextError(t, stopping))
	require.ErrorIs(t, awaitManagerContextError(t, started), context.Canceled)
	require.NoError(t, parent.Err(), "Stop must cancel the manager lifecycle, not its caller")
	require.EqualValues(t, 0, plugin.legacyCalls.Load())
	require.EqualValues(t, 1, stopped.Load())
	require.Empty(t, manager.active)
	require.False(t, manager.started)
}

func TestManagerStopCancelsBlockingProviderFactory(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered := make(chan context.Context, 1)
	manager, err := NewManagerWithProviders(managerContextConfig(t, PluginConfig{
		ID: "fake", Enabled: true, AutoDetect: true, DriverName: "a.example.com",
	}), Runtime{}, map[string]ProviderFactory{"fake": func(_ PluginConfig, runtime Runtime) (skeleton.GenericPlugin, error) {
		entered <- runtime.Context
		<-runtime.Context.Done()
		// Cancellation must not be mistaken for an optional hardware skip.
		return nil, errors.Join(ErrUnsupportedNode, runtime.Context.Err())
	}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- manager.StartWithContext(parent) }()
	ctx := awaitManagerStartupContext(t, entered)
	require.NoError(t, ctx.Err())
	stopping := make(chan error, 1)
	go func() { stopping <- manager.Stop() }()
	require.NoError(t, awaitManagerContextError(t, stopping))
	require.ErrorIs(t, awaitManagerContextError(t, started), context.Canceled)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Empty(t, manager.active)
	require.False(t, manager.started)
}

func TestManagerCanceledStartupRollsBackInReverseOrder(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered := make(chan context.Context, 1)
	stops := make(chan string, 8)
	plugins := map[string]*managerContextPlugin{
		"a": {name: "a.example.com", start: func(context.Context) error { return nil }, stop: func() error { stops <- "a"; return nil }},
		"b": {name: "b.example.com", start: func(ctx context.Context) error { entered <- ctx; <-ctx.Done(); return ctx.Err() }, stop: func() error { stops <- "b"; return nil }},
	}
	manager, err := NewManagerWithProviders(managerContextConfig(t,
		PluginConfig{ID: "a", Provider: "fake", Enabled: true, Required: true, DriverName: plugins["a"].name},
		PluginConfig{ID: "b", Provider: "fake", Enabled: true, DriverName: plugins["b"].name},
	), Runtime{}, map[string]ProviderFactory{"fake": func(config PluginConfig, _ Runtime) (skeleton.GenericPlugin, error) {
		return plugins[config.ID], nil
	}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- manager.StartWithContext(parent) }()
	awaitManagerStartupContext(t, entered)
	cancel()
	require.ErrorIs(t, awaitManagerContextError(t, started), context.Canceled)
	require.Equal(t, "b", <-stops)
	require.Equal(t, "a", <-stops)
	require.Empty(t, manager.active)
	require.False(t, manager.started)
	require.NoError(t, manager.Stop())
	require.Empty(t, stops, "completed rollback must not stop drivers again")
}

func TestManagerCanceledStartupRetainsFailedCleanupUntilRetry(t *testing.T) {
	for _, failingID := range []string{"a", "b"} {
		t.Run(failingID, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			entered := make(chan context.Context, 1)
			cleanupErr := errors.New("fake driver cleanup failed")
			var cleanupAllowed atomic.Bool
			var factoryCalls atomic.Int32
			plugins := map[string]*managerContextPlugin{}
			for _, id := range []string{"a", "b"} {
				id := id
				plugins[id] = &managerContextPlugin{name: id + ".example.com", stop: func() error {
					if id == failingID && !cleanupAllowed.Load() {
						return cleanupErr
					}
					return nil
				}}
			}
			plugins["a"].start = func(context.Context) error { return nil }
			plugins["b"].start = func(ctx context.Context) error { entered <- ctx; <-ctx.Done(); return ctx.Err() }
			manager, err := NewManagerWithProviders(managerContextConfig(t,
				PluginConfig{ID: "a", Provider: "fake", Enabled: true, Required: true, DriverName: plugins["a"].name},
				PluginConfig{ID: "b", Provider: "fake", Enabled: true, DriverName: plugins["b"].name},
			), Runtime{}, map[string]ProviderFactory{"fake": func(config PluginConfig, _ Runtime) (skeleton.GenericPlugin, error) {
				factoryCalls.Add(1)
				return plugins[config.ID], nil
			}})
			require.NoError(t, err)
			started := make(chan error, 1)
			go func() { started <- manager.StartWithContext(parent) }()
			awaitManagerStartupContext(t, entered)
			cancel()
			err = awaitManagerContextError(t, started)
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, cleanupErr)
			require.Equal(t, []skeleton.GenericPlugin{plugins[failingID]}, manager.active)
			require.False(t, manager.started)
			require.Error(t, manager.StartWithContext(context.Background()))
			require.EqualValues(t, 2, factoryCalls.Load(), "restart must not construct replacement drivers while cleanup is incomplete")
			require.ErrorIs(t, manager.Stop(), cleanupErr)
			require.Equal(t, []skeleton.GenericPlugin{plugins[failingID]}, manager.active)
			cleanupAllowed.Store(true)
			require.NoError(t, manager.Stop())
			require.Empty(t, manager.active)
		})
	}
}

func TestManagerPendingStopsCancelStartupBeforeFactory(t *testing.T) {
	var factoryCalls atomic.Int32
	manager, err := NewManagerWithProviders(managerContextConfig(t, PluginConfig{
		ID: "fake", Enabled: true, DriverName: "a.example.com",
	}), Runtime{}, map[string]ProviderFactory{"fake": func(config PluginConfig, _ Runtime) (skeleton.GenericPlugin, error) {
		factoryCalls.Add(1)
		return &managerContextPlugin{name: config.DriverName, start: func(context.Context) error { return nil }, stop: func() error { return nil }}, nil
	}})
	require.NoError(t, err)
	// Reproduce the state of two Stop callers waiting for the lifecycle lock
	// before StartWithContext has published its cancel function.
	manager.contextMu.Lock()
	manager.stopRequests = 2
	manager.contextMu.Unlock()
	require.ErrorIs(t, manager.StartWithContext(context.Background()), context.Canceled)
	require.EqualValues(t, 0, factoryCalls.Load())
	manager.contextMu.Lock()
	manager.stopRequests--
	manager.contextMu.Unlock()
	require.ErrorIs(t, manager.StartWithContext(context.Background()), context.Canceled)
	require.EqualValues(t, 0, factoryCalls.Load(), "one pending Stop must still prevent startup")
	manager.contextMu.Lock()
	manager.stopRequests--
	manager.contextMu.Unlock()
	require.NoError(t, manager.StartWithContext(context.Background()))
	require.EqualValues(t, 1, factoryCalls.Load())
	require.NoError(t, manager.Stop())
}

func TestManagerRestartGetsFreshRuntimeContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var contexts []context.Context
	var plugins []*managerContextPlugin
	manager, err := NewManagerWithProviders(managerContextConfig(t, PluginConfig{
		ID: "fake", Enabled: true, Required: true, DriverName: "a.example.com",
	}), Runtime{Context: context.Background()}, map[string]ProviderFactory{"fake": func(config PluginConfig, runtime Runtime) (skeleton.GenericPlugin, error) {
		contexts = append(contexts, runtime.Context)
		plugin := &managerContextPlugin{name: config.DriverName, start: func(ctx context.Context) error {
			require.Same(t, runtime.Context, ctx)
			return ctx.Err()
		}, stop: func() error { return nil }}
		plugins = append(plugins, plugin)
		return plugin, nil
	}})
	require.NoError(t, err)
	require.NoError(t, manager.StartWithContext(parent))
	require.Len(t, contexts, 1)
	require.NoError(t, contexts[0].Err())
	require.NoError(t, manager.Stop())
	require.ErrorIs(t, contexts[0].Err(), context.Canceled)
	require.NoError(t, parent.Err())
	require.NoError(t, manager.StartWithContext(parent))
	require.Len(t, contexts, 2)
	require.NotSame(t, contexts[0], contexts[1])
	require.NoError(t, contexts[1].Err())
	require.NotSame(t, plugins[0], plugins[1])
	require.NoError(t, manager.Stop())
	require.ErrorIs(t, contexts[1].Err(), context.Canceled)
}

func TestManagerCanceledFactoryCompletionCleansReturnedPlugin(t *testing.T) {
	for _, factoryReturnsError := range []bool{false, true} {
		for _, cleanupFails := range []bool{false, true} {
			name := "success"
			if factoryReturnsError {
				name = "context-error"
			}
			if cleanupFails {
				name += "/cleanup-error"
			}
			t.Run(name, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				cleanupErr := errors.New("fake factory object cleanup failed")
				var cleanupAllowed atomic.Bool
				cleanupAllowed.Store(!cleanupFails)
				var startupCalls, stopCalls atomic.Int32
				plugin := &managerContextPlugin{name: "a.example.com", start: func(context.Context) error {
					startupCalls.Add(1)
					return nil
				}, stop: func() error {
					stopCalls.Add(1)
					if !cleanupAllowed.Load() {
						return cleanupErr
					}
					return nil
				}}
				manager, err := NewManagerWithProviders(managerContextConfig(t, PluginConfig{
					ID: "fake", Enabled: true, Required: true, DriverName: plugin.name,
				}), Runtime{}, map[string]ProviderFactory{"fake": func(_ PluginConfig, runtime Runtime) (skeleton.GenericPlugin, error) {
					// Construction obtained an object just as cancellation arrived.
					// Ownership still belongs to the manager, even with an error.
					cancel()
					if factoryReturnsError {
						return plugin, runtime.Context.Err()
					}
					return plugin, nil
				}})
				require.NoError(t, err)
				err = manager.StartWithContext(parent)
				require.ErrorIs(t, err, context.Canceled)
				require.EqualValues(t, 0, startupCalls.Load())
				require.EqualValues(t, 0, plugin.legacyCalls.Load())
				require.Greater(t, stopCalls.Load(), int32(0), "manager must release the object returned by the canceled factory")
				if cleanupFails {
					require.ErrorIs(t, err, cleanupErr)
					require.Equal(t, []skeleton.GenericPlugin{plugin}, manager.active)
					require.Error(t, manager.StartWithContext(context.Background()))
					cleanupAllowed.Store(true)
				}
				require.NoError(t, manager.Stop())
				require.Empty(t, manager.active)
			})
		}
	}
}

func TestManagerCanceledEmptyStartupDoesNotBecomeStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager, err := NewManagerWithProviders(managerContextConfig(t), Runtime{}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, manager.StartWithContext(ctx), context.Canceled)
	require.False(t, manager.started)
	require.NoError(t, manager.Stop())
	require.NoError(t, manager.StartWithContext(context.Background()))
	require.True(t, manager.started)
	require.NoError(t, manager.Stop())
}
