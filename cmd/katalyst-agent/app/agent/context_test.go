// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type wrapperContextPlugin struct {
	start       func(context.Context) error
	stopErr     error
	legacyCalls atomic.Int32
	stopCalls   atomic.Int32
}

func (*wrapperContextPlugin) Name() string { return "fake.example.com" }
func (p *wrapperContextPlugin) Start() error {
	p.legacyCalls.Add(1)
	return errors.New("contextual plugin started without agent context")
}
func (p *wrapperContextPlugin) StartWithContext(ctx context.Context) error { return p.start(ctx) }
func (p *wrapperContextPlugin) Stop() error {
	p.stopCalls.Add(1)
	return p.stopErr
}

func awaitWrapperContext(t *testing.T, started <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-started:
		return ctx
	case <-time.After(5 * time.Second):
		t.Fatal("wrapper did not invoke contextual startup")
		return nil
	}
}

func awaitWrapperReturn(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("wrapper did not return after cancellation")
	}
}

func TestPluginWrapperPassesAgentContextToStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	started := make(chan context.Context, 1)
	plugin := &wrapperContextPlugin{start: func(ctx context.Context) error { started <- ctx; return nil }}
	wrapper := &PluginWrapper{GenericPlugin: plugin}
	done := make(chan struct{})
	go func() { wrapper.Run(ctx); close(done) }()
	require.Same(t, ctx, awaitWrapperContext(t, started))
	select {
	case <-done:
		t.Fatal("wrapper returned while the agent context was still active")
	default:
	}
	require.EqualValues(t, 0, plugin.legacyCalls.Load())
	require.EqualValues(t, 0, plugin.stopCalls.Load())
	cancel()
	awaitWrapperReturn(t, done)
	require.EqualValues(t, 1, plugin.stopCalls.Load())
}

func TestPluginWrapperCanceledStartupReturnsWithoutFatal(t *testing.T) {
	const childEnv = "KATALYST_TEST_WRAPPER_CANCELED_STARTUP"
	if os.Getenv(childEnv) != "1" {
		// A subprocess makes a regression to Fatalf observable as a failing exit
		// status without terminating the rest of the package's tests.
		executable, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, executable, "-test.run=^TestPluginWrapperCanceledStartupReturnsWithoutFatal$", "-test.timeout=15s")
		command.Env = append(os.Environ(), childEnv+"=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "canceled startup terminated the wrapper process: %s", output)
		return
	}
	for _, cleanupErr := range []error{nil, errors.New("fake driver cleanup failed")} {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan context.Context, 1)
		plugin := &wrapperContextPlugin{stopErr: cleanupErr, start: func(ctx context.Context) error {
			started <- ctx
			<-ctx.Done()
			return ctx.Err()
		}}
		wrapper := &PluginWrapper{GenericPlugin: plugin}
		done := make(chan struct{})
		go func() { wrapper.Run(ctx); close(done) }()
		require.Same(t, ctx, awaitWrapperContext(t, started))
		cancel()
		awaitWrapperReturn(t, done)
		require.EqualValues(t, 0, plugin.legacyCalls.Load())
		require.EqualValues(t, 1, plugin.stopCalls.Load())
	}
}

// Terminal plugin errors must reach the existing agent failure/restart path.
type wrapperObservedPlugin struct {
	wrapperContextPlugin
	done chan struct{}
	err  error
}

func (p *wrapperObservedPlugin) Done() <-chan struct{} { return p.done }
func (p *wrapperObservedPlugin) Err() error            { return p.err }
func TestPluginWrapperObservesTerminalFailure(t *testing.T) {
	const childEnv = "KATALYST_TEST_WRAPPER_TERMINAL_FAILURE"
	if os.Getenv(childEnv) == "1" {
		done := make(chan struct{})
		close(done)
		p := &wrapperObservedPlugin{wrapperContextPlugin: wrapperContextPlugin{start: func(context.Context) error { return nil }}, done: done, err: errors.New("terminal vendor failure")}
		(&PluginWrapper{GenericPlugin: p}).Run(context.Background())
		t.Fatal("terminal failure was ignored")
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestPluginWrapperObservesTerminalFailure$", "-test.timeout=5s")
	command.Env = append(os.Environ(), childEnv+"=1")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "run fake.example.com failed: terminal vendor failure")
	require.NoError(t, ctx.Err())
}
