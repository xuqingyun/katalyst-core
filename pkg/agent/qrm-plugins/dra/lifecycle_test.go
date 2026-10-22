// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type lifecyclePlugin func(context.Context) error

func (p lifecyclePlugin) Run(ctx context.Context) error { return p(ctx) }

func lifecycleManager(t *testing.T, factory func(NativeFlags, Runtime) (Plugin, error)) *Manager {
	t.Helper()
	m, err := NewManagerWithProviders(Config{}, Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}, nil
	}}, map[string]Provider{"test": {DefaultModels: []string{"test"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return true, nil }, New: factory}})
	require.NoError(t, err)
	m.retryDelay = 2 * time.Millisecond
	return m
}
func draReady() bool {
	return general.GetRegisterReadinessCheckResult()[general.HealthzCheckName(AgentName)].Ready
}

func TestContinuousFailureLeavesOtherComponentRunning(t *testing.T) {
	for _, kind := range []string{"factory-error", "nil-plugin", "run-error", "unexpected-return"} {
		t.Run(kind, func(t *testing.T) {
			var attempts, sibling atomic.Int32
			failure := errors.New("unsupported hardware")
			m := lifecycleManager(t, func(_ NativeFlags, rt Runtime) (Plugin, error) {
				attempts.Add(1)
				switch kind {
				case "factory-error":
					return nil, failure
				case "nil-plugin":
					return nil, nil
				}
				return lifecyclePlugin(func(ctx context.Context) error {
					if kind == "run-error" {
						return failure
					}
					return nil
				}), nil
			})
			require.Zero(t, attempts.Load(), "factory failure must not abort Component initialization")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			peerDone := make(chan struct{})
			go func() { m.Run(ctx); close(done) }()
			go func() {
				defer close(peerDone)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						sibling.Add(1)
					}
				}
			}()
			require.Eventually(t, func() bool { return attempts.Load() >= 5 && sibling.Load() >= 5 }, time.Second, time.Millisecond)
			require.False(t, draReady())
			require.NoError(t, ctx.Err(), "DRA must not cancel the containing Agent context")
			select {
			case <-done:
				t.Fatal("manager exhausted retries")
			default:
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("manager did not stop")
			}
			<-peerDone
		})
	}
}

func TestRecoveryReadinessAndSerialAttempts(t *testing.T) {
	var attempts, active atomic.Int32
	started := make(chan Runtime, 4)
	exit := make(chan struct{})
	failure := errors.New("dependency not available")
	m := lifecycleManager(t, func(_ NativeFlags, rt Runtime) (Plugin, error) {
		require.Zero(t, active.Load(), "previous Run must finish before constructing another plugin")
		n := attempts.Add(1)
		if n == 1 {
			return nil, failure
		}
		return lifecyclePlugin(func(ctx context.Context) error {
			require.EqualValues(t, 1, active.Add(1))
			defer active.Add(-1)
			if n == 2 {
				return failure
			}
			started <- rt
			if n == 3 {
				select {
				case <-exit:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	var third Runtime
	select {
	case third = <-started:
	case <-time.After(time.Second):
		t.Fatal("failed attempts did not retry")
	}
	require.False(t, draReady(), "Run still starting is not readiness")
	third.Ready()
	require.True(t, draReady())
	close(exit)
	var fourth Runtime
	select {
	case fourth = <-started:
	case <-time.After(time.Second):
		t.Fatal("running exit did not retry")
	}
	require.ErrorIs(t, third.Context.Err(), context.Canceled)
	require.False(t, draReady())
	third.Ready()
	require.False(t, draReady(), "stale readiness from a finished attempt must be ignored")
	fourth.Ready()
	require.True(t, draReady())
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("running cancellation did not finish")
	}
	require.Zero(t, active.Load())
	require.EqualValues(t, 4, attempts.Load())
	require.False(t, draReady())
	fourth.Ready()
	require.False(t, draReady())
}

func TestCancellationDuringRetryBackoff(t *testing.T) {
	var attempts atomic.Int32
	attempted := make(chan struct{}, 1)
	m := lifecycleManager(t, func(NativeFlags, Runtime) (Plugin, error) {
		attempts.Add(1)
		attempted <- struct{}{}
		return nil, errors.New("missing dependency")
	})
	m.retryDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	<-attempted
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retry backoff blocked cancellation")
	}
	require.EqualValues(t, 1, attempts.Load())
	require.False(t, draReady())
}
