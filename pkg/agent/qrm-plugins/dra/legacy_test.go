// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestLegacyStopDeadlineRetainsInFlightCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var count atomic.Int32
	p := NewLegacyPlugin(func(ctx context.Context) (func() error, error) {
		return func() error { count.Add(1); close(entered); <-release; return nil }, nil
	})
	require.NoError(t, p.Start(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.Stop(ctx), context.DeadlineExceeded)
	<-entered
	ctx2, cancel2 := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel2()
	require.ErrorIs(t, p.Stop(ctx2), context.DeadlineExceeded)
	require.EqualValues(t, 1, count.Load())
	close(release)
	require.NoError(t, p.Stop(t.Context()))
	require.NoError(t, p.Stop(t.Context()))
	require.EqualValues(t, 1, count.Load())
}
func TestLegacyCleanupErrorCanRetry(t *testing.T) {
	failure := errors.New("shutdown failed")
	var count atomic.Int32
	p := NewLegacyPlugin(func(context.Context) (func() error, error) {
		return func() error {
			if count.Add(1) == 1 {
				return failure
			}
			return nil
		}, nil
	})
	require.NoError(t, p.Start(t.Context()))
	require.ErrorIs(t, p.Stop(t.Context()), failure)
	require.NoError(t, p.Stop(t.Context()))
	require.NoError(t, p.Stop(t.Context()))
	require.EqualValues(t, 2, count.Load())
}
func TestLegacyStopCancelsBlockedStartupAndCleansPartialHandle(t *testing.T) {
	entered := make(chan struct{})
	var cleaned atomic.Bool
	p := NewLegacyPlugin(func(ctx context.Context) (func() error, error) {
		close(entered)
		<-ctx.Done()
		return func() error { cleaned.Store(true); return nil }, ctx.Err()
	})
	result := make(chan error, 1)
	go func() { result <- p.Start(t.Context()) }()
	<-entered
	require.NoError(t, p.Stop(t.Context()))
	require.ErrorIs(t, <-result, context.Canceled)
	require.True(t, cleaned.Load())
}
func TestLegacyStopBeforeStartAndFailedConstructor(t *testing.T) {
	failure := errors.New("startup failed")
	var cleaned atomic.Bool
	p := NewLegacyPlugin(func(context.Context) (func() error, error) {
		return func() error { cleaned.Store(true); return nil }, failure
	})
	require.NoError(t, p.Stop(t.Context()))
	require.ErrorIs(t, p.Start(t.Context()), failure)
	require.NoError(t, p.Stop(t.Context()))
	require.True(t, cleaned.Load())
}
