// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type legacyCleanup struct {
	done chan struct{}
	err  error
}
type legacyPlugin struct {
	mu       sync.Mutex
	start    func(context.Context) (func() error, error)
	cancel   context.CancelFunc
	started  chan struct{}
	shutdown func() error
	cleanup  *legacyCleanup
}

// NewLegacyPlugin connects a vendor constructor and context-less shutdown to
// the lifecycle interface. A failed constructor should return its cleanup too.
// Timed-out cleanup remains tracked; subsequent Stop calls join it before retry.
func NewLegacyPlugin(start func(context.Context) (func() error, error)) Plugin {
	return &legacyPlugin{start: start}
}
func (p *legacyPlugin) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started != nil {
		p.mu.Unlock()
		return fmt.Errorf("vendor plugin already started")
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.started = make(chan struct{})
	p.mu.Unlock()
	shutdown, err := p.start(ctx)
	p.mu.Lock()
	p.shutdown = shutdown
	close(p.started)
	p.mu.Unlock()
	return errors.Join(err, ctx.Err())
}
func (p *legacyPlugin) Stop(ctx context.Context) error {
	p.mu.Lock()
	if p.cancel != nil {
		p.cancel()
	}
	started := p.started
	p.mu.Unlock()
	if started == nil {
		return nil
	}
	select {
	case <-started:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.mu.Lock()
	if p.shutdown == nil {
		p.mu.Unlock()
		return nil
	}
	task := p.cleanup
	if task == nil {
		task = &legacyCleanup{done: make(chan struct{})}
		p.cleanup = task
		shutdown := p.shutdown
		go func() { task.err = shutdown(); close(task.done) }()
	}
	p.mu.Unlock()
	select {
	case <-task.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.mu.Lock()
	if p.cleanup == task {
		p.cleanup = nil
		if task.err == nil {
			p.shutdown = nil
		}
	}
	p.mu.Unlock()
	return task.err
}
