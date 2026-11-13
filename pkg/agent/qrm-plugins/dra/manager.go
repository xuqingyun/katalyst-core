// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

var ErrUnsupportedNode = errors.New("DRA provider unsupported on node")

// Manager starts at most one complete vendor plugin after node selection.
// It does not implement or route the vendor's DRA RPC or persistence protocol.
type Manager struct {
	mu            sync.Mutex
	contextMu     sync.Mutex
	cancel        context.CancelFunc
	starting      bool
	stopRequests  int
	parentStop    func() bool
	monitorCancel context.CancelFunc
	config        Config
	runtime       Runtime
	factories     map[string]ProviderFactory
	active        Plugin
	started       bool
	selected      string
	done          chan struct{}
	doneClosed    bool
	runtimeErr    error
}

func (*Manager) Name() string   { return AgentName }
func (m *Manager) Start() error { return m.StartWithContext(context.Background()) }
func (m *Manager) StartWithContext(parent context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return nil
	}
	if m.active != nil {
		return fmt.Errorf("previous DRA driver shutdown is incomplete")
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return err
	}
	// The running context keeps parent values, but cancellation is managed so
	// successful startup can drain before cancellation. Stop still cancels startup.
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	m.contextMu.Lock()
	m.cancel, m.starting = cancel, true
	if m.stopRequests > 0 {
		cancel()
	}
	m.contextMu.Unlock()
	defer func() { m.contextMu.Lock(); m.starting = false; m.contextMu.Unlock() }()
	m.done, m.doneClosed, m.runtimeErr, m.selected = make(chan struct{}), false, nil, ""
	m.parentStop = context.AfterFunc(parent, func() {
		if err := m.Stop(); err != nil {
			klog.ErrorS(err, "DRA shutdown after parent cancellation failed")
		}
	})
	fail := func(err error) error { return errors.Join(err, m.stop(context.Background())) }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	selected, err := m.selectPlugin(ctx)
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	if err != nil {
		return fail(err)
	}
	if selected == nil {
		klog.InfoS("No DRA plugin matches node; vendor initialization skipped", "node", m.runtime.NodeName)
		m.started = true
		return nil
	}
	m.selected = selected.ID
	runtime := m.runtime
	runtime.Context = ctx
	instance := *selected
	instance.NodeSelector = selected.NodeSelector.DeepCopy()
	instance.Config = make(map[string]json.RawMessage, len(selected.Config))
	for name, value := range selected.Config {
		instance.Config[name] = append(json.RawMessage(nil), value...)
	}
	plugin, err := m.factories[selected.Provider](instance, runtime)
	// Retain every returned object, including partial failures, for cleanup.
	m.active = plugin
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	if err != nil {
		if ctx.Err() == nil && plugin == nil && selected.AutoDetect && !selected.Required && errors.Is(err, ErrUnsupportedNode) {
			klog.InfoS("Selected DRA plugin does not support node; no fallback attempted", "plugin", selected.ID)
			m.started = true
			return nil
		}
		return errors.Join(fmt.Errorf("construct DRA plugin %s: %w", selected.ID, err), m.stop(context.Background()))
	}
	if plugin == nil {
		return fail(fmt.Errorf("DRA provider %s returned nil plugin", selected.Provider))
	}
	err = plugin.Start(ctx)
	if err == nil {
		if ready, ok := plugin.(ReadyPlugin); ok {
			err = ready.Ready(ctx)
		}
	}
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	if err != nil {
		return fail(fmt.Errorf("start DRA plugin %s: %w", selected.ID, err))
	}
	m.started = true
	if observable, ok := plugin.(ObservablePlugin); ok {
		monitorCtx, monitorCancel := context.WithCancel(context.Background())
		m.monitorCancel = monitorCancel
		done := observable.Done()
		go m.observe(monitorCtx, done, observable)
	}
	return nil
}
func (m *Manager) observe(ctx context.Context, done <-chan struct{}, plugin ObservablePlugin) {
	select {
	case <-ctx.Done():
		return
	case <-done:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	err := plugin.Err()
	if err == nil {
		err = errors.New("DRA plugin exited unexpectedly")
	}
	m.runtimeErr = errors.Join(err, m.stop(context.Background()))
	if !m.doneClosed {
		close(m.done)
		m.doneClosed = true
	}
}
func (m *Manager) Done() <-chan struct{}  { m.mu.Lock(); defer m.mu.Unlock(); return m.done }
func (m *Manager) Err() error             { m.mu.Lock(); defer m.mu.Unlock(); return m.runtimeErr }
func (m *Manager) SelectedPlugin() string { m.mu.Lock(); defer m.mu.Unlock(); return m.selected }
func (m *Manager) Stop() error            { return m.StopWithContext(context.Background()) }
func (m *Manager) StopWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.contextMu.Lock()
	m.stopRequests++
	if m.starting && m.cancel != nil {
		m.cancel()
	}
	m.contextMu.Unlock()
	m.mu.Lock()
	err := m.stop(ctx)
	m.mu.Unlock()
	m.contextMu.Lock()
	m.stopRequests--
	m.contextMu.Unlock()
	return err
}
func (m *Manager) stop(parent context.Context) error {
	if m.monitorCancel != nil {
		m.monitorCancel()
		m.monitorCancel = nil
	}
	ctx, timeout := context.WithTimeout(parent, time.Duration(m.config.ShutdownTimeoutSeconds)*time.Second)
	defer timeout()
	m.started = false
	if m.active != nil {
		if err := m.active.Stop(ctx); err != nil {
			return err
		}
		m.active = nil
	}
	m.contextMu.Lock()
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.contextMu.Unlock()
	if m.parentStop != nil {
		m.parentStop()
		m.parentStop = nil
	}
	if m.done != nil && !m.doneClosed {
		close(m.done)
		m.doneClosed = true
	}
	return nil
}
