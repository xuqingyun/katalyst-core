// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const AgentName = "qrm-dra-plugin"

// Config separates model selection from the selected provider's native flags.
type Config struct {
	ProviderModels map[string][]string `json:"provider-models,omitempty"`
	NativeFlags    NativeFlags         `json:"native-flags,omitempty"`
}

type NativeFlags map[string]json.RawMessage

type Runtime struct {
	Context    context.Context
	NodeName   string
	RESTConfig *rest.Config
	GetNode    func(context.Context) (*corev1.Node, error)
	// Ready is called only after the native driver has completed startup.
	Ready func()
}

// Plugin owns its registration, RPCs, devices and persistent state. Run blocks
// until the context is canceled or the plugin fails, including its cleanup.
type Plugin interface{ Run(context.Context) error }

// Providers treat the supplied Node, model list and native flags as read-only.
type Provider struct {
	// DefaultModels is the built-in allowlist; Match receives the configured list.
	DefaultModels []string
	Match         func(context.Context, *corev1.Node, []string) (bool, error)
	New           func(NativeFlags, Runtime) (Plugin, error)
}

var registry = struct {
	sync.Mutex
	providers map[string]Provider
}{providers: make(map[string]Provider)}

func RegisterProvider(name string, provider Provider) {
	registry.Lock()
	defer registry.Unlock()
	if name == "" || provider.Match == nil || provider.New == nil || registry.providers[name].New != nil {
		panic("invalid or duplicate DRA provider: " + name)
	}
	registry.providers[name] = provider
}

// Manager only selects and runs a provider; kubelet routes claims to that
// provider's native driver endpoint. The containing agent enables this component.
type Manager struct {
	name       string
	provider   Provider
	flags      NativeFlags
	runtime    Runtime
	retryDelay time.Duration
}

func NewManager(config Config, runtime Runtime) (*Manager, error) {
	registry.Lock()
	providers := maps.Clone(registry.providers)
	registry.Unlock()
	return NewManagerWithProviders(config, runtime, providers)
}

func NewManagerWithProviders(config Config, runtime Runtime, providers map[string]Provider) (*Manager, error) {
	if runtime.NodeName == "" || runtime.GetNode == nil {
		return nil, fmt.Errorf("DRA selection requires node name and fetcher")
	}
	if runtime.Context == nil {
		runtime.Context = context.Background()
	}
	node, err := runtime.GetNode(runtime.Context)
	if err != nil {
		return nil, fmt.Errorf("get Node for DRA selection: %w", err)
	}
	if node == nil || node.Name != runtime.NodeName {
		return nil, fmt.Errorf("DRA fetcher returned a different or nil Node")
	}
	for name := range config.ProviderModels {
		if _, exists := providers[name]; !exists {
			return nil, fmt.Errorf("DRA provider %q is not compiled into this agent", name)
		}
	}
	var selected string
	for name, p := range providers {
		if name == "" || p.Match == nil || p.New == nil {
			return nil, fmt.Errorf("invalid DRA provider %q", name)
		}
		models, configured := config.ProviderModels[name]
		if !configured {
			models = p.DefaultModels
		}
		if len(models) == 0 {
			continue
		}
		matches, err := p.Match(runtime.Context, node, models)
		if err != nil {
			return nil, fmt.Errorf("match DRA provider %s: %w", name, err)
		}
		if !matches {
			continue
		}
		if selected != "" {
			return nil, fmt.Errorf("Node %s matches multiple DRA providers (%s, %s)", node.Name, selected, name)
		}
		selected = name
	}
	if selected == "" {
		return &Manager{}, nil
	}
	general.RegisterHeartbeatCheck(AgentName, 0, general.HealthzCheckStateNotReady, 0)
	return &Manager{name: selected, provider: providers[selected], flags: config.NativeFlags, runtime: runtime, retryDelay: time.Second}, nil
}

// Run isolates provider failures from the other agent Components. Each attempt
// completes, including native cleanup, before the next one is constructed.
func (m *Manager) Run(ctx context.Context) {
	if m.name == "" {
		klog.InfoS("Skipping DRA: no provider matches this node")
		return
	}
	delay := m.retryDelay
	for ctx.Err() == nil {
		attempt, cancel := context.WithCancel(ctx)
		runtime := m.runtime
		runtime.Context = attempt
		var lock sync.Mutex
		finished, ready := false, false
		runtime.Ready = func() {
			lock.Lock()
			defer lock.Unlock()
			if !finished && attempt.Err() == nil {
				ready = true
				_ = general.UpdateHealthzState(AgentName, general.HealthzCheckStateReady, "")
			}
		}
		_ = general.UpdateHealthzState(AgentName, general.HealthzCheckStateNotReady, "starting DRA provider")
		klog.InfoS("Starting DRA provider", "provider", m.name)
		plugin, err := m.provider.New(m.flags, runtime)
		if err == nil {
			if plugin == nil {
				err = fmt.Errorf("DRA provider %s returned nil plugin", m.name)
			} else {
				err = plugin.Run(attempt)
			}
		}
		if err == nil {
			err = fmt.Errorf("DRA provider exited before cancellation")
		}
		lock.Lock()
		finished = true
		cancel()
		_ = general.UpdateHealthzState(AgentName, general.HealthzCheckStateNotReady, err.Error())
		if ready {
			delay = m.retryDelay
		}
		lock.Unlock()
		if ctx.Err() != nil {
			return
		}
		klog.ErrorS(err, "DRA provider unavailable; retrying", "provider", m.name, "retryAfter", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(2*delay, 30*time.Second)
	}
}
