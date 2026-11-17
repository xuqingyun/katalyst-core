// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

const AgentName = "qrm-dra-manager"

// Config is a flat set of options interpreted by the selected provider.
// It does not declare driver instances or per-provider lifecycle settings.
type Config map[string]json.RawMessage

type Runtime struct {
	Context    context.Context
	NodeName   string
	RESTConfig *rest.Config
	GetNode    func(context.Context) (*corev1.Node, error)
}

// Plugin owns its registration, RPCs, devices and persistent state. Run blocks
// until the context is canceled or the plugin fails, including its cleanup.
type Plugin interface{ Run(context.Context) error }

type Provider struct {
	// DefaultModels is the built-in allowlist; Match receives the configured list.
	DefaultModels []string
	Match         func(context.Context, *corev1.Node, []string) (bool, error)
	New           func(Config, Runtime) (Plugin, error)
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

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data, err = yaml.YAMLToJSONStrict(data)
	if err != nil {
		return nil, err
	}
	var config Config
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&config); err != nil {
		return nil, err
	}
	if config == nil {
		return nil, fmt.Errorf("DRA config must be an object")
	}
	return config, nil
}

// Manager only selects and runs a provider; kubelet routes claims to that
// provider's native driver endpoint. The containing agent enables this component.
type Manager struct {
	name   string
	plugin Plugin
}

func NewManager(config Config, runtime Runtime) (*Manager, error) {
	registry.Lock()
	providers := make(map[string]Provider, len(registry.providers))
	for name, provider := range registry.providers {
		providers[name] = provider
	}
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
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	sort.Strings(names)
	providerModels, err := resolveProviderModels(config, providers)
	if err != nil {
		return nil, err
	}
	var selected string
	for _, name := range names {
		p := providers[name]
		if name == "" || p.Match == nil || p.New == nil {
			return nil, fmt.Errorf("invalid DRA provider %q", name)
		}
		models := providerModels[name]
		if len(models) == 0 {
			continue
		}
		sort.Strings(models)
		matches, err := p.Match(runtime.Context, node.DeepCopy(), models)
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
	options := make(Config, len(config))
	for key, value := range config {
		if key != providerModelsKey {
			options[key] = append(json.RawMessage(nil), value...)
		}
	}
	plugin, err := providers[selected].New(options, runtime)
	if err != nil {
		return nil, fmt.Errorf("create DRA provider %s: %w", selected, err)
	}
	if plugin == nil {
		return nil, fmt.Errorf("DRA provider %s returned nil plugin", selected)
	}
	return &Manager{name: selected, plugin: plugin}, nil
}

// Run satisfies the agent Component lifecycle. Selected plugins must stay up
// until cancellation; an unexpected exit uses the agent's fail-fast policy.
func (m *Manager) Run(ctx context.Context) {
	if m.plugin == nil {
		klog.InfoS("Skipping DRA: no provider matches this node")
		return
	}
	klog.InfoS("Starting DRA provider", "provider", m.name)
	if err := m.plugin.Run(ctx); err != nil {
		if ctx.Err() == nil {
			klog.Fatalf("DRA provider %s exited: %v", m.name, err)
		}
		klog.ErrorS(err, "DRA provider shutdown failed", "provider", m.name)
	}
	if ctx.Err() == nil {
		klog.Fatalf("DRA provider %s exited before cancellation", m.name)
	}
}
