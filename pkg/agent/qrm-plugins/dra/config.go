// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

const AgentName = "qrm-dra-manager"
const DefaultDriverName = "compute-domain.nvidia.com"

type Config struct {
	RegistrationDir        string         `json:"registrationDir"`
	StateDir               string         `json:"stateDir"`
	ShutdownTimeoutSeconds int            `json:"shutdownTimeoutSeconds,omitempty"`
	Plugins                []PluginConfig `json:"plugins"`
}
type PluginConfig struct {
	ID           string                     `json:"id"`
	Provider     string                     `json:"provider,omitempty"`
	Enabled      bool                       `json:"enabled"`
	Required     bool                       `json:"required"`
	AutoDetect   bool                       `json:"autoDetect"`
	DriverName   string                     `json:"driverName"`
	StateDir     string                     `json:"stateDir,omitempty"`
	NodeSelector *metav1.LabelSelector      `json:"nodeSelector"`
	Config       map[string]json.RawMessage `json:"config,omitempty"`
}
type Runtime struct {
	Context         context.Context
	NodeName        string
	RESTConfig      *rest.Config
	RegistrationDir string
	// GetNode uses the containing agent's node fetcher. Selection runs before factories.
	GetNode func(context.Context) (*corev1.Node, error)
}
type ProviderFactory func(PluginConfig, Runtime) (Plugin, error)

var providerRegistry = struct {
	sync.Mutex
	factories map[string]ProviderFactory
}{factories: map[string]ProviderFactory{}}

func RegisterProvider(name string, factory ProviderFactory) {
	providerRegistry.Lock()
	defer providerRegistry.Unlock()
	if factory == nil || providerRegistry.factories[name] != nil {
		panic("invalid or duplicate DRA provider: " + name)
	}
	providerRegistry.factories[name] = factory
}
func LoadConfig(path string) (Config, error) {
	var config Config
	data, err := os.ReadFile(path)
	if err != nil {
		return config, err
	}
	data, err = yaml.YAMLToJSONStrict(data)
	if err != nil {
		return config, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&config)
	return config, err
}
func NewManager(config Config, runtime Runtime) (*Manager, error) {
	providerRegistry.Lock()
	defer providerRegistry.Unlock()
	return NewManagerWithProviders(config, runtime, providerRegistry.factories)
}
func NewManagerWithProviders(config Config, runtime Runtime, factories map[string]ProviderFactory) (*Manager, error) {
	config.Plugins = append([]PluginConfig(nil), config.Plugins...)
	registry := make(map[string]ProviderFactory, len(factories))
	for name, factory := range factories {
		registry[name] = factory
	}
	if !filepath.IsAbs(config.RegistrationDir) || !filepath.IsAbs(config.StateDir) {
		return nil, fmt.Errorf("DRA registrationDir and stateDir must be absolute")
	}
	if config.ShutdownTimeoutSeconds < 0 {
		return nil, fmt.Errorf("DRA shutdownTimeoutSeconds must not be negative")
	}
	if config.ShutdownTimeoutSeconds == 0 {
		config.ShutdownTimeoutSeconds = 30
	}
	ids, drivers, states := map[string]bool{}, map[string]bool{}, map[string]bool{}
	enabled := false
	for i := range config.Plugins {
		p := &config.Plugins[i]
		options := make(map[string]json.RawMessage, len(p.Config))
		for key, value := range p.Config {
			options[key] = append(json.RawMessage(nil), value...)
		}
		p.Config = options
		p.NodeSelector = p.NodeSelector.DeepCopy()
		if !p.Enabled {
			continue
		}
		enabled = true
		if len(validation.IsDNS1123Label(p.ID)) != 0 || ids[p.ID] {
			return nil, fmt.Errorf("invalid or duplicate DRA plugin id %q", p.ID)
		}
		if len(validation.IsDNS1123Subdomain(p.DriverName)) != 0 || drivers[p.DriverName] {
			return nil, fmt.Errorf("invalid or duplicate DRA driver %q", p.DriverName)
		}
		if p.Provider == "" {
			p.Provider = p.ID
		}
		if registry[p.Provider] == nil {
			return nil, fmt.Errorf("DRA provider %q is not compiled into this agent", p.Provider)
		}
		positive := p.NodeSelector != nil && len(p.NodeSelector.MatchLabels) != 0
		if p.NodeSelector != nil {
			for _, expression := range p.NodeSelector.MatchExpressions {
				positive = positive || expression.Operator == metav1.LabelSelectorOpIn && len(expression.Values) != 0
			}
		}
		if !positive {
			return nil, fmt.Errorf("DRA plugin %s requires an explicit nodeSelector allowlist (matchLabels or In)", p.ID)
		}
		if _, err := metav1.LabelSelectorAsSelector(p.NodeSelector); err != nil {
			return nil, fmt.Errorf("DRA plugin %s nodeSelector: %w", p.ID, err)
		}
		if p.StateDir == "" {
			p.StateDir = filepath.Join(config.StateDir, p.DriverName)
		}
		p.StateDir = filepath.Clean(p.StateDir)
		if !filepath.IsAbs(p.StateDir) || states[p.StateDir] {
			return nil, fmt.Errorf("invalid or shared DRA state directory %q", p.StateDir)
		}
		ids[p.ID], drivers[p.DriverName], states[p.StateDir] = true, true, true
	}
	if enabled && runtime.GetNode == nil {
		return nil, fmt.Errorf("DRA node selection requires a node fetcher")
	}
	runtime.RegistrationDir = config.RegistrationDir
	return &Manager{config: config, runtime: runtime, factories: registry}, nil
}
