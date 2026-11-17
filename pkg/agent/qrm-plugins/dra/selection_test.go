// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProviderModelDefaultsAndListOverrides(t *testing.T) {
	for _, tc := range []struct{ name, model, config, want string }{
		{"default", "GB200", "", "a"},
		{"default-second-model", "GB300", "", "a"},
		{"reassigned-model", "GB200", `{"a":["GB300"],"b":["GB200"]}`, "b"},
		{"case-insensitive-model", "GB300", `{"a":["gb300"]}`, "a"},
		{"replaces-provider-list", "GB200", `{"a":["GB300"]}`, ""},
		{"empty-map-keeps-defaults", "GB200", `{}`, "a"},
		{"other-provider-default-retained", "OTHER", `{"a":["GB200","GB300"]}`, "b"},
		{"provider-disabled", "GB200", `{"a":[]}`, ""},
		{"all-providers-disabled", "GB200", `{"a":[],"b":[]}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := []string{}
			created := []string{}
			providers := map[string]Provider{}
			for _, name := range []string{"a", "b"} {
				name := name
				models := []string{"OTHER"}
				if name == "a" {
					models = []string{"GB200", "GB300"}
				}
				providers[name] = Provider{DefaultModels: models, Match: func(_ context.Context, node *corev1.Node, allowed []string) (bool, error) {
					called = append(called, name)
					for _, model := range allowed {
						if node.Labels["model"] == model {
							return true, nil
						}
					}
					return false, nil
				}, New: func(config Config, _ Runtime) (Plugin, error) {
					require.NotContains(t, config, providerModelsKey, "manager setting must not become a native flag")
					created = append(created, name)
					return &fakePlugin{make(chan struct{}), make(chan struct{})}, nil
				}}
			}
			cfg := Config{}
			if tc.config != "" {
				cfg[providerModelsKey] = json.RawMessage(tc.config)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{"model": tc.model}}}
			manager, err := NewManagerWithProviders(cfg, Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) { return node, nil }}, providers)
			require.NoError(t, err)
			require.Equal(t, tc.want, manager.name)
			if tc.want == "" {
				require.Empty(t, created)
			} else {
				require.Equal(t, []string{tc.want}, created)
			}
			if tc.name == "all-providers-disabled" {
				require.Empty(t, called, "disabled providers must not probe hardware")
			}
			if tc.config != "" {
				require.Equal(t, tc.config, string(cfg[providerModelsKey]), "do not mutate input config")
			}
			require.Equal(t, []string{"GB200", "GB300"}, providers["a"].DefaultModels, "do not mutate compiled defaults")
		})
	}
}

func TestInvalidProviderModelListsDoNotConstructPlugins(t *testing.T) {
	p := Provider{DefaultModels: []string{"GB200"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) {
		t.Fatal("invalid config must not probe hardware")
		return false, nil
	}, New: func(Config, Runtime) (Plugin, error) {
		t.Fatal("invalid config must not construct plugin")
		return nil, nil
	}}
	providers := map[string]Provider{"a": p, "b": {DefaultModels: []string{"OTHER"}, Match: p.Match, New: p.New}}
	rt := Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}, nil
	}}
	for _, raw := range []string{`null`, `[]`, `"bad"`, `{"a":"GB200"}`, `{"a":null}`, `{"a":[42]}`, `{"unknown":[]}`, `{"a":[""]}`, `{"a":["GB200","gb200"]}`, `{"a":["GB200"],"b":["GB200"]}`} {
		_, err := NewManagerWithProviders(Config{providerModelsKey: json.RawMessage(raw)}, rt, providers)
		require.Error(t, err, raw)
	}
	_, err := NewManagerWithProviders(Config{"model-providers": json.RawMessage(`{"GB200":"a"}`)}, rt, providers)
	require.ErrorContains(t, err, "replaced")
}

func TestConflictingDefaultsResolvedByProviderList(t *testing.T) {
	p := Provider{DefaultModels: []string{"GB200"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return true, nil }, New: func(Config, Runtime) (Plugin, error) { return &fakePlugin{}, nil }}
	providers := map[string]Provider{"a": p, "b": p}
	_, err := resolveProviderModels(Config{}, providers)
	require.ErrorContains(t, err, "appears more than once")
	models, err := resolveProviderModels(Config{providerModelsKey: json.RawMessage(`{"a":[]}`)}, providers)
	require.NoError(t, err)
	require.Empty(t, models["a"])
	require.Equal(t, []string{"GB200"}, models["b"])
}
