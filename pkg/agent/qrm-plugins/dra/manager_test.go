// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakePlugin struct {
	started chan struct{}
	stopped chan struct{}
}

func (p *fakePlugin) Run(ctx context.Context) error {
	close(p.started)
	<-ctx.Done()
	close(p.stopped)
	return nil
}

func TestSingleProviderSelectedBeforeConstruction(t *testing.T) {
	for _, model := range []string{"cpu", "vendor-a", "vendor-b", "conflict"} {
		t.Run(model, func(t *testing.T) {
			created := []string{}
			plugin := &fakePlugin{make(chan struct{}), make(chan struct{})}
			providers := map[string]Provider{}
			for _, name := range []string{"vendor-a", "vendor-b"} {
				name := name
				providers[name] = Provider{DefaultModels: []string{name}, Match: func(_ context.Context, node *corev1.Node, _ []string) (bool, error) {
					return node.Labels["model"] == name || node.Labels["model"] == "conflict", nil
				}, New: func(config Config, runtime Runtime) (Plugin, error) {
					created = append(created, name)
					require.Equal(t, `"same value"`, string(config["option"]))
					config["option"][0] = 'x'
					return plugin, nil
				}}
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{"model": model}}}
			config := Config{"option": json.RawMessage(`"same value"`)}
			manager, err := NewManagerWithProviders(config, Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) { return node, nil }}, providers)
			if model == "conflict" {
				require.ErrorContains(t, err, "multiple DRA providers")
				require.Empty(t, created)
				return
			}
			require.NoError(t, err)
			require.Equal(t, `"same value"`, string(config["option"]))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { manager.Run(ctx); close(done) }()
			if model == "cpu" {
				require.Empty(t, created)
			} else {
				require.Equal(t, []string{model}, created)
				select {
				case <-plugin.started:
				case <-time.After(time.Second):
					t.Fatal("plugin did not start")
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("manager did not stop")
			}
			if model != "cpu" {
				select {
				case <-plugin.stopped:
				default:
					t.Fatal("cleanup not completed")
				}
			}
		})
	}
}

func TestProviderSelectionErrors(t *testing.T) {
	rt := Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}, nil
	}}
	fail := errors.New("failure")
	for _, tc := range []struct {
		name      string
		rt        Runtime
		providers map[string]Provider
		want      string
	}{
		{"no fetcher", Runtime{}, nil, "fetcher"},
		{"wrong node", Runtime{NodeName: "other", GetNode: rt.GetNode}, nil, "different"},
		{"get node error", Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) { return nil, fail }}, nil, "failure"},
		{"invalid provider", rt, map[string]Provider{"a": {}}, "invalid"},
		{"match error", rt, map[string]Provider{"a": {DefaultModels: []string{"A"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return false, fail }, New: func(Config, Runtime) (Plugin, error) { t.Fatal("factory called"); return nil, nil }}}, "match"},
		{"factory error", rt, map[string]Provider{"a": {DefaultModels: []string{"A"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return true, nil }, New: func(Config, Runtime) (Plugin, error) { return nil, fail }}}, "create"},
		{"nil plugin", rt, map[string]Provider{"a": {DefaultModels: []string{"A"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return true, nil }, New: func(Config, Runtime) (Plugin, error) { return nil, nil }}}, "nil plugin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewManagerWithProviders(Config{}, tc.rt, tc.providers)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestFlatConfigLoading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dra.yaml")
	for _, tc := range []struct {
		data  string
		valid bool
	}{{"cdi-root: /var/run/cdi\n", true}, {"{}", true}, {"null", false}, {"[]", false}, {"cdi-root: /first\ncdi-root: /second\n", false}} {
		require.NoError(t, os.WriteFile(path, []byte(tc.data), 0600))
		cfg, err := LoadConfig(path)
		if tc.valid {
			require.NoError(t, err)
			require.NotNil(t, cfg)
		} else {
			require.Error(t, err)
		}
	}
}
