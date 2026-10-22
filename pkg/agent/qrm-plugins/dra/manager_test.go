// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakePlugin struct {
	started chan struct{}
	stopped chan struct{}
	ready   func()
}

func (p *fakePlugin) Run(ctx context.Context) error {
	if p.ready != nil {
		p.ready()
	}
	close(p.started)
	<-ctx.Done()
	close(p.stopped)
	return nil
}

func TestSingleProviderSelectedBeforeConstruction(t *testing.T) {
	for _, model := range []string{"cpu", "vendor-a", "vendor-b", "conflict"} {
		t.Run(model, func(t *testing.T) {
			created := []string{}
			plugin := &fakePlugin{started: make(chan struct{}), stopped: make(chan struct{})}
			providers := map[string]Provider{}
			for _, name := range []string{"vendor-a", "vendor-b"} {
				name := name
				providers[name] = Provider{DefaultModels: []string{name}, Match: func(_ context.Context, node *corev1.Node, _ []string) (bool, error) {
					return node.Labels["model"] == name || node.Labels["model"] == "conflict", nil
				}, New: func(flags NativeFlags, rt Runtime) (Plugin, error) {
					created = append(created, name)
					plugin.ready = rt.Ready
					require.Equal(t, `"same value"`, string(flags["option"]))
					return plugin, nil
				}}
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{"model": model}}}
			config := Config{NativeFlags: NativeFlags{"option": json.RawMessage(`"same value"`)}}
			manager, err := NewManagerWithProviders(config, Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) { return node, nil }}, providers)
			if model == "conflict" {
				require.ErrorContains(t, err, "multiple DRA providers")
				require.Empty(t, created)
				return
			}
			require.NoError(t, err)
			require.Equal(t, `"same value"`, string(config.NativeFlags["option"]))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { manager.Run(ctx); close(done) }()
			if model != "cpu" {
				select {
				case <-plugin.started:
					require.Equal(t, []string{model}, created)
				case <-time.After(time.Second):
					t.Fatal("plugin did not start")
				}
			}
			cancel()
			select {
			case <-done:
				if model == "cpu" {
					require.Empty(t, created)
				}
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
		{"match error", rt, map[string]Provider{"a": {DefaultModels: []string{"A"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return false, fail }, New: func(NativeFlags, Runtime) (Plugin, error) { t.Fatal("factory called"); return nil, nil }}}, "match"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewManagerWithProviders(Config{}, tc.rt, tc.providers)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
