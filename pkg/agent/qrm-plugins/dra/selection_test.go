// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestModelListsPassedToProvider(t *testing.T) {
	for _, tc := range []struct {
		name   string
		models map[string][]string
		want   []string
	}{
		{"defaults", nil, []string{"GB200", "GB300"}},
		{"empty-map", map[string][]string{}, []string{"GB200", "GB300"}},
		{"override-unchanged", map[string][]string{"a": {" gb300 ", "gb200", "gb200"}}, []string{" gb300 ", "gb200", "gb200"}},
		{"disabled", map[string][]string{"a": {}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}
			called := false
			p := Provider{DefaultModels: []string{"GB200", "GB300"}, Match: func(_ context.Context, got *corev1.Node, models []string) (bool, error) {
				called = true
				require.Same(t, node, got)
				require.Equal(t, tc.want, models, "Core must not rewrite provider model lists")
				return true, nil
			}, New: func(NativeFlags, Runtime) (Plugin, error) { return &fakePlugin{}, nil }}
			m, err := NewManagerWithProviders(Config{ProviderModels: tc.models}, Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) { return node, nil }}, map[string]Provider{"a": p})
			require.NoError(t, err)
			require.Equal(t, tc.name != "disabled", called)
			if called {
				require.Equal(t, "a", m.name)
			}
		})
	}
}

func TestOverlappingModelListsUseActualMatch(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}
	rt := Runtime{NodeName: "node", GetNode: func(context.Context) (*corev1.Node, error) { return node, nil }}
	p := Provider{DefaultModels: []string{"GB200"}, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return true, nil }, New: func(NativeFlags, Runtime) (Plugin, error) { return &fakePlugin{}, nil }}
	providers := map[string]Provider{"a": p, "b": p}
	_, err := NewManagerWithProviders(Config{}, rt, providers)
	require.ErrorContains(t, err, "multiple DRA providers")
	providers["b"] = Provider{DefaultModels: p.DefaultModels, Match: func(context.Context, *corev1.Node, []string) (bool, error) { return false, nil }, New: p.New}
	m, err := NewManagerWithProviders(Config{}, rt, providers)
	require.NoError(t, err)
	require.Equal(t, "a", m.name)
	m, err = NewManagerWithProviders(Config{ProviderModels: map[string][]string{"a": {}}}, rt, providers)
	require.NoError(t, err)
	require.Empty(t, m.name)
	_, err = NewManagerWithProviders(Config{ProviderModels: map[string][]string{"unknown": {}}}, rt, providers)
	require.ErrorContains(t, err, "not compiled")
	flags := NativeFlags{}
	p.New = func(got NativeFlags, _ Runtime) (Plugin, error) {
		require.Equal(t, flags, got)
		return &fakePlugin{}, nil
	}
	_, err = NewManagerWithProviders(Config{NativeFlags: flags}, rt, map[string]Provider{"a": p})
	require.NoError(t, err)
}
