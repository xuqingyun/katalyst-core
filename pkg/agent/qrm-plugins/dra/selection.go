// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (m *Manager) selectPlugin(ctx context.Context) (*PluginConfig, error) {
	enabled := false
	for _, p := range m.config.Plugins {
		enabled = enabled || p.Enabled
	}
	if !enabled {
		return nil, nil
	}
	node, err := m.runtime.GetNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("get Node for DRA selection: %w", err)
	}
	if node == nil || node.Name != m.runtime.NodeName {
		return nil, fmt.Errorf("DRA node fetcher returned a different or nil Node")
	}
	var selected *PluginConfig
	for i := range m.config.Plugins {
		p := &m.config.Plugins[i]
		if !p.Enabled {
			continue
		}
		selector, _ := metav1.LabelSelectorAsSelector(p.NodeSelector)
		if !selector.Matches(labels.Set(node.Labels)) {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("DRA node %s matches multiple plugins (%s, %s); selectors must be exclusive", node.Name, selected.ID, p.ID)
		}
		selected = p
	}
	return selected, nil
}
