// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import "context"

// Plugin is the complete vendor driver. The vendor owns kubelet RPC,
// registration, publication, checkpoint, CDI and device preparation.
type Plugin interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// PluginFuncs connects existing native lifecycle names without a data-plane adapter.
type PluginFuncs struct {
	StartFunc func(context.Context) error
	StopFunc  func(context.Context) error
}

func (p PluginFuncs) Start(ctx context.Context) error { return p.StartFunc(ctx) }
func (p PluginFuncs) Stop(ctx context.Context) error  { return p.StopFunc(ctx) }

// ReadyPlugin optionally waits for vendor registration and initialization.
type ReadyPlugin interface{ Ready(context.Context) error }

// ObservablePlugin reports termination, not transient device health failures.
type ObservablePlugin interface {
	Done() <-chan struct{}
	Err() error
}
