// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const NodeLabel = "dra.katalyst.kubewharf.io/node"
const InventoryAnnotation = "dra.katalyst.kubewharf.io/inventory-hash"

func nodeKey(name string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(name)))[:16] }

const ManagedLabel = "dra.katalyst.kubewharf.io/managed-by"
const DriverLabel = "dra.katalyst.kubewharf.io/driver"

type PublisherConfig struct {
	NodeName        string
	DriverName      string
	OwnerName       string
	ReconcilePeriod time.Duration
	// SliceName optionally preserves a provider's existing resource names.
	// The returned names must be unique across drivers and nodes.
	SliceName func(node, driver string, shard int) string
}

type Publisher struct {
	Config    *PublisherConfig
	Client    kubernetes.Interface
	Inventory func(context.Context) (Inventory, error)
	mu        sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func (p *Publisher) Start(ctx context.Context, inventory Inventory) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		return nil
	}
	if p.Config == nil || p.Config.ReconcilePeriod <= 0 || p.Inventory == nil {
		return fmt.Errorf("DRA publisher requires configuration, positive reconcile period and inventory callback")
	}
	if err := p.Reconcile(ctx, inventory); err != nil {
		return err
	}
	run, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(p.Config.ReconcilePeriod)
		defer ticker.Stop()
		for {
			select {
			case <-run.Done():
				return
			case <-ticker.C:
				inv, err := p.Inventory(run)
				if err == nil {
					err = p.Reconcile(run, inv)
				}
				if err != nil {
					klog.ErrorS(err, "DRA inventory publication failed; retaining previous resources", "driver", p.Config.DriverName)
				}
			}
		}
	}()
	return nil
}
func (p *Publisher) Stop() {
	p.mu.Lock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// BuildSlices keeps each shard within the API's 128-device limit, including
// one empty slice for an explicitly discovered empty inventory.
func BuildSlices(node *corev1.Node, inventory Inventory, generation int64, driverName, ownerName string) []resourcev1.ResourceSlice {
	return (&PublisherConfig{DriverName: driverName, OwnerName: ownerName}).BuildSlices(node, inventory, generation)
}

// BuildSlices applies the configured driver, owner and optional naming hook.
func (c *PublisherConfig) BuildSlices(node *corev1.Node, inventory Inventory, generation int64) []resourcev1.ResourceSlice {
	driverName, ownerName := c.DriverName, c.OwnerName
	name := c.SliceName
	if name == nil {
		name = sliceName
	}
	data, _ := json.Marshal(inventory)
	inventoryHash := fmt.Sprintf("%x", sha256.Sum256(data))
	count := (len(inventory.Devices) + 127) / 128
	if count == 0 {
		count = 1
	}
	out := make([]resourcev1.ResourceSlice, count)
	for i := range out {
		start := i * 128
		end := start + 128
		if end > len(inventory.Devices) {
			end = len(inventory.Devices)
		}
		out[i] = resourcev1.ResourceSlice{ObjectMeta: metav1.ObjectMeta{
			Name:            name(node.Name, driverName, i),
			Labels:          map[string]string{ManagedLabel: ownerName, NodeLabel: nodeKey(node.Name)},
			Annotations:     map[string]string{InventoryAnnotation: inventoryHash},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID}}},
			Spec: resourcev1.ResourceSliceSpec{Driver: driverName, NodeName: ptr.To(node.Name),
				Pool:    resourcev1.ResourcePool{Name: inventory.PoolName, Generation: generation, ResourceSliceCount: int64(count)},
				Devices: inventory.Devices[start:end]}}
	}
	for i := range out {
		out[i].Labels[DriverLabel] = nodeKey(driverName)
	}
	return out
}

func sliceName(node, driver string, shard int) string {
	return fmt.Sprintf("katalyst-dra-%s-%s-%d", nodeKey(driver), nodeKey(node), shard)
}

func (p *Publisher) Reconcile(ctx context.Context, inventory Inventory) error {
	if inventory.PoolName == "" {
		return fmt.Errorf("DRA inventory pool name is required")
	}
	node, err := p.Client.CoreV1().Nodes().Get(ctx, p.Config.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	client := p.Client.ResourceV1().ResourceSlices()
	current, err := client.List(ctx, metav1.ListOptions{FieldSelector: "spec.driver=" + p.Config.DriverName + ",spec.nodeName=" + node.Name})
	if err != nil {
		return err
	}
	owned := map[string]resourcev1.ResourceSlice{}
	generation := int64(1)
	for _, slice := range current.Items {
		if slice.Spec.Driver != p.Config.DriverName || slice.Spec.NodeName == nil || *slice.Spec.NodeName != node.Name {
			continue
		}
		if slice.Labels[ManagedLabel] != p.Config.OwnerName || slice.Labels[NodeLabel] != nodeKey(node.Name) {
			return fmt.Errorf("foreign ResourceSlice %s advertises this driver/node; transfer ownership before enabling QRM", slice.Name)
		}
		found := false
		for _, owner := range slice.OwnerReferences {
			if owner.Kind == "Node" && owner.Name == node.Name && owner.UID == node.UID {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("ResourceSlice %s has a different node owner", slice.Name)
		}
		if slice.Spec.Pool.Generation > generation {
			generation = slice.Spec.Pool.Generation
		}
		owned[slice.Name] = slice
	}
	desired := p.Config.BuildSlices(node, inventory, generation)
	same := len(owned) == len(desired)
	for _, slice := range desired {
		old, ok := owned[slice.Name]
		same = same && ok && reflect.DeepEqual(old.Spec, slice.Spec)
	}
	// A previous reconciliation may have committed only some shards. Resume
	// their generation when they already carry this inventory hash, rather than
	// increasing the generation forever on each retry.
	resume := false
	for _, old := range owned {
		if old.Spec.Pool.Generation == generation && old.Annotations[InventoryAnnotation] == desired[0].Annotations[InventoryAnnotation] {
			resume = true
		}
	}
	if !same && !resume && len(owned) > 0 {
		generation++
	}
	desired = p.Config.BuildSlices(node, inventory, generation)
	desiredNames := map[string]bool{}
	for _, slice := range desired {
		desiredNames[slice.Name] = true
		if old, ok := owned[slice.Name]; ok {
			if reflect.DeepEqual(old.Spec, slice.Spec) && old.Annotations[InventoryAnnotation] == slice.Annotations[InventoryAnnotation] {
				continue
			}
			slice.ResourceVersion = old.ResourceVersion
			slice.UID = old.UID
			if _, err = client.Update(ctx, &slice, metav1.UpdateOptions{}); err != nil {
				return err
			}
		} else if _, err = client.Create(ctx, &slice, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	for name, old := range owned {
		if !desiredNames[name] {
			err = client.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: ptr.To(old.UID), ResourceVersion: ptr.To(old.ResourceVersion)}})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
