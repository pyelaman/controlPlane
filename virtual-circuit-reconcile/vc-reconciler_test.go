package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestVirtualCircuitReconciler_LifecycleAndFinalizerSafety(t *testing.T) {
	ctx := context.Background()
	store := &MockKubeStore{objects: make(map[string]*VirtualCircuit)}

	var provisionCount, teardownCount int
	failTeardown := true

	reconciler := &VirtualCircuitReconciler{
		Store: store,
		ProvisionEVPN: func(ctx context.Context, spec VirtualCircuitSpec) error {
			provisionCount++
			return nil
		},
		TeardownEVPN: func(ctx context.Context, spec VirtualCircuitSpec) error {
			teardownCount++
			if failTeardown {
				return errors.New("leaf switch unreachable")
			}
			return nil
		},
	}

	vcName := "vc-ny5-to-azure"
	store.objects[vcName] = &VirtualCircuit{
		Name: vcName,
		Spec: VirtualCircuitSpec{VNI: 500300, PeerIP: "169.254.30.2", RemoteASN: 12076},
	}

	// 1. Create reconciliation attaches finalizer and provisions EVPN
	if err := reconciler.Reconcile(ctx, vcName); err != nil {
		t.Fatalf("unexpected error during creation reconcile: %v", err)
	}
	if !slices.Contains(store.objects[vcName].Finalizers, circuitFinalizer) {
		t.Fatal("expected finalizer to be attached to VirtualCircuit")
	}
	if !store.objects[vcName].Status.Provisioned || provisionCount != 1 {
		t.Fatalf("expected circuit provisioned once, got count=%d", provisionCount)
	}

	// 2. Trigger deletion while southbound switch is unreachable -> CRD MUST NOT be deleted
	now := time.Now()
	store.objects[vcName].DeletionTimestamp = &now
	if err := reconciler.Reconcile(ctx, vcName); err == nil {
		t.Fatal("expected error when southbound teardown fails")
	}
	if _, exists := store.objects[vcName]; !exists {
		t.Fatal("safety violation: VirtualCircuit deleted from store despite failed hardware teardown")
	}

	// 3. Switch recovers -> teardown succeeds, finalizer is stripped, and object is removed
	failTeardown = false
	if err := reconciler.Reconcile(ctx, vcName); err != nil {
		t.Fatalf("unexpected error during recovery teardown: %v", err)
	}
	if _, exists := store.objects[vcName]; exists {
		t.Fatal("expected VirtualCircuit to be removed from store after finalizer cleanup")
	}
	if teardownCount != 2 {
		t.Errorf("expected 2 teardown attempts, got %d", teardownCount)
	}
}
