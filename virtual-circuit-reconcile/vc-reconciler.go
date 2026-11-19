package main

import (
	"context"
	"fmt"
	"slices"
	"time"
)

const circuitFinalizer = "://equinix.com"

type VirtualCircuitSpec struct {
	VNI       uint32
	PeerIP    string
	RemoteASN uint32
}

type VirtualCircuitStatus struct {
	Provisioned bool
	Phase       string
}

type VirtualCircuit struct {
	Name              string
	Finalizers        []string
	DeletionTimestamp *time.Time
	Spec              VirtualCircuitSpec
	Status            VirtualCircuitStatus
}

// MockKubeStore simulates Kubernetes etcd object storage
type MockKubeStore struct {
	objects map[string]*VirtualCircuit
}

type VirtualCircuitReconciler struct {
	Store         *MockKubeStore
	ProvisionEVPN func(ctx context.Context, spec VirtualCircuitSpec) error
	TeardownEVPN  func(ctx context.Context, spec VirtualCircuitSpec) error
}

func (r *VirtualCircuitReconciler) Reconcile(ctx context.Context, name string) error {
	vc, exists := r.Store.objects[name]
	if !exists {
		return nil
	}

	// 1. Handle Deletion via Finalizer
	if vc.DeletionTimestamp != nil {
		if slices.Contains(vc.Finalizers, circuitFinalizer) {
			fmt.Printf("[RECONCILE] Deletion triggered for %s. Executing southbound EVPN teardown...\n", name)
			if err := r.TeardownEVPN(ctx, vc.Spec); err != nil {
				return err
			}
			// Remove Finalizer so Kubernetes can garbage-collect the CRD
			vc.Finalizers = slices.DeleteFunc(vc.Finalizers, func(f string) bool { return f == circuitFinalizer })
			delete(r.Store.objects, name)
			fmt.Printf("[RECONCILE] Finalizer removed. Object %s deleted from cluster.\n", name)
		}
		return nil
	}

	// 2. Register Finalizer on creation
	if !slices.Contains(vc.Finalizers, circuitFinalizer) {
		vc.Finalizers = append(vc.Finalizers, circuitFinalizer)
		fmt.Printf("[RECONCILE] Attached finalizer %s to %s\n", circuitFinalizer, name)
	}

	// 3. Reconcile desired EVPN circuit state
	if err := r.ProvisionEVPN(ctx, vc.Spec); err != nil {
		return err
	}

	vc.Status.Provisioned = true
	vc.Status.Phase = "Active"
	fmt.Printf("[RECONCILE] %s Status updated -> Phase: %s, VNI: %d, Peer: %s\n",
		name, vc.Status.Phase, vc.Spec.VNI, vc.Spec.PeerIP)
	return nil
}

func main() {
	store := &MockKubeStore{objects: make(map[string]*VirtualCircuit)}
	reconciler := &VirtualCircuitReconciler{
		Store: store,
		ProvisionEVPN: func(ctx context.Context, spec VirtualCircuitSpec) error {
			fmt.Printf("  --> [SOUTHBOUND] Configured BGP EVPN VNI %d with Peer %s (AS%d)\n", spec.VNI, spec.PeerIP, spec.RemoteASN)
			return nil
		},
		TeardownEVPN: func(ctx context.Context, spec VirtualCircuitSpec) error {
			fmt.Printf("  --> [SOUTHBOUND] Purged BGP EVPN VNI %d and flushed MAC tables\n", spec.VNI)
			return nil
		},
	}

	ctx := context.Background()
	circuitName := "vc-sv1-to-aws-directconnect"

	// Step 1: User applies VirtualCircuit YAML manifest
	store.objects[circuitName] = &VirtualCircuit{
		Name: circuitName,
		Spec: VirtualCircuitSpec{VNI: 500200, PeerIP: "169.254.20.2", RemoteASN: 64512},
	}
	_ = reconciler.Reconcile(ctx, circuitName)

	// Step 2: User executes `kubectl delete virtualcircuit vc-sv1-to-aws-directconnect`
	fmt.Println("\n[EVENT] User requested CRD deletion...")
	now := time.Now()
	store.objects[circuitName].DeletionTimestamp = &now
	_ = reconciler.Reconcile(ctx, circuitName)
}
