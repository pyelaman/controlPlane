package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// CircuitSpec represents the desired multi-tenant L2/L3 state (e.g., from a Kubernetes CRD)
type CircuitSpec struct {
	TenantID  string
	CircuitID string
	OuterVLAN uint16 // QinQ Service Provider Tag
	InnerVLAN uint16 // Customer Tag
	VNI       uint32 // EVPN-LAN Virtual Network Identifier
	BGPPeerIP string
	PeerASN   uint32
	IPSecPSK  string
}

// DeviceClient abstracts gNMI / NETCONF / GoBGP gRPC southbound calls
type DeviceClient interface {
	ApplyEVPNConfig(ctx context.Context, spec CircuitSpec) error
	ConfigureBGPPeer(ctx context.Context, spec CircuitSpec) error
	Close() error
}

// MockSouthboundDriver simulates a gRPC/gNMI network device connection
type MockSouthboundDriver struct {
	DeviceName string
}

func (m *MockSouthboundDriver) ApplyEVPNConfig(ctx context.Context, spec CircuitSpec) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(25 * time.Millisecond): // Simulate gRPC network RTT
		return nil
	}
}

func (m *MockSouthboundDriver) ConfigureBGPPeer(ctx context.Context, spec CircuitSpec) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(15 * time.Millisecond):
		return nil
	}
}

func (m *MockSouthboundDriver) Close() error { return nil }

// Reconciler manages concurrent provisioning across leaf switches and edge routers
type FabricReconciler struct {
	logger  *slog.Logger
	devices map[string]DeviceClient
	mu      sync.RWMutex
}

func NewFabricReconciler(logger *slog.Logger) *FabricReconciler {
	return &FabricReconciler{
		logger:  logger,
		devices: make(map[string]DeviceClient),
	}
}

func (r *FabricReconciler) RegisterDevice(id string, client DeviceClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.devices[id] = client
}

// ProvisionMultiTenantCircuit pushes L2 EVPN and L3 BGP state concurrently across target devices
// with strict timeout enforcement and error aggregation.
func (r *FabricReconciler) ProvisionMultiTenantCircuit(parentCtx context.Context, targetDevices []string, spec CircuitSpec) error {
	// Enforce a strict SLA timeout for southbound hardware programming
	ctx, cancel := context.WithTimeout(parentCtx, 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errChan := make(chan error, len(targetDevices))

	for _, devID := range targetDevices {
		r.mu.RLock()
		client, exists := r.devices[devID]
		r.mu.RUnlock()

		if !exists {
			return fmt.Errorf("target leaf/edge device %s not registered in inventory", devID)
		}

		wg.Add(1)
		go func(id string, dev DeviceClient) {
			defer wg.Done()

			// Step 1: Program L2 EVPN-LAN / QinQ translation via gNMI Set
			if err := dev.ApplyEVPNConfig(ctx, spec); err != nil {
				errChan <- fmt.Errorf("[%s] EVPN gNMI programming failed: %w", id, err)
				return
			}

			// Step 2: Program L3 BGP EVPN Peer via GoBGP/gRPC
			if err := dev.ConfigureBGPPeer(ctx, spec); err != nil {
				errChan <- fmt.Errorf("[%s] BGP peer programming failed: %w", id, err)
				return
			}

			r.logger.Info("Successfully reconciled tenant circuit",
				"device", id,
				"tenant", spec.TenantID,
				"vni", spec.VNI,
				"qinq", fmt.Sprintf("%d.%d", spec.OuterVLAN, spec.InnerVLAN),
			)
		}(devID, client)
	}

	wg.Wait()
	close(errChan)

	// Check for any partial failures (In production, trigger compensating rollback here)
	for err := range errChan {
		return err
	}

	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	reconciler := NewFabricReconciler(logger)

	// Register leaf switches across two Equinix IBX metros
	reconciler.RegisterDevice("sv1-leaf-01", &MockSouthboundDriver{DeviceName: "sv1-leaf-01"})
	reconciler.RegisterDevice("ny5-leaf-02", &MockSouthboundDriver{DeviceName: "ny5-leaf-02"})

	spec := CircuitSpec{
		TenantID:  "ent-fin-9921",
		CircuitID: "vc-88412",
		OuterVLAN: 1050,
		InnerVLAN: 100,
		VNI:       500100,
		BGPPeerIP: "169.254.10.2",
		PeerASN:   65100,
	}

	if err := reconciler.ProvisionMultiTenantCircuit(context.Background(),
		[]string{"sv1-leaf-01", "ny5-leaf-02"}, spec); err != nil {
		logger.Error("Circuit provisioning failed", "error", err)
		os.Exit(1)
	}
}
