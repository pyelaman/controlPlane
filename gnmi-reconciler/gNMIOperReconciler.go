// Driver for Idempotency & Drift Detection (gNMI Operational Reconciler)
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type OpenConfigBGPSession struct {
	NeighborIP string `json:"neighbor_ip"`
	PeerASN    uint32 `json:"peer_asn"`
	VRFName    string `json:"vrf_name"`
	HoldTime   uint16 `json:"hold_time"`
}

func (s OpenConfigBGPSession) StateHash() [32]byte {
	raw, _ := json.Marshal(s)
	return sha256.Sum256(raw)
}

type GNMIClient interface {
	GetOperationalBGP(ctx context.Context, target, vrf, peerIP string) (OpenConfigBGPSession, error)
	ReplaceConfigBGP(ctx context.Context, target string, desired OpenConfigBGPSession) error
}

// MockSwitch simulates a physical spine/leaf switch whose state drifts mid-flight.
type MockSwitch struct {
	mu           sync.Mutex
	currentState OpenConfigBGPSession
}

func (m *MockSwitch) GetOperationalBGP(ctx context.Context, target, vrf, peerIP string) (OpenConfigBGPSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentState, nil
}

func (m *MockSwitch) ReplaceConfigBGP(ctx context.Context, target string, desired OpenConfigBGPSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentState = desired
	return nil
}

func RunDriftDetector(ctx context.Context, client GNMIClient, switchTarget string, desired OpenConfigBGPSession, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	desiredHash := desired.StateHash()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			actual, err := client.GetOperationalBGP(ctx, switchTarget, desired.VRFName, desired.NeighborIP)
			if err != nil {
				continue
			}
			actualHash := actual.StateHash()
			if actualHash == desiredHash {
				fmt.Printf("[TICK] Switch %s matches desired state (Hash: %x). No action taken.\n",
					switchTarget, actualHash[:4])
				continue
			}

			fmt.Printf("[DRIFT DETECTED] Switch %s HoldTime=%d (Expected=%d). Pushing gNMI Set Replace...\n",
				switchTarget, actual.HoldTime, desired.HoldTime)
			_ = client.ReplaceConfigBGP(ctx, switchTarget, desired)
		}
	}
}

func main() {
	desired := OpenConfigBGPSession{
		NeighborIP: "169.254.100.2",
		PeerASN:    65200,
		VRFName:    "VRF-TENANT-FINANCE",
		HoldTime:   9, // Aggressive 9-second hold timer
	}

	sw := &MockSwitch{currentState: desired}
	ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
	defer cancel()

	go RunDriftDetector(ctx, sw, "ny5-spine-01", desired, 150*time.Millisecond)

	// Simulate an out-of-band CLI engineer manually changing HoldTime on the switch at 250ms
	time.Sleep(250 * time.Millisecond)
	sw.mu.Lock()
	sw.currentState.HoldTime = 180
	sw.mu.Unlock()
	fmt.Println("  >>> [OUT-OF-BAND MUTATION] Manual CLI change altered HoldTime to 180s!")

	<-ctx.Done()
	fmt.Println("[SHUTDOWN] Drift detector stopped cleanly.")
}
