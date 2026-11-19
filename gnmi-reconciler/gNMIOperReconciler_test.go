package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type spyGNMIClient struct {
	MockSwitch
	replaceCalls atomic.Int32
}

func (s *spyGNMIClient) ReplaceConfigBGP(ctx context.Context, target string, desired OpenConfigBGPSession) error {
	s.replaceCalls.Add(1)
	return s.MockSwitch.ReplaceConfigBGP(ctx, target, desired)
}

func TestRunDriftDetector_IdempotencyAndRemediation(t *testing.T) {
	desired := OpenConfigBGPSession{
		NeighborIP: "169.254.100.2",
		PeerASN:    65200,
		VRFName:    "VRF-TENANT-FIN",
		HoldTime:   9,
	}

	spy := &spyGNMIClient{
		MockSwitch: MockSwitch{currentState: desired},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunDriftDetector(ctx, spy, "ny5-spine-01", desired, 20*time.Millisecond)

	// Wait 2 ticks with no drift -> ReplaceConfigBGP must NOT be called (idempotency)
	time.Sleep(50 * time.Millisecond)
	if calls := spy.replaceCalls.Load(); calls != 0 {
		t.Fatalf("expected 0 gNMI replace calls when state matches intent, got %d", calls)
	}

	// Inject out-of-band configuration drift
	spy.mu.Lock()
	spy.currentState.HoldTime = 180
	spy.mu.Unlock()

	// Wait for reconciler tick to detect hash mismatch and remediate
	time.Sleep(50 * time.Millisecond)
	if calls := spy.replaceCalls.Load(); calls != 1 {
		t.Fatalf("expected exactly 1 gNMI replace call after drift, got %d", calls)
	}

	actual, _ := spy.GetOperationalBGP(ctx, "ny5-spine-01", desired.VRFName, desired.NeighborIP)
	if actual.HoldTime != 9 {
		t.Errorf("expected HoldTime remediated to 9, got %d", actual.HoldTime)
	}
}
