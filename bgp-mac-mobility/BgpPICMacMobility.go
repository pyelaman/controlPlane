// Driver for Zero-Drop Failover State Machine (BGP PIC & MAC Mobility)
package main

import (
	"context"
	"fmt"
	"sync/atomic"
)

type LinkState uint32

const (
	StateActive   LinkState = 1
	StateDraining LinkState = 2
	StateStandby  LinkState = 3
)

// IndirectNextHop mirrors the hardware ASIC/DPDK shared pointer table.
// Mutating PrimaryVTEP atomically shifts 100k+ dependent EVPN routes in O(1) time.
type IndirectNextHop struct {
	ID            string
	PrimaryVTEP   atomic.Value // string (e.g., "10.255.0.1")
	BackupVTEP    string
	MACSeqNum     atomic.Uint32 // BGP EVPN RFC 7432 MAC Mobility Sequence
	CurrentStatus atomic.Uint32
}

// GracefulVTEPMigration executes a hitless failover aware of ASIC pointer mechanics.
func (inh *IndirectNextHop) GracefulVTEPMigration(ctx context.Context,
	pushASICPointer func(vtep string, seq uint32) error) error {
	// Phase 1: Enter Draining state (control plane stops advertising new prefixes to this VTEP)
	inh.CurrentStatus.Store(uint32(StateDraining))

	// Phase 2: Increment EVPN MAC Mobility sequence so remote leaves update Type-2 tables cleanly
	newSeq := inh.MACSeqNum.Add(1)

	// Phase 3: Atomically swap the hardware Indirect Next-Hop pointer BEFORE tearing down old tunnels
	if err := pushASICPointer(inh.BackupVTEP, newSeq); err != nil {
		inh.CurrentStatus.Store(uint32(StateActive)) // Rollback state
		return fmt.Errorf("ASIC INH pointer swap failed: %w", err)
	}

	inh.PrimaryVTEP.Store(inh.BackupVTEP)
	inh.CurrentStatus.Store(uint32(StateStandby))
	return nil
}

func main() {
	inh := &IndirectNextHop{
		ID:         "inh-sv1-core-01",
		BackupVTEP: "10.255.0.2",
	}
	inh.PrimaryVTEP.Store("10.255.0.1")
	inh.MACSeqNum.Store(10)
	inh.CurrentStatus.Store(uint32(StateActive))

	fmt.Printf("[PRE-MIGRATION]  INH ID: %s | Active VTEP: %s | MAC Seq: %d | State: %d\n",
		inh.ID, inh.PrimaryVTEP.Load(), inh.MACSeqNum.Load(), inh.CurrentStatus.Load())

	// Simulate southbound gRPC/SDK call to program Broadcom/DPDK hardware table
	mockHardwareUpdate := func(vtep string, seq uint32) error {
		fmt.Printf("  --> [HARDWARE SDK] Atomically updating ASIC INH pointer -> VTEP: %s, EVPN Mobility Seq: %d\n", vtep, seq)
		return nil
	}

	if err := inh.GracefulVTEPMigration(context.Background(), mockHardwareUpdate); err != nil {
		fmt.Printf("Migration failed: %v\n", err)
		return
	}

	fmt.Printf("[POST-MIGRATION] INH ID: %s | Active VTEP: %s | MAC Seq: %d | State: %d\n",
		inh.ID, inh.PrimaryVTEP.Load(), inh.MACSeqNum.Load(), inh.CurrentStatus.Load())
}
