package main

import (
	"context"
	"errors"
	"testing"
)

func TestGracefulVTEPMigration_SuccessAndRollback(t *testing.T) {
	inh := &IndirectNextHop{
		ID:         "inh-sv1-core-01",
		BackupVTEP: "10.255.0.2",
	}
	inh.PrimaryVTEP.Store("10.255.0.1")
	inh.MACSeqNum.Store(10)
	inh.CurrentStatus.Store(uint32(StateActive))

	// Case 1: Southbound ASIC SDK failure triggers atomic state rollback
	failSDK := func(vtep string, seq uint32) error {
		return errors.New("ASIC table full")
	}

	err := inh.GracefulVTEPMigration(context.Background(), failSDK)
	if err == nil {
		t.Fatal("expected error when ASIC SDK fails, got nil")
	}
	if inh.PrimaryVTEP.Load() != "10.255.0.1" {
		t.Errorf("expected PrimaryVTEP to remain 10.255.0.1 on failure, got %v", inh.PrimaryVTEP.Load())
	}
	if inh.CurrentStatus.Load() != uint32(StateActive) {
		t.Errorf("expected state rollback to StateActive (%d), got %d", StateActive, inh.CurrentStatus.Load())
	}

	// Case 2: Successful hitless migration swaps VTEP and increments MAC mobility sequence
	var programmedVTEP string
	var programmedSeq uint32
	successSDK := func(vtep string, seq uint32) error {
		programmedVTEP = vtep
		programmedSeq = seq
		return nil
	}

	if err := inh.GracefulVTEPMigration(context.Background(), successSDK); err != nil {
		t.Fatalf("unexpected migration error: %v", err)
	}
	if programmedVTEP != "10.255.0.2" || programmedSeq != 12 {
		t.Errorf("expected SDK call with VTEP=10.255.0.2, Seq=12; got VTEP=%s, Seq=%d", programmedVTEP, programmedSeq)
	}
	if inh.CurrentStatus.Load() != uint32(StateStandby) {
		t.Errorf("expected final state StateStandby (%d), got %d", StateStandby, inh.CurrentStatus.Load())
	}
}
