package main

import (
	"encoding/json"
	"testing"
)

func TestBuildCNFNetworkAttachment(t *testing.T) {
	// Case 1: Empty PCIe address must fail validation
	if _, err := BuildCNFNetworkAttachment("", 1050, true); err == nil {
		t.Fatal("expected error when PCIe BDF address is empty")
	}

	// Case 2: Virtual Router CNF requires spoofchk=off and trust=on
	routerJSON, err := BuildCNFNetworkAttachment("0000:3b:02.1", 1050, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsedRouter MultusSRIOVConfig
	if err := json.Unmarshal([]byte(routerJSON), &parsedRouter); err != nil {
		t.Fatalf("invalid JSON generated: %v", err)
	}
	if parsedRouter.Type != "sriov" || parsedRouter.DeviceID != "0000:3b:02.1" || parsedRouter.VLAN != 1050 {
		t.Errorf("unexpected SR-IOV hardware parameters: %+v", parsedRouter)
	}
	if parsedRouter.SpoofChk != "off" || parsedRouter.Trust != "on" {
		t.Errorf("router CNF requires spoofchk=off, trust=on; got spoofchk=%s, trust=%s",
			parsedRouter.SpoofChk, parsedRouter.Trust)
	}

	// Case 3: Standard non-router workload must enforce strict spoof checking
	workloadJSON, err := BuildCNFNetworkAttachment("0000:3b:02.2", 200, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsedWorkload MultusSRIOVConfig
	_ = json.Unmarshal([]byte(workloadJSON), &parsedWorkload)
	if parsedWorkload.SpoofChk != "on" || parsedWorkload.Trust != "off" {
		t.Errorf("standard workload requires spoofchk=on, trust=off; got spoofchk=%s, trust=%s",
			parsedWorkload.SpoofChk, parsedWorkload.Trust)
	}
}
