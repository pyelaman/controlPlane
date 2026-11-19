package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type MultusSRIOVConfig struct {
	CNIVersion string                 `json:"cniVersion"`
	Type       string                 `json:"type"`
	DeviceID   string                 `json:"deviceID"`
	VLAN       int                    `json:"vlan"`
	SpoofChk   string                 `json:"spoofchk"`
	Trust      string                 `json:"trust"`
	IPAM       map[string]interface{} `json:"ipam"`
}

func BuildCNFNetworkAttachment(pciAddr string, vlanID int, isRouterCNF bool) (string, error) {
	if pciAddr == "" {
		return "", fmt.Errorf("PCIe Bus:Device.Function address is required")
	}

	spoofCheck := "on"
	trustMode := "off"
	if isRouterCNF {
		spoofCheck = "off"
		trustMode = "on"
	}

	cfg := MultusSRIOVConfig{
		CNIVersion: "0.3.1",
		Type:       "sriov",
		DeviceID:   pciAddr,
		VLAN:       vlanID,
		SpoofChk:   spoofCheck,
		Trust:      trustMode,
		IPAM:       map[string]interface{}{},
	}

	payload, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to serialize SR-IOV CNI config: %w", err)
	}

	return string(payload), nil
}

func main() {
	// Generate Multus CNI config for a DPDK/VPP Virtual Edge Router Pod
	// Passing physical NIC Virtual Function 0000:3b:02.1 directly into the pod namespace
	cniJSON, err := BuildCNFNetworkAttachment("0000:3b:02.1", 1050, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating SR-IOV config: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("--- Generated Multus SR-IOV NetworkAttachmentDefinition Config ---")
	fmt.Println(cniJSON)
}
