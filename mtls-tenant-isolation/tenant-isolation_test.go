package main

import (
	"crypto/tls"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestValidateTenantIsolation_TableDriven(t *testing.T) {
	validBoundary := TenantBoundary{
		TenantSPIFFEID:  "spiffe://example.org/ns/fabric/sa/tenant-fin-01",
		OuterSVLAN:      1050,
		EVPNRouteTarget: "65000:500100",
	}

	tests := []struct {
		name         string
		callerSPIFFE string
		boundary     TenantBoundary
		wantErr      bool
	}{
		{
			name:         "Authorized tenant and valid S-VLAN",
			callerSPIFFE: "spiffe://example.org/ns/fabric/sa/tenant-fin-01",
			boundary:     validBoundary,
			wantErr:      false,
		},
		{
			name:         "Cross-tenant spoofing attempt",
			callerSPIFFE: "spiffe://example.org/ns/fabric/sa/tenant-rogue-99",
			boundary:     validBoundary,
			wantErr:      true,
		},
		{
			name:         "Reserved S-VLAN below threshold (<100)",
			callerSPIFFE: "spiffe://example.org/ns/fabric/sa/tenant-fin-01",
			boundary: TenantBoundary{
				TenantSPIFFEID: "spiffe://example.org/ns/fabric/sa/tenant-fin-01",
				OuterSVLAN:     10,
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTenantIsolation(tc.callerSPIFFE, tc.boundary)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateTenantIsolation() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestSPIFFEMutualTLSHandshake(t *testing.T) {
	serverSPIFFE := "spiffe://://equinix.com"
	serverCert, serverPool, err := generateMockSPIFFECert(serverSPIFFE)
	if err != nil {
		t.Fatalf("failed to generate server SVID: %v", err)
	}

	clientCert, _, err := generateMockSPIFFECert("spiffe://://equinix.com")
	if err != nil {
		t.Fatalf("failed to generate client SVID: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	srvCreds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverCert}})
	grpcSrv := grpc.NewServer(grpc.Creds(srvCreds))
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	// Verify connection succeeds when expected SPIFFE ID matches certificate URI SAN
	conn, err := NewSecureSouthboundConn(lis.Addr().String(), clientCert, serverPool, serverSPIFFE)
	if err != nil {
		t.Fatalf("expected valid mTLS connection, got error: %v", err)
	}
	_ = conn.Close()
}
