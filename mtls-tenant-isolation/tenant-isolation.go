package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type TenantBoundary struct {
	TenantSPIFFEID  string
	OuterSVLAN      uint16
	EVPNRouteTarget string
}

// NewSecureSouthboundConn builds a gRPC client enforcing mTLS with SPIFFE URI SAN verification.
func NewSecureSouthboundConn(targetAddr string, clientCert tls.Certificate, trustBundle *x509.CertPool, expectedServerSPIFFE string) (*grpc.ClientConn, error) {
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      trustBundle,
		MinVersion:   tls.VersionTLS13,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
				return fmt.Errorf("missing verified certificate chain")
			}
			leafCert := verifiedChains[0][0]
			for _, uri := range leafCert.URIs {
				if uri.String() == expectedServerSPIFFE {
					return nil
				}
			}
			return fmt.Errorf("SPIFFE ID mismatch: expected %s", expectedServerSPIFFE)
		},
	}

	return grpc.NewClient(targetAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
}

func ValidateTenantIsolation(callerSPIFFE string, boundary TenantBoundary) error {
	if !strings.EqualFold(callerSPIFFE, boundary.TenantSPIFFEID) {
		return fmt.Errorf("security violation: identity %s unauthorized for S-VLAN %d", callerSPIFFE, boundary.OuterSVLAN)
	}
	if boundary.OuterSVLAN < 100 || boundary.OuterSVLAN > 4094 {
		return fmt.Errorf("invalid 802.1ad S-VLAN boundary: %d", boundary.OuterSVLAN)
	}
	return nil
}

// Helper to mint a mock X.509 SVID with a SPIFFE URI SAN
func generateMockSPIFFECert(spiffeURI string) (tls.Certificate, *x509.CertPool, error) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	uri, _ := url.Parse(spiffeURI)

	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "equinix-fabric-workload"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		URIs:                  []*url.URL{uri},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	pool := x509.NewCertPool()
	cert, _ := x509.ParseCertificate(derBytes)
	pool.AddCert(cert)

	tlsCert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
		Leaf:        cert,
	}
	return tlsCert, pool, nil
}

func main() {
	validSPIFFE := "spiffe://://equinix.com"
	rogueSPIFFE := "spiffe://://equinix.com"

	cert, _, err := generateMockSPIFFECert(validSPIFFE)
	if err != nil {
		panic(err)
	}

	extractedURI := cert.Leaf.URIs[0].String()
	fmt.Printf("[AUTH] Extracted X.509 SVID URI SAN: %s\n", extractedURI)

	boundary := TenantBoundary{
		TenantSPIFFEID:  validSPIFFE,
		OuterSVLAN:      1050,
		EVPNRouteTarget: "65000:500100",
	}

	// Test 1: Authorized Tenant Request
	if err := ValidateTenantIsolation(extractedURI, boundary); err != nil {
		fmt.Printf("[FAIL] Unexpected rejection: %v\n", err)
	} else {
		fmt.Printf("[ALLOW] Identity verified. Provisioning QinQ S-VLAN %d mapped to EVPN RT %s\n",
			boundary.OuterSVLAN, boundary.EVPNRouteTarget)
	}

	// Test 2: Cross-Tenant Lateral Movement Attempt
	if err := ValidateTenantIsolation(rogueSPIFFE, boundary); err != nil {
		fmt.Printf("[BLOCKED] Caught unauthorized cross-tenant call -> %v\n", err)
	}
}
