package config

import (
	"testing"
)

func TestPeeringForwardSpotsDefaultsFalseWhenOmitted(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `peering:
  enabled: true
  peers:
    - enabled: true
      host: "peer.example.net"
      remote_callsign: "K1PEER"
      port: 7300
`
	writeTestConfigOverlay(t, dir, "peering.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Peering.ForwardSpots {
		t.Fatalf("expected omitted peering.forward_spots to default false")
	}
}

func TestPeeringForwardSpotsHonorsExplicitFalse(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `peering:
  enabled: true
  forward_spots: false
  peers:
    - enabled: true
      host: "peer.example.net"
      remote_callsign: "K1PEER"
      port: 7300
`
	writeTestConfigOverlay(t, dir, "peering.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Peering.ForwardSpots {
		t.Fatalf("expected explicit peering.forward_spots=false to remain false")
	}
}

func TestPeeringForwardSpotsHonorsExplicitTrue(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `peering:
  enabled: true
  forward_spots: true
  peers:
    - enabled: true
      host: "peer.example.net"
      remote_callsign: "K1PEER"
      port: 7300
`
	writeTestConfigOverlay(t, dir, "peering.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Peering.ForwardSpots {
		t.Fatalf("expected explicit peering.forward_spots=true to remain true")
	}
}
