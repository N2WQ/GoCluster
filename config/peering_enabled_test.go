package config

import (
	"testing"
)

func TestPeeringPeerEnabledDefaultsFalseWhenOmitted(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `peering:
  enabled: true
  peers:
    - host: "peer-omitted.example.net"
      port: 7300
`
	writeTestConfigOverlay(t, dir, "peering.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if len(cfg.Peering.Peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(cfg.Peering.Peers))
	}
	if cfg.Peering.Peers[0].Enabled {
		t.Fatalf("expected omitted peering.peers[0].enabled to default false")
	}
}

func TestPeeringPeerEnabledHonorsExplicitFalse(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `peering:
  enabled: true
  peers:
    - enabled: false
      host: "peer-disabled.example.net"
      port: 7300
`
	writeTestConfigOverlay(t, dir, "peering.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if len(cfg.Peering.Peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(cfg.Peering.Peers))
	}
	if cfg.Peering.Peers[0].Enabled {
		t.Fatalf("expected explicit peering.peers[0].enabled=false to remain false")
	}
}

func TestPeeringPeerEnabledHonorsExplicitTrue(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `peering:
  enabled: true
  peers:
    - enabled: true
      host: "peer-enabled.example.net"
      remote_callsign: "K1PEER"
      port: 7300
`
	writeTestConfigOverlay(t, dir, "peering.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if len(cfg.Peering.Peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(cfg.Peering.Peers))
	}
	if !cfg.Peering.Peers[0].Enabled {
		t.Fatalf("expected explicit peering.peers[0].enabled=true to remain true")
	}
}
