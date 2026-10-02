package config

import (
	"fmt"
	"strings"
	"testing"
)

func TestCanonicalPeeringCallDXGrammar(t *testing.T) {
	for raw, want := range map[string]string{
		" k1abc-01 ": "K1ABC-1", "K1ABC/P": "K1ABC", "EA8/K1ABC/MM-00": "K1ABC",
		"K1ABC/P-01": "K1ABC-1", "EA8/K1ABC": "K1ABC", "K1ABC/": "K1ABC",
		"K1ABC-0000000000000000000000000001": "K1ABC-1",
	} {
		if got, ok := CanonicalPeeringCall(raw); !ok || got != want {
			t.Errorf("%q => %q,%v; want %q", raw, got, ok, want)
		}
		if got, ok := CanonicalPeeringCall(want); !ok || got != want {
			t.Errorf("unstable canonical identity %q => %q,%v", want, got, ok)
		}
	}
	for _, raw := range []string{"NODE", "123", "K1ABC.P", "K1ABC-123", "K1ABC^", "W1AW/P", "K1ABC-01/P", "EA8/K1ABC-00/MM", "1XX99/1YY99999ABCDEFGH-99/ABCDEFG/AM"} {
		if got, ok := CanonicalPeeringCall(raw); ok {
			t.Errorf("accepted %q => %q", raw, got)
		}
	}
}

func TestPeeringWireValidation(t *testing.T) {
	base := func() *Config {
		return &Config{Peering: PeeringConfig{Enabled: true, MaxPeers: 64, LocalCallsign: "K1LOCAL", NodeVersion: "5457", NodeBuild: "633", LegacyVersion: "5401", PC92Bitmap: 5, HopCount: 99, MaxLineLength: 65536, PC92MaxBytes: 65536,
			Peers: []PeeringPeer{{Enabled: true, Host: "peer.invalid", Port: 7300, RemoteCallsign: "K1PEER", LoginCallsign: "K1LOCAL"}}}}
	}
	for _, tc := range []struct {
		name string
		edit func(*PeeringConfig)
	}{
		{"local", func(c *PeeringConfig) { c.LocalCallsign = "NODE" }},
		{"login mismatch", func(c *PeeringConfig) { c.Peers[0].LoginCallsign = "K2OTHER" }},
		{"unknown remote", func(c *PeeringConfig) { c.Peers[0].RemoteCallsign = "" }},
		{"version injection", func(c *PeeringConfig) { c.NodeVersion = "5457^PC20" }},
		{"build nonnumeric", func(c *PeeringConfig) { c.NodeBuild = "git123" }},
		{"version limit", func(c *PeeringConfig) { c.NodeVersion = "12345678901" }},
		{"line limit", func(c *PeeringConfig) { c.MaxLineLength = 65537 }},
		{"pc92 limit before clamp", func(c *PeeringConfig) { c.PC92MaxBytes = 65537 }},
		{"flags", func(c *PeeringConfig) { c.PC92Bitmap = 1 }},
		{"external flags 6", func(c *PeeringConfig) { c.PC92Bitmap = 6 }},
		{"external flags 7", func(c *PeeringConfig) { c.PC92Bitmap = 7 }},
		{"backoff base limit", func(c *PeeringConfig) { c.Backoff.BaseMS = 300001 }},
		{"backoff max limit", func(c *PeeringConfig) { c.Backoff.MaxMS = 300001 }},
		{"hop", func(c *PeeringConfig) { c.HopCount = 100 }},
		{"SSID collision", func(c *PeeringConfig) {
			c.Peers[0].RemoteCallsign = "K1PEER-1"
			p := c.Peers[0]
			p.RemoteCallsign = "K1PEER-01"
			c.Peers = append(c.Peers, p)
		}},
		{"peer cap", func(c *PeeringConfig) {
			p := c.Peers[0]
			c.Peers = nil
			for i := 0; i < 65; i++ {
				p.RemoteCallsign = fmt.Sprintf("K%dPEER", i)
				c.Peers = append(c.Peers, p)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.edit(&cfg.Peering)
			if err := normalizePeeringConfig(cfg); err == nil {
				t.Fatal("expected contract rejection")
			}
		})
	}
	cfg := base()
	cfg.Peering.NodeBuild = ""
	cfg.Peering.KeepaliveSeconds, cfg.Peering.ConfigSeconds = 0, 0
	cfg.Peering.MaxLineLength, cfg.Peering.PC92MaxBytes = 1024, 512
	if err := normalizePeeringConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Peering.NodeBuild != "" || cfg.Peering.KeepaliveSeconds != 0 || cfg.Peering.ConfigSeconds != 0 || cfg.Peering.PC92MaxBytes != 512 {
		t.Fatal("explicit empty/zero/smaller-bound sentinel lost")
	}
}

func TestActivePeeringContractOwnsNormalization(t *testing.T) {
	cfg := PeeringConfig{MaxPeers: 64, LocalCallsign: "EA8/N0CALL/P-00", NodeVersion: "5457", LegacyVersion: "5457", PC92Bitmap: 5,
		Peers: []PeeringPeer{{Enabled: true, RemoteCallsign: "EA8/K1PEER/P-01", LoginCallsign: "N0CALL-0"}}}
	normalized, local, err := NormalizeActivePeeringWireContract(cfg, "N0CALL")
	if err != nil || local != "N0CALL" || normalized.Peers[0].RemoteCallsign != "K1PEER-1" || normalized.Peers[0].LoginCallsign != local {
		t.Fatalf("normalized=%+v local=%s err=%v", normalized, local, err)
	}
	if cfg.Peers[0].RemoteCallsign != "EA8/K1PEER/P-01" || cfg.Peers[0].LoginCallsign != "N0CALL-0" {
		t.Fatal("constructor mutated caller-owned peer storage")
	}
	withDormant := cfg
	withDormant.Peers = append(append([]PeeringPeer(nil), cfg.Peers...), make([]PeeringPeer, 10000)...)
	bounded, _, err := NormalizeActivePeeringWireContract(withDormant, "N0CALL")
	if err != nil || len(bounded.Peers) != 1 || cap(bounded.Peers) != 1 {
		t.Fatalf("active peer copy retained dormant entries: len=%d cap=%d err=%v", len(bounded.Peers), cap(bounded.Peers), err)
	}
	if _, _, err := NormalizeActivePeeringWireContract(cfg, "N9OTHER"); err == nil {
		t.Fatal("accepted argument/config mismatch")
	}
	cfg.Enabled = false
	cfg.LocalCallsign = "INVALID"
	cfg.PC92Bitmap = 7
	if err := validatePeeringWireContract(&cfg); err != nil {
		t.Fatalf("disabled dormant config rejected: %v", err)
	}
	if _, _, err := NormalizeActivePeeringWireContract(cfg, "N0CALL"); err == nil {
		t.Fatal("disabled bit bypassed active constructor validation")
	}
}

func BenchmarkCanonicalPeeringCall(b *testing.B) {
	for _, call := range []string{"K1ABC", "K1ABC-1", "EA8/K1ABC/P-01"} {
		b.Run(call, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, ok := CanonicalPeeringCall(call); !ok {
					b.Fatal(call)
				}
			}
		})
	}
}

func TestPeeringWireConfigLoadEmptyBuild(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "peering.yaml", "peering:\n  enabled: true\n  node_build: \"\"\n  config_seconds: 0\n  keepalive_seconds: 0\n")
	cfg, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(cfg.Peering.NodeBuild) != "" || cfg.Peering.ConfigSeconds != 0 || cfg.Peering.KeepaliveSeconds != 0 {
		t.Fatal("loader did not preserve wire sentinels")
	}
}
