package config

import (
	"fmt"
	"strings"
	"testing"
)

func TestCanonicalPeeringCallDXGrammar(t *testing.T) {
	for raw, want := range map[string]string{
		" k1abc-01 ": "K1ABC-1", "K1ABC/P": "K1ABC/P", "EA8/K1ABC-00/MM": "EA8/K1ABC/MM",
		"1XX99/1YY99999ABCDEFGH-99/ABCDEFG/AM": "1XX99/1YY99999ABCDEFGH-99/ABCDEFG/AM",
	} {
		if got, ok := CanonicalPeeringCall(raw); !ok || got != want {
			t.Errorf("%q => %q,%v; want %q", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"NODE", "123", "K1ABC.P", "K1ABC-123", "K1ABC^", "K1ABC/"} {
		if got, ok := CanonicalPeeringCall(raw); ok {
			t.Errorf("accepted %q => %q", raw, got)
		}
	}
}

func TestPeeringWireValidation(t *testing.T) {
	base := func() *Config {
		return &Config{Peering: PeeringConfig{Enabled: true, LocalCallsign: "K1LOCAL", NodeVersion: "5457", NodeBuild: "633", LegacyVersion: "5401", PC92Bitmap: 5, HopCount: 99, MaxLineLength: 65536, PC92MaxBytes: 65536,
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
