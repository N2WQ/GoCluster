package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPeeringMaxPeersLoadRejectsInvalidRawValues(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, scalar := range []string{"missing", "null", "0", "-1", "65", "1.5", "1.0", "6.4e1", `"8"`, "true", "[]", "{}", ".inf", ".nan", "18446744073709551616"} {
			t.Run(fmt.Sprintf("enabled=%t/%s", enabled, scalar), func(t *testing.T) {
				dir := testConfigDir(t)
				writeTestConfigOverlay(t, dir, "peering.yaml", fmt.Sprintf("peering:\n  enabled: %t\n", enabled))
				// Preserve the original scalar bytes. A YAML round trip in the
				// fixture helper can turn 1.0 into 1 and conceal the lossy decode.
				file := filepath.Join(dir, "peering.yaml")
				data, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				lines := strings.Split(string(data), "\n")
				found := false
				for i, line := range lines {
					if strings.HasPrefix(strings.TrimSpace(line), "max_peers:") {
						found = true
						indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
						lines[i] = indent + "max_peers: " + scalar
						if scalar == "missing" {
							lines[i] = ""
						}
					}
				}
				if !found {
					t.Fatal("shipped peering.max_peers fixture missing")
				}
				if err := os.WriteFile(file, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
					t.Fatal(err)
				}
				if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "peering.max_peers") {
					t.Fatalf("invalid raw cap %q: expected max_peers error, got %v", scalar, err)
				}
			})
		}
	}
}

func TestPeeringMaxPeersLoadPopulation(t *testing.T) {
	for _, n := range []int{1, 2, 8, 63, 64} {
		for _, enabled := range []bool{false, true} {
			for _, extra := range []int{0, 1} {
				t.Run(fmt.Sprintf("cap=%d/enabled=%t/extra=%d", n, enabled, extra), func(t *testing.T) {
					dir := testConfigDir(t)
					var body strings.Builder
					fmt.Fprintf(&body, "peering:\n  enabled: %t\n  max_peers: %d\n  peers:\n", enabled, n)
					for i := 0; i < n+extra; i++ {
						fmt.Fprintf(&body, "    - enabled: true\n      direction: both\n      host: peer.example.invalid\n      port: 7300\n      remote_callsign: K%dPEER\n", i)
					}
					// A valid dormant row must not consume an active identity.
					body.WriteString("    - enabled: false\n      direction: inbound\n      remote_callsign: K999PEER\n")
					writeTestConfigOverlay(t, dir, "peering.yaml", body.String())
					cfg, err := Load(dir)
					if enabled && extra != 0 {
						if err == nil || !strings.Contains(err.Error(), "peering.max_peers") {
							t.Fatalf("active registry above cap accepted: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if cfg.Peering.MaxPeers != n || len(cfg.Peering.Peers) != n+extra+1 {
						t.Fatalf("cap changed or registry silently subset: cap=%d rows=%d", cfg.Peering.MaxPeers, len(cfg.Peering.Peers))
					}
				})
			}
		}
	}
}

func TestActivePeeringMaxPeersBoundaries(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 2, 8, 63, 64, 65} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			cfg := PeeringConfig{MaxPeers: n, LocalCallsign: "N0CALL", NodeVersion: "5457", LegacyVersion: "5401", PC92Bitmap: 5}
			if n < 1 || n > 64 {
				if _, _, err := NormalizeActivePeeringWireContract(cfg, "N0CALL"); err == nil || !strings.Contains(err.Error(), "peering.max_peers") {
					t.Fatalf("invalid constructor cap accepted: %v", err)
				}
				if err := validatePeeringWireContract(&cfg); err == nil {
					t.Fatal("disabled config bypassed cap validation")
				}
				return
			}
			for i := 0; i < n; i++ {
				cfg.Peers = append(cfg.Peers, PeeringPeer{Enabled: true, Direction: PeeringPeerDirectionBoth, RemoteCallsign: fmt.Sprintf("K%dPEER", i)})
			}
			cfg.Peers = append(cfg.Peers, PeeringPeer{})
			normalized, _, err := NormalizeActivePeeringWireContract(cfg, "N0CALL")
			if err != nil {
				t.Fatal(err)
			}
			if normalized.MaxPeers != n || len(normalized.Peers) != n || cap(normalized.Peers) != n {
				t.Fatalf("constructor retained incorrect active population: cap=%d rows=%d backing=%d", normalized.MaxPeers, len(normalized.Peers), cap(normalized.Peers))
			}
			cfg.Peers = append(cfg.Peers, PeeringPeer{Enabled: true, RemoteCallsign: "K999PEER"})
			if _, _, err := NormalizeActivePeeringWireContract(cfg, "N0CALL"); err == nil || !strings.Contains(err.Error(), "peering.max_peers") {
				t.Fatalf("disabled bit bypassed constructor population cap: %v", err)
			}
		})
	}
}
