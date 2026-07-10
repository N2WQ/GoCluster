package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestCheckedInPublicConfigLoads(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "data", "config"))
	if err != nil {
		t.Fatalf("Load(../data/config) error: %v", err)
	}

	if cfg.Server.NodeID != "N0CALL-1" {
		t.Fatalf("server.node_id = %q, want public example callsign", cfg.Server.NodeID)
	}
	if cfg.RBN.Callsign != "N0CALL-1" || cfg.RBNDigital.Callsign != "N0CALL-1" {
		t.Fatalf("RBN callsigns = %q/%q, want public example callsign", cfg.RBN.Callsign, cfg.RBNDigital.Callsign)
	}
	if len(cfg.HumanTelnet) != 2 {
		t.Fatalf("human_telnet entries = %d, want 2 public examples", len(cfg.HumanTelnet))
	}
	for i, human := range cfg.HumanTelnet {
		if human.Enabled {
			t.Fatalf("human_telnet[%d] is enabled in public config", i)
		}
		wantHost := fmt.Sprintf("upstream%d.example.invalid", i+1)
		wantCallsign := fmt.Sprintf("N0CALL-%d", i+2)
		if human.Host != wantHost || human.Callsign != wantCallsign {
			t.Fatalf("human_telnet[%d] = %q/%q, want %q/%q", i, human.Host, human.Callsign, wantHost, wantCallsign)
		}
	}
	if cfg.Peering.LocalCallsign != "N0CALL-1" {
		t.Fatalf("peering.local_callsign = %q, want public example callsign", cfg.Peering.LocalCallsign)
	}
	if cfg.Peering.Enabled {
		t.Fatalf("peering.enabled = true, want public config disabled by default")
	}
	if cfg.Reputation.IPInfoDownloadEnabled {
		t.Fatalf("reputation.ipinfo_download_enabled = true, want public config download disabled by default")
	}
	if cfg.Reputation.IPInfoDownloadToken != "REPLACE_WITH_IPINFO_TOKEN" {
		t.Fatalf("reputation.ipinfo_download_token is not the public placeholder")
	}
	if cfg.Reputation.IPInfoAPIEnabled {
		t.Fatalf("reputation.ipinfo_api_enabled = true, want public config API disabled by default")
	}
}

func TestCheckedInPublicConfigPeerExamples(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "data", "config"))
	if err != nil {
		t.Fatalf("Load(../data/config) error: %v", err)
	}
	if len(cfg.Peering.Peers) == 0 {
		t.Fatalf("expected public example peers")
	}

	hostPattern := regexp.MustCompile(`^peer\d+\.example\.invalid$`)
	remotePattern := regexp.MustCompile(`^N0PEER-\d+$`)
	for i, peer := range cfg.Peering.Peers {
		if peer.Enabled {
			t.Fatalf("peer %d is enabled in public config", i)
		}
		if !hostPattern.MatchString(peer.Host) {
			t.Fatalf("peer %d host is not a public example hostname", i)
		}
		if peer.Password != "" {
			t.Fatalf("peer %d password is non-empty in public config", i)
		}
		if peer.LoginCallsign != "N0CALL-1" {
			t.Fatalf("peer %d login_callsign = %q, want public example callsign", i, peer.LoginCallsign)
		}
		if !remotePattern.MatchString(peer.RemoteCallsign) {
			t.Fatalf("peer %d remote_callsign = %q, want public peer example", i, peer.RemoteCallsign)
		}
	}
}

func TestCheckedInPublicConfigTextBackstops(t *testing.T) {
	app := readCheckedInConfigFile(t, "app.yaml")
	ingest := readCheckedInConfigFile(t, "ingest.yaml")
	peering := readCheckedInConfigFile(t, "peering.yaml")
	reputation := readCheckedInConfigFile(t, "reputation.yaml")

	assertConfigLinesMatch(t, app, `^\s*node_id:\s*`, `^\s*node_id:\s*"N0CALL-\d+"\s*(#.*)?$`, "app.yaml node_id")
	assertConfigLinesMatch(t, ingest, `^\s*callsign:\s*`, `^\s*callsign:\s*"N0CALL-\d+"\s*(#.*)?$`, "ingest.yaml callsign")
	assertConfigLinesMatch(t, ingest, `^\s*host:\s*`, `^\s*host:\s*"(telnet\.reversebeacon\.net|upstream\d+\.example\.invalid)"\s*(#.*)?$`, "ingest.yaml host")
	assertConfigLinesMatch(t, peering, `^\s*enabled:\s*`, `^\s*enabled:\s*false\s*(#.*)?$`, "peering.yaml enabled")
	assertConfigLinesMatch(t, peering, `^\s*host:\s*`, `^\s*host:\s*"peer\d+\.example\.invalid"\s*(#.*)?$`, "peering.yaml host")
	assertConfigLinesMatch(t, peering, `^\s*password:\s*`, `^\s*password:\s*""\s*(#.*)?$`, "peering.yaml password")
	assertConfigLinesMatch(t, peering, `^\s*login_callsign:\s*`, `^\s*login_callsign:\s*"N0CALL-\d+"\s*(#.*)?$`, "peering.yaml login_callsign")
	assertConfigLinesMatch(t, peering, `^\s*remote_callsign:\s*`, `^\s*remote_callsign:\s*"N0PEER-\d+"\s*(#.*)?$`, "peering.yaml remote_callsign")
	assertConfigLinesMatch(t, reputation, `^\s*ipinfo_download_enabled:\s*`, `^\s*ipinfo_download_enabled:\s*false\s*(#.*)?$`, "reputation.yaml ipinfo_download_enabled")
	assertConfigLinesMatch(t, reputation, `^\s*ipinfo_download_token:\s*`, `^\s*ipinfo_download_token:\s*"REPLACE_WITH_IPINFO_TOKEN"\s*(#.*)?$`, "reputation.yaml ipinfo_download_token")
	assertConfigLinesMatch(t, reputation, `^\s*ipinfo_api_enabled:\s*`, `^\s*ipinfo_api_enabled:\s*false\s*(#.*)?$`, "reputation.yaml ipinfo_api_enabled")
	assertConfigLinesMatch(t, reputation, `^\s*ipinfo_api_token:\s*`, `^\s*ipinfo_api_token:\s*""\s*(#.*)?$`, "reputation.yaml ipinfo_api_token")
}

func readCheckedInConfigFile(t *testing.T, name string) string {
	t.Helper()

	b, err := os.ReadFile(filepath.Join("..", "data", "config", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

func assertConfigLinesMatch(t *testing.T, text, linePattern, allowedPattern, description string) {
	t.Helper()

	lineRE := regexp.MustCompile(linePattern)
	allowedRE := regexp.MustCompile(allowedPattern)
	matched := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if !lineRE.MatchString(line) {
			continue
		}
		matched = true
		if !allowedRE.MatchString(strings.TrimRight(line, "\r")) {
			t.Fatalf("%s contains a non-public release value", description)
		}
	}
	if !matched {
		t.Fatalf("%s was not found", description)
	}
}
