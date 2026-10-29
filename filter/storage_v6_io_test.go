package filter

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestV6StorageAuthoritativeLoad(t *testing.T) {
	usePresetTestDir(t)
	record := &V6UserRecord{
		Configuration: v6StorageTestConfiguration(), RecentIPs: []string{"192.0.2.1"},
		LastLoginUTC: time.Date(2026, 10, 21, 12, 0, 0, 123, time.UTC),
		Preset:       &V6PresetReference{Name: "BASE", Baseline: &V6SavedPreset{Configuration: v6StorageTestConfiguration()}},
	}
	payload, err := PrepareV6UserRecord(" n2wq-1 ", record)
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Commit(); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadV6UserRecord(" n2wq-1 ")
	if err != nil || loaded.ConfigurationVersion != 6 || loaded.Preset.Baseline.ConfigurationVersion != 6 ||
		!loaded.LastLoginUTC.Equal(record.LastLoginUTC) || len(loaded.RecentIPs) != 1 ||
		!loaded.Configuration.Equal(loaded.Preset.Baseline.Configuration) {
		t.Fatalf("authoritative load=%+v, %v", loaded, err)
	}
	canonical, _, err := PrepareV6Configuration(record.Configuration)
	if err != nil || !loaded.Configuration.Equal(canonical) {
		t.Fatal("load lost exact settings, controls or canonical rule definitions")
	}
	if _, err := LoadV6UserRecord("N2WQ-2"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SSID destination aliased or missing cause lost: %v", err)
	}
	for _, call := range []string{"", "..", `..\N2WQ`, `N2WQ\..\BAD`, "N2WQ:1", "N2WQ\nBAD"} {
		if _, err := LoadV6UserRecord(call); err == nil {
			t.Fatalf("invalid read callsign %q accepted", call)
		}
		if _, err := PrepareV6UserRecord(call, record); err == nil {
			t.Fatalf("invalid write callsign %q accepted", call)
		}
	}
}

func TestV6StorageLoadRefusalPreservesDestination(t *testing.T) {
	usePresetTestDir(t)
	path := filepath.Join(UserDataDir, "N2WQ-1.yaml")
	for _, raw := range []string{
		"all_bands: true\n",
		"configuration_version: 4\nall_bands: true\n",
		"configuration_version: 7\nconfiguration: {}\n",
		"configuration_version: 6\nconfiguration: [\n",
		strings.Repeat("x", MaxUserRecordBytes+1),
	} {
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if record, err := LoadV6UserRecord("N2WQ-1"); err == nil || record != nil {
			t.Fatalf("refused destination accepted: %v", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, []byte(raw)) {
			t.Fatal("read changed refused destination bytes")
		}
	}
}
