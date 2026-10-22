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

func TestV5StorageAuthoritativeLoad(t *testing.T) {
	usePresetTestDir(t)
	record := &V5UserRecord{
		Configuration: v5StorageTestConfiguration(), RecentIPs: []string{"192.0.2.1"},
		LastLoginUTC: time.Date(2026, 10, 21, 12, 0, 0, 123, time.UTC),
		Preset:       &V5PresetReference{Name: "BASE", Baseline: &V5SavedPreset{Configuration: v5StorageTestConfiguration()}},
	}
	payload, err := PrepareV5UserRecord(" n2wq-1 ", record)
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Commit(); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadV5UserRecord(" n2wq-1 ")
	if err != nil || loaded.ConfigurationVersion != 5 || loaded.Preset.Baseline.ConfigurationVersion != 5 ||
		!loaded.LastLoginUTC.Equal(record.LastLoginUTC) || len(loaded.RecentIPs) != 1 ||
		!loaded.Configuration.Equal(loaded.Preset.Baseline.Configuration) {
		t.Fatalf("authoritative load=%+v, %v", loaded, err)
	}
	canonical, _, err := PrepareV5Configuration(record.Configuration)
	if err != nil || !loaded.Configuration.Equal(canonical) {
		t.Fatal("load lost exact settings, controls or canonical rule definitions")
	}
	if _, err := LoadV5UserRecord("N2WQ-2"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SSID destination aliased or missing cause lost: %v", err)
	}
	for _, call := range []string{"", "..", `..\N2WQ`, `N2WQ\..\BAD`, "N2WQ:1", "N2WQ\nBAD"} {
		if _, err := LoadV5UserRecord(call); err == nil {
			t.Fatalf("invalid read callsign %q accepted", call)
		}
		if _, err := PrepareV5UserRecord(call, record); err == nil {
			t.Fatalf("invalid write callsign %q accepted", call)
		}
	}
}

func TestV5StorageLoadRefusalPreservesDestination(t *testing.T) {
	usePresetTestDir(t)
	path := filepath.Join(UserDataDir, "N2WQ-1.yaml")
	for _, raw := range []string{
		"all_bands: true\n",
		"configuration_version: 4\nall_bands: true\n",
		"configuration_version: 6\nconfiguration: {}\n",
		"configuration_version: 5\nconfiguration: [\n",
		strings.Repeat("x", MaxUserRecordBytes+1),
	} {
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if record, err := LoadV5UserRecord("N2WQ-1"); err == nil || record != nil {
			t.Fatalf("refused destination accepted: %v", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, []byte(raw)) {
			t.Fatal("read changed refused destination bytes")
		}
	}
}
