package main

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/uls"
)

func TestReplayFrozenLicenseInputsSurviveOriginalReplacement(t *testing.T) {
	original := replayLicenseDB(t, "K1ABD")
	allowlist := filepath.Join(t.TempDir(), "exceptions.txt")
	if err := os.WriteFile(allowlist, []byte("K1ABC\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantDBHash, _ := sha256File(original)
	wantAllowHash, _ := sha256File(allowlist)
	r := newReplayRunner(replayRequest{})
	r.outDir = t.TempDir()
	r.cfg = &config.Config{FCCULS: config.FCCULSConfig{Enabled: true, DBPath: original, AllowlistPath: allowlist, CacheTTLSeconds: 120}}
	t.Cleanup(r.close)
	if err := r.configureExternalDependencies(); err != nil {
		t.Fatal(err)
	}
	ownedDir := r.licenseInputsDir
	if got := r.licenseInputs.FCC; got.SHA256 != wantDBHash || got.Availability != "available" || !got.EnforcementEnabled || got.Bytes <= 0 || got.SourcePath != original {
		t.Fatalf("FCC provenance=%+v", got)
	}
	if got := r.licenseInputs.Allowlist; got.SHA256 != wantAllowHash || got.Availability != "available" {
		t.Fatalf("allowlist provenance=%+v", got)
	}
	// Windows deletion proves the consumer opened the frozen handle instead of
	// the configured source. Replacement bytes cannot change this run's facts.
	if err := os.Remove(original); err != nil {
		t.Fatalf("source still owned by consumer: %v", err)
	}
	replacement := replayLicenseDB(t, "K1ABC")
	data, err := os.ReadFile(replacement)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(original, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(allowlist, []byte("K1ABE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		call  string
		found bool
	}{{"K1ABD", true}, {"K1ABC", false}} {
		if got := uls.LookupUS(tc.call); !got.Available || got.Found != tc.found {
			t.Fatalf("original replacement changed %s: %+v", tc.call, got)
		}
	}
	if !uls.AllowlistMatch(291, "K1ABC") || uls.AllowlistMatch(291, "K1ABE") {
		t.Fatal("original allowlist edit changed frozen policy")
	}
	if err := r.closeWithError(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ownedDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("frozen directory survived close: %v", err)
	}
	if got := uls.LookupUS("K1ABD"); got.Available || uls.AllowlistMatchAny("K1ABC") || uls.LicenseChecksEnabled() || uls.LookupStats().Entries != 0 {
		t.Fatalf("run authority survived close: %+v", got)
	}
	// The next run observes the new bytes, with no previous handle, exception,
	// enforcement flag, or positive/negative cached fact carried forward.
	next := newReplayRunner(replayRequest{})
	next.outDir = t.TempDir()
	next.cfg = &config.Config{FCCULS: config.FCCULSConfig{DBPath: original, AllowlistPath: allowlist}}
	t.Cleanup(next.close)
	if err := next.configureExternalDependencies(); err != nil {
		t.Fatal(err)
	}
	if got := uls.LookupUS("K1ABC"); !got.Found || uls.LicenseChecksEnabled() || !uls.AllowlistMatch(291, "K1ABE") || uls.AllowlistMatch(291, "K1ABC") {
		t.Fatalf("consecutive run inherited old authority: %+v", got)
	}
	if next.licenseInputs.FCC.EnforcementEnabled || next.licenseInputs.FCC.Availability != "available" || next.licenseInputs.FCC.SHA256 == wantDBHash {
		t.Fatalf("disabled facts/provenance conflated: %+v", next.licenseInputs.FCC)
	}
}

func TestReplayFrozenLicenseInputsPartialInitializationCleanup(t *testing.T) {
	for _, failure := range []string{"missing_ised", "missing_allowlist", "missing_cty"} {
		t.Run(failure, func(t *testing.T) {
			r := newReplayRunner(replayRequest{})
			r.outDir = t.TempDir()
			r.cfg = &config.Config{FCCULS: config.FCCULSConfig{Enabled: true, DBPath: replayLicenseDB(t, "K1ABC")}}
			switch failure {
			case "missing_ised":
				r.cfg.ISED = config.ISEDConfig{Enabled: true, DBPath: filepath.Join(t.TempDir(), "missing.db")}
			case "missing_allowlist":
				r.cfg.FCCULS.AllowlistPath = filepath.Join(t.TempDir(), "missing.txt")
			case "missing_cty":
				r.cfg.CTY.Enabled = true
				r.cfg.CTY.File = filepath.Join(t.TempDir(), "missing.plist")
			}
			if err := r.configureExternalDependencies(); err == nil {
				t.Fatal("setup unexpectedly succeeded")
			}
			dir := r.licenseInputsDir
			if dir == "" {
				t.Fatal("partial freeze did not establish owner")
			}
			if failure == "missing_cty" && !uls.LookupUS("K1ABC").Available {
				t.Fatal("fixture did not open handles before CTY failure")
			}
			if err := r.closeWithError(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial owner retained: %v", err)
			}
			if uls.LookupUS("K1ABC").Available || uls.LookupCanadian("VE3ABC").Available || uls.LicenseChecksEnabled() || uls.CanadianLicenseChecksEnabled() {
				t.Fatal("partial setup authority leaked")
			}
			if err := os.Remove(r.cfg.FCCULS.DBPath); err != nil {
				t.Fatalf("source handle leaked: %v", err)
			}
		})
	}
}

func TestReplayFrozenLicenseReadinessIsNotConfiguredPath(t *testing.T) {
	badDB := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(badDB, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newReplayRunner(replayRequest{})
	r.outDir = t.TempDir()
	r.cfg = &config.Config{FCCULS: config.FCCULSConfig{Enabled: true, DBPath: badDB}, ISED: config.ISEDConfig{DBPath: filepath.Join(t.TempDir(), "missing.db")}}
	t.Cleanup(r.close)
	if err := r.configureExternalDependencies(); err != nil {
		t.Fatal(err)
	}
	if r.licenseInputs.FCC.Availability != "unavailable" || r.licenseInputs.FCC.SHA256 == "" || !r.licenseInputs.FCC.EnforcementEnabled || r.licenseInputs.ISED.Availability != "unavailable" || r.licenseInputs.ISED.SHA256 != "" || r.licenseInputs.Allowlist.Availability != "not_configured" {
		t.Fatalf("readiness distinctions=%+v", r.licenseInputs)
	}
	if got := uls.LookupUS("K1ABC"); got.Available {
		t.Fatalf("corrupt snapshot invented absence: %+v", got)
	}
}

func TestReplayFrozenMalformedAllowlistPreservesWarningPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed.txt")
	if err := os.WriteFile(path, []byte("K1ABC\n[\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantHash, err := sha256File(path)
	if err != nil {
		t.Fatal(err)
	}
	r := newReplayRunner(replayRequest{})
	r.outDir = t.TempDir()
	r.cfg = &config.Config{FCCULS: config.FCCULSConfig{Enabled: true, DBPath: replayLicenseDB(t), AllowlistPath: path}}
	t.Cleanup(r.close)
	if err = r.configureExternalDependencies(); err != nil {
		t.Fatalf("malformed allowlist changed warning into setup failure: %v", err)
	}
	if r.licenseInputs.Allowlist.SHA256 != wantHash || r.licenseInputs.Allowlist.Availability != "unavailable" || uls.AllowlistMatchAny("K1ABC") {
		t.Fatalf("malformed allowlist claimed loaded exceptions: %+v", r.licenseInputs.Allowlist)
	}
	if got := uls.LookupUS("K1ABC"); !uls.ShouldRejectDXLicense(291, "K1ABC", false, got) {
		t.Fatalf("partial malformed input supplied exception: %+v", got)
	}
}

func TestReplayLicenseProvenanceFlowsToManifestAndHistory(t *testing.T) {
	at := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	r := newReplayRunner(replayRequest{})
	r.outDir = t.TempDir()
	r.archiveDir = t.TempDir()
	r.dayStart, r.dayCompact = at, "20261010"
	r.cfg = &config.Config{FCCULS: config.FCCULSConfig{Enabled: true, DBPath: replayLicenseDB(t, "K1ABC")}}
	t.Cleanup(r.close)
	if err := r.configureExternalDependencies(); err != nil {
		t.Fatal(err)
	}
	r.zipPath = filepath.Join(t.TempDir(), "history.zip")
	f, err := os.Create(r.zipPath)
	if err != nil {
		t.Fatal(err)
	}
	z := zip.NewWriter(f)
	w, err := z.Create("20261010.csv")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("callsign,freq,band,dx,db,date,mode,tx_mode\n")); err != nil {
		t.Fatal(err)
	}
	if err = z.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err = r.openReplayOutputs(); err != nil {
		t.Fatal(err)
	}
	if r.manifest.LicenseInputs != r.licenseInputs {
		t.Fatal("output initialization omitted license provenance")
	}
	r.manifest.FinishedAtUTC = at.Add(time.Hour).Format(time.RFC3339Nano)
	if err = writeReplayRunHistory(r.archiveDir, "", r.cfg, r.manifest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(r.archiveDir, "rbn_replay_history", "runs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var record replayRunRecord
	if err = json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.LicenseInputs != r.licenseInputs || !strings.Contains(string(data), "historical_utc_at_each_check_including_release") || !strings.Contains(string(data), "process_time") {
		t.Fatalf("history omitted clocks/authority: %s", data)
	}
}
