package filter

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestUpdateRecentIPs(t *testing.T) {
	recent := []string{"1.1.1.1", "2.2.2.2"}
	recent = UpdateRecentIPs(recent, "3.3.3.3")
	want := []string{"3.3.3.3", "1.1.1.1", "2.2.2.2"}
	if !reflect.DeepEqual(recent, want) {
		t.Fatalf("expected %v, got %v", want, recent)
	}

	recent = UpdateRecentIPs(recent, "1.1.1.1")
	want = []string{"1.1.1.1", "3.3.3.3", "2.2.2.2"}
	if !reflect.DeepEqual(recent, want) {
		t.Fatalf("expected %v, got %v", want, recent)
	}

	recent = UpdateRecentIPs(recent, "4.4.4.4")
	recent = UpdateRecentIPs(recent, "5.5.5.5")
	recent = UpdateRecentIPs(recent, "6.6.6.6")
	want = []string{"6.6.6.6", "5.5.5.5", "4.4.4.4", "1.1.1.1", "3.3.3.3"}
	if !reflect.DeepEqual(recent, want) {
		t.Fatalf("expected %v, got %v", want, recent)
	}
}

func TestMergeRecentIPs(t *testing.T) {
	primary := []string{"198.51.100.1", "203.0.113.5"}
	fallback := []string{"203.0.113.5", "192.0.2.9", "198.51.100.1", "203.0.113.6"}
	merged := MergeRecentIPs(primary, fallback)
	want := []string{"198.51.100.1", "203.0.113.5", "192.0.2.9", "203.0.113.6"}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("expected %v, got %v", want, merged)
	}
}

func TestTouchUserRecordIP(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	record, created, err := TouchUserRecordIP("k3to", "203.0.113.9")
	if err != nil {
		t.Fatalf("TouchUserRecordIP failed: %v", err)
	}
	if !created {
		t.Fatalf("expected new record to be created")
	}
	if record.DedupePolicy != DedupePolicyMed {
		t.Fatalf("expected default dedupe policy MED, got %q", record.DedupePolicy)
	}
	if len(record.RecentIPs) != 1 || record.RecentIPs[0] != "203.0.113.9" {
		t.Fatalf("unexpected recent IPs: %v", record.RecentIPs)
	}

	record, created, err = TouchUserRecordIP("k3to", "198.51.100.10")
	if err != nil {
		t.Fatalf("TouchUserRecordIP failed: %v", err)
	}
	if created {
		t.Fatalf("expected existing record to be reused")
	}
	want := []string{"198.51.100.10", "203.0.113.9"}
	if !reflect.DeepEqual(record.RecentIPs, want) {
		t.Fatalf("expected %v, got %v", want, record.RecentIPs)
	}
}

func TestTouchUserRecordLogin(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	firstLogin := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record, created, prevLogin, prevIP, err := TouchUserRecordLogin("k3to", "203.0.113.9", firstLogin)
	if err != nil {
		t.Fatalf("TouchUserRecordLogin failed: %v", err)
	}
	if !created {
		t.Fatalf("expected new record to be created")
	}
	if record.DedupePolicy != DedupePolicyMed {
		t.Fatalf("expected default dedupe policy MED, got %q", record.DedupePolicy)
	}
	if !record.LastLoginUTC.Equal(firstLogin) {
		t.Fatalf("expected last login %v, got %v", firstLogin, record.LastLoginUTC)
	}
	if !prevLogin.IsZero() {
		t.Fatalf("expected zero previous login, got %v", prevLogin)
	}
	if prevIP != "" {
		t.Fatalf("expected empty previous IP, got %q", prevIP)
	}

	secondLogin := firstLogin.Add(2 * time.Hour)
	record, created, prevLogin, prevIP, err = TouchUserRecordLogin("k3to", "198.51.100.10", secondLogin)
	if err != nil {
		t.Fatalf("TouchUserRecordLogin failed: %v", err)
	}
	if created {
		t.Fatalf("expected existing record to be reused")
	}
	if !record.LastLoginUTC.Equal(secondLogin) {
		t.Fatalf("expected last login %v, got %v", secondLogin, record.LastLoginUTC)
	}
	if !prevLogin.Equal(firstLogin) {
		t.Fatalf("expected previous login %v, got %v", firstLogin, prevLogin)
	}
	if prevIP != "203.0.113.9" {
		t.Fatalf("expected previous IP %q, got %q", "203.0.113.9", prevIP)
	}
	want := []string{"198.51.100.10", "203.0.113.9"}
	if !reflect.DeepEqual(record.RecentIPs, want) {
		t.Fatalf("expected %v, got %v", want, record.RecentIPs)
	}
}

func TestTouchUserRecordLoginUsesConfiguredDefaultDedupe(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	login := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record, created, _, _, err := TouchUserRecordLoginWithDefaultDedupe("k3to", "203.0.113.9", login, DedupePolicySlow)
	if err != nil {
		t.Fatalf("TouchUserRecordLoginWithDefaultDedupe failed: %v", err)
	}
	if !created {
		t.Fatalf("expected new record to be created")
	}
	if record.DedupePolicy != DedupePolicySlow {
		t.Fatalf("expected default dedupe policy SLOW, got %q", record.DedupePolicy)
	}

	if err := os.WriteFile(filepath.Join(tmp, "SAVED.yaml"), []byte("dialect: go\ndedupe_policy: MED\n"), 0o644); err != nil {
		t.Fatalf("write saved-policy record: %v", err)
	}
	saved, created, _, _, err := TouchUserRecordLoginWithDefaultDedupe("SAVED", "198.51.100.10", login.Add(time.Hour), DedupePolicySlow)
	if err != nil {
		t.Fatalf("TouchUserRecordLoginWithDefaultDedupe saved policy failed: %v", err)
	}
	if created {
		t.Fatalf("expected saved record to be reused")
	}
	if saved.DedupePolicy != DedupePolicyMed {
		t.Fatalf("expected saved MED policy to remain authoritative, got %q", saved.DedupePolicy)
	}
}

func TestUserRecordRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	f := NewFilter()
	f.SetMode("CW", true)
	f.SetEvent("POTA", true)
	f.NearbyEnabled = true
	record := &UserRecord{
		Filter:                  *f,
		RecentIPs:               []string{"203.0.113.10", "198.51.100.42"},
		DedupePolicy:            DedupePolicySlow,
		Grid:                    "FN31",
		NoiseClass:              "URBAN",
		PathMinObservationCount: 42,
	}
	if err := SaveUserRecord("k3to", record); err != nil {
		t.Fatalf("SaveUserRecord failed: %v", err)
	}

	loaded, err := LoadUserRecord("k3to")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !loaded.Modes["CW"] {
		t.Fatalf("expected CW mode to remain enabled after reload")
	}
	if !loaded.Events["POTA"] || loaded.AllEvents {
		t.Fatalf("expected POTA event filter to remain enabled after reload: events=%+v all=%v", loaded.Events, loaded.AllEvents)
	}
	if !reflect.DeepEqual(loaded.RecentIPs, record.RecentIPs) {
		t.Fatalf("expected recent IPs %v, got %v", record.RecentIPs, loaded.RecentIPs)
	}
	if loaded.Grid != "FN31" {
		t.Fatalf("expected grid FN31, got %s", loaded.Grid)
	}
	if loaded.NoiseClass != "URBAN" {
		t.Fatalf("expected noise class URBAN, got %s", loaded.NoiseClass)
	}
	if loaded.PathMinObservationCount != 42 {
		t.Fatalf("expected path min observation count 42, got %d", loaded.PathMinObservationCount)
	}
	if loaded.DedupePolicy != DedupePolicySlow {
		t.Fatalf("expected dedupe policy SLOW, got %s", loaded.DedupePolicy)
	}
	if !loaded.NearbyEnabled {
		t.Fatalf("expected NEARBY to remain enabled after reload")
	}
}

func TestUserRecordNormalizesEventFilters(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	if err := os.WriteFile(filepath.Join(tmp, "K3TO.yaml"), []byte(`
events:
  pota: true
  bogus: true
  sota: false
blockevents:
  wwff: true
  bad: true
allevents: false
dialect: go
dedupe_policy: MED
`), 0o644); err != nil {
		t.Fatalf("write user record: %v", err)
	}

	loaded, err := LoadUserRecord("K3TO")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !loaded.Events["POTA"] || len(loaded.Events) != 1 {
		t.Fatalf("expected only POTA event allowlist after load, got %+v", loaded.Events)
	}
	if !loaded.BlockEvents["WWFF"] || len(loaded.BlockEvents) != 1 {
		t.Fatalf("expected only WWFF event blocklist after load, got %+v", loaded.BlockEvents)
	}
	if loaded.AllEvents {
		t.Fatalf("expected explicit event allowlist to keep AllEvents=false")
	}
}

func TestUserRecordPreservesEventBlockAll(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	if err := os.WriteFile(filepath.Join(tmp, "N0CALL.yaml"), []byte(`
blockallevents: true
allevents: false
dialect: go
dedupe_policy: MED
`), 0o644); err != nil {
		t.Fatalf("write user record: %v", err)
	}

	loaded, err := LoadUserRecord("N0CALL")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !loaded.BlockAllEvents || loaded.AllEvents {
		t.Fatalf("expected EVENT block-all to preserve AllEvents=false, got all=%v blockAll=%v", loaded.AllEvents, loaded.BlockAllEvents)
	}
}

func TestLoadUserRecordLegacyFilter(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	f := NewFilter()
	f.SetBand("20m", true)
	raw, err := yaml.Marshal(f)
	if err != nil {
		t.Fatalf("yaml marshal failed: %v", err)
	}
	path := filepath.Join(UserDataDir, "LEGACY.yaml")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write legacy filter failed: %v", err)
	}

	record, err := LoadUserRecord("LEGACY")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !record.Bands["20m"] {
		t.Fatalf("expected legacy band filter to load into user record")
	}
	if record.DedupePolicy != DedupePolicyMed {
		t.Fatalf("expected legacy record to default dedupe policy MED, got %q", record.DedupePolicy)
	}
}

func TestLoadUserRecordMigratesLegacyExplicitModesToIncludeUnknown(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	raw := []byte("modes:\n  CW: true\n  USB: true\nblockmodes: {}\nallmodes: false\n")
	path := filepath.Join(UserDataDir, "LEGACY_UNKNOWN.yaml")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write legacy filter failed: %v", err)
	}

	record, err := LoadUserRecord("LEGACY_UNKNOWN")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !record.Modes["CW"] || !record.Modes["USB"] {
		t.Fatalf("expected existing explicit modes to remain enabled: %+v", record.Modes)
	}
	if !record.Modes[UnknownModeToken] {
		t.Fatalf("expected UNKNOWN to be added to legacy explicit allowlist")
	}
}

func TestLoadUserRecordKeepsExistingUnknownModeEntry(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	raw := []byte("modes:\n  CW: true\n  UNKNOWN: true\nblockmodes: {}\nallmodes: false\n")
	path := filepath.Join(UserDataDir, "LEGACY_HAS_UNKNOWN.yaml")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write legacy filter failed: %v", err)
	}

	record, err := LoadUserRecord("LEGACY_HAS_UNKNOWN")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !record.Modes[UnknownModeToken] {
		t.Fatalf("expected UNKNOWN to remain enabled")
	}
	if got := len(record.Modes); got != 2 {
		t.Fatalf("expected no extra mode mutations, got %d entries: %+v", got, record.Modes)
	}
}

func TestLoadUserRecordPreservesExplicitUnknownBlock(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	raw := []byte("modes:\n  CW: true\nblockmodes:\n  UNKNOWN: true\nallmodes: false\n")
	path := filepath.Join(UserDataDir, "LEGACY_BLOCK_UNKNOWN.yaml")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write legacy filter failed: %v", err)
	}

	record, err := LoadUserRecord("LEGACY_BLOCK_UNKNOWN")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if record.Modes[UnknownModeToken] {
		t.Fatalf("expected UNKNOWN not to be auto-added when explicitly blocked")
	}
	if !record.BlockModes[UnknownModeToken] {
		t.Fatalf("expected explicit UNKNOWN block to remain")
	}
}

func TestLoadUserRecordLeavesAllModesRecordsPermissive(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	raw := []byte("modes: {}\nblockmodes: {}\nallmodes: true\n")
	path := filepath.Join(UserDataDir, "LEGACY_ALL_MODES.yaml")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write legacy filter failed: %v", err)
	}

	record, err := LoadUserRecord("LEGACY_ALL_MODES")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !record.AllModes {
		t.Fatalf("expected all-modes record to remain permissive")
	}
	if len(record.Modes) != 0 {
		t.Fatalf("expected all-modes record not to gain explicit UNKNOWN entry: %+v", record.Modes)
	}
}

func TestSaveUserFilterPreservesRecentIPs(t *testing.T) {
	tmp := t.TempDir()
	orig := UserDataDir
	UserDataDir = tmp
	t.Cleanup(func() { UserDataDir = orig })

	base := NewFilter()
	base.SetMode("CW", true)
	record := &UserRecord{
		Filter:       *base,
		RecentIPs:    []string{"192.0.2.1"},
		DedupePolicy: DedupePolicySlow,
	}
	if err := SaveUserRecord("N0CALL", record); err != nil {
		t.Fatalf("SaveUserRecord failed: %v", err)
	}

	updated := NewFilter()
	updated.SetMode("USB", true)
	if err := SaveUserFilter("N0CALL", updated); err != nil {
		t.Fatalf("SaveUserFilter failed: %v", err)
	}

	loaded, err := LoadUserRecord("N0CALL")
	if err != nil {
		t.Fatalf("LoadUserRecord failed: %v", err)
	}
	if !reflect.DeepEqual(loaded.RecentIPs, record.RecentIPs) {
		t.Fatalf("expected recent IPs %v, got %v", record.RecentIPs, loaded.RecentIPs)
	}
	if !loaded.Modes["USB"] {
		t.Fatalf("expected updated filter settings to persist")
	}
	if loaded.DedupePolicy != DedupePolicySlow {
		t.Fatalf("expected dedupe policy to persist, got %q", loaded.DedupePolicy)
	}
}
