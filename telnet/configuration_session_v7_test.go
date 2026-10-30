package telnet

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

const v7LoginOldLiteral = "configuration_version: 6\nconfiguration:\n  filters:\n    rules: [{slot: 0, action: pass, expr: 'DISCARDED INVALID OLD EXPRESSION'}]\n    include_beacons: false\n    allow_self: false\n    allow_toxic: false\n  settings: {dialect: cc, grid: FN31, noise_class: URBAN, dedupe_policy: SLOW, path_min_observation_count: 42, solar_summary_minutes: 30}\nrecent_ips: [192.0.2.1]\nlast_login_utc: 2026-10-21T12:00:00Z\n"

func writeV7LoginOldProfile(t *testing.T, c *Client) string {
	t.Helper()
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	if err := os.WriteFile(path, []byte(v7LoginOldLiteral), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeV7LoginOldLibrary(t *testing.T, c *Client) string {
	t.Helper()
	path := presetLibraryPathFor(c.callsign)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	raw := "presets:\n  OLD:\n    configuration_version: 6\n    configuration:\n      filters: {rules: [{slot: 0, action: pass, expr: 'OLD PRESET DISCARDED'}]}\n      settings: {dialect: cc}\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestV7LoginResetPreservesSharedSettingsAndPendingNotice(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	path := writeV7LoginOldProfile(t, c)
	before := []byte(v7LoginOldLiteral)
	now := time.Unix(1800000000, 0).UTC()
	result := s.restoreV7ClientRecordOwned(c, now)
	if result.loadError != nil || !result.profileReset || c.recordProtected || c.dialect != DialectCC || c.v7Notice.ProfileResetGeneration != 7 || c.v7Notice.ProfileAcknowledgedGeneration != 0 {
		t.Fatalf("reset/notice outcome: %+v", result)
	}
	if c.configuredSettings.Grid != "FN31" || c.configuredSettings.NoiseClass != "URBAN" || c.configuredSettings.DedupePolicy != "SLOW" || c.configuredSettings.PathMinObservationCount != 42 || c.configuredSettings.SolarSummaryMinutes != 30 {
		t.Fatal("reset changed unrelated configured preferences")
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	if record.Configuration.Filters.GO.AllowSelf != filter.DefaultBoolDefault || record.Configuration.Filters.CC.AllowSelf != filter.DefaultBoolDefault || record.Presets.GO != nil || record.Presets.CC != nil || !record.LastLoginUTC.Equal(now) {
		t.Fatal("reset did not install both defaults/metadata")
	}
	backup, err := os.ReadFile(path + ".pre-v7.bak")
	if err != nil || !bytes.Equal(backup, before) {
		t.Fatal("original profile backup was not preserved")
	}
	message, err := s.renderV7CCLoginOwned(c, record.Configuration, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(message, "PASS RULE") || !strings.Contains(message, "Missed spots are not replayed") || c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != 0 {
		t.Fatal("native login taught Boolean rules, lost pause guidance or started a pause")
	}
	if strings.Contains(result.warning, "filters were reset") {
		t.Fatal("generic startup duplicated the durable reset notice")
	}
	// Later login uses the saved bank, even after an ordinary edit.
	record.Configuration.Filters.CC.Modes = []string{"FT8"}
	payload, err := filter.PrepareV7UserRecord(c.callsign, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Commit(); err != nil {
		t.Fatal(err)
	}
	next := configurationTestClient(s, c.callsign)
	again := s.restoreV7ClientRecordOwned(next, now.Add(time.Hour))
	if again.loadError != nil || again.profileReset || !next.filter.IsCC() {
		t.Fatalf("returning native login repeated reset: %+v", again)
	}
	current, err := next.captureV7Configuration()
	if err != nil || len(current.Filters.CC.Modes) != 1 || current.Filters.CC.Modes[0] != "FT8" {
		t.Fatal("returning login lost saved native edits", err)
	}
}

func TestV7LoginUpgradeCommitFailureUsesProtectedTemporaryState(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	path := writeV7LoginOldProfile(t, c)
	fault := errors.New("upgrade commit refused")
	s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { return fault }
	result := s.restoreV7ClientRecordOwned(c, time.Now().UTC())
	if !errors.Is(result.loadError, fault) || result.profileReset || !c.recordProtected || !c.preserveRecordOnExit.Load() || c.v7Notice.ProfileResetGeneration != 0 {
		t.Fatalf("failed upgrade became writable/completed: %+v", result)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, []byte(v7LoginOldLiteral)) {
		t.Fatal("failed upgrade overwrote original")
	}
	if err := c.saveFilterOwned(); !errors.Is(err, errProtectedRecord) {
		t.Fatalf("temporary fallback could autosave: %v", err)
	}
}

func TestV7LoginFreshSaveFailureKeepsUnsavedDefaultsWithoutLibraryReset(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-2")
	library := writeV7LoginOldLibrary(t, c)
	before, err := os.ReadFile(library)
	if err != nil {
		t.Fatal(err)
	}
	s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	result := s.restoreV7ClientRecordOwned(c, time.Now().UTC())
	if !result.created || result.loadError != nil || c.recordProtected || !c.configurationUnsaved || c.dialect != DialectGo || c.v7Notice.ProfileResetGeneration != 0 {
		t.Fatalf("fresh-save failure changed existing contract: %+v", result)
	}
	after, err := os.ReadFile(library)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed new profile changed shared library")
	}
	if _, err := os.Stat(library + ".pre-v7.bak"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed initial save reached preset reset")
	}
}

func TestV7LoginCurrentMetadataFailureDoesNotResetPreferences(t *testing.T) {
	s, old, record, path := v7NoticeFixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	c := configurationTestClient(s, old.callsign)
	result := s.restoreV7ClientRecordOwned(c, time.Now().UTC())
	if result.loadError != nil || result.profileReset || c.recordProtected || !strings.Contains(result.warning, "timestamp/IP") {
		t.Fatalf("metadata failure became reset/protection: %+v", result)
	}
	current, err := c.captureV7Configuration()
	if err != nil || !current.Equal(record.Configuration) {
		t.Fatal("metadata failure lost restored preferences", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed metadata update changed profile")
	}
}

func TestV7LoginReservationBusyDoesNotPrepareOrWrite(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	path := writeV7LoginOldProfile(t, c)
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	commits := 0
	s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
	result := s.restoreV7ClientRecordOwned(c, time.Now().UTC())
	if !errors.Is(result.loadError, errConfigurationTransferBusy) || commits != 0 || c.filterBanks != nil || !c.preserveRecordOnExit.Load() {
		t.Fatal("busy login prepared or wrote unreserved configuration")
	}
	select {
	case <-c.done:
	default:
		t.Fatal("busy login was not interrupted")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, []byte(v7LoginOldLiteral)) {
		t.Fatal("busy login changed original")
	}
}

func TestV7NewSSIDResetsSharedOldPresetsAfterProfileCreation(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-2")
	library := writeV7LoginOldLibrary(t, c)
	result := s.restoreV7ClientRecordOwned(c, time.Now().UTC())
	if result.loadError != nil || !result.created || result.profileReset || c.dialect != DialectGo || c.v7Notice.ProfileResetGeneration != 0 || c.v7LibraryResetGeneration != 7 {
		t.Fatalf("new SSID reset outcome: %+v", result)
	}
	if _, err := filter.LoadV7Preset(c.callsign, "OLD"); !errors.Is(err, filter.ErrPresetNotFound) {
		t.Fatal("new SSID could load old preset", err)
	}
	if _, err := os.Stat(library + ".pre-v7.bak"); err != nil {
		t.Fatal("new SSID preset reset lost original backup", err)
	}
}
