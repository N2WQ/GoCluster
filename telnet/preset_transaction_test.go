package telnet

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

func presetTransactionClient(s *Server) *Client {
	c := newTestClient()
	c.server, c.callsign = s, "N2WQ-1"
	c.setDedupePolicy(dedupePolicyMed)
	return c
}

func presetDiskBytes(t *testing.T, path string) []byte {
	t.Helper()
	bs, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bs
}

func presetLibraryPath() string {
	return filepath.Join(filter.UserDataDir, "presets", hex.EncodeToString([]byte("N2WQ"))+".yaml")
}

func TestPresetSaveUsesOneExactCapture(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	c.configurationInitialized = true // Empty settings explicitly select defaults.
	c.filter.Bands = map[string]bool{"20m": false}
	c.filter.AllBands = false
	c.filter.Modes = nil
	c.filter.AllModes = false
	c.filter.AllowWCY = nil
	c.filter.SetWWVEnabled(false)
	c.filter.DXCallsigns = []string{"K1*", "W1*", "K1*"}
	s.saveConfigurationFn = func(call string, cfg filter.Configuration, ref *filter.PresetReference, ips []string) error {
		if ref.Name != "CONTEST" || ref.Baseline.Grid != "" || ref.Baseline.NoiseClass != "" || ref.Baseline.DedupePolicy != "" || ref.Baseline.AllowWCY != nil {
			t.Fatal("SAVE association changed configured defaults")
		}
		if value, present := cfg.Filters.Bands.Allow["20m"]; !present || value || cfg.Filters.Modes.AllowAll || len(cfg.Filters.Modes.Allow) != 0 || cfg.Filters.AllowWWV != filter.DefaultBoolFalse {
			t.Fatal("SAVE changed false entries or an empty mode selection")
		}
		// A persistence callback observes an already captured snapshot. Mutating
		// the source here must not change either the named save or its baseline.
		c.filter.Bands["20m"] = true
		c.filter.DXCallsigns[0] = "MUTATED*"
		return filter.SaveConfiguration(call, cfg, ref, ips)
	}
	response, handled := s.handlePresetCommand(c, "SAVE PRESET CONTEST")
	if !handled || response != "Saved preset CONTEST for N2WQ.\n" {
		t.Fatalf("SAVE: %q", response)
	}
	for _, set := range []*filter.SavedPreset{c.presetReference.Baseline, mustLoadPreset(t, "CONTEST")} {
		if value, present := set.Bands["20m"]; !present || value || set.AllBands || set.AllModes || len(set.Modes) != 0 || set.AllowWWV == nil || *set.AllowWWV || set.AllowWCY != nil || !reflect.DeepEqual(set.DXCallsigns, []string{"K1*", "W1*", "K1*"}) {
			t.Fatal("named snapshot and association did not retain the same exact capture")
		}
	}
	bs := presetDiskBytes(t, filepath.Join(filter.UserDataDir, "N2WQ-1.yaml"))
	for _, literal := range []string{"configuration_version: 2", "name: CONTEST", "20m: false", "allmodes: false", "allow_wwv: false"} {
		if !bytes.Contains(bs, []byte(literal)) {
			t.Fatalf("saved record lacks literal %q", literal)
		}
	}
	for _, absent := range []string{"allow_wcy:", "grid:", "noise_class:", "dedupe_policy:"} {
		if bytes.Contains(bs, []byte(absent)) {
			t.Fatalf("SAVE materialized default field %q", absent)
		}
	}
}

func mustLoadPreset(t *testing.T, name string) *filter.SavedPreset {
	t.Helper()
	set, err := filter.LoadPreset("N2WQ-1", name)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestPresetSavePartialSuccessRetainsDurableAssociation(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	c.filter.DXCallsigns = []string{"K1*"}
	if response, _ := s.handlePresetCommand(c, "SAVE PRESET OLD"); response != "Saved preset OLD for N2WQ.\n" {
		t.Fatal(response)
	}
	old := c.presetReference
	path := filepath.Join(filter.UserDataDir, "N2WQ-1.yaml")
	before := presetDiskBytes(t, path)
	c.filter.DXCallsigns = []string{"W1*"}
	s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
		return errors.New("association commit failed")
	}
	response, _ := s.handlePresetCommand(c, "SAVE PRESET NEW")
	want := "Saved preset NEW, but could not persist its association for N2WQ-1.\nPrevious preset association and baseline retained.\n"
	if response != want || c.presetReference != old || !bytes.Equal(before, presetDiskBytes(t, path)) {
		t.Fatalf("partial SAVE changed the previous association: %q", response)
	}
	if got := mustLoadPreset(t, "NEW").DXCallsigns; !reflect.DeepEqual(got, []string{"W1*"}) {
		t.Fatalf("partial SAVE lost the successful library snapshot: %v", got)
	}
	s.saveConfigurationFn = nil
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	// A subsequent ordinary preference save may persist W1*, but the existing
	// association must still recover OLD with its original K1* baseline.
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || record.Preset == nil || record.Preset.Name != "OLD" || !reflect.DeepEqual(record.DXCallsigns, []string{"W1*"}) || !reflect.DeepEqual(record.Preset.Baseline.DXCallsigns, []string{"K1*"}) {
		t.Fatalf("ordinary save lost the previous baseline: record=%+v err=%v", record, err)
	}
	bs := presetDiskBytes(t, path)
	if !bytes.Contains(bs, []byte("name: OLD")) || bytes.Contains(bs, []byte("name: NEW")) || !bytes.Contains(bs, []byte("K1*")) || !bytes.Contains(bs, []byte("W1*")) {
		t.Fatal("disk association/baseline do not match partial-success contract")
	}
	reconnected := presetTransactionClient(s)
	s.restoreClientRecordOwned(reconnected, time.Now().UTC())
	if reconnected.presetReference == nil || reconnected.presetReference.Name != "OLD" || !reflect.DeepEqual(reconnected.presetReference.Baseline.DXCallsigns, []string{"K1*"}) || !reflect.DeepEqual(reconnected.filter.DXCallsigns, []string{"W1*"}) {
		t.Fatal("reconnect did not restore the retained association and current preferences")
	}
}

func TestPresetProtectedSaveDoesNotWriteLibrary(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	if err := filter.SavePreset(c.callsign, "EXISTING", &filter.SavedPreset{Filter: *filter.NewFilter(), Grid: "FN31"}); err != nil {
		t.Fatal(err)
	}
	path := presetLibraryPath()
	before := presetDiskBytes(t, path)
	c.recordProtected = true
	c.filter.DXCallsigns = []string{strings.Repeat("A", filter.MaxPresetBytes+1)}
	for _, command := range []string{"SAVE PRESET EXISTING", "SAVE PRESET NEW"} {
		response, _ := s.handlePresetCommand(c, command)
		if response != "SAVE PRESET failed: saved user record is protected; changes in this session are temporary\n" || !bytes.Equal(before, presetDiskBytes(t, path)) {
			t.Fatalf("protected SAVE reached capture/library mutation: %q", response)
		}
	}
}

func TestPresetLoadExceedsConfigReadbackBudget(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	pattern := strings.Repeat("A", 70000)
	if err := filter.SavePreset(c.callsign, "LARGE", &filter.SavedPreset{Filter: filter.Filter{DXCallsigns: []string{pattern}}, DedupePolicy: "MED"}); err != nil {
		t.Fatal(err)
	}
	pointer := c.filter
	response, _ := s.handlePresetCommand(c, "LOAD PRESET LARGE")
	if response != "Loaded preset LARGE; defaults saved for N2WQ-1.\n" || c.filter != pointer || !reflect.DeepEqual(c.filter.DXCallsigns, []string{pattern}) {
		t.Fatalf("LOAD incorrectly applied the CONFIG readback budget: %q", response)
	}
	if _, err := c.captureConfiguration(maxYAMLBytes); !errors.Is(err, errReadbackTooLarge) {
		t.Fatalf("large configuration did not exceed readback preflight: %v", err)
	}
	bs := presetDiskBytes(t, filepath.Join(filter.UserDataDir, "N2WQ-1.yaml"))
	if bytes.Count(bs, []byte(pattern)) != 2 || !bytes.Contains(bs, []byte("name: LARGE")) {
		t.Fatal("large LOAD did not persist the complete configuration and baseline")
	}
}

func TestPresetFailedLoadRetainsAssociation(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	c.filter.DXCallsigns = []string{"K1*"}
	if response, _ := s.handlePresetCommand(c, "SAVE PRESET OLD"); response != "Saved preset OLD for N2WQ.\n" {
		t.Fatal(response)
	}
	if err := filter.SavePreset(c.callsign, "NEW", &filter.SavedPreset{Filter: filter.Filter{DXCallsigns: []string{"W1*"}}, Grid: "FN31"}); err != nil {
		t.Fatal(err)
	}
	old, pointer := c.presetReference, c.filter
	path := filepath.Join(filter.UserDataDir, "N2WQ-1.yaml")
	before := presetDiskBytes(t, path)
	s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
		return errors.New("commit failed")
	}
	response, _ := s.handlePresetCommand(c, "LOAD PRESET NEW")
	if response != "LOAD PRESET failed: commit failed\n" || c.presetReference != old || c.filter != pointer || !reflect.DeepEqual(c.filter.DXCallsigns, []string{"K1*"}) || !bytes.Equal(before, presetDiskBytes(t, path)) {
		t.Fatalf("failed LOAD changed live/durable association or preferences: %q", response)
	}
}

func TestPresetLoadAppliedBaselineSurvivesLibraryChanges(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	s.dedupeSlowEnabled = false
	s.gridLookup = func(string) (string, bool, bool) { return "IO91", true, true }
	if err := filter.SavePreset(c.callsign, "CONTEST", &filter.SavedPreset{Filter: *filter.NewFilter(), DedupePolicy: "SLOW"}); err != nil {
		t.Fatal(err)
	}
	response, _ := s.handlePresetCommand(c, "LOAD PRESET CONTEST")
	want := "Loaded preset CONTEST; defaults saved for N2WQ-1.\nNote: dedupe SLOW unavailable; using FAST.\n"
	if response != want || c.configuredSettings.Grid != "" || c.grid != "IO91" || !c.gridDerived || c.configuredSettings.DedupePolicy != "FAST" || c.presetReference.Baseline.DedupePolicy != "FAST" {
		t.Fatalf("LOAD did not separate applied preferences and runtime fallbacks: %q", response)
	}
	if mustLoadPreset(t, "CONTEST").DedupePolicy != "SLOW" {
		t.Fatal("LOAD changed the shared named preset")
	}
	path := filepath.Join(filter.UserDataDir, "N2WQ-1.yaml")
	bs := presetDiskBytes(t, path)
	if bytes.Count(bs, []byte("dedupe_policy: FAST")) != 2 || bytes.Contains(bs, []byte("grid:")) {
		t.Fatal("disk configuration/baseline lost applied dedupe or stored the lookup GRID")
	}
	if err := filter.SavePreset(c.callsign, "CONTEST", &filter.SavedPreset{Filter: *filter.NewFilter(), Grid: "FN31"}); err != nil {
		t.Fatal(err)
	}
	if response, _ = s.handlePresetCommand(c, "DELETE PRESET CONTEST"); response != "Deleted preset CONTEST.\n" {
		t.Fatal(response)
	}
	if !bytes.Equal(bs, presetDiskBytes(t, path)) || c.presetReference.Name != "CONTEST" || c.presetReference.Baseline.Grid != "" || c.presetReference.Baseline.DedupePolicy != "FAST" {
		t.Fatal("library overwrite/deletion changed the applied snapshot")
	}
}

func TestPresetLoadUnchangedCadenceResetsFromPublication(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	stale := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	c.setSolarSummaryMinutes(30, stale)
	if err := filter.SavePreset(c.callsign, "SOLAR", &filter.SavedPreset{Filter: *filter.NewFilter(), SolarSummaryMinutes: 30}); err != nil {
		t.Fatal(err)
	}
	var commitFinished time.Time
	s.saveConfigurationFn = func(call string, cfg filter.Configuration, ref *filter.PresetReference, ips []string) error {
		if err := filter.SaveConfiguration(call, cfg, ref, ips); err != nil {
			return err
		}
		commitFinished = time.Now().UTC()
		return nil
	}
	response, _ := s.handlePresetCommand(c, "LOAD PRESET SOLAR")
	after := time.Now().UTC()
	if response != "Loaded preset SOLAR; defaults saved for N2WQ-1.\n" || !c.solarNextSummaryAt.After(commitFinished) || c.solarNextSummaryAt.After(nextSolarSummaryAt(after, 30)) || c.getSolarSummaryMinutes() != 30 {
		t.Fatalf("same-cadence LOAD retained a stale solar clock: response=%q next=%v", response, c.solarNextSummaryAt)
	}
}
