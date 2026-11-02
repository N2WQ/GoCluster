package telnet

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

// Large positive ADIF integers exercise sparse operands without depending on a
// particular DXCC table. Every row stays within its independent 512-value cap.
func v7CCCapacityCandidate(base filter.CCConfiguration, count int) filter.CCConfiguration {
	c := base.Clone()
	values := make([]int, filter.MaxCCCountries)
	for i := range values {
		values[i] = math.MaxInt - i
	}
	row := func() filter.CCCategory {
		n := min(count, len(values))
		count -= n
		return filter.CCCategory{Action: filter.CCPass, Countries: values[:n]}
	}
	for _, name := range []string{"DOC", "DXCTY"} {
		entry := filter.CCGeography{Global: row(), Bands: make(map[string]filter.CCCategory)}
		for _, band := range filter.CCSupportedBands() {
			entry.Bands[band] = row()
		}
		c.Geography[name] = entry
	}
	return c
}

func v7NearestCCCapacity(t *testing.T) (filter.CCConfiguration, filter.CCConfiguration) {
	t.Helper()
	base, _ := v7PreparedFixture(t)
	low, high := 0, 2*(1+len(filter.CCSupportedBands()))*filter.MaxCCCountries
	encodedLength := func(count int) int {
		raw, err := yaml.Marshal(v7CCCapacityCandidate(base.Filters.CC, count))
		if err != nil {
			t.Fatal(err)
		}
		return len(raw)
	}
	if encodedLength(high) <= filter.MaxCCCanonicalBytes {
		t.Fatal("fixture cannot cross native byte cap")
	}
	for low+1 < high {
		middle := (low + high) / 2
		if encodedLength(middle) <= filter.MaxCCCanonicalBytes {
			low = middle
		} else {
			high = middle
		}
	}
	if gap := filter.MaxCCCanonicalBytes - encodedLength(low); gap < 0 || gap >= 64 {
		t.Fatalf("native fixture is not within one operand of cap: gap=%d", gap)
	}
	return v7CCCapacityCandidate(base.Filters.CC, low), v7CCCapacityCandidate(base.Filters.CC, high)
}

func TestV7CapacityNativeNearestCanonicalLimit(t *testing.T) {
	accepted, refused := v7NearestCCCapacity(t)
	canonical, plan, err := filter.PrepareCCConfiguration(accepted)
	if err != nil {
		t.Fatalf("nearest native candidate refused: %v", err)
	}
	raw, err := yaml.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Counts().CanonicalBytes != len(raw) || len(raw) > 131_072 || len(raw) < 131_008 {
		t.Fatalf("native positive capacity not measured independently: bytes=%d counts=%+v", len(raw), plan.Counts())
	}
	if _, _, err := filter.PrepareCCConfiguration(refused); !errors.Is(err, filter.ErrStorageAdmission) {
		t.Fatalf("next native operand crossed byte cap without refusal: %v", err)
	}
	// Native human output shares the schema-seven final-frame bound. Qualify
	// complete inspection as well as the canonical bank's byte allowance.
	s, c := v7MachineTestClient(t)
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	cfg, _ := v7PreparedFixture(t)
	cfg.Filters.CC = canonical
	cfg.Settings.Dialect = "cc"
	if err := s.v7ConfigurationReadbackFits(c, cfg, c, filter.V7PresetReferences{}); err != nil {
		t.Fatalf("near-128KiB native profile lost complete machine/human inspection: %v", err)
	}
	t.Logf("native canonical accepted=%d bytes; next operand refused", len(raw))
}

func v7WidestCapacityStatus() configurationReadbackStatus {
	return configurationReadbackStatus{
		Effective: readbackEffectiveStatus{Dialect: "cc", Grid: "AA00AA", NoiseClass: "INDUSTRIAL", DedupePolicy: "SLOW", PathMinObservationCount: math.MaxInt, SolarSummaryMinutes: math.MaxInt},
		Session:   readbackSessionStatus{Callsign: strings.Repeat("1", 32), DiagnosticComments: "DEDUPE", PauseRemaining: math.MaxInt, SuppressedSpots: math.MaxUint64, UnsavedChanges: true},
		Server:    readbackServerStatus{DefaultDialect: "go", DefaultDedupePolicy: "SLOW", DefaultNoiseClass: "INDUSTRIAL", PathMinObservationCount: math.MaxInt, AutoReadPauseMinRows: math.MaxInt, AutoReadPauseSeconds: math.MaxInt},
	}
}

func TestV7CapacityCompleteStoredProfileAndWidestFrame(t *testing.T) {
	cfg, _ := v7PreparedFixture(t)
	cc, _ := v7NearestCCCapacity(t)
	cfg.Filters.CC = cc
	cfg.Settings.Dialect, cfg.Settings.NoiseClass = "cc", "NN"
	base, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Exact retained shared strings are a storage compatibility surface. This
	// deliberately does not claim an ordinary SET NOISE command accepts them.
	cfg.Settings.NoiseClass += strings.Repeat("N", filter.MaxV7ConfigurationBytes-len(base))
	raw, err := yaml.Marshal(cfg)
	if err != nil || len(raw) != 196_608 {
		t.Fatalf("stored full-config fixture bytes=%d err=%v", len(raw), err)
	}
	canonical, runtime, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatalf("exact complete-config cap refused: %v", err)
	}
	overflow := cfg.Clone()
	overflow.Settings.NoiseClass += "N"
	if _, _, _, err := filter.PrepareV7Configuration(overflow); !errors.Is(err, filter.ErrStorageAdmission) {
		t.Fatalf("192KiB plus one byte admitted: %v", err)
	}
	goPreset, err := filter.V7PresetFromConfiguration(canonical, "go")
	if err != nil {
		t.Fatal(err)
	}
	ccPreset, err := filter.V7PresetFromConfiguration(canonical, "cc")
	if err != nil {
		t.Fatal(err)
	}
	refs := filter.V7PresetReferences{
		GO: &filter.V7PresetReference{Name: strings.Repeat("G", 32), Baseline: goPreset},
		CC: &filter.V7PresetReference{Name: strings.Repeat("C", 32), Baseline: ccPreset},
	}
	record := &filter.V7UserRecord{Configuration: canonical, Presets: refs, RecentIPs: []string{"2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"}, LastLoginUTC: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), Notice: filter.V7NoticeState{ProfileResetGeneration: 7, ProfileAcknowledgedGeneration: 7, LibraryAcknowledgedGeneration: 7}}
	stored, err := filter.EncodeV7UserRecord(record)
	if err != nil {
		t.Fatalf("both-bank candidate plus both applied references refused: %v", err)
	}
	restored, err := filter.DecodeV7UserRecord(stored)
	if err != nil || !restored.Configuration.Equal(canonical) || restored.Presets.GO.Name != refs.GO.Name || restored.Presets.CC.Name != refs.CC.Name {
		t.Fatalf("large two-reference profile roundtrip failed: %v", err)
	}
	record.RecentIPs = []string{strings.Repeat("I", filter.MaxUserRecordBytes+1)}
	if _, err := filter.EncodeV7UserRecord(record); !errors.Is(err, filter.ErrStorageAdmission) {
		t.Fatalf("oversized complete-profile metadata admitted: %v", err)
	}
	status := v7WidestCapacityStatus()
	presets := v7ReadbackPresetStatuses(canonical, refs)
	status.Configured, status.Preset = canonical.Settings, presets["cc"]
	requestID, revision := strings.Repeat("1", 32), strings.Repeat("\"", 128)
	type referenceSettings struct {
		Dialect                 string `yaml:"dialect"`
		Grid                    string `yaml:"grid"`
		NoiseClass              string `yaml:"noise_class"`
		DedupePolicy            string `yaml:"dedupe_policy"`
		PathMinObservationCount int    `yaml:"path_min_observation_count"`
		SolarSummaryMinutes     int    `yaml:"solar_summary_minutes"`
		CWSpeed                 bool   `yaml:"cw_speed"`
	}
	settings := canonical.Settings
	reference := referenceSettings{settings.Dialect, settings.Grid, settings.NoiseClass,
		settings.DedupePolicy, settings.PathMinObservationCount, settings.SolarSummaryMinutes, settings.CWSpeed}
	type referenceConfiguration struct {
		Filters  filter.V7FilterBanks `yaml:"filters"`
		Settings referenceSettings    `yaml:"settings"`
	}
	type referenceStatus struct {
		Configured    referenceSettings               `yaml:"configured"`
		Effective     readbackEffectiveStatus         `yaml:"effective"`
		Session       readbackSessionStatus           `yaml:"session"`
		Server        readbackServerStatus            `yaml:"server"`
		Preset        readbackPresetStatus            `yaml:"preset"`
		ActiveDialect string                          `yaml:"active_dialect"`
		Presets       map[string]readbackPresetStatus `yaml:"presets"`
	}
	// Measure the entire encoded envelope independently, including duplicated
	// configured settings, escaped revision, both max-name references and CRLF.
	want := v7ReferenceYAMLFrame(t, struct {
		Version       int                    `yaml:"schema_version"`
		RequestID     string                 `yaml:"request_id"`
		Resource      string                 `yaml:"resource"`
		Revision      string                 `yaml:"revision"`
		Configuration referenceConfiguration `yaml:"configuration"`
		Status        referenceStatus        `yaml:"status"`
	}{Version: 7, RequestID: requestID, Resource: "CONFIG", Revision: revision,
		Configuration: referenceConfiguration{canonical.Filters, reference},
		Status:        referenceStatus{reference, status.Effective, status.Session, status.Server, status.Preset, "cc", presets}})
	got, err := renderV7YAMLReadback(canonical, runtime.PreparedRules(), status, presets, "CONFIG", requestID, revision)
	if len(want) <= 262_144 {
		if err != nil || got != want {
			t.Fatalf("admissible widest complete frame differs: want=%d got=%d err=%v", len(want), len(got), err)
		}
	} else if err == nil {
		var normal, compact any
		if len(got) > maxV7YAMLBytes || yaml.Unmarshal([]byte(want), &normal) != nil || yaml.Unmarshal([]byte(got), &compact) != nil || !reflect.DeepEqual(normal, compact) {
			t.Fatalf("compact frame exceeded limit or changed YAML values: regular=%d compact=%d", len(want), len(got))
		}
	} else if got != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatalf("independently oversized complete frame admitted: want=%d got=%d err=%v", len(want), len(got), err)
	}
	t.Logf("full configuration=%d stored two-reference profile=%d widest CRLF frame=%d (limit 262144)", len(raw), len(stored), len(want))
}

// An existing profile can have fewer than 42 bytes of spare response budget.
// Its prior output omitted cw_speed; the updated view must not deny login merely
// because both new explicit false values cross the final-frame limit.
func TestV7CapacityExistingProfileWithExplicitCWSpeed(t *testing.T) {
	cfg, _ := v7PreparedFixture(t)
	cfg.Filters.CC = v7CCCapacityCandidate(cfg.Filters.CC, 6118)
	cfg.Settings.Dialect = "N"
	status := v7WidestCapacityStatus()
	status.Effective.Dialect = "go"
	presets := map[string]readbackPresetStatus{
		"go": {Name: strings.Repeat("1", 32)}, "cc": {Name: strings.Repeat("1", 32)},
	}
	status.Preset = presets["go"]
	requestID, revision := strings.Repeat("1", 32), strings.Repeat("\"", 128)
	type oldStatus struct {
		configurationReadbackStatus `yaml:",inline"`
		ActiveDialect               string                          `yaml:"active_dialect"`
		Presets                     map[string]readbackPresetStatus `yaml:"presets"`
	}
	type oldEnvelope struct {
		Version       int                    `yaml:"schema_version"`
		RequestID     string                 `yaml:"request_id"`
		Resource      string                 `yaml:"resource"`
		Revision      string                 `yaml:"revision"`
		Configuration filter.V7Configuration `yaml:"configuration"`
		Status        oldStatus              `yaml:"status"`
	}
	oldFrame := func(candidate filter.V7Configuration) string {
		view := status
		view.Configured = candidate.Settings
		return v7ReferenceYAMLFrame(t, oldEnvelope{7, requestID, "CONFIG", revision, candidate, oldStatus{view, "go", presets}})
	}
	low, high := 0, 100000
	for low+1 < high {
		middle := (low + high) / 2
		candidate := cfg.Clone()
		candidate.Settings.Dialect = strings.Repeat("N", middle)
		encoded, err := yaml.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) <= filter.MaxV7ConfigurationBytes && len(oldFrame(candidate)) <= maxV7YAMLBytes {
			low = middle
		} else {
			high = middle
		}
	}
	cfg.Settings.Dialect = strings.Repeat("N", low)
	canonical, runtime, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	status.Configured = canonical.Settings
	old := oldFrame(canonical)
	if len(old) > maxV7YAMLBytes || len(old)+42 <= maxV7YAMLBytes {
		t.Fatalf("fixture does not cross new explicit-field boundary: old=%d", len(old))
	}
	got, err := renderV7YAMLReadback(canonical, runtime.PreparedRules(), status, presets, "CONFIG", requestID, revision)
	if err != nil || len(got) > maxV7YAMLBytes {
		t.Fatalf("existing profile lost complete readback: bytes=%d err=%v", len(got), err)
	}
	var before, after map[string]any
	if err := yaml.Unmarshal([]byte(old), &before); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte(got), &after); err != nil {
		t.Fatal(err)
	}
	config := after["configuration"].(map[string]any)["settings"].(map[string]any)
	configured := after["status"].(map[string]any)["configured"].(map[string]any)
	for _, values := range []map[string]any{config, configured} {
		if value, present := values["cw_speed"]; !present || value != false {
			t.Fatalf("explicit CW speed off missing: %v", values["cw_speed"])
		}
		delete(values, "cw_speed")
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("compact readback changed prior configuration or status values")
	}
	s, client := v7MachineTestClient(t)
	record := &filter.V7UserRecord{ConfigurationVersion: filter.V7ConfigurationVersion, Configuration: canonical}
	if err := s.admitV7LoginRecord(client, record, time.Now()); err != nil {
		prepared, _, prepErr := s.prepareV7LoginRuntime(client, record, time.Now())
		fitErr := s.v7ConfigurationReadbackFits(prepared, canonical, prepared, record.Presets)
		_, loginErr := s.renderV7CCLoginOwned(prepared, canonical, false)
		_, noticeErr := renderV7ResetNotice(canonical, prepared.dialect, v7ResetNoticeEvents{profileGeneration: 7, libraryGeneration: 7}, false)
		t.Fatalf("profile login admission failed: %v; prepare=%v fit=%v login=%v notice=%v", err, prepErr, fitErr, loginErr, noticeErr)
	}
	t.Logf("prior widest frame=%d bytes, compact frame=%d bytes", len(old), len(got))

	// An enabled setting has an extra stored field. Free two country operands
	// and use the remaining profile budget for the preserved string,
	// so the complete regular readback still crosses the output limit.
	enabledCfg := canonical.Clone()
	enabledCfg.Filters.CC = v7CCCapacityCandidate(cfg.Filters.CC, 6116)
	enabledCfg.Settings.CWSpeed = true
	enabledRaw, err := yaml.Marshal(enabledCfg)
	if err != nil || len(enabledRaw) > filter.MaxV7ConfigurationBytes {
		t.Fatalf("cannot construct enabled fixture: bytes=%d err=%v", len(enabledRaw), err)
	}
	enabledCfg.Settings.Dialect += strings.Repeat("N", filter.MaxV7ConfigurationBytes-len(enabledRaw))
	enabledCfg, enabledRuntime, _, err := filter.PrepareV7Configuration(enabledCfg)
	if err != nil {
		t.Fatal(err)
	}
	enabledPrior := enabledCfg.Clone()
	enabledPrior.Settings.CWSpeed = false
	oldEnabled := oldFrame(enabledPrior)
	status.Configured = enabledCfg.Settings
	regularEnabled := v7ReferenceYAMLFrame(t, struct {
		Version       int    `yaml:"schema_version"`
		RequestID     string `yaml:"request_id"`
		Resource      string `yaml:"resource"`
		Revision      string `yaml:"revision"`
		Configuration struct {
			Filters  filter.V7FilterBanks `yaml:"filters"`
			Settings v7SettingsReadback   `yaml:"settings"`
		} `yaml:"configuration"`
		Status v7ReadbackStatus `yaml:"status"`
	}{Version: 7, RequestID: requestID, Resource: "CONFIG", Revision: revision,
		Configuration: struct {
			Filters  filter.V7FilterBanks `yaml:"filters"`
			Settings v7SettingsReadback   `yaml:"settings"`
		}{enabledCfg.Filters, v7SettingsView(enabledCfg.Settings)},
		Status: v7ReadbackStatus{Configured: v7SettingsView(status.Configured), Effective: status.Effective,
			Session: status.Session, Server: status.Server, Preset: status.Preset, ActiveDialect: "go", Presets: presets}})
	if len(oldEnabled) > maxV7YAMLBytes || len(regularEnabled) <= maxV7YAMLBytes {
		t.Fatalf("enabled fixture does not require compact fallback: old=%d regular=%d", len(oldEnabled), len(regularEnabled))
	}
	compactEnabled, err := renderV7YAMLReadback(enabledCfg, enabledRuntime.PreparedRules(), status, presets, "CONFIG", requestID, revision)
	if err != nil || len(compactEnabled) > maxV7YAMLBytes {
		t.Fatalf("enabled compact readback refused: bytes=%d err=%v", len(compactEnabled), err)
	}
	var enabledBefore, enabledAfter map[string]any
	if err := yaml.Unmarshal([]byte(oldEnabled), &enabledBefore); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte(compactEnabled), &enabledAfter); err != nil {
		t.Fatal(err)
	}
	for _, values := range []map[string]any{
		enabledAfter["configuration"].(map[string]any)["settings"].(map[string]any),
		enabledAfter["status"].(map[string]any)["configured"].(map[string]any),
	} {
		if value, present := values["cw_speed"]; !present || value != true {
			t.Fatalf("explicit CW speed on missing: %v", values["cw_speed"])
		}
		delete(values, "cw_speed")
	}
	if !reflect.DeepEqual(enabledBefore, enabledAfter) {
		t.Fatal("enabled compact readback changed prior configuration or status values")
	}
	enabledRecord := &filter.V7UserRecord{ConfigurationVersion: filter.V7ConfigurationVersion, Configuration: enabledCfg}
	if err := s.admitV7LoginRecord(client, enabledRecord, time.Now()); err != nil {
		t.Fatalf("enabled profile lost login admission: %v", err)
	}
	t.Logf("enabled prior=%d regular=%d compact=%d bytes", len(oldEnabled), len(regularEnabled), len(compactEnabled))

	// Removing two country operands frees bytes that can be moved into a
	// duplicated setting. This keeps the stored config bounded while making
	// its previous full readback too large. Compact encoding must not widen the
	// pre-existing admission boundary for such a candidate.
	over := canonical.Clone()
	over.Filters.CC = v7CCCapacityCandidate(cfg.Filters.CC, 6116)
	overRaw, err := yaml.Marshal(over)
	if err != nil || len(overRaw) > filter.MaxV7ConfigurationBytes {
		t.Fatalf("cannot construct over-boundary fixture: bytes=%d err=%v", len(overRaw), err)
	}
	over.Settings.Dialect += strings.Repeat("N", filter.MaxV7ConfigurationBytes-len(overRaw))
	over, overRuntime, _, err := filter.PrepareV7Configuration(over)
	if err != nil {
		t.Fatal(err)
	}
	priorOver := oldFrame(over)
	if len(priorOver) <= maxV7YAMLBytes {
		t.Fatalf("over-boundary fixture had a previously admissible frame: %d", len(priorOver))
	}
	for _, enabled := range []bool{false, true} {
		over.Settings.CWSpeed = enabled
		status.Configured = over.Settings
		compact := v7ReferenceYAMLFrame(t, struct {
			Version       int    `yaml:"schema_version"`
			RequestID     string `yaml:"request_id"`
			Resource      string `yaml:"resource"`
			Revision      string `yaml:"revision"`
			Configuration struct {
				Filters  filter.V7FilterBanks `yaml:"filters"`
				Settings v7SettingsReadback   `yaml:"settings"`
			} `yaml:"configuration"`
			Status v7CompactReadbackStatus `yaml:"status"`
		}{Version: 7, RequestID: requestID, Resource: "CONFIG", Revision: revision,
			Configuration: struct {
				Filters  filter.V7FilterBanks `yaml:"filters"`
				Settings v7SettingsReadback   `yaml:"settings"`
			}{over.Filters, v7SettingsView(over.Settings)},
			Status: v7CompactReadbackStatus{Configured: v7SettingsView(status.Configured), Effective: status.Effective, Session: status.Session,
				Server: status.Server, Preset: status.Preset, ActiveDialect: "go", Presets: presets}})
		if len(compact) > maxV7YAMLBytes {
			t.Fatalf("fixture does not test false-green compact admission: cw_speed=%v compact=%d", enabled, len(compact))
		}
		rejected, err := renderV7YAMLReadback(over, overRuntime.PreparedRules(), status, presets, "CONFIG", requestID, revision)
		if rejected != "" || !errors.Is(err, errReadbackTooLarge) {
			t.Fatalf("compact formatting widened prior admission: cw_speed=%v bytes=%d err=%v", enabled, len(rejected), err)
		}
	}
}
