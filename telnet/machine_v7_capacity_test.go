package telnet

import (
	"errors"
	"math"
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
	// Measure the entire encoded envelope independently, including duplicated
	// configured settings, escaped revision, both max-name references and CRLF.
	want := v7ReferenceYAMLFrame(t, struct {
		Version       int                    `yaml:"schema_version"`
		RequestID     string                 `yaml:"request_id"`
		Resource      string                 `yaml:"resource"`
		Revision      string                 `yaml:"revision"`
		Configuration filter.V7Configuration `yaml:"configuration"`
		Status        v7ReadbackStatus       `yaml:"status"`
	}{7, requestID, "CONFIG", revision, canonical, v7ReadbackStatus{status, "cc", presets}})
	got, err := renderV7YAMLReadback(canonical, runtime.PreparedRules(), status, presets, "CONFIG", requestID, revision)
	if len(want) <= 262_144 {
		if err != nil || got != want {
			t.Fatalf("admissible widest complete frame differs: want=%d got=%d err=%v", len(want), len(got), err)
		}
	} else if got != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatalf("independently oversized complete frame admitted: want=%d got=%d err=%v", len(want), len(got), err)
	}
	t.Logf("full configuration=%d stored two-reference profile=%d widest CRLF frame=%d (limit 262144)", len(raw), len(stored), len(want))
}
