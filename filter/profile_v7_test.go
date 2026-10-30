package filter

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func v7TestConfiguration(t *testing.T) V7Configuration {
	t.Helper()
	c, err := DefaultV7Configuration(SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "SLOW", SolarSummaryMinutes: 30})
	if err != nil {
		t.Fatal(err)
	}
	c.Filters.GO = V6FilterConfiguration{Rules: []RuleDefinition{{Slot: 3, Action: RulePass, Expr: "COMMENT=GO-ONLY"}}, AllowSelf: DefaultBoolFalse}
	c.Filters.CC.Modes = []string{"CW"}
	c.Filters.CC.Sources = []string{"HUMAN"}
	c.Filters.CC.AllowWWV = DefaultBoolFalse
	c, _, _, err = PrepareV7Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestV7ProfileCodecOwnsBothBanksAndBaselines(t *testing.T) {
	c := v7TestConfiguration(t)
	goSet, err := V7PresetFromConfiguration(c, "go")
	if err != nil {
		t.Fatal(err)
	}
	ccSet, err := V7PresetFromConfiguration(c, "cc")
	if err != nil {
		t.Fatal(err)
	}
	record := &V7UserRecord{Configuration: c, Presets: V7PresetReferences{GO: &V7PresetReference{Name: "GO", Baseline: goSet}, CC: &V7PresetReference{Name: "CC", Baseline: ccSet}}, RecentIPs: []string{"192.0.2.1"}, LastLoginUTC: time.Unix(1700000000, 0).UTC(), Notice: V7NoticeState{ProfileResetGeneration: 7}}
	data, err := EncodeV7UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeV7UserRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfigurationVersion != 7 || !got.Configuration.Equal(c) || !got.Presets.GO.Baseline.MatchesConfiguration(c) || !got.Presets.CC.Baseline.MatchesConfiguration(c) || got.Notice.ProfileResetGeneration != 7 || !got.LastLoginUTC.Equal(record.LastLoginUTC) {
		t.Fatal("roundtrip lost bank, baseline or metadata")
	}
	before := got.Configuration.Fingerprint()
	record.Configuration.Filters.CC.Modes[0] = "FT8"
	record.Configuration.Filters.GO.Rules[0].Expr = "MODE=FT8"
	record.Presets.CC.Baseline.CC.Modes[0] = "RTTY"
	if got.Configuration.Fingerprint() != before || got.Presets.CC.Baseline.CC.Modes[0] != "CW" {
		t.Fatal("codec retained mutable input state")
	}
	got.Notice.ProfileAcknowledgedGeneration = 7
	got.RecentIPs = []string{"198.51.100.1"}
	if got.Configuration.Fingerprint() != before {
		t.Fatal("readonly metadata entered preference fingerprint")
	}
	other := got.Configuration.Clone()
	other.Settings.Dialect = "go"
	if !got.Presets.GO.Baseline.MatchesConfiguration(other) || !got.Presets.CC.Baseline.MatchesConfiguration(other) {
		t.Fatal("selector change modified bank baselines")
	}
}

func TestV7CodecRejectsMalformedOwners(t *testing.T) {
	c := v7TestConfiguration(t)
	data, err := EncodeV7UserRecord(&V7UserRecord{Configuration: c})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{
		bytes.Replace(data, []byte("configuration_version: 7"), []byte("configuration_version: 6"), 1),
		append([]byte("unknown: true\n"), data...),
		bytes.Replace(data, []byte("notice:"), []byte("notice: null\nignored_notice:"), 1),
		append(data, []byte("configuration_version: 7\n")...),
	} {
		if _, err := DecodeV7UserRecord(raw); err == nil {
			t.Fatalf("accepted malformed v7 owner: %.100s", raw)
		}
	}
	if _, err := EncodeV7UserRecord(&V7UserRecord{Configuration: c, Notice: V7NoticeState{ProfileAcknowledgedGeneration: 7}}); err == nil {
		t.Fatal("acknowledged nonexistent profile event")
	}
}

func TestV7ProfileWorkBudgetIncludesBothReferences(t *testing.T) {
	c := v7TestConfiguration(t)
	c.Settings.NoiseClass = strings.Repeat("N", 180000)
	goSet, err := V7PresetFromConfiguration(c, "go")
	if err != nil {
		t.Fatal(err)
	}
	ccSet, err := V7PresetFromConfiguration(c, "cc")
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeV7UserRecord(&V7UserRecord{Configuration: c, Presets: V7PresetReferences{GO: &V7PresetReference{Name: "GO", Baseline: goSet}, CC: &V7PresetReference{Name: "CC", Baseline: ccSet}}})
	if err != nil {
		t.Fatal(err)
	}
	needle := strings.Repeat("N", 180000)
	raw := strings.Replace(string(data), needle, "&shared "+needle, 1)
	raw = strings.ReplaceAll(raw, needle, "*shared")
	// Restore the defining anchor after replacing the other two occurrences.
	raw = strings.Replace(raw, "&shared *shared", "&shared "+needle, 1)
	raw += "recent_ips:\n" + strings.Repeat("    - "+strings.Repeat("X", 6000)+"\n", 128)
	if len(raw) > MaxUserRecordBytes {
		t.Fatal("fixture exceeded physical rather than shared work boundary")
	}
	if _, err := DecodeV7UserRecord([]byte(raw)); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("shared baseline work was not refused: %v", err)
	}
}

func TestV7PresetRestoresOnlyTaggedBank(t *testing.T) {
	c := v7TestConfiguration(t)
	set, err := V7PresetFromConfiguration(c, "cc")
	if err != nil {
		t.Fatal(err)
	}
	changed := c.Clone()
	changed.Filters.GO.Rules[0].Expr = "MODE=RTTY"
	changed.Filters.CC.Modes = []string{"FT8"}
	changed.Settings.Dialect = "go"
	got, err := ApplyV7Preset(changed, set)
	if err != nil {
		t.Fatal(err)
	}
	if got.Settings.Dialect != "cc" || got.Filters.GO.Rules[0].Expr != "MODE=RTTY" || !got.Filters.CC.Equal(c.Filters.CC) {
		t.Fatal("tagged LOAD changed wrong bank or selection")
	}
	bad := *set
	bad.GO = &c.Filters.GO
	if _, err := bad.Clone(); err == nil {
		t.Fatal("accepted two-bank named preset")
	}
}

func TestV7ProfileMetadataAdmissionPrecedesEncoding(t *testing.T) {
	c := v7TestConfiguration(t)
	if _, err := EncodeV7UserRecord(&V7UserRecord{Configuration: c, RecentIPs: []string{strings.Repeat("x", MaxUserRecordBytes+1)}}); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("oversized metadata admitted: %v", err)
	}
}
