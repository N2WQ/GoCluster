package filter

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter/expr"
	"gopkg.in/yaml.v3"
)

func TestV6StorageRuleAdmission(t *testing.T) {
	for _, rule := range []string{
		"null", "{}", "{action: pass, expr: 'MODE=CW'}", "{slot: 0, expr: 'MODE=CW'}",
		"{slot: 0, action: pass}", "{slot: null, action: pass, expr: 'MODE=CW'}",
		"{slot: '0', action: pass, expr: 'MODE=CW'}", "{slot: false, action: pass, expr: 'MODE=CW'}",
		"{slot: 0, action: null, expr: 'MODE=CW'}", "{slot: 0, action: true, expr: 'MODE=CW'}",
		"{slot: 0, action: pass, expr: null}", "{slot: 0, action: pass, expr: true}",
		"{slot: -1, action: pass, expr: 'MODE=CW'}", "{slot: 10, action: pass, expr: 'MODE=CW'}",
		"{slot: 0, action: PASS, expr: 'MODE=CW'}", "{slot: 0, action: other, expr: 'MODE=CW'}",
		"{slot: 0, action: pass, expr: ''}", "{slot: 0, action: pass, expr: 'SNR=BAD'}",
		"{slot: 0, action: pass, expr: 'BOGUS=1'}", "{slot: 0, action: pass, expr: 'MODE=CW AND'}",
		"{slot: 0, slot: 1, action: pass, expr: 'MODE=CW'}",
		"{slot: 0, action: pass, expr: 'MODE=CW', unknown: false}",
	} {
		raw := "configuration_version: 6\nconfiguration: {filters: {rules: [" + rule + "]}, settings: {}}\n"
		if _, err := DecodeV6UserRecord([]byte(raw)); err == nil {
			t.Fatalf("invalid rule admitted: %s", rule)
		}
	}
	for _, rules := range []string{
		"{slot: 0, action: pass, expr: 'MODE=CW'}, {slot: 0, action: reject, expr: 'MODE=FT8'}",
		strings.TrimSuffix(strings.Repeat("{slot: 0, action: pass, expr: 'MODE=CW'},", MaxRuleSlots+1), ","),
		fmt.Sprintf("{slot: 0, action: pass, expr: %q}", strings.Repeat("A", expr.MaxBytes+1)),
	} {
		raw := "configuration_version: 6\nconfiguration: {filters: {rules: [" + rules + "]}, settings: {}}\n"
		if _, err := DecodeV6UserRecord([]byte(raw)); err == nil {
			t.Fatal("duplicate/oversized rules admitted")
		}
	}
	for _, mutate := range []func(*V6Configuration){
		func(c *V6Configuration) { c.Filters.Rules = make([]RuleDefinition, MaxRuleSlots+1) },
		func(c *V6Configuration) { c.Filters.Rules[0].Expr = strings.Repeat("A", expr.MaxBytes+1) },
		func(c *V6Configuration) { c.Filters.Rules[0].Action = "PASS" },
		func(c *V6Configuration) { c.Filters.Rules[1].Slot = c.Filters.Rules[0].Slot },
		func(c *V6Configuration) { c.Filters.Rules[0].Slot = 10 },
		func(c *V6Configuration) { c.Filters.AllowWWV = 255 },
	} {
		record := &V6UserRecord{Configuration: v6StorageTestConfiguration()}
		mutate(&record.Configuration)
		before := record.Configuration.Clone()
		if _, err := EncodeV6UserRecord(record); err == nil {
			t.Fatal("invalid native configuration encoded")
		}
		if !reflect.DeepEqual(record.Configuration, before) {
			t.Fatal("refusal mutated caller")
		}
	}
}

func TestV6StorageExactRuleBounds(t *testing.T) {
	record := &V6UserRecord{}
	for slot := range MaxRuleSlots {
		record.Configuration.Filters.Rules = append(record.Configuration.Filters.Rules, RuleDefinition{slot, RulePass, "MODE=CW"})
	}
	// The raw input bound precedes compilation even when canonicalization would
	// remove all padding. Exactly 1024 bytes must remain acceptable.
	record.Configuration.Filters.Rules[0].Expr = strings.Repeat(" ", expr.MaxBytes-len("MODE=CW")) + "MODE=CW"
	raw, err := yaml.Marshal(&V6UserRecord{ConfigurationVersion: 6, Configuration: record.Configuration})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeV6UserRecord(raw)
	if err != nil || len(got.Configuration.Filters.Rules) != MaxRuleSlots || got.Configuration.Filters.Rules[0].Expr != "MODE=CW" {
		t.Fatal("exact raw expression/count boundary refused", err)
	}
	if _, err := EncodeV6UserRecord(record); err != nil {
		t.Fatal("exact native expression/count boundary refused", err)
	}
	record.Configuration.Filters.Rules[0].Expr = " " + record.Configuration.Filters.Rules[0].Expr
	raw, err = yaml.Marshal(&V6UserRecord{ConfigurationVersion: 6, Configuration: record.Configuration})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeV6UserRecord(raw); err == nil {
		t.Fatal("oversized raw text was accepted after canonicalization")
	}
}

func v6StorageExactPreset(t *testing.T) *V6SavedPreset {
	t.Helper()
	set := &V6SavedPreset{ConfigurationVersion: 6, Configuration: V6Configuration{
		Settings: SettingsConfiguration{Grid: "A"},
	}}
	base, err := yaml.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	set.Configuration.Settings.Grid = strings.Repeat("A", MaxPresetBytes-len(base)+1)
	data, err := yaml.Marshal(set)
	if err != nil || len(data) != MaxPresetBytes {
		t.Fatalf("standalone exact preset fixture: %d, %v", len(data), err)
	}
	return set
}

func TestV6StorageStandaloneExactSizeOracle(t *testing.T) {
	set := v6StorageExactPreset(t)
	for _, record := range []*V6UserRecord{
		{Configuration: set.Configuration},
		{Preset: &V6PresetReference{Name: "TEST", Baseline: set}},
	} {
		data, err := EncodeV6UserRecord(record)
		if err != nil {
			t.Fatal("exact standalone preset rejected", err)
		}
		if _, err := DecodeV6UserRecord(data); err != nil {
			t.Fatal("exact standalone preset failed reloading", err)
		}
	}
	collection := &V6PresetCollection{Presets: map[string]*V6SavedPreset{"TEST": set}}
	data, err := EncodeV6PresetCollection(collection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeV6PresetCollection(data); err != nil {
		t.Fatal("exact named preset failed reloading", err)
	}
	set.Configuration.Settings.Grid += "A"
	standalone, err := yaml.Marshal(set)
	if err != nil || len(standalone) != MaxPresetBytes+1 {
		t.Fatal("oversize fixture did not cross exact standalone boundary", err)
	}
	proxy, err := yaml.Marshal(set.Configuration)
	if err != nil || len(proxy) > MaxPresetBytes {
		t.Fatal("fixture did not distinguish standalone encoding from configuration proxy", err)
	}
	for _, record := range []*V6UserRecord{
		{ConfigurationVersion: 6, Configuration: set.Configuration},
		{ConfigurationVersion: 6, Preset: &V6PresetReference{Name: "TEST", Baseline: set}},
	} {
		if _, err := EncodeV6UserRecord(record); !errors.Is(err, ErrStorageAdmission) {
			t.Fatal("oversized standalone configuration/baseline encoded", err)
		}
		// Raw is small enough; failure must come from normalized standalone size.
		raw, err := yaml.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeV6UserRecord(raw); !errors.Is(err, ErrStorageAdmission) {
			t.Fatal("oversized standalone configuration/baseline decoded", err)
		}
	}
	if _, err := EncodeV6PresetCollection(collection); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("oversized named preset encoded", err)
	}
	raw, err := yaml.Marshal(collection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeV6PresetCollection(raw); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("oversized named preset decoded", err)
	}
	for _, grid := range []string{strings.Repeat("A", MaxPresetBytes+1), strings.Repeat("\x01", MaxPresetBytes/2)} {
		if _, err := EncodeV6UserRecord(&V6UserRecord{Configuration: V6Configuration{Settings: SettingsConfiguration{Grid: grid}}}); !errors.Is(err, ErrStorageAdmission) {
			t.Fatal("lower bound or exact escaped-byte guard bypassed", err)
		}
	}
}

func TestV6StoragePreflightDoesNotChargeDiscardedRuleWhitespace(t *testing.T) {
	canonical := &V6SavedPreset{ConfigurationVersion: 6, Configuration: V6Configuration{
		Settings: SettingsConfiguration{Grid: "A"},
	}}
	for slot := range MaxRuleSlots {
		canonical.Configuration.Filters.Rules = append(canonical.Configuration.Filters.Rules, RuleDefinition{slot, RulePass, "MODE=CW"})
	}
	base, err := yaml.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	canonical.Configuration.Settings.Grid = strings.Repeat("A", MaxPresetBytes-len(base)+1)
	exact, err := yaml.Marshal(canonical)
	if err != nil || len(exact) != MaxPresetBytes {
		t.Fatal("canonical standalone fixture is not exact", err)
	}
	raw := canonical.Configuration.Clone()
	for i := range raw.Filters.Rules {
		raw.Filters.Rules[i].Expr = strings.Repeat(" ", expr.MaxBytes-len("MODE=CW")) + "MODE=CW"
	}
	input := &V6UserRecord{ConfigurationVersion: 6, Configuration: raw}
	data, err := EncodeV6UserRecord(input)
	if err != nil {
		t.Fatal("raw whitespace was incorrectly treated as canonical size", err)
	}
	disk, err := yaml.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeV6UserRecord(disk)
	if err != nil || !decoded.Configuration.Equal(canonical.Configuration) {
		t.Fatal("raw standalone representation was used instead of canonical encoding", err)
	}
	if reencoded, err := EncodeV6UserRecord(decoded); err != nil || !bytes.Equal(reencoded, data) {
		t.Fatal("canonical boundary was not stable", err)
	}
}

func TestV6StorageUserExactByteBounds(t *testing.T) {
	record := &V6UserRecord{RecentIPs: []string{"A"}}
	base, err := EncodeV6UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	record.RecentIPs[0] = strings.Repeat("A", MaxUserRecordBytes-len(base)+1)
	data, err := EncodeV6UserRecord(record)
	if err != nil || len(data) != MaxUserRecordBytes {
		t.Fatalf("exact user boundary: %d %v", len(data), err)
	}
	if _, err := DecodeV6UserRecord(data); err != nil {
		t.Fatal("exact encoded record did not reload", err)
	}
	record.RecentIPs[0] += "A"
	if _, err := EncodeV6UserRecord(record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("prospective exact size guard bypassed", err)
	}
	if _, err := DecodeV6UserRecord(append(data, ' ')); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("raw record bound bypassed", err)
	}
	for _, extra := range []int{0, 1} {
		raw := []byte(v6StorageMinimalRecord + strings.Repeat(" ", MaxUserRecordBytes-len(v6StorageMinimalRecord)+extra))
		_, err := DecodeV6UserRecord(raw)
		if extra == 0 && err != nil || extra == 1 && !errors.Is(err, ErrStorageAdmission) {
			t.Fatalf("raw boundary extra=%d: %v", extra, err)
		}
	}
	// One physical scalar becomes five metadata values. Raw/work limits fit,
	// but normalization must still enforce the exact final record-byte limit.
	raw := v6StorageMinimalRecord + "recent_ips: [&ip " + strings.Repeat("A", 220_000) + ", *ip, *ip, *ip, *ip]\n"
	if _, err := DecodeV6UserRecord([]byte(raw)); !errors.Is(err, ErrStorageAdmission) || strings.Contains(err.Error(), "work") {
		t.Fatal("canonical record bound was not independently enforced", err)
	}
	if _, err := DecodeV6PresetCollection(bytes.Repeat([]byte(" "), maxPresetCollectionBytes+1)); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("raw collection byte bound bypassed", err)
	}
}

func TestV6StorageSharedWorkAndAliasRefusals(t *testing.T) {
	// A single source config and baseline each fit standalone, and only five
	// recent IPs survive normalization. All raw aliases must nevertheless pay.
	config := "{filters: {rules: []}, settings: {grid: &text " + strings.Repeat("A", 32<<10) + "}}"
	raw := "configuration_version: 6\nconfiguration: &cfg " + config +
		"\npreset: {name: TEST, baseline: {configuration_version: 6, configuration: *cfg}}\nrecent_ips: [" +
		strings.TrimSuffix(strings.Repeat("*text,", 64), ",") + "]\n"
	if _, err := DecodeV6UserRecord([]byte(raw)); !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "work") {
		t.Fatal("repeated aliases bypassed shared raw work admission", err)
	}
	// Removing only the excess, normalized-away metadata makes the same document
	// valid, proving no alias blanket-ban or accidental per-owner size refusal.
	small := strings.Replace(raw, strings.TrimSuffix(strings.Repeat("*text,", 64), ","), "*text,*text", 1)
	if _, err := DecodeV6UserRecord([]byte(small)); err != nil {
		t.Fatal("bounded shared aliases refused", err)
	}
	var record V6UserRecord
	budget := uint64(180_000)
	if err := decodeV6StorageDocument([]byte(small), MaxUserRecordBytes, storageDecodeLimits{work: budget}, &record); err != nil {
		t.Fatal("bounded shared-budget fixture refused", err)
	}
	// Same root still fits the smaller budget without its baseline. A nested
	// baseline must not get a fresh budget that would also admit the whole record.
	withoutBaseline := strings.Replace(small, "\npreset: {name: TEST, baseline: {configuration_version: 6, configuration: *cfg}}", "", 1)
	budget = 150_000
	if err := decodeV6StorageDocument([]byte(withoutBaseline), MaxUserRecordBytes, storageDecodeLimits{work: budget}, &record); err != nil {
		t.Fatal("isolated root did not fit reduced budget", err)
	}
	if err := decodeV6StorageDocument([]byte(small), MaxUserRecordBytes, storageDecodeLimits{work: budget}, &record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("nested baseline did not share the document budget", err)
	}
	for _, cyclic := range []string{
		"configuration_version: 6\nconfiguration: &cycle {filters: {rules: [*cycle]}, settings: {}}\n",
		v6StorageMinimalRecord + "recent_ips: &cycle [*cycle]\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: []}, settings: {grid: &cycle [*cycle]}}\n",
	} {
		if _, err := DecodeV6UserRecord([]byte(cyclic)); err == nil {
			t.Fatal("recursive alias admitted")
		}
	}
	// All twenty presets borrow one physical large configuration. The shared
	// collection budget refuses before typed maps/oversized snapshots are built.
	large := "presets:\n  P0: &set {configuration_version: 6, configuration: {filters: {rules: []}, settings: {grid: &text " +
		strings.Repeat("A", 256<<10) + ", dialect: *text, noise_class: *text, dedupe_policy: *text}}}\n"
	for i := 1; i < MaxPresets; i++ {
		large += fmt.Sprintf("  P%d: *set\n", i)
	}
	if _, err := DecodeV6PresetCollection([]byte(large)); !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "work") {
		t.Fatal("named presets opened separate work budgets", err)
	}
}

func TestV6StorageDefaultBoolTypedExtension(t *testing.T) {
	type unrelatedByte uint8
	for _, raw := range []string{"DEFAULT", "false", "true"} {
		node := storageMetadataDocument(t, raw).Content[0]
		if err := validateStoredValueWithFields(node, reflect.TypeFor[DefaultBool](), nil); err != nil {
			t.Fatal("DefaultBool strict validator refused supported literal", err)
		}
		for _, typ := range []reflect.Type{reflect.TypeFor[uint8](), reflect.TypeFor[unrelatedByte]()} {
			if err := validateStoredValueWithFields(node, typ, nil); err == nil {
				t.Fatal("strict extension leaked to unrelated byte types")
			}
		}
	}
	for _, raw := range []string{"null", `"false"`, "default", "0", "[]", "{}"} {
		node := storageMetadataDocument(t, raw).Content[0]
		if err := validateStoredValueWithFields(node, reflect.TypeFor[DefaultBool](), nil); err == nil {
			t.Fatal("DefaultBool strict validator admitted malformed value", raw)
		}
	}
}
