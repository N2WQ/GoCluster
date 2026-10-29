package filter

import (
	"fmt"
	"reflect"
	"testing"
)

func FuzzLoginUpgradeDecode(f *testing.F) {
	const aliasedV5Control = "configuration: {filters: {rules: []}, settings: {path_min_observation_count: &v5 5}}\nconfiguration_version: *v5\n"
	for version := 0; version <= 4; version++ {
		raw := oldLoginFixture
		if version != 0 {
			raw = fmt.Sprintf("configuration_version: %d\n", version) + raw
		}
		f.Add([]byte(raw))
	}
	for _, raw := range []string{
		retiredV5LiteralRecord,
		aliasedV5Control,
		"configuration_version: 5\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: NEARBY}]}, settings: {}}\n",
		"configuration_version: 4\nconfiguration_version: 4\n",
		"configuration_version: null\n", "configuration_version: 7\n", "null\n",
		"configuration_version: '4'\n", "configuration_version: 4\nallow_self: null\n",
		"<<: &old {grid: FN31, allow_self: false, allmodes: true}\n",
		"<<: &old {configuration_version: 7}\n",
		"callsigns: [&call LZ3NY]\nblock_callsigns: [*call]\n",
		"recent_ips: &recursive [*recursive]\n",
	} {
		f.Add([]byte(raw))
	}
	// Local immutable defaults keep this parser target independent of process
	// configuration, disk paths, login ownership and persistence callbacks.
	defaults, err := PrepareDefaultRuleSet([]string{"CW", "RTTY"}, []string{"HUMAN"})
	if err != nil {
		f.Fatal(err)
	}
	want := defaults.Definitions()
	const fuzzRawLimit = 64 << 10
	const fuzzWorkLimit = 256 << 10
	// Corpus entries alone cannot detect a decoder that rejects every new old-
	// generation shape. These controls must reach the actual reset decoder before
	// fuzzing begins, with independently stated retained values and default rules.
	for _, control := range []struct {
		raw      string
		settings SettingsConfiguration
	}{
		{retiredV5LiteralRecord, SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "SLOW", PathMinObservationCount: 42, SolarSummaryMinutes: 15}},
		{aliasedV5Control, SettingsConfiguration{PathMinObservationCount: 5}},
	} {
		node, state, err := parseUpgradeDocument([]byte(control.raw), fuzzRawLimit, fuzzWorkLimit)
		if err != nil {
			f.Fatal("positive retired-v5 corpus could not parse", err)
		}
		version, err := upgradeStoredVersion(node)
		if err != nil || version != 5 {
			f.Fatal("positive retired-v5 corpus lost its historical version", version, err)
		}
		record, err := decodeRetiredLogin(node, version, state, defaults)
		if err != nil || record == nil {
			f.Fatal("positive retired-v5 corpus was rejected", err)
		}
		if record.ConfigurationVersion != 6 || record.Preset != nil || record.PreparedRules() != defaults ||
			!reflect.DeepEqual(record.PreparedRules().Definitions(), want) || recordConfiguration(record).Settings != control.settings {
			f.Fatal("positive retired-v5 corpus lost reset defaults or retained settings")
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzRawLimit {
			return
		}
		node, state, err := parseUpgradeDocument(data, fuzzRawLimit, fuzzWorkLimit)
		if err != nil {
			return
		}
		version, err := upgradeStoredVersion(node)
		if err != nil || version >= CurrentConfigurationVersion {
			return
		}
		record, err := decodeRetiredLogin(node, version, state, defaults)
		if err != nil {
			return
		}
		if record.Preset != nil ||
			!reflect.DeepEqual(record.PreparedRules().Definitions(), want) {
			t.Fatal("accepted old profile restored retired data or altered prepared defaults")
		}
		stored, err := V6UserRecordFromRuntime(record)
		if err != nil {
			t.Fatal("accepted old profile cannot bind to v6", err)
		}
		encoded, err := EncodeV6UserRecord(stored)
		if err != nil {
			t.Fatal("accepted bounded old profile cannot encode canonical v6", err)
		}
		decoded, err := DecodeV6UserRecord(encoded)
		if err != nil || decoded.ConfigurationVersion != CurrentConfigurationVersion ||
			!decoded.Configuration.Equal(stored.Configuration) {
			t.Fatal("accepted old profile cannot round-trip strict v6", err)
		}
	})
}
