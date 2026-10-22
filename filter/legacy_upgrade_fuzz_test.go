package filter

import (
	"fmt"
	"reflect"
	"testing"
)

func FuzzLoginUpgradeDecode(f *testing.F) {
	for version := 0; version <= 4; version++ {
		raw := oldLoginFixture
		if version != 0 {
			raw = fmt.Sprintf("configuration_version: %d\n", version) + raw
		}
		f.Add([]byte(raw))
	}
	for _, raw := range []string{
		"configuration_version: 4\nconfiguration_version: 4\n",
		"configuration_version: null\n", "configuration_version: 6\n", "null\n",
		"configuration_version: '4'\n", "configuration_version: 4\nallow_self: null\n",
		"<<: &old {grid: FN31, allow_self: false, allmodes: true}\n",
		"<<: &old {configuration_version: 6}\n",
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
		record, err := decodeLegacyLogin(node, version, state, defaults)
		if err != nil {
			return
		}
		if record.Preset != nil || record.NearbyEnabled ||
			!reflect.DeepEqual(record.PreparedRules().Definitions(), want) {
			t.Fatal("accepted old profile restored retired data or altered prepared defaults")
		}
		stored, err := V5UserRecordFromRuntime(record)
		if err != nil {
			t.Fatal("accepted old profile cannot bind to v5", err)
		}
		encoded, err := EncodeV5UserRecord(stored)
		if err != nil {
			t.Fatal("accepted bounded old profile cannot encode canonical v5", err)
		}
		decoded, err := DecodeV5UserRecord(encoded)
		if err != nil || decoded.ConfigurationVersion != CurrentConfigurationVersion ||
			!decoded.Configuration.Equal(stored.Configuration) {
			t.Fatal("accepted old profile cannot round-trip strict v5", err)
		}
	})
}
