package filter

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLegacyMergedStateFieldsProtected(t *testing.T) {
	usePresetTestDir(t)
	for _, field := range []string{"dxstates", "blockdxstates", "destates", "blockdestates", "alldxstates", "blockalldxstates", "alldestates", "blockalldestates"} {
		value := "{ZZ: false}"
		if strings.Contains(field, "all") {
			value = "false"
		}
		entry := field + ": " + value
		for name, raw := range map[string]string{
			"mapping":  "<<: {" + entry + "}\nbands: {20m: true}\n",
			"alias":    "legacy: &legacy {" + entry + "}\n<<: *legacy\n",
			"sequence": "legacy: &legacy {" + entry + "}\n<<: [{bands: {20m: true}}, *legacy]\n",
			"nested":   "legacy: &legacy {" + entry + "}\n<<: {<<: *legacy}\n",
		} {
			t.Run(field+"/"+name, func(t *testing.T) {
				assertStateStorageProtected(t, raw)
			})
		}
	}
	for name, raw := range map[string]string{
		"aliased field key":    "key: &key dxstates\n? *key\n: {CA: false}\n",
		"aliased state map":    "states: &states {CA: false}\n<<: {dxstates: *states}\n",
		"merged future marker": "<<: {configuration_version: 4, bands: {20m: true}}\n",
		"malformed merge":      "<<: [42]\n",
	} {
		t.Run(name, func(t *testing.T) { assertStateStorageProtected(t, raw) })
	}
}

func assertStateStorageProtected(t *testing.T, raw string) {
	t.Helper()
	path := userRecordPath("N2WQ-1")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ-1"); err == nil {
		t.Fatalf("admitted invalid stored state: %s", raw)
	}
	if err := SaveConfiguration("N2WQ-1", ConfigurationFromFilter(NewFilter(), SettingsConfiguration{}), nil, nil); err == nil {
		t.Fatal("write replaced protected record")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != raw {
		t.Fatalf("protected bytes changed: %v", err)
	}
}

func TestLegacyOrdinaryMergesRemainAccepted(t *testing.T) {
	usePresetTestDir(t)
	for name, raw := range map[string]string{
		"mapping":                  "<<: {bands: {20m: true}, allbands: false}\n",
		"alias":                    "legacy: &legacy {bands: {20m: true}, allbands: false}\n<<: *legacy\n",
		"sequence":                 "legacy: &legacy {bands: {20m: true}}\n<<: [*legacy, {allbands: false}]\n",
		"nested":                   "legacy: &legacy {bands: {20m: true}, allbands: false}\n<<: {<<: *legacy}\n",
		"unused state anchor":      "legacy: &legacy {dxstates: {ZZ: false}}\nbands: {20m: true}\nallbands: false\n",
		"quoted merge-looking key": "\"<<\": {dxstates: {ZZ: false}}\nbands: {20m: true}\nallbands: false\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := userRecordPath("N2WQ-1")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			record, err := LoadUserRecord("N2WQ-1")
			if err != nil || record == nil || !record.Bands["20m"] || record.AllBands || !record.AllDXStates || !record.AllDEStates {
				t.Fatalf("legacy merge migration changed: %+v %v", record, err)
			}
			got, err := os.ReadFile(path)
			if err != nil || string(got) != raw {
				t.Fatalf("read rewrote legacy bytes: %v", err)
			}
		})
	}
}

func TestStatePrecheckAliasesAndCardinality(t *testing.T) {
	var entries strings.Builder
	for i := 0; i <= maxStateRuleEntries; i++ {
		fmt.Fprintf(&entries, "K%d: false, ", i)
	}
	for _, raw := range []string{
		"configuration_version: 2\ndxstates: &states {" + entries.String() + "}\ndestates: *states\n",
		"configuration_version: 2\n<<: {dxstates: &states {" + entries.String() + "}}\ndestates: *states\n",
	} {
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(raw), &document); err != nil {
			t.Fatal(err)
		}
		if err := validateStoredStateFields(document.Content[0], 2); err == nil || !strings.Contains(err.Error(), "bounded state map") {
			t.Fatalf("map bound was not checked before keys/decode: %v", err)
		}
	}
	var document yaml.Node
	if err := yaml.Unmarshal([]byte("&legacy\n<<: *legacy\n"), &document); err != nil {
		t.Fatal(err)
	}
	if err := validateStoredStateFields(document.Content[0], 0); err != nil {
		t.Fatal(err)
	}
	var record UserRecord
	if err := document.Content[0].Decode(&record); err == nil {
		t.Fatal("recursive merge was admitted")
	}
}

func TestStateStoredVocabularyAtBound(t *testing.T) {
	usePresetTestDir(t)
	var entries strings.Builder
	// Literal oracle proves the stored-map bound includes inactive province keys.
	for _, code := range strings.Fields("AA AB AE AK AL AP AR AS AZ BC CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA MA MB MD ME MI MN MO MP MS MT NB NC ND NE NH NJ NL NM NS NT NU NV NY OH OK ON OR PA PE PR QC RI SC SD SK TN TX UM UT VA VI VT WA WI WV WY YT") {
		fmt.Fprintf(&entries, "%s: false, ", code)
	}
	raw := "configuration_version: 2\n"
	for _, field := range []string{"dxstates", "blockdxstates", "destates", "blockdestates"} {
		raw += field + ": {" + entries.String() + "}\n"
	}
	path := userRecordPath("N2WQ-1")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := LoadUserRecord("N2WQ-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, states := range []map[string]bool{record.DXStates, record.BlockDXStates, record.DEStates, record.BlockDEStates} {
		if len(states) != 73 {
			t.Fatalf("stored vocabulary lost inactive entries: %v", states)
		}
		if value, exists := states["YT"]; !exists || value {
			t.Fatal("stored territory false entry changed")
		}
	}
	if err := SaveUserRecord("N2WQ-1", record); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ-1"); err != nil {
		t.Fatalf("full mixed vocabulary no longer readable after save: %v", err)
	}
}

func TestLegacyAliasedMergeLookingKeyMatchesYAMLDecoder(t *testing.T) {
	raw := "key: &key <<\n? *key\n: {dxstates: {ZZ: false}}\nbands: {20m: true}\nallbands: false\n"
	// The plain decoder is the compatibility oracle: YAML merge recognition
	// requires an actual scalar key before resolving aliases used as field names.
	type plainRecord UserRecord
	var plain plainRecord
	if err := yaml.Unmarshal([]byte(raw), &plain); err != nil {
		t.Fatal(err)
	}
	if !plain.Bands["20m"] || plain.AllBands || len(plain.DXStates) != 0 {
		t.Fatalf("unexpected plain YAML decoder behavior: %+v", plain)
	}
	var record UserRecord
	if err := yaml.Unmarshal([]byte(raw), &record); err != nil {
		t.Fatal(err)
	}
	if !record.Bands["20m"] || record.AllBands || !record.AllDXStates || !record.AllDEStates {
		t.Fatalf("legacy compatibility differs from YAML decoder: %+v", record)
	}
}
