package filter

import (
	"os"
	"testing"
)

func assertMinSNRStorageProtected(t *testing.T, raw string) {
	t.Helper()
	path := userRecordPath("N2WQ-1")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ-1"); err == nil {
		t.Fatal("invalid native record admitted")
	}
	if err := SaveConfiguration("N2WQ-1", Configuration{}, nil, nil); err == nil {
		t.Fatal("invalid destination overwritten")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != raw {
		t.Fatal("refusal changed destination bytes")
	}
}

func TestNativeSNRStorageRules(t *testing.T) {
	usePresetTestDir(t)
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "SNR>=-10"})
	if err := SaveUserRecord("N2WQ-1", &UserRecord{Filter: *f}); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadUserRecord("N2WQ-1")
	if err != nil || !loaded.Matches(minSNRTestSpot("FT8", -10, true, true)) ||
		loaded.Matches(minSNRTestSpot("FT8", -11, true, true)) {
		t.Fatal("native persistence lost SNR rule", err)
	}
	for _, raw := range []string{
		"configuration_version: 4\nmin_snr: {CW: 0}\n",
		"min_snr: {CW: 0}\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: [], min_snr: {CW: 0}}, settings: {}}\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: 'SNR=bad'}]}, settings: {}}\n",
	} {
		assertMinSNRStorageProtected(t, raw)
	}
}
