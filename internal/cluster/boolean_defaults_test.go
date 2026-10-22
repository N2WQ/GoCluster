package cluster

import (
	"strings"
	"testing"

	"dxcluster/config"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestTaxonomyAdmissionPreparesBooleanDefaults(t *testing.T) {
	t.Cleanup(func() {
		if err := filter.ConfigureDefaultRules(nil, nil); err != nil {
			t.Fatal(err)
		}
	})
	cfg := &config.Config{Filter: config.FilterConfig{
		DefaultModes: []string{"CW", "FT8", "CW"}, DefaultSources: []string{"HUMAN"},
	}}
	r := newClusterRuntime(BuildInfo{}, cfg, "", config.LoadDiagnostics{})
	if !r.validateTaxonomyReferences() {
		t.Fatalf("valid defaults refused before listen: %v", r.startupErr)
	}
	f := filter.NewFilter()
	if f.PreparedRules().Counts().Rules != 1 || !f.Matches(spot.NewSpot("K1ABC", "W1ABC", 14074, "FT8")) {
		t.Fatal("runtime consumer did not use the admitted configured modes")
	}
	skimmer := spot.NewSpot("K1ABC", "W1ABC", 14074, "FT8")
	skimmer.IsHuman = false
	if f.Matches(skimmer) {
		t.Fatal("configured HUMAN default source was not included in rule zero")
	}
	before := f.PreparedRules().Definitions()
	for _, sources := range [][]string{{"INVALID"}, {"ALL", "INVALID"}, {strings.Repeat("H", 1025)}} {
		cfg.Filter.DefaultSources = sources
		if r.validateTaxonomyReferences() || r.startupErr == nil ||
			!strings.Contains(r.startupErr.Error(), "default Boolean rule") {
			t.Fatalf("invalid defaults did not visibly stop pre-listen admission: %v", r.startupErr)
		}
		after := filter.NewFilter().PreparedRules().Definitions()
		if len(after) != len(before) || after[0] != before[0] {
			t.Fatal("failed default admission replaced the last admitted default")
		}
	}
}
