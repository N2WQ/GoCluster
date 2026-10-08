package filter

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/spot"
)

func minSNRTestSpot(mode string, report int, hasReport, human bool) *spot.Spot {
	s := spot.NewSpot("K1ABC", "W1XYZ", 14025, mode)
	s.Report, s.HasReport, s.IsHuman = report, hasReport, human
	s.EnsureNormalized()
	return s
}

func TestMinSNRMatcherBoundaries(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		minimum int
		report  int
		want    bool
	}{
		{"FT8", -10, -11, false}, {"FT8", -10, -10, true}, {"FT8", -10, -9, true},
		{"CW", 0, -1, false}, {"CW", 0, 0, true}, {"CW", 0, 1, true},
		{"RTTY", 10, 9, false}, {"RTTY", 10, 10, true}, {"RTTY", 10, 11, true},
	} {
		t.Run(fmt.Sprintf("%s/%d/%d", tc.mode, tc.minimum, tc.report), func(t *testing.T) {
			f := NewFilter()
			f.ResetModes()
			f.MinSNR = map[string]int{tc.mode: tc.minimum}
			s := minSNRTestSpot(tc.mode, tc.report, true, false)
			if f.Matches(s) != tc.want || f.MatchesWithPath(s, PathClassInsufficient) != tc.want {
				t.Fatalf("inclusive minimum %d report %d: expected %v", tc.minimum, tc.report, tc.want)
			}
			delete(f.MinSNR, tc.mode)
			if !f.Matches(s) {
				t.Fatal("missing threshold must disable this category")
			}
		})
	}
	// A selected mode cannot constrain an unrelated supported mode.
	f := NewFilter()
	f.ResetModes()
	f.MinSNR = map[string]int{"FT8": 10}
	if !f.Matches(minSNRTestSpot("CW", -100, true, false)) {
		t.Fatal("mode-local minimum constrained CW")
	}
}

func TestMinSNRExemptionsAndComposition(t *testing.T) {
	for _, tc := range []struct {
		name      string
		human     bool
		hasReport bool
		report    int
		want      bool
	}{
		{"human report", true, true, -100, true},
		{"human missing", true, false, -100, true},
		{"automated missing nonzero", false, false, -100, true},
		{"automated missing zero", false, false, 0, true},
		{"automated real zero", false, true, 0, false},
		{"automated passing", false, true, 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFilter()
			f.MinSNR = map[string]int{"CW": 10}
			s := minSNRTestSpot("CW", tc.report, tc.hasReport, tc.human)
			if f.Matches(s) != tc.want {
				t.Fatalf("human=%v present=%v report=%d: expected %v", tc.human, tc.hasReport, tc.report, tc.want)
			}
			f.SetBand("20m", false)
			if f.Matches(s) {
				t.Fatal("MINSNR exemption/passing threshold bypassed band rejection")
			}
			f.ResetBands()
			if tc.human {
				f.SetSource("HUMAN", false)
			} else {
				f.SetSource("SKIMMER", false)
			}
			if f.Matches(s) {
				t.Fatal("MINSNR exemption/passing threshold bypassed source rejection")
			}
		})
	}
}

func TestMinSNRDormantTaxonomy(t *testing.T) {
	previous := spot.CurrentTaxonomy()
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
	configure := func(modes string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "taxonomy.yaml")
		if err := os.WriteFile(path, []byte("modes:\n"+modes+"events: []\n"), 0600); err != nil {
			t.Fatal(err)
		}
		taxonomy, err := spot.LoadTaxonomyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		spot.ConfigureTaxonomy(taxonomy)
	}
	cw := "  - {name: CW, display: CW, filter_visible: true}\n"
	resurrect := "  - {name: RETURNING, display: RETURNING, filter_visible: true}\n"
	f := NewFilter()
	f.ResetModes()
	f.MinSNR = map[string]int{"RETURNING": 0}
	before := ConfigurationFromFilter(f, SettingsConfiguration{}).Clone()
	configure(cw + resurrect)
	if !IsActiveMinSNRMode("RETURNING") || f.Matches(minSNRTestSpot("RETURNING", -1, true, false)) {
		t.Fatal("canonical supported threshold did not reject")
	}
	configure(cw)
	if IsActiveMinSNRMode("RETURNING") || !f.Matches(minSNRTestSpot("RETURNING", -1, true, false)) {
		t.Fatal("removed mode remained subject to MINSNR")
	}
	if !strings.Contains(f.MinSNRSummary(), "RETURNING>=0 dB (inactive)") {
		t.Fatal("dormant threshold is not visible")
	}
	configure(cw + "  - {name: FT8, display: FT8, filter_visible: true, variants: [RETURNING]}\n")
	if IsActiveMinSNRMode("RETURNING") || !f.Matches(minSNRTestSpot("RETURNING", -1, true, false)) {
		t.Fatal("alias taxonomy redirected a saved exact threshold")
	}
	// Even a normalized spot from before the taxonomy reload cannot activate a
	// saved key that the current taxonomy has turned into an alias.
	stale := minSNRTestSpot("RETURNING", -1, true, false)
	stale.ModeNorm = "RETURNING"
	if !f.Matches(stale) {
		t.Fatal("stale normalized mode activated an alias threshold")
	}
	configure(cw + resurrect)
	if !IsActiveMinSNRMode("RETURNING") || f.Matches(minSNRTestSpot("RETURNING", -1, true, false)) {
		t.Fatal("returning canonical mode failed to reactivate")
	}
	if !before.Equal(ConfigurationFromFilter(f, SettingsConfiguration{})) {
		t.Fatal("taxonomy change rewrote stored threshold identity")
	}
	for _, key := range []string{"", "cw", " CW", "CW ", "C.W", "CW/RTTY", "Å"} {
		if ValidMinSNRModeKey(key) || IsActiveMinSNRMode(key) {
			t.Fatalf("invalid exact key accepted: %q", key)
		}
	}
	if !ValidMinSNRModeKey("NEW-MODE_123") || IsActiveMinSNRMode("NEW-MODE_123") {
		t.Fatal("valid unknown key must be dormant")
	}
}

func TestMinSNRConfigurationIdentity(t *testing.T) {
	f := NewFilter()
	f.MinSNR = map[string]int{"FT8": -10, "CW": 0, "DORMANT": 1}
	base := ConfigurationFromFilter(f, SettingsConfiguration{}).Clone()
	f.MinSNR["FT8"] = 20
	if base.Filters.MinSNR["FT8"] != -10 || base.FilterValue().MinSNR["FT8"] != -10 {
		t.Fatal("detached configuration borrowed live thresholds")
	}
	reordered := base.Clone()
	reordered.Filters.MinSNR = map[string]int{"DORMANT": 1, "CW": 0, "FT8": -10}
	if !base.Equal(reordered) || base.Fingerprint() != reordered.Fingerprint() {
		t.Fatal("threshold insertion order changed configuration identity")
	}
	for _, mutate := range []func(map[string]int){
		func(m map[string]int) { delete(m, "CW") },
		func(m map[string]int) { m["FT8"] = 10 },
		func(m map[string]int) { m["DORMANT"] = 2 },
		func(m map[string]int) { m["NEW"] = 0 },
	} {
		changed := base.Clone()
		mutate(changed.Filters.MinSNR)
		if base.Equal(changed) || base.Fingerprint() == changed.Fingerprint() {
			t.Fatal("threshold presence/value change lost from revision identity")
		}
	}
	nilMap := Configuration{}
	emptyMap := Configuration{Filters: FilterConfiguration{MinSNR: map[string]int{}}}
	if !nilMap.Equal(emptyMap) || nilMap.Fingerprint() != emptyMap.Fingerprint() {
		t.Fatal("nil and empty disabled thresholds have different identities")
	}
	set, err := base.Preset()
	if err != nil {
		t.Fatal(err)
	}
	set.MinSNR["CW"] = 100
	if base.Filters.MinSNR["CW"] != 0 {
		t.Fatal("preset borrowed configuration threshold map")
	}
	for _, reset := range []func(*Filter){(*Filter).Reset, (*Filter).ResetToDefaults, (*Filter).ResetMinSNR} {
		f.MinSNR = maps.Clone(base.Filters.MinSNR)
		reset(f)
		if len(f.MinSNR) != 0 {
			t.Fatal("reset retained active/dormant thresholds")
		}
	}
}

func TestMinSNRBoundsAndChurn(t *testing.T) {
	c := Configuration{Filters: FilterConfiguration{MinSNR: make(map[string]int)}}
	for i := 0; i < 128; i++ {
		c.Filters.MinSNR[fmt.Sprintf("MODE%d", i)] = -10
	}
	if MaxMinSNREntries != 128 || MaxMinSNRKeyBytes != 65536 || c.ValidateMinSNRRules() != nil {
		t.Fatal("literal entry cap rejected")
	}
	c.Filters.MinSNR["EXTRA"] = 0
	if c.ValidateMinSNRRules() == nil {
		t.Fatal("129 dormant entries admitted")
	}
	if _, err := c.Preset(); err == nil {
		t.Fatal("oversized threshold map was cloned into preset")
	}
	delete(c.Filters.MinSNR, "EXTRA")
	for i := 0; i < 1000; i++ {
		delete(c.Filters.MinSNR, fmt.Sprintf("MODE%d", i))
		c.Filters.MinSNR[fmt.Sprintf("MODE%d", i+128)] = i
		if len(c.Filters.MinSNR) != 128 || c.ValidateMinSNRRules() != nil {
			t.Fatalf("churn retained historical entries at iteration %d", i)
		}
	}
	c.Filters.MinSNR = map[string]int{strings.Repeat("A", 65536): 0}
	if c.ValidateMinSNRRules() != nil {
		t.Fatal("literal 65,536 raw bytes rejected")
	}
	if c.MinimumSizeFits(65536) || !c.MinimumSizeFits(65540) {
		t.Fatal("threshold keys/entry bytes omitted from lower-size preflight")
	}
	c.Filters.MinSNR[strings.Repeat("A", 65536)] = -1
	c.Filters.MinSNR["B"] = 0
	if c.ValidateMinSNRRules() == nil {
		t.Fatal("aggregate raw bytes above 65,536 admitted")
	}
	for _, key := range []string{"", "ft8", "FT8 ", "F/T8"} {
		c.Filters.MinSNR = map[string]int{key: 0}
		if c.ValidateMinSNRRules() == nil {
			t.Fatalf("invalid stored key %q admitted", key)
		}
	}
}

func BenchmarkMinSNRMatches(b *testing.B) {
	for _, tc := range []struct {
		name   string
		report int
		want   bool
	}{
		{"negative-reject", -11, false}, {"equal-pass", -10, true}, {"above-pass", -9, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			f := NewFilter()
			f.ResetModes()
			f.MinSNR = map[string]int{"FT8": -10}
			s := minSNRTestSpot("FT8", tc.report, true, false)
			if got := f.Matches(s); got != tc.want {
				b.Fatalf("invalid benchmark path: got %v want %v", got, tc.want)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if f.Matches(s) != tc.want {
					b.Fatal("matcher result changed")
				}
			}
		})
	}
}
