package filter

import (
	"testing"

	"dxcluster/spot"
)

func TestCCGeographyIndependentCategoriesAndLists(t *testing.T) {
	cfg := ccTestBase()
	cfg.Geography = map[string]CCGeography{
		"DOC":     {Global: CCCategory{Action: CCPass, Countries: []int{291, 1}}},
		"DOS":     {Global: CCCategory{Action: CCPass, States: []string{"CA", "TX"}}},
		"DXCTY":   {Global: CCCategory{Action: CCPass, Countries: []int{230, 248}}},
		"DXSTATE": {Global: CCCategory{Action: CCPass, States: []string{"ON", "QC"}}},
	}
	_, program, err := PrepareCCConfiguration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		name string
		edit func(*spot.Spot)
		want bool
	}{
		{"all-first-values", func(*spot.Spot) {}, true},
		{"all-second-values", func(s *spot.Spot) {
			s.DEMetadata.ADIF, s.DEMetadata.State, s.DXMetadata.ADIF, s.DXMetadata.State = 1, "TX", 248, "QC"
		}, true},
		{"doc-alone-fails", func(s *spot.Spot) { s.DEMetadata.ADIF = 999 }, false},
		{"dos-alone-fails", func(s *spot.Spot) { s.DEMetadata.State = "NY" }, false},
		{"dxcty-alone-fails", func(s *spot.Spot) { s.DXMetadata.ADIF = 999 }, false},
		{"dxstate-alone-fails", func(s *spot.Spot) { s.DXMetadata.State = "AB" }, false},
		{"de-unknown", func(s *spot.Spot) { s.DEMetadata.ADIF = 0 }, false},
		{"dx-unknown", func(s *spot.Spot) { s.DXMetadata.State = "" }, false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			sp := ccTestSpot()
			probe.edit(sp)
			if actual := program.Matches(sp, EvaluationContext{}); actual != probe.want {
				t.Fatalf("independent literal expectation: got %t want %t", actual, probe.want)
			}
		})
	}
}

func TestCCCategoryUnknownEmptyAndStateMetadataContract(t *testing.T) {
	for _, family := range []string{"DOC", "DOS", "DXCTY", "DXSTATE"} {
		for _, action := range []CCAction{CCPass, CCReject} {
			selection := CCCategory{Action: action}
			country := family == "DOC" || family == "DXCTY"
			if country {
				selection.Countries = []int{291}
			} else {
				selection.States = []string{"CA"}
			}
			cfg := ccTestBase()
			cfg.Geography = map[string]CCGeography{family: {Global: selection}}
			_, program, err := PrepareCCConfiguration(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, probe := range []struct {
				adif  int
				state string
				match bool
			}{
				{291, "CA", true}, {230, "ON", false}, {0, "", false}, {-1, "ZZ", false},
				{0, "CA", !country}, {291, "ca", country},
			} {
				sp := ccTestSpot()
				metadata := &sp.DEMetadata
				if family == "DXCTY" || family == "DXSTATE" {
					metadata = &sp.DXMetadata
				}
				metadata.ADIF, metadata.State = probe.adif, probe.state
				want := probe.match
				if action == CCReject {
					want = !want
				}
				if actual := program.Matches(sp, EvaluationContext{}); actual != want {
					t.Fatalf("%s %s metadata=%+v: got %t want %t", family, action, probe, actual, want)
				}
			}
			cfg.Geography[family] = CCGeography{Global: CCCategory{Action: action}}
			_, empty, err := PrepareCCConfiguration(cfg)
			if err != nil || empty.Matches(ccTestSpot(), EvaluationContext{}) != (action == CCReject) {
				t.Fatalf("%s explicit empty %s list: %v", family, action, err)
			}
		}
	}
}

func TestCCBandOverrideAndGlobalReplacement(t *testing.T) {
	initial := ccTestBase()
	initial.Geography = map[string]CCGeography{"DOC": {
		Global: CCCategory{Action: CCPass, Countries: []int{291}},
		Bands:  map[string]CCCategory{"20": {Action: CCPass, Countries: []int{230}}, "4": {Action: CCOff}},
	}}
	canonical, previous, err := PrepareCCConfiguration(initial)
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		frequency float64
		adif      int
		want      bool
	}{
		{14025, 230, true}, {14025, 291, false}, {7020, 291, true}, {7020, 230, false},
		{70001, 0, true}, {1000, 291, true}, {1000, 230, false},
	} {
		sp := ccTestSpot()
		sp.Frequency, sp.DEMetadata.ADIF = probe.frequency, probe.adif
		sp.Band, sp.BandNorm = "20m", "20m"
		if actual := previous.Matches(sp, EvaluationContext{}); actual != probe.want {
			t.Fatalf("native per-band override at %+v: %t", probe, actual)
		}
	}
	replaced, err := canonical.WithGeography("DOC", "", CCCategory{Action: CCPass, Countries: []int{291}})
	if err != nil {
		t.Fatal(err)
	}
	if len(replaced.Geography["DOC"].Bands) != 0 || replaced.Equal(canonical) || replaced.Fingerprint() == canonical.Fingerprint() {
		t.Fatal("equal global list incorrectly retained per-band overrides")
	}
	_, current, err := PrepareCCConfiguration(replaced)
	if err != nil {
		t.Fatal(err)
	}
	sp := ccTestSpot()
	sp.DEMetadata.ADIF = 230
	if current.Matches(sp, EvaluationContext{}) || !previous.Matches(sp, EvaluationContext{}) || len(canonical.Geography["DOC"].Bands) != 2 {
		t.Fatal("global edit lost snapshot isolation or failed to replace every band row")
	}
	cleared, err := replaced.WithGeography("DOC", "", CCCategory{Action: CCOff})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Geography["DOC"].Global.Action != CCOff || len(cleared.Geography["DOC"].Bands) != 0 || !slicesCCStringsEqual(cleared.Modes, replaced.Modes) {
		t.Fatal("category OFF changed another selection")
	}
}

func slicesCCStringsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func TestCCModeAndSourceEditsAreIndependent(t *testing.T) {
	cfg := CCConfiguration{Modes: []string{"CW"}, Sources: []string{"HUMAN", "SKIMMER"}}
	without, err := cfg.WithMode("CW", false)
	if err != nil {
		t.Fatal(err)
	}
	_, deny, err := PrepareCCConfiguration(without)
	if err != nil || deny.Matches(ccTestSpot(), EvaluationContext{}) || len(without.Sources) != 2 {
		t.Fatalf("mode edit changed sources or empty means defaults: %v", err)
	}
	restored, err := without.WithMode("cw", true)
	if err != nil {
		t.Fatal(err)
	}
	_, both, err := PrepareCCConfiguration(restored)
	if err != nil {
		t.Fatal(err)
	}
	sp := ccTestSpot()
	for _, human := range []bool{false, true} {
		sp.IsHuman = human
		if !both.Matches(sp, EvaluationContext{}) {
			t.Fatalf("terminal global mode selection did not cover source human=%t", human)
		}
	}
	humanOnly, err := restored.WithSource("SKIMMER", false)
	if err != nil {
		t.Fatal(err)
	}
	_, selected, err := PrepareCCConfiguration(humanOnly)
	if err != nil {
		t.Fatal(err)
	}
	sp.IsHuman = false
	if selected.Matches(sp, EvaluationContext{}) || !slicesCCStringsEqual(humanOnly.Modes, restored.Modes) {
		t.Fatal("source edit changed modes or retained disabled source")
	}
	sp.IsHuman = true
	if !selected.Matches(sp, EvaluationContext{}) {
		t.Fatal("unrelated selected source was lost")
	}
}
