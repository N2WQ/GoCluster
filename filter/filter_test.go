package filter

import (
	"slices"
	"sync"
	"testing"

	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func requireH3Mappings(t *testing.T) {
	t.Helper()
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatal(err)
	}
}

func nativeRuleFilter(t testing.TB, definitions ...RuleDefinition) *Filter {
	t.Helper()
	_, value, err := PrepareV5Configuration(V5Configuration{
		Filters: V5FilterConfiguration{Rules: definitions},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &value
}

func TestNativeBooleanMatching(t *testing.T) {
	cases := []struct {
		expression string
		value      spot.Spot
		path       string
		want       bool
	}{
		{"BAND=20m", spot.Spot{BandNorm: "20M"}, "", true},
		{"BAND=20m", spot.Spot{Band: "40m"}, "", false},
		{"MODE=PSK", spot.Spot{Mode: "PSK31"}, "", true},
		{"MODE=FT2", spot.Spot{Mode: "FT2"}, "", true},
		{"MODE=UNKNOWN", spot.Spot{}, "", true},
		{"DECALL=W1XYZ", spot.Spot{DECall: "W1XYZ-1", DECallStripped: "W1XYZ"}, "", true},
		{"DXCONT=EU", spot.Spot{DXMetadata: spot.CallMetadata{Continent: "NA"}}, "", false},
		{"DXCONT=EU", spot.Spot{}, "", false},
		{"DXZONE=14", spot.Spot{DXMetadata: spot.CallMetadata{CQZone: 14}}, "", true},
		{"DXDXCC=291 AND DEDXCC=110", spot.Spot{DXMetadata: spot.CallMetadata{ADIF: 291}, DEMetadata: spot.CallMetadata{ADIF: 110}}, "", true},
		{"DXGRID2=FN", spot.Spot{DXMetadata: spot.CallMetadata{Grid: "FN31"}}, "", true},
		{"SOURCE=HUMAN", spot.Spot{IsHuman: true}, "", true},
		{"EVENT=NONE,POTA", spot.Spot{}, "", true},
		{"EVENT=POTA", spot.Spot{}, "", false},
		{"CONF=?", spot.Spot{Mode: "FT8", Confidence: "V"}, "", false},
		{"PATH=UNLIKELY", spot.Spot{}, PathClassClosed, false},
		{"PATH=CLOSED", spot.Spot{}, PathClassClosed, true},
		{"PATH=INSUFFICIENT", spot.Spot{}, "", true},
	}
	for i := range cases {
		test := &cases[i]
		test.value.IsHuman = true
		t.Run(test.expression+"/"+test.path, func(t *testing.T) {
			f := nativeRuleFilter(t, RuleDefinition{0, RulePass, test.expression})
			if got := f.MatchesWithPath(&test.value, test.path); got != test.want {
				t.Fatalf("native matcher=%v, want %v", got, test.want)
			}
		})
	}
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "DXCALL=K1*"}, RuleDefinition{9, RuleReject, "DXCALL=K1ABC"})
	if f.Matches(&spot.Spot{DXCall: "K1ABC", IsHuman: true}) || !f.Matches(&spot.Spot{DXCall: "K1XYZ", IsHuman: true}) {
		t.Fatal("native delivery lost reject precedence")
	}
}

func TestNativeMatcherUsesOnlyPreparedGeneration(t *testing.T) {
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "MODE=CW"})
	value := &spot.Spot{Mode: "CW", DXCall: "K1ABC", DECall: "N2ABC", HasReport: true, Comment: "POTA", IsHuman: true}
	if !f.Matches(value) || f.PathFilterActive() {
		t.Fatal("unconfigured fields affected native Boolean truth")
	}
	f.preparedRules = mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=FT8"})
	if f.Matches(value) {
		t.Fatal("replacement generation did not control matching")
	}
}

func TestConfiguredDefaultRulesPublishAfterValidation(t *testing.T) {
	previous := configuredDefaultRules.Load()
	t.Cleanup(func() { configuredDefaultRules.Store(previous) })
	if err := ConfigureDefaultRules([]string{"CW"}, []string{"HUMAN"}); err != nil {
		t.Fatal(err)
	}
	first := NewFilter()
	want := []RuleDefinition{{0, RulePass, "MODE=CW AND SOURCE=HUMAN"}}
	if !slices.Equal(first.PreparedRules().Definitions(), want) ||
		!first.Matches(&spot.Spot{Mode: "CW", IsHuman: true}) ||
		first.Matches(&spot.Spot{Mode: "CW"}) || first.Matches(&spot.Spot{Mode: "FT8", IsHuman: true}) {
		t.Fatal("new-user defaults are not ordinary visible PASS slot zero")
	}
	for _, sources := range [][]string{{"invalid"}, {"ALL", "invalid"}} {
		if err := ConfigureDefaultRules([]string{"CW"}, sources); err == nil ||
			NewFilter().PreparedRules() != first.PreparedRules() {
			t.Fatal("invalid defaults changed the published generation")
		}
	}
	if err := ConfigureDefaultRules([]string{"invalid"}, nil); err == nil {
		t.Fatal("invalid mode silently defaulted")
	}
	if err := ConfigureDefaultRules([]string{"FT8"}, nil); err != nil {
		t.Fatal(err)
	}
	if NewFilter().PreparedRules() == first.PreparedRules() ||
		!first.Matches(&spot.Spot{Mode: "CW", IsHuman: true}) {
		t.Fatal("publishing defaults mutated existing filters")
	}
	empty := nativeRuleFilter(t)
	if empty.PreparedRules() != nil || !empty.Matches(&spot.Spot{Mode: "USB"}) {
		t.Fatal("restored-empty installed configured defaults")
	}
}

func TestMatchSnapshotDetachedAndNilClosed(t *testing.T) {
	var absent *Filter
	if absent.MatchSnapshot().MatchesWithPath(&spot.Spot{}, "") ||
		absent.Matches(&spot.Spot{}) || absent.MatchSnapshot().SelfEnabled() ||
		absent.MatchSnapshot().AllowsToxicity(&spot.Spot{}) {
		t.Fatal("nil filter snapshot did not fail closed")
	}
	if !(&Filter{}).MatchSnapshot().MatchesWithPath(&spot.Spot{}, "") {
		t.Fatal("nonnull unrestricted filter confused with nil")
	}
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "MODE=CW AND PATH=HIGH"})
	allow := true
	f.IncludeBeacons, f.AllowSelf, f.AllowToxic = &allow, &allow, &allow
	snapshot := f.MatchSnapshot()
	f.preparedRules = nil
	f.NearbyEnabled, f.NearbyUserFine, f.NearbyUserCoarse = true, 10, 11
	allow = false
	value := &spot.Spot{Mode: "CW", IsBeacon: true, ToxicityStatus: spot.ToxicityToxic, IsHuman: true}
	if !snapshot.MatchesWithPath(value, PathClassHigh) || !snapshot.PathFilterActive() ||
		!snapshot.SelfEnabled() || !snapshot.AllowsToxicity(value) {
		t.Fatal("snapshot borrowed mutable controls/rules/NEARBY state")
	}
	if snapshot.MatchesWithPath(value, PathClassLow) || f.MatchesWithPath(value, PathClassHigh) {
		t.Fatal("snapshot and later filter generations were not independent")
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if !snapshot.MatchesWithPath(value, PathClassHigh) {
			panic("snapshot truth changed")
		}
	}); allocations != 0 {
		t.Fatalf("warm snapshot matcher allocated: %g", allocations)
	}
}

func TestConfigureDefaultRulesConcurrentPublication(t *testing.T) {
	previous := configuredDefaultRules.Load()
	t.Cleanup(func() { configuredDefaultRules.Store(previous) })
	if err := ConfigureDefaultRules([]string{"CW"}, nil); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	failures := make(chan string, 4)
	for worker := range 4 {
		workers.Go(func() {
			for i := range 50 {
				mode := "CW"
				if (worker+i)%2 != 0 {
					mode = "FT8"
				}
				if err := ConfigureDefaultRules([]string{mode}, nil); err != nil {
					failures <- err.Error()
					return
				}
				f := NewFilter()
				cw := f.Matches(&spot.Spot{Mode: "CW"})
				ft8 := f.Matches(&spot.Spot{Mode: "FT8"})
				if cw == ft8 || f.PreparedRules() == nil {
					failures <- "reader observed a partial/unrestricted default generation"
					return
				}
			}
		})
	}
	workers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

func TestNearbyNativeProjectionAndSnapshotCells(t *testing.T) {
	requireH3Mappings(t)
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "DXCONT=EU AND MODE=CW"})
	fine, coarse := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
	if err := f.EnableNearby(fine, coarse); err != nil {
		t.Fatal(err)
	}
	snapshot := f.MatchSnapshot()
	f.UpdateNearbyUserCells(pathreliability.EncodeCell("EM10"), pathreliability.EncodeCoarseCell("EM10"))
	s := spot.NewSpotNormalized("K1ABC", "N2ABC", 14025, "CW")
	s.IsHuman = true
	s.DXMetadata = spot.CallMetadata{Grid: "FN31", Continent: "NA"}
	if !snapshot.MatchesWithPath(s, "") || f.Matches(s) {
		t.Fatal("NEARBY geometry or detached cells changed")
	}
	s.Mode, s.ModeNorm = "FT8", "FT8"
	if snapshot.MatchesWithPath(s, "") {
		t.Fatal("NEARBY bypassed nongeographic rules")
	}
}
