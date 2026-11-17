package filter

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"dxcluster/spot"
)

// These literal vectors come from the approved contract, not the production
// catalog/partition. The legacy oracle uses the existing manual transcription.
var metaBandIntervals = map[string][][2]float64{
	"WARC":      {{10100, 10150}, {18068, 18168}, {24890, 24990}},
	"HFCONTEST": {{1800, 2000}, {3500, 4000}, {7000, 7300}, {14000, 14350}, {21000, 21450}, {28000, 29700}},
	"HF":        {{1800, 2000}, {3500, 4000}, {5260, 5405}, {7000, 7300}, {10100, 10150}, {14000, 14350}, {18068, 18168}, {21000, 21450}, {24890, 24990}, {28000, 29700}},
	"VHF":       {{50000, 54000}, {70000, 70650}, {144000, 148000}, {220000, 225000}},
	"UHF":       {{420000, 450000}, {902000, 928000}, {1240000, 1300000}, {2300000, 2310000}, {2390000, 2450000}},
	"VHFUP":     {{50000, 54000}, {70000, 70650}, {144000, 148000}, {220000, 225000}, {420000, 450000}, {902000, 928000}, {1240000, 1300000}, {2300000, 2310000}, {2390000, 2450000}},
}

type metaBandEdit struct {
	action    CCAction
	selectors []string
}

func metaOracleCovers(selector string, frequency float64) bool {
	if frequency <= 0 || math.IsNaN(frequency) || math.IsInf(frequency, 0) {
		return false
	}
	if intervals, exists := metaBandIntervals[strings.ToUpper(selector)]; exists {
		for _, interval := range intervals {
			if interval[0] <= frequency && frequency <= interval[1] {
				return true
			}
		}
		return false
	}
	for index, slot := range ccSlotGolden {
		upper := frequency <= slot.HighKHz
		if index+1 < len(ccSlotGolden) && ccSlotGolden[index+1].LowKHz == slot.HighKHz {
			upper = frequency < slot.HighKHz
		}
		if frequency >= slot.LowKHz && upper {
			return selector == slot.Name || selector == slot.Band
		}
	}
	return false
}

func metaOracleDecision(edits []metaBandEdit, frequency float64) bool {
	active, result := false, true
	for _, edit := range edits {
		if edit.action == CCOff {
			active, result = false, true
			continue
		}
		if !active {
			active = true
			result = edit.action == CCReject
		}
		for _, selector := range edit.selectors {
			if metaOracleCovers(selector, frequency) {
				result = edit.action == CCPass
			}
		}
	}
	return result
}

func metaBoundaryProbes() []float64 {
	result := []float64{0, -1, math.NaN(), math.Inf(1), 1000, 500000, 2400000, 2350000, 3500000, 47000001}
	for _, intervals := range metaBandIntervals {
		for _, interval := range intervals {
			result = append(result, (interval[0]+interval[1])/2)
			for _, endpoint := range interval {
				result = append(result, math.Nextafter(endpoint, math.Inf(-1)), endpoint, math.Nextafter(endpoint, math.Inf(1)))
			}
		}
	}
	for _, slot := range ccSlotGolden {
		for _, endpoint := range []float64{slot.LowKHz, slot.HighKHz} {
			result = append(result, math.Nextafter(endpoint, math.Inf(-1)), endpoint, math.Nextafter(endpoint, math.Inf(1)))
		}
	}
	return result
}

func TestMetaBandCCCoverageAndBoundaries(t *testing.T) {
	for _, name := range []string{"WARC", "HFCONTEST", "HF", "VHF", "UHF", "VHFUP"} {
		for _, action := range []CCAction{CCPass, CCReject} {
			t.Run(name+"/"+string(action), func(t *testing.T) {
				edit := metaBandEdit{action, []string{name}}
				config, err := ccTestBase().WithDXBM(action, edit.selectors)
				if err != nil {
					t.Fatal(err)
				}
				_, program, err := PrepareCCConfiguration(config)
				if err != nil {
					t.Fatal(err)
				}
				for _, frequency := range metaBoundaryProbes() {
					sp := ccTestSpot()
					sp.Frequency, sp.Band, sp.BandNorm = frequency, "20m", "20m"
					want := metaOracleDecision([]metaBandEdit{edit}, frequency)
					if got := program.Matches(sp, EvaluationContext{}); got != want {
						t.Fatalf("%s %s frequency %.17g: got %t want %t", action, name, frequency, got, want)
					}
				}
			})
		}
	}
}

func TestMetaBandGOCCPhysicalAgreement(t *testing.T) {
	for _, name := range []string{"WARC", "HFCONTEST", "HF", "VHF", "UHF", "VHFUP"} {
		goRules, err := PrepareRuleSet([]RuleDefinition{{Slot: 0, Action: RulePass, Expr: "BAND=" + name}})
		if err != nil {
			t.Fatal(err)
		}
		config, err := ccTestBase().WithDXBM(CCPass, []string{name})
		if err != nil {
			t.Fatal(err)
		}
		_, ccProgram, err := PrepareCCConfiguration(config)
		if err != nil {
			t.Fatal(err)
		}
		for _, frequency := range metaBoundaryProbes() {
			// Keep adjacent floating-point probes exact; NewSpot deliberately
			// rounds ingestion frequencies to 10Hz before it classifies them.
			sp := ccTestSpot()
			sp.Frequency, sp.Band = frequency, spot.FreqToBand(frequency)
			sp.BandNorm = spot.NormalizeBand(sp.Band)
			want := metaOracleCovers(name, frequency)
			if goRules.Matches(sp, EvaluationContext{}) != want || ccProgram.Matches(sp, EvaluationContext{}) != want {
				t.Fatalf("%s at %.17g: GO/CC differ from physical range contract", name, frequency)
			}
		}
	}
}

func TestMetaBandCCEditSequencesAndOwnership(t *testing.T) {
	edits := []metaBandEdit{
		{CCPass, []string{"20-CW"}}, {CCReject, []string{"HF"}},
		{CCPass, []string{"UHF"}}, {CCReject, []string{"MW"}},
		{CCPass, []string{"VHF"}}, {CCReject, []string{"1-SSB"}},
		{CCPass, []string{"HF", "UHF"}}, {CCReject, []string{"20-RTTY", "WARC"}},
		{CCOff, nil}, {CCReject, []string{"HFCONTEST"}},
		{CCPass, []string{"WARC", "6"}}, {CCReject, []string{"VHFUP"}},
		{CCPass, []string{"1-CW"}}, {CCPass, []string{"UHF"}},
	}
	config := ccTestBase()
	for step, edit := range edits {
		var err error
		config, err = config.WithDXBM(edit.action, edit.selectors)
		if err != nil {
			t.Fatal(err)
		}
		canonical, program, err := PrepareCCConfiguration(config)
		if err != nil {
			t.Fatal(err)
		}
		before := canonical.Fingerprint()
		exported := program.CanonicalConfig()
		exported.DXBM.Slots["20-CW"] = false
		if exported.DXBM.FrequencyOverrides != nil {
			clear(exported.DXBM.FrequencyOverrides)
		}
		for _, frequency := range metaBoundaryProbes() {
			sp := ccTestSpot()
			sp.Frequency = frequency
			if got, want := program.Matches(sp, EvaluationContext{}), metaOracleDecision(edits[:step+1], frequency); got != want {
				t.Fatalf("step %d after %s %v frequency %.17g: got %t want %t", step, edit.action, edit.selectors, frequency, got, want)
			}
		}
		if program.CanonicalConfig().Fingerprint() != before {
			t.Fatal("export mutated prepared owner")
		}
		if _, err := config.WithDXBM(CCPass, []string{"UHF", "invalid"}); err == nil || config.Fingerprint() != before {
			t.Fatal("invalid mixed edit was not atomic")
		}
	}
}

func TestMetaBandCCBoundedAdmission(t *testing.T) {
	config := ccTestBase()
	for index := range 2000 {
		action := CCPass
		if index%2 != 0 {
			action = CCReject
		}
		var err error
		config, err = config.WithDXBM(action, []string{"VHFUP"})
		if err != nil {
			t.Fatal(err)
		}
		if len(config.DXBM.FrequencyOverrides) > len(ccFrequencyCells) {
			t.Fatal("retained edit history grew")
		}
	}
	canonical, _, err := PrepareCCConfiguration(config)
	if err != nil {
		t.Fatal(err)
	}
	for name, invalid := range map[string]CCFrequencyOverrides{
		"unknown":   {"bad": true},
		"duplicate": {"P:70000": true, "p:70000": false},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := canonical.Clone()
			candidate.DXBM.FrequencyOverrides = invalid
			if _, _, err := PrepareCCConfiguration(candidate); err == nil {
				t.Fatal("invalid overrides admitted")
			}
		})
	}
	oversized := canonical.Clone()
	oversized.DXBM.FrequencyOverrides = make(CCFrequencyOverrides)
	for index := 0; index <= len(ccFrequencyCells); index++ {
		oversized.DXBM.FrequencyOverrides[fmt.Sprint(index)] = true
	}
	if _, _, err := PrepareCCConfiguration(oversized); err == nil {
		t.Fatal("overlarge raw map admitted")
	}
	off, err := canonical.WithDXBM(CCOff, nil)
	if err != nil || off.DXBM.Active || !off.DXBM.BaseAllow || len(off.DXBM.FrequencyOverrides) != 0 {
		t.Fatal("OFF retained selection")
	}
	cells := CCSupportedFrequencyCells()
	cells[0].ID = "changed"
	if slices.Equal(cells, ccFrequencyCells) {
		t.Fatal("mutable discovery snapshot")
	}
}

func TestMetaBandCCMatchingAllocs(t *testing.T) {
	config, err := ccMaximumTestConfiguration().WithDXBM(CCReject, []string{"VHFUP", "HF"})
	if err != nil {
		t.Fatal(err)
	}
	_, program, err := PrepareCCConfiguration(config)
	if err != nil {
		t.Fatal(err)
	}
	if program.Counts().FrequencyOverrides == 0 {
		t.Fatal("fixture bypasses new selection")
	}
	sp := ccMaximumTestSpot()
	sp.Frequency = 70050
	if program.Matches(sp, EvaluationContext{}) {
		t.Fatal("reject fixture incorrectly matches")
	}
	if allocs := testing.AllocsPerRun(1000, func() { ccMatchingSink = program.Matches(sp, EvaluationContext{}) }); allocs != 0 {
		t.Fatalf("matching allocated %g times", allocs)
	}
}

func TestMetaBandCCMaximumRetainedAndStoredEnvelope(t *testing.T) {
	config := ccMaximumTestConfiguration()
	config.DXBM.FrequencyOverrides = make(CCFrequencyOverrides, len(ccFrequencyCells))
	for _, cell := range ccFrequencyCells {
		config.DXBM.FrequencyOverrides[cell.ID] = !ccNativeFrequencyDecision(config.DXBM, cell.probe())
	}
	canonical, program, err := PrepareCCConfiguration(config)
	if err != nil {
		t.Fatal(err)
	}
	if program.Counts().FrequencyOverrides != len(ccFrequencyCells) {
		t.Fatal("maximum fixture contains redundant or missing cells")
	}
	full := v7TestConfiguration(t)
	full.Filters.CC = canonical
	goSet, err := V7PresetFromConfiguration(full, "go")
	if err != nil {
		t.Fatal(err)
	}
	ccSet, err := V7PresetFromConfiguration(full, "cc")
	if err != nil {
		t.Fatal(err)
	}
	record := &V7UserRecord{Configuration: full, Presets: V7PresetReferences{GO: &V7PresetReference{Name: "GO", Baseline: goSet}, CC: &V7PresetReference{Name: "CC", Baseline: ccSet}}}
	encoded, err := EncodeV7UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeV7UserRecord(encoded)
	if err != nil || !decoded.Configuration.Equal(full) || !decoded.Presets.CC.Baseline.MatchesConfiguration(full) {
		t.Fatalf("maximum profile roundtrip: %v", err)
	}
	t.Logf("fixed partition=%d cells, canonical CC=%d bytes, complete profile and both baselines=%d bytes", len(ccFrequencyCells), program.Counts().CanonicalBytes, len(encoded))
}

func BenchmarkMetaBandCCMatching(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprint(enabled), func(b *testing.B) {
			config := ccMaximumTestConfiguration()
			var err error
			if enabled {
				config, err = config.WithDXBM(CCPass, []string{"VHFUP"})
				if err != nil {
					b.Fatal(err)
				}
			}
			_, program, err := PrepareCCConfiguration(config)
			if err != nil {
				b.Fatal(err)
			}
			sp := ccMaximumTestSpot()
			sp.Frequency = 70050
			if !program.Matches(sp, EvaluationContext{}) {
				b.Fatal("benchmark must reach final geography gate")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ccMatchingSink = program.Matches(sp, EvaluationContext{})
			}
		})
	}
}

func FuzzMetaBandCCEdits(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6})
	f.Add([]byte{6, 6, 6, 6, 2, 1, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 100 {
			return
		}
		selectors := []string{"HF", "WARC", "VHF", "UHF", "VHFUP", "MW", "1-SSB", "20-CW"}
		config := ccTestBase()
		var edits []metaBandEdit
		for _, value := range data {
			action := CCPass
			if value&8 != 0 {
				action = CCReject
			}
			edit := metaBandEdit{action, []string{selectors[int(value)%len(selectors)]}}
			var err error
			config, err = config.WithDXBM(action, edit.selectors)
			if err != nil {
				t.Fatal(err)
			}
			edits = append(edits, edit)
		}
		_, program, err := PrepareCCConfiguration(config)
		if err != nil {
			t.Fatal(err)
		}
		for _, frequency := range []float64{14025, 14070, 70000, 70650, 224500, 420000, 450000, 500000, 902000, 2350000, 2390000} {
			sp := ccTestSpot()
			sp.Frequency = frequency
			if got, want := program.Matches(sp, EvaluationContext{}), metaOracleDecision(edits, frequency); got != want {
				t.Fatalf("at %g got %t want %t", frequency, got, want)
			}
		}
	})
}
