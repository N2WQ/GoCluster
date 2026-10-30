package filter

import (
	"math"
	"slices"
	"testing"
)

// Independent transcription of CC User Manual 1.10 (October 2026), Appendix C,
// page 84. Expected slots and endpoints must not be generated from production.
var ccSlotGolden = []CCSlotDefinition{
	{Name: "160-CW", Band: "160", LowKHz: 1800, HighKHz: 1850},
	{Name: "160-SSB", Band: "160", LowKHz: 1850, HighKHz: 2000},
	{Name: "80-CW", Band: "80", LowKHz: 3500, HighKHz: 3580},
	{Name: "80-RTTY", Band: "80", LowKHz: 3580, HighKHz: 3700},
	{Name: "80-SSB", Band: "80", LowKHz: 3700, HighKHz: 4000},
	{Name: "60-SSB", Band: "60", LowKHz: 5260, HighKHz: 5405},
	{Name: "40-CW", Band: "40", LowKHz: 7000, HighKHz: 7040},
	{Name: "40-RTTY", Band: "40", LowKHz: 7040, HighKHz: 7100},
	{Name: "40-SSB", Band: "40", LowKHz: 7100, HighKHz: 7300},
	{Name: "30-CW", Band: "30", LowKHz: 10100, HighKHz: 10130},
	{Name: "30-RTTY", Band: "30", LowKHz: 10130, HighKHz: 10150},
	{Name: "20-CW", Band: "20", LowKHz: 14000, HighKHz: 14070},
	{Name: "20-RTTY", Band: "20", LowKHz: 14070, HighKHz: 14150},
	{Name: "20-SSB", Band: "20", LowKHz: 14150, HighKHz: 14350},
	{Name: "17-CW", Band: "17", LowKHz: 18068, HighKHz: 18100},
	{Name: "17-RTTY", Band: "17", LowKHz: 18100, HighKHz: 18110},
	{Name: "17-SSB", Band: "17", LowKHz: 18110, HighKHz: 18168},
	{Name: "15-CW", Band: "15", LowKHz: 21000, HighKHz: 21070},
	{Name: "15-RTTY", Band: "15", LowKHz: 21070, HighKHz: 21200},
	{Name: "15-SSB", Band: "15", LowKHz: 21200, HighKHz: 21450},
	{Name: "12-CW", Band: "12", LowKHz: 24890, HighKHz: 24920},
	{Name: "12-RTTY", Band: "12", LowKHz: 24920, HighKHz: 24930},
	{Name: "12-SSB", Band: "12", LowKHz: 24930, HighKHz: 24990},
	{Name: "10-CW", Band: "10", LowKHz: 28000, HighKHz: 28070},
	{Name: "10-RTTY", Band: "10", LowKHz: 28070, HighKHz: 28300},
	{Name: "10-SSB", Band: "10", LowKHz: 28300, HighKHz: 29700},
	{Name: "6-CW", Band: "6", LowKHz: 50000, HighKHz: 50080},
	{Name: "6-SSB", Band: "6", LowKHz: 50080, HighKHz: 50500},
	{Name: "6-FM", Band: "6", LowKHz: 50500, HighKHz: 54000},
	{Name: "4-MTR", Band: "4", LowKHz: 70000, HighKHz: 70650},
	{Name: "2-CW", Band: "2", LowKHz: 144000, HighKHz: 144100},
	{Name: "2-SSB", Band: "2", LowKHz: 144100, HighKHz: 144500},
	{Name: "2-FM", Band: "2", LowKHz: 144500, HighKHz: 148000},
	{Name: "1-CW", Band: "1", LowKHz: 220000, HighKHz: 221000},
	{Name: "1-FM", Band: "1", LowKHz: 221000, HighKHz: 222000},
	{Name: "1-SSB", Band: "1", LowKHz: 222000, HighKHz: 224000},
	{Name: "MW-MW", Band: "MW", LowKHz: 500000, HighKHz: 47000000},
}

func TestCCNativeSlotGoldenAndEndpoints(t *testing.T) {
	if actual := CCSupportedSlots(); !slices.Equal(actual, ccSlotGolden) {
		t.Fatalf("native slot table differs from independent manual transcription: %+v", actual)
	}
	wantBands := []string{"160", "80", "60", "40", "30", "20", "17", "15", "12", "10", "6", "4", "2", "1", "MW"}
	if actual := CCSupportedBands(); !slices.Equal(actual, wantBands) {
		t.Fatalf("native band vocabulary: %v", actual)
	}
	for index, golden := range ccSlotGolden {
		t.Run(golden.Name, func(t *testing.T) {
			config, err := ccTestBase().WithDXBM(CCPass, []string{golden.Name})
			if err != nil {
				t.Fatal(err)
			}
			_, program, err := PrepareCCConfiguration(config)
			if err != nil {
				t.Fatal(err)
			}
			highIncluded := index == len(ccSlotGolden)-1 || ccSlotGolden[index+1].LowKHz != golden.HighKHz
			for _, probe := range []struct {
				frequency float64
				want      bool
			}{
				{golden.LowKHz - .001, false}, {golden.LowKHz, true},
				{(golden.LowKHz + golden.HighKHz) / 2, true},
				{golden.HighKHz, highIncluded}, {golden.HighKHz + .001, false},
			} {
				sp := ccTestSpot()
				sp.Frequency, sp.Mode, sp.ModeNorm = probe.frequency, "FT8", "FT8"
				if got := program.Matches(sp, EvaluationContext{}); got != probe.want {
					t.Fatalf("frequency %v with unrelated decoded FT8: got %t want %t", probe.frequency, got, probe.want)
				}
			}
		})
	}
}

func TestCCWholeBandsAndFrequencyOnlyClassification(t *testing.T) {
	for _, band := range []string{"160", "80", "60", "40", "30", "20", "17", "15", "12", "10", "6", "4", "2", "1", "MW"} {
		config, err := ccTestBase().WithDXBM(CCPass, []string{band})
		if err != nil {
			t.Fatal(err)
		}
		_, program, err := PrepareCCConfiguration(config)
		if err != nil {
			t.Fatal(err)
		}
		for _, golden := range ccSlotGolden {
			sp := ccTestSpot()
			sp.Frequency = (golden.LowKHz + golden.HighKHz) / 2
			sp.Band, sp.BandNorm = "20m", "20m"
			if got := program.Matches(sp, EvaluationContext{}); got != (band == golden.Band) {
				t.Fatalf("whole band %s, slot %s, deliberately conflicting cached band: %t", band, golden.Name, got)
			}
		}
	}
	for _, bad := range []string{"HF", "VHF", "UHF", "70cm", "1.25m", "20-FT8", "4-CW", "160-RTTY"} {
		if _, err := ccTestBase().WithDXBM(CCPass, []string{bad}); err == nil {
			t.Fatalf("invented native selector accepted: %s", bad)
		}
	}
}

func TestCCDXBMIncrementalStateAndUnknownFrequency(t *testing.T) {
	config := ccTestBase()
	var err error
	for _, edit := range []struct {
		action CCAction
		items  []string
		probes map[float64]bool
	}{
		{CCPass, []string{"20-CW"}, map[float64]bool{14025: true, 14074: false, 7020: false, 1000: false}},
		{CCPass, []string{"40"}, map[float64]bool{7020: true, 7060: true, 7200: true, 14025: true}},
		{CCReject, []string{"40-RTTY"}, map[float64]bool{7039.99: true, 7040: false, 7099: false, 7100: true}},
		{CCOff, nil, map[float64]bool{14025: true, 14074: true, 1000: true}},
		{CCReject, []string{"20-CW"}, map[float64]bool{14025: false, 14074: true, 7020: true, 1000: true}},
		{CCPass, []string{"20-CW"}, map[float64]bool{14025: true, 14074: true, 7020: true}},
		{CCReject, []string{"40-CW"}, map[float64]bool{7020: false, 14025: true, 14074: true}},
	} {
		config, err = config.WithDXBM(edit.action, edit.items)
		if err != nil {
			t.Fatal(err)
		}
		_, program, err := PrepareCCConfiguration(config)
		if err != nil {
			t.Fatal(err)
		}
		for frequency, want := range edit.probes {
			sp := ccTestSpot()
			sp.Frequency = frequency
			if got := program.Matches(sp, EvaluationContext{}); got != want {
				t.Fatalf("after %s %v at %v: got %t want %t", edit.action, edit.items, frequency, got, want)
			}
		}
		if edit.action == CCPass && len(edit.items) == 1 && edit.items[0] == "20-CW" && config.DXBM.BaseAllow && !config.DXBM.Active {
			t.Fatal("active allow-all was incorrectly collapsed to OFF")
		}
	}
	for _, action := range []CCAction{CCPass, CCReject} {
		cfg, err := ccTestBase().WithDXBM(action, []string{"20-CW"})
		if err != nil {
			t.Fatal(err)
		}
		_, program, err := PrepareCCConfiguration(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, frequency := range []float64{0, -1, math.NaN(), math.Inf(1), 1000, 53500000} {
			sp := ccTestSpot()
			sp.Frequency = frequency
			if got := program.Matches(sp, EvaluationContext{}); got != (action == CCReject) {
				t.Fatalf("unknown frequency %v under %s: %t", frequency, action, got)
			}
		}
	}
}
