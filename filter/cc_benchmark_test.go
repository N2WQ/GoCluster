package filter

import (
	"testing"

	"dxcluster/spot"
)

var ccMatchingSink bool

// Every band gets a distinct maximal country selection, with the matching
// number at its final sorted position. This prevents shared-list/early-exit
// fixtures from hiding maximum accepted native work.
func ccMaximumTestConfiguration() CCConfiguration {
	config := CCConfiguration{Modes: SupportedModes(), Sources: []string{"HUMAN", "SKIMMER"}, Geography: map[string]CCGeography{}}
	for _, name := range []string{"DOC", "DOS", "DXCTY", "DXSTATE"} {
		geography := CCGeography{Bands: map[string]CCCategory{}}
		for index := -1; index < 15; index++ {
			category := CCCategory{Action: CCPass}
			if name == "DOC" || name == "DXCTY" {
				category.Countries = make([]int, 512)
				for i := range category.Countries[:511] {
					category.Countries[i] = (index+1)*600 + i + 1
				}
				category.Countries[511] = 1_000_000_000
			} else {
				category.States = spot.StateCodes()
			}
			if index < 0 {
				geography.Global = category
			} else {
				geography.Bands[ccNativeBands[index]] = category
			}
		}
		config.Geography[name] = geography
	}
	config.DXBM = CCDXBM{Active: true, BaseAllow: false, Slots: map[string]bool{}}
	for _, slot := range ccNativeSlots {
		config.DXBM.Slots[slot.Name] = true
	}
	return config
}

func ccMaximumTestSpot() *spot.Spot {
	sp := ccTestSpot()
	sp.DEMetadata.ADIF, sp.DXMetadata.ADIF = 1_000_000_000, 1_000_000_000
	sp.DEMetadata.State, sp.DXMetadata.State = "YT", "YT"
	return sp
}

func TestCCMaximumMatchingAllocationAndLateGate(t *testing.T) {
	config := ccMaximumTestConfiguration()
	canonical, program, err := PrepareCCConfiguration(config)
	if err != nil {
		t.Fatal(err)
	}
	counts := program.Counts()
	if counts.Countries != 16*2*512 || counts.States != 16*2*73 || counts.GeographyRows != 64 || counts.DXBMSlots != 37 || counts.CanonicalBytes > MaxCCCanonicalBytes {
		t.Fatalf("maximum fixture did not exercise the admitted native envelope: %+v", counts)
	}
	sp := ccMaximumTestSpot()
	for _, test := range []struct {
		name    string
		action  CCAction
		missing bool
		want    bool
	}{
		{"all-pass", CCPass, false, true}, {"last-gate-reject", CCReject, false, false},
		{"last-gate-missing-pass", CCPass, true, false}, {"last-gate-missing-reject", CCReject, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := canonical.Clone()
			last := candidate.Geography["DXSTATE"]
			row := last.Bands["20"]
			row.Action = test.action
			last.Bands["20"] = row
			candidate.Geography["DXSTATE"] = last
			_, prepared, err := PrepareCCConfiguration(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if test.missing {
				sp.DXMetadata.State = ""
			} else {
				sp.DXMetadata.State = "YT"
			}
			if got := prepared.Matches(sp, EvaluationContext{}); got != test.want {
				t.Fatalf("fixture outcome %t, want %t", got, test.want)
			}
			allocations := testing.AllocsPerRun(1000, func() {
				ccMatchingSink = prepared.Matches(sp, EvaluationContext{})
			})
			if allocations != 0 || ccMatchingSink != test.want {
				t.Fatalf("normalized maximum matching: allocations=%g result=%t", allocations, ccMatchingSink)
			}
		})
	}
}

func BenchmarkCCMatching(b *testing.B) {
	for _, lateReject := range []bool{false, true} {
		name := "maximum-all-pass"
		config := ccMaximumTestConfiguration()
		if lateReject {
			name = "maximum-last-gate-reject"
			last := config.Geography["DXSTATE"]
			row := last.Bands["20"]
			row.Action = CCReject
			last.Bands["20"] = row
			config.Geography["DXSTATE"] = last
		}
		b.Run(name, func(b *testing.B) {
			_, program, err := PrepareCCConfiguration(config)
			if err != nil {
				b.Fatal(err)
			}
			sp := ccMaximumTestSpot()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ccMatchingSink = program.Matches(sp, EvaluationContext{})
			}
		})
	}
}
