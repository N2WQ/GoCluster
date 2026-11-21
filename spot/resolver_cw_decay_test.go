package spot

import (
	"strconv"
	"testing"
	"time"
)

func TestCWDecayIndependentAuthority(t *testing.T) {
	r := NewSignalResolver(SignalResolverConfig{CWDecayComparison: true, HysteresisWindows: 2, EvalMinInterval: time.Millisecond})
	states := make(map[ResolverSignalKey]*resolverKeyState)
	key := NewResolverSignalKey(7020, "40m", "CW", 400)
	start := time.Unix(1900000000, 0).UTC()
	for i, ev := range []struct {
		sec        int
		call, site string
	}{{0, "W1ABC", "R1"}, {1, "W1ABC", "R2"}, {2, "W1ABC", "R3"}, {181, "W1ABD", "R4"}, {182, "W1ABD", "R5"}} {
		now := start.Add(time.Duration(ev.sec) * time.Second)
		r.applyEvidence(states, ResolverEvidence{Key: key, DXCall: ev.call, Spotter: ev.site, ObservedAt: now, FrequencyKHz: 7020, RecencyWindow: 300 * time.Second})
		if i == 3 {
			for range 3 {
				s, ok := r.LookupCWDecayEligible(key, now.Add(time.Millisecond))
				if !ok || s.State != ResolverStateUncertain {
					t.Fatalf("lookup advanced the first contested evaluation: %+v", s)
				}
			}
		}
	}
	at := start.Add(183 * time.Second)
	control, _ := r.Lookup(key)
	shadow, ok := r.LookupCWDecayEligible(key, at)
	if !ok || control.Winner != "W1ABC" || shadow.Winner != "W1ABD" || shadow.State != ResolverStateConfident {
		t.Fatalf("separate winners: ordinary=%s decay=%+v", control.Winner, shadow)
	}
	// Independent milliweight vector for ages 183, 182, 181, 2 and 1 s.
	if shadow.WinnerWeightedSupportMilli != 1966 || shadow.TotalWeightedSupportMilli != 2333 {
		t.Fatalf("winner/total %d/%d", shadow.WinnerWeightedSupportMilli, shadow.TotalWeightedSupportMilli)
	}
	if control.Compact().cwDecay != nil || control.Compact().receiverEvidence != nil {
		t.Fatal("a delayed compact selection retained a receiver generation")
	}
	if future, _ := r.LookupCWDecayEligible(key, start.Add(182*time.Second)); !future.EvidenceLoss {
		t.Fatal("a focal or later publication authorized a decision")
	}
	prior := r.CaptureCWDecayPrior(key)
	r.applyEvidence(states, ResolverEvidence{Key: key, DXCall: "W1ABD", Spotter: "FOCAL", ObservedAt: at, FrequencyKHz: 7020, RecencyWindow: 300 * time.Second})
	excludingFocal, ok := r.LookupCWDecayPrior(prior, key, at)
	if !ok || excludingFocal.WinnerSupport != 2 || excludingFocal.TotalReporters != 5 {
		t.Fatalf("capture lost its pre-focal population: %+v", excludingFocal)
	}
	st := states[key]
	r.recordResolverCapLoss(st, at)
	r.publishLossControl(st, at)
	if loss, ok := r.LookupCWDecayEligible(key, at.Add(time.Second)); !ok || !loss.EvidenceLoss {
		t.Fatal("an incomplete control publication authorized a decision")
	}
	r.retireKey(states, key, st, false)
	if _, ok := r.LookupCWDecayEligible(key, at); ok {
		t.Fatal("retired key leaked comparison authority")
	}
	if old, _ := r.LookupCWDecayPrior(prior, key, at); !old.EvidenceLoss {
		t.Fatal("old focal capture survived key retirement")
	}
}

func TestCWDecaySingleCandidateReconcilesPendingState(t *testing.T) {
	r := NewSignalResolver(SignalResolverConfig{CWDecayComparison: true, HysteresisWindows: 3})
	key := NewResolverSignalKey(7030, "40m", "CW", 400)
	at := time.Unix(1900000000, 0)
	states := make(map[ResolverSignalKey]*resolverKeyState)
	r.applyEvidence(states, ResolverEvidence{Key: key, DXCall: "W1ABC", Spotter: "N0AAA", ObservedAt: at, FrequencyKHz: 7030, RecencyWindow: 5 * time.Minute})
	st := states[key]
	ordinary, ok := r.Lookup(key)
	if !ok || ordinary.cwDecay != &cwDecayUsesSingleIdentity {
		t.Fatal("missing optimized single identity")
	}
	// Model a challenger whose incumbent has disappeared between owner
	// publications. Require the full independent transition for this case.
	st.cwDecay.stable, st.cwDecay.pending, st.cwDecay.wins = "W1ABD", "W1ABC", 1
	actual := r.rankCWDecaySnapshot(st, ordinary, publishedResolverEvidence(st), at)
	if actual == &cwDecayUsesSingleIdentity || actual.Winner != "W1ABC" || actual.State != ResolverStateConfident ||
		st.cwDecay.stable != "W1ABC" || st.cwDecay.pending != "" || st.cwDecay.wins != 0 {
		t.Fatalf("independent transition lost: %+v shadow=%+v", actual, st.cwDecay)
	}
}

func BenchmarkCWDecayCrowdedKey(b *testing.B) {
	key := NewResolverSignalKey(7020, "40m", "CW", 400)
	start := time.Unix(1900000000, 0)
	for _, enabled := range []bool{false, true} {
		b.Run("enabled="+strconv.FormatBool(enabled), func(b *testing.B) {
			r := NewSignalResolver(SignalResolverConfig{CWDecayComparison: enabled, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64})
			states := make(map[ResolverSignalKey]*resolverKeyState)
			for call := range 16 {
				for receiver := range 64 {
					r.applyEvidence(states, ResolverEvidence{Key: key, DXCall: "W1A" + strconv.Itoa(call),
						Spotter: "R" + strconv.Itoa(receiver), ObservedAt: start.Add(time.Duration(call) * time.Second),
						RecencyWindow: 300 * time.Second, FrequencyKHz: 7020})
				}
			}
			at := start.Add(20 * time.Second)
			r.evaluateKey(states[key], at)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r.evaluateKey(states[key], at)
			}
		})
	}
}

func BenchmarkCWDecayOwnerEvaluation(b *testing.B) {
	key := NewResolverSignalKey(7020, "40m", "CW", 400)
	start := time.Unix(1900000000, 0)
	for _, tc := range []struct {
		name                  string
		comparison, contested bool
	}{{"ordinary_single", false, false}, {"decay_single", true, false},
		{"ordinary_contested", false, true}, {"decay_contested", true, true}} {
		b.Run(tc.name, func(b *testing.B) {
			r := NewSignalResolver(SignalResolverConfig{CWDecayComparison: tc.comparison})
			states := make(map[ResolverSignalKey]*resolverKeyState)
			r.applyEvidence(states, ResolverEvidence{Key: key, DXCall: "W1ABC", Spotter: "R1", ObservedAt: start, RecencyWindow: 300 * time.Second, FrequencyKHz: 7020})
			if tc.contested {
				r.applyEvidence(states, ResolverEvidence{Key: key, DXCall: "W1ABD", Spotter: "R2", ObservedAt: start.Add(time.Second), RecencyWindow: 300 * time.Second, FrequencyKHz: 7020})
			}
			now := start.Add(10 * time.Second)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r.evaluateKey(states[key], now)
			}
		})
	}
}
