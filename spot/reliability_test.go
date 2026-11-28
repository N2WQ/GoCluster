package spot

import (
	"math"
	"testing"
	"time"
)

func TestReceiverReliabilityLookupPrecedence(t *testing.T) {
	global := SpotterReliability{
		"K1ABC": .84, "K1ABC-#": .85, "K1ABC-1": .86, "K1ABC-2": .87,
		"K1DEF": .88, "K1GHI": .89, "K1GHI-#": 0, "K1JKL-#": math.NaN(),
	}
	cw := SpotterReliability{"K1ABC": .90, "K1DEF": .91, "K1GHI": .92, "K1JKL": .93}
	rtty := SpotterReliability{"K1ABC-#": .94, "K1DEF": .95}
	for _, tc := range []struct {
		name, mode, receiver string
		want, wantGlobal     float64
	}{
		{"exact mode", "rtty", " k1abc-# ", .94, .85},
		{"exact global before bare mode", "cw", "K1ABC-#", .85, .85},
		{"ordinary mode override", "CW", "K1ABC", .90, .84},
		{"bare mode first", "CW", "K1DEF-#", .91, .88},
		{"bare RTTY mode first", "RTTY", "K1DEF-#", .95, .88},
		{"global-only mode fallback", "SSB", "K1DEF-#", .88, .88},
		{"numeric receiver", "CW", "K1ABC-1-#", .86, .86},
		{"other numeric receiver", "CW", "K1ABC-2-#", .87, .87},
		{"unknown numeric receiver", "CW", "K1ABC-3-#", 1, 1},
		{"exact zero", "CW", "K1GHI-#", 0, 0},
		{"exact invalid", "CW", "K1JKL-#", math.NaN(), math.NaN()},
		{"unknown", "CW", "K1MNO-#", 1, 1},
		{"repeated marker", "CW", "K1ABC-#-#", 1, 1},
		{"other suffix", "CW", "K1ABC-@", 1, 1},
		{"hash without hyphen", "CW", "K1ABC#", 1, 1},
		{"empty base", "CW", "-#", 1, 1},
		{"empty input", "CW", " ", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reliabilityForMode(global, cw, rtty, tc.mode, tc.receiver); got != tc.want && !(math.IsNaN(got) && math.IsNaN(tc.want)) {
				t.Errorf("mode lookup = %g, want %g", got, tc.want)
			}
			if got := reliabilityFor(global, tc.receiver); got != tc.wantGlobal && !(math.IsNaN(got) && math.IsNaN(tc.wantGlobal)) {
				t.Errorf("global lookup = %g, want %g", got, tc.wantGlobal)
			}
		})
	}
	if got := reliabilityFor(nil, "K1DEF-#"); got != 1 {
		t.Errorf("nil table lookup = %g, want 1", got)
	}
}

func TestTaggedReceiverReliabilityAdmission(t *testing.T) {
	for _, mode := range []string{"CW", "RTTY"} {
		t.Run(mode, func(t *testing.T) {
			weights := SpotterReliability{"K1ABC-1": .9494, "K1ABC-2": .9495, "K1ABC-3": .981}
			cfg := SignalResolverConfig{MinSpotterReliability: .95}
			if mode == "CW" {
				cfg.SpotterReliabilityCW = weights
			} else {
				cfg.SpotterReliabilityRTTY = weights
			}
			resolver, driver := b21ResolverDriver(t, cfg)
			at := time.Date(2026, 11, 28, 4, 0, 0, 0, time.UTC)
			key := NewResolverSignalKey(14020, "20m", mode, 400)
			for _, receiver := range []string{"K1ABC-1-#", "K1ABC-2-#", "K1ABC-3-#"} {
				if !resolver.Enqueue(b21Evidence(key, "N0AAA", receiver, at)) {
					t.Fatalf("failed to queue %s", receiver)
				}
			}
			if got := driver.Step(at.Add(time.Millisecond)); got != 3 {
				t.Fatalf("processed %d reports, want 3", got)
			}
			snapshot, found := resolver.Lookup(key)
			if dropped := resolver.MetricsSnapshot().DropReliability; dropped != 1 || !found || snapshot.TotalReporters != 2 {
				t.Fatalf("drops=%d, found=%t, reporters=%d, want 1 drop and 2 reporters", dropped, found, snapshot.TotalReporters)
			}
			want := map[string]int{"K1ABC-2-#": 950, "K1ABC-3-#": 981}
			snapshot.ReceiverEvidence().Range(func(call string, observation ResolverReceiverObservation) bool {
				if weight, ok := want[observation.Receiver]; !ok || weight != observation.WeightMilli || call != "N0AAA" {
					t.Errorf("unexpected evidence for %s: %+v", call, observation)
				}
				delete(want, observation.Receiver)
				return true
			})
			if len(want) != 0 {
				t.Errorf("missing receiver evidence: %v", want)
			}
		})
	}
}

func BenchmarkReceiverReliabilityLookup(b *testing.B) {
	global := SpotterReliability{"K1ABC": .92, "K1DEF-#": .93}
	cw := SpotterReliability{"K1ABC-1": .94}
	for _, tc := range []struct{ name, receiver string }{
		{"mode_hit", "K1ABC-1"}, {"global_hit", "K1DEF-#"}, {"miss", "K1GHI"},
		{"tagged_fallback", "K1ABC-1-#"}, {"tagged_miss", "K1GHI-#"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			reliabilityForMode(global, cw, nil, "CW", tc.receiver)
			b.ReportAllocs()
			for b.Loop() {
				reliabilityForMode(global, cw, nil, "CW", tc.receiver)
			}
		})
	}
}
