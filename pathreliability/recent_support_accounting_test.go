package pathreliability

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"
)

func recentSupportConfig(mode string, halfLife int) Config {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = mode
	cfg.BandHalfLifeSec = map[string]int{"20m": halfLife}
	cfg.StaleAfterHalfLifeMultiplier, cfg.MaxPredictionAgeHalfLifeMultiplier = 10000, 10000
	cfg.ReceiverFineSlots, cfg.ReceiverCoarseSlots = 6, 12
	cfg.ReceiverMaxEffectiveCount, cfg.ReceiverMaxEffectiveWeight = 8, 8
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = 1, 1
	cfg.MinEffectiveWeight = .01
	cfg.VOACAPFallback.Enabled, cfg.Native160Fallback.Enabled = false, false
	return cfg
}

func recentSupportTime(second int64) time.Time { return time.Unix(second, 0).UTC() }

func recentSupportReport(t *testing.T, store *Store, observed, arrived int64, weight float64, hash uint64) {
	t.Helper()
	a := store.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -10.5, weight,
		ObservationTimes{ObservedAt: recentSupportTime(observed), ArrivedAt: recentSupportTime(arrived)}, hash)
	if a.Status != AdmissionAccepted || !a.EffectiveObservation.Equal(recentSupportTime(observed)) {
		t.Fatalf("valid report rejected or retimed: %+v", a)
	}
}

func recentSupportBucket(t *testing.T, store *Store, coarse bool) bucket {
	t.Helper()
	key := uint64(0x100020000)
	if coarse {
		key = 0x30004
	}
	sh := store.shardForKey(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	b := sh.buckets[key]
	if b == nil {
		t.Fatalf("missing literal fixture key %x", key)
	}
	return *b
}

// Fault fixtures change only specified numerical state, under the same lock
// that owns production updates. Normal report evidence always enters publicly.
func recentSupportMutate(t *testing.T, store *Store, key uint64, change func(*bucket)) {
	t.Helper()
	sh := store.shardForKey(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	b := sh.buckets[key]
	if b == nil {
		t.Fatalf("missing fault-fixture key %x", key)
	}
	change(b)
}

func recentSupportWant(t *testing.T, got ReportSupport, mode string, raw, admitted float64) {
	t.Helper()
	want := ReportSupport{RawUsable: raw, Admitted: admitted, AdmittedAvailable: mode != ReceiverContributionOff, Enforced: mode == ReceiverContributionEnforce}
	if mode == ReceiverContributionOff {
		want.Admitted = 0
	}
	if got != want {
		t.Fatalf("support=%+v, want %+v", got, want)
	}
	units, available := got.ActiveUnits()
	wantUnits := raw
	if mode == ReceiverContributionEnforce {
		wantUnits = admitted
	}
	if !available || units != wantUnits {
		t.Fatalf("active support=%g/%v, want %g/true", units, available, wantUnits)
	}
}

func TestRecentSupportAccounting(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			t.Run("report_units", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				// At T=20 the weights .25,2,4 have ages0,H,2H:
				// units=1+.5+.25=1.75, mass=.25+1+1=2.25.
				recentSupportReport(t, store, 0, 0, 4, 1)
				recentSupportReport(t, store, 10, 10, 2, 2)
				recentSupportReport(t, store, 20, 20, .25, 3)
				fine, coarse := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(20))
				for _, sample := range []Sample{fine, coarse} {
					recentSupportWant(t, sample.RecentSupport, mode, 1.75, 1.75)
					if sample.Weight != 2.25 || sample.RawCount != 3 || sample.ObservationCount != 3 || sample.SamplingUnix != 20 {
						t.Fatalf("report support was confused with signal or lifetime count: %+v", sample)
					}
				}
			})
			t.Run("fresh_low_signal", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, store, 0, 0, .01, 1)
				fine, _ := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(0))
				recentSupportWant(t, fine.RecentSupport, mode, 1, 1)
				if fine.Weight != .01 {
					t.Fatalf("signal mass changed: %+v", fine)
				}
			})
			t.Run("delayed", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, store, 0, 10, 2, 1)
				fine, _ := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(20))
				recentSupportWant(t, fine.RecentSupport, mode, .25, .25)
				if fine.Weight != .5 || fine.RawCount != 1 || fine.AgeSec != 20 || recentSupportBucket(t, store, false).recentRawUsable != .5 {
					t.Fatalf("arrival rejuvenated support or lookup mutated its owner: %+v", fine)
				}
			})
			t.Run("arrival_order", func(t *testing.T) {
				for _, order := range [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
					store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
					for _, i := range order {
						recentSupportReport(t, store, int64(i*10), int64(i*10), []float64{4, 2, .25}[i], uint64(i+1))
					}
					fine, _ := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(20))
					recentSupportWant(t, fine.RecentSupport, mode, 1.75, 1.75)
					if fine.Weight != 2.25 || fine.RawCount != 3 {
						t.Fatalf("fully admitted permutation %v changed evidence: %+v", order, fine)
					}
				}
			})
			t.Run("common_reference", func(t *testing.T) {
				p := NewPredictor(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, p.combined, 0, 0, 2, 1)
				a := p.UpdateWithReceiverHash(BucketCombined, 2, 1, 4, 3, "20m", -10.5, 1,
					ObservationTimes{ObservedAt: recentSupportTime(20), ArrivedAt: recentSupportTime(20)}, false, 2)
				if !a.Accepted() {
					t.Fatalf("reverse admission failed: %+v", a)
				}
				got := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, recentSupportTime(10))
				recentSupportWant(t, got.ObservedRecentSupport, mode, 1.25, 1.25)
				if got.Source != SourceCombined || got.ObservedSamplingUnix != 20 || !got.HasP50 || got.P50DB != -10.5 {
					t.Fatalf("directions used different support references or rounded before summing: %+v", got)
				}
			})
		})
	}
}

func TestRecentSupportCaptureOwnership(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
			for hash := uint64(1); hash <= 12; hash++ {
				recentSupportReport(t, store, 0, 0, 1, hash)
			}
			capture := store.captureBucket(0x30004, 12, true)
			before := capture
			recentSupportReport(t, store, 0, 0, 1, 1)
			sample := store.evaluateCapture(&capture, 10, 10, 12, sampleDistribution)
			recentSupportWant(t, sample.RecentSupport, mode, 6, 6)
			if capture != before || sample.Weight != 6 || sample.RawCount != 12 || sample.CappedReceiverCount != 0 {
				t.Fatalf("captured support/slots were mutated or retained live state: %+v", sample)
			}
			if b := recentSupportBucket(t, store, true); b.recentRawUsable != 13 || b.cappedCount != 13 {
				t.Fatalf("live update did not differ from saved capture: %+v", b)
			}
		})
	}
}

func TestReportSupportActiveUnits(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   ReportSupport
		want float64
		ok   bool
	}{
		{"off", ReportSupport{RawUsable: 21}, 21, true},
		{"shadow", ReportSupport{RawUsable: 21, Admitted: .25, AdmittedAvailable: true}, 21, true},
		{"enforced_fraction", ReportSupport{RawUsable: 21, Admitted: .25, AdmittedAvailable: true, Enforced: true}, .25, true},
		{"explicit_zero", ReportSupport{RawUsable: 21, AdmittedAvailable: true, Enforced: true}, 0, true},
		{"unavailable", ReportSupport{RawUsable: 21, Enforced: true}, 0, false},
		{"raw_zero", ReportSupport{}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.in
			got, ok := tc.in.ActiveUnits()
			if got != tc.want || ok != tc.ok || tc.in != before {
				t.Fatalf("active=%g/%v, want %g/%v; support=%+v", got, ok, tc.want, tc.ok, tc.in)
			}
		})
	}
}

func TestReportUnitsMeetMinimum(t *testing.T) {
	cases := []struct {
		name  string
		units float64
		floor int
		want  bool
	}{
		{"zero", 0, 1, false}, {"fraction_below", 10.5, 11, false}, {"fraction_above", 10.5, 10, true},
		{"below", 21 - 0x1p-40, 21, false}, {"equal", 21, 21, true}, {"above", 21 + 0x1p-40, 21, true},
		{"adjacent_below", math.Nextafter(21, 0), 21, false}, {"adjacent_above", math.Nextafter(21, math.Inf(1)), 21, true},
		{"negative", -1, 1, false}, {"nan", math.NaN(), 1, false}, {"positive_infinity", math.Inf(1), 1, false}, {"negative_infinity", math.Inf(-1), 1, false},
	}
	if strconv.IntSize == 64 {
		firstInexact := int64(1<<53 + 1)
		maximum := int64(1<<63 - 1)
		cases = append(cases, []struct {
			name  string
			units float64
			floor int
			want  bool
		}{
			{"large_rounds_down_fail", 0x1p53, int(firstInexact), false},
			{"large_rounds_down_pass", 0x1p53 + 2, int(firstInexact), true},
			{"large_rounds_up_fail", 0x1p53 + 2, int(firstInexact + 2), false},
			{"large_rounds_up_pass", 0x1p53 + 4, int(firstInexact + 2), true},
			{"max_int_below", math.Nextafter(0x1p63, 0), int(maximum), false},
			{"max_int_above", 0x1p63, int(maximum), true},
		}...)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := reportUnitsMeetMinimum(tc.units, tc.floor); got != tc.want {
				t.Fatalf("units %.17g floor %d: got %v, want %v", tc.units, tc.floor, got, tc.want)
			}
		})
	}
}

func recentSupportPredictorReports(t *testing.T, p *Predictor, coarseRX, coarseTX CellID, reports int, weight float64, observed int64) {
	t.Helper()
	for i := 0; i < reports; i++ {
		a := p.UpdateWithReceiverHash(BucketCombined, InvalidCell, InvalidCell, coarseRX, coarseTX, "20m", -10.5, weight,
			ObservationTimes{ObservedAt: recentSupportTime(observed), ArrivedAt: recentSupportTime(observed)}, false, uint64(1+i%6))
		if !a.Accepted() {
			t.Fatalf("support gate seed rejected: %+v", a)
		}
	}
}

func TestRecentSupportGate(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, tc := range []struct {
			name                  string
			directions, reports   int
			query                 int64
			weight                float64
			shared, zero, corrupt bool
			units, signal         float64
			count                 uint32
			reason                InsufficientReason
		}{
			{"single_direction", 1, 21, 0, 1, false, false, false, 21, 10.5, 21, InsufficientNone},
			{"distinct_directions", 2, 21, 0, 1, false, false, false, 42, 21, 42, InsufficientNone},
			{"fractional_sum", 2, 21, 10, 1, false, false, false, 21, 10.5, 42, InsufficientNone},
			{"aged_below", 1, 21, 10, 1, false, false, false, 10.5, 5.25, 21, InsufficientLowCount},
			{"shared_once", 1, 11, 0, 1, true, false, false, 11, 5.5, 11, InsufficientLowCount},
			{"zero_no_fallback", 1, 21, 0, 1, false, true, false, 0, 10.5, 21, InsufficientLowCount},
			{"low_signal", 1, 21, 0, .01, false, false, false, 21, .105, 21, InsufficientLowWeight},
			{"required_distribution_unavailable", 1, 21, 0, 1, false, false, true, 21, 10.5, 21, InsufficientNoSample},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				cfg.MinObservationCount, cfg.MinEffectiveWeight = 21, .5
				p := NewPredictor(cfg, []string{"20m"})
				coarseTX := CellID(4)
				if tc.shared {
					coarseTX = 3
				}
				recentSupportPredictorReports(t, p, 3, coarseTX, tc.reports, tc.weight, 0)
				if tc.directions == 2 {
					recentSupportPredictorReports(t, p, coarseTX, 3, tc.reports, tc.weight, 0)
				}
				if tc.zero || tc.corrupt {
					recentSupportMutate(t, p.combined, 0x30004, func(b *bucket) {
						if tc.zero {
							b.recentRawUsable, b.cappedCount = 0, 0
						} else {
							b.snrBins[14] = float32(math.NaN())
						}
					})
				}
				got := p.Predict(InvalidCell, InvalidCell, 3, coarseTX, "20m", "FT8", 0, recentSupportTime(tc.query))
				recentSupportWant(t, got.ObservedRecentSupport, mode, tc.units, tc.units)
				wantSource := SourceInsufficient
				if tc.reason == InsufficientNone {
					wantSource = SourceCombined
				}
				if got.Source != wantSource || got.InsufficientReason != tc.reason || got.RawCount != tc.count ||
					got.HasP50 == tc.corrupt || got.ObservedDistributionAvailable == tc.corrupt || math.Abs(got.Weight-tc.signal) > 1e-12 {
					t.Fatalf("support/weight/distribution gates were coupled: %+v", got)
				}
				if !tc.corrupt && got.P50DB != -10.5 {
					t.Fatalf("support qualification changed the retained p50: %+v", got)
				}
				if tc.name == "fractional_sum" && mode == ReceiverContributionEnforce && got.CappedCount != 20 {
					t.Fatalf("legacy integer diagnostics must stay floored independently: %+v", got)
				}
			})
		}
	}
}

func TestRecentSupportMergeBeforeDisplay(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, weights := range [][2]float64{{1, 1}, {1, 0}, {0, 1}, {0, 0}} {
			t.Run(fmt.Sprintf("%s/signal_%g_%g", mode, weights[0], weights[1]), func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				left := Sample{Weight: weights[0], RawCount: 21, ObservationCount: 21, Count: 21, CappedCount: 10,
					RecentSupport: ReportSupport{RawUsable: 21, Admitted: 10.5, AdmittedAvailable: mode != ReceiverContributionOff, Enforced: mode == ReceiverContributionEnforce}}
				if mode == ReceiverContributionOff {
					left.RecentSupport.Admitted = 0
				}
				right := left
				right.Weight = weights[1]
				got, ok := mergeSamples(left, right, cfg)
				recentSupportWant(t, got.RecentSupport, mode, 42, 21)
				if !ok || got.RawCount != 42 || got.CappedCount != 20 {
					t.Fatalf("display coefficients or zero signal lost selected support: %+v", got)
				}
			})
		}
	}
}

func TestRecentSupportStaleDiagnostics(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, tc := range []struct {
			name           string
			fresh, expired bool
			units          float64
			rawCount       uint32
			weight         float64
			reason         InsufficientReason
		}{
			{"all_stale", false, false, 1, 4, 0, InsufficientStale},
			{"fresh_excludes_stale", true, false, 1, 1, .5, InsufficientStale},
			{"store_expired", false, true, 0, 0, 0, InsufficientNoSample},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				cfg.MinObservationCount, cfg.MaxPredictionAgeHalfLifeMultiplier = 2, 1
				if tc.expired {
					cfg.StaleAfterHalfLifeMultiplier = 1
				}
				p := NewPredictor(cfg, []string{"20m"})
				recentSupportPredictorReports(t, p, 3, 4, 4, 1, 0)
				if tc.fresh {
					recentSupportPredictorReports(t, p, 4, 3, 1, 1, 20)
				}
				got := p.Predict(InvalidCell, InvalidCell, 3, 4, "20m", "FT8", 0, recentSupportTime(20))
				recentSupportWant(t, got.ObservedRecentSupport, mode, tc.units, tc.units)
				if got.Source != SourceInsufficient || got.InsufficientReason != tc.reason || got.RawCount != tc.rawCount || got.Weight != tc.weight || got.HasP50 != tc.fresh || got.CapWouldBlock != (tc.fresh && mode == ReceiverContributionShadow) {
					t.Fatalf("stale support funded fresh evidence or expired support was resurrected: %+v", got)
				}
			})
		}
	}
}

// The legacy count conversion adds 1e-9 before flooring and the legacy weight
// flag allows 1e-9 slack. Both can report no limiting even though the exact
// admitted report quantity misses the floor. Shadow must still expose that
// failure without changing the raw-supported observed glyph.
func TestRecentSupportShadowIgnoresRoundedCapFlag(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		beacon               bool
		reports              int
		capWeight, admitted  float64
		weight, cappedWeight float64
		required             uint32
	}{
		{"ordinary", false, 21, 7 - 0x1p-40, 21 - 3*0x1p-40, 10.5, 10.5 - 3*0x1p-41, 3},
		{"beacon", true, 11, 4 - 0x1p-40, 11 - 2*0x1p-40, 11, 11 - 2*0x1p-40, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := recentSupportConfig(ReceiverContributionShadow, 10)
			cfg.MinObservationCount, cfg.BeaconMinObservationCount = 21, 11
			cfg.MinEffectiveWeight, cfg.ReceiverMaxEffectiveWeight = .5, tc.capWeight
			p := NewPredictor(cfg, []string{"20m"})
			for i := 0; i < tc.reports; i++ {
				a := p.UpdateWithReceiverHash(BucketCombined, InvalidCell, InvalidCell, 3, 4, "20m", -10.5, 1,
					ObservationTimes{ObservedAt: recentSupportTime(0), ArrivedAt: recentSupportTime(0)}, tc.beacon, uint64(i%3+1))
				if a.Status != AdmissionAccepted {
					t.Fatalf("fresh shadow witness rejected: %+v", a)
				}
			}
			// Ordinary has seven reports per hash; beacon has 4, 4, 3. Only
			// the final clipped reports lose 2^-40 admitted units each.
			_, coarse := p.combined.Lookup(InvalidCell, InvalidCell, 3, 4, "20m", recentSupportTime(0))
			recentSupportWant(t, coarse.RecentSupport, ReceiverContributionShadow, float64(tc.reports), tc.admitted)
			if coarse.CappedCount != uint32(tc.reports) || coarse.CapLimited || coarse.CappedReceiverCount != 3 || coarse.CappedWeight != tc.admitted || !(tc.admitted < float64(tc.reports)) {
				t.Fatalf("shadow witness failed to establish a hidden fractional deficit: %+v", coarse)
			}
			var got Result
			if tc.beacon {
				got = p.PredictBeaconReceiveOnlyWithMinObservationCount(InvalidCell, InvalidCell, 3, 4, "20m", "FT8", 0, 11, recentSupportTime(0))
			} else {
				got = p.Predict(InvalidCell, InvalidCell, 3, 4, "20m", "FT8", 0, recentSupportTime(0))
			}
			recentSupportWant(t, got.ObservedRecentSupport, ReceiverContributionShadow, float64(tc.reports), tc.admitted)
			if got.Source != SourceCombined || got.InsufficientReason != InsufficientNone || !got.HasP50 || !got.ObservedDistributionAvailable || got.P50DB != -10.5 ||
				got.Count != uint32(tc.reports) || got.CappedCount != uint32(tc.reports) || got.CapLimited || !got.CapWouldBlock ||
				got.ReceiverCount != 3 || got.ReceiverRequired != tc.required || got.Weight != tc.weight || got.CappedWeight != tc.cappedWeight || got.BeaconRX != tc.beacon {
				t.Fatalf("legacy flag bypassed the exact shadow support floor: %+v", got)
			}
		})
	}
}
