package pathreliability

import (
	"math"
	"testing"
	"time"
)

// These public-policy vectors use different SNR bins at each resolution so an
// overlapping mixture or an unintended fallback cannot produce the same answer.
// Literal medians follow normative-population-vectors.md, not selector helpers.
type s6PolicyPath struct{ rx, tx, coarseRX, coarseTX CellID }

var s6PolicyQuery = s6PolicyPath{1, 2, 3, 4}

func s6PolicyConfig(mode string) Config {
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 180}
	cfg.MaxPredictionAgeHalfLifeMultiplier = 3
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = 1, 1
	cfg.MinEffectiveWeight = 0.01
	cfg.ReceiverContributionMode = mode
	cfg.ReceiverMaxEffectiveCount, cfg.ReceiverMaxEffectiveWeight = 8, 8
	cfg.VOACAPFallback.Enabled, cfg.Native160Fallback.Enabled = false, false
	return cfg
}

func s6PolicyNow() time.Time { return time.Unix(1_800_000_000, 0).UTC() }

func s6PolicyLayer(fine, reverse bool) s6PolicyPath {
	p := s6PolicyPath{coarseRX: 3, coarseTX: 4}
	if fine {
		p = s6PolicyPath{rx: 1, tx: 2}
	}
	if reverse {
		p.rx, p.tx, p.coarseRX, p.coarseTX = p.tx, p.rx, p.coarseTX, p.coarseRX
	}
	return p
}

func s6PolicyReports(t *testing.T, p *Predictor, path s6PolicyPath, reports, receivers int, db, weight float64, at time.Time) {
	t.Helper()
	for i := 0; i < reports; i++ {
		admission := p.UpdateWithReceiverHash(BucketCombined, path.rx, path.tx, path.coarseRX, path.coarseTX,
			"20m", db, weight, ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, uint64(1+i%receivers))
		if !admission.Accepted() {
			t.Fatalf("report %d rejected: %+v", i, admission)
		}
	}
}

func s6PolicyPredict(p *Predictor, path s6PolicyPath, noise float64, now time.Time, beacon bool) Result {
	if beacon {
		return p.PredictBeaconReceiveOnlyWithMinObservationCount(path.rx, path.tx, path.coarseRX, path.coarseTX, "20m", "FT8", noise, 1, now)
	}
	return p.PredictWithMinObservationCount(path.rx, path.tx, path.coarseRX, path.coarseTX, "20m", "FT8", noise, 1, now)
}

func s6PolicyWant(t *testing.T, got Result, db, weight float64, count uint32) {
	t.Helper()
	if got.Source != SourceCombined || got.InsufficientReason != InsufficientNone || !got.HasP50 || !got.ObservedDistributionAvailable || got.P50DB != db {
		t.Fatalf("want observed p50 %g, got %+v", db, got)
	}
	if !(math.Abs(got.Weight-weight) <= 1e-12) || got.ObservationCount != count || got.RawCount != count {
		t.Fatalf("want weight %g and lifetime/raw counts %d, got %+v", weight, count, got)
	}
}

// Mutation is restricted to numerical-failure and exact boundary controls. All
// ordinary evidence, histograms, receiver ownership and counts enter publicly.
func s6PolicyMutate(t *testing.T, p *Predictor, path s6PolicyPath, change func(*bucket)) {
	t.Helper()
	band, ok := p.combined.bandIndex.Lookup("20m")
	if !ok {
		t.Fatal("20m missing from fixture")
	}
	key := packKey(path.rx, path.tx, band)
	if path.rx == InvalidCell {
		key = packCoarseKey(path.coarseRX, path.coarseTX, band)
	}
	sh := p.combined.shardForKey(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	b := sh.buckets[key]
	if b == nil {
		t.Fatalf("missing fixture bucket for %+v", path)
	}
	change(b)
}

func TestS6FinePairQualification(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, tc := range []struct {
			name                                    string
			belowRX, belowTX, staleRX, concentrated bool
			wantFine                                bool
			agedReports                             int
		}{
			{"inclusive_20", false, false, false, false, true, 0},
			{"rx_adjacent_below", true, false, false, false, false, 0},
			{"tx_adjacent_below", false, true, false, false, false, 0},
			{"stale_rx_at_threshold", false, false, true, false, false, 0},
			{"active_raw_or_capped_weight", false, false, false, true, mode != ReceiverContributionEnforce, 0},
			{"aged_20_to_10", false, false, false, false, false, 20},
			{"aged_40_to_inclusive_20", false, false, false, false, true, 40},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				p, now := NewPredictor(s6PolicyConfig(mode), []string{"20m"}), s6PolicyNow()
				receivers := 6
				if tc.concentrated {
					receivers = 1
				}
				fineReports, fineAt := 20, now
				if tc.agedReports != 0 {
					fineReports, fineAt = tc.agedReports, now.Add(-180*time.Second)
				}
				for _, reverse := range []bool{false, true} {
					s6PolicyReports(t, p, s6PolicyLayer(true, reverse), fineReports, receivers, -11, 1, fineAt)
					s6PolicyReports(t, p, s6PolicyLayer(false, reverse), 6, 6, -20, 1, now)
				}
				for _, edge := range []struct{ reverse, below, stale bool }{{false, tc.belowRX, tc.staleRX}, {true, tc.belowTX, false}} {
					if edge.below || edge.stale {
						s6PolicyMutate(t, p, s6PolicyLayer(true, edge.reverse), func(b *bucket) {
							if edge.below {
								b.weight, b.cappedWeight = math.Nextafter(20, 0), math.Nextafter(20, 0)
							}
							if edge.stale {
								b.newestObservation = now.Unix() - 541
							}
						})
					}
				}
				got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
				if tc.wantFine {
					s6PolicyWant(t, got, -10.5, 20, uint32(2*fineReports))
				} else {
					s6PolicyWant(t, got, -19.5, 6, 12)
				}
			})
		}
	}
	t.Run("shared_fine_is_not_two_directions", func(t *testing.T) {
		p, now := NewPredictor(s6PolicyConfig(ReceiverContributionOff), []string{"20m"}), s6PolicyNow()
		s6PolicyReports(t, p, s6PolicyPath{rx: 1, tx: 1}, 20, 6, -11, 1, now)
		s6PolicyReports(t, p, s6PolicyLayer(false, false), 6, 6, -20, 1, now)
		s6PolicyWant(t, s6PolicyPredict(p, s6PolicyPath{1, 1, 3, 4}, 0, now, false), -19.5, 3, 6)
	})
}

func TestS6CommonResolutionNoFineFill(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "receive_coarse"
		if reverse {
			name = "transmit_coarse"
		}
		t.Run(name, func(t *testing.T) {
			p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
			// Only the opposite fine leg is strong. Mixing resolutions would add
			// its ten-dB-stronger reports and increase both support and weight.
			s6PolicyReports(t, p, s6PolicyLayer(true, !reverse), 24, 6, -10, 1, now)
			s6PolicyReports(t, p, s6PolicyLayer(false, reverse), 6, 6, -20, 1, now)
			s6PolicyWant(t, s6PolicyPredict(p, s6PolicyQuery, 0, now, false), -19.5, 3, 6)
		})
	}
}

func TestS6CoarseAvailabilityMatrix(t *testing.T) {
	for _, rx := range []string{"missing", "fresh", "stale"} {
		for _, tx := range []string{"missing", "fresh", "stale"} {
			t.Run(rx+"/"+tx, func(t *testing.T) {
				p, now := NewPredictor(s6PolicyConfig(ReceiverContributionOff), []string{"20m"}), s6PolicyNow()
				s6PolicyReports(t, p, s6PolicyLayer(true, false), 5, 5, -11, 1, now)
				s6PolicyReports(t, p, s6PolicyLayer(true, true), 5, 5, 4, 1, now)
				for i, state := range []string{rx, tx} {
					if state == "missing" {
						continue
					}
					at := now
					if state == "stale" {
						at = now.Add(-541 * time.Second)
					}
					s6PolicyReports(t, p, s6PolicyLayer(false, i == 1), 3, 3, -20, 1, at)
				}
				got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
				switch {
				case rx == "fresh" && tx == "fresh":
					s6PolicyWant(t, got, -19.5, 3, 6)
				case rx == "fresh" || tx == "fresh":
					s6PolicyWant(t, got, -19.5, 1.5, 3)
				default:
					s6PolicyWant(t, got, -10.5, 5, 10)
				}
			})
		}
	}
}

func TestS6FreshUnusableCoarseBlocksFineRetry(t *testing.T) {
	for _, fault := range []string{"count_only", "empty_positive", "nan_bin", "infinite_bin", "negative_bin", "nan_weight"} {
		t.Run(fault, func(t *testing.T) {
			p, now := NewPredictor(s6PolicyConfig(ReceiverContributionOff), []string{"20m"}), s6PolicyNow()
			s6PolicyReports(t, p, s6PolicyLayer(true, false), 10, 6, -11, 1, now)
			s6PolicyReports(t, p, s6PolicyLayer(false, false), 7, 6, -20, 1, now)
			s6PolicyMutate(t, p, s6PolicyLayer(false, false), func(b *bucket) {
				b.snrBins = snrHistogram{}
				switch fault {
				case "count_only":
					b.weight, b.cappedWeight = 0, 0
				case "nan_bin":
					b.snrBins[5] = float32(math.NaN())
				case "infinite_bin":
					b.snrBins[5] = float32(math.Inf(1))
				case "negative_bin":
					b.snrBins[5] = -1
				case "nan_weight":
					b.weight = math.NaN()
				}
			})
			got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
			if got.Source != SourceInsufficient || got.HasP50 || got.ObservedDistributionAvailable || got.ObservationCount != 7 || got.RawCount != 7 {
				t.Fatalf("fresh unusable coarse must block good fine retry: %+v", got)
			}
		})
	}
}

func TestS6FreshnessAndStoreExpiryBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		age        int
		multiplier float64
		wantCoarse bool
	}{
		{"prediction_540_fresh", 540, 3, true},
		{"prediction_541_stale", 541, 3, false},
		{"fractional_ceiling_181_fresh", 181, 1.002, true},
		{"fractional_ceiling_182_stale", 182, 1.002, false},
		{"prediction_disabled_700", 700, 0, true},
		{"store_900_inclusive", 900, 0, true},
		{"store_901_expired", 901, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, now := s6PolicyConfig(ReceiverContributionOff), s6PolicyNow()
			cfg.MaxPredictionAgeHalfLifeMultiplier = tc.multiplier
			p := NewPredictor(cfg, []string{"20m"})
			s6PolicyReports(t, p, s6PolicyLayer(true, false), 3, 3, -11, 1, now)
			s6PolicyReports(t, p, s6PolicyLayer(false, false), 1, 1, -20, 1, now.Add(-time.Duration(tc.age)*time.Second))
			got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
			if tc.wantCoarse {
				// Independent half-life formula; age and lifetime count distinguish
				// selecting aged coarse from returning the fresh fine distribution.
				s6PolicyWant(t, got, -19.5, 0.5*math.Exp2(-float64(tc.age)/180), 1)
				if got.AgeSec != int64(tc.age) {
					t.Fatalf("age=%d, want %d", got.AgeSec, tc.age)
				}
			} else {
				s6PolicyWant(t, got, -10.5, 1.5, 3)
			}
		})
	}
	t.Run("expired_counts_not_resurrected_without_hint", func(t *testing.T) {
		cfg, now := s6PolicyConfig(ReceiverContributionOff), s6PolicyNow()
		cfg.MaxPredictionAgeHalfLifeMultiplier = 0
		p := NewPredictor(cfg, []string{"20m"})
		s6PolicyReports(t, p, s6PolicyLayer(false, false), 9, 6, -20, 1, now.Add(-901*time.Second))
		got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
		if got.Source != SourceInsufficient || got.HasP50 || got.Count != 0 || got.RawCount != 0 || got.ObservationCount != 0 {
			t.Fatalf("expired capture leaked diagnostics: %+v", got)
		}
	})
}

func TestS6StaleDiagnosticsSelectedPopulationOnly(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		allStale, shared, excludedFine bool
		wantCount                      uint32
	}{
		{"excluded_stale_fine", false, false, true, 6},
		{"selected_stale_counterpart", false, false, false, 2},
		{"all_stale_coarse_attempted_layer", true, false, false, 18},
		{"all_stale_shared_coarse_once", true, true, false, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, now := s6PolicyConfig(ReceiverContributionOff), s6PolicyNow()
			if !tc.allStale && !tc.excludedFine {
				cfg.MinObservationCount = 5
			}
			p, query := NewPredictor(cfg, []string{"20m"}), s6PolicyQuery
			old := now.Add(-541 * time.Second)
			fineTime := now
			if tc.allStale || tc.excludedFine {
				fineTime = old
			}
			s6PolicyReports(t, p, s6PolicyLayer(true, false), 5, 5, -11, 1, fineTime)
			s6PolicyReports(t, p, s6PolicyLayer(true, true), 5, 5, 4, 1, fineTime)
			switch {
			case tc.shared:
				query.coarseTX = query.coarseRX
				s6PolicyReports(t, p, s6PolicyPath{coarseRX: 3, coarseTX: 3}, 7, 6, -20, 1, old)
			case tc.allStale:
				s6PolicyReports(t, p, s6PolicyLayer(false, false), 7, 6, -20, 1, old)
				s6PolicyReports(t, p, s6PolicyLayer(false, true), 11, 6, -20, 1, old)
			case tc.excludedFine:
				s6PolicyReports(t, p, s6PolicyLayer(false, false), 6, 6, -20, 1, now)
			default:
				s6PolicyReports(t, p, s6PolicyLayer(false, false), 2, 2, -20, 1, now)
				s6PolicyReports(t, p, s6PolicyLayer(false, true), 7, 6, -20, 1, old)
			}
			got := s6PolicyPredict(p, query, 0, now, false)
			if tc.excludedFine {
				s6PolicyWant(t, got, -19.5, 3, 6)
				return
			}
			if got.Source != SourceInsufficient || got.InsufficientReason != InsufficientStale || got.ObservationCount != tc.wantCount || got.RawCount != tc.wantCount {
				t.Fatalf("want stale count %d from selected layer, got %+v", tc.wantCount, got)
			}
			if got.HasP50 == tc.allStale {
				t.Fatalf("incorrect surviving-distribution availability: %+v", got)
			}
		})
	}
}

func TestS6SharedHintNoiseDiscountAndWeightGate(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, layer := range []string{"coarse", "fine_hint"} {
			t.Run(mode+"/"+layer, func(t *testing.T) {
				p, now := NewPredictor(s6PolicyConfig(mode), []string{"20m"}), s6PolicyNow()
				query, path := s6PolicyPath{1, 2, 3, 3}, s6PolicyPath{coarseRX: 3, coarseTX: 3}
				if layer == "fine_hint" {
					query, path = s6PolicyPath{rx: 1, tx: 1}, s6PolicyPath{rx: 1, tx: 1}
				}
				s6PolicyReports(t, p, path, 12, 6, -11, 1, now)
				for _, beacon := range []bool{false, true} {
					weight := 6.0
					if beacon {
						weight = 12
					}
					got := s6PolicyPredict(p, query, 6, now, beacon)
					s6PolicyWant(t, got, -16.5, weight, 12)
					if !(math.Abs(got.RawWeight-weight) <= 1e-12) || !(math.Abs(got.CappedWeight-weight) <= 1e-12) {
						t.Fatalf("shared scalar discount applied incorrectly: %+v", got)
					}
					if got.Count != 12 || got.CappedCount != 12 || got.BeaconRX != beacon {
						t.Fatalf("shared evidence duplicated: %+v", got)
					}
					if mode != ReceiverContributionOff && got.ReceiverCount != 6 {
						t.Fatalf("shared receivers counted twice: %+v", got)
					}
				}
			})
		}
		t.Run(mode+"/weak_signal_gate", func(t *testing.T) {
			cfg, now := s6PolicyConfig(mode), s6PolicyNow()
			cfg.MinEffectiveWeight = 0.5
			p, query := NewPredictor(cfg, []string{"20m"}), s6PolicyPath{1, 2, 3, 3}
			s6PolicyReports(t, p, s6PolicyPath{coarseRX: 3, coarseTX: 3}, 8, 6, -11, 0.1, now)
			ordinary := s6PolicyPredict(p, query, 6, now, false)
			if ordinary.Source != SourceInsufficient || ordinary.InsufficientReason != InsufficientLowWeight || !ordinary.HasP50 || ordinary.P50DB != -16.5 || !(math.Abs(ordinary.Weight-0.4) <= 1e-12) || ordinary.ObservationCount != 8 {
				t.Fatalf("shared ordinary discount gate: %+v", ordinary)
			}
			s6PolicyWant(t, s6PolicyPredict(p, query, 6, now, true), -16.5, 0.8, 8)
		})
	}
}

func TestS6DirectionalRolesAndNoise(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		rx, tx               int
		txWeight, db, weight float64
		count                uint32
	}{
		{"receive_only", 5, 0, 1, -16.5, 2.5, 5},
		{"transmit_only", 0, 5, 1, 4.5, 2.5, 5},
		{"receive_majority_60_40", 5, 5, 1, -16.5, 5, 10},
		{"unequal_signal_mass", 5, 10, 1, 4.5, 7, 15},
		{"exact_materialized_tie", 5, 3, 2.5, -6, 6, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
			s6PolicyReports(t, p, s6PolicyLayer(false, false), tc.rx, 6, -11, 1, now)
			s6PolicyReports(t, p, s6PolicyLayer(false, true), tc.tx, 6, 4, tc.txWeight, now)
			s6PolicyWant(t, s6PolicyPredict(p, s6PolicyQuery, 6, now, false), tc.db, tc.weight, tc.count)
		})
	}
}

func TestS6SelectedDistributionFailureIsTerminal(t *testing.T) {
	for _, layer := range []string{"fine_pair", "coarse_pair"} {
		for _, fault := range []string{"empty", "nan", "positive_infinity", "negative", "shift_overflow", "scaling_underflow"} {
			t.Run(layer+"/"+fault, func(t *testing.T) {
				p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
				fineCount := 10
				if layer == "fine_pair" {
					fineCount = 20
				}
				for _, reverse := range []bool{false, true} {
					s6PolicyReports(t, p, s6PolicyLayer(true, reverse), fineCount, 6, -11, 1, now)
					s6PolicyReports(t, p, s6PolicyLayer(false, reverse), 6, 6, -20, 1, now)
				}
				s6PolicyMutate(t, p, s6PolicyLayer(layer == "fine_pair", fault != "shift_overflow"), func(b *bucket) { s6PolicyBadBins(b, fault) })
				count, weight := uint32(12), 6.0
				if layer == "fine_pair" {
					count, weight = 40, 20
				}
				noise := 0.0
				if fault == "shift_overflow" {
					noise = 1
				}
				got := s6PolicyPredict(p, s6PolicyQuery, noise, now, false)
				if got.Source != SourceInsufficient || got.HasP50 || got.ObservedDistributionAvailable || got.P50Glyph != "" || got.InsufficientReason != InsufficientNoSample || got.ObservationCount != count || got.RawCount != count || !(math.Abs(got.Weight-weight) <= 1e-12) {
					t.Fatalf("selected failure produced partial/fallback estimate: %+v", got)
				}
			})
		}
	}
}

func s6PolicyBadBins(b *bucket, fault string) {
	b.snrBins = snrHistogram{}
	switch fault {
	case "nan":
		b.snrBins[14] = float32(math.NaN())
	case "positive_infinity":
		b.snrBins[14] = float32(math.Inf(1))
	case "negative":
		b.snrBins[14] = -1
	case "shift_overflow":
		b.snrBins[0], b.snrBins[1] = math.MaxFloat32, math.MaxFloat32
	case "scaling_underflow":
		b.snrBins[14] = math.SmallestNonzeroFloat32
	}
}

func TestS6ExcludedDistributionDoesNotPoison(t *testing.T) {
	for _, selected := range []string{"fine_pair", "coarse_pair"} {
		for _, fault := range []string{"empty", "nan", "positive_infinity", "negative"} {
			t.Run(selected+"/"+fault, func(t *testing.T) {
				p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
				fineCount := 10
				if selected == "fine_pair" {
					fineCount = 20
				}
				for _, reverse := range []bool{false, true} {
					s6PolicyReports(t, p, s6PolicyLayer(true, reverse), fineCount, 6, -11, 1, now)
					s6PolicyReports(t, p, s6PolicyLayer(false, reverse), 6, 6, -20, 1, now)
					s6PolicyMutate(t, p, s6PolicyLayer(selected != "fine_pair", reverse), func(b *bucket) { s6PolicyBadBins(b, fault) })
				}
				got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
				if selected == "fine_pair" {
					s6PolicyWant(t, got, -10.5, 20, 40)
				} else {
					s6PolicyWant(t, got, -19.5, 6, 12)
				}
			})
		}
	}
}

func TestS6RemovesLocalExtraEmphasis(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode+"/six_local_seven_other", func(t *testing.T) {
			p, now := NewPredictor(s6PolicyConfig(mode), []string{"20m"}), s6PolicyNow()
			s6PolicyReports(t, p, s6PolicyQuery, 6, 6, -10, 1, now)
			// These seven reports have other fine endpoints but the same coarse
			// endpoints. The selected coarse population has 7 weak vs 6 strong;
			// the removed local emphasis had 7 weak vs 12 strong (-9.5 dB).
			s6PolicyReports(t, p, s6PolicyPath{5, 6, 3, 4}, 7, 6, -20, 1, now)
			got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
			s6PolicyWant(t, got, -19.5, 6.5, 13)
			if got.Count != 13 || got.CappedCount != 13 {
				t.Fatalf("selected population counts: %+v", got)
			}
		})
		t.Run(mode+"/lifetime_floor_survives_one_half_life_in_s6", func(t *testing.T) {
			cfg, observed := s6PolicyConfig(mode), s6PolicyNow()
			cfg.MinObservationCount = 21
			p := NewPredictor(cfg, []string{"20m"})
			s6PolicyReports(t, p, s6PolicyLayer(false, false), 21, 6, -11, 1, observed)
			got := s6PolicyPredict(p, s6PolicyQuery, 0, observed.Add(180*time.Second), false)
			s6PolicyWant(t, got, -10.5, 5.25, 21)
			wantCount, wantCapped := uint32(21), uint32(10)
			if mode == ReceiverContributionEnforce {
				wantCount = 10
			}
			if mode == ReceiverContributionOff {
				wantCapped = 21
			}
			if got.Count != wantCount || got.CappedCount != wantCapped {
				t.Fatalf("S6 must retain lifetime qualification and existing capped rounding: %+v", got)
			}
		})
	}
}

func TestS6BeaconLayerSelectionAndIsolation(t *testing.T) {
	for _, tc := range []struct {
		name               string
		fine               int
		coarseAge          int
		coarse             bool
		wantDB, wantWeight float64
		wantCount          uint32
	}{
		{"fine_at_20", 20, 0, true, -16.5, 20, 20},
		{"fine_below_20_coarse_fresh", 19, 0, true, -24, 6, 6},
		{"coarse_540_fresh", 3, 540, true, -24, 0.75, 6},
		{"fine_hint_coarse_missing", 3, 0, false, -16.5, 3, 3},
		{"fine_hint_coarse_541_stale", 3, 541, true, -16.5, 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
			s6PolicyReports(t, p, s6PolicyLayer(true, false), tc.fine, 6, -11, 1, now)
			if tc.coarse {
				s6PolicyReports(t, p, s6PolicyLayer(false, false), 6, 6, -20, 1, now.Add(-time.Duration(tc.coarseAge)*time.Second))
			}
			// Reverse evidence is stronger, corrupt and clock-ahead. Receive-only
			// prediction must neither inspect its bins nor advance its reference.
			s6PolicyReports(t, p, s6PolicyLayer(true, true), 24, 6, 20, 1, now.Add(60*time.Second))
			s6PolicyMutate(t, p, s6PolicyLayer(true, true), func(b *bucket) { s6PolicyBadBins(b, "nan") })
			got := s6PolicyPredict(p, s6PolicyQuery, 6, now, true)
			// -20 dB shifted by -6 reaches the histogram's -24 dB underflow
			// representative, rather than inventing an out-of-range midpoint.
			s6PolicyWant(t, got, tc.wantDB, tc.wantWeight, tc.wantCount)
			if !got.BeaconRX || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != now.Unix() {
				t.Fatalf("reverse contaminated beacon: %+v", got)
			}
		})
	}
}
