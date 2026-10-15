package telnet

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// Five unit-weight reports are observed one half-life before arrival. Each
// receiver's .25 signal cap admits half its .5 aged report: U=2.5, A=1.25.
// The common reference is later than the consumer's requested clock.
func recentProvenanceFixture(t *testing.T, band string, beacon, noDistribution bool) (*Server, *Client, pathreliability.VOACAPClosedRequest, pathreliability.Result) {
	t.Helper()
	requested := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
	reference := requested.Add(30 * time.Second)
	cfg := recentConsumerConfig(t, pathreliability.ReceiverContributionEnforce)
	cfg.ReceiverMaxEffectiveWeight = 0.25
	cfg.VOACAPFallback.ReliabilityGatedOpenEnabled, cfg.VOACAPFallback.ReliabilitySparseUpgradeEnabled = true, true
	dxGrid := "IO91"
	if band == "160m" {
		dxGrid = "FN20"
		cfg.Native160Fallback.Enabled, cfg.Native160Fallback.DisplayEnabled = true, true
	}
	if noDistribution {
		cfg.BandHalfLifeSec[band] = 1
		cfg.MaxPredictionAgeHalfLifeMultiplier = 0
		cfg.StaleAfterHalfLifeMultiplier = 1000
	}
	s, c, sp := recentConsumerFixture(t, cfg, requested, band, dxGrid, beacon)
	if noDistribution {
		// 2^-160 is below the float32 histogram increment domain. Timestamp
		// admission still records one lifetime report, but usable units are0.
		recentConsumerReports(t, s, c, sp, 1, 1, 1, -20, reference.Add(-160*time.Second), reference, false)
	} else {
		recentConsumerReports(t, s, c, sp, 5, 5, 1, -20, reference.Add(-180*time.Second), reference, false)
	}
	base := recentConsumerPrediction(t, s, c, sp).result
	raw, admitted := 2.5, 1.25
	wantCount := uint32(5)
	if noDistribution {
		raw, admitted, wantCount = 0, 0, 1
	}
	recentConsumerWantSupport(t, base.ObservedRecentSupport, raw, admitted, pathreliability.ReceiverContributionEnforce)
	if base.Source != pathreliability.SourceInsufficient && band != "160m" {
		t.Fatalf("fixture did not reach insufficient observed evidence: %+v", base)
	}
	if base.ObservationCount != wantCount || !base.HasObservedSamplingTime || base.ObservedSamplingUnix != reference.Unix() || base.ObservedDistributionAvailable == noDistribution {
		t.Fatalf("fixture lost lifetime/reference/distribution boundary: %+v", base)
	}
	if base.HasP50 == noDistribution || (!noDistribution && base.P50DB != -19.5) {
		t.Fatalf("fixture did not retain the literal observed median: %+v", base)
	}
	req := pathreliability.VOACAPClosedRequest{UserCell: c.gridCell, DXCell: pathreliability.CellID(sp.DXCellID), UserGrid: c.grid, DXGrid: dxGrid, Band: band, Mode: "FT8"}
	return s, c, req, base
}

func TestPathRecentSupportProvenance(t *testing.T) {
	for _, beacon := range []bool{false, true} {
		for _, tc := range []struct {
			name, band                  string
			snr                         int
			noDistribution, unavailable bool
			source                      pathreliability.PredictionSource
		}{
			{"closed", "20m", -34, false, false, pathreliability.SourceVOACAPClosed},
			{"aligned", "20m", -19, false, false, pathreliability.SourceVOACAPAligned},
			{"sparse_upgrade", "20m", -15, false, false, pathreliability.SourceVOACAPSparseUpgrade},
			{"open_zero_usable", "20m", -19, true, false, pathreliability.SourceVOACAPOpen},
			{"unavailable", "20m", -19, false, true, pathreliability.SourceInsufficient},
			{"native160", "160m", 0, false, false, pathreliability.SourceNative160},
		} {
			t.Run(fmt.Sprintf("%s/beacon=%t", tc.name, beacon), func(t *testing.T) {
				s, c, req, base := recentProvenanceFixture(t, tc.band, beacon, tc.noDistribution)
				forecast := forecastWithReqSNRReliability(tc.snr, 0.95)
				forecast.AgeSec, forecast.Record.HourUTC = 42, 4
				fallback := &countingClosedFallback{forecast: forecast, ok: !tc.unavailable}
				if tc.source != pathreliability.SourceNative160 {
					s.pathClosedFallback = fallback
				}
				sp := spotForRecentProvenance(req, beacon, s.now())
				prediction := recentConsumerPrediction(t, s, c, sp)
				got := prediction.result
				raw, admitted := 2.5, 1.25
				if tc.noDistribution {
					raw, admitted = 0, 0
				}
				recentConsumerWantSupport(t, got.ObservedRecentSupport, raw, admitted, pathreliability.ReceiverContributionEnforce)
				if got.Source != tc.source || got.BeaconRX != beacon || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != s.now().Unix()+30 || got.ObservedDistributionAvailable != base.ObservedDistributionAvailable {
					t.Fatalf("fallback lost complete observed provenance: %+v", got)
				}
				if tc.source == pathreliability.SourceVOACAPClosed && (!base.HasP50 || !got.HasP50 || got.P50DB != -34) {
					t.Fatalf("CLOSED must retain precedence over positive insufficient observed evidence: base=%+v replacement=%+v", base, got)
				}
				if tc.source == pathreliability.SourceVOACAPClosed || tc.source == pathreliability.SourceVOACAPOpen {
					if got.AgeSec != 42 || got.VOACAPAgeSec != 42 || got.VOACAPHourUTC != 4 {
						t.Fatalf("observed support replaced forecast metadata: %+v", got)
					}
				} else if got.ObservationCount != 5 || got.RawCount != 5 || got.CappedCount != 1 {
					t.Fatalf("copy-base fallback changed legacy diagnostics: %+v", got)
				}
				if tc.source != pathreliability.SourceNative160 && fallback.calls != 1 {
					t.Fatalf("actual fallback calls=%d, want1", fallback.calls)
				}
				if !prediction.at.Equal(s.now()) || !sp.Time.Equal(s.now()) {
					t.Fatal("observed reference overwrote request/source timestamps")
				}
			})
		}
	}
}

// Reconstruct only the normal consumer input; forecast/support expected values
// above stay literal, and the real Server performs all fallback decisions.
func spotForRecentProvenance(req pathreliability.VOACAPClosedRequest, beacon bool, at time.Time) *spot.Spot {
	frequency := 14074.0
	if req.Band == "160m" {
		frequency = 1840
	}
	sp := spot.NewSpot("K1S7", "N0S7", frequency, "FT8")
	sp.BandNorm, sp.DXCellID, sp.DXMetadata.Grid, sp.IsBeacon, sp.Time = req.Band, uint16(req.DXCell), req.DXGrid, beacon, at
	return sp
}

func TestPathRecentSupportProvenanceOpenFractionalBase(t *testing.T) {
	for _, beacon := range []bool{false, true} {
		t.Run(fmt.Sprintf("beacon=%t", beacon), func(t *testing.T) {
			s, _, req, base := recentProvenanceFixture(t, "20m", beacon, false)
			// Explicit missing-distribution fault for this wrapper-only control:
			// it isolates copying nonzero fractional support through OPEN, whose
			// natural public zero-distribution vector is separately covered above.
			base.HasP50, base.ObservedDistributionAvailable = false, false
			base.InsufficientReason = pathreliability.InsufficientNoSample
			forecast := forecastWithReqSNRReliability(-19, 0.95)
			forecast.AgeSec, forecast.Record.HourUTC = 42, 4
			s.pathClosedFallback = &countingClosedFallback{forecast: forecast, ok: true}
			got := s.pathResultWithClosedFallback(base, req, s.now())
			recentConsumerWantSupport(t, got.ObservedRecentSupport, 2.5, 1.25, pathreliability.ReceiverContributionEnforce)
			if got.Source != pathreliability.SourceVOACAPOpen || got.HasP50 || got.ObservedDistributionAvailable || got.BeaconRX != beacon || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != s.now().Unix()+30 || got.VOACAPAgeSec != 42 {
				t.Fatalf("OPEN lost fractional-support provenance: %+v", got)
			}
		})
	}
}

func TestPathRecentSupportCapOffFallbackAvailability(t *testing.T) {
	for _, beacon := range []bool{false, true} {
		t.Run(fmt.Sprintf("beacon=%t", beacon), func(t *testing.T) {
			cfg, requested := recentConsumerConfig(t, pathreliability.ReceiverContributionOff), time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
			reference := requested.Add(30 * time.Second)
			s, c, sp := recentConsumerFixture(t, cfg, requested, "20m", "IO91", beacon)
			recentConsumerReports(t, s, c, sp, 5, 5, 1, -20, reference.Add(-180*time.Second), reference, false)
			s.pathClosedFallback = &countingClosedFallback{forecast: forecastWithReqSNRReliability(-34, 0.95), ok: true}
			got := recentConsumerPrediction(t, s, c, sp).result
			recentConsumerWantSupport(t, got.ObservedRecentSupport, 2.5, 0, pathreliability.ReceiverContributionOff)
			if got.Source != pathreliability.SourceVOACAPClosed || got.ObservedSamplingUnix != reference.Unix() || !got.HasObservedSamplingTime || !got.ObservedDistributionAvailable {
				t.Fatalf("cap-off availability lost through CLOSED: %+v", got)
			}
		})
	}
}

func TestPathRecentSupportObservedAuthority(t *testing.T) {
	for _, beacon := range []bool{false, true} {
		t.Run(fmt.Sprintf("beacon=%t", beacon), func(t *testing.T) {
			cfg, at := recentConsumerConfig(t, pathreliability.ReceiverContributionEnforce), time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
			s, c, sp := recentConsumerFixture(t, cfg, at, "20m", "IO91", beacon)
			reports, receivers := 24, 3
			if beacon {
				reports, receivers = 16, 2
			}
			recentConsumerReports(t, s, c, sp, reports, receivers, 1, -10, at, at, false)
			fallback := &countingClosedFallback{forecast: forecastWithReqSNRReliability(-34, 0.95), ok: true}
			s.pathClosedFallback = fallback
			got := recentConsumerPrediction(t, s, c, sp).result
			recentConsumerWantSupport(t, got.ObservedRecentSupport, float64(reports), float64(reports), pathreliability.ReceiverContributionEnforce)
			if got.Source != pathreliability.SourceCombined || got.P50DB != -9.5 || !got.HasP50 || fallback.calls != 0 {
				t.Fatalf("CLOSED forecast replaced sufficient recent observed evidence: %+v calls=%d", got, fallback.calls)
			}
		})
	}
}

func TestPathRecentSupportForecastDiagnosticFormat(t *testing.T) {
	for _, beacon := range []bool{false, true} {
		for _, tc := range []struct {
			source pathreliability.PredictionSource
			snr    int
			want   string
		}{
			{pathreliability.SourceVOACAPClosed, -34, "vcap|-34|h04|s112"},
			{pathreliability.SourceVOACAPOpen, -19, "vop|-19r95h04s112"},
			{pathreliability.SourceVOACAPAligned, -19, "valn|-20/-19h04s112"},
			{pathreliability.SourceVOACAPSparseUpgrade, -15, "vup|-20/-15r95s112"},
			{pathreliability.SourceNative160, 0, "n160|d100"},
		} {
			t.Run(fmt.Sprintf("source_%d/beacon=%t", tc.source, beacon), func(t *testing.T) {
				res := pathreliability.Result{Source: tc.source, Class: "LOW", P50DB: -19.5, HasP50: tc.source != pathreliability.SourceVOACAPOpen, BeaconRX: beacon, VOACAPFT8SNRDB: tc.snr, VOACAPHourUTC: 4, VOACAPSSN: 112, VOACAPReqSNRReliability: 0.95, VOACAPHasReqSNRReliability: true, Native160CivilDarkFraction: 1,
					ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 2.5, Admitted: 1.25, AdmittedAvailable: true, Enforced: true}}
				want := tc.want
				if beacon {
					want = "b" + want
				}
				if got := diagPathTag(pathPrediction{result: res}, true); got != want {
					t.Fatalf("forecast tag changed to carry observed support: %q, want %q", got, want)
				}
			})
		}
	}
}
