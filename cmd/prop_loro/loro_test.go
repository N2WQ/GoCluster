package main

import (
	"compress/gzip"
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/propprior"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
)

type stubVOACAP struct {
	fn    func(userCell, dxCell pathreliability.CellID, band string, at time.Time, ssn int) (pathreliability.VOACAPCachedForecast, bool, error)
	stats voacapEvalStats
}

func (s *stubVOACAP) Forecast(
	_ context.Context,
	userCell, dxCell pathreliability.CellID,
	_, _, _, _ float64,
	band string,
	at time.Time,
	ssn int,
	_ pathreliability.Config,
) (pathreliability.VOACAPCachedForecast, bool, error) {
	if s.fn == nil {
		return pathreliability.VOACAPCachedForecast{}, false, nil
	}
	return s.fn(userCell, dxCell, band, at, ssn)
}

func (s *stubVOACAP) Stats() voacapEvalStats { return s.stats }
func (s *stubVOACAP) Close() error           { return nil }

func TestGeoGoldenVectors(t *testing.T) {
	// Golden vector 1: FN31 (New England) <-> PM95 (Tokyo): ~10,850 km, polar, NA<->JA.
	fnLat, fnLon, ok := pathreliability.GridCenterLatLon("FN31")
	if !ok {
		t.Fatal("FN31 parse failed")
	}
	pmLat, pmLon, ok := pathreliability.GridCenterLatLon("PM95")
	if !ok {
		t.Fatal("PM95 parse failed")
	}
	pfNAJA := computePathFeatures(fnLat, fnLon, pmLat, pmLon)
	if pfNAJA.distKM < 10500 || pfNAJA.distKM > 11200 {
		t.Fatalf("FN31<->PM95 distKM=%.1f, want ~10850 km", pfNAJA.distKM)
	}
	if !pfNAJA.isLongHaul || !pfNAJA.isPolar || !pfNAJA.isNAJA {
		t.Fatalf("FN31<->PM95 expected longHaul=true polar=true naJA=true, got %+v", pfNAJA)
	}

	// Golden vector 2: FN31 <-> EM12 (Texas): ~2,350 km, non-polar, not NA<->JA.
	emLat, emLon, ok := pathreliability.GridCenterLatLon("EM12")
	if !ok {
		t.Fatal("EM12 parse failed")
	}
	pfShort := computePathFeatures(fnLat, fnLon, emLat, emLon)
	if pfShort.distKM < 2100 || pfShort.distKM > 2600 {
		t.Fatalf("FN31<->EM12 distKM=%.1f, want ~2350 km", pfShort.distKM)
	}
	if pfShort.isLongHaul || pfShort.isPolar || pfShort.isNAJA {
		t.Fatalf("FN31<->EM12 expected longHaul=false polar=false naJA=false, got %+v", pfShort)
	}
}

func TestCausalSpaceWeatherZeroLookahead(t *testing.T) {
	dir := t.TempDir()
	swFile := filepath.Join(dir, "spaceweather-2026-11-28.jsonl")
	content := strings.Join([]string{
		`{"ts_unix":100,"ts_utc":"2026-11-28T00:01:40Z","reason":"gen_change","gen":1,"sfi_now":91,"sfi_72h_mean":90,"hp30_lag1":2.0,"hp30_12h_max":2.0,"hp30_24h_mean":2.0,"bz_south_3h":0,"vsw_3h":400,"dst_now":0,"dst_24h_min":0,"voacap_ssn":{"available":true,"ssn":36}}`,
		`{"ts_unix":110,"ts_utc":"2026-11-28T00:01:50Z","reason":"gen_change","gen":2,"sfi_now":180,"sfi_72h_mean":175,"hp30_lag1":6.0,"hp30_12h_max":6.0,"hp30_24h_mean":4.0,"bz_south_3h":12,"vsw_3h":650,"dst_now":-80,"dst_24h_min":-90,"voacap_ssn":{"available":true,"ssn":110}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(swFile, []byte(content), 0o644); err != nil {
		t.Fatalf("write sw: %v", err)
	}
	tl, _, err := loadSpaceWeatherTimeline(dir)
	if err != nil {
		t.Fatalf("loadSpaceWeatherTimeline: %v", err)
	}

	if _, ok := tl.lookupCausal(99); ok {
		t.Fatal("expected no snapshot before t=100")
	}
	s105, ok := tl.lookupCausal(105)
	if !ok || s105.SFINow != 91 || s105.VOACAPSSN.SSN != 36 {
		t.Fatalf("t=105 must select t=100 (SFI=91, SSN=36) with zero lookahead; got %+v ok=%v", s105, ok)
	}
	s110, ok := tl.lookupCausal(110)
	if !ok || s110.SFINow != 180 || s110.VOACAPSSN.SSN != 110 {
		t.Fatalf("t=110 must select t=110 (SFI=180, SSN=110); got %+v ok=%v", s110, ok)
	}
}

func TestMetricGoldenVectorsAndRelAUC(t *testing.T) {
	var acc modelAccum
	// 4 synthetic FT8 observations with exact known residuals: +2, -4, +6, 0 dB.
	// MAE = (2+4+6+0)/4 = 3.0; RMSE = sqrt((4+16+36+0)/4) = sqrt(14) ~= 3.742;
	// MeanBias = (2-4+6+0)/4 = +1.0; FalseClosed = 1/4 = 0.25; z<=1: 3/4=0.75 (with sigma=4).
	acc.add(-10, -10, "HIGH", -12, "HIGH", false, true, 4.0)
	acc.add(-14, -14, "MEDIUM", -10, "HIGH", false, true, 4.0)
	acc.add(-12, -12, "HIGH", -18, "LOW", false, true, 4.0)
	acc.add(-20, -20, "LOW", -20, "CLOSED", true, false, 4.0)

	s := acc.summary()
	if s.Count != 4 || s.MAE != 3.0 || math.Abs(s.RMSE-3.742) > 0.002 || s.MeanBias != 1.0 {
		t.Fatalf("unexpected golden metrics: %+v", s)
	}
	if s.FalseClosedRate != 0.25 || s.ZAbsLE1Rate != 0.75 || s.ZAbsLE2Rate != 1.0 {
		t.Fatalf("unexpected FC/z rates: %+v", s)
	}

	// Perfect separation ROC-AUC = 1.0; Brier = (0.1^2 + 0.2^2 + 0.1^2 + 0.2^2)/4 = 0.025.
	var rel relCalibAccum
	rel.add(0.90, true)
	rel.add(0.80, true)
	rel.add(0.10, false)
	rel.add(0.20, false)
	rs := rel.summary()
	if rs.ROCAUC != 1.0 || math.Abs(rs.BrierScore-0.025) > 1e-4 {
		t.Fatalf("unexpected Rel AUC/Brier: %+v", rs)
	}
}

func TestLOROZeroLeakageAndMatchedPairFairness(t *testing.T) {
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatalf("InitH3MappingsFromDir: %v", err)
	}
	model, err := propprior.LoadEmbedded()
	if err != nil {
		t.Fatalf("LoadEmbedded: %v", err)
	}
	pathCfg := pathreliability.DefaultConfig()
	pathCfg.Enabled = true
	pathCfg.MinObservationCount = 3
	pathCfg.ReceiverContributionMode = pathreliability.ReceiverContributionOff
	ppFB := pathreliability.DefaultPropPriorFallbackConfig()
	ppFB.Enabled = true
	pathCfg.PropPriorFallback = &ppFB

	// Two space-weather records: gen=0 cold at t0, gen=1 live at t0+60.
	t0 := time.Date(2026, 11, 28, 0, 0, 0, 0, time.UTC).Unix()
	tl := &swTimeline{
		snaps: []swSnapshot{
			{TSUnix: t0, Gen: 0, SFINow: 120, SFI72hMean: 120, Hp30Lag1: 2, Hp3012hMax: 2, Hp3024hMean: 2, Vsw3h: 400},
			{
				TSUnix: t0 + 60, Gen: 1, SFINow: 110, SFI72hMean: 108, Hp30Lag1: 2.5, Hp3012hMax: 3, Hp3024hMean: 2, Vsw3h: 420,
				VOACAPSSN: struct {
					Available bool   `json:"available"`
					SSN       int    `json:"ssn"`
					Source    string `json:"source"`
				}{Available: true, SSN: 40, Source: "ssn_state_file"},
			},
		},
	}

	// Stub VOACAP fails on 10m to prove strict matched-pair exclusion.
	stub := &stubVOACAP{
		fn: func(_, _ pathreliability.CellID, band string, at time.Time, ssn int) (pathreliability.VOACAPCachedForecast, bool, error) {
			if band == "10m" {
				return pathreliability.VOACAPCachedForecast{}, false, nil
			}
			return buildVOACAPCachedForecast(
				voacapHourCell{FT8SNRDB: -12, Rel: 0.90, HasRel: true, Valid: true},
				voacapHourCell{FT8SNRDB: -14, Rel: 0.88, HasRel: true, Valid: true},
				at.UTC().Hour(), 14.18, ssn, pathCfg,
			), true, nil
		},
		stats: voacapEvalStats{MaxPairsCap: 10, UniquePairsEvaluated: 10, UniquePairsSkipped: 3, SpotsSkippedCap: 5, TruncatedByCap: true},
	}

	eng, err := newLOROEngine(loroConfig{
		HoldoutUnit:   "receiver",
		Folds:         5,
		Fold:          0,
		WarmupSeconds: 0,
		ExcludeColdSW: true,
	}, pathCfg, model, stub, tl)
	if err != nil {
		t.Fatalf("newLOROEngine: %v", err)
	}

	ctx := context.Background()
	// 1. Cold-SW spot from held-out receiver (de_hash=5 -> 5%5==0) must be skipped.
	_ = eng.processSpot(ctx, capturedSpot{
		observedUnix: t0 + 10, arrivedUnix: t0 + 11,
		band: "20m", mode: "CW", source: "RBN", rawSNRDB: 10, ft8DB: 3,
		deGrid: "FN31", dxGrid: "PM95", deCall: "K1HOLD", deHash: 5,
	})
	// 2. 10 live spots ONLY from held-out receiver on 20m FN31<->PM95:
	// Because held-out receiver MUST NEVER enter Predictor, all 10 must hit fallback_no_p50 (zero leakage!).
	for i := range 10 {
		_ = eng.processSpot(ctx, capturedSpot{
			observedUnix: t0 + 100 + int64(i*5), arrivedUnix: t0 + 101 + int64(i*5),
			band: "20m", mode: "CW", source: "RBN", rawSNRDB: 12, ft8DB: 5,
			deGrid: "FN31", dxGrid: "PM95", deCall: "K1HOLD", deHash: 5,
		})
	}
	// 3. 1 held-out spot on 10m where VOACAP returns unavailable -> must be counted unmatched, not bias matched count!
	_ = eng.processSpot(ctx, capturedSpot{
		observedUnix: t0 + 200, arrivedUnix: t0 + 201,
		band: "10m", mode: "CW", source: "RBN", rawSNRDB: 5, ft8DB: -2,
		deGrid: "FN31", dxGrid: "PM95", deCall: "K1HOLD", deHash: 5,
	})

	sc, err := eng.finalize(ctx, "mem", nil, nil, spotReadStats{RowsRead: 12})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if sc.Manifest.ColdSWSkipped != 1 {
		t.Fatalf("ColdSWSkipped = %d, want 1", sc.Manifest.ColdSWSkipped)
	}
	if sc.Manifest.TrainUpdates != 0 {
		t.Fatalf("TrainUpdates = %d, want 0 (held-out receiver must never train)", sc.Manifest.TrainUpdates)
	}
	if sc.Manifest.HeldOutHFMatched != 10 || sc.Manifest.HeldOutUnmatched != 1 {
		t.Fatalf("matched=%d unmatched=%d, want 10 and 1", sc.Manifest.HeldOutHFMatched, sc.Manifest.HeldOutUnmatched)
	}
	if sc.RegimeBCombined.MatchedCount != 0 || sc.RegimeBFallbackNoP50.MatchedCount != 10 {
		t.Fatalf("LORO leakage detected: combined=%d fallback_no_p50=%d, want 0 and 10",
			sc.RegimeBCombined.MatchedCount, sc.RegimeBFallbackNoP50.MatchedCount)
	}
	if sc.LongHaulPolar["20m_na_ja"].MatchedCount != 10 {
		t.Fatalf("20m_na_ja matched=%d, want 10", sc.LongHaulPolar["20m_na_ja"].MatchedCount)
	}

	// Verify prominent truncation banner in Markdown when cap truncated pairs.
	md := renderScorecardMarkdown(sc)
	if !strings.Contains(md, "WARNING — PAIR CAP TRUNCATION ACTIVE") {
		t.Fatalf("expected truncation warning banner in scorecard.md:\n%s", md)
	}
}

func TestStreamSpotFilesConcatenatedGzipAndHeaders(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spots-2026-11-28-00.csv.gz")

	writeMember := func(flags int, body string) {
		f, err := os.OpenFile(path, flags, 0o644)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		gz := gzip.NewWriter(f)
		if _, err := gz.Write([]byte(body)); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = gz.Close()
		_ = f.Close()
	}
	hdr := "observed_unix,arrived_unix,band,mode,source,raw_snr_db,ft8_db,de_grid,dx_grid,de_call,de_hash,is_beacon,de_grid_derived,dx_grid_derived\n"
	writeMember(os.O_CREATE|os.O_TRUNC|os.O_WRONLY, hdr+"1795827500,1795827501,20m,CW,RBN,15,8.00,FN31,PM95,K1TTT,100,0,0,0\n")
	writeMember(os.O_CREATE|os.O_APPEND|os.O_WRONLY, hdr+"1795827490,1795827491,40m,FT8,PSK,-12,-12.00,FN31,JO22,W1AW,200,0,0,0\n")

	var got []capturedSpot
	stats, err := streamSpotFiles([]string{path}, 0, 0, func(rec capturedSpot) error {
		got = append(got, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("streamSpotFiles: %v", err)
	}
	if stats.RowsRead != 2 || stats.RowsBad != 0 || len(got) != 2 {
		t.Fatalf("unexpected stats/rows: stats=%+v len=%d", stats, len(got))
	}
	// Must be sorted chronologically ascending by observed_unix.
	if got[0].observedUnix != 1795827490 || got[1].observedUnix != 1795827500 {
		t.Fatalf("spots not chronologically sorted: %+v", got)
	}
}
