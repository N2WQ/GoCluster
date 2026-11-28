package pathreliability

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/internal/propprior"
	"dxcluster/internal/voacap"
)

func loadTestPropPriorModel(t testing.TB) *propprior.Model {
	t.Helper()
	model, err := propprior.LoadEmbedded()
	if err != nil {
		t.Fatalf("propprior.LoadEmbedded: %v", err)
	}
	return model
}

func testCellForGrid(grid string) CellID {
	var h uint16 = 1
	for i := range len(grid) {
		h = h*31 + uint16(grid[i])
	}
	if CellID(h) == InvalidCell {
		return 1
	}
	return CellID(h)
}

func testPropPriorConfig() Config {
	cfg := DefaultConfig()
	cfg.VOACAPFallback.Enabled = false
	fb := DefaultPropPriorFallbackConfig()
	cfg.PropPriorFallback = &fb
	if err := cfg.finalize(); err != nil {
		panic(err)
	}
	return cfg
}

func TestPropPriorFallbackConfigValidationAndMutualExclusivity(t *testing.T) {
	// 1) Optional-when-omitted: removing prop_prior_fallback from YAML still loads cleanly.
	omittedPath := writeTempConfigWithoutKey(t, "prop_prior_fallback")
	cfgOmitted, err := LoadFile(omittedPath)
	if err != nil {
		t.Fatalf("LoadFile with omitted prop_prior_fallback failed: %v", err)
	}
	if cfgOmitted.PropPriorFallback != nil || cfgOmitted.PropPriorFallbackActive() {
		t.Fatalf("expected nil/inactive PropPriorFallback when omitted, got %+v", cfgOmitted.PropPriorFallback)
	}

	// 2) Null section must be rejected.
	nullPath := writeTempConfigOverlay(t, "prop_prior_fallback: null\n")
	if _, err := LoadFile(nullPath); err == nil {
		t.Fatalf("expected error for null prop_prior_fallback section")
	}

	// 3) Mutual exclusivity with voacap_fallback.enabled.
	cfg := testPropPriorConfig()
	cfg.VOACAPFallback.Enabled = true
	if err := cfg.finalize(); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutual exclusivity error, got %v", err)
	}

	// 4) Enabled requires path_reliability.enabled.
	cfg = testPropPriorConfig()
	cfg.Enabled = false
	if err := cfg.finalize(); err == nil || !strings.Contains(err.Error(), "requires path reliability enabled") {
		t.Fatalf("expected requires path reliability enabled error, got %v", err)
	}

	// 5) Bounds & band validation.
	badCases := []struct {
		name   string
		mutate func(*PropPriorFallbackConfig)
	}{
		{"bucket_too_small", func(c *PropPriorFallbackConfig) { c.CacheBucketSeconds = 30 }},
		{"bucket_too_large", func(c *PropPriorFallbackConfig) { c.CacheBucketSeconds = 7200 }},
		{"capacity_not_pow2", func(c *PropPriorFallbackConfig) { c.CacheCapacity = 1000 }},
		{"capacity_too_small", func(c *PropPriorFallbackConfig) { c.CacheCapacity = 128 }},
		{"capacity_too_large", func(c *PropPriorFallbackConfig) { c.CacheCapacity = 524288 }},
		{"req_snr_nan", func(c *PropPriorFallbackConfig) { c.ReqSNRDB = math.NaN() }},
		{"req_snr_out_of_range", func(c *PropPriorFallbackConfig) { c.ReqSNRDB = -50 }},
		{"snr_offset_out_of_range", func(c *PropPriorFallbackConfig) { c.SNROffsetDB = 25 }},
		{"forecast_hours_zero", func(c *PropPriorFallbackConfig) { c.ForecastHours = 0 }},
		{"forecast_hours_25", func(c *PropPriorFallbackConfig) { c.ForecastHours = 25 }},
		{"band_160m_rejected", func(c *PropPriorFallbackConfig) { c.CenterFrequenciesMHz = []float64{1.9, 14.18} }},
		{"band_6m_rejected", func(c *PropPriorFallbackConfig) { c.CenterFrequenciesMHz = []float64{14.18, 50.3} }},
		{"duplicate_band", func(c *PropPriorFallbackConfig) { c.CenterFrequenciesMHz = []float64{14.1, 14.2} }},
		{"rel_non_monotonic", func(c *PropPriorFallbackConfig) { c.ReliabilityMinLow = 0.60; c.ReliabilityMinMedium = 0.50 }},
		{"sparse_max_negative", func(c *PropPriorFallbackConfig) { c.SparseP50DiagnosticMaxObservationCount = -1 }},
	}
	for _, tc := range badCases {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultPropPriorFallbackConfig()
			tc.mutate(&c)
			if err := c.finalize(); err == nil {
				t.Fatalf("case %s unexpectedly succeeded", tc.name)
			}
		})
	}
}

func TestPropPriorFallbackReciprocityNoiseAndGenerationInvalidation(t *testing.T) {
	model := loadTestPropPriorModel(t)
	store := propprior.NewSpaceWeatherStore()
	cfg := testPropPriorConfig()
	fb, err := NewPropPriorFallback(cfg, model, store)
	if err != nil {
		t.Fatalf("NewPropPriorFallback: %v", err)
	}

	now := time.Date(2026, 6, 15, 16, 0, 0, 0, time.UTC)
	userGrid := "FN20"
	dxGrid := "JO22"
	userCell := testCellForGrid(userGrid)
	dxCell := testCellForGrid(dxGrid)

	reqQuiet := VOACAPClosedRequest{
		UserCell:              userCell,
		DXCell:                dxCell,
		UserGrid:              userGrid,
		DXGrid:                dxGrid,
		Band:                  "20m",
		Mode:                  "CW",
		ReceiveNoisePenaltyDB: 0,
	}

	// CheckCachedForecast must be observation-only and miss on cold cache.
	if _, ok := fb.CheckCachedForecast(reqQuiet, now); ok {
		t.Fatalf("CheckCachedForecast hit on empty cache")
	}
	if snap := fb.Snapshot(now); snap.CacheEntries != 0 || snap.CacheHits != 0 || snap.CacheMisses != 0 {
		t.Fatalf("CheckCachedForecast mutated state: %+v", snap)
	}

	// First evaluation misses cache and populates one reciprocal entry.
	check1 := fb.CheckForecastDetailed(reqQuiet, now)
	if check1.Status != VOACAPForecastCheckReady || !check1.CacheMiss || !check1.Forecast.PropPrior {
		t.Fatalf("unexpected first check: %+v", check1)
	}
	relQuiet, ok := check1.Forecast.ReqSNRReliability()
	if !ok || relQuiet < 0 || relQuiet > 1 {
		t.Fatalf("invalid REL: %v ok=%v", relQuiet, ok)
	}

	// Reverse direction with DIFFERENT SET NOISE (URBAN = 17 dB) MUST hit the
	// exact same reciprocal cache entry and apply noise ONLY to ReceiveDB
	// (and 0.6*penalty to EffectiveDB), leaving TransmitDB and REL untouched.
	reqReverseUrban := VOACAPClosedRequest{
		UserCell:              dxCell,
		DXCell:                userCell,
		UserGrid:              dxGrid,
		DXGrid:                userGrid,
		Band:                  "20m",
		Mode:                  "CW",
		ReceiveNoisePenaltyDB: 17.0,
	}
	check2 := fb.CheckForecastDetailed(reqReverseUrban, now.Add(2*time.Minute))
	if check2.Status != VOACAPForecastCheckReady || check2.CacheMiss {
		t.Fatalf("reverse direction did not hit reciprocal cache: %+v", check2)
	}
	if math.Abs(check2.Forecast.TransmitDB()-check1.Forecast.TransmitDB()) > 1e-4 {
		t.Fatalf("TransmitDB changed across noise classes: quiet=%v urban=%v", check1.Forecast.TransmitDB(), check2.Forecast.TransmitDB())
	}
	if math.Abs((check1.Forecast.ReceiveDB()-check2.Forecast.ReceiveDB())-17.0) > 1e-4 {
		t.Fatalf("ReceiveDB delta = %v, want 17.0", check1.Forecast.ReceiveDB()-check2.Forecast.ReceiveDB())
	}
	wantEffDelta := cfg.MergeReceiveWeight * 17.0
	if math.Abs((check1.Forecast.EffectiveDB()-check2.Forecast.EffectiveDB())-wantEffDelta) > 1e-4 {
		t.Fatalf("EffectiveDB delta = %v, want %v", check1.Forecast.EffectiveDB()-check2.Forecast.EffectiveDB(), wantEffDelta)
	}
	relUrban, _ := check2.Forecast.ReqSNRReliability()
	if math.Abs(relUrban-relQuiet) > 1e-6 {
		t.Fatalf("ReqSNRReliability drifted with SET NOISE: quiet=%v urban=%v", relQuiet, relUrban)
	}

	// CheckCachedForecast now hits without mutating hit counters.
	hitsBefore := fb.Snapshot(now).CacheHits
	if cached, ok := fb.CheckCachedForecast(reqQuiet, now); !ok || math.Abs(cached.EffectiveDB()-check1.Forecast.EffectiveDB()) > 1e-4 {
		t.Fatalf("CheckCachedForecast failed on populated entry: ok=%v got=%+v", ok, cached)
	}
	if fb.Snapshot(now).CacheHits != hitsBefore {
		t.Fatalf("CheckCachedForecast mutated CacheHits")
	}

	// Advance SpaceWeatherStore generation with severe storm -> immediate cache
	// invalidation on the very next lookup inside the same 5m time bucket!
	for i := range 24 {
		ts := now.Add(-time.Duration(24-i) * time.Hour)
		store.RecordHp30(ts, 7.5, "gfz_hp30")
		store.RecordHp30(ts.Add(30*time.Minute), 7.5, "gfz_hp30")
		store.RecordDst(ts, -130.0, "kyoto")
		store.RecordBzGSM(ts, -16.0, "rtsw")
		store.RecordVsw(ts, 750.0, "rtsw")
	}
	checkStorm := fb.CheckForecastDetailed(reqQuiet, now)
	if checkStorm.Status != VOACAPForecastCheckReady || !checkStorm.CacheMiss {
		t.Fatalf("space-weather generation update did not invalidate cache: %+v", checkStorm)
	}
	if checkStorm.Forecast.EffectiveDB() >= check1.Forecast.EffectiveDB() {
		t.Fatalf("expected storm degradation on transatlantic/high-mid path: quiet=%.2f storm=%.2f",
			check1.Forecast.EffectiveDB(), checkStorm.Forecast.EffectiveDB())
	}
}

func TestPropPriorFallbackBandGating160mPrecedenceAndSNROffset(t *testing.T) {
	model := loadTestPropPriorModel(t)
	cfg := testPropPriorConfig()
	cfg.PropPriorFallback.SNROffsetDB = 3.5
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true

	fb, err := NewPropPriorFallback(cfg, model, nil)
	if err != nil {
		t.Fatalf("NewPropPriorFallback: %v", err)
	}

	now := time.Date(2026, 6, 15, 16, 0, 0, 0, time.UTC)
	uCell := testCellForGrid("FN20")
	dCell := testCellForGrid("JO22")

	// 160m and 6m MUST be rejected with InvalidUnsupportedBand so native_160m_fallback
	// retains precedence on 160m and VHF never extrapolates.
	for _, band := range []string{"160m", "6m", "2m"} {
		chk := fb.CheckForecastDetailed(VOACAPClosedRequest{
			UserCell: uCell, DXCell: dCell, UserGrid: "FN20", DXGrid: "JO22", Band: band, Mode: "CW",
		}, now)
		if chk.Status != VOACAPForecastCheckInvalidRequest || chk.InvalidReason != VOACAPInvalidUnsupportedBand {
			t.Fatalf("band %s: status=%s reason=%s, want invalid_request/invalid_unsupported_band", band, chk.Status, chk.InvalidReason)
		}
	}

	// Verify 160m falls through to Native160FallbackResult cleanly.
	res160 := Native160FallbackResult(Result{Source: SourceInsufficient}, cfg, VOACAPClosedRequest{
		UserCell: uCell, DXCell: dCell, UserGrid: "FN20", DXGrid: "JO22", Band: "160m", Mode: "CW",
	}, now)
	if res160.Source != SourceNative160 {
		t.Fatalf("expected native 160m fallback after propprior declined 160m, got source=%v", res160.Source)
	}

	// Verify snr_offset_db shifts SNR by exact +3.5 dB compared to offset 0.
	cfgZero := testPropPriorConfig()
	fbZero, err := NewPropPriorFallback(cfgZero, model, nil)
	if err != nil {
		t.Fatalf("NewPropPriorFallback zero: %v", err)
	}
	req := VOACAPClosedRequest{UserCell: uCell, DXCell: dCell, UserGrid: "FN20", DXGrid: "JO22", Band: "20m", Mode: "CW"}
	gotZero := fbZero.CheckForecastDetailed(req, now)
	gotOff := fb.CheckForecastDetailed(req, now)
	if math.Abs((gotOff.Forecast.EffectiveDB()-gotZero.Forecast.EffectiveDB())-3.5) > 1e-4 {
		t.Fatalf("snr_offset_db delta = %v, want 3.5", gotOff.Forecast.EffectiveDB()-gotZero.Forecast.EffectiveDB())
	}

	// Verify invalid grids/cells return clean invalid reasons without panic.
	badGrid := fb.CheckForecastDetailed(VOACAPClosedRequest{UserCell: uCell, DXCell: dCell, UserGrid: "BAD", DXGrid: "JO22", Band: "20m"}, now)
	if badGrid.Status != VOACAPForecastCheckInvalidRequest || badGrid.InvalidReason != VOACAPInvalidUserGrid {
		t.Fatalf("bad user grid: %+v", badGrid)
	}
	badCell := fb.CheckForecastDetailed(VOACAPClosedRequest{UserCell: InvalidCell, DXCell: dCell, UserGrid: "FN20", DXGrid: "JO22", Band: "20m"}, now)
	if badCell.Status != VOACAPForecastCheckInvalidRequest || badCell.InvalidReason != VOACAPInvalidUserCell {
		t.Fatalf("bad user cell: %+v", badCell)
	}
}

func TestPropPriorFallbackBoundedCacheChurnAndConcurrency(t *testing.T) {
	model := loadTestPropPriorModel(t)
	store := propprior.NewSpaceWeatherStore()
	cfg := testPropPriorConfig()
	cfg.PropPriorFallback.CacheCapacity = 256 // Smallest valid power-of-2 capacity to force heavy eviction.
	fb, err := NewPropPriorFallback(cfg, model, store)
	if err != nil {
		t.Fatalf("NewPropPriorFallback: %v", err)
	}

	now := time.Date(2026, 6, 15, 16, 0, 0, 0, time.UTC)
	// Insert 100,000 distinct cache keys directly to verify strict bounded occupancy & eviction accounting.
	for i := range 100_000 {
		key := propPriorCacheKey{
			loCell:     CellID(1 + (i % 5000)),
			hiCell:     CellID(1 + ((i * 37) % 5000)),
			bandIdx:    uint8(i % propPriorBandCount),
			timeBucket: int64(i / 5000),
		}
		if key.loCell > key.hiCell {
			key.loCell, key.hiCell = key.hiCell, key.loCell
		}
		fb.cache.store(key, 1, -10.0, 5.0, false)
	}
	snap := fb.Snapshot(now)
	if snap.CacheEntries > snap.CacheCapacity {
		t.Fatalf("cache entries %d exceeded capacity %d", snap.CacheEntries, snap.CacheCapacity)
	}
	if snap.CacheEvictions == 0 {
		t.Fatalf("expected non-zero evictions under 100k churn into 256-slot cache")
	}

	// Concurrent writer (SpaceWeatherStore batches) + readers (CheckForecastDetailed + CheckForecastWindow) under -race.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 200 {
			select {
			case <-stop:
				return
			default:
				store.RecordHp30Batch([]propprior.Sample{{BinStartUTC: now.Add(-time.Hour), Value: 1.5 + float64(i%5)}}, "test")
			}
		}
	}()
	for g := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			bands := []string{"80m", "40m", "20m", "15m", "10m"}
			for i := range 100 {
				req := VOACAPClosedRequest{
					UserCell: testCellForGrid("FN20"),
					DXCell:   testCellForGrid("JO22"),
					UserGrid: "FN20",
					DXGrid:   "JO22",
					Band:     bands[(id+i)%len(bands)],
					Mode:     "CW",
				}
				chk := fb.CheckForecastDetailed(req, now)
				if chk.Status != VOACAPForecastCheckReady {
					t.Errorf("concurrent CheckForecastDetailed failed: %+v", chk)
					return
				}
				if i%25 == 0 {
					w, st := fb.CheckForecastWindowWait(req, now, 50*time.Millisecond)
					if st != VOACAPForecastWindowReady || len(w.Records) != 24 {
						t.Errorf("concurrent CheckForecastWindowWait failed: st=%v len=%d", st, len(w.Records))
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(stop)
}

func TestPropPriorFallbackDiurnalPhysicsAndOfflineCalibrationMatrix(t *testing.T) {
	model := loadTestPropPriorModel(t)
	quietStore := propprior.NewSpaceWeatherStore()
	stormStore := propprior.NewSpaceWeatherStore()

	dayUTC := time.Date(2026, 3, 21, 16, 0, 0, 0, time.UTC)
	nightUTC := time.Date(2026, 3, 21, 4, 0, 0, 0, time.UTC)

	// Hydrate quiet (SFI=150, Hp30=1.5, Dst=-5) vs severe storm (SFI=150, Hp30=7.5, Dst=-140, Bz=-15).
	for i := range 48 {
		ts30 := dayUTC.Add(-time.Duration(48-i) * 30 * time.Minute)
		quietStore.RecordHp30(ts30, 1.5, "gfz")
		stormStore.RecordHp30(ts30, 7.5, "gfz")
	}
	for i := range 24 {
		ts1h := dayUTC.Add(-time.Duration(24-i) * time.Hour)
		quietStore.RecordDst(ts1h, -5, "kyoto")
		quietStore.RecordBzGSM(ts1h, 2, "rtsw")
		quietStore.RecordVsw(ts1h, 380, "rtsw")
		stormStore.RecordDst(ts1h, -140, "kyoto")
		stormStore.RecordBzGSM(ts1h, -15, "rtsw")
		stormStore.RecordVsw(ts1h, 750, "rtsw")
	}
	quietStore.RecordSFI(dayUTC.AddDate(0, 0, -1), 150, "noaa")
	stormStore.RecordSFI(dayUTC.AddDate(0, 0, -1), 150, "noaa")

	cfg := testPropPriorConfig()
	fbQuiet, err := NewPropPriorFallback(cfg, model, quietStore)
	if err != nil {
		t.Fatalf("NewPropPriorFallback quiet: %v", err)
	}
	fbStorm, err := NewPropPriorFallback(cfg, model, stormStore)
	if err != nil {
		t.Fatalf("NewPropPriorFallback storm: %v", err)
	}

	// Physics sanity checks:
	// 1) 10m transatlantic (FN20 -> JO22) must be significantly stronger in daytime (16Z) than night (04Z).
	req10m := VOACAPClosedRequest{UserCell: testCellForGrid("FN20"), DXCell: testCellForGrid("JO22"), UserGrid: "FN20", DXGrid: "JO22", Band: "10m", Mode: "CW"}
	day10 := fbQuiet.CheckForecastDetailed(req10m, dayUTC).Forecast.EffectiveDB()
	night10 := fbQuiet.CheckForecastDetailed(req10m, nightUTC).Forecast.EffectiveDB()
	if day10 <= night10+5.0 {
		t.Fatalf("expected 10m day > night+5dB: day=%.2f night=%.2f", day10, night10)
	}

	// 2) 80m transatlantic (FN20 -> JO22) must be stronger at night (04Z) than mid-day (16Z).
	req80m := VOACAPClosedRequest{UserCell: testCellForGrid("FN20"), DXCell: testCellForGrid("JO22"), UserGrid: "FN20", DXGrid: "JO22", Band: "80m", Mode: "CW"}
	day80 := fbQuiet.CheckForecastDetailed(req80m, dayUTC).Forecast.EffectiveDB()
	night80 := fbQuiet.CheckForecastDetailed(req80m, nightUTC).Forecast.EffectiveDB()
	if night80 <= day80+5.0 {
		t.Fatalf("expected 80m night > day+5dB: night=%.2f day=%.2f", night80, day80)
	}

	// 3) High-latitude / polar path (FN20 -> KP20 Helsinki) must degrade under severe storm on 20m.
	reqPolar := VOACAPClosedRequest{UserCell: testCellForGrid("FN20"), DXCell: testCellForGrid("KP20"), UserGrid: "FN20", DXGrid: "KP20", Band: "20m", Mode: "CW"}
	polarQuiet := fbQuiet.CheckForecastDetailed(reqPolar, dayUTC).Forecast.EffectiveDB()
	polarStorm := fbStorm.CheckForecastDetailed(reqPolar, dayUTC).Forecast.EffectiveDB()
	if polarStorm >= polarQuiet-2.0 {
		t.Fatalf("expected polar 20m storm degradation >= 2 dB: quiet=%.2f storm=%.2f", polarQuiet, polarStorm)
	}

	// 4) Parse reference VOACAP method-20 fixture and log deterministic canonical calibration table.
	voacapRaw, err := os.ReadFile(filepath.Join("..", "internal", "voacap", "testdata", "voacap_output_method20.txt"))
	if err != nil {
		t.Fatalf("read voacap_output_method20.txt: %v", err)
	}
	parsedVOACAP, err := voacap.ParsePredictions(voacapRaw)
	if err != nil || len(parsedVOACAP) == 0 {
		t.Fatalf("ParsePredictions failed: len=%d err=%v", len(parsedVOACAP), err)
	}

	type pathCase struct {
		name, uGrid, dGrid string
	}
	paths := []pathCase{
		{"Transatlantic", "FN20", "JO22"},
		{"Polar", "FN20", "KP20"},
		{"Transpacific", "CM87", "PM95"},
		{"Regional-NVIS", "EM73", "FN20"},
	}
	bands := []string{"80m", "40m", "20m", "15m", "10m"}
	var summary strings.Builder
	summary.WriteString("\nOffline PropPrior Canonical Calibration Matrix (CW QUIET, SFI=150):\n")
	summary.WriteString("Path           Band  DayQuiet(dB/G/REL)   NightQuiet(dB/G/REL)  DayStorm(dB/G/REL)\n")
	for _, p := range paths {
		for _, b := range bands {
			req := VOACAPClosedRequest{
				UserCell: testCellForGrid(p.uGrid), DXCell: testCellForGrid(p.dGrid),
				UserGrid: p.uGrid, DXGrid: p.dGrid, Band: b, Mode: "CW",
			}
			dq := fbQuiet.CheckForecastDetailed(req, dayUTC).Forecast
			nq := fbQuiet.CheckForecastDetailed(req, nightUTC).Forecast
			ds := fbStorm.CheckForecastDetailed(req, dayUTC).Forecast
			dqRel, _ := dq.ReqSNRReliability()
			nqRel, _ := nq.ReqSNRReliability()
			dsRel, _ := ds.ReqSNRReliability()
			if math.IsNaN(dq.EffectiveDB()) || math.IsNaN(nq.EffectiveDB()) || math.IsNaN(ds.EffectiveDB()) {
				t.Fatalf("non-finite calibration entry for %s %s", p.name, b)
			}
			fmt.Fprintf(&summary, "%-14s %-4s  %6.1f/%s/%0.2f         %6.1f/%s/%0.2f          %6.1f/%s/%0.2f\n",
				p.name, b,
				dq.EffectiveDB(), GlyphForDB(dq.EffectiveDB(), "CW", cfg), dqRel,
				nq.EffectiveDB(), GlyphForDB(nq.EffectiveDB(), "CW", cfg), nqRel,
				ds.EffectiveDB(), GlyphForDB(ds.EffectiveDB(), "CW", cfg), dsRel,
			)
		}
	}
	// Reference check against parsed VOACAP method-20 hour 1 records (band SNR/REL range sanity).
	fmt.Fprintf(&summary, "VOACAP method20 fixture parsed %d hourly rows (hour 1 %.1f MHz FT8-equiv SNR=%d dB, REL=%.2f)\n",
		len(parsedVOACAP), parsedVOACAP[0].FrequencyMHz, parsedVOACAP[0].FT8SNRDB, parsedVOACAP[0].Reliability)
	t.Log(summary.String())
}

func TestPropPriorFallbackShowPropWindowInstant24h(t *testing.T) {
	model := loadTestPropPriorModel(t)
	cfg := testPropPriorConfig()
	fb, err := NewPropPriorFallback(cfg, model, nil)
	if err != nil {
		t.Fatalf("NewPropPriorFallback: %v", err)
	}
	now := time.Date(2026, 6, 15, 16, 22, 0, 0, time.UTC)
	req := VOACAPClosedRequest{
		UserCell:              testCellForGrid("FN20"),
		DXCell:                testCellForGrid("JO22"),
		UserGrid:              "FN20",
		DXGrid:                "JO22",
		Band:                  "20m",
		Mode:                  "CW",
		ReceiveNoisePenaltyDB: 4.0,
	}
	win, status := fb.CheckForecastWindowWait(req, now, 750*time.Millisecond)
	if status != VOACAPForecastWindowReady {
		t.Fatalf("CheckForecastWindowWait status=%v, want Ready", status)
	}
	if len(win.Records) != 24 {
		t.Fatalf("window len=%d, want 24", len(win.Records))
	}
	for i, rec := range win.Records {
		wantHour := (16 + i) % 24
		if rec.Record.HourUTC != wantHour {
			t.Fatalf("record[%d].HourUTC=%d, want %d", i, rec.Record.HourUTC, wantHour)
		}
		if !rec.PropPrior {
			t.Fatalf("record[%d].PropPrior=false, want true", i)
		}
	}
}

var benchPropPriorCheckSink VOACAPForecastCheck

func BenchmarkPropPriorFallbackCacheHit(b *testing.B) {
	model := loadTestPropPriorModel(b)
	cfg := testPropPriorConfig()
	fb, err := NewPropPriorFallback(cfg, model, nil)
	if err != nil {
		b.Fatalf("NewPropPriorFallback: %v", err)
	}
	now := time.Date(2026, 6, 15, 16, 0, 0, 0, time.UTC)
	req := VOACAPClosedRequest{
		UserCell:              testCellForGrid("FN20"),
		DXCell:                testCellForGrid("JO22"),
		UserGrid:              "FN20",
		DXGrid:                "JO22",
		Band:                  "20m",
		Mode:                  "CW",
		ReceiveNoisePenaltyDB: 4.0,
	}
	// Warm the reciprocal entry once.
	if chk := fb.CheckForecastDetailed(req, now); chk.Status != VOACAPForecastCheckReady {
		b.Fatalf("warmup failed: %+v", chk)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var res VOACAPForecastCheck
	for range b.N {
		res = fb.CheckForecastDetailed(req, now)
	}
	benchPropPriorCheckSink = res
}

func BenchmarkPropPriorFallbackCacheMiss(b *testing.B) {
	model := loadTestPropPriorModel(b)
	cfg := testPropPriorConfig()
	fb, err := NewPropPriorFallback(cfg, model, nil)
	if err != nil {
		b.Fatalf("NewPropPriorFallback: %v", err)
	}
	base := time.Date(2026, 6, 15, 16, 0, 0, 0, time.UTC)
	req := VOACAPClosedRequest{
		UserCell:              testCellForGrid("FN20"),
		DXCell:                testCellForGrid("JO22"),
		UserGrid:              "FN20",
		DXGrid:                "JO22",
		Band:                  "20m",
		Mode:                  "CW",
		ReceiveNoisePenaltyDB: 4.0,
	}

	b.ReportAllocs()
	b.ResetTimer()
	var res VOACAPForecastCheck
	for i := range b.N {
		// Advance time bucket by 5m on every iteration to force a true cache miss.
		res = fb.CheckForecastDetailed(req, base.Add(time.Duration(i)*5*time.Minute))
	}
	benchPropPriorCheckSink = res
}
