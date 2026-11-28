package propprior

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestSpaceWeatherColdQuietDefaults(t *testing.T) {
	now := time.Date(2025, 3, 21, 12, 0, 0, 0, time.UTC)

	var nilStore *SpaceWeatherStore
	snapNil := nilStore.Evaluate(now)
	if snapNil.Features.SFINow != DefaultSFIQuiet || snapNil.Features.Hp30Lag1 != QuietHp30 || snapNil.Features.Vsw3h != QuietVswKMS {
		t.Fatalf("nil store quiet defaults mismatch: %+v", snapNil)
	}

	store := NewSpaceWeatherStore()
	snap := store.Evaluate(now)
	if snap.SFI.HasValid || snap.Hp30.HasValid || snap.Bz.HasValid || snap.Vsw.HasValid || snap.Dst.HasValid {
		t.Fatalf("empty store should report HasValid=false on all feeds: %+v", snap)
	}
	if snap.Features.Hp30Lag1 != QuietHp30 || snap.Features.Hp3012hMax != QuietHp30 || snap.Features.Hp3024hMean != QuietHp30 ||
		snap.Features.BzSouth3h != 0 || snap.Features.Vsw3h != QuietVswKMS ||
		snap.Features.DstNow != QuietDstNT || snap.Features.Dst24hMin != QuietDstNT ||
		snap.Features.SFINow != DefaultSFIQuiet || snap.Features.SFI72hMean != DefaultSFIQuiet ||
		snap.SFIDelta != 0 || snap.DstRecovery != 0 {
		t.Fatalf("unexpected cold snapshot: %+v", snap)
	}
}

func TestSpaceWeatherCausalGapRuleParity(t *testing.T) {
	store := NewSpaceWeatherStore()
	t0 := time.Date(2025, 3, 21, 0, 0, 0, 0, time.UTC)

	// Strictly causal completed-bin lag check:
	// Ingesting Hp30 at 00:00 must NOT appear when querying inside [00:00, 00:30),
	// and MUST appear starting at 00:30.
	if !store.RecordHp30(t0, 8.0, "gfz") {
		t.Fatalf("RecordHp30 failed")
	}
	if snap := store.Evaluate(t0.Add(15 * time.Minute)); snap.Features.Hp30Lag1 != QuietHp30 || snap.Hp30.HasValid {
		t.Fatalf("in-progress Hp30 bin leaked early: %+v", snap)
	}
	if snap := store.Evaluate(t0.Add(30 * time.Minute)); snap.Features.Hp30Lag1 != 8.0 || !snap.Hp30.HasValid || snap.Hp30.GapHours != 0 {
		t.Fatalf("completed Hp30 bin not visible at t0+30m: %+v", snap)
	}

	// Exact 6.0h flat hold (12 missed 30-min bins -> targetEnd = t0 + 6h00m -> query at t0 + 6h30m).
	snapFlat := store.Evaluate(t0.Add(6*time.Hour + 30*time.Minute))
	if math.Abs(snapFlat.Features.Hp30Lag1-8.0) > 1e-12 {
		t.Fatalf("at 6.0h gap Hp30Lag1=%v, want flat 8.0", snapFlat.Features.Hp30Lag1)
	}
	if snapFlat.Hp30.GapHours != 6.0 {
		t.Fatalf("at 6.0h gap GapHours=%v, want 6.0", snapFlat.Hp30.GapHours)
	}

	// First decay step at 6.5h gap (targetEnd = t0 + 6h30m -> query at t0 + 7h00m).
	want6p5 := QuietHp30 + math.Pow(0.5, 0.5/6.0)*(8.0-QuietHp30)
	snap6p5 := store.Evaluate(t0.Add(7 * time.Hour))
	if math.Abs(snap6p5.Features.Hp30Lag1-want6p5) > 1e-12 {
		t.Fatalf("at 6.5h gap Hp30Lag1=%v, want %v", snap6p5.Features.Hp30Lag1, want6p5)
	}

	// Exact 12.0h gap (6.0h past flat hold -> exact 0.5x decay toward QuietHp30=2.0 -> 5.0).
	snap12 := store.Evaluate(t0.Add(12*time.Hour + 30*time.Minute))
	if math.Abs(snap12.Features.Hp30Lag1-5.0) > 1e-12 {
		t.Fatalf("at 12.0h gap Hp30Lag1=%v, want 5.0", snap12.Features.Hp30Lag1)
	}

	// Signed Bz_gsm gap-fill BEFORE max(0, -bz_fill) rectification:
	// Case A: southward Bz = -12 nT at 00:00, missing for 12h -> at 12:00 filled Bz = -6 nT -> bz_south = 6 nT.
	// At query 13:00 (targetEnd=12:00), trailing 3h bins are 10:00, 11:00, 12:00 (gaps 10h, 11h, 12h).
	store.RecordBzGSM(t0, -12.0, "rtsw")
	snapBz := store.Evaluate(t0.Add(13 * time.Hour))
	b10 := 12.0 * math.Pow(0.5, 4.0/6.0)
	b11 := 12.0 * math.Pow(0.5, 5.0/6.0)
	b12 := 12.0 * 0.5 // Exact 1 half-life (6h past the 6h flat hold).
	wantBz3h := (b10 + b11 + b12) / 3.0
	if math.Abs(snapBz.Features.BzSouth3h-wantBz3h) > 1e-12 {
		t.Fatalf("BzSouth3h=%v, want %v", snapBz.Features.BzSouth3h, wantBz3h)
	}

	// Case B: northward Bz = +10 nT followed by gap stays positive -> BzSouth3h == 0.
	storeNorth := NewSpaceWeatherStore()
	storeNorth.RecordBzGSM(t0, +10.0, "rtsw")
	if got := storeNorth.Evaluate(t0.Add(10 * time.Hour)).Features.BzSouth3h; got != 0 {
		t.Fatalf("northward Bz must yield BzSouth3h=0, got %v", got)
	}

	// Dst recovery & dropout stress test:
	// Hour 00:00 Dst = -120, Hour 05:00 Dst = -40 -> at query 06:00 (targetEnd=05:00):
	// DstNow = -40, Dst24hMin = -120, DstRecovery = 80.
	store.RecordDst(t0, -120.0, "kyoto")
	store.RecordDst(t0.Add(5*time.Hour), -40.0, "kyoto")
	snapDst := store.Evaluate(t0.Add(6 * time.Hour))
	if snapDst.Features.DstNow != -40.0 || snapDst.Features.Dst24hMin != -120.0 || snapDst.DstRecovery != 80.0 {
		t.Fatalf("Dst recovery mismatch: now=%v min=%v rec=%v", snapDst.Features.DstNow, snapDst.Features.Dst24hMin, snapDst.DstRecovery)
	}

	// Deeper Dst than prior min clamps DstRecovery at 0 (never negative).
	store.RecordDst(t0.Add(6*time.Hour), -150.0, "kyoto")
	snapDeep := store.Evaluate(t0.Add(7 * time.Hour))
	if snapDeep.Features.DstNow != -150.0 || snapDeep.Features.Dst24hMin != -150.0 || snapDeep.DstRecovery != 0 {
		t.Fatalf("Dst clamp mismatch: now=%v min=%v rec=%v", snapDeep.Features.DstNow, snapDeep.Features.Dst24hMin, snapDeep.DstRecovery)
	}

	// Vsw 3h trailing mean check.
	store.RecordVsw(t0, 500.0, "rtsw")
	store.RecordVsw(t0.Add(time.Hour), 560.0, "rtsw")
	store.RecordVsw(t0.Add(2*time.Hour), 620.0, "rtsw")
	snapVsw := store.Evaluate(t0.Add(3 * time.Hour))
	if math.Abs(snapVsw.Features.Vsw3h-560.0) > 1e-12 {
		t.Fatalf("Vsw3h=%v, want 560.0", snapVsw.Features.Vsw3h)
	}
}

func TestSpaceWeatherSFIColdStartAndSlowDecay(t *testing.T) {
	if _, err := NewSpaceWeatherStoreWithPolicy(SFIPolicy{QuietDefault: 0, ForwardFillDays: 3, DecayHalfLifeDays: 7}); !errors.Is(err, ErrInvalidSFIPolicy) {
		t.Fatalf("expected ErrInvalidSFIPolicy for QuietDefault=0, got %v", err)
	}
	if _, err := NewSpaceWeatherStoreWithPolicy(SFIPolicy{QuietDefault: 120, ForwardFillDays: -1, DecayHalfLifeDays: 7}); !errors.Is(err, ErrInvalidSFIPolicy) {
		t.Fatalf("expected ErrInvalidSFIPolicy for negative ForwardFillDays, got %v", err)
	}
	if _, err := NewSpaceWeatherStoreWithPolicy(SFIPolicy{QuietDefault: 120, ForwardFillDays: 3, DecayHalfLifeDays: 0}); !errors.Is(err, ErrInvalidSFIPolicy) {
		t.Fatalf("expected ErrInvalidSFIPolicy for zero DecayHalfLifeDays, got %v", err)
	}

	store := NewSpaceWeatherStore()
	day0 := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)

	// 1 observation (200 SFU on day 0), queried on day 1 -> min_samples=1, NO fake flare delta!
	if !store.RecordSFI(day0, 200.0, "gfz") {
		t.Fatalf("RecordSFI day0 failed")
	}
	// Same-day query must NOT leak today's incomplete bin.
	if sameDay := store.Evaluate(day0.Add(12 * time.Hour)); sameDay.SFI.HasValid || sameDay.Features.SFINow != DefaultSFIQuiet {
		t.Fatalf("same-day SFI leaked before UTC rollover: %+v", sameDay)
	}
	s1 := store.Evaluate(day0.AddDate(0, 0, 1))
	if s1.Features.SFINow != 200.0 || s1.Features.SFI72hMean != 200.0 || s1.SFIDelta != 0.0 {
		t.Fatalf("1-day cold start mismatch: now=%v mean=%v delta=%v", s1.Features.SFINow, s1.Features.SFI72hMean, s1.SFIDelta)
	}

	// 2 observations (200 on day0, 180 on day1), queried on day2 -> mean=190, delta=-10.
	store.RecordSFI(day0.AddDate(0, 0, 1), 180.0, "gfz")
	s2 := store.Evaluate(day0.AddDate(0, 0, 2))
	if s2.Features.SFINow != 180.0 || s2.Features.SFI72hMean != 190.0 || s2.SFIDelta != -10.0 {
		t.Fatalf("2-day cold start mismatch: now=%v mean=%v delta=%v", s2.Features.SFINow, s2.Features.SFI72hMean, s2.SFIDelta)
	}

	// Outage test from single observation of 200 SFU on day0:
	outageStore := NewSpaceWeatherStore()
	outageStore.RecordSFI(day0, 200.0, "gfz")
	// Missing days 1, 2, 3 (gap=1,2,3 days -> 24h, 48h, 72h): forward-filled at 200.0.
	for missingDays := 1; missingDays <= 3; missingDays++ {
		snap := outageStore.Evaluate(day0.AddDate(0, 0, missingDays+1))
		if math.Abs(snap.Features.SFINow-200.0) > 1e-12 {
			t.Fatalf("missingDays=%d SFINow=%v, want flat 200.0", missingDays, snap.Features.SFINow)
		}
	}
	// Missing day 4 (1 day past 72h ffill): decays by 0.5^(1/7).
	wantDay4 := DefaultSFIQuiet + math.Pow(0.5, 1.0/DefaultSFIDecayHalfLifeD)*(200.0-DefaultSFIQuiet)
	sDay4 := outageStore.Evaluate(day0.AddDate(0, 0, 5))
	if math.Abs(sDay4.Features.SFINow-wantDay4) > 1e-12 {
		t.Fatalf("missingDay=4 SFINow=%v, want %v", sDay4.Features.SFINow, wantDay4)
	}
	// Missing day 10 (7 days past 72h ffill -> exact 1 half-life -> 160.0).
	sDay10 := outageStore.Evaluate(day0.AddDate(0, 0, 11))
	if math.Abs(sDay10.Features.SFINow-160.0) > 1e-12 {
		t.Fatalf("missingDay=10 SFINow=%v, want 160.0", sDay10.Features.SFINow)
	}
}

func TestSpaceWeatherSentinelRejection(t *testing.T) {
	store := NewSpaceWeatherStore()
	ts := time.Date(2025, 3, 21, 12, 0, 0, 0, time.UTC)

	if store.RecordHp30(time.Time{}, 2.0, "gfz") || store.RecordHp30(ts, -0.1, "gfz") || store.RecordHp30(ts, 99.0, "gfz") || store.RecordHp30(ts, math.NaN(), "gfz") {
		t.Fatalf("invalid Hp30 accepted")
	}
	if store.RecordBzGSM(time.Time{}, 0, "rtsw") || store.RecordBzGSM(ts, 999.9, "rtsw") || store.RecordBzGSM(ts, -900.0, "rtsw") || store.RecordBzGSM(ts, math.Inf(1), "rtsw") {
		t.Fatalf("invalid BzGSM accepted")
	}
	if store.RecordVsw(time.Time{}, 400, "rtsw") || store.RecordVsw(ts, 99.9, "rtsw") || store.RecordVsw(ts, 9000.0, "rtsw") || store.RecordVsw(ts, math.NaN(), "rtsw") {
		t.Fatalf("invalid Vsw accepted")
	}
	if store.RecordDst(time.Time{}, 0, "kyoto") || store.RecordDst(ts, 99999, "kyoto") || store.RecordDst(ts, -9000, "kyoto") || store.RecordDst(ts, math.Inf(-1), "kyoto") {
		t.Fatalf("invalid Dst accepted")
	}
	if store.RecordSFI(time.Time{}, 120, "gfz") || store.RecordSFI(ts, 0, "gfz") || store.RecordSFI(ts, -10, "gfz") || store.RecordSFI(ts, math.NaN(), "gfz") {
		t.Fatalf("invalid SFI accepted")
	}
}

func TestSpaceWeatherBackfillHistoricalAndLongOutage(t *testing.T) {
	t0 := time.Date(2025, 3, 21, 0, 0, 0, 0, time.UTC)

	// Chronological vs out-of-order ingestion inside the 24h ring must match identically.
	chrono := NewSpaceWeatherStore()
	backfill := NewSpaceWeatherStore()

	samples := []struct {
		off time.Duration
		val float64
	}{
		{0 * time.Minute, 1.0},
		{30 * time.Minute, 3.0},
		{6 * time.Hour, 7.0},
		{15 * time.Hour, 5.0},
		{20 * time.Hour, 2.0},
	}
	for _, s := range samples {
		chrono.RecordHp30(t0.Add(s.off), s.val, "gfz")
	}
	// Record latest first, then backfill earlier bins inside the retained window.
	for i := len(samples) - 1; i >= 0; i-- {
		if !backfill.RecordHp30(t0.Add(samples[i].off), samples[i].val, "gfz") {
			t.Fatalf("backfill sample %d unexpectedly rejected", i)
		}
	}

	q := t0.Add(22 * time.Hour)
	sc := chrono.Evaluate(q)
	sb := backfill.Evaluate(q)
	if math.Abs(sc.Features.Hp30Lag1-sb.Features.Hp30Lag1) > 1e-12 ||
		math.Abs(sc.Features.Hp3012hMax-sb.Features.Hp3012hMax) > 1e-12 ||
		math.Abs(sc.Features.Hp3024hMean-sb.Features.Hp3024hMean) > 1e-12 {
		t.Fatalf("chronological vs backfill mismatch:\nchrono=%+v\nbackfill=%+v", sc.Features, sb.Features)
	}

	// Observation older than retained 24h ring window must be rejected safely.
	if chrono.RecordHp30(t0.Add(-24*time.Hour), 9.0, "gfz") {
		t.Fatalf("expected too-old Hp30 bin to be rejected")
	}

	// Historical query strictly before retained ring must NOT alias newer data.
	hist := chrono.Evaluate(t0.Add(-48 * time.Hour))
	if hist.Hp30.HasValid || hist.Features.Hp30Lag1 != QuietHp30 {
		t.Fatalf("historical query aliased newer ring slots: %+v", hist)
	}

	// 10-year outage must evaluate in O(1) and converge cleanly to quiet defaults.
	longOut := chrono.Evaluate(t0.AddDate(10, 0, 0))
	if math.Abs(longOut.Features.Hp30Lag1-QuietHp30) > 1e-12 {
		t.Fatalf("10-year outage did not converge to quiet Hp30: %v", longOut.Features.Hp30Lag1)
	}

	// 10-year ingest jump must also complete in O(window) and record the new sample cleanly.
	tFuture := t0.AddDate(10, 0, 0)
	if !chrono.RecordHp30(tFuture, 6.0, "gfz") {
		t.Fatalf("RecordHp30 after 10-year gap failed")
	}
	afterJump := chrono.Evaluate(tFuture.Add(30 * time.Minute))
	if afterJump.Features.Hp30Lag1 != 6.0 {
		t.Fatalf("Hp30Lag1 after 10-year jump=%v, want 6.0", afterJump.Features.Hp30Lag1)
	}
}

func TestSpaceWeatherConcurrentRace(t *testing.T) {
	store := NewSpaceWeatherStore()
	base := time.Date(2025, 3, 21, 0, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(6)
	go func() {
		defer wg.Done()
		for i := range 200 {
			store.RecordHp30(base.Add(time.Duration(i)*30*time.Minute), float64(i%9), "gfz")
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 100 {
			store.RecordBzGSM(base.Add(time.Duration(i)*time.Hour), float64(i%20)-10.0, "rtsw")
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 100 {
			store.RecordVsw(base.Add(time.Duration(i)*time.Hour), 350.0+float64(i%10)*20.0, "rtsw")
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 100 {
			store.RecordDst(base.Add(time.Duration(i)*time.Hour), -float64(i%60), "kyoto")
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 10 {
			store.RecordSFI(base.AddDate(0, 0, i), 130.0+float64(i), "gfz")
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 400 {
			snap := store.Evaluate(base.Add(time.Duration(i) * 15 * time.Minute))
			if !isFinite64(snap.Features.Hp30Lag1) || !isFinite64(snap.Features.SFINow) {
				t.Errorf("non-finite concurrent snapshot: %+v", snap)
				return
			}
		}
	}()
	wg.Wait()

	// Verify no lost updates across the 5 independent feeds.
	final := store.Evaluate(base.AddDate(0, 0, 10))
	if !final.Hp30.HasValid || !final.Bz.HasValid || !final.Vsw.HasValid || !final.Dst.HasValid || !final.SFI.HasValid {
		t.Fatalf("concurrent writes lost a feed update: %+v", final)
	}
}

func TestSpaceWeatherEvaluateZeroAllocs(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("alloc count check skipped under -race")
	}
	store := NewSpaceWeatherStore()
	base := time.Date(2025, 3, 21, 0, 0, 0, 0, time.UTC)
	for i := range 48 {
		store.RecordHp30(base.Add(time.Duration(i)*30*time.Minute), 2.5, "gfz")
	}
	for i := range 24 {
		ts := base.Add(time.Duration(i) * time.Hour)
		store.RecordBzGSM(ts, -2.0, "rtsw")
		store.RecordVsw(ts, 450.0, "rtsw")
		store.RecordDst(ts, -25.0, "kyoto")
	}
	store.RecordSFI(base, 150.0, "gfz")

	freshTime := base.Add(25 * time.Hour)
	freshAllocs := testing.AllocsPerRun(100, func() {
		benchSnapSink = store.Evaluate(freshTime)
	})
	if freshAllocs != 0 {
		t.Fatalf("Evaluate(fresh) allocated %v allocs/op, want 0", freshAllocs)
	}

	outageTime := base.AddDate(10, 0, 0)
	outageAllocs := testing.AllocsPerRun(100, func() {
		benchSnapSink = store.Evaluate(outageTime)
	})
	if outageAllocs != 0 {
		t.Fatalf("Evaluate(10-year outage) allocated %v allocs/op, want 0", outageAllocs)
	}
}

var benchSnapSink SpaceWeatherSnapshot

func BenchmarkSpaceWeatherEvaluateFresh(b *testing.B) {
	store := NewSpaceWeatherStore()
	base := time.Date(2025, 3, 21, 0, 0, 0, 0, time.UTC)
	for i := range 48 {
		store.RecordHp30(base.Add(time.Duration(i)*30*time.Minute), 2.5, "gfz")
	}
	for i := range 24 {
		ts := base.Add(time.Duration(i) * time.Hour)
		store.RecordBzGSM(ts, -2.0, "rtsw")
		store.RecordVsw(ts, 450.0, "rtsw")
		store.RecordDst(ts, -25.0, "kyoto")
	}
	store.RecordSFI(base, 150.0, "gfz")
	q := base.Add(25 * time.Hour)

	b.ReportAllocs()
	b.ResetTimer()
	var snap SpaceWeatherSnapshot
	for range b.N {
		snap = store.Evaluate(q)
	}
	benchSnapSink = snap
}

func BenchmarkSpaceWeatherEvaluateTenYearGap(b *testing.B) {
	store := NewSpaceWeatherStore()
	base := time.Date(2025, 3, 21, 0, 0, 0, 0, time.UTC)
	store.RecordHp30(base, 6.0, "gfz")
	store.RecordBzGSM(base, -8.0, "rtsw")
	store.RecordVsw(base, 650.0, "rtsw")
	store.RecordDst(base, -90.0, "kyoto")
	store.RecordSFI(base, 200.0, "gfz")
	q := base.AddDate(10, 0, 0)

	b.ReportAllocs()
	b.ResetTimer()
	var snap SpaceWeatherSnapshot
	for range b.N {
		snap = store.Evaluate(q)
	}
	benchSnapSink = snap
}

func TestSpaceWeatherBatchAtomicityAndParity(t *testing.T) {
	var nilStore *SpaceWeatherStore
	if got := nilStore.RecordHp30Batch([]Sample{{BinStartUTC: time.Now().UTC(), Value: 2.0}}, "gfz"); got != 0 {
		t.Fatalf("nil store accepted %d samples, want 0", got)
	}

	single := NewSpaceWeatherStore()
	batch := NewSpaceWeatherStore()
	base := time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)

	hpSamples := make([]Sample, 0, 72)
	for i := range 70 {
		ts := base.Add(time.Duration(i) * 30 * time.Minute)
		val := float64(i%9) * 0.5
		single.RecordHp30(ts, val, "gfz_hp30")
		hpSamples = append(hpSamples, Sample{BinStartUTC: ts, Value: val})
	}
	// Mix invalid values in batch; they must be skipped without blocking valid samples.
	hpSamples = append(hpSamples,
		Sample{BinStartUTC: time.Time{}, Value: 3.0},
		Sample{BinStartUTC: base.Add(70 * 30 * time.Minute), Value: math.NaN()},
		Sample{BinStartUTC: base.Add(71 * 30 * time.Minute), Value: 99.0},
	)

	beforePtr := batch.state.Load()
	if got := batch.RecordHp30Batch([]Sample{{BinStartUTC: base, Value: -1.0}}, "gfz_hp30"); got != 0 {
		t.Fatalf("all-invalid batch accepted %d, want 0", got)
	}
	if batch.state.Load() != beforePtr {
		t.Fatalf("all-invalid batch swapped atomic state pointer")
	}

	if got := batch.RecordHp30Batch(hpSamples, "gfz_hp30"); got != 70 {
		t.Fatalf("RecordHp30Batch accepted %d, want 70", got)
	}
	if batch.state.Load() == beforePtr {
		t.Fatalf("valid batch did not publish new atomic state pointer")
	}

	bzSamples := make([]Sample, 0, 24)
	vswSamples := make([]Sample, 0, 24)
	dstSamples := make([]Sample, 0, 24)
	for i := range 24 {
		ts := base.Add(time.Duration(i) * time.Hour)
		bz := float64(i%12) - 8.0
		vsw := 350.0 + float64(i)*10.0
		dst := -float64(i * 4)
		single.RecordBzGSM(ts, bz, "noaa_rtsw_mag")
		single.RecordVsw(ts, vsw, "noaa_rtsw_wind")
		single.RecordDst(ts, dst, "noaa_kyoto_dst")
		bzSamples = append(bzSamples, Sample{BinStartUTC: ts, Value: bz})
		vswSamples = append(vswSamples, Sample{BinStartUTC: ts, Value: vsw})
		dstSamples = append(dstSamples, Sample{BinStartUTC: ts, Value: dst})
	}
	if got := batch.RecordBzGSMBatch(bzSamples, "noaa_rtsw_mag"); got != 24 {
		t.Fatalf("RecordBzGSMBatch accepted %d, want 24", got)
	}
	if got := batch.RecordVswBatch(vswSamples, "noaa_rtsw_wind"); got != 24 {
		t.Fatalf("RecordVswBatch accepted %d, want 24", got)
	}
	if got := batch.RecordDstBatch(dstSamples, "noaa_kyoto_dst"); got != 24 {
		t.Fatalf("RecordDstBatch accepted %d, want 24", got)
	}

	sfiSamples := []Sample{
		{BinStartUTC: base.AddDate(0, 0, -2), Value: 140.0},
		{BinStartUTC: base.AddDate(0, 0, -1), Value: 150.0},
		{BinStartUTC: base, Value: 160.0},
		{BinStartUTC: base.AddDate(0, 0, 1), Value: -5.0}, // invalid
	}
	for _, s := range sfiSamples {
		single.RecordSFI(s.BinStartUTC, s.Value, "noaa_f107")
	}
	if got := batch.RecordSFIBatch(sfiSamples, "noaa_f107"); got != 3 {
		t.Fatalf("RecordSFIBatch accepted %d, want 3", got)
	}

	q := base.Add(35 * time.Hour)
	gotSingle := single.Evaluate(q)
	gotBatch := batch.Evaluate(q)
	if gotSingle.Generation != single.Generation() || gotSingle.Generation != 145 {
		t.Fatalf("single generation = %d (store=%d), want 145", gotSingle.Generation, single.Generation())
	}
	if gotBatch.Generation != batch.Generation() || gotBatch.Generation != 5 {
		t.Fatalf("batch generation = %d (store=%d), want 5", gotBatch.Generation, batch.Generation())
	}
	gotSingle.Generation = 0
	gotBatch.Generation = 0
	if gotSingle != gotBatch {
		t.Fatalf("batch parity mismatch:\nsingle=%+v\nbatch =%+v", gotSingle, gotBatch)
	}
}

func TestSpaceWeatherGenerationMonotonicAndAtomic(t *testing.T) {
	var nilStore *SpaceWeatherStore
	if got := nilStore.Generation(); got != 0 {
		t.Fatalf("nil store generation = %d, want 0", got)
	}

	store := NewSpaceWeatherStore()
	if got := store.Generation(); got != 0 {
		t.Fatalf("initial generation = %d, want 0", got)
	}
	ts := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)

	// Rejected samples must not advance generation.
	if store.RecordHp30(ts, -1.0, "gfz") {
		t.Fatalf("invalid Hp30 accepted")
	}
	if got := store.Generation(); got != 0 {
		t.Fatalf("rejected write advanced generation to %d", got)
	}

	if !store.RecordHp30(ts, 3.0, "gfz") {
		t.Fatalf("valid Hp30 rejected")
	}
	if got := store.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}
	if snap := store.Evaluate(ts.Add(time.Hour)); snap.Generation != 1 {
		t.Fatalf("snapshot generation = %d, want 1", snap.Generation)
	}

	if got := store.RecordDstBatch([]Sample{
		{BinStartUTC: ts, Value: -20},
		{BinStartUTC: ts.Add(time.Hour), Value: -25},
	}, "kyoto"); got != 2 {
		t.Fatalf("RecordDstBatch accepted %d, want 2", got)
	}
	if got := store.Generation(); got != 2 {
		t.Fatalf("generation after batch = %d, want 2", got)
	}
	if snap := store.Evaluate(ts.Add(2 * time.Hour)); snap.Generation != 2 {
		t.Fatalf("snapshot generation after batch = %d, want 2", snap.Generation)
	}
}
