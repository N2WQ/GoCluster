// Reporting tests use exact independent clock/bin fixtures and the same narrow
// retry coordinator as production, without timing sleeps or global test hooks.
package pathreliability

import (
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestBoundedReportScan(t *testing.T) {
	tests := []struct {
		name      string
		query     int64
		seen      []int64
		usable    []bool
		wantTimes []int64
		wantAsOf  int64
		wantOK    bool
	}{
		{"coherent", 20, []int64{20}, []bool{true}, []int64{20}, 20, true},
		{"one_retry", 10, []int64{30, 30}, []bool{true, true}, []int64{10, 30}, 30, true},
		{"retry_exhausted", 10, []int64{30, 40}, []bool{true, true}, []int64{10, 30}, 30, false},
		{"unsafe_first_scan", 20, []int64{20}, []bool{false}, []int64{20}, 20, false},
		{"unsafe_retry", 10, []int64{30, 30}, []bool{true, false}, []int64{10, 30}, 30, false},
		{"provisional_unsafe_expires", 10, []int64{30, 30}, []bool{false, true}, []int64{10, 30}, 30, true},
		{"epoch_zero", 0, []int64{0}, []bool{true}, []int64{0}, 0, true},
		{"negative_reference", -20, []int64{-10, -10}, []bool{true, true}, []int64{-20, -10}, -10, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var scanTimes []int64
			asOf, available := boundedReportScan(tc.query, func(reference int64) (int64, bool) {
				i := len(scanTimes)
				if i >= len(tc.seen) {
					t.Fatalf("unexpected scan %d at %d", i, reference)
				}
				scanTimes = append(scanTimes, reference)
				return tc.seen[i], tc.usable[i]
			})
			if !reflect.DeepEqual(scanTimes, tc.wantTimes) {
				t.Fatalf("scans=%v; want %v", scanTimes, tc.wantTimes)
			}
			if asOf != tc.wantAsOf || available != tc.wantOK {
				t.Fatalf("asOf=%d available=%v, want %d/%v", asOf, available, tc.wantAsOf, tc.wantOK)
			}
		})
	}
}

func TestStoreReportingCommonReferenceAndHalfLives(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 10, "40m": 20}
	store := NewStore(cfg, []string{"20m", "40m"})
	putReportTestBucket(store, packKey(1, 2, 0), &bucket{weight: 1, count: 1, lastUpdate: 0, newestObservation: 0})
	putReportTestBucket(store, packCoarseKey(3, 4, 1), &bucket{weight: 1, count: 1, lastUpdate: 20, newestObservation: 20})

	// At common time 20, the independently calculated masses are .25 and 1.
	// Mixed query/reference clamping would place .5 in the wrong first-band bin.
	hist := store.weightHistogramSnapshot(time.Unix(10, 0), []float64{.3, .75, 1.1})
	if !hist.available || hist.asOfUnix != 20 || len(hist.byBand) != 2 {
		t.Fatalf("unexpected coherent histogram metadata: %+v", hist)
	}
	if !reflect.DeepEqual(hist.byBand[0].bins, []int{1, 0, 0, 0}) ||
		!reflect.DeepEqual(hist.byBand[1].bins, []int{0, 0, 1, 0}) {
		t.Fatalf("common-time masses must classify .25 and 1: %+v", hist.byBand)
	}
	store.RefreshStatsSnapshot(time.Unix(10, 0))
	stats := store.statsSnapshot()
	if !stats.available || !stats.hasSnapshot || stats.asOfUnix != 20 || stats.fine != 1 || stats.coarse != 1 {
		t.Fatalf("unexpected common-time count publication: %+v", stats)
	}
	if !reflect.DeepEqual(stats.byBand, []bandCounts{{fine: 1}, {coarse: 1}}) {
		t.Fatalf("wrong per-band counts: %+v", stats.byBand)
	}

	// Moving beyond every reference uses requested time: .125 and sqrt(.5).
	later := store.weightHistogramSnapshot(time.Unix(30, 0), []float64{.2, .8})
	if !later.available || later.asOfUnix != 30 ||
		!reflect.DeepEqual(later.byBand[0].bins, []int{1, 0, 0}) ||
		!reflect.DeepEqual(later.byBand[1].bins, []int{0, 1, 0}) {
		t.Fatalf("unexpected later histogram: %+v", later)
	}
}

func TestStoreReportingExpiryUsesNewestObservation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultHalfLifeSec = 10
	cfg.StaleAfterHalfLifeMultiplier = 0
	cfg.StaleAfterSeconds = 50
	store := NewStore(cfg, []string{"20m"})
	// Advancing accounting to 100 must not refresh a newest observation at zero.
	putReportTestBucket(store, packKey(1, 2, 0), &bucket{weight: 1, count: 2, lastUpdate: 100, newestObservation: 0})
	putReportTestBucket(store, packCoarseKey(3, 4, 0), &bucket{weight: 1, count: 1, lastUpdate: 100, newestObservation: 50})
	store.RefreshStatsSnapshot(time.Unix(10, 0))
	stats := store.statsSnapshot()
	if !stats.available || stats.asOfUnix != 100 || stats.fine != 0 || stats.coarse != 1 {
		t.Fatalf("expiry must use observation age at common time, including boundary: %+v", stats)
	}
	hist := store.weightHistogramSnapshot(time.Unix(10, 0), []float64{.5})
	if !hist.available || hist.asOfUnix != 100 || hist.byBand[0].total != 1 {
		t.Fatalf("weight histogram must share observation-based expiry: %+v", hist)
	}

	store.RefreshStatsSnapshot(time.Unix(101, 0))
	stats = store.statsSnapshot()
	if stats.fine != 0 || stats.coarse != 0 {
		t.Fatalf("boundary bucket must expire after 50 seconds: %+v", stats)
	}
}

func TestStoreReportingCacheAvailabilityAndOrdering(t *testing.T) {
	store := NewStore(DefaultConfig(), []string{"20m"})
	initial := store.statsSnapshot()
	if initial.available || initial.hasSnapshot || len(initial.byBand) != 1 {
		t.Fatalf("initial cached zero counters are not a completed publication: %+v", initial)
	}
	store.publishStatsSnapshot(pathStoreStatsSnapshot{available: false, asOfUnix: 20})
	if unavailable := store.statsSnapshot(); unavailable.available || unavailable.hasSnapshot {
		t.Fatalf("initial failure must not invent a prior publication: %+v", unavailable)
	}

	putReportTestBucket(store, packKey(1, 2, 0), &bucket{weight: 1, count: 1})
	store.RefreshStatsSnapshot(time.Unix(0, 0))
	zero := store.statsSnapshot()
	if !zero.available || !zero.hasSnapshot || zero.asOfUnix != 0 || zero.fine != 1 {
		t.Fatalf("epoch zero is a valid completed publication: %+v", zero)
	}
	zero.byBand[0].fine = 999
	if store.statsSnapshot().byBand[0].fine != 1 {
		t.Fatal("mutating a copied snapshot changed the retained publication")
	}

	store.RefreshStatsSnapshot(time.Unix(100, 0))
	store.RefreshStatsSnapshot(time.Unix(50, 0))
	if current := store.statsSnapshot(); !current.available || current.asOfUnix != 100 || current.fine != 1 {
		t.Fatalf("older coherent refresh rewound a valid cache: %+v", current)
	}
	store.publishStatsSnapshot(pathStoreStatsSnapshot{available: false, asOfUnix: 110, fine: 999})
	failed := store.statsSnapshot()
	if failed.available || !failed.hasSnapshot || failed.asOfUnix != 100 || failed.fine != 1 || failed.byBand[0].fine != 1 {
		t.Fatalf("failed scan must preserve visibly unavailable dated counts: %+v", failed)
	}
	store.RefreshStatsSnapshot(time.Unix(50, 0))
	if older := store.statsSnapshot(); older.available || older.asOfUnix != 100 {
		t.Fatalf("older refresh must not restore availability: %+v", older)
	}
	store.RefreshStatsSnapshot(time.Unix(120, 0))
	if recovered := store.statsSnapshot(); !recovered.available || recovered.asOfUnix != 120 || recovered.fine != 1 {
		t.Fatalf("new coherent refresh must recover availability: %+v", recovered)
	}
}

func TestStoreReportingUnsafeWeightIsUnavailable(t *testing.T) {
	for _, weight := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1} {
		store := NewStore(DefaultConfig(), []string{"20m"})
		putReportTestBucket(store, packKey(1, 2, 0), &bucket{weight: 1, count: 1, lastUpdate: 10, newestObservation: 10})
		putReportTestBucket(store, packKey(3, 4, 0), &bucket{weight: weight, count: 1, lastUpdate: 10, newestObservation: 10})
		hist := store.weightHistogramSnapshot(time.Unix(10, 0), []float64{.5, 2})
		if hist.available || hist.byBand != nil || hist.asOfUnix != 10 {
			t.Fatalf("unsafe weight %g must discard all distribution bins: %+v", weight, hist)
		}
		if store.WeightHistogramByBand(time.Unix(10, 0), []float64{.5, 2}) != nil {
			t.Fatalf("legacy histogram wrapper exposed partial bins for %g", weight)
		}
	}
}

func TestStoreReportingRawCountWithoutUsableMass(t *testing.T) {
	store := NewStore(DefaultConfig(), []string{"20m"})
	putReportTestBucket(store, packKey(1, 2, 0), &bucket{count: 12, lastUpdate: 50, newestObservation: 0})
	store.RefreshStatsSnapshot(time.Unix(70, 0))
	stats := store.statsSnapshot()
	if !stats.available || !stats.hasSnapshot || stats.fine != 1 || stats.asOfUnix != 70 {
		t.Fatalf("eligible raw-report population must remain visible without mass: %+v", stats)
	}
	hist := store.weightHistogramSnapshot(time.Unix(70, 0), []float64{.5, 1})
	if !hist.available || hist.asOfUnix != 70 || hist.byBand[0].total != 0 {
		t.Fatalf("count-only evidence must not acquire histogram mass: %+v", hist)
	}
}

func TestStoreReportingSaturatingClockDifference(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StaleAfterHalfLifeMultiplier = 0
	cfg.StaleAfterSeconds = math.MaxInt64
	store := NewStore(cfg, []string{"20m"})
	putReportTestBucket(store, packKey(1, 2, 0),
		&bucket{weight: 1, count: 1, lastUpdate: math.MinInt64, newestObservation: math.MinInt64})
	query := time.Unix(math.MaxInt64, 0)
	store.RefreshStatsSnapshot(query)
	stats := store.statsSnapshot()
	// A saturated age equals the configured threshold, so strict-greater expiry
	// retains the population while its ancient usable mass completely decays.
	if !stats.available || stats.asOfUnix != math.MaxInt64 || stats.fine != 1 {
		t.Fatalf("extreme clocks must retain the stated saturated expiry boundary: %+v", stats)
	}
	hist := store.weightHistogramSnapshot(query, []float64{.5})
	if !hist.available || hist.asOfUnix != math.MaxInt64 || hist.byBand[0].total != 0 {
		t.Fatalf("subtraction overflow must not amplify ancient mass: %+v", hist)
	}
}

func TestStoreReportingWeightedMassSurvivesFactorUnderflow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultHalfLifeSec = 1
	cfg.StaleAfterHalfLifeMultiplier = 0
	cfg.StaleAfterSeconds = 2000
	store := NewStore(cfg, []string{"20m"})
	putReportTestBucket(store, packKey(1, 2, 0), &bucket{weight: 0x1p100, count: 1})
	// The unit factor 2^-1100 underflows in float64, but weighted raw mass
	// 2^100 * 2^-1100 = 2^-1000 remains representable for diagnostic binning.
	hist := store.weightHistogramSnapshot(time.Unix(1100, 0), []float64{0x1p-1001, 0x1p-999})
	if !hist.available || hist.asOfUnix != 1100 || hist.byBand[0].total != 1 ||
		!reflect.DeepEqual(hist.byBand[0].bins, []int{0, 1, 0}) {
		t.Fatalf("diagnostics prematurely underflowed weighted raw mass: %+v", hist)
	}
}

func TestStoreReportingConcurrentSnapshotCopies(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StaleAfterHalfLifeMultiplier = 0
	cfg.StaleAfterSeconds = 10000
	store := NewStore(cfg, []string{"20m", "40m"})
	var workers sync.WaitGroup
	start := make(chan struct{})
	workers.Go(func() {
		<-start
		for i := 0; i < 1000; i++ {
			key := packKey(CellID(i%16+1), 100, uint16(i%2))
			sh := &store.shards[key%uint64(len(store.shards))]
			sh.mu.Lock()
			if i%3 == 0 {
				delete(sh.buckets, key)
			} else {
				sh.buckets[key] = &bucket{weight: 1, count: 1, lastUpdate: int64(i), newestObservation: int64(i)}
			}
			sh.mu.Unlock()
		}
	})
	workers.Go(func() {
		<-start
		for i := 0; i < 200; i++ {
			store.RefreshStatsSnapshot(time.Unix(1000, 0))
		}
	})
	defer workers.Wait()
	close(start)
	for i := 0; i < 1000; i++ {
		snapshot := store.statsSnapshot()
		fine, coarse := 0, 0
		for _, band := range snapshot.byBand {
			fine += band.fine
			coarse += band.coarse
		}
		if fine != snapshot.fine || coarse != snapshot.coarse || len(snapshot.byBand) != 2 {
			t.Fatalf("totals and bands must come from one bounded publication: %+v", snapshot)
		}
		if snapshot.hasSnapshot && (!snapshot.available || snapshot.asOfUnix != 1000) {
			t.Fatalf("unexpected completed publication: %+v", snapshot)
		}
		// A separate reader may freely change its copy while refreshes continue.
		snapshot.byBand[0].fine = 999
	}
}

func putReportTestBucket(store *Store, key uint64, b *bucket) {
	sh := &store.shards[key%uint64(len(store.shards))]
	sh.mu.Lock()
	sh.buckets[key] = b
	sh.mu.Unlock()
}

func BenchmarkStoreReportingCoherentHistogram(b *testing.B) {
	cfg := DefaultConfig()
	store := NewStore(cfg, []string{"20m", "40m"})
	for i := 0; i < 20000; i++ {
		putReportTestBucket(store, packKey(CellID(i+1), 30000, uint16(i%2)),
			&bucket{weight: 1, count: 1, lastUpdate: int64(i % 21), newestObservation: int64(i % 21)})
	}
	edges := []float64{.25, .5, 1, 2}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := store.weightHistogramSnapshot(time.Unix(10, 0), edges)
		if !result.available || result.asOfUnix != 20 || result.byBand[0].total != 10000 || result.byBand[1].total != 10000 {
			b.Fatalf("invalid benchmark distribution: %+v", result)
		}
	}
}

func TestStoreReportingProvisionalUnsafeBucketExpiresAtCommonTime(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StaleAfterHalfLifeMultiplier = 0
	cfg.StaleAfterSeconds = 50
	store := NewStore(cfg, []string{"20m"})
	putReportTestBucket(store, packKey(1, 2, 0), &bucket{weight: math.NaN(), count: 1, lastUpdate: 0, newestObservation: 0})
	putReportTestBucket(store, packCoarseKey(3, 4, 0), &bucket{weight: 1, count: 1, lastUpdate: 100, newestObservation: 100})
	hist := store.weightHistogramSnapshot(time.Unix(0, 0), []float64{.5, 2})
	if !hist.available || hist.asOfUnix != 100 || len(hist.byBand) != 1 || hist.byBand[0].total != 1 {
		t.Fatalf("expired provisional failure poisoned coherent report=%+v", hist)
	}
}
