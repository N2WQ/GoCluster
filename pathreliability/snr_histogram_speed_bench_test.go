package pathreliability

import (
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// Frozen clocks and complete cycles make input/call frequencies identical in
// the independently launched baseline and candidate measurement processes.
func BenchmarkMedianSpeedPrimary(b *testing.B) {
	medianSpeedPredictionBenchmarks(b, -1)
}

func BenchmarkMedianSpeedFallbackMix(b *testing.B) {
	for _, percent := range []int{0, 10, 50, 100} {
		b.Run(fmt.Sprintf("fallback=%d", percent), func(b *testing.B) { medianSpeedPredictionBenchmarks(b, percent) })
	}
}

func medianSpeedPredictionBenchmarks(b *testing.B, fallbackPercent int) {
	for _, beacon := range []bool{false, true} {
		kind := "normal"
		if beacon {
			kind = "beacon"
		}
		b.Run(kind, func(b *testing.B) {
			for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
				b.Run(mode, func(b *testing.B) {
					corpus := newMedianSpeedCorpus(b, mode, beacon, fallbackPercent)
					cycle := len(corpus.cases)
					if b.N > 1 && b.N%cycle != 0 {
						b.Fatalf("fixed confirmation work must contain complete %d-call cycles, N=%d", cycle, b.N)
					}
					// Fixed warm-up is untimed and equal for both measurement builds.
					for i := 0; i < 6000; i++ {
						corpus.predict(corpus.cases[i%cycle].path)
					}
					b.ReportAllocs()
					b.ResetTimer()
					var result Result
					for i := 0; i < b.N; i++ {
						result = corpus.predict(corpus.cases[i%cycle].path)
					}
					b.StopTimer()
					medianSpeedLogTiming(b)
					if result != corpus.cases[(b.N-1)%cycle].expected {
						b.Fatalf("timed prediction changed output: %+v", result)
					}
					for _, entry := range corpus.cases {
						if got := corpus.sample(entry.path); exactP50Snapshot(&got.snrBins) != entry.bits {
							b.Fatal("timed prediction changed histogram")
						}
					}
					b.ReportMetric(float64(cycle), "cycle-calls")
					b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
					runtime.KeepAlive(corpus)
				})
			}
		})
	}
}

func medianSpeedDirectCases() []struct {
	name string
	h    snrHistogram
} {
	var dense, varying snrHistogram
	for i := range dense {
		dense[i] = math.MaxFloat32
		varying[i] = .5 + float32(i%3)*.125
	}
	return []struct {
		name string
		h    snrHistogram
	}{
		{"one", snrHistogram{25: 1}},
		{"one_subnormal", snrHistogram{49: math.SmallestNonzeroFloat32}},
		{"two_equal", snrHistogram{0: 1, 49: 1}},
		{"two_wide", snrHistogram{0: math.MaxFloat32, 49: math.SmallestNonzeroFloat32}},
		{"certified_three", snrHistogram{2: 1, 19: 1, 35: 1}},
		{"dense_max", dense},
		{"dense_varying", varying},
		{"fallback_strict", snrHistogram{2: 2, 19: float32(math.Ldexp(1, -56)), 35: 1}},
		{"fallback_true_tie", snrHistogram{5: 1, 30: float32(math.Ldexp(1, -56)), 35: 1, 45: float32(math.Ldexp(1, -56))}},
		{"fallback_near_tie", snrHistogram{2: 2, 19: float32(math.Ldexp(1, -56)), 35: 2}},
	}
}

func BenchmarkMedianSpeedDirect(b *testing.B) {
	for _, entry := range medianSpeedDirectCases() {
		b.Run(entry.name, func(b *testing.B) {
			before := exactP50Snapshot(&entry.h)
			want, wantOK := exactP50Reference(entry.h)
			if allocations := testing.AllocsPerRun(100, func() { entry.h.p50DB() }); allocations != 0 {
				b.Fatalf("direct median allocates %g", allocations)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var got float64
			var ok bool
			for i := 0; i < b.N; i++ {
				got, ok = entry.h.p50DB()
			}
			b.StopTimer()
			medianSpeedLogTiming(b)
			if got != want || ok != wantOK || exactP50Snapshot(&entry.h) != before {
				b.Fatalf("direct median differs from Rat or mutates: got=%g/%v expected=%g/%v", got, ok, want, wantOK)
			}
		})
	}
}

// This synthetic running-process workload preserves the existing bounded
// update/admission pattern, adding contribution-mode coverage and untimed exact
// final-output guards. Parallel cap admission retains arrival-order dependence.
func BenchmarkMedianSpeedRuntime(b *testing.B) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		b.Run(mode, func(b *testing.B) {
			for _, parallel := range []bool{false, true} {
				kind := "Serial"
				if parallel {
					kind = "Parallel"
				}
				b.Run(kind, func(b *testing.B) {
					fixture := newMedianSpeedRuntimeFixture(b, mode)
					var sequence atomic.Uint64
					var failed atomic.Bool
					b.ReportAllocs()
					b.ResetTimer()
					if parallel {
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if !fixture.step(sequence.Add(1) - 1) {
									failed.Store(true)
								}
							}
						})
					} else {
						for i := 0; i < b.N; i++ {
							if !fixture.step(uint64(i)) {
								failed.Store(true)
							}
						}
						sequence.Store(uint64(b.N))
					}
					b.StopTimer()
					medianSpeedLogTiming(b)
					if failed.Load() || sequence.Load() != uint64(b.N) {
						b.Fatal("runtime fixture lost an observed result or operation")
					}
					query := fixture.base.Add(time.Duration(sequence.Load()/accountingRuntimeClockStep) * time.Second)
					medianSpeedRuntimeGuard(b, fixture, query)
					b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
					b.ReportMetric(float64(fixture.predictor.TotalBuckets()), "resident-buckets")
					b.ReportMetric(float64(sequence.Load()/accountingRuntimeClockStep), "synthetic-seconds")
					b.ReportMetric(float64(unsafe.Sizeof(bucket{})), "logical-B/bucket")
					accountingWriteLiveHeap(b, fixture.predictor)
					runtime.KeepAlive(fixture)
				})
			}
		})
	}
}

// Benchmark display ns/op is rounded. Keep the stopped timer's integer elapsed
// duration so acceptance ratios are lossless elapsed_ns/N fractions.
func medianSpeedLogTiming(b *testing.B) {
	b.Helper()
	b.Logf("MEDIAN_SPEED_TIMING N=%d elapsed_ns=%d", b.N, b.Elapsed().Nanoseconds())
}

func newMedianSpeedRuntimeFixture(tb testing.TB, mode string) *accountingRuntimeFixture {
	tb.Helper()
	cfg := accountingRuntimeConfig()
	cfg.ReceiverContributionMode = mode
	fixture := &accountingRuntimeFixture{predictor: NewPredictor(cfg, []string{"20m"}), paths: make([]accountingRuntimePath, accountingRuntimePaths), counts: make([]accountingRuntimeCounts, accountingRuntimePaths), base: time.Unix(1_700_000_000, 0).UTC()}
	for i := range fixture.paths {
		fixture.paths[i] = accountingPath(i)
		accountingSeedPath(fixture.predictor, fixture.paths[i], fixture.base, true)
	}
	medianSpeedRuntimeGuard(tb, fixture, fixture.base)
	return fixture
}

func medianSpeedRuntimeGuard(tb testing.TB, fixture *accountingRuntimeFixture, query time.Time) {
	tb.Helper()
	sidecars, buckets := 0, 0
	for i := range fixture.predictor.combined.shards {
		shard := &fixture.predictor.combined.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.buckets {
			buckets++
			if entry.extraSlots != nil {
				sidecars++
			}
		}
		shard.mu.RUnlock()
	}
	wantSidecars := 2 * len(fixture.paths)
	if fixture.predictor.cfg.ReceiverContributionMode == ReceiverContributionOff {
		wantSidecars = 0
	}
	if buckets != 4*len(fixture.paths) || sidecars != wantSidecars || unsafe.Sizeof(bucket{}) != 448 {
		tb.Fatalf("runtime retained owners=%d/%d want=%d/%d bucket=%d", buckets, sidecars, 4*len(fixture.paths), wantSidecars, unsafe.Sizeof(bucket{}))
	}
	seed := uint64(accountingRuntimeReceivers * accountingRuntimeSeedCount)
	for i, path := range fixture.paths {
		for _, beacon := range []bool{false, true} {
			corpus := medianSpeedCorpus{predictor: fixture.predictor, base: query, beacon: beacon}
			var sample sampleWithBins
			var result Result
			if beacon {
				sample, _, _ = fixture.predictor.receiveFromStoreWithDistribution(fixture.predictor.combined, path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", 2, query)
				result = fixture.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 2, 1, query)
			} else {
				sample, _, _ = fixture.predictor.mergeFromStoreWithDistribution(fixture.predictor.combined, path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", 2, query)
				result = fixture.predictor.Predict(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 2, query)
			}
			want, available := exactP50Reference(sample.snrBins)
			class, glyph := medianSpeedFT8Class(want, corpus.predictor.cfg)
			wantRaw := seed + fixture.counts[i].forward.Load()
			if !beacon {
				wantRaw += seed + fixture.counts[i].reverse.Load()
			}
			if !available || !result.HasP50 || result.P50DB != want || result.Class != class || result.Glyph != glyph || result.Source != SourceCombined || result.RawCount != uint32(wantRaw) || result.Weight <= 0 || !result.ObservedDistributionAvailable || result.ObservedSamplingUnix < query.Unix() || (fixture.predictor.cfg.ReceiverContributionMode == ReceiverContributionEnforce && result.ReceiverCount < result.ReceiverRequired) {
				tb.Fatalf("runtime path%d beacon=%v result=%+v Rat=%g/%v raw=%d", i, beacon, result, want, available, wantRaw)
			}
		}
	}
}

func TestMedianSpeedDirectAllocations(t *testing.T) {
	for _, entry := range medianSpeedDirectCases() {
		t.Run(entry.name, func(t *testing.T) {
			checkExactP50Reference(t, entry.h)
			if allocations := testing.AllocsPerRun(100, func() { entry.h.p50DB() }); allocations != 0 {
				t.Fatalf("direct branch allocates %g", allocations)
			}
		})
	}
}

func TestMedianSpeedRuntimePreflight(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			fixture := newMedianSpeedRuntimeFixture(t, mode)
			const operations = accountingRuntimeClockStep + accountingRuntimePaths
			for i := 0; i < operations; i++ {
				if !fixture.step(uint64(i)) {
					t.Fatalf("observed estimate missing at operation%d", i)
				}
			}
			query := fixture.base.Add(time.Duration(operations/accountingRuntimeClockStep) * time.Second)
			medianSpeedRuntimeGuard(t, fixture, query)
		})
	}
}
