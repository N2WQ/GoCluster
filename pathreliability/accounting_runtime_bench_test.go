package pathreliability

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

const (
	accountingRuntimePaths     = 4096
	accountingRuntimeReceivers = 12
	accountingRuntimeSeedCount = 8
	accountingRuntimeClockStep = accountingRuntimePaths * 8
	accountingRuntimeMaxSecs   = 365 * 24 * 60 * 60
)

type accountingRuntimePath struct {
	receiver, sender, receiverCoarse, senderCoarse CellID
}

type accountingRuntimeCounts struct{ forward, reverse atomic.Uint64 }

type accountingRuntimeFixture struct {
	predictor *Predictor
	paths     []accountingRuntimePath
	counts    []accountingRuntimeCounts
	base      time.Time
}

// BenchmarkPathAccountingRuntime is a running-process fixture, not external RF
// traffic. Fixed synthetic keys keep endpoint mappings stable and retain no
// event history. -test.cpu=4 selects four RunParallel workers. Native CPU/mutex
// profiling flags capture the actual Store/Predictor work in this process.
func BenchmarkPathAccountingRuntime(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "Serial"
		if parallel {
			name = "Parallel"
		}
		b.Run(name, func(b *testing.B) {
			fixture := newAccountingRuntimeFixture(b)
			var sequence atomic.Uint64
			var failed atomic.Bool
			b.ReportAllocs()
			b.ResetTimer()
			if parallel {
				b.SetParallelism(1)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if !fixture.step(sequence.Add(1) - 1) {
							failed.Store(true)
							return
						}
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					if !fixture.step(uint64(i)) {
						failed.Store(true)
						break
					}
				}
				sequence.Store(uint64(b.N))
			}
			b.StopTimer()
			if failed.Load() || sequence.Load() != uint64(b.N) {
				b.Fatal("runtime fixture lost an observed estimate or exceeded its bounded clock")
			}
			query := fixture.base.Add(time.Duration(sequence.Load()/accountingRuntimeClockStep) * time.Second)
			fixture.guard(b, query)
			b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
			b.ReportMetric(float64(fixture.predictor.TotalBuckets()), "resident-buckets")
			b.ReportMetric(float64(sequence.Load()/accountingRuntimeClockStep), "synthetic-seconds")
			accountingWriteLiveHeap(b, fixture.predictor)
			runtime.KeepAlive(fixture)
		})
	}
}

func newAccountingRuntimeFixture(b *testing.B) *accountingRuntimeFixture {
	b.Helper()
	fixture := &accountingRuntimeFixture{
		predictor: NewPredictor(accountingRuntimeConfig(), []string{"20m"}),
		paths:     make([]accountingRuntimePath, accountingRuntimePaths),
		counts:    make([]accountingRuntimeCounts, accountingRuntimePaths),
		base:      time.Unix(1_700_000_000, 0).UTC(),
	}
	for i := range fixture.paths {
		fixture.paths[i] = accountingPath(i)
		accountingSeedPath(fixture.predictor, fixture.paths[i], fixture.base, true)
	}
	fixture.guard(b, fixture.base)
	return fixture
}

// step advances synthetic time only after a bounded batch, rather than turning
// arbitrary b.N directly into a duration. Eight seeded reports per receiver
// fill caps; advancing time opens fractional room for fresh and delayed input.
func (fixture *accountingRuntimeFixture) step(sequence uint64) bool {
	seconds := sequence / accountingRuntimeClockStep
	if seconds >= accountingRuntimeMaxSecs {
		return false
	}
	index := int(sequence % accountingRuntimePaths)
	path := fixture.paths[index]
	cycle := sequence / accountingRuntimePaths
	reverse := cycle&1 != 0
	if reverse {
		path = path.reversed()
	}
	arrival := fixture.base.Add(time.Duration(seconds) * time.Second)
	observed := arrival
	if cycle%4 == 3 {
		observed = observed.Add(-60 * time.Second)
	}
	receiver := (cycle/2)%accountingRuntimeReceivers + 1
	weight := 1.0
	if cycle%3 == 2 {
		weight = 2
	}
	accountingUpdate(fixture.predictor, path, -21+float64(receiver), weight, observed, arrival, receiver)
	if reverse {
		fixture.counts[index].reverse.Add(1)
	} else {
		fixture.counts[index].forward.Add(1)
	}
	path = fixture.paths[index]
	if !accountingObserved(fixture.predictor.Predict(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 2, arrival)) {
		return false
	}
	return sequence&3 != 0 || accountingObserved(fixture.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 2, 1, arrival))
}

func (fixture *accountingRuntimeFixture) guard(b *testing.B, query time.Time) {
	b.Helper()
	accountingGuardOwners(b, fixture.predictor, 4*len(fixture.paths), 2*len(fixture.paths))
	seed := uint64(accountingRuntimeReceivers * accountingRuntimeSeedCount)
	for i, path := range fixture.paths {
		normal := fixture.predictor.Predict(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 2, query)
		beacon := fixture.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 2, 1, query)
		wantForward := seed + fixture.counts[i].forward.Load()
		wantNormal := wantForward + seed + fixture.counts[i].reverse.Load()
		if !accountingObserved(normal) || !accountingObserved(beacon) || uint64(normal.RawCount) != wantNormal || uint64(beacon.RawCount) != wantForward {
			b.Fatalf("path %d lost admission/distribution: normal=%+v beacon=%+v want counts=%d/%d", i, normal, beacon, wantNormal, wantForward)
		}
	}
}

// BenchmarkPathAccountingRetained measures live HeapAlloc after collection,
// independently of logical struct size. Use -test.benchtime=1x for fixed 20k/40k
// populations. Purge/compact/refreshed-zero checks run outside the timed fill.
func BenchmarkPathAccountingRetained(b *testing.B) {
	for _, paths := range []int{10_000, 20_000} {
		name := "Buckets20000"
		if paths == 20_000 {
			name = "Buckets40000"
		}
		b.Run(name, func(b *testing.B) {
			var retained, empty float64
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				live, cleared := accountingRetainedIteration(b, paths)
				retained += float64(live)
				empty += float64(cleared)
			}
			b.ReportMetric(retained/float64(b.N), "live-retained-B")
			b.ReportMetric(retained/(float64(b.N)*float64(paths*2)), "retained-B/bucket")
			b.ReportMetric(empty/float64(b.N), "empty-store-B")
			b.ReportMetric(float64(unsafe.Sizeof(bucket{})), "logical-B/bucket")
			b.ReportMetric(float64(paths), "resident-sidecars")
		})
	}
}

func accountingRetainedIteration(b *testing.B, paths int) (int64, int64) {
	b.Helper()
	b.StopTimer()
	runtime.GC()
	var before, live, empty runtime.MemStats
	runtime.ReadMemStats(&before)
	base := time.Unix(1_700_000_000, 0).UTC()
	b.StartTimer()
	predictor := NewPredictor(accountingRuntimeConfig(), []string{"20m"})
	for i := 0; i < paths; i++ {
		accountingSeedPath(predictor, accountingPath(i), base, false)
	}
	b.StopTimer()
	accountingGuardOwners(b, predictor, paths*2, paths)
	accountingGuardCache(b, predictor, base, paths)
	if !accountingObserved(predictor.Predict(1, 1001, 1, 401, "20m", "FT8", 2, base)) {
		b.Fatal("retained fixture has no observed p50")
	}
	runtime.GC()
	runtime.ReadMemStats(&live)
	runtime.KeepAlive(predictor)
	delta := int64(live.HeapAlloc) - int64(before.HeapAlloc)
	if delta <= 0 {
		b.Fatalf("live retained heap is not measurable: before=%d live=%d", before.HeapAlloc, live.HeapAlloc)
	}
	accountingWriteLiveHeap(b, predictor)
	purgeAt := base.Add(721 * time.Second)
	accountingPurgeGuard(b, predictor, purgeAt)
	// Insert a different finite key population after deletion, then purge again.
	for i := 0; i < paths; i++ {
		accountingSeedPath(predictor, accountingPath(20_000+i), purgeAt, false)
	}
	accountingGuardOwners(b, predictor, paths*2, paths)
	accountingGuardCache(b, predictor, purgeAt, paths)
	accountingPurgeGuard(b, predictor, purgeAt.Add(721*time.Second))
	runtime.GC()
	runtime.ReadMemStats(&empty)
	runtime.KeepAlive(predictor)
	return delta, int64(empty.HeapAlloc) - int64(before.HeapAlloc)
}

func accountingRuntimeConfig() Config {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.ReceiverFineSlots, cfg.ReceiverCoarseSlots = 6, 12
	cfg.ReceiverMaxEffectiveCount, cfg.ReceiverMaxEffectiveWeight = 8, 8
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = 1, 1
	cfg.MinEffectiveWeight = 0.01
	cfg.DefaultHalfLifeSec = 240
	cfg.BandHalfLifeSec = map[string]int{"20m": 240}
	cfg.StaleAfterHalfLifeMultiplier = 3
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1.25
	return cfg
}

// Each synthetic fine endpoint always has the same coarse endpoint; identifiers
// stay below 1201/601 even across the 40k-path churn population. No H3 lookup or
// geographic accuracy assertion is part of this resource fixture.
func accountingPath(index int) accountingRuntimePath {
	receiver := CellID(index%200 + 1)
	row := index / 200
	return accountingRuntimePath{receiver, CellID(1001 + row), receiver, CellID(401 + row)}
}

func (path accountingRuntimePath) reversed() accountingRuntimePath {
	return accountingRuntimePath{path.sender, path.receiver, path.senderCoarse, path.receiverCoarse}
}

func accountingSeedPath(predictor *Predictor, path accountingRuntimePath, at time.Time, bidirectional bool) {
	for receiver := uint64(1); receiver <= accountingRuntimeReceivers; receiver++ {
		for report := 0; report < accountingRuntimeSeedCount; report++ {
			accountingUpdate(predictor, path, -21+float64(receiver), 1, at, at, receiver)
			if bidirectional {
				accountingUpdate(predictor, path.reversed(), -21+float64(receiver), 1, at, at, receiver)
			}
		}
	}
}

// The sole baseline adaptation is replacing this ObservationTimes argument with
// observed in an isolated a4cbe65 fixture copy. Ignore the new return value; raw
// count guards independently detect dropped updates on both implementations.
func accountingUpdate(predictor *Predictor, path accountingRuntimePath, db, weight float64, observed, arrived time.Time, receiver uint64) {
	predictor.UpdateWithReceiverHash(BucketCombined, path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", db, weight, ObservationTimes{ObservedAt: observed, ArrivedAt: arrived}, false, receiver)
}

func accountingObserved(result Result) bool {
	return result.Source == SourceCombined && result.HasP50 && result.P50DB != 0 && result.Weight > 0 && !math.IsNaN(result.P50DB) && !math.IsInf(result.P50DB, 0) && !math.IsNaN(result.Weight) && !math.IsInf(result.Weight, 0)
}

func accountingGuardOwners(b *testing.B, predictor *Predictor, buckets, sidecars int) {
	b.Helper()
	actualBuckets, actualSidecars := 0, 0
	for i := range predictor.combined.shards {
		shard := &predictor.combined.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.buckets {
			actualBuckets++
			if entry == nil {
				shard.mu.RUnlock()
				b.Fatal("retained bucket map contains a nil owner")
			}
			if entry.extraSlots != nil {
				actualSidecars++
				if entry.extraSlots.slots == nil {
					shard.mu.RUnlock()
					b.Fatal("coarse sidecar lacks its receiver array")
				}
			}
			populated := 0
			for slot := 0; slot < accountingRuntimeReceivers; slot++ {
				value := entry.receiverSlotAt(slot, false)
				if value == nil || value.hash == 0 {
					continue
				}
				populated++
				if value.weight < 0 || value.count < 0 || value.weight > 8+1e-8 || value.count > 8+1e-8 || math.IsNaN(value.weight) || math.IsNaN(value.count) || math.IsInf(value.weight, 0) || math.IsInf(value.count, 0) {
					shard.mu.RUnlock()
					b.Fatalf("receiver trust exceeded its finite cap: %+v", value)
				}
			}
			wantPopulated := 6
			if entry.extraSlots != nil {
				wantPopulated = accountingRuntimeReceivers
			}
			if populated != wantPopulated {
				shard.mu.RUnlock()
				b.Fatalf("receiver population=%d want=%d", populated, wantPopulated)
			}
		}
		shard.mu.RUnlock()
	}
	if actualBuckets != buckets || actualSidecars != sidecars || predictor.TotalBuckets() != buckets {
		b.Fatalf("retained owners=%d/%d want=%d/%d", actualBuckets, actualSidecars, buckets, sidecars)
	}
}

func accountingGuardCache(b *testing.B, predictor *Predictor, at time.Time, paths int) {
	b.Helper()
	predictor.RefreshStatsSnapshot(at)
	stats := predictor.Stats(at)
	bands := predictor.StatsByBand(at)
	if stats.CombinedFine != paths || stats.CombinedCoarse != paths || len(bands) != 1 || bands[0].Fine != paths || bands[0].Coarse != paths {
		b.Fatalf("refreshed cache counts=%+v bands=%+v want paths=%d", stats, bands, paths)
	}
}

func accountingPurgeGuard(b *testing.B, predictor *Predictor, at time.Time) {
	b.Helper()
	predictor.PurgeStale(at)
	predictor.Compact(1, 0.5)
	accountingGuardOwners(b, predictor, 0, 0)
	accountingGuardCache(b, predictor, at, 0)
}

// Optional live heap output uses explicit task-only environment variables.
// The artifact root must be an existing ignored .tmp/path-timestamp-v2 directory;
// the resolved parent must remain within it. No global Store is retained.
func accountingWriteLiveHeap(b *testing.B, owner *Predictor) {
	b.Helper()
	target := os.Getenv("GOCLUSTER_PATH_ACCOUNTING_HEAP_PROFILE")
	if target == "" {
		return
	}
	root := os.Getenv("GOCLUSTER_PATH_ACCOUNTING_ARTIFACT_ROOT")
	if root == "" {
		b.Fatal("heap profile requires an explicit task artifact root")
	}
	root, err := filepath.Abs(root)
	if err != nil || filepath.Base(root) != "path-timestamp-v2" || filepath.Base(filepath.Dir(root)) != ".tmp" {
		b.Fatal("heap profile requires the existing ignored task artifact root")
	}
	target, err = filepath.Abs(target)
	if err != nil {
		b.Fatalf("resolve heap profile: %v", err)
	}
	extension := filepath.Ext(target)
	target = strings.TrimSuffix(target, extension) + "-" + strings.ReplaceAll(b.Name(), "/", "-") + extension
	resolvedRoot, rootErr := filepath.EvalSymlinks(root)
	resolvedParent, parentErr := filepath.EvalSymlinks(filepath.Dir(target))
	if rootErr != nil || parentErr != nil || !accountingPathWithin(resolvedRoot, resolvedParent) {
		b.Fatal("heap profile parent must exist inside the resolved task artifact root")
	}
	target = filepath.Join(resolvedParent, filepath.Base(target))
	if info, statErr := os.Lstat(target); statErr == nil {
		if !info.Mode().IsRegular() {
			b.Fatal("live heap profile target must be a regular file")
		}
	} else if !os.IsNotExist(statErr) {
		b.Fatalf("inspect live heap profile target: %v", statErr)
	}
	file, err := os.Create(target)
	if err != nil {
		b.Fatalf("create live heap profile: %v", err)
	}
	runtime.GC()
	writeErr := pprof.WriteHeapProfile(file)
	closeErr := file.Close()
	runtime.KeepAlive(owner)
	if writeErr != nil || closeErr != nil {
		b.Fatalf("live heap profile write=%v close=%v", writeErr, closeErr)
	}
	b.Logf("live_heap_profile=%s", target)
}

func accountingPathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
