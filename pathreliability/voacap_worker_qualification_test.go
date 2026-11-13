// File role: Opt-in bounded whole-worker evidence, separate from protected
// baseline/candidate bookkeeping timings. Fake work is never native evidence.
package pathreliability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/internal/voacap"
	"github.com/cockroachdb/pebble"
)

type v4WorkerPhase struct {
	Name           string                              `json:"name"`
	AtUTC          time.Time                           `json:"at_utc"`
	Statuses       [VOACAPForecastCheckStatusCount]int `json:"ordinary_status_counts"`
	Snapshot       VOACAPClosedFallbackSnapshot        `json:"snapshot"`
	HeapAllocBytes uint64                              `json:"heap_alloc_bytes"`
	HeapObjects    uint64                              `json:"heap_objects"`
}

type v4WorkerQualificationReport struct {
	Schema             int                 `json:"schema"`
	Native             bool                `json:"native"`
	Input              string              `json:"input"`
	Capacity           int                 `json:"capacity"`
	RequestedCycles    int                 `json:"requested_cycles"`
	FakeServiceMicros  int                 `json:"fake_service_microseconds"`
	PersistedSamples   int                 `json:"persisted_samples_checked"`
	HeapProfileState   string              `json:"heap_profile_state,omitempty"`
	HeapProfileOwners  int                 `json:"heap_profile_owners,omitempty"`
	ElapsedSeconds     float64             `json:"elapsed_seconds"`
	HydrationHeapBytes int64               `json:"hydration_heap_bytes"`
	ForecasterCalls    int64               `json:"forecaster_calls"`
	Stats              VOACAPFallbackStats `json:"stats"`
	Phases             []v4WorkerPhase     `json:"phases"`
}

func v4QualificationInt(t *testing.T, name string, fallback, minimum, maximum int) int {
	t.Helper()
	text := os.Getenv(name)
	if text == "" {
		return fallback
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < minimum || value > maximum {
		t.Fatalf("%s must be between %d and %d", name, minimum, maximum)
	}
	return value
}

func v4QualificationWrite(t *testing.T, path string, report v4WorkerQualificationReport) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func v4QualificationWait(t *testing.T, f *VOACAPClosedFallback, target uint64) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if snap := f.Snapshot(); snap.WorkerDurationCount == target && snap.InflightEntries == 0 && !snap.WorkerBusy {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("worker completion fence not reached target=%d actual=%+v", target, f.Snapshot())
}

func v4QualificationProfile(t *testing.T, env string) func() {
	t.Helper()
	path := os.Getenv(env)
	if path == "" {
		return func() {}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		file.Close()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func v4QualificationHeap(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	writeErr := pprof.WriteHeapProfile(file)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("Go heap profile write/close: %v %v", writeErr, closeErr)
	}
}

func v4QualificationSolar(provider *v4SolarProvider, ssn int) {
	provider.set(ssn, true)
	// The controlled SSN is real input to the worker, but is not a fetched NOAA
	// observation or a claimed flux conversion. Keep its provenance unknown.
	provider.mu.Lock()
	provider.input.Provenance = voacap.SolarInputProvenance{}
	provider.mu.Unlock()
}

// These bounded representative checks follow the full-cycle fence. Accounting
// alone cannot prove that the actual input or its completed value was stored.
func v4QualificationStored(t *testing.T, f *VOACAPClosedFallback, store *VOACAPForecastCacheStore, ssn int, cells ...int) int {
	t.Helper()
	snap := f.Snapshot()
	index := snap.ForecastCacheIndex
	if !index.Ready || index.Dirty || index.Entries != index.RetentionEntries || index.Entries != index.EligibilityEntries || index.Entries != snap.CacheEntries {
		t.Fatalf("post-completion index/live ownership not trusted: %+v", snap)
	}
	checked := 0
	for i, cell := range cells {
		if i > 0 && cell == cells[i-1] {
			continue
		}
		req := testClosedRequestWithDX(CellID(cell))
		key := f.cacheKey(req)
		forecast, ok := f.CheckCachedForecast(req, f.now())
		if !ok || forecast.SSN != ssn {
			t.Fatalf("post-completion actual input cell=%d got=%+v usable=%t expected_ssn=%d", cell, forecast, ok, ssn)
		}
		f.mu.Lock()
		entry, exists := f.cache[key]
		f.mu.Unlock()
		if !exists || entry.metadata == nil || len(entry.forecast.Records) != f.fallback.ForecastHours {
			t.Fatalf("completed cache payload missing cell=%d", cell)
		}
		value, closer, err := store.db.Get(voacapForecastCacheKeyBytes(key))
		if err != nil {
			t.Fatalf("completed raw disk owner cell=%d: %v", cell, err)
		}
		var record voacapForecastCacheDiskRecord
		decodeErr := json.Unmarshal(value, &record)
		closeErr := closer.Close()
		if decodeErr != nil || closeErr != nil {
			t.Fatalf("completed raw decode/close cell=%d: %v %v", cell, decodeErr, closeErr)
		}
		if record.SchemaVersion != voacapForecastCacheSchemaVersion || record.ModelGeneration != voacapForecastCacheModelGeneration || record.Metadata.SolarInput.SSN != ssn || record.Metadata.SolarInput != entry.metadata.solar || record.WindowUnixNS != entry.forecast.WindowStartUTC.UnixNano() || record.Metadata.WindowEndUnixNS != entry.windowEndNS || record.Metadata.ExpiresUnixNS != entry.expiresNS || record.Metadata.OriginUnixNS != entry.metadata.originNS || len(record.Records) != len(entry.forecast.Records) {
			t.Fatalf("published/stored immutable owner differs cell=%d record=%+v", cell, record)
		}
		for j, row := range record.Records {
			if row != entry.forecast.Records[j] {
				t.Fatalf("published/stored row differs cell=%d row=%d", cell, j)
			}
		}
		checked++
	}
	return checked
}

// TestVOACAPV4WorkerQualification exercises actual cache/queue/store ownership
// with controlled time. Set GOCLUSTER_VOACAP_V4_WORKER_OUTPUT; optional ENTRIES
// (128..50000), CYCLES (1..512), SERVICE_US (0..10000) and CPU use the same
// GOCLUSTER_VOACAP_V4_WORKER_ prefix. Setup seeding is excluded from CPU capture.
func TestVOACAPV4WorkerQualification(t *testing.T) {
	output := os.Getenv("GOCLUSTER_VOACAP_V4_WORKER_OUTPUT")
	if output == "" {
		t.Skip("opt-in whole-worker fake-service qualification")
	}
	capacity := v4QualificationInt(t, "GOCLUSTER_VOACAP_V4_WORKER_ENTRIES", 50000, 128, 50000)
	cycles := v4QualificationInt(t, "GOCLUSTER_VOACAP_V4_WORKER_CYCLES", 256, 1, 512)
	if cycles > capacity/4 {
		cycles = capacity / 4
	}
	serviceUS := v4QualificationInt(t, "GOCLUSTER_VOACAP_V4_WORKER_SERVICE_US", 0, 0, 10000)
	base := time.Date(2026, 1, 30, 0, 0, 0, 0, time.UTC)
	realAnchor := time.Now()
	var clock, calls atomic.Int64
	clock.Store(base.UnixNano())
	var failNext, holdNext atomic.Bool
	var entered, release chan struct{}
	var gateOnce *sync.Once
	forecaster := fakeClosedForecaster{fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		calls.Add(1)
		if holdNext.Swap(false) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return VOACAPClosedForecast{}, ctx.Err()
			}
		}
		if serviceUS > 0 {
			timer := time.NewTimer(time.Duration(serviceUS) * time.Microsecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return VOACAPClosedForecast{}, ctx.Err()
			}
		}
		if failNext.Swap(false) {
			return VOACAPClosedForecast{}, errors.New("qualification failure")
		}
		return v4FullForecast(job, 24), nil
	}}
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.ForecastHours = 24
	cfg.VOACAPFallback.CacheTTLSeconds = 86400
	cfg.VOACAPFallback.DelaySeconds = 600
	cfg.VOACAPFallback.MaxCacheEntries = capacity
	cfg.VOACAPFallback.MaxDelayEntries = 50000
	cfg.VOACAPFallback.MaxQueueDepth = 1000
	provider := &v4SolarProvider{}
	v4QualificationSolar(provider, 112)
	f := newTestClosedFallback(t, cfg, forecaster, provider)
	f.now = func() time.Time { return time.Unix(0, clock.Load()).UTC().Add(time.Since(realAnchor)) }
	store := openTestVOACAPForecastCacheStore(t)
	t.Cleanup(func() { closeTestVOACAPForecastCacheStore(t, store) })
	const migration = 32
	batch := store.db.NewBatch()
	for i := 1; i <= capacity-migration; i++ {
		req := testClosedRequest()
		req.DXCell = CellID(i)
		entry := v4Entry(f, req, base, base, 112)
		value, err := json.Marshal(diskRecordFromForecastCache(f.cacheKey(req), entry))
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(voacapForecastCacheKeyBytes(f.cacheKey(req)), value, nil); err != nil {
			t.Fatal(err)
		}
		if i%voacapForecastCachePruneChunkSize == 0 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			batch.Close()
			batch = store.db.NewBatch()
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	for i := capacity - migration + 1; i <= capacity; i++ {
		v4SeedLegacy(t, store, base.Add(time.Minute), CellID(i), nil)
	}
	if err := store.db.Flush(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	realAnchor = started
	stopProfile := v4QualificationProfile(t, "GOCLUSTER_VOACAP_V4_WORKER_CPU")
	restored, err := f.AttachForecastCacheStore(store, base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if restored.Loaded != capacity-migration || restored.RebuildEligible != migration {
		t.Fatalf("mixed restart ownership: %+v", restored)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	startV4Fixture(t, f)
	t.Cleanup(func() {
		if gateOnce != nil {
			gateOnce.Do(func() { close(release) })
		}
	})
	report := v4WorkerQualificationReport{Schema: 1, Input: "controlled clock and SSN; synthetic forecaster; no native execution or live solar feed", Capacity: capacity, RequestedCycles: cycles, FakeServiceMicros: serviceUS, HydrationHeapBytes: int64(after.HeapAlloc) - int64(before.HeapAlloc)}
	var phaseCounts [VOACAPForecastCheckStatusCount]int
	issue := func(cell int) VOACAPForecastCheck {
		req := testClosedRequest()
		req.DXCell = CellID(cell)
		check := f.CheckForecastDetailed(req, f.now())
		phaseCounts[check.Status]++
		return check
	}
	capture := func(name string) {
		input, known := provider.CurrentSolarInput(f.now())
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		report.Phases = append(report.Phases, v4WorkerPhase{Name: name, AtUTC: f.now(), Statuses: phaseCounts, Snapshot: f.DedicatedDiagnosticSnapshot(f.now(), input.SSN, known), HeapAllocBytes: mem.HeapAlloc, HeapObjects: mem.HeapObjects})
		phaseCounts = [VOACAPForecastCheckStatusCount]int{}
	}
	arm := func() {
		entered = make(chan struct{})
		release = make(chan struct{})
		gateOnce = &sync.Once{}
		holdNext.Store(true)
	}
	unblock := func() { gateOnce.Do(func() { close(release) }) }
	target := uint64(0)
	// Retirement remains request driven. These markers fill the remaining
	// capacity; no eager startup work exists.
	clock.Store(base.Add(time.Minute).UnixNano())
	for i := capacity - migration + 1; i <= capacity; i++ {
		if got := issue(i); got.Status != VOACAPForecastCheckQueued {
			t.Fatalf("retired demand not immediate: %+v", got)
		}
	}
	target += migration
	v4QualificationWait(t, f, target)
	report.PersistedSamples += v4QualificationStored(t, f, store, 112, capacity-migration+1, capacity-migration/2, capacity)
	capture("retired-identity-demand")
	if path := os.Getenv("GOCLUSTER_VOACAP_V4_WORKER_HEAP"); path != "" {
		v4QualificationHeap(t, path)
		report.HeapProfileState = "full capacity after hydration and requested retired-key rebuilds, after GC; not exact peak"
		report.HeapProfileOwners = f.Snapshot().CacheEntries
		if report.HeapProfileOwners != capacity {
			t.Fatalf("full-capacity heap witness has %d owners expected %d", report.HeapProfileOwners, capacity)
		}
	}
	clock.Store(base.Add(23 * time.Hour).UnixNano())
	for i := 1; i <= cycles; i++ {
		if got := issue(i); got.Status != VOACAPForecastCheckReady {
			t.Fatalf("final-hour owner unavailable: %+v", got)
		}
	}
	target += uint64(cycles)
	v4QualificationWait(t, f, target)
	report.PersistedSamples += v4QualificationStored(t, f, store, 112, 1, (cycles+1)/2, cycles)
	capture("same-input-final-hour-renewal")
	for _, change := range []struct {
		hour int
		ssn  int
	}{{36, 113}, {60, 114}} {
		clock.Store(base.Add(time.Duration(change.hour) * time.Hour).UnixNano())
		v4QualificationSolar(provider, change.ssn)
		for i := 1; i <= cycles; i++ {
			if got := issue(i); got.Status != VOACAPForecastCheckReady || got.Forecast.SSN == change.ssn {
				t.Fatalf("warm input replacement did not preserve old actual input: %+v", got)
			}
		}
		immediate := 0
		if change.ssn == 114 {
			immediate = min(cycles, 32)
			target += uint64(immediate)
			v4QualificationWait(t, f, target)
		}
		capture(fmt.Sprintf("ssn-%d-initial-demand", change.ssn))
		clock.Add(int64(601 * time.Second))
		for i := 1; i <= cycles; i++ {
			issue(i)
		}
		target += uint64(cycles - immediate)
		v4QualificationWait(t, f, target)
		report.PersistedSamples += v4QualificationStored(t, f, store, change.ssn, 1, (cycles+1)/2, cycles)
		capture(fmt.Sprintf("ssn-%d-replacement", change.ssn))
		if change.ssn == 113 {
			clock.Store(base.Add(37 * time.Hour).UnixNano())
			commands := min(cycles, 32)
			for i := 1; i <= commands; i++ {
				req := testClosedRequest()
				req.DXCell = CellID(i)
				f.CheckForecastWindowWait(req, f.now(), 0)
			}
			target += uint64(commands)
			v4QualificationWait(t, f, target)
			report.PersistedSamples += v4QualificationStored(t, f, store, 113, 1, (commands+1)/2, commands)
			capture("partial-command-refresh")
			clock.Store(base.Add(59 * time.Hour).UnixNano())
			for i := commands + 1; i <= cycles; i++ {
				issue(i)
			}
			target += uint64(cycles - commands)
			v4QualificationWait(t, f, target)
			report.PersistedSamples += v4QualificationStored(t, f, store, 113, 1, (cycles+1)/2, cycles)
			capture("same-input-final-hour-59h-non-command-paths")
		}
	}
	clock.Store(base.Add(83*time.Hour + 30*time.Minute).UnixNano())
	failNext.Store(true)
	issue(1)
	target++
	v4QualificationWait(t, f, target)
	if got := issue(1); got.Status != VOACAPForecastCheckReady || f.Snapshot().InflightEntries != 0 {
		t.Fatalf("ordinary renewal ignored retry: %+v", got)
	}
	_, status := f.CheckForecastWindowWait(testClosedRequestWithDX(1), f.now(), 0)
	if status != VOACAPForecastWindowRefreshing {
		t.Fatalf("command did not bypass retry: %v", status)
	}
	target++
	v4QualificationWait(t, f, target)
	report.PersistedSamples += v4QualificationStored(t, f, store, 114, 1)
	capture("failure-backoff-command-retry")
	clock.Store(base.Add(108 * time.Hour).UnixNano())
	if got := issue(1); got.Status != VOACAPForecastCheckDelayWait {
		t.Fatalf("expired cold owner bypassed warmup: %+v", got)
	}
	capture("post-expiry-gap")
	clock.Add(int64(601 * time.Second))
	issue(1)
	target++
	v4QualificationWait(t, f, target)
	report.PersistedSamples += v4QualificationStored(t, f, store, 114, 1)
	// Hold one active job to witness actual queue saturation and oldest age.
	clock.Store(base.Add(109 * time.Hour).UnixNano())
	for i := 1; i <= 1002; i++ {
		issue(capacity + 100 + i)
	}
	clock.Add(int64(601 * time.Second))
	arm()
	issue(capacity + 101)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("queue service gate not entered")
	}
	for i := 2; i <= 1002; i++ {
		issue(capacity + 100 + i)
	}
	time.Sleep(2 * time.Second)
	capture("saturated-queue")
	saturated := report.Phases[len(report.Phases)-1]
	if saturated.Snapshot.QueueDepth != 1000 || !saturated.Snapshot.QueueOldestAgeKnown || saturated.Snapshot.QueueOldestAge < 2*time.Second || saturated.Statuses[VOACAPForecastCheckQueueFull] != 1 {
		t.Fatalf("queue/rejection witness failed: %+v", saturated)
	}
	unblock()
	target += 1001
	v4QualificationWait(t, f, target)
	if got := issue(capacity + 1102); got.Status != VOACAPForecastCheckQueued {
		t.Fatalf("queue-full demand lost elapsed warmup: %+v", got)
	}
	target++
	v4QualificationWait(t, f, target)
	report.PersistedSamples += v4QualificationStored(t, f, store, 114, capacity+1100, capacity+1101, capacity+1102)
	capture("drained-rejected-demand-retry")
	report.ForecasterCalls = calls.Load()
	report.Stats = f.StatsSnapshot()
	report.ElapsedSeconds = time.Since(started).Seconds()
	if report.ForecasterCalls != int64(target) || report.Stats.RunFailure != 1 || report.Stats.RunSuccess+report.Stats.RunFailure != int64(target) {
		t.Fatalf("worker completion accounting failed: target=%d report=%+v", target, report)
	}
	stopProfile()
	v4QualificationWrite(t, output, report)
}

func testClosedRequestWithDX(cell CellID) VOACAPClosedRequest {
	req := testClosedRequest()
	req.DXCell = cell
	return req
}

// The native sample is deliberately small and separate. It proves the actual
// forecaster/worker/store integration with controlled SSNs, including a month
// boundary; it is not the fake capacity workload or a production load claim.
func TestVOACAPV4NativeWorkerQualification(t *testing.T) {
	output, home := os.Getenv("GOCLUSTER_VOACAP_V4_NATIVE_OUTPUT"), os.Getenv("GOCLUSTER_VOACAP_WORKER_HOME")
	if output == "" || home == "" {
		t.Skip("set explicit isolated native home and native output")
	}
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.ForecastHours = 24
	cfg.VOACAPFallback.CacheTTLSeconds = 86400
	cfg.VOACAPFallback.DelaySeconds = 600
	cfg.VOACAPFallback.VOACAPHome = home
	provider := &v4SolarProvider{}
	v4QualificationSolar(provider, 112)
	f := newTestClosedFallback(t, cfg, NewVOACAPRunnerClosedForecaster(cfg.VOACAPFallback), provider)
	anchor := time.Now()
	var clock atomic.Int64
	clock.Store(time.Date(2026, 1, 31, 23, 15, 0, 0, time.UTC).UnixNano())
	f.now = func() time.Time { return time.Unix(0, clock.Load()).UTC().Add(time.Since(anchor)) }
	store := openTestVOACAPForecastCacheStore(t)
	t.Cleanup(func() { closeTestVOACAPForecastCacheStore(t, store) })
	if _, err := f.AttachForecastCacheStore(store, f.now()); err != nil {
		t.Fatal(err)
	}
	startV4Fixture(t, f)
	started := time.Now()
	stopProfile := v4QualificationProfile(t, "GOCLUSTER_VOACAP_V4_NATIVE_CPU")
	report := v4WorkerQualificationReport{Schema: 1, Native: true, Input: "controlled integer SSN with unknown solar provenance; actual native forecaster", Capacity: cfg.VOACAPFallback.MaxCacheEntries, RequestedCycles: 2}
	window, status := f.CheckForecastWindowWait(testClosedRequest(), f.now(), 2*time.Second)
	if status != VOACAPForecastWindowReady || len(window.Records) != 24 {
		t.Fatalf("native month-crossing publication unavailable: status=%v rows=%d", status, len(window.Records))
	}
	v4QualificationWait(t, f, 1)
	report.PersistedSamples += v4QualificationStored(t, f, store, 112, 2)
	report.Phases = append(report.Phases, v4WorkerPhase{Name: "native-month-boundary", AtUTC: f.now(), Snapshot: f.DedicatedDiagnosticSnapshot(f.now(), 112, true)})
	clock.Store(time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	v4QualificationSolar(provider, 114)
	if got := f.CheckForecastDetailed(testClosedRequest(), f.now()); got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 112 {
		t.Fatalf("native old-input bridge unavailable: %+v", got)
	}
	clock.Add(int64(601 * time.Second))
	f.CheckForecastDetailed(testClosedRequest(), f.now())
	v4QualificationWait(t, f, 2)
	report.PersistedSamples += v4QualificationStored(t, f, store, 114, 2)
	if got, ok := f.CheckCachedForecast(testClosedRequest(), f.now()); !ok || got.SSN != 114 {
		t.Fatalf("native replacement input not published: ok=%t %+v", ok, got)
	}
	report.Phases = append(report.Phases, v4WorkerPhase{Name: "native-next-input", AtUTC: f.now(), Snapshot: f.DedicatedDiagnosticSnapshot(f.now(), 114, true)})
	report.Stats = f.StatsSnapshot()
	report.ForecasterCalls = int64(f.Snapshot().WorkerDurationCount)
	report.ElapsedSeconds = time.Since(started).Seconds()
	if report.Stats.RunSuccess != 2 || report.Stats.RunFailure != 0 {
		t.Fatalf("native worker accounting: %+v", report)
	}
	stopProfile()
	v4QualificationWrite(t, output, report)
}
