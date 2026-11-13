package pathreliability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"dxcluster/internal/voacap"
	"github.com/cockroachdb/pebble"
)

type v4SolarProvider struct {
	mu    sync.Mutex
	input voacap.SolarInputSnapshot
	known bool
}

func (p *v4SolarProvider) set(ssn int, known bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.known = known
	p.input = voacap.SolarInputSnapshot{SSN: ssn, Provenance: voacap.SolarInputProvenance{LatestNoonFluxSFU: float64(ssn), BlendedFluxSFU: float64(ssn), SourceID: voacap.SolarSourceNOAANoonF107, Flags: voacap.SolarInputBlendPresent}}
}

func (p *v4SolarProvider) CurrentSolarInput(time.Time) (voacap.SolarInputSnapshot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.input, p.known
}
func (p *v4SolarProvider) CurrentSSN(now time.Time) (int, bool) {
	input, ok := p.CurrentSolarInput(now)
	return input.SSN, ok
}

func v4FullForecast(job VOACAPClosedJob, hours int) VOACAPClosedForecast {
	out := VOACAPClosedForecast{WindowStartUTC: job.WindowStartUTC, OutputPath: "deleted/temporary/output"}
	for i := 0; i < hours; i++ {
		out.Records = append(out.Records, VOACAPHourlyForecast{HourUTC: job.WindowStartUTC.Add(time.Duration(i) * time.Hour).Hour(), FrequencyMHz: job.FrequencyMHz, FT8SNRDB: -20})
	}
	return out
}

func v4Fixture(t *testing.T, forecaster VOACAPClosedForecaster) (*VOACAPClosedFallback, *v4SolarProvider, time.Time) {
	t.Helper()
	now := time.Date(2026, 6, 8, 20, 10, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.ForecastHours = 2
	cfg.VOACAPFallback.CacheTTLSeconds = 86400
	cfg.VOACAPFallback.DelaySeconds = 0
	provider := &v4SolarProvider{}
	provider.set(112, true)
	f := newTestClosedFallback(t, cfg, forecaster, provider)
	f.now = func() time.Time { return now }
	return f, provider, now
}

func v4Entry(f *VOACAPClosedFallback, req VOACAPClosedRequest, start, stored time.Time, ssn int) voacapCacheEntry {
	job := VOACAPClosedJob{Request: req, WindowStartUTC: start, FrequencyMHz: 14.1}
	forecast := v4FullForecast(job, f.fallback.ForecastHours)
	forecast.OutputPath = ""
	return voacapCacheEntry{forecast: forecast, storedAt: stored, metadata: &voacapForecastMetadata{solar: voacap.SolarInputSnapshot{SSN: ssn}, originNS: stored.UnixNano()}, windowEndNS: start.Add(time.Duration(f.fallback.ForecastHours) * time.Hour).UnixNano(), expiresNS: stored.Add(24 * time.Hour).UnixNano(), hourMask: forecastCacheHourMask(forecast.Records), flags: voacapMetadataInitialized}
}

func startV4Fixture(t *testing.T, f *VOACAPClosedFallback) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.Start(ctx)
	t.Cleanup(func() { cancel(); f.Wait() })
}

func TestVOACAPV4DequeueCapturesCoherentLatestInput(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	jobs := make(chan VOACAPClosedJob, 2)
	f, provider, now := v4Fixture(t, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		jobs <- job
		if job.Request.DXCell == testClosedRequest().DXCell {
			close(entered)
			<-release
		}
		return v4FullForecast(job, 2), nil
	}})
	startV4Fixture(t, f)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	req := testClosedRequest()
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckQueued {
		t.Fatalf("first queue: %+v", got)
	}
	<-entered
	other := req
	other.DXCell++
	if got := f.CheckForecastDetailed(other, now); got.Status != VOACAPForecastCheckQueued {
		t.Fatalf("second queue: %+v", got)
	}
	provider.set(113, true)
	releaseOnce.Do(func() { close(release) })
	waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 2 })
	first, second := <-jobs, <-jobs
	if first.SSN != 112 || first.SolarInput.SSN != 112 || first.SolarInput.Provenance.BlendedFluxSFU != 112 || second.SSN != 113 || second.SolarInput.SSN != 113 || second.SolarInput.Provenance.BlendedFluxSFU != 113 {
		t.Fatalf("mixed/deferred snapshots: first=%+v second=%+v", first, second)
	}
	for _, request := range []VOACAPClosedRequest{req, other} {
		f.mu.Lock()
		entry := f.cache[f.cacheKey(request)]
		f.mu.Unlock()
		if entry.forecast.OutputPath != "" || len(entry.forecast.Records) != 2 || cap(entry.forecast.Records) != 2 {
			t.Fatalf("retained runtime output/backing: %+v", entry)
		}
	}
}

func TestVOACAPV4IncompleteReplacementPreservesOriginalOwner(t *testing.T) {
	for _, failure := range []string{"short", "duplicate", "wrong_band", "wrong_window", "nan_rel", "infinite_rel", "nan_solar"} {
		t.Run(failure, func(t *testing.T) {
			f, provider, now := v4Fixture(t, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
				out := v4FullForecast(job, 2)
				switch failure {
				case "short":
					out.Records = out.Records[:1]
				case "duplicate":
					out.Records[1].HourUTC = out.Records[0].HourUTC
				case "wrong_band":
					out.Records[1].FrequencyMHz = 7.1
				case "wrong_window":
					out.WindowStartUTC = out.WindowStartUTC.Add(time.Hour)
				case "nan_rel":
					out.Records[0].ReqSNRReliability = math.NaN()
				case "infinite_rel":
					out.Records[0].ReceiveReqSNRReliability = math.Inf(1)
				}
				return out, nil
			}})
			req := testClosedRequest()
			key := f.cacheKey(req)
			old := v4Entry(f, req, now.Truncate(time.Hour), now.Add(-time.Minute), 111)
			oldMetadata := *old.metadata
			f.cache[key] = old
			provider.set(112, true)
			if failure == "nan_solar" {
				provider.mu.Lock()
				provider.input.Provenance.BlendedFluxSFU = math.NaN()
				provider.mu.Unlock()
			}
			startV4Fixture(t, f)
			if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 111 {
				t.Fatalf("old bridge unavailable: %+v", got)
			}
			waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 1 })
			f.mu.Lock()
			got := f.cache[key]
			delay := f.delays[f.delayKey(req)]
			f.mu.Unlock()
			if got.metadata == nil || *got.metadata != oldMetadata || got.windowEndNS != old.windowEndNS || got.expiresNS != old.expiresNS || !got.storedAt.Equal(old.storedAt) || got.forecast.Records[0] != old.forecast.Records[0] || delay.kind != voacapDelayFailure {
				t.Fatalf("failed replacement changed original validity: got=%+v delay=%+v", got, delay)
			}
		})
	}
}

func TestVOACAPV4RenewalRespectsOrdinaryFailureBackoff(t *testing.T) {
	f, _, now := v4Fixture(t, fakeClosedForecaster{fn: func(context.Context, VOACAPClosedJob) (VOACAPClosedForecast, error) {
		return VOACAPClosedForecast{}, errors.New("expected failure")
	}})
	f.fallback.DelaySeconds = 600
	req := testClosedRequest()
	entry := v4Entry(f, req, now.Truncate(time.Hour).Add(-time.Hour), now.Add(-time.Minute), 112)
	f.cache[f.cacheKey(req)] = entry
	startV4Fixture(t, f)
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckReady {
		t.Fatalf("renewable forecast unavailable: %+v", got)
	}
	waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 1 })
	f.CheckForecastDetailed(req, now.Add(time.Second))
	if snap := f.Snapshot(); snap.QueueDepth != 0 || snap.InflightEntries != 0 || snap.WorkerDurationCount != 1 {
		t.Fatalf("renewal bypassed failure backoff: %+v", snap)
	}
	_, status := f.CheckForecastWindowWait(req, now.Add(time.Second), time.Second)
	if status != VOACAPForecastWindowFailed {
		t.Fatalf("command did not bypass retry: %v", status)
	}
}

func TestVOACAPV4BusyIncludesPersistenceAndCoalescesChangedInput(t *testing.T) {
	f, provider, now := v4Fixture(t, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		return v4FullForecast(job, 2), nil
	}})
	store := openTestVOACAPForecastCacheStore(t)
	t.Cleanup(func() { closeTestVOACAPForecastCacheStore(t, store) })
	if _, err := f.AttachForecastCacheStore(store, now); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var clock atomic.Int64
	clock.Store(now.UnixNano())
	f.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	store.faults = &voacapForecastCacheFaults{commit: func(batch *pebble.Batch, options *pebble.WriteOptions) error {
		close(entered)
		<-release
		return batch.Commit(options)
	}}
	startV4Fixture(t, f)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	req := testClosedRequest()
	window, status := f.CheckForecastWindowWait(req, now, time.Second)
	if status != VOACAPForecastWindowReady || len(window.Records) != 2 {
		t.Fatalf("publication waited for disk: %v %+v", status, window)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		f.mu.Lock()
		entry := f.cache[f.cacheKey(req)]
		f.mu.Unlock()
		record := diskRecordFromForecastCache(f.cacheKey(req), entry)
		_, reason := validateCurrentForecastCacheRecord(voacapForecastCacheKeyBytes(f.cacheKey(req)), record, now, 0, f.fallback)
		t.Fatalf("persistence not reached: index=%+v record=%+v reason=%d", store.IndexSnapshot(), record, reason)
	}
	provider.set(113, true)
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 112 {
		t.Fatalf("changed-input bridge: %+v", got)
	}
	if snap := f.Snapshot(); !snap.WorkerBusy || snap.InflightEntries != 1 || snap.QueueDepth != 0 || snap.WorkerDurationCount != 0 {
		t.Fatalf("persistence omitted from busy/pending: %+v", snap)
	}
	clock.Add(int64(2 * time.Second))
	if snap := f.DedicatedDiagnosticSnapshot(now.Add(2*time.Second), 113, true); !snap.WorkerBusy || snap.WorkerBusyFor != 2*time.Second {
		t.Fatalf("in-progress busy time omitted persistence: %+v", snap)
	}
	releaseOnce.Do(func() { close(release) })
	waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 1 })
	if snap := f.Snapshot(); snap.WorkerBusy || snap.InflightEntries != 0 || snap.WorkerDurationSum != 2*time.Second || snap.WorkerDurationMax != 2*time.Second {
		t.Fatalf("worker did not retire: %+v", snap)
	}
}

func TestVOACAPV4ActiveInputChangeKeepsFrozenResultAndRequestDrivenDelay(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	var releaseOnce sync.Once
	f, provider, now := v4Fixture(t, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return v4FullForecast(job, 2), nil
	}})
	f.fallback.DelaySeconds = 600
	startV4Fixture(t, f)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	req := testClosedRequest()
	_, status := f.CheckForecastWindowWait(req, now, 0)
	if status != VOACAPForecastWindowRefreshing {
		t.Fatalf("command did not schedule cold work: %v", status)
	}
	<-entered
	provider.set(113, true)
	if got := f.CheckForecastDetailed(req, now.Add(time.Second)); got.Status != VOACAPForecastCheckInflight {
		t.Fatalf("active request invented new queued work: %+v", got)
	}
	releaseOnce.Do(func() { close(release) })
	waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 1 })
	if snap := f.Snapshot(); snap.QueueDepth != 0 || snap.InflightEntries != 0 || calls.Load() != 1 {
		t.Fatalf("completion started an automatic follow-up: %+v calls=%d", snap, calls.Load())
	}
	if got := f.CheckForecastDetailed(req, now.Add(2*time.Second)); got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 112 || f.Snapshot().InflightEntries != 0 {
		t.Fatalf("old actual input/delay lost: %+v", got)
	}
	next := now.Add(602 * time.Second)
	f.now = func() time.Time { return next }
	f.CheckForecastDetailed(req, next)
	waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 2 })
	if got, ok := f.CheckCachedForecast(req, next); !ok || got.SSN != 113 {
		t.Fatalf("later request did not drive current replacement: ok=%t forecast=%+v", ok, got)
	}
}

func TestVOACAPV4RetiredGeometrySeedsOnlyBoundedOneTimeEligibility(t *testing.T) {
	f, _, now := v4Fixture(t, fakeClosedForecaster{fn: func(context.Context, VOACAPClosedJob) (VOACAPClosedForecast, error) {
		return VOACAPClosedForecast{}, errors.New("expected failure")
	}})
	f.fallback.DelaySeconds = 600
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	req := testClosedRequest()
	key := f.cacheKey(req)
	start := now.Truncate(time.Hour)
	legacy := voacapForecastCacheDiskRecord{SchemaVersion: 1, ModelGeneration: 1, Key: voacapForecastCacheDiskKey{UserCell: uint16(key.userCell), DXCell: uint16(key.dxCell), Band: key.band, FrequencyKHz: key.frequencyKHz, Year: start.Year(), Month: int(start.Month()), SSN: 112, Direction: "bidirectional"}, StoredAtUnixNS: now.Add(-time.Minute).UnixNano(), WindowUnixNS: start.UnixNano(), Records: []VOACAPHourlyForecast{{HourUTC: start.Hour(), FrequencyMHz: 14.1}}}
	value, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	oldKey := []byte(fmt.Sprintf("%s%04x/%04x/%s/%06d/%04d/%02d/%04d/bidirectional", voacapForecastCacheLegacyKeyPrefix, key.userCell, key.dxCell, key.band, key.frequencyKHz, start.Year(), start.Month(), 112))
	if err := store.db.Set(oldKey, value, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	stats, err := f.AttachForecastCacheStore(store, now)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Retired != 1 || stats.RebuildEligible != 1 || f.Snapshot().CacheEntries != 0 || f.Snapshot().QueueDepth != 0 {
		t.Fatalf("retirement restored/enqueued payload: stats=%+v snapshot=%+v", stats, f.Snapshot())
	}
	if _, closer, err := store.db.Get(oldKey); err == nil {
		closer.Close()
		t.Fatal("legacy payload still persisted")
	} else if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatal(err)
	}
	f.running.Store(true)
	f.queue = make(chan *voacapPendingWork, 1)
	f.queue <- &voacapPendingWork{}
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckQueueFull {
		t.Fatalf("retired marker not immediately eligible: %+v", got)
	}
	if f.delays[f.delayKey(req)].kind != voacapDelayRetired {
		t.Fatal("queue-full consumed retired marker")
	}
	<-f.queue
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckQueued {
		t.Fatalf("retired marker did not admit: %+v", got)
	}
	if _, ok := f.delays[f.delayKey(req)]; ok {
		t.Fatal("queue admission did not consume marker")
	}
	startV4Fixture(t, f)
	waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 1 })
	if got := f.CheckForecastDetailed(req, now.Add(time.Second)); got.Status != VOACAPForecastCheckDelayWait {
		t.Fatalf("retired marker survived ordinary failure: %+v", got)
	}
}

func TestVOACAPV4ExactDiagnosticInventoryAllocationsAndMetadataBound(t *testing.T) {
	f, _, now := v4Fixture(t, fakeClosedForecaster{})
	req := testClosedRequest()
	for i := 0; i < 5; i++ {
		r := req
		r.DXCell += CellID(i)
		entry := v4Entry(f, r, now.Truncate(time.Hour), now.Add(-time.Minute), 112)
		switch i {
		case 0:
			entry.metadata.solar.SSN = 113
		case 2:
			entry.expiresNS = now.Add(-time.Nanosecond).UnixNano()
		case 3:
			entry.forecast.Records = entry.forecast.Records[1:]
			entry.hourMask = forecastCacheHourMask(entry.forecast.Records)
		case 4:
			entry.windowEndNS = now.UnixNano()
		}
		f.cache[f.cacheKey(r)] = entry
	}
	snap := f.DedicatedDiagnosticSnapshot(now, 113, true)
	if snap.CacheEntries != 5 || snap.UsableCacheEntries != 2 || snap.DifferentSSNEntries != 1 || !snap.SSNComparisonKnown {
		t.Fatalf("incorrect usable denominator: %+v", snap)
	}
	unknown := f.DedicatedDiagnosticSnapshot(now, 0, false)
	if unknown.SSNComparisonKnown || unknown.UsableCacheEntries != 2 || unknown.DifferentSSNEntries != 0 {
		t.Fatalf("unavailable input claimed known mismatch: %+v", unknown)
	}
	if got := testing.AllocsPerRun(100, func() { snap = f.DedicatedDiagnosticSnapshot(now, 113, true) }); got != 0 {
		t.Fatalf("diagnostic allocated: %v", got)
	}
	if snap.UsableCacheEntries != 2 {
		t.Fatal("allocation witness lost inventory")
	}
	if got := unsafe.Sizeof(voacapForecastMetadata{}) + unsafe.Sizeof(voacapCacheEntry{}) - 96; got > 128 {
		t.Fatalf("metadata exceeds envelope: %d", got)
	}
}

func v4SeedLegacy(t *testing.T, store *VOACAPForecastCacheStore, now time.Time, cell CellID, mutate func(*voacapForecastCacheDiskRecord)) {
	t.Helper()
	start := now.Truncate(time.Hour)
	record := voacapForecastCacheDiskRecord{SchemaVersion: 1, ModelGeneration: 1, Key: voacapForecastCacheDiskKey{UserCell: 1, DXCell: uint16(cell), Band: "20m", FrequencyKHz: 14100, Year: start.Year(), Month: int(start.Month()), SSN: 112, Direction: voacapDirectionBidirectional}, StoredAtUnixNS: now.Add(-time.Minute).UnixNano(), WindowUnixNS: start.UnixNano(), Records: []VOACAPHourlyForecast{{HourUTC: start.Hour(), FrequencyMHz: 14.1}}}
	if mutate != nil {
		mutate(&record)
	}
	value, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte(fmt.Sprintf("%s0001/%04x/20m/014100/%04d/%02d/0112/bidirectional", voacapForecastCacheLegacyKeyPrefix, cell, start.Year(), start.Month()))
	if err := store.db.Set(key, value, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
}

func v4DiskPrefixCount(t *testing.T, store *VOACAPForecastCacheStore, prefix string) int {
	t.Helper()
	iter, err := store.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: append([]byte(prefix), 0xff)})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestVOACAPV4RetirementRejectsInvalidAndExpiredIdentities(t *testing.T) {
	f, _, now := v4Fixture(t, fakeClosedForecaster{})
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	v4SeedLegacy(t, store, now, 2, nil)
	v4SeedLegacy(t, store, now, 3, func(r *voacapForecastCacheDiskRecord) { r.StoredAtUnixNS = now.Add(-9 * time.Hour).UnixNano() })
	v4SeedLegacy(t, store, now, 4, func(r *voacapForecastCacheDiskRecord) { r.ModelGeneration = 99 })
	v4SeedLegacy(t, store, now, 5, func(r *voacapForecastCacheDiskRecord) { r.Records = append(r.Records, r.Records[0]) })
	v4SeedLegacy(t, store, now, 6, func(r *voacapForecastCacheDiskRecord) { r.Key.SSN = 999 })
	stats, err := f.AttachForecastCacheStore(store, now)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Retired != 5 || stats.RebuildEligible != 1 || f.Snapshot().DelayEntries != 1 || f.Snapshot().CacheEntries != 0 || v4DiskPrefixCount(t, store, voacapForecastCacheLegacyKeyPrefix) != 0 {
		t.Fatalf("unrecognized identity gained eligibility: stats=%+v snapshot=%+v", stats, f.Snapshot())
	}
}

func TestVOACAPV4RetirementUncertainCommitRecoveryAndBounds(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%t", applied), func(t *testing.T) {
			f, _, now := v4Fixture(t, fakeClosedForecaster{})
			f.fallback.MaxDelayEntries = 3
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			for i := 1; i <= 300; i++ {
				v4SeedLegacy(t, store, now, CellID(i), nil)
			}
			calls := 0
			store.faults = &voacapForecastCacheFaults{commit: func(batch *pebble.Batch, options *pebble.WriteOptions) error {
				calls++
				if applied && calls == 1 {
					if err := batch.Commit(options); err != nil {
						return err
					}
				}
				return errors.New("injected uncertain retirement")
			}}
			if _, err := f.AttachForecastCacheStore(store, now); err == nil {
				t.Fatal("retirement fault was not observed")
			}
			want := 300
			if applied {
				want -= voacapForecastCachePruneChunkSize
			}
			if got := v4DiskPrefixCount(t, store, voacapForecastCacheLegacyKeyPrefix); got != want {
				t.Fatalf("raw retired prefix count=%d want=%d", got, want)
			}
			if snap := store.IndexSnapshot(); snap.Ready || !snap.Dirty {
				t.Fatalf("uncertain retirement certified indexes: %+v", snap)
			}
			req := testClosedRequest()
			entry := v4Entry(f, req, now.Truncate(time.Hour), now, 112)
			if err := store.storeForecast(f.cacheKey(req), entry, f.fallback, now); err == nil {
				t.Fatal("write admitted while retirement remained uncertain")
			}
			if got := v4DiskPrefixCount(t, store, voacapForecastCacheEntryKeyPrefix); got != 0 {
				t.Fatalf("new owners bypassed old-owner retirement: %d", got)
			}
			store.faults = nil
			if _, err := f.AttachForecastCacheStore(store, now); err != nil {
				t.Fatal(err)
			}
			if got := v4DiskPrefixCount(t, store, voacapForecastCacheLegacyKeyPrefix); got != 0 {
				t.Fatalf("recovery left old payloads: %d", got)
			}
			if snap := f.Snapshot(); snap.DelayEntries != 3 || snap.QueueDepth != 0 || snap.CacheEntries != 0 {
				t.Fatalf("retirement exceeded marker/queue bounds: %+v", snap)
			}
			if err := store.storeForecast(f.cacheKey(req), entry, f.fallback, now); err != nil {
				t.Fatal(err)
			}
			if got := v4DiskPrefixCount(t, store, voacapForecastCacheEntryKeyPrefix); got != 1 {
				t.Fatalf("recovery failed to admit new owner: %d", got)
			}
		})
	}
}

func TestVOACAPV4CommandWaitUsesCompletionHour(t *testing.T) {
	f, _, now := v4Fixture(t, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		return v4FullForecast(job, 2), nil
	}})
	requested := now.Truncate(time.Hour).Add(time.Hour - time.Second)
	completed := requested.Add(2 * time.Second)
	f.now = func() time.Time { return completed }
	startV4Fixture(t, f)
	window, status := f.CheckForecastWindowWait(testClosedRequest(), requested, time.Second)
	if status != VOACAPForecastWindowReady || len(window.Records) != 2 || window.Records[0].Record.HourUTC != completed.Hour() {
		t.Fatalf("successful rollover publication reported failure/old hour: status=%v window=%+v", status, window)
	}
}

func TestVOACAPV4ShutdownClosesAdmissionAndPendingOwners(t *testing.T) {
	f, _, now := v4Fixture(t, fakeClosedForecaster{fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		<-ctx.Done()
		return VOACAPClosedForecast{}, ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	f.Start(ctx)
	req := testClosedRequest()
	f.CheckForecastDetailed(req, now)
	cancel()
	var requests sync.WaitGroup
	requests.Add(1)
	go func() {
		defer requests.Done()
		for i := 0; i < 1000; i++ {
			f.CheckForecastDetailed(req, now)
		}
	}()
	f.Wait()
	requests.Wait()
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckNotRunning {
		t.Fatalf("retired worker admitted work: %+v", got)
	}
	if snap := f.Snapshot(); snap.InflightEntries != 0 || snap.QueueDepth != 0 || snap.WorkerBusy {
		t.Fatalf("shutdown stranded pending owners: %+v", snap)
	}
}

func TestVOACAPV4KeyRepresentation(t *testing.T) {
	if got := unsafe.Sizeof(voacapCacheKey{}); got != 32 {
		t.Fatalf("cache key grew beyond its compact physical layout: %d", got)
	}
	if got := unsafe.Sizeof(voacapDelayKey{}); got != 32 {
		t.Fatalf("delay key grew beyond its compact physical layout: %d", got)
	}
	if got := unsafe.Sizeof(voacapDelayEntry{}); got != 56 {
		t.Fatalf("delay value no longer preserves its full timestamps and state: %d", got)
	}
	f := &VOACAPClosedFallback{}
	key := voacapCacheKey{userCell: 1, dxCell: 2, band: "20m", frequencyKHz: 14100, direction: voacapDirectionBidirectionalID}
	delay := f.delayKeyForCacheKey(key)
	if delay.userCell != key.userCell || delay.dxCell != key.dxCell || delay.band != key.band || delay.frequencyKHz != key.frequencyKHz || delay.direction != voacapDirectionBidirectionalID {
		t.Fatalf("canonical delay identity changed: %+v", delay)
	}
	if got := string(voacapForecastCacheKeyBytes(key)); got != "voacapfc/v2/e/0001/0002/20m/014100/bidirectional" {
		t.Fatalf("compact identity changed persisted key: %q", got)
	}
	diskKey := diskKeyFromForecastCacheKey(key)
	if diskKey.Direction != "bidirectional" {
		t.Fatalf("compact identity changed wire direction: %q", diskKey.Direction)
	}
	if restored, ok := diskKey.cacheKey(); !ok || restored != key {
		t.Fatalf("wire identity did not round trip: %+v, %t", restored, ok)
	}
	for _, direction := range []voacapDirectionID{0, 2, 255} {
		invalid := key
		invalid.direction = direction
		for _, boundary := range []struct {
			name string
			run  func()
		}{
			{"delay", func() { f.delayKeyForCacheKey(invalid) }},
			{"disk-record", func() { diskKeyFromForecastCacheKey(invalid) }},
			{"disk-key", func() { voacapForecastCacheKeyBytes(invalid) }},
		} {
			t.Run(fmt.Sprintf("%d/%s", direction, boundary.name), func(t *testing.T) {
				defer func() {
					if recover() == nil {
						t.Fatal("noncanonical internal direction silently aliased a canonical path")
					}
				}()
				boundary.run()
			})
		}
	}
	t.Logf("physical cache/delay keys=%d/%d bytes; unchanged delay value=%d bytes", unsafe.Sizeof(voacapCacheKey{}), unsafe.Sizeof(voacapDelayKey{}), unsafe.Sizeof(voacapDelayEntry{}))
}

func TestVOACAPV4WarmBookkeepingAllocationsAtCapacity(t *testing.T) {
	f, _, now := v4Fixture(t, fakeClosedForecaster{})
	f.fallback.ForecastHours = 24
	f.fallback.MaxCacheEntries = 50000
	f.fallback.MaxDelayEntries = 50000
	req := testClosedRequest()
	for i := 1; i <= 50000; i++ {
		r := req
		r.DXCell = CellID(i)
		entry := v4Entry(f, r, now.Truncate(time.Hour), now, 112)
		rows := make([]VOACAPHourlyForecast, len(entry.forecast.Records))
		copy(rows, entry.forecast.Records)
		entry.forecast.Records = rows
		f.addCacheLocked(f.cacheKey(r), entry)
		f.addDelayLocked(f.delayKey(r), voacapDelayEntry{firstSeen: now, lastSeen: now})
	}
	f.lastPrune = now
	entry := f.cache[f.cacheKey(req)]
	if got := testing.AllocsPerRun(100, func() { f.addCacheLocked(f.cacheKey(req), entry); f.delayElapsedLocked(f.delayKey(req), now) }); got != 0 {
		t.Fatalf("full-capacity warm refresh/touch allocated: %v", got)
	}
	if got := testing.AllocsPerRun(100, func() {
		if check := f.CheckForecastDetailed(req, now); check.Status != VOACAPForecastCheckReady {
			t.Fatal("cache-hit witness failed")
		}
	}); got != 0 {
		t.Fatalf("full-capacity cache hit allocated: %v", got)
	}
	next := CellID(50000)
	insertAllocs := testing.AllocsPerRun(20, func() { next++; key := f.cacheKey(req); key.dxCell = next; f.addCacheLocked(key, entry) })
	if insertAllocs != 0 {
		t.Fatalf("full-capacity insertion bookkeeping allocated: %v", insertAllocs)
	}
	t.Logf("50000-owner capacity insertion bookkeeping allocations/op=%g; map element bytes=%d", insertAllocs, unsafe.Sizeof(voacapCacheEntry{}))
}

func TestVOACAPV4PersistedBandDoesNotRetainPaddedInput(t *testing.T) {
	band := strings.Repeat(" ", 60000) + "20m"
	key, ok := (voacapForecastCacheDiskKey{UserCell: 1, DXCell: 2, Band: band, FrequencyKHz: 14100, Direction: voacapDirectionBidirectional}).cacheKey()
	if !ok || key.band != "20m" {
		t.Fatalf("canonical band admission failed: %+v %t", key, ok)
	}
	start := uintptr(unsafe.Pointer(unsafe.StringData(band)))
	retained := uintptr(unsafe.Pointer(unsafe.StringData(key.band)))
	if retained >= start && retained < start+uintptr(len(band)) {
		t.Fatal("short band retains padded serialized backing")
	}
}

func TestVOACAPV4ClockRollbackRejectsReplacementBeforePublication(t *testing.T) {
	var clock atomic.Int64
	f, _, now := v4Fixture(t, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		clock.Add(-int64(time.Second))
		return v4FullForecast(job, 2), nil
	}})
	clock.Store(now.UnixNano())
	f.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	req := testClosedRequest()
	key := f.cacheKey(req)
	old := v4Entry(f, req, now.Truncate(time.Hour), now.Add(-time.Minute), 111)
	oldMetadata := *old.metadata
	f.cache[key] = old
	startV4Fixture(t, f)
	f.CheckForecastDetailed(req, now)
	waitUntil(t, func() bool { return f.Snapshot().WorkerDurationCount == 1 })
	f.mu.Lock()
	got := f.cache[key]
	f.mu.Unlock()
	if got.metadata == nil || *got.metadata != oldMetadata || got.windowEndNS != old.windowEndNS || got.expiresNS != old.expiresNS || !got.storedAt.Equal(old.storedAt) {
		t.Fatalf("rollback published a disk-inadmissible owner: %+v", got)
	}
}

var v4MetadataAllocationWitness *voacapForecastMetadata

func TestVOACAPV4MetadataPhysicalEnvelope(t *testing.T) {
	const objects = 10000
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < objects; i++ {
		v4MetadataAllocationWitness = new(voacapForecastMetadata)
	}
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(v4MetadataAllocationWitness)
	v4MetadataAllocationWitness = nil
	physical := (after.TotalAlloc - before.TotalAlloc) / objects
	entryBytes := unsafe.Sizeof(voacapCacheEntry{})
	additional := physical + uint64(entryBytes) - 96
	t.Logf("entry=%d metadata_object_nominal=%d metadata_object_allocated=%d additional_nominal=%d additional_allocated=%d", entryBytes, unsafe.Sizeof(voacapForecastMetadata{}), physical, entryBytes+unsafe.Sizeof(voacapForecastMetadata{})-96, additional)
	if entryBytes > 128 || additional > 128 {
		t.Fatalf("owned metadata exceeds approved physical envelope: entry=%d additional=%d", entryBytes, additional)
	}
}
