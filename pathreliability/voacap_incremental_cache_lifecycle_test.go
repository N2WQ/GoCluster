package pathreliability

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestVOACAPIncrementalCacheMissingSSNThenCompletion(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig()
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 1), incrementalCacheEntry(now, now.Add(-2*time.Hour), 20), nil)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 2), incrementalCacheEntry(now, now, 20), nil)
	provider := &incrementalSSNProvider{}
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, provider)
	stats, err := fallback.AttachForecastCacheStore(store, now)
	if err != nil || !stats.SSNUnavailable || fallback.Snapshot().CacheEntries != 0 {
		t.Fatalf("SSN-unavailable attach: stats=%+v snapshot=%+v error=%v", stats, fallback.Snapshot(), err)
	}
	if snap := store.IndexSnapshot(); !snap.Attached || snap.Ready || snap.Entries != 0 {
		t.Fatalf("unavailable SSN cannot publish trusted metadata: %+v", snap)
	}
	assertIncrementalRawCells(t, store, []uint16{1, 2})
	provider.ssn.Store(112)
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 3), incrementalCacheEntry(now, now, 20), cfg.VOACAPFallback, now); err != nil {
		t.Fatalf("completion after SSN becomes available: %v", err)
	}
	assertIncrementalRawCells(t, store, []uint16{2, 3})
	assertIncrementalIndexReady(t, store, 2)
	if _, err := fallback.AttachForecastCacheStore(store, now); err != nil {
		t.Fatalf("known-SSN reattach: %v", err)
	}
	provider.ssn.Store(0)
	stats, err = fallback.AttachForecastCacheStore(store, now)
	if err != nil || !stats.SSNUnavailable {
		t.Fatalf("unknown-SSN reattach: stats=%+v error=%v", stats, err)
	}
	if snap := store.IndexSnapshot(); !snap.Attached || snap.Ready || snap.Dirty || snap.Entries != 0 || snap.RetentionEntries != 0 || snap.EligibilityEntries != 0 {
		t.Fatalf("skipped rehydration retained previously trusted ownership: %+v", snap)
	}
	assertIncrementalRawCells(t, store, []uint16{2, 3})
	provider.ssn.Store(112)
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 4), incrementalCacheEntry(now, now, 20), cfg.VOACAPFallback, now); err != nil {
		t.Fatalf("completion after skipped rehydration: %v", err)
	}
	assertIncrementalRawCells(t, store, []uint16{2, 3, 4})
	assertIncrementalIndexReady(t, store, 3)
}

func TestVOACAPIncrementalCacheFailedHydrationRecovery(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig()
	for _, failure := range []string{"iterator", "partial_scan"} {
		t.Run(failure, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			for cell := CellID(1); cell <= 3; cell++ {
				writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, cell), incrementalCacheEntry(now, now, 20), nil)
			}
			injected := errors.New("injected hydration failure")
			faults := &voacapForecastCacheFaults{}
			if failure == "iterator" {
				faults.newIter = func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error) { return nil, injected }
			} else {
				faults.scanStep = func(processed int) error {
					if processed == 2 {
						return injected
					}
					return nil
				}
			}
			incrementalSetFaults(store, faults)
			fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
			if _, err := fallback.AttachForecastCacheStore(store, now); !errors.Is(err, injected) {
				t.Fatalf("failed hydration error=%v, want injected failure", err)
			}
			if fallback.Snapshot().CacheEntries != 0 || store.IndexSnapshot().Ready {
				t.Fatalf("partial hydration published trusted state: fallback=%+v index=%+v", fallback.Snapshot(), store.IndexSnapshot())
			}
			assertIncrementalRawCells(t, store, []uint16{1, 2, 3})
			incrementalSetFaults(store, nil)
			if err := store.storeForecast(testVOACAPForecastCacheKey(now, 4), incrementalCacheEntry(now, now, 20), cfg.VOACAPFallback, now); err != nil {
				t.Fatalf("completion repair after hydration failure: %v", err)
			}
			assertIncrementalRawCells(t, store, []uint16{1, 2, 3, 4})
			assertIncrementalIndexReady(t, store, 4)
		})
	}
}

func TestVOACAPIncrementalCacheOlderCompletedJobSSNAuthoritative(t *testing.T) {
	// A completed job validates persistence with the generation it actually ran,
	// even if the provider advances while that job is in flight.
	now := time.Now().UTC()
	cfg := testVOACAPFallbackConfig()
	provider := &incrementalSSNProvider{}
	provider.ssn.Store(113)
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	newerKey := testVOACAPForecastCacheKey(now, 2)
	newerKey.ssn = 113
	if err := store.storeForecast(newerKey, incrementalCacheEntry(now.Truncate(time.Hour), now, now.Hour()), cfg.VOACAPFallback, now); err != nil {
		t.Fatal(err)
	}
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		if job.SSN != 112 {
			return VOACAPClosedForecast{}, fmt.Errorf("job generation=%d, want 112", job.SSN)
		}
		return incrementalCacheEntry(job.WindowStartUTC, now, job.WindowStartUTC.Hour(), job.WindowStartUTC.Add(time.Hour).Hour()).forecast, nil
	}}, provider)
	if _, err := fallback.AttachForecastCacheStore(store, now); err != nil {
		t.Fatal(err)
	}
	oldKey := testVOACAPForecastCacheKey(now, 1)
	fallback.runJob(context.Background(), VOACAPClosedJob{SSN: 112, WindowStartUTC: now.Truncate(time.Hour), Request: testClosedRequest(), key: oldKey})
	records := assertIncrementalRawCells(t, store, []uint16{1})
	for _, record := range records {
		if record.Key.SSN != 112 {
			t.Fatalf("completed job resampled provider SSN: %+v", record.Key)
		}
	}
	assertIncrementalIndexReady(t, store, 1)
}

func TestVOACAPIncrementalCacheSnapshotWaitAndCloseFence(t *testing.T) {
	now := time.Now().UTC()
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 0
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{fn: func(_ context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		return incrementalCacheEntry(job.WindowStartUTC, now, job.WindowStartUTC.Hour(), job.WindowStartUTC.Add(time.Hour).Hour()).forecast, nil
	}}, fixedSSNProvider{ssn: 112})
	if _, err := fallback.AttachForecastCacheStore(store, now); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	incrementalSetFaults(store, &voacapForecastCacheFaults{commit: func(batch *pebble.Batch, opts *pebble.WriteOptions) error {
		close(entered)
		<-release
		return batch.Commit(opts)
	}})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() { unblock(); cancel(); fallback.Wait() }()
	_, status := fallback.CheckForecastWindowWait(testClosedRequest(), now, time.Second)
	if status != VOACAPForecastWindowReady {
		t.Fatalf("memory publication/notification blocked behind disk: status=%v", status)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("persistence commit never reached blocking seam")
	}
	snapshotDone := make(chan VOACAPClosedFallbackSnapshot, 1)
	go func() { snapshotDone <- fallback.Snapshot() }()
	select {
	case snap := <-snapshotDone:
		if snap.CacheEntries != 1 || !snap.ForecastCacheIndex.Attached {
			t.Fatalf("forecast visible before disk completion: %+v", snap)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback snapshot blocked behind disk commit")
	}
	cancel()
	waitEntered, waitDone := make(chan struct{}), make(chan struct{})
	go func() { close(waitEntered); fallback.Wait(); close(waitDone) }()
	<-waitEntered
	select {
	case <-waitDone:
		t.Fatal("Wait returned before persistence completion")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("Wait did not fence released persistence")
	}
	assertIncrementalRawCells(t, store, []uint16{uint16(testClosedRequest().DXCell)})
	assertIncrementalIndexReady(t, store, 1)
	if err := fallback.CloseForecastCacheStore(); err != nil {
		t.Fatalf("close after Wait: %v", err)
	}
	if snap := fallback.Snapshot(); snap.ForecastCacheIndex.Attached {
		t.Fatalf("closed store remains attached: %+v", snap)
	}
}

type incrementalSSNProvider struct{ ssn atomic.Int32 }

func (p *incrementalSSNProvider) CurrentSSN(time.Time) (int, bool) {
	ssn := int(p.ssn.Load())
	return ssn, ssn > 0
}
