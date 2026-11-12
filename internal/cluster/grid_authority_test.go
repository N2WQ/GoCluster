package cluster

import (
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/gridstore"
)

type gridAuthorityFixture struct {
	store              *gridstore.Store
	cache              *callMetaCache
	handle             *gridStoreHandle
	coord              *gridCoordinator
	metrics            *gridMetrics
	update             func(string, string)
	lookup, syncLookup func(string) (string, bool, bool)
	stop               func()
	closed             bool
}

func newGridAuthorityFixture(t *testing.T, cache *callMetaCache, interval time.Duration, ops gridWriterOps, ctyLookup func() *cty.CTYDatabase) *gridAuthorityFixture {
	t.Helper()
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	f := &gridAuthorityFixture{store: store, cache: cache, handle: newGridStoreHandle(store)}
	f.update, _, f.metrics, f.stop, f.lookup, f.syncLookup = startGridWriterWithOps(f.handle, interval, cache, 0, true, ctyLookup, ops)
	f.coord = f.handle.authority
	t.Cleanup(func() {
		f.stop()
		if !f.closed {
			_ = store.Close()
		}
	})
	return f
}

func authorityRecord(grid string, derived bool, at time.Time) gridstore.Record {
	return gridstore.Record{Call: "K1ABC", Grid: sql.NullString{String: grid, Valid: grid != ""}, GridDerived: derived, FirstSeen: at, UpdatedAt: at, Observations: 1}
}

func authorityReply(t *testing.T, resp <-chan gridLookupResult) gridLookupResult {
	t.Helper()
	select {
	case got := <-resp:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("actual worker reply missing")
		return gridLookupResult{}
	}
}

func authorityRequest(t *testing.T, requests <-chan gridLookupRequest) gridLookupRequest {
	t.Helper()
	select {
	case req := <-requests:
		return req
	case <-time.After(5 * time.Second):
		t.Fatal("actual worker read witness missing")
		return gridLookupRequest{}
	}
}

// The public timeout is expected, never the positive oracle. We inspect the
// actual worker's one buffered response after releasing its controlled read.
func authorityTimedCaller(t *testing.T, lookup func(string) (string, bool, bool), requests <-chan gridLookupRequest) gridLookupRequest {
	t.Helper()
	caller := make(chan gridLookupResult, 1)
	go func() { grid, derived, ok := lookup("K1ABC"); caller <- gridLookupResult{grid, derived, ok} }()
	req := authorityRequest(t, requests)
	if got := authorityReply(t, caller); got.ok || got.grid != "" {
		t.Fatalf("held public caller did not time out: %+v", got)
	}
	if req.resp == nil || cap(req.resp) != 1 {
		t.Fatal("did not capture actual buffered synchronous response")
	}
	return req
}

func assertAuthorityDrained(t *testing.T, f *gridAuthorityFixture) {
	t.Helper()
	f.stop()
	f.coord.mu.Lock()
	defer f.coord.mu.Unlock()
	if f.coord.entries != nil || f.coord.keyBytes != 0 || f.coord.pinBytes != 0 || f.coord.pending != 0 || f.metrics.activeKeys.Load() != 0 || f.metrics.copiedTextBytes.Load() != 0 || f.metrics.pendingReports.Load() != 0 {
		t.Fatalf("authority retained owners/backing: entries=%d keys=%d pins=%d writes=%d", len(f.coord.entries), f.coord.keyBytes, f.coord.pinBytes, f.coord.pending)
	}
}

func TestGridAuthorityWorkerReply(t *testing.T) {
	for _, tc := range []struct {
		name, old, report string
		derived, clear    bool
	}{
		{"derived_to_reported", "FN20", "FN31AB", true, false},
		{"reported_to_newer_reported", "FN20AA", "FN31AB", false, false},
		{"equal_text_provenance", "FN20", "FN20", true, false},
		{"clear_yields_miss", "FN20", "FN31AB", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests, release := make(chan gridLookupRequest, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Hour, gridWriterOps{afterRead: func(req gridLookupRequest, _ *gridstore.Record) { requests <- req; <-release }}, nil)
			t.Cleanup(unblock)
			if err := f.store.Upsert(authorityRecord(tc.old, tc.derived, time.Now().Add(-time.Minute))); err != nil {
				t.Fatal(err)
			}
			req := authorityTimedCaller(t, f.syncLookup, requests)
			f.update("K1ABC", tc.report)
			if tc.clear {
				f.cache.Clear()
			}
			unblock()
			got := authorityReply(t, req.resp)
			want := gridLookupResult{grid: tc.report, ok: true}
			if tc.clear {
				want = gridLookupResult{}
			}
			if got != want {
				t.Fatalf("actual worker reply=%+v want %+v", got, want)
			}
			if total := f.metrics.supersededCandidates.Load(); total != 1 {
				t.Fatalf("superseded completion counted %d times", total)
			}
			assertAuthorityDrained(t, f)
			if grid, derived, ok := f.cache.LookupGrid("K1ABC", nil); !ok || derived || grid != tc.report {
				t.Fatalf("cache differs from committed report: %s/%v/%v", grid, derived, ok)
			}
		})
	}
}

func TestGridAuthorityStoredReported(t *testing.T) {
	for _, mode := range []string{"nil_cache", "expired_cache", "public_sync"} {
		t.Run(mode, func(t *testing.T) {
			var cache *callMetaCache
			if mode != "nil_cache" {
				cache = newCallMetaCache(64, time.Nanosecond)
			}
			f := newGridAuthorityFixture(t, cache, time.Hour, gridWriterOps{}, nil)
			if err := f.store.Upsert(authorityRecord("FN31AB", false, time.Now().Add(-time.Hour))); err != nil {
				t.Fatal(err)
			}
			grid, derived, ok := f.syncLookup("K1ABC")
			if grid != "FN31AB" || derived || !ok {
				t.Fatalf("actual successful synchronous row lost: %s/%v/%v", grid, derived, ok)
			}
			if cache != nil {
				if _, _, hit := cache.LookupGrid("K1ABC", nil); hit {
					t.Fatal("disk hydration reset persisted age")
				}
			}
			assertAuthorityDrained(t, f)
		})
	}
}

func TestGridAuthorityPins(t *testing.T) {
	for _, mode := range []string{"positive_positive_nil", "positive_positive_expired", "miss_after_purge", "derived_after_purge", "clear_invalidates"} {
		t.Run(mode, func(t *testing.T) {
			requests := make(chan gridLookupRequest, 2)
			releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
			var read atomic.Int32
			var once sync.Once
			unblock := func() {
				once.Do(func() {
					for _, ch := range releases {
						select {
						case <-ch:
						default:
							close(ch)
						}
					}
				})
			}
			var cache *callMetaCache
			if mode == "positive_positive_expired" {
				cache = newCallMetaCache(64, time.Nanosecond)
			}
			var db *cty.CTYDatabase
			if mode == "derived_after_purge" {
				db = gridRetirementCTY(t)
			}
			f := newGridAuthorityFixture(t, cache, time.Hour, gridWriterOps{afterRead: func(req gridLookupRequest, _ *gridstore.Record) { i := read.Add(1) - 1; requests <- req; <-releases[i] }}, func() *cty.CTYDatabase { return db })
			t.Cleanup(unblock)
			at := time.Now().Add(-time.Hour)
			if err := f.store.Upsert(authorityRecord("EM12AA", false, at)); err != nil {
				t.Fatal(err)
			}
			first := authorityTimedCaller(t, f.syncLookup, requests)
			if strings.HasPrefix(mode, "positive_positive") {
				if err := f.store.Upsert(authorityRecord("FN31AB", false, at.Add(time.Minute))); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.store.PurgeOlderThan(time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			second := authorityTimedCaller(t, f.syncLookup, requests)
			close(releases[1])
			secondResult := authorityReply(t, second.resp)
			if strings.HasPrefix(mode, "positive_positive") {
				if secondResult != (gridLookupResult{grid: "FN31AB", ok: true}) {
					t.Fatalf("new reported read=%+v", secondResult)
				}
				f.coord.mu.Lock()
				pinBytes := f.coord.pinBytes
				f.coord.mu.Unlock()
				if pinBytes != 6 {
					t.Fatalf("reported pin not retained for other active reader: %d", pinBytes)
				}
			} else if mode == "derived_after_purge" {
				if secondResult != (gridLookupResult{grid: "FN20", derived: true, ok: true}) {
					t.Fatalf("derived read borrowed old reported pin: %+v", secondResult)
				}
			} else if secondResult.ok {
				t.Fatalf("missing read borrowed a reported pin: %+v", secondResult)
			}
			if mode == "clear_invalidates" {
				f.coord.invalidateCache()
			}
			close(releases[0])
			firstResult := authorityReply(t, first.resp)
			if strings.HasPrefix(mode, "positive_positive") {
				if firstResult != (gridLookupResult{grid: "FN31AB", ok: true}) {
					t.Fatalf("older actual reported read defeated stored pin: %+v", firstResult)
				}
				if total := f.metrics.supersededCandidates.Load(); total != 1 {
					t.Fatalf("pin replacement counted %d times, want once", total)
				}
			} else if mode == "clear_invalidates" {
				if firstResult.ok {
					t.Fatalf("old positive published across Clear: %+v", firstResult)
				}
			} else if firstResult != (gridLookupResult{grid: "EM12AA", ok: true}) {
				t.Fatalf("independently positive read lost its own tuple: %+v", firstResult)
			}
			// Prevent cleanup from closing the individually released gates again.
			once.Do(func() {})
			assertAuthorityDrained(t, f)
		})
	}
}

func TestGridAuthorityIsolation(t *testing.T) {
	requests, release := make(chan gridLookupRequest, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Hour, gridWriterOps{afterRead: func(req gridLookupRequest, _ *gridstore.Record) { requests <- req; <-release }}, nil)
	t.Cleanup(unblock)
	if err := f.store.Upsert(authorityRecord("FN31AB", false, time.Now())); err != nil {
		t.Fatal(err)
	}
	req := authorityTimedCaller(t, f.syncLookup, requests)
	f.update("W6XYZ", "DM04AA")
	unblock()
	if got := authorityReply(t, req.resp); got != (gridLookupResult{grid: "FN31AB", ok: true}) {
		t.Fatalf("unrelated call invalidated positive read: %+v", got)
	}
	assertAuthorityDrained(t, f)
}

// This exercises the actual lookup, update and final writer drain. The callback
// is reached only after the disk miss, so the report cannot influence that read.
func TestGridAuthorityLateDerivedCache(t *testing.T) {
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(64, 0)
	db := gridRetirementCTY(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	update, _, _, stop, lookup, _ := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, func() *cty.CTYDatabase {
		enterOnce.Do(func() { close(entered) })
		<-release
		return db
	})
	t.Cleanup(func() { unblock(); stop(); _ = store.Close() })
	lookup("K1ABC")
	waitGridRetirement(t, entered)
	update("K1ABC", "FN31AB")
	cache.Clear()
	unblock()
	stop()
	rec, err := store.Get("K1ABC")
	if err != nil || rec == nil || rec.Grid.String != "FN31AB" || rec.GridDerived {
		t.Fatalf("reported disk tuple = %+v / %v", rec, err)
	}
	if grid, derived, ok := cache.LookupGrid("K1ABC", nil); grid != "FN31AB" || derived || !ok {
		t.Fatalf("cache masked committed reported row: %q/%v/%v; want FN31AB/reported/true", grid, derived, ok)
	}
}
