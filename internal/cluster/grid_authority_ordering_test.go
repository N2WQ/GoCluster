package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/gridstore"
)

func TestGridAuthorityRawFallbackAfterDerived(t *testing.T) {
	requests := make(chan gridLookupRequest, 2)
	firstGate, secondGate := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	reads := 0
	ops := gridWriterOps{afterRead: func(req gridLookupRequest, _ *gridstore.Record) {
		mu.Lock()
		reads++
		n := reads
		mu.Unlock()
		requests <- req
		if n == 1 {
			<-firstGate
		} else {
			<-secondGate
		}
	}}
	db := gridRetirementCTY(t)
	f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Hour, ops, func() *cty.CTYDatabase { return db })
	t.Cleanup(func() {
		select {
		case <-firstGate:
		default:
			close(firstGate)
		}
		select {
		case <-secondGate:
		default:
			close(secondGate)
		}
	})
	rec := authorityRecord("EM12AB", false, time.Now())
	rec.Call = "K1ABC-1"
	if err := f.store.Upsert(rec); err != nil {
		t.Fatal(err)
	}
	first := authorityTimedCaller(t, f.syncLookup, requests)
	caller := make(chan gridLookupResult, 1)
	go func() { grid, derived, ok := f.syncLookup("K1ABC-1"); caller <- gridLookupResult{grid, derived, ok} }()
	second := authorityRequest(t, requests)
	if got := authorityReply(t, caller); got.ok {
		t.Fatal("held raw caller did not time out")
	}
	close(firstGate)
	if got := authorityReply(t, first.resp); got != (gridLookupResult{grid: "FN20", derived: true, ok: true}) {
		t.Fatalf("base derived=%+v", got)
	}
	close(secondGate)
	if got := authorityReply(t, second.resp); got != (gridLookupResult{grid: "EM12AB", ok: true}) {
		t.Fatalf("independent reported raw row rejected: %+v", got)
	}
	assertAuthorityDrained(t, f)
	if grid, derived, ok := f.cache.LookupGrid("K1ABC", nil); grid != "EM12AB" || derived || !ok {
		t.Fatalf("derived cache masked raw report: %s/%v/%v", grid, derived, ok)
	}
}

func TestGridAuthorityReportedHydrationSurvivesCacheLoss(t *testing.T) {
	for _, mode := range []string{"clear", "lru", "ttl"} {
		t.Run(mode, func(t *testing.T) {
			requests := make(chan gridLookupRequest, 2)
			gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
			var mu sync.Mutex
			reads := 0
			ttl := time.Duration(0)
			if mode == "ttl" {
				ttl = time.Nanosecond
			}
			db := gridRetirementCTY(t)
			f := newGridAuthorityFixture(t, newCallMetaCache(16, ttl), time.Hour, gridWriterOps{afterRead: func(req gridLookupRequest, _ *gridstore.Record) {
				mu.Lock()
				i := reads
				reads++
				mu.Unlock()
				requests <- req
				<-gates[i]
			}}, func() *cty.CTYDatabase { return db })
			t.Cleanup(func() {
				for _, gate := range gates {
					select {
					case <-gate:
					default:
						close(gate)
					}
				}
			})
			rec := authorityRecord("EM12AB", false, time.Now())
			rec.Call = "K1ABC-1"
			if err := f.store.Upsert(rec); err != nil {
				t.Fatal(err)
			}
			derived := authorityTimedCaller(t, f.syncLookup, requests)
			caller := make(chan gridLookupResult, 1)
			go func() { a, b, c := f.syncLookup("K1ABC-1"); caller <- gridLookupResult{a, b, c} }()
			reported := authorityRequest(t, requests)
			if got := authorityReply(t, caller); got.ok {
				t.Fatal("held caller did not time out")
			}
			close(gates[1])
			if got := authorityReply(t, reported.resp); got != (gridLookupResult{grid: "EM12AB", ok: true}) {
				t.Fatalf("reported read=%+v", got)
			}
			switch mode {
			case "clear":
				f.cache.Clear()
			case "lru":
				for i := 0; ; i++ {
					key := fmt.Sprintf("W6E%04d", i)
					if f.cache.shardFor(key) == f.cache.shardFor("K1ABC") {
						f.cache.UpdateGrid(key, "FN20", false)
						break
					}
				}
			case "ttl":
				if _, _, ok := f.cache.LookupGrid("K1ABC", nil); ok {
					t.Fatal("short TTL did not invalidate resident")
				}
			}
			close(gates[0])
			if got := authorityReply(t, derived.resp); got.ok {
				t.Fatalf("late derived borrowed report or survived cache loss: %+v", got)
			}
			assertAuthorityDrained(t, f)
			if grid, derived, ok := f.cache.LookupGrid("K1ABC", nil); ok && (derived || grid != "EM12AB") {
				t.Fatalf("late derived republished after reported hydration: %s/%v/%v", grid, derived, ok)
			}
		})
	}
}

// These drive real disk batches with owned envelopes; no alternate writer is
// used. A's aggregate timestamp is deliberately later than B's live timestamp.
func TestGridAuthorityCommitWinner(t *testing.T) {
	for _, mode := range []string{"newer_commits", "newer_failed", "newer_dropped"} {
		t.Run(mode, func(t *testing.T) {
			store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
			if err != nil {
				t.Fatal(err)
			}
			cache := newCallMetaCache(20000, 0)
			g := bindGridCoordinator(newGridStoreHandle(store), cache)
			metrics := &gridMetrics{}
			g.metrics = metrics
			updates := make(chan gridWriteRequest, 8192)
			done := make(chan struct{})
			ackA, releaseA, writeB, releaseB := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var onceA, onceB, stopOnce sync.Once
			closed := false
			unblock := func() {
				select {
				case <-releaseA:
				default:
					close(releaseA)
				}
				select {
				case <-releaseB:
				default:
					close(releaseB)
				}
			}
			stop := func() { stopOnce.Do(func() { close(updates); <-done }) }
			t.Cleanup(func() {
				unblock()
				stop()
				if !closed {
					_ = store.Close()
				}
			})
			ops := gridWriterOps{
				afterWrite: func(batch []gridstore.Record, err error) {
					for _, rec := range batch {
						if rec.Call == "K1ABC" && rec.Grid.String == "EM12AA" {
							if err != nil {
								t.Errorf("A write failed: %v", err)
							}
							onceA.Do(func() { close(ackA); <-releaseA })
						}
					}
				},
				beforeWrite: func(batch []gridstore.Record) {
					for _, rec := range batch {
						if rec.Call == "K1ABC" && rec.Grid.String == "FN31AB" {
							onceB.Do(func() { close(writeB); <-releaseB })
						}
					}
				},
			}
			go runGridWriter(g, updates, done, time.Millisecond, 0, metrics, ops)
			g.enqueueReport(authorityRecord("EM12AA", false, time.Now().Add(time.Hour)), updates)
			waitGridRetirement(t, ackA)
			if mode == "newer_dropped" {
				for i := 0; i < 8192; i++ {
					r := authorityRecord("FN20", false, time.Now())
					r.Call = fmt.Sprintf("W6A%05d", i)
					g.enqueueReport(r, updates)
				}
			}
			g.enqueueReport(authorityRecord("FN31AB", false, time.Now()), updates)
			close(releaseA)
			if mode != "newer_dropped" {
				waitGridRetirement(t, writeB)
				if grid, derived, ok := cache.LookupGrid("K1ABC", nil); grid != "FN31AB" || derived || !ok {
					t.Fatalf("old A acknowledgement clobbered live B: %s/%v/%v", grid, derived, ok)
				}
				cache.Clear()
				if mode == "newer_failed" {
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					closed = true
				}
				close(releaseB)
			}
			stop()
			grid, derived, ok := cache.LookupGrid("K1ABC", nil)
			if mode == "newer_failed" {
				if ok {
					t.Fatalf("old A republished after failed B: %s/%v", grid, derived)
				}
			} else if grid != "FN31AB" || derived || !ok {
				t.Fatalf("final B authority lost: %s/%v/%v", grid, derived, ok)
			}
			if mode == "newer_commits" {
				rec, err := store.Get("K1ABC")
				if err != nil || rec == nil || rec.Grid.String != "FN31AB" || rec.GridDerived {
					t.Fatalf("legitimate B commit lost: %+v/%v", rec, err)
				}
			}
			if mode == "newer_dropped" {
				rec, err := store.Get("K1ABC")
				if err != nil || rec == nil || rec.Grid.String != "EM12AA" {
					t.Fatalf("dropped B unexpectedly persisted: %+v/%v", rec, err)
				}
			}
			g.mu.Lock()
			retained := len(g.entries)
			g.mu.Unlock()
			if retained != 0 || metrics.pendingReports.Load() != 0 || metrics.copiedTextBytes.Load() != 0 {
				t.Fatalf("writer retained owners: %d", retained)
			}
		})
	}
}

func TestGridAuthorityEpochs(t *testing.T) {
	for _, mode := range []string{"cache_clear_negative", "store_replacement_negative", "negative_pin_fence"} {
		t.Run(mode, func(t *testing.T) {
			f := newGridAuthorityFixture(t, nil, time.Hour, gridWriterOps{}, nil)
			old, _ := f.coord.beginLookup("K1ABC")
			switch mode {
			case "store_replacement_negative":
				next, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = next.Close() })
				f.handle.Set(nil)
				f.handle.Set(next)
			case "cache_clear_negative":
				f.coord.invalidateCache()
			}
			fresh, _ := f.coord.beginLookup("K1ABC")
			rec := authorityRecord("FN31AB", false, time.Now())
			if mode == "negative_pin_fence" {
				f.coord.finishLookup(fresh, nil, nil)
				if got, _ := f.coord.finishLookup(old, &rec, nil); got != (gridLookupResult{grid: "FN31AB", ok: true}) {
					t.Fatalf("own positive lost: %+v", got)
				}
				if old.entry.pin != nil {
					t.Fatal("negative witness permitted recreating a pin")
				}
			} else {
				f.coord.finishLookup(fresh, &rec, nil)
				negativeIdentity := fresh.entry.negative
				f.coord.finishLookup(old, nil, nil)
				if fresh.entry.pin == nil || fresh.entry.negative != negativeIdentity {
					t.Fatal("old-epoch negative erased current pin authority")
				}
			}
			f.coord.endLookup(old)
			f.coord.endLookup(fresh)
			assertAuthorityDrained(t, f)
		})
	}
	t.Run("retired_pending_does_not_contest_new_store", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		db := gridRetirementCTY(t)
		f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Millisecond, gridWriterOps{afterWrite: func(_ []gridstore.Record, _ error) { once.Do(func() { close(entered); <-release }) }}, func() *cty.CTYDatabase { return db })
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		f.update("K1ABC", "EM12AA")
		waitGridRetirement(t, entered)
		f.handle.Set(nil)
		if grid, _, ok := f.lookup("K1ABC"); !ok || grid != "EM12AA" {
			t.Fatal("Set(nil) removed offline live cache")
		}
		next, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = next.Close() })
		f.handle.Set(next)
		grid, derived, ok := f.syncLookup("K1ABC")
		if grid != "FN20" || !derived || !ok {
			t.Fatalf("retired owner contested independent new-store lookup: %s/%v/%v", grid, derived, ok)
		}
		close(release)
		assertAuthorityDrained(t, f)
		if grid, derived, ok := f.cache.LookupGrid("K1ABC", nil); grid != "FN20" || !derived || !ok {
			t.Fatalf("retired acknowledgement replaced new authority: %s/%v/%v", grid, derived, ok)
		}
	})
}

func TestGridAuthorityPendingCommit(t *testing.T) {
	for _, mode := range []string{"new_lookup_after_clear", "failed_owner_retires"} {
		t.Run(mode, func(t *testing.T) {
			writeEntered, writeRelease, readRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
			requests := make(chan gridLookupRequest, 1)
			writeResult := make(chan error, 1)
			var writeOnce sync.Once
			ops := gridWriterOps{
				beforeWrite: func(_ []gridstore.Record) { writeOnce.Do(func() { close(writeEntered); <-writeRelease }) },
				afterWrite:  func(_ []gridstore.Record, err error) { writeResult <- err },
				afterRead:   func(req gridLookupRequest, _ *gridstore.Record) { requests <- req; <-readRelease },
			}
			f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Millisecond, ops, nil)
			t.Cleanup(func() {
				select {
				case <-writeRelease:
				default:
					close(writeRelease)
				}
				select {
				case <-readRelease:
				default:
					close(readRelease)
				}
			})
			if err := f.store.Upsert(authorityRecord("EM12AA", mode == "new_lookup_after_clear", time.Now().Add(-time.Hour))); err != nil {
				t.Fatal(err)
			}
			f.update("K1ABC", "FN31AB")
			waitGridRetirement(t, writeEntered)
			f.cache.Clear()
			req := authorityTimedCaller(t, f.syncLookup, requests)
			if mode == "failed_owner_retires" {
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				f.closed = true
				close(writeRelease)
				select {
				case err := <-writeResult:
					if err == nil {
						t.Fatal("closed store write unexpectedly succeeded")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("write failure witness missing")
				}
				waitGridCondition(t, func() bool { return f.metrics.pendingReports.Load() == 0 })
			}
			close(readRelease)
			if got := authorityReply(t, req.resp); got.ok {
				t.Fatalf("contested read escaped pending report boundary: %+v", got)
			}
			if mode == "new_lookup_after_clear" {
				close(writeRelease)
				select {
				case err := <-writeResult:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("successful write witness missing")
				}
			}
			assertAuthorityDrained(t, f)
			grid, derived, ok := f.cache.LookupGrid("K1ABC", nil)
			if mode == "new_lookup_after_clear" {
				if grid != "FN31AB" || derived || !ok {
					t.Fatalf("post-Clear commit failed to establish cache: %s/%v/%v", grid, derived, ok)
				}
			} else if ok {
				t.Fatalf("failed owner retirement admitted old stored report: %s/%v", grid, derived)
			}
		})
	}
}

func TestGridAuthorityPinSource(t *testing.T) {
	for _, oldGrid := range []string{"EM12AA", "FN31AB"} {
		t.Run(oldGrid, func(t *testing.T) {
			f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Hour, gridWriterOps{}, nil)
			token, _ := f.coord.beginLookup("K1ABC")
			rec := authorityRecord(oldGrid, false, time.Now().Add(-time.Hour))
			// Model an already selected newer cache tuple independently of the
			// candidate; it cannot become stored evidence just by winning selection.
			f.cache.UpdateGrid("K1ABC", "FN31AB", false)
			got, _ := f.coord.finishLookup(token, &rec, nil)
			if got != (gridLookupResult{grid: "FN31AB", ok: true}) || token.entry.pin != nil {
				t.Fatalf("cache-only winner promoted to stored pin: result=%+v pin=%+v", got, token.entry.pin)
			}
			if total := f.metrics.supersededCandidates.Load(); total != 1 {
				t.Fatalf("timestamp/source replacement counted %d times, want once", total)
			}
			f.coord.endLookup(token)
			assertAuthorityDrained(t, f)
		})
	}
	t.Run("derived_source_rejected", func(t *testing.T) {
		requests, release := make(chan gridLookupRequest, 1), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		db := gridRetirementCTY(t)
		f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Hour, gridWriterOps{afterRead: func(req gridLookupRequest, _ *gridstore.Record) { requests <- req; <-release }}, func() *cty.CTYDatabase { return db })
		t.Cleanup(unblock)
		req := authorityTimedCaller(t, f.syncLookup, requests)
		f.cache.UpdateGrid("K1ABC", "FN31", true)
		unblock()
		got := authorityReply(t, req.resp)
		if got != (gridLookupResult{grid: "FN31", derived: true, ok: true}) {
			t.Fatalf("actual worker did not select newer cached estimate: %+v", got)
		}
		if total := f.metrics.supersededCandidates.Load(); total != 1 {
			t.Fatalf("derived source replacement counted %d times", total)
		}
		assertAuthorityDrained(t, f)
		if grid, derived, ok := f.cache.LookupGrid("K1ABC", nil); grid != "FN31" || !derived || !ok {
			t.Fatalf("cache differs from actual reply: %s/%v/%v", grid, derived, ok)
		}
		if count, err := f.store.Count(); err != nil || count != 0 {
			t.Fatalf("rejected raw synthesized estimate reached writer: count=%d err=%v", count, err)
		}
	})
}

func TestGridAuthorityDelayedReleaseWhileWriteBlocked(t *testing.T) {
	requirePathReportH3Mappings(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Millisecond, gridWriterOps{beforeWrite: func(_ []gridstore.Record) { once.Do(func() { close(entered); <-release }) }}, nil)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	f.cache.UpdateGrid("K2XYZ", "QF56", false)
	f.update("W6AAA", "DM04AA")
	waitGridRetirement(t, entered)
	p, envelope := correctedGridHeldInput(t, nil)
	p.gridLookup, p.gridUpdate = f.lookup, f.update
	p.gridLookupSync = func(string) (string, bool, bool) {
		t.Error("delayed release used synchronous hook")
		return "", false, false
	}
	done := make(chan struct{})
	go func() { p.handleStabilizerRelease(envelope); close(done) }()
	waitGridRetirement(t, done)
	rows := p.buf.GetRecent(1)
	if len(rows) != 1 || rows[0].DXCallNorm != "K1ABD" || rows[0].DXMetadata.Grid != "" {
		t.Fatal("blocked write prevented unknown-grid delayed output")
	}
	close(release)
	assertAuthorityDrained(t, f)
	if rows[0].DXMetadata.Grid != "" {
		t.Fatal("late work mutated sealed output")
	}
}
