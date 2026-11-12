// File role: Protects canonical metadata identity from admission through retirement.
package cluster

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/gridstore"
)

func TestMetadataKeyNormalization(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"W1ABC -1", "W1ABC"}, {"w1abc\t-1", "W1ABC"},
		{" \tw1abc \t-1 \t", "W1ABC"}, {"W1ABC\u00a0-#", "W1ABC"},
		{"VE6WZ-1-#", "VE6WZ"}, {"VE6WZ-TEST", "VE6WZ"},
		{"VE6WZ-1/P", "VE6WZ-1/P"}, {"VE6WZ/P", "VE6WZ/P"},
		{"K1ABC/VE3-#", "K1ABC/VE3"}, {" K1ABC/VE3 \t-# ", "K1ABC/VE3"},
		{"K1ABC /P", "K1ABC /P"}, {"K1ABC/-1", "K1ABC/"},
		{"-1", "-1"}, {"\xff-1", "\ufffd"}, {"\u023f-1", "\u2c7e"},
		{"", ""}, {" \t\u00a0", ""},
	} {
		t.Run(fmt.Sprintf("%q", tc.input), func(t *testing.T) {
			got := normalizeCallForMetadata(tc.input)
			if got != tc.want || normalizeCallForMetadata(got) != got {
				t.Fatalf("input=%q key=%q repeated=%q want=%q", tc.input, got, normalizeCallForMetadata(got), tc.want)
			}
		})
	}
	if allocs := testing.AllocsPerRun(1000, func() { normalizeCallForMetadata("W1ABC") }); allocs != 0 {
		t.Fatalf("canonical metadata cleanup allocates: %v", allocs)
	}
}

func FuzzMetadataKeyNormalization(f *testing.F) {
	for _, input := range []string{"W1ABC -1", "W1ABC\t-#", "K1ABC/VE3 \t-1", "VE6WZ-1/P", "", " \t", "\xff-1", "\u023f\u00a0-1"} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		key := normalizeCallForMetadata(input)
		if repeated := normalizeCallForMetadata(key); key != repeated {
			t.Fatalf("input=%q key=%q repeated=%q", input, key, repeated)
		}
		if strings.TrimSpace(key) != key || strings.ToUpper(key) != key || len(key) > 3*len(input) {
			t.Fatalf("noncanonical or oversized metadata key: input=%q key=%q", input, key)
		}
	})
}

func TestGridMetadataKeyAdmission(t *testing.T) {
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, cached := range []bool{false, true} {
		for _, report := range []bool{false, true} {
			for _, input := range []string{"W1ABC", " w1abc \t-1 ", "", " \t\u00a0"} {
				t.Run(fmt.Sprintf("cache=%t/report=%t/%q", cached, report, input), func(t *testing.T) {
					var cache *callMetaCache
					if cached {
						cache = newCallMetaCache(64, 0)
						cache.UpdateGrid("K1KEEP", "FN20", false)
					}
					g := bindGridCoordinator(newGridStoreHandle(store), cache)
					g.metrics = &gridMetrics{}
					queue := make(chan gridWriteRequest, 1)
					rec := authorityRecord("FN31AB", !report, time.Now())
					rec.Call = input
					if report {
						g.enqueueReport(rec, queue)
					} else {
						g.enqueueMetadata(rec, nil, queue)
					}
					empty := strings.TrimSpace(input) == ""
					wantOwners := uint64(0)
					if report && !empty {
						wantOwners = 1
					}
					if g.pending != wantOwners || uint64(len(g.entries)) != wantOwners || g.metrics.pendingReports.Load() != wantOwners || g.metrics.activeKeys.Load() != wantOwners {
						t.Fatalf("admission owners=%d pending=%d want=%d", len(g.entries), g.pending, wantOwners)
					}
					if cache != nil {
						population := 0
						for i := range cache.shards {
							population += len(cache.shards[i].entries)
						}
						if population != 1+int(wantOwners) {
							t.Fatalf("unexpected cache mutation: entries=%d", population)
						}
					}
					if empty {
						if len(queue) != 0 || g.keyBytes != 0 || g.metrics.copiedTextBytes.Load() != 0 {
							t.Fatal("empty metadata key mutated admission")
						}
						return
					}
					select {
					case req := <-queue:
						if req.record.Call != "W1ABC" {
							t.Fatalf("queued key=%q want W1ABC", req.record.Call)
						}
						if report && (g.entries["W1ABC"] == nil || g.entries["W1ABC"].live != req.reportedRevision) {
							t.Fatal("queued report and registered owner disagree")
						}
						g.mu.Lock()
						g.releaseWriteLocked(req)
						g.mu.Unlock()
					default:
						t.Fatal("nonempty metadata admission missing")
					}
					if g.entries != nil || g.pending != 0 || g.keyBytes != 0 || g.metrics.copiedTextBytes.Load() != 0 {
						t.Fatal("admission owner not released")
					}
				})
			}
		}
	}
}

func TestGridMetadataKeyHook(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, terminal := range []string{"commit", "failure"} {
			t.Run(fmt.Sprintf("cache=%t/%s", cached, terminal), func(t *testing.T) {
				entered := make(chan []gridstore.Record, 1)
				release := make(chan struct{})
				results := make(chan error, 2)
				var held, released sync.Once
				unblock := func() { released.Do(func() { close(release) }) }
				ops := gridWriterOps{
					beforeWrite: func(batch []gridstore.Record) { held.Do(func() { entered <- batch; <-release }) },
					afterWrite:  func(_ []gridstore.Record, err error) { results <- err },
				}
				var cache *callMetaCache
				if cached {
					cache = newCallMetaCache(64, 0)
				}
				f := newGridAuthorityFixture(t, cache, time.Millisecond, ops, nil)
				t.Cleanup(unblock)
				f.update(" w1abc \t-1 ", "FN31AB")
				select {
				case batch := <-entered:
					if len(batch) != 1 || batch[0].Call != "W1ABC" || batch[0].Grid.String != "FN31AB" || batch[0].GridDerived {
						t.Fatalf("actual hook batch=%+v", batch)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("actual writer did not reach the storage boundary")
				}
				if terminal == "failure" {
					if err := f.store.Close(); err != nil {
						t.Fatal(err)
					}
					f.closed = true
				}
				unblock()
				assertAuthorityDrained(t, f)
				firstErr := <-results
				if (terminal == "failure") != (firstErr != nil) {
					t.Fatalf("actual first write result=%v terminal=%s", firstErr, terminal)
				}
				if terminal != "failure" {
					rec, err := f.store.Get("W1ABC")
					if err != nil || rec == nil || rec.Grid.String != "FN31AB" || rec.GridDerived {
						t.Fatalf("stored current authority=%+v err=%v", rec, err)
					}
				}
				if cache != nil {
					if grid, derived, ok := cache.LookupGrid("W1ABC", nil); !ok || derived || grid != "FN31AB" {
						t.Fatalf("current cache=%q/%t/%t want=FN31AB", grid, derived, ok)
					}
				}
			})
		}
	}
}

func TestGridMetadataKeyCoalescing(t *testing.T) {
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cache := newCallMetaCache(64, 0)
	g := bindGridCoordinator(newGridStoreHandle(store), cache)
	g.metrics = &gridMetrics{}
	queue := make(chan gridWriteRequest, 3)
	at := time.Now().UTC()
	first := authorityRecord("FN31AB", false, at)
	first.Call = " w1abc \t-1 "
	second := authorityRecord("DM04AA", false, at.Add(time.Second))
	second.Call = "W1ABC"
	meta := gridstore.Record{Call: "w1abc -#", CTYValid: true, CTYADIF: 291, UpdatedAt: at.Add(time.Hour)}
	g.enqueueReport(first, queue)
	g.enqueueReport(second, queue)
	g.enqueueMetadata(meta, nil, queue)
	if len(queue) != 3 || g.pending != 2 || len(g.entries) != 1 {
		t.Fatalf("aliases did not share admission: queued=%d pending=%d owners=%d", len(queue), g.pending, len(g.entries))
	}
	winner := g.entries["W1ABC"].live
	close(queue)
	writes := 0
	ops := gridWriterOps{beforeWrite: func(batch []gridstore.Record) {
		writes++
		if len(batch) != 1 || batch[0].Call != "W1ABC" || batch[0].Grid.String != "DM04AA" || batch[0].GridDerived || batch[0].Observations != 2 || !batch[0].CTYValid {
			t.Fatalf("aliases were not coalesced together: %+v", batch)
		}
		g.mu.Lock()
		validOwner := g.pending == 1 && g.entries["W1ABC"].live == winner && g.entries["W1ABC"].writes == 1
		g.mu.Unlock()
		if !validOwner {
			t.Fatal("coalescing lost the surviving report revision")
		}
		// A warm live report could mask a wrong retained request revision.
		// Only the actual selected commit may restore the cleared cache.
		cache.Clear()
	}}
	runGridWriter(g, queue, make(chan struct{}), time.Hour, 0, g.metrics, ops)
	if writes != 1 || g.entries != nil || g.pending != 0 || g.keyBytes != 0 || g.metrics.activeKeys.Load() != 0 || g.metrics.pendingReports.Load() != 0 || g.metrics.copiedTextBytes.Load() != 0 {
		t.Fatal("coalesced batch did not retire exactly once")
	}
	rec, err := store.Get("W1ABC")
	count, countErr := store.Count()
	if err != nil || countErr != nil || count != 1 || rec == nil || rec.Grid.String != "DM04AA" || rec.GridDerived || rec.Observations != 2 {
		t.Fatalf("coalesced storage=%+v count=%d err=%v/%v", rec, count, err, countErr)
	}
	if grid, derived, ok := cache.LookupGrid("W1ABC", nil); !ok || derived || grid != "DM04AA" {
		t.Fatalf("coalesced cache=%q/%t/%t", grid, derived, ok)
	}
}
