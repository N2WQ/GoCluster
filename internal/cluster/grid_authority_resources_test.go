package cluster

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestGridAuthorityBounds(t *testing.T) {
	f := newGridAuthorityFixture(t, nil, time.Hour, gridWriterOps{}, nil)
	g := f.coord
	updates := make(chan gridWriteRequest, 8192)
	pending := make(map[string]gridWriteRequest)
	const keyBytes = 3072
	key := func(i int) string {
		suffix := fmt.Sprintf("%08d", i)
		return strings.Repeat("K", keyBytes-len(suffix)) + suffix
	}
	frame := strings.Repeat("X", 1<<20) + key(0) + strings.Repeat("Y", 1<<20)
	savedSource := frame[1<<20 : (1<<20)+keyBytes]
	enqueue := func(i int) {
		rec := authorityRecord("FN31AB", false, time.Now())
		rec.Call = key(i)
		if i == 0 {
			rec.Call = savedSource
		}
		g.enqueueReport(rec, updates)
	}
	for i := 0; i < 8192; i++ {
		enqueue(i)
	}
	for i := 0; i < 500; i++ {
		g.coalesce(pending, <-updates)
		enqueue(8192 + i)
	}
	handoff := <-updates
	enqueue(8692)
	var tokens []gridLookupToken
	const maxGridBytes = 3 * 65535
	for i := 0; i < 5; i++ {
		token, _ := g.beginLookup(key(9000 + i))
		tokens = append(tokens, token)
		// A decoded legacy field has uint16 length (gridstore/store.go), and
		// can contain invalid UTF-8. Normalize that actual 65535-byte input.
		rec := authorityRecord(strings.Repeat("\xff", 65535), false, time.Now())
		rec.Call = token.entry.call
		result, _ := g.finishLookup(token, &rec, nil)
		if !result.ok || len(result.grid) != maxGridBytes {
			t.Fatal("maximum stored positive not selected")
		}
		if unsafe.StringData(token.entry.pin.grid) == unsafe.StringData(result.grid) {
			t.Fatal("stored pin reused its source backing")
		}
	}
	g.mu.Lock()
	if len(g.entries) != 8698 || g.keyBytes != 8698*keyBytes || g.pinBytes != 5*maxGridBytes || g.pending != 8693 {
		t.Fatalf("owner bound mismatch keys=%d text=%d pins=%d reports=%d", len(g.entries), g.keyBytes, g.pinBytes, g.pending)
	}
	if unsafe.StringData(g.entries[savedSource].call) == unsafe.StringData(savedSource) {
		t.Fatal("normalized key not compactly copied")
	}
	runtime.KeepAlive(frame)
	g.releaseWriteLocked(handoff)
	for len(updates) > 0 {
		g.releaseWriteLocked(<-updates)
	}
	g.mu.Unlock()
	g.finishBatch(pending, errors.New("controlled dropped batch"))
	for _, token := range tokens {
		g.endLookup(token)
	}
	assertAuthorityDrained(t, f)
}

func TestGridAuthorityTextBounds(t *testing.T) {
	for _, mode := range []string{"invalid_utf8", "unicode_expansion"} {
		t.Run(mode, func(t *testing.T) {
			raw := strings.Repeat("\xff", 1024)
			if mode == "unicode_expansion" {
				raw = strings.Repeat("\u023f", 512)
			}
			if len(raw) != 1024 {
				t.Fatal("fixture lost source wire bound")
			}
			key := normalizeCallForMetadata(raw)
			if len(key) > 3*1024 || len(key) <= len(raw) {
				t.Fatalf("normalizer expansion bound: raw=%d normalized=%d", len(raw), len(key))
			}
			g := newGridCoordinator(nil)
			g.handle = newGridStoreHandle(nil)
			token, _ := g.beginLookup(key)
			if g.keyBytes != uint64(len(key)) {
				t.Fatal("expanded compact key accounting mismatch")
			}
			g.finishLookup(token, nil, nil)
			g.endLookup(token)
			if g.entries != nil || g.keyBytes != 0 {
				t.Fatal("normalized key retained")
			}
		})
	}
	t.Run("reported_pin_compacts_real_substring", func(t *testing.T) {
		frame := strings.Repeat("X", 1<<20) + strings.Repeat("A", 65535) + strings.Repeat("Y", 1<<20)
		source := frame[1<<20 : (1<<20)+65535]
		g := newGridCoordinator(nil)
		g.handle = newGridStoreHandle(nil)
		token, _ := g.beginLookup("K1ABC")
		rec := authorityRecord(source, false, time.Now())
		result, _ := g.finishLookup(token, &rec, nil)
		if len(result.grid) != 65535 || token.entry.pin == nil || unsafe.StringData(token.entry.pin.grid) == unsafe.StringData(source) {
			t.Fatal("stored pin retained full source frame")
		}
		runtime.KeepAlive(frame)
		g.endLookup(token)
		if g.pinBytes != 0 || g.entries != nil {
			t.Fatal("compact pin retained after last reader")
		}
	})
}

func TestGridAuthorityAnchoredChurn(t *testing.T) {
	f := newGridAuthorityFixture(t, newCallMetaCache(16, 0), time.Hour, gridWriterOps{}, nil)
	g := f.coord
	initial := authorityRecord("EM12", true, time.Now().Add(-time.Hour))
	initial.Call = "K1ANCHOR"
	if err := f.store.Upsert(initial); err != nil {
		t.Fatal(err)
	}
	anchor, captured := g.beginLookup("K1ANCHOR")
	stale, err := captured.Get("K1ANCHOR")
	if err != nil || stale == nil {
		t.Fatalf("held actual read missing: %+v/%v", stale, err)
	}
	updates := make(chan gridWriteRequest, 2048)
	for i := 0; i < 2048; i++ {
		rec := authorityRecord("FN31AB", false, time.Now())
		rec.Call = fmt.Sprintf("K1PRIME%05d", i)
		g.enqueueReport(rec, updates)
	}
	g.mu.Lock()
	for len(updates) > 0 {
		g.releaseWriteLocked(<-updates)
	}
	g.mu.Unlock()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 12000; i++ {
		call := strings.Repeat("K", 3050) + fmt.Sprintf("%08d", i)
		token, _ := g.beginLookup(call)
		g.finishLookup(token, nil, nil)
		g.endLookup(token)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	g.mu.Lock()
	entries, peak, rebuilds, text := len(g.entries), g.peak, g.rebuilds, g.keyBytes+g.pinBytes
	g.mu.Unlock()
	if entries != 1 || peak > 64 || rebuilds < 10 || text != uint64(len("K1ANCHOR")) {
		t.Fatalf("historical backing not compacted: entries=%d peak=%d rebuilds=%d text=%d", entries, peak, rebuilds, text)
	}
	if after.HeapAlloc > before.HeapAlloc+4<<20 {
		t.Fatalf("anchored churn retained heap: before=%d after=%d", before.HeapAlloc, after.HeapAlloc)
	}
	t.Logf("anchored churn: before_heap=%d after_heap=%d rebuilds=%d peak=%d", before.HeapAlloc, after.HeapAlloc, rebuilds, peak)
	current := authorityRecord("FN31AB", false, time.Now())
	current.Call = "K1ANCHOR"
	g.enqueueReport(current, updates)
	pending := make(map[string]gridWriteRequest)
	g.coalesce(pending, <-updates)
	batch, store := g.prepareBatch(pending)
	writeErr := store.UpsertBatch(batch)
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	g.finishBatch(pending, writeErr)
	for i := 0; ; i++ {
		call := fmt.Sprintf("W6E%05d", i)
		if f.cache.shardFor(call) == f.cache.shardFor("K1ANCHOR") {
			f.cache.UpdateGrid(call, "DM04", false)
			break
		}
	}
	if result, _ := g.finishLookup(anchor, stale, nil); result.ok {
		t.Fatalf("held stale read survived report/cache-loss/compaction: %+v", result)
	}
	if _, _, ok := f.cache.LookupGrid("K1ANCHOR", nil); ok {
		t.Fatal("held stale selection recreated cache authority")
	}
	g.endLookup(anchor)
	assertAuthorityDrained(t, f)
}

func TestGridAuthorityWarmAllocations(t *testing.T) {
	f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Hour, gridWriterOps{}, nil)
	f.cache.UpdateGrid("K1ABC", "FN31AB", false)
	for _, mode := range []string{"cache_hit", "unchanged_update"} {
		t.Run(mode, func(t *testing.T) {
			if allocations := testing.AllocsPerRun(1000, func() {
				if mode == "cache_hit" {
					if grid, derived, ok := f.lookup("K1ABC"); grid != "FN31AB" || derived || !ok {
						t.Fatal("warm hit lost")
					}
				} else {
					f.update("K1ABC", "FN31AB")
				}
			}); allocations != 0 {
				t.Fatalf("warm allocations=%v", allocations)
			}
		})
	}
	assertAuthorityDrained(t, f)
}

func TestGridAuthorityDiagnostics(t *testing.T) {
	m := &gridMetrics{}
	m.activeKeys.Store(math.MaxUint64)
	m.copiedTextBytes.Store(math.MaxUint64)
	m.pendingReports.Store(math.MaxUint64)
	m.supersededCandidates.Store(math.MaxUint64)
	for _, mode := range []string{"maximum_unsigned", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			metrics := m
			if mode == "unavailable" {
				metrics = nil
			}
			rows := formatGridAuthorityLines(metrics)
			if len(rows) != 4 {
				t.Fatalf("rows=%d", len(rows))
			}
			for _, row := range rows {
				if len(row) > 56 {
					t.Fatalf("row exceeds 60-column pane interior: %q", row)
				}
				if mode == "maximum_unsigned" && !strings.Contains(row, "18446744073709551615") {
					t.Fatalf("unsigned counter lost: %q", row)
				}
				if mode == "unavailable" && !strings.HasSuffix(row, "=--") {
					t.Fatalf("placeholder lost: %q", row)
				}
			}
		})
	}
}
