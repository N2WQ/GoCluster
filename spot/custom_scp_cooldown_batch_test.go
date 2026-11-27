// File role: Falsifies Custom SCP 6-hour refresh cooldown boundaries, immediate
// new-spotter/cell-change admission, and single-batch RecordSpot persistence.
package spot

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCustomSCPRefreshCooldownBoundaries(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{
		HorizonDays:       60,
		StaticHorizonDays: 395,
		MaxSpottersPerKey: 4,
	})
	t0 := time.Now().UTC().Truncate(time.Second).Add(-24 * time.Hour)
	key := customSCPKey{call: "W1AW", band: "20m", bucket: "cw"}
	obsKey := observationKeyString(key, "K1TTT")
	metaKey := customSCPMetaPrefix + key.call

	// Initial admission writes both m| and o|.
	store.recordObservation("W1AW", "20m", "CW", "K1TTT", 101, 10, true, t0)
	requireCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(t0.Unix()))
	requireCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(t0.Unix(), 101))

	// At 6h - 1s with the same spotter and H3 cell, both memory and Pebble stay unchanged.
	beforeBoundary := t0.Add(customSCPDefaultRefreshCooldown - time.Second)
	store.recordObservation("W1AW", "20m", "CW", "K1TTT", 101, 10, true, beforeBoundary)
	requireCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(t0.Unix()))
	requireCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(t0.Unix(), 101))
	store.mu.RLock()
	if got := store.static["W1AW"]; got != t0.Unix() {
		store.mu.RUnlock()
		t.Fatalf("static timestamp advanced within cooldown: got %d want %d", got, t0.Unix())
	}
	entry := store.entries[key]
	if entry == nil || entry.lastSeen != t0.Unix() {
		store.mu.RUnlock()
		t.Fatalf("entry.lastSeen advanced within cooldown: got %+v want %d", entry, t0.Unix())
	}
	store.mu.RUnlock()
	assertCustomSCPEvictionIndex(t, store)
	assertCustomSCPCleanupCoupling(t, store)

	// New spotters within < 6h admit immediately so multi-spotter corroboration works right away.
	tNewSpotter := t0.Add(time.Minute)
	store.recordObservation("W1AW", "20m", "CW", "W3LPL", 202, 10, true, tNewSpotter)
	requireCustomSCPCleanupRaw(t, store, observationKeyString(key, "W3LPL"), customSCPCleanupObservationBytes(tNewSpotter.Unix(), 202))
	// Static m|W1AW remains at t0 because tNewSpotter is < 6h after t0.
	requireCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(t0.Unix()))
	if got := store.RecentSupportCount("W1AW", "20m", "CW", tNewSpotter); got != 2 {
		t.Fatalf("new spotter within cooldown not immediately counted: got %d want 2", got)
	}

	// Same spotter within < 6h with a changed H3 cell updates immediately.
	tCellChange := t0.Add(2 * time.Minute)
	store.recordObservation("W1AW", "20m", "CW", "K1TTT", 303, 10, true, tCellChange)
	requireCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(tCellChange.Unix(), 303))
	// Static m|W1AW still stays at t0 until 6h elapses.
	requireCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(t0.Unix()))

	// Exact 6h boundary refreshes static membership m|W1AW; K1TTT was refreshed at t0+2m so wait until tCellChange+6h.
	tStaticBoundary := t0.Add(customSCPDefaultRefreshCooldown)
	store.recordObservation("W1AW", "20m", "CW", "K1TTT", 303, 10, true, tStaticBoundary)
	requireCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(tStaticBoundary.Unix()))
	// K1TTT is only 5h58m after tCellChange, so o| stays at tCellChange.
	requireCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(tCellChange.Unix(), 303))

	// Exact 6h after tCellChange refreshes K1TTT observation.
	tObsBoundary := tCellChange.Add(customSCPDefaultRefreshCooldown)
	store.recordObservation("W1AW", "20m", "CW", "K1TTT", 303, 10, true, tObsBoundary)
	requireCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(tObsBoundary.Unix(), 303))
	// Static m|W1AW stays at tStaticBoundary because tObsBoundary is only 2m after tStaticBoundary.
	requireCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(tStaticBoundary.Unix()))

	assertCustomSCPEvictionIndex(t, store)
	assertCustomSCPCleanupCoupling(t, store)

	// Reopen confirms disk and memory match identically.
	reopened := reopenCustomSCPCleanupTestStore(t, store)
	requireCustomSCPCleanupRaw(t, reopened, metaKey, customSCPCleanupStaticBytes(tStaticBoundary.Unix()))
	requireCustomSCPCleanupRaw(t, reopened, obsKey, customSCPCleanupObservationBytes(tObsBoundary.Unix(), 303))
	requireCustomSCPCleanupRaw(t, reopened, observationKeyString(key, "W3LPL"), customSCPCleanupObservationBytes(tNewSpotter.Unix(), 202))
	assertCustomSCPEvictionIndex(t, reopened)
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPRecordSpotFamilySingleBatchAndOverflowReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scp")
	opts := CustomSCPOptions{
		Path:                   path,
		HorizonDays:            60,
		StaticHorizonDays:      395,
		MaxKeys:                2,
		MaxSpottersPerKey:      2,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC().Truncate(time.Second)
	recordSpot := func(dx, de string, ts time.Time) {
		sp := NewSpot(dx, de, 14025.0, "CW")
		sp.Confidence = "V"
		sp.Time = ts
		sp.HasReport = true
		sp.Report = 15
		sp.DEGridNorm = "FN20"
		store.RecordSpot(sp)
	}

	// Fill both family keys (K1BAT/KH6 and K1BAT) to MaxSpottersPerKey=2, then overflow with a 3rd spotter.
	recordSpot("KH6/K1BAT", "N0AAA", now.Add(-3*time.Minute))
	recordSpot("KH6/K1BAT", "N0BBB", now.Add(-2*time.Minute))
	recordSpot("KH6/K1BAT", "N0CCC", now.Add(-time.Minute))

	for _, call := range []string{"K1BAT/KH6", "K1BAT"} {
		key := customSCPKey{call: call, band: "20m", bucket: "cw"}
		requireCustomSCPCleanupMissing(t, store, observationKeyString(key, "N0AAA"))
		requireCustomSCPCleanupRaw(t, store, customSCPMetaPrefix+call, customSCPCleanupStaticBytes(now.Add(-3*time.Minute).Unix()))
		if got := countObservationRecords(t, store, key); got != 2 {
			t.Fatalf("%s persisted observation count = %d, want 2", call, got)
		}
	}

	// Recording a third distinct call evicts the oldest key (K1BAT, tie-broken before K1BAT/KH6) in the same batch.
	recordSpot("W9EVICT", "N0DDD", now)
	requireCustomSCPCleanupMissing(t, store, observationKeyString(customSCPKey{call: "K1BAT", band: "20m", bucket: "cw"}, "N0BBB"))
	requireCustomSCPCleanupMissing(t, store, observationKeyString(customSCPKey{call: "K1BAT", band: "20m", bucket: "cw"}, "N0CCC"))
	requireCustomSCPCleanupRaw(t, store, customSCPMetaPrefix+"W9EVICT", customSCPCleanupStaticBytes(now.Unix()))
	assertCustomSCPEvictionIndex(t, store)
	assertCustomSCPCleanupCoupling(t, store)

	// Reopen verifies batched sets, overflow deletes, and eviction deletes survived cleanly.
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() {
		_ = reopened.Close()
	}()
	if got := reopened.StatsSnapshot().ObservationKeys; got != 2 {
		t.Fatalf("reopened observation keys = %d, want 2", got)
	}
	requireCustomSCPCleanupMissing(t, reopened, observationKeyString(customSCPKey{call: "K1BAT", band: "20m", bucket: "cw"}, "N0BBB"))
	requireCustomSCPCleanupRaw(t, reopened, customSCPMetaPrefix+"W9EVICT", customSCPCleanupStaticBytes(now.Unix()))
	assertCustomSCPEvictionIndex(t, reopened)
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPRecordSpotConcurrentReadersAndCleanup(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{
		HorizonDays:       60,
		StaticHorizonDays: 395,
		MaxKeys:           64,
		MaxSpottersPerKey: 4,
	})
	now := time.Now().UTC().Truncate(time.Second)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			sp := NewSpot(fmt.Sprintf("KH6/K%dRACE", i%12), fmt.Sprintf("N0%02d", i%6), 14025.0, "CW")
			sp.Confidence = "V"
			sp.Time = now.Add(time.Duration(i) * time.Second)
			sp.HasReport = true
			sp.Report = 12
			sp.DEGridNorm = "FN20"
			store.RecordSpot(sp)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			_ = store.StaticContains(fmt.Sprintf("K%dRACE", i%12))
			_ = store.RecentSupportCount(fmt.Sprintf("K%dRACE/KH6", i%12), "20m", "CW", now.Add(time.Minute))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			store.cleanup(now.Add(time.Minute))
		}
	}()
	wg.Wait()
	assertCustomSCPEvictionIndex(t, store)
	assertCustomSCPCleanupCoupling(t, store)
}
