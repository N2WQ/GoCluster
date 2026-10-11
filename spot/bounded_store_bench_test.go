package spot

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkCustomSCPRecordOverflow(b *testing.B) {
	const maxSpotters = 64

	now := time.Now().UTC()
	oldestSpotter := "N000"
	newSpotter := "NZZZ"
	oldestObs := customSCPSpotterObs{
		seenUnix: now.Add(-time.Duration(maxSpotters) * time.Second).Unix(),
		cellRes1: 100,
	}
	entry := &customSCPEntry{
		spotters: makeCustomSCPSpotters(maxSpotters),
		lastSeen: now.Add(-time.Second).Unix(),
	}
	for i := 0; i < maxSpotters; i++ {
		spotter := fmt.Sprintf("N%03d", i)
		entryUpsertSpotter(entry, spotter, customSCPSpotterObs{
			seenUnix: now.Add(time.Duration(i-maxSpotters) * time.Second).Unix(),
			cellRes1: uint16(100 + i),
		}, maxSpotters)
	}

	store := &CustomSCPStore{
		opts:                sanitizeCustomSCPOptions(CustomSCPOptions{MaxSpottersPerKey: maxSpotters}),
		entries:             make(map[customSCPKey]*customSCPEntry, 1),
		entryExpiry:         newCustomSCPEntryExpiryQueue(1),
		static:              make(map[string]int64, 1),
		observationSpotters: maxSpotters,
	}
	store.mu.Lock()
	retainCustomSCPTestStaticLocked(store, "K1BENCH", now.Unix())
	retainCustomSCPTestEntryLocked(store, customSCPKey{call: "K1BENCH", band: "40m", bucket: "cw"}, entry)
	store.mu.Unlock()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.recordObservation("K1BENCH", "40m", "CW", newSpotter, 999, 0, false, now)
		b.StopTimer()
		store.mu.Lock()
		if removed, ok := entryDeleteSpotter(entry, newSpotter); ok {
			store.releaseInternedStringLocked(removed.spotter)
		}
		entryUpsertSpotter(entry, store.retainInternedStringLocked(oldestSpotter), oldestObs, store.opts.MaxSpottersPerKey)
		entry.lastSeen = now.Add(-time.Second).Unix()
		store.mu.Unlock()
		b.StartTimer()
	}
}

func BenchmarkCustomSCPRecordExistingSpotterUpdate(b *testing.B) {
	now := time.Now().UTC()
	entry := &customSCPEntry{
		spotters: makeCustomSCPSpotters(4),
		lastSeen: now.Add(-time.Second).Unix(),
	}
	entryUpsertSpotter(entry, "N0AAA", customSCPSpotterObs{
		seenUnix: now.Add(-time.Second).Unix(),
		cellRes1: 101,
	}, 4)

	store := &CustomSCPStore{
		opts:                sanitizeCustomSCPOptions(CustomSCPOptions{MaxSpottersPerKey: 4}),
		entries:             make(map[customSCPKey]*customSCPEntry, 1),
		entryExpiry:         newCustomSCPEntryExpiryQueue(1),
		static:              make(map[string]int64, 1),
		observationSpotters: 1,
	}
	store.mu.Lock()
	retainCustomSCPTestStaticLocked(store, "K1BENCH", now.Unix())
	retainCustomSCPTestEntryLocked(store, customSCPKey{call: "K1BENCH", band: "40m", bucket: "cw"}, entry)
	store.markEntryForCleanupLocked(customSCPKey{call: "K1BENCH", band: "40m", bucket: "cw"}, entry)
	store.mu.Unlock()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.recordObservation("K1BENCH", "40m", "CW", "N0AAA", 202, 0, false, now.Add(time.Duration(i+1)*time.Second))
	}
}

func BenchmarkRecentBandRecordOverflow(b *testing.B) {
	const maxSpotters = 64

	now := time.Now().UTC()
	store := NewRecentBandStoreWithOptions(RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: maxSpotters,
	})
	key, ok := store.normalizeKey("K1BENCH", "40m", "CW")
	if !ok {
		b.Fatalf("normalize recent-band key")
	}
	entry := &recentBandEntry{
		spotters: make(map[string]time.Time, maxSpotters+1),
		lastSeen: now.Add(-time.Second),
	}
	oldestSpotter := "W000"
	oldestSeen := now.Add(-time.Duration(maxSpotters) * time.Second)
	for i := 0; i < maxSpotters; i++ {
		spotter := fmt.Sprintf("W%03d", i)
		entry.spotters[spotter] = now.Add(time.Duration(i-maxSpotters) * time.Second)
	}
	store.shards[0].entries = map[recentBandKey]*recentBandEntry{key: entry}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Record("K1BENCH", "40m", "CW", "WZZZ", now)
		b.StopTimer()
		delete(entry.spotters, "WZZZ")
		entry.spotters[oldestSpotter] = oldestSeen
		entry.lastSeen = now.Add(-time.Second)
		b.StartTimer()
	}
}

func BenchmarkCustomSCPLoadOversizedKey(b *testing.B) {
	const maxSpotters = 64

	now := time.Now().UTC()
	store := &CustomSCPStore{
		opts: sanitizeCustomSCPOptions(CustomSCPOptions{MaxSpottersPerKey: maxSpotters}),
	}
	key := customSCPKey{call: "K1LOAD", band: "40m", bucket: "cw"}
	entry := &customSCPEntry{
		spotters: makeCustomSCPSpotters(maxSpotters),
		lastSeen: now.Unix(),
	}
	oldestSpotter := "L000"
	oldestObs := customSCPSpotterObs{
		seenUnix: now.Add(-time.Duration(maxSpotters) * time.Second).Unix(),
		cellRes1: 200,
	}
	for i := 0; i <= maxSpotters; i++ {
		spotter := fmt.Sprintf("L%03d", i)
		entryUpsertSpotter(entry, spotter, customSCPSpotterObs{
			seenUnix: now.Add(time.Duration(i-maxSpotters) * time.Second).Unix(),
			cellRes1: uint16(200 + i),
		}, maxSpotters)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pendingOverflow := false
		store.trimPendingEntryOnLoadLocked(key, entry, &pendingOverflow, nil)
		b.StopTimer()
		entryUpsertSpotter(entry, oldestSpotter, oldestObs, store.opts.MaxSpottersPerKey)
		b.StartTimer()
	}
}

func BenchmarkCustomSCPSnapshotSupport(b *testing.B) {
	for _, spotters := range []int{1, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("spotters_%02d", spotters), func(b *testing.B) {
			now := time.Now().UTC()
			entry := &customSCPEntry{
				spotters: makeCustomSCPSpotters(spotters),
				lastSeen: now.Unix(),
			}
			for i := 0; i < spotters; i++ {
				entryUpsertSpotter(entry, fmt.Sprintf("N%03d", i), customSCPSpotterObs{
					seenUnix: now.Add(-time.Duration(i) * time.Second).Unix(),
					cellRes1: uint16(100 + i),
				}, spotters)
			}
			store := &CustomSCPStore{
				opts:                sanitizeCustomSCPOptions(CustomSCPOptions{MaxSpottersPerKey: spotters}),
				entries:             make(map[customSCPKey]*customSCPEntry, 1),
				entryExpiry:         newCustomSCPEntryExpiryQueue(1),
				observationSpotters: spotters,
			}
			key := customSCPKey{call: "K1BENCH", band: "40m", bucket: "cw"}
			store.mu.Lock()
			retainCustomSCPTestEntryLocked(store, key, entry)
			store.markEntryForCleanupLocked(key, entry)
			store.mu.Unlock()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshot := store.snapshotFor("K1BENCH", "40m", "CW", now, store.opts.CoreMinScore)
				if snapshot.uniqueSpotters != spotters || snapshot.uniqueCells != spotters {
					b.Fatalf("expected %d eligible spotters and cells, got %+v", spotters, snapshot)
				}
			}
		})
	}
}

// Mixed retained populations must keep lookup allocation tied to qualifying
// cells, not copy every row to filter it. Configured larger caps retain the
// preexisting bounded overflow-map path only above the 64-cell stack allowance.
func BenchmarkB11CustomSCPEligibleSnapshot(b *testing.B) {
	for _, fixture := range []struct {
		name     string
		retained int
		mixed    bool
	}{
		{"mixed_64_eligible_32", 64, true},
		{"mixed_128_eligible_64", 128, true},
		{"fresh_128_overflow_cells", 128, false},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			now := time.Now().UTC().Truncate(time.Second)
			entry := &customSCPEntry{spotters: makeCustomSCPSpotters(fixture.retained), lastSeen: now.Unix()}
			want := fixture.retained
			if fixture.mixed {
				want /= 2
			}
			for i := 0; i < fixture.retained; i++ {
				seen := now.Add(-time.Duration(i) * time.Second)
				if fixture.mixed && i%2 != 0 {
					seen = seen.Add(-55 * 24 * time.Hour)
				}
				entryUpsertSpotter(entry, fmt.Sprintf("N%03d", i), customSCPSpotterObs{seenUnix: seen.Unix(), cellRes1: uint16(100 + i)}, fixture.retained)
			}
			store := &CustomSCPStore{
				opts:                sanitizeCustomSCPOptions(CustomSCPOptions{MaxSpottersPerKey: fixture.retained}),
				entries:             make(map[customSCPKey]*customSCPEntry, 1),
				entryExpiry:         newCustomSCPEntryExpiryQueue(1),
				observationSpotters: fixture.retained,
			}
			store.mu.Lock()
			key := retainCustomSCPTestEntryLocked(store, customSCPKey{call: "K1BENCH", band: "40m", bucket: "cw"}, entry)
			store.markEntryForCleanupLocked(key, entry)
			store.mu.Unlock()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got := store.snapshotForNormalizedCall("K1BENCH", "40m", "CW", now, store.opts.CoreMinScore)
				if got.uniqueSpotters != want || got.uniqueCells != want || got.latestSeenUnix != now.Unix() || got.score != 10 {
					b.Fatalf("eligible population changed under bounded lookup: got %+v want %d receivers/cells", got, want)
				}
			}
		})
	}
}

func BenchmarkCustomSCPCleanupFewExpired(b *testing.B) {
	benchmarkCustomSCPCleanup(b, 1024, 4)
}

func BenchmarkCustomSCPCleanupManyExpired(b *testing.B) {
	benchmarkCustomSCPCleanup(b, 1024, 1024)
}

func benchmarkCustomSCPCleanup(b *testing.B, totalEntries, expiredEntries int) {
	now := time.Now().UTC()
	opts := sanitizeCustomSCPOptions(CustomSCPOptions{
		HorizonDays:       30,
		StaticHorizonDays: 30,
		MaxSpottersPerKey: 64,
	})

	buildStore := func() *CustomSCPStore {
		store := &CustomSCPStore{
			opts:         opts,
			entries:      make(map[customSCPKey]*customSCPEntry, totalEntries),
			entryExpiry:  newCustomSCPEntryExpiryQueue(totalEntries),
			static:       make(map[string]int64),
			staticExpiry: newCustomSCPStaticExpiryQueue(totalEntries),
		}
		for i := 0; i < totalEntries; i++ {
			seenAt := now.Add(-1 * time.Hour)
			if i < expiredEntries {
				seenAt = now.Add(-31 * 24 * time.Hour)
			}
			key := customSCPKey{
				call:   fmt.Sprintf("K%04d", i),
				band:   "40m",
				bucket: "cw",
			}
			entry := &customSCPEntry{
				spotters: customSCPTestSpotters(map[string]customSCPSpotterObs{
					"N0AAA": {seenUnix: seenAt.Unix(), cellRes1: 101},
				}),
			}
			key = retainCustomSCPTestEntryLocked(store, key, entry)
			store.observationSpotters++
			store.markEntryForCleanupLocked(key, entry)
		}
		return store
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store := buildStore()
		b.StartTimer()
		store.cleanup(now)
	}
}

func BenchmarkCustomSCPInternerChurn(b *testing.B) {
	values := make([]string, 4096)
	for i := range values {
		values[i] = fmt.Sprintf("S%04d", i)
	}
	var interner customSCPInterner

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := interner.retain(values[i&(len(values)-1)])
		interner.release(value)
	}
	if interner.totalRefs != 0 || len(interner.refs) != 0 || interner.releaseMisses != 0 {
		b.Fatalf("interner did not fully release: refs=%d distinct=%d misses=%d", interner.totalRefs, len(interner.refs), interner.releaseMisses)
	}
}
