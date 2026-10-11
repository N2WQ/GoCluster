// File role: Falsifies Custom SCP cleanup ordering and bounded reclamation with
// real Pebble readback, reopen checks, and concurrent evidence mutations.
package spot

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCustomSCPCleanupCandidateRefresh(t *testing.T) {
	for _, kind := range []string{"memory_stale", "memory_overflow", "disk_stale", "disk_malformed", "disk_only_repair"} {
		t.Run(kind, func(t *testing.T) {
			store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
			now := freshCustomSCPTestTime()
			key := customSCPKey{call: "K1REF", band: "40m", bucket: "cw"}
			obsKey := observationKeyString(key, "N0AAA")
			metaKey := customSCPMetaPrefix + key.call
			var candidates []customSCPCleanupDelete
			switch kind {
			case "memory_stale":
				seedCustomSCPCleanupEntry(t, store, key, map[string]customSCPSpotterObs{
					"N0AAA": {seenUnix: now.Add(-48 * time.Hour).Unix(), cellRes1: 101},
				})
				candidates, _ = captureCustomSCPMemoryDeletes(store, now)
			case "memory_overflow":
				seedCustomSCPCleanupEntry(t, store, key, map[string]customSCPSpotterObs{
					"N0AAA": {seenUnix: now.Add(-3 * time.Minute).Unix(), cellRes1: 101},
					"N0BBB": {seenUnix: now.Add(-2 * time.Minute).Unix(), cellRes1: 102},
					"N0CCC": {seenUnix: now.Add(-time.Minute).Unix(), cellRes1: 103},
				})
				candidates, _ = captureCustomSCPMemoryDeletes(store, now)
				if len(candidates) != 1 || !candidates[0].evicted {
					t.Fatalf("expected one authoritative overflow candidate: %+v", candidates)
				}
			case "disk_malformed":
				putCustomSCPCleanupRaw(t, store, obsKey, []byte{1, 2})
				putCustomSCPCleanupRaw(t, store, metaKey, []byte{3})
				candidates = captureCustomSCPDiskDeletes(t, store, now)
			default:
				old := now.Add(-48 * time.Hour).Unix()
				putCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(old, 101))
				putCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(old))
				candidates = captureCustomSCPDiskDeletes(t, store, now)
			}
			if len(candidates) == 0 {
				t.Fatal("fixture did not capture a deletion candidate")
			}
			refreshed := now.Add(time.Minute)
			if kind == "disk_only_repair" {
				// A fresh disk-only replacement is not an authoritative eviction.
				putCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(refreshed.Unix(), 202))
				putCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(refreshed.Unix()))
			} else {
				store.recordObservation(key.call, key.band, "CW", "N0AAA", 202, 0, false, refreshed)
			}
			applyCustomSCPCleanupTestDeletes(store, candidates)
			requireCustomSCPCleanupRaw(t, store, obsKey, customSCPCleanupObservationBytes(refreshed.Unix(), 202))
			requireCustomSCPCleanupRaw(t, store, metaKey, customSCPCleanupStaticBytes(refreshed.Unix()))
			assertCustomSCPCleanupCoupling(t, store)
			reopened := reopenCustomSCPCleanupTestStore(t, store)
			requireCustomSCPCleanupRaw(t, reopened, obsKey, customSCPCleanupObservationBytes(refreshed.Unix(), 202))
			requireCustomSCPCleanupRaw(t, reopened, metaKey, customSCPCleanupStaticBytes(refreshed.Unix()))
			// Recovery is queried after the refreshed stored timestamp, not at
			// the earlier deletion-capture clock when that row is still future.
			if !reopened.StaticContains(key.call) || reopened.RecentSupportCount(key.call, key.band, "CW", refreshed) == 0 {
				t.Fatal("refreshed static membership and recent support did not survive reopen")
			}
			assertCustomSCPCleanupCoupling(t, reopened)
		})
	}
}

func TestCustomSCPCleanupExpiredChunksAndEquality(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
	now := freshCustomSCPTestTime()
	const expiredKeys = customSCPCleanupChunkSize + 7
	for i := 0; i < expiredKeys; i++ {
		seedCustomSCPCleanupEntry(t, store, customSCPKey{call: fmt.Sprintf("K1OLD%04d", i), band: "40m", bucket: "cw"}, map[string]customSCPSpotterObs{
			"N0OLD": {seenUnix: now.Add(-48 * time.Hour).Unix(), cellRes1: 101},
		})
	}
	cutoff := store.observationHorizonCutoffUnix(now)
	equality := customSCPKey{call: "K1EQUAL", band: "40m", bucket: "cw"}
	seedCustomSCPCleanupEntry(t, store, equality, map[string]customSCPSpotterObs{
		"N0EQ": {seenUnix: cutoff, cellRes1: 102},
	})
	fresh := customSCPKey{call: "K1FRESH", band: "20m", bucket: "cw"}
	store.recordObservation(fresh.call, fresh.band, "CW", "N0NEW", 202, 0, false, now)
	var chunkSizes []int
	for {
		candidates, more := captureCustomSCPMemoryDeletes(store, now)
		chunkSizes = append(chunkSizes, len(candidates))
		if len(candidates) > customSCPCleanupChunkSize {
			t.Fatalf("unbounded memory deletion chunk: %d", len(candidates))
		}
		applyCustomSCPCleanupTestDeletes(store, candidates)
		assertCustomSCPCleanupCoupling(t, store)
		if !more {
			break
		}
	}
	if !reflect.DeepEqual(chunkSizes, []int{512, 512, 14}) {
		t.Fatalf("expected two full chunks and a tail, got %v", chunkSizes)
	}
	for i := 0; i < expiredKeys; i++ {
		call := fmt.Sprintf("K1OLD%04d", i)
		requireCustomSCPCleanupMissing(t, store, customSCPMetaPrefix+call)
		requireCustomSCPCleanupMissing(t, store, observationKeyString(customSCPKey{call: call, band: "40m", bucket: "cw"}, "N0OLD"))
	}
	// Equality belongs to this explicit cleanup cutoff. Startup uses the later
	// real clock, so only the safely fresh control is expected to survive reopen.
	requireCustomSCPCleanupRaw(t, store, observationKeyString(equality, "N0EQ"), customSCPCleanupObservationBytes(cutoff, 102))
	requireCustomSCPCleanupRaw(t, store, customSCPMetaPrefix+equality.call, customSCPCleanupStaticBytes(cutoff))
	stats := store.StatsSnapshot()
	if stats.StaleObservationsPruned != expiredKeys || stats.StaleStaticPruned != expiredKeys {
		t.Fatalf("pruning diagnostics did not match genuine removals: %+v", stats)
	}
	reopened := reopenCustomSCPCleanupTestStore(t, store)
	requireCustomSCPCleanupRaw(t, reopened, observationKeyString(fresh, "N0NEW"), customSCPCleanupObservationBytes(now.Unix(), 202))
	if reopened.RecentSupportCount(fresh.call, fresh.band, "CW", now) != 1 {
		t.Fatal("fresh control did not survive chunked cleanup and reopen")
	}
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPCleanupOverflowChunks(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
	now := freshCustomSCPTestTime()
	key := customSCPKey{call: "K1BIG", band: "40m", bucket: "cw"}
	const observations = customSCPCleanupChunkSize + 9
	spotters := make(map[string]customSCPSpotterObs, observations)
	for i := 0; i < observations; i++ {
		spotters[fmt.Sprintf("N0%04d", i)] = customSCPSpotterObs{seenUnix: now.Add(time.Duration(i) * time.Second).Unix(), cellRes1: 101}
	}
	seedCustomSCPCleanupEntry(t, store, key, spotters)
	first, more := captureCustomSCPMemoryDeletes(store, now)
	if len(first) != 512 || !more {
		t.Fatalf("expected full overflow chunk, got %d more=%v", len(first), more)
	}
	for _, candidate := range first {
		if !candidate.evicted {
			t.Fatal("fresh overflow removal was incorrectly classified as expiry")
		}
	}
	applyCustomSCPCleanupTestDeletes(store, first)
	assertCustomSCPCleanupCoupling(t, store)
	tail, more := captureCustomSCPMemoryDeletes(store, now)
	if len(tail) != 7 || more {
		t.Fatalf("expected seven overflow tail deletions, got %d more=%v", len(tail), more)
	}
	applyCustomSCPCleanupTestDeletes(store, tail)
	for i := 0; i < observations-2; i++ {
		requireCustomSCPCleanupMissing(t, store, observationKeyString(key, fmt.Sprintf("N0%04d", i)))
	}
	if store.StatsSnapshot().OverflowObservationsPruned != observations-2 {
		t.Fatalf("overflow pruning diagnostics: %+v", store.StatsSnapshot())
	}
	reopened := reopenCustomSCPCleanupTestStore(t, store)
	for i := observations - 2; i < observations; i++ {
		requireCustomSCPCleanupRaw(t, reopened, observationKeyString(key, fmt.Sprintf("N0%04d", i)), customSCPCleanupObservationBytes(now.Add(time.Duration(i)*time.Second).Unix(), 101))
	}
	if countObservationRecords(t, reopened, key) != 2 {
		t.Fatal("overflow removal did not converge on disk")
	}
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPCleanupEvictsOversizedPrimaryView(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{MaxKeys: 2})
	now := freshCustomSCPTestTime()
	const keys = customSCPCleanupChunkSize + 7
	for i := 0; i < keys; i++ {
		seedCustomSCPCleanupEntry(t, store, customSCPKey{call: fmt.Sprintf("K1CAP%04d", i), band: "40m", bucket: "cw"}, map[string]customSCPSpotterObs{
			"N0CAP": {seenUnix: now.Add(time.Duration(i) * time.Second).Unix(), cellRes1: 101},
		})
	}
	store.cleanup(now)
	for i := 0; i < keys-2; i++ {
		key := customSCPKey{call: fmt.Sprintf("K1CAP%04d", i), band: "40m", bucket: "cw"}
		requireCustomSCPCleanupMissing(t, store, observationKeyString(key, "N0CAP"))
		if !store.StaticContains(key.call) {
			t.Fatal("observation-key eviction erased independent static membership")
		}
	}
	if stats := store.StatsSnapshot(); stats.ObservationKeys != 2 || stats.ObservationSpotters != 2 {
		t.Fatalf("oversized primary view did not converge: %+v", stats)
	}
	assertCustomSCPCleanupCoupling(t, store)
	reopened := reopenCustomSCPCleanupTestStore(t, store)
	for i := keys - 2; i < keys; i++ {
		key := customSCPKey{call: fmt.Sprintf("K1CAP%04d", i), band: "40m", bucket: "cw"}
		requireCustomSCPCleanupRaw(t, reopened, observationKeyString(key, "N0CAP"), customSCPCleanupObservationBytes(now.Add(time.Duration(i)*time.Second).Unix(), 101))
	}
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPCleanupDiskScanChunksOwnKeys(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
	now := freshCustomSCPTestTime()
	const malformedRows = customSCPCleanupChunkSize + 7
	for i := 0; i < malformedRows; i++ {
		putCustomSCPCleanupRaw(t, store, fmt.Sprintf("o|K1SCAN|40m|cw|N0%04d", i), []byte{1})
	}
	freshKey := "o|K1SAFE|20m|cw|N0SAFE"
	putCustomSCPCleanupRaw(t, store, freshKey, customSCPCleanupObservationBytes(now.Unix(), 202))
	store.dbMu.RLock()
	iter, err := store.db.NewIter(nil)
	if err != nil {
		store.dbMu.RUnlock()
		t.Fatal(err)
	}
	iter.First()
	var chunks [][]customSCPCleanupDelete
	for iter.Valid() {
		chunk := scanCustomSCPCleanupChunk(iter, store.observationHorizonCutoffUnix(now), store.staticHorizonCutoffUnix(now))
		if len(chunk) > customSCPCleanupChunkSize {
			t.Fatalf("unbounded disk candidates: %d", len(chunk))
		}
		chunks = append(chunks, chunk)
	}
	if err := iter.Close(); err != nil {
		store.dbMu.RUnlock()
		t.Fatal(err)
	}
	store.dbMu.RUnlock()
	// Apply after the iterator has advanced and been disposed, which would expose
	// candidates retaining Pebble's borrowed key buffers.
	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
		applyCustomSCPCleanupTestDeletes(store, chunk)
	}
	if total != malformedRows || len(chunks) != 2 {
		t.Fatalf("disk scan did not cover full chunk and tail: rows=%d chunks=%d", total, len(chunks))
	}
	for i := 0; i < malformedRows; i++ {
		requireCustomSCPCleanupMissing(t, store, fmt.Sprintf("o|K1SCAN|40m|cw|N0%04d", i))
	}
	requireCustomSCPCleanupRaw(t, store, freshKey, customSCPCleanupObservationBytes(now.Unix(), 202))
	reopened := reopenCustomSCPCleanupTestStore(t, store)
	requireCustomSCPCleanupRaw(t, reopened, freshKey, customSCPCleanupObservationBytes(now.Unix(), 202))
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPCleanupFreshDiskScanBoundsWork(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
	now := freshCustomSCPTestTime()
	const rows = customSCPCleanupChunkSize + 7
	for i := 0; i < rows; i++ {
		putCustomSCPCleanupRaw(t, store, fmt.Sprintf("o|K1ONLY|40m|cw|N0%04d", i), customSCPCleanupObservationBytes(now.Unix(), 101))
	}
	store.dbMu.RLock()
	defer store.dbMu.RUnlock()
	iter, err := store.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			t.Error(err)
		}
	}()
	iter.First()
	first := scanCustomSCPCleanupChunk(iter, store.observationHorizonCutoffUnix(now), store.staticHorizonCutoffUnix(now))
	if len(first) != 0 || !iter.Valid() || string(iter.Key()) != "o|K1ONLY|40m|cw|N00512" {
		t.Fatalf("fresh-only scan exceeded its row budget: candidates=%d valid=%v", len(first), iter.Valid())
	}
	tail := scanCustomSCPCleanupChunk(iter, store.observationHorizonCutoffUnix(now), store.staticHorizonCutoffUnix(now))
	if len(tail) != 0 || iter.Valid() {
		t.Fatalf("fresh-only tail did not finish: candidates=%d valid=%v", len(tail), iter.Valid())
	}
}

func TestCustomSCPConcurrentCleanupWrites(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{MaxKeys: 32, MaxSpottersPerKey: 4})
	now := freshCustomSCPTestTime()
	for i := 0; i < 48; i++ {
		seedCustomSCPCleanupEntry(t, store, customSCPKey{call: fmt.Sprintf("K1RACE%02d", i), band: "40m", bucket: "cw"}, map[string]customSCPSpotterObs{
			"N0OLD": {seenUnix: now.Add(-48 * time.Hour).Unix(), cellRes1: 101},
		})
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for i := 0; i < 96; i++ {
				call := fmt.Sprintf("K1RACE%02d", i%16)
				switch worker {
				case 0, 1:
					store.recordObservation(call, "40m", "CW", fmt.Sprintf("N%d%02d", worker, i%8), uint16(101+i%4), 0, false, now.Add(time.Duration(i)*time.Second))
				case 2:
					store.cleanup(now)
				default:
					store.RecentSupportCount(call, "40m", "CW", now)
					store.StatsSnapshot()
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	store.cleanup(now)
	if stats := store.StatsSnapshot(); stats.ObservationKeys == 0 || stats.ObservationKeys > 32 || stats.ObservationSpotters > 32*4 {
		t.Fatalf("concurrent fixture did not mutate bounded evidence: %+v", stats)
	}
	assertCustomSCPCleanupCoupling(t, store)
}
