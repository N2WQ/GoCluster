// File role: Provides local Custom SCP cleanup fixtures and independent raw
// persistence, primary/secondary ownership, counter, and interner assertions.
package spot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func newCustomSCPCleanupTestStore(t *testing.T, opts CustomSCPOptions) *CustomSCPStore {
	t.Helper()
	opts.Path = filepath.Join(t.TempDir(), "scp")
	opts = customSCPCleanupTestOptions(opts)
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func customSCPCleanupTestOptions(opts CustomSCPOptions) CustomSCPOptions {
	opts.HorizonDays, opts.StaticHorizonDays = 1, 1
	if opts.MaxKeys == 0 {
		opts.MaxKeys = 2048
	}
	if opts.MaxSpottersPerKey == 0 {
		opts.MaxSpottersPerKey = 2
	}
	opts.CoreMinScore, opts.CoreMinH3Cells = 1, 1
	return sanitizeCustomSCPOptions(opts)
}

func reopenCustomSCPCleanupTestStore(t *testing.T, store *CustomSCPStore) *CustomSCPStore {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCustomSCPStore(store.opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	return reopened
}

func seedCustomSCPCleanupEntry(t *testing.T, store *CustomSCPStore, key customSCPKey, spotters map[string]customSCPSpotterObs) {
	t.Helper()
	entry := &customSCPEntry{spotters: customSCPTestSpotters(spotters)}
	store.mu.Lock()
	key = retainCustomSCPTestEntryLocked(store, key, entry)
	store.observationSpotters += len(entry.spotters)
	store.active.Add(key.call, key.band)
	store.markEntryForCleanupLocked(key, entry)
	retainCustomSCPTestStaticLocked(store, key.call, entry.lastSeen)
	store.mu.Unlock()
	for de, obs := range spotters {
		putCustomSCPCleanupRaw(t, store, observationKeyString(key, de), customSCPCleanupObservationBytes(obs.seenUnix, obs.cellRes1))
	}
	putCustomSCPCleanupRaw(t, store, customSCPMetaPrefix+key.call, customSCPCleanupStaticBytes(entry.lastSeen))
}

func captureCustomSCPMemoryDeletes(store *CustomSCPStore, now time.Time) ([]customSCPCleanupDelete, bool) {
	store.dbMu.RLock()
	defer store.dbMu.RUnlock()
	return store.pruneCleanupChunk(store.observationHorizonCutoffUnix(now), store.staticHorizonCutoffUnix(now))
}

func captureCustomSCPDiskDeletes(t *testing.T, store *CustomSCPStore, now time.Time) []customSCPCleanupDelete {
	t.Helper()
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
	return scanCustomSCPCleanupChunk(iter, store.observationHorizonCutoffUnix(now), store.staticHorizonCutoffUnix(now))
}

func applyCustomSCPCleanupTestDeletes(store *CustomSCPStore, candidates []customSCPCleanupDelete) {
	store.dbMu.RLock()
	defer store.dbMu.RUnlock()
	store.applyCleanupDeletes(candidates)
}

func customSCPCleanupStaticBytes(seen int64) []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(seen))
	return value
}

func customSCPCleanupObservationBytes(seen int64, cell uint16) []byte {
	value := make([]byte, 10)
	binary.BigEndian.PutUint64(value[:8], uint64(seen))
	binary.BigEndian.PutUint16(value[8:], cell)
	return value
}

func putCustomSCPCleanupRaw(t *testing.T, store *CustomSCPStore, key string, value []byte) {
	t.Helper()
	if err := store.db.Set([]byte(key), value, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
}

func getCustomSCPCleanupRaw(t *testing.T, store *CustomSCPStore, key string) []byte {
	t.Helper()
	value, closer, err := store.db.Get([]byte(key))
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	owned := bytes.Clone(value)
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	return owned
}

func requireCustomSCPCleanupRaw(t *testing.T, store *CustomSCPStore, key string, expected []byte) {
	t.Helper()
	if got := getCustomSCPCleanupRaw(t, store, key); !bytes.Equal(got, expected) {
		t.Fatalf("raw %s: got %x want %x", key, got, expected)
	}
}

func requireCustomSCPCleanupMissing(t *testing.T, store *CustomSCPStore, key string) {
	t.Helper()
	_, closer, err := store.db.Get([]byte(key))
	if closer != nil {
		if closeErr := closer.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("expected raw %s removed, got %v", key, err)
	}
}

func assertCustomSCPCleanupCoupling(t *testing.T, store *CustomSCPStore) {
	t.Helper()
	assertCustomSCPInternerInvariant(t, store)
	store.mu.RLock()
	defer store.mu.RUnlock()
	spotters := 0
	calls := make(map[string]struct{})
	bandCalls := make(map[string]map[string]struct{})
	if len(store.entryExpiry.items) != len(store.entries) || len(store.entryExpiry.indexes) != len(store.entries) {
		t.Fatalf("entry expiry ownership diverged: items=%d indexes=%d primary=%d", len(store.entryExpiry.items), len(store.entryExpiry.indexes), len(store.entries))
	}
	for key, entry := range store.entries {
		spotters += len(entry.spotters)
		calls[key.call] = struct{}{}
		if bandCalls[key.band] == nil {
			bandCalls[key.band] = make(map[string]struct{})
		}
		bandCalls[key.band][key.call] = struct{}{}
		idx, ok := store.entryExpiry.indexes[key]
		if !ok || idx < 0 || idx >= len(store.entryExpiry.items) || store.entryExpiry.items[idx].key != key {
			t.Fatalf("missing/corrupt expiry index for %+v", key)
		}
		oldest, latest := int64(0), int64(0)
		for _, obs := range entry.spotters {
			seen := int64(obs.seenUnix)
			if oldest == 0 || seen < oldest {
				oldest = seen
			}
			if seen > latest {
				latest = seen
			}
		}
		if entry.oldestSeenUnix != oldest || entry.lastSeen != latest {
			t.Fatalf("entry ages diverged for %+v", key)
		}
		due := oldest
		if len(entry.spotters) > store.opts.MaxSpottersPerKey {
			due = customSCPImmediateCleanupDueUnix
		}
		if store.entryExpiry.items[idx].dueUnix != due {
			t.Fatalf("entry expiry due time diverged for %+v", key)
		}
	}
	if spotters != store.observationSpotters || len(calls) != store.active.Total() {
		t.Fatal("observation/active counters diverged from retained evidence")
	}
	bandCounts := store.active.CountsByBand()
	if len(bandCounts) != len(bandCalls) {
		t.Fatal("active band-count cardinality diverged")
	}
	for band, set := range bandCalls {
		if bandCounts[band] != len(set) {
			t.Fatalf("active band count diverged for %s", band)
		}
	}
	if len(store.staticExpiry.items) != len(store.static) || len(store.staticExpiry.indexes) != len(store.static) {
		t.Fatal("static expiry ownership diverged")
	}
	for call, seen := range store.static {
		idx, ok := store.staticExpiry.indexes[call]
		if !ok || idx < 0 || idx >= len(store.staticExpiry.items) || store.staticExpiry.items[idx].call != call || store.staticExpiry.items[idx].dueUnix != seen {
			t.Fatalf("missing/corrupt static expiry index for %s", call)
		}
	}
}
