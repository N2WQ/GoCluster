package spot

import (
	"cmp"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

func TestCustomSCPEvictionUsesNewestRetainedReceiver(t *testing.T) {
	s := newCustomSCPCleanupTestStore(t, CustomSCPOptions{MaxKeys: 2})
	now := freshCustomSCPTestTime()
	withOldReceiver := customSCPKey{call: "K1AAA", band: "20m", bucket: "cw"}
	victim := customSCPKey{call: "K1BBB", band: "20m", bucket: "cw"}
	older := now.Add(-20 * time.Hour)
	s.recordObservation(withOldReceiver.call, "20m", "CW", "N0OLD", 101, 10, true, older)
	s.recordObservation(withOldReceiver.call, "20m", "CW", "N0NEW", 202, 10, true, now)
	s.recordObservation(victim.call, "20m", "CW", "N0B", 303, 10, true, now.Add(-10*time.Hour))
	if s.entryExpiry.items[0].key != withOldReceiver {
		t.Fatal("fixture must put a different key first for expiry")
	}

	s.recordObservation("K1CCC", "20m", "CW", "N0C", 404, 10, true, older)
	requireCustomSCPCleanupMissing(t, s, observationKeyString(victim, "N0B"))
	requireCustomSCPCleanupRaw(t, s, observationKeyString(withOldReceiver, "N0OLD"), customSCPCleanupObservationBytes(older.Unix(), 101))
	requireCustomSCPCleanupRaw(t, s, "o|K1CCC|20m|cw|N0C", customSCPCleanupObservationBytes(older.Unix(), 404))
	if !s.staticContainsAt(victim.call, now) || s.StatsSnapshot().LiveKeyEvictions != 1 {
		t.Fatal("eviction lost static membership or failed to count a live victim")
	}
	assertCustomSCPEvictionIndex(t, s)
	assertCustomSCPCleanupCoupling(t, s)
}

func TestCustomSCPEvictionBreaksTiesByFullKey(t *testing.T) {
	for _, tc := range []struct {
		name, firstCall, secondCall, firstBand, secondBand, firstMode, secondMode string
		victim                                                                    customSCPKey
	}{
		{"call", "K1BBB", "K1AAA", "20m", "40m", "CW", "RTTY", customSCPKey{"K1AAA", "40m", "rtty"}},
		{"band", "K1AAA", "K1AAA", "40m", "20m", "CW", "RTTY", customSCPKey{"K1AAA", "20m", "rtty"}},
		{"mode", "K1AAA", "K1AAA", "20m", "20m", "RTTY", "CW", customSCPKey{"K1AAA", "20m", "cw"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newCustomSCPCleanupTestStore(t, CustomSCPOptions{MaxKeys: 2})
			now := freshCustomSCPTestTime()
			s.recordObservation(tc.firstCall, tc.firstBand, tc.firstMode, "N0A", 101, 10, true, now)
			s.recordObservation(tc.secondCall, tc.secondBand, tc.secondMode, "N0B", 202, 10, true, now)
			s.recordObservation("K1CCC", "20m", "CW", "N0C", 303, 10, true, now)
			if s.entries[tc.victim] != nil || len(s.entries) != 2 {
				t.Fatalf("wrong victim among tied keys: %+v", tc.victim)
			}
			requireCustomSCPCleanupMissing(t, s, observationKeyString(tc.victim, "N0B"))
			if !s.staticContainsAt(tc.victim.call, now) {
				t.Fatal("capacity removal discarded independent static membership")
			}
			assertCustomSCPEvictionIndex(t, s)
		})
	}
}

func TestCustomSCPEvictionIndexChurnAndExpiry(t *testing.T) {
	const limit = 61
	s := newCustomSCPCleanupTestStore(t, CustomSCPOptions{MaxKeys: limit})
	now := freshCustomSCPTestTime()
	rng := rand.New(rand.NewSource(1729))
	var evictions uint64
	for i := 0; i < 1500; i++ {
		call := fmt.Sprintf("K9E%03d", rng.Intn(4*limit))
		de := fmt.Sprintf("N0E%d", rng.Intn(4))
		key := customSCPKey{call, "20m", "cw"}
		victim, atCap := customSCPEvictionReferenceForTest(s, key)
		wantKeys := len(s.entries)
		if !atCap && s.entries[key] == nil {
			wantKeys++
		}
		seen := now.Add(-time.Duration(rng.Intn(22*60)) * time.Minute)
		s.recordObservation(call, "20m", "CW", de, 101, 10, true, seen)
		if atCap {
			evictions++
			if s.entries[victim] != nil {
				t.Fatalf("iteration %d retained scan oracle victim %+v", i, victim)
			}
		}
		if len(s.entries) != wantKeys || s.entries[key] == nil || s.StatsSnapshot().LiveKeyEvictions != evictions {
			t.Fatalf("iteration %d violated admission or eviction accounting", i)
		}
		if i%19 == 0 {
			s.snapshotForNormalizedCall(call, "20m", "CW", now.Add(12*time.Hour), 1)
		}
		assertCustomSCPEvictionIndex(t, s)
	}
	oldBacking := s.eviction[:cap(s.eviction)]
	s.cleanup(now.Add(26 * time.Hour))
	if stats := s.StatsSnapshot(); stats.ObservationKeys != 0 || stats.EvictionIndexItems != 0 ||
		stats.EvictionIndexSlots != 0 || stats.LiveKeyEvictions != evictions {
		t.Fatalf("expiry did not retire the index independently of capacity: %+v", stats)
	}
	for i, item := range oldBacking {
		if item != (customSCPEvictionItem{}) {
			t.Fatalf("retired slot %d still has an owner", i)
		}
	}
	assertCustomSCPCleanupCoupling(t, s)
}

func customSCPEvictionReferenceForTest(s *CustomSCPStore, incoming customSCPKey) (customSCPKey, bool) {
	if len(s.entries) < s.opts.MaxKeys || s.entries[incoming] != nil {
		return customSCPKey{}, false
	}
	var victim customSCPKey
	var oldest int64
	found := false
	for key, entry := range s.entries {
		if !found || compareCustomSCPEvictionForTest(key, entry.lastSeen, victim, oldest) < 0 {
			victim, oldest, found = key, entry.lastSeen, true
		}
	}
	return victim, found
}

func compareCustomSCPEvictionForTest(a customSCPKey, ageA int64, b customSCPKey, ageB int64) int {
	return cmp.Or(cmp.Compare(ageA, ageB), cmp.Compare(a.call, b.call),
		cmp.Compare(a.band, b.band), cmp.Compare(a.bucket, b.bucket))
}

func assertCustomSCPEvictionIndex(t *testing.T, s *CustomSCPStore) {
	t.Helper()
	h := s.eviction
	if len(h) != len(s.entries) || cap(h) > s.opts.MaxKeys {
		t.Fatalf("heap size/backing %d/%d, primary %d, configured maximum %d", len(h), cap(h), len(s.entries), s.opts.MaxKeys)
	}
	for key, entry := range s.entries {
		i := entry.evictionSlot - 1
		if i < 0 || i >= len(h) || h[i].key != key || h[i].entry != entry || h[i].lastSeen != entry.lastSeen {
			t.Fatalf("wrong index owner or age for %+v", key)
		}
	}
	for i := 1; i < len(h); i++ {
		parent := h[(i-1)/2]
		if compareCustomSCPEvictionForTest(h[i].key, h[i].lastSeen, parent.key, parent.lastSeen) < 0 {
			t.Fatalf("child %d outranks parent", i)
		}
	}
	backing := h[:cap(h)]
	for _, item := range backing[len(h):] {
		if item != (customSCPEvictionItem{}) {
			t.Fatal("unused heap slot retains a key or entry")
		}
	}
}

func TestCustomSCPEvictionReopenUsesSequentialAdmission(t *testing.T) {
	s := newCustomSCPCleanupTestStore(t, CustomSCPOptions{MaxKeys: 3})
	now := freshCustomSCPTestTime()
	s.recordObservation("K1AAA", "20m", "CW", "N0A", 101, 10, true, now)
	s.recordObservation("K1BBB", "20m", "CW", "N0B", 202, 10, true, now.Add(-time.Hour))
	s.recordObservation("K1CCC", "20m", "CW", "N0C", 303, 10, true, now.Add(-2*time.Hour))
	s.opts.MaxKeys = 2
	reopened := reopenCustomSCPCleanupTestStore(t, s)
	requireCustomSCPCleanupMissing(t, reopened, "o|K1BBB|20m|cw|N0B")
	requireCustomSCPCleanupRaw(t, reopened, "o|K1AAA|20m|cw|N0A", customSCPCleanupObservationBytes(now.Unix(), 101))
	requireCustomSCPCleanupRaw(t, reopened, "o|K1CCC|20m|cw|N0C", customSCPCleanupObservationBytes(now.Add(-2*time.Hour).Unix(), 303))
	if stats := reopened.StatsSnapshot(); stats.LiveKeyEvictions != 0 || !reopened.staticContainsAt("K1BBB", now) {
		t.Fatalf("reopen counted startup eviction as live or lost static membership: %+v", stats)
	}
	assertCustomSCPEvictionIndex(t, reopened)
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPEvictionReopenReplacesNormalizedOwner(t *testing.T) {
	s := newCustomSCPCleanupTestStore(t, CustomSCPOptions{MaxKeys: 2})
	now := freshCustomSCPTestTime()
	putCustomSCPCleanupRaw(t, s, "o|K1AAA|20m|cw|N0A", customSCPCleanupObservationBytes(now.Add(-time.Hour).Unix(), 101))
	putCustomSCPCleanupRaw(t, s, "o|K1BBB|20m|cw|N0B", customSCPCleanupObservationBytes(now.Add(-2*time.Hour).Unix(), 202))
	putCustomSCPCleanupRaw(t, s, "o|k1aaa|20m|cw|N0C", customSCPCleanupObservationBytes(now.Unix(), 303))
	reopened := reopenCustomSCPCleanupTestStore(t, s)
	entry := reopened.entries[customSCPKey{"K1AAA", "20m", "cw"}]
	if len(reopened.entries) != 1 || !entryHasSpotter(entry, "N0C") || entryHasSpotter(entry, "N0A") {
		t.Fatal("expected the later raw group to own the normalized entry")
	}
	requireCustomSCPCleanupMissing(t, reopened, "o|K1BBB|20m|cw|N0B")
	assertCustomSCPEvictionIndex(t, reopened)
}

func TestCustomSCPOpenCacheLockedPathCanRetry(t *testing.T) {
	opts := CustomSCPOptions{Path: filepath.Join(t.TempDir(), "scp"), CacheSizeBytes: 1 << 20}
	first, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenCustomSCPStore(opts)
	if err == nil {
		_ = second.Close()
		t.Fatal("concurrent writable open unexpectedly succeeded")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomSCPEvictionGrowthClampsBacking(t *testing.T) {
	const maxKeys = 2051
	var index customSCPEvictionHeap
	for i := range maxKeys {
		key := customSCPKey{call: fmt.Sprintf("K8G%04d", i), band: "20m", bucket: "cw"}
		index.upsert(key, &customSCPEntry{lastSeen: int64(i + 100)}, maxKeys)
		if cap(index) > maxKeys {
			t.Fatalf("backing exceeds cap at key %d", i)
		}
	}
	if len(index) != maxKeys || cap(index) != maxKeys {
		t.Fatalf("final length/capacity = %d/%d", len(index), cap(index))
	}

	// Exercise decreasing and increasing ranks on the very same heap slot.
	last := index[maxKeys-1]
	last.entry.lastSeen = 50
	index.upsert(last.key, last.entry, maxKeys)
	if index[0].entry != last.entry || last.entry.evictionSlot != 1 {
		t.Fatal("a decreased rank did not move its owner to the root")
	}
	last.entry.lastSeen = 10000
	index.upsert(last.key, last.entry, maxKeys)
	if index[0].key.call != "K8G0000" || last.entry.evictionSlot == 1 {
		t.Fatal("an increased rank did not retire the old root rank")
	}
}
