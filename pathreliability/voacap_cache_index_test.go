package pathreliability

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestVOACAPCacheIndexRefreshReusesOwnedNodes(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	for cell := CellID(1); cell <= 4; cell++ {
		if err := store.storeForecast(testVOACAPForecastCacheKey(now, cell), incrementalCacheEntry(now, now, 20, 22), cfg, now); err != nil {
			t.Fatal(err)
		}
	}
	const refreshedKey = "voacapfc/v1/e/0001/0002/20m/014100/2026/06/0112/bidirectional"
	store.mu.Lock()
	original := store.index[refreshedKey]
	store.mu.Unlock()
	if original == nil {
		t.Fatal("literal canonical key missing from initial index")
	}
	for iteration := 1; iteration <= 128; iteration++ {
		at := now.Add(time.Duration(iteration) * time.Millisecond)
		if err := store.storeForecast(testVOACAPForecastCacheKey(now, 2), incrementalCacheEntry(now, at, 20, 21, 23), cfg, at); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		same := store.index[refreshedKey] == original
		store.mu.Unlock()
		if !same {
			t.Fatalf("refresh %d replaced the owned node", iteration)
		}
		assertVOACAPCacheIndexBijection(t, store, cfg.MaxCacheEntries)
	}
	bad := incrementalCacheEntry(now, now.Add(time.Second))
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 2), bad, cfg, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	detached := original.retentionIndex == -1 && original.eligibilityIndex == -1 && store.index[refreshedKey] == nil
	store.mu.Unlock()
	if !detached {
		t.Fatal("deleted owner left a node in a heap or map")
	}
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 2), incrementalCacheEntry(now, now.Add(2*time.Second), 20), cfg, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	replacement := store.index[refreshedKey]
	store.mu.Unlock()
	if replacement == nil || replacement == original {
		t.Fatal("reinsert must establish a newly owned node after deletion")
	}
	assertVOACAPCacheIndexBijection(t, store, cfg.MaxCacheEntries)
	assertIncrementalRawCells(t, store, []uint16{1, 2, 3, 4})
}

func TestVOACAPCacheIndexCapacityTTLAndReinsertChurn(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries, cfg.CacheTTLSeconds = 7, 10
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	want := make(map[uint16]time.Time)
	for iteration := 0; iteration < 224; iteration++ {
		at := now.Add(time.Duration(iteration/14) * 2 * time.Second)
		cell := uint16(iteration%31 + 1)
		if iteration%4 == 0 {
			cell = 2 // A popular key refreshes among genuinely new insertions.
		}
		storedAt := at.Add(-time.Duration(iteration%3) * time.Second)
		entry := incrementalCacheEntry(now, storedAt, 20, 22)
		if iteration%17 == 0 {
			entry.forecast.Records = nil
		}
		if err := store.storeForecast(testVOACAPForecastCacheKey(now, CellID(cell)), entry, cfg, at); err != nil {
			t.Fatalf("operation %d: %v", iteration, err)
		}
		// Small independent brute-force model: expire all owners, replace the
		// named key, then choose the seven newest (numeric cell order breaks
		// equal timestamps because all other literal canonical fields agree).
		for retained, timestamp := range want {
			if at.Sub(timestamp) > 10*time.Second {
				delete(want, retained)
			}
		}
		if len(entry.forecast.Records) == 0 {
			delete(want, cell)
		} else {
			want[cell] = storedAt
		}
		if len(want) > 7 {
			var victim uint16
			var oldest time.Time
			for candidate, timestamp := range want {
				if victim == 0 || timestamp.Before(oldest) || timestamp.Equal(oldest) && candidate > victim {
					victim, oldest = candidate, timestamp
				}
			}
			delete(want, victim)
		}
		cells := make([]uint16, 0, len(want))
		for retained := range want {
			cells = append(cells, retained)
		}
		slices.Sort(cells)
		assertIncrementalRawCells(t, store, cells)
		assertVOACAPCacheIndexBijection(t, store, 7)
	}
}

func TestVOACAPCacheIndexOrdinaryWriteDoesNotScan(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 64
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	for cell := CellID(1); cell <= 64; cell++ {
		if err := store.storeForecast(testVOACAPForecastCacheKey(now, cell), incrementalCacheEntry(now, now, 20, 21, 22), cfg, now); err != nil {
			t.Fatal(err)
		}
	}
	var iterations, scanSteps int
	injected := errors.New("ordinary write attempted an unrelated DB scan")
	incrementalSetFaults(store, &voacapForecastCacheFaults{
		newIter: func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error) {
			iterations++
			return nil, injected
		},
		scanStep: func(int) error { scanSteps++; return injected },
	})
	for iteration := 1; iteration <= 32; iteration++ {
		at := now.Add(time.Duration(iteration) * time.Millisecond)
		if err := store.storeForecast(testVOACAPForecastCacheKey(now, 32), incrementalCacheEntry(now, at, 20, 21, 22), cfg, at); err != nil {
			t.Fatalf("ordinary refresh %d scanned unrelated records: %v", iteration, err)
		}
	}
	at := now.Add(time.Second)
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 65), incrementalCacheEntry(now, at, 20, 21, 22), cfg, at); err != nil {
		t.Fatalf("ordinary capacity eviction scanned unrelated records: %v", err)
	}
	if iterations != 0 || scanSteps != 0 {
		t.Fatalf("ordinary refresh/eviction scan work: iterators=%d decoded steps=%d", iterations, scanSteps)
	}
	assertVOACAPCacheIndexBijection(t, store, 64)
	if records := readRawVOACAPRetentionRecords(t, store); len(records) != 64 {
		t.Fatalf("capacity result before repair has %d entries", len(records))
	} else {
		const evicted = "voacapfc/v1/e/0001/0040/20m/014100/2026/06/0112/bidirectional"
		const inserted = "voacapfc/v1/e/0001/0041/20m/014100/2026/06/0112/bidirectional"
		if _, found := records[evicted]; found {
			t.Fatal("equal-time largest canonical victim was retained")
		}
		if _, found := records[inserted]; !found {
			t.Fatal("newer incoming record missing before repair")
		}
	}
}

func TestVOACAPCacheIndexWholePopulationHourCertificate(t *testing.T) {
	start := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries, cfg.CacheTTLSeconds, cfg.ForecastHours = 64, 6*3600, 4
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	for cell := CellID(1); cell <= 63; cell++ {
		if err := store.storeForecast(testVOACAPForecastCacheKey(start, cell), incrementalCacheEntry(start, start, 20, 22), cfg, start); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	// Every owner has exactly the two actual hours, with no inferred hour 21.
	root := store.eligibility[0]
	mask, minStart, maxStart := root.subtreeHourMask, root.subtreeMinStart, root.subtreeMaxStart
	store.mu.Unlock()
	if mask != 0x00500000 || minStart != start.UnixNano() || maxStart != start.UnixNano() {
		t.Fatalf("whole-population certificate: mask=%08x starts=(%d,%d)", mask, minStart, maxStart)
	}
	incrementalSetFaults(store, &voacapForecastCacheFaults{newIter: func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error) {
		return nil, errors.New("direct 20-to-22 operation must not scan DB")
	}})
	at := start.Add(2 * time.Hour)
	if err := store.storeForecast(testVOACAPForecastCacheKey(at, 64), incrementalCacheEntry(at, at, 22), cfg, at); err != nil {
		t.Fatal(err)
	}
	if records := readRawVOACAPRetentionRecords(t, store); len(records) != 64 {
		t.Fatalf("direct hour jump lost qualifying owners: %d records", len(records))
	}
	assertVOACAPCacheIndexBijection(t, store, 64)
}

func TestVOACAPCacheIndexBoundedHydrationAndPruneFailure(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 8
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed_prune_applied=%t", applied), func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			// More than two prune chunks and far more candidates than capacity.
			// Every entry is valid; lower canonical keys must survive equal times.
			for cell := CellID(1); cell <= 600; cell++ {
				writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, cell), incrementalCacheEntry(now, now, 20), nil)
			}
			injected := errors.New("second bounded prune commit failed")
			var commits, visited, candidatePeak int
			incrementalSetFaults(store, &voacapForecastCacheFaults{
				scanStep: func(processed int) error {
					visited = processed
					candidatePeak = max(candidatePeak, len(store.index), len(store.retention))
					if len(store.index) > 8 || len(store.retention) > 8 {
						return fmt.Errorf("unbounded scan candidates: map=%d heap=%d", len(store.index), len(store.retention))
					}
					return nil
				},
				commit: func(batch *pebble.Batch, opts *pebble.WriteOptions) error {
					commits++
					if batch.Count() > 256 {
						return fmt.Errorf("prune chunk has %d mutations, approved bound 256", batch.Count())
					}
					if commits == 2 {
						if applied {
							if err := batch.Commit(opts); err != nil {
								return err
							}
						}
						return injected
					}
					return batch.Commit(opts)
				},
			})
			loaded, _, err := store.loadCurrent(now, 112, cfg)
			if !errors.Is(err, injected) || len(loaded) != 0 || commits != 2 || visited <= 256 || candidatePeak != 8 {
				t.Fatalf("bounded failed hydration: loaded=%d commits=%d visited=%d peak=%d error=%v", len(loaded), commits, visited, candidatePeak, err)
			}
			if snap := store.IndexSnapshot(); snap.Ready || !snap.Dirty || snap.Entries != 0 || snap.RetentionEntries != 0 || snap.EligibilityEntries != 0 {
				t.Fatalf("failed chunk published or retained candidate ownership: %+v", snap)
			}
			// Witness real partial disk effects before recovery, including a
			// successfully applied second batch whose caller received an error.
			wantCount := 600 - 256
			if applied {
				wantCount -= 256
			}
			if keys := incrementalRawCacheKeys(t, store); len(keys) != wantCount {
				t.Fatalf("raw partial-prune size=%d, want %d before recovery", len(keys), wantCount)
			}
			incrementalSetFaults(store, nil)
			if err := store.storeForecast(testVOACAPForecastCacheKey(now, 601), incrementalCacheEntry(now, now, 20), cfg, now); err != nil {
				t.Fatalf("completion after failed prune must recover actual DB state: %v", err)
			}
			assertIncrementalRawCells(t, store, []uint16{1, 2, 3, 4, 5, 6, 7, 8})
			assertVOACAPCacheIndexBijection(t, store, 8)
			reopened := reopenVOACAPRetentionStore(t, store)
			assertIncrementalRawCells(t, reopened, []uint16{1, 2, 3, 4, 5, 6, 7, 8})
		})
	}
}

func TestVOACAPCacheIndexConcurrentLoadWriteAndSnapshot(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 16
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	if _, _, err := store.loadCurrent(now, 112, cfg); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsSeen := make(chan error, 8)
	var group sync.WaitGroup
	for writer := 0; writer < 3; writer++ {
		group.Add(1)
		go func(writer int) {
			defer group.Done()
			<-start
			for iteration := 0; iteration < 32; iteration++ {
				cell := CellID(writer*32 + iteration + 1)
				if err := store.storeForecast(testVOACAPForecastCacheKey(now, cell), incrementalCacheEntry(now, now, 20, 22), cfg, now); err != nil {
					errorsSeen <- err
					return
				}
			}
		}(writer)
	}
	group.Add(1)
	go func() {
		defer group.Done()
		<-start
		for iteration := 0; iteration < 32; iteration++ {
			if _, _, err := store.loadCurrent(now, 112, cfg); err != nil {
				errorsSeen <- err
				return
			}
		}
	}()
	group.Add(1)
	go func() {
		defer group.Done()
		<-start
		for iteration := 0; iteration < 1024; iteration++ {
			snap := store.IndexSnapshot()
			if snap.Ready && (snap.Dirty || snap.Entries > 16 || snap.RetentionEntries != snap.Entries || snap.EligibilityEntries != snap.Entries) {
				errorsSeen <- fmt.Errorf("inconsistent cached snapshot: %+v", snap)
				return
			}
		}
	}()
	close(start)
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	assertIncrementalRawCells(t, store, []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	assertVOACAPCacheIndexBijection(t, store, 16)
}

func TestVOACAPCacheIndexConcurrentCloseIsSerialized(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 1), incrementalCacheEntry(now, now, 20), cfg, now); err != nil {
		t.Fatal(err)
	}
	start, firstOperation := make(chan struct{}), make(chan struct{})
	var signalOnce sync.Once
	var snapshots atomic.Int64
	errorsSeen := make(chan error, 4)
	var group sync.WaitGroup
	group.Add(3)
	go func() {
		defer group.Done()
		<-start
		for iteration := 0; iteration < 64; iteration++ {
			if err := store.storeForecast(testVOACAPForecastCacheKey(now, CellID(iteration%8+1)), incrementalCacheEntry(now, now, 20), cfg, now); err != nil {
				errorsSeen <- err
				return
			}
			signalOnce.Do(func() { close(firstOperation) })
		}
	}()
	go func() {
		defer group.Done()
		<-start
		for iteration := 0; iteration < 64; iteration++ {
			if _, _, err := store.loadCurrent(now, 112, cfg); err != nil {
				errorsSeen <- err
				return
			}
			signalOnce.Do(func() { close(firstOperation) })
			store.IndexSnapshot()
			snapshots.Add(1)
		}
	}()
	go func() {
		defer group.Done()
		<-start
		<-firstOperation
		if err := store.Close(); err != nil {
			errorsSeen <- err
		}
	}()
	close(start)
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	if snapshots.Load() != 64 {
		t.Fatalf("snapshot stimuli=%d, want 64 including closed-state operations", snapshots.Load())
	}
	if snap := store.IndexSnapshot(); snap.Attached || snap.Ready || snap.Entries != 0 || snap.RetentionEntries != 0 || snap.EligibilityEntries != 0 {
		t.Fatalf("closed store retained published ownership: %+v", snap)
	}
}

// Independently checks the owner/map/heap bijection and both heap orderings.
// The subtree oracle walks descendants, rather than reproducing the incremental
// aggregate-update algorithm that is being tested.
func assertVOACAPCacheIndexBijection(t *testing.T, store *VOACAPForecastCacheStore, capacity int) {
	t.Helper()
	records := readRawVOACAPRetentionRecords(t, store)
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.index) != len(records) || len(store.retention) != len(store.index) || len(store.eligibility) != len(store.index) || len(store.index) > capacity {
		t.Fatalf("owner/map/heap sizes: disk=%d map=%d retention=%d eligibility=%d capacity=%d", len(records), len(store.index), len(store.retention), len(store.eligibility), capacity)
	}
	seenRetention := make(map[*voacapForecastCacheIndexEntry]bool, len(store.index))
	seenEligibility := make(map[*voacapForecastCacheIndexEntry]bool, len(store.index))
	for position, node := range store.retention {
		if node == nil || node.retentionIndex != position || store.index[node.dbKey] != node || seenRetention[node] {
			t.Fatalf("retention node %d is nil, duplicated, orphaned or mispositioned", position)
		}
		seenRetention[node] = true
		if position > 0 {
			parent := store.retention[(position-1)/2]
			if parent.storedAtNS > node.storedAtNS || parent.storedAtNS == node.storedAtNS && parent.dbKey < node.dbKey {
				t.Fatalf("retention victim ordering broken at %d", position)
			}
		}
	}
	for position, node := range store.eligibility {
		if node == nil || node.eligibilityIndex != position || store.index[node.dbKey] != node || seenEligibility[node] {
			t.Fatalf("eligibility node %d is nil, duplicated, orphaned or mispositioned", position)
		}
		seenEligibility[node] = true
		if position > 0 && store.eligibility[(position-1)/2].windowStartNS > node.windowStartNS {
			t.Fatalf("eligibility window ordering broken at %d", position)
		}
		var mask uint32 = 0x00ffffff
		minStart, maxStart := int64(1<<63-1), int64(-1<<63)
		pending := []int{position}
		for len(pending) > 0 {
			at := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			descendant := store.eligibility[at]
			mask &= descendant.hourMask
			minStart = min(minStart, descendant.windowStartNS)
			maxStart = max(maxStart, descendant.windowStartNS)
			for _, child := range []int{2*at + 1, 2*at + 2} {
				if child < len(store.eligibility) {
					pending = append(pending, child)
				}
			}
		}
		if node.subtreeHourMask != mask || node.subtreeMinStart != minStart || node.subtreeMaxStart != maxStart {
			t.Fatalf("eligibility subtree %d aggregate=(%08x,%d,%d), independently scanned=(%08x,%d,%d)", position, node.subtreeHourMask, node.subtreeMinStart, node.subtreeMaxStart, mask, minStart, maxStart)
		}
	}
	for key, node := range store.index {
		record, found := records[key]
		if !found || node.dbKey != key || !seenRetention[node] || !seenEligibility[node] || node.storedAtNS != record.StoredAtUnixNS || node.windowStartNS != record.WindowUnixNS {
			t.Fatalf("index owner %q does not have exact disk/map/heap metadata", key)
		}
		var wantMask uint32
		for _, record := range record.Records {
			if record.HourUTC >= 0 && record.HourUTC <= 23 {
				wantMask |= 1 << uint(record.HourUTC)
			}
		}
		if node.hourMask != wantMask {
			t.Fatalf("index owner %q hour mask=%08x want=%08x", key, node.hourMask, wantMask)
		}
	}
}
