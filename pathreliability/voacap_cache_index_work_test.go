package pathreliability

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestVOACAPCacheIndexFiftyThousandHourCertificateSkipsDescendants(t *testing.T) {
	start := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries, cfg.CacheTTLSeconds, cfg.ForecastHours = 50000, 6*3600, 4
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	writeVOACAPCacheIndexRawPopulation(t, store, start, 50000)
	if keys := incrementalRawCacheKeys(t, store); len(keys) != 50000 {
		t.Fatalf("raw population=%d, want 50,000 before hydration", len(keys))
	}
	loaded, stats, err := store.loadCurrent(start, 112, cfg)
	if err != nil || len(loaded) != 50000 || stats.Loaded != 50000 || stats.Pruned != 0 {
		t.Fatalf("50,000-owner hydration: loaded=%d stats=%+v error=%v", len(loaded), stats, err)
	}
	assertVOACAPCacheIndexBijection(t, store, 50000)
	at := start.Add(2 * time.Hour)
	func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		root := store.eligibility[0]
		if root.subtreeHourMask != 0x00500000 || root.subtreeMinStart != start.UnixNano() || root.subtreeMaxStart != start.UnixNano() {
			t.Fatalf("50,000-owner certificate was not independently established: %+v", root)
		}
		// A certified root must return without reading any descendant. Poison
		// the pointer slots only for the search, and restore the exact heap
		// before any mutation. No production instrumentation is needed.
		saved := append(voacapForecastCacheEligibilityHeap(nil), store.eligibility...)
		defer copy(store.eligibility, saved)
		for position := 1; position < len(store.eligibility); position++ {
			store.eligibility[position] = nil
		}
		if victim := store.eligibility.findInvalid(at, 4); victim != nil {
			t.Fatalf("certified 50,000-owner root returned victim %q", victim.dbKey)
		}
	}()
	incrementalSetFaults(store, &voacapForecastCacheFaults{newIter: func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error) {
		return nil, errors.New("50,000-owner direct hour jump attempted DB traversal")
	}})
	if err := store.storeForecast(testVOACAPForecastCacheKey(at, 1), incrementalCacheEntry(start, at, 20, 22), cfg, at); err != nil {
		t.Fatalf("ordinary direct 20-to-22 refresh: %v", err)
	}
	assertIncrementalIndexReady(t, store, 50000)
	assertVOACAPCacheIndexBijection(t, store, 50000)
	if keys := incrementalRawCacheKeys(t, store); len(keys) != 50000 {
		t.Fatalf("ordinary direct jump retained %d owners, want 50,000 before repair", len(keys))
	}
}

func TestVOACAPCacheIndexMixedInvalidSkipsCertifiedSubtrees(t *testing.T) {
	start := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries, cfg.CacheTTLSeconds, cfg.ForecastHours = 32, 6*3600, 4
	for _, test := range []struct {
		name string
		cell CellID
		key  string
	}{
		{"left_only_bad16", 16, "voacapfc/v1/e/0001/0010/20m/014100/2026/06/0112/bidirectional"},
		{"right_only_bad31", 31, "voacapfc/v1/e/0001/001f/20m/014100/2026/06/0112/bidirectional"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			for cell := CellID(1); cell <= 31; cell++ {
				hours := []int{20, 22}
				if cell == test.cell {
					hours = []int{20}
				}
				if err := store.storeForecast(testVOACAPForecastCacheKey(start, cell), incrementalCacheEntry(start, start, hours...), cfg, start); err != nil {
					t.Fatal(err)
				}
			}
			assertVOACAPCacheIndexBijection(t, store, 32)
			at := start.Add(2 * time.Hour)
			func() {
				store.mu.Lock()
				defer store.mu.Unlock()
				bad := store.index[test.key]
				if bad == nil || bad.hourMask != 0x00100000 || store.eligibility[0].subtreeHourMask != 0x00100000 {
					t.Fatal("unique actual-hour victim and uncertified root were not established")
				}
				path := make(map[int]bool)
				for position := bad.eligibilityIndex; ; position = (position - 1) / 2 {
					path[position] = true
					if position == 0 {
						break
					}
					if test.cell == 16 && position%2 != 1 || test.cell == 31 && position%2 != 0 {
						t.Fatalf("fixture did not establish %s heap search path at position %d", test.name, position)
					}
				}
				keep := make(map[int]bool)
				for position := range path {
					keep[position] = true
					for _, child := range []int{2*position + 1, 2*position + 2} {
						if child < len(store.eligibility) {
							keep[child] = true
							if !path[child] && store.eligibility[child].subtreeHourMask != 0x00500000 {
								t.Fatalf("sibling root %d is not certified for actual hour 22", child)
							}
						}
					}
				}
				saved := append(voacapForecastCacheEligibilityHeap(nil), store.eligibility...)
				defer copy(store.eligibility, saved)
				poisoned := 0
				for position := range store.eligibility {
					if !keep[position] {
						store.eligibility[position] = nil
						poisoned++
					}
				}
				if poisoned == 0 {
					t.Fatal("fixture did not poison certified sibling descendants")
				}
				if victim := store.eligibility.findInvalid(at, 4); victim != bad {
					t.Fatalf("indexed search returned %v, want the sole exact missing-hour owner", victim)
				}
			}()
			incrementalSetFaults(store, &voacapForecastCacheFaults{newIter: func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error) {
				return nil, errors.New("one invalid owner required a full DB scan")
			}})
			if err := store.storeForecast(testVOACAPForecastCacheKey(at, 32), incrementalCacheEntry(at, at, 22), cfg, at); err != nil {
				t.Fatalf("prune sole invalid owner: %v", err)
			}
			want := make([]uint16, 0, 31)
			for cell := uint16(1); cell <= 31; cell++ {
				if cell != uint16(test.cell) {
					want = append(want, cell)
				}
			}
			want = append(want, 32)
			assertIncrementalRawCells(t, store, want)
			assertVOACAPCacheIndexBijection(t, store, 32)
		})
	}
}

func TestVOACAPCacheIndexVariedWindowSiftsMaintainAggregates(t *testing.T) {
	start := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries, cfg.CacheTTLSeconds, cfg.ForecastHours = 32, 6*3600, 6
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	hourSets := [][]int{{20}, {20, 21, 22}, {20, 23}, {20, 21}, {20, 22, 23}}
	for cell := CellID(1); cell <= 31; cell++ {
		window := start.Add(-time.Duration(int(cell)%3) * time.Hour)
		if err := store.storeForecast(testVOACAPForecastCacheKey(start, cell), incrementalCacheEntry(window, start, hourSets[int(cell)%5]...), cfg, start); err != nil {
			t.Fatal(err)
		}
	}
	assertVOACAPCacheIndexBijection(t, store, 32)
	for iteration := 1; iteration <= 96; iteration++ {
		cell := CellID(1 + iteration*11%31)
		at := start.Add(time.Duration(iteration) * time.Millisecond)
		window := start.Add(-time.Duration(iteration*7%3) * time.Hour)
		if err := store.storeForecast(testVOACAPForecastCacheKey(start, cell), incrementalCacheEntry(window, at, hourSets[iteration%5]...), cfg, at); err != nil {
			t.Fatalf("varied-window refresh %d: %v", iteration, err)
		}
		assertVOACAPCacheIndexBijection(t, store, 32)
		store.mu.Lock()
		currentVictim := store.eligibility.findInvalid(at, 6)
		futureVictim := store.eligibility.findInvalid(start.Add(time.Hour), 6)
		currentOK := currentVictim == nil
		futureOK := futureVictim != nil && futureVictim.hourMask&0x00200000 == 0
		store.mu.Unlock()
		if !currentOK || !futureOK {
			t.Fatalf("actual-hour search after sift %d: current-valid=%t future-missing-hour-victim=%t", iteration, currentOK, futureOK)
		}
	}
}

func writeVOACAPCacheIndexRawPopulation(t *testing.T, store *VOACAPForecastCacheStore, now time.Time, count int) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	batch := store.db.NewBatch()
	defer func() { _ = batch.Close() }()
	for cell := 1; cell <= count; cell++ {
		key := fmt.Sprintf("voacapfc/v1/e/0001/%04x/20m/014100/2026/06/0112/bidirectional", cell)
		value := fmt.Sprintf(`{"schema_version":1,"model_generation":1,"key":{"user_cell":1,"dx_cell":%d,"band":"20m","frequency_khz":14100,"year":2026,"month":6,"ssn":112,"direction":"bidirectional"},"stored_at_unix_ns":%d,"window_unix_ns":%d,"records":[{"HourUTC":20,"FrequencyMHz":14.1,"FT8SNRDB":-14},{"HourUTC":22,"FrequencyMHz":14.1,"FT8SNRDB":-14}]}`, cell, now.UnixNano(), now.UnixNano())
		if err := batch.Set([]byte(key), []byte(value), nil); err != nil {
			t.Fatalf("raw population cell %d: %v", cell, err)
		}
		if cell%512 == 0 || cell == count {
			if err := batch.Commit(pebble.NoSync); err != nil {
				t.Fatalf("raw population commit: %v", err)
			}
			batch.Reset()
		}
	}
}
