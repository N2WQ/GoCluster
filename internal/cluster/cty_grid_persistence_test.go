package cluster

import (
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/gridstore"
)

// Literal grids come from the published coordinate convention and cell bounds,
// not another invocation of the conversion under test. Stop is the real worker
// completion/final-flush barrier, so a tight synchronous timeout cannot hide work.
func TestGridLookupShippedCTYPersistence(t *testing.T) {
	db := loadShippedClusterCTY(t)
	cases := []struct{ call, grid string }{
		{"K1ABC", "FN42"}, {"W6ABC", "DM05"}, {"JA1ABC", "PM95"},
		{"VE3ABC", "FN03"}, {"VK2ZZZ", "QF56"},
	}
	for _, mode := range []string{"async", "sync"} {
		t.Run(mode, func(t *testing.T) {
			path := t.TempDir()
			store, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatal(err)
			}
			cache := newCallMetaCache(64, 0)
			closed := false
			var ctyReads atomic.Int64
			_, _, _, stop, async, syncLookup := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, func() *cty.CTYDatabase {
				ctyReads.Add(1)
				return db
			})
			t.Cleanup(func() {
				stop()
				if !closed {
					_ = store.Close()
				}
			})
			lookup := async
			if mode == "sync" {
				lookup = syncLookup
			}
			for _, tc := range cases {
				if grid, derived, ok := lookup(tc.call); ok && (grid != tc.grid || !derived) {
					t.Errorf("%s returned %q/%v, want %s/derived", tc.call, grid, derived, tc.grid)
				}
			}
			stop()
			if ctyReads.Load() != int64(len(cases)) {
				t.Fatalf("cold backfill did not consume CTY: %d reads", ctyReads.Load())
			}
			for _, tc := range cases {
				grid, derived, ok := cache.LookupGrid(tc.call, nil)
				if !ok || grid != tc.grid || !derived {
					t.Errorf("%s cache = %q/%v/%v, want %s/derived", tc.call, grid, derived, ok, tc.grid)
				}
			}
			closed = true
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, tc := range cases {
				rec, err := reopened.Get(tc.call)
				if err != nil || rec == nil || !rec.Grid.Valid || rec.Grid.String != tc.grid || !rec.GridDerived {
					t.Fatalf("%s reopened = %+v/%v, want %s/derived", tc.call, rec, err, tc.grid)
				}
			}
			// A cold process must hydrate these persisted estimates, without needing
			// to recalculate them from CTY merely to recover their grid/provenance.
			restartCache := newCallMetaCache(64, 0)
			_, _, _, restartStop, restartLookup, _ := startGridWriter(newGridStoreHandle(reopened), time.Hour, restartCache, 0, true, func() *cty.CTYDatabase {
				t.Error("restart tried to recalculate a persisted grid")
				return nil
			})
			defer restartStop()
			for _, tc := range cases {
				restartLookup(tc.call)
			}
			restartStop()
			for _, tc := range cases {
				grid, derived, ok := restartCache.LookupGrid(tc.call, nil)
				if !ok || grid != tc.grid || !derived {
					t.Fatalf("restart lost %s: %q/%v/%v", tc.call, grid, derived, ok)
				}
			}
		})
	}
}
