package cluster

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/gridstore"
)

func cacheCTYSnapshot(tb testing.TB, country string) *cty.CTYDatabase {
	tb.Helper()
	rows := ""
	if country != "" {
		rows = fmt.Sprintf(`<key>K1</key><dict><key>Country</key><string>%s</string><key>ADIF</key><integer>291</integer><key>CQZone</key><integer>5</integer><key>ITUZone</key><integer>8</integer><key>Latitude</key><real>42.38</real><key>Longitude</key><real>71.65</real></dict>`, country)
	}
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<plist version="1.0"><dict>` + rows + `</dict></plist>`))
	if err != nil {
		tb.Fatal(err)
	}
	return db
}

func assertActiveCTY(t *testing.T, info *cty.PrefixInfo, ok bool, country string) {
	t.Helper()
	if country == "" {
		if ok || info != nil {
			t.Fatalf("active miss replaced by %+v/%v", info, ok)
		}
		return
	}
	if !ok || info == nil || info.Country != country || info.ADIF != 291 || info.Latitude != 42.38 || info.Longitude != 71.65 {
		t.Fatalf("active %s classification/coordinates lost: %+v/%v", country, info, ok)
	}
}

func TestCTYCacheUsesSnapshot(t *testing.T) {
	for _, tc := range []struct{ before, after string }{{"Alpha", "Beta"}, {"Alpha", ""}, {"", "Beta"}} {
		t.Run(tc.before+"_to_"+tc.after, func(t *testing.T) {
			cache := newCallMetaCache(32, 0)
			cache.UpdateGrid("K1ABC", "FN31", false)
			for _, country := range []string{tc.before, tc.after} {
				db := cacheCTYSnapshot(t, country)
				for i := 0; i < 2; i++ {
					info, ok, cached := cache.LookupCTY("K1ABC", db)
					assertActiveCTY(t, info, ok, country)
					if cached != (i == 1) {
						t.Fatalf("lookup %d cached=%v, want %v", i, cached, i == 1)
					}
				}
			}
			if grid, derived, ok := cache.LookupGrid("K1ABC", nil); !ok || grid != "FN31" || derived {
				t.Fatalf("classification refresh changed reported grid: %s/%v/%v", grid, derived, ok)
			}
		})
	}
}

// This controls the actual production publication boundary after resolution.
// Neither shard scheduling nor repeated stress can force the critical ordering.
func TestCTYCacheLateSnapshotPublication(t *testing.T) {
	for _, tc := range []struct{ name, old, fresh string }{
		{"positive_to_negative", "Alpha", ""},
		{"negative_to_positive", "", "Beta"},
		{"positive_to_positive", "Alpha", "Beta"},
	} {
		for _, clear := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clear=%v", tc.name, clear), func(t *testing.T) {
				cache := newCallMetaCache(32, 0)
				oldDB, freshDB := cacheCTYSnapshot(t, tc.old), cacheCTYSnapshot(t, tc.fresh)
				resolved, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				go func() {
					defer close(done)
					source := oldDB.SnapshotIdentity()
					info, ok := oldDB.LookupCallsignPortable("K1ABC")
					close(resolved)
					<-release
					cache.storeCTYResult("K1ABC", source, info, ok)
				}()
				t.Cleanup(func() { unblock(); waitGridRetirement(t, done) })
				waitGridRetirement(t, resolved)
				if clear {
					cache.Clear() // the scheduler publishes a new database then clears
				}
				info, ok, _ := cache.LookupCTY("K1ABC", freshDB)
				assertActiveCTY(t, info, ok, tc.fresh)
				unblock()
				waitGridRetirement(t, done)
				for i := 0; i < 2; i++ {
					info, ok, cached := cache.LookupCTY("K1ABC", freshDB)
					assertActiveCTY(t, info, ok, tc.fresh)
					if cached != (i == 1) {
						t.Fatalf("late old publication reused by fresh caller: iteration=%d cached=%v", i, cached)
					}
				}
			})
		}
	}
}

func TestCTYCacheWarmAllocationsAndChurn(t *testing.T) {
	for _, country := range []string{"Alpha", ""} {
		db := cacheCTYSnapshot(t, country)
		cache := newCallMetaCache(32, 0)
		cache.LookupCTY("K1ABC", db)
		if allocs := testing.AllocsPerRun(1000, func() {
			if _, _, cached := cache.LookupCTY("K1ABC", db); !cached {
				t.Fatal("warm cache unexpectedly missed")
			}
		}); allocs != 0 {
			t.Fatalf("warm %q classification allocated %v times", country, allocs)
		}
	}
	cache := newCallMetaCache(32, 0)
	for i := 0; i < 512; i++ {
		// Every publication has an independent identity; eviction must release
		// its entry's token instead of accumulating a history of publications.
		db := cacheCTYSnapshot(t, "Alpha")
		cache.LookupCTY(fmt.Sprintf("K1ZXAA%04d", i), db)
		if cache.EntryCount() > cache.capacity {
			t.Fatal("snapshot churn exceeded existing cache bound")
		}
	}
	for i := range cache.shards {
		shard := &cache.shards[i]
		shard.mu.Lock()
		if len(shard.entries) > shard.max || shard.order.Len() != len(shard.entries) {
			t.Errorf("shard %d retained uncoupled entries", i)
		}
		shard.mu.Unlock()
	}
	cache.Clear()
	if cache.EntryCount() != 0 {
		t.Fatal("clear retained snapshot entries")
	}
}

func TestApplyRecordNeverOwnsCTY(t *testing.T) {
	for _, derived := range []bool{false, true} {
		for _, state := range []string{"cold", "positive", "negative"} {
			t.Run(fmt.Sprintf("%s/derived=%v", state, derived), func(t *testing.T) {
				country := "Alpha"
				if state == "negative" {
					country = ""
				}
				db := cacheCTYSnapshot(t, country)
				cache := newCallMetaCache(32, 0)
				if state != "cold" {
					cache.LookupCTY("K1ABC", db)
				}
				cache.ApplyRecord(gridstore.Record{Call: "K1ABC", Grid: sql.NullString{String: "FN31", Valid: true},
					GridDerived: derived, UpdatedAt: time.Now().UTC(), CTYValid: true, CTYADIF: 202,
					CTYCQZone: 8, CTYITUZone: 11, CTYCountry: "Stale disk country", CTYContinent: "NA"})
				grid, gotDerived, gridOK := cache.LookupGrid("K1ABC", nil)
				if !gridOK || grid != "FN31" || gotDerived != derived {
					t.Fatalf("disk grid/provenance lost: %s/%v/%v", grid, gotDerived, gridOK)
				}
				info, ok, cached := cache.LookupCTY("K1ABC", db)
				assertActiveCTY(t, info, ok, country)
				if cached != (state != "cold") {
					t.Fatalf("disk changed checked state: cached=%v state=%s", cached, state)
				}
			})
		}
	}
}

func TestGridHydrationUsesActiveCTY(t *testing.T) {
	for _, mode := range []string{"sync", "async"} {
		for _, country := range []string{"Alpha", ""} {
			for _, derived := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/derived=%v", mode, country, derived), func(t *testing.T) {
					store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
					if err != nil {
						t.Fatal(err)
					}
					defer store.Close()
					now := time.Now().UTC()
					if err := store.Upsert(gridstore.Record{Call: "K1ABC", Grid: sql.NullString{String: "FN31", Valid: true},
						GridDerived: derived, FirstSeen: now, UpdatedAt: now, CTYValid: true,
						CTYADIF: 202, CTYCountry: "Stale disk country"}); err != nil {
						t.Fatal(err)
					}
					db := cacheCTYSnapshot(t, country)
					cache := newCallMetaCache(32, 0)
					_, _, _, stop, async, syncLookup := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, func() *cty.CTYDatabase { return db })
					defer stop()
					lookup := async
					if mode == "sync" {
						lookup = syncLookup
					}
					lookup("K1ABC")
					stop() // joins actual hydration, including a timed-out sync caller
					grid, gotDerived, ok := cache.LookupGrid("K1ABC", nil)
					if !ok || grid != "FN31" || gotDerived != derived {
						t.Fatalf("hydration lost stored grid: %s/%v/%v", grid, gotDerived, ok)
					}
					info, found, cached := cache.LookupCTY("K1ABC", db)
					assertActiveCTY(t, info, found, country)
					if cached {
						t.Fatal("disk row marked classification checked")
					}
				})
			}
		}
	}
}

func TestGridExpiredHydrationReLookup(t *testing.T) {
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	old := time.Now().UTC().Add(-2 * time.Hour)
	rec := gridstore.Record{Call: "K1ABC", Grid: sql.NullString{String: "FN31", Valid: true},
		FirstSeen: old, UpdatedAt: old, CTYValid: true, CTYADIF: 291, CTYCountry: "Old disk classification"}
	if err := store.Upsert(rec); err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(32, time.Hour)
	cache.ApplyRecord(rec)
	if _, _, ok := cache.LookupGrid("K1ABC", nil); ok {
		t.Fatal("expired grid did not expire")
	}
	if removed, err := store.PurgeOlderThan(time.Now().UTC().Add(-time.Hour)); err != nil || removed != 1 {
		t.Fatalf("purge = %d/%v, want one", removed, err)
	}
	db := loadShippedClusterCTY(t)
	_, _, _, stop, lookup, _ := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, func() *cty.CTYDatabase { return db })
	defer stop()
	lookup("K1ABC")
	stop()
	grid, derived, ok := cache.LookupGrid("K1ABC", nil)
	if !ok || grid != "FN42" || !derived {
		t.Fatalf("relookup = %s/%v/%v, want FN42/derived", grid, derived, ok)
	}
	info, found, _ := cache.LookupCTY("K1ABC", db)
	assertActiveCTY(t, info, found, "United States")
}
