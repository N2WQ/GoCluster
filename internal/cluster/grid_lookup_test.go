package cluster

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/gridstore"
)

func TestGridLookupSyncUsesBaseIdentity(t *testing.T) {
	dir := t.TempDir()
	store, err := gridstore.Open(dir, gridstore.Options{
		CacheSizeBytes:        1 << 20,
		MemTableSizeBytes:     1 << 20,
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 4,
		WriteQueueDepth:       4,
	})
	if err != nil {
		t.Fatalf("open gridstore: %v", err)
	}
	now := time.Now().UTC()
	if err := store.Upsert(gridstore.Record{
		Call:         "KW7MM-#",
		Grid:         sql.NullString{String: "EM12", Valid: true},
		Observations: 1,
		FirstSeen:    now,
		UpdatedAt:    now,
	}); err != nil {
		store.Close()
		t.Fatalf("upsert grid record: %v", err)
	}

	cache := newCallMetaCache(64, 0)
	storeHandle := newGridStoreHandle(store)
	_, _, _, stop, gridLookup, gridLookupSync := startGridWriter(storeHandle, 50*time.Millisecond, cache, 0, true, nil)
	defer store.Close()
	if stop != nil {
		defer stop()
	}

	if gridLookupSync == nil {
		t.Fatalf("expected sync lookup to be enabled")
	}
	if gridLookup == nil {
		t.Fatalf("expected async lookup to be available")
	}

	grid, derived, ok := gridLookupSync("KW7MM-#")
	if !ok || grid != "EM12" || derived {
		t.Fatalf("gridLookupSync returned %q ok=%v derived=%v, want EM12 derived=false", grid, ok, derived)
	}
	grid, derived, ok = gridLookup("KW7MM")
	if !ok || grid != "EM12" || derived {
		t.Fatalf("gridLookup returned %q ok=%v derived=%v, want EM12 derived=false", grid, ok, derived)
	}
}

func TestGridLookupDerivesWhenRecordMissingGrid(t *testing.T) {
	const plist = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
<key>K1</key>
	<dict>
		<key>Country</key>
		<string>Alpha</string>
		<key>Prefix</key>
		<string>K1</string>
		<key>ExactCallsign</key>
		<false/>
		<key>Latitude</key>
		<real>40.0</real>
		<key>Longitude</key>
		<real>75.0</real>
	</dict>
</dict>
</plist>`
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(plist))
	if err != nil {
		t.Fatalf("load cty database: %v", err)
	}

	dir := t.TempDir()
	store, err := gridstore.Open(dir, gridstore.Options{
		CacheSizeBytes:        1 << 20,
		MemTableSizeBytes:     1 << 20,
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 4,
		WriteQueueDepth:       4,
	})
	if err != nil {
		t.Fatalf("open gridstore: %v", err)
	}
	now := time.Now().UTC()
	if err := store.Upsert(gridstore.Record{
		Call:         "K1ABC",
		CTYValid:     true,
		CTYADIF:      291,
		CTYCQZone:    5,
		CTYITUZone:   8,
		CTYContinent: "NA",
		CTYCountry:   "United States",
		Observations: 1,
		FirstSeen:    now,
		UpdatedAt:    now,
	}); err != nil {
		store.Close()
		t.Fatalf("upsert cty-only record: %v", err)
	}

	cache := newCallMetaCache(64, 0)
	ctyLookup := func() *cty.CTYDatabase { return db }
	storeHandle := newGridStoreHandle(store)
	_, _, _, stop, gridLookup, gridLookupSync := startGridWriter(storeHandle, 50*time.Millisecond, cache, 0, true, ctyLookup)
	defer store.Close()
	if stop != nil {
		defer stop()
	}

	grid, derived, ok := gridLookupSync("K1ABC")
	if !ok || grid != "FN20" || !derived {
		t.Fatalf("gridLookupSync returned %q ok=%v derived=%v, want FN20 derived=true", grid, ok, derived)
	}
	grid, derived, ok = gridLookup("K1ABC")
	if !ok || grid != "FN20" || !derived {
		t.Fatalf("gridLookup returned %q ok=%v derived=%v, want FN20 derived=true", grid, ok, derived)
	}
}
