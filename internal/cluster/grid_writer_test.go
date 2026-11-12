package cluster

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"dxcluster/gridstore"
)

func TestMergePendingGridRecord(t *testing.T) {
	actual := gridstore.Record{Grid: sql.NullString{String: "FN20", Valid: true}}
	derived := gridstore.Record{Grid: sql.NullString{String: "EM12", Valid: true}, GridDerived: true}
	for _, tc := range []struct {
		name     string
		existing gridstore.Record
		incoming gridstore.Record
		grid     string
		derived  bool
	}{
		{"actual_then_derived", actual, derived, "FN20", false},
		{"derived_then_actual", derived, actual, "FN20", false},
		{"cty_then_derived", gridstore.Record{CTYValid: true}, derived, "EM12", true},
		{"actual_then_cty", actual, gridstore.Record{GridDerived: true, CTYValid: true}, "FN20", false},
		{"derived_then_cty", derived, gridstore.Record{CTYValid: true}, "EM12", true},
		{"actual_then_actual", actual, gridstore.Record{Grid: sql.NullString{String: "FN31", Valid: true}}, "FN31", false},
		{"derived_then_derived", derived, gridstore.Record{Grid: sql.NullString{String: "FN31", Valid: true}, GridDerived: true}, "FN31", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mergePendingGridRecord(tc.existing, tc.incoming)
			if !got.Grid.Valid || got.Grid.String != tc.grid || got.GridDerived != tc.derived {
				t.Fatalf("merged locator = %q valid=%v derived=%v; want %q derived=%v", got.Grid.String, got.Grid.Valid, got.GridDerived, tc.grid, tc.derived)
			}
		})
	}

	t.Run("metadata_after_rejected_derived_grid", func(t *testing.T) {
		first := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
		later := first.Add(time.Hour)
		expires := later.Add(24 * time.Hour)
		existing := gridstore.Record{
			Call: "K1ABC", Grid: actual.Grid, Observations: 2,
			FirstSeen: later, UpdatedAt: first,
		}
		incoming := gridstore.Record{
			Call: "K1ABC", Grid: derived.Grid, GridDerived: true,
			CTYValid: true, CTYADIF: 291, CTYCQZone: 5, CTYITUZone: 8,
			CTYContinent: "NA", CTYCountry: "United States", Observations: 3,
			FirstSeen: first, UpdatedAt: later, ExpiresAt: &expires,
		}
		want := gridstore.Record{
			Call: "K1ABC", Grid: actual.Grid,
			CTYValid: true, CTYADIF: 291, CTYCQZone: 5, CTYITUZone: 8,
			CTYContinent: "NA", CTYCountry: "United States", Observations: 5,
			FirstSeen: first, UpdatedAt: later, ExpiresAt: &expires,
		}
		if got := mergePendingGridRecord(existing, incoming); !reflect.DeepEqual(got, want) {
			t.Fatalf("merged metadata = %+v; want %+v", got, want)
		}
		for _, count := range []int{0, -1} {
			if got := mergePendingGridRecord(want, gridstore.Record{Observations: count}); !reflect.DeepEqual(got, want) {
				t.Fatalf("empty metadata/count %d changed retained record: %+v", count, got)
			}
		}
	})
}

func TestGridWriterPendingProvenancePersistence(t *testing.T) {
	first := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
	later := first.Add(time.Hour)
	latest := later.Add(time.Hour)
	expires := latest.Add(24 * time.Hour)
	actual := func(call, grid string) gridstore.Record {
		return gridstore.Record{Call: call, Grid: sql.NullString{String: grid, Valid: true}, Observations: 2, FirstSeen: first, UpdatedAt: later}
	}
	derived := func(call, grid string) gridstore.Record {
		return gridstore.Record{Call: call, Grid: sql.NullString{String: grid, Valid: true}, GridDerived: true, Observations: 1, FirstSeen: later, UpdatedAt: latest, ExpiresAt: &expires}
	}
	ctyOnly := gridstore.Record{Call: "K1CCC", CTYValid: true, CTYADIF: 291, CTYCQZone: 5, CTYITUZone: 8,
		CTYContinent: "NA", CTYCountry: "United States", FirstSeen: first, UpdatedAt: first}
	// Closed, prebuffered input and a one-hour ticker force all six records
	// through one production pending window and its final flush, without races
	// against the eight-millisecond lookup deadline or separate durable upserts.
	records := []gridstore.Record{
		actual("K1AAA", "FN20"), derived("K1AAA", "EM12"),
		derived("K1BBB", "EM12"), actual("K1BBB", "FN31"),
		ctyOnly, derived("K1CCC", "FN20"),
	}
	dir := t.TempDir()
	opts := gridstore.Options{CacheSizeBytes: 1 << 20, MemTableSizeBytes: 1 << 20,
		L0CompactionThreshold: 2, L0StopWritesThreshold: 4, WriteQueueDepth: 4}
	store, err := gridstore.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Errorf("close store: %v", err)
			}
		}
	})
	metrics := &gridMetrics{}
	done := make(chan struct{})
	coord := bindGridCoordinator(newGridStoreHandle(store), nil)
	updates := make(chan gridWriteRequest, len(records))
	for _, rec := range records {
		if rec.Grid.Valid && !rec.GridDerived {
			coord.enqueueReport(rec, updates)
		} else {
			coord.enqueueMetadata(rec, nil, updates)
		}
	}
	close(updates)
	runGridWriter(coord, updates, done, time.Hour, 0, metrics, gridWriterOps{})
	select {
	case <-done:
	default:
		t.Fatal("writer returned without releasing completion")
	}
	if got := metrics.learnedTotal.Load(); got != 3 {
		t.Fatalf("learned grid count = %d; want 3 coalesced records", got)
	}
	if count, err := store.Count(); err != nil || count != 3 {
		t.Fatalf("stored count = %d err=%v; want 3", count, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = gridstore.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(16, 0)
	for _, want := range []gridstore.Record{
		{Call: "K1AAA", Grid: sql.NullString{String: "FN20", Valid: true}, Observations: 3,
			FirstSeen: first, UpdatedAt: latest, ExpiresAt: &expires},
		{Call: "K1BBB", Grid: sql.NullString{String: "FN31", Valid: true}, Observations: 3,
			FirstSeen: first, UpdatedAt: latest, ExpiresAt: &expires},
		{Call: "K1CCC", Grid: sql.NullString{String: "FN20", Valid: true}, GridDerived: true,
			CTYValid: true, CTYADIF: 291, CTYCQZone: 5, CTYITUZone: 8,
			CTYContinent: "NA", CTYCountry: "United States", Observations: 1,
			FirstSeen: first, UpdatedAt: latest, ExpiresAt: &expires},
	} {
		t.Run(want.Call, func(t *testing.T) {
			got, err := store.Get(want.Call)
			if err != nil || got == nil {
				t.Fatalf("reopened record: %+v err=%v", got, err)
			}
			if !reflect.DeepEqual(*got, want) {
				t.Fatalf("reopened record = %+v; want %+v", *got, want)
			}
			cache.ApplyRecord(*got)
			grid, derived, ok := cache.LookupGrid(want.Call, nil)
			if !ok || grid != want.Grid.String || derived != want.GridDerived {
				t.Fatalf("hydrated grid = %q/%v/%v; want %q/%v/true", grid, derived, ok, want.Grid.String, want.GridDerived)
			}
		})
	}
}
