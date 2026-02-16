package gridstore

import (
	"database/sql"
	"testing"
	"time"
)

func TestUpsertBatchMerge(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	later := now.Add(2 * time.Hour)
	expires := now.Add(24 * time.Hour)

	first := Record{
		Call:         "k1abc",
		IsKnown:      false,
		Grid:         sql.NullString{String: "aa00", Valid: true},
		CTYValid:     true,
		CTYADIF:      291,
		CTYCQZone:    5,
		CTYITUZone:   8,
		CTYContinent: "NA",
		CTYCountry:   "United States",
		Observations: 1,
		FirstSeen:    now,
		UpdatedAt:    now,
	}
	if err := store.UpsertBatch([]Record{first}); err != nil {
		t.Fatalf("upsert batch first: %v", err)
	}

	got := mustGet(t, store, "K1ABC")
	assertGrid(t, got, "AA00")
	assertKnown(t, got, false)
	assertCTY(t, got, true, 291, 5, 8, "NA", "United States")
	assertObservations(t, got, 1)
	assertUnixTime(t, "first_seen", got.FirstSeen, now)
	assertUnixTime(t, "updated_at", got.UpdatedAt, now)

	second := Record{
		Call:         "K1ABC",
		IsKnown:      true,
		Grid:         sql.NullString{},
		Observations: 2,
		FirstSeen:    later,
		UpdatedAt:    later,
		ExpiresAt:    &expires,
	}
	if err := store.UpsertBatch([]Record{second}); err != nil {
		t.Fatalf("upsert batch second: %v", err)
	}

	merged := mustGet(t, store, "k1abc")
	assertGrid(t, merged, "AA00")
	assertKnown(t, merged, true)
	assertCTY(t, merged, true, 291, 5, 8, "NA", "United States")
	assertObservations(t, merged, 3)
	assertUnixTime(t, "first_seen", merged.FirstSeen, now)
	assertUnixTime(t, "updated_at", merged.UpdatedAt, later)
	if merged.ExpiresAt == nil {
		t.Fatalf("expected expires_at to be set")
	}
	assertUnixTime(t, "expires_at", *merged.ExpiresAt, expires)

	if count, err := store.Count(); err != nil {
		t.Fatalf("count failed: %v", err)
	} else if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

func TestDerivedGridDoesNotOverwriteActual(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	now := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	actual := Record{
		Call:         "K1ABC",
		Grid:         sql.NullString{String: "FN20", Valid: true},
		GridDerived:  false,
		Observations: 1,
		FirstSeen:    now,
		UpdatedAt:    now,
	}
	if err := store.UpsertBatch([]Record{actual}); err != nil {
		t.Fatalf("upsert actual: %v", err)
	}
	derived := Record{
		Call:         "K1ABC",
		Grid:         sql.NullString{String: "FN21", Valid: true},
		GridDerived:  true,
		Observations: 1,
		FirstSeen:    now.Add(time.Minute),
		UpdatedAt:    now.Add(time.Minute),
	}
	if err := store.UpsertBatch([]Record{derived}); err != nil {
		t.Fatalf("upsert derived: %v", err)
	}
	got := mustGet(t, store, "K1ABC")
	assertGrid(t, got, "FN20")
	if got.GridDerived {
		t.Fatalf("expected actual grid to remain non-derived")
	}

	store = openTestStore(t)
	defer store.Close()
	if err := store.UpsertBatch([]Record{derived}); err != nil {
		t.Fatalf("upsert derived first: %v", err)
	}
	if err := store.UpsertBatch([]Record{actual}); err != nil {
		t.Fatalf("upsert actual second: %v", err)
	}
	got = mustGet(t, store, "K1ABC")
	assertGrid(t, got, "FN20")
	if got.GridDerived {
		t.Fatalf("expected actual grid to clear derived flag")
	}
}

func TestClearKnownFlags(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	now := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	recs := []Record{
		{Call: "K1AAA", IsKnown: true, Observations: 1, FirstSeen: now, UpdatedAt: now},
		{Call: "K1BBB", IsKnown: false, Observations: 1, FirstSeen: now, UpdatedAt: now},
	}
	if err := store.UpsertBatch(recs); err != nil {
		t.Fatalf("upsert batch: %v", err)
	}
	if err := store.ClearKnownFlags(); err != nil {
		t.Fatalf("clear known flags: %v", err)
	}

	assertKnown(t, mustGet(t, store, "K1AAA"), false)
	assertKnown(t, mustGet(t, store, "K1BBB"), false)
}

func TestPurgeOlderThan(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	old := time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC)
	fresh := old.Add(2 * time.Hour)

	recs := []Record{
		{Call: "K1OLD", Observations: 1, FirstSeen: old, UpdatedAt: old},
		{Call: "K1NEW", Observations: 1, FirstSeen: fresh, UpdatedAt: fresh},
	}
	if err := store.UpsertBatch(recs); err != nil {
		t.Fatalf("upsert batch: %v", err)
	}

	removed, err := store.PurgeOlderThan(old.Add(30 * time.Minute))
	if err != nil {
		t.Fatalf("purge older than: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}

	if rec, err := store.Get("K1OLD"); err != nil {
		t.Fatalf("get old: %v", err)
	} else if rec != nil {
		t.Fatalf("expected old record to be removed")
	}
	if rec, err := store.Get("K1NEW"); err != nil {
		t.Fatalf("get new: %v", err)
	} else if rec == nil {
		t.Fatalf("expected new record to remain")
	}
	if count, err := store.Count(); err != nil {
		t.Fatalf("count failed: %v", err)
	} else if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func mustGet(t *testing.T, store *Store, call string) *Record {
	t.Helper()
	rec, err := store.Get(call)
	if err != nil {
		t.Fatalf("get %s: %v", call, err)
	}
	if rec == nil {
		t.Fatalf("record %s not found", call)
	}
	return rec
}

func assertGrid(t *testing.T, rec *Record, grid string) {
	t.Helper()
	if !rec.Grid.Valid {
		t.Fatalf("expected grid valid=true, got %v", rec.Grid.Valid)
	}
	if rec.Grid.String != grid {
		t.Fatalf("expected grid %q, got %q", grid, rec.Grid.String)
	}
}

func assertKnown(t *testing.T, rec *Record, known bool) {
	t.Helper()
	if rec.IsKnown != known {
		t.Fatalf("expected known=%v, got %v", known, rec.IsKnown)
	}
}

func assertCTY(t *testing.T, rec *Record, valid bool, adif, cq, itu int, continent, country string) {
	t.Helper()
	if rec.CTYValid != valid {
		t.Fatalf("expected cty valid=%v, got %v", valid, rec.CTYValid)
	}
	if !valid {
		return
	}
	if rec.CTYADIF != adif {
		t.Fatalf("expected cty adif=%d, got %d", adif, rec.CTYADIF)
	}
	if rec.CTYCQZone != cq {
		t.Fatalf("expected cty cq=%d, got %d", cq, rec.CTYCQZone)
	}
	if rec.CTYITUZone != itu {
		t.Fatalf("expected cty itu=%d, got %d", itu, rec.CTYITUZone)
	}
	if rec.CTYContinent != continent {
		t.Fatalf("expected cty continent=%q, got %q", continent, rec.CTYContinent)
	}
	if rec.CTYCountry != country {
		t.Fatalf("expected cty country=%q, got %q", country, rec.CTYCountry)
	}
}

func assertObservations(t *testing.T, rec *Record, expected int) {
	t.Helper()
	if rec.Observations != expected {
		t.Fatalf("expected observations=%d, got %d", expected, rec.Observations)
	}
}

func assertUnixTime(t *testing.T, field string, got time.Time, want time.Time) {
	t.Helper()
	if got.Unix() != want.Unix() {
		t.Fatalf("expected %s=%d, got %d", field, want.Unix(), got.Unix())
	}
}
