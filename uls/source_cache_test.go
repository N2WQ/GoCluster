package uls

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func canadianLookupFixture(t testing.TB) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ised.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), `CREATE TABLE CA(call_sign TEXT PRIMARY KEY,state TEXT);
CREATE TABLE Events(id INTEGER PRIMARY KEY,special TEXT,start_day TEXT,end_day TEXT,trustee TEXT,use_by TEXT,uncertain INTEGER,prefix_kind INTEGER);
CREATE INDEX idx_Events_special ON Events(special);
CREATE TABLE SourceMeta(id INTEGER PRIMARY KEY,main_sha TEXT,special_sha TEXT);
INSERT INTO CA VALUES('VE3ABC','ON');
INSERT INTO Events VALUES(1,'CG3','2026-10-07','2026-10-07','VE3ABC','VE3',0,1);
PRAGMA user_version=1;`)
	if err == nil {
		_, err = db.ExecContext(t.Context(), "INSERT INTO SourceMeta VALUES(1,?,?);", strings.Repeat("a", 64), strings.Repeat("b", 64))
	}
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("fixture: %v %v", err, closeErr)
	}
	return path
}

func TestCanadianMidnightRequeriesPositiveNegativeAndCold(t *testing.T) {
	SetCanadianLicenseDBPath(canadianLookupFixture(t))
	t.Cleanup(func() { SetCanadianLicenseDBPath("") })
	before := time.Date(2026, 10, 7, 23, 59, 59, 0, time.UTC)
	after := before.Add(time.Second)
	clock := func() time.Time { return before }
	if got := lookupSource(canadianSnapshot, "CG3ABC", clock); got != (LookupResult{Available: true, Found: true, State: "ON"}) {
		t.Fatal(got)
	}
	clock = func() time.Time { return after }
	if got := lookupSource(canadianSnapshot, "CG3ABC", clock); !got.Available || got.Found {
		t.Fatalf("expired positive cache: %+v", got)
	}
	clock = func() time.Time { return before }
	if got := lookupSource(canadianSnapshot, "CG3ABC", clock); !got.Found || got.State != "ON" {
		t.Fatalf("previous-date negative cache: %+v", got)
	}
	ResetCanadianLicenseDB()
	calls := 0
	clock = func() time.Time {
		calls++
		if calls == 1 {
			return before
		}
		return after
	}
	if got := lookupSource(canadianSnapshot, "CG3ABC", clock); got.Available {
		t.Fatalf("actual cold query crossed midnight: %+v", got)
	}
	cache := licenseCache.Load()
	cache.mu.Lock()
	_, cached := cache.entries[licenseCacheKey{source: canadianSource, call: "CG3ABC"}]
	cache.mu.Unlock()
	if cached {
		t.Fatal("cross-midnight cold query inserted a stale fact")
	}
}

func BenchmarkLookupCanadianWarm(b *testing.B) {
	SetCanadianLicenseDBPath(canadianLookupFixture(b))
	defer SetCanadianLicenseDBPath("")
	LookupCanadian("VE3ABC")
	b.ReportAllocs()
	b.ResetTimer()
	defer b.StopTimer()
	for n := 0; n < b.N; n++ {
		LookupCanadian("VE3ABC")
	}
}

func TestLicenseCacheNamespacesAndAggregateBound(t *testing.T) {
	cache := newLicenseCache(time.Hour, 7)
	now := time.Now().UTC()
	us, ca := licenseCacheKey{call: "VE3ABC"}, licenseCacheKey{source: canadianSource, call: "VE3ABC"}
	cache.set(us, 3, 0, LookupResult{Available: true}, now)
	cache.set(ca, 5, 1, LookupResult{Available: true, Found: true, State: "ON"}, now)
	if result, ok := cache.get(us, 3, 0, now); !ok || result.Found {
		t.Fatal("Canadian fact crossed FCC namespace")
	}
	cache.removeSource(fccSource)
	if _, ok := cache.get(us, 3, 0, now); ok {
		t.Fatal("FCC transition retained its entries")
	}
	if result, ok := cache.get(ca, 5, 1, now); !ok || result.State != "ON" {
		t.Fatal("FCC transition invalidated Canadian entry")
	}
	for n := 0; n < 1000; n++ {
		key := licenseCacheKey{source: licenseSourceID(n % 2), call: fmt.Sprint(n)}
		cache.set(key, uint64(n), int64(n), LookupResult{Available: true}, now)
	}
	if len(cache.entries) != 7 || len(cache.slots) != 7 {
		t.Fatalf("aggregate retention: entries=%d slots=%d", len(cache.entries), len(cache.slots))
	}
	for key, entry := range cache.entries {
		if cache.slots[entry.slot].key != key {
			t.Fatal("cache retained orphan index")
		}
	}
}

func TestCanadianMidnightRejectsWarmAndInFlightFacts(t *testing.T) {
	before := time.Date(2026, 10, 7, 23, 59, 59, 0, time.UTC)
	after := before.Add(time.Second)
	day := canadianLookupDay(before)
	for _, fact := range []LookupResult{{Available: true}, {Available: true, Found: true, State: "ON"}} {
		cache := newLicenseCache(6*time.Hour, 4)
		key := licenseCacheKey{source: canadianSource, call: "CG3ABC"}
		cache.set(key, 4, day, fact, before)
		if _, ok := cache.get(key, 4, canadianLookupDay(after), after); ok {
			t.Fatal("positive/negative cache survived UTC date boundary")
		}
	}
	cache := licenseCache.Load()
	generation := canadianSnapshot.generation.Load()
	key := licenseCacheKey{source: canadianSource, call: "CG3MIDNIGHT"}
	got := finishLookup(canadianSnapshot, cache, key, generation, day, LookupResult{Available: true, Found: true, State: "ON"}, before, func() time.Time { return after })
	if got.Available {
		t.Fatal("in-flight previous-date fact returned after midnight")
	}
	if _, ok := cache.get(key, generation, day, before); ok {
		t.Fatal("in-flight previous-date fact cached after midnight")
	}
}

func TestSourceTransitionLeavesOtherGenerationUsable(t *testing.T) {
	SetLicenseDBPath(fixtureDB(t, false))
	t.Cleanup(func() { CloseLicenseDatabases(); SetCanadianRefreshInProgress(false) })
	if result := LookupUS("K1ABC"); result.State != "CA" {
		t.Fatal(result)
	}
	cache := licenseCache.Load()
	generation := LookupStats().Generation
	SetCanadianRefreshInProgress(true)
	ResetCanadianLicenseDB()
	if result := LookupUS("K1ABC"); result.State != "CA" || !result.Available || generation != LookupStats().Generation || cache != licenseCache.Load() {
		t.Fatal("Canadian transition invalidated FCC ownership or facts")
	}
	if result := LookupCanadian("VE3ABC"); result.Available {
		t.Fatal("Canadian refresh supplied cached facts")
	}
}

func TestFCCPoolWaitHonorsQueryBudget(t *testing.T) {
	db, err := sql.Open("sqlite", fixtureDB(t, false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	done := make(chan LookupResult, 1)
	go func() {
		done <- queryLicenseContext(ctx, db, "SELECT state FROM AM WHERE call_sign=?;", "K1ABC", fccSnapshot.generation.Load())
	}()
	select {
	case got := <-done:
		if got.Available {
			t.Fatal("blocked pool supplied factual result")
		}
	case <-time.After(time.Second):
		t.Fatal("pool wait ignored finite lookup budget")
	}
}

func TestFCCDetachedQueryCannotConsumeNewGenerationDiagnostic(t *testing.T) {
	SetLicenseDBPath(fixtureDB(t, false))
	t.Cleanup(func() { SetLicenseDBPath("") })
	db, _ := getLicenseDB()
	oldGeneration := fccSnapshot.generation.Load()
	ResetLicenseDB()
	result := queryLicenseContext(t.Context(), db, "SELECT invalid FROM AM WHERE call_sign=?;", "K1ABC", oldGeneration)
	if result.Available || fccSnapshot.loggedError.Load() {
		t.Fatal("detached query consumed current diagnostic")
	}
	db, _ = getLicenseDB()
	result = queryLicenseContext(t.Context(), db, "SELECT invalid FROM AM WHERE call_sign=?;", "K1ABC", fccSnapshot.generation.Load())
	if result.Available || !fccSnapshot.loggedError.Load() {
		t.Fatal("current query failure not reported")
	}
}
