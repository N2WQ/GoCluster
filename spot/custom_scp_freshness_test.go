// File role: Locks per-observation historical qualification independently of
// retained history, stored bytes, and consumer-specific score/cell gates.
package spot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"

	"github.com/cockroachdb/pebble"
)

func newCustomSCPFreshnessStore(t *testing.T, opts CustomSCPOptions) *CustomSCPStore {
	t.Helper()
	opts.Path = filepath.Join(t.TempDir(), "scp")
	if opts.HorizonDays == 0 {
		opts.HorizonDays = 400
	}
	if opts.CoreMinScore == 0 {
		opts.CoreMinScore = 5
	}
	if opts.CoreMinH3Cells == 0 {
		opts.CoreMinH3Cells = 1
	}
	if opts.SFloorMinScore == 0 {
		opts.SFloorMinScore = 3
	}
	if opts.SFloorExactMinH3Cells == 0 {
		opts.SFloorExactMinH3Cells = 1
	}
	if opts.SFloorFamilyMinH3Cells == 0 {
		opts.SFloorFamilyMinH3Cells = 1
	}
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

// This public-API counterexample compiles against the defective baseline. A
// fresh receiver cannot refresh two other receivers' 54/55-day observations.
func TestB11CustomSCPIndividualFreshness(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{HorizonDays: 60})
	now := time.Now().UTC().Truncate(time.Second)
	for _, row := range []struct {
		receiver string
		age      time.Duration
		cell     uint16
	}{
		{"N0AAA", 55 * 24 * time.Hour, 101},
		{"N0BBB", 54 * 24 * time.Hour, 202},
		{"N0CCC", time.Minute, 303},
	} {
		store.recordObservation("K1FRESH", "20m", "CW", row.receiver, row.cell, 0, false, now.Add(-row.age))
	}
	if got := store.RecentSupportCount("K1FRESH", "20m", "CW", now); got != 1 {
		t.Fatalf("one independently fresh receiver must qualify at score 5; got %d (three observations remain retained)", got)
	}
	if store.HasRecentSupport("K1FRESH", "20m", "CW", 2, now) {
		t.Fatal("old receivers must not satisfy two-receiver corroboration")
	}
	if got := store.StatsSnapshot().ObservationSpotters; got != 3 {
		t.Fatalf("qualification must preserve all three retained observations; got %d", got)
	}
	assertCustomSCPCleanupCoupling(t, store)
}

func TestB11CustomSCPEligibleCells(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{CoreMinH3Cells: 2})
	now := time.Now().UTC().Truncate(time.Second)
	for _, row := range []struct {
		receiver string
		age      time.Duration
		cell     uint16
	}{
		{"N0AAA", time.Minute, 101}, {"N0BBB", time.Minute, 101},
		{"N0CCC", 55 * 24 * time.Hour, 202}, {"N0DDD", time.Minute, 0},
	} {
		store.recordObservation("K1CELLS", "20m", "CW", row.receiver, row.cell, 0, false, now.Add(-row.age))
	}
	got := store.snapshotFor("K1CELLS", "20m", "CW", now, 5)
	want := customSCPSnapshot{uniqueSpotters: 3, uniqueCells: 1, latestSeenUnix: now.Add(-time.Minute).Unix(), score: 10}
	if got != want {
		t.Fatalf("eligible receivers must supply all counted cells; got %+v want %+v", got, want)
	}
	if store.HasRecentSupport("K1CELLS", "20m", "CW", 2, now) || store.RecentSupportCount("K1CELLS", "20m", "CW", now) != 0 {
		t.Fatal("the old distinct cell must not satisfy the core two-cell gate")
	}
	if store.StatsSnapshot().ObservationSpotters != 4 {
		t.Fatal("cell qualification must not remove retained rows")
	}
	assertCustomSCPCleanupCoupling(t, store)
}

func TestB11CustomSCPRailMinimums(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2})
	now := time.Now().UTC().Truncate(time.Second)
	for _, row := range []struct {
		receiver string
		age      time.Duration
		cell     uint16
	}{
		{"N0AAA", time.Minute, 101}, {"N0BBB", 20 * 24 * time.Hour, 202}, {"N0CCC", 20 * 24 * time.Hour, 202},
	} {
		store.recordObservation("K1RAIL", "20m", "CW", row.receiver, row.cell, 0, false, now.Add(-row.age))
	}
	if got := store.RecentSupportCount(" k1rail ", "20m", "CW", now); got != 1 {
		t.Fatalf("core score 5 must count only the fresh receiver; got %d", got)
	}
	if got := store.RecentSupportCountNormalized("K1RAIL", "20m", "CW", now); got != 1 {
		t.Fatalf("normalized core query must use the same score minimum; got %d", got)
	}
	if store.HasRecentSupport("K1RAIL", "20m", "CW", 2, now) {
		t.Fatal("core minimum must not borrow floor-eligible receivers")
	}
	if !store.HasSFloorSupportExact("K1RAIL", "20m", "CW", 3, now) ||
		!store.HasSFloorSupportFamily([]string{"K1RAIL"}, "20m", "CW", 3, now) ||
		!store.HasSFloorSupportFamilyKeyPair("K1RAIL", "", "20m", "CW", 3, now) {
		t.Fatal("existing floor score 3 must independently admit all three receivers and both cells")
	}
	if store.HasSFloorSupportExact("K1RAIL", "20m", "CW", 4, now) {
		t.Fatal("floor qualification must still enforce its receiver requirement")
	}
	assertCustomSCPCleanupCoupling(t, store)
}

func TestB11CustomSCPAgeBoundaries(t *testing.T) {
	// Literal seconds and scores come from the accepted age contract. Expected
	// eligibility is not calculated with tierForAge or scoreForTier.
	for _, row := range []struct {
		age   int64
		score int
	}{
		{0, 10}, {43199, 10}, {43200, 10}, {43201, 7},
		{172799, 7}, {172800, 7}, {172801, 5},
		{604799, 5}, {604800, 5}, {604801, 3},
		{2591999, 3}, {2592000, 3}, {2592001, 2},
		{7775999, 2}, {7776000, 2}, {7776001, 1},
		{15551999, 1}, {15552000, 1}, {15552001, 0},
	} {
		t.Run(fmt.Sprintf("age_%d_score_%d", row.age, row.score), func(t *testing.T) {
			store := newCustomSCPFreshnessStore(t, CustomSCPOptions{})
			now := time.Now().UTC().Truncate(time.Second)
			seen := now.Unix() - row.age
			store.recordObservation("K1AGE", "20m", "CW", "N0AAA", 101, 0, false, time.Unix(seen, 0))
			for _, minimum := range []int{1, 2, 3, 4, 5, 6, 7, 8, 10, 11} {
				want := customSCPSnapshot{}
				if row.score >= minimum {
					want = customSCPSnapshot{uniqueSpotters: 1, uniqueCells: 1, latestSeenUnix: seen, score: row.score}
				}
				if got := store.snapshotFor("K1AGE", "20m", "CW", now, minimum); got != want {
					t.Fatalf("age=%d minimum=%d got %+v want %+v", row.age, minimum, got, want)
				}
				if got := store.snapshotForNormalizedCall("K1AGE", "20m", "CW", now, minimum); got != want {
					t.Fatalf("normalized age=%d minimum=%d got %+v want %+v", row.age, minimum, got, want)
				}
			}
			if store.StatsSnapshot().ObservationSpotters != 1 {
				t.Fatal("score expiry inside the 400-day retention horizon deleted the row")
			}
		})
	}
}

func TestB11CustomSCPStoredSecondPrecision(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{})
	base := time.Now().UTC().Truncate(time.Second)
	store.recordObservation("K1SECOND", "20m", "CW", "N0AAA", 101, 0, false, base.Add(900*time.Millisecond))
	if got := store.snapshotFor("K1SECOND", "20m", "CW", base.Add(100*time.Millisecond), 10); got.uniqueSpotters != 1 || got.score != 10 {
		t.Fatalf("subsecond source and query share one stored second: %+v", got)
	}
	boundary := base.Add(12 * time.Hour)
	if got := store.snapshotFor("K1SECOND", "20m", "CW", boundary.Add(999*time.Millisecond), 10); got.uniqueSpotters != 1 {
		t.Fatalf("inclusive 12-hour boundary is evaluated at stored-second precision: %+v", got)
	}
	if got := store.snapshotFor("K1SECOND", "20m", "CW", boundary.Add(time.Second), 10); got != (customSCPSnapshot{}) {
		t.Fatalf("the following stored second no longer qualifies at score 10: %+v", got)
	}
}

func TestB11CustomSCPNewestFutureQuarantine(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{})
	now := time.Now().UTC().Truncate(time.Second)
	future := now.Add(time.Minute)
	store.recordObservation("K1FUTURE", "20m", "CW", "N0AAA", 101, 0, false, now.Add(-5*time.Minute))
	store.recordObservation("K1FUTURE", "20m", "CW", "N0AAA", 202, 0, false, future)
	store.recordObservation("K1FUTURE", "20m", "CW", "N0BBB", 303, 0, false, now.Add(-time.Minute))
	// An older arrival cannot replace the already retained newest observation.
	store.recordObservation("K1FUTURE", "20m", "CW", "N0AAA", 404, 0, false, now)
	key := customSCPKey{call: "K1FUTURE", band: "20m", bucket: "cw"}
	requireCustomSCPCleanupRaw(t, store, observationKeyString(key, "N0AAA"), customSCPCleanupObservationBytes(future.Unix(), 202))
	want := customSCPSnapshot{uniqueSpotters: 1, uniqueCells: 1, latestSeenUnix: now.Add(-time.Minute).Unix(), score: 10}
	if got := store.snapshotFor("K1FUTURE", "20m", "CW", now, 5); got != want {
		t.Fatalf("future row must not lend count, cell or latest time to the current population: got %+v want %+v", got, want)
	}
	store.recordObservation("K1ONLY", "20m", "CW", "N0AAA", 202, 0, false, future)
	if got := store.snapshotFor("K1ONLY", "20m", "CW", now, 1); got != (customSCPSnapshot{}) {
		t.Fatalf("all-future entry must have no query authority: %+v", got)
	}
	want = customSCPSnapshot{uniqueSpotters: 2, uniqueCells: 2, latestSeenUnix: future.Unix(), score: 10}
	if got := store.snapshotFor("K1FUTURE", "20m", "CW", future, 5); got != want {
		t.Fatalf("exact timestamp catch-up must restore the retained row: got %+v want %+v", got, want)
	}
	requireCustomSCPCleanupRaw(t, store, observationKeyString(key, "N0AAA"), customSCPCleanupObservationBytes(future.Unix(), 202))
	if got := store.StatsSnapshot().ObservationSpotters; got != 3 {
		t.Fatalf("future quarantine must preserve retained rows; got %d", got)
	}
	assertCustomSCPCleanupCoupling(t, store)
}

func TestB11CustomSCPQualificationPreservesBytesAndReopen(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{})
	now := time.Now().UTC().Truncate(time.Second)
	key := customSCPKey{call: "K1BYTES", band: "20m", bucket: "cw"}
	rows := []struct {
		receiver string
		seen     int64
		cell     uint16
	}{
		{"N0AAA", now.Add(-55 * 24 * time.Hour).Unix(), 101},
		{"N0BBB", now.Add(-20 * 24 * time.Hour).Unix(), 202},
		{"N0CCC", now.Add(-time.Minute).Unix(), 303},
		{"N0DDD", now.Add(time.Minute).Unix(), 404},
	}
	for _, row := range rows {
		store.recordObservation(key.call, key.band, "CW", row.receiver, row.cell, 0, false, time.Unix(row.seen, 0))
	}
	before := store.StatsSnapshot()
	checkRaw := func(s *CustomSCPStore) {
		t.Helper()
		// Compare independent 8/10-byte records before reopened lookup can
		// normalize fields or prune evidence. No decoded read is the oracle.
		for _, row := range rows {
			requireCustomSCPCleanupRaw(t, s, observationKeyString(key, row.receiver), customSCPCleanupObservationBytes(row.seen, row.cell))
		}
		requireCustomSCPCleanupRaw(t, s, customSCPMetaPrefix+key.call, customSCPCleanupStaticBytes(rows[3].seen))
	}
	checkQueries := func(s *CustomSCPStore) {
		t.Helper()
		for _, minimum := range []int{5, 1, 3, 11, 5} {
			wantCount, wantCells := 1, 1
			switch minimum {
			case 1:
				wantCount, wantCells = 3, 3
			case 3:
				wantCount, wantCells = 2, 2
			case 11:
				wantCount, wantCells = 0, 0
			}
			got := s.snapshotFor(key.call, key.band, "CW", now, minimum)
			if got.uniqueSpotters != wantCount || got.uniqueCells != wantCells {
				t.Fatalf("minimum=%d qualified %+v want receivers=%d cells=%d", minimum, got, wantCount, wantCells)
			}
		}
	}
	checkRaw(store)
	checkQueries(store)
	checkRaw(store)
	if after := store.StatsSnapshot(); after != before {
		t.Fatalf("qualification within retention changed retained ownership: before=%+v after=%+v", before, after)
	}
	assertCustomSCPCleanupCoupling(t, store)
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
	checkRaw(reopened)
	checkQueries(reopened)
	checkRaw(reopened)
	if got := reopened.StatsSnapshot().ObservationSpotters; got != 4 {
		t.Fatalf("reopen must recover all retained rows, including old/future ones; got %d", got)
	}
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestB11CustomSCPNormalizedOwnedInputConcurrentReaders(t *testing.T) {
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatalf("InitH3Mappings unavailable: %v", err)
	}
	if pathreliability.EncodeCoarseCell("FN31AA") == pathreliability.InvalidCell {
		t.Fatal("fixture must provide a real coarse cell for the existing support gates")
	}
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{})
	now := time.Now().UTC().Truncate(time.Second)
	source := NewSpot("K1READ", "N0AAA", 14020, "CW")
	source.Confidence, source.Time, source.DEMetadata.Grid = "V", now.Add(-time.Minute), "FN31AA"
	source.EnsureNormalized()
	source.SealForAsync()
	if source.SnapshotForAsync() != source {
		t.Fatal("fixture must have normalized sealed pointers that async snapshot reuses")
	}
	type fields struct {
		dx, de, mode, band, grid, confidence         string
		dxNorm, deNorm, modeNorm, bandNorm, gridNorm string
		stamp                                        time.Time
		frequency                                    float64
	}
	readFields := func() fields {
		return fields{source.DXCall, source.DECall, source.Mode, source.Band, source.DEMetadata.Grid, source.Confidence,
			source.DXCallNorm, source.DECallNorm, source.ModeNorm, source.BandNorm, source.DEGridNorm, source.Time, source.Frequency}
	}
	before := readFields()
	store.RecordSpot(source)
	if got := store.RecentSupportCountNormalized("K1READ", "20m", "CW", now); got != 1 {
		t.Fatalf("normalized V admission must establish one eligible receiver before concurrency; got %d", got)
	}
	start := make(chan struct{})
	failures := make(chan string, 4)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for i := 0; i < 256; i++ {
				switch worker {
				case 0:
					if readFields() != before {
						failures <- "concurrent reader saw a changed normalized source"
						return
					}
				case 1:
					store.RecordSpot(source)
				case 2:
					if store.RecentSupportCountNormalized("K1READ", "20m", "CW", now) != 1 ||
						!store.HasSFloorSupportExact("K1READ", "20m", "CW", 1, now) {
						failures <- "concurrent query lost the stable eligible receiver"
						return
					}
				case 3:
					if i%16 == 0 {
						store.cleanup(now)
					}
				}
				runtime.Gosched()
			}
		}()
	}
	close(start)
	workers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if got := readFields(); got != before {
		t.Errorf("record/query/cleanup mutated the fully normalized owned source: got %+v want %+v", got, before)
	}
	if got := store.StatsSnapshot().ObservationSpotters; got != 1 {
		t.Fatalf("same-receiver concurrent recording duplicated retained rows: %d", got)
	}
	assertCustomSCPCleanupCoupling(t, store)
}

func TestB12CustomSCPConsumerRequirementsDoNotRewriteHistory(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{CoreMinScore: 5, CoreMinH3Cells: 2})
	now := time.Now().UTC().Truncate(time.Second)
	key := customSCPKey{call: "K1REQ", band: "20m", bucket: "cw"}
	rows := []struct {
		receiver string
		seen     int64
		cell     uint16
	}{
		{"N0AAA", now.Add(-time.Minute).Unix(), 101},
		{"N0BBB", now.Add(-20 * 24 * time.Hour).Unix(), 202},
		{"N0CCC", now.Add(-20 * 24 * time.Hour).Unix(), 202},
	}
	for _, row := range rows {
		store.recordObservation(key.call, key.band, "CW", row.receiver, row.cell, 0, false, time.Unix(row.seen, 0))
		requireCustomSCPCleanupRaw(t, store, observationKeyString(key, row.receiver), customSCPCleanupObservationBytes(row.seen, row.cell))
	}
	beforeStats, beforeOptions := store.StatsSnapshot(), store.opts
	store.mu.RLock()
	beforeEntry := store.entries[key]
	beforeRows := append([]customSCPSpotterEntry(nil), beforeEntry.spotters...)
	store.mu.RUnlock()
	if got := store.RecentSupportCount(key.call, key.band, "CW", now); got != 0 {
		t.Fatalf("default core two-cell gate must remain independent of explicit queries: %d", got)
	}
	queries := []struct {
		name      string
		req       CustomSCPRequirements
		wantCount int
		minUnique int
		wantBool  bool
	}{
		{"resolver_partial_count", CustomSCPRequirements{MinScore: 5, MinH3Cells: 1}, 1, 2, false},
		{"resolver_one_receiver", CustomSCPRequirements{MinScore: 5, MinH3Cells: 1}, 1, 1, true},
		{"stabilizer_later_tier", CustomSCPRequirements{MinScore: 3, MinH3Cells: 2}, 3, 3, true},
		{"stabilizer_receiver_gate", CustomSCPRequirements{MinScore: 3, MinH3Cells: 2}, 3, 4, false},
		{"eligible_cell_gate", CustomSCPRequirements{MinScore: 5, MinH3Cells: 2}, 0, 1, false},
		{"unattainable_score", CustomSCPRequirements{MinScore: 11, MinH3Cells: 1}, 0, 1, false},
		{"insufficient_cells", CustomSCPRequirements{MinScore: 1, MinH3Cells: 3}, 0, 1, false},
	}
	// Strict and loose consumers repeatedly alternate in both orders. A query
	// must not install its requirements into the canonical store or its rows.
	for pass := 0; pass < 2; pass++ {
		for i := range queries {
			index := i
			if pass == 1 {
				index = len(queries) - 1 - i
			}
			query := queries[index]
			if got := store.RecentSupportCountWithRequirements(" k1req ", key.band, "CW", query.req, now); got != query.wantCount {
				t.Errorf("pass=%d %s raw count=%d want %d", pass, query.name, got, query.wantCount)
			}
			if got := store.RecentSupportCountNormalizedWithRequirements(key.call, key.band, "CW", query.req, now); got != query.wantCount {
				t.Errorf("pass=%d %s normalized count=%d want %d", pass, query.name, got, query.wantCount)
			}
			if got := store.HasRecentSupportWithRequirements(key.call, key.band, "CW", query.minUnique, query.req, now); got != query.wantBool {
				t.Errorf("pass=%d %s receiver qualification=%t want %t", pass, query.name, got, query.wantBool)
			}
		}
	}
	if store.opts != beforeOptions || store.StatsSnapshot() != beforeStats {
		t.Fatal("consumer queries changed store requirements or canonical retained-state statistics")
	}
	store.mu.RLock()
	entry := store.entries[key]
	unchanged := entry == beforeEntry && len(entry.spotters) == len(beforeRows)
	if unchanged {
		for i := range beforeRows {
			unchanged = unchanged && entry.spotters[i] == beforeRows[i]
		}
	}
	store.mu.RUnlock()
	if !unchanged {
		t.Fatal("consumer queries replaced the canonical entry or changed retained observations")
	}
	for _, row := range rows {
		requireCustomSCPCleanupRaw(t, store, observationKeyString(key, row.receiver), customSCPCleanupObservationBytes(row.seen, row.cell))
	}
	requireCustomSCPCleanupRaw(t, store, customSCPMetaPrefix+key.call, customSCPCleanupStaticBytes(rows[0].seen))
	assertCustomSCPCleanupCoupling(t, store)
}

func TestB12CustomSCPRequirementsNormalizedSkipsNormalizeCache(t *testing.T) {
	originalCache := normalizeCallCache
	t.Cleanup(func() { normalizeCallCache = originalCache })
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{})
	now := time.Now().UTC().Truncate(time.Second)
	store.recordObservation("K1FAST", "20m", "CW", "N0AAA", 101, 0, false, now.Add(-time.Minute))
	normalizeCallCache = NewCallCache(16, time.Hour)
	req := CustomSCPRequirements{MinScore: 5, MinH3Cells: 1}
	if got := store.RecentSupportCountNormalizedWithRequirements("K1FAST", "20m", "CW", req, now); got != 1 {
		t.Fatalf("normalized requirement query must retain the eligible partial count: %d", got)
	}
	if _, cached := normalizeCallCache.Get("K1FAST"); cached {
		t.Fatal("normalized requirement query populated the callsign cache")
	}
	if got := store.RecentSupportCountWithRequirements("K1FAST", "20m", "CW", req, now); got != 1 {
		t.Fatalf("raw requirement query disagreed with normalized query: %d", got)
	}
	if _, cached := normalizeCallCache.Get("K1FAST"); !cached {
		t.Fatal("raw requirement query stopped using callsign normalization")
	}
}

func TestB12CustomSCPRequirementsPreservePartialCountWithoutCapCoercion(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{MaxSpottersPerKey: 1})
	now := time.Now().UTC().Truncate(time.Second)
	store.recordObservation("K1CAP", "20m", "CW", "N0AAA", 101, 0, false, now.Add(-time.Minute))
	req := CustomSCPRequirements{MinScore: 5, MinH3Cells: 1}
	if store.RecentSupportCount("K1CAP", "20m", "CW", now) != 1 ||
		store.RecentSupportCountWithRequirements("K1CAP", "20m", "CW", req, now) != 1 {
		t.Fatal("count compatibility must retain one eligible receiver below the default unique minimum")
	}
	// B1.4 preserves B1.2 partial counts while removing Boolean cap coercion.
	if store.HasRecentSupport("K1CAP", "20m", "CW", 2, now) ||
		store.HasRecentSupportWithRequirements("K1CAP", "20m", "CW", 2, req, now) {
		t.Fatal("an eligible partial count cannot satisfy an impossible Boolean minimum")
	}
}

func BenchmarkB12CustomSCPRequirementsNormalized(b *testing.B) {
	for _, query := range []struct {
		name string
		req  CustomSCPRequirements
		want int
	}{
		{"score5_four_receivers", CustomSCPRequirements{MinScore: 5, MinH3Cells: 1}, 4},
		{"score3_eight_receivers", CustomSCPRequirements{MinScore: 3, MinH3Cells: 5}, 8},
	} {
		b.Run(query.name, func(b *testing.B) {
			now := time.Now().UTC().Truncate(time.Second)
			entry := &customSCPEntry{spotters: makeCustomSCPSpotters(8)}
			for i := 0; i < 8; i++ {
				seen := now.Add(-time.Duration(i) * time.Second)
				if i >= 4 {
					seen = seen.Add(-20 * 24 * time.Hour)
				}
				entryUpsertSpotter(entry, fmt.Sprintf("N%03d", i), customSCPSpotterObs{seenUnix: seen.Unix(), cellRes1: uint16(100 + i)}, 8)
			}
			store := &CustomSCPStore{
				opts:                sanitizeCustomSCPOptions(CustomSCPOptions{CoreMinScore: 10, CoreMinH3Cells: 4}),
				entries:             make(map[customSCPKey]*customSCPEntry, 1),
				entryExpiry:         newCustomSCPEntryExpiryQueue(1),
				observationSpotters: 8,
			}
			store.mu.Lock()
			key := retainCustomSCPTestEntryLocked(store, customSCPKey{call: "K1BENCH", band: "20m", bucket: "cw"}, entry)
			store.markEntryForCleanupLocked(key, entry)
			store.mu.Unlock()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := store.RecentSupportCountNormalizedWithRequirements("K1BENCH", "20m", "CW", query.req, now); got != query.want {
					b.Fatalf("explicit normalized requirements count=%d want %d", got, query.want)
				}
			}
		})
	}
}

type b13FamilyRow struct {
	call, receiver string
	seen           time.Time
	cell           uint16
}

func b13FamilyRows(call, receiverPrefix string, count, cells int, firstCell uint16, seen time.Time) []b13FamilyRow {
	rows := make([]b13FamilyRow, count)
	for i := range rows {
		rows[i] = b13FamilyRow{call, fmt.Sprintf("%s%03d", receiverPrefix, i), seen, firstCell + uint16(i%cells)}
	}
	return rows
}

// Observation-only disk input avoids RecordSpot/recordObservation creating
// independent static membership or mirroring slash evidence into both keys.
func b13FamilyStore(t testing.TB, rows []b13FamilyRow, maxSpotters int) *CustomSCPStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scp")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		var value [10]byte
		binary.BigEndian.PutUint64(value[:8], uint64(row.seen.Unix()))
		binary.BigEndian.PutUint16(value[8:], row.cell)
		key := fmt.Sprintf("o|%s|20m|cw|%s", row.call, row.receiver)
		if err := db.Set([]byte(key), value[:], pebble.Sync); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenCustomSCPStore(CustomSCPOptions{
		Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxSpottersPerKey: maxSpotters,
		CoreMinScore: 5, CoreMinH3Cells: 2,
		SFloorMinScore: 5, SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if got := store.StatsSnapshot(); got.ObservationSpotters != len(rows) || got.StaticCalls != 0 {
		t.Fatalf("raw fixture must recover every observation without static membership: %+v rows=%d", got, len(rows))
	}
	return store
}

func TestB13CustomSCPFamilyMembersQualifyIndependently(t *testing.T) {
	const vote, base = "W1AW/KH6", "W1AW"
	for _, tc := range []struct {
		name                 string
		voteCount, voteCells int
		baseCount, baseCells int
		voteAge, baseAge     time.Duration
		want                 bool
	}{
		{"higher_score_fails_receivers", 2, 2, 3, 2, time.Minute, 24 * time.Hour, true},
		{"higher_score_fails_cells", 3, 1, 3, 2, time.Minute, 24 * time.Hour, true},
		{"same_score_higher_count_fails_cells", 4, 1, 3, 2, 24 * time.Hour, 24 * time.Hour, true},
		{"score_and_count_tie_fails_cells", 3, 1, 3, 2, 24 * time.Hour, 24 * time.Hour, true},
		{"receivers_cannot_pool", 2, 2, 2, 2, time.Minute, time.Minute, false},
		{"cells_cannot_pool", 3, 1, 3, 1, time.Minute, time.Minute, false},
		{"different_gates_cannot_pool", 3, 1, 2, 2, time.Minute, time.Minute, false},
		{"fresh_score_cannot_refresh_other_member", 2, 2, 3, 2, time.Minute, 20 * 24 * time.Hour, false},
		{"both_scores_ineligible", 3, 2, 3, 2, 20 * 24 * time.Hour, 20 * 24 * time.Hour, false},
		{"missing_vote_qualifying_base", 0, 2, 3, 2, time.Minute, time.Minute, true},
		{"both_members_missing", 0, 2, 0, 2, time.Minute, time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			rows := append(b13FamilyRows(vote, "N0A", tc.voteCount, tc.voteCells, 101, now.Add(-tc.voteAge)),
				b13FamilyRows(base, "N0B", tc.baseCount, tc.baseCells, 201, now.Add(-tc.baseAge))...)
			store := b13FamilyStore(t, rows, 8)
			before := store.StatsSnapshot()
			for _, calls := range [][]string{{vote, base}, {base, vote}} {
				if got := store.HasSFloorSupportFamily(calls, "20m", "CW", 3, now); got != tc.want {
					t.Errorf("slice order=%q independent member qualification=%t want %t", calls, got, tc.want)
				}
				if got := store.HasSFloorSupportFamilyKeyPair(calls[0], calls[1], "20m", "CW", 3, now); got != tc.want {
					t.Errorf("pair order=%q independent member qualification=%t want %t", calls, got, tc.want)
				}
			}
			if after := store.StatsSnapshot(); after != before || store.StaticContains(vote) || store.StaticContains(base) {
				t.Fatalf("within-horizon qualification changed retained observations or static recognition: before=%+v after=%+v", before, after)
			}
			for _, row := range rows {
				key := fmt.Sprintf("o|%s|20m|cw|%s", row.call, row.receiver)
				requireCustomSCPCleanupRaw(t, store, key, customSCPCleanupObservationBytes(row.seen.Unix(), row.cell))
			}
			assertCustomSCPCleanupCoupling(t, store)
		})
	}
}

func TestB13CustomSCPFamilyEmptyAndRepeatedMembers(t *testing.T) {
	var nilStore *CustomSCPStore
	now := time.Now().UTC().Truncate(time.Second)
	if nilStore.HasSFloorSupportFamily([]string{"W1AW"}, "20m", "CW", 3, now) ||
		nilStore.HasSFloorSupportFamilyKeyPair("W1AW", "", "20m", "CW", 3, now) {
		t.Fatal("nil family store must return no support")
	}
	for _, count := range []int{2, 3} {
		t.Run(fmt.Sprintf("receivers_%d", count), func(t *testing.T) {
			store := b13FamilyStore(t, b13FamilyRows("W1AW", "N0A", count, 2, 101, now.Add(-time.Minute)), 8)
			if store.HasSFloorSupportFamily(nil, "20m", "CW", 3, now) ||
				store.HasSFloorSupportFamily([]string{"", "K1MISS"}, "20m", "CW", 3, now) ||
				store.HasSFloorSupportFamilyKeyPair("", "", "20m", "CW", 3, now) {
				t.Fatal("empty or missing family identities must return no support")
			}
			for _, calls := range [][]string{{"W1AW", "W1AW"}, {"", "W1AW"}, {"W1AW", ""}} {
				want := count == 3
				if got := store.HasSFloorSupportFamily(calls, "20m", "CW", 3, now); got != want {
					t.Errorf("repeated/empty slice members cannot multiply receiver evidence: calls=%q got=%t want=%t", calls, got, want)
				}
				if got := store.HasSFloorSupportFamilyKeyPair(calls[0], calls[1], "20m", "CW", 3, now); got != want {
					t.Errorf("repeated/empty pair members cannot multiply receiver evidence: calls=%q got=%t want=%t", calls, got, want)
				}
			}
		})
	}
}

func TestB13CustomSCPFamilyVisitsExpiredMemberAfterSuccess(t *testing.T) {
	const vote, base = "W1AW/KH6", "W1AW"
	for _, api := range []string{"slice", "pair"} {
		t.Run(api, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			rows := append(b13FamilyRows(vote, "N0A", 3, 2, 101, now.Add(-time.Minute)),
				b13FamilyRows(base, "N0B", 3, 2, 201, now.Add(-59*24*time.Hour))...)
			store := b13FamilyStore(t, rows, 8)
			if store.StatsSnapshot().ObservationKeys != 2 || !store.HasSFloorSupportExact(vote, "20m", "CW", 3, now) {
				t.Fatal("fixture must start with two retained keys and an independently qualifying first member")
			}
			queryAt := now.Add(2 * 24 * time.Hour)
			var got bool
			if api == "slice" {
				got = store.HasSFloorSupportFamily([]string{vote, base}, "20m", "CW", 3, queryAt)
			} else {
				got = store.HasSFloorSupportFamilyKeyPair(vote, base, "20m", "CW", 3, queryAt)
			}
			if !got {
				t.Error("first member must still qualify independently at age two days")
			}
			if stats := store.StatsSnapshot(); stats.ObservationKeys != 1 || stats.ObservationSpotters != 3 {
				t.Fatalf("successful first member must not prevent the previously visited second member's 60-day horizon pruning: %+v", stats)
			}
			assertCustomSCPCleanupCoupling(t, store)
		})
	}
}

func TestB13CustomSCPExactQualificationRemainsSeparate(t *testing.T) {
	const vote, base = "W1AW/KH6", "W1AW"
	now := time.Now().UTC().Truncate(time.Second)
	store := b13FamilyStore(t, b13FamilyRows(vote, "N0A", 3, 2, 101, now.Add(-time.Minute)), 8)
	if !store.HasSFloorSupportExact(vote, "20m", "CW", 3, now) {
		t.Fatal("exact threshold three must remain independently satisfied")
	}
	if store.HasSFloorSupportFamily([]string{vote, base}, "20m", "CW", 4, now) ||
		store.HasSFloorSupportFamilyKeyPair(vote, base, "20m", "CW", 4, now) {
		t.Fatal("family threshold four must remain unsatisfied without vetoing exact qualification")
	}
}

func BenchmarkB13CustomSCPFamilyKeyPair(b *testing.B) {
	const vote, base = "W1AW/KH6", "W1AW"
	for _, tc := range []struct {
		name      string
		baseCells int
		want      bool
	}{{"later_member_qualifies", 2, true}, {"neither_qualifies", 1, false}} {
		b.Run(tc.name, func(b *testing.B) {
			now := time.Now().UTC().Truncate(time.Second)
			rows := append(b13FamilyRows(vote, "N0A", 64, 1, 101, now.Add(-time.Minute)),
				b13FamilyRows(base, "N0B", 64, tc.baseCells, 201, now.Add(-24*time.Hour))...)
			store := b13FamilyStore(b, rows, 64)
			if store.HasSFloorSupportExact(vote, "20m", "CW", 64, now) ||
				store.HasSFloorSupportExact(base, "20m", "CW", 64, now) != tc.want {
				b.Fatal("benchmark must exercise two retained 64-receiver members with literal separate cell qualification")
			}
			if got := store.HasSFloorSupportFamilyKeyPair(vote, base, "20m", "CW", 64, now); got != tc.want {
				b.Fatalf("family qualification=%t want %t before timing", got, tc.want)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := store.HasSFloorSupportFamilyKeyPair(vote, base, "20m", "CW", 64, now); got != tc.want {
					b.Fatalf("family qualification=%t want %t during timing", got, tc.want)
				}
			}
		})
	}
}

func b14CheckReceiverMinimum(t *testing.T, store *CustomSCPStore, requirements CustomSCPRequirements, minimum int, want bool, now time.Time) {
	t.Helper()
	for _, query := range []struct {
		name string
		got  bool
	}{
		{"default", store.HasRecentSupport("W1AW/KH6", "20m", "CW", minimum, now)},
		{"consumer_requirements", store.HasRecentSupportWithRequirements("W1AW/KH6", "20m", "CW", minimum, requirements, now)},
		{"exact_floor", store.HasSFloorSupportExact("W1AW/KH6", "20m", "CW", minimum, now)},
		{"family_pair", store.HasSFloorSupportFamilyKeyPair("W1AW/KH6", "W1AW", "20m", "CW", minimum, now)},
		{"family_slice", store.HasSFloorSupportFamily([]string{"W1AW/KH6", "W1AW"}, "20m", "CW", minimum, now)},
	} {
		if query.got != want {
			t.Errorf("%s min=%d cap=%d qualification=%t want %t; storage capacity cannot lower the required receiver count", query.name, minimum, store.opts.MaxSpottersPerKey, query.got, want)
		}
	}
}

func TestB14CustomSCPImpossibleReceiverMinimumNeverCoerces(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	rows := append(b13FamilyRows("W1AW/KH6", "N0A", 3, 2, 101, now.Add(-time.Minute)),
		b13FamilyRows("W1AW", "N0B", 3, 2, 201, now.Add(-time.Minute))...)
	store := b13FamilyStore(t, rows, 3)
	before := store.StatsSnapshot()
	requirements := CustomSCPRequirements{MinScore: 5, MinH3Cells: 2}
	checkRaw := func() {
		t.Helper()
		for _, row := range rows {
			key := fmt.Sprintf("o|%s|20m|cw|%s", row.call, row.receiver)
			requireCustomSCPCleanupRaw(t, store, key, customSCPCleanupObservationBytes(row.seen.Unix(), row.cell))
		}
	}
	checkRaw()
	for _, query := range []struct {
		minimum int
		want    bool
	}{{3, true}, {4, false}, {0, true}, {-1, true}, {4, false}, {3, true}} {
		b14CheckReceiverMinimum(t, store, requirements, query.minimum, query.want, now)
	}
	// Both members have three disjoint receivers, but none has four. Six
	// retained family observations cannot pool into an impossible per-key gate.
	for _, count := range []int{
		store.RecentSupportCount("W1AW/KH6", "20m", "CW", now),
		store.RecentSupportCountNormalized("W1AW/KH6", "20m", "CW", now),
		store.RecentSupportCountWithRequirements("W1AW/KH6", "20m", "CW", requirements, now),
		store.RecentSupportCountNormalizedWithRequirements("W1AW/KH6", "20m", "CW", requirements, now),
	} {
		if count != 3 {
			t.Errorf("an impossible Boolean request must not erase the retained eligible partial count: got=%d want=3", count)
		}
	}
	checkRaw()
	if after := store.StatsSnapshot(); after != before || store.StaticContains("W1AW/KH6") || store.StaticContains("W1AW") {
		t.Errorf("capacity refusal mutated observation-only retained history: before=%+v after=%+v", before, after)
	}
	assertCustomSCPCleanupCoupling(t, store)
}

func TestB14CustomSCPNonpositiveMinimumStillMeansTwo(t *testing.T) {
	for _, tc := range []struct {
		count, cap int
		want       bool
	}{{1, 1, false}, {1, 3, false}, {2, 3, true}} {
		t.Run(fmt.Sprintf("receivers_%d_cap_%d", tc.count, tc.cap), func(t *testing.T) {
			store := newCustomSCPFreshnessStore(t, CustomSCPOptions{
				MaxSpottersPerKey: tc.cap, CoreMinScore: 5, CoreMinH3Cells: 1,
				SFloorMinScore: 5, SFloorExactMinH3Cells: 1, SFloorFamilyMinH3Cells: 1,
			})
			now := time.Now().UTC().Truncate(time.Second)
			for i := 0; i < tc.count; i++ {
				store.recordObservation("W1AW/KH6", "20m", "CW", fmt.Sprintf("N0A%03d", i), 101, 0, false, now.Add(-time.Minute))
			}
			before := store.StatsSnapshot()
			for _, minimum := range []int{0, -1} {
				b14CheckReceiverMinimum(t, store, CustomSCPRequirements{MinScore: 5, MinH3Cells: 1}, minimum, tc.want, now)
			}
			if after := store.StatsSnapshot(); after != before {
				t.Errorf("default-minimum queries changed retained population: before=%+v after=%+v", before, after)
			}
			assertCustomSCPCleanupCoupling(t, store)
		})
	}
}

func TestB14Delta2HistoryValidationRequiresDistanceTwo(t *testing.T) {
	// Insertions/deletions cost one character in all existing call models.
	// Verify the real gate, rather than assuming Morse/Baudot scaling can
	// make a two-character truncation reachable at maximum distance one.
	for _, tc := range []struct{ mode, model string }{
		{"CW", "plain"}, {"CW", "morse"}, {"RTTY", "plain"}, {"RTTY", "baudot"},
	} {
		t.Run(tc.mode+"_"+tc.model, func(t *testing.T) {
			if distance := CallDistance("K1ABC", "K1ABCDE", tc.mode, tc.model, tc.model); distance != 2 {
				t.Fatalf("two added characters must cost two edits: %d", distance)
			}
			settings := CorrectionSettings{
				MinConsensusReports: 3, MinAdvantage: 1, MinConfidencePercent: 50,
				DistanceModelCW: tc.model, DistanceModelRTTY: tc.model,
				FamilyPolicy: CorrectionFamilyPolicy{Configured: true, TruncationEnabled: true,
					TruncationMaxLengthDelta: 2, TruncationMinShorterLength: 3, TruncationAllowPrefix: true},
				TruncationAdvantagePolicy:    CorrectionTruncationAdvantagePolicy{Configured: true},
				TruncationDelta2RailsEnabled: true, TruncationDelta2RequireCandidateValidated: true,
			}
			for _, maximum := range []int{1, 2} {
				settings.MaxEditDistance = maximum
				got := EvaluateResolverPrimaryGates("K1ABC", "K1ABCDE", "20m", tc.mode,
					0, 3, 100, 0, 3000, settings, time.Now().UTC(), ResolverPrimaryGateOptions{})
				want := "max_edit_distance"
				if maximum == 2 {
					want = "truncation_delta2_candidate_unvalidated"
				}
				if got.Allow || got.Reason != want {
					t.Fatalf("max distance %d must reach only its eligible gate: %+v want reason=%s", maximum, got, want)
				}
			}
		})
	}
}

func b15RequireStaticRaw(t *testing.T, db *pebble.DB, want map[string][]byte) {
	t.Helper()
	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			t.Error(err)
		}
	}()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		expected, ok := want[key]
		if !ok || !bytes.Equal(iter.Value(), expected) {
			t.Errorf("static eligibility must preserve exact persisted input: key=%q got=%x want=%x", key, iter.Value(), expected)
		}
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	if count != len(want) {
		t.Errorf("static eligibility changed persisted population: got %d keys want %d", count, len(want))
	}
}

// Literal static-only input does not acquire observation support through an
// admission helper. Inspect all bytes before the production decoder opens them.
func b15StaticOnlyStore(t *testing.T, stamps map[string]int64) (*CustomSCPStore, map[string][]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scp")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	raw := make(map[string][]byte, len(stamps))
	for call, stamp := range stamps {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(stamp))
		key := "m|" + call
		raw[key] = value
		if err := db.Set([]byte(key), value, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	b15RequireStaticRaw(t, db, raw)
	err = db.Close()
	closed = true
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenCustomSCPStore(CustomSCPOptions{Path: path, HorizonDays: 60, StaticHorizonDays: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if stats := store.StatsSnapshot(); stats.StaticCalls != len(stamps) || stats.ObservationKeys != 0 || stats.ObservationSpotters != 0 {
		t.Fatalf("literal fixture must retain static input without independent observation support: %+v", stats)
	}
	return store, raw
}

func b15ReopenStaticStore(t *testing.T, store *CustomSCPStore) *CustomSCPStore {
	t.Helper()
	options := store.opts
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCustomSCPStore(options)
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

func TestB15CustomSCPStaticEligibilityStoredSeconds(t *testing.T) {
	// A future marker survives load's lower-horizon pruning. Explicit clocks
	// test both inclusive bounds at the persisted second, independently of now.
	stamp := time.Now().UTC().Truncate(time.Second).Add(2 * time.Hour)
	store, raw := b15StaticOnlyStore(t, map[string]int64{"K1CLOCK": stamp.Unix()})
	queries := []struct {
		name string
		now  time.Time
		want int
	}{
		{"before_one_second", stamp.Add(-time.Second), 0},
		{"before_last_nanosecond", stamp.Add(-time.Nanosecond), 0},
		{"at_stored_second", stamp, 1},
		{"after_within_stored_second", stamp.Add(999 * time.Millisecond), 1},
		{"after_one_second", stamp.Add(time.Second), 1},
		{"inclusive_lower_horizon", stamp.Add(24 * time.Hour), 1},
		{"lower_horizon_same_second", stamp.Add(24*time.Hour + 999*time.Millisecond), 1},
		{"past_lower_horizon_second", stamp.Add(24*time.Hour + time.Second), 0},
		{"before_again", stamp.Add(-time.Nanosecond), 0},
	}
	check := func(store *CustomSCPStore) {
		t.Helper()
		before := store.StatsSnapshot()
		for _, query := range queries {
			if got := store.StaticCallCount(query.now); got != query.want {
				t.Errorf("%s static count=%d want %d at %s for stored second %d", query.name, got, query.want, query.now.Format(time.RFC3339Nano), stamp.Unix())
			}
			if got, want := store.staticContainsAt("K1CLOCK", query.now), query.want == 1; got != want {
				t.Errorf("%s static membership=%t want %t at %s for stored second %d", query.name, got, want, query.now.Format(time.RFC3339Nano), stamp.Unix())
			}
		}
		if store.StaticContains(" k1clock ") {
			t.Error("process-clock recognition must exclude a retained future static marker")
		}
		if after := store.StatsSnapshot(); after != before || after.StaticCalls != 1 {
			t.Errorf("eligibility clocks must not reclaim retained static input: before=%+v after=%+v", before, after)
		}
		b15RequireStaticRaw(t, store.db, raw)
		assertCustomSCPCleanupCoupling(t, store)
	}
	check(store)
	check(b15ReopenStaticStore(t, store))
}

func TestB15CustomSCPStaticZeroClockAndNil(t *testing.T) {
	var nilStore *CustomSCPStore
	if nilStore.StaticContains("K1CLOCK") || nilStore.StaticCallCount(time.Time{}) != 0 || nilStore.StaticCallCount(time.Now().UTC()) != 0 {
		t.Fatal("nil static store must provide no recognition or count")
	}
	if nilStore.staticContainsAt("K1CLOCK", time.Time{}) || nilStore.staticContainsAt("K1CLOCK", time.Now().UTC()) {
		t.Fatal("nil static store must provide no explicit-clock recognition")
	}
	now := time.Now().UTC().Truncate(time.Second)
	store, raw := b15StaticOnlyStore(t, map[string]int64{
		"K1PAST": now.Add(-time.Hour).Unix(), "K1FUTURE": now.Add(2 * time.Hour).Unix(),
	})
	check := func(store *CustomSCPStore) {
		t.Helper()
		before := store.StatsSnapshot()
		if got := store.StaticCallCount(time.Time{}); got != 1 {
			t.Errorf("zero query clock must use current eligibility, not the two retained markers: got %d want 1", got)
		}
		if !store.StaticContains(" k1past ") || store.StaticContains("K1FUTURE") || store.StaticContains("") || store.StaticContains("K1MISSING") {
			t.Error("process-clock recognition must distinguish existing, future, empty and missing calls")
		}
		for _, clock := range []time.Time{time.Time{}, now} {
			for _, query := range []struct {
				call string
				want bool
			}{{"K1PAST", true}, {" k1past ", true}, {"K1FUTURE", false}, {"", false}, {" \t ", false}, {"K1MISSING", false}} {
				if got := store.staticContainsAt(query.call, clock); got != query.want {
					t.Errorf("static membership for %q at %s=%t want %t", query.call, clock.Format(time.RFC3339Nano), got, query.want)
				}
			}
		}
		if after := store.StatsSnapshot(); after != before || after.StaticCalls != 2 {
			t.Errorf("zero-clock eligibility changed retained static population: before=%+v after=%+v", before, after)
		}
		b15RequireStaticRaw(t, store.db, raw)
	}
	check(store)
	check(b15ReopenStaticStore(t, store))
}

func TestB15CustomSCPStaticConcurrentImmutableReaders(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	current := now.Add(-time.Hour)
	future := now.Add(2 * time.Hour)
	store, raw := b15StaticOnlyStore(t, map[string]int64{
		"K1CURRENT": current.Unix(), "K1FUTURE": future.Unix(), "K1BOUND": current.Unix(),
	})
	type staticQuery struct {
		call      string
		now       time.Time
		want      bool
		wantCount int
	}
	queries := []staticQuery{
		{"K1CURRENT", now, true, 2},
		{"K1FUTURE", now, false, 2},
		{"K1FUTURE", future, true, 3},
		{"K1BOUND", current.Add(24 * time.Hour), true, 3},
		{"K1BOUND", current.Add(24*time.Hour + 999*time.Millisecond), true, 3},
		{"K1BOUND", current.Add(24*time.Hour + time.Second), false, 1},
		{"K1BOUND", current.Add(-time.Nanosecond), false, 0},
	}
	for _, query := range queries {
		if normalized := NormalizeCallsign(query.call); normalized != query.call {
			t.Fatalf("concurrent fixture call %q is not fully normalized: %q", query.call, normalized)
		}
	}
	unchangedQueries := append([]staticQuery(nil), queries...)
	check := func(store *CustomSCPStore) {
		t.Helper()
		before := store.StatsSnapshot()
		const workers, passes = 8, 128
		start := make(chan struct{})
		failures := make(chan error, workers)
		var readers sync.WaitGroup
		readers.Add(workers)
		for worker := 0; worker < workers; worker++ {
			go func(worker int) {
				defer readers.Done()
				<-start
				for pass := 0; pass < passes; pass++ {
					for _, query := range queries {
						if got := store.staticContainsAt(query.call, query.now); got != query.want {
							failures <- fmt.Errorf("reader %d pass %d static membership for %s at %s=%t want %t", worker, pass, query.call, query.now.Format(time.RFC3339Nano), got, query.want)
							return
						}
						if got := store.StaticCallCount(query.now); got != query.wantCount {
							failures <- fmt.Errorf("reader %d pass %d static count at %s=%d want %d", worker, pass, query.now.Format(time.RFC3339Nano), got, query.wantCount)
							return
						}
					}
				}
			}(worker)
		}
		close(start)
		readers.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
		for i, query := range queries {
			if query != unchangedQueries[i] {
				t.Errorf("concurrent query %d mutated: before=%+v after=%+v", i, unchangedQueries[i], query)
			}
		}
		if after := store.StatsSnapshot(); after != before || after.StaticCalls != 3 {
			t.Errorf("concurrent eligibility queries changed retained state: before=%+v after=%+v", before, after)
		}
		b15RequireStaticRaw(t, store.db, raw)
		assertCustomSCPCleanupCoupling(t, store)
	}
	check(store)
	check(b15ReopenStaticStore(t, store))
}

func TestB15CustomSCPFutureVStaticAdmissionRemainsRetained(t *testing.T) {
	store := newCustomSCPFreshnessStore(t, CustomSCPOptions{HorizonDays: 60, StaticHorizonDays: 1})
	stamp := time.Now().UTC().Truncate(time.Second).Add(2 * time.Hour)
	source := NewSpot("K1LEARN", "N0AAA", 14020, "CW")
	source.Confidence, source.Time = "V", stamp
	store.RecordSpot(source)
	check := func(store *CustomSCPStore) {
		t.Helper()
		before := store.StatsSnapshot()
		if before.StaticCalls != 1 || before.ObservationSpotters != 1 {
			t.Fatalf("existing admission must retain the future V timestamp normally: %+v", before)
		}
		if store.StaticContains("K1LEARN") || store.StaticCallCount(time.Time{}) != 0 {
			t.Error("a learned future marker must remain retained without providing current static recognition")
		}
		if got := store.RecentSupportCount("K1LEARN", "20m", "CW", time.Now().UTC()); got != 0 {
			t.Errorf("future observation remains a separate ineligible population: got %d want 0", got)
		}
		requireCustomSCPCleanupRaw(t, store, "m|K1LEARN", customSCPCleanupStaticBytes(stamp.Unix()))
		requireCustomSCPCleanupRaw(t, store, "o|K1LEARN|20m|cw|N0AAA", customSCPCleanupObservationBytes(stamp.Unix(), 0))
		if after := store.StatsSnapshot(); after != before {
			t.Errorf("recognition queries changed future admitted ownership: before=%+v after=%+v", before, after)
		}
		assertCustomSCPCleanupCoupling(t, store)
	}
	check(store)
	check(b15ReopenStaticStore(t, store))
}

func BenchmarkB15CustomSCPStaticContains(b *testing.B) {
	for _, tc := range []struct {
		name string
		age  time.Duration
		want bool
	}{{"current", time.Hour, true}, {"future", -2 * time.Hour, false}} {
		b.Run(tc.name, func(b *testing.B) {
			stamp := time.Now().UTC().Add(-tc.age).Unix()
			store := &CustomSCPStore{
				opts:   sanitizeCustomSCPOptions(CustomSCPOptions{StaticHorizonDays: 1}),
				static: map[string]int64{"K1BENCH": stamp},
			}
			if got := store.StaticContains("K1BENCH"); got != tc.want {
				b.Fatalf("warm static query=%t want %t", got, tc.want)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := store.StaticContains("K1BENCH"); got != tc.want {
					b.Fatalf("static query changed during measurement: got %t want %t", got, tc.want)
				}
			}
		})
	}
}
