// File role: Locks per-observation historical qualification independently of
// retained history, stored bytes, and consumer-specific score/cell gates.
package spot

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
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
