package pathreliability

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestVOACAPIncrementalCacheUninitializedPreseedRecovery(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 1), incrementalCacheEntry(now, now.Add(-2*time.Hour), 20), nil)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 2), incrementalCacheEntry(now, now, 20), nil)
	writeTestVOACAPForecastCacheRawValue(t, store, []byte(voacapForecastCacheEntryKeyPrefix+"malformed"), []byte("{"))
	if snap := store.IndexSnapshot(); !snap.Attached || snap.Ready || snap.Entries != 0 {
		t.Fatalf("preseed must remain uninitialized: %+v", snap)
	}
	if keys := incrementalRawCacheKeys(t, store); len(keys) != 3 {
		t.Fatalf("raw preseed size=%d, want 3 before any repair", len(keys))
	}
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 3), incrementalCacheEntry(now, now, 20), cfg, now); err != nil {
		t.Fatalf("first completion recovery: %v", err)
	}
	assertIncrementalRawCells(t, store, []uint16{2, 3})
	assertIncrementalIndexReady(t, store, 2)
	reopened := reopenVOACAPRetentionStore(t, store)
	assertIncrementalRawCells(t, reopened, []uint16{2, 3})
}

func TestVOACAPIncrementalCacheHydrationValidationMatrix(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	for _, test := range []struct {
		name   string
		mutate func(*voacapForecastCacheDiskRecord)
		reason voacapForecastCacheRejectReason
	}{
		{"schema", func(r *voacapForecastCacheDiskRecord) { r.SchemaVersion = 0 }, voacapForecastCacheRejectStaleGeneration},
		{"model", func(r *voacapForecastCacheDiskRecord) { r.ModelGeneration = 0 }, voacapForecastCacheRejectStaleGeneration},
		{"user_cell_zero", func(r *voacapForecastCacheDiskRecord) { r.Key.UserCell = 0 }, voacapForecastCacheRejectInvalid},
		{"dx_cell_zero", func(r *voacapForecastCacheDiskRecord) { r.Key.DXCell = 0 }, voacapForecastCacheRejectInvalid},
		{"band_empty", func(r *voacapForecastCacheDiskRecord) { r.Key.Band = "" }, voacapForecastCacheRejectInvalid},
		{"frequency_zero", func(r *voacapForecastCacheDiskRecord) { r.Key.FrequencyKHz = 0 }, voacapForecastCacheRejectInvalid},
		{"year_zero", func(r *voacapForecastCacheDiskRecord) { r.Key.Year = 0 }, voacapForecastCacheRejectInvalid},
		{"month_zero", func(r *voacapForecastCacheDiskRecord) { r.Key.Month = 0 }, voacapForecastCacheRejectInvalid},
		{"month_thirteen", func(r *voacapForecastCacheDiskRecord) { r.Key.Month = 13 }, voacapForecastCacheRejectInvalid},
		{"ssn_zero", func(r *voacapForecastCacheDiskRecord) { r.Key.SSN = 0 }, voacapForecastCacheRejectInvalid},
		{"direction", func(r *voacapForecastCacheDiskRecord) { r.Key.Direction = "receive" }, voacapForecastCacheRejectInvalid},
		{"key_value_mismatch", func(r *voacapForecastCacheDiskRecord) { r.Key.DXCell = 2 }, voacapForecastCacheRejectInvalid},
		{"stored_time_zero", func(r *voacapForecastCacheDiskRecord) { r.StoredAtUnixNS = 0 }, voacapForecastCacheRejectInvalid},
		{"window_time_zero", func(r *voacapForecastCacheDiskRecord) { r.WindowUnixNS = 0 }, voacapForecastCacheRejectInvalid},
		{"empty_records", func(r *voacapForecastCacheDiskRecord) { r.Records = nil }, voacapForecastCacheRejectInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 1), incrementalCacheEntry(now, now, 20), test.mutate)
			writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 100), incrementalCacheEntry(now, now, 20), nil)
			if len(incrementalRawCacheKeys(t, store)) != 2 {
				t.Fatal("malformed fixture missing before hydration")
			}
			loaded, stats, err := store.loadCurrent(now, 112, cfg)
			if err != nil || len(loaded) != 1 || stats.Loaded != 1 || stats.Pruned != 1 || stats.Expired != 0 || stats.StaleWindow != 0 || stats.Overflow != 0 {
				t.Fatalf("validation result: loaded=%d stats=%+v error=%v", len(loaded), stats, err)
			}
			if test.reason == voacapForecastCacheRejectInvalid && (stats.Invalid != 1 || stats.StaleGeneration != 0) || test.reason == voacapForecastCacheRejectStaleGeneration && (stats.StaleGeneration != 1 || stats.Invalid != 0) {
				t.Fatalf("wrong validation classification: %+v", stats)
			}
			assertIncrementalRawCells(t, store, []uint16{100})
			assertIncrementalIndexReady(t, store, 1)
		})
	}
	// A syntactically valid record with a different *valid* SSN must carry the
	// matching physical key, rather than accidentally testing key corruption.
	t.Run("valid_other_ssn", func(t *testing.T) {
		store := openTestVOACAPForecastCacheStore(t)
		defer closeTestVOACAPForecastCacheStore(t, store)
		key := testVOACAPForecastCacheKey(now, 1)
		key.ssn = 113
		record := diskRecordFromForecastCache(key, incrementalCacheEntry(now, now, 20))
		value, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		writeTestVOACAPForecastCacheRawValue(t, store, []byte("voacapfc/v1/e/0001/0001/20m/014100/2026/06/0113/bidirectional"), value)
		_, stats, err := store.loadCurrent(now, 112, cfg)
		if err != nil || stats.Pruned != 1 || stats.StaleWindow != 1 || stats.Invalid != 0 {
			t.Fatalf("SSN generation classification: stats=%+v error=%v", stats, err)
		}
		assertIncrementalRawCells(t, store, nil)
		assertIncrementalIndexReady(t, store, 0)
	})
}

func TestVOACAPIncrementalCacheActualHourGap(t *testing.T) {
	start := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.CacheTTLSeconds = 6 * 3600
	cfg.ForecastHours = 4
	for _, throughGap := range []bool{false, true} {
		t.Run(fmt.Sprintf("operation_at_missing_21=%t", throughGap), func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			if err := store.storeForecast(testVOACAPForecastCacheKey(start, 1), incrementalCacheEntry(start, start, 20, 22), cfg, start); err != nil {
				t.Fatal(err)
			}
			if throughGap {
				at := start.Add(time.Hour)
				if err := store.storeForecast(testVOACAPForecastCacheKey(at, 2), incrementalCacheEntry(at, at, 21, 22), cfg, at); err != nil {
					t.Fatal(err)
				}
				assertIncrementalRawCells(t, store, []uint16{2})
			}
			at := start.Add(2 * time.Hour)
			if err := store.storeForecast(testVOACAPForecastCacheKey(at, 3), incrementalCacheEntry(at, at, 22), cfg, at); err != nil {
				t.Fatal(err)
			}
			want := []uint16{1, 3}
			if throughGap {
				want = []uint16{2, 3}
			}
			// Read disk before a hydration pass can conceal intermediate-hour pruning.
			assertIncrementalRawCells(t, store, want)
			assertIncrementalIndexReady(t, store, 2)
			reopened := reopenVOACAPRetentionStore(t, store)
			assertIncrementalRawCells(t, reopened, want)
		})
	}
}

func TestVOACAPIncrementalCacheClockBoundaries(t *testing.T) {
	start := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		storedAt  time.Time
		operation time.Time
		ttl       int
		horizon   int
		wantSeed  bool
	}{
		{"ttl_equal", start, start.Add(600 * time.Second), 600, 4, true},
		{"ttl_plus_1ns", start, start.Add(600*time.Second + time.Nanosecond), 600, 4, false},
		{"future_storage_equal_1min", start.Add(time.Minute), start, 3600, 4, true},
		{"future_storage_plus_1ns", start.Add(time.Minute + time.Nanosecond), start, 3600, 4, false},
		{"window_start_equal", start, start, 6 * 3600, 2, true},
		{"window_before_start_1ns", start, start.Add(-time.Nanosecond), 6 * 3600, 2, false},
		{"window_before_end_1ns", start, start.Add(2*time.Hour - time.Nanosecond), 6 * 3600, 2, true},
		{"window_end_equal", start, start.Add(2 * time.Hour), 6 * 3600, 2, false},
		{"backward_clock_within_skew", start.Add(40 * time.Minute), start.Add(39 * time.Minute), 6 * 3600, 4, true},
		{"backward_clock_exceeds_skew", start.Add(40 * time.Minute), start.Add(39*time.Minute - time.Nanosecond), 6 * 3600, 4, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testVOACAPFallbackConfig().VOACAPFallback
			cfg.CacheTTLSeconds, cfg.ForecastHours = tc.ttl, tc.horizon
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			entry := incrementalCacheEntry(start, tc.storedAt, 20, 21, 22, 23)
			// Seed at its storage time when that time is admissible; the operation
			// below exercises a warmed index, including backward-clock repair.
			seedAt := start
			if tc.storedAt.After(start.Add(time.Minute)) && tc.storedAt.Before(start.Add(time.Hour)) {
				seedAt = tc.storedAt
			}
			if err := store.storeForecast(testVOACAPForecastCacheKey(start, 1), entry, cfg, seedAt); err != nil {
				t.Fatalf("seed: %v", err)
			}
			incoming := incrementalCacheEntry(tc.operation.Truncate(time.Hour), tc.operation, tc.operation.Hour())
			if err := store.storeForecast(testVOACAPForecastCacheKey(tc.operation, 2), incoming, cfg, tc.operation); err != nil {
				t.Fatalf("boundary operation: %v", err)
			}
			want := []uint16{2}
			if tc.wantSeed {
				want = []uint16{1, 2}
			}
			assertIncrementalRawCells(t, store, want)
			assertIncrementalIndexReady(t, store, len(want))
		})
	}
}

func TestVOACAPIncrementalCacheCalendarAndContextChanges(t *testing.T) {
	tests := []struct {
		name      string
		start     time.Time
		operation time.Time
		ssn       int
		wantSeed  bool
	}{
		{"midnight_same_month", time.Date(2026, 6, 8, 23, 0, 0, 0, time.UTC), time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC), 112, true},
		{"month_boundary", time.Date(2026, 6, 30, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), 112, false},
		{"year_boundary", time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), 112, false},
		{"ssn_change", time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC), time.Date(2026, 6, 8, 20, 1, 0, 0, time.UTC), 113, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testVOACAPFallbackConfig().VOACAPFallback
			cfg.CacheTTLSeconds, cfg.ForecastHours = 6*3600, 4
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			if err := store.storeForecast(testVOACAPForecastCacheKey(tc.start, 1), incrementalCacheEntry(tc.start, tc.start, tc.start.Hour(), tc.operation.Hour()), cfg, tc.start); err != nil {
				t.Fatal(err)
			}
			key := testVOACAPForecastCacheKey(tc.operation, 2)
			key.ssn = tc.ssn
			if err := store.storeForecast(key, incrementalCacheEntry(tc.operation.Truncate(time.Hour), tc.operation, tc.operation.Hour()), cfg, tc.operation); err != nil {
				t.Fatal(err)
			}
			want := []uint16{2}
			if tc.wantSeed {
				want = []uint16{1, 2}
			}
			assertIncrementalRawCells(t, store, want)
			assertIncrementalIndexReady(t, store, len(want))
		})
	}
	for _, change := range []string{"ttl", "horizon", "capacity"} {
		t.Run("config_"+change, func(t *testing.T) {
			start := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
			cfg := testVOACAPFallbackConfig().VOACAPFallback
			cfg.CacheTTLSeconds, cfg.ForecastHours = 6*3600, 4
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			for cell := CellID(1); cell <= 4; cell++ {
				entry := incrementalCacheEntry(start, start.Add(time.Duration(cell)*time.Second), 20, 21, 22, 23)
				if err := store.storeForecast(testVOACAPForecastCacheKey(start, cell), entry, cfg, start); err != nil {
					t.Fatal(err)
				}
			}
			at := start.Add(time.Hour)
			switch change {
			case "ttl":
				cfg.CacheTTLSeconds = 600
			case "horizon":
				cfg.ForecastHours = 1
			case "capacity":
				cfg.MaxCacheEntries = 2
			}
			if err := store.storeForecast(testVOACAPForecastCacheKey(at, 5), incrementalCacheEntry(at, at, 21), cfg, at); err != nil {
				t.Fatal(err)
			}
			want := []uint16{5}
			if change == "capacity" {
				want = []uint16{4, 5}
			}
			assertIncrementalRawCells(t, store, want)
			assertIncrementalIndexReady(t, store, len(want))
		})
	}
}

func TestVOACAPIncrementalCacheMarshalFailurePreservesOld(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	for name, invalid := range map[string]float64{"nan": math.NaN(), "positive_infinity": math.Inf(1), "negative_infinity": math.Inf(-1)} {
		t.Run(name, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			key := testVOACAPForecastCacheKey(now, 1)
			old := incrementalCacheEntry(now, now, 20)
			if err := store.storeForecast(key, old, cfg, now); err != nil {
				t.Fatal(err)
			}
			var commits int
			incrementalSetFaults(store, &voacapForecastCacheFaults{commit: func(batch *pebble.Batch, opts *pebble.WriteOptions) error {
				commits++
				return batch.Commit(opts)
			}})
			bad := incrementalCacheEntry(now, now.Add(time.Second), 20)
			bad.forecast.Records[0].ReqSNRReliability = invalid
			if err := store.storeForecast(key, bad, cfg, now); err == nil {
				t.Fatal("non-JSON numerical value must fail encoding")
			}
			if commits != 0 {
				t.Fatalf("encoding failure attempted %d commits", commits)
			}
			records := assertIncrementalRawCells(t, store, []uint16{1})
			for _, record := range records {
				if record.StoredAtUnixNS != old.storedAt.UnixNano() || record.Records[0].ReqSNRReliability != 0 {
					t.Fatalf("encoding failure replaced prior disk record: %+v", record)
				}
			}
			assertIncrementalIndexReady(t, store, 1)
		})
	}
}

func TestVOACAPIncrementalCacheEncodableInvalidReplacementDeletesOld(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	for _, failure := range []string{"empty_records", "missing_current_hour", "expired_storage", "future_storage", "future_window"} {
		t.Run(failure, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			for cell := CellID(1); cell <= 2; cell++ {
				if err := store.storeForecast(testVOACAPForecastCacheKey(now, cell), incrementalCacheEntry(now, now, 20), cfg, now); err != nil {
					t.Fatal(err)
				}
			}
			bad := incrementalCacheEntry(now, now, 20)
			switch failure {
			case "empty_records":
				bad.forecast.Records = nil
			case "missing_current_hour":
				bad.forecast.Records[0].HourUTC = 21
			case "expired_storage":
				bad.storedAt = now.Add(-time.Hour - time.Nanosecond)
			case "future_storage":
				bad.storedAt = now.Add(time.Minute + time.Nanosecond)
			case "future_window":
				bad.forecast.WindowStartUTC = now.Add(time.Hour)
			}
			if err := store.storeForecast(testVOACAPForecastCacheKey(now, 1), bad, cfg, now); err != nil {
				t.Fatalf("encodable invalid replacement: %v", err)
			}
			assertIncrementalRawCells(t, store, []uint16{2})
			assertIncrementalIndexReady(t, store, 1)
		})
	}
}

func TestVOACAPIncrementalCachePreservesDecoderRecordPolicy(t *testing.T) {
	// The restart cache validates representation, generation and hour eligibility.
	// It must not introduce an unrelated RF-policy gate for record numbers.
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	entry := incrementalCacheEntry(now, now, 20, 25)
	entry.forecast.Records[0].FrequencyMHz = 0
	entry.forecast.Records[0].ReqSNRReliability = 2
	entry.forecast.Records[0].FT8SNRDB = 300
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 1), entry, cfg, now); err != nil {
		t.Fatal(err)
	}
	records := assertIncrementalRawCells(t, store, []uint16{1})
	for _, record := range records {
		if len(record.Records) != 2 || record.Records[0].FrequencyMHz != 0 || record.Records[0].ReqSNRReliability != 2 || record.Records[0].FT8SNRDB != 300 || record.Records[1].HourUTC != 25 {
			t.Fatalf("restart cache changed pre-existing record-admission semantics: %+v", record)
		}
	}
	loaded, stats, err := store.loadCurrent(now, 112, cfg)
	if err != nil || len(loaded) != 1 || stats.Pruned != 0 {
		t.Fatalf("decoder parity after hydration: loaded=%d stats=%+v error=%v", len(loaded), stats, err)
	}
}

func TestVOACAPIncrementalCacheCommitFailureRecovery(t *testing.T) {
	now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 2
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied_before_error=%t", applied), func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			for cell := CellID(1); cell <= 2; cell++ {
				if err := store.storeForecast(testVOACAPForecastCacheKey(now, cell), incrementalCacheEntry(now, now.Add(time.Duration(int(cell)-3)*time.Second), 20), cfg, now); err != nil {
					t.Fatal(err)
				}
			}
			injected := errors.New("injected atomic commit result")
			var commits int
			incrementalSetFaults(store, &voacapForecastCacheFaults{commit: func(batch *pebble.Batch, opts *pebble.WriteOptions) error {
				commits++
				if applied {
					if err := batch.Commit(opts); err != nil {
						return err
					}
				}
				return injected
			}})
			if err := store.storeForecast(testVOACAPForecastCacheKey(now, 3), incrementalCacheEntry(now, now, 20), cfg, now); !errors.Is(err, injected) {
				t.Fatalf("commit error=%v, want injected failure", err)
			}
			if commits != 1 {
				t.Fatalf("ordinary replacement must use one atomic batch, commits=%d", commits)
			}
			want := []uint16{1, 2}
			if applied {
				want = []uint16{2, 3}
			}
			assertIncrementalRawCells(t, store, want)
			if snap := store.IndexSnapshot(); snap.Ready || !snap.Dirty {
				t.Fatalf("uncertain persistence result left metadata trusted: %+v", snap)
			}
			incrementalSetFaults(store, nil)
			at := now.Add(time.Second)
			if err := store.storeForecast(testVOACAPForecastCacheKey(at, 4), incrementalCacheEntry(now, at, 20), cfg, at); err != nil {
				t.Fatalf("next completion dirty recovery: %v", err)
			}
			want = []uint16{2, 4}
			if applied {
				want = []uint16{3, 4}
			}
			assertIncrementalRawCells(t, store, want)
			assertIncrementalIndexReady(t, store, 2)
			reopened := reopenVOACAPRetentionStore(t, store)
			assertIncrementalRawCells(t, reopened, want)
		})
	}
}

func incrementalCacheEntry(window, storedAt time.Time, hours ...int) voacapCacheEntry {
	entry := voacapCacheEntry{storedAt: storedAt.UTC(), forecast: VOACAPClosedForecast{WindowStartUTC: window.UTC()}}
	for _, hour := range hours {
		entry.forecast.Records = append(entry.forecast.Records, VOACAPHourlyForecast{FT8SNRDB: -14, VOACAPSNRDBHz: 16, HourUTC: hour, FrequencyMHz: 14.1})
	}
	return entry
}

func incrementalSetFaults(store *VOACAPForecastCacheStore, faults *voacapForecastCacheFaults) {
	store.mu.Lock()
	store.faults = faults
	store.mu.Unlock()
}

func assertIncrementalIndexReady(t *testing.T, store *VOACAPForecastCacheStore, want int) {
	t.Helper()
	snap := store.IndexSnapshot()
	if !snap.Attached || !snap.Ready || snap.Dirty || snap.Entries != want || snap.RetentionEntries != want || snap.EligibilityEntries != want {
		t.Fatalf("trusted index snapshot=%+v, want exactly %d owner/map/heap entries", snap, want)
	}
}

func incrementalRawCacheKeys(t *testing.T, store *VOACAPForecastCacheStore) []string {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	iter, err := store.db.NewIter(&pebble.IterOptions{LowerBound: []byte(voacapForecastCacheEntryKeyPrefix), UpperBound: voacapForecastCacheEntryKeyUpperBound()})
	if err != nil {
		t.Fatalf("raw iterator: %v", err)
	}
	defer iter.Close()
	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("raw iterator error: %v", err)
	}
	return keys
}

func assertIncrementalRawCells(t *testing.T, store *VOACAPForecastCacheStore, want []uint16) map[string]voacapForecastCacheDiskRecord {
	t.Helper()
	records := readRawVOACAPRetentionRecords(t, store)
	cells := make([]uint16, 0, len(records))
	for _, record := range records {
		cells = append(cells, record.Key.DXCell)
	}
	slices.Sort(cells)
	if !slices.Equal(cells, want) {
		t.Fatalf("raw disk cells=%v, want %v before hydration/recovery", cells, want)
	}
	return records
}
