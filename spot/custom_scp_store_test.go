package spot

import (
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"
)

func assertCustomSCPInternerInvariant(t *testing.T, store *CustomSCPStore) {
	t.Helper()
	if store == nil {
		return
	}
	store.mu.RLock()
	defer store.mu.RUnlock()

	expected := make(map[string]int)
	for call := range store.static {
		expected[call]++
	}
	for key, entry := range store.entries {
		expected[key.call]++
		expected[key.band]++
		expected[key.bucket]++
		for _, spotter := range entry.spotters {
			expected[spotter.spotter]++
		}
	}
	expectedRefs := 0
	for _, refs := range expected {
		expectedRefs += refs
	}
	if store.interner.releaseMisses != 0 {
		t.Fatalf("expected no interner release misses, got %d", store.interner.releaseMisses)
	}
	if store.interner.totalRefs != expectedRefs {
		t.Fatalf("expected %d interner refs, got %d", expectedRefs, store.interner.totalRefs)
	}
	if len(store.interner.refs) != len(expected) {
		t.Fatalf("expected %d interned strings, got %d", len(expected), len(store.interner.refs))
	}
	for value, want := range expected {
		ref, ok := store.interner.refs[value]
		if !ok {
			t.Fatalf("missing interner ref for %q", value)
		}
		if ref.value != value {
			t.Fatalf("interner canonical value mismatch for %q: %q", value, ref.value)
		}
		if ref.refs != want {
			t.Fatalf("expected %d refs for %q, got %d", want, value, ref.refs)
		}
	}
	for value := range store.interner.refs {
		if _, ok := expected[value]; !ok {
			t.Fatalf("unexpected interner ref for %q", value)
		}
	}
}

func retainCustomSCPTestEntryLocked(store *CustomSCPStore, key customSCPKey, entry *customSCPEntry) customSCPKey {
	retainedKey := customSCPKey{
		call:   store.retainInternedStringLocked(key.call),
		band:   store.retainInternedStringLocked(key.band),
		bucket: store.retainInternedStringLocked(key.bucket),
	}
	entry.spotters = compactCustomSCPSpotters(entry.spotters)
	for i := range entry.spotters {
		entry.spotters[i].spotter = store.retainInternedStringLocked(entry.spotters[i].spotter)
	}
	store.entries[retainedKey] = entry
	return retainedKey
}

func customSCPTestSpotters(spotters map[string]customSCPSpotterObs) []customSCPSpotterEntry {
	entry := &customSCPEntry{spotters: makeCustomSCPSpotters(len(spotters))}
	for spotter, obs := range spotters {
		entryUpsertSpotter(entry, spotter, obs, len(spotters))
	}
	return compactCustomSCPSpotters(entry.spotters)
}

func requireCustomSCPEntryHasSpotter(t *testing.T, entry *customSCPEntry, spotter string) {
	t.Helper()
	if !entryHasSpotter(entry, spotter) {
		t.Fatalf("expected retained spotter %s", spotter)
	}
}

func requireCustomSCPEntryMissingSpotter(t *testing.T, entry *customSCPEntry, spotter string) {
	t.Helper()
	if entryHasSpotter(entry, spotter) {
		t.Fatalf("expected spotter %s to be absent", spotter)
	}
}

func retainCustomSCPTestStaticLocked(store *CustomSCPStore, call string, seenUnix int64) {
	retained := store.retainStaticCallLocked(call, seenUnix)
	store.upsertStaticExpiryLocked(retained, store.static[retained])
}

func freshCustomSCPTestTime() time.Time {
	return time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
}

func TestCustomSCPStoreCWAndRTTYSNRThresholds(t *testing.T) {
	opts := CustomSCPOptions{
		Path:           filepath.Join(t.TempDir(), "scp"),
		CoreMinScore:   1,
		CoreMinH3Cells: 1,
		MinSNRDBCW:     4,
		MinSNRDBRTTY:   3,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	callCW := "K1CW"
	callRTTY := "K1RY"
	band := "40m"

	// Below threshold: CW report=3 should be rejected when min_snr_db_cw=4.
	store.recordObservation(callCW, band, "CW", "N0AAA", 101, 3, true, now)
	if got := store.RecentSupportCount(callCW, band, "CW", now); got != 0 {
		t.Fatalf("expected no CW support below threshold, got %d", got)
	}

	// At threshold: CW report=4 should be accepted.
	store.recordObservation(callCW, band, "CW", "N0BBB", 102, 4, true, now)
	if got := store.RecentSupportCount(callCW, band, "CW", now); got != 1 {
		t.Fatalf("expected one CW support at threshold, got %d", got)
	}

	// Missing report should be rejected when the RTTY SNR gate is enabled.
	store.recordObservation(callRTTY, band, "RTTY", "N0CCC", 201, 0, false, now)
	if got := store.RecentSupportCount(callRTTY, band, "RTTY", now); got != 0 {
		t.Fatalf("expected no RTTY support without report when gate enabled, got %d", got)
	}

	// At threshold: RTTY report=3 should be accepted.
	store.recordObservation(callRTTY, band, "RTTY", "N0DDD", 202, 3, true, now)
	if got := store.RecentSupportCount(callRTTY, band, "RTTY", now); got != 1 {
		t.Fatalf("expected one RTTY support at threshold, got %d", got)
	}
}

func TestCustomSCPStoreVoiceBucketSharesUSBAndLSB(t *testing.T) {
	store, err := OpenCustomSCPStore(CustomSCPOptions{
		Path:           filepath.Join(t.TempDir(), "scp"),
		CoreMinScore:   1,
		CoreMinH3Cells: 1,
	})
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	call := "K1SSB"
	band := "40m"

	store.recordObservation(call, band, "USB", "N0AAA", 101, 0, false, now)

	if got := store.RecentSupportCount(call, band, "LSB", now); got != 1 {
		t.Fatalf("expected USB evidence to be visible via LSB voice bucket, got %d", got)
	}
	if got := store.RecentSupportCount(call, band, "CW", now); got != 0 {
		t.Fatalf("expected CW bucket to stay isolated from voice, got %d", got)
	}
}

func TestCustomSCPStoreH3CellDiversityGate(t *testing.T) {
	store, err := OpenCustomSCPStore(CustomSCPOptions{
		Path:           filepath.Join(t.TempDir(), "scp"),
		CoreMinScore:   1,
		CoreMinH3Cells: 2,
	})
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	call := "K1DIVERSE"
	band := "40m"

	// Two unique spotters in the same coarse H3 cell are not diverse enough.
	store.recordObservation(call, band, "CW", "N0AAA", 101, 10, true, now)
	store.recordObservation(call, band, "CW", "N0BBB", 101, 10, true, now)
	if store.HasRecentSupport(call, band, "CW", 2, now) {
		t.Fatalf("expected H3 diversity gate to reject same-cell support")
	}

	// Add a third spotter in a distant grid to satisfy coarse-cell diversity.
	store.recordObservation(call, band, "CW", "N0CCC", 202, 10, true, now)
	if !store.HasRecentSupport(call, band, "CW", 2, now) {
		t.Fatalf("expected H3 diversity gate to pass after multi-cell evidence")
	}
}

func TestCustomSCPStoreRecordSpotAdmitsVOnly(t *testing.T) {
	store, err := OpenCustomSCPStore(CustomSCPOptions{
		Path:           filepath.Join(t.TempDir(), "scp"),
		CoreMinScore:   1,
		CoreMinH3Cells: 1,
	})
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	makeSpot := func(spotter, confidence string) *Spot {
		return &Spot{
			DXCall:     "K1ABC",
			DECall:     spotter,
			DEGridNorm: "FN20",
			Frequency:  7020,
			Band:       "40m",
			Mode:       "CW",
			HasReport:  true,
			Report:     12,
			Time:       now,
			Confidence: confidence,
		}
	}

	store.RecordSpot(makeSpot("N0AAA", "S"))
	if store.StaticContains("K1ABC") {
		t.Fatalf("expected S confidence to be rejected from static membership")
	}

	store.RecordSpot(makeSpot("N0BBB", "P"))
	if store.StaticContains("K1ABC") {
		t.Fatalf("expected P confidence to be rejected from static membership")
	}

	store.RecordSpot(makeSpot("N0CCC", "C"))
	if store.StaticContains("K1ABC") {
		t.Fatalf("expected C confidence to be rejected from static membership")
	}

	store.RecordSpot(makeSpot("N0DDD", "V"))
	if !store.StaticContains("K1ABC") {
		t.Fatalf("expected V-admitted call to enter static membership")
	}
	if got := store.ActiveCallCount(now); got != 1 {
		t.Fatalf("expected exactly one active call after V admission, got %d", got)
	}
}

func TestCustomSCPStoreRecordSpotAdmitsFT8VOnly(t *testing.T) {
	store, err := OpenCustomSCPStore(CustomSCPOptions{
		Path:           filepath.Join(t.TempDir(), "scp"),
		CoreMinScore:   1,
		CoreMinH3Cells: 1,
	})
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	makeSpot := func(spotter, confidence string) *Spot {
		return &Spot{
			DXCall:     "K1FT8",
			DECall:     spotter,
			DEGridNorm: "FN31",
			Frequency:  14074.1,
			Band:       "20m",
			Mode:       "FT8",
			HasReport:  true,
			Report:     -10,
			Time:       now,
			Confidence: confidence,
		}
	}

	store.RecordSpot(makeSpot("N0AAA", "P"))
	if store.StaticContains("K1FT8") {
		t.Fatalf("expected non-V FT8 confidence to be rejected from static membership")
	}

	store.RecordSpot(makeSpot("N0BBB", "V"))
	if !store.StaticContains("K1FT8") {
		t.Fatalf("expected V-admitted FT8 call to enter static membership")
	}
	if snap := store.snapshotFor("K1FT8", "20m", "FT8", now); snap.uniqueSpotters != 1 {
		t.Fatalf("expected FT8 snapshot to track exact FT mode, got %+v", snap)
	}
	if snap := store.snapshotFor("K1FT8", "20m", "FT4", now); snap.uniqueSpotters != 0 {
		t.Fatalf("expected FT4 snapshot to remain isolated from FT8 evidence, got %+v", snap)
	}
}

func TestCustomSCPStoreLoadPrunesLegacyOversizedKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scp")
	opts := CustomSCPOptions{
		Path:                   path,
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      2,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}

	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	key := customSCPKey{call: "K1ABC", band: "40m", bucket: "cw"}
	now := time.Now().UTC()
	spotters := []struct {
		call string
		seen int64
		cell uint16
	}{
		{call: "N0AAA", seen: now.Add(-4 * time.Minute).Unix(), cell: 101},
		{call: "N0BBB", seen: now.Add(-3 * time.Minute).Unix(), cell: 102},
		{call: "N0CCC", seen: now.Add(-2 * time.Minute).Unix(), cell: 103},
		{call: "N0DDD", seen: now.Add(-1 * time.Minute).Unix(), cell: 104},
	}
	for _, entry := range spotters {
		setRawObservation(t, store, key, entry.call, entry.seen, entry.cell)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close custom store: %v", err)
	}

	reopened, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("reopen custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	stats := reopened.StatsSnapshot()
	if stats.ObservationSpotters != 2 {
		t.Fatalf("expected 2 retained spotters after load trim, got %+v", stats)
	}
	if stats.OversizedKeysSeenOnLoad != 1 {
		t.Fatalf("expected one oversized key on load, got %+v", stats)
	}
	if stats.OverflowObservationsPruned != 2 {
		t.Fatalf("expected two overflow deletions on load, got %+v", stats)
	}

	entry := reopened.entries[key]
	if entry == nil {
		t.Fatalf("expected loaded entry for %+v", key)
	}
	if len(entry.spotters) != 2 {
		t.Fatalf("expected 2 retained spotters in memory, got %d", len(entry.spotters))
	}
	requireCustomSCPEntryHasSpotter(t, entry, "N0CCC")
	requireCustomSCPEntryHasSpotter(t, entry, "N0DDD")
	if got := countObservationRecords(t, reopened, key); got != 2 {
		t.Fatalf("expected 2 persisted observation records after load trim, got %d", got)
	}
	assertCustomSCPInternerInvariant(t, reopened)
}

func TestCustomSCPStoreRecordObservationOverflowRetainsNewestDeterministically(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      2,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	seenAt := freshCustomSCPTestTime()
	for _, spotter := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		store.recordObservation("K1ABC", "40m", "CW", spotter, 101, 0, false, seenAt)
	}

	key := customSCPKey{call: "K1ABC", band: "40m", bucket: "cw"}
	entry := store.entries[key]
	if entry == nil {
		t.Fatalf("expected retained custom SCP entry")
	}
	if len(entry.spotters) != 2 {
		t.Fatalf("expected 2 retained spotters, got %d", len(entry.spotters))
	}
	requireCustomSCPEntryMissingSpotter(t, entry, "N0AAA")
	requireCustomSCPEntryHasSpotter(t, entry, "N0BBB")
	requireCustomSCPEntryHasSpotter(t, entry, "N0CCC")
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPStoreHasSFloorSupportFamilyKeyPairMatchesSlice(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      4,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := freshCustomSCPTestTime()
	store.recordObservation("W1AW", "40m", "CW", "N0AAA", 101, 0, false, now)
	voteKey, baseKey, ok := CorrectionFamilyKeyPair("W1AW/5")
	if !ok {
		t.Fatal("expected slash family key pair")
	}
	viaSlice := store.HasSFloorSupportFamily([]string{voteKey, baseKey}, "40m", "CW", 1, now)
	viaPair := store.HasSFloorSupportFamilyKeyPair(voteKey, baseKey, "40m", "CW", 1, now)
	if viaPair != viaSlice || !viaPair {
		t.Fatalf("expected pair helper to match slice helper and find support, viaPair=%v viaSlice=%v", viaPair, viaSlice)
	}
}

func TestCustomSCPSpotterSliceUpsertMaintainsSortedNewestWins(t *testing.T) {
	entry := &customSCPEntry{spotters: makeCustomSCPSpotters(4)}

	if inserted, updated := entryUpsertSpotter(entry, "N0BBB", customSCPSpotterObs{seenUnix: 20, cellRes1: 202}, 4); !inserted || !updated {
		t.Fatalf("expected initial N0BBB insert, inserted=%v updated=%v", inserted, updated)
	}
	if inserted, updated := entryUpsertSpotter(entry, "N0AAA", customSCPSpotterObs{seenUnix: 10, cellRes1: 101}, 4); !inserted || !updated {
		t.Fatalf("expected initial N0AAA insert, inserted=%v updated=%v", inserted, updated)
	}
	if got := []string{entry.spotters[0].spotter, entry.spotters[1].spotter}; got[0] != "N0AAA" || got[1] != "N0BBB" {
		t.Fatalf("expected lexical spotter order, got %v", got)
	}
	if inserted, updated := entryUpsertSpotter(entry, "N0AAA", customSCPSpotterObs{seenUnix: 5, cellRes1: 999}, 4); inserted || updated {
		t.Fatalf("expected older duplicate to be ignored, inserted=%v updated=%v", inserted, updated)
	}
	if obs, ok := entrySpotterObs(entry, "N0AAA"); !ok || obs.seenUnix != 10 || obs.cellRes1 != 101 {
		t.Fatalf("expected older duplicate to leave N0AAA unchanged, obs=%+v ok=%v", obs, ok)
	}
	if inserted, updated := entryUpsertSpotter(entry, "N0AAA", customSCPSpotterObs{seenUnix: 30, cellRes1: 303}, 4); inserted || !updated {
		t.Fatalf("expected newer duplicate to update in place, inserted=%v updated=%v", inserted, updated)
	}
	if obs, ok := entrySpotterObs(entry, "N0AAA"); !ok || obs.seenUnix != 30 || obs.cellRes1 != 303 {
		t.Fatalf("expected newer duplicate to update N0AAA, obs=%+v ok=%v", obs, ok)
	}
}

func TestCustomSCPSpotterSeenUnixEncodingBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int64
		want int64
	}{
		{name: "negative", in: -1, want: 0},
		{name: "zero", in: 0, want: 0},
		{name: "normal", in: 1_776_000_000, want: 1_776_000_000},
		{name: "max", in: customSCPMaxSpotterSeenUnix, want: customSCPMaxSpotterSeenUnix},
		{name: "overflow", in: customSCPMaxSpotterSeenUnix + 1, want: customSCPMaxSpotterSeenUnix},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeCustomSCPSpotterSeenUnix(encodeCustomSCPSpotterSeenUnix(tc.in))
			if got != tc.want {
				t.Fatalf("encode/decode seenUnix=%d got %d want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestCustomSCPExpiryQueuesMaintainIndexesAfterRootPop(t *testing.T) {
	key1 := customSCPKey{call: "K1AAA", band: "20m", bucket: "cw"}
	key2 := customSCPKey{call: "K1BBB", band: "20m", bucket: "cw"}
	key3 := customSCPKey{call: "K1CCC", band: "20m", bucket: "cw"}

	entryQueue := newCustomSCPEntryExpiryQueue(3)
	entryQueue.push(customSCPEntryExpiryItem{key: key1, dueUnix: 1})
	entryQueue.push(customSCPEntryExpiryItem{key: key2, dueUnix: 2})
	entryQueue.push(customSCPEntryExpiryItem{key: key3, dueUnix: 3})
	entryQueue.popRoot()
	if entryQueue.indexesDirty {
		t.Fatalf("expected entry queue index to stay clean after root pop")
	}
	if _, ok := entryQueue.indexes[key1]; ok {
		t.Fatalf("expected popped entry key to leave the index")
	}
	idx, ok := entryQueue.indexes[key2]
	if !ok || entryQueue.items[idx].key != key2 {
		t.Fatalf("expected maintained entry index for key2, ok=%v idx=%d", ok, idx)
	}

	staticQueue := newCustomSCPStaticExpiryQueue(3)
	staticQueue.push(customSCPStaticExpiryItem{call: "K1AAA", dueUnix: 1})
	staticQueue.push(customSCPStaticExpiryItem{call: "K1BBB", dueUnix: 2})
	staticQueue.push(customSCPStaticExpiryItem{call: "K1CCC", dueUnix: 3})
	staticQueue.popRoot()
	if staticQueue.indexesDirty {
		t.Fatalf("expected static queue index to stay clean after root pop")
	}
	if _, ok := staticQueue.indexes["K1AAA"]; ok {
		t.Fatalf("expected popped static call to leave the index")
	}
	staticIdx, ok := staticQueue.indexes["K1BBB"]
	if !ok || staticQueue.items[staticIdx].call != "K1BBB" {
		t.Fatalf("expected maintained static index for K1BBB, ok=%v idx=%d", ok, staticIdx)
	}
}

func TestCustomSCPStoreSpotterSliceChurnBoundsInternerAndCounters(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      3,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := freshCustomSCPTestTime()
	for i := 0; i < 20; i++ {
		store.recordObservation("K1CHURN", "40m", "CW", "N0"+string(rune('A'+i)), uint16(100+i), 0, false, now.Add(time.Duration(i)*time.Second))
	}

	key := customSCPKey{call: "K1CHURN", band: "40m", bucket: "cw"}
	entry := store.entries[key]
	if entry == nil {
		t.Fatalf("expected churn entry")
	}
	if len(entry.spotters) != opts.MaxSpottersPerKey {
		t.Fatalf("expected retained spotters capped at %d, got %d", opts.MaxSpottersPerKey, len(entry.spotters))
	}
	if stats := store.StatsSnapshot(); stats.ObservationSpotters != opts.MaxSpottersPerKey {
		t.Fatalf("expected observation spotter counter capped at %d, got %+v", opts.MaxSpottersPerKey, stats)
	}
	requireCustomSCPEntryHasSpotter(t, entry, "N0R")
	requireCustomSCPEntryHasSpotter(t, entry, "N0S")
	requireCustomSCPEntryHasSpotter(t, entry, "N0T")
	requireCustomSCPEntryMissingSpotter(t, entry, "N0A")
	if _, ok := store.interner.refs["N0A"]; ok {
		t.Fatalf("expected trimmed historical spotter interner ref to be released")
	}
	assertCustomSCPInternerInvariant(t, store)

	store.mu.Lock()
	store.deleteEntryLocked(key)
	store.mu.Unlock()
	if stats := store.StatsSnapshot(); stats.ObservationSpotters != 0 || stats.ObservationKeys != 0 {
		t.Fatalf("expected entry deletion to clear observation cardinality, got %+v", stats)
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPInternerReleasesRecordPrunedSpotters(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            1,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      4,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	key := customSCPKey{call: "K1PRUNE", band: "40m", bucket: "cw"}
	staleEntry := &customSCPEntry{
		spotters: customSCPTestSpotters(map[string]customSCPSpotterObs{
			"N0OLD": {seenUnix: now.Add(-48 * time.Hour).Unix(), cellRes1: 101},
		}),
		lastSeen: now.Add(-48 * time.Hour).Unix(),
	}
	store.mu.Lock()
	key = retainCustomSCPTestEntryLocked(store, key, staleEntry)
	store.observationSpotters = 1
	store.markEntryForCleanupLocked(key, staleEntry)
	store.mu.Unlock()

	store.recordObservation("K1PRUNE", "40m", "CW", "N0NEW", 102, 0, false, now)

	entry := store.entries[key]
	if entry == nil {
		t.Fatalf("expected entry to survive after fresh observation")
	}
	requireCustomSCPEntryMissingSpotter(t, entry, "N0OLD")
	if _, ok := store.interner.refs["N0OLD"]; ok {
		t.Fatalf("expected stale spotter interner ref to be released")
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPInternerReleasesSnapshotPrunedEntry(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            1,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      4,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := freshCustomSCPTestTime()
	key := customSCPKey{call: "K1SNAP", band: "40m", bucket: "cw"}
	entry := &customSCPEntry{
		spotters: customSCPTestSpotters(map[string]customSCPSpotterObs{
			"N0OLD": {seenUnix: now.Add(-48 * time.Hour).Unix(), cellRes1: 101},
		}),
		lastSeen: now.Add(-48 * time.Hour).Unix(),
	}
	store.mu.Lock()
	key = retainCustomSCPTestEntryLocked(store, key, entry)
	store.observationSpotters = 1
	store.markEntryForCleanupLocked(key, entry)
	store.mu.Unlock()

	if snap := store.snapshotFor("K1SNAP", "40m", "CW", now); snap.uniqueSpotters != 0 {
		t.Fatalf("expected stale snapshot to return no support, got %+v", snap)
	}
	if _, ok := store.entries[key]; ok {
		t.Fatalf("expected stale entry to be deleted by snapshot pruning")
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPInternerReleasesMaxKeyEviction(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxKeys:                1,
		MaxSpottersPerKey:      4,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := freshCustomSCPTestTime()
	store.recordObservation("K1OLD", "40m", "CW", "N0OLD", 101, 0, false, now.Add(-time.Minute))
	store.recordObservation("K1NEW", "20m", "RTTY", "N0NEW", 202, 3, true, now)

	if len(store.entries) != 1 {
		t.Fatalf("expected one retained entry after max-key eviction, got %d", len(store.entries))
	}
	if _, ok := store.interner.refs["N0OLD"]; ok {
		t.Fatalf("expected evicted entry spotter ref to be released")
	}
	if _, ok := store.interner.refs["40m"]; ok {
		t.Fatalf("expected evicted entry band ref to be released")
	}
	if _, ok := store.interner.refs["cw"]; ok {
		t.Fatalf("expected evicted entry bucket ref to be released")
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPStatsSnapshotReportsRetainedStateCardinality(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      4,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := freshCustomSCPTestTime()
	store.recordObservation("K1STAT", "40m", "CW", "N0AAA", 101, 0, false, now)

	stats := store.StatsSnapshot()
	if stats.InternedStrings != 4 {
		t.Fatalf("expected 4 distinct interned strings, got %+v", stats)
	}
	if stats.InternedRefs != 5 {
		t.Fatalf("expected 5 interned refs, got %+v", stats)
	}
	if stats.InternReleaseMisses != 0 {
		t.Fatalf("expected no interner release misses, got %+v", stats)
	}
	if stats.EntryExpiryItems != 1 || stats.StaticExpiryItems != 1 {
		t.Fatalf("expected expiry index stats to track retained state, got %+v", stats)
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPStoreStaticCallCountUsesStaticHorizon(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            60,
		StaticHorizonDays:      395,
		MaxSpottersPerKey:      4,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := freshCustomSCPTestTime()
	store.recordObservation("K1DUP", "40m", "CW", "N0AAA", 101, 0, false, now.Add(-10*24*time.Hour))
	store.recordObservation("K1DUP", "20m", "CW", "N0BBB", 102, 0, false, now.Add(-9*24*time.Hour))
	store.recordObservation("K1FRESH", "40m", "CW", "N0CCC", 103, 0, false, now.Add(-8*24*time.Hour))
	store.mu.Lock()
	retainCustomSCPTestStaticLocked(store, "K1STALE", now.Add(-396*24*time.Hour).Unix())
	store.mu.Unlock()

	if got := store.StaticCallCount(now); got != 2 {
		t.Fatalf("expected two horizon-valid static calls, got %d", got)
	}
	if got := store.ActiveCallCount(now); got != 2 {
		t.Fatalf("expected recent active count to remain observation-horizon based, got %d", got)
	}
	if stats := store.StatsSnapshot(); stats.StaticCalls != 3 {
		t.Fatalf("expected retained static diagnostic count to include stale entry until cleanup, got %+v", stats)
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPTrimSpottersLockedDoesNotRefreshLastSeen(t *testing.T) {
	store := &CustomSCPStore{
		opts: sanitizeCustomSCPOptions(CustomSCPOptions{MaxSpottersPerKey: 1}),
	}
	entry := &customSCPEntry{
		spotters: customSCPTestSpotters(map[string]customSCPSpotterObs{
			"N0AAA": {seenUnix: 10, cellRes1: 101},
			"N0BBB": {seenUnix: 20, cellRes1: 102},
		}),
		lastSeen: 999,
	}
	store.entries = make(map[customSCPKey]*customSCPEntry, 1)
	retainCustomSCPTestEntryLocked(store, customSCPKey{call: "K1TRIM", band: "40m", bucket: "cw"}, entry)
	store.observationSpotters = len(entry.spotters)

	if trimmed := store.trimSpottersLocked(entry, nil); trimmed != 1 {
		t.Fatalf("expected one trimmed spotter, got %d", trimmed)
	}
	if entry.lastSeen != 999 {
		t.Fatalf("expected normal overflow trim to leave lastSeen unchanged, got %d", entry.lastSeen)
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPStoreCleanupPrunesOverflowAndStaleStatic(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      2,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	key := customSCPKey{call: "K1BIG", band: "20m", bucket: "cw"}
	store.mu.Lock()
	entry := &customSCPEntry{
		spotters: customSCPTestSpotters(map[string]customSCPSpotterObs{
			"N0AAA": {seenUnix: now.Add(-4 * time.Minute).Unix(), cellRes1: 201},
			"N0BBB": {seenUnix: now.Add(-3 * time.Minute).Unix(), cellRes1: 202},
			"N0CCC": {seenUnix: now.Add(-2 * time.Minute).Unix(), cellRes1: 203},
			"N0DDD": {seenUnix: now.Add(-1 * time.Minute).Unix(), cellRes1: 204},
		}),
		lastSeen: now.Add(-1 * time.Minute).Unix(),
	}
	key = retainCustomSCPTestEntryLocked(store, key, entry)
	store.observationSpotters = 4
	retainCustomSCPTestStaticLocked(store, "K1OLD", now.Add(-31*24*time.Hour).Unix())
	store.markEntryForCleanupLocked(key, store.entries[key])
	store.mu.Unlock()
	for _, spotter := range store.entries[key].spotters {
		setRawObservation(t, store, key, spotter.spotter, decodeCustomSCPSpotterSeenUnix(spotter.seenUnix), spotter.cellRes1)
	}
	store.persistStaticLocked("K1OLD", now.Add(-31*24*time.Hour).Unix())
	idx, ok := store.entryExpiry.indexes[key]
	if !ok || store.entryExpiry.items[idx].dueUnix != customSCPImmediateCleanupDueUnix {
		t.Fatalf("expected oversized entry queued for immediate cleanup, ok=%v idx=%d", ok, idx)
	}

	store.cleanup(now)

	entry = store.entries[key]
	if entry == nil {
		t.Fatalf("expected retained entry after cleanup")
	}
	if len(entry.spotters) != 2 {
		t.Fatalf("expected cleanup to retain 2 newest spotters, got %d", len(entry.spotters))
	}
	requireCustomSCPEntryHasSpotter(t, entry, "N0CCC")
	requireCustomSCPEntryHasSpotter(t, entry, "N0DDD")
	if got := countObservationRecords(t, store, key); got != 2 {
		t.Fatalf("expected cleanup to reduce persisted observation records to 2, got %d", got)
	}
	if store.StaticContains("K1OLD") {
		t.Fatalf("expected stale static membership to age out")
	}
	if got := countStaticRecord(t, store, "K1OLD"); got != 0 {
		t.Fatalf("expected stale static record deleted from Pebble, got %d", got)
	}
	stats := store.StatsSnapshot()
	if stats.StaleStaticPruned == 0 {
		t.Fatalf("expected stale static prune counter to increase, got %+v", stats)
	}
	if stats.OverflowObservationsPruned < 2 {
		t.Fatalf("expected overflow prune counter to record cleanup trimming, got %+v", stats)
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPStoreCleanupIgnoresUpdatedEntryExpiry(t *testing.T) {
	opts := CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      2,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}
	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Now().UTC()
	key := customSCPKey{call: "K1FRESH", band: "20m", bucket: "cw"}
	store.mu.Lock()
	entry := &customSCPEntry{
		spotters: customSCPTestSpotters(map[string]customSCPSpotterObs{
			"N0AAA": {seenUnix: now.Add(-29 * 24 * time.Hour).Unix(), cellRes1: 201},
		}),
	}
	key = retainCustomSCPTestEntryLocked(store, key, entry)
	store.observationSpotters = 1
	store.markEntryForCleanupLocked(key, entry)
	entryUpsertSpotter(entry, "N0AAA", customSCPSpotterObs{seenUnix: now.Add(-1 * time.Hour).Unix(), cellRes1: 201}, store.opts.MaxSpottersPerKey)
	store.markEntryForCleanupLocked(key, entry)
	store.mu.Unlock()

	store.cleanup(now)

	store.mu.RLock()
	entry = store.entries[key]
	if entry == nil {
		store.mu.RUnlock()
		t.Fatalf("expected refreshed entry to survive cleanup")
	}
	if len(entry.spotters) != 1 {
		store.mu.RUnlock()
		t.Fatalf("expected refreshed entry to retain one spotter, got %d", len(entry.spotters))
	}
	oldestSeen := entry.oldestSeenUnix
	cutoff := store.observationHorizonCutoffUnix(now)
	store.mu.RUnlock()
	if oldestSeen < cutoff {
		t.Fatalf("expected updated expiry to reflect fresh observation, got oldest=%d cutoff=%d", oldestSeen, cutoff)
	}
	assertCustomSCPInternerInvariant(t, store)
}

func TestCustomSCPStoreLoadDeletesStaleStaticMembership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scp")
	opts := CustomSCPOptions{
		Path:                   path,
		HorizonDays:            30,
		StaticHorizonDays:      30,
		MaxSpottersPerKey:      2,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}

	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	staleSeen := time.Now().UTC().Add(-31 * 24 * time.Hour).Unix()
	store.persistStaticLocked("K1STALE", staleSeen)
	if err := store.Close(); err != nil {
		t.Fatalf("close custom store: %v", err)
	}

	reopened, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("reopen custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	if reopened.StaticContains("K1STALE") {
		t.Fatalf("expected stale static membership to be ignored on load")
	}
	if got := countStaticRecord(t, reopened, "K1STALE"); got != 0 {
		t.Fatalf("expected stale static record deleted on load, got %d", got)
	}
	if stats := reopened.StatsSnapshot(); stats.StaleStaticPruned == 0 {
		t.Fatalf("expected stale static prune count on load, got %+v", stats)
	}
}

func TestCustomSCPStoreStaticMembershipOutlivesObservationHorizon(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scp")
	opts := CustomSCPOptions{
		Path:                   path,
		HorizonDays:            60,
		StaticHorizonDays:      395,
		MaxSpottersPerKey:      2,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	}

	store, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}

	call := "K1YEAR"
	key := customSCPKey{call: call, band: "20m", bucket: "cw"}
	now := time.Now().UTC()
	staticSeen := now.Add(-120 * 24 * time.Hour).Unix()
	obsSeen := now.Add(-61 * 24 * time.Hour).Unix()

	store.mu.Lock()
	retainCustomSCPTestStaticLocked(store, call, staticSeen)
	store.mu.Unlock()
	store.persistStaticLocked(call, staticSeen)
	setRawObservation(t, store, key, "N0OLD", obsSeen, 101)
	if err := store.Close(); err != nil {
		t.Fatalf("close custom store: %v", err)
	}

	reopened, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatalf("reopen custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	if !reopened.StaticContains(call) {
		t.Fatalf("expected static membership to survive beyond observation horizon")
	}
	if got := reopened.RecentSupportCount(call, "20m", "CW", now); got != 0 {
		t.Fatalf("expected observation support to age out after 60-day horizon, got %d", got)
	}
	if got := countObservationRecords(t, reopened, key); got != 0 {
		t.Fatalf("expected stale observation record to be deleted on load, got %d", got)
	}
	if got := countStaticRecord(t, reopened, call); got != 1 {
		t.Fatalf("expected static membership record to remain, got %d", got)
	}
	assertCustomSCPInternerInvariant(t, reopened)
}

func setRawObservation(t *testing.T, store *CustomSCPStore, key customSCPKey, spotter string, seenUnix int64, cell uint16) {
	t.Helper()
	value := make([]byte, 10)
	binary.BigEndian.PutUint64(value[:8], uint64(seenUnix))
	binary.BigEndian.PutUint16(value[8:10], cell)
	if err := store.db.Set([]byte(observationKeyString(key, spotter)), value, nil); err != nil {
		t.Fatalf("set raw observation: %v", err)
	}
}

func countObservationRecords(t *testing.T, store *CustomSCPStore, key customSCPKey) int {
	t.Helper()
	iter, err := store.db.NewIter(nil)
	if err != nil {
		t.Fatalf("new iter: %v", err)
	}
	defer iter.Close()
	count := 0
	prefix := observationPrefixForKey(key)
	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == prefix {
			count++
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	return count
}

func countStaticRecord(t *testing.T, store *CustomSCPStore, call string) int {
	t.Helper()
	iter, err := store.db.NewIter(nil)
	if err != nil {
		t.Fatalf("new iter: %v", err)
	}
	defer iter.Close()
	key := customSCPMetaPrefix + call
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if string(iter.Key()) == key {
			count++
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	return count
}
