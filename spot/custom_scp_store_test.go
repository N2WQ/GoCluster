package spot

import (
	"path/filepath"
	"testing"
	"time"
)

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
