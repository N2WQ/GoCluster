package correctionflow

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestDiagnosticsRawAndCanonicalPopulationBounds(t *testing.T) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 8192, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64,
		HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	t.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for offset := -2; offset <= 2; offset++ {
		for candidate := 0; candidate < 16; candidate++ {
			for receiver := 0; receiver < 64; receiver++ {
				if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: fmt.Sprintf("K1A%02d", candidate), Spotter: fmt.Sprintf("N0A%02d", receiver),
					FrequencyKHz: 7010 + float64(offset)*.5, RecencyWindow: time.Minute}) {
					t.Fatal("bounded fixture refused")
				}
			}
		}
	}
	if driver.Drain() != 5120 {
		t.Fatal("bounded fixture lost raw incidences")
	}
	driver.Sweep(at.Add(time.Millisecond))
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodBucketRadius = 2
	before := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1A00", at.Add(time.Second))
	var capture ResolverSelectionDiagnostics
	after, err := SelectResolverPrimarySnapshotForCallWithDiagnostics(r, key, cfg, "K1A00", at.Add(time.Second), &capture)
	if err != nil || !reflect.DeepEqual(before, after) || capture.RawSpellings != 80 || capture.RawIncidences != 5120 || len(capture.CanonicalRows) != 1024 {
		t.Fatalf("at-limit capture failed or truncated: %v raw=%d/%d canonical=%d", err, capture.RawSpellings, capture.RawIncidences, len(capture.CanonicalRows))
	}
	diagnosticTestJSON(t, capture, capture.EncodedByteUpperBound())
}

func TestDiagnosticsCanonicalUnionCanExceed64Receivers(t *testing.T) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 512, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64,
		HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	t.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for offset := 0; offset < 2; offset++ {
		for alias, call := range []string{"KH6/W1AW", "W1AW/KH6"} {
			for receiver := 0; receiver < 64; receiver++ {
				if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: call, Spotter: fmt.Sprintf("N%d%dA%02d", offset, alias, receiver),
					FrequencyKHz: 7010 + float64(offset)*.5, RecencyWindow: time.Minute}) {
					t.Fatal("canonical fixture refused")
				}
			}
		}
	}
	if driver.Drain() != 256 {
		t.Fatal("canonical fixture lost raw incidences")
	}
	driver.Sweep(at.Add(time.Millisecond))
	var capture ResolverSelectionDiagnostics
	selected, err := SelectResolverPrimarySnapshotForCallWithDiagnostics(r, key, accountingConfig(), "W1AW", at.Add(time.Second), &capture)
	if err != nil || !selected.UsedNeighborhood || len(capture.CanonicalRows) != 256 || len(capture.Output.Snapshot.Candidates) != 1 || capture.Output.Snapshot.Candidates[0].Support != 256 {
		t.Fatalf("canonical receiver union was capped at raw-candidate limit: err=%v rows=%d selection=%+v", err, len(capture.CanonicalRows), selected)
	}
	diagnosticTestJSON(t, capture, capture.EncodedByteUpperBound())
}

func TestDiagnosticsOversizedReceiverPopulationIsSeparateFailure(t *testing.T) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 8192, MaxCandidatesPerKey: 2, MaxReportersPerCand: 5121,
		HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	t.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for receiver := 0; receiver < 5121; receiver++ {
		if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: "K1ABC", Spotter: fmt.Sprintf("N0A%04d", receiver), FrequencyKHz: 7010, RecencyWindow: time.Minute}) {
			t.Fatal("oversized fixture refused its configured capacity")
		}
	}
	if driver.Drain() != 5121 {
		t.Fatal("oversized fixture lost input")
	}
	driver.Sweep(at.Add(time.Millisecond))
	before := SelectResolverPrimarySnapshotForCall(r, key, accountingConfig(), "K1ABC")
	if before.Snapshot.ReceiverEvidence().Len() <= 5120 {
		t.Fatal("oversized publication was not established")
	}
	var capture ResolverSelectionDiagnostics
	after, err := SelectResolverPrimarySnapshotForCallWithDiagnostics(r, key, accountingConfig(), "K1ABC", time.Time{}, &capture)
	if err == nil || !reflect.DeepEqual(before, after) || len(capture.Reads[2].RawRows) != 0 {
		t.Fatal("receiver-copy cap changed ordinary selection or allocated oversized capture")
	}
}
