// File role: Exercises temporal key reclamation through real replay admission
// and drain ownership. Replay output accounting is not a client receipt oracle.
package main

import (
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestReplayTemporalReclaimFreshAdmission(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	cfg := replayTemporalFreshConfig()
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 1, 5
	cfg.TemporalDecoder.MinMarginScore = 80
	cfg.TemporalDecoder.MaxEventsPerKey, cfg.TemporalDecoder.MaxActiveKeys = 1, 1
	cfg.TemporalDecoder.MaxPending, cfg.TemporalDecoder.OverflowAction = 8, "abstain"
	r, freshKey := replayTemporalFreshRunner(t, cfg, at, 2*time.Minute)
	oldKey := spot.NewResolverSignalKey(14030, "20m", "CW", 500)
	replayTemporalReclaimSeed(t, r, oldKey, at, 2, 2)
	older := replayTemporalReclaimSpot(at)
	if !r.maybeHoldTemporal(older, spot.ResolverEvidence{Key: oldKey}, true, at) || len(r.temporalPending) != 1 {
		t.Fatal("older replay request was not actually held")
	}
	r.drainTemporal(at.Add(time.Second), false)
	if len(r.temporalPending) != 1 || len(r.temporalQueue) != 1 || r.abMetrics.Output.ConfidenceCounts.Samples != 0 ||
		!r.temporalQueue[0].due.Equal(at.Add(5*time.Second)) {
		t.Fatal("tied replay request did not defer to its actual max wait")
	}
	newAt := at.Add(time.Second)
	// A narrow owner window removes the old dissent before its expansion.
	if !r.resolver.Enqueue(spot.ResolverEvidence{Key: oldKey, ObservedAt: newAt, DXCall: "K1ABC", Spotter: "N0AAA",
		FrequencyKHz: 14030, RecencyWindow: time.Nanosecond}) || r.driver.Drain() != 1 {
		t.Fatal("old replay memberships were not retired")
	}
	replayTemporalReclaimSeed(t, r, oldKey, newAt, 3, 1)
	newer := replayTemporalReclaimSpot(newAt)
	if !r.maybeHoldTemporal(newer, spot.ResolverEvidence{Key: oldKey}, true, newAt) || len(r.temporalPending) != 2 {
		t.Fatal("newer same-key replay request was not actually held")
	}
	r.drainTemporal(at.Add(2*time.Second), false)
	if len(r.temporalPending) != 1 || len(r.temporalQueue) != 1 || r.temporalDecoder.PendingCount() != 1 || r.abMetrics.Temporal.Proposed != 1 {
		t.Fatal("newer replay request did not complete before the evicted older request")
	}
	r.drainTemporal(at.Add(5*time.Second), false)
	if len(r.temporalPending) != 0 || len(r.temporalQueue) != 0 || r.temporalDecoder.PendingCount() != 0 ||
		r.abMetrics.Temporal.AbstainLowMargin != 1 || r.abMetrics.Temporal.HandoffAttempts != 2 ||
		r.abMetrics.Temporal.Committed != 0 || r.abMetrics.Temporal.ConfirmedHistoryKeys != 0 ||
		r.abMetrics.Output.ConfidenceCounts != (replayConfidenceCounts{Samples: 2, V: 2}) {
		t.Fatalf("setup did not retire exactly two unchanged replay outputs: temporal=%+v output=%+v", r.abMetrics.Temporal, r.abMetrics.Output)
	}
	freshAt := at.Add(6 * time.Second)
	incoming := replayTemporalFreshSpot("K1ABD", freshAt)
	if !r.maybeHoldTemporal(incoming, spot.ResolverEvidence{Key: freshKey}, true, freshAt) {
		t.Fatal("fresh replay request bypassed temporal admission")
	}
	if len(r.temporalPending) != 1 || len(r.temporalQueue) != 1 || r.temporalDecoder.PendingCount() != 1 {
		// maybeHoldTemporal also returns true after overflow output. Ownership,
		// not that return value, distinguishes admission on the baseline.
		t.Errorf("fresh replay key was refused after final eviction: pending=%d heap=%d decoder=%d call=%s glyph=%s",
			len(r.temporalPending), len(r.temporalQueue), r.temporalDecoder.PendingCount(), incoming.DXCall, incoming.Confidence)
	} else {
		if r.abMetrics.Output.ConfidenceCounts.Samples != 2 || incoming.DXCall != "K1ABD" ||
			!r.temporalQueue[0].due.Equal(freshAt.Add(time.Second)) {
			t.Fatal("fresh replay request emitted or mutated before its hold deadline")
		}
		r.drainTemporal(freshAt.Add(time.Second-time.Nanosecond), false)
		if len(r.temporalPending) != 1 || r.abMetrics.Output.ConfidenceCounts.Samples != 2 {
			t.Fatal("fresh replay hold released before its deadline")
		}
		r.drainTemporal(freshAt.Add(time.Second), false)
		r.drainTemporal(freshAt.Add(time.Second), false)
	}
	if incoming.DXCall != "K1ABC" || incoming.DXCallNorm != "K1ABC" || incoming.Confidence != "C" {
		t.Errorf("fresh replay output call=%s normalized=%s glyph=%s want K1ABC/K1ABC/C", incoming.DXCall, incoming.DXCallNorm, incoming.Confidence)
	}
	if len(r.temporalPending) != 0 || len(r.temporalQueue) != 0 || r.temporalDecoder.PendingCount() != 0 ||
		r.abMetrics.Temporal.Proposed != 2 || r.abMetrics.Temporal.Committed != 1 || r.abMetrics.Temporal.PathSwitches != 0 ||
		r.abMetrics.Temporal.HandoffAttempts != 3 || r.abMetrics.Temporal.AbstainLowMargin != 1 ||
		r.abMetrics.Temporal.ConfirmedHistoryKeys != 1 || r.abMetrics.Temporal.ConfirmedHistoryEvictions != 0 ||
		r.abMetrics.Output.ConfidenceCounts != (replayConfidenceCounts{Samples: 3, V: 2, C: 1}) ||
		r.tracker.CallCorrections() != 1 || len(r.stabilityCollector.corrections) != 1 {
		t.Errorf("fresh replay admission/mutation/output accounting: temporal=%+v output=%+v corrections=%d stability=%d",
			r.abMetrics.Temporal, r.abMetrics.Output, r.tracker.CallCorrections(), len(r.stabilityCollector.corrections))
	}
}

func replayTemporalReclaimSpot(at time.Time) *spot.Spot {
	s := spot.NewSpotNormalized("K1ABC", "N0AAA", 14030, "CW")
	s.Time = at
	return s
}

func replayTemporalReclaimSeed(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, at time.Time, winners, dissenters int) {
	t.Helper()
	for _, group := range []struct {
		call      string
		frequency float64
		receivers []string
	}{
		{"K1ABC", 14030, []string{"N0AAA", "N0BBB", "N0CCC"}[:winners]},
		{"K1ABD", 14030.2, []string{"N0DDD", "N0EEE"}[:dissenters]},
	} {
		for _, receiver := range group.receivers {
			if !r.resolver.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: group.call, Spotter: receiver,
				FrequencyKHz: group.frequency, RecencyWindow: 2 * time.Minute}) {
				t.Fatal("literal replay receiver was not admitted")
			}
		}
	}
	if processed := r.driver.Drain(); processed != winners+dissenters {
		t.Fatalf("replay receiver reports=%d want %d", processed, winners+dissenters)
	}
	r.driver.Sweep(at.Add(2 * time.Nanosecond))
	selection := correctionflow.SelectResolverPrimarySnapshotForCall(r.resolver, key, r.cfg.CallCorrection, "K1ABC")
	if !selection.SnapshotOK || selection.Snapshot.TotalReporters != winners+dissenters ||
		correctionflow.ResolverSupportForCall(selection.Snapshot, "K1ABC") != winners ||
		correctionflow.ResolverSupportForCall(selection.Snapshot, "K1ABD") != dissenters {
		t.Fatalf("literal replay population was not published: %+v", selection)
	}
}
