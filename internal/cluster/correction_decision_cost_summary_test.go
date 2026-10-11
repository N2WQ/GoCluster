//go:build !qualification

// File role: Checks actual consumer silence/cost and operator summaries without
// treating query-clock expiry or repeated reads as owner lifecycle events.
package cluster

import (
	"log"
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
)

func b32CaptureAllLogs(t testing.TB) *b32LogWriter {
	t.Helper()
	w := &b32LogWriter{}
	previousWriter, previousFlags, previousPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(w)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})
	return w
}

func b32AssertNoLogs(t testing.TB, w *b32LogWriter) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() != 0 {
		t.Fatalf("unchanged/no-snapshot consumer must never log: %q", w.buf.String())
	}
}

func b32SilentSelection(present bool) correctionflow.ResolverPrimarySelection {
	return correctionflow.ResolverPrimarySelection{SnapshotOK: present, Snapshot: spot.ResolverSnapshot{
		Key: spot.NewResolverSignalKey(14020, "20m", "CW", 500), State: spot.ResolverStateConfident,
		Winner: "K1ABC", WinnerSupport: 2, RunnerUp: "K1ABD", RunnerSupport: 1, TotalReporters: 3,
		WinnerWeightedSupportMilli: 2000, RunnerWeightedSupportMilli: 1000, TotalWeightedSupportMilli: 3000,
	}}
}

// Cached literal selections isolate the event addition at the real consumer;
// these costs do not include live resolver lookup, pipeline or network work.
func TestB32DecisionSilentConsumerCost(t *testing.T) {
	for _, present := range []bool{true, false} {
		name := "missing_snapshot"
		if present {
			name = "healthy_unchanged"
		}
		t.Run(name, func(t *testing.T) {
			w := b32CaptureAllLogs(t)
			at := time.Now().UTC().Truncate(time.Second)
			s := b32OriginalSpot("K1ABC", at)
			cfg, selection := correctionLossConfig(), b32SilentSelection(present)
			suppressed := false
			allocations := testing.AllocsPerRun(1000, func() {
				suppressed = maybeApplyResolverCorrectionWithSelectionOverride(s, nil, spot.ResolverEvidence{}, false, cfg,
					nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &selection, at, nil)
			})
			if suppressed || s.DXCallNorm != "K1ABC" || s.ConfidencePercentOK != present || (present && (s.Confidence != "V" || s.ConfidencePercent != 66)) || (!present && (s.Confidence != "?" || s.ConfidencePercent != 0)) {
				t.Fatalf("literal silent consumer outcome changed: suppressed=%t spot=%+v", suppressed, s)
			}
			b32AssertNoLogs(t, w)
			if allocations != 0 {
				t.Fatalf("silent actual consumer allocated %g objects/call, want zero", allocations)
			}
		})
	}
}

func BenchmarkB32DecisionSilentConsumer(b *testing.B) {
	for _, present := range []bool{true, false} {
		name := "missing_snapshot"
		if present {
			name = "healthy_unchanged"
		}
		b.Run(name, func(b *testing.B) {
			w := b32CaptureAllLogs(b)
			at := time.Date(2026, time.October, 11, 12, 0, 0, 0, time.UTC)
			s := b32OriginalSpot("K1ABC", at)
			cfg, selection := correctionLossConfig(), b32SilentSelection(present)
			call := func() bool {
				return maybeApplyResolverCorrectionWithSelectionOverride(s, nil, spot.ResolverEvidence{}, false, cfg,
					nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &selection, at, nil)
			}
			if call() || s.DXCallNorm != "K1ABC" || s.ConfidencePercentOK != present || (present && s.ConfidencePercent != 66) {
				b.Fatal("benchmark must exercise unchanged/missing actual consumer")
			}
			b32AssertNoLogs(b, w)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				call()
			}
			b.StopTimer()
			b32AssertNoLogs(b, w)
		})
	}
}

func TestB32ResolverPressureSummaryActualOwnerLifecycle(t *testing.T) {
	r, d, key := correctionLossResolver(t, 2, 4)
	if err := d.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, at, 2*time.Minute, "N0A1", "N0A2", "N0A3", "N0A4")
	correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at, 2*time.Minute, "N0A1", "N0A2", "N0A3")
	snapshot, ok := r.Lookup(key)
	if !ok || snapshot.TotalReporters != 4 || snapshot.TotalWeightedSupportMilli != 4000 || snapshot.ReceiverEvidence() == nil || snapshot.ReceiverEvidence().Len() != 7 ||
		correctionflow.ResolverSupportForCall(snapshot, "K1ABC") != 4 || correctionflow.ResolverSupportForCall(snapshot, "K1ABD") != 3 {
		t.Fatal("four distinct receivers must have seven retained candidate observations")
	}
	correctionLossAdmit(t, r, d, key, "K1ABE", 14020, at.Add(time.Millisecond), 2*time.Minute, "N0C1")
	correctionLossEvictions(t, r, 1, 0)
	want := "Resolver Pressure: 1 (C) / 0 (R) evict 1 (C) / 0 (R) hw 2 (C) / 4 (R) | loss rows=3 protected=1 recover=0 idle-retire=0"
	if got := formatResolverPressureSummary(r); got != want {
		t.Fatalf("one candidate eviction discards three shared-receiver observations:\n got %q\nwant %q", got, want)
	}
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, at.Add(2*time.Millisecond), 2*time.Minute, "N0A5")
	correctionLossEvictions(t, r, 1, 1)
	want = "Resolver Pressure: 1 (C) / 1 (R) evict 1 (C) / 1 (R) hw 2 (C) / 4 (R) | loss rows=4 protected=1 recover=0 idle-retire=0"
	for i := 0; i < 5; i++ {
		if s, ok := r.LookupEligible(key, at.Add(time.Hour)); !ok || s.EvidenceLoss || s.TotalReporters != 0 {
			t.Fatal("far query must expire eligible rows without owner mutation")
		}
		if got := formatResolverPressureSummary(r); got != want {
			t.Fatalf("query-only expiry changed owner summary: got %q want %q", got, want)
		}
	}
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, at.Add(2*time.Second), time.Second, "N0A5")
	want = "Resolver Pressure: 1 (C) / 1 (R) evict 1 (C) / 1 (R) hw 2 (C) / 4 (R) | loss rows=4 protected=0 recover=1 idle-retire=0"
	if got := formatResolverPressureSummary(r); got != want {
		t.Fatalf("actual owner recovery summary: got %q want %q", got, want)
	}
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, at.Add(3*time.Second), 2*time.Minute, "N0A5")
	want = "Resolver Pressure: 1 (C) / 1 (R) evict 1 (C) / 1 (R) hw 2 (C) / 4 (R) | loss rows=4 protected=1 recover=1 idle-retire=0"
	if got := formatResolverPressureSummary(r); got != want {
		t.Fatalf("window expansion reactivates existing loss: got %q want %q", got, want)
	}
	if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
		t.Fatal(err)
	}
	want = "Resolver Pressure: 1 (C) / 1 (R) evict 1 (C) / 1 (R) hw 2 (C) / 4 (R) | loss rows=4 protected=0 recover=1 idle-retire=0"
	if _, ok := r.Lookup(key); ok || r.MetricsSnapshot().ActiveKeys != 1 || formatResolverPressureSummary(r) != want {
		t.Fatal("logical retirement must hide published loss before physical owner retirement")
	}
	d.Sweep(at.Add(3 * time.Second))
	want = "Resolver Pressure: 1 (C) / 1 (R) evict 1 (C) / 1 (R) hw 2 (C) / 4 (R) | loss rows=4 protected=0 recover=1 idle-retire=1"
	for i := 0; i < 5; i++ {
		d.Sweep(at.Add(3 * time.Second))
		if got := formatResolverPressureSummary(r); got != want || r.MetricsSnapshot().ActiveKeys != 0 {
			t.Fatalf("physical retirement must count once: got %q want %q", got, want)
		}
	}
}

func TestB32CorrectionSummaryLossVisibleBeyondTopTwo(t *testing.T) {
	r, d, key := correctionLossResolver(t, 2, 8)
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, at, 2*time.Minute, "N0AAA")
	correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at, 2*time.Minute, "N0BBB", "N0CCC")
	correctionLossAdmit(t, r, d, key, "K1ABE", 14020, at, 2*time.Minute, "N0DDD")
	correctionLossEvictions(t, r, 1, 0)
	snapshot, ok := r.LookupEligible(key, time.Now().UTC())
	if !ok || !snapshot.EvidenceLoss {
		t.Fatal("actual cap loss must precede its consumer summary")
	}
	tracker, surface, cfg := stats.NewTracker(), &b32CallSurface{}, correctionLossConfig()
	apply := func(call string, selection correctionflow.ResolverPrimarySelection) {
		s := b32OriginalSpot(call, at)
		if maybeApplyResolverCorrectionWithSelectionOverride(s, nil, spot.ResolverEvidence{}, false, cfg,
			nil, nil, tracker, surface, nil, nil, nil, nil, nil, nil, &selection, at, nil) {
			t.Fatal("summary fixture unexpectedly suppressed its input")
		}
	}
	for i := 0; i < 3; i++ {
		apply("K1ABC", b32SilentSelection(true))
	}
	for i := 0; i < 2; i++ {
		apply("K1ABC", b32SilentSelection(false))
	}
	apply("K1ABC", correctionflow.ResolverPrimarySelection{SnapshotOK: true, Snapshot: snapshot})
	want := "CorrGate: 6 (T) / 0 (A) / 6 (R) / 0 (FB) [rej:resolver_same_call=3, resolver_no_snapshot=2 ap:none] [resolver_primary=6] loss=1"
	if got := formatCorrectionDecisionSummary(tracker); got != want || tracker.CallCorrections() != 0 || tracker.TemporalCommitted() != 0 {
		t.Fatalf("rank-three loss must remain visible without counting a mutation: got %q want %q", got, want)
	}
	apply("K1ABD", b32SilentSelection(true)) // One actual accepted A-to-B identity change.
	want = "CorrGate: 7 (T) / 1 (A) / 6 (R) / 0 (FB) [rej:resolver_same_call=3, resolver_no_snapshot=2 ap:resolver_applied=1] [resolver_primary=7] loss=1"
	for i := 0; i < 5; i++ {
		if got := formatCorrectionDecisionSummary(tracker); got != want || tracker.CallCorrections() != 1 || tracker.TemporalCommitted() != 0 {
			t.Fatalf("summary reads changed actual accepted-only accounting: got %q want %q", got, want)
		}
	}
}
