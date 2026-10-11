// File role: Checks fixed replay loss diagnostics in actual serialized artifacts.
// Owner publication counters remain separate from query-time eligibility changes.
package main

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
	"dxcluster/stats"
)

func TestB32ReplayLossArtifactsPreserveColumnsAndLiteralCounters(t *testing.T) {
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	samples := []resolverSample{
		{TS: at, Metrics: spot.SignalResolverMetrics{CapLossDiscards: 4, LossPublicationRecoveries: 2, LossIdleRetirements: 1, PublishedLossKeys: 3}},
		{TS: at.Add(time.Minute), Metrics: spot.SignalResolverMetrics{CapPressureCandidates: 1, CapLossDiscards: 7, LossPublicationRecoveries: 3, LossIdleRetirements: 2, PublishedLossKeys: 2}},
		{TS: at.Add(2 * time.Minute), Metrics: spot.SignalResolverMetrics{CapPressureCandidates: 1, CapLossDiscards: 10, LossPublicationRecoveries: 5, LossIdleRetirements: 3}},
	}
	intervals, hits, gates := computeIntervalsAndGates(samples, 32)
	if len(intervals) != 2 || len(hits) != 1 {
		t.Fatalf("existing pressure thresholds changed: intervals=%d hits=%d", len(intervals), len(hits))
	}
	wantHeader := []string{"T0", "T1", "Minutes", "dStateConfident", "dStateProbable", "dStateUncertain", "dStateSplit",
		"dQ", "dK", "dCcap", "dR", "dPC", "dPR", "dEC", "dER", "DEPTH", "QueueDepthRatio", "SplitSharePct",
		"dCapLossDiscards", "dLossPublicationRecoveries", "dLossIdleRetirements", "PublishedLossKeys"}
	wantSuffixes := [][]string{{"3", "1", "1", "2"}, {"3", "2", "1", "0"}}
	for _, artifact := range []struct {
		name string
		rows []intervalRow
		want [][]string
	}{
		{"resolver_intervals.csv", intervals, wantSuffixes},
		{"resolver_threshold_hits.csv", hits, wantSuffixes[:1]},
	} {
		path := filepath.Join(t.TempDir(), artifact.name)
		if err := writeIntervalsCSV(path, artifact.rows); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		rows, readErr := csv.NewReader(f).ReadAll()
		closeErr := f.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("serialized CSV read/close errors: %v/%v", readErr, closeErr)
		}
		if len(rows) != len(artifact.want)+1 || !reflect.DeepEqual(rows[0], wantHeader) {
			t.Fatalf("%s columns/row count=%v, want header %v", artifact.name, rows, wantHeader)
		}
		for i, want := range artifact.want {
			if !reflect.DeepEqual(rows[i+1][18:], want) {
				t.Errorf("%s row %d counters=%v, want %v", artifact.name, i, rows[i+1][18:], want)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "gates.json")
	if err := writeJSONAtomic(path, gates); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Overall map[string]json.RawMessage `json:"overall"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"cap_loss_discards": "10", "loss_publication_recoveries": "5", "loss_idle_retirements": "3", "published_loss_keys": "0"} {
		if got := string(document.Overall[name]); got != want {
			t.Errorf("gates.json overall.%s=%q, want cumulative/final %s", name, got, want)
		}
	}
}

func TestB32ReplayRunbookKeepsRareLossVisible(t *testing.T) {
	tracker := stats.NewTracker()
	for i := range 5 {
		tracker.ObserveCallCorrectionDecision("resolver_primary", "rejected", "resolver_no_snapshot", 0)
		if i < 4 {
			tracker.ObserveCallCorrectionDecision("resolver_primary", "rejected", "resolver_same_call", 1)
		}
	}
	tracker.ObserveCallCorrectionDecision("resolver_primary", "rejected", "resolver_evidence_loss", 0)
	if top := formatTopCounterSummary(tracker.CorrectionDecisionReasons(), 2); strings.Contains(top, "resolver_evidence_loss") {
		t.Fatalf("fixture did not put rare loss below the existing top two: %s", top)
	}
	if top := formatTopCounterSummary(tracker.CorrectionDecisionReasons(), 3); top != "resolver_no_snapshot:5 resolver_same_call:4 resolver_evidence_loss:1" {
		t.Fatalf("rare loss must remain counted below the display cutoff: %s", top)
	}
	if got := formatCorrectionDecisionSummary(tracker); !strings.Contains(got, "evidence_loss:1") ||
		!strings.Contains(got, "resolver_no_snapshot:5") || !strings.Contains(got, "resolver_same_call:4") {
		t.Fatalf("rare loss or existing top reasons disappeared: %s", got)
	}
	line := formatResolverSummaryFromMetrics(spot.SignalResolverMetrics{CapLossDiscards: 7, LossPublicationRecoveries: 2,
		LossIdleRetirements: 1, PublishedLossKeys: 3})
	if !strings.Contains(line, "loss discards 7 publication recoveries 2 idle retirements 1 published keys 3") {
		t.Fatalf("runbook lifecycle counter meanings/numbers missing: %s", line)
	}
}

func TestB32ReplayLossSelectionHasNamedSerializedCounter(t *testing.T) {
	var metrics replayABMetrics
	metrics.ObserveResolverSnapshot(spot.ResolverSnapshot{State: spot.ResolverStateUncertain, EvidenceLoss: true}, true)
	metrics.ObserveResolverSnapshot(spot.ResolverSnapshot{State: spot.ResolverStateConfident}, true)
	metrics.ObserveResolverSnapshot(spot.ResolverSnapshot{EvidenceLoss: true}, false)
	path := filepath.Join(t.TempDir(), "gates.json")
	gates := gatesSummary{}
	gates.Overall.ABMetrics = metrics
	if err := writeJSONAtomic(path, gates); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Overall struct {
			ABMetrics struct {
				Resolver map[string]json.RawMessage `json:"resolver"`
			} `json:"ab_metrics"`
		} `json:"overall"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if got := string(document.Overall.ABMetrics.Resolver["evidence_loss_selections"]); got != "1" {
		t.Fatalf("named actual loss selection count=%q, want 1", got)
	}
	if metrics.Resolver.Samples != 3 || metrics.Resolver.MissingSnapshot != 1 || metrics.Output.ConfidenceCounts.Samples != 0 {
		t.Fatalf("loss instrumentation changed existing selection/output accounting: %+v", metrics)
	}
}

func TestB32ReplayLossDiagnosticsSeparateQueryExpiryAndOwnerPublication(t *testing.T) {
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 8, MaxCandidatesPerKey: 2,
		MaxReportersPerCand: 2, EvalMinInterval: time.Nanosecond, HysteresisWindows: 1, SweepInterval: time.Hour})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	admit := func(receiver string, observed time.Time) {
		t.Helper()
		if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: observed, DXCall: "K1ABC", Spotter: receiver,
			FrequencyKHz: 7010, RecencyWindow: time.Minute}) || d.Drain() != 1 {
			t.Fatal("fixture did not admit one receiver")
		}
	}
	admit("N0AAA", at)
	admit("N0BBB", at)
	admit("N0CCC", at.Add(30*time.Second))
	before := r.MetricsSnapshot()
	if before.CapLossDiscards != 1 || before.LossPublicationRecoveries != 0 || before.LossIdleRetirements != 0 || before.PublishedLossKeys != 1 {
		t.Fatalf("fixture did not publish one actual cap discard: %+v", before)
	}
	for range 3 {
		fresh, ok := r.LookupEligible(key, at.Add(61*time.Second))
		if !ok || fresh.EvidenceLoss {
			t.Fatalf("query-only expiry control did not recover eligibility: %+v available=%t", fresh, ok)
		}
	}
	after := r.MetricsSnapshot()
	if after.CapLossDiscards != 1 || after.LossPublicationRecoveries != 0 || after.LossIdleRetirements != 0 || after.PublishedLossKeys != 1 {
		t.Fatalf("query expiry changed owner counters/latest-publication gauge: %+v", after)
	}
	if line := formatResolverSummaryFromMetrics(after); !strings.Contains(line, "loss discards 1 publication recoveries 0 idle retirements 0 published keys 1") {
		t.Fatalf("runbook relabeled query recovery as owner publication: %s", line)
	}
	path := filepath.Join(t.TempDir(), "runbook_samples.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	tracker := stats.NewTracker()
	tracker.ObserveCallCorrectionDecision("resolver_primary", "rejected", "resolver_evidence_loss", 0)
	runner := &replayRunner{resolver: r, driver: d, tracker: tracker, runbookSamplesFile: file}
	if err := runner.emitSample(at.Add(31 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if text := string(data); !strings.Contains(text, "2026/10/11 12:00:31 CorrGate:") ||
		!strings.Contains(text, "evidence_loss:1") || !strings.Contains(text, "loss discards 1 publication recoveries 0 idle retirements 0 published keys 1") {
		t.Fatalf("actual runbook sample lost fixed diagnostic fields: %s", text)
	}
	admit("N0CCC", at.Add(61*time.Second))
	published := r.MetricsSnapshot()
	if published.CapLossDiscards != 1 || published.LossPublicationRecoveries != 1 || published.PublishedLossKeys != 0 {
		t.Fatalf("actual owner recovery was not counted once: %+v", published)
	}
	if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup(key); ok || r.MetricsSnapshot().LossIdleRetirements != 0 {
		t.Fatal("logical retirement either retained authority or counted physical owner work")
	}
	d.Sweep(at.Add(61 * time.Second))
	retired := r.MetricsSnapshot()
	if retired.LossIdleRetirements != 1 || retired.CapLossDiscards != 1 || retired.LossPublicationRecoveries != 1 || retired.PublishedLossKeys != 0 {
		t.Fatalf("physical idle retirement metadata was not counted exactly once: %+v", retired)
	}
}
