//go:build !qualification

// File role: Connects actual zero-weight receiver admission to retained P/0%
// delivery, reason-specific retry budgets, temporal release and V-only history.
// Loaded production owners and a successful isolated occurrence control precede
// every no-learning or unavailable-history assertion.
package cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type supportBasisRuntimeFixture struct {
	familyDeliveryFixture
	key      spot.ResolverSignalKey
	control  familyDeliveryInput
	incoming familyDeliveryInput
	events   func() []string
}

func newSupportBasisRuntimeFixture(t *testing.T, source string, custom bool, path string) supportBasisRuntimeFixture {
	t.Helper()
	f := supportBasisRuntimeFixture{familyDeliveryFixture: acceptedLearningPathsFixture(t, source, custom, 1, 8, true, false)}
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	f.control = acceptedLearningPathsControl(t, f.familyDeliveryFixture, at)
	f.incoming = familyDeliveryInput{"K1ABC", "N0AAA", "zero weighted source", "P", at.Add(time.Second)}
	p := f.pipeline
	p.correctionCfg.MinConsensusReports, p.correctionCfg.MinAdvantage, p.correctionCfg.MinConfidencePercent = 2, 0, 0
	p.correctionCfg.MaxEditDistance, p.correctionCfg.MinSpotterReliability = 1, 0
	p.correctionCfg.ResolverNeighborhoodEnabled, p.correctionCfg.ResolverRecentPlus1Enabled, p.correctionCfg.BayesBonus.Enabled = false, false, false
	p.correctionCfg.StabilizerMaxChecks, p.correctionCfg.StabilizerAmbiguousMaxChecks = 5, 3
	p.correctionCfg.StabilizerPDelayConfidencePercent, p.correctionCfg.StabilizerPDelayMaxChecks = 0, 2
	p.correctionCfg.TemporalDecoder.Enabled = false
	if strings.HasPrefix(path, "retry_") {
		p.correctionCfg.StabilizerPDelayConfidencePercent = 25
		if path == "retry_suppress" {
			p.correctionCfg.StabilizerTimeoutAction = stabilizerTimeoutSuppress
		}
	}
	if strings.HasPrefix(path, "temporal_") {
		margin := 0
		if path == "temporal_abstain" {
			margin = 1_000_000
		}
		p.correctionCfg.TemporalDecoder = runtimeTemporalTestConfig(margin).TemporalDecoder
		if path == "temporal_abstain" {
			// The configured overflow action also owns failed terminal confidence gates.
			p.correctionCfg.TemporalDecoder.OverflowAction = "abstain"
		}
	}
	p.temporal = newRuntimeTemporalController(p.correctionCfg)
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 8, MaxActiveKeys: 2, MaxCandidatesPerKey: 4, MaxReportersPerCand: 4,
		InactiveTTL: time.Hour, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, MaxEditDistance: 1, DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		SpotterReliability: spot.SpotterReliability{"N0AAA": 0, "N0BBB": 0, "N0CCC": 1},
	})
	t.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	f.key = spot.NewResolverSignalKey(14030, "20m", "CW", p.correctionCfg.FrequencyToleranceHz)
	observed := at.Add(-time.Second)
	correctionLossAdmit(t, r, driver, f.key, "K1ABC", 14030, observed, 2*time.Minute, "N0AAA", "N0BBB")
	correctionLossAdmit(t, r, driver, f.key, "K1ABD", 14030, observed, 2*time.Minute, "N0CCC")
	p.signalResolver = r
	selected := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, f.key, p.correctionCfg, "K1ABC", time.Now().UTC())
	snapshot := selected.Snapshot
	if !selected.SnapshotOK || selected.NeighborhoodSplit || snapshot.EvidenceLoss || snapshot.SupportBasis != spot.ResolverSupportBasisWeighted ||
		snapshot.State != spot.ResolverStateConfident || snapshot.Winner != "K1ABD" || snapshot.WinnerSupport != 1 ||
		snapshot.TotalReporters != 3 || snapshot.TotalWeightedSupportMilli != 1000 || snapshot.ReceiverEvidence() == nil || snapshot.ReceiverEvidence().Len() != 3 ||
		correctionflow.ResolverSupportForCall(snapshot, "K1ABC") != 2 || correctionflow.ResolverWeightedSupportForCall(snapshot, "K1ABC") != 0 {
		t.Fatalf("actual A2-zero/B1-unit eligible owner population is required: %+v", selected)
	}
	if percent, ok := correctionflow.ResolverCallConfidencePercent(snapshot, "K1ABC"); !ok || percent != 0 {
		t.Fatalf("known weighted zero must be available: percentage=%d available=%t", percent, ok)
	}
	if percent, ok := correctionflow.ResolverCallConfidencePercent(snapshot, "K1ABD"); !ok || percent != 100 {
		t.Fatalf("positive unit receiver must supply the whole weighted denominator: %d/%t", percent, ok)
	}
	correctionLossEvictions(t, r, 0, 0)
	f.events = b32CaptureDecisions(t, p, "ui")
	return f
}

func (f supportBasisRuntimeFixture) assertPending(t *testing.T, holds uint64) {
	t.Helper()
	p := f.pipeline
	if p.tracker.StabilizerHeld() != holds || p.tracker.StabilizerHeldByReason()[stabilizerDelayReasonPLowConfidence.String()] != holds || f.ring.GetCount() != 1 ||
		p.tracker.StabilizerReleasedDelayed() != 0 || p.tracker.StabilizerSuppressedTimeout() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatal("weighted zero did not retain actual low-P ownership before fanout")
	}
	acceptedLearningPathsWho(t, f.familyDeliveryFixture, f.incoming.call, 0)
	if acceptedLearningPathsSupport(f.familyDeliveryFixture, f.incoming.call) != 0 {
		t.Fatal("pending/reheld weighted-zero source learned receiver history")
	}
}

func (f supportBasisRuntimeFixture) resolveRetries(t *testing.T, suppress bool) {
	t.Helper()
	for completed := 0; completed < 2; completed++ {
		f.assertPending(t, uint64(completed+1))
		envelope := temporalDeliveryReceiveRetry(t, f.pipeline.telnetStabilizer, f.key, completed)
		if envelope.delayReason != stabilizerDelayReasonPLowConfidence.String() || envelope.peerOriginal == nil ||
			envelope.spot.Confidence != "P" || !envelope.spot.ConfidencePercentOK || envelope.spot.ConfidencePercent != 0 {
			t.Fatal("actual scheduler lost known-zero confidence or low-P reason")
		}
		f.pipeline.handleStabilizerRelease(envelope)
	}
	p := f.pipeline
	if p.telnetStabilizer.Pending() != 0 || p.tracker.StabilizerHeld() != 2 {
		t.Fatal("low-P budget must exhaust exactly at two checks despite base-five/ambiguity-three budgets")
	}
	if suppress {
		if p.tracker.StabilizerSuppressedTimeout() != 1 || p.tracker.StabilizerSuppressedByReason()[stabilizerDelayReasonPLowConfidence.String()] != 1 || p.tracker.StabilizerReleasedDelayed() != 0 {
			t.Fatal("known-zero exhaustion did not preserve timeout suppression accounting")
		}
	} else if p.tracker.StabilizerReleasedDelayed() != 1 || p.tracker.StabilizerReleasedDelayedByReason()[stabilizerDelayReasonPLowConfidence.String()] != 1 || p.tracker.StabilizerSuppressedTimeout() != 0 {
		t.Fatal("known-zero exhaustion did not preserve delayed release accounting")
	}
}

func (f supportBasisRuntimeFixture) resolveTemporal(t *testing.T, abstain bool) {
	t.Helper()
	p := f.pipeline
	if p.tracker.TemporalPending() != 1 || p.telnetStabilizer.Pending() != 0 || f.ring.GetCount() != 1 {
		t.Fatal("mixed weighted-zero input did not first enter actual temporal ownership")
	}
	acceptedLearningPathsWho(t, f.familyDeliveryFixture, f.incoming.call, 0)
	for _, pending := range p.temporal.pending {
		if pending.selection.Snapshot.SupportBasis != spot.ResolverSupportBasisWeighted || pending.selection.Snapshot.TotalWeightedSupportMilli != 1000 || pending.selection.Snapshot.ReceiverEvidence() != nil {
			t.Fatal("temporal compaction lost the weighted denominator or retained receiver rows")
		}
	}
	releases := p.temporal.Drain(time.Now().UTC(), true)
	wantAction := correctionflow.TemporalDecisionActionApply
	if abstain {
		wantAction = correctionflow.TemporalDecisionActionAbstain
	}
	if len(releases) != 1 || releases[0].decision.Action != wantAction || (!abstain && releases[0].decision.Winner != "K1ABD") {
		t.Fatalf("actual terminal temporal decision did not qualify the intended path: %+v", releases)
	}
	p.tracker.DecrementTemporalPending()
	p.processSpot(releases[0].pending.spot, &releases[0])
	if p.tracker.TemporalCommitted() != 0 || p.tracker.TemporalPathSwitches() != 0 || p.tracker.TemporalHandoffAttempts() != 1 ||
		len(p.temporal.pending) != 0 || p.temporal.decoder.PendingCount() != 0 {
		t.Fatal("temporal release changed accepted identity/accounting or retained request ownership")
	}
	if (abstain && p.tracker.TemporalAbstainLowMargin() != 1) || (!abstain && p.tracker.TemporalProposed() != 1) {
		t.Fatal("temporal fixture did not exercise exactly one expected proposal/abstention")
	}
}

func (f supportBasisRuntimeFixture) assertZeroReceipts(t *testing.T) {
	t.Helper()
	for _, destination := range []string{"normal", "DIAG", "SELF"} {
		conn, reader := f.normalConn, f.normalReader
		switch destination {
		case "DIAG":
			conn, reader = f.diagConn, f.diagReader
		case "SELF":
			conn, reader = f.selfConn, f.selfReader
		}
		line := correctionEvidenceReadDX(t, conn, reader)
		if len(line) < 78 || strings.TrimSpace(line[27:39]) != f.incoming.call || line[71:72] != "P" {
			t.Fatalf("%s must deliver retained A/P under minimum-report rejection: %q", destination, line)
		}
		if destination == "DIAG" && strings.TrimSpace(line[39:71]) != "CW 10 dB 0%" {
			t.Fatalf("known weighted zero must display available DIAG 0%% while preserving the input report: %q", line)
		}
		if destination != "DIAG" && strings.Contains(line[39:71], "%") {
			t.Fatalf("ordinary output gained a diagnostic percentage: %q", line)
		}
	}
	f.assertPeerReceipts(t, []familyDeliveryInput{f.incoming}, f.incoming.call)
}

func (f supportBasisRuntimeFixture) assertStored(t *testing.T, accepted []familyDeliveryInput) {
	t.Helper()
	rows := f.ring.GetRecent(8)
	f.assertRows(t, "weighted-zero sealed ring", rows, accepted, true)
	for _, row := range rows {
		if row.DXCallNorm == f.incoming.call && (!row.ConfidencePercentOK || row.ConfidencePercent != 0 || row.SnapshotForAsync() != row) {
			t.Fatal("retained A lost available zero or immutable output ownership")
		}
	}
	f.writer.Stop()
	familyDeliveryAssertArchiveBytes(t, f.archiveCfg.DBPath, accepted)
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err = reopened.Recent(8)
	if err != nil {
		t.Fatal(err)
	}
	f.assertRows(t, "weighted-zero reopened archive", rows, accepted, false)
	for _, row := range rows {
		if row.ConfidencePercentOK || row.ConfidencePercent != 0 {
			t.Fatal("archive acquired percentage fields beyond its existing format")
		}
	}
	acceptedLearningPathsAssertCustomDisk(t, f.familyDeliveryFixture, accepted)
}

func TestSupportBasisRuntimeZeroWeightedDelivery(t *testing.T) {
	source := acceptedLearningPathsConfigSource(t)
	for _, custom := range []bool{false, true} {
		for _, path := range []string{"immediate", "retry_release", "retry_suppress", "temporal_apply", "temporal_abstain"} {
			t.Run(fmt.Sprintf("custom_%t/%s", custom, path), func(t *testing.T) {
				temporalDeliveryDirectory(t)
				f := newSupportBasisRuntimeFixture(t, source, custom, path)
				f.pipeline.processSpot(f.incoming.spot(), nil)
				suppressed := path == "retry_suppress"
				switch path {
				case "retry_release", "retry_suppress":
					f.resolveRetries(t, suppressed)
				case "temporal_apply", "temporal_abstain":
					f.resolveTemporal(t, path == "temporal_abstain")
				}
				wantEvaluations := 1
				if strings.HasPrefix(path, "retry_") {
					wantEvaluations = 3
				} else if path == "temporal_abstain" {
					wantEvaluations = 0 // Existing abstention bypasses the ordinary correction wrapper.
				}
				if reasons := f.pipeline.tracker.CorrectionDecisionReasons(); reasons[resolverGateDecisionReason("min_reports")] != uint64(wantEvaluations) || f.pipeline.tracker.CallCorrections() != 0 {
					t.Fatalf("actual B1 minimum-report rejection must retain A without a correction: %v", reasons)
				}
				wantEvent := "Call decision: phase=evaluation outcome=rejected reason=resolver_gate_min_reports input=K1ABC call=K1ABC proposed=K1ABD at=14030.0kHz share=0% basis=weighted recognition=separate suppressed=false"
				if events := f.events(); len(events) != wantEvaluations {
					t.Fatalf("weighted-zero diagnostic evaluation count=%d want %d", len(events), wantEvaluations)
				} else {
					for _, event := range events {
						if event != wantEvent {
							t.Fatalf("known zero lost weighted diagnostic basis: %q", event)
						}
					}
				}
				wantWho, wantHistory := 1, 1
				if suppressed {
					wantWho, wantHistory = 0, 0
				} else if custom {
					wantHistory = 0 // P/0% remains outside existing CustomSCP V-only admission.
				}
				acceptedLearningPathsWho(t, f.familyDeliveryFixture, f.incoming.call, wantWho)
				if got := acceptedLearningPathsSupport(f.familyDeliveryFixture, f.incoming.call); got != wantHistory {
					t.Fatalf("weighted-zero resolved receiver history=%d want %d", got, wantHistory)
				}
				if custom && f.custom.StaticContains(f.incoming.call) {
					t.Fatal("weighted-zero P acquired V-only learned static membership")
				}
				accepted := []familyDeliveryInput{f.control, f.incoming}
				if suppressed {
					barrier := familyDeliveryInput{"K1BAR", "K1BAR", "zero suppression barrier", "V", f.incoming.at.Add(time.Second)}
					f.pipeline.processSpot(barrier.spot(), nil)
					f.assertReceipts(t, []familyDeliveryInput{barrier}, []familyDeliveryInput{barrier}, []familyDeliveryInput{barrier}, barrier.call)
					accepted = []familyDeliveryInput{f.control, barrier}
				} else {
					f.assertZeroReceipts(t)
				}
				f.assertStored(t, accepted)
			})
		}
	}
}
