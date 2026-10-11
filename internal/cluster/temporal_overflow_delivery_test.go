//go:build !qualification

package cluster

import (
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestRuntimeTemporalOverflowDeliveredFreshAuthority(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, action := range []string{"fallback_resolver", "bypass", "abstain"} {
		for _, population := range []string{"expired_unswept", "fresh_new_settled"} {
			t.Run(action+"/"+population, func(t *testing.T) {
				cfg := temporalDeliveryConfig()
				cfg.TemporalDecoder.MaxPending, cfg.TemporalDecoder.OverflowAction = 1, action
				at := time.Now().UTC().Truncate(time.Second)
				old := at.Add(-10 * time.Second)
				r, driver, key := temporalDeliveryPopulation(t, old, time.Second, "K1ABC")
				saved := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
				if !saved.SnapshotOK || saved.Snapshot.Winner != "K1ABC" || saved.Snapshot.TotalReporters != 4 {
					t.Fatal("overflow control did not publish saved A3/B1 authority")
				}
				fixture := newAmbiguityDeliveryFixture(t, r, cfg)
				p := fixture.pipeline
				p.recentBandStore, p.customSCPStore = nil, nil
				controller := newRuntimeTemporalController(cfg)
				p.temporal = controller
				filler := newDeliveryTestSpot("K1ABD", "N0AAA", at)
				// A newly held request may capture an unswept old publication.
				// Keep its deadline in the future so the real output loop's due
				// retirement would not reclaim this capacity before admission.
				temporalDeliveryObserve(t, controller, time.Now().UTC(), key, filler, saved)
				if len(controller.pending) != 1 || controller.decoder.PendingCount() != 1 {
					t.Fatal("actual overflow fixture did not fill the decoder's one-request capacity")
				}
				if due, ok := controller.NextDue(); !ok || !due.After(time.Now().UTC()) {
					t.Fatal("overflow filler is already due")
				}
				want := correctionEvidenceDeliveryCase{wantCall: "K1ABD", wantGlyph: "?"}
				if population == "fresh_new_settled" {
					fresh := time.Now().UTC()
					temporalDeliveryRetirePopulation(t, r, driver, key, fresh, "K1ABE")
					temporalDeliverySeed(t, r, driver, key, fresh, 2*time.Minute, "K1ABE", "K1ABD", 3, 1)
					current, ok := r.Lookup(key)
					if !ok || current.Winner != "K1ABE" || current.TotalReporters != 4 ||
						correctionflow.ResolverSupportForCall(current, "K1ABC") != 0 {
						t.Fatalf("fresh overflow population did not retire saved A and settle C3/B1: %+v", current)
					}
					if action == "abstain" {
						want.wantGlyph, want.wantPercent = "P", 25
					} else {
						want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = "K1ABE", "C", 75, 1
					}
				} else if current, ok := r.Lookup(key); !ok || current.TotalReporters != 4 ||
					current.ReceiverEvidence().RecencyWindow() != time.Second {
					t.Fatal("expired overflow fixture was swept before the incoming request")
				}
				incoming := newDeliveryTestSpot("K1ABD", "N0AAA", at)
				incoming.SourceType = spot.SourceManual
				// The driver stays idle. This incoming evidence is enqueued, while
				// maybeHoldTemporalSpot encounters the actual occupied capacity.
				p.processSpot(incoming, nil)
				fallback, bypass, abstain := p.tracker.TemporalFallbackResolver(), p.tracker.TemporalOverflowBypass(), p.tracker.TemporalAbstainLowMargin()
				if fallback+bypass+abstain != 1 || action == "fallback_resolver" && fallback != 1 ||
					action == "bypass" && bypass != 1 || action == "abstain" && abstain != 1 {
					t.Fatalf("incoming request did not reach its configured overflow action: fallback=%d bypass=%d abstain=%d", fallback, bypass, abstain)
				}
				if p.tracker.TemporalCommitted() != want.wantCorrections || p.tracker.TemporalPathSwitches() != 0 {
					t.Error("overflow acceptance/history counters used saved authority")
				}
				temporalDeliveryAssertAccounting(t, p, 0, 1, int(want.wantCorrections))
				if len(controller.pending) != 1 || len(controller.queue) != 1 || controller.decoder.PendingCount() != 1 || r.MetricsSnapshot().QueueDepth != 1 {
					t.Fatal("overflow control consumed the original hold or processed incoming evidence")
				}
				if population == "expired_unswept" {
					temporalDeliveryCheckWithoutPercent(t, fixture, want, at, true)
				} else {
					fixture.check(t, want, at)
				}
				// Retire the filler without reentering output: it is capacity proof,
				// not a second delivered observation or accepted correction.
				if releases := controller.Drain(time.Now().UTC(), true); len(releases) != 1 ||
					len(controller.pending) != 0 || len(controller.queue) != 0 || controller.decoder.PendingCount() != 0 {
					t.Fatal("forced cleanup stranded the original overflow-control hold")
				}
				temporalDeliveryAssertAccounting(t, p, 0, 1, int(want.wantCorrections))
			})
		}
	}
}

func TestRuntimeTemporalForceBeforeDeadlineDelivered(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := temporalDeliveryConfig()
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 73*60*60, 74*60*60
	at := time.Now().UTC().Truncate(time.Second)
	old := at.Add(-10 * time.Second)
	r, _, key := temporalDeliveryPopulation(t, old, time.Second, "K1ABC")
	saved := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
	fixture := newAmbiguityDeliveryFixture(t, r, cfg)
	p := fixture.pipeline
	p.recentBandStore, p.customSCPStore = nil, nil
	controller := newRuntimeTemporalController(cfg)
	p.temporal = controller
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	temporalDeliveryObserve(t, controller, old, key, s, saved)
	now := time.Now().UTC()
	if releases := controller.Drain(now, false); len(releases) != 0 {
		t.Fatal("ordinary drain released the future-deadline control")
	}
	if due, ok := controller.NextDue(); !ok || !due.Equal(old.Add(73*time.Hour)) || !due.After(now.Add(72*time.Hour)) {
		t.Fatal("forced delivery control did not retain its future deadline")
	}
	releases := controller.Drain(now, true)
	if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionApply ||
		releases[0].decision.Winner != "K1ABC" || releases[0].decision.CommitLatency != now.Sub(old) || !releases[0].releaseAt.Equal(now) {
		t.Fatalf("forced future-deadline drain did not preserve the actual release clock/proposal: %+v", releases)
	}
	p.processSpot(releases[0].pending.spot, &releases[0])
	if p.tracker.TemporalCommitted() != 0 || p.tracker.TemporalPathSwitches() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatal("forced retirement accepted expired saved evidence")
	}
	temporalDeliveryAssertAccounting(t, p, 1, 1, 0)
	if len(controller.pending) != 0 || len(controller.queue) != 0 || controller.decoder.PendingCount() != 0 {
		t.Fatal("forced actual-time delivery stranded pending ownership")
	}
	temporalDeliveryCheckWithoutPercent(t, fixture, correctionEvidenceDeliveryCase{wantCall: "K1ABD", wantGlyph: "?"}, at, true)
}
