//go:build !qualification

package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
)

func TestCallCorrectionTemporalDeliveredAcceptanceHistory(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, mode := range []string{"cty_rejection", "invalid_base_rejection", "cty_suppression", "invalid_base_suppression", "unchanged_proposal", "early_age_exit"} {
		t.Run(mode, func(t *testing.T) {
			cfg := temporalDeliveryConfig()
			suppressed := strings.HasSuffix(mode, "_suppression")
			if suppressed {
				cfg.InvalidAction = "suppress"
			}
			at := time.Now().UTC().Truncate(time.Second)
			observed := at.Add(-3 * time.Second)
			winner, subject := "K1ABC", "K1ABD"
			if strings.HasPrefix(mode, "invalid_base_") {
				winner = "K1ABC?"
				if !shouldRejectCTYCall(winner) || spot.CallDistance(subject, winner, "CW", "morse", "baudot") > cfg.MaxEditDistance {
					t.Fatal("invalid-base fixture did not qualify distance before base rejection")
				}
			}
			if mode == "unchanged_proposal" {
				subject = winner
			}
			r, driver, key := temporalDeliveryPopulation(t, observed, 2*time.Minute, winner)
			fixture := newAmbiguityDeliveryFixture(t, r, cfg)
			p := fixture.pipeline
			if strings.HasPrefix(mode, "cty_") {
				// Empty valid CTY data is a deterministic miss, independently of
				// host files or license-store availability.
				db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0"?><plist version="1.0"><dict></dict></plist>`))
				if err != nil {
					t.Fatal(err)
				}
				p.ctyLookup = func() *cty.CTYDatabase { return db }
			}
			controller := newRuntimeTemporalController(cfg)
			p.temporal = controller
			s := newDeliveryTestSpot(subject, "N0AAA", at)
			s.SourceType = spot.SourceManual
			if mode == "early_age_exit" {
				p.spotPolicy.MaxAgeSeconds = 1
				s.Time = at.Add(-time.Hour)
			}
			selection := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, subject)
			temporalDeliveryObserve(t, controller, observed, key, s, selection)
			release := temporalDeliveryDrainApply(t, controller, time.Now().UTC(), winner)
			p.processSpot(release.pending.spot, &release)
			wantHandoffs := uint64(1)
			if mode == "early_age_exit" || suppressed {
				wantHandoffs = 0
			}
			temporalDeliveryAssertAccounting(t, p, 1, wantHandoffs, 0)
			if p.tracker.TemporalCommitted() != 0 || p.tracker.TemporalPathSwitches() != 0 || p.tracker.CallCorrections() != 0 {
				t.Errorf("rejected/unchanged proposal updated accepted history counters: commits=%d switches=%d corrections=%d",
					p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches(), p.tracker.CallCorrections())
			}
			want := correctionEvidenceDeliveryCase{wantCall: subject, wantGlyph: "B"}
			if mode == "early_age_exit" || suppressed {
				if suppressed {
					reason := resolverDecisionCTYMiss
					if strings.HasPrefix(mode, "invalid_base_") {
						reason = resolverDecisionInvalidBaseCall
					}
					if p.tracker.CorrectionDecisionReasons()[reason] != 1 {
						t.Fatalf("suppression did not reach requested ordinary rejection gate %s", reason)
					}
				}
				if fixture.ring.GetCount() != 0 {
					t.Error("early-age exit/suppression reached delivered output")
				}
				fixture.writer.Stop()
				reopened, err := archive.NewWriter(fixture.archiveCfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(reopened.Stop)
				rows, err := reopened.Recent(2)
				if err != nil || len(rows) != 0 {
					t.Errorf("early-age exit/suppression reached persisted history: rows=%v err=%v", rows, err)
				}
			} else if mode == "unchanged_proposal" {
				want.wantGlyph, want.wantPercent = "V", 75
				fixture.check(t, want, at)
			} else {
				reason := resolverDecisionCTYMiss
				if strings.HasPrefix(mode, "invalid_base_") {
					reason = resolverDecisionInvalidBaseCall
				}
				if p.tracker.CorrectionDecisionReasons()[reason] != 1 {
					t.Errorf("fixture did not reach requested rejection gate %s: %v", reason, p.tracker.CorrectionDecisionReasons())
				}
				temporalDeliveryCheckWithoutPercent(t, fixture, want, at, false)
			}
			// Reevaluate the same key and decoder with a different legitimately
			// accepted winner. A proposal rejected above cannot create a false
			// confirmed path switch here. Narrow the live window once to retire
			// the old membership; expanding it afterward must not resurrect it.
			fresh := time.Now().UTC()
			temporalDeliveryRetirePopulation(t, r, driver, key, fresh, "K1ABD")
			temporalDeliverySeed(t, r, driver, key, fresh, 2*time.Minute, "K1ABD", "K1ABE", 3, 1)
			current, ok := r.Lookup(key)
			if !ok || current.Winner != "K1ABD" || current.TotalReporters != 4 ||
				correctionflow.ResolverSupportForCall(current, winner) != 0 {
				t.Fatalf("history control retained obsolete first-proposal support: %+v", current)
			}
			second := newAmbiguityDeliveryFixture(t, r, cfg)
			second.pipeline.temporal, second.pipeline.tracker = controller, p.tracker
			secondAt := time.Now().UTC().Truncate(time.Second)
			next := newDeliveryTestSpot("K1ABE", "N0AAA", secondAt)
			next.SourceType = spot.SourceManual
			selection = correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
			temporalDeliveryObserve(t, controller, secondAt.Add(-3*time.Second), key, next, selection)
			release = temporalDeliveryDrainApply(t, controller, time.Now().UTC(), "K1ABD")
			second.pipeline.processSpot(release.pending.spot, &release)
			temporalDeliveryAssertAccounting(t, second.pipeline, 2, wantHandoffs+1, 1)
			if p.tracker.TemporalCommitted() != 1 || p.tracker.TemporalPathSwitches() != 0 {
				t.Errorf("first accepted mutation inherited rejected history: commits=%d switches=%d want 1,0",
					p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches())
			}
			second.check(t, correctionEvidenceDeliveryCase{wantCall: "K1ABD", wantGlyph: "C", wantPercent: 75, wantCorrections: 1}, secondAt)
		})
	}
}

func TestCallCorrectionTemporalDeliveredRetryAcceptsOnce(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := temporalDeliveryConfig()
	cfg.StabilizerEnabled = true
	cfg.StabilizerMaxChecks, cfg.StabilizerAmbiguousMaxChecks = 2, 2
	at := time.Now().UTC().Truncate(time.Second)
	old := at.Add(-10 * time.Second)
	r, driver, key := temporalDeliveryPopulation(t, old, time.Second, "K1ABC")
	saved := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
	fixture := newAmbiguityDeliveryFixture(t, r, cfg)
	p := fixture.pipeline
	p.stabilizerEnabled = true
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 2)
	p.telnetStabilizer.Start()
	t.Cleanup(p.telnetStabilizer.Stop)
	controller := newRuntimeTemporalController(cfg)
	p.temporal = controller
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	temporalDeliveryObserve(t, controller, old, key, s, saved)
	temporalDeliverySeed(t, r, driver, key, time.Now().UTC(), 2*time.Minute, "K1ABC", "K1ABD", 3, 2)
	release := temporalDeliveryDrainApply(t, controller, time.Now().UTC(), "K1ABC")
	p.processSpot(release.pending.spot, &release)
	temporalDeliveryAssertAccounting(t, p, 1, 0, 0)
	if fixture.ring.GetCount() != 0 || p.tracker.TemporalCommitted() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatalf("retry fixture blocked by premature identity mutation/commit before hold: ring=%d commits=%d corrections=%d",
			fixture.ring.GetCount(), p.tracker.TemporalCommitted(), p.tracker.CallCorrections())
	}
	first := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
	if first.temporalResult == nil || first.temporalResult.decoder != controller.decoder ||
		first.temporalResult.key != key || first.temporalResult.tracker != p.tracker {
		t.Fatal("initial actual scheduler release lost the request-owned temporal result")
	}
	p.handleStabilizerRelease(first) // Same live split: preserve the request through one actual retry.
	temporalDeliveryAssertAccounting(t, p, 1, 0, 0)
	if fixture.ring.GetCount() != 0 || p.tracker.TemporalCommitted() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatal("unchanged split retry counted acceptance or emitted before final release")
	}
	second := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 1)
	if second.temporalResult != first.temporalResult {
		t.Fatal("actual scheduler retry replaced the request-owned temporal result pointer")
	}
	temporalDeliverySeed(t, r, driver, key, time.Now().UTC(), 2*time.Minute, "K1ABC", "K1ABD", 4, 2)
	p.handleStabilizerRelease(second)
	temporalDeliveryAssertAccounting(t, p, 1, 1, 1)
	if p.tracker.TemporalCommitted() != 1 || p.tracker.TemporalPathSwitches() != 0 || p.tracker.CallCorrections() != 1 {
		t.Errorf("accepted retry must confirm exactly once: commits=%d switches=%d corrections=%d",
			p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches(), p.tracker.CallCorrections())
	}
	if len(controller.Drain(time.Now().UTC(), true)) != 0 || len(controller.pending) != 0 || p.telnetStabilizer.Pending() != 0 {
		t.Fatal("accepted held request retained release ownership after final mutation")
	}
	fixture.check(t, correctionEvidenceDeliveryCase{wantCall: "K1ABC", wantGlyph: "C", wantPercent: 66, wantCorrections: 1}, at)
}

func TestCallCorrectionTemporalResultAcceptsDistinctMutations(t *testing.T) {
	// An accepted C glyph does not normally enter the stabilizer. Exercise the
	// actual ordinary gates twice with one request context, separately from the
	// reachable scheduler ownership test above.
	cfg := temporalDeliveryConfig()
	at := time.Now().UTC().Add(-3 * time.Second)
	r, driver, key := temporalDeliveryPopulation(t, at, 2*time.Minute, "K1ABC")
	controller := newRuntimeTemporalController(cfg)
	p := &outputPipeline{signalResolver: r, correctionCfg: cfg, tracker: stats.NewTracker(), temporal: controller}
	result := &runtimeTemporalResult{decoder: controller.decoder, key: key, tracker: p.tracker}
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	apply := func() {
		t.Helper()
		now := time.Now().UTC()
		selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, normalizedDXCall(s), now)
		if maybeApplyResolverCorrectionWithSelectionOverride(s, r, spot.ResolverEvidence{Key: key}, true, cfg,
			nil, nil, p.tracker, nil, nil, nil, nil, nil, nil, nil, &selection, now, result) {
			t.Fatal("ordinary mutation control unexpectedly suppressed the request")
		}
	}
	apply()
	if normalizedDXCall(s) != "K1ABC" || p.tracker.CallCorrections() != 1 || p.tracker.TemporalCommitted() != 1 {
		t.Fatal("first ordinary-gate accepted B-to-A mutation was not confirmed")
	}
	apply() // Same ordinary winner: no identity mutation and no second acceptance.
	if p.tracker.CallCorrections() != 1 || p.tracker.TemporalCommitted() != 1 || p.tracker.TemporalPathSwitches() != 0 {
		t.Fatal("unchanged ordinary retry counted another accepted mutation")
	}
	fresh := time.Now().UTC()
	temporalDeliveryRetirePopulation(t, r, driver, key, fresh, "K1ABE")
	temporalDeliverySeed(t, r, driver, key, fresh, 2*time.Minute, "K1ABE", "K1ABC", 3, 1)
	apply()
	if normalizedDXCall(s) != "K1ABE" || p.tracker.CallCorrections() != 2 || p.tracker.TemporalCommitted() != 2 ||
		p.tracker.TemporalPathSwitches() != 1 {
		t.Fatalf("later real A-to-C mutation was hidden by a consumed acceptance flag: call=%s corrections=%d committed=%d switches=%d",
			normalizedDXCall(s), p.tracker.CallCorrections(), p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches())
	}
	temporalDeliveryAssertAccounting(t, p, 0, 0, 1)
	// A repeated confirmation of C must not switch. This observes the retained
	// identity through the public confirmation contract without exposing its map.
	if controller.decoder.ConfirmCorrection(key, "K1ABE") {
		t.Fatal("confirmed history retained A after the accepted A-to-C mutation")
	}
}

func temporalDeliveryAssertAccounting(t *testing.T, p *outputPipeline, proposed, handoffs uint64, historyKeys int) {
	t.Helper()
	if got := p.tracker.TemporalProposed(); got != proposed {
		t.Errorf("temporal Apply proposals=%d want %d", got, proposed)
	}
	if got := p.tracker.TemporalHandoffAttempts(); got != handoffs {
		t.Errorf("request consumer handoff attempts=%d want %d", got, handoffs)
	}
	if got := p.temporal.decoder.ConfirmedHistoryMetrics(); got.Keys != historyKeys || got.Evictions != 0 {
		t.Errorf("confirmed mutation history=%+v want keys=%d evictions=0", got, historyKeys)
	}
}

func temporalDeliveryReceiveRetry(t *testing.T, s *telnetSpotStabilizer, key spot.ResolverSignalKey, checks int) *telnetStabilizerEnvelope {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case envelope := <-s.ReleaseChan():
		if envelope == nil || envelope.spot == nil || envelope.checksCompleted != checks || !envelope.hasResolverKey ||
			envelope.resolverKey != key || !envelope.evidenceEnqueued {
			t.Fatalf("actual retry scheduler lost held request context: %+v", envelope)
		}
		return envelope
	case <-timer.C:
		t.Fatal("actual stabilizer scheduler did not publish the held temporal request")
		return nil
	}
}
