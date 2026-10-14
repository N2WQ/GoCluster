//go:build !qualification

// File role: Proves that a missing replacement cannot consume an otherwise
// licensed original, and that accepted mutation remains distinct from delivery.
package cluster

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/uls"
)

func replacementLicenseFixture(t *testing.T, candidateFound bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "licenses.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), `CREATE TABLE AM (unique_system_identifier INTEGER, call_sign TEXT, state TEXT NOT NULL);
CREATE INDEX idx_AM_call_sign ON AM(call_sign);
INSERT INTO AM VALUES(1,'K1ABC','CA'),(2,'N0AAA','AP'); PRAGMA user_version=1;`)
	if err == nil && candidateFound {
		_, err = db.ExecContext(t.Context(), "INSERT INTO AM VALUES(3,'K1ABD','TX');")
	}
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("license fixture: query=%v close=%v", err, closeErr)
	}
	previous := uls.LicenseChecksEnabled()
	uls.SetRefreshInProgress(false)
	uls.SetAllowlistPath("")
	uls.SetLicenseChecksEnabled(true)
	uls.SetLicenseDBPath(path)
	t.Cleanup(func() {
		uls.SetLicenseDBPath("")
		uls.SetAllowlistPath("")
		uls.SetLicenseChecksEnabled(previous)
	})
	if got := uls.LookupUS("K1ABC"); !got.Available || !got.Found {
		t.Fatalf("original was not independently licensed: %+v", got)
	}
	if got := uls.LookupUS("K1ABD"); !got.Available || got.Found != candidateFound {
		t.Fatalf("replacement authority=%+v want found=%t", got, candidateFound)
	}
}

func replacementLicenseConfig(t *testing.T) config.CallCorrectionConfig {
	t.Helper()
	loaded, err := config.Load(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := loaded.CallCorrection
	if cfg.MinSpotterReliability != .95 || cfg.StabilizerMaxChecks != 12 {
		t.Fatalf("reviewed shipped admission/budgets changed: cutoff=%g checks=%d", cfg.MinSpotterReliability, cfg.StabilizerMaxChecks)
	}
	// Isolate licensing from bucket competition and initial temporal scheduling.
	// Retry tests retain the loaded delay budgets; no old defaults are copied.
	cfg.ResolverNeighborhoodEnabled = false
	cfg.TemporalDecoder.Enabled = false
	return cfg
}

func replacementLicensePopulation(t *testing.T, cfg config.CallCorrectionConfig, at time.Time) (*spot.SignalResolver, spot.ResolverSignalKey) {
	return replacementLicensePopulationCount(t, cfg, at, 3)
}

func replacementLicensePopulationCount(t *testing.T, cfg config.CallCorrectionConfig, at time.Time, winnerReports int) (*spot.SignalResolver, spot.ResolverSignalKey) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, FreqGuardMinSeparationKHz: cfg.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio: cfg.FreqGuardRunnerUpRatio, MaxEditDistance: cfg.MaxEditDistance,
		DistanceModelCW: cfg.DistanceModelCW, DistanceModelRTTY: cfg.DistanceModelRTTY,
		MinSpotterReliability: cfg.MinSpotterReliability,
	})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", cfg.FrequencyToleranceHz)
	calls := make([]string, winnerReports+1)
	for i := range winnerReports {
		calls[i] = "K1ABD"
	}
	calls[winnerReports] = "K1ABC"
	for i, call := range calls {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call,
			Spotter: []string{"N0AAA", "N0BBB", "N0CCC", "N0DDD", "N0EEE"}[i], FrequencyKHz: 14020, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("eligible equal-unit-weight population was refused")
		}
	}
	if d.Drain() != winnerReports+1 {
		t.Fatal("population was not consumed")
	}
	d.Sweep(at.Add(time.Nanosecond))
	snap, ok := r.LookupEligible(key, time.Now().UTC())
	if !ok || snap.Winner != "K1ABD" || snap.TotalReporters != winnerReports+1 || snap.WinnerSupport != winnerReports ||
		snap.WinnerWeightedSupportMilli != winnerReports*1000 || snap.TotalWeightedSupportMilli != (winnerReports+1)*1000 ||
		correctionflow.ResolverSupportForCall(snap, "K1ABC") != 1 {
		t.Fatalf("literal eligible B3/A1 precondition failed: %+v", snap)
	}
	gate := spot.EvaluateResolverPrimaryGates("K1ABC", "K1ABD", "20m", "CW", 1, winnerReports, winnerReports*100/(winnerReports+1), 1000, winnerReports*1000,
		buildCorrectionSettings(cfg, cfg.MinConsensusReports, 2*time.Minute, cfg.FrequencyToleranceHz, nil), at,
		spot.ResolverPrimaryGateOptions{})
	if !gate.Allow {
		t.Fatalf("licensing target was hidden by an earlier gate: %+v", gate)
	}
	return r, key
}

func TestReplacementLicenseRejectsBeforeMutation(t *testing.T) {
	cfg := replacementLicenseConfig(t)
	replacementLicenseFixture(t, false)
	db := loadIngestCTY(t)
	at := time.Now().UTC().Add(-time.Second)
	r, _ := replacementLicensePopulation(t, cfg, at)
	p := newDeliveryTestPipeline(nil, nil, nil)
	p.correctionCfg, p.signalResolver, p.tracker = cfg, r, stats.NewTracker()
	// applyResolverStage intentionally bypasses mutation without a telnet owner;
	// the actual pipeline fixture supplies that owner in delivered regressions.
	s := spot.NewSpot("K1ABC", "N0AAA", 14020, "CW")
	if applyLicenseGate(s, db, nil, nil) {
		t.Fatal("licensed original failed ordinary final validation")
	}
	metadata := s.DXMetadata
	evidence, ok := buildResolverEvidenceSnapshot(s, cfg, nil, time.Now().UTC())
	if !ok {
		t.Fatal("could not build existing resolver input")
	}
	if maybeApplyResolverCorrection(s, r, evidence, true, cfg, db, nil, p.tracker, nil, nil, nil, nil, nil, nil, nil) {
		t.Fatal("missing replacement suppressed the original")
	}
	if s.DXCallNorm != "K1ABC" || s.Confidence == "C" || s.ConfidencePercent != 25 || !s.ConfidencePercentOK ||
		p.tracker.CallCorrections() != 0 || s.DXMetadata != metadata || applyLicenseGate(s, db, nil, nil) {
		t.Fatalf("replacement consumed a licensed original: call=%s glyph=%s share=%d/%t corrections=%d metadata=%+v",
			s.DXCallNorm, s.Confidence, s.ConfidencePercent, s.ConfidencePercentOK, p.tracker.CallCorrections(), s.DXMetadata)
	}
}

// This uses the normal and DIAG sockets, original-peer publication, sealed ring,
// and reopened archive helpers. Absence and successful correction are separate
// controls so permanent rejection cannot satisfy the delivery contract.
func TestReplacementLicenseDeliveredOriginalAndCorrection(t *testing.T) {
	cfg := replacementLicenseConfig(t)
	temporalDeliveryDirectory(t)
	for _, path := range []string{"immediate", "stabilizer_retry", "temporal_apply"} {
		for _, found := range []bool{false, true} {
			name := "missing_retain_original"
			if found {
				name = "found_apply_replacement"
			}
			t.Run(path+"/"+name, func(t *testing.T) {
				replacementLicenseFixture(t, found)
				at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
				winnerReports := 3
				if path == "stabilizer_retry" {
					winnerReports = 4
				} // 20% is below shipped strict 25% hold threshold.
				r, key := replacementLicensePopulationCount(t, cfg, at, winnerReports)
				f := newAmbiguityDeliveryFixture(t, r, cfg)
				db := loadIngestCTY(t)
				f.pipeline.ctyLookup = func() *cty.CTYDatabase { return db }
				s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
				s.SourceType = spot.SourceManual
				switch path {
				case "immediate":
					f.pipeline.processSpotBody(s, nil)
				case "temporal_apply":
					// Obtain a real decoder Apply; the fixture changes temporal lag
					// only to exercise the release branch, preserving shipped stabilizer budgets.
					temporalCfg := cfg
					temporalCfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
					f.pipeline.correctionCfg = temporalCfg
					controller := newRuntimeTemporalController(temporalCfg)
					f.pipeline.temporal = controller
					selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, temporalCfg, "K1ABC", at)
					temporalDeliveryObserve(t, controller, at.Add(-3*time.Second), key, s, selection)
					release := temporalDeliveryDrainApply(t, controller, time.Now().UTC(), "K1ABD")
					f.pipeline.processSpotBody(release.pending.spot, &release)
					wantCommits := uint64(0)
					if found {
						wantCommits = 1
					}
					if f.pipeline.tracker.TemporalCommitted() != wantCommits ||
						controller.decoder.ConfirmedHistoryMetrics().Keys != int(wantCommits) {
						t.Fatal("license rejection changed accepted temporal accounting")
					}
				case "stabilizer_retry":
					f.pipeline.stabilizerEnabled = true
					f.pipeline.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 1)
					f.pipeline.telnetStabilizer.Start()
					t.Cleanup(f.pipeline.telnetStabilizer.Stop)
					envelope := &telnetStabilizerEnvelope{spot: s, peerOriginal: captureOriginalPeerPublication(s),
						resolverKey: key, hasResolverKey: true, evidenceEnqueued: true}
					f.pipeline.handleStabilizerRelease(envelope)
					for checks := 1; f.ring.GetCount() == 0 && checks < cfg.StabilizerMaxChecks; checks++ {
						if s.DXCallNorm != "K1ABC" || f.pipeline.tracker.CallCorrections() != 0 {
							t.Fatal("rejected replacement changed identity while held")
						}
						retry := temporalDeliveryReceiveRetry(t, f.pipeline.telnetStabilizer, key, checks)
						f.pipeline.handleStabilizerRelease(retry)
					}
					if !found && f.pipeline.tracker.StabilizerHeld() == 0 {
						t.Fatal("missing replacement fixture did not exercise an actual retry")
					}
					if f.pipeline.telnetStabilizer.Pending() != 0 {
						t.Fatal("retry retained delivery ownership")
					}
				}
				if !found && (s.DXCallNorm != "K1ABC" || f.ring.GetCount() != 1) {
					t.Fatalf("licensed original lost before delivery: call=%s ring=%d", s.DXCallNorm, f.ring.GetCount())
				}
				want := correctionEvidenceDeliveryCase{subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "P", wantPercent: 100 / (winnerReports + 1)}
				if found {
					want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = "K1ABD", "C", winnerReports*100/(winnerReports+1), 1
				}
				f.check(t, want, at)
			})
		}
	}
}

func TestReplacementLicenseLaterFinalRejectionKeepsAcceptedMutation(t *testing.T) {
	cfg := replacementLicenseConfig(t)
	replacementLicenseFixture(t, true)
	db := loadIngestCTY(t)
	at := time.Now().UTC().Add(-time.Second)
	r, key := replacementLicensePopulation(t, cfg, at)
	controller := newRuntimeTemporalController(runtimeTemporalTestConfig(0))
	tracker := stats.NewTracker()
	result := &runtimeTemporalResult{decoder: controller.decoder, key: key, tracker: tracker}
	s := spot.NewSpot("K1ABC", "N0AAA", 14020, "CW")
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, s.DXCallNorm, time.Now().UTC())
	if maybeApplyResolverCorrectionWithSelectionOverride(s, r, spot.ResolverEvidence{Key: key}, true, cfg,
		db, nil, tracker, nil, nil, nil, nil, nil, nil, nil, &selection, time.Now().UTC(), result) {
		t.Fatal("eligible replacement was suppressed")
	}
	if s.DXCallNorm != "K1ABD" || tracker.CallCorrections() != 1 || tracker.TemporalCommitted() != 1 {
		t.Fatal("positive control did not accept and confirm replacement")
	}
	// Replace actual authority after acceptance. Generation invalidation must
	// prevent the previous cached Found result from overriding the final gate.
	replacementLicenseFixture(t, false)
	if !applyLicenseGate(s, db, nil, nil) {
		t.Fatal("later confirmed absence did not remain authoritative")
	}
	if s.DXCallNorm != "K1ABD" || s.Confidence != "C" || s.ConfidencePercent != 75 ||
		tracker.CallCorrections() != 1 || tracker.TemporalCommitted() != 1 ||
		tracker.TemporalHandoffAttempts() != 0 || controller.decoder.ConfirmedHistoryMetrics().Keys != 1 {
		t.Fatal("final output refusal erased accepted mutation or invented a handoff")
	}
}
