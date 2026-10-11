//go:build !qualification

package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/uls"
)

type confidenceAttributionCase struct {
	name, rejection, winner, original string
	paths                             []string
	weights                           spot.SpotterReliability
	winnerWeight, totalWeight         int
	wantPercent                       int
	missingOriginal                   bool
}

func TestCallCorrectionDeliveredRejectedConfidenceAttribution(t *testing.T) {
	temporalDeliveryDirectory(t)
	paths := []string{"immediate", "temporal_apply", "stabilizer_retry"}
	for _, tc := range []confidenceAttributionCase{
		{name: "cty_equal", rejection: "cty", winner: "K1ABC", original: "K1ABD", paths: paths,
			winnerWeight: 2000, totalWeight: 3000, wantPercent: 33},
		{name: "invalid_equal", rejection: "invalid", winner: "K1ABC?", original: "K1ABD", paths: paths,
			winnerWeight: 2000, totalWeight: 3000, wantPercent: 33},
		{name: "accepted_counterfactual", winner: "K1ABC", original: "K1ABD", paths: paths,
			winnerWeight: 2000, totalWeight: 3000, wantPercent: 66},
		{name: "cty_weighted_retained_1000", rejection: "cty", winner: "K1ABC", original: "K1ABD", paths: []string{"immediate"},
			weights:      spot.SpotterReliability{"N0AAA": 0.900, "N0BBB": 0.950, "N0DDD": 1.000},
			winnerWeight: 1850, totalWeight: 2850, wantPercent: 35},
		{name: "invalid_weighted_retained_900", rejection: "invalid", winner: "K1ABC?", original: "K1ABD", paths: []string{"immediate"},
			weights:      spot.SpotterReliability{"N0AAA": 0.950, "N0BBB": 1.000, "N0DDD": 0.900},
			winnerWeight: 1950, totalWeight: 2850, wantPercent: 31},
		{name: "cty_original_has_no_support", rejection: "cty", winner: "K1ABC", original: "K1ABE", paths: []string{"immediate"},
			winnerWeight: 2000, totalWeight: 3000, missingOriginal: true},
	} {
		for _, path := range tc.paths {
			t.Run(tc.name+"/"+path, func(t *testing.T) {
				confidenceAttributionDeliveryCase(t, tc, path)
			})
		}
	}
}

func TestCallCorrectionTemporalDeliveredRetryRejectedConfidenceAttribution(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := confidenceAttributionConfig()
	cfg.StabilizerEnabled = true
	cfg.StabilizerMaxChecks, cfg.StabilizerAmbiguousMaxChecks = 2, 2
	cfg.RecentBandRecordMinUniqueSpotters = 2
	at := time.Now().UTC().Truncate(time.Second)
	old := at.Add(-10 * time.Second)
	tc := confidenceAttributionCase{name: "cty_full_temporal_retry", rejection: "cty", winner: "K1ABC", original: "K1ABD",
		winnerWeight: 2000, totalWeight: 3000, wantPercent: 33}
	r, driver, key := confidenceAttributionPopulation(t, old, tc.winner, nil)
	confidenceAttributionRequirePopulation(t, r, key, tc)
	saved := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, tc.original)
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	p := f.pipeline
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0"?><plist version="1.0"><dict></dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	p.stabilizerEnabled = true
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 2)
	p.telnetStabilizer.Start()
	t.Cleanup(p.telnetStabilizer.Stop)
	controller := newRuntimeTemporalController(cfg)
	p.temporal = controller
	for _, receiver := range []string{"N0AAA", "N0BBB"} {
		p.recentBandStore.(*spot.RecentBandStore).Record(tc.original, "20m", "CW", receiver, old.Add(-time.Minute))
	}
	s := newDeliveryTestSpot(tc.original, "N0AAA", at)
	s.SourceType = spot.SourceManual
	temporalDeliveryObserve(t, controller, old, key, s, saved)
	// This newer, currently eligible split holds both real scheduler checks.
	// Its past receipt clock also makes deterministic one-second pruning remove
	// the extra receivers before final B2/A1, without waiting or moving release time.
	splitAt := at.Add(-3 * time.Second)
	temporalDeliverySeed(t, r, driver, key, splitAt, 2*time.Minute, tc.winner, tc.original, 3, 2)
	driver.Sweep(splitAt.Add(time.Millisecond))
	initial, ok := r.Lookup(key)
	if !ok || initial.State != spot.ResolverStateSplit || initial.TotalReporters != 5 {
		t.Fatalf("full retry fixture did not publish qualifying B3/A2 split: %+v", initial)
	}
	release := temporalDeliveryDrainApply(t, controller, time.Now().UTC(), tc.winner)
	p.processSpot(release.pending.spot, &release)
	temporalDeliveryAssertAccounting(t, p, 1, 0, 0)
	if f.ring.GetCount() != 0 || p.tracker.StabilizerHeld() != 1 || p.tracker.TemporalCommitted() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatal("actual temporal release failed to retain unaccepted delivery ownership")
	}
	first := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
	if first.temporalResult == nil || first.temporalResult.decoder != controller.decoder ||
		first.temporalResult.key != key || first.temporalResult.tracker != p.tracker || first.temporalResult.handoff.Load() ||
		first.delayReason != stabilizerDelayReasonAmbiguous.String() {
		t.Fatal("initial actual scheduler release lost unaccepted temporal request or ambiguity context")
	}
	p.handleStabilizerRelease(first)
	temporalDeliveryAssertAccounting(t, p, 1, 0, 0)
	if f.ring.GetCount() != 0 || p.tracker.StabilizerHeld() != 2 || p.tracker.TemporalCommitted() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatal("unchanged split retry emitted or counted a correction before final release")
	}
	second := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 1)
	if second.temporalResult != first.temporalResult || second.temporalResult.handoff.Load() ||
		second.delayReason != stabilizerDelayReasonAmbiguous.String() {
		t.Fatal("actual retry replaced temporal request ownership or prematurely counted handoff")
	}
	fresh := time.Now().UTC()
	temporalDeliveryRetirePopulation(t, r, driver, key, fresh, tc.winner)
	// Keep new observations at an already observed clock. Advancing their
	// timestamps would make them future evidence on a coarse Windows clock.
	temporalDeliverySeed(t, r, driver, key, fresh, 2*time.Minute, tc.winner, tc.original, 2, 1)
	driver.Sweep(fresh.Add(time.Millisecond))
	confidenceAttributionRequirePopulation(t, r, key, tc)
	p.handleStabilizerRelease(second)
	temporalDeliveryAssertAccounting(t, p, 1, 1, 0)
	if p.tracker.TemporalCommitted() != 0 || p.tracker.TemporalPathSwitches() != 0 || p.tracker.CallCorrections() != 0 ||
		p.tracker.CorrectionDecisionReasons()[resolverDecisionCTYMiss] != 1 || !second.temporalResult.handoff.Load() {
		t.Errorf("final CTY rejection changed acceptance or handoff accounting: commits=%d switches=%d corrections=%d reasons=%v",
			p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches(), p.tracker.CallCorrections(), p.tracker.CorrectionDecisionReasons())
	}
	if p.tracker.StabilizerReleasedDelayed() != 1 || p.telnetStabilizer.Pending() != 0 ||
		len(controller.Drain(time.Now().UTC(), true)) != 0 || len(controller.pending) != 0 || controller.decoder.PendingCount() != 0 {
		t.Fatal("rejected held request retained ownership after its one actual final delivery")
	}
	f.check(t, correctionEvidenceDeliveryCase{subject: tc.original, wantCall: tc.original, wantGlyph: "B", wantPercent: 33}, at)
}

func TestCallCorrectionDeliveredCTYHitConfidenceAttribution(t *testing.T) {
	temporalDeliveryDirectory(t)
	// Reuse the actual local FCC fixture so final license admission cannot
	// depend on a host database. Its K1ABC record is a positive license hit.
	configureFCCStateFixture(t, true)
	cfg := confidenceAttributionConfig()
	at := time.Now().UTC().Truncate(time.Second)
	tc := confidenceAttributionCase{name: "cty_hit_counterfactual", winner: "K1ABC", original: "K1ABD",
		winnerWeight: 2000, totalWeight: 3000, wantPercent: 66}
	if license := uls.LookupForADIF(291, tc.winner); !license.Available || !license.Found || license.State != "CA" {
		t.Fatalf("success fixture did not establish its independent local license hit: %+v", license)
	}
	r, _, key := confidenceAttributionPopulation(t, at.Add(-3*time.Second), tc.winner, nil)
	confidenceAttributionRequirePopulation(t, r, key, tc)
	// The K1 row uses the existing ingest fixture's authoritative CTY fields.
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<plist><dict><key>K1</key><dict><key>Country</key><string>United States</string><key>Prefix</key><string>K1</string><key>ADIF</key><integer>291</integer><key>Continent</key><string>NA</string><key>CQZone</key><integer>5</integer></dict></dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	if info, ok := db.LookupCallsign(tc.winner); !ok || info.Prefix != "K1" || info.ADIF != 291 {
		t.Fatalf("success fixture did not establish an authoritative K1 CTY hit: %+v", info)
	}
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	f.pipeline.ctyLookup = func() *cty.CTYDatabase { return db }
	s := newDeliveryTestSpot(tc.original, "N0AAA", at)
	s.SourceType = spot.SourceManual
	f.pipeline.processSpot(s, nil)
	if f.pipeline.tracker.CorrectionDecisionReasons()[resolverDecisionCTYMiss] != 0 ||
		f.pipeline.tracker.CorrectionDecisionReasons()[resolverDecisionInvalidBaseCall] != 0 {
		t.Error("positive CTY hit reached a candidate rejection branch")
	}
	f.check(t, correctionEvidenceDeliveryCase{subject: tc.original, wantCall: tc.winner, wantGlyph: "C", wantPercent: 66, wantCorrections: 1}, at)
}

func confidenceAttributionDeliveryCase(t *testing.T, tc confidenceAttributionCase, path string) {
	t.Helper()
	cfg := confidenceAttributionConfig()
	at := time.Now().UTC().Truncate(time.Second)
	observed := at.Add(-3 * time.Second)
	r, driver, key := confidenceAttributionPopulation(t, observed, tc.winner, tc.weights)
	if path == "stabilizer_retry" {
		// The first actual check must be ambiguous and held. Prune all of this
		// population before publishing final B2/A1, rather than keeping A2/B3.
		temporalDeliverySeed(t, r, driver, key, observed.Add(time.Millisecond), 2*time.Minute, tc.winner, "K1ABD", 3, 2)
		driver.Sweep(observed.Add(2 * time.Millisecond))
		initial, ok := r.Lookup(key)
		if !ok || initial.State != spot.ResolverStateSplit || initial.TotalReporters != 5 {
			t.Fatalf("actual retry fixture did not publish qualifying B3/A2 split: %+v", initial)
		}
		cfg.StabilizerEnabled = true
		cfg.StabilizerMaxChecks, cfg.StabilizerAmbiguousMaxChecks = 2, 2
		cfg.RecentBandRecordMinUniqueSpotters = 2
	}
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	p := f.pipeline
	if tc.rejection == "cty" {
		db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0"?><plist version="1.0"><dict></dict></plist>`))
		if err != nil {
			t.Fatal(err)
		}
		p.ctyLookup = func() *cty.CTYDatabase { return db }
	}
	if tc.rejection == "invalid" && (!shouldRejectCTYCall(tc.winner) ||
		spot.CallDistance(tc.original, tc.winner, "CW", "morse", "baudot") > cfg.MaxEditDistance) {
		t.Fatal("invalid-base fixture fails syntax rejection or prior distance eligibility")
	}
	s := newDeliveryTestSpot(tc.original, "N0AAA", at)
	s.SourceType = spot.SourceManual
	switch path {
	case "immediate":
		confidenceAttributionRequirePopulation(t, r, key, tc)
		p.processSpot(s, nil)
	case "temporal_apply":
		confidenceAttributionRequirePopulation(t, r, key, tc)
		controller := newRuntimeTemporalController(cfg)
		p.temporal = controller
		selection := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, tc.original)
		temporalDeliveryObserve(t, controller, observed, key, s, selection)
		release := temporalDeliveryDrainApply(t, controller, time.Now().UTC(), tc.winner)
		p.processSpot(release.pending.spot, &release)
	case "stabilizer_retry":
		p.stabilizerEnabled = true
		store := p.recentBandStore.(*spot.RecentBandStore)
		for _, receiver := range []string{"N0AAA", "N0BBB"} {
			store.Record(tc.original, "20m", "CW", receiver, observed.Add(-time.Minute))
		}
		p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 1)
		p.telnetStabilizer.Start()
		t.Cleanup(p.telnetStabilizer.Stop)
		p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s, peerOriginal: captureOriginalPeerPublication(s), resolverKey: key,
			hasResolverKey: true, evidenceEnqueued: true, delayReason: stabilizerDelayReasonAmbiguous.String()})
		if f.ring.GetCount() != 0 || p.tracker.StabilizerHeld() != 1 {
			t.Fatal("retry fixture did not retain ownership before first actual scheduler publication")
		}
		retry := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 1)
		fresh := time.Now().UTC()
		temporalDeliveryRetirePopulation(t, r, driver, key, fresh, tc.winner)
		temporalDeliverySeed(t, r, driver, key, fresh, 2*time.Minute, tc.winner, "K1ABD", 2, 1)
		driver.Sweep(fresh.Add(time.Millisecond))
		confidenceAttributionRequirePopulation(t, r, key, tc)
		p.handleStabilizerRelease(retry)
		if p.tracker.StabilizerReleasedDelayed() != 1 || p.telnetStabilizer.Pending() != 0 {
			t.Error("final actual retry did not retire one held delivery")
		}
	default:
		t.Fatal("unknown attribution path")
	}
	want := correctionEvidenceDeliveryCase{subject: tc.original, wantCall: tc.original, wantGlyph: "B", wantPercent: tc.wantPercent}
	if tc.rejection == "" {
		want.wantCall, want.wantGlyph, want.wantCorrections = tc.winner, "C", 1
	} else {
		reason := resolverDecisionCTYMiss
		if tc.rejection == "invalid" {
			reason = resolverDecisionInvalidBaseCall
		}
		if p.tracker.CorrectionDecisionReasons()[reason] != 1 {
			t.Errorf("fixture did not reach requested rejection %s: %v", reason, p.tracker.CorrectionDecisionReasons())
		}
	}
	if path == "temporal_apply" && p.tracker.TemporalCommitted() != want.wantCorrections {
		t.Error("confidence attribution changed actual temporal mutation accounting")
	}
	if tc.missingOriginal {
		temporalDeliveryCheckWithoutPercent(t, f, want, at, true)
	} else {
		f.check(t, want, at)
	}
}

func confidenceAttributionConfig() config.CallCorrectionConfig {
	cfg := temporalDeliveryConfig()
	cfg.MinConsensusReports, cfg.MinAdvantage, cfg.MinConfidencePercent = 2, 1, 45
	return cfg
}

func confidenceAttributionPopulation(t *testing.T, at time.Time, winner string, weights spot.SpotterReliability) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance: 6, DistanceModelCW: "morse", DistanceModelRTTY: "baudot", SpotterReliability: weights,
	})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	temporalDeliverySeed(t, r, driver, key, at, 2*time.Minute, winner, "K1ABD", 2, 1)
	driver.Sweep(at.Add(time.Millisecond))
	return r, driver, key
}

func confidenceAttributionRequirePopulation(t *testing.T, r *spot.SignalResolver, key spot.ResolverSignalKey, tc confidenceAttributionCase) {
	t.Helper()
	snap, ok := r.Lookup(key)
	wantOriginalSupport := 1
	if tc.missingOriginal {
		wantOriginalSupport = 0
	}
	if !ok || snap.State != spot.ResolverStateConfident || snap.Winner != tc.winner ||
		snap.WinnerSupport != 2 || snap.TotalReporters != 3 || snap.WinnerWeightedSupportMilli != tc.winnerWeight ||
		snap.TotalWeightedSupportMilli != tc.totalWeight || correctionflow.ResolverSupportForCall(snap, tc.original) != wantOriginalSupport {
		t.Fatalf("literal B2/A1 eligible weighted population failed: %+v want winnerWeight=%d totalWeight=%d originalSupport=%d",
			snap, tc.winnerWeight, tc.totalWeight, wantOriginalSupport)
	}
	if snap.ReceiverEvidence() == nil || snap.ReceiverEvidence().Len() != 3 {
		t.Fatal("attribution fixture did not publish three distinct receiver observations")
	}
	clock := time.Now().UTC()
	eligible, eligibleOK := r.LookupEligible(key, clock)
	if !eligibleOK || eligible.State != spot.ResolverStateConfident || eligible.Winner != tc.winner ||
		eligible.TotalReporters != 3 || eligible.TotalWeightedSupportMilli != tc.totalWeight ||
		eligible.ReceiverEvidence() == nil || eligible.ReceiverEvidence().Len() != 3 {
		t.Fatalf("fixture lacks the literal eligible population at release clock %s: %+v", clock, eligible)
	}
	t.Logf("independent share oracle: %s; B support=2 weight=%d total=%d emitted desired=%d%%",
		tc.name, tc.winnerWeight, tc.totalWeight, tc.wantPercent)
}
