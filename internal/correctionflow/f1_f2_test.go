package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func f1PendingPopulation(t testing.TB) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey, config.CallCorrectionConfig, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 1024, HysteresisWindows: 100, EvalMinInterval: time.Nanosecond,
		SweepInterval: time.Nanosecond, FreqGuardMinSeparationKHz: .1,
		FreqGuardRunnerUpRatio: .6, MaxEditDistance: 3, MinSpotterReliability: .95,
	})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010.05, "40m", "CW", 500)
	cfg := accountingConfig()
	cfg.FreqGuardMinSeparationKHz, cfg.MaxEditDistance = .1, 3
	cfg.DistanceModelCW = "plain"
	f1Add(t, r, d, key, "K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}, 7010.05, at)
	f1Add(t, r, d, key, "K1ABD", []string{"N0BBA", "N0BBB", "N0BBC", "N0BBD", "N0BBE"}, 7010.05, at.Add(time.Second))
	f1Add(t, r, d, key, "K1ABD", []string{"N0BBA", "N0BBB", "N0BBC"}, 7010.55, at.Add(2*time.Second))
	s, ok := r.Lookup(key)
	if !ok || s.State != spot.ResolverStateUncertain || s.Winner != "K1ABC" {
		t.Fatalf("fixture did not establish unfinished A3/B5 switch: %+v", s)
	}
	return r, d, key, cfg, at.Add(2*time.Second + time.Millisecond)
}

func TestF1PendingTransitionConstrainsFreshSupportUntilOwnerSettles(t *testing.T) {
	r, d, key, cfg, at := f1PendingPopulation(t)
	f1Add(t, r, d, key, "K1ABD", []string{"N0CCA", "N0CCB", "N0CCC", "N0CCD"}, 7010.55, at.Add(time.Second))
	before := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
	if before.Snapshot.State != spot.ResolverStateUncertain || before.allowsReplacement("K1ABD") || before.Snapshot.TotalReporters != 12 || ResolverSupportForCall(before.Snapshot, "K1ABD") != 9 {
		t.Fatalf("decisive new support bypassed unfinished owner switch: %+v", before)
	}
	for i := 0; i < 100; i++ {
		d.Sweep(at.Add(2*time.Second + time.Duration(i)*time.Millisecond))
	}
	after := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
	if after.Snapshot.State != spot.ResolverStateConfident || after.Snapshot.Winner != "K1ABD" || !after.allowsReplacement("K1ABD") || after.Snapshot.TotalReporters != 12 {
		t.Fatalf("settled owner did not restore legitimate recovery: %+v", after)
	}
}

func f1FreshReceivers(prefix string, n int) []string {
	rows := make([]string, n)
	for i := range rows {
		rows[i] = prefix + string(rune('A'+i))
	}
	return rows
}

func TestF1BroadTransitionRelevanceAndExpiry(t *testing.T) {
	for _, expiry := range []string{"neither", "incumbent", "challenger", "both"} {
		t.Run(expiry, func(t *testing.T) {
			r, d, key, cfg, at := f1PendingPopulation(t)
			start := at.Add(-2*time.Second - time.Millisecond)
			if expiry == "challenger" {
				f1Add(t, r, d, key, "K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}, 7010.05, start.Add(3*time.Second))
			}
			f1Add(t, r, d, key, "K1ABF", f1FreshReceivers("N0CC", 20), 7009.55, start.Add(10*time.Second))
			clock := start.Add(11 * time.Second)
			switch expiry {
			case "incumbent":
				clock = start.Add(time.Minute + time.Nanosecond)
			case "challenger":
				clock = start.Add(time.Minute + time.Second + time.Nanosecond)
			case "both":
				clock = start.Add(time.Minute + 3*time.Second + time.Nanosecond)
			}
			published, _ := r.Lookup(key)
			incumbent, challenger, pending := published.PendingTransition()
			if !pending || incumbent != "K1ABC" || challenger != "K1ABD" {
				t.Fatalf("fixture lost pending owner pair: %+v", published)
			}
			selected := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABE", clock)
			blocked := expiry == "neither"
			if selected.allowsReplacement("K1ABF") == blocked || selected.Snapshot.Winner != "K1ABF" || (selected.Snapshot.State == spot.ResolverStateUncertain) != blocked {
				t.Fatalf("expiry=%s: A/B competitor relevance gave wrong D/C authority: %+v", expiry, selected)
			}
			projected, ok := r.LookupEligible(key, clock)
			if !ok || projected.State != spot.ResolverStateUncertain {
				t.Fatalf("projection invented settled bucket authority: %+v", projected)
			}
			after, _ := r.Lookup(key)
			if a, b, ok := after.PendingTransition(); !ok || a != incumbent || b != challenger || after.State != published.State {
				t.Fatal("fresh selection advanced owner hysteresis")
			}
		})
	}
}

func TestF1ExcludedUnrelatedPendingNeighborDoesNotFreeze(t *testing.T) {
	r, d, _, cfg, at := f1PendingPopulation(t)
	otherKey := spot.NewResolverSignalKey(7011.05, "40m", "CW", 500)
	f1Add(t, r, d, otherKey, "DL9ZZZ", f1FreshReceivers("N0UU", 3), 7011.05, at.Add(time.Second))
	f1Add(t, r, d, otherKey, "DL9ZZY", f1FreshReceivers("N0VV", 5), 7011.05, at.Add(2*time.Second))
	cfg.ResolverNeighborhoodBucketRadius = 1
	// Place the comparable A/B population outside this request's radius. Its only
	// neighboring source carries an unrelated pending U/V pair.
	f1Add(t, r, d, otherKey, "K1ABD", f1FreshReceivers("N0WW", 12), 7011.55, at.Add(3*time.Second))
	upper := spot.NewResolverSignalKey(7011.55, "40m", "CW", 500)
	selected := SelectResolverPrimarySnapshotForCall(r, upper, cfg, "K1ABE")
	if selected.Snapshot.Winner != "K1ABD" || selected.Snapshot.State != spot.ResolverStateConfident || !selected.allowsReplacement("K1ABD") || selected.Snapshot.TotalReporters != 12 {
		t.Fatalf("excluded unrelated pending population froze settled B: %+v", selected)
	}
}

func TestF2SynthesizedTieAndActualTemporalProposal(t *testing.T) {
	at := time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC)
	for _, proposalCase := range []bool{false, true} {
		r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 128, HysteresisWindows: 1,
			EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
			FreqGuardMinSeparationKHz: .1, FreqGuardRunnerUpRatio: .9, MaxEditDistance: 3,
			SpotterReliability: spot.SpotterReliability{"N0CCA": .2, "N0CCB": .2, "N0CCC": .2}})
		d, err := spot.NewSignalResolverDriver(r)
		if err != nil {
			t.Fatal(err)
		}
		key := spot.NewResolverSignalKey(7010.24, "40m", "CW", 500)
		f1Add(t, r, d, key, "K1ABD", []string{"N0BBA", "N0BBB", "N0BBC"}, 7010.24, at)
		if proposalCase {
			f1Add(t, r, d, key, "K1ABC", []string{"N0AAA", "N0AAB"}, 7010.26, at)
		}
		f1Add(t, r, d, key, "K1ABF", []string{"N0CCA", "N0CCB", "N0CCC"}, 7010.26, at)
		cfg := accountingConfig()
		cfg.DistanceModelCW, cfg.FreqGuardMinSeparationKHz, cfg.FreqGuardRunnerUpRatio, cfg.MaxEditDistance = "plain", .1, .9, 3
		selected := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABE", at.Add(time.Second))
		if proposalCase {
			if selected.Snapshot.State != spot.ResolverStateConfident || selected.Snapshot.Winner != "K1ABD" || selected.allowsReplacement("K1ABF") {
				t.Fatalf("ordinary B should pass; raw-tied temporal F must not: %+v", selected)
			}
			decision := TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABF", Selection: selected}
			released := SelectTemporalReleaseSnapshot(r, key, cfg, "K1ABE", decision, at.Add(time.Second))
			if released.Snapshot.Winner != "K1ABD" || selectionForTemporalWinner(selected, "K1ABF").Snapshot.Winner != "K1ABD" {
				t.Fatalf("actual temporal proposal bypassed its qualifying raw tie: %+v", released)
			}
		} else if selected.Snapshot.State != spot.ResolverStateSplit || selected.Snapshot.TotalReporters != 6 || selected.Snapshot.TotalWeightedSupportMilli != 3600 {
			t.Fatalf("synthesized raw 3:3 bypassed protection with weights 3000:600: %+v", selected)
		}
	}
}

func TestF2ExactActualProposalChecksEveryRawTiedCompetitor(t *testing.T) {
	at := time.Date(2026, 10, 14, 14, 0, 0, 0, time.UTC)
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 128, HysteresisWindows: 1,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		FreqGuardMinSeparationKHz: .1, FreqGuardRunnerUpRatio: .6, MaxEditDistance: 3})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	f1Add(t, r, d, key, "K1ABD", []string{"N0AAA", "N0AAB", "N0AAC", "N0AAD", "N0AAE"}, 7010, at)
	f1Add(t, r, d, key, "K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}, 7010, at.Add(10*time.Millisecond))
	f1Add(t, r, d, key, "K1ABF", []string{"N0AAD", "N0AAE", "N0AAF"}, 7010, at.Add(20*time.Millisecond))
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodEnabled, cfg.DistanceModelCW, cfg.FreqGuardMinSeparationKHz, cfg.MaxEditDistance = false, "plain", .1, 3
	current := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
	if current.Snapshot.Winner != "K1ABD" || current.Snapshot.State != spot.ResolverStateConfident || current.Snapshot.TotalReporters != 6 {
		t.Fatalf("B5 with overlapping membership must retain ordinary authority: %+v", current)
	}
	decision := TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABC", Selection: current}
	released := SelectTemporalReleaseSnapshot(r, key, cfg, "K1ABE", decision, at.Add(time.Second))
	if released.Snapshot.Winner != "K1ABD" {
		t.Fatalf("actual C3 proposal bypassed disjoint D3 tie below ordinary B5: %+v", released)
	}
}

func TestF1UncertainOnlyEvidenceCannotNominateReplacement(t *testing.T) {
	r, d, key, cfg, at := f1PendingPopulation(t)
	f1Add(t, r, d, key, "K1ABF", f1FreshReceivers("N0DD", 20), 7010.05, at.Add(time.Second))
	selected := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
	if selected.allowsReplacement("K1ABF") || ResolverSupportForCall(selected.Snapshot, "K1ABF") != 20 || selected.Snapshot.TotalReporters != 28 {
		t.Fatalf("uncertain-only dissent was lost or gained replacement authority: %+v", selected)
	}
}

func TestF1ZeroWeightMembershipStillConstrainsPendingTransition(t *testing.T) {
	at := time.Date(2026, 10, 14, 16, 0, 0, 0, time.UTC)
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 128, HysteresisWindows: 100,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		FreqGuardMinSeparationKHz: .1, FreqGuardRunnerUpRatio: .6, MaxEditDistance: 3,
		SpotterReliability: spot.SpotterReliability{"N0AAA": 0, "N0AAB": 0, "N0AAC": 0}})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	f1Add(t, r, d, key, "K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}, 7010, at)
	f1Add(t, r, d, key, "K1ABD", f1FreshReceivers("N0BB", 5), 7010, at.Add(time.Second))
	f1Add(t, r, d, key, "K1ABF", f1FreshReceivers("N0CC", 20), 7010.5, at.Add(2*time.Second))
	cfg := accountingConfig()
	cfg.DistanceModelCW, cfg.FreqGuardMinSeparationKHz, cfg.MaxEditDistance = "plain", .1, 3
	selected := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
	if selected.Snapshot.State != spot.ResolverStateUncertain || selected.allowsReplacement("K1ABF") || ResolverSupportForCall(selected.Snapshot, "K1ABC") != 3 || selected.Snapshot.TotalReporters != 28 || selected.Snapshot.TotalWeightedSupportMilli != 25000 {
		t.Fatalf("eligible zero-weight membership lost its pending constraint: %+v", selected)
	}
	if percent, ok := ResolverCallConfidencePercent(selected.Snapshot, "K1ABC"); !ok || percent != 0 {
		t.Fatalf("zero-weight dissent share=%d/%t want 0,true", percent, ok)
	}
}

func TestF1NoSettledNomineeKeepsDeterministicUnion(t *testing.T) {
	at := time.Date(2026, 10, 14, 17, 0, 0, 0, time.UTC)
	weights := spot.SpotterReliability{"N0AAA": 0, "N0AAB": 0, "N0AAC": 0, "N0BBA": 0, "N0BBB": 0, "N0BBC": 0}
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 32,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, SpotterReliability: weights})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	f1Add(t, r, d, key, "K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}, 7010, at)
	f1Add(t, r, d, key, "K1ABD", []string{"N0BBA", "N0BBB", "N0BBC"}, 7010.5, at.Add(time.Second))
	cfg := accountingConfig()
	cfg.DistanceModelCW = "plain"
	for i := 0; i < 32; i++ {
		selected := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
		s := selected.Snapshot
		if !selected.SnapshotOK || s.State != spot.ResolverStateUncertain || s.TotalReporters != 6 || len(s.CandidateRanks) != 2 ||
			s.CandidateRanks[0].Call != "K1ABC" || s.CandidateRanks[1].Call != "K1ABD" || selected.allowsReplacement("K1ABC") || selected.allowsReplacement("K1ABD") {
			t.Fatalf("no-nominee union lost counts, authority or deterministic order: %+v", selected)
		}
		if _, available := ResolverCallConfidencePercent(s, "K1ABC"); available {
			t.Fatal("all-zero weighted union invented an available percentage")
		}
	}
}

func f1Add(t testing.TB, r *spot.SignalResolver, d *spot.SignalResolverDriver, key spot.ResolverSignalKey, call string, receivers []string, frequency float64, at time.Time) {
	t.Helper()
	for _, rx := range receivers {
		if !r.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: rx,
			FrequencyKHz: frequency, ObservedAt: at, RecencyWindow: time.Minute}) {
			t.Fatal("fixture evidence refused")
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
}

func TestF1NeighborhoodPreservesPendingExactDissent(t *testing.T) {
	r, _, key, cfg, at := f1PendingPopulation(t)
	for _, fresh := range []bool{false, true} {
		var selected ResolverPrimarySelection
		if fresh {
			selected = SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABE", at)
		} else {
			selected = SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABE")
		}
		s := selected.Snapshot
		if !selected.SnapshotOK || s.TotalReporters != 8 || ResolverSupportForCall(s, "K1ABC") != 3 || ResolverSupportForCall(s, "K1ABD") != 5 {
			t.Fatalf("fresh=%t: repeated nearby B3 erased exact A3/B5 dissent: %+v", fresh, selected)
		}
		if percent, ok := ResolverCallConfidencePercent(s, "K1ABD"); !ok || percent != 62 {
			t.Fatalf("fresh=%t: B share=%d/%t, want floor(5/8)=62", fresh, percent, ok)
		}
		if s.State != spot.ResolverStateUncertain && s.State != spot.ResolverStateSplit {
			t.Fatalf("pending switch authorized correction: %+v", selected)
		}
	}
}
