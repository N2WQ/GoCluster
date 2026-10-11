// File role: Exercises cap-loss generation authority through the shared selectors
// using admitted evidence and a process clock independent of observation time.
package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

type b31GenerationFixture struct {
	resolver *spot.SignalResolver
	driver   *spot.SignalResolverDriver
	key      spot.ResolverSignalKey
	at       time.Time
	cfg      config.CallCorrectionConfig
}

func newB31GenerationFixture(t *testing.T, maxReporters int) b31GenerationFixture {
	t.Helper()
	return newB31GenerationFixtureWithReliability(t, maxReporters, nil)
}

func newB31GenerationFixtureWithReliability(t *testing.T, maxReporters int, reliability map[string]float64) b31GenerationFixture {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 64, MaxActiveKeys: 4, MaxCandidatesPerKey: 2, MaxReportersPerCand: maxReporters,
		InactiveTTL: 12 * time.Hour, EvalMinInterval: time.Nanosecond, SweepInterval: time.Hour,
		HysteresisWindows: 1, FreqGuardRunnerUpRatio: .9, MaxEditDistance: 3,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		SpotterReliability: reliability,
	})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	return b31GenerationFixture{
		resolver: r, driver: d,
		key: spot.NewResolverSignalKey(14020, "20m", "CW", 500),
		at:  time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC),
		cfg: config.CallCorrectionConfig{Enabled: true, RecencySeconds: 60,
			MinConsensusReports: 1, MinConfidencePercent: 1, MaxEditDistance: 3,
			DistanceModelCW: "plain", DistanceModelRTTY: "plain", FrequencyToleranceHz: 500,
			FreqGuardRunnerUpRatio: .9},
	}
}

func (f b31GenerationFixture) processElapsed(t *testing.T, elapsed time.Duration) {
	t.Helper()
	if err := f.driver.SetProcessElapsed(elapsed); err != nil {
		t.Fatal(err)
	}
}

func (f b31GenerationFixture) add(t *testing.T, offset int, call, receiver string, at time.Time) {
	t.Helper()
	key := f.key
	key.Bucket += int64(offset)
	if !f.resolver.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: call,
		Spotter: receiver, FrequencyKHz: 14020 + .5*float64(offset), RecencyWindow: time.Minute}) {
		t.Fatal("generation fixture evidence was not enqueued")
	}
	if got := f.driver.Drain(); got != 1 {
		t.Fatalf("processed evidence=%d, want 1", got)
	}
}

func (f b31GenerationFixture) selection(at time.Time) ResolverPrimarySelection {
	return SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, f.cfg, "K1ABE", at)
}

func TestB31ResolverLossGenerationRetiresWithoutSweep(t *testing.T) {
	f := newB31GenerationFixture(t, 2)
	for _, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		f.add(t, 0, "K1ABC", receiver, f.at)
	}
	f.driver.Sweep(f.at.Add(time.Millisecond))
	old := f.selection(f.at.Add(time.Second))
	metrics := f.resolver.MetricsSnapshot()
	if !old.SnapshotOK || !old.Snapshot.EvidenceLoss || old.Snapshot.Generation == 0 ||
		metrics.CapPressureReporters != 1 || metrics.EvictedReporters != 1 {
		t.Fatalf("fixture did not publish actual receiver-cap loss: selection=%+v metrics=%+v", old, metrics)
	}
	if old.generationWitnesses[2] != old.Snapshot.Generation {
		t.Fatalf("exact selection lost owner generation: witnesses=%v generation=%d", old.generationWitnesses, old.Snapshot.Generation)
	}
	if percent, ok := ResolverCallConfidencePercent(old.Snapshot, "K1ABC"); ok || percent != 0 {
		t.Fatalf("cap-loss population claimed a percentage: %d available=%v", percent, ok)
	}
	if compact := old.Snapshot.Compact(); !compact.EvidenceLoss || compact.Generation != old.Snapshot.Generation {
		t.Fatalf("compact proposal lost generation protection: %+v", compact)
	}

	// Keep the observation/query clocks unchanged and perform no owner sweep:
	// only elapsed process idle time reaches the hard retirement boundary.
	f.processElapsed(t, 10*time.Minute-time.Nanosecond)
	if before := f.selection(f.at.Add(time.Second)); !before.SnapshotOK || !before.Snapshot.EvidenceLoss {
		t.Fatalf("loss generation retired before the exact deadline: %+v", before)
	}
	f.processElapsed(t, 10*time.Minute)
	if retired := f.selection(f.at.Add(time.Second)); retired.SnapshotOK {
		t.Fatalf("logically retired generation retained authority without a sweep: %+v", retired)
	}
	proposal := TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABC", Selection: old}
	if released := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(time.Second)); released.SnapshotOK {
		t.Fatalf("saved proposal resurrected a logically retired publication: %+v", released)
	}

	// Owner admission at the deadline starts a distinct generation. Its three
	// fresh receivers independently favor ABD 2:1, while ABC remains supported
	// so an unchecked stale temporal override would observably select ABC.
	for i, row := range []struct{ call, receiver string }{
		{"K1ABD", "N0DDD"}, {"K1ABD", "N0EEE"}, {"K1ABC", "N0FFF"},
	} {
		f.add(t, 0, row.call, row.receiver, f.at.Add(2*time.Second+time.Duration(i)*time.Nanosecond))
	}
	f.driver.Sweep(f.at.Add(2*time.Second + time.Millisecond))
	fresh := f.selection(f.at.Add(3 * time.Second))
	if !fresh.SnapshotOK || fresh.Snapshot.EvidenceLoss || fresh.Snapshot.Generation == 0 ||
		fresh.Snapshot.Generation == old.Snapshot.Generation || fresh.Snapshot.Winner != "K1ABD" ||
		fresh.Snapshot.TotalReporters != 3 || ResolverSupportForCall(fresh.Snapshot, "K1ABC") != 1 {
		t.Fatalf("fresh independently qualified generation was not established: %+v", fresh)
	}
	released := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(3*time.Second))
	if released.Snapshot.Winner != "K1ABD" || released.Snapshot.Generation != fresh.Snapshot.Generation || released.Snapshot.EvidenceLoss {
		t.Fatalf("stale generation overrode legitimate fresh ordinary selection: %+v", released)
	}
	proposal.Selection = fresh
	if allowed := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(3*time.Second)); allowed.Snapshot.Winner != "K1ABC" {
		t.Fatalf("matching-generation supported proposal was disabled: %+v", allowed)
	}
}

func TestB31ResolverIncomparableNeighborLossAndRetiredWitness(t *testing.T) {
	f := newB31GenerationFixture(t, 2)
	f.cfg.ResolverNeighborhoodEnabled = true
	f.cfg.ResolverNeighborhoodBucketRadius = 1
	f.cfg.ResolverNeighborhoodMaxDistance = 1
	for _, row := range []struct{ call, receiver string }{
		{"K1ABD", "N0AAA"}, {"K1ABD", "N0BBB"}, {"K1ABC", "N0CCC"},
	} {
		f.add(t, 0, row.call, row.receiver, f.at)
	}
	for _, receiver := range []string{"N0DDD", "N0EEE", "N0FFF"} {
		f.add(t, 1, "W9XYZ", receiver, f.at)
	}
	f.driver.Sweep(f.at.Add(time.Millisecond))
	protected := f.selection(f.at.Add(time.Second))
	if !protected.SnapshotOK || !protected.Snapshot.EvidenceLoss || protected.generationWitnesses[2] == 0 || protected.generationWitnesses[3] == 0 {
		t.Fatalf("incomparable neighbor was filtered before admitting its cap loss: %+v", protected)
	}
	if metrics := f.resolver.MetricsSnapshot(); metrics.CapPressureReporters != 1 || metrics.EvictedReporters != 1 {
		t.Fatalf("neighbor fixture did not cause actual receiver eviction: %+v", metrics)
	}
	f.processElapsed(t, 10*time.Minute)
	// The exact generation has no loss and remains valid. The latest selector
	// cannot visit the retired neighbor, but the old proposal must remember it.
	fresh := f.selection(f.at.Add(time.Second))
	if !fresh.SnapshotOK || fresh.Snapshot.EvidenceLoss || fresh.Snapshot.Winner != "K1ABD" || fresh.generationWitnesses[3] != 0 {
		t.Fatalf("fresh exact fallback did not exclude the logically retired neighbor: %+v", fresh)
	}
	proposal := TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABC", Selection: protected}
	released := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(time.Second))
	if released.Snapshot.Winner != "K1ABD" || released.Snapshot.EvidenceLoss {
		t.Fatalf("latest visit list erased an old neighbor-generation obligation: %+v", released)
	}
}

func TestB31ResolverExpiredLossDoesNotLatchSavedProposal(t *testing.T) {
	// B remains the owner's settled winner with A's full retained tally too;
	// expiry therefore restores current authority rather than settling a winner.
	f := newB31GenerationFixtureWithReliability(t, 2, map[string]float64{
		"N0AAA": .75, "N0BBB": .75, "N0CCC": .75,
	})
	for _, receiver := range []string{"N0AAA", "N0BBB"} {
		f.add(t, 0, "K1ABC", receiver, f.at)
	}
	f.add(t, 0, "K1ABC", "N0CCC", f.at.Add(30*time.Second))
	for _, receiver := range []string{"N0DDD", "N0EEE"} {
		f.add(t, 0, "K1ABD", receiver, f.at.Add(30*time.Second))
	}
	f.driver.Sweep(f.at.Add(30*time.Second + time.Millisecond))
	old := f.selection(f.at.Add(31 * time.Second))
	if !old.SnapshotOK || !old.Snapshot.EvidenceLoss || old.Snapshot.Generation == 0 {
		t.Fatalf("fixture did not retain an active loss proposal: %+v", old)
	}
	// The discarded observation at t=0 expires; rows at t=30 remain eligible.
	// No process-idle retirement or owner sweep is involved in this read.
	fresh := f.selection(f.at.Add(61 * time.Second))
	if !fresh.SnapshotOK || fresh.Snapshot.EvidenceLoss || fresh.Snapshot.Generation != old.Snapshot.Generation ||
		fresh.Snapshot.Winner != "K1ABD" || fresh.Snapshot.State != spot.ResolverStateConfident || fresh.Snapshot.TotalReporters != 3 ||
		fresh.Snapshot.TotalWeightedSupportMilli != 2750 || ResolverSupportForCall(fresh.Snapshot, "K1ABC") != 1 ||
		ResolverWeightedSupportForCall(fresh.Snapshot, "K1ABC") != 750 {
		t.Fatalf("current eligibility did not recover the same generation independently: %+v", fresh)
	}
	proposal := TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABC", Selection: old}
	released := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(61*time.Second))
	if !released.SnapshotOK || released.Snapshot.EvidenceLoss || released.Snapshot.Winner != "K1ABC" ||
		released.Snapshot.State != spot.ResolverStateProbable || ResolverSupportForCall(released.Snapshot, "K1ABC") != 1 || released.Snapshot.TotalReporters != 3 {
		t.Fatalf("saved loss flag latched after current same-generation recovery: %+v", released)
	}
}
