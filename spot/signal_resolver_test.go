package spot

import (
	"testing"
	"time"
)

func TestShouldRejectAsAmbiguousMultiSignal(t *testing.T) {
	if !shouldRejectAsAmbiguousMultiSignal(3, 2, 7010.0, 7010.1, 1.0, 0.6, 0, 1, 3, false) {
		t.Fatalf("expected split guard to trigger for disjoint near-equal supports")
	}
	if shouldRejectAsAmbiguousMultiSignal(3, 2, 7010.0, 7010.1, 1.0, 0.6, 2, 1, 3, false) {
		t.Fatalf("expected high reporter overlap to bypass split guard")
	}
}

func TestSignalResolverQueueSaturationIsNonBlocking(t *testing.T) {
	resolver := NewSignalResolver(SignalResolverConfig{
		QueueSize:           1,
		MaxActiveKeys:       4,
		MaxCandidatesPerKey: 4,
		MaxReportersPerCand: 4,
	})
	key := NewResolverSignalKey(7010.0, "40m", "CW", 500)
	ev := ResolverEvidence{
		ObservedAt:    time.Now().UTC(),
		Key:           key,
		DXCall:        "DL6LD",
		Spotter:       "K1AAA",
		FrequencyKHz:  7010.0,
		RecencyWindow: 10 * time.Second,
	}
	if ok := resolver.Enqueue(ev); !ok {
		t.Fatalf("expected first enqueue to succeed")
	}
	if ok := resolver.Enqueue(ev); ok {
		t.Fatalf("expected second enqueue to fail when queue is full")
	}
	metrics := resolver.MetricsSnapshot()
	if metrics.DropQueueFull != 1 {
		t.Fatalf("expected one queue-full drop, got %d", metrics.DropQueueFull)
	}
}

func TestSignalResolverObserveCurrentDecisionClasses(t *testing.T) {
	resolver := NewSignalResolver(SignalResolverConfig{})
	key := NewResolverSignalKey(7010.0, "40m", "CW", 500)

	resolver.snapshots.Store(key, ResolverSnapshot{Key: key, State: ResolverStateSplit})
	resolver.ObserveCurrentDecision(key, "DL6LD", true)

	resolver.snapshots.Store(key, ResolverSnapshot{Key: key, State: ResolverStateConfident, Winner: "DL6LN"})
	resolver.ObserveCurrentDecision(key, "DL6LD", true)

	resolver.snapshots.Store(key, ResolverSnapshot{Key: key, State: ResolverStateUncertain})
	resolver.ObserveCurrentDecision(key, "DL6LD", true)

	resolver.snapshots.Store(key, ResolverSnapshot{Key: key, State: ResolverStateProbable, Winner: "DL6LD"})
	resolver.ObserveCurrentDecision(key, "DL6LD", false)

	metrics := resolver.MetricsSnapshot()
	if metrics.DisagreeSplitCorrected != 1 {
		t.Fatalf("expected split disagreement count 1, got %d", metrics.DisagreeSplitCorrected)
	}
	if metrics.DisagreeConfidentDifferentWinner != 1 {
		t.Fatalf("expected confident-different-winner count 1, got %d", metrics.DisagreeConfidentDifferentWinner)
	}
	if metrics.DisagreeUncertainCorrected != 1 {
		t.Fatalf("expected uncertain-corrected count 1, got %d", metrics.DisagreeUncertainCorrected)
	}
	if metrics.DecisionsComparable != 2 {
		t.Fatalf("expected comparable decisions 2, got %d", metrics.DecisionsComparable)
	}
	if metrics.DecisionAgreement != 1 || metrics.DecisionDisagreement != 1 {
		t.Fatalf("expected agreement/disagreement 1/1, got %d/%d", metrics.DecisionAgreement, metrics.DecisionDisagreement)
	}
}

func TestSignalResolverHysteresisTransition(t *testing.T) {
	resolver := NewSignalResolver(SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    32,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      2,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	seed := []ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "DL6LD", Spotter: "K1AAA", FrequencyKHz: 7010.0, RecencyWindow: 20 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "DL6LD", Spotter: "K1AAB", FrequencyKHz: 7010.0, RecencyWindow: 20 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "DL6LN", Spotter: "K1AAC", FrequencyKHz: 7010.1, RecencyWindow: 20 * time.Second},
	}
	for _, ev := range seed {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue seed evidence")
		}
	}
	waitForResolverSnapshotState(t, resolver, key, 500*time.Millisecond, func(s ResolverSnapshot) bool {
		return s.Winner == "DL6LD" && (s.State == ResolverStateConfident || s.State == ResolverStateProbable)
	})

	firstWave := []ResolverEvidence{
		{ObservedAt: now.Add(20 * time.Millisecond), Key: key, DXCall: "DL6LN", Spotter: "K1AAD", FrequencyKHz: 7010.1, RecencyWindow: 20 * time.Second},
		{ObservedAt: now.Add(20 * time.Millisecond), Key: key, DXCall: "DL6LN", Spotter: "K1AAE", FrequencyKHz: 7010.1, RecencyWindow: 20 * time.Second},
		{ObservedAt: now.Add(20 * time.Millisecond), Key: key, DXCall: "DL6LN", Spotter: "K1AAF", FrequencyKHz: 7010.1, RecencyWindow: 20 * time.Second},
	}
	for _, ev := range firstWave {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue first transition wave")
		}
	}
	waitForResolverSnapshotState(t, resolver, key, 500*time.Millisecond, func(s ResolverSnapshot) bool {
		return s.Winner == "DL6LD" && s.State == ResolverStateUncertain
	})

	secondWave := ResolverEvidence{
		ObservedAt:    now.Add(40 * time.Millisecond),
		Key:           key,
		DXCall:        "DL6LN",
		Spotter:       "K1AAG",
		FrequencyKHz:  7010.1,
		RecencyWindow: 20 * time.Second,
	}
	if ok := resolver.Enqueue(secondWave); !ok {
		t.Fatalf("failed to enqueue second transition wave")
	}
	waitForResolverSnapshotState(t, resolver, key, 500*time.Millisecond, func(s ResolverSnapshot) bool {
		return s.Winner == "DL6LN" && (s.State == ResolverStateConfident || s.State == ResolverStateProbable)
	})
}

func TestSignalResolverSplitState(t *testing.T) {
	resolver := NewSignalResolver(SignalResolverConfig{
		QueueSize:                 64,
		MaxActiveKeys:             16,
		MaxCandidatesPerKey:       8,
		MaxReportersPerCand:       32,
		InactiveTTL:               time.Minute,
		EvalMinInterval:           5 * time.Millisecond,
		SweepInterval:             5 * time.Millisecond,
		HysteresisWindows:         2,
		FreqGuardMinSeparationKHz: 1.0,
		FreqGuardRunnerUpRatio:    0.6,
		MaxEditDistance:           3,
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	evs := []ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "DL6LD", Spotter: "K1AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "DL6LD", Spotter: "K1AAB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "DL6LD", Spotter: "K1AAE", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "DL6LN", Spotter: "K1AAC", FrequencyKHz: 7010.1, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "DL6LN", Spotter: "K1AAD", FrequencyKHz: 7010.1, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range evs {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue split evidence")
		}
	}

	waitForResolverSnapshotState(t, resolver, key, 500*time.Millisecond, func(s ResolverSnapshot) bool {
		return s.State == ResolverStateSplit
	})
}

func TestSignalResolverResourceCaps(t *testing.T) {
	resolver := NewSignalResolver(SignalResolverConfig{
		QueueSize:                 64,
		MaxActiveKeys:             1,
		MaxCandidatesPerKey:       2,
		MaxReportersPerCand:       1,
		InactiveTTL:               time.Minute,
		EvalMinInterval:           5 * time.Millisecond,
		SweepInterval:             5 * time.Millisecond,
		FreqGuardMinSeparationKHz: 1.0,
		FreqGuardRunnerUpRatio:    0.6,
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	now := time.Now().UTC()
	key1 := NewResolverSignalKey(7010.0, "40m", "CW", 500)
	key2 := NewResolverSignalKey(7020.0, "40m", "CW", 500)
	evs := []ResolverEvidence{
		{ObservedAt: now, Key: key1, DXCall: "DL6LD", Spotter: "K1AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key2, DXCall: "DL6LN", Spotter: "K1AAB", FrequencyKHz: 7020.0, RecencyWindow: 30 * time.Second}, // max keys
		{ObservedAt: now, Key: key1, DXCall: "DL6LN", Spotter: "K1AAC", FrequencyKHz: 7010.1, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key1, DXCall: "DL6LZ", Spotter: "K1AAD", FrequencyKHz: 7010.2, RecencyWindow: 30 * time.Second}, // max candidates
		{ObservedAt: now, Key: key1, DXCall: "DL6LN", Spotter: "K1AAE", FrequencyKHz: 7010.1, RecencyWindow: 30 * time.Second}, // max reporters
	}
	for _, ev := range evs {
		resolver.Enqueue(ev)
	}
	waitForResolverSnapshotState(t, resolver, key1, 500*time.Millisecond, func(s ResolverSnapshot) bool {
		return s.Winner != ""
	})
	metrics := resolver.MetricsSnapshot()
	if metrics.DropMaxKeys == 0 {
		t.Fatalf("expected max-keys drops > 0")
	}
	if metrics.DropMaxCandidates != 0 {
		t.Fatalf("expected max-candidates drops == 0 with eviction, got %d", metrics.DropMaxCandidates)
	}
	if metrics.DropMaxReporters != 0 {
		t.Fatalf("expected max-reporters drops == 0 with eviction, got %d", metrics.DropMaxReporters)
	}
	if metrics.CapPressureCandidates == 0 {
		t.Fatalf("expected candidate cap pressure > 0")
	}
	if metrics.CapPressureReporters == 0 {
		t.Fatalf("expected reporter cap pressure > 0")
	}
	if metrics.EvictedCandidates == 0 {
		t.Fatalf("expected candidate evictions > 0")
	}
	if metrics.EvictedReporters == 0 {
		t.Fatalf("expected reporter evictions > 0")
	}
	if metrics.HighWaterCandidates == 0 {
		t.Fatalf("expected candidate high-water > 0")
	}
	if metrics.HighWaterReporters == 0 {
		t.Fatalf("expected reporter high-water > 0")
	}
}

func TestChooseResolverCandidateEvictionDeterministic(t *testing.T) {
	base := time.Date(2026, 2, 23, 20, 0, 0, 0, time.UTC)
	candidates := map[string]*resolverCandidate{
		"DL6LD": {
			lastSeen: base,
			reporters: map[string]time.Time{
				"K1AAA": base,
			},
		},
		"DL6LN": {
			lastSeen: base.Add(10 * time.Second),
			reporters: map[string]time.Time{
				"K1AAB": base,
			},
		},
		"DL6LZ": {
			lastSeen: base.Add(10 * time.Second),
			reporters: map[string]time.Time{
				"K1AAC": base,
				"K1AAD": base,
			},
		},
	}
	evict, ok := chooseResolverCandidateEviction(candidates)
	if !ok {
		t.Fatalf("expected eviction candidate")
	}
	if evict != "DL6LD" {
		t.Fatalf("expected oldest weakest candidate DL6LD, got %q", evict)
	}

	// Tie on support and time should break lexicographically.
	candidates = map[string]*resolverCandidate{
		"ZZ1ZZ": {lastSeen: base, reporters: map[string]time.Time{"K1AAA": base}},
		"AA1AA": {lastSeen: base, reporters: map[string]time.Time{"K1AAB": base}},
	}
	evict, ok = chooseResolverCandidateEviction(candidates)
	if !ok {
		t.Fatalf("expected eviction candidate on tie")
	}
	if evict != "AA1AA" {
		t.Fatalf("expected lexicographic tie-break AA1AA, got %q", evict)
	}
}

func TestChooseResolverReporterEvictionDeterministic(t *testing.T) {
	base := time.Date(2026, 2, 23, 20, 0, 0, 0, time.UTC)
	reporters := map[string]time.Time{
		"K1AAB": base.Add(3 * time.Second),
		"K1AAA": base,
		"K1AAZ": base,
	}
	evict, ok := chooseResolverReporterEviction(reporters)
	if !ok {
		t.Fatalf("expected reporter eviction candidate")
	}
	if evict != "K1AAA" {
		t.Fatalf("expected oldest lexicographic reporter K1AAA, got %q", evict)
	}
}

func waitForResolverSnapshotState(t *testing.T, resolver *SignalResolver, key ResolverSignalKey, timeout time.Duration, predicate func(ResolverSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if snap, ok := resolver.Lookup(key); ok && predicate(snap) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap, _ := resolver.Lookup(key)
	t.Fatalf("timed out waiting for resolver snapshot; last=%+v", snap)
}
