package cluster

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// Admission tests use the arrival clock and existing heap/controller ownership,
// with no preceding Drain that could hide an expired-burst admission defect.
func p4FTContext(call, receiver, mode string, observedAt time.Time) outputSpotContext {
	s := spot.NewSpot(call, receiver, 14074.1, mode)
	s.Time = observedAt
	return outputSpotContext{spot: s, modeUpper: mode}
}

func p4FTSeed(t testing.TB, c *ftConfidenceController, at time.Time, ctx outputSpotContext) *ftConfidencePendingGroup {
	t.Helper()
	held, _, retired := c.Observe(at, ctx)
	if !held || len(retired) != 0 {
		t.Fatalf("predeadline seed failed or retired a group: held=%t retired=%d", held, len(retired))
	}
	key, _, ok := buildFTConfidenceKey(ctx.spot)
	if !ok || c.pending[key] == nil {
		t.Fatal("seed omitted its pending FT group")
	}
	return c.pending[key]
}

func TestP4FTObserveExpiryBoundaries(t *testing.T) {
	configured := config.CallCorrectionConfig{
		PMinUniqueSpotters: 3, VMinUniqueSpotters: 4,
		FT8QuietGapSeconds: 7, FT8HardCapSeconds: 14,
		FT4QuietGapSeconds: 4, FT4HardCapSeconds: 9,
		FT2QuietGapSeconds: 2, FT2HardCapSeconds: 5,
	}
	for _, policy := range []struct {
		name    string
		cfg     config.CallCorrectionConfig
		timings []struct {
			mode        string
			quiet, hard time.Duration
		}
	}{
		{"defaults", config.CallCorrectionConfig{}, []struct {
			mode        string
			quiet, hard time.Duration
		}{{"FT8", 6 * time.Second, 12 * time.Second}, {"FT4", 5 * time.Second, 10 * time.Second}, {"FT2", 3 * time.Second, 6 * time.Second}}},
		{"configured", configured, []struct {
			mode        string
			quiet, hard time.Duration
		}{{"FT8", 7 * time.Second, 14 * time.Second}, {"FT4", 4 * time.Second, 9 * time.Second}, {"FT2", 2 * time.Second, 5 * time.Second}}},
	} {
		for _, timing := range policy.timings {
			for _, boundary := range []string{"quiet", "hard"} {
				for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
					t.Run(policy.name+"/"+timing.mode+"/"+boundary+"/"+delta.String(), func(t *testing.T) {
						c := newFTConfidenceController(policy.cfg)
						base := time.Unix(1_700_000_000, 0).UTC()
						old := p4FTSeed(t, c, base, p4FTContext("K1ABC", "N0AAA", timing.mode, base))
						deadline := base.Add(timing.quiet)
						if boundary == "hard" {
							deadline = base.Add(timing.hard)
							// Keep the same DE alive strictly inside each quiet gap.
							for arrival := base.Add(timing.quiet / 2); arrival.Before(deadline); arrival = arrival.Add(timing.quiet / 2) {
								p4FTSeed(t, c, arrival, p4FTContext("K1ABC", "N0AAA", timing.mode, base.Add(time.Hour)))
							}
						}
						if !old.due.Equal(deadline) || !old.hardDeadline.Equal(base.Add(timing.hard)) {
							t.Fatalf("fixture failed literal %s deadline: due=%s hard=%s", boundary, old.due, old.hardDeadline)
						}
						oldContexts := len(old.contexts)
						arrival := deadline.Add(delta)
						held, unique, retired := c.Observe(arrival, p4FTContext("K1ABC", "N0BBB", timing.mode, base.Add(2*time.Hour)))
						current := c.pending[old.key]
						if !held || current == nil {
							t.Fatal("boundary arrival was not held")
						}
						if delta < 0 {
							if unique != 2 || len(retired) != 0 || current != old || len(old.contexts) != oldContexts+1 {
								t.Fatalf("strictly predeadline input split its live burst: unique=%d retired=%d same=%t", unique, len(retired), current == old)
							}
						} else {
							if unique != 1 || len(retired) != 1 || retired[0] != old || current == old || len(old.contexts) != oldContexts || len(old.spotters) != 1 ||
								len(current.contexts) != 1 || len(current.spotters) != 1 || c.pendingSpots != 1 || !current.firstSeen.Equal(arrival) ||
								!current.due.Equal(arrival.Add(timing.quiet)) || !current.hardDeadline.Equal(arrival.Add(timing.hard)) {
								t.Fatalf("exclusive %s boundary joined or lost old burst: unique=%d retired=%d oldContexts=%d pendingSpots=%d", boundary, unique, len(retired), len(old.contexts), c.pendingSpots)
							}
							if again := c.Drain(arrival, false); len(again) != 0 {
								t.Fatal("retired burst was drained twice")
							}
						}
					})
				}
			}
		}
	}
}

func TestP4FTObserveRetiresAllDueInDueSequenceOrder(t *testing.T) {
	c := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	a := p4FTSeed(t, c, base, p4FTContext("K1AAA", "N0AAA", "FT8", base))
	b := p4FTSeed(t, c, base.Add(time.Second), p4FTContext("K1BBB", "N0BBB", "FT8", base))
	d := p4FTSeed(t, c, base.Add(time.Second), p4FTContext("K1CCC", "N0CCC", "FT8", base))
	p4FTSeed(t, c, base.Add(2*time.Second), p4FTContext("K1AAA", "N0DDD", "FT8", base)) // stale A heap head at +6s
	held, unique, retired := c.Observe(base.Add(7*time.Second), p4FTContext("K1NEW", "N0EEE", "FT8", base))
	if !held || unique != 1 || len(retired) != 2 || retired[0] != b || retired[1] != d || c.pending[a.key] != a || c.pendingSpots != 3 {
		t.Fatalf("arrival failed all-due ordered retirement: held=%t unique=%d retired=%v pendingSpots=%d", held, unique, retired, c.pendingSpots)
	}
	if due, ok := c.NextDue(); !ok || !due.Equal(base.Add(8*time.Second)) {
		t.Fatalf("stale head hid next live deadline: due=%s available=%t", due, ok)
	}
	_, _, retired = c.Observe(base.Add(7*time.Second), p4FTContext("K1NEW", "N0FFF", "FT8", base))
	if len(retired) != 0 || len(b.contexts) != 1 || len(d.contexts) != 1 {
		t.Fatal("same-clock Observe released or modified an old burst twice")
	}
	_, _, retired = c.Observe(base.Add(8*time.Second), p4FTContext("K1NEW", "N0GGG", "FT8", base))
	if len(retired) != 1 || retired[0] != a || len(a.contexts) != 2 {
		t.Fatal("unrelated later-due group did not retire exactly once")
	}
}

func TestP4FTObserveRejectsStaleGenerationAndReclaimsCaps(t *testing.T) {
	t.Run("stale same-key generation", func(t *testing.T) {
		c := newFTConfidenceController(config.CallCorrectionConfig{})
		base := time.Unix(1_700_000_000, 0).UTC()
		old := p4FTSeed(t, c, base, p4FTContext("K1ABC", "N0AAA", "FT8", base))
		_, _, retired := c.Observe(base.Add(6*time.Second), p4FTContext("K1ABC", "N0BBB", "FT8", base))
		if len(retired) != 1 || retired[0] != old {
			t.Fatal("fixture did not retire the first generation")
		}
		current := c.pending[old.key]
		// An earlier stale node must not retire the still-live replacement.
		// Normal reschedule nodes are also exercised by the all-due test.
		c.queue.push(ftConfidenceItem{key: old.key, due: old.due, seq: old.seq})
		held, unique, retired := c.Observe(old.due.Add(time.Nanosecond), p4FTContext("K1OTHER", "N0DDD", "CW", base))
		if held || unique != 1 || len(retired) != 0 || c.pending[old.key] != current || len(current.contexts) != 1 || c.pendingSpots != 1 {
			t.Fatal("an earlier stale heap node retired or modified the live replacement generation")
		}
		// At the live deadline, a same-deadline stale node must still produce
		// one release. This is once-only coverage, not an isolated sequence oracle.
		c.queue.push(ftConfidenceItem{key: old.key, due: current.due, seq: old.seq})
		_, unique, retired = c.Observe(current.due, p4FTContext("K1ABC", "N0CCC", "FT8", base))
		if unique != 1 || len(retired) != 1 || retired[0] != current || len(current.contexts) != 1 || c.pendingSpots != 1 {
			t.Fatal("stale same-key heap node retired the new generation incorrectly")
		}
	})
	t.Run("live group cap with spare spot capacity", func(t *testing.T) {
		c := newFTConfidenceController(config.CallCorrectionConfig{})
		c.maxPendingGroups, c.maxPendingSpots = 1, 3
		base := time.Unix(1_700_000_000, 0).UTC()
		old := p4FTSeed(t, c, base, p4FTContext("K1AAA", "N0AAA", "FT8", base))
		held, unique, retired := c.Observe(base.Add(time.Second), p4FTContext("K1BBB", "N0BBB", "FT8", base))
		if held || unique != 1 || len(retired) != 0 || len(c.pending) != 1 || c.pendingSpots != 1 || c.pending[old.key] != old ||
			len(old.contexts) != 1 || len(old.spotters) != 1 || c.queue.Len() != 1 {
			t.Fatal("live group cap was bypassed or confused with available spot capacity")
		}
	})
	t.Run("groups and spots reclaimed before admission", func(t *testing.T) {
		c := newFTConfidenceController(config.CallCorrectionConfig{})
		c.maxPendingGroups, c.maxPendingSpots = 2, 3
		base := time.Unix(1_700_000_000, 0).UTC()
		a := p4FTSeed(t, c, base, p4FTContext("K1AAA", "N0AAA", "FT8", base))
		b := p4FTSeed(t, c, base, p4FTContext("K1BBB", "N0BBB", "FT8", base))
		p4FTSeed(t, c, base, p4FTContext("K1BBB", "N0CCC", "FT8", base))
		if len(c.pending) != 2 || c.pendingSpots != 3 {
			t.Fatal("fixture did not fill both caps")
		}
		held, unique, retired := c.Observe(base.Add(6*time.Second), p4FTContext("K1CCC", "N0DDD", "FT8", base))
		if !held || unique != 1 || len(retired) != 2 || retired[0] != a || retired[1] != b || len(c.pending) != 1 || c.pendingSpots != 1 {
			t.Fatal("due state caused avoidable cap fail-open or missing release")
		}
		p4FTSeed(t, c, base.Add(6*time.Second), p4FTContext("K1DDD", "N0EEE", "FT8", base))
		p4FTSeed(t, c, base.Add(6*time.Second), p4FTContext("K1CCC", "N0FFF", "FT8", base))
		held, unique, retired = c.Observe(base.Add(7*time.Second), p4FTContext("K1CCC", "N0GGG", "FT8", base))
		if held || unique != 3 || len(retired) != 0 || len(c.pending) != 2 || c.pendingSpots != 3 {
			t.Fatal("live cap pressure changed existing fail-open/count semantics")
		}
	})
}

func TestP4FTObserveRetiresBeforeUnsupportedModeAdmission(t *testing.T) {
	c := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	old := p4FTSeed(t, c, base, p4FTContext("K1ABC", "N0AAA", "FT8", base))
	held, unique, retired := c.Observe(base.Add(6*time.Second), p4FTContext("K1OTHER", "N0BBB", "CW", base))
	if held || unique != 1 || len(retired) != 1 || retired[0] != old || len(c.pending) != 0 || c.pendingSpots != 0 {
		t.Fatal("unsupported current key skipped unrelated due retirement")
	}
}

func TestP4FTObserveRetiresBeforeNilContextReturn(t *testing.T) {
	c := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	old := p4FTSeed(t, c, base, p4FTContext("K1ABC", "N0AAA", "FT8", base))
	held, unique, retired := c.Observe(base.Add(6*time.Second), outputSpotContext{})
	if held || unique != 1 || len(retired) != 1 || retired[0] != old || len(old.contexts) != 1 || len(old.spotters) != 1 ||
		len(c.pending) != 0 || c.pendingSpots != 0 || c.queue.Len() != 0 {
		t.Fatal("nil incoming context skipped unrelated due retirement before returning")
	}
	if _, _, again := c.Observe(base.Add(6*time.Second), outputSpotContext{}); len(again) != 0 {
		t.Fatal("nil-context return retired the same due generation twice")
	}
}

func TestP4FTObservePreservesEmptyHeapScratch(t *testing.T) {
	c := newFTConfidenceController(config.CallCorrectionConfig{})
	capacity := cap(c.dueScratch)
	if capacity == 0 {
		t.Fatal("controller did not prepare bounded due scratch")
	}
	base := time.Unix(1_700_000_000, 0).UTC()
	p4FTSeed(t, c, base, p4FTContext("K1ABC", "N0AAA", "FT8", base))
	if cap(c.dueScratch) != capacity {
		t.Fatal("empty-heap admission discarded reusable due scratch")
	}
	if len(c.Drain(base, true)) != 1 || len(c.Drain(base, false)) != 0 || cap(c.dueScratch) != capacity {
		t.Fatal("empty retirement discarded bounded reusable capacity")
	}
}

func TestP4FTObserveLateDuplicateCannotUpgradeOldBurst(t *testing.T) {
	c := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	old := p4FTSeed(t, c, base, p4FTContext("K1ABC", "N0AAA", "FT8", base))
	p4FTSeed(t, c, base.Add(time.Second), p4FTContext("K1ABC", "N0BBB", "FT8", base))
	_, unique, retired := c.Observe(base.Add(7*time.Second), p4FTContext("K1ABC", " n0bbb-1 ", "FT8", base))
	if unique != 1 || len(retired) != 1 || retired[0] != old || len(old.contexts) != 2 || len(old.spotters) != 2 {
		t.Fatal("late normalized duplicate mutated the old P burst")
	}
	current := c.pending[old.key]
	if len(current.contexts) != 1 || len(current.spotters) != 1 || ftConfidenceGlyphForUniqueSpotters(len(old.spotters), c.policy) != "P" {
		t.Fatal("new duplicate arrival borrowed old receiver support")
	}
	p4FTSeed(t, c, base.Add(8*time.Second), p4FTContext("K1ABC", "N0CCC", "FT8", base))
	if len(old.contexts) != 2 || len(old.spotters) != 2 || len(current.spotters) != 2 {
		t.Fatal("later evidence upgraded a retired generation")
	}
}

func TestP4FTObserveChurnConvergesRetainedCardinality(t *testing.T) {
	c := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	seen := make(map[*ftConfidencePendingGroup]bool)
	const bursts = 256
	for burst := 0; burst < bursts; burst++ {
		at := base.Add(time.Duration(burst) * (6*time.Second + 2*time.Nanosecond))
		held, unique, retired := c.Observe(at, p4FTContext("K1ABC", "N0AAA", "FT8", base))
		if !held || unique != 1 || len(retired) != min(burst, 1) {
			t.Fatalf("churn burst %d retained old support or lost expiry", burst)
		}
		for _, group := range retired {
			if seen[group] || len(group.contexts) != 2 || len(group.spotters) != 2 {
				t.Fatal("churn emitted a generation twice or with wrong membership")
			}
			seen[group] = true
		}
		p4FTSeed(t, c, at.Add(time.Nanosecond), p4FTContext("K1ABC", "N0BBB", "FT8", base))
		if len(c.pending) != 1 || c.pendingSpots != 2 || c.queue.Len() > 2 {
			t.Fatalf("churn failed bounded retained state: groups=%d contexts=%d heap=%d", len(c.pending), c.pendingSpots, c.queue.Len())
		}
	}
	for _, group := range c.Drain(base.Add(time.Hour), true) {
		if seen[group] {
			t.Fatal("final drain repeated a retired generation")
		}
		seen[group] = true
	}
	if len(seen) != bursts || len(c.pending) != 0 || c.pendingSpots != 0 || c.queue.Len() != 0 || len(c.Drain(base.Add(time.Hour), true)) != 0 {
		t.Fatalf("churn did not converge after final release: generations=%d groups=%d spots=%d heap=%d", len(seen), len(c.pending), c.pendingSpots, c.queue.Len())
	}
	for _, item := range c.dueScratch[:cap(c.dueScratch)] {
		if item != (ftConfidenceItem{}) {
			t.Fatal("retired scratch capacity still pins a key")
		}
	}
}
