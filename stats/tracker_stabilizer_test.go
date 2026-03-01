package stats

import "testing"

func TestTrackerStabilizerCounters(t *testing.T) {
	tr := NewTracker()
	tr.IncrementStabilizerHeld()
	tr.IncrementStabilizerHeld()
	tr.IncrementStabilizerHeldReason("unknown_or_nonrecent")
	tr.IncrementStabilizerHeldReason("unknown_or_nonrecent")
	tr.IncrementStabilizerHeldReason("ambiguous_resolver")
	tr.ObserveStabilizerGlyphTurns("?", 2)
	tr.ObserveStabilizerGlyphTurns("P", 1)
	tr.ObserveStabilizerGlyphTurns("p", 3)
	tr.IncrementStabilizerReleasedImmediate()
	tr.IncrementStabilizerReleasedImmediateReason("none")
	tr.IncrementStabilizerReleasedDelayed()
	tr.IncrementStabilizerReleasedDelayedReason("p_low_confidence")
	tr.IncrementStabilizerSuppressedTimeout()
	tr.IncrementStabilizerSuppressedTimeoutReason("ambiguous_resolver")
	tr.IncrementStabilizerOverflowRelease()

	if tr.StabilizerHeld() != 2 {
		t.Fatalf("expected held=2, got %d", tr.StabilizerHeld())
	}
	if tr.StabilizerReleasedImmediate() != 1 {
		t.Fatalf("expected immediate=1, got %d", tr.StabilizerReleasedImmediate())
	}
	if tr.StabilizerReleasedDelayed() != 1 {
		t.Fatalf("expected delayed=1, got %d", tr.StabilizerReleasedDelayed())
	}
	if tr.StabilizerSuppressedTimeout() != 1 {
		t.Fatalf("expected suppressed=1, got %d", tr.StabilizerSuppressedTimeout())
	}
	if tr.StabilizerOverflowRelease() != 1 {
		t.Fatalf("expected overflow_release=1, got %d", tr.StabilizerOverflowRelease())
	}

	heldByReason := tr.StabilizerHeldByReason()
	if heldByReason["unknown_or_nonrecent"] != 2 || heldByReason["ambiguous_resolver"] != 1 {
		t.Fatalf("unexpected held_by_reason counters: %v", heldByReason)
	}
	glyphTurns := tr.StabilizerGlyphTurnStats()
	if got := glyphTurns["?"]; got.Samples != 1 || got.AverageTurns != 2 {
		t.Fatalf("unexpected glyph '?' turn stats: %+v", got)
	}
	if got := glyphTurns["P"]; got.Samples != 2 || got.AverageTurns != 2 {
		t.Fatalf("unexpected glyph 'P' turn stats: %+v", got)
	}
	immediateByReason := tr.StabilizerReleasedImmediateByReason()
	if immediateByReason["none"] != 1 {
		t.Fatalf("unexpected immediate_by_reason counters: %v", immediateByReason)
	}
	delayedByReason := tr.StabilizerReleasedDelayedByReason()
	if delayedByReason["p_low_confidence"] != 1 {
		t.Fatalf("unexpected delayed_by_reason counters: %v", delayedByReason)
	}
	suppressedByReason := tr.StabilizerSuppressedByReason()
	if suppressedByReason["ambiguous_resolver"] != 1 {
		t.Fatalf("unexpected suppressed_by_reason counters: %v", suppressedByReason)
	}
}
