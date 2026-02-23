package stats

import "testing"

func TestTrackerStabilizerCounters(t *testing.T) {
	tr := NewTracker()
	tr.IncrementStabilizerHeld()
	tr.IncrementStabilizerHeld()
	tr.IncrementStabilizerReleasedImmediate()
	tr.IncrementStabilizerReleasedDelayed()
	tr.IncrementStabilizerSuppressedTimeout()
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
}

