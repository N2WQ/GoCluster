package main

import (
	"testing"
	"time"

	"dxcluster/spot"
)

func TestTelnetSpotStabilizerReleasesAfterDelay(t *testing.T) {
	stab := newTelnetSpotStabilizer(25*time.Millisecond, 8)
	stab.Start()
	t.Cleanup(stab.Stop)

	s := spot.NewSpot("K1ABC", "W1XYZ", 7010.0, "CW")
	if ok := stab.Enqueue(s); !ok {
		t.Fatalf("expected enqueue to succeed")
	}

	select {
	case got := <-stab.ReleaseChan():
		if got == nil || got.spot != s {
			t.Fatalf("expected released spot pointer to match enqueued spot")
		}
		if got.checksCompleted != 0 {
			t.Fatalf("expected checksCompleted=0 for first release, got %d", got.checksCompleted)
		}
		if got.delayReason != "" {
			t.Fatalf("expected empty delay reason for default enqueue, got %q", got.delayReason)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for delayed release")
	}
}

func TestTelnetSpotStabilizerEnqueueRespectsMaxPending(t *testing.T) {
	stab := newTelnetSpotStabilizer(100*time.Millisecond, 1)
	// Intentionally do not start the scheduler: this keeps pending at the cap.

	first := spot.NewSpot("K1AAA", "W1XYZ", 7010.0, "CW")
	if ok := stab.Enqueue(first); !ok {
		t.Fatalf("expected first enqueue to succeed")
	}

	second := spot.NewSpot("K1BBB", "W1XYZ", 7011.0, "CW")
	if ok := stab.Enqueue(second); ok {
		t.Fatalf("expected second enqueue to fail at max pending")
	}
	if pending := stab.Pending(); pending != 1 {
		t.Fatalf("expected pending count 1, got %d", pending)
	}
}

func TestTelnetSpotStabilizerReleasesInEnqueueOrderForSameDeadline(t *testing.T) {
	stab := newTelnetSpotStabilizer(20*time.Millisecond, 8)
	stab.Start()
	t.Cleanup(stab.Stop)

	first := spot.NewSpot("K1AAA", "W1XYZ", 7010.0, "CW")
	second := spot.NewSpot("K1BBB", "W1XYZ", 7011.0, "CW")
	if ok := stab.Enqueue(first); !ok {
		t.Fatalf("expected first enqueue to succeed")
	}
	if ok := stab.Enqueue(second); !ok {
		t.Fatalf("expected second enqueue to succeed")
	}

	timeout := time.After(500 * time.Millisecond)
	got := make([]*spot.Spot, 0, 2)
	for len(got) < 2 {
		select {
		case envelope := <-stab.ReleaseChan():
			if envelope == nil || envelope.spot == nil {
				t.Fatalf("expected non-nil release envelope")
			}
			got = append(got, envelope.spot)
		case <-timeout:
			t.Fatalf("timed out waiting for releases")
		}
	}
	if got[0] != first || got[1] != second {
		t.Fatalf("expected FIFO release order, got [%p %p], want [%p %p]", got[0], got[1], first, second)
	}
}

func TestTelnetSpotStabilizerReleaseCarriesChecksCompleted(t *testing.T) {
	stab := newTelnetSpotStabilizer(20*time.Millisecond, 8)
	stab.Start()
	t.Cleanup(stab.Stop)

	s := spot.NewSpot("K1CHK", "W1XYZ", 7010.0, "CW")
	if ok := stab.EnqueueWithChecks(s, 2, "ambiguous_resolver"); !ok {
		t.Fatalf("expected enqueue with checks to succeed")
	}

	select {
	case got := <-stab.ReleaseChan():
		if got == nil || got.spot != s {
			t.Fatalf("expected released spot pointer to match enqueued spot")
		}
		if got.checksCompleted != 2 {
			t.Fatalf("expected checksCompleted=2, got %d", got.checksCompleted)
		}
		if got.delayReason != "ambiguous_resolver" {
			t.Fatalf("expected delayReason to round-trip, got %q", got.delayReason)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for delayed release")
	}
}

func TestTelnetSpotStabilizerReleaseCarriesResolverContext(t *testing.T) {
	stab := newTelnetSpotStabilizer(20*time.Millisecond, 8)
	stab.Start()
	t.Cleanup(stab.Stop)

	s := spot.NewSpot("K1CTX", "W1XYZ", 7010.0, "CW")
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	if ok := stab.EnqueueWithContext(s, 1, "unknown_or_nonrecent", key, true, true); !ok {
		t.Fatalf("expected enqueue with context to succeed")
	}

	select {
	case got := <-stab.ReleaseChan():
		if got == nil || got.spot != s {
			t.Fatalf("expected released spot pointer to match enqueued spot")
		}
		if !got.hasResolverKey {
			t.Fatalf("expected hasResolverKey=true")
		}
		if got.resolverKey != key {
			t.Fatalf("expected resolver key %q, got %q", key.String(), got.resolverKey.String())
		}
		if !got.evidenceEnqueued {
			t.Fatalf("expected evidenceEnqueued=true")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for delayed release")
	}
}
