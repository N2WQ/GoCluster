package cluster

import (
	"container/heap"
	"sync"
	"testing"
	"time"

	"dxcluster/spot"
)

func stabilizerRetirementWait(t *testing.T, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func stabilizerRetirementAssertClosed(t *testing.T, s *telnetSpotStabilizer) {
	t.Helper()
	if s.Pending() != 0 || len(s.in) != 0 {
		t.Fatalf("retained held work: pending=%d input=%d", s.Pending(), len(s.in))
	}
	select {
	case <-s.done:
	default:
		t.Fatal("Stop returned before scheduler completion")
	}
	select {
	case envelope, ok := <-s.ReleaseChan():
		if ok || envelope != nil {
			t.Fatal("unreleased work was delivered instead of dropped")
		}
	default:
		t.Fatal("Stop returned before closing the release channel")
	}
	if s.Enqueue(spot.NewSpot("K9END", "W1XYZ", 7030, "CW")) {
		t.Fatal("terminal stabilizer accepted another reservation")
	}
	if s.Pending() != 0 || len(s.in) != 0 {
		t.Fatal("refused post-Stop input changed retained state")
	}
}

func TestStabilizerRetirementBeforeStartDropsInputAndCannotRevive(t *testing.T) {
	s := newTelnetSpotStabilizer(time.Hour, 2)
	t.Cleanup(s.Stop)
	for _, call := range []string{"K1AAA", "K1BBB"} {
		if !s.Enqueue(spot.NewSpot(call, "W1XYZ", 14030, "CW")) {
			t.Fatal("pre-Start admission refused a fitting reservation")
		}
	}
	if s.Enqueue(spot.NewSpot("K1CCC", "W1XYZ", 14030, "CW")) {
		t.Fatal("pre-Start admission exceeded the fixed pending cap")
	}
	if s.Pending() != 2 || len(s.in) != 2 {
		t.Fatal("pre-Start fixture did not own two input reservations")
	}
	s.Stop()
	stabilizerRetirementAssertClosed(t, s)
	s.Start()
	s.Start()
	s.Stop()
	stabilizerRetirementAssertClosed(t, s)
}

func TestStabilizerRetirementDropsLongSchedulerHolds(t *testing.T) {
	s := newTelnetSpotStabilizer(25*time.Hour, 4)
	t.Cleanup(s.Stop)
	s.Start()
	s.Start()
	for _, call := range []string{"K2AAA", "K2BBB", "K2CCC"} {
		if !s.Enqueue(spot.NewSpot(call, "W1XYZ", 14030, "CW")) {
			t.Fatal("long-hold admission failed")
		}
	}
	stabilizerRetirementWait(t, func() bool { return len(s.in) == 0 }, "scheduler input consumption")
	if s.Pending() != 3 || len(s.release) != 0 {
		t.Fatal("long-hold fixture did not retain three unreleased reservations")
	}
	s.Stop()
	stabilizerRetirementAssertClosed(t, s)
}

func TestStabilizerRetirementUnblocksPublicationAndDropsRemainingInput(t *testing.T) {
	s := newTelnetSpotStabilizer(time.Millisecond, 4)
	t.Cleanup(s.Stop)
	// Released envelopes already belong to the separate output consumer. Fill
	// that boundary so the real scheduler's next publication must block.
	sentinel := &telnetStabilizerEnvelope{spot: spot.NewSpot("K3OLD", "W1XYZ", 14030, "CW")}
	for range cap(s.release) {
		s.release <- sentinel
	}
	s.Start()
	blocked := spot.NewSpot("K3PUB", "W1XYZ", 14030, "CW")
	if !s.Enqueue(blocked) {
		t.Fatal("publication control was refused")
	}
	stabilizerRetirementWait(t, func() bool { return s.Pending() == 0 && len(s.in) == 0 }, "blocked scheduler publication")
	for _, call := range []string{"K3AAA", "K3BBB"} {
		if !s.Enqueue(spot.NewSpot(call, "W1XYZ", 14030, "CW")) {
			t.Fatal("input control was refused")
		}
	}
	if len(s.in) != 2 || s.Pending() != 2 {
		t.Fatal("blocked scheduler failed to retain the two input controls")
	}
	s.Stop()
	if s.Pending() != 0 || len(s.in) != 0 || len(s.release) != cap(s.release) {
		t.Fatalf("incorrect retirement: pending=%d input=%d released=%d", s.Pending(), len(s.in), len(s.release))
	}
	for range cap(s.release) {
		if got, ok := <-s.ReleaseChan(); !ok || got != sentinel {
			t.Fatal("Stop discarded or replaced consumer-owned release data")
		}
	}
	stabilizerRetirementAssertClosed(t, s)
}

func TestStabilizerRetirementPreservesActuallyReleasedEnvelope(t *testing.T) {
	s := newTelnetSpotStabilizer(time.Millisecond, 2)
	t.Cleanup(s.Stop)
	s.Start()
	sp := spot.NewSpot("K4ABC", "W1XYZ", 14030, "CW")
	key := spot.NewResolverSignalKey(14030, "20m", "CW", 500)
	if !s.EnqueueWithContext(sp, 3, "still-risky", key, true, true) {
		t.Fatal("context admission failed")
	}
	stabilizerRetirementWait(t, func() bool { return len(s.release) == 1 }, "actual scheduled release")
	s.Stop()
	got, ok := <-s.ReleaseChan()
	if !ok || got == nil || got.spot != sp || got.checksCompleted != 3 || got.delayReason != "still-risky" ||
		got.resolverKey != key || !got.hasResolverKey || !got.evidenceEnqueued {
		t.Fatalf("consumer-owned release changed across Stop: %+v, open=%v", got, ok)
	}
	// A received envelope stays caller-owned after Stop; retirement neither
	// clears it nor revokes the spot or context carried through the boundary.
	sp.Comment = "received after scheduler retirement"
	if got.spot.Comment != sp.Comment {
		t.Fatal("released spot ownership was revoked")
	}
	stabilizerRetirementAssertClosed(t, s)
}

func TestStabilizerRetirementConcurrentStartStopAndAdmission(t *testing.T) {
	for round := range 20 {
		t.Run(string(rune('A'+round)), func(t *testing.T) {
			s := newTelnetSpotStabilizer(time.Hour, 8)
			sp := spot.NewSpot("K5ABC", "W1XYZ", 14030, "CW")
			gate := make(chan struct{})
			joined := make(chan struct{})
			var gateOnce sync.Once
			var owners sync.WaitGroup
			t.Cleanup(func() {
				gateOnce.Do(func() { close(gate) })
				s.Stop()
				select {
				case <-joined:
				case <-time.After(2 * time.Second):
					t.Error("test-owned lifecycle callers did not join")
				}
			})
			for i := range 24 {
				owners.Add(1)
				go func() {
					defer owners.Done()
					<-gate
					switch i % 3 {
					case 0:
						s.Start()
					case 1:
						s.Stop()
					case 2:
						for range 8 {
							s.Enqueue(sp)
						}
					}
				}()
			}
			go func() { owners.Wait(); close(joined) }()
			gateOnce.Do(func() { close(gate) })
			select {
			case <-joined:
			case <-time.After(2 * time.Second):
				t.Fatal("concurrent lifecycle callers did not finish")
			}
			s.Stop()
			stabilizerRetirementAssertClosed(t, s)
		})
	}
}

func TestStabilizerRetirementDoesNotHideUnownedPendingMismatch(t *testing.T) {
	s := newTelnetSpotStabilizer(time.Hour, 4)
	t.Cleanup(s.Stop)
	// Deliberately corrupt accounting to distinguish item retirement from an
	// unconditional zero reset. Only the real admitted item belongs to Stop.
	s.pending.Add(1)
	if !s.Enqueue(spot.NewSpot("K6ABC", "W1XYZ", 14030, "CW")) {
		t.Fatal("accounting control admission failed")
	}
	s.Stop()
	if s.Pending() != 1 || len(s.in) != 0 {
		t.Fatalf("Stop hid an unowned counter mismatch: pending=%d input=%d", s.Pending(), len(s.in))
	}
	select {
	case got, ok := <-s.ReleaseChan():
		if ok || got != nil {
			t.Fatal("accounting control unexpectedly released")
		}
	default:
		t.Fatal("accounting control release channel remains open")
	}
}

func TestStabilizerHeapPopClearsRetiredBackingReference(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	queue := telnetStabilizerHeap{
		{spot: spot.NewSpot("K7AAA", "W1XYZ", 14030, "CW"), due: at, seq: 0},
		{spot: spot.NewSpot("K7BBB", "W1XYZ", 14030, "CW"), due: at, seq: 1},
		{spot: spot.NewSpot("K7CCC", "W1XYZ", 14030, "CW"), due: at, seq: 2},
	}
	backing := queue[:cap(queue)]
	heap.Init(&queue)
	for want := range 3 {
		got, ok := heap.Pop(&queue).(*telnetStabilizerItem)
		if !ok || got == nil || got.seq != uint64(want) {
			t.Fatalf("equal-due order changed: got=%+v want sequence=%d", got, want)
		}
		if backing[len(queue)] != nil {
			t.Fatal("heap backing slice retained a retired item")
		}
	}
}
