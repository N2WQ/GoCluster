package cluster

import (
	"container/heap"
	"testing"
	"time"

	"dxcluster/spot"
)

func futureTemporalDrainFixture(t testing.TB) (*runtimeTemporalController, time.Time) {
	t.Helper()
	controller := newRuntimeTemporalController(runtimeTemporalTestConfig(0))
	start := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	accepted, _, reason := controller.Observe(start, key, "K1ABC", runtimeTemporalPending{
		spot:      spot.NewSpot("K1ABC", "W1XYZ", 7010, "CW"),
		selection: runtimeTemporalTestSelection(key, "K1ABC", 3200, "K1ABD", 1800),
	})
	if !accepted {
		t.Fatalf("fixture refused temporal hold: %s", reason)
	}
	return controller, start.Add(time.Second)
}

func TestRuntimeTemporalNoDueDrainAllocation(t *testing.T) {
	controller, now := futureTemporalDrainFixture(t)
	due, ok := controller.NextDue()
	if !ok || !due.Equal(now.Add(time.Second)) {
		t.Fatal("fixture is not before its real due time")
	}
	allocations := testing.AllocsPerRun(100, func() {
		if releases := controller.Drain(now, false); len(releases) != 0 {
			t.Fatal("future hold was released early")
		}
	})
	if allocations != 0 {
		t.Fatalf("no-due controller drain allocated %g times, want zero", allocations)
	}
	if len(controller.pending) != 1 || len(controller.queue) != 1 || controller.pending[1].id != 1 {
		t.Fatal("no-due drain changed request ownership")
	}
	if after, ok := controller.NextDue(); !ok || !after.Equal(due) {
		t.Fatal("no-due drain changed its deadline")
	}
}

func TestPopRuntimeTemporalDueBoundaries(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	for _, force := range []bool{false, true} {
		queue := runtimeTemporalHeap{
			{id: 1, due: now.Add(time.Nanosecond), seq: 3},
			{id: 2, due: now.Add(-time.Nanosecond), seq: 2},
			{id: 3, due: now, seq: 1},
			{id: 4, due: now, seq: 0},
		}
		heap.Init(&queue)
		got := popRuntimeTemporalDue(&queue, now, force)
		want := []uint64{2, 4, 3}
		if force {
			want = append(want, 1)
		}
		if len(got) != len(want) {
			t.Fatalf("force=%v released %d want %d", force, len(got), len(want))
		}
		for i, id := range want {
			if got[i].id != id {
				t.Fatalf("force=%v sequence[%d]=%d want %d", force, i, got[i].id, id)
			}
		}
		remaining := 1
		if force {
			remaining = 0
		}
		if len(queue) != remaining {
			t.Fatal("due selection changed future ownership")
		}
	}
	if len(popRuntimeTemporalDue(nil, now, false)) != 0 {
		t.Fatal("nil heap released work")
	}
	for _, queue := range []runtimeTemporalHeap{nil, {}, {nil}} {
		before := len(queue)
		if len(popRuntimeTemporalDue(&queue, now, true)) != 0 || len(queue) != before {
			t.Fatal("empty/nil-head behavior changed")
		}
	}
}

func BenchmarkRuntimeTemporalDrainNoDue(b *testing.B) {
	controller, now := futureTemporalDrainFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(controller.Drain(now, false)) != 0 {
			b.Fatal("future hold released early")
		}
	}
	b.StopTimer()
	if len(controller.pending) != 1 || len(controller.queue) != 1 {
		b.Fatal("benchmark lost future state")
	}
}

func BenchmarkPopRuntimeTemporalDueControl(b *testing.B) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	for _, force := range []bool{false, true} {
		name := "due"
		if force {
			name = "force_future"
		}
		b.Run(name, func(b *testing.B) {
			due := now
			if force {
				due = now.Add(time.Hour)
			}
			first, second := &runtimeTemporalItem{id: 1, due: due, seq: 0}, &runtimeTemporalItem{id: 2, due: due, seq: 1}
			queue := make(runtimeTemporalHeap, 0, 2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				queue = append(queue[:0], first, second)
				got := popRuntimeTemporalDue(&queue, now, force)
				if len(got) != 2 || got[0] != first || got[1] != second || len(queue) != 0 {
					b.Fatal("control changed due/force ordering")
				}
			}
		})
	}
}
