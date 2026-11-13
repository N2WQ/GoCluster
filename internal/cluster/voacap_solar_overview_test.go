package cluster

import (
	"dxcluster/pathreliability"
	"strings"
	"testing"
	"time"
)

type changingOverviewSolarInput struct{ calls int }

func (p *changingOverviewSolarInput) CurrentSSN(time.Time) (int, bool) {
	p.calls++
	return 73 + p.calls, true
}

type countedOverviewInventory struct {
	calls, cheapCalls, input int
	known                    bool
}

func (p *countedOverviewInventory) Snapshot() pathreliability.VOACAPClosedFallbackSnapshot {
	p.cheapCalls++
	return pathreliability.VOACAPClosedFallbackSnapshot{}
}
func (p *countedOverviewInventory) DedicatedDiagnosticSnapshot(_ time.Time, ssn int, known bool) pathreliability.VOACAPClosedFallbackSnapshot {
	p.calls++
	p.input, p.known = ssn, known
	return pathreliability.VOACAPClosedFallbackSnapshot{CacheEntries: 3, UsableCacheEntries: 2, DifferentSSNEntries: 1, SSNComparisonKnown: known, QueueDepth: 1, QueueOldestAgeKnown: true, QueueOldestAge: 2 * time.Minute, WorkerBusy: true, WorkerBusyFor: 3 * time.Second, WorkerDurationCount: 2, WorkerDurationSum: 4 * time.Second, WorkerDurationMax: 3 * time.Second}
}

func TestOverviewUsesOneCoherentVOACAPPublication(t *testing.T) {
	input, inventory := &changingOverviewSolarInput{}, &countedOverviewInventory{}
	lines := buildOverviewLinesForStatsTest(input, inventory)
	text := strings.Join(lines, "\n")
	if input.calls != 1 || inventory.calls != 1 || inventory.cheapCalls != 0 || inventory.input != 74 || !inventory.known {
		t.Fatalf("input=%+v inventory=%+v", input, inventory)
	}
	for _, want := range []string{"VOACAP SSN[-]: 74", "SSN=74; differs=1/2 usable", "busy=3s; oldest waiting=2m0s", "2 completed; mean=2s; max=3s"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
}

func TestOverviewVOACAPZeroAndUnknownAreDistinct(t *testing.T) {
	for _, known := range []bool{true, false} {
		inventory := &countedOverviewInventory{}
		text := strings.Join(buildOverviewLinesForStatsTest(fixedOverviewSSNProvider{ssn: 0, ok: known}, inventory), "\n")
		want := "SSN=0; differs=1/2 usable"
		if !known {
			want = "SSN=n/a; differs=n/a/2 usable"
		}
		if !strings.Contains(text, want) {
			t.Fatalf("known=%v missing %q in %s", known, want, text)
		}
	}
}
