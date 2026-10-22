package telnet

import (
	"runtime"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestMatchingSnapshotRetainsProgramControlsAndPath(t *testing.T) {
	s, c, sp, _ := newPathPredictionReuseFixture(t, 1, true, false)
	c.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=HIGH"})
	c.filter.SetSelfEnabled(false)
	captured := c.captureMatchingSnapshot()
	next := &Client{
		filter: testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RuleReject, Expr: "MODE=FT8"}),
		grid:   "EM10", gridCell: 3, noiseClass: "URBAN", pathMinObservationCount: 100,
	}
	s.publishConfigurationState(c, filter.SettingsConfiguration{}, next, nil, time.Now(), false)
	matches, prediction, predicted := s.matchSnapshot(captured, sp)
	if !matches || !predicted || prediction.grid != "FN31" || prediction.userCell != 1 ||
		prediction.noiseClass != "QUIET" || prediction.minObservation != 1 || captured.filter.SelfEnabled() {
		t.Fatalf("capture mixed old/new state: matched=%v predicted=%v prediction=%+v self=%v",
			matches, predicted, prediction, captured.filter.SelfEnabled())
	}
	if matched, _, _ := s.matchSnapshot(c.captureMatchingSnapshot(), sp); matched {
		t.Fatal("new capture did not use the replacement REJECT")
	}
}

func TestMatchingSnapshotConcurrentPublicationIsCoherent(t *testing.T) {
	a := &Client{
		filter: testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW AND PATH=HIGH"}),
		grid:   "FN31", gridCell: 1, gridCoarseCell: 2, noiseClass: "QUIET", pathMinObservationCount: 3,
	}
	a.filter.SetSelfEnabled(false)
	b := &Client{
		filter: testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=CLOSED"}),
		grid:   "EM10", gridCell: 3, gridCoarseCell: 4, noiseClass: "URBAN", pathMinObservationCount: 100,
	}
	c := &Client{filter: testRuleFilter(t), configurationInitialized: true}
	s := &Server{}
	s.publishConfigurationState(c, filter.SettingsConfiguration{}, a, nil, time.Time{}, false)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 500 {
			next := a
			if i%2 != 0 {
				next = b
			}
			s.publishConfigurationState(c, filter.SettingsConfiguration{}, next, nil, time.Time{}, false)
			runtime.Gosched()
		}
	}()
	defer func() { <-done }()
	cw := spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
	ft8 := spot.NewSpot("K1ABC", "W1ABC", 14074, "FT8")
	for range 500 {
		got := c.captureMatchingSnapshot()
		switch got.path.grid {
		case "FN31":
			if got.path.gridCell != 1 || got.path.gridCoarseCell != 2 || got.path.noiseClass != "QUIET" ||
				got.path.pathMinObservationCount != 3 || got.filter.SelfEnabled() ||
				!got.filter.MatchesWithPath(cw, filter.PathClassHigh) || got.filter.MatchesWithPath(ft8, filter.PathClassClosed) {
				t.Fatalf("mixed FN31 capture: %+v", got.path)
			}
		case "EM10":
			if got.path.gridCell != 3 || got.path.gridCoarseCell != 4 || got.path.noiseClass != "URBAN" ||
				got.path.pathMinObservationCount != 100 || !got.filter.SelfEnabled() ||
				got.filter.MatchesWithPath(cw, filter.PathClassHigh) || !got.filter.MatchesWithPath(ft8, filter.PathClassClosed) {
				t.Fatalf("mixed EM10 capture: %+v", got.path)
			}
		default:
			t.Fatalf("unexpected captured grid %q", got.path.grid)
		}
		runtime.Gosched()
	}
}

func TestMatchingSnapshotWarmAllocations(t *testing.T) {
	c := &Client{filter: testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: `MODE=CW AND COMMENT="CQ"`})}
	sp := spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
	sp.Comment = "CQ test"
	s := &Server{}
	matches := false
	if got := testing.AllocsPerRun(1000, func() {
		matches, _, _ = s.matchSnapshot(c.captureMatchingSnapshot(), sp)
	}); got != 0 {
		t.Fatalf("warm capture/evaluation allocations = %v; want 0", got)
	}
	if !matches {
		t.Fatal("allocation workload did not evaluate the expected match")
	}
}

func BenchmarkMatchingSnapshot(b *testing.B) {
	c := &Client{filter: testRuleFilter(b, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: `MODE=CW AND COMMENT="CQ"`})}
	sp := spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
	sp.Comment = "CQ test"
	s := &Server{}
	b.ReportAllocs()
	for b.Loop() {
		if matched, _, _ := s.matchSnapshot(c.captureMatchingSnapshot(), sp); !matched {
			b.Fatal("expected match")
		}
	}
}

func TestMatchingSnapshotMissingFilterFailsClosed(t *testing.T) {
	var c *Client
	s := &Server{}
	sp := spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
	for _, snapshot := range []clientMatchingSnapshot{c.captureMatchingSnapshot(), (&Client{}).captureMatchingSnapshot()} {
		if matched, _, _ := s.matchSnapshot(snapshot, sp); matched || snapshot.filter.SelfEnabled() {
			t.Fatal("missing filter broadened admission")
		}
	}
	if matched, _, _ := s.matchSnapshot(clientMatchingSnapshot{path: pathState{gridCell: pathreliability.InvalidCell}}, nil); matched {
		t.Fatal("missing spot matched")
	}
}
