package telnet

import (
	"runtime"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/internal/testsupport"
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

func TestMatchingSnapshotNearbyPreservesBooleanDelivery(t *testing.T) {
	near, far, nearCoarse, _ := requireDistinctPathCells(t)
	for _, tc := range []struct {
		expression, continent string
		endpoint              pathreliability.CellID
		want                  bool
	}{
		{"NEARBY AND DXCONT=EU", "NA", near, false},
		{"NEARBY OR DXCONT=EU", "EU", far, true},
		{"NEARBY", "NA", near, true},
		{"NOT NEARBY", "NA", far, true},
		{"NEARBY AND MODE=FT8", "EU", near, false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			s := &Server{dedupeFastEnabled: true}
			c := &Client{callsign: "N0USER", filter: passRuleTestFilter(t, tc.expression),
				gridCell: near, gridCoarseCell: nearCoarse, spotChan: make(chan *spotEnvelope, 1)}
			c.setDedupePolicy(dedupePolicyFast)
			sp := spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
			sp.DXCellID, sp.DECellID = uint16(tc.endpoint), uint16(tc.endpoint)
			sp.DXMetadata.Continent = tc.continent
			s.deliverJob(broadcastJob{spot: sp, clients: []*Client{c}, allowFast: true})
			if (len(c.spotChan) == 1) != tc.want || c.captureHistoryFilter().matches(s, sp) != tc.want {
				t.Fatal("live/history wiring changed the ordinary Boolean result")
			}
		})
	}
}

func TestMatchingSnapshotLocationDemandAndIsolation(t *testing.T) {
	near, far, nearCoarse, farCoarse := requireDistinctPathCells(t)
	for _, expression := range []string{"MODE=CW", `COMMENT="NEARBY"`} {
		c := &Client{filter: passRuleTestFilter(t, expression), grid: "FN31"}
		captured := c.captureMatchingSnapshot()
		if captured.path.gridCell != 0 || captured.path.gridCoarseCell != 0 {
			t.Fatal("location-free capture introduced mapping work")
		}
	}
	for _, expression := range []string{"NEARBY", "NEARBY AND (PATH=INSUFFICIENT OR MODE=CW)"} {
		c := &Client{filter: passRuleTestFilter(t, expression), grid: "FN31"}
		s := &Server{}
		captured := c.captureMatchingSnapshot()
		if captured.path.gridCell != near || captured.path.gridCoarseCell != nearCoarse {
			t.Fatal("capture did not resolve both effective cells from its GRID")
		}
		if expression == "NEARBY" && (c.gridCell != 0 || c.gridCoarseCell != 0) {
			t.Fatal("NEARBY published snapshot-local geometry")
		}
		next := &Client{filter: c.filter, grid: "IO91", gridCell: far, gridCoarseCell: farCoarse}
		s.publishConfigurationState(c, filter.SettingsConfiguration{Grid: "IO91"}, next, nil, time.Time{}, false)
		sp := spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
		sp.DXCellID, sp.DECellID = uint16(near), uint16(near)
		oldMatch, _, _ := s.matchSnapshot(captured, sp)
		newMatch, _, _ := s.matchSnapshot(c.captureMatchingSnapshot(), sp)
		if !oldMatch || newMatch {
			t.Fatal("an evaluation mixed captured and newly published location")
		}
	}
	// PATH's existing lazy publication must not fill a newer GRID's cache.
	c := &Client{grid: "FN31"}
	old := c.pathSnapshot()
	c.grid = "IO91"
	resolved := c.resolvePathSnapshot(old)
	if resolved.gridCell != near || c.gridCell != 0 || c.grid != "IO91" {
		t.Fatal("stale PATH resolution published onto a newer GRID")
	}
}

func BenchmarkMatchingSnapshotLocationContext(b *testing.B) {
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(b)); err != nil {
		b.Fatal(err)
	}
	near := pathreliability.EncodeCell("FN31")
	coarse := pathreliability.EncodeCoarseCell("FN31")
	if near == 0 || coarse == 0 {
		b.Fatal("benchmark needs mapped user cells")
	}
	for _, tc := range []struct {
		name, expression string
		cached, coarse   bool
		want             bool
	}{
		{"no-nearby-missing-user-cache", "MODE=CW", false, false, true},
		{"single-cached", "NEARBY", true, false, true},
		{"repeated-cached", "NEARBY AND NEARBY", true, false, true},
		{"short-circuit", "MODE=FT8 AND NEARBY", true, false, false},
		{"missing-user-cache", "NEARBY", false, false, true},
		{"coarse", "NEARBY", true, true, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := &Client{filter: passRuleTestFilter(b, tc.expression), grid: "FN31"}
			if tc.cached {
				c.gridCell, c.gridCoarseCell = near, coarse
			}
			sp := spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
			sp.DXCellID, sp.DECellID = uint16(near), uint16(near)
			sp.DXMetadata.Grid, sp.DEMetadata.Grid = "FN31", "FN31"
			if tc.coarse {
				sp.BandNorm, sp.Band, sp.Frequency = "80m", "80m", 3520
			}
			s := &Server{}
			b.ReportAllocs()
			for b.Loop() {
				captured := c.captureMatchingSnapshot()
				matched, _, _ := s.matchSnapshot(captured, sp)
				if matched != tc.want {
					b.Fatal("benchmark did not reach its intended outcome")
				}
				if tc.name == "no-nearby-missing-user-cache" && (captured.path.gridCell != 0 || captured.path.gridCoarseCell != 0) {
					b.Fatal("location-free capture performed mapping")
				}
			}
		})
	}
}
