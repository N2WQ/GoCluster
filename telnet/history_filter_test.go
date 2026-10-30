package telnet

import (
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestHistoryFilterSnapshotOwnsMatchingInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Client)
	}{
		{"band rule", func(c *Client) { *c.filter = *passRuleTestFilter(t, "BAND=40M") }},
		{"call pattern rule", func(c *Client) { *c.filter = *passRuleTestFilter(t, "DXCALL=W6*") }},
		{"toggle pointer", func(c *Client) { *c.filter.AllowToxic = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{callsign: "N0USER", filter: passRuleTestFilter(t, "DXCALL=K1*")}
			c.filter.AllowToxic = filter.DefaultBoolTrue.Pointer()
			sp := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
			sp.ToxicityStatus = spot.ToxicityToxic
			snapshot := c.captureHistoryFilter()
			if !snapshot.matches(nil, sp) {
				t.Fatal("initial fixture must pass")
			}
			c.filterMu.Lock()
			tc.mutate(c)
			c.filterMu.Unlock()
			if !snapshot.matches(nil, sp) {
				t.Fatal("mutating live rules altered captured predicate")
			}
			if c.captureHistoryFilter().matches(nil, sp) {
				t.Fatal("fresh predicate must observe rejecting mutation")
			}
		})
	}
}

func TestHistoryFilterSnapshotOwnsControlsAndImmutableRules(t *testing.T) {
	c := &Client{filter: passRuleTestFilter(t, "MODE=CW")}
	c.filter.IncludeBeacons, c.filter.AllowToxic = filter.DefaultBoolTrue.Pointer(), filter.DefaultBoolTrue.Pointer()
	snapshot := c.captureHistoryFilter()
	owned := snapshot.client.filter
	if owned == c.filter || owned.PreparedRules() != c.filter.PreparedRules() ||
		owned.IncludeBeacons == c.filter.IncludeBeacons || owned.AllowToxic == c.filter.AllowToxic {
		t.Fatal("snapshot must share only immutable programs, not mutable controls")
	}
	definitions := owned.PreparedRules().Definitions()
	definitions[0].Expr = "MODE=FT8"
	*c.filter.IncludeBeacons, *c.filter.AllowToxic = false, false
	if owned.PreparedRules().Definitions()[0].Expr != "MODE=CW" || !owned.BeaconsEnabled() || !owned.ToxicEnabled() {
		t.Fatal("snapshot borrowed public definitions or control pointers")
	}
}

func TestHistoryFilterSnapshotGridOwnedRuntimeCells(t *testing.T) {
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t)
	c := &Client{callsign: "N0USER", filter: passRuleTestFilter(t, "NEARBY AND MODE=CW"), grid: "FN31", gridCell: userCell, gridCoarseCell: userCoarse}
	sp := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
	sp.DXMetadata.Grid, sp.DEMetadata.Grid = "FN31", "FN31"
	sp.DXCellID, sp.DECellID = uint16(userCell), uint16(userCell)
	snapshot := c.captureHistoryFilter()
	if !snapshot.matches(nil, sp) {
		t.Fatal("captured NEARBY lacks GRID-owned location")
	}
	c.pathMu.Lock()
	c.grid = "IO91"
	c.gridCell, c.gridCoarseCell = dxCell, dxCoarse
	c.pathMu.Unlock()
	if !snapshot.matches(nil, sp) || c.captureHistoryFilter().matches(nil, sp) {
		t.Fatal("history failed to isolate captured GRID context")
	}
	if snapshot.digest == c.historyFilterDigest() {
		t.Fatal("effective geometry change excluded from digest")
	}
	if snapshot.client.filter.PreparedRules() != c.filter.PreparedRules() || snapshot.matching.path.gridCell != userCell || snapshot.matching.path.gridCoarseCell != userCoarse {
		t.Fatal("page lost immutable rules or captured cell pair")
	}
}

func TestHistoryFilterSnapshotSelfMatchPreservesHistoryException(t *testing.T) {
	c := &Client{callsign: "K1ABC-7", filter: testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RuleReject, Expr: "BAND=20M"})}
	no := false
	c.filter.AllowSelf = &no
	c.filter.AllowToxic = &no
	snapshot := c.captureHistoryFilter()
	sp := spot.NewSpot("K1ABC/P", "N2ABC", 14025, "CW")
	if !snapshot.matches(nil, sp) {
		t.Fatal("history self match must preserve band/self-toggle bypass")
	}
	sp.ToxicityStatus = spot.ToxicityToxic
	if snapshot.matches(nil, sp) {
		t.Fatal("self match must still enforce toxicity")
	}
	if snapshot.matches(nil, nil) {
		t.Fatal("nil spot must reject")
	}
}

func TestHistoryFilterDigestRelevantProjection(t *testing.T) {
	c := &Client{callsign: "N0USER", filter: unrestrictedTestFilter(), grid: "FN31", noiseClass: "QUIET"}
	initial := c.historyFilterDigest()
	no := false
	c.filter.AllowWWV, c.filter.AllowWCY, c.filter.AllowAnnounce, c.filter.AllowSelf = &no, &no, &no, &no
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "CC", Grid: "IO91", DedupePolicy: "slow", SolarSummaryMinutes: 60}
	c.solarSummaryMinutes = 60
	if got := c.historyFilterDigest(); got != initial {
		t.Fatal("unrelated preferences changed history digest")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Client)
	}{
		{"matching rule", func(c *Client) { *c.filter = *passRuleTestFilter(t, "BAND=40M") }},
		{"toxicity", func(c *Client) { c.filter.AllowToxic = &no }},
		{"grid", func(c *Client) { c.grid = "IO91" }},
		{"noise", func(c *Client) { c.noiseClass = "URBAN" }},
		{"path floor", func(c *Client) { c.pathMinObservationCount = 30 }},
		{"fine cell", func(c *Client) { c.gridCell = 101 }},
		{"coarse cell", func(c *Client) { c.gridCoarseCell = 202 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{filter: passRuleTestFilter(t, "NEARBY"), grid: "FN31", gridCell: 10, gridCoarseCell: 11, noiseClass: "QUIET"}
			before := c.historyFilterDigest()
			tc.mutate(c)
			if c.historyFilterDigest() == before {
				t.Fatal("matching input excluded from history digest")
			}
		})
	}
}

func TestHistoryFilterDigestLazyCellCacheStable(t *testing.T) {
	requireH3Mappings(t)
	for _, rule := range []string{"MODE=CW", "NEARBY", "PATH=HIGH"} {
		t.Run(rule, func(t *testing.T) {
			c := &Client{filter: passRuleTestFilter(t, rule), grid: "FN31"}
			before := c.historyFilterDigest()
			snapshot := c.captureHistoryFilter()
			c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell(c.grid), pathreliability.EncodeCoarseCell(c.grid)
			if c.historyFilterDigest() != before || snapshot.digest != before {
				t.Fatal("lazy cache fill invalidated effective history settings")
			}
			if rule != "MODE=CW" && (snapshot.matching.path.gridCell != c.gridCell || snapshot.matching.path.gridCoarseCell != c.gridCoarseCell) {
				t.Fatal("matching snapshot and digest resolved different cell pairs")
			}
		})
	}
}

func TestHistoryFilterSnapshotSeesCurrentPropagationObservations(t *testing.T) {
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t)
	cfg := pathreliability.DefaultConfig()
	cfg.MinEffectiveWeight, cfg.MinObservationCount = 0.1, 1
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	now := time.Now().UTC()
	s := &Server{pathPredictor: predictor, noiseModel: cfg.NoiseModel(), nowFn: func() time.Time { return now }}
	c := &Client{callsign: "N0USER", filter: passRuleTestFilter(t, "PATH=HIGH"), grid: "FN31", gridCell: userCell, noiseClass: "QUIET"}
	sp := spot.NewSpot("K1ABC", "N2ABC", 14074, "FT8")
	sp.DXCellID, sp.DXMetadata.Grid = uint16(dxCell), "IO91"
	snapshot := c.captureHistoryFilter()
	if snapshot.matches(s, sp) {
		t.Fatal("HIGH-only filter must reject missing observations")
	}
	predictor.Update(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 25, 10, pathreliability.ObservationTimes{ObservedAt: now, ArrivedAt: now}, false)
	if !snapshot.matches(s, sp) {
		t.Fatalf("same snapshot must use newly available HIGH observations, class=%s", s.pathClassForClient(snapshot.client, sp))
	}
	if c.historyFilterDigest() != snapshot.digest {
		t.Fatal("propagation observation changed configuration digest")
	}
	c.pathMu.Lock()
	c.pathMinObservationCount = 100
	c.pathMu.Unlock()
	if !snapshot.matches(s, sp) || c.captureHistoryFilter().matches(s, sp) {
		t.Fatal("captured path floor must remain isolated from live settings")
	}
}

func TestHistoryFilterSnapshotConcurrentLiveRuleMutation(t *testing.T) {
	first, second := passRuleTestFilter(t, "DXCALL=K1*"), passRuleTestFilter(t, "BAND=40M AND DXCALL=W6*")
	c := &Client{callsign: "N0USER", filter: first}
	states := [2]filter.Filter{*first, *second}
	snapshot := c.captureHistoryFilter()
	sp := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			c.filterMu.Lock()
			*c.filter = states[i%2]
			c.filterMu.Unlock()
		}
	})
	for range 1000 {
		if !snapshot.matches(nil, sp) {
			t.Error("live mutation changed detached predicate")
			break
		}
	}
	wg.Wait()
}
