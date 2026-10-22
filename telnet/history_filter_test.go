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

func TestHistoryFilterSnapshotNearbyRuntimeCells(t *testing.T) {
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t)
	c := &Client{callsign: "N0USER", filter: passRuleTestFilter(t, "MODE=CW"), grid: "FN31", gridCell: userCell}
	if err := c.filter.EnableNearby(userCell, userCoarse); err != nil {
		t.Fatal(err)
	}
	sp := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
	sp.DXMetadata.Grid, sp.DEMetadata.Grid = "FN31", "IO91"
	sp.DXCellID = uint16(userCell)
	snapshot := c.captureHistoryFilter()
	if !snapshot.matches(nil, sp) {
		t.Fatal("captured NEARBY must retain effective user cell")
	}
	c.filterMu.Lock()
	c.filter.UpdateNearbyUserCells(dxCell, dxCoarse)
	c.filterMu.Unlock()
	// Both ends must differ from the new user's cell to make rejection decisive.
	sp.DEMetadata.Grid = "FN31"
	sp.DECellID = uint16(userCell)
	if !snapshot.matches(nil, sp) || c.captureHistoryFilter().matches(nil, sp) {
		t.Fatal("NEARBY snapshot did not isolate runtime cells")
	}
	if snapshot.digest == c.historyFilterDigest() {
		t.Fatal("runtime NEARBY changes must affect digest")
	}
	if snapshot.client.filter.PreparedRules() != c.filter.PreparedRules() ||
		snapshot.client.filter.NearbyUserFine != userCell || snapshot.client.filter.NearbyUserCoarse != userCoarse {
		t.Fatal("page lost immutable rules or captured effective cells")
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
	c.dialect = DialectName("CC")
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "CC", Grid: "IO91", DedupePolicy: "slow", SolarSummaryMinutes: 60}
	c.solarSummaryMinutes = 60
	if got := c.historyFilterDigest(); got != initial {
		t.Fatal("unrelated preferences changed history digest")
	}
	for _, tc := range []struct {
		name   string
		mutate func()
	}{
		{"matching rule", func() { *c.filter = *passRuleTestFilter(t, "BAND=40M") }},
		{"toxicity", func() { c.filter.AllowToxic = &no }},
		{"grid", func() { c.grid = "IO91" }},
		{"noise", func() { c.noiseClass = "URBAN" }},
		{"path floor", func() { c.pathMinObservationCount = 30 }},
		{"nearby fine cell", func() { c.filter.NearbyUserFine = 1 }},
		{"nearby coarse cell", func() { c.filter.NearbyUserCoarse = 1 }},
	} {
		before := c.historyFilterDigest()
		tc.mutate()
		if c.historyFilterDigest() == before {
			t.Errorf("%s did not affect history digest", tc.name)
		}
	}
}

func TestHistoryFilterDigestLazyCellCacheStable(t *testing.T) {
	requireH3Mappings(t)
	c := &Client{filter: unrestrictedTestFilter(), grid: "FN31"}
	before := c.historyFilterDigest()
	snapshot := c.captureHistoryFilter()
	c.gridCell = pathreliability.EncodeCell(c.grid)
	if c.historyFilterDigest() != before || snapshot.digest != before || snapshot.client.gridCell != c.gridCell {
		t.Fatal("lazy cache fill changed effective history settings")
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
