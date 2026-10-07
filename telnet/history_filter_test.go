package telnet

import (
	"reflect"
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
		{"band map", func(c *Client) { c.filter.BlockBands["20m"] = true }},
		{"call patterns", func(c *Client) { c.filter.DXCallsigns[0] = "W6*" }},
		{"toggle pointer", func(c *Client) { *c.filter.AllowToxic = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{callsign: "N0USER", filter: filter.NewFilter()}
			c.filter.DXCallsigns = []string{"K1*"}
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

func TestHistoryFilterSnapshotDetachesEveryRuleCollection(t *testing.T) {
	c := &Client{filter: filter.NewFilter()}
	live := reflect.ValueOf(c.filter).Elem()
	for i := range live.NumField() {
		field := live.Field(i)
		if field.Kind() == reflect.Map {
			key := reflect.New(field.Type().Key()).Elem()
			if key.Kind() == reflect.String {
				key.SetString("fixture")
			} else {
				key.SetInt(7)
			}
			field.SetMapIndex(key, reflect.ValueOf(true))
		}
		if field.Kind() == reflect.Slice {
			field.Set(reflect.ValueOf([]string{"fixture"}))
		}
	}
	snapshot := c.captureHistoryFilter()
	owned := reflect.ValueOf(snapshot.client.filter).Elem()
	for i := range live.NumField() {
		original, detached := live.Field(i), owned.Field(i)
		name := live.Type().Field(i).Name
		switch original.Kind() {
		case reflect.Map:
			count := original.Len()
			if original.Pointer() == detached.Pointer() || !reflect.DeepEqual(original.Interface(), detached.Interface()) {
				t.Errorf("%s not faithfully detached", name)
			}
			original.Clear()
			if detached.Len() != count {
				t.Errorf("%s changed with original", name)
			}
		case reflect.Slice:
			original.Index(0).SetString("changed")
			if detached.Index(0).String() != "fixture" {
				t.Errorf("%s still borrows backing storage", name)
			}
		case reflect.Pointer:
			if original.Type().Elem().Kind() == reflect.Bool && original.Pointer() == detached.Pointer() {
				t.Errorf("%s still borrows toggle", name)
			}
		}
	}
}

func TestHistoryFilterSnapshotNearbyRuntimeCells(t *testing.T) {
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t)
	c := &Client{callsign: "N0USER", filter: filter.NewFilter(), grid: "FN31", gridCell: userCell}
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
	if snapshot.client.filter.NearbySnapshot != nil {
		t.Fatal("restore-only location maps must not be retained by page")
	}
}

func TestHistoryFilterSnapshotSelfMatchPreservesHistoryException(t *testing.T) {
	c := &Client{callsign: "K1ABC-7", filter: filter.NewFilter()}
	c.filter.BlockAllBands = true
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
	c := &Client{callsign: "N0USER", filter: filter.NewFilter(), grid: "FN31", noiseClass: "QUIET"}
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
		{"matching rule", func() { c.filter.BlockBands["20m"] = true }},
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
	c := &Client{filter: filter.NewFilter(), grid: "FN31"}
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
	c := &Client{callsign: "N0USER", filter: filter.NewFilter(), grid: "FN31", gridCell: userCell, noiseClass: "QUIET"}
	c.filter.ResetModes()
	c.filter.AllPathClasses = false
	c.filter.PathClasses[filter.PathClassHigh] = true
	sp := spot.NewSpot("K1ABC", "N2ABC", 14074, "FT8")
	sp.DXCellID, sp.DXMetadata.Grid = uint16(dxCell), "IO91"
	snapshot := c.captureHistoryFilter()
	if snapshot.matches(s, sp) {
		t.Fatal("HIGH-only filter must reject missing observations")
	}
	predictor.Update(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 25, 10, now, false)
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
	c := &Client{callsign: "N0USER", filter: filter.NewFilter()}
	c.filter.DXCallsigns = []string{"K1*"}
	snapshot := c.captureHistoryFilter()
	sp := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			c.filterMu.Lock()
			c.filter.BlockBands["20m"] = i%2 == 0
			c.filter.DXCallsigns[0] = "W6*"
			*c.filter.AllowToxic = i%2 == 0
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
