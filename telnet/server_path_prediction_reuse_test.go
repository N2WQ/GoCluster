package telnet

import (
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

type countingPathClosedFallback struct {
	forecast      pathreliability.VOACAPCachedForecast
	ok            bool
	forecastCalls atomic.Int64
	cachedCalls   atomic.Int64
}

func (f *countingPathClosedFallback) CheckForecast(pathreliability.VOACAPClosedRequest, time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	f.forecastCalls.Add(1)
	return f.forecast, f.ok
}

func (f *countingPathClosedFallback) CheckCachedForecast(pathreliability.VOACAPClosedRequest, time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	f.cachedCalls.Add(1)
	return f.forecast, f.ok
}

func TestPathPredictionEnvelopeNoPathFilterComputesOnlyAtFormatting(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, true)

	env := deliverPathPredictionReuseSpot(t, server, client, sp)
	if env.pathPrediction != nil {
		t.Fatalf("unfiltered admission must not attach path prediction")
	}
	if got := fallback.cachedCalls.Load(); got != 0 {
		t.Fatalf("unfiltered admission cached VOACAP lookups = %d, want 0", got)
	}

	server.formatSpotEnvelopeForClient(client, env)

	if got := fallback.cachedCalls.Load(); got != 1 {
		t.Fatalf("formatting cached VOACAP lookups = %d, want 1", got)
	}
	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 1 || stats.VOACAPP50CompareChecked != 1 {
		t.Fatalf("display should record one prediction and one compare, got %+v", stats)
	}
}

func TestCapturedPathStateSurvivesGridChange(t *testing.T) {
	requireDistinctPathCells(t)
	server, client, sp, _ := newPathPredictionReuseFixture(t, 1, true, false)
	oldCell := pathreliability.EncodeCell("FN31")
	newCell := pathreliability.EncodeCell("EM10")
	if oldCell == pathreliability.InvalidCell || newCell == pathreliability.InvalidCell || oldCell == newCell {
		t.Fatal("fixture needs distinct mapped grids")
	}
	client.gridCell = pathreliability.InvalidCell
	client.pathMinObservationCount = 3
	captured := client.pathSnapshot()

	client.pathMu.Lock()
	client.grid, client.gridCell = "EM10", newCell
	client.noiseClass, client.pathMinObservationCount = "URBAN", 100
	client.gridDerived = true
	client.pathMu.Unlock()

	resolved := client.resolvePathSnapshot(captured)
	if resolved.grid != "FN31" || resolved.gridCell != oldCell {
		t.Fatalf("resolved state mixed configurations: %+v", resolved)
	}
	prediction, ok := server.computePathPredictionForState(captured, sp)
	if !ok || prediction.grid != "FN31" || prediction.userCell != oldCell ||
		prediction.noiseClass != "QUIET" || prediction.minObservation != 3 || prediction.userDerived {
		t.Fatalf("prediction did not retain captured inputs: ok=%v prediction=%+v", ok, prediction)
	}
	current := client.pathSnapshot()
	if current.grid != "EM10" || current.gridCell != newCell || current.pathMinObservationCount != 100 {
		t.Fatalf("old cache publication overwrote current settings: %+v", current)
	}
}

func TestPathSnapshotCachePublication(t *testing.T) {
	requireDistinctPathCells(t)
	oldCell := pathreliability.EncodeCell("FN31")
	newCell := pathreliability.EncodeCell("EM10")
	for _, test := range []struct {
		name        string
		currentGrid string
		currentCell pathreliability.CellID
		wantCell    pathreliability.CellID
	}{
		{"unchanged", "FN31", pathreliability.InvalidCell, oldCell},
		{"changed-unresolved", "EM10", pathreliability.InvalidCell, pathreliability.InvalidCell},
		{"changed-resolved", "EM10", newCell, newCell},
		{"already-filled", "FN31", newCell, newCell},
	} {
		t.Run(test.name, func(t *testing.T) {
			captured := pathState{grid: "FN31", gridCell: pathreliability.InvalidCell}
			client := &Client{grid: test.currentGrid, gridCell: test.currentCell}
			resolved := client.resolvePathSnapshot(captured)
			if resolved.gridCell != oldCell || client.pathSnapshot().gridCell != test.wantCell {
				t.Fatalf("resolved=%d cache=%d; want resolved=%d cache=%d",
					resolved.gridCell, client.pathSnapshot().gridCell, oldCell, test.wantCell)
			}
		})
	}
}

func TestPathSnapshotCacheWithoutDX(t *testing.T) {
	requireDistinctPathCells(t)
	server, client, sp, _ := newPathPredictionReuseFixture(t, 1, true, false)
	client.gridCell = pathreliability.InvalidCell
	sp.DXCellID, sp.DXMetadata.Grid = 0, ""
	if _, ok := server.computePathPredictionForClient(client, sp); ok {
		t.Fatal("missing DX location produced a prediction")
	}
	if got, want := client.pathSnapshot().gridCell, pathreliability.EncodeCell("FN31"); got != want {
		t.Fatalf("unsuccessful prediction lost user-cell cache fill: got %d want %d", got, want)
	}
}

func TestPathSnapshotConcurrentGridChanges(t *testing.T) {
	requireDistinctPathCells(t)
	client := &Client{grid: "FN31", gridCell: pathreliability.InvalidCell}
	done := make(chan struct{})
	go func() {
		defer close(done)
		grids := [...]string{"FN31", "EM10"}
		for i := range 200 {
			client.pathMu.Lock()
			client.grid, client.gridCell = grids[i%len(grids)], pathreliability.InvalidCell
			client.pathMu.Unlock()
			runtime.Gosched()
		}
	}()
	defer func() { <-done }()
	for range 200 {
		captured := client.pathSnapshot()
		resolved := client.resolvePathSnapshot(captured)
		want := pathreliability.EncodeCell(captured.grid)
		if want == pathreliability.InvalidCell || resolved.grid != captured.grid || resolved.gridCell != want {
			t.Fatalf("snapshot mixed grids: captured=%+v resolved=%+v wantCell=%d", captured, resolved, want)
		}
		runtime.Gosched()
	}
}

func TestPathPredictionEnvelopePathFilterDisplayReusesAdmissionPrediction(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, true)
	client.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=HIGH"})

	env := deliverPathPredictionReuseSpot(t, server, client, sp)
	if env.pathPrediction == nil {
		t.Fatalf("path-filtered admission should attach path prediction")
	}
	if got := fallback.cachedCalls.Load(); got != 1 {
		t.Fatalf("admission cached VOACAP lookups = %d, want 1", got)
	}

	server.formatSpotEnvelopeForClient(client, env)

	if got := fallback.cachedCalls.Load(); got != 1 {
		t.Fatalf("formatting should reuse admission prediction; cached VOACAP lookups = %d, want 1", got)
	}
	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 1 || stats.VOACAPP50CompareChecked != 1 {
		t.Fatalf("reuse should record one display prediction and one admission compare, got %+v", stats)
	}
}

func TestPathPredictionEnvelopeNonPathRejectionDoesNotCompute(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, true)
	client.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RuleReject, Expr: "BAND=20m"})

	server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})

	select {
	case env := <-client.spotChan:
		t.Fatalf("matching REJECT should block spot, got envelope %+v", env)
	default:
	}
	if got := fallback.cachedCalls.Load(); got != 0 {
		t.Fatalf("non-PATH rejection should not compute admission prediction; cached VOACAP lookups = %d, want 0", got)
	}
	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 0 || stats.VOACAPP50CompareChecked != 0 {
		t.Fatalf("non-PATH rejection should not record prediction counters, got %+v", stats)
	}
}

func TestPathPredictionEnvelopeDiagPathReusesAdmissionPrediction(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, false)
	client.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=HIGH"})
	client.setDiagMode(diagModePath)

	env := deliverPathPredictionReuseSpot(t, server, client, sp)
	line := server.formatSpotEnvelopeForClient(client, env)

	if got := fallback.cachedCalls.Load(); got != 1 {
		t.Fatalf("SET DIAG PATH should reuse admission prediction; cached VOACAP lookups = %d, want 1", got)
	}
	if !strings.Contains(line, "n1/u1|") {
		t.Fatalf("expected path diagnostic token from cached prediction, got %q", line)
	}
	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 1 || stats.VOACAPP50CompareChecked != 1 {
		t.Fatalf("diag path reuse should record one display prediction and one compare, got %+v", stats)
	}
}

func TestPathPredictionEnvelopeRecomputesWhenClientStateChanges(t *testing.T) {
	tests := []struct {
		name    string
		samples int
		mutate  func(*Client)
		seedAlt func(*pathreliability.Predictor, time.Time, pathreliability.CellID, pathreliability.CellID)
	}{
		{
			name:    "grid",
			samples: 1,
			mutate: func(client *Client) {
				client.pathMu.Lock()
				client.grid = "FN33"
				client.gridCell = pathreliability.CellID(3)
				client.pathMu.Unlock()
			},
			seedAlt: func(predictor *pathreliability.Predictor, now time.Time, _, dxCell pathreliability.CellID) {
				predictor.Update(pathreliability.BucketCombined, pathreliability.CellID(3), dxCell, pathreliability.InvalidCell, pathreliability.InvalidCell, "20m", -12, 1, pathreliability.ObservationTimes{ObservedAt: now, ArrivedAt: now}, false)
			},
		},
		{
			name:    "noise",
			samples: 1,
			mutate: func(client *Client) {
				client.pathMu.Lock()
				client.noiseClass = "URBAN"
				client.pathMu.Unlock()
			},
		},
		{
			name:    "sample floor",
			samples: 2,
			mutate: func(client *Client) {
				client.pathMu.Lock()
				client.pathMinObservationCount = 2
				client.pathMu.Unlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, client, sp, fallback := newPathPredictionReuseFixture(t, tt.samples, true, true)
			client.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=HIGH"})
			if tt.seedAlt != nil {
				tt.seedAlt(server.pathPredictor, server.now(), pathreliability.CellID(1), pathreliability.CellID(2))
			}

			env := deliverPathPredictionReuseSpot(t, server, client, sp)
			tt.mutate(client)
			server.formatSpotEnvelopeForClient(client, env)

			if got := fallback.cachedCalls.Load(); got != 2 {
				t.Fatalf("state change should force recompute; cached VOACAP lookups = %d, want 2", got)
			}
		})
	}
}

func TestPathPredictionEnvelopeDropDoesNotRecordDisplayCounters(t *testing.T) {
	server, client, sp, _ := newPathPredictionReuseFixture(t, 1, true, true)
	client.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=HIGH"})
	client.spotChan <- &spotEnvelope{spot: spot.NewSpot("FULL", "DE1AA", 14074, "FT8")}

	server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})

	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 0 || stats.VOACAPP50CompareChecked != 1 {
		t.Fatalf("dropped queued spot should not record display counters, got %+v", stats)
	}
}

func TestPathPredictionEnvelopeSparseTraceRecordedOnlyAtFormatting(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, false, true)
	client.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=CLOSED"})

	env := deliverPathPredictionReuseSpot(t, server, client, sp)
	admissionStats := server.PathPredictionStatsSnapshot()
	if admissionStats.Total != 0 || admissionStats.SparseP50VOACAP.Total != 0 {
		t.Fatalf("admission should not record display or sparse trace counters, got %+v", admissionStats)
	}

	server.formatSpotEnvelopeForClient(client, env)

	if got := fallback.forecastCalls.Load(); got != 1 {
		t.Fatalf("formatting should reuse sparse admission prediction; fallback forecast calls = %d, want 1", got)
	}
	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 1 || stats.VOACAPClosed != 1 || stats.SparseP50VOACAP.Total != 1 {
		t.Fatalf("formatting should record one closed display prediction and one sparse trace, got %+v", stats)
	}
}

func BenchmarkPathPredictionEnvelopeNoFilterDisplay(b *testing.B) {
	server, client, sp := newPathPredictionReuseBenchmarkFixture(b, 1, false, true)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})
		env := receivePathPredictionReuseEnvelope(b, client)
		_ = server.formatSpotEnvelopeForClient(client, env)
	}
}

func BenchmarkPathPredictionEnvelopePathFilterDisplay(b *testing.B) {
	server, client, sp := newPathPredictionReuseBenchmarkFixture(b, 1, true, true)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})
		env := receivePathPredictionReuseEnvelope(b, client)
		_ = server.formatSpotEnvelopeForClient(client, env)
	}
}

func BenchmarkPathPredictionEnvelopePathFilterNoDisplay(b *testing.B) {
	server, client, sp := newPathPredictionReuseBenchmarkFixture(b, 1, true, false)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})
		env := receivePathPredictionReuseEnvelope(b, client)
		_ = server.formatSpotEnvelopeForClient(client, env)
	}
}

func newPathPredictionReuseFixture(t testing.TB, samples int, sufficient bool, pathDisplay bool) (*Server, *Client, *spot.Spot, *countingPathClosedFallback) {
	t.Helper()
	now := time.Date(2026, time.June, 11, 18, 0, 0, 0, time.UTC)
	cfg := pathreliability.DefaultConfig()
	cfg.MinObservationCount = 1
	cfg.MinEffectiveWeight = 0.1
	cfg.GlyphSymbols.Closed = "!"
	if !sufficient {
		cfg.MinObservationCount = 2
	}
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	userCell := pathreliability.CellID(1)
	dxCell := pathreliability.CellID(2)
	for i := 0; i < samples; i++ {
		predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, pathreliability.InvalidCell, pathreliability.InvalidCell, "20m", -12, 1, pathreliability.ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, uint64(i+1))
	}
	fallback := &countingPathClosedFallback{
		forecast: pathreliability.VOACAPCachedForecast{
			Record: pathreliability.VOACAPHourlyForecast{FT8SNRDB: -34, HourUTC: 18, FrequencyMHz: 14.1},
			SSN:    112,
		},
		ok: true,
	}
	server := &Server{
		pathPredictor:      predictor,
		pathDisplay:        pathDisplay,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return now },
		pathClosedFallback: fallback,
	}
	client := &Client{
		filter:     testRuleFilter(t),
		spotChan:   make(chan *spotEnvelope, 1),
		grid:       "FN31",
		gridCell:   userCell,
		noiseClass: "QUIET",
	}
	sp := spot.NewSpot("DX1AA", "DE1AA", 14074, "FT8")
	sp.BandNorm = "20m"
	sp.DXCellID = uint16(dxCell)
	sp.DXMetadata.Grid = "FN32"
	if !client.filter.MatchesWithPath(sp, filter.PathClassHigh) {
		t.Fatalf("fixture filter does not pass synthetic spot")
	}
	return server, client, sp, fallback
}

func newPathPredictionReuseBenchmarkFixture(b *testing.B, samples int, pathFilter bool, pathDisplay bool) (*Server, *Client, *spot.Spot) {
	b.Helper()
	server, client, sp, _ := newPathPredictionReuseFixture(b, samples, true, pathDisplay)
	if pathFilter {
		client.filter = testRuleFilter(b, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND PATH=HIGH"})
	}
	return server, client, sp
}

func deliverPathPredictionReuseSpot(t *testing.T, server *Server, client *Client, sp *spot.Spot) *spotEnvelope {
	t.Helper()
	server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})
	return receivePathPredictionReuseEnvelope(t, client)
}

func receivePathPredictionReuseEnvelope(t testing.TB, client *Client) *spotEnvelope {
	t.Helper()
	select {
	case env := <-client.spotChan:
		return env
	default:
		t.Fatalf("expected one queued spot")
		return nil
	}
}
