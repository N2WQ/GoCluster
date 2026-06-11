package telnet

import (
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

func TestPathPredictionEnvelopePathFilterDisplayReusesAdmissionPrediction(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, true)
	client.filter.SetPathClass(filter.PathClassHigh, true)

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

func TestPathPredictionEnvelopePathBlockAllDoesNotCompute(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, true)
	client.filter.BlockAllPathClasses = true

	server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})

	select {
	case env := <-client.spotChan:
		t.Fatalf("REJECT PATH ALL should block spot, got envelope %+v", env)
	default:
	}
	if got := fallback.cachedCalls.Load(); got != 0 {
		t.Fatalf("REJECT PATH ALL should not compute admission prediction; cached VOACAP lookups = %d, want 0", got)
	}
	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 0 || stats.VOACAPP50CompareChecked != 0 {
		t.Fatalf("REJECT PATH ALL should not record prediction counters, got %+v", stats)
	}
}

func TestPathPredictionEnvelopeDiagPathReusesAdmissionPrediction(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, false)
	client.filter.SetPathClass(filter.PathClassHigh, true)
	client.setDiagMode(diagModePath)

	env := deliverPathPredictionReuseSpot(t, server, client, sp)
	line := server.formatSpotEnvelopeForClient(client, env)

	if got := fallback.cachedCalls.Load(); got != 1 {
		t.Fatalf("SET DIAG PATH should reuse admission prediction; cached VOACAP lookups = %d, want 1", got)
	}
	if !strings.Contains(line, "n1|") {
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
				predictor.Update(pathreliability.BucketCombined, pathreliability.CellID(3), dxCell, pathreliability.InvalidCell, pathreliability.InvalidCell, "20m", -12, 1, now, false)
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
			client.filter.SetPathClass(filter.PathClassHigh, true)
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
	client.filter.SetPathClass(filter.PathClassHigh, true)
	client.spotChan <- &spotEnvelope{spot: spot.NewSpot("FULL", "DE1AA", 14074, "FT8")}

	server.deliverJob(broadcastJob{spot: sp, clients: []*Client{client}, allowFast: true, allowMed: true, allowSlow: true})

	stats := server.PathPredictionStatsSnapshot()
	if stats.Total != 0 || stats.VOACAPP50CompareChecked != 1 {
		t.Fatalf("dropped queued spot should not record display counters, got %+v", stats)
	}
}

func TestPathPredictionEnvelopeSparseTraceRecordedOnlyAtFormatting(t *testing.T) {
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, false, true)
	client.filter.SetPathClass(filter.PathClassClosed, true)

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
		predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, pathreliability.InvalidCell, pathreliability.InvalidCell, "20m", -12, 1, now.Add(time.Duration(i)*time.Second), false, uint64(i+1))
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
		filter:     filter.NewFilter(),
		spotChan:   make(chan *spotEnvelope, 1),
		grid:       "FN31",
		gridCell:   userCell,
		noiseClass: "QUIET",
	}
	client.filter.SetMode("FT8", true)
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
		client.filter.SetPathClass(filter.PathClassHigh, true)
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
