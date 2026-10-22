package telnet

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/solarweather"
	"dxcluster/spot"
)

type timestampProvenanceFallback struct {
	forecast pathreliability.VOACAPCachedForecast
	calls    int
	lastAt   time.Time
}

func (f *timestampProvenanceFallback) CheckForecast(_ pathreliability.VOACAPClosedRequest, at time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	f.calls++
	f.lastAt = at
	return f.forecast, true
}

// This fixture drives the actual Store and Predictor through the telnet
// consumer. Arrival/reference is deliberately later than the request, so
// substituting a request or forecast clock cannot satisfy provenance checks.
func timestampProvenanceFixture(t *testing.T, cfg pathreliability.Config, requested, reference time.Time, band, dxGrid string, usable, beacon bool) (*Server, *Client, *spot.Spot) {
	t.Helper()
	requireH3Mappings(t)
	p := pathreliability.NewPredictor(cfg, []string{band})
	userCell, dxCell := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCell(dxGrid)
	userCoarse, dxCoarse := pathreliability.EncodeCoarseCell("FN31"), pathreliability.EncodeCoarseCell(dxGrid)
	observed := reference
	if !usable {
		observed = reference.Add(-160 * time.Second)
	}
	admission := p.UpdateWithReceiverHash(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, band, -19, 1,
		pathreliability.ObservationTimes{ObservedAt: observed, ArrivedAt: reference}, false, 123)
	if !admission.Accepted() {
		t.Fatalf("fixture report rejected: %+v", admission)
	}
	s := &Server{pathPredictor: p, pathDisplay: true, noiseModel: cfg.NoiseModel(), nowFn: func() time.Time { return requested }}
	c := &Client{filter: passRuleTestFilter(t, "MODE=FT8"), spotChan: make(chan *spotEnvelope, 1), grid: "FN31", gridCell: userCell, noiseClass: "QUIET"}
	freq := 14074.0
	if band == "160m" {
		freq = 1840
	}
	sp := spot.NewSpot("DX1AA", "DE1AA", freq, "FT8")
	sp.BandNorm, sp.DXCellID, sp.DXMetadata.Grid, sp.IsBeacon = band, uint16(dxCell), dxGrid, beacon
	sp.Time = time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)
	return s, c, sp
}

func timestampProvenanceConfig(band string) pathreliability.Config {
	cfg := pathreliability.DefaultConfig()
	cfg.ReceiverContributionMode = pathreliability.ReceiverContributionOff
	cfg.BandHalfLifeSec = map[string]int{band: 1}
	cfg.StaleAfterHalfLifeMultiplier = 1000
	cfg.MaxPredictionAgeHalfLifeMultiplier = 0
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = 4, 4
	cfg.MinEffectiveWeight = .01
	cfg.VOACAPFallback.ReliabilityGatedOpenEnabled = true
	cfg.VOACAPFallback.ReliabilitySparseUpgradeEnabled = true
	cfg.Native160Fallback.Enabled, cfg.Native160Fallback.DisplayEnabled = true, true
	return cfg
}

func TestTimestampProvenanceConnectedFallbacks(t *testing.T) {
	requested := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
	reference := requested.Add(30 * time.Second)
	for _, beacon := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			band      string
			usable    bool
			forecast  int
			want      pathreliability.PredictionSource
			wantP50   bool
			wantAvail bool
		}{
			{name: "closed without observed distribution", band: "20m", forecast: -34, want: pathreliability.SourceVOACAPClosed, wantP50: true},
			{name: "closed with sparse observed distribution", band: "20m", usable: true, forecast: -34, want: pathreliability.SourceVOACAPClosed, wantP50: true, wantAvail: true},
			{name: "open without observed distribution", band: "20m", forecast: -19, want: pathreliability.SourceVOACAPOpen},
			{name: "aligned", band: "20m", usable: true, forecast: -19, want: pathreliability.SourceVOACAPAligned, wantP50: true, wantAvail: true},
			{name: "sparse upgraded", band: "20m", usable: true, forecast: -15, want: pathreliability.SourceVOACAPSparseUpgrade, wantP50: true, wantAvail: true},
			{name: "native 160", band: "160m", want: pathreliability.SourceNative160},
			{name: "native 160 with sparse observed distribution", band: "160m", usable: true, want: pathreliability.SourceNative160, wantP50: true, wantAvail: true},
		} {
			t.Run(fmt.Sprintf("%s/beacon=%t", tc.name, beacon), func(t *testing.T) {
				cfg := timestampProvenanceConfig(tc.band)
				dxGrid := "IO91"
				if tc.band == "160m" {
					dxGrid = "FN20"
				}
				s, c, sp := timestampProvenanceFixture(t, cfg, requested, reference, tc.band, dxGrid, tc.usable, beacon)
				if tc.want != pathreliability.SourceNative160 {
					forecast := forecastWithReqSNRReliability(tc.forecast, .95)
					forecast.AgeSec = 42
					forecast.Record.HourUTC = requested.Hour()
					s.pathClosedFallback = &timestampProvenanceFallback{forecast: forecast}
				}
				prediction, ok := s.computePathPredictionForClient(c, sp)
				if !ok {
					t.Fatal("connected prediction unavailable")
				}
				got := prediction.result
				if got.Source != tc.want || got.HasP50 != tc.wantP50 || got.BeaconRX != beacon {
					t.Fatalf("fallback changed source/p50/direction: %+v", got)
				}
				if !got.HasObservedSamplingTime || got.ObservedSamplingUnix != reference.Unix() || got.ObservedDistributionAvailable != tc.wantAvail {
					t.Fatalf("fallback lost the observed sampling contract: %+v", got)
				}
				if !prediction.at.Equal(requested) || !sp.Time.Equal(time.Date(2025, time.January, 7, 4, 9, 0, 0, time.UTC)) {
					t.Fatalf("observed metadata replaced request/source clocks: request=%v spot=%v", prediction.at, sp.Time)
				}
				if tc.want == pathreliability.SourceVOACAPClosed || tc.want == pathreliability.SourceVOACAPOpen {
					if got.AgeSec != 42 || got.VOACAPAgeSec != 42 || got.VOACAPHourUTC != 4 {
						t.Fatalf("observed metadata replaced forecast diagnostics: %+v", got)
					}
				}
			})
		}
	}
}

func TestTimestampProvenanceEpochSurvivesClosedReplacement(t *testing.T) {
	requested, reference := time.Unix(-30, 0), time.Unix(0, 0)
	s, c, sp := timestampProvenanceFixture(t, timestampProvenanceConfig("20m"), requested, reference, "20m", "IO91", false, false)
	s.pathClosedFallback = &timestampProvenanceFallback{forecast: forecastWithReqSNRReliability(-34, .95)}
	prediction, ok := s.computePathPredictionForClient(c, sp)
	if !ok || prediction.result.Source != pathreliability.SourceVOACAPClosed || !prediction.result.HasP50 ||
		!prediction.result.HasObservedSamplingTime || prediction.result.ObservedSamplingUnix != 0 || prediction.result.ObservedDistributionAvailable {
		t.Fatalf("synthetic closed p50 erased epoch provenance or invented observed availability: %+v", prediction)
	}
}

func TestTimestampProvenanceAdmissionReuseAndFixedComment(t *testing.T) {
	for _, tc := range []struct {
		name     string
		elapsed  time.Duration
		calls    int
		rollover bool
	}{
		{name: "reused", elapsed: 500 * time.Millisecond, calls: 1},
		{name: "exact reuse boundary", elapsed: time.Second, calls: 1},
		{name: "expired request", elapsed: time.Second + time.Nanosecond, calls: 2},
		{name: "backward request clock", elapsed: -time.Nanosecond, calls: 2},
		{name: "UTC hour rollover", elapsed: 500 * time.Millisecond, calls: 2, rollover: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requested := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
			if tc.rollover {
				requested = time.Date(2026, time.June, 18, 23, 59, 59, 500_000_000, time.UTC)
			}
			reference := requested.Add(30 * time.Second)
			s, c, sp := timestampProvenanceFixture(t, timestampProvenanceConfig("20m"), requested, reference, "20m", "IO91", false, false)
			forecast := forecastWithReqSNRReliability(-34, .95)
			forecast.Record.HourUTC = requested.Hour()
			fallback := &timestampProvenanceFallback{forecast: forecast}
			s.pathClosedFallback = fallback
			c.filter = passRuleTestFilter(t, "MODE=FT8 AND PATH=CLOSED")
			c.setDiagMode(diagModePath)
			env := deliverPathPredictionReuseSpot(t, s, c, sp)
			if env.pathPrediction == nil || !env.pathPrediction.at.Equal(requested) || env.pathPrediction.result.ObservedSamplingUnix != reference.Unix() {
				t.Fatalf("admission failed to preserve separate request/observed clocks: %+v", env.pathPrediction)
			}
			now := requested.Add(tc.elapsed)
			s.nowFn = func() time.Time { return now }
			line := s.formatSpotEnvelopeForClient(c, env)
			if fallback.calls != tc.calls {
				t.Fatalf("reuse depended on observed time: calls=%d want=%d", fallback.calls, tc.calls)
			}
			wantForecastTime := requested
			if tc.calls == 2 {
				wantForecastTime = now
			}
			if !fallback.lastAt.Equal(wantForecastTime) {
				t.Fatalf("fallback used observed instead of request/recompute time: %v want %v", fallback.lastAt, wantForecastTime)
			}
			wantTag := fmt.Sprintf("vcap|-34|h%02d|s112", requested.Hour())
			if len(strings.TrimRight(line, "\r\n")) != 78 || !strings.Contains(line, wantTag) || !strings.HasSuffix(strings.TrimSpace(line), "0409Z") {
				t.Fatalf("sampling metadata changed fixed comment/source-time layout: %q", line)
			}
			if stats := s.PathPredictionStatsSnapshot(); stats.Total != 1 || stats.VOACAPClosed != 1 {
				t.Fatalf("reuse/recompute changed final display counting: %+v", stats)
			}
		})
	}
}

func TestTimestampProvenanceStaleRawDiagnosticsDoNotChangeFallback(t *testing.T) {
	for _, beacon := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			forecast int
			rel      float64
			want     pathreliability.PredictionSource
		}{
			{name: "closed", forecast: -34, rel: .95, want: pathreliability.SourceVOACAPClosed},
			{name: "open", forecast: -19, rel: .95, want: pathreliability.SourceVOACAPOpen},
			{name: "REL blocks", forecast: -19, rel: .1, want: pathreliability.SourceInsufficient},
		} {
			t.Run(fmt.Sprintf("%s/beacon=%t", tc.name, beacon), func(t *testing.T) {
				cfg := timestampProvenanceConfig("20m")
				cfg.ReceiverContributionMode = pathreliability.ReceiverContributionEnforce
				cfg.BandHalfLifeSec["20m"], cfg.MaxPredictionAgeHalfLifeMultiplier = 10, 1
				arrived := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
				requested := arrived.Add(11 * time.Second)
				s, c, sp := timestampProvenanceFixture(t, cfg, requested, arrived, "20m", "IO91", true, beacon)
				for i := 1; i < 19; i++ {
					s.pathPredictor.UpdateWithReceiverHash(pathreliability.BucketCombined, c.gridCell, pathreliability.CellID(sp.DXCellID),
						pathreliability.EncodeCoarseCell(c.grid), pathreliability.EncodeCoarseCell(sp.DXMetadata.Grid), "20m", -19, 1,
						pathreliability.ObservationTimes{ObservedAt: arrived, ArrivedAt: arrived}, false, 123)
				}
				base, ok := s.computePathPredictionForClient(c, sp)
				if !ok || base.result.Source != pathreliability.SourceInsufficient || base.result.InsufficientReason != pathreliability.InsufficientStale ||
					base.result.HasP50 || base.result.Weight != 0 || base.result.RawCount != 19 || base.result.ObservationCount != 19 || base.result.Count != 2 || base.result.CappedCount != 2 {
					t.Fatalf("stale diagnostics or eligibility changed: %+v", base.result)
				}
				forecast := forecastWithReqSNRReliability(tc.forecast, tc.rel)
				s.pathClosedFallback = &timestampProvenanceFallback{forecast: forecast}
				prediction, ok := s.computePathPredictionForClient(c, sp)
				if !ok || prediction.result.Source != tc.want || !prediction.result.HasObservedSamplingTime ||
					prediction.result.ObservedSamplingUnix != requested.Unix() || prediction.result.ObservedDistributionAvailable || !prediction.sparseVOACAPTrace.NoP50 || prediction.sparseVOACAPTrace.VeryLowCount {
					t.Fatalf("restored stale raw counts changed fallback/no-p50 routing: %+v", prediction)
				}
				stats := s.PathPredictionStatsSnapshot()
				if tc.want == pathreliability.SourceVOACAPClosed && (stats.VOACAPFallbackClosedNoP50 != 1 || stats.VOACAPFallbackClosedSparseP50 != 0) {
					t.Fatalf("stale raw counts became sparse p50 stage evidence: %+v", stats)
				}
				if tc.want == pathreliability.SourceInsufficient {
					wantTag := "n19/u2.3|stale|vrel"
					if beacon {
						wantTag = "brx|" + wantTag
					}
					if diagPathTag(prediction, true) != wantTag {
						t.Fatalf("stale raw count diagnostic changed layout: %q want %q", diagPathTag(prediction, true), wantTag)
					}
				}
			})
		}
	}
}

func TestTimestampProvenanceSolarUsesRequestClock(t *testing.T) {
	requested := time.Date(2026, time.June, 18, 16, 0, 0, 0, time.UTC)
	reference := requested.Add(12 * time.Hour)
	s, c, sp := timestampProvenanceFixture(t, timestampProvenanceConfig("20m"), requested, reference, "20m", "FN20", true, false)
	s.pathClosedFallback = &timestampProvenanceFallback{forecast: forecastWithReqSNRReliability(-19, .95)}
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/goes" {
			fmt.Fprintf(w, `[{"time_tag":%q,"flux":0.0001,"energy":"0.1-0.8nm"}]`, requested.Format(time.RFC3339))
			return
		}
		fmt.Fprintf(w, `[["time_tag","Kp"],[%q,"0"]]`, requested.Format("2006-01-02 15:04:05.000"))
	}))
	defer feed.Close()
	cfg := solarweather.DefaultConfig()
	cfg.Enabled, cfg.FetchIntervalSec, cfg.RequestTimeoutSec = true, 3600, 1
	cfg.GOES.URL, cfg.Kp.URL = feed.URL+"/goes", feed.URL+"/kp"
	cfg.GOES.MaxAgeSec, cfg.Kp.MaxAgeSec = 48*60*60, 48*60*60
	cfg.RLevels = []solarweather.RLevel{{Name: "R3", MinFluxWM2: .0001, HoldMinutes: 24 * 60, Bands: []string{"20m"}}}
	s.solarWeather = solarweather.NewManager(cfg, log.New(io.Discard, "", 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.solarWeather.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot := s.solarWeather.Snapshot(requested)
		if snapshot.ActiveRLevel >= 0 && !snapshot.KpTime.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("local fixture feed did not establish the R event")
		}
		time.Sleep(time.Millisecond)
	}
	prediction, ok := s.computePathPredictionForClient(c, sp)
	if !ok || !prediction.at.Equal(requested) || prediction.result.ObservedSamplingUnix != reference.Unix() {
		t.Fatalf("solar fixture did not retain distinct clocks: %+v", prediction)
	}
	if got := s.pathGlyphFromPrediction(prediction); got != cfg.Glyphs.R {
		t.Fatalf("solar glyph used observed night instead of requested day: %q", got)
	}
	input := solarweather.PathInput{UserGrid: "FN31", DXGrid: "FN20", UserCell: c.gridCell, DXCell: pathreliability.CellID(sp.DXCellID), Band: "20m"}
	if glyph, kind := s.solarWeather.OverrideGlyph(reference, input); glyph != "" || kind != solarweather.OverrideNone {
		t.Fatalf("fixture lacks discriminating daylight/night clocks: glyph=%q kind=%v", glyph, kind)
	}
}
