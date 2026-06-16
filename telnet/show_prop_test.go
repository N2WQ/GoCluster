package telnet

import (
	"math"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/pathreliability"
)

func TestParseShowPropCommand(t *testing.T) {
	tests := []struct {
		line    string
		handled bool
		want    showPropCommand
		wantErr bool
	}{
		{line: "SHOW DX", handled: false},
		{line: "SHOW PROP", handled: true, wantErr: true},
		{line: "SHOW PROP IT9 20 CW", handled: true, want: showPropCommand{target: "IT9", band: "20m", mode: "CW"}},
		{line: "SH PROP IT9 CW 20m", handled: true, want: showPropCommand{target: "IT9", band: "20m", mode: "CW"}},
		{line: "SHOW/PROP FN32 FT8", handled: true, want: showPropCommand{target: "FN32", mode: "FT8"}},
		{line: "SHOW PROP IT9 20m 40m", handled: true, wantErr: true},
	}
	for _, tc := range tests {
		got, handled, errMsg := parseShowPropCommand(tc.line)
		if handled != tc.handled {
			t.Fatalf("parseShowPropCommand(%q) handled=%v want %v", tc.line, handled, tc.handled)
		}
		if !handled {
			continue
		}
		if tc.wantErr {
			if errMsg == "" {
				t.Fatalf("parseShowPropCommand(%q) expected usage error", tc.line)
			}
			continue
		}
		if errMsg != "" {
			t.Fatalf("parseShowPropCommand(%q) unexpected error %q", tc.line, errMsg)
		}
		if got != tc.want {
			t.Fatalf("parseShowPropCommand(%q) = %+v, want %+v", tc.line, got, tc.want)
		}
	}
}

func FuzzParseShowPropCommand(f *testing.F) {
	for _, seed := range []string{
		"SHOW PROP IT9 20m FT8",
		"SH PROP FN32 CW",
		"SHOW/PROP FN32",
		"SHOW PROP",
		"SHOW PROP IT9 20m 40m FT8",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		_, _, _ = parseShowPropCommand(line)
	})
}

func TestHandleShowPropSingleBandExplicitGrid(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{14.1}
	cfg.VOACAPFallback.ForecastHours = 8
	now := time.Date(2026, time.June, 8, 18, 10, 0, 0, time.UTC)
	noisePenalty := cfg.NoiseModel().Penalty("SUBURBAN")
	fallback := &fakeShowPropFallback{
		cfg: cfg,
		windows: map[string]pathreliability.VOACAPCachedForecastWindow{
			"20m": {
				Records: []pathreliability.VOACAPCachedForecast{
					testShowPropForecast(cfg, 18, -10, -20, noisePenalty),
					testShowPropForecast(cfg, 19, -20, -25, noisePenalty),
					testShowPropForecast(cfg, 20, -35, -30, noisePenalty),
				},
			},
		},
	}
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"20m"}),
		pathClosedFallback: fallback,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return now },
	}
	client := testShowPropClient("FN31", "SUBURBAN")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP FN32 20m FT8")
	if !handled {
		t.Fatalf("SHOW PROP not handled")
	}
	for _, want := range []string{
		"PROP FN31 -> FN32 target=FN32 source=grid mode=FT8 band=20m noise=SUBURBAN ssn=112 hours=8",
		"UTC  EFF  RX  TX  REL",
		"18Z  -    !   -   LOW",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("response missing %q:\n%s", want, resp)
		}
	}
	for _, hidden := range []string{"19Z", "20Z", "UNLIKELY", "CLOSED"} {
		if strings.Contains(resp, hidden) {
			t.Fatalf("response contains hidden row marker %q:\n%s", hidden, resp)
		}
	}
	if len(fallback.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(fallback.requests))
	}
	req := fallback.requests[0]
	if req.UserGrid != "FN31" || req.DXGrid != "FN32" || req.Band != "20m" || req.Mode != "FT8" || req.ReceiveNoisePenaltyDB != noisePenalty {
		t.Fatalf("unexpected request: %+v", req)
	}
}

func TestHandleShowPropAllBandsPartialCache(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{7.15, 14.1}
	now := time.Date(2026, time.June, 8, 18, 0, 0, 0, time.UTC)
	fallback := &fakeShowPropFallback{
		cfg: cfg,
		statuses: map[string]pathreliability.VOACAPForecastWindowStatus{
			"20m": pathreliability.VOACAPForecastWindowRefreshing,
		},
		windows: map[string]pathreliability.VOACAPCachedForecastWindow{
			"20m": {
				Records: []pathreliability.VOACAPCachedForecast{
					testShowPropForecast(cfg, 18, -5, -5, 0),
				},
			},
		},
	}
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"20m", "40m"}),
		pathClosedFallback: fallback,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return now },
	}
	client := testShowPropClient("FN30", "QUIET")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP FN32 CW")
	if !handled {
		t.Fatalf("SHOW PROP not handled")
	}
	for _, want := range []string{
		"BAND  UTC  EFF  RX  TX  REL",
		"40m   Still computing; ask again shortly.",
		"20m   18Z  =    =   =   MEDIUM",
		"20m   Refreshing; ask again shortly for full horizon.",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("response missing %q:\n%s", want, resp)
		}
	}
	if len(fallback.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(fallback.requests))
	}
	if fallback.requests[0].Band != "40m" || fallback.requests[1].Band != "20m" {
		t.Fatalf("unexpected band request order: %+v", fallback.requests)
	}
}

func TestHandleShowPropSingleBandSuppressesAllClosedAndUnlikely(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{14.1}
	fallback := &fakeShowPropFallback{
		cfg: cfg,
		windows: map[string]pathreliability.VOACAPCachedForecastWindow{
			"20m": {
				Records: []pathreliability.VOACAPCachedForecast{
					testShowPropForecast(cfg, 18, -25, -25, 0),
					testShowPropForecast(cfg, 19, -35, -35, 0),
				},
			},
		},
	}
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"20m"}),
		pathClosedFallback: fallback,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return time.Date(2026, time.June, 8, 18, 0, 0, 0, time.UTC) },
	}
	client := testShowPropClient("FN31", "QUIET")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP FN32 20m FT8")
	if !handled {
		t.Fatalf("SHOW PROP not handled")
	}
	if !strings.Contains(resp, showPropNoOpenRowsMessage) {
		t.Fatalf("expected no-open-rows message, got:\n%s", resp)
	}
	for _, hidden := range []string{"UTC  EFF", "18Z", "19Z", "UNLIKELY", "CLOSED"} {
		if strings.Contains(resp, hidden) {
			t.Fatalf("response contains hidden row marker %q:\n%s", hidden, resp)
		}
	}
}

func TestHandleShowPropAllBandsSuppressesClosedAndUnlikelyPerBand(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{7.15, 14.1}
	fallback := &fakeShowPropFallback{
		cfg: cfg,
		windows: map[string]pathreliability.VOACAPCachedForecastWindow{
			"40m": {
				Records: []pathreliability.VOACAPCachedForecast{
					testShowPropForecast(cfg, 18, -16, -16, 0),
				},
			},
			"20m": {
				Records: []pathreliability.VOACAPCachedForecast{
					testShowPropForecast(cfg, 18, -8, -8, 0),
				},
			},
		},
	}
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"20m", "40m"}),
		pathClosedFallback: fallback,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return time.Date(2026, time.June, 8, 18, 0, 0, 0, time.UTC) },
	}
	client := testShowPropClient("FN31", "QUIET")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP FN32 CW")
	if !handled {
		t.Fatalf("SHOW PROP not handled")
	}
	for _, want := range []string{
		"BAND  UTC  EFF  RX  TX  REL",
		"40m   " + showPropNoOpenRowsMessage,
		"20m   18Z  -    -   -   LOW",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("response missing %q:\n%s", want, resp)
		}
	}
	if strings.Contains(resp, "UNLIKELY") || strings.Contains(resp, "CLOSED") {
		t.Fatalf("response includes hidden reliability class:\n%s", resp)
	}
}

func TestHandleShowPropColdMiss(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{14.1}
	fallback := &fakeShowPropFallback{cfg: cfg}
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"20m"}),
		pathClosedFallback: fallback,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return time.Date(2026, time.June, 8, 18, 0, 0, 0, time.UTC) },
	}
	client := testShowPropClient("FN31", "QUIET")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP FN32 20m")
	if !handled {
		t.Fatalf("SHOW PROP not handled")
	}
	if !strings.Contains(resp, "Still computing; ask again shortly.") {
		t.Fatalf("expected cold miss response, got:\n%s", resp)
	}
	if len(fallback.requests) != 1 || fallback.requests[0].Mode != "CW" {
		t.Fatalf("expected omitted mode to default to CW, requests=%+v", fallback.requests)
	}
	if got := fallback.waits[0]; got != 750*time.Millisecond {
		t.Fatalf("single-band wait = %s, want 750ms", got)
	}
}

func TestHandleShowPropSSBNormalizesPerBand(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{7.15, 14.1}
	fallback := &fakeShowPropFallback{cfg: cfg}
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"20m", "40m"}),
		pathClosedFallback: fallback,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return time.Date(2026, time.June, 8, 18, 0, 0, 0, time.UTC) },
	}
	client := testShowPropClient("FN31", "QUIET")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP FN32 SSB")
	if !handled {
		t.Fatalf("SHOW PROP not handled")
	}
	if !strings.Contains(resp, "mode=SSB") {
		t.Fatalf("expected generic SSB mode in header, got:\n%s", resp)
	}
	if len(fallback.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(fallback.requests))
	}
	if fallback.requests[0].Band != "40m" || fallback.requests[0].Mode != "LSB" ||
		fallback.requests[1].Band != "20m" || fallback.requests[1].Mode != "USB" {
		t.Fatalf("unexpected SSB band modes: %+v", fallback.requests)
	}
}

func TestHandleShowPropGridStoreAndCTYTargets(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{14.1}
	now := time.Date(2026, time.June, 8, 18, 0, 0, 0, time.UTC)
	fallback := &fakeShowPropFallback{
		cfg: cfg,
		windows: map[string]pathreliability.VOACAPCachedForecastWindow{
			"20m": {Records: []pathreliability.VOACAPCachedForecast{testShowPropForecast(cfg, 18, -5, -5, 0)}},
		},
	}
	server := &Server{
		pathPredictor:      pathreliability.NewPredictor(cfg, []string{"20m"}),
		pathClosedFallback: fallback,
		noiseModel:         cfg.NoiseModel(),
		nowFn:              func() time.Time { return now },
		gridLookup: func(call string) (string, bool, bool) {
			if call == "K1ABC" {
				return "FN32", true, true
			}
			return "", false, false
		},
		ctyLookup: func() *cty.CTYDatabase { return testShowPropCTY(t) },
	}
	client := testShowPropClient("FN31", "QUIET")

	resp, handled := server.handleShowPropCommand(client, "SHOW PROP K1ABC 20m")
	if !handled || !strings.Contains(resp, "source=gridstore-derived") {
		t.Fatalf("expected gridstore-derived target, handled=%v response:\n%s", handled, resp)
	}
	resp, handled = server.handleShowPropCommand(client, "SHOW PROP IT9 20m")
	if !handled || !strings.Contains(resp, "source=cty-derived") {
		t.Fatalf("expected cty-derived target, handled=%v response:\n%s", handled, resp)
	}
}

func testShowPropClient(grid string, noise string) *Client {
	return &Client{
		grid:           grid,
		gridCell:       pathreliability.EncodeCell(grid),
		gridCoarseCell: pathreliability.EncodeCoarseCell(grid),
		noiseClass:     noise,
	}
}

func testShowPropForecast(cfg pathreliability.Config, hour int, receive int, transmit int, noisePenalty float64) pathreliability.VOACAPCachedForecast {
	effective := cfg.MergeReceiveWeight*(float64(receive)-noisePenalty) + cfg.MergeTransmitWeight*float64(transmit)
	return pathreliability.VOACAPCachedForecast{
		Record: pathreliability.VOACAPHourlyForecast{
			FT8SNRDB:          int(math.Round(effective)),
			HourUTC:           hour,
			FrequencyMHz:      14.1,
			ReceiveFT8SNRDB:   receive,
			TransmitFT8SNRDB:  transmit,
			HasDirectionalSNR: true,
		},
		EffectiveFT8SNRDB:     effective,
		HasEffectiveFT8SNRDB:  true,
		ReceiveNoisePenaltyDB: noisePenalty,
		SSN:                   112,
	}
}

type fakeShowPropFallback struct {
	cfg      pathreliability.Config
	windows  map[string]pathreliability.VOACAPCachedForecastWindow
	statuses map[string]pathreliability.VOACAPForecastWindowStatus
	waits    []time.Duration
	requests []pathreliability.VOACAPClosedRequest
}

func (f *fakeShowPropFallback) CheckForecast(pathreliability.VOACAPClosedRequest, time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	return pathreliability.VOACAPCachedForecast{}, false
}

func (f *fakeShowPropFallback) CheckForecastWindow(req pathreliability.VOACAPClosedRequest, _ time.Time) (pathreliability.VOACAPCachedForecastWindow, bool) {
	f.requests = append(f.requests, req)
	if f.windows == nil {
		return pathreliability.VOACAPCachedForecastWindow{}, false
	}
	window, ok := f.windows[req.Band]
	return window, ok
}

func (f *fakeShowPropFallback) CheckForecastWindowWait(req pathreliability.VOACAPClosedRequest, _ time.Time, wait time.Duration) (pathreliability.VOACAPCachedForecastWindow, pathreliability.VOACAPForecastWindowStatus) {
	f.requests = append(f.requests, req)
	f.waits = append(f.waits, wait)
	window, ok := f.windows[req.Band]
	status, hasStatus := f.statuses[req.Band]
	if !hasStatus {
		if ok {
			status = pathreliability.VOACAPForecastWindowReady
		} else {
			status = pathreliability.VOACAPForecastWindowRefreshing
		}
	}
	return window, status
}

func testShowPropCTY(t *testing.T) *cty.CTYDatabase {
	t.Helper()
	const plist = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
<key>IT9</key>
	<dict>
		<key>Country</key>
		<string>Sicily</string>
		<key>Prefix</key>
		<string>IT9</string>
		<key>Latitude</key>
		<real>37.5</real>
		<key>Longitude</key>
		<real>14.0</real>
		<key>ExactCallsign</key>
		<false/>
	</dict>
</dict>
</plist>`
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(plist))
	if err != nil {
		t.Fatalf("load CTY test db: %v", err)
	}
	return db
}
