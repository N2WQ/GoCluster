package dxsummit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func testConfig(url string) config.DXSummitConfig {
	return config.DXSummitConfig{
		Enabled:                true,
		Name:                   SourceNode,
		BaseURL:                url,
		PollIntervalSeconds:    30,
		MaxRecordsPerPoll:      500,
		RequestTimeoutMS:       1000,
		LookbackSeconds:        300,
		StartupBackfillSeconds: 0,
		IncludeBands:           []string{"HF", "VHF", "UHF"},
		SpotChannelSize:        8,
		MaxResponseBytes:       1 << 20,
	}
}

func TestParseRecordPreservesDXSummitMarkerAndCommentFields(t *testing.T) {
	info := "IM98<>GG87 FT8 -14 db"
	sp, err := parseRecord(rawSpot{
		ID:          66711548,
		DECall:      "EA5JLX-@",
		DXCall:      "K2GOD",
		Info:        &info,
		Frequency:   21074.4,
		Time:        "2026-04-21T19:59:09",
		DELatitude:  floatPtr(39.4),
		DELongitude: floatPtr(-0.4),
		DXLatitude:  floatPtr(40.7),
		DXLongitude: floatPtr(-74.0),
	})
	if err != nil {
		t.Fatalf("parseRecord error: %v", err)
	}
	if sp.DECall != "EA5JLX-@" || sp.DECallNorm != "EA5JLX-@" {
		t.Fatalf("expected visible marker preserved, got DECall=%q DECallNorm=%q", sp.DECall, sp.DECallNorm)
	}
	if sp.SourceType != spot.SourceUpstream || sp.SourceNode != SourceNode || !sp.IsHuman {
		t.Fatalf("unexpected source fields: %+v", sp)
	}
	if sp.Mode != "FT8" || sp.ModeProvenance != spot.ModeProvenanceCommentExplicit {
		t.Fatalf("expected explicit FT8 mode, got mode=%q provenance=%q", sp.Mode, sp.ModeProvenance)
	}
	if !sp.HasReport || sp.Report != -14 {
		t.Fatalf("expected parsed -14 dB report, got has=%t report=%d", sp.HasReport, sp.Report)
	}
	if !sp.Time.Equal(time.Date(2026, 4, 21, 19, 59, 9, 0, time.UTC)) {
		t.Fatalf("unexpected source time %s", sp.Time)
	}
	if sp.DEMetadata.Grid != "" || sp.DXMetadata.Grid != "" {
		t.Fatalf("expected DXSummit coordinates not to populate grids, got de=%q dx=%q", sp.DEMetadata.Grid, sp.DXMetadata.Grid)
	}
}

func TestDXSummitHumanCommentRouting(t *testing.T) {
	for _, tc := range []struct {
		comment, mode, remaining string
		report                   int
		present                  bool
	}{
		{"CW UP 2 QRT 1800Z CWT", "CW", "UP 2 QRT 1800Z CWT", 0, false},
		{"FT8 -14 POTA", "FT8", "POTA", -14, true},
		{"FT8 UP 2 -12 dB", "FT8", "UP 2", -12, true},
		{"FT8 S9 +10 dB", "FT8", "S9 +10 dB", 0, false},
	} {
		info := tc.comment
		got, err := parseRecord(rawSpot{ID: 1, DECall: "W1AW", DXCall: "K1ABC", Frequency: 14074, Info: &info, Time: "2026-04-21T19:59:09"})
		if err != nil {
			t.Fatal(err)
		}
		if got.Mode != tc.mode || got.Comment != tc.remaining || got.HasReport != tc.present || got.Report != tc.report || !got.IsHuman || got.Time.UTC().Format("1504Z") != "1959Z" || parseRecordMode(&info, 14074) != tc.mode {
			t.Fatalf("DXSummit human route %q: %+v", info, got)
		}
	}
}

func TestParseRecordCanonicalizesDXNumericSSID(t *testing.T) {
	sp, err := parseRecord(rawSpot{
		ID:        66711549,
		DECall:    "EA5JLX-@",
		DXCall:    "K2GOD-2",
		Frequency: 21074.4,
		Time:      "2026-04-21T19:59:09",
	})
	if err != nil {
		t.Fatalf("parseRecord error: %v", err)
	}
	if sp.DXCall != "K2GOD" || sp.DXCallNorm != "K2GOD" {
		t.Fatalf("expected canonical DX K2GOD, got DXCall=%q DXCallNorm=%q", sp.DXCall, sp.DXCallNorm)
	}
}

func TestParseRecordStripsOnlyTerminalSkimmerMarker(t *testing.T) {
	tests := []struct {
		name string
		de   string
		want string
	}{
		{name: "plain skimmer marker", de: "N2WQ-#", want: "N2WQ"},
		{name: "numeric ssid preserved before marker", de: "N2WQ-1-#", want: "N2WQ-1"},
		{name: "numeric ssid without marker preserved", de: "N2WQ-1", want: "N2WQ-1"},
		{name: "dxsummit marker preserved", de: "EA5JLX-@", want: "EA5JLX-@"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp, err := parseRecord(rawSpot{
				ID:        100,
				DECall:    tt.de,
				DXCall:    "K2GOD",
				Frequency: 21074.4,
				Time:      "2026-04-21T19:59:09",
			})
			if err != nil {
				t.Fatalf("parseRecord error: %v", err)
			}
			if sp.DECall != tt.want || sp.DECallNorm != tt.want {
				t.Fatalf("expected DECall=%q DECallNorm=%q, got DECall=%q DECallNorm=%q", tt.want, tt.want, sp.DECall, sp.DECallNorm)
			}
		})
	}
}

func TestParseRecordAcceptsHFVHFUHFAndNilInfo(t *testing.T) {
	tests := []struct {
		name string
		freq float64
		info *string
		band string
	}{
		{name: "hf", freq: 14074.0, info: strPtr("FT8"), band: "20m"},
		{name: "vhf nil info", freq: 144300.0, band: "2m"},
		{name: "uhf 13cm", freq: 2400040.0, info: strPtr("CW"), band: "13cm"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp, err := parseRecord(rawSpot{
				ID:        100,
				DECall:    "K1ABC",
				DXCall:    "W1XYZ",
				Info:      tt.info,
				Frequency: tt.freq,
				Time:      "2026-04-21T19:59:09",
			})
			if err != nil {
				t.Fatalf("parseRecord error: %v", err)
			}
			if sp.Band != tt.band {
				t.Fatalf("band = %q, want %q", sp.Band, tt.band)
			}
		})
	}
}

func TestParseRecordRejectsMalformedMarkerAndUnsupportedFrequency(t *testing.T) {
	info := "FT8"
	tests := []struct {
		name string
		raw  rawSpot
	}{
		{
			name: "embedded at marker",
			raw: rawSpot{
				ID:        1,
				DECall:    "EA5@JLX",
				DXCall:    "K2GOD",
				Info:      &info,
				Frequency: 21074.4,
				Time:      "2026-04-21T19:59:09",
			},
		},
		{
			name: "unsupported gap",
			raw: rawSpot{
				ID:        2,
				DECall:    "EA5JLX",
				DXCall:    "K2GOD",
				Info:      &info,
				Frequency: 2350000,
				Time:      "2026-04-21T19:59:09",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseRecord(tt.raw); err == nil {
				t.Fatal("expected parse error")
			}
		})
	}
}

func TestParseRecordReportsInvalidCalls(t *testing.T) {
	info := "FT8"
	var got struct {
		source string
		role   string
		reason string
		call   string
		de     string
		dx     string
		mode   string
		detail string
	}
	reporter := func(source, role, reason, call, deCall, dxCall, mode, detail string) {
		got.source = source
		got.role = role
		got.reason = reason
		got.call = call
		got.de = deCall
		got.dx = dxCall
		got.mode = mode
		got.detail = detail
	}
	_, err := parseRecordWithReporter(rawSpot{
		ID:        10,
		DECall:    "BAD!",
		DXCall:    "K2GOD",
		Info:      &info,
		Frequency: 21074.4,
		Time:      "2026-04-21T19:59:09",
	}, reporter)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if got.source != SourceNode || got.role != "DE" || got.reason != "invalid_callsign" || got.call != "BAD!" || got.de != "BAD!" || got.dx != "K2GOD" || got.mode != "FT8" || got.detail != "source_parser" {
		t.Fatalf("unexpected bad-call report: %+v", got)
	}
}

func TestPollWarningsAndFailureCounters(t *testing.T) {
	t.Run("full page warning", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `[{"id":1,"de_call":"K1ABC","dx_call":"W1XYZ","info":"CW","frequency":14020,"time":"2026-04-21T19:59:01"}]`)
		}))
		defer server.Close()
		cfg := testConfig(server.URL)
		cfg.MaxRecordsPerPoll = 1
		c := NewClientWithHTTPClient(cfg, server.Client())
		c.SetLogger(nil)
		if !c.poll(context.Background(), false) {
			t.Fatal("expected poll success")
		}
		if got := c.HealthSnapshot().TruncationWarnings; got != 1 {
			t.Fatalf("truncation warnings = %d, want 1", got)
		}
	})

	t.Run("non 200", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", http.StatusBadGateway)
		}))
		defer server.Close()
		c := NewClientWithHTTPClient(testConfig(server.URL), server.Client())
		c.SetLogger(nil)
		if c.poll(context.Background(), false) {
			t.Fatal("expected poll failure")
		}
		if got := c.HealthSnapshot().RequestErrors; got != 1 {
			t.Fatalf("request errors = %d, want 1", got)
		}
	})

	t.Run("decode error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `not-json`)
		}))
		defer server.Close()
		c := NewClientWithHTTPClient(testConfig(server.URL), server.Client())
		c.SetLogger(nil)
		if c.poll(context.Background(), false) {
			t.Fatal("expected poll failure")
		}
		if got := c.HealthSnapshot().ParseErrors; got != 1 {
			t.Fatalf("parse errors = %d, want 1", got)
		}
	})

	t.Run("request timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
		}))
		defer server.Close()
		cfg := testConfig(server.URL)
		cfg.RequestTimeoutMS = 20
		c := NewClientWithHTTPClient(cfg, server.Client())
		c.SetLogger(nil)
		if c.poll(context.Background(), false) {
			t.Fatal("expected poll failure")
		}
		if got := c.HealthSnapshot().RequestErrors; got != 1 {
			t.Fatalf("request errors = %d, want 1", got)
		}
	})
}

func TestRequestURLUsesBoundedWindowAndBandIncludes(t *testing.T) {
	c := NewClientWithHTTPClient(testConfig("http://example.test/api/v1/spots"), nil)
	start := time.Date(2026, 4, 21, 19, 54, 0, 0, time.UTC)
	end := time.Date(2026, 4, 21, 19, 59, 0, 0, time.UTC)
	uri, err := c.requestURL(start, end)
	if err != nil {
		t.Fatalf("requestURL error: %v", err)
	}
	for _, want := range []string{
		"limit=500",
		"from_time=1776801240",
		"to_time=1776801540",
		"include=HF%2CVHF%2CUHF",
		"refresh=1776801540000",
	} {
		if !strings.Contains(uri, want) {
			t.Fatalf("expected %q in %s", want, uri)
		}
	}
}

func TestPollFiltersHighWaterAndEmitsOldestFirst(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.URL.Query().Get("include"); got != "HF,VHF,UHF" {
			t.Fatalf("include query = %q", got)
		}
		if got := r.URL.Query().Get("limit"); got != "500" {
			t.Fatalf("limit query = %q", got)
		}
		fmt.Fprint(w, `[
{"id":2,"de_call":"K2DEF","dx_call":"W2XYZ","info":"CW","frequency":14020,"time":"2026-04-21T19:59:02"},
{"id":1,"de_call":"K1ABC","dx_call":"W1XYZ","info":"SSB","frequency":14250,"time":"2026-04-21T19:59:01"}
]`)
	}))
	defer server.Close()

	c := NewClientWithHTTPClient(testConfig(server.URL), server.Client())
	c.SetLogger(nil)
	c.SetNowFunc(func() time.Time { return time.Date(2026, 4, 21, 20, 0, 0, 0, time.UTC) })
	if !c.poll(context.Background(), false) {
		t.Fatal("expected first poll success")
	}
	first := <-c.GetSpotChannel()
	second := <-c.GetSpotChannel()
	if first.DXCall != "W1XYZ" || second.DXCall != "W2XYZ" {
		t.Fatalf("expected oldest-to-newest emission, got %s then %s", first.DXCall, second.DXCall)
	}
	if !c.poll(context.Background(), false) {
		t.Fatal("expected second poll success")
	}
	select {
	case sp := <-c.GetSpotChannel():
		t.Fatalf("expected duplicate suppression, got %v", sp)
	default:
	}
	if calls != 2 {
		t.Fatalf("server calls = %d, want 2", calls)
	}
	if got := c.HealthSnapshot().DuplicateRows; got != 2 {
		t.Fatalf("duplicate rows = %d, want 2", got)
	}
}

func TestStartupSeedOnlySetsCursorWithoutReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":9,"de_call":"K1ABC","dx_call":"W1XYZ","info":"CW","frequency":14020,"time":"2026-04-21T19:59:01"}]`)
	}))
	defer server.Close()

	c := NewClientWithHTTPClient(testConfig(server.URL), server.Client())
	c.SetLogger(nil)
	if !c.poll(context.Background(), true) {
		t.Fatal("expected startup poll success")
	}
	select {
	case sp := <-c.GetSpotChannel():
		t.Fatalf("expected seed-only startup to emit no spots, got %v", sp)
	default:
	}
	if got := c.HealthSnapshot().LastSeenID; got != 9 {
		t.Fatalf("last seen ID = %d, want 9", got)
	}
}

func TestStartupBackfillEmitsOnlyRowsInsideWindow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[
{"id":2,"de_call":"K2DEF","dx_call":"W2XYZ","info":"CW","frequency":14020,"time":"2026-04-21T19:59:30"},
{"id":1,"de_call":"K1ABC","dx_call":"W1XYZ","info":"SSB","frequency":14250,"time":"2026-04-21T19:58:00"}
]`)
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.StartupBackfillSeconds = 60
	c := NewClientWithHTTPClient(cfg, server.Client())
	c.SetLogger(nil)
	c.SetNowFunc(func() time.Time { return time.Date(2026, 4, 21, 20, 0, 0, 0, time.UTC) })
	if !c.poll(context.Background(), true) {
		t.Fatal("expected startup poll success")
	}
	select {
	case sp := <-c.GetSpotChannel():
		if sp.DXCall != "W2XYZ" {
			t.Fatalf("expected in-window row, got %s", sp.DXCall)
		}
	default:
		t.Fatal("expected one startup backfill spot")
	}
	select {
	case sp := <-c.GetSpotChannel():
		t.Fatalf("expected old row to be skipped, got %v", sp)
	default:
	}
	if got := c.HealthSnapshot().LastSeenID; got != 2 {
		t.Fatalf("last seen ID = %d, want 2", got)
	}
}

func TestPollResponseTooLargeAndQueueDrop(t *testing.T) {
	t.Run("too large", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, strings.Repeat("x", 32))
		}))
		defer server.Close()
		cfg := testConfig(server.URL)
		cfg.MaxResponseBytes = 8
		c := NewClientWithHTTPClient(cfg, server.Client())
		c.SetLogger(nil)
		if c.poll(context.Background(), false) {
			t.Fatal("expected poll failure")
		}
		if got := c.HealthSnapshot().ResponseTooLarge; got != 1 {
			t.Fatalf("response too large = %d, want 1", got)
		}
	})

	t.Run("queue drop", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `[
{"id":2,"de_call":"K2DEF","dx_call":"W2XYZ","info":"CW","frequency":14020,"time":"2026-04-21T19:59:02"},
{"id":1,"de_call":"K1ABC","dx_call":"W1XYZ","info":"SSB","frequency":14250,"time":"2026-04-21T19:59:01"}
]`)
		}))
		defer server.Close()
		cfg := testConfig(server.URL)
		cfg.SpotChannelSize = 1
		c := NewClientWithHTTPClient(cfg, server.Client())
		c.SetLogger(nil)
		if !c.poll(context.Background(), false) {
			t.Fatal("expected poll success")
		}
		if got := c.HealthSnapshot().SpotDrops; got != 1 {
			t.Fatalf("spot drops = %d, want 1", got)
		}
	})
}

func TestStopCancelsInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	cfg := testConfig(server.URL)
	cfg.PollIntervalSeconds = 1
	cfg.RequestTimeoutMS = 900
	c := NewClientWithHTTPClient(cfg, server.Client())
	c.SetLogger(nil)
	if err := c.Connect(); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("server request did not start")
	}
	c.Stop()
	select {
	case _, ok := <-c.GetSpotChannel():
		if ok {
			t.Fatal("expected spot channel to close after Stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("spot channel did not close after Stop")
	}
}

func TestRunDoesNotOverlapPollRequests(t *testing.T) {
	var active atomic.Int64
	var maxActive atomic.Int64
	requests := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		for {
			observed := maxActive.Load()
			if current <= observed || maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		defer active.Add(-1)
		select {
		case requests <- struct{}{}:
		default:
		}
		select {
		case <-time.After(1200 * time.Millisecond):
			fmt.Fprint(w, `[]`)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.PollIntervalSeconds = 1
	cfg.RequestTimeoutMS = 2000
	c := NewClientWithHTTPClient(cfg, server.Client())
	c.SetLogger(nil)
	if err := c.Connect(); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer c.Stop()
	for i := 0; i < 3; i++ {
		select {
		case <-requests:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for request %d", i+1)
		}
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("max active requests = %d, want 1", got)
	}
}

func strPtr(s string) *string {
	return &s
}

func floatPtr(v float64) *float64 {
	return &v
}
