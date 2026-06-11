package voacap

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestApplySunspotObservationFirstForecast(t *testing.T) {
	cfg := validExperimentConfig()
	var st ForecastState
	obs := SunspotObservation{ObservedAtUTC: mustTime(t, "2026-06-08T12:00:00Z"), RawWolfEstimate: 100}

	decision, err := ApplySunspotObservation(&st, obs, cfg)
	if err != nil {
		t.Fatalf("ApplySunspotObservation() error: %v", err)
	}
	if decision.Transition != ForecastTransitionForecastNeeded || !decision.ForecastRequired {
		t.Fatalf("decision = %#v, want forecast needed", decision)
	}
	if st.EWMA != 100 || !st.EWMAInitialized {
		t.Fatalf("state EWMA = %.2f initialized=%v, want 100 true", st.EWMA, st.EWMAInitialized)
	}
	if st.LastForecastSSNInitialized {
		t.Fatalf("forecast baseline updated before success")
	}
}

func TestApplySunspotObservationStaleDoesNotUpdateEWMA(t *testing.T) {
	cfg := validExperimentConfig()
	st := ForecastState{
		LastObservedAtUTC:          mustTime(t, "2026-06-08T12:00:00Z"),
		LastRawSSN:                 100,
		EWMA:                       100,
		EWMAInitialized:            true,
		LastForecastSSN:            100,
		LastForecastSSNInitialized: true,
	}
	obs := SunspotObservation{ObservedAtUTC: mustTime(t, "2026-06-08T12:00:00Z"), RawWolfEstimate: 160}

	decision, err := ApplySunspotObservation(&st, obs, cfg)
	if err != nil {
		t.Fatalf("ApplySunspotObservation() error: %v", err)
	}
	if decision.Transition != ForecastTransitionStale || decision.ForecastRequired {
		t.Fatalf("decision = %#v, want stale without forecast", decision)
	}
	if st.EWMA != 100 || st.LastRawSSN != 100 {
		t.Fatalf("state changed for stale observation: %#v", st)
	}
}

func TestApplySunspotObservationStaleRetriesMissingForecast(t *testing.T) {
	cfg := validExperimentConfig()
	st := ForecastState{
		LastObservedAtUTC: mustTime(t, "2026-06-08T12:00:00Z"),
		LastRawSSN:        100,
		EWMA:              100,
		EWMAInitialized:   true,
	}
	obs := SunspotObservation{ObservedAtUTC: mustTime(t, "2026-06-08T12:00:00Z"), RawWolfEstimate: 100}

	decision, err := ApplySunspotObservation(&st, obs, cfg)
	if err != nil {
		t.Fatalf("ApplySunspotObservation() error: %v", err)
	}
	if decision.Transition != ForecastTransitionForecastNeeded || !decision.ForecastRequired {
		t.Fatalf("decision = %#v, want forecast retry", decision)
	}
	if st.EWMA != 100 {
		t.Fatalf("stale retry changed EWMA to %.2f", st.EWMA)
	}
}

func TestApplyFetchUnchangedRetriesMissingForecast(t *testing.T) {
	cfg := validExperimentConfig()
	st := ForecastState{EWMA: 147, EWMAInitialized: true}

	decision, err := ApplyFetchUnchanged(&st, cfg)
	if err != nil {
		t.Fatalf("ApplyFetchUnchanged() error: %v", err)
	}
	if decision.Transition != ForecastTransitionForecastNeeded || !decision.ForecastRequired {
		t.Fatalf("decision = %#v, want forecast retry", decision)
	}
}

func TestApplySunspotObservationBelowThresholdDoesNotForecast(t *testing.T) {
	cfg := validExperimentConfig()
	st := ForecastState{
		LastObservedAtUTC:          mustTime(t, "2026-06-08T12:00:00Z"),
		LastRawSSN:                 100,
		EWMA:                       100,
		EWMAInitialized:            true,
		LastForecastSSN:            100,
		LastForecastSSNInitialized: true,
	}
	obs := SunspotObservation{ObservedAtUTC: mustTime(t, "2026-06-08T20:00:00Z"), RawWolfEstimate: 110}

	decision, err := ApplySunspotObservation(&st, obs, cfg)
	if err != nil {
		t.Fatalf("ApplySunspotObservation() error: %v", err)
	}
	if decision.Transition != ForecastTransitionFresh || decision.ForecastRequired {
		t.Fatalf("decision = %#v, want fresh without forecast", decision)
	}
	if decision.EWMA != 105 {
		t.Fatalf("EWMA = %.2f, want 105", decision.EWMA)
	}
}

func TestApplySunspotObservationAtThresholdForecasts(t *testing.T) {
	cfg := validExperimentConfig()
	st := ForecastState{
		LastObservedAtUTC:          mustTime(t, "2026-06-08T12:00:00Z"),
		LastRawSSN:                 100,
		EWMA:                       100,
		EWMAInitialized:            true,
		LastForecastSSN:            100,
		LastForecastSSNInitialized: true,
	}
	obs := SunspotObservation{ObservedAtUTC: mustTime(t, "2026-06-08T20:00:00Z"), RawWolfEstimate: 124}

	decision, err := ApplySunspotObservation(&st, obs, cfg)
	if err != nil {
		t.Fatalf("ApplySunspotObservation() error: %v", err)
	}
	if decision.Transition != ForecastTransitionForecastNeeded || !decision.ForecastRequired {
		t.Fatalf("decision = %#v, want forecast", decision)
	}
	if decision.EWMA != 112 {
		t.Fatalf("EWMA = %.2f, want 112", decision.EWMA)
	}
}

func TestMarkForecastSuccessAndFailureBaseline(t *testing.T) {
	st := ForecastState{EWMA: 112, EWMAInitialized: true}
	now := mustTime(t, "2026-06-08T20:05:00Z")

	decision, err := MarkForecastSuccess(&st, 112.004, "out.dat", 1234, now)
	if err != nil {
		t.Fatalf("MarkForecastSuccess() error: %v", err)
	}
	if decision.Transition != ForecastTransitionSuccess {
		t.Fatalf("transition = %s, want success", decision.Transition)
	}
	if st.LastForecastSSN != 112 || !st.LastForecastSSNInitialized {
		t.Fatalf("forecast baseline = %.2f initialized=%v, want 112 true", st.LastForecastSSN, st.LastForecastSSNInitialized)
	}

	failure := MarkForecastFailure(&st, errors.New("boom"), now.Add(time.Minute))
	if failure.Transition != ForecastTransitionFailure {
		t.Fatalf("transition = %s, want failure", failure.Transition)
	}
	if st.LastForecastSSN != 112 {
		t.Fatalf("failure changed forecast baseline to %.2f", st.LastForecastSSN)
	}
	if !strings.Contains(st.LastForecastError, "boom") {
		t.Fatalf("LastForecastError = %q, want boom", st.LastForecastError)
	}
}

func TestBuildExperimentDeckUsesConfiguredHoursAndFrequencies(t *testing.T) {
	cfg := validExperimentConfig()
	cfg.ForecastHours = 4
	cfg.CenterFrequenciesMHz = []float64{3.5, 7.0}

	deck, err := BuildExperimentDeck(cfg, 112.4, mustTime(t, "2026-06-08T20:00:00Z"))
	if err != nil {
		t.Fatalf("BuildExperimentDeck() error: %v", err)
	}
	body := string(deck)
	for _, want := range []string{
		"TIME          1    4    1    1",
		"MONTH      2026 6.00",
		"SUNSPOT    112.",
		"CIRCUIT   42.36N   071.06W    52.23N    021.01E  S     0",
		"SYSTEM       1. 153. 3.00  50. 10.0 3.00 0.10",
		"FREQUENCY  3.50 7.00 0.00 0.00",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("deck missing %q:\n%s", want, body)
		}
	}
}

func TestBuildPathDeckUsesConfiguredVOACAPStartHour(t *testing.T) {
	deck, err := BuildPathDeck(PathDeckRequest{
		Comment:              "current hour path",
		Transmit:             DeckEndpoint{Label: "fn31", Latitude: 41.5, Longitude: -73},
		Receive:              DeckEndpoint{Label: "jo90", Latitude: 50.5, Longitude: 19},
		SSN:                  147,
		Now:                  mustTime(t, "2026-06-09T17:00:00Z"),
		ForecastHours:        8,
		StartVOACAPHour:      17,
		CenterFrequenciesMHz: []float64{14.1},
	})
	if err != nil {
		t.Fatalf("BuildPathDeck() error: %v", err)
	}
	if body := string(deck); !strings.Contains(body, "TIME         17   24    1    1") {
		t.Fatalf("deck should cover the current UTC hour through midnight:\n%s", body)
	}
}

func TestBuildPathDeckDefaultsToMethod30(t *testing.T) {
	deck, err := BuildPathDeck(PathDeckRequest{
		Comment:              "default method path",
		Transmit:             DeckEndpoint{Label: "fn31", Latitude: 41.5, Longitude: -73},
		Receive:              DeckEndpoint{Label: "jo90", Latitude: 50.5, Longitude: 19},
		SSN:                  147,
		Now:                  mustTime(t, "2026-06-09T17:00:00Z"),
		ForecastHours:        8,
		CenterFrequenciesMHz: []float64{14.1},
	})
	if err != nil {
		t.Fatalf("BuildPathDeck() error: %v", err)
	}
	if body := string(deck); !strings.Contains(body, "METHOD       30    0") {
		t.Fatalf("deck should preserve Method 30 as the direct-call default:\n%s", body)
	}
}

func TestBuildPathDeckUsesExplicitMethod20(t *testing.T) {
	deck, err := BuildPathDeck(PathDeckRequest{
		Comment:              "method 20 path",
		Transmit:             DeckEndpoint{Label: "fn31", Latitude: 41.5, Longitude: -73},
		Receive:              DeckEndpoint{Label: "fn32", Latitude: 42.5, Longitude: -73},
		SSN:                  147,
		Now:                  mustTime(t, "2026-06-09T17:00:00Z"),
		ForecastHours:        8,
		CenterFrequenciesMHz: []float64{14.1},
		Method:               PathMethodCompleteSystem,
	})
	if err != nil {
		t.Fatalf("BuildPathDeck() error: %v", err)
	}
	if body := string(deck); !strings.Contains(body, "METHOD       20    0") {
		t.Fatalf("deck should use explicit Method 20:\n%s", body)
	}
}

func TestBuildPathDeckRejectsUnsupportedMethod(t *testing.T) {
	_, err := BuildPathDeck(PathDeckRequest{
		Comment:              "bad method",
		Transmit:             DeckEndpoint{Label: "fn31", Latitude: 41.5, Longitude: -73},
		Receive:              DeckEndpoint{Label: "fn32", Latitude: 42.5, Longitude: -73},
		SSN:                  147,
		Now:                  mustTime(t, "2026-06-09T17:00:00Z"),
		ForecastHours:        8,
		CenterFrequenciesMHz: []float64{14.1},
		Method:               21,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported VOACAP method") {
		t.Fatalf("BuildPathDeck() error = %v, want unsupported method validation", err)
	}
}

func TestRecommendedPathMethodUses7000KMBoundary(t *testing.T) {
	origin := DeckEndpoint{Label: "origin", Latitude: 0, Longitude: 0}
	longitudeAt7000KM := pathMethodDistanceThresholdKM / earthMeanRadiusKM * 180 / math.Pi
	tests := []struct {
		name    string
		lon     float64
		want    PathMethod
		wantMin float64
		wantMax float64
	}{
		{name: "under threshold", lon: longitudeAt7000KM - 0.01, want: PathMethodCompleteSystem, wantMin: 6998, wantMax: 7000},
		{name: "at threshold", lon: longitudeAt7000KM, want: PathMethodShortLongPathSmoothing, wantMin: 6999.999, wantMax: 7000.1},
		{name: "over threshold", lon: longitudeAt7000KM + 0.01, want: PathMethodShortLongPathSmoothing, wantMin: 7000, wantMax: 7002},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method, distanceKM, err := RecommendedPathMethod(origin, DeckEndpoint{Label: "target", Latitude: 0, Longitude: tt.lon})
			if err != nil {
				t.Fatalf("RecommendedPathMethod() error: %v", err)
			}
			if method != tt.want {
				t.Fatalf("method = %d, want %d at distance %.3f", method, tt.want, distanceKM)
			}
			if distanceKM < tt.wantMin || distanceKM > tt.wantMax {
				t.Fatalf("distance = %.3f, want within %.3f..%.3f", distanceKM, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestBuildPathDeckWrapsConfiguredVOACAPStartHour(t *testing.T) {
	deck, err := BuildPathDeck(PathDeckRequest{
		Comment:              "wrapped path",
		Transmit:             DeckEndpoint{Label: "fn31", Latitude: 41.5, Longitude: -73},
		Receive:              DeckEndpoint{Label: "jo90", Latitude: 50.5, Longitude: 19},
		SSN:                  147,
		Now:                  mustTime(t, "2026-06-09T22:00:00Z"),
		ForecastHours:        7,
		StartVOACAPHour:      22,
		CenterFrequenciesMHz: []float64{14.1},
	})
	if err != nil {
		t.Fatalf("BuildPathDeck() error: %v", err)
	}
	if body := string(deck); !strings.Contains(body, "TIME         22    4    1    1") {
		t.Fatalf("deck should wrap over midnight:\n%s", body)
	}
}

func TestBuildPathDeckRejectsInvalidVOACAPStartHour(t *testing.T) {
	_, err := BuildPathDeck(PathDeckRequest{
		Comment:              "bad hour",
		Transmit:             DeckEndpoint{Label: "fn31", Latitude: 41.5, Longitude: -73},
		Receive:              DeckEndpoint{Label: "jo90", Latitude: 50.5, Longitude: 19},
		SSN:                  147,
		Now:                  mustTime(t, "2026-06-09T22:00:00Z"),
		ForecastHours:        7,
		StartVOACAPHour:      25,
		CenterFrequenciesMHz: []float64{14.1},
	})
	if err == nil || !strings.Contains(err.Error(), "start VOACAP hour") {
		t.Fatalf("BuildPathDeck() error = %v, want start hour validation", err)
	}
}

func TestHourForUTC(t *testing.T) {
	tests := []struct {
		at   string
		want int
	}{
		{at: "2026-06-09T00:00:00Z", want: 24},
		{at: "2026-06-09T17:00:00Z", want: 17},
	}
	for _, tt := range tests {
		if got := HourForUTC(mustTime(t, tt.at)); got != tt.want {
			t.Fatalf("HourForUTC(%s) = %d, want %d", tt.at, got, tt.want)
		}
	}
}

func TestBuildPathDeckUsesDirectedEndpoints(t *testing.T) {
	deck, err := BuildPathDeck(PathDeckRequest{
		Comment:              "directed path",
		Transmit:             DeckEndpoint{Label: "fn31", Latitude: 41.5, Longitude: -73},
		Receive:              DeckEndpoint{Label: "jo90", Latitude: 50.5, Longitude: 19},
		SSN:                  147,
		Now:                  mustTime(t, "2026-06-08T20:00:00Z"),
		ForecastHours:        8,
		CenterFrequenciesMHz: []float64{14.1},
	})
	if err != nil {
		t.Fatalf("BuildPathDeck() error: %v", err)
	}
	body := string(deck)
	for _, want := range []string{
		"COMMENT    directed path",
		"SUNSPOT    147.",
		"LABEL     FN31",
		"CIRCUIT   41.50N   073.00W    50.50N    019.00E  S     0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("directed deck missing %q:\n%s", want, body)
		}
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %s: %v", value, err)
	}
	return parsed
}
