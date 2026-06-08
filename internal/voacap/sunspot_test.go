package voacap

import (
	"os"
	"testing"
	"time"
)

func TestParseNOAASunspotReportGroupsRowsByTime(t *testing.T) {
	body := readFixture(t, "testdata/noaa_sunspot_report.json")

	series, err := ParseNOAASunspotReport(body)
	if err != nil {
		t.Fatalf("ParseNOAASunspotReport returned error: %v", err)
	}
	if len(series) != 4 {
		t.Fatalf("len(series) = %d, want 4", len(series))
	}

	first := series[0]
	if got, want := first.GroupCount, 2; got != want {
		t.Fatalf("first.GroupCount = %d, want %d", got, want)
	}
	if got, want := first.SpotCount, 7; got != want {
		t.Fatalf("first.SpotCount = %d, want %d", got, want)
	}
	if got, want := first.RawWolfEstimate, 27; got != want {
		t.Fatalf("first.RawWolfEstimate = %d, want %d", got, want)
	}
	if got, want := first.Observatory, "A"; got != want {
		t.Fatalf("first.Observatory = %q, want %q", got, want)
	}

	if got, want := series[1].Station, "12345"; got != want {
		t.Fatalf("numeric Station parsed as %q, want %q", got, want)
	}
}

func TestRollingNOAASunspotAveragesUsesStrictWindowStart(t *testing.T) {
	body := readFixture(t, "testdata/noaa_sunspot_report.json")
	series, err := ParseNOAASunspotReport(body)
	if err != nil {
		t.Fatalf("ParseNOAASunspotReport returned error: %v", err)
	}

	rolling, err := RollingNOAASunspotAverages(series, 8*time.Hour)
	if err != nil {
		t.Fatalf("RollingNOAASunspotAverages returned error: %v", err)
	}
	if len(rolling) != 4 {
		t.Fatalf("len(rolling) = %d, want 4", len(rolling))
	}

	atEight := rolling[2]
	if got, want := atEight.ObservationCount, 2; got != want {
		t.Fatalf("08:00 observation count = %d, want %d", got, want)
	}
	if got, want := atEight.Average, 11.5; got != want {
		t.Fatalf("08:00 average = %v, want %v", got, want)
	}

	latest := rolling[3]
	if got, want := latest.ObservationCount, 3; got != want {
		t.Fatalf("latest observation count = %d, want %d", got, want)
	}
	if got, want := latest.Average, 11.0; got != want {
		t.Fatalf("latest average = %v, want %v", got, want)
	}
}

func TestLatestNOAASunspotAverageReportsNoUsableRows(t *testing.T) {
	body := []byte(`[
		{"time_tag":"bad","Observatory":"A","Station":"STA","Numspot":1},
		{"time_tag":"2026-06-08T00:00:00Z","Observatory":"B","Station":"STB","Numspot":-2}
	]`)

	_, ok, err := LatestNOAASunspotAverage(body, 8*time.Hour)
	if err != nil {
		t.Fatalf("LatestNOAASunspotAverage returned error: %v", err)
	}
	if ok {
		t.Fatal("LatestNOAASunspotAverage ok = true, want false")
	}
}

func TestRollingNOAASunspotAveragesRejectsNonPositiveWindow(t *testing.T) {
	_, err := RollingNOAASunspotAverages([]SunspotObservation{{RawWolfEstimate: 1}}, 0)
	if err == nil {
		t.Fatal("RollingNOAASunspotAverages returned nil error for zero window")
	}
}

func TestUpdateSunspotEWMAInitializesAndAppliesHalfLife(t *testing.T) {
	t0 := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	first := SunspotObservation{ObservedAtUTC: t0, RawWolfEstimate: 100}
	ewma, err := UpdateSunspotEWMA(0, time.Time{}, false, first, 8*time.Hour)
	if err != nil {
		t.Fatalf("initial UpdateSunspotEWMA returned error: %v", err)
	}
	if got, want := ewma.Average, 100.0; got != want {
		t.Fatalf("initial EWMA = %v, want %v", got, want)
	}
	if got, want := ewma.Alpha, 1.0; got != want {
		t.Fatalf("initial alpha = %v, want %v", got, want)
	}

	second := SunspotObservation{ObservedAtUTC: t0.Add(8 * time.Hour), RawWolfEstimate: 140}
	ewma, err = UpdateSunspotEWMA(ewma.Average, first.ObservedAtUTC, true, second, 8*time.Hour)
	if err != nil {
		t.Fatalf("second UpdateSunspotEWMA returned error: %v", err)
	}
	if got, want := ewma.Average, 120.0; got != want {
		t.Fatalf("8h half-life EWMA = %v, want %v", got, want)
	}
	if got, want := ewma.Alpha, 0.5; got != want {
		t.Fatalf("8h half-life alpha = %v, want %v", got, want)
	}
}

func TestShouldRecomputeVOACAP(t *testing.T) {
	trigger, delta, err := ShouldRecomputeVOACAP(100, 0, false, 0.12)
	if err != nil {
		t.Fatalf("initial ShouldRecomputeVOACAP returned error: %v", err)
	}
	if !trigger {
		t.Fatal("initial ShouldRecomputeVOACAP trigger = false, want true")
	}
	if delta != 0 {
		t.Fatalf("initial delta = %v, want 0", delta)
	}

	trigger, delta, err = ShouldRecomputeVOACAP(111.9, 100, true, 0.12)
	if err != nil {
		t.Fatalf("below-threshold ShouldRecomputeVOACAP returned error: %v", err)
	}
	if trigger {
		t.Fatal("below-threshold trigger = true, want false")
	}
	if got, want := round2(delta), 0.12; got != want {
		t.Fatalf("below-threshold rounded delta = %v, want %v", got, want)
	}

	trigger, _, err = ShouldRecomputeVOACAP(112, 100, true, 0.12)
	if err != nil {
		t.Fatalf("threshold ShouldRecomputeVOACAP returned error: %v", err)
	}
	if !trigger {
		t.Fatal("threshold trigger = false, want true")
	}
}

func TestParseNOAASunspotReportRejectsInvalidJSON(t *testing.T) {
	_, err := ParseNOAASunspotReport([]byte(`{`))
	if err == nil {
		t.Fatal("ParseNOAASunspotReport returned nil error for invalid JSON")
	}
}

func FuzzParseNOAASunspotReport(f *testing.F) {
	f.Add(`[]`)
	f.Add(`[
		{"time_tag":"2026-06-08T00:00:00Z","Observatory":"A","Station":"STA","Numspot":3}
	]`)
	f.Add(`[
		{"time_tag":"bad","Observatory":"A","Station":"STA","Numspot":"not-a-number"}
	]`)

	f.Fuzz(func(t *testing.T, body string) {
		_, _ = ParseNOAASunspotReport([]byte(body))
	})
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return body
}
