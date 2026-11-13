package voacap

import (
	"math"
	"testing"
	"time"
	"unsafe"
)

func TestSolarFluxInverseIndependentVectors(t *testing.T) {
	// Independent P.371 polynomial vectors supplied by the scientific review:
	// R=73 -> F=121.58681; R=74 -> F=122.44564; R=73.5 -> F=122.0160025.
	// The quiet-Sun clamp is the approved product policy, not ITU calibration.
	for _, tc := range []struct {
		name string
		flux float64
		ssn  int
	}{
		{"zero", 0, 0}, {"below_intercept", 63.699999, 0}, {"intercept", 63.7, 0},
		{"R73", 121.58681, 73}, {"R74", 122.44564, 74},
		{"below_half", 122.0160025 - 0.000001, 73},
		{"half", 122.0160025, 74}, {"above_half", 122.0160025 + 0.000001, 74},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ssnFromSolarFlux(tc.flux)
			if err != nil || got != tc.ssn {
				t.Fatalf("flux %.12f -> %d, %v; want %d", tc.flux, got, err, tc.ssn)
			}
		})
	}
	for _, bad := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64} {
		if _, err := ssnFromSolarFlux(bad); err == nil {
			t.Fatalf("invalid/unrepresentable flux %v accepted", bad)
		}
	}
}

func TestSolarFluxEWMAUsesActualElapsedTimeWithoutIntermediateRound(t *testing.T) {
	if got := advanceFluxEWMA(100.125, 140.375, 48*time.Hour, 48*time.Hour); got != 120.25 {
		t.Fatalf("two-day half-life = %.15f, want 120.25", got)
	}
	if got := advanceFluxEWMA(100.125, 140.375, 96*time.Hour, 48*time.Hour); got != 130.3125 {
		t.Fatalf("four-day gap = %.15f, want 130.3125", got)
	}
	if got := advanceFluxEWMA(100.125, 140.375, 0, 48*time.Hour); got != 100.125 {
		t.Fatalf("zero elapsed time smoothed twice: %v", got)
	}
}

func TestParseNoonFluxSortsAndValidatesComponents(t *testing.T) {
	now := mustTime(t, "2026-06-12T21:00:00Z")
	body := []byte(`[
		{"time_tag":"2026-06-10T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":131.9,"ninety_day_mean":112.25},
		{"time_tag":"2026-06-09T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":0,"ninety_day_mean":null},
		{"time_tag":"2026-06-08T20:00:00","frequency":2800,"reporting_schedule":"Morning","flux":900,"ninety_day_mean":900},
		{"time_tag":"2026-06-08T20:00:00","frequency":1400,"reporting_schedule":"Noon","flux":900,"ninety_day_mean":900},
		{"time_tag":"2026-06-11T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":-1,"ninety_day_mean":110.125},
		{"time_tag":"2026-06-12T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":125.125,"ninety_day_mean":"invalid"},
		{"time_tag":"2026-06-13T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":900,"ninety_day_mean":900},
		{"time_tag":"invalid","frequency":2800,"reporting_schedule":"Noon","flux":900}
	]`)
	series, err := parseNOAANoonFlux(body, now)
	if err != nil || len(series) != 4 {
		t.Fatalf("parsed %d rows, %v; want four", len(series), err)
	}
	if !series[0].hasFlux || series[0].flux != 0 || series[0].hasMean ||
		series[1].flux != 131.9 || series[1].mean != 112.25 ||
		series[2].hasFlux || !series[2].hasMean || series[2].mean != 110.125 ||
		series[3].flux != 125.125 || series[3].hasMean {
		t.Fatalf("component validity/fractional values lost: %+v", series)
	}
}

func TestParseNoonFluxDuplicatesCannotChooseByInputOrder(t *testing.T) {
	a := `{"time_tag":"2026-06-08T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":100,"ninety_day_mean":110}`
	b := `{"time_tag":"2026-06-08T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":101,"ninety_day_mean":110}`
	for _, body := range []string{"[" + a + "," + a + "]", "[" + a + "," + b + "]", "[" + b + "," + a + "," + a + "]"} {
		series, err := parseNOAANoonFlux([]byte(body), mustTime(t, "2026-06-09T00:00:00Z"))
		if err != nil || len(series) != 1 || !series[0].hasMean || series[0].mean != 110 {
			t.Fatalf("duplicate mean lost: %+v %v", series, err)
		}
		wantFlux := body == "["+a+","+a+"]"
		if series[0].hasFlux != wantFlux {
			t.Fatalf("conflicting flux selected by order: %+v", series)
		}
	}
}

func TestSolarInputEnvelopeBound(t *testing.T) {
	if got := unsafe.Sizeof(SolarInputProvenance{}); got != 80 {
		t.Fatalf("provenance = %d bytes, approved envelope is 80", got)
	}
	if got := unsafe.Sizeof(SolarInputSnapshot{}); got > 88 {
		t.Fatalf("solar snapshot = %d bytes, approved maximum 88", got)
	}
}

func FuzzParseNOAANoonFlux(f *testing.F) {
	f.Add(`[]`)
	f.Add(`[null,{}]`)
	f.Add(`[{"time_tag":"2026-06-08T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":131.9,"ninety_day_mean":null}]`)
	f.Add(`[{"time_tag":"2026-06-08T20:00:00","frequency":2800,"reporting_schedule":"Noon","flux":0,"ninety_day_mean":1e309}]`)
	now := time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, body string) {
		series, err := parseNOAANoonFlux([]byte(body), now)
		if err != nil {
			return
		}
		for i, row := range series {
			if row.at.After(now) || (!row.hasFlux && !row.hasMean) ||
				(row.hasFlux && !finiteNonnegativeFlux(row.flux)) || (row.hasMean && !finiteNonnegativeFlux(row.mean)) ||
				(i > 0 && !row.at.After(series[i-1].at)) {
				t.Fatalf("invalid/noncanonical result: %+v", series)
			}
		}
	})
}
