package voacap

// This file owns the runtime flux input only. The manual Wolf-estimate helpers
// in sunspot.go deliberately retain their separate experiment contract.

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"time"
)

const NOAANoonF107URL = "https://services.swpc.noaa.gov/json/f107_cm_flux.json"

type solarFluxObservation struct {
	at      time.Time
	flux    float64
	mean    float64
	hasFlux bool
	hasMean bool
}

type noaaF107Row struct {
	TimeTag           string          `json:"time_tag"`
	Frequency         json.RawMessage `json:"frequency"`
	Flux              json.RawMessage `json:"flux"`
	ReportingSchedule string          `json:"reporting_schedule"`
	NinetyDayMean     json.RawMessage `json:"ninety_day_mean"`
}

// parseNOAANoonFlux validates components independently. A missing published
// mean must not discard valid Noon flux, and vice versa. Its sorted scratch
// slice is bounded by the caller's response-byte cap and is never retained.
// Exact duplicates collapse. Conflicting values at one timestamp invalidate
// that component, so response ordering cannot choose between contradictory rows.
func parseNOAANoonFlux(body []byte, now time.Time) ([]solarFluxObservation, error) {
	var rows []noaaF107Row
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	series := make([]solarFluxObservation, 0, len(rows))
	for _, row := range rows {
		frequency, ok := nonnegativeJSONNumber(row.Frequency)
		if !ok || frequency != 2800 || row.ReportingSchedule != "Noon" {
			continue
		}
		at, ok := parseNOAATime(row.TimeTag)
		if !ok || at.IsZero() || at.After(now) {
			continue
		}
		flux, hasFlux := nonnegativeJSONNumber(row.Flux)
		mean, hasMean := nonnegativeJSONNumber(row.NinetyDayMean)
		if hasFlux || hasMean {
			series = append(series, solarFluxObservation{at: at, flux: flux, mean: mean, hasFlux: hasFlux, hasMean: hasMean})
		}
	}
	sort.Slice(series, func(i, j int) bool { return series[i].at.Before(series[j].at) })
	return mergeNoonDuplicates(series), nil
}

func mergeNoonDuplicates(series []solarFluxObservation) []solarFluxObservation {
	used := 0
	for start := 0; start < len(series); {
		merged := series[start]
		fluxConflict, meanConflict := false, false
		end := start + 1
		for end < len(series) && series[end].at.Equal(merged.at) {
			row := series[end]
			if row.hasFlux {
				fluxConflict = fluxConflict || (merged.hasFlux && row.flux != merged.flux)
				merged.flux, merged.hasFlux = row.flux, true
			}
			if row.hasMean {
				meanConflict = meanConflict || (merged.hasMean && row.mean != merged.mean)
				merged.mean, merged.hasMean = row.mean, true
			}
			end++
		}
		merged.hasFlux = merged.hasFlux && !fluxConflict
		merged.hasMean = merged.hasMean && !meanConflict
		if merged.hasFlux || merged.hasMean {
			series[used] = merged
			used++
		}
		start = end
	}
	return series[:used]
}

func nonnegativeJSONNumber(raw json.RawMessage) (float64, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil || !finiteNonnegativeFlux(value) {
		return 0, false
	}
	return value, true
}

func finiteNonnegativeFlux(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

// advanceFluxEWMA uses actual elapsed observation time and never rounds an
// intermediate. expm1 avoids cancellation for intervals small versus half-life.
func advanceFluxEWMA(previous, flux float64, elapsed, halfLife time.Duration) float64 {
	alpha := -math.Expm1(-math.Ln2 * (elapsed.Seconds() / halfLife.Seconds()))
	return (1-alpha)*previous + alpha*flux
}

// ssnFromSolarFlux inverts F=63.7+0.728*R+0.00089*R^2. The rationalized form
// avoids subtraction cancellation near the quiet-Sun intercept and avoids a
// doubled numerator. The nonnegative <=63.7 clamp is the product policy; this
// daily surrogate is not an official or calibrated R12 observation.
func ssnFromSolarFlux(flux float64) (int, error) {
	if !finiteNonnegativeFlux(flux) {
		return 0, errors.New("solar flux must be finite and >= 0")
	}
	if flux <= 63.7 {
		return 0, nil
	}
	delta := flux - 63.7
	value := delta / (0.364 + math.Sqrt(0.364*0.364+0.00089*delta))
	rounded := math.Round(value)
	// float64(maxInt) can round upward, so compare against the first excluded
	// power of two rather than permit an overflowing float-to-int conversion.
	if !finiteNonnegativeFlux(rounded) || rounded >= math.Ldexp(1, strconv.IntSize-1) {
		return 0, errors.New("solar-derived SSN exceeds the supported integer range")
	}
	return int(rounded), nil
}
