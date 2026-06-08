package voacap

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	NOAASunspotReportURL = "https://services.swpc.noaa.gov/json/sunspot_report.json"

	noaaSunspotReportSource = "NOAA SWPC sunspot_report.json"
)

// SunspotObservation is the grouped near-real-time NOAA report input used for
// VOACAP SSN experiments. It is not the official corrected International
// Sunspot Number; RawWolfEstimate is the local estimate 10*groups+spots.
type SunspotObservation struct {
	ObservedAtUTC   time.Time
	Source          string
	Observatory     string
	Station         string
	GroupCount      int
	SpotCount       int
	RawWolfEstimate int
}

// RollingSunspotAverage describes the moving average ending at Observation.
type RollingSunspotAverage struct {
	Observation      SunspotObservation
	Window           time.Duration
	ObservationCount int
	Average          float64
}

// EWMASunspotAverage is the exponentially weighted average after ingesting one
// new observation at Observation. Alpha records how much weight that observation
// received after applying the elapsed time and configured half-life.
type EWMASunspotAverage struct {
	Observation SunspotObservation
	HalfLife    time.Duration
	Average     float64
	Alpha       float64
	Initialized bool
}

type noaaSunspotRow struct {
	TimeTag     string          `json:"time_tag"`
	Observatory json.RawMessage `json:"Observatory"`
	Station     json.RawMessage `json:"Station"`
	Numspot     json.RawMessage `json:"Numspot"`
}

// ParseNOAASunspotReport groups NOAA sunspot_report.json rows by observation
// time and returns sorted raw Wolf-number estimates. Malformed rows are skipped
// so one bad observatory record does not discard the usable feed.
func ParseNOAASunspotReport(body []byte) ([]SunspotObservation, error) {
	var rows []noaaSunspotRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}

	byTime := make(map[time.Time]*SunspotObservation)
	for _, row := range rows {
		observedAt, ok := parseNOAATime(row.TimeTag)
		if !ok {
			continue
		}
		spots, ok := parseJSONInt(row.Numspot)
		if !ok || spots < 0 {
			continue
		}

		observation := byTime[observedAt]
		if observation == nil {
			observation = &SunspotObservation{
				ObservedAtUTC: observedAt,
				Source:        noaaSunspotReportSource,
				Observatory:   parseJSONScalarString(row.Observatory),
				Station:       parseJSONScalarString(row.Station),
			}
			byTime[observedAt] = observation
		}
		observation.GroupCount++
		observation.SpotCount += spots
		observation.RawWolfEstimate = 10*observation.GroupCount + observation.SpotCount
	}

	series := make([]SunspotObservation, 0, len(byTime))
	for _, observation := range byTime {
		series = append(series, *observation)
	}
	sort.Slice(series, func(i, j int) bool {
		return series[i].ObservedAtUTC.Before(series[j].ObservedAtUTC)
	})
	return series, nil
}

// RollingNOAASunspotAverages computes moving averages over sorted or unsorted
// observations. The window follows the PowerShell experiment's semantics:
// samples are included when start < observed_at <= current_observed_at.
func RollingNOAASunspotAverages(series []SunspotObservation, window time.Duration) ([]RollingSunspotAverage, error) {
	if window <= 0 {
		return nil, errors.New("rolling window must be positive")
	}
	if len(series) == 0 {
		return nil, nil
	}

	ordered := append([]SunspotObservation(nil), series...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ObservedAtUTC.Before(ordered[j].ObservedAtUTC)
	})

	rolling := make([]RollingSunspotAverage, 0, len(ordered))
	start := 0
	sum := 0
	for i, observation := range ordered {
		sum += observation.RawWolfEstimate
		windowStart := observation.ObservedAtUTC.Add(-window)
		for start <= i && !ordered[start].ObservedAtUTC.After(windowStart) {
			sum -= ordered[start].RawWolfEstimate
			start++
		}
		count := i - start + 1
		rolling = append(rolling, RollingSunspotAverage{
			Observation:      observation,
			Window:           window,
			ObservationCount: count,
			Average:          round2(float64(sum) / float64(count)),
		})
	}
	return rolling, nil
}

// LatestNOAASunspotAverage parses the NOAA report body and returns the most
// recent moving average. A false ok value means the body was syntactically valid
// but contained no usable observations.
func LatestNOAASunspotAverage(body []byte, window time.Duration) (RollingSunspotAverage, bool, error) {
	series, err := ParseNOAASunspotReport(body)
	if err != nil {
		return RollingSunspotAverage{}, false, err
	}
	rolling, err := RollingNOAASunspotAverages(series, window)
	if err != nil {
		return RollingSunspotAverage{}, false, err
	}
	if len(rolling) == 0 {
		return RollingSunspotAverage{}, false, nil
	}
	return rolling[len(rolling)-1], true, nil
}

// UpdateSunspotEWMA advances an SSN EWMA only when the caller has a genuinely
// new observation. The first sample initializes the average exactly.
func UpdateSunspotEWMA(previousAverage float64, previousObservedAt time.Time, initialized bool, observation SunspotObservation, halfLife time.Duration) (EWMASunspotAverage, error) {
	if halfLife <= 0 {
		return EWMASunspotAverage{}, errors.New("half-life must be positive")
	}
	if !initialized {
		return EWMASunspotAverage{
			Observation: observation,
			HalfLife:    halfLife,
			Average:     float64(observation.RawWolfEstimate),
			Alpha:       1,
			Initialized: true,
		}, nil
	}

	dt := observation.ObservedAtUTC.Sub(previousObservedAt)
	if dt < 0 {
		dt = 0
	}
	alpha := 1 - math.Pow(0.5, dt.Seconds()/halfLife.Seconds())
	average := previousAverage + alpha*(float64(observation.RawWolfEstimate)-previousAverage)
	return EWMASunspotAverage{
		Observation: observation,
		HalfLife:    halfLife,
		Average:     round2(average),
		Alpha:       alpha,
		Initialized: true,
	}, nil
}

// ShouldRecomputeVOACAP compares the smoothed SSN to the SSN value used for
// the last VOACAP run. A missing prior run always triggers the initial compute.
func ShouldRecomputeVOACAP(smoothedSSN float64, lastVOACAPSSN float64, initialized bool, threshold float64) (bool, float64, error) {
	if threshold < 0 {
		return false, 0, errors.New("threshold must be non-negative")
	}
	if !initialized {
		return true, 0, nil
	}
	if lastVOACAPSSN == 0 {
		if smoothedSSN == 0 {
			return false, 0, nil
		}
		return true, math.Inf(1), nil
	}
	delta := math.Abs(smoothedSSN-lastVOACAPSSN) / math.Abs(lastVOACAPSSN)
	return delta >= threshold, delta, nil
}

func parseNOAATime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		t, err := time.Parse(layout, value)
		if err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func parseJSONInt(raw json.RawMessage) (int, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false
	}
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		if math.Trunc(f) != f {
			return 0, false
		}
		return int(f), true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseJSONScalarString(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return strconv.Itoa(n)
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		if math.Trunc(f) == f {
			return strconv.FormatInt(int64(f), 10)
		}
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return ""
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}
