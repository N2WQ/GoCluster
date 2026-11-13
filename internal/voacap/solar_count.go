package voacap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// countReportSample is a local index of validated reported groups. The JSON
// service does not establish complete disk coverage or an explicit spotless
// encoding; an absent/ambiguous report is never manufactured into zero.
type countReportSample struct {
	at  time.Time
	raw float64
}
type countReportKey struct {
	at                   time.Time
	station, observatory string
}
type countGroup struct {
	spots  int64
	status int64
}
type countReport struct {
	groups  map[string]countGroup
	invalid bool
}

const maxExactCount int64 = 1<<53 - 1

// parseRuntimeSunspotReport reduces only coherent report identities. Obsid is an
// opaque local group identifier, not Region (which may be null or repeated).
// This is an explicit conservative local interpretation of NOAA's JSON, not a
// claim that its undocumented representation is a complete official SSN report.
func parseRuntimeSunspotReport(body []byte, now time.Time) ([]countReportSample, error) {
	if bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		return nil, errors.New("missing sunspot report array")
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	reports := make(map[countReportKey]*countReport)
	competing := make(map[time.Time]map[countReportKey]bool)
	for _, raw := range rows {
		fields, duplicates, err := decodeJSONObject(raw)
		if err != nil {
			return nil, fmt.Errorf("sunspot group: %w", err)
		}
		for _, key := range []string{"time_tag", "Station", "Observatory"} {
			if duplicates[key] {
				return nil, fmt.Errorf("ambiguous sunspot report identity %q", key)
			}
		}
		var timestamp string
		if err = json.Unmarshal(fields["time_tag"], &timestamp); err != nil {
			return nil, errors.New("unassignable sunspot group timestamp")
		}
		at, ok := parseNOAATime(timestamp)
		if !ok || at.Unix() <= 0 {
			return nil, errors.New("unassignable sunspot group timestamp")
		}
		station, ok := opaqueCountIdentity(fields["Station"])
		if !ok {
			return nil, errors.New("unassignable sunspot group station")
		}
		var observatory string
		if err = json.Unmarshal(fields["Observatory"], &observatory); err != nil || strings.TrimSpace(observatory) == "" {
			return nil, errors.New("unassignable sunspot group observatory")
		}
		key := countReportKey{at, station, observatory}
		report := reports[key]
		if report == nil {
			report = &countReport{groups: make(map[string]countGroup)}
			reports[key] = report
			if competing[at] == nil {
				competing[at] = make(map[countReportKey]bool)
			}
			competing[at][key] = true
		}
		if len(duplicates) > 0 {
			report.invalid = true
		}
		id, identified := opaqueCountIdentity(fields["Obsid"])
		spots, validSpots := exactCountInteger(fields["Numspot"])
		status, validStatus := exactCountInteger(fields["Report_Status"])
		var typ string
		typeErr := json.Unmarshal(fields["Type"], &typ)
		if at.After(now) || !identified || !validSpots || !validStatus || status < 1 || status > 3 || typeErr != nil || typ != "spt" {
			report.invalid = true
			continue
		}
		group := countGroup{spots, status}
		if previous, exists := report.groups[id]; exists && previous != group {
			report.invalid = true
		} else {
			report.groups[id] = group
		}
	}
	series := make([]countReportSample, 0, len(reports))
	for key, report := range reports {
		if report.invalid || len(report.groups) == 0 || len(competing[key.at]) != 1 {
			continue
		}
		total := int64(0)
		valid := true
		for _, group := range report.groups {
			if group.spots > maxExactCount-10-total {
				valid = false
				break
			}
			total += 10 + group.spots
		}
		if valid {
			series = append(series, countReportSample{key.at, float64(total)})
		}
	}
	sort.Slice(series, func(i, j int) bool { return series[i].at.Before(series[j].at) })
	if len(series) == 0 {
		return nil, errors.New("sunspot report has no unambiguous usable observations")
	}
	return series, nil
}

// Identifiers are opaque canonical integers or nonempty strings. Numeric/string
// decimal integers share an identity; arbitrary strings are not interpreted.
func opaqueCountIdentity(raw json.RawMessage) (string, bool) {
	if n, ok := exactCountInteger(raw); ok {
		return strconv.FormatInt(n, 10), true
	}
	var s string
	if json.Unmarshal(raw, &s) != nil || strings.TrimSpace(s) == "" {
		return "", false
	}
	// Numeric-looking invalid strings cannot bypass integer validation.
	s = strings.TrimSpace(s)
	if strings.ContainsAny(s[:1], ".+-") || s[0] >= '0' && s[0] <= '9' {
		return "", false
	}
	return s, true
}
func exactCountInteger(raw json.RawMessage) (int64, bool) {
	text := strings.TrimSpace(string(raw))
	if strings.HasPrefix(text, "\"") {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return 0, false
		}
		text = strings.TrimSpace(s)
	}
	if text == "" {
		return 0, false
	}
	for _, c := range text {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	value, err := strconv.ParseInt(text, 10, 64)
	return value, err == nil && value <= maxExactCount
}
func validRawCount(v float64) bool { return finiteNonnegativeCount(v) && v >= 10 && math.Trunc(v) == v }
func finiteNonnegativeCount(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 && v <= float64(maxExactCount)
}
func roundedCountSSN(v float64) (int, error) {
	rounded := math.Round(v)
	if !finiteNonnegativeCount(v) || rounded >= math.Ldexp(1, strconv.IntSize-1) {
		return 0, errors.New("local sunspot index cannot be represented as SSN")
	}
	return int(rounded), nil
}
func advanceCountEWMA(previous, value float64, elapsed, halfLife time.Duration) float64 {
	alpha := -math.Expm1(-math.Ln2 * float64(elapsed) / float64(halfLife))
	return previous + alpha*(value-previous)
}
