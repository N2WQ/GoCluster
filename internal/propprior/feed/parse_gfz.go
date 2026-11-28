// File role: GFZ Potsdam columnar JSON parsers (Hp30, Fobs) and NOAA Planetary
// K-index dual-schema parser with 30-minute bin expansion for Hp30 fallback.
// Crawler notes: All numeric JSON fields decode via pointer slices/fields so
// JSON null never aliases valid 0.0 observations.
package feed

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"dxcluster/internal/propprior"
)

const (
	hp30StepSec int64 = 1800
	kpStepSec   int64 = 10800
	sfiStepSec  int64 = 86400

	hp30MinValid = 0.0
	hp30MaxValid = 15.0
	kpMinValid   = 0.0
	kpMaxValid   = 9.0
	sfiMinValid  = 50.0
	sfiMaxValid  = 500.0

	gfzTimeLayout = "2006-01-02T15:04:05Z"
)

// BuildGFZURL builds a bounded-window query URL for the GFZ Potsdam JSON API.
// status=def is intentionally omitted so nowcast observations are included.
func BuildGFZURL(baseURL, index string, startUTC, endUTC time.Time) (string, error) {
	if err := validateHTTPURL(baseURL); err != nil {
		return "", fmt.Errorf("invalid GFZ base URL: %w", err)
	}
	idx := strings.TrimSpace(index)
	if idx == "" {
		return "", errors.New("GFZ index must not be empty")
	}
	if startUTC.IsZero() || endUTC.IsZero() || endUTC.Before(startUTC) {
		return "", errors.New("invalid GFZ query time window")
	}
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("start", startUTC.UTC().Format(gfzTimeLayout))
	q.Set("end", endUTC.UTC().Format(gfzTimeLayout))
	q.Set("index", idx)
	q.Del("status")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// ParseGFZHp30 parses GFZ Potsdam columnar JSON for index=Hp30 into ascending,
// strictly completed 30-minute bins (< floor(now/1800)).
func ParseGFZHp30(body []byte, now time.Time) ([]propprior.Sample, error) {
	return parseGFZColumnar(body, "Hp30", hp30StepSec, hp30MinValid, hp30MaxValid, now, false)
}

// ParseGFZFobs parses GFZ Potsdam columnar JSON for index=Fobs into ascending
// UTC daily bins (<= floor(now/86400), rejecting future days).
func ParseGFZFobs(body []byte, now time.Time) ([]propprior.Sample, error) {
	return parseGFZColumnar(body, "Fobs", sfiStepSec, sfiMinValid, sfiMaxValid, now, true)
}

func parseGFZColumnar(
	body []byte,
	indexKey string,
	stepSec int64,
	minVal, maxVal float64,
	now time.Time,
	allowCurrentBin bool,
) ([]propprior.Sample, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode GFZ %s JSON: %w", indexKey, err)
	}
	dtRaw, ok := raw["datetime"]
	if !ok {
		return nil, fmt.Errorf("GFZ %s payload missing datetime array", indexKey)
	}
	valRaw, ok := raw[indexKey]
	if !ok {
		return nil, fmt.Errorf("GFZ %s payload missing %s array", indexKey, indexKey)
	}
	var timestamps []string
	if err := json.Unmarshal(dtRaw, &timestamps); err != nil {
		return nil, fmt.Errorf("decode GFZ %s datetime array: %w", indexKey, err)
	}
	var values []*float64
	if err := json.Unmarshal(valRaw, &values); err != nil {
		return nil, fmt.Errorf("decode GFZ %s value array: %w", indexKey, err)
	}
	if len(timestamps) != len(values) {
		return nil, fmt.Errorf("GFZ %s array length mismatch: datetime=%d values=%d", indexKey, len(timestamps), len(values))
	}

	nowBin := floorDiv(now.UTC().Unix(), stepSec)
	byBin := make(map[int64]float64, len(timestamps))
	for i := range timestamps {
		if values[i] == nil {
			continue
		}
		v := *values[i]
		if !isFinite(v) || v < minVal || v > maxVal {
			continue
		}
		ts, ok := parseUTCTimestamp(timestamps[i])
		if !ok {
			continue
		}
		bin := floorDiv(ts.Unix(), stepSec)
		if allowCurrentBin {
			if bin > nowBin {
				continue
			}
		} else if bin >= nowBin {
			continue
		}
		byBin[bin] = v
	}
	return sortedBinSamples(byBin, stepSec), nil
}

type noaaKpObjectRow struct {
	TimeTag string   `json:"time_tag"`
	KpUpper *float64 `json:"Kp"`
	KpLower *float64 `json:"kp"`
}

// ParseNOAAKp parses NOAA SWPC noaa-planetary-k-index.json in either live
// array-of-objects format or legacy 2D string-array format, expanding each
// 3-hour Kp bin across its six 30-minute sub-bins strictly < floor(now/1800).
func ParseNOAAKp(body []byte, now time.Time) ([]propprior.Sample, error) {
	kind, err := detectJSONArrayElementKind(body)
	if err != nil {
		return nil, fmt.Errorf("detect NOAA Kp schema: %w", err)
	}
	if kind == 0 {
		return nil, nil
	}

	nowHpBin := floorDiv(now.UTC().Unix(), hp30StepSec)
	byBin := make(map[int64]float64)

	switch kind {
	case '{':
		var rows []noaaKpObjectRow
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("decode NOAA Kp object array: %w", err)
		}
		for _, row := range rows {
			valPtr := row.KpUpper
			if valPtr == nil {
				valPtr = row.KpLower
			}
			if valPtr == nil {
				continue
			}
			addExpandedKpBin(byBin, row.TimeTag, *valPtr, nowHpBin)
		}
	case '[':
		var rows [][]json.RawMessage
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("decode NOAA Kp 2D array: %w", err)
		}
		if len(rows) <= 1 {
			return nil, nil
		}
		timeIdx, kpIdx, err := findHeaderIndices(rows[0], "time_tag", "kp")
		if err != nil {
			return nil, fmt.Errorf("NOAA Kp header: %w", err)
		}
		for i := 1; i < len(rows); i++ {
			row := rows[i]
			if timeIdx >= len(row) || kpIdx >= len(row) {
				continue
			}
			tsStr, ok := parseRawString(row[timeIdx])
			if !ok {
				continue
			}
			val, ok := parseRawFloat(row[kpIdx])
			if !ok {
				continue
			}
			addExpandedKpBin(byBin, tsStr, val, nowHpBin)
		}
	default:
		return nil, fmt.Errorf("unexpected NOAA Kp array element token %q", kind)
	}

	return sortedBinSamples(byBin, hp30StepSec), nil
}

func addExpandedKpBin(byBin map[int64]float64, rawTime string, kp float64, nowHpBin int64) {
	if !isFinite(kp) || kp < kpMinValid || kp > kpMaxValid {
		return
	}
	ts, ok := parseUTCTimestamp(rawTime)
	if !ok {
		return
	}
	// Align to the 3-hour bin start, then expand across its six 30-min sub-bins.
	kpStartUnix := floorDiv(ts.Unix(), kpStepSec) * kpStepSec
	baseHpBin := floorDiv(kpStartUnix, hp30StepSec)
	for sub := range int64(6) {
		hpBin := baseHpBin + sub
		if hpBin >= nowHpBin {
			break
		}
		byBin[hpBin] = kp
	}
}

func detectJSONArrayElementKind(body []byte) (byte, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return 0, errors.New("expected top-level JSON array")
	}
	rest := bytes.TrimSpace(trimmed[1:])
	if len(rest) == 0 {
		return 0, errors.New("truncated JSON array")
	}
	if rest[0] == ']' {
		return 0, nil
	}
	return rest[0], nil
}

func findHeaderIndices(header []json.RawMessage, timeCol, valueCol string) (int, int, error) {
	timeIdx, valIdx := -1, -1
	for i, cell := range header {
		name, ok := parseRawString(cell)
		if !ok {
			continue
		}
		lower := strings.ToLower(strings.TrimSpace(name))
		if lower == timeCol && timeIdx < 0 {
			timeIdx = i
		}
		if lower == valueCol && valIdx < 0 {
			valIdx = i
		}
	}
	if timeIdx < 0 || valIdx < 0 {
		return -1, -1, fmt.Errorf("missing required header columns %q/%q", timeCol, valueCol)
	}
	return timeIdx, valIdx, nil
}

func parseRawString(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return strings.TrimSpace(s), true
}

func parseRawFloat(raw json.RawMessage) (float64, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, false
	}
	var v float64
	if err := json.Unmarshal(trimmed, &v); err == nil {
		return v, isFinite(v)
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err != nil {
		return 0, false
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || !isFinite(v) {
		return 0, false
	}
	return v, true
}

func sortedBinSamples(byBin map[int64]float64, stepSec int64) []propprior.Sample {
	if len(byBin) == 0 {
		return nil
	}
	bins := make([]int64, 0, len(byBin))
	for b := range byBin {
		bins = append(bins, b)
	}
	sort.Slice(bins, func(i, j int) bool { return bins[i] < bins[j] })
	out := make([]propprior.Sample, len(bins))
	for i, b := range bins {
		out[i] = propprior.Sample{
			BinStartUTC: time.Unix(b*stepSec, 0).UTC(),
			Value:       byBin[b],
		}
	}
	return out
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func floorDiv(a, b int64) int64 {
	q := a / b
	r := a % b
	if r != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
