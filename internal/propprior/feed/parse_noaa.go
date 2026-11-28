// File role: NOAA SWPC space-weather JSON parsers for F10.7_obs, RTSW 1-minute
// magnetic field & solar wind, Kyoto Dst mirror, and Geospace 7-day Dst.
// Crawler notes: Parses no-Z ISO-8601 timestamps explicitly as UTC, decodes all
// numeric fields via *float64 so JSON null never becomes 0.0, filters RTSW rows
// by active==true, and drops in-progress/future hourly bins so ring24 never
// evicts the oldest completed hour.
package feed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"dxcluster/internal/propprior"
)

const (
	omniStepSec int64 = 3600

	bzMinValid  = -200.0
	bzMaxValid  = 200.0
	vswMinValid = 150.0
	vswMaxValid = 2500.0
	dstMinValid = -1500.0
	dstMaxValid = 300.0
)

var utcTimeLayouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.000",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05",
}

func parseUTCTimestamp(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	for _, layout := range utcTimeLayouts {
		t, err := time.ParseInLocation(layout, trimmed, time.UTC)
		if err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

type noaaF107Row struct {
	TimeTag           string   `json:"time_tag"`
	Flux              *float64 `json:"flux"`
	ReportingSchedule string   `json:"reporting_schedule"`
}

// ParseNOAAF107 parses NOAA SWPC f107_cm_flux.json (reverse chronological) into
// ascending UTC daily bins (<= floor(now/86400)). For each UTC calendar day it
// prefers reporting_schedule == "Noon" and falls back to the latest valid
// intraday reading only when Noon is absent.
func ParseNOAAF107(body []byte, now time.Time) ([]propprior.Sample, error) {
	var rows []noaaF107Row
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode NOAA F10.7 JSON: %w", err)
	}
	nowDayBin := floorDiv(now.UTC().Unix(), sfiStepSec)

	type dayChoice struct {
		val    float64
		ts     time.Time
		isNoon bool
	}
	byDay := make(map[int64]dayChoice)
	for _, row := range rows {
		if row.Flux == nil {
			continue
		}
		v := *row.Flux
		if !isFinite(v) || v < sfiMinValid || v > sfiMaxValid {
			continue
		}
		ts, ok := parseUTCTimestamp(row.TimeTag)
		if !ok {
			continue
		}
		dayBin := floorDiv(ts.Unix(), sfiStepSec)
		if dayBin > nowDayBin {
			continue
		}
		isNoon := strings.EqualFold(strings.TrimSpace(row.ReportingSchedule), "Noon")
		cur, exists := byDay[dayBin]
		if !exists {
			byDay[dayBin] = dayChoice{val: v, ts: ts, isNoon: isNoon}
			continue
		}
		if isNoon && !cur.isNoon {
			byDay[dayBin] = dayChoice{val: v, ts: ts, isNoon: true}
			continue
		}
		if isNoon == cur.isNoon && ts.After(cur.ts) {
			byDay[dayBin] = dayChoice{val: v, ts: ts, isNoon: isNoon}
		}
	}

	byBin := make(map[int64]float64, len(byDay))
	for dayBin, choice := range byDay {
		byBin[dayBin] = choice.val
	}
	return sortedBinSamples(byBin, sfiStepSec), nil
}

// ParseSFIFallback parses either GFZ columnar Fobs JSON (object) or NOAA
// 10cm-flux-30-day.json (array of objects), returning ascending UTC daily bins
// <= floor(now/86400) plus the detected source tag.
func ParseSFIFallback(body []byte, now time.Time) ([]propprior.Sample, string, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, "", fmt.Errorf("empty SFI fallback payload")
	}
	if trimmed[0] == '{' {
		samples, err := ParseGFZFobs(trimmed, now)
		return samples, "gfz_fobs_fallback", err
	}
	if trimmed[0] == '[' {
		var rows []struct {
			TimeTag string   `json:"time_tag"`
			Flux    *float64 `json:"flux"`
		}
		if err := json.Unmarshal(trimmed, &rows); err != nil {
			return nil, "", fmt.Errorf("decode NOAA 10cm-flux-30-day JSON: %w", err)
		}
		nowDayBin := floorDiv(now.UTC().Unix(), sfiStepSec)
		type dayObs struct {
			val float64
			ts  time.Time
		}
		byDay := make(map[int64]dayObs)
		for _, row := range rows {
			if row.Flux == nil {
				continue
			}
			v := *row.Flux
			if !isFinite(v) || v < sfiMinValid || v > sfiMaxValid {
				continue
			}
			ts, ok := parseUTCTimestamp(row.TimeTag)
			if !ok {
				continue
			}
			dayBin := floorDiv(ts.Unix(), sfiStepSec)
			if dayBin > nowDayBin {
				continue
			}
			if cur, exists := byDay[dayBin]; !exists || ts.After(cur.ts) {
				byDay[dayBin] = dayObs{val: v, ts: ts}
			}
		}
		byBin := make(map[int64]float64, len(byDay))
		for dayBin, obs := range byDay {
			byBin[dayBin] = obs.val
		}
		return sortedBinSamples(byBin, sfiStepSec), "noaa_10cm_fallback", nil
	}
	return nil, "", fmt.Errorf("unexpected SFI fallback JSON token %q", trimmed[0])
}

type rtswMagRow struct {
	TimeTag string   `json:"time_tag"`
	Active  bool     `json:"active"`
	Source  string   `json:"source"`
	BzGSM   *float64 `json:"bz_gsm"`
}

// ParseRTSWMag parses NOAA SWPC rtsw_mag_1m.json, filtering active==true rows,
// deduplicating identical timestamps deterministically, and averaging signed
// Bz_gsm into strictly completed UTC hourly bins (< floor(now/3600)).
func ParseRTSWMag(body []byte, now time.Time) ([]propprior.Sample, error) {
	var rows []rtswMagRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode NOAA RTSW mag JSON: %w", err)
	}
	obs := make([]timestampedObs, 0, len(rows)/2)
	for _, row := range rows {
		if !row.Active || row.BzGSM == nil {
			continue
		}
		v := *row.BzGSM
		if !isFinite(v) || v < bzMinValid || v > bzMaxValid {
			continue
		}
		ts, ok := parseUTCTimestamp(row.TimeTag)
		if !ok {
			continue
		}
		obs = append(obs, timestampedObs{unix: ts.Unix(), source: row.Source, val: v})
	}
	return aggregateCompletedHours(obs, now), nil
}

type rtswWindRow struct {
	TimeTag     string   `json:"time_tag"`
	Active      bool     `json:"active"`
	Source      string   `json:"source"`
	ProtonSpeed *float64 `json:"proton_speed"`
}

// ParseRTSWWind parses NOAA SWPC rtsw_wind_1m.json, filtering active==true rows
// (explicitly dropping null proton_speed rows), deduplicating timestamps
// deterministically, and averaging speed into completed UTC hourly bins.
func ParseRTSWWind(body []byte, now time.Time) ([]propprior.Sample, error) {
	var rows []rtswWindRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode NOAA RTSW wind JSON: %w", err)
	}
	obs := make([]timestampedObs, 0, len(rows)/2)
	for _, row := range rows {
		if !row.Active || row.ProtonSpeed == nil {
			continue
		}
		v := *row.ProtonSpeed
		if !isFinite(v) || v < vswMinValid || v > vswMaxValid {
			continue
		}
		ts, ok := parseUTCTimestamp(row.TimeTag)
		if !ok {
			continue
		}
		obs = append(obs, timestampedObs{unix: ts.Unix(), source: row.Source, val: v})
	}
	return aggregateCompletedHours(obs, now), nil
}

type dstObjectRow struct {
	TimeTag string   `json:"time_tag"`
	Dst     *float64 `json:"dst"`
}

// ParseKyotoDst parses NOAA SWPC kyoto-dst.json in either live object-array
// format or legacy 2D string-array format into ascending, strictly completed
// UTC hourly bins (< floor(now/3600)), rejecting out-of-range and 9999 values.
func ParseKyotoDst(body []byte, now time.Time) ([]propprior.Sample, error) {
	kind, err := detectJSONArrayElementKind(body)
	if err != nil {
		return nil, fmt.Errorf("detect Kyoto Dst schema: %w", err)
	}
	if kind == 0 {
		return nil, nil
	}
	nowHourBin := floorDiv(now.UTC().Unix(), omniStepSec)
	byBin := make(map[int64]float64)

	switch kind {
	case '{':
		var rows []dstObjectRow
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("decode Kyoto Dst object array: %w", err)
		}
		for _, row := range rows {
			if row.Dst == nil {
				continue
			}
			addCompletedDstHour(byBin, row.TimeTag, *row.Dst, nowHourBin)
		}
	case '[':
		var rows [][]json.RawMessage
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("decode Kyoto Dst 2D array: %w", err)
		}
		if len(rows) <= 1 {
			return nil, nil
		}
		timeIdx, dstIdx, err := findHeaderIndices(rows[0], "time_tag", "dst")
		if err != nil {
			return nil, fmt.Errorf("kyoto dst header: %w", err)
		}
		for i := 1; i < len(rows); i++ {
			row := rows[i]
			if timeIdx >= len(row) || dstIdx >= len(row) {
				continue
			}
			tsStr, ok := parseRawString(row[timeIdx])
			if !ok {
				continue
			}
			val, ok := parseRawFloat(row[dstIdx])
			if !ok {
				continue
			}
			addCompletedDstHour(byBin, tsStr, val, nowHourBin)
		}
	default:
		return nil, fmt.Errorf("unexpected Kyoto Dst array element token %q", kind)
	}

	return sortedBinSamples(byBin, omniStepSec), nil
}

// ParseGeospaceDst parses NOAA SWPC geospace_dst_7_day.json (1-minute cadence
// including ~1h future forecast rows), dropping all in-progress and future
// hours (>= floor(now/3600)) and averaging completed UTC hours ascending.
func ParseGeospaceDst(body []byte, now time.Time) ([]propprior.Sample, error) {
	var rows []dstObjectRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode Geospace Dst JSON: %w", err)
	}
	obs := make([]timestampedObs, 0, len(rows))
	for _, row := range rows {
		if row.Dst == nil {
			continue
		}
		v := *row.Dst
		if !isFinite(v) || v < dstMinValid || v > dstMaxValid {
			continue
		}
		ts, ok := parseUTCTimestamp(row.TimeTag)
		if !ok {
			continue
		}
		obs = append(obs, timestampedObs{unix: ts.Unix(), val: v})
	}
	return aggregateCompletedHours(obs, now), nil
}

func addCompletedDstHour(byBin map[int64]float64, rawTime string, dst float64, nowHourBin int64) {
	if !isFinite(dst) || dst < dstMinValid || dst > dstMaxValid {
		return
	}
	ts, ok := parseUTCTimestamp(rawTime)
	if !ok {
		return
	}
	hourBin := floorDiv(ts.Unix(), omniStepSec)
	if hourBin >= nowHourBin {
		return
	}
	byBin[hourBin] = dst
}

type timestampedObs struct {
	unix   int64
	source string
	val    float64
}

func aggregateCompletedHours(obs []timestampedObs, now time.Time) []propprior.Sample {
	if len(obs) == 0 {
		return nil
	}
	nowHourBin := floorDiv(now.UTC().Unix(), omniStepSec)
	// Sort stably by (timestamp, source) before deduplicating identical
	// timestamps so output is deterministic and invariant to upstream order.
	sort.SliceStable(obs, func(i, j int) bool {
		if obs[i].unix != obs[j].unix {
			return obs[i].unix < obs[j].unix
		}
		return obs[i].source < obs[j].source
	})

	type hourAccum struct {
		sum   float64
		count int
	}
	byHour := make(map[int64]hourAccum)
	var prevUnix int64
	hasPrev := false
	for _, o := range obs {
		if hasPrev && o.unix == prevUnix {
			continue
		}
		prevUnix = o.unix
		hasPrev = true
		hourBin := floorDiv(o.unix, omniStepSec)
		if hourBin >= nowHourBin {
			continue
		}
		acc := byHour[hourBin]
		acc.sum += o.val
		acc.count++
		byHour[hourBin] = acc
	}
	byBin := make(map[int64]float64, len(byHour))
	for hourBin, acc := range byHour {
		if acc.count > 0 {
			byBin[hourBin] = acc.sum / float64(acc.count)
		}
	}
	return sortedBinSamples(byBin, omniStepSec)
}
