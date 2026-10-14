// File role: Owns dated path diagnostic formatting and the logger's weight window.
// Crawler notes: Availability differs from a known zero; cached counts retain
// their publication time. The one-hour window accepts only advancing samples.
// Related docs: data/config/PATH_PREDICTIONS.md, docs/decisions/ADR-0300-path-observation-time-accounting.md.
// Related tests: path_reporting_test.go.
package cluster

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"dxcluster/pathreliability"

	"github.com/dustin/go-humanize"
)

func pathBucketLogLine(snapshot pathreliability.PredictorStatsSnapshot) string {
	var line strings.Builder
	fmt.Fprintf(&line, "Path buckets (5m): available=%t has_asof=%t asof_unix=%d", snapshot.Available && snapshot.HasSnapshot, snapshot.HasSnapshot, snapshot.AsOfUnix)
	if snapshot.HasSnapshot {
		for _, band := range snapshot.Bands {
			fmt.Fprintf(&line, " %s f=%s c=%s", band.Band, humanize.Comma(int64(band.Fine)), humanize.Comma(int64(band.Coarse)))
		}
	}
	return line.String()
}

func pathWeightLogLine(histogram pathreliability.WeightHistogram) string {
	var line strings.Builder
	fmt.Fprintf(&line, "Path weight dist (5m): available=%t has_asof=%t asof_unix=%d", histogram.Available, histogram.Available, histogram.AsOfUnix)
	if histogram.Available {
		// Explicit zero bands distinguish a valid empty distribution from a
		// missing band or failed scan in the existing offline report reader.
		for _, band := range histogram.Bands {
			fmt.Fprintf(&line, " %s t=%s", band.Band, humanize.Comma(int64(band.Total)))
			for i, count := range band.Bins {
				fmt.Fprintf(&line, " %s=%s", weightBinLabel(i, histogram.Edges), humanize.Comma(int64(count)))
			}
		}
	}
	return line.String()
}

type pathGe10Sample struct {
	ts  time.Time
	val int
}

// pathWeightWindow is owned by one five-minute logger goroutine. Bands come
// from the predictor's fixed band list; samples expire after one hour. One
// cursor prevents repeated/older snapshots from becoming new observations.
type pathWeightWindow struct {
	samples     map[string][]pathGe10Sample
	lastAsOf    int64
	hasLastAsOf bool
}

func (window *pathWeightWindow) record(histogram pathreliability.WeightHistogram) bool {
	if !histogram.Available || len(histogram.Edges) == 0 || len(histogram.Bands) == 0 ||
		(window.hasLastAsOf && histogram.AsOfUnix <= window.lastAsOf) {
		return false
	}
	for _, band := range histogram.Bands {
		if band.Band == "" || len(band.Bins) != len(histogram.Edges)+1 {
			return false
		}
		for _, count := range band.Bins {
			if count < 0 {
				return false
			}
		}
	}
	if window.samples == nil {
		window.samples = make(map[string][]pathGe10Sample, len(histogram.Bands))
	}
	at := time.Unix(histogram.AsOfUnix, 0).UTC()
	cutoff := at.Add(-time.Hour)
	for band, samples := range window.samples {
		kept := 0
		for _, sample := range samples {
			if !sample.ts.Before(cutoff) {
				samples[kept] = sample
				kept++
			}
		}
		if kept == 0 {
			delete(window.samples, band)
		} else {
			window.samples[band] = samples[:kept]
		}
	}
	for _, band := range histogram.Bands {
		ge10 := band.Bins[len(band.Bins)-1]
		window.samples[band.Band] = append(window.samples[band.Band], pathGe10Sample{ts: at, val: ge10})
	}
	window.lastAsOf, window.hasLastAsOf = histogram.AsOfUnix, true
	return true
}

func (window *pathWeightWindow) stats(band string) (min, med, p75, max int, ok bool) {
	samples := window.samples[band]
	if len(samples) == 0 {
		return 0, 0, 0, 0, false
	}
	values := make([]int, len(samples))
	for i, sample := range samples {
		values[i] = sample.val
	}
	sort.Ints(values)
	min, max = values[0], values[len(values)-1]
	mid := len(values) / 2
	if len(values)%2 == 1 {
		med = values[mid]
	} else {
		lo, hi := values[mid-1], values[mid]
		med = lo + (hi-lo)/2 + (hi-lo)%2
	}
	if len(values) == 1 {
		p75 = values[0]
	} else {
		position := int(math.Round(0.75 * float64(len(values)-1)))
		p75 = values[position]
	}
	return min, med, p75, max, true
}

func pathVarianceLogLine(histogram pathreliability.WeightHistogram, window *pathWeightWindow) string {
	var line strings.Builder
	fmt.Fprintf(&line, "Path ge10 variance (5m): available=true has_asof=true asof_unix=%d", histogram.AsOfUnix)
	for _, band := range histogram.Bands {
		min, med, p75, max, ok := window.stats(band.Band)
		if !ok {
			continue
		}
		degenerate := 0
		if max == 0 {
			degenerate = 1
		}
		fmt.Fprintf(&line, " %s min=%d med=%d p75=%d max=%d deg=%d", band.Band, min, med, p75, max, degenerate)
	}
	return line.String()
}
