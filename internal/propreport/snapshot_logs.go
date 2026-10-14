// File role: Validates path snapshot availability and sampling provenance in logs.
// Crawler notes: Modern count/weight rows pair within one emission, then use
// their actual common UTC sample time. Repeated references replace only complete
// bands; legacy logs retain emission-time rules.
// Related docs: docs/decisions/ADR-0301-exact-path-median-and-snapshot-recovery.md.
// Related tests: snapshot_logs_test.go, snapshot_recovery_test.go.
package propreport

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type snapshotLogMetadata struct {
	modern    bool
	valid     bool
	available bool
	hasAsOf   bool
	asOfUnix  int64
}

// parseSnapshotLogMetadata never downgrades a partially marked modern record
// to the legacy grammar. Explicit has_asof separates a valid Unix epoch sample
// from a missing reference; malformed or duplicate fields are unavailable.
func parseSnapshotLogMetadata(line string) snapshotLogMetadata {
	values := make(map[string]string, 3)
	modern, invalid := snapshotMetadataMarker.MatchString(line), false
	for _, field := range strings.Fields(line) {
		name, value, hasEquals := strings.Cut(field, "=")
		normalizedName := strings.ToLower(name)
		switch normalizedName {
		case "available", "has_asof", "asof_unix":
			modern = true
			if _, duplicate := values[normalizedName]; duplicate || !hasEquals || name != normalizedName {
				invalid = true
			}
			values[normalizedName] = value
		}
	}
	if !modern {
		return snapshotLogMetadata{valid: true}
	}
	result := snapshotLogMetadata{modern: true}
	if invalid || len(values) != 3 {
		return result
	}
	if values["available"] != "true" && values["available"] != "false" ||
		values["has_asof"] != "true" && values["has_asof"] != "false" {
		return result
	}
	asOf, err := strconv.ParseInt(values["asof_unix"], 10, 64)
	if err != nil {
		return result
	}
	result.valid = true
	result.available = values["available"] == "true"
	result.hasAsOf = values["has_asof"] == "true"
	result.asOfUnix = asOf
	return result
}

func (m snapshotLogMetadata) sampleTime(date time.Time) (time.Time, bool) {
	if !m.valid || !m.available || !m.hasAsOf {
		return time.Time{}, false
	}
	sample := time.Unix(m.asOfUnix, 0).UTC()
	year, month, day := sample.Date()
	wantYear, wantMonth, wantDay := date.UTC().Date()
	return sample, year == wantYear && month == wantMonth && day == wantDay
}

var (
	snapshotMetadataMarker = regexp.MustCompile(`(?i)\b(?:available|has_asof|asof_unix)\b`)
	snapshotBandToken      = regexp.MustCompile(`(?:^|\s)(\d+\.?\d*cm|\d+m)(?:\s|$)`)
	snapshotCountRe        = regexp.MustCompile(`^(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)$`)
)

// parseSnapshotBandFields validates an entire modern band payload. Missing,
// duplicate, overflowing, negative, or trailing partial values stay absent
// instead of becoming quantitative zeros. Allocation is bounded by log input,
// and no parsed records are retained beyond this report generation.
func parseSnapshotBandFields(line, prefix string, names []string) map[string][]int {
	_, payload, found := strings.Cut(line, prefix)
	if !found {
		return nil
	}
	tokens := snapshotBandToken.FindAllStringSubmatchIndex(payload, -1)
	result := make(map[string][]int, len(tokens))
	invalidBands := make(map[string]bool)
	for i, token := range tokens {
		band := payload[token[2]:token[3]]
		if _, duplicate := result[band]; duplicate || invalidBands[band] {
			delete(result, band)
			invalidBands[band] = true
			continue
		}
		end := len(payload)
		if i+1 < len(tokens) {
			end = tokens[i+1][0]
		}
		fields := strings.Fields(payload[token[1]:end])
		if len(fields) != len(names) {
			invalidBands[band] = true
			continue
		}
		values := make([]int, len(names))
		valid := true
		for j, field := range fields {
			value, found := strings.CutPrefix(field, names[j]+"=")
			if !found || !snapshotCountRe.MatchString(value) {
				valid = false
				break
			}
			parsed, err := strconv.Atoi(strings.ReplaceAll(value, ",", ""))
			if err != nil {
				valid = false
				break
			}
			values[j] = parsed
		}
		if valid {
			result[band] = values
		} else {
			invalidBands[band] = true
		}
	}
	return result
}

func parseModernSnapshotWeights(line string) map[string]weightBins {
	fields := parseSnapshotBandFields(line, "Path weight dist (5m):",
		[]string{"t", "<1", "1-2", "2-3", "3-5", "5-10", ">=10"})
	result := make(map[string]weightBins, len(fields))
	for band, values := range fields {
		remaining := values[0]
		consistent := true
		for _, count := range values[1:] {
			if count > remaining {
				consistent = false
				break
			}
			remaining -= count
		}
		if consistent && remaining == 0 {
			result[band] = weightBins{Total: values[0], Lt1: values[1], Ge10: values[6]}
		}
	}
	return result
}

type pathSnapshotEmission struct {
	sequence       int
	emittedAt      string
	hasCounts      bool
	hasWeights     bool
	countMetadata  snapshotLogMetadata
	weightMetadata snapshotLogMetadata
	counts         map[string]int
	weights        map[string]weightBins
}

type pathSnapshotSample struct {
	hour    int
	counts  map[string]int
	weights map[string]weightBins
}

// collectPathSnapshotSamples pairs records before deduplicating their sample
// reference. Pair identity is the logger's shared emission prefix, not AsOf:
// independent emissions at an equal reference cannot supply each other's
// missing fields. Scratch maps live only for this finite input log.
func collectPathSnapshotSamples(entries []string, date time.Time) (map[string]map[int][]hourStat, map[int]map[string][]ge10Variance) {
	emissions := make(map[string]*pathSnapshotEmission)
	varianceHours := make(map[int]map[string][]ge10Variance)
	modernVariance := make(map[int64]map[string]ge10Variance)
	for sequence, entry := range entries {
		if match := bucketsRe.FindStringSubmatch(entry); len(match) == 2 {
			emission := snapshotEmission(emissions, match[1], sequence)
			emission.hasCounts = true
			emission.countMetadata = parseSnapshotLogMetadata(entry)
			emission.counts = parseSnapshotCounts(entry, emission.countMetadata.modern)
			continue
		}
		if match := weightsRe.FindStringSubmatch(entry); len(match) == 2 {
			emission := snapshotEmission(emissions, match[1], sequence)
			emission.hasWeights = true
			emission.weightMetadata = parseSnapshotLogMetadata(entry)
			emission.weights = parseSnapshotWeights(entry, emission.weightMetadata.modern)
			continue
		}
		if match := ge10VarRe.FindStringSubmatch(entry); len(match) == 2 {
			metadata := parseSnapshotLogMetadata(entry)
			if metadata.modern {
				variance := parseSnapshotVariance(entry)
				if _, usable := metadata.sampleTime(date); usable && len(variance) > 0 {
					if previous := modernVariance[metadata.asOfUnix]; previous != nil {
						for band, value := range variance {
							previous[band] = value
						}
					} else {
						modernVariance[metadata.asOfUnix] = variance
					}
				}
			}
		}
	}

	ordered := make([]*pathSnapshotEmission, 0, len(emissions))
	for _, emission := range emissions {
		ordered = append(ordered, emission)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].sequence < ordered[j].sequence })
	bandHours := make(map[string]map[int][]hourStat)
	modernSamples := make(map[int64]pathSnapshotSample)
	for _, emission := range ordered {
		if !emission.hasCounts {
			continue
		}
		if emission.countMetadata.modern || emission.weightMetadata.modern {
			if sample, usable := modernSnapshotPair(emission, date); usable {
				asOf := emission.countMetadata.asOfUnix
				if previous, present := modernSamples[asOf]; present {
					// Merge complete pairs only. Independent count/weight merging
					// would fabricate evidence from different emission prefixes.
					for band, count := range sample.counts {
						previous.counts[band] = count
						previous.weights[band] = sample.weights[band]
					}
				} else {
					modernSamples[asOf] = sample
				}
			}
			continue
		}
		emittedAt, err := time.Parse("2006/01/02 15:04:05", emission.emittedAt)
		if err == nil {
			// Legacy logs omit zero-weight bands and have no availability contract.
			appendSnapshotBandHours(bandHours, emittedAt.Hour(), emission.counts, emission.weights, true)
		}
	}
	sampleTimes := make([]int64, 0, len(modernSamples))
	for asOf := range modernSamples {
		sampleTimes = append(sampleTimes, asOf)
	}
	sort.Slice(sampleTimes, func(i, j int) bool { return sampleTimes[i] < sampleTimes[j] })
	for _, asOf := range sampleTimes {
		sample := modernSamples[asOf]
		appendSnapshotBandHours(bandHours, sample.hour, sample.counts, sample.weights, false)
	}
	varianceTimes := make([]int64, 0, len(modernVariance))
	for asOf := range modernVariance {
		varianceTimes = append(varianceTimes, asOf)
	}
	sort.Slice(varianceTimes, func(i, j int) bool { return varianceTimes[i] < varianceTimes[j] })
	for _, asOf := range varianceTimes {
		appendSnapshotVariance(varianceHours, time.Unix(asOf, 0).UTC().Hour(), modernVariance[asOf])
	}
	return bandHours, varianceHours
}

func snapshotEmission(emissions map[string]*pathSnapshotEmission, emittedAt string, sequence int) *pathSnapshotEmission {
	emission := emissions[emittedAt]
	if emission == nil {
		emission = &pathSnapshotEmission{emittedAt: emittedAt}
		emissions[emittedAt] = emission
	}
	emission.sequence = sequence
	return emission
}

func parseSnapshotCounts(line string, modern bool) map[string]int {
	result := make(map[string]int)
	if modern {
		for band, fields := range parseSnapshotBandFields(line, "Path buckets (5m):", []string{"f", "c"}) {
			result[band] = fields[0]
		}
		return result
	}
	for _, match := range bandBuckets.FindAllStringSubmatch(line, -1) {
		result[match[1]] = parseInt(match[2])
	}
	return result
}

func parseSnapshotWeights(line string, modern bool) map[string]weightBins {
	if modern {
		return parseModernSnapshotWeights(line)
	}
	result := make(map[string]weightBins)
	for _, match := range bandWeights.FindAllStringSubmatch(line, -1) {
		result[match[1]] = weightBins{Total: parseInt(match[2]), Lt1: parseInt(match[3]), Ge10: parseInt(match[8])}
	}
	return result
}

func modernSnapshotPair(emission *pathSnapshotEmission, date time.Time) (pathSnapshotSample, bool) {
	counts, weights := emission.countMetadata, emission.weightMetadata
	if !emission.hasWeights || !counts.modern || !weights.modern || counts.asOfUnix != weights.asOfUnix {
		return pathSnapshotSample{}, false
	}
	sampleTime, usable := counts.sampleTime(date)
	if _, weightUsable := weights.sampleTime(date); !usable || !weightUsable {
		return pathSnapshotSample{}, false
	}
	usableCounts := make(map[string]int)
	for band, count := range emission.counts {
		if _, present := emission.weights[band]; !present {
			continue
		}
		if _, supported := allowedBands[band]; supported {
			usableCounts[band] = count
		}
	}
	return pathSnapshotSample{hour: sampleTime.Hour(), counts: usableCounts, weights: emission.weights}, len(usableCounts) > 0
}

func appendSnapshotBandHours(destination map[string]map[int][]hourStat, hour int, counts map[string]int, weights map[string]weightBins, legacy bool) {
	for band, count := range counts {
		if _, supported := allowedBands[band]; !supported {
			continue
		}
		if destination[band] == nil {
			destination[band] = make(map[int][]hourStat)
		}
		weight := weights[band]
		destination[band][hour] = append(destination[band][hour], hourStat{
			Hour: fmt.Sprintf("%02d:00", hour), FMed: count, Ge10Med: weight.Ge10, Lt1Med: weight.Lt1,
			legacyVarianceAssumed: legacy,
		})
	}
}

func parseSnapshotVariance(line string) map[string]ge10Variance {
	result := make(map[string]ge10Variance)
	fields := parseSnapshotBandFields(line, "Path ge10 variance (5m):", []string{"min", "med", "p75", "max", "deg"})
	for band, values := range fields {
		// The publisher defines degeneracy exactly as max == 0. Contradictory
		// flags are unavailable evidence, rather than measurements of zero.
		if values[0] <= values[1] && values[1] <= values[2] && values[2] <= values[3] &&
			values[4] <= 1 && (values[4] == 1) == (values[3] == 0) {
			result[band] = ge10Variance{Min: values[0], Med: values[1], P75: values[2], Max: values[3], Deg: values[4] == 1}
		}
	}
	return result
}

func appendSnapshotVariance(destination map[int]map[string][]ge10Variance, hour int, variance map[string]ge10Variance) {
	if destination[hour] == nil {
		destination[hour] = make(map[string][]ge10Variance)
	}
	for band, value := range variance {
		destination[hour][band] = append(destination[hour][band], value)
	}
}
