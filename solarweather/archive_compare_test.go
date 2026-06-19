package solarweather

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	phase1ArchiveSpotPrefix = "s|"
	phase1ArchiveSpotKeyLen = len(phase1ArchiveSpotPrefix) + 8 + 4

	phase1ArchiveRecordVersionV2         = 2
	phase1ArchiveRecordVersionV3         = 3
	phase1ArchiveRecordVersionV4         = 4
	phase1ArchiveRecordVersion           = 5
	phase1ArchiveRecordFixedHeaderSizeV2 = 28
	phase1ArchiveRecordFixedHeaderSize   = 36
)

const (
	phase1ArchiveFieldDXCall = iota
	phase1ArchiveFieldDECall
	phase1ArchiveFieldDECallStripped
	phase1ArchiveFieldMode
	phase1ArchiveFieldComment
	phase1ArchiveFieldSource
	phase1ArchiveFieldSourceNode
	phase1ArchiveFieldConfidence
	phase1ArchiveFieldBand
	phase1ArchiveFieldDXGrid
	phase1ArchiveFieldDEGrid
	phase1ArchiveFieldDXCont
	phase1ArchiveFieldDECont
	phase1ArchiveFieldEvents
	phase1ArchiveFieldToxicityStatus
	phase1ArchiveFieldToxicityCategories
	phase1ArchiveFieldToxicityModel
	phase1ArchiveFieldCount
)

const (
	phase1ArchiveFieldCountV3       = phase1ArchiveFieldEvents
	phase1ArchiveFieldCountV4       = phase1ArchiveFieldEvents + 1
	phase1ArchiveRecordHeaderSizeV2 = phase1ArchiveRecordFixedHeaderSizeV2 + phase1ArchiveFieldCountV3*2
	phase1ArchiveRecordHeaderSizeV3 = phase1ArchiveRecordFixedHeaderSize + phase1ArchiveFieldCountV3*2
	phase1ArchiveRecordHeaderSizeV4 = phase1ArchiveRecordFixedHeaderSize + phase1ArchiveFieldCountV4*2
	phase1ArchiveRecordHeaderSize   = phase1ArchiveRecordFixedHeaderSize + phase1ArchiveFieldCount*2
)

func TestPhase1ArchiveSolarComparison(t *testing.T) {
	archivePath := strings.TrimSpace(os.Getenv("PHASE1_SOLAR_ARCHIVE_PATH"))
	if archivePath == "" {
		t.Skip("set PHASE1_SOLAR_ARCHIVE_PATH to run archive-backed phase 1 solar comparison")
	}

	opts := phase1ArchiveCompareOptions{
		ArchivePath: archivePath,
		OutDir:      phase1RepoRelativeOutDir(phase1EnvString("PHASE1_SOLAR_OUT_DIR", filepath.Join("tmp", "phase1-solar-comparison"))),
		Band:        phase1NormalizeBand(phase1EnvString("PHASE1_SOLAR_BAND", "160m")),
		MaxScan:     phase1EnvInt("PHASE1_SOLAR_MAX_SCAN", 200000),
		MaxCases:    phase1EnvInt("PHASE1_SOLAR_MAX_CASES", 50000),
		WriteRows:   phase1EnvBool("PHASE1_SOLAR_WRITE_ROWS", false),
		TopN:        phase1EnvInt("PHASE1_SOLAR_TOP_N", 25),
	}
	if opts.TopN < 0 {
		opts.TopN = 0
	}
	report, rows, err := phase1RunArchiveSolarComparison(opts)
	if err != nil {
		t.Fatalf("archive solar comparison: %v", err)
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	summaryPath := filepath.Join(opts.OutDir, "archive-solar-summary-"+stamp+".json")
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	if err := os.WriteFile(summaryPath, raw, 0o644); err != nil {
		t.Fatalf("write summary: %v", err)
	}
	t.Logf("summary=%s", summaryPath)
	if opts.WriteRows {
		rowsPath := filepath.Join(opts.OutDir, "archive-solar-rows-"+stamp+".csv")
		if err := phase1WriteArchiveRowsCSV(rowsPath, rows); err != nil {
			t.Fatalf("write rows csv: %v", err)
		}
		t.Logf("rows=%s", rowsPath)
	}
	t.Logf("scanned=%d decoded=%d usable=%d unique_paths=%d band=%q",
		report.Scanned, report.Decoded, report.Usable, report.UniquePaths, report.Band)
	t.Logf("daylight_abs_error mean=%.4f max=%.4f", report.DaylightAbsMean, report.DaylightAbsMax)
	t.Logf("dark_abs_error mean=%.4f max=%.4f", report.DarkAbsMean, report.DarkAbsMax)
	for _, threshold := range report.Thresholds {
		t.Logf("dark>=%.2f mismatch=%d/%d analytic_only=%d sample9_only=%d both_dark=%d both_not_dark=%d",
			threshold.Threshold,
			threshold.Mismatch,
			report.Usable,
			threshold.AnalyticOnly,
			threshold.SampleOnly,
			threshold.BothDark,
			threshold.BothNotDark,
		)
	}
}

type phase1ArchiveCompareOptions struct {
	ArchivePath string
	OutDir      string
	Band        string
	MaxScan     int
	MaxCases    int
	WriteRows   bool
	TopN        int
}

type phase1ArchiveCompareReport struct {
	GeneratedAtUTC   string                  `json:"generated_at_utc"`
	ArchivePath      string                  `json:"archive_path"`
	Band             string                  `json:"band"`
	MaxScan          int                     `json:"max_scan"`
	MaxCases         int                     `json:"max_cases"`
	Scanned          int                     `json:"scanned"`
	Decoded          int                     `json:"decoded"`
	DecodeErrors     int                     `json:"decode_errors"`
	FirstDecodeError string                  `json:"first_decode_error,omitempty"`
	BandSkipped      int                     `json:"band_skipped"`
	MissingGrid      int                     `json:"missing_grid"`
	InvalidGrid      int                     `json:"invalid_grid"`
	Unknown          int                     `json:"unknown"`
	Usable           int                     `json:"usable"`
	UniquePaths      int                     `json:"unique_paths"`
	EarliestUTC      string                  `json:"earliest_utc,omitempty"`
	LatestUTC        string                  `json:"latest_utc,omitempty"`
	DaylightAbsMean  float64                 `json:"daylight_abs_mean"`
	DaylightAbsMax   float64                 `json:"daylight_abs_max"`
	DarkAbsMean      float64                 `json:"dark_abs_mean"`
	DarkAbsMax       float64                 `json:"dark_abs_max"`
	Thresholds       []phase1ThresholdReport `json:"thresholds"`
	DistanceBuckets  []phase1BucketReport    `json:"distance_buckets"`
	TopDisagreements []phase1ArchiveCaseRow  `json:"top_disagreements"`
}

type phase1ThresholdReport struct {
	Threshold    float64 `json:"threshold"`
	BothDark     int     `json:"both_dark"`
	BothNotDark  int     `json:"both_not_dark"`
	AnalyticOnly int     `json:"analytic_only"`
	SampleOnly   int     `json:"sample9_only"`
	Mismatch     int     `json:"mismatch"`
}

type phase1BucketReport struct {
	Bucket          string  `json:"bucket"`
	Cases           int     `json:"cases"`
	DaylightAbsMean float64 `json:"daylight_abs_mean"`
	DaylightAbsMax  float64 `json:"daylight_abs_max"`
	DarkAbsMean     float64 `json:"dark_abs_mean"`
	DarkAbsMax      float64 `json:"dark_abs_max"`
	Mismatch50      int     `json:"mismatch_dark_50"`
	Mismatch75      int     `json:"mismatch_dark_75"`
	Mismatch90      int     `json:"mismatch_dark_90"`
}

type phase1ArchiveCaseRow struct {
	TimeUTC              string  `json:"time_utc"`
	Band                 string  `json:"band"`
	DECall               string  `json:"de_call"`
	DXCall               string  `json:"dx_call"`
	DEGrid               string  `json:"de_grid"`
	DXGrid               string  `json:"dx_grid"`
	DistanceKM           float64 `json:"distance_km"`
	AnalyticDaylightFrac float64 `json:"analytic_daylight_fraction"`
	AnalyticDarkFrac     float64 `json:"analytic_dark_fraction"`
	Sample9DaylightFrac  float64 `json:"sample9_daylight_fraction"`
	Sample9DarkFrac      float64 `json:"sample9_dark_fraction"`
	Sample9MinElevation  float64 `json:"sample9_min_elevation_deg"`
	Sample9MaxElevation  float64 `json:"sample9_max_elevation_deg"`
	DarkAbsDelta         float64 `json:"dark_abs_delta"`
	DaylightAbsDelta     float64 `json:"daylight_abs_delta"`
}

type phase1ArchiveSpot struct {
	At      time.Time
	Band    string
	DECall  string
	DXCall  string
	DEGrid  string
	DXGrid  string
	FreqKHz float64
}

func phase1RunArchiveSolarComparison(opts phase1ArchiveCompareOptions) (phase1ArchiveCompareReport, []phase1ArchiveCaseRow, error) {
	if opts.MaxScan <= 0 {
		opts.MaxScan = 200000
	}
	if opts.MaxCases <= 0 {
		opts.MaxCases = 50000
	}
	if opts.Band == "" || opts.Band == "all" {
		opts.Band = "all"
	}

	db, err := pebble.Open(opts.ArchivePath, &pebble.Options{ReadOnly: true})
	if err != nil {
		return phase1ArchiveCompareReport{}, nil, fmt.Errorf("open read-only pebble: %w", err)
	}
	defer db.Close()

	lower := []byte(phase1ArchiveSpotPrefix)
	upper := phase1PrefixUpperBound(lower)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return phase1ArchiveCompareReport{}, nil, fmt.Errorf("archive iterator: %w", err)
	}
	defer iter.Close()

	horizonCfg := DefaultConfig()
	horizonCfg.Enabled = true
	horizonCfg.Sun.TwilightDegrees = 0
	horizonCfg.normalize()
	civilCfg := horizonCfg
	civilCfg.Sun.TwilightDegrees = 6

	report := phase1ArchiveCompareReport{
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		ArchivePath:    opts.ArchivePath,
		Band:           opts.Band,
		MaxScan:        opts.MaxScan,
		MaxCases:       opts.MaxCases,
		Thresholds: []phase1ThresholdReport{
			{Threshold: 0.50},
			{Threshold: 0.75},
			{Threshold: 0.90},
		},
	}
	stats := phase1CompareStats{}
	paths := make(map[string]struct{})
	buckets := map[string]*phase1CompareStats{}
	var rows []phase1ArchiveCaseRow
	topRows := make([]phase1ArchiveCaseRow, 0, opts.TopN+1)
	var earliest time.Time
	var latest time.Time

	for ok := iter.Last(); ok && report.Scanned < opts.MaxScan && report.Usable < opts.MaxCases; ok = iter.Prev() {
		report.Scanned++
		ts, ok := phase1ParseArchiveSpotKey(iter.Key())
		if !ok {
			continue
		}
		rec, err := phase1DecodeArchiveSpot(ts, iter.Value())
		if err != nil {
			report.DecodeErrors++
			if report.FirstDecodeError == "" {
				report.FirstDecodeError = err.Error()
			}
			continue
		}
		report.Decoded++
		if opts.Band != "all" && rec.Band != opts.Band {
			report.BandSkipped++
			continue
		}
		if rec.DEGrid == "" || rec.DXGrid == "" {
			report.MissingGrid++
			continue
		}
		deVec, ok := gridVector(rec.DEGrid)
		if !ok {
			report.InvalidGrid++
			continue
		}
		dxVec, ok := gridVector(rec.DXGrid)
		if !ok {
			report.InvalidGrid++
			continue
		}

		sun := SunVectorECEF(rec.At)
		analytic := phase1AnalyticExposure(deVec, dxVec, sun, horizonCfg, civilCfg)
		sample9 := phase1Sample9Exposure(deVec, dxVec, sun)
		stats.observe(analytic, sample9)
		if analytic.Unknown {
			report.Unknown++
			continue
		}

		report.Usable++
		if earliest.IsZero() || rec.At.Before(earliest) {
			earliest = rec.At
		}
		if latest.IsZero() || rec.At.After(latest) {
			latest = rec.At
		}
		pathKey := rec.Band + "|" + rec.DEGrid + "|" + rec.DXGrid
		paths[pathKey] = struct{}{}

		for i := range report.Thresholds {
			phase1ObserveThreshold(&report.Thresholds[i], analytic.DarkFraction, sample9.DarkFraction)
		}

		row := phase1ArchiveCaseRow{
			TimeUTC:              rec.At.UTC().Format(time.RFC3339),
			Band:                 rec.Band,
			DECall:               rec.DECall,
			DXCall:               rec.DXCall,
			DEGrid:               rec.DEGrid,
			DXGrid:               rec.DXGrid,
			DistanceKM:           angleBetween(deVec, dxVec) * 6371.0,
			AnalyticDaylightFrac: analytic.DaylightFraction,
			AnalyticDarkFrac:     analytic.DarkFraction,
			Sample9DaylightFrac:  sample9.DaylightFraction,
			Sample9DarkFrac:      sample9.DarkFraction,
			Sample9MinElevation:  sample9.MinElevationDeg,
			Sample9MaxElevation:  sample9.MaxElevationDeg,
			DarkAbsDelta:         math.Abs(analytic.DarkFraction - sample9.DarkFraction),
			DaylightAbsDelta:     math.Abs(analytic.DaylightFraction - sample9.DaylightFraction),
		}
		bucket := phase1DistanceBucket(row.DistanceKM)
		if buckets[bucket] == nil {
			buckets[bucket] = &phase1CompareStats{}
		}
		buckets[bucket].observe(analytic, sample9)
		if opts.WriteRows {
			rows = append(rows, row)
		}
		topRows = append(topRows, row)
		sort.Slice(topRows, func(i, j int) bool {
			if topRows[i].DarkAbsDelta == topRows[j].DarkAbsDelta {
				return topRows[i].DaylightAbsDelta > topRows[j].DaylightAbsDelta
			}
			return topRows[i].DarkAbsDelta > topRows[j].DarkAbsDelta
		})
		if len(topRows) > opts.TopN {
			topRows = topRows[:opts.TopN]
		}
	}
	if err := iter.Error(); err != nil {
		return phase1ArchiveCompareReport{}, nil, fmt.Errorf("archive iterate: %w", err)
	}

	report.UniquePaths = len(paths)
	if !earliest.IsZero() {
		report.EarliestUTC = earliest.UTC().Format(time.RFC3339)
	}
	if !latest.IsZero() {
		report.LatestUTC = latest.UTC().Format(time.RFC3339)
	}
	report.DaylightAbsMean = stats.meanDaylightAbs()
	report.DaylightAbsMax = stats.maxDaylightAbs
	report.DarkAbsMean = stats.meanDarkAbs()
	report.DarkAbsMax = stats.maxDarkAbs
	report.TopDisagreements = topRows
	for _, name := range []string{"0-1000", "1000-3000", "3000-7000", "7000-12000", "12000+"} {
		if stat := buckets[name]; stat != nil {
			report.DistanceBuckets = append(report.DistanceBuckets, phase1BucketReport{
				Bucket:          name,
				Cases:           stat.cases - stat.unknown,
				DaylightAbsMean: stat.meanDaylightAbs(),
				DaylightAbsMax:  stat.maxDaylightAbs,
				DarkAbsMean:     stat.meanDarkAbs(),
				DarkAbsMax:      stat.maxDarkAbs,
				Mismatch50:      stat.darkMismatch50,
				Mismatch75:      stat.darkMismatch75,
				Mismatch90:      stat.darkMismatch90,
			})
		}
	}
	return report, rows, nil
}

func phase1ObserveThreshold(report *phase1ThresholdReport, analyticDark, sampleDark float64) {
	analytic := phase1AtLeast(analyticDark, report.Threshold)
	sampled := phase1AtLeast(sampleDark, report.Threshold)
	switch {
	case analytic && sampled:
		report.BothDark++
	case !analytic && !sampled:
		report.BothNotDark++
	case analytic:
		report.AnalyticOnly++
		report.Mismatch++
	default:
		report.SampleOnly++
		report.Mismatch++
	}
}

func phase1WriteArchiveRowsCSV(path string, rows []phase1ArchiveCaseRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"time_utc",
		"band",
		"de_call",
		"dx_call",
		"de_grid",
		"dx_grid",
		"distance_km",
		"analytic_daylight_fraction",
		"analytic_dark_fraction",
		"sample9_daylight_fraction",
		"sample9_dark_fraction",
		"sample9_min_elevation_deg",
		"sample9_max_elevation_deg",
		"dark_abs_delta",
		"daylight_abs_delta",
	}); err != nil {
		return err
	}
	for _, row := range rows {
		if err := w.Write([]string{
			row.TimeUTC,
			row.Band,
			row.DECall,
			row.DXCall,
			row.DEGrid,
			row.DXGrid,
			phase1FormatFloat(row.DistanceKM),
			phase1FormatFloat(row.AnalyticDaylightFrac),
			phase1FormatFloat(row.AnalyticDarkFrac),
			phase1FormatFloat(row.Sample9DaylightFrac),
			phase1FormatFloat(row.Sample9DarkFrac),
			phase1FormatFloat(row.Sample9MinElevation),
			phase1FormatFloat(row.Sample9MaxElevation),
			phase1FormatFloat(row.DarkAbsDelta),
			phase1FormatFloat(row.DaylightAbsDelta),
		}); err != nil {
			return err
		}
	}
	return w.Error()
}

func phase1DecodeArchiveSpot(ts int64, raw []byte) (phase1ArchiveSpot, error) {
	if len(raw) < phase1ArchiveRecordHeaderSizeV2 {
		return phase1ArchiveSpot{}, fmt.Errorf("invalid archive record: len=%d head=%s", len(raw), phase1HeadHex(raw))
	}
	fixedHeaderSize, headerSize, fieldN, ok := phase1ArchiveRecordLayout(raw[0])
	if !ok || len(raw) < headerSize {
		return phase1ArchiveSpot{}, fmt.Errorf("invalid archive record: version=%d len=%d header=%d head=%s", raw[0], len(raw), headerSize, phase1HeadHex(raw))
	}
	freq := math.Float64frombits(binary.BigEndian.Uint64(raw[4:]))
	offset := fixedHeaderSize
	lengths := [phase1ArchiveFieldCount]int{}
	for i := 0; i < fieldN; i++ {
		lengths[i] = int(binary.BigEndian.Uint16(raw[offset:]))
		offset += 2
	}
	dataOffset := headerSize
	fields := [phase1ArchiveFieldCount]string{}
	for i := 0; i < fieldN; i++ {
		l := lengths[i]
		if l == 0 {
			continue
		}
		if dataOffset+l > len(raw) {
			return phase1ArchiveSpot{}, fmt.Errorf("invalid archive record: field=%d len=%d need=%d fixed=%d header=%d lengths=%s head=%s", i, len(raw), dataOffset+l, fixedHeaderSize, headerSize, phase1LengthPreview(raw, fixedHeaderSize, fieldN), phase1HeadHex(raw))
		}
		fields[i] = string(raw[dataOffset : dataOffset+l])
		dataOffset += l
	}
	if dataOffset != len(raw) {
		return phase1ArchiveSpot{}, fmt.Errorf("invalid archive record: data_offset=%d len=%d fixed=%d header=%d lengths=%s head=%s", dataOffset, len(raw), fixedHeaderSize, headerSize, phase1LengthPreview(raw, fixedHeaderSize, fieldN), phase1HeadHex(raw))
	}
	band := phase1NormalizeBand(fields[phase1ArchiveFieldBand])
	if band == "" {
		band = phase1FreqToBand(freq)
	}
	return phase1ArchiveSpot{
		At:      time.Unix(0, ts).UTC(),
		Band:    band,
		DECall:  strings.TrimSpace(fields[phase1ArchiveFieldDECall]),
		DXCall:  strings.TrimSpace(fields[phase1ArchiveFieldDXCall]),
		DEGrid:  strings.TrimSpace(fields[phase1ArchiveFieldDEGrid]),
		DXGrid:  strings.TrimSpace(fields[phase1ArchiveFieldDXGrid]),
		FreqKHz: freq,
	}, nil
}

func phase1ArchiveRecordLayout(version byte) (fixedHeaderSize int, headerSize int, fieldN int, ok bool) {
	switch version {
	case phase1ArchiveRecordVersionV2:
		return phase1ArchiveRecordFixedHeaderSizeV2, phase1ArchiveRecordHeaderSizeV2, phase1ArchiveFieldCountV3, true
	case phase1ArchiveRecordVersionV3:
		return phase1ArchiveRecordFixedHeaderSize, phase1ArchiveRecordHeaderSizeV3, phase1ArchiveFieldCountV3, true
	case phase1ArchiveRecordVersionV4:
		return phase1ArchiveRecordFixedHeaderSize, phase1ArchiveRecordHeaderSizeV4, phase1ArchiveFieldCountV4, true
	case phase1ArchiveRecordVersion:
		return phase1ArchiveRecordFixedHeaderSize, phase1ArchiveRecordHeaderSize, phase1ArchiveFieldCount, true
	default:
		return 0, 0, 0, false
	}
}

func phase1ParseArchiveSpotKey(key []byte) (int64, bool) {
	if len(key) != phase1ArchiveSpotKeyLen || !bytes.HasPrefix(key, []byte(phase1ArchiveSpotPrefix)) {
		return 0, false
	}
	ts := int64(binary.BigEndian.Uint64(key[len(phase1ArchiveSpotPrefix):]))
	return ts, true
}

func phase1PrefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func phase1HeadHex(raw []byte) string {
	if len(raw) > 16 {
		raw = raw[:16]
	}
	return fmt.Sprintf("%x", raw)
}

func phase1LengthPreview(raw []byte, offset int, fieldN int) string {
	if offset < 0 || fieldN < 0 || offset+fieldN*2 > len(raw) {
		return ""
	}
	parts := make([]string, 0, fieldN)
	for i := 0; i < fieldN; i++ {
		parts = append(parts, strconv.Itoa(int(binary.BigEndian.Uint16(raw[offset+i*2:]))))
	}
	return strings.Join(parts, ",")
}

func phase1RepoRelativeOutDir(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join("..", path)
}

func phase1DistanceBucket(distanceKM float64) string {
	switch {
	case distanceKM < 1000:
		return "0-1000"
	case distanceKM < 3000:
		return "1000-3000"
	case distanceKM < 7000:
		return "3000-7000"
	case distanceKM < 12000:
		return "7000-12000"
	default:
		return "12000+"
	}
}

func phase1NormalizeBand(label string) string {
	cleaned := strings.ToLower(strings.TrimSpace(label))
	if cleaned == "" {
		return ""
	}
	replacements := []struct {
		old string
		new string
	}{
		{old: "meters", new: "m"},
		{old: "meter", new: "m"},
		{old: "centimeters", new: "cm"},
		{old: "centimeter", new: "cm"},
	}
	for _, replacement := range replacements {
		cleaned = strings.ReplaceAll(cleaned, replacement.old, replacement.new)
	}
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	if cleaned == "" {
		return ""
	}
	last := cleaned[len(cleaned)-1]
	if last >= '0' && last <= '9' {
		cleaned += "m"
	}
	return cleaned
}

func phase1FreqToBand(freq float64) string {
	switch {
	case freq >= 1800 && freq <= 2000:
		return "160m"
	case freq >= 3500 && freq <= 4000:
		return "80m"
	case freq >= 5330 && freq <= 5405:
		return "60m"
	case freq >= 7000 && freq <= 7300:
		return "40m"
	case freq >= 10100 && freq <= 10150:
		return "30m"
	case freq >= 14000 && freq <= 14350:
		return "20m"
	case freq >= 18068 && freq <= 18168:
		return "17m"
	case freq >= 21000 && freq <= 21450:
		return "15m"
	case freq >= 24890 && freq <= 24990:
		return "12m"
	case freq >= 28000 && freq <= 29700:
		return "10m"
	case freq >= 50000 && freq <= 54000:
		return "6m"
	default:
		return ""
	}
}

func phase1EnvString(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func phase1EnvInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func phase1EnvBool(name string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func phase1FormatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}
