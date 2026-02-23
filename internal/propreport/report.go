package propreport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"dxcluster/internal/openaiutil"
	"dxcluster/pathreliability"
	"gopkg.in/yaml.v3"
)

type Logger interface {
	Printf(format string, args ...any)
}

type Options struct {
	Date             time.Time
	LogPath          string
	JSONOut          string
	ReportOut        string
	PathConfigPath   string
	OpenAIConfigPath string
	NoLLM            bool
	Logger           Logger
}

type Result struct {
	JSONPath   string
	ReportPath string
	Summary    reportSummary
}

type reportSummary struct {
	DateUTC           string                  `json:"date_utc"`
	LogFile           string                  `json:"log_file"`
	Timezone          string                  `json:"timezone"`
	ModelContext      modelContext            `json:"model_context"`
	Bands             []bandSummary           `json:"bands"`
	BandGroups        map[string][]string     `json:"band_groups"`
	CoverageMedians   map[string]coverageStat `json:"coverage_medians_by_band"`
	PredictionsByHour []predictionHour        `json:"predictions_by_hour"`
	SourceMixByHour   []sourceMixHour         `json:"source_mix_by_hour"`
	Thresholds        classificationThreshold `json:"thresholds"`
}

type bandSummary struct {
	Band           string      `json:"band"`
	Hours          []hourStat  `json:"hours"`
	EvidenceLevel  string      `json:"evidence_level"`
	StrongRanges   []rangeStat `json:"strong_ranges"`
	WeakRanges     []rangeStat `json:"weak_ranges"`
	ModerateRanges []rangeStat `json:"moderate_ranges"`
	OverallFRange  rangeValue  `json:"overall_f_range"`
	OverallGRange  rangeValue  `json:"overall_ge10_range"`
	OverallLRange  rangeValue  `json:"overall_lt1_range"`
}

type hourStat struct {
	Hour            string `json:"hour"`
	FMed            int    `json:"f_med"`
	Ge10Med         int    `json:"ge10_med"`
	Lt1Med          int    `json:"lt1_med"`
	UniqueSpotters  int    `json:"unique_spotters"`
	UniqueGridPairs int    `json:"unique_grid_pairs"`
	Ge10Min         int    `json:"ge10_min"`
	Ge10P75         int    `json:"ge10_p75"`
	Ge10Max         int    `json:"ge10_max"`
	Ge10Degenerate  bool   `json:"ge10_degenerate"`
}

type rangeValue struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type rangeStat struct {
	Hours  string     `json:"hours"`
	FRange rangeValue `json:"f_range"`
	GRange rangeValue `json:"ge10_range"`
	LRange rangeValue `json:"lt1_range"`
}

type classificationThreshold struct {
	StrongRule string `json:"strong_rule"`
	WeakRule   string `json:"weak_rule"`
}

type predictionHour struct {
	Hour            string  `json:"hour"`
	Samples         int     `json:"samples"`
	AvgTotal        float64 `json:"avg_total"`
	AvgCombined     float64 `json:"avg_combined"`
	AvgInsufficient float64 `json:"avg_insufficient"`
	AvgNoSample     float64 `json:"avg_no_sample"`
	AvgLowWeight    float64 `json:"avg_low_weight"`
}

type sourceMixHour struct {
	Hour     string `json:"hour"`
	Total    int    `json:"total"`
	RBN      int    `json:"rbn"`
	RBNFT    int    `json:"rbn_ft"`
	PSK      int    `json:"psk"`
	HUMAN    int    `json:"human"`
	PEER     int    `json:"peer"`
	UPSTREAM int    `json:"upstream"`
	OTHER    int    `json:"other"`
}

type coverageStat struct {
	SpottersMedian  int `json:"spotters_median"`
	GridPairsMedian int `json:"grid_pairs_median"`
}

type ge10Variance struct {
	Min int
	Med int
	P75 int
	Max int
	Deg bool
}

type modelContext struct {
	ClampMin                     float64            `json:"clamp_min"`
	ClampMax                     float64            `json:"clamp_max"`
	DefaultHalfLifeSec           int                `json:"default_half_life_seconds"`
	BandHalfLifeSec              map[string]int     `json:"band_half_life_seconds"`
	StaleAfterSeconds            int                `json:"stale_after_seconds"`
	StaleAfterHalfLifeMultiplier float64            `json:"stale_after_half_life_multiplier"`
	StaleAfterByBand             map[string]int     `json:"stale_after_by_band_seconds"`
	MinEffectiveWeight           float64            `json:"min_effective_weight"`
	MinFineWeight                float64            `json:"min_fine_weight"`
	ReverseHintDiscount          float64            `json:"reverse_hint_discount"`
	MergeReceiveWeight           float64            `json:"merge_receive_weight"`
	MergeTransmitWeight          float64            `json:"merge_transmit_weight"`
	NoiseOffsets                 map[string]float64 `json:"noise_offsets"`
}

type openAIConfig struct {
	APIKey       string  `yaml:"api_key"`
	Model        string  `yaml:"model"`
	Endpoint     string  `yaml:"endpoint"`
	MaxTokens    int     `yaml:"max_tokens"`
	Temperature  float64 `yaml:"temperature"`
	SystemPrompt string  `yaml:"system_prompt"`
}

type weightBins struct {
	Total int
	Lt1   int
	Ge10  int
}

type predTotals struct {
	Total        int
	Combined     int
	Insufficient int
	NoSample     int
	LowWeight    int
}

var (
	tsRe          = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`)
	bucketsRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path buckets`)
	weightsRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path weight dist`)
	predsRe       = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path predictions`)
	sourceMixRe   = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path source mix`)
	spottersRe    = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path unique spotters`)
	pairsRe       = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path unique grid pairs`)
	ge10VarRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path ge10 variance`)
	ansiRe        = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	bandBuckets   = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+f=([\d,]+)\s+c=([\d,]+)`)
	bandWeights   = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+t=([\d,]+)\s+<1=([\d,]+)\s+1-2=([\d,]+)\s+2-3=([\d,]+)\s+3-5=([\d,]+)\s+5-10=([\d,]+)\s+>=10=([\d,]+)`)
	predsFields   = regexp.MustCompile(`total=(\d+).*derived=(\d+).*combined=(\d+).*insufficient=(\d+).*no_sample=(\d+).*low_weight=(\d+)`)
	sourceFields  = regexp.MustCompile(`([A-Za-z\-]+)=([\d,]+)`)
	hourField     = regexp.MustCompile(`hour=(\d{2})`)
	bandCounts    = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)=([\d,]+)`)
	ge10VarFields = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+min=(\d+)\s+med=(\d+)\s+p75=(\d+)\s+max=(\d+)\s+deg=(\d)`)
)

func parseLog(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLineBytes = 1024 * 1024
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	lines := make([]string, 0, 4096)
	for scanner.Scan() {
		line := ansiRe.ReplaceAllString(scanner.Text(), "")
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	combined := make([]string, 0, len(lines))
	var buf strings.Builder
	for _, line := range lines {
		if tsRe.MatchString(line) {
			if buf.Len() > 0 {
				combined = append(combined, buf.String())
				buf.Reset()
			}
			buf.WriteString(strings.TrimRight(line, "\n"))
			continue
		}
		if buf.Len() > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(strings.TrimRight(line, "\n"))
	}
	if buf.Len() > 0 {
		combined = append(combined, buf.String())
	}
	return combined, nil
}

// ParseLog exposes the log parsing helper for callers that need raw entries.
func ParseLog(path string) ([]string, error) {
	return parseLog(path)
}

func parseInt(val string) int {
	val = strings.ReplaceAll(val, ",", "")
	out, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return out
}

func parseHour(ts string, line string) (int, bool) {
	if m := hourField.FindStringSubmatch(line); len(m) == 2 {
		h, err := strconv.Atoi(m[1])
		if err == nil && h >= 0 && h <= 23 {
			return h, true
		}
	}
	tsTime, err := time.Parse("2006/01/02 15:04:05", ts)
	if err != nil {
		return 0, false
	}
	return tsTime.Hour(), true
}

func updateBandHourMax(byHour map[int]map[string]int, hour int, line string, bandCounts *regexp.Regexp) {
	if byHour[hour] == nil {
		byHour[hour] = make(map[string]int)
	}
	for _, match := range bandCounts.FindAllStringSubmatch(line, -1) {
		band := match[1]
		count := parseInt(match[2])
		if count > byHour[hour][band] {
			byHour[hour][band] = count
		}
	}
}

func median(vals []int) int {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]int(nil), vals...)
	sort.Ints(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return int(math.Round(float64(sorted[mid-1]+sorted[mid]) / 2))
}

func percentile(vals []int, p float64) int {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]int(nil), vals...)
	sort.Ints(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := int(math.Round((p / 100) * float64(len(sorted)-1)))
	if pos < 0 {
		pos = 0
	}
	if pos >= len(sorted) {
		pos = len(sorted) - 1
	}
	return sorted[pos]
}

func bandSortKey(b string) (int, float64, string) {
	if strings.HasSuffix(b, "m") && !strings.HasSuffix(b, "cm") {
		v, err := strconv.ParseFloat(strings.TrimSuffix(b, "m"), 64)
		if err == nil {
			return 0, v, b
		}
	}
	if strings.HasSuffix(b, "cm") {
		v, err := strconv.ParseFloat(strings.TrimSuffix(b, "cm"), 64)
		if err == nil {
			return 1, v, b
		}
	}
	return 2, 0, b
}

func buildRanges(hours []int, stats map[int]hourStat, label string) []rangeStat {
	if len(hours) == 0 {
		return nil
	}
	sort.Ints(hours)
	var ranges []rangeStat
	start := hours[0]
	prev := hours[0]
	flush := func(s, e int) {
		var fVals, gVals, lVals []int
		for h := s; h <= e; h++ {
			hs, ok := stats[h]
			if !ok {
				continue
			}
			fVals = append(fVals, hs.FMed)
			gVals = append(gVals, hs.Ge10Med)
			lVals = append(lVals, hs.Lt1Med)
		}
		if len(fVals) == 0 {
			return
		}
		r := rangeStat{
			Hours:  fmt.Sprintf("%02d:00–%02d:00", s, e),
			FRange: rangeValue{Min: minInt(fVals), Max: maxInt(fVals)},
			GRange: rangeValue{Min: minInt(gVals), Max: maxInt(gVals)},
			LRange: rangeValue{Min: minInt(lVals), Max: maxInt(lVals)},
		}
		if s == e {
			r.Hours = fmt.Sprintf("%02d:00", s)
		}
		_ = label
		ranges = append(ranges, r)
	}
	for _, h := range hours[1:] {
		if h == prev+1 {
			prev = h
			continue
		}
		flush(start, prev)
		start = h
		prev = h
	}
	flush(start, prev)
	return ranges
}

func minInt(vals []int) int {
	if len(vals) == 0 {
		return 0
	}
	min := vals[0]
	for _, v := range vals[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func maxInt(vals []int) int {
	if len(vals) == 0 {
		return 0
	}
	max := vals[0]
	for _, v := range vals[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

func loadOpenAIConfig(path string) (openAIConfig, error) {
	if strings.TrimSpace(path) == "" {
		return openAIConfig{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return openAIConfig{}, err
	}
	var cfg openAIConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return openAIConfig{}, err
	}
	return cfg, nil
}

func buildModelContext(cfg pathreliability.Config, bands []string) modelContext {
	staleByBand := make(map[string]int, len(bands))
	for _, band := range bands {
		halfLife := cfg.DefaultHalfLifeSec
		if v, ok := cfg.BandHalfLifeSec[band]; ok && v > 0 {
			halfLife = v
		}
		stale := cfg.StaleAfterSeconds
		if cfg.StaleAfterHalfLifeMultiplier > 0 && halfLife > 0 {
			stale = int(math.Round(cfg.StaleAfterHalfLifeMultiplier * float64(halfLife)))
		}
		staleByBand[band] = stale
	}
	return modelContext{
		ClampMin:                     cfg.ClampMin,
		ClampMax:                     cfg.ClampMax,
		DefaultHalfLifeSec:           cfg.DefaultHalfLifeSec,
		BandHalfLifeSec:              cfg.BandHalfLifeSec,
		StaleAfterSeconds:            cfg.StaleAfterSeconds,
		StaleAfterHalfLifeMultiplier: cfg.StaleAfterHalfLifeMultiplier,
		StaleAfterByBand:             staleByBand,
		MinEffectiveWeight:           cfg.MinEffectiveWeight,
		MinFineWeight:                cfg.MinFineWeight,
		ReverseHintDiscount:          cfg.ReverseHintDiscount,
		MergeReceiveWeight:           cfg.MergeReceiveWeight,
		MergeTransmitWeight:          cfg.MergeTransmitWeight,
		NoiseOffsets:                 cfg.NoiseOffsets,
	}
}

var bandGroups = map[string][]string{
	"low":  {"160m", "80m", "60m"},
	"mid":  {"40m", "30m", "20m"},
	"high": {"17m", "15m", "12m", "10m"},
}

var allowedBands = func() map[string]struct{} {
	allowed := make(map[string]struct{})
	for _, group := range bandGroups {
		for _, band := range group {
			allowed[band] = struct{}{}
		}
	}
	return allowed
}()

func Generate(ctx context.Context, opts Options) (Result, error) {
	var result Result
	if ctx == nil {
		return result, errors.New("propreport: nil context")
	}
	logf := func(format string, args ...any) {
		if opts.Logger != nil {
			opts.Logger.Printf(format, args...)
		}
	}

	date := opts.Date
	if date.IsZero() {
		date = time.Now().UTC()
	}
	date = date.UTC()

	logPath := strings.TrimSpace(opts.LogPath)
	if logPath == "" {
		logPath = filepath.Join("data", "logs", fmt.Sprintf("%s.log", date.Format("02-Jan-2006")))
	}
	jsonOut := strings.TrimSpace(opts.JSONOut)
	if jsonOut == "" {
		jsonOut = filepath.Join("data", "reports", fmt.Sprintf("prop-%s.json", date.Format("2006-01-02")))
	}
	reportOut := strings.TrimSpace(opts.ReportOut)
	if reportOut == "" {
		reportOut = filepath.Join("data", "reports", fmt.Sprintf("prop-%s.md", date.Format("2006-01-02")))
	}

	pathConfigPath := strings.TrimSpace(opts.PathConfigPath)
	if pathConfigPath == "" {
		pathConfigPath = filepath.Join("data", "config", "path_reliability.yaml")
	}
	openAIConfigPath := strings.TrimSpace(opts.OpenAIConfigPath)
	if openAIConfigPath == "" {
		openAIConfigPath = filepath.Join("data", "config", "openai.yaml")
	}

	pathCfg, err := pathreliability.LoadFile(pathConfigPath)
	if err != nil {
		logf("Warning: failed to load path reliability config (%s): %v", pathConfigPath, err)
		pathCfg = pathreliability.DefaultConfig()
	}

	entries, err := parseLog(logPath)
	if err != nil {
		return result, err
	}

	bucketByTS := make(map[string]map[string]int)
	weightByTS := make(map[string]map[string]weightBins)
	predByTS := make(map[string]predTotals)
	sourceMixByHour := make(map[int]*sourceMixHour)
	spottersByHour := make(map[int]map[string]int)
	pairsByHour := make(map[int]map[string]int)
	ge10VarByHour := make(map[int]map[string][]ge10Variance)

	for _, entry := range entries {
		if m := bucketsRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			buckets := make(map[string]int)
			for _, match := range bandBuckets.FindAllStringSubmatch(entry, -1) {
				band := match[1]
				buckets[band] = parseInt(match[2])
			}
			bucketByTS[ts] = buckets
			continue
		}
		if m := weightsRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			weights := make(map[string]weightBins)
			for _, match := range bandWeights.FindAllStringSubmatch(entry, -1) {
				band := match[1]
				weights[band] = weightBins{
					Total: parseInt(match[2]),
					Lt1:   parseInt(match[3]),
					Ge10:  parseInt(match[8]),
				}
			}
			weightByTS[ts] = weights
			continue
		}
		if m := predsRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			fields := predsFields.FindStringSubmatch(entry)
			if len(fields) == 7 {
				predByTS[ts] = predTotals{
					Total:        parseInt(fields[1]),
					Combined:     parseInt(fields[3]),
					Insufficient: parseInt(fields[4]),
					NoSample:     parseInt(fields[5]),
					LowWeight:    parseInt(fields[6]),
				}
			}
		}
		if m := sourceMixRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			hour, ok := parseHour(ts, entry)
			if !ok {
				continue
			}
			mix := sourceMixByHour[hour]
			if mix == nil {
				mix = &sourceMixHour{Hour: fmt.Sprintf("%02d:00", hour)}
				sourceMixByHour[hour] = mix
			}
			fields := sourceFields.FindAllStringSubmatch(entry, -1)
			for _, f := range fields {
				if len(f) != 3 {
					continue
				}
				label := f[1]
				val := parseInt(f[2])
				switch label {
				case "total":
					mix.Total += val
				case "RBN":
					mix.RBN += val
				case "RBN-FT":
					mix.RBNFT += val
				case "PSK":
					mix.PSK += val
				case "HUMAN":
					mix.HUMAN += val
				case "PEER":
					mix.PEER += val
				case "UPSTREAM":
					mix.UPSTREAM += val
				case "OTHER":
					mix.OTHER += val
				}
			}
		}
		if m := spottersRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			hour, ok := parseHour(ts, entry)
			if !ok {
				continue
			}
			updateBandHourMax(spottersByHour, hour, entry, bandCounts)
		}
		if m := pairsRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			hour, ok := parseHour(ts, entry)
			if !ok {
				continue
			}
			updateBandHourMax(pairsByHour, hour, entry, bandCounts)
		}
		if m := ge10VarRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			hour, ok := parseHour(ts, entry)
			if !ok {
				continue
			}
			if ge10VarByHour[hour] == nil {
				ge10VarByHour[hour] = make(map[string][]ge10Variance)
			}
			for _, match := range ge10VarFields.FindAllStringSubmatch(entry, -1) {
				if len(match) != 7 {
					continue
				}
				band := match[1]
				minVal := parseInt(match[2])
				medVal := parseInt(match[3])
				p75Val := parseInt(match[4])
				maxVal := parseInt(match[5])
				degVal := parseInt(match[6])
				ge10VarByHour[hour][band] = append(ge10VarByHour[hour][band], ge10Variance{
					Min: minVal,
					Med: medVal,
					P75: p75Val,
					Max: maxVal,
					Deg: degVal > 0,
				})
			}
		}
	}

	bandHourStats := make(map[string]map[int][]hourStat)
	for ts, buckets := range bucketByTS {
		tsTime, err := time.Parse("2006/01/02 15:04:05", ts)
		if err != nil {
			continue
		}
		hour := tsTime.Hour()
		weights := weightByTS[ts]
		for band, f := range buckets {
			if _, ok := allowedBands[band]; !ok {
				continue
			}
			w := weights[band]
			if bandHourStats[band] == nil {
				bandHourStats[band] = make(map[int][]hourStat)
			}
			bandHourStats[band][hour] = append(bandHourStats[band][hour], hourStat{
				Hour:    fmt.Sprintf("%02d:00", hour),
				FMed:    f,
				Ge10Med: w.Ge10,
				Lt1Med:  w.Lt1,
			})
		}
	}

	bands := make([]string, 0, len(bandHourStats))
	for band := range bandHourStats {
		bands = append(bands, band)
	}
	sort.Slice(bands, func(i, j int) bool {
		ai, vi, si := bandSortKey(bands[i])
		aj, vj, sj := bandSortKey(bands[j])
		if ai != aj {
			return ai < aj
		}
		if vi != vj {
			return vi < vj
		}
		return si < sj
	})

	summaries := make([]bandSummary, 0, len(bands))
	for _, band := range bands {
		hourMap := bandHourStats[band]
		hours := make([]int, 0, len(hourMap))
		hourStats := make(map[int]hourStat, len(hourMap))
		var fVals, gVals, lVals []int
		for hour, list := range hourMap {
			hours = append(hours, hour)
			var fList, gList, lList []int
			for _, v := range list {
				fList = append(fList, v.FMed)
				gList = append(gList, v.Ge10Med)
				lList = append(lList, v.Lt1Med)
			}
			spotterCount := 0
			if byBand, ok := spottersByHour[hour]; ok {
				spotterCount = byBand[band]
			}
			pairCount := 0
			if byBand, ok := pairsByHour[hour]; ok {
				pairCount = byBand[band]
			}
			ge10Min := 0
			ge10P75 := 0
			ge10Max := 0
			ge10Deg := false
			if byBand, ok := ge10VarByHour[hour]; ok {
				if vars := byBand[band]; len(vars) > 0 {
					var mins, p75s, maxs []int
					degCount := 0
					for _, v := range vars {
						mins = append(mins, v.Min)
						p75s = append(p75s, v.P75)
						maxs = append(maxs, v.Max)
						if v.Deg {
							degCount++
						}
					}
					ge10Min = median(mins)
					ge10P75 = median(p75s)
					ge10Max = median(maxs)
					if ge10Max == 0 || degCount > len(vars)/2 {
						ge10Deg = true
					}
				}
			}
			stat := hourStat{
				Hour:            fmt.Sprintf("%02d:00", hour),
				FMed:            median(fList),
				Ge10Med:         median(gList),
				Lt1Med:          median(lList),
				UniqueSpotters:  spotterCount,
				UniqueGridPairs: pairCount,
				Ge10Min:         ge10Min,
				Ge10P75:         ge10P75,
				Ge10Max:         ge10Max,
				Ge10Degenerate:  ge10Deg,
			}
			hourStats[hour] = stat
			fVals = append(fVals, stat.FMed)
			gVals = append(gVals, stat.Ge10Med)
			lVals = append(lVals, stat.Lt1Med)
		}

		sort.Ints(hours)
		statsSlice := make([]hourStat, 0, len(hours))
		for _, h := range hours {
			statsSlice = append(statsSlice, hourStats[h])
		}

		maxF := maxInt(fVals)
		maxG := maxInt(gVals)
		evidence := "mixed"
		var strongHours, weakHours, moderateHours []int
		if maxF == 0 && maxG == 0 {
			evidence = "none"
		} else {
			fMed := percentile(fVals, 50)
			gP25 := percentile(gVals, 25)
			gP75 := percentile(gVals, 75)
			for _, h := range hours {
				stat := hourStats[h]
				if stat.Ge10Med >= gP75 && stat.FMed >= fMed {
					strongHours = append(strongHours, h)
				} else if stat.Ge10Med <= gP25 && stat.FMed <= fMed {
					weakHours = append(weakHours, h)
				} else {
					moderateHours = append(moderateHours, h)
				}
			}
		}

		summary := bandSummary{
			Band:           band,
			Hours:          statsSlice,
			EvidenceLevel:  evidence,
			StrongRanges:   buildRanges(strongHours, hourStats, "strong"),
			WeakRanges:     buildRanges(weakHours, hourStats, "weak"),
			ModerateRanges: buildRanges(moderateHours, hourStats, "moderate"),
			OverallFRange:  rangeValue{Min: minInt(fVals), Max: maxInt(fVals)},
			OverallGRange:  rangeValue{Min: minInt(gVals), Max: maxInt(gVals)},
			OverallLRange:  rangeValue{Min: minInt(lVals), Max: maxInt(lVals)},
		}
		summaries = append(summaries, summary)
	}

	predHours := make(map[int][]predTotals)
	for ts, totals := range predByTS {
		tsTime, err := time.Parse("2006/01/02 15:04:05", ts)
		if err != nil {
			continue
		}
		hour := tsTime.Hour()
		predHours[hour] = append(predHours[hour], totals)
	}

	predSummary := make([]predictionHour, 0, len(predHours))
	var predHoursKeys []int
	for h := range predHours {
		predHoursKeys = append(predHoursKeys, h)
	}
	sort.Ints(predHoursKeys)
	for _, h := range predHoursKeys {
		rows := predHours[h]
		if len(rows) == 0 {
			continue
		}
		var total, combined, insufficient, noSample, lowWeight int
		for _, r := range rows {
			total += r.Total
			combined += r.Combined
			insufficient += r.Insufficient
			noSample += r.NoSample
			lowWeight += r.LowWeight
		}
		count := len(rows)
		predSummary = append(predSummary, predictionHour{
			Hour:            fmt.Sprintf("%02d:00", h),
			Samples:         count,
			AvgTotal:        float64(total) / float64(count),
			AvgCombined:     float64(combined) / float64(count),
			AvgInsufficient: float64(insufficient) / float64(count),
			AvgNoSample:     float64(noSample) / float64(count),
			AvgLowWeight:    float64(lowWeight) / float64(count),
		})
	}

	sourceMixSummary := make([]sourceMixHour, 0, len(sourceMixByHour))
	var sourceHours []int
	for h := range sourceMixByHour {
		sourceHours = append(sourceHours, h)
	}
	sort.Ints(sourceHours)
	for _, h := range sourceHours {
		if mix := sourceMixByHour[h]; mix != nil {
			sourceMixSummary = append(sourceMixSummary, *mix)
		}
	}

	presentBands := make(map[string]struct{}, len(summaries))
	for i := range summaries {
		band := &summaries[i]
		presentBands[band.Band] = struct{}{}
	}
	filteredGroups := make(map[string][]string, len(bandGroups))
	for name, group := range bandGroups {
		for _, band := range group {
			if _, ok := presentBands[band]; ok {
				filteredGroups[name] = append(filteredGroups[name], band)
			}
		}
		if len(filteredGroups[name]) == 0 {
			delete(filteredGroups, name)
		}
	}

	coverageMedians := make(map[string]coverageStat, len(summaries))
	for i := range summaries {
		band := &summaries[i]
		var spotters, pairs []int
		for j := range band.Hours {
			hour := &band.Hours[j]
			if hour.UniqueSpotters > 0 {
				spotters = append(spotters, hour.UniqueSpotters)
			}
			if hour.UniqueGridPairs > 0 {
				pairs = append(pairs, hour.UniqueGridPairs)
			}
		}
		coverageMedians[band.Band] = coverageStat{
			SpottersMedian:  median(spotters),
			GridPairsMedian: median(pairs),
		}
	}

	summary := reportSummary{
		DateUTC:           date.Format("2006-01-02"),
		LogFile:           logPath,
		Timezone:          "UTC",
		ModelContext:      buildModelContext(pathCfg, bands),
		Bands:             summaries,
		BandGroups:        filteredGroups,
		CoverageMedians:   coverageMedians,
		PredictionsByHour: predSummary,
		SourceMixByHour:   sourceMixSummary,
		Thresholds: classificationThreshold{
			StrongRule: "strong if ge10_med >= p75(ge10) and f_med >= p50(f)",
			WeakRule:   "weak if ge10_med <= p25(ge10) and f_med <= p50(f)",
		},
	}

	jsonBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return result, err
	}

	if err := os.MkdirAll(filepath.Dir(jsonOut), 0o755); err != nil {
		return result, err
	}
	if err := os.WriteFile(jsonOut, jsonBytes, 0o644); err != nil {
		return result, err
	}

	finalReport := buildFinalReport(summary)
	if !opts.NoLLM {
		openaiCfg, err := loadOpenAIConfig(openAIConfigPath)
		if err != nil {
			logf("Warning: failed to load OpenAI config (%s): %v", openAIConfigPath, err)
		} else {
			reqCtx := ctx
			if _, ok := reqCtx.Deadline(); !ok {
				var cancel context.CancelFunc
				reqCtx, cancel = context.WithTimeout(reqCtx, 60*time.Second)
				defer cancel()
			}
			llmText, err := openaiutil.Generate(reqCtx, openaiutil.Config{
				APIKey:       openaiCfg.APIKey,
				Model:        openaiCfg.Model,
				Endpoint:     openaiCfg.Endpoint,
				MaxTokens:    openaiCfg.MaxTokens,
				Temperature:  openaiCfg.Temperature,
				SystemPrompt: openaiCfg.SystemPrompt,
			}, string(jsonBytes))
			if err != nil {
				logf("Warning: OpenAI request failed: %v", err)
			} else if strings.TrimSpace(llmText) != "" {
				finalReport += "\n\nLLM narrative\n\n" + strings.TrimSpace(llmText) + "\n"
			}
		}
	}

	if err := os.MkdirAll(filepath.Dir(reportOut), 0o755); err != nil {
		return result, err
	}
	if err := os.WriteFile(reportOut, []byte(finalReport+"\n"), 0o644); err != nil {
		return result, err
	}

	result.JSONPath = jsonOut
	result.ReportPath = reportOut
	result.Summary = summary
	return result, nil
}

func buildFinalReport(summary reportSummary) string {
	var b strings.Builder
	logName := filepath.Base(summary.LogFile)
	b.WriteString(fmt.Sprintf("I reviewed the entire %s log (%s) and summarized per band, by hour how much evidence we have (active fine buckets) and how strong it is (weight distribution). All times are UTC from the log.\n\n", summary.DateUTC, logName))
	b.WriteString("How to read this\n\n")
	b.WriteString("f_med = median count of active fine buckets for the hour (higher = more evidence).\n")
	b.WriteString("ge10_med = median count of buckets with decayed weight ≥10 (strong evidence).\n")
	b.WriteString("lt1_med = median count of buckets with weight <1 (weak evidence).\n")
	b.WriteString("Interpretation: High f_med + high ge10_med = strong evidence. High lt1_med with low ge10_med = weak/fragile evidence.\n\n")
	b.WriteString("Model context for this run\n\n")
	writeModelContext(&b, summary.ModelContext, summary.Bands)
	b.WriteString("\n")

	bandMap := make(map[string]bandSummary, len(summary.Bands))
	for i := range summary.Bands {
		band := &summary.Bands[i]
		bandMap[band.Band] = *band
	}

	b.WriteString("Evidence quality & coverage\n\n")
	b.WriteString(coverageSummary(summary.Bands, summary.SourceMixByHour, summary.CoverageMedians))
	b.WriteString("\n\n")
	b.WriteString("Strength bucket degeneracy\n\n")
	b.WriteString(degeneracySummary(summary.Bands))
	b.WriteString("\n\n")

	writeGroupSection(&b, "Low bands", summary.BandGroups["low"], bandMap, "These show the clearest time-of-day patterns in evidence:")
	writeGroupSection(&b, "Mid bands", summary.BandGroups["mid"], bandMap, "These show sustained evidence with varying strength by hour:")
	writeGroupSection(&b, "High bands", summary.BandGroups["high"], bandMap, "These show useful daytime evidence windows:")

	b.WriteString("Prediction activity by hour (overall)\n\n")
	b.WriteString(predictionActivitySummary(summary.PredictionsByHour))
	b.WriteString("\n\n")

	b.WriteString("Plain‑English takeaway\n\n")
	b.WriteString(deterministicTakeaway(summary, bandMap))
	b.WriteString("\n")

	return b.String()
}

func writeGroupSection(b *strings.Builder, title string, bands []string, bandMap map[string]bandSummary, lead string) {
	if len(bands) == 0 {
		return
	}
	b.WriteString(title + " (" + strings.Join(bands, " / ") + ")\n")
	b.WriteString(lead + "\n\n")
	for _, band := range bands {
		writeBandDetail(b, bandMap[band])
	}
}

func writeModelContext(b *strings.Builder, ctx modelContext, bands []bandSummary) {
	if b == nil {
		return
	}
	fmt.Fprintf(b, "Clamp: %.1f to %.1f dB. Default half-life: %ds. Stale after: %ds or %.2fx half-life per band.\n",
		ctx.ClampMin, ctx.ClampMax, ctx.DefaultHalfLifeSec, ctx.StaleAfterSeconds, ctx.StaleAfterHalfLifeMultiplier)
	fmt.Fprintf(b, "Min effective weight: %.2f. Min fine weight: %.2f. Reverse hint discount: %.2f.\n",
		ctx.MinEffectiveWeight, ctx.MinFineWeight, ctx.ReverseHintDiscount)
	fmt.Fprintf(b, "Merge weights: receive %.2f / transmit %.2f.\n", ctx.MergeReceiveWeight, ctx.MergeTransmitWeight)
	if len(ctx.NoiseOffsets) > 0 {
		keys := make([]string, 0, len(ctx.NoiseOffsets))
		for k := range ctx.NoiseOffsets {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%g", k, ctx.NoiseOffsets[k]))
		}
		b.WriteString("Noise offsets (dB): " + strings.Join(parts, ", ") + ".\n")
	}
	if len(bands) > 0 {
		b.WriteString("Per-band half-life/stale (seconds): ")
		parts := make([]string, 0, len(bands))
		for i := range bands {
			band := &bands[i]
			hl := ctx.DefaultHalfLifeSec
			if v, ok := ctx.BandHalfLifeSec[band.Band]; ok && v > 0 {
				hl = v
			}
			stale := ctx.StaleAfterSeconds
			if v, ok := ctx.StaleAfterByBand[band.Band]; ok && v > 0 {
				stale = v
			}
			parts = append(parts, fmt.Sprintf("%s hl=%d stale=%d", band.Band, hl, stale))
		}
		b.WriteString(strings.Join(parts, "; "))
		b.WriteString(".\n")
	}
}

func coverageSummary(bands []bandSummary, mixes []sourceMixHour, medians map[string]coverageStat) string {
	if len(bands) == 0 {
		return "No coverage data available."
	}
	overallMix := sourceMixHour{}
	for _, mix := range mixes {
		overallMix.Total += mix.Total
		overallMix.RBN += mix.RBN
		overallMix.RBNFT += mix.RBNFT
		overallMix.PSK += mix.PSK
		overallMix.HUMAN += mix.HUMAN
		overallMix.PEER += mix.PEER
		overallMix.UPSTREAM += mix.UPSTREAM
		overallMix.OTHER += mix.OTHER
	}
	var mixParts []string
	if overallMix.Total > 0 {
		mixParts = append(mixParts, fmt.Sprintf("total=%d", overallMix.Total))
		mixParts = append(mixParts, fmt.Sprintf("RBN=%d", overallMix.RBN))
		mixParts = append(mixParts, fmt.Sprintf("RBN-FT=%d", overallMix.RBNFT))
		mixParts = append(mixParts, fmt.Sprintf("PSK=%d", overallMix.PSK))
		mixParts = append(mixParts, fmt.Sprintf("HUMAN=%d", overallMix.HUMAN))
		mixParts = append(mixParts, fmt.Sprintf("PEER=%d", overallMix.PEER))
		mixParts = append(mixParts, fmt.Sprintf("UPSTREAM=%d", overallMix.UPSTREAM))
		mixParts = append(mixParts, fmt.Sprintf("OTHER=%d", overallMix.OTHER))
	}
	var b strings.Builder
	if len(mixParts) > 0 {
		b.WriteString("Source mix totals across the day: " + strings.Join(mixParts, ", ") + ".\n")
	}
	b.WriteString("Median unique spotters/grid pairs per band (non-zero hours only): ")
	parts := make([]string, 0, len(bands))
	for i := range bands {
		band := &bands[i]
		stat := medians[band.Band]
		spotterStr := "n/a"
		pairStr := "n/a"
		if stat.SpottersMedian > 0 {
			spotterStr = fmt.Sprintf("%d", stat.SpottersMedian)
		}
		if stat.GridPairsMedian > 0 {
			pairStr = fmt.Sprintf("%d", stat.GridPairsMedian)
		}
		parts = append(parts, fmt.Sprintf("%s %s/%s", band.Band, spotterStr, pairStr))
	}
	b.WriteString(strings.Join(parts, "; "))
	b.WriteString(".")
	return b.String()
}

func degeneracySummary(bands []bandSummary) string {
	if len(bands) == 0 {
		return "No degeneracy data available."
	}
	degenerate := make([]string, 0)
	for i := range bands {
		band := &bands[i]
		if len(band.Hours) == 0 {
			continue
		}
		var degCount int
		var maxVals []int
		for _, h := range band.Hours {
			if h.Ge10Degenerate {
				degCount++
			}
			maxVals = append(maxVals, h.Ge10Max)
		}
		if degCount > len(band.Hours)/2 || median(maxVals) == 0 {
			degenerate = append(degenerate, band.Band)
		}
	}
	if len(degenerate) == 0 {
		return "No bands show degenerate ge10 buckets (ge10 variance is informative across the day)."
	}
	return "Degenerate ge10 buckets (ge10 rarely reaches strong levels): " + strings.Join(degenerate, ", ") + "."
}

func writeBandDetail(b *strings.Builder, band bandSummary) {
	b.WriteString(band.Band + "\n\n")
	if len(band.StrongRanges) > 0 {
		hours := rangeHours(band.StrongRanges)
		fMin, fMax, gMin, gMax := rangeValues(band.StrongRanges)
		fmt.Fprintf(b, "Evidence highest around %s (f_med ~%d–%d, ge10_med ~%d–%d).\n", hours, fMin, fMax, gMin, gMax)
	} else {
		fmt.Fprintf(b, "No strong-evidence window; strongest observed f_med ~%d–%d, ge10_med ~%d–%d.\n", band.OverallFRange.Min, band.OverallFRange.Max, band.OverallGRange.Min, band.OverallGRange.Max)
	}
	if len(band.WeakRanges) > 0 {
		hours := rangeHours(band.WeakRanges)
		fMin, fMax, gMin, gMax := rangeValues(band.WeakRanges)
		fmt.Fprintf(b, "Drops %s (f_med ~%d–%d, ge10_med ~%d–%d).\n", hours, fMin, fMax, gMin, gMax)
	} else {
		b.WriteString("No clear weak window in this log.\n")
	}
	if len(band.ModerateRanges) > 0 {
		hours := rangeHours(band.ModerateRanges)
		fMin, fMax, gMin, gMax := rangeValues(band.ModerateRanges)
		fmt.Fprintf(b, "Moderate evidence %s (f_med ~%d–%d, ge10_med ~%d–%d).\n", hours, fMin, fMax, gMin, gMax)
	}
	fmt.Fprintf(b, "Conclusion: %s.\n\n", deterministicConclusion(band))
}

func rangeHours(ranges []rangeStat) string {
	parts := make([]string, 0, len(ranges))
	for _, r := range ranges {
		parts = append(parts, r.Hours)
	}
	return strings.Join(parts, ", ")
}

func rangeValues(ranges []rangeStat) (int, int, int, int) {
	fVals := make([]int, 0, 2*len(ranges))
	gVals := make([]int, 0, 2*len(ranges))
	for _, r := range ranges {
		fVals = append(fVals, r.FRange.Min, r.FRange.Max)
		gVals = append(gVals, r.GRange.Min, r.GRange.Max)
	}
	return minInt(fVals), maxInt(fVals), minInt(gVals), maxInt(gVals)
}

func deterministicConclusion(band bandSummary) string {
	if band.OverallFRange.Max == 0 && band.OverallGRange.Max == 0 {
		return "no evidence; predictions are effectively unavailable"
	}
	if band.OverallGRange.Max == 0 {
		return "weak evidence overall; predictions are fragile"
	}
	if band.OverallGRange.Max >= 200 && band.OverallFRange.Max >= 1000 {
		return "robust evidence overall; predictions should be strong"
	}
	if band.OverallGRange.Max >= 50 && band.OverallFRange.Max >= 300 {
		return "moderate evidence overall; predictions are usable but variable"
	}
	return "limited evidence overall; predictions are weak or inconsistent"
}

func groupConclusion(bands []bandSummary) string {
	if len(bands) == 0 {
		return "no evidence; predictions are effectively unavailable"
	}
	none := 0
	weak := 0
	moderate := 0
	strong := 0
	for i := range bands {
		b := &bands[i]
		switch {
		case b.OverallFRange.Max == 0 && b.OverallGRange.Max == 0:
			none++
		case b.OverallGRange.Max == 0:
			weak++
		case b.OverallGRange.Max >= 200 && b.OverallFRange.Max >= 1000:
			strong++
		case b.OverallGRange.Max >= 50 && b.OverallFRange.Max >= 300:
			moderate++
		default:
			weak++
		}
	}
	if strong > 0 && strong >= moderate && strong >= weak {
		return "robust evidence in at least some bands; predictions are strong in those windows"
	}
	if moderate > 0 && moderate >= weak {
		return "moderate evidence across a subset of bands; predictions are usable but variable"
	}
	if none == len(bands) {
		return "no evidence; predictions are effectively unavailable"
	}
	return "weak evidence overall; predictions are fragile or unreliable"
}

func predictionActivitySummary(hours []predictionHour) string {
	if len(hours) == 0 {
		return "No prediction activity recorded for this day."
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Hour < hours[j].Hour })
	maxTotal := 0.0
	minTotal := hours[0].AvgTotal
	var maxHour, minHour string
	var lowSample []string
	for _, h := range hours {
		if h.AvgTotal > maxTotal {
			maxTotal = h.AvgTotal
			maxHour = h.Hour
		}
		if h.AvgTotal < minTotal {
			minTotal = h.AvgTotal
			minHour = h.Hour
		}
		if h.AvgInsufficient >= h.AvgCombined {
			lowSample = append(lowSample, h.Hour)
		}
	}
	s := fmt.Sprintf("Peak prediction volume occurs around %s (avg_total %.1f), with the lowest activity around %s (avg_total %.1f).",
		maxHour, maxTotal, minHour, minTotal)
	if len(lowSample) > 0 {
		s += fmt.Sprintf(" Hours dominated by insufficient samples: %s.", strings.Join(lowSample, ", "))
	}
	return s
}

func deterministicTakeaway(summary reportSummary, bandMap map[string]bandSummary) string {
	groupBands := func(group []string) []bandSummary {
		out := make([]bandSummary, 0, len(group))
		for _, band := range group {
			if b, ok := bandMap[band]; ok {
				out = append(out, b)
			}
		}
		return out
	}
	low := groupBands(summary.BandGroups["low"])
	mid := groupBands(summary.BandGroups["mid"])
	high := groupBands(summary.BandGroups["high"])

	lowConclusion := groupConclusion(low)
	midConclusion := groupConclusion(mid)
	highConclusion := groupConclusion(high)

	var lines []string
	if len(low) > 0 {
		lines = append(lines, fmt.Sprintf("Low bands (%s): %s.", strings.Join(summary.BandGroups["low"], "/"), lowConclusion))
	}
	if len(mid) > 0 {
		lines = append(lines, fmt.Sprintf("Mid bands (%s): %s.", strings.Join(summary.BandGroups["mid"], "/"), midConclusion))
	}
	if len(high) > 0 {
		lines = append(lines, fmt.Sprintf("High bands (%s): %s.", strings.Join(summary.BandGroups["high"], "/"), highConclusion))
	}
	return strings.Join(lines, " ")
}
