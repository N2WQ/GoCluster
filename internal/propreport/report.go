package propreport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/internal/openaiutil"
	"dxcluster/internal/yamlconfig"
	"dxcluster/pathreliability"
)

type Logger interface {
	Printf(format string, args ...any)
}

type Options struct {
	Date             time.Time
	LogPath          string
	JSONOut          string
	ReportOut        string
	ConfigDir        string
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
	P50DiagTotals     p50DiagTotals           `json:"p50_diag_totals"`
	P50DiagByHour     []p50DiagHour           `json:"p50_diag_by_hour"`
	P50ShadowTotals   p50ShadowTotals         `json:"p50_shadow_totals"`
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
	Hour             string  `json:"hour"`
	Samples          int     `json:"samples"`
	AvgTotal         float64 `json:"avg_total"`
	AvgCombined      float64 `json:"avg_combined"`
	AvgInsufficient  float64 `json:"avg_insufficient"`
	AvgNoSample      float64 `json:"avg_no_sample"`
	AvgLowCount      float64 `json:"avg_low_count"`
	AvgLowWeight     float64 `json:"avg_low_weight"`
	AvgStale         float64 `json:"avg_stale"`
	AvgCapLimited    float64 `json:"avg_cap_limited"`
	AvgCapWouldBlock float64 `json:"avg_cap_would_block"`
}

type p50DiagTotals struct {
	Observed  int
	Missing   int
	DLeNeg6   int
	DNeg5Pos5 int
	D6To11    int
	DGe12     int
	NLt17     int
	N17To99   int
	N100To499 int
	NGe500    int
}

type p50ShadowTotals struct {
	Observed  int
	Missing   int
	Same      int
	MeanGT    int
	P50GT     int
	SevHiLow  int
	SevHiNone int
	N         [4]int
	Band      [12]int
	Mode      [5]int
	Source    [7]int
	Pair      [25]int
}

type p50DiagHour struct {
	Hour         string  `json:"hour"`
	Samples      int     `json:"samples"`
	AvgObserved  float64 `json:"avg_observed"`
	AvgMissing   float64 `json:"avg_missing"`
	AvgDLeNeg6   float64 `json:"avg_d_le_neg6"`
	AvgDNeg5Pos5 float64 `json:"avg_d_neg5_pos5"`
	AvgD6To11    float64 `json:"avg_d_6_11"`
	AvgDGe12     float64 `json:"avg_d_ge12"`
	AvgNLt17     float64 `json:"avg_n_lt17"`
	AvgN17To99   float64 `json:"avg_n_17_99"`
	AvgN100To499 float64 `json:"avg_n_100_499"`
	AvgNGe500    float64 `json:"avg_n_ge500"`
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
	ClampMin                           float64                       `json:"clamp_min"`
	ClampMax                           float64                       `json:"clamp_max"`
	DefaultHalfLifeSec                 int                           `json:"default_half_life_seconds"`
	BandHalfLifeSec                    map[string]int                `json:"band_half_life_seconds"`
	StaleAfterSeconds                  int                           `json:"stale_after_seconds"`
	StaleAfterHalfLifeMultiplier       float64                       `json:"stale_after_half_life_multiplier"`
	StaleAfterByBand                   map[string]int                `json:"stale_after_by_band_seconds"`
	MaxPredictionAgeHalfLifeMultiplier float64                       `json:"max_prediction_age_half_life_multiplier"`
	MaxPredictionAgeByBand             map[string]int                `json:"max_prediction_age_by_band_seconds"`
	MinEffectiveWeight                 float64                       `json:"min_effective_weight"`
	MinObservationCount                int                           `json:"min_observation_count"`
	ReceiverContributionMode           string                        `json:"receiver_contribution_mode"`
	ReceiverFineSlots                  int                           `json:"receiver_fine_slots"`
	ReceiverCoarseSlots                int                           `json:"receiver_coarse_slots"`
	ReceiverMaxEffectiveCount          uint32                        `json:"receiver_max_effective_count"`
	ReceiverMaxEffectiveWeight         float64                       `json:"receiver_max_effective_weight"`
	MinFineWeight                      float64                       `json:"min_fine_weight"`
	ReverseHintDiscount                float64                       `json:"reverse_hint_discount"`
	MergeReceiveWeight                 float64                       `json:"merge_receive_weight"`
	MergeTransmitWeight                float64                       `json:"merge_transmit_weight"`
	NoiseOffsetsByBand                 map[string]map[string]float64 `json:"noise_offsets_by_band"`
}

type openAIConfig struct {
	APIKey       string  `yaml:"api_key"`
	Model        string  `yaml:"model"`
	Endpoint     string  `yaml:"endpoint"`
	MaxTokens    int     `yaml:"max_tokens"`
	Temperature  float64 `yaml:"temperature"`
	SystemPrompt string  `yaml:"system_prompt"`
}

var requiredOpenAIConfigPaths = []yamlconfig.Path{
	{"api_key"},
	{"model"},
	{"endpoint"},
	{"max_tokens"},
	{"temperature"},
	{"system_prompt"},
}

type weightBins struct {
	Total int
	Lt1   int
	Ge10  int
}

type predTotals struct {
	Total         int
	Combined      int
	Insufficient  int
	NoSample      int
	LowCount      int
	LowWeight     int
	Stale         int
	CapLimited    int
	CapWouldBlock int
}

var (
	tsRe          = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`)
	bucketsRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path buckets`)
	weightsRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path weight dist`)
	predsRe       = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path predictions`)
	p50DiagRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path p50 diag`)
	p50ShadowRe   = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path p50 shadow`)
	sourceMixRe   = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path source mix`)
	spottersRe    = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path unique spotters`)
	pairsRe       = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path unique grid pairs`)
	ge10VarRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path ge10 variance`)
	ansiRe        = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	bandBuckets   = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+f=([\d,]+)\s+c=([\d,]+)`)
	bandWeights   = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+t=([\d,]+)\s+<1=([\d,]+)\s+1-2=([\d,]+)\s+2-3=([\d,]+)\s+3-5=([\d,]+)\s+5-10=([\d,]+)\s+>=10=([\d,]+)`)
	predsFields   = regexp.MustCompile(`\b(total|derived|combined|insufficient|no_sample|low_count|low_weight|stale|cap_limited|cap_would_block)=([\d,]+)`)
	p50DiagFields = regexp.MustCompile(`\b(observed|missing|d_le_neg6|d_neg5_pos5|d_6_11|d_ge12|n_lt17|n_17_99|n_100_499|n_ge500)=([\d,]+)`)
	counterFields = regexp.MustCompile(`([A-Za-z0-9_./-]+)=([\d,]+)`)
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

func parsePredictionTotals(line string) (predTotals, bool) {
	matches := predsFields.FindAllStringSubmatch(line, -1)
	if len(matches) == 0 {
		return predTotals{}, false
	}
	values := make(map[string]int, len(matches))
	for _, match := range matches {
		if len(match) != 3 {
			continue
		}
		values[match[1]] = parseInt(match[2])
	}
	for _, required := range []string{"total", "combined", "insufficient", "no_sample", "low_weight"} {
		if _, ok := values[required]; !ok {
			return predTotals{}, false
		}
	}
	return predTotals{
		Total:         values["total"],
		Combined:      values["combined"],
		Insufficient:  values["insufficient"],
		NoSample:      values["no_sample"],
		LowCount:      values["low_count"],
		LowWeight:     values["low_weight"],
		Stale:         values["stale"],
		CapLimited:    values["cap_limited"],
		CapWouldBlock: values["cap_would_block"],
	}, true
}

func parseP50DiagTotals(line string) (p50DiagTotals, bool) {
	matches := p50DiagFields.FindAllStringSubmatch(line, -1)
	if len(matches) == 0 {
		return p50DiagTotals{}, false
	}
	values := make(map[string]int, len(matches))
	for _, match := range matches {
		if len(match) != 3 {
			continue
		}
		values[match[1]] = parseInt(match[2])
	}
	if _, ok := values["observed"]; !ok {
		return p50DiagTotals{}, false
	}
	return p50DiagTotals{
		Observed:  values["observed"],
		Missing:   values["missing"],
		DLeNeg6:   values["d_le_neg6"],
		DNeg5Pos5: values["d_neg5_pos5"],
		D6To11:    values["d_6_11"],
		DGe12:     values["d_ge12"],
		NLt17:     values["n_lt17"],
		N17To99:   values["n_17_99"],
		N100To499: values["n_100_499"],
		NGe500:    values["n_ge500"],
	}, true
}

func parseCounterFields(line string) map[string]int {
	matches := counterFields.FindAllStringSubmatch(line, -1)
	out := make(map[string]int, len(matches))
	for _, match := range matches {
		if len(match) == 3 {
			out[match[1]] = parseInt(match[2])
		}
	}
	return out
}

func parseP50ShadowLine(line string, totals *p50ShadowTotals) bool {
	if totals == nil {
		return false
	}
	values := parseCounterFields(line)
	if len(values) == 0 {
		return false
	}
	switch {
	case strings.Contains(line, "Path p50 shadow n "):
		totals.N[0] = values["n_lt17"]
		totals.N[1] = values["n_17_99"]
		totals.N[2] = values["n_100_499"]
		totals.N[3] = values["n_ge500"]
	case strings.Contains(line, "Path p50 shadow band "):
		for i, key := range []string{"160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "other"} {
			totals.Band[i] = values[key]
		}
	case strings.Contains(line, "Path p50 shadow mode "):
		for i, key := range []string{"CW", "FT", "RTTY", "PHONE", "OTHER"} {
			totals.Mode[i] = values[key]
		}
	case strings.Contains(line, "Path p50 shadow source "):
		for i, key := range []string{"RBN", "RBN-FT", "PSK", "HUMAN", "PEER", "UPSTREAM", "OTHER"} {
			totals.Source[i] = values[key]
		}
	case strings.Contains(line, "Path p50 shadow pair "):
		classes := []string{"I", "U", "L", "M", "H"}
		for meanIdx, meanClass := range classes {
			for p50Idx, p50Class := range classes {
				totals.Pair[meanIdx*len(classes)+p50Idx] = values[meanClass+"/"+p50Class]
			}
		}
	default:
		totals.Observed = values["observed"]
		totals.Missing = values["missing"]
		totals.Same = values["same"]
		totals.MeanGT = values["mean_gt"]
		totals.P50GT = values["p50_gt"]
		totals.SevHiLow = values["sev_hi_low"]
		totals.SevHiNone = values["sev_hi_none"]
	}
	return true
}

func addP50DiagTotals(left p50DiagTotals, right p50DiagTotals) p50DiagTotals {
	left.Observed += right.Observed
	left.Missing += right.Missing
	left.DLeNeg6 += right.DLeNeg6
	left.DNeg5Pos5 += right.DNeg5Pos5
	left.D6To11 += right.D6To11
	left.DGe12 += right.DGe12
	left.NLt17 += right.NLt17
	left.N17To99 += right.N17To99
	left.N100To499 += right.N100To499
	left.NGe500 += right.NGe500
	return left
}

func addP50ShadowTotals(left p50ShadowTotals, right p50ShadowTotals) p50ShadowTotals {
	left.Observed += right.Observed
	left.Missing += right.Missing
	left.Same += right.Same
	left.MeanGT += right.MeanGT
	left.P50GT += right.P50GT
	left.SevHiLow += right.SevHiLow
	left.SevHiNone += right.SevHiNone
	for i := range left.N {
		left.N[i] += right.N[i]
	}
	for i := range left.Band {
		left.Band[i] += right.Band[i]
	}
	for i := range left.Mode {
		left.Mode[i] += right.Mode[i]
	}
	for i := range left.Source {
		left.Source[i] += right.Source[i]
	}
	for i := range left.Pair {
		left.Pair[i] += right.Pair[i]
	}
	return left
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
		return openAIConfig{}, fmt.Errorf("OpenAI config path is required")
	}
	var cfg openAIConfig
	if err := yamlconfig.DecodeFile(path, &cfg, requiredOpenAIConfigPaths); err != nil {
		return openAIConfig{}, err
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.Model = strings.TrimSpace(cfg.Model)
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.SystemPrompt = strings.TrimSpace(cfg.SystemPrompt)
	if cfg.Model == "" {
		return openAIConfig{}, fmt.Errorf("openai.yaml model must not be empty")
	}
	if cfg.Endpoint == "" {
		return openAIConfig{}, fmt.Errorf("openai.yaml endpoint must not be empty")
	}
	parsed, err := url.ParseRequestURI(cfg.Endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return openAIConfig{}, fmt.Errorf("openai.yaml endpoint must be an absolute URL")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return openAIConfig{}, fmt.Errorf("openai.yaml endpoint scheme must be http or https")
	}
	if cfg.MaxTokens <= 0 {
		return openAIConfig{}, fmt.Errorf("openai.yaml max_tokens must be > 0")
	}
	if math.IsNaN(cfg.Temperature) || math.IsInf(cfg.Temperature, 0) || cfg.Temperature < 0 {
		return openAIConfig{}, fmt.Errorf("openai.yaml temperature must be >= 0")
	}
	if cfg.SystemPrompt == "" {
		return openAIConfig{}, fmt.Errorf("openai.yaml system_prompt must not be empty")
	}
	if cfg.APIKey == "" && strings.TrimSpace(os.Getenv("OPENAI_API_KEY")) == "" {
		return openAIConfig{}, fmt.Errorf("OpenAI API key missing; set openai.yaml api_key or OPENAI_API_KEY")
	}
	return cfg, nil
}

func resolveConfigDir(configDir, legacyPath string) string {
	resolved := strings.TrimSpace(configDir)
	if legacy := strings.TrimSpace(legacyPath); legacy != "" {
		resolved = legacy
	}
	if resolved == "" {
		resolved = filepath.Join("data", "config")
	}
	if strings.EqualFold(filepath.Base(resolved), "path_reliability.yaml") {
		resolved = filepath.Dir(resolved)
	}
	return resolved
}

func buildModelContext(cfg pathreliability.Config, bands []string) modelContext {
	staleByBand := make(map[string]int, len(bands))
	maxAgeByBand := make(map[string]int, len(bands))
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
		if cfg.MaxPredictionAgeHalfLifeMultiplier > 0 && halfLife > 0 {
			maxAgeByBand[band] = int(math.Ceil(cfg.MaxPredictionAgeHalfLifeMultiplier * float64(halfLife)))
		} else {
			maxAgeByBand[band] = 0
		}
	}
	return modelContext{
		ClampMin:                           cfg.ClampMin,
		ClampMax:                           cfg.ClampMax,
		DefaultHalfLifeSec:                 cfg.DefaultHalfLifeSec,
		BandHalfLifeSec:                    cfg.BandHalfLifeSec,
		StaleAfterSeconds:                  cfg.StaleAfterSeconds,
		StaleAfterHalfLifeMultiplier:       cfg.StaleAfterHalfLifeMultiplier,
		StaleAfterByBand:                   staleByBand,
		MaxPredictionAgeHalfLifeMultiplier: cfg.MaxPredictionAgeHalfLifeMultiplier,
		MaxPredictionAgeByBand:             maxAgeByBand,
		MinEffectiveWeight:                 cfg.MinEffectiveWeight,
		MinObservationCount:                cfg.MinObservationCount,
		ReceiverContributionMode:           cfg.ReceiverContributionMode,
		ReceiverFineSlots:                  cfg.ReceiverFineSlots,
		ReceiverCoarseSlots:                cfg.ReceiverCoarseSlots,
		ReceiverMaxEffectiveCount:          cfg.ReceiverMaxEffectiveCount,
		ReceiverMaxEffectiveWeight:         cfg.ReceiverMaxEffectiveWeight,
		MinFineWeight:                      cfg.MinFineWeight,
		ReverseHintDiscount:                cfg.ReverseHintDiscount,
		MergeReceiveWeight:                 cfg.MergeReceiveWeight,
		MergeTransmitWeight:                cfg.MergeTransmitWeight,
		NoiseOffsetsByBand:                 cloneNestedFloatMap(cfg.NoiseOffsetsByBand),
	}
}

func cloneNestedFloatMap(in map[string]map[string]float64) map[string]map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]map[string]float64, len(in))
	for k, inner := range in {
		copied := make(map[string]float64, len(inner))
		for innerK, v := range inner {
			copied[innerK] = v
		}
		out[k] = copied
	}
	return out
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
		logPath = filepath.Join("data", "logs", "propagation", fmt.Sprintf("%s.log", date.Format("02-Jan-2006")))
	}
	jsonOut := strings.TrimSpace(opts.JSONOut)
	if jsonOut == "" {
		jsonOut = filepath.Join("data", "reports", fmt.Sprintf("prop-%s.json", date.Format("2006-01-02")))
	}
	reportOut := strings.TrimSpace(opts.ReportOut)
	if reportOut == "" {
		reportOut = filepath.Join("data", "reports", fmt.Sprintf("prop-%s.md", date.Format("2006-01-02")))
	}

	configDir := resolveConfigDir(opts.ConfigDir, opts.PathConfigPath)
	openAIConfigPath := strings.TrimSpace(opts.OpenAIConfigPath)
	if openAIConfigPath == "" {
		openAIConfigPath = filepath.Join("data", "config", "openai.yaml")
	}

	cfg, err := config.Load(configDir)
	if err != nil {
		return result, fmt.Errorf("load config directory for path model context %q: %w", configDir, err)
	}
	pathCfg := cfg.PathReliability

	var openaiCfg openAIConfig
	if !opts.NoLLM {
		var err error
		openaiCfg, err = loadOpenAIConfig(openAIConfigPath)
		if err != nil {
			return result, fmt.Errorf("load OpenAI config %q: %w", openAIConfigPath, err)
		}
	}

	entries, err := parseLog(logPath)
	if err != nil {
		return result, err
	}

	bucketByTS := make(map[string]map[string]int)
	weightByTS := make(map[string]map[string]weightBins)
	predByTS := make(map[string]predTotals)
	p50DiagByTS := make(map[string]p50DiagTotals)
	p50ShadowByTS := make(map[string]*p50ShadowTotals)
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
			totals, ok := parsePredictionTotals(entry)
			if ok {
				predByTS[ts] = totals
			}
		}
		if m := p50DiagRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			totals, ok := parseP50DiagTotals(entry)
			if ok {
				p50DiagByTS[ts] = totals
			}
		}
		if m := p50ShadowRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			totals := p50ShadowByTS[ts]
			if totals == nil {
				totals = &p50ShadowTotals{}
				p50ShadowByTS[ts] = totals
			}
			parseP50ShadowLine(entry, totals)
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
		var total, combined, insufficient, noSample, lowCount, lowWeight, stale, capLimited, capWouldBlock int
		for _, r := range rows {
			total += r.Total
			combined += r.Combined
			insufficient += r.Insufficient
			noSample += r.NoSample
			lowCount += r.LowCount
			lowWeight += r.LowWeight
			stale += r.Stale
			capLimited += r.CapLimited
			capWouldBlock += r.CapWouldBlock
		}
		count := len(rows)
		predSummary = append(predSummary, predictionHour{
			Hour:             fmt.Sprintf("%02d:00", h),
			Samples:          count,
			AvgTotal:         float64(total) / float64(count),
			AvgCombined:      float64(combined) / float64(count),
			AvgInsufficient:  float64(insufficient) / float64(count),
			AvgNoSample:      float64(noSample) / float64(count),
			AvgLowCount:      float64(lowCount) / float64(count),
			AvgLowWeight:     float64(lowWeight) / float64(count),
			AvgStale:         float64(stale) / float64(count),
			AvgCapLimited:    float64(capLimited) / float64(count),
			AvgCapWouldBlock: float64(capWouldBlock) / float64(count),
		})
	}

	p50DiagHours := make(map[int][]p50DiagTotals)
	var p50DiagTotalsAll p50DiagTotals
	for ts, totals := range p50DiagByTS {
		p50DiagTotalsAll = addP50DiagTotals(p50DiagTotalsAll, totals)
		tsTime, err := time.Parse("2006/01/02 15:04:05", ts)
		if err != nil {
			continue
		}
		hour := tsTime.Hour()
		p50DiagHours[hour] = append(p50DiagHours[hour], totals)
	}

	p50DiagSummary := make([]p50DiagHour, 0, len(p50DiagHours))
	var p50DiagHourKeys []int
	for h := range p50DiagHours {
		p50DiagHourKeys = append(p50DiagHourKeys, h)
	}
	sort.Ints(p50DiagHourKeys)
	for _, h := range p50DiagHourKeys {
		rows := p50DiagHours[h]
		if len(rows) == 0 {
			continue
		}
		var observed, missing, dLeNeg6, dNeg5Pos5, d6To11, dGe12, nLt17, n17To99, n100To499, nGe500 int
		for _, r := range rows {
			observed += r.Observed
			missing += r.Missing
			dLeNeg6 += r.DLeNeg6
			dNeg5Pos5 += r.DNeg5Pos5
			d6To11 += r.D6To11
			dGe12 += r.DGe12
			nLt17 += r.NLt17
			n17To99 += r.N17To99
			n100To499 += r.N100To499
			nGe500 += r.NGe500
		}
		count := len(rows)
		p50DiagSummary = append(p50DiagSummary, p50DiagHour{
			Hour:         fmt.Sprintf("%02d:00", h),
			Samples:      count,
			AvgObserved:  float64(observed) / float64(count),
			AvgMissing:   float64(missing) / float64(count),
			AvgDLeNeg6:   float64(dLeNeg6) / float64(count),
			AvgDNeg5Pos5: float64(dNeg5Pos5) / float64(count),
			AvgD6To11:    float64(d6To11) / float64(count),
			AvgDGe12:     float64(dGe12) / float64(count),
			AvgNLt17:     float64(nLt17) / float64(count),
			AvgN17To99:   float64(n17To99) / float64(count),
			AvgN100To499: float64(n100To499) / float64(count),
			AvgNGe500:    float64(nGe500) / float64(count),
		})
	}

	var p50ShadowTotalsAll p50ShadowTotals
	for _, totals := range p50ShadowByTS {
		if totals != nil {
			p50ShadowTotalsAll = addP50ShadowTotals(p50ShadowTotalsAll, *totals)
		}
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
		P50DiagTotals:     p50DiagTotalsAll,
		P50DiagByHour:     p50DiagSummary,
		P50ShadowTotals:   p50ShadowTotalsAll,
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
	fmt.Fprintf(&b, "I reviewed the entire %s log (%s) and summarized per band, by hour how much evidence we have (active fine buckets) and how strong it is (weight distribution). All times are UTC from the log.\n\n", summary.DateUTC, logName)
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
	if summary.P50DiagTotals.Observed > 0 {
		b.WriteString("PATHP50 diagnostic comparison\n\n")
		b.WriteString(p50DiagSummaryText(summary.P50DiagTotals))
		if summary.P50ShadowTotals.Observed > 0 {
			b.WriteString("\n\n")
			b.WriteString(p50ShadowSummaryText(summary.P50ShadowTotals))
		}
		b.WriteString("\n\n")
	}

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
	fmt.Fprintf(b, "Min effective weight: %.2f. Min observations: %d. Min fine weight: %.2f. Reverse hint discount: %.2f.\n",
		ctx.MinEffectiveWeight, ctx.MinObservationCount, ctx.MinFineWeight, ctx.ReverseHintDiscount)
	fmt.Fprintf(b, "Receiver contribution caps: mode=%s fine_slots=%d coarse_slots=%d max_count=%d max_weight=%.2f.\n",
		ctx.ReceiverContributionMode, ctx.ReceiverFineSlots, ctx.ReceiverCoarseSlots, ctx.ReceiverMaxEffectiveCount, ctx.ReceiverMaxEffectiveWeight)
	fmt.Fprintf(b, "Merge weights: receive %.2f / transmit %.2f.\n", ctx.MergeReceiveWeight, ctx.MergeTransmitWeight)
	if ctx.MaxPredictionAgeHalfLifeMultiplier > 0 {
		fmt.Fprintf(b, "Prediction freshness gate: %.2fx half-life; older selected evidence is treated as insufficient.\n", ctx.MaxPredictionAgeHalfLifeMultiplier)
	} else {
		b.WriteString("Prediction freshness gate: disabled.\n")
	}
	if len(ctx.NoiseOffsetsByBand) > 0 {
		b.WriteString("Noise offsets by band (dB): ")
		b.WriteString(formatNoiseOffsetsByBand(ctx.NoiseOffsetsByBand))
		b.WriteString(".\n")
	}
	if len(bands) > 0 {
		b.WriteString("Per-band half-life/stale/max-age (seconds): ")
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
			maxAge := ctx.MaxPredictionAgeByBand[band.Band]
			parts = append(parts, fmt.Sprintf("%s hl=%d stale=%d max_age=%d", band.Band, hl, stale, maxAge))
		}
		b.WriteString(strings.Join(parts, "; "))
		b.WriteString(".\n")
	}
}

func formatNoiseOffsetsByBand(offsets map[string]map[string]float64) string {
	classes := make([]string, 0, len(offsets))
	for class := range offsets {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	parts := make([]string, 0, len(classes))
	for _, class := range classes {
		byBand := offsets[class]
		bands := make([]string, 0, len(byBand))
		for band := range byBand {
			bands = append(bands, band)
		}
		sort.Slice(bands, func(i, j int) bool {
			gi, vi, si := bandSortKey(bands[i])
			gj, vj, sj := bandSortKey(bands[j])
			if gi != gj {
				return gi < gj
			}
			if vi != vj {
				return vi < vj
			}
			return si < sj
		})
		values := make([]string, 0, len(bands))
		for _, band := range bands {
			values = append(values, fmt.Sprintf("%s=%g", band, byBand[band]))
		}
		parts = append(parts, fmt.Sprintf("%s{%s}", class, strings.Join(values, ",")))
	}
	return strings.Join(parts, "; ")
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
	var lowCountSample []string
	var lowWeightSample []string
	var staleSample []string
	var capWouldBlockSample []string
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
		if h.AvgLowCount > h.AvgLowWeight && h.AvgLowCount > 0 {
			lowCountSample = append(lowCountSample, h.Hour)
		}
		if h.AvgLowWeight > h.AvgLowCount && h.AvgLowWeight > 0 {
			lowWeightSample = append(lowWeightSample, h.Hour)
		}
		if h.AvgStale > 0 {
			staleSample = append(staleSample, h.Hour)
		}
		if h.AvgCapWouldBlock > 0 {
			capWouldBlockSample = append(capWouldBlockSample, h.Hour)
		}
	}
	s := fmt.Sprintf("Peak prediction volume occurs around %s (avg_total %.1f), with the lowest activity around %s (avg_total %.1f).",
		maxHour, maxTotal, minHour, minTotal)
	if len(lowSample) > 0 {
		s += fmt.Sprintf(" Hours dominated by insufficient samples: %s.", strings.Join(lowSample, ", "))
	}
	if len(lowCountSample) > 0 {
		s += fmt.Sprintf(" Hours mostly count-limited: %s.", strings.Join(lowCountSample, ", "))
	}
	if len(lowWeightSample) > 0 {
		s += fmt.Sprintf(" Hours mostly weight-limited: %s.", strings.Join(lowWeightSample, ", "))
	}
	if len(staleSample) > 0 {
		s += fmt.Sprintf(" Hours with stale selected evidence: %s.", strings.Join(staleSample, ", "))
	}
	if len(capWouldBlockSample) > 0 {
		s += fmt.Sprintf(" Hours where receiver caps would block shadow predictions: %s.", strings.Join(capWouldBlockSample, ", "))
	}
	return s
}

func p50DiagSummaryText(totals p50DiagTotals) string {
	if totals.Observed == 0 {
		return "No PATHP50 diagnostic comparison data recorded for this day."
	}
	return fmt.Sprintf(
		"Diagnostic-observed PATHP50 comparisons: observed %d, missing %d. Delta buckets mean-minus-p50: <=-6 %d, -5..5 %d, 6..11 %d, >=12 %d. Sample buckets: n<17 %d, n=17..99 %d, n=100..499 %d, n>=500 %d.",
		totals.Observed, totals.Missing, totals.DLeNeg6, totals.DNeg5Pos5, totals.D6To11, totals.DGe12, totals.NLt17, totals.N17To99, totals.N100To499, totals.NGe500,
	)
}

func p50ShadowSummaryText(totals p50ShadowTotals) string {
	if totals.Observed == 0 {
		return "No PATHP50 shadow glyph comparison data recorded for this day."
	}
	return fmt.Sprintf(
		"PATHP50 shadow glyph comparison: same %d, mean stronger %d, p50 stronger %d, severe mean-optimistic %d, severe p50-missing %d. Sample buckets: n<17 %d, n=17..99 %d, n=100..499 %d, n>=500 %d. Mode context: CW %d, FT %d, RTTY %d, PHONE %d, OTHER %d.",
		totals.Same, totals.MeanGT, totals.P50GT, totals.SevHiLow, totals.SevHiNone,
		totals.N[0], totals.N[1], totals.N[2], totals.N[3],
		totals.Mode[0], totals.Mode[1], totals.Mode[2], totals.Mode[3], totals.Mode[4],
	)
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
