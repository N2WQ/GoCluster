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
	"dxcluster/internal/logutil"
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
	DateUTC               string                  `json:"date_utc"`
	LogFile               string                  `json:"log_file"`
	Timezone              string                  `json:"timezone"`
	ModelContext          modelContext            `json:"model_context"`
	Bands                 []bandSummary           `json:"bands"`
	BandGroups            map[string][]string     `json:"band_groups"`
	CoverageMedians       map[string]coverageStat `json:"coverage_medians_by_band"`
	PredictionsByHour     []predictionHour        `json:"predictions_by_hour"`
	SparseP50VOACAPByHour []sparseP50VOACAPHour   `json:"sparse_p50_voacap_by_hour"`
	CapShadowByHour       []capShadowHour         `json:"cap_shadow_by_hour"`
	CapP50ShadowByHour    []capP50ShadowHour      `json:"cap_p50_shadow_by_hour"`
	SourceMixByHour       []sourceMixHour         `json:"source_mix_by_hour"`
	Thresholds            classificationThreshold `json:"thresholds"`
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
	Hour                           string  `json:"hour"`
	Samples                        int     `json:"samples"`
	AvgTotal                       float64 `json:"avg_total"`
	AvgCombined                    float64 `json:"avg_combined"`
	AvgVOACAPClosed                float64 `json:"avg_voacap_closed"`
	AvgVOACAPAligned               float64 `json:"avg_voacap_aligned"`
	AvgVOACAPSparseUpgrade         float64 `json:"avg_voacap_sparse_upgrade"`
	AvgVOACAPOpen                  float64 `json:"avg_voacap_open"`
	AvgBeaconRX                    float64 `json:"avg_beacon_rx"`
	AvgBeaconRXInsufficient        float64 `json:"avg_beacon_rx_insufficient"`
	AvgBeaconRXNoSample            float64 `json:"avg_beacon_rx_no_sample"`
	AvgBeaconRXLowCount            float64 `json:"avg_beacon_rx_low_count"`
	AvgBeaconRXLowReceiver         float64 `json:"avg_beacon_rx_low_receiver"`
	AvgBeaconRXLowWeight           float64 `json:"avg_beacon_rx_low_weight"`
	AvgBeaconRXStale               float64 `json:"avg_beacon_rx_stale"`
	AvgBeaconRXVOACAPClosed        float64 `json:"avg_beacon_rx_voacap_closed"`
	AvgBeaconRXVOACAPAligned       float64 `json:"avg_beacon_rx_voacap_aligned"`
	AvgBeaconRXVOACAPSparseUpgrade float64 `json:"avg_beacon_rx_voacap_sparse_upgrade"`
	AvgBeaconRXVOACAPOpen          float64 `json:"avg_beacon_rx_voacap_open"`
	AvgInsufficient                float64 `json:"avg_insufficient"`
	AvgNoSample                    float64 `json:"avg_no_sample"`
	AvgLowCount                    float64 `json:"avg_low_count"`
	AvgLowReceiver                 float64 `json:"avg_low_receiver"`
	AvgLowWeight                   float64 `json:"avg_low_weight"`
	AvgStale                       float64 `json:"avg_stale"`
	AvgCapLimited                  float64 `json:"avg_cap_limited"`
	AvgCapWouldBlock               float64 `json:"avg_cap_would_block"`
}

type sparseP50VOACAPHour struct {
	Hour                       string  `json:"hour"`
	Samples                    int     `json:"samples"`
	AvgTotal                   float64 `json:"avg_total"`
	AvgNoP50                   float64 `json:"avg_no_p50"`
	AvgVeryLowCount            float64 `json:"avg_very_low_count"`
	AvgBeaconRX                float64 `json:"avg_beacon_rx"`
	AvgNonBeacon               float64 `json:"avg_non_beacon"`
	AvgCacheMissTotal          float64 `json:"avg_cache_miss_total"`
	AvgCacheHit                float64 `json:"avg_cache_hit"`
	AvgQueued                  float64 `json:"avg_queued"`
	AvgDelayed                 float64 `json:"avg_delayed"`
	AvgInflight                float64 `json:"avg_inflight"`
	AvgInvalidRequest          float64 `json:"avg_invalid_request"`
	AvgInvalidUnsupportedBand  float64 `json:"avg_invalid_unsupported_band"`
	AvgInvalidEmptyUnknownBand float64 `json:"avg_invalid_empty_unknown_band"`
	AvgInvalidUserGrid         float64 `json:"avg_invalid_user_grid"`
	AvgInvalidDXGrid           float64 `json:"avg_invalid_dx_grid"`
	AvgInvalidUserCell         float64 `json:"avg_invalid_user_cell"`
	AvgInvalidDXCell           float64 `json:"avg_invalid_dx_cell"`
	AvgSSNUnavailable          float64 `json:"avg_ssn_unavailable"`
	AvgNoCurrentHour           float64 `json:"avg_no_current_hour"`
	AvgQueueFull               float64 `json:"avg_queue_full"`
	AvgNotRunning              float64 `json:"avg_not_running"`
	AvgDisabled                float64 `json:"avg_disabled"`
	AvgUnavailable             float64 `json:"avg_unavailable"`
	AvgClosed                  float64 `json:"avg_closed"`
	AvgAligned                 float64 `json:"avg_aligned"`
	AvgSparseUpgrade           float64 `json:"avg_sparse_upgrade"`
	AvgOpenRELPass             float64 `json:"avg_open_rel_pass"`
	AvgOpenRELFail             float64 `json:"avg_open_rel_fail"`
	AvgNotClosed               float64 `json:"avg_not_closed"`
	AvgRELMissing              float64 `json:"avg_rel_missing"`
	AvgRELBelowFloor           float64 `json:"avg_rel_below_floor"`
	AvgRELMultiTier            float64 `json:"avg_rel_multi_tier"`
}

type capShadowHour struct {
	Hour       string                   `json:"hour"`
	Samples    int                      `json:"samples"`
	Candidates []capShadowCandidateHour `json:"candidates"`
}

type capShadowCandidateHour struct {
	MaxEffectiveCount uint32  `json:"max_effective_count"`
	AvgPass           float64 `json:"avg_pass"`
	AvgLowCount       float64 `json:"avg_low_count"`
	AvgLowReceiver    float64 `json:"avg_low_receiver"`
	AvgLowWeight      float64 `json:"avg_low_weight"`
	AvgBlock          float64 `json:"avg_block"`
}

type capP50ShadowHour struct {
	Hour       string                      `json:"hour"`
	Samples    int                         `json:"samples"`
	Candidates []capP50ShadowCandidateHour `json:"candidates"`
}

type capP50ShadowCandidateHour struct {
	MaxEffectiveCount uint32  `json:"max_effective_count"`
	AvgPassUnlikely   float64 `json:"avg_pass_unlikely"`
	AvgPassLow        float64 `json:"avg_pass_low"`
	AvgPassMedium     float64 `json:"avg_pass_medium"`
	AvgPassHigh       float64 `json:"avg_pass_high"`
	AvgSame           float64 `json:"avg_same"`
	AvgStronger       float64 `json:"avg_stronger"`
	AvgWeaker         float64 `json:"avg_weaker"`
	AvgToInsufficient float64 `json:"avg_to_insufficient"`
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
	DefaultHalfLifeSec                 int                `json:"default_half_life_seconds"`
	BandHalfLifeSec                    map[string]int     `json:"band_half_life_seconds"`
	StaleAfterSeconds                  int                `json:"stale_after_seconds"`
	StaleAfterHalfLifeMultiplier       float64            `json:"stale_after_half_life_multiplier"`
	StaleAfterByBand                   map[string]int     `json:"stale_after_by_band_seconds"`
	MaxPredictionAgeHalfLifeMultiplier float64            `json:"max_prediction_age_half_life_multiplier"`
	MaxPredictionAgeByBand             map[string]int     `json:"max_prediction_age_by_band_seconds"`
	MinEffectiveWeight                 float64            `json:"min_effective_weight"`
	MinObservationCount                int                `json:"min_observation_count"`
	BeaconMinObservationCount          int                `json:"beacon_min_observation_count"`
	ReceiverContributionMode           string             `json:"receiver_contribution_mode"`
	ReceiverFineSlots                  int                `json:"receiver_fine_slots"`
	ReceiverCoarseSlots                int                `json:"receiver_coarse_slots"`
	ReceiverMaxEffectiveCount          uint32             `json:"receiver_max_effective_count"`
	ReceiverMaxEffectiveWeight         float64            `json:"receiver_max_effective_weight"`
	MinFineWeight                      float64            `json:"min_fine_weight"`
	ReverseHintDiscount                float64            `json:"reverse_hint_discount"`
	MergeReceiveWeight                 float64            `json:"merge_receive_weight"`
	MergeTransmitWeight                float64            `json:"merge_transmit_weight"`
	NoiseOffsets                       map[string]float64 `json:"noise_offsets"`
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
	Total                       int
	Combined                    int
	VOACAPClosed                int
	VOACAPAligned               int
	VOACAPSparseUpgrade         int
	VOACAPOpen                  int
	BeaconRX                    int
	BeaconRXInsufficient        int
	BeaconRXNoSample            int
	BeaconRXLowCount            int
	BeaconRXLowReceiver         int
	BeaconRXLowWeight           int
	BeaconRXStale               int
	BeaconRXVOACAPClosed        int
	BeaconRXVOACAPAligned       int
	BeaconRXVOACAPSparseUpgrade int
	BeaconRXVOACAPOpen          int
	Insufficient                int
	NoSample                    int
	LowCount                    int
	LowReceiver                 int
	LowWeight                   int
	Stale                       int
	CapLimited                  int
	CapWouldBlock               int
}

type sparseP50VOACAPTotals struct {
	Total                   int
	NoP50                   int
	VeryLowCount            int
	BeaconRX                int
	NonBeacon               int
	CacheMissTotal          int
	CacheHit                int
	Queued                  int
	Delayed                 int
	Inflight                int
	InvalidRequest          int
	InvalidUnsupportedBand  int
	InvalidEmptyUnknownBand int
	InvalidUserGrid         int
	InvalidDXGrid           int
	InvalidUserCell         int
	InvalidDXCell           int
	SSNUnavailable          int
	NoCurrentHour           int
	QueueFull               int
	NotRunning              int
	Disabled                int
	Unavailable             int
	Closed                  int
	Aligned                 int
	SparseUpgrade           int
	OpenRELPass             int
	OpenRELFail             int
	NotClosed               int
	RELMissing              int
	RELBelowFloor           int
	RELMultiTier            int
}

type capShadowTotals struct {
	Total      int
	Candidates []capShadowCandidateTotals
}

type capShadowCandidateTotals struct {
	MaxEffectiveCount uint32
	Pass              int
	LowCount          int
	LowReceiver       int
	LowWeight         int
	Block             int
}

type capP50ShadowTotals struct {
	Total      int
	Candidates []capP50ShadowCandidateTotals
}

type capP50ShadowCandidateTotals struct {
	MaxEffectiveCount uint32
	PassUnlikely      int
	PassLow           int
	PassMedium        int
	PassHigh          int
	Same              int
	Stronger          int
	Weaker            int
	ToInsufficient    int
}

var (
	tsRe                  = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`)
	bucketsRe             = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path buckets`)
	weightsRe             = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path weight dist`)
	predsRe               = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path predictions`)
	sparseP50VOACAPRe     = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Sparse p50 VOACAP`)
	capShadowRe           = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path cap shadow`)
	capP50ShadowRe        = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path cap p50 shadow`)
	sourceMixRe           = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path source mix`)
	spottersRe            = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path unique spotters`)
	pairsRe               = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path unique grid pairs`)
	ge10VarRe             = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Path ge10 variance`)
	ansiRe                = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	bandBuckets           = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+f=([\d,]+)\s+c=([\d,]+)`)
	bandWeights           = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+t=([\d,]+)\s+<1=([\d,]+)\s+1-2=([\d,]+)\s+2-3=([\d,]+)\s+3-5=([\d,]+)\s+5-10=([\d,]+)\s+>=10=([\d,]+)`)
	predsFields           = regexp.MustCompile(`\b(total|derived|combined|voacap_closed|voacap_aligned|voacap_sparse_upgrade|voacap_open|beacon_rx|beacon_rx_insufficient|beacon_rx_no_sample|beacon_rx_low_count|beacon_rx_low_receiver|beacon_rx_low_weight|beacon_rx_stale|beacon_rx_voacap_closed|beacon_rx_voacap_aligned|beacon_rx_voacap_sparse_upgrade|beacon_rx_voacap_open|insufficient|no_sample|low_count|low_receiver|low_weight|stale|cap_limited|cap_would_block)=([\d,]+)`)
	sparseP50VOACAPFields = regexp.MustCompile(`\b(total|no_p50|very_low_count|beacon_rx|non_beacon|cache_miss_total|cache_hit|queued|delayed|inflight|invalid_request|invalid_unsupported_band|invalid_empty_unknown_band|invalid_user_grid|invalid_dx_grid|invalid_user_cell|invalid_dx_cell|ssn_unavailable|no_current_hour|queue_full|not_running|disabled|unavailable|closed|aligned|sparse_upgrade|open_rel_pass|open_rel_fail|not_closed|rel_missing|rel_below_floor|rel_multi_tier)=([\d,]+)`)
	totalField            = regexp.MustCompile(`\btotal=([\d,]+)`)
	capShadowField        = regexp.MustCompile(`\bcap(\d+)_(pass|low_count|low_receiver|low_weight|block)=([\d,]+)`)
	capP50ShadowField     = regexp.MustCompile(`\bcap(\d+)_p50_(pass_unlikely|pass_low|pass_medium|pass_high|same|stronger|weaker|to_insufficient)=([\d,]+)`)
	sourceFields          = regexp.MustCompile(`([A-Za-z\-]+)=([\d,]+)`)
	hourField             = regexp.MustCompile(`hour=(\d{2})`)
	bandCounts            = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)=([\d,]+)`)
	ge10VarFields         = regexp.MustCompile(`(\d+\.?\d*cm|\d+m)\s+min=(\d+)\s+med=(\d+)\s+p75=(\d+)\s+max=(\d+)\s+deg=(\d)`)
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
		Total:                       values["total"],
		Combined:                    values["combined"],
		VOACAPClosed:                values["voacap_closed"],
		VOACAPAligned:               values["voacap_aligned"],
		VOACAPSparseUpgrade:         values["voacap_sparse_upgrade"],
		VOACAPOpen:                  values["voacap_open"],
		BeaconRX:                    values["beacon_rx"],
		BeaconRXInsufficient:        values["beacon_rx_insufficient"],
		BeaconRXNoSample:            values["beacon_rx_no_sample"],
		BeaconRXLowCount:            values["beacon_rx_low_count"],
		BeaconRXLowReceiver:         values["beacon_rx_low_receiver"],
		BeaconRXLowWeight:           values["beacon_rx_low_weight"],
		BeaconRXStale:               values["beacon_rx_stale"],
		BeaconRXVOACAPClosed:        values["beacon_rx_voacap_closed"],
		BeaconRXVOACAPAligned:       values["beacon_rx_voacap_aligned"],
		BeaconRXVOACAPSparseUpgrade: values["beacon_rx_voacap_sparse_upgrade"],
		BeaconRXVOACAPOpen:          values["beacon_rx_voacap_open"],
		Insufficient:                values["insufficient"],
		NoSample:                    values["no_sample"],
		LowCount:                    values["low_count"],
		LowReceiver:                 values["low_receiver"],
		LowWeight:                   values["low_weight"],
		Stale:                       values["stale"],
		CapLimited:                  values["cap_limited"],
		CapWouldBlock:               values["cap_would_block"],
	}, true
}

func parseSparseP50VOACAPTotals(line string) (sparseP50VOACAPTotals, bool) {
	matches := sparseP50VOACAPFields.FindAllStringSubmatch(line, -1)
	if len(matches) == 0 {
		return sparseP50VOACAPTotals{}, false
	}
	values := make(map[string]int, len(matches))
	for _, match := range matches {
		if len(match) != 3 {
			continue
		}
		values[match[1]] = parseInt(match[2])
	}
	for _, required := range []string{"total", "no_p50", "cache_miss_total"} {
		if _, ok := values[required]; !ok {
			return sparseP50VOACAPTotals{}, false
		}
	}
	return sparseP50VOACAPTotals{
		Total:                   values["total"],
		NoP50:                   values["no_p50"],
		VeryLowCount:            values["very_low_count"],
		BeaconRX:                values["beacon_rx"],
		NonBeacon:               values["non_beacon"],
		CacheMissTotal:          values["cache_miss_total"],
		CacheHit:                values["cache_hit"],
		Queued:                  values["queued"],
		Delayed:                 values["delayed"],
		Inflight:                values["inflight"],
		InvalidRequest:          values["invalid_request"],
		InvalidUnsupportedBand:  values["invalid_unsupported_band"],
		InvalidEmptyUnknownBand: values["invalid_empty_unknown_band"],
		InvalidUserGrid:         values["invalid_user_grid"],
		InvalidDXGrid:           values["invalid_dx_grid"],
		InvalidUserCell:         values["invalid_user_cell"],
		InvalidDXCell:           values["invalid_dx_cell"],
		SSNUnavailable:          values["ssn_unavailable"],
		NoCurrentHour:           values["no_current_hour"],
		QueueFull:               values["queue_full"],
		NotRunning:              values["not_running"],
		Disabled:                values["disabled"],
		Unavailable:             values["unavailable"],
		Closed:                  values["closed"],
		Aligned:                 values["aligned"],
		SparseUpgrade:           values["sparse_upgrade"],
		OpenRELPass:             values["open_rel_pass"],
		OpenRELFail:             values["open_rel_fail"],
		NotClosed:               values["not_closed"],
		RELMissing:              values["rel_missing"],
		RELBelowFloor:           values["rel_below_floor"],
		RELMultiTier:            values["rel_multi_tier"],
	}, true
}

func parseCapShadowTotals(line string) (capShadowTotals, bool) {
	total := 0
	if m := totalField.FindStringSubmatch(line); len(m) == 2 {
		total = parseInt(m[1])
	}
	matches := capShadowField.FindAllStringSubmatch(line, -1)
	if len(matches) == 0 {
		return capShadowTotals{}, false
	}
	byCap := make(map[uint32]*capShadowCandidateTotals, 3)
	var caps []uint32
	for _, match := range matches {
		if len(match) != 4 {
			continue
		}
		capValue64, err := strconv.ParseUint(match[1], 10, 32)
		if err != nil || capValue64 == 0 {
			continue
		}
		capValue := uint32(capValue64)
		candidate := byCap[capValue]
		if candidate == nil {
			candidate = &capShadowCandidateTotals{MaxEffectiveCount: capValue}
			byCap[capValue] = candidate
			caps = append(caps, capValue)
		}
		value := parseInt(match[3])
		switch match[2] {
		case "pass":
			candidate.Pass = value
		case "low_count":
			candidate.LowCount = value
		case "low_receiver":
			candidate.LowReceiver = value
		case "low_weight":
			candidate.LowWeight = value
		case "block":
			candidate.Block = value
		}
	}
	if len(caps) == 0 {
		return capShadowTotals{}, false
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i] < caps[j] })
	out := capShadowTotals{
		Total:      total,
		Candidates: make([]capShadowCandidateTotals, 0, len(caps)),
	}
	for _, capValue := range caps {
		out.Candidates = append(out.Candidates, *byCap[capValue])
	}
	return out, true
}

func buildSparseP50VOACAPSummary(byTS map[string]*sparseP50VOACAPTotals) []sparseP50VOACAPHour {
	hours := make(map[int][]*sparseP50VOACAPTotals)
	for ts, totals := range byTS {
		tsTime, err := time.Parse("2006/01/02 15:04:05", ts)
		if err != nil {
			continue
		}
		hours[tsTime.Hour()] = append(hours[tsTime.Hour()], totals)
	}
	keys := make([]int, 0, len(hours))
	for h := range hours {
		keys = append(keys, h)
	}
	sort.Ints(keys)
	summary := make([]sparseP50VOACAPHour, 0, len(keys))
	for _, h := range keys {
		rows := hours[h]
		if len(rows) == 0 {
			continue
		}
		var total, noP50, veryLowCount, beaconRX, nonBeacon, cacheMissTotal, cacheHit int
		var queued, delayed, inflight, invalidRequest, invalidUnsupportedBand int
		var invalidEmptyUnknownBand, invalidUserGrid, invalidDXGrid, invalidUserCell, invalidDXCell int
		var ssnUnavailable, noCurrentHour int
		var queueFull, notRunning, disabled, unavailable int
		var closed, aligned, sparseUpgrade, openRELPass, openRELFail, notClosed int
		var relMissing, relBelowFloor, relMultiTier int
		for _, r := range rows {
			total += r.Total
			noP50 += r.NoP50
			veryLowCount += r.VeryLowCount
			beaconRX += r.BeaconRX
			nonBeacon += r.NonBeacon
			cacheMissTotal += r.CacheMissTotal
			cacheHit += r.CacheHit
			queued += r.Queued
			delayed += r.Delayed
			inflight += r.Inflight
			invalidRequest += r.InvalidRequest
			invalidUnsupportedBand += r.InvalidUnsupportedBand
			invalidEmptyUnknownBand += r.InvalidEmptyUnknownBand
			invalidUserGrid += r.InvalidUserGrid
			invalidDXGrid += r.InvalidDXGrid
			invalidUserCell += r.InvalidUserCell
			invalidDXCell += r.InvalidDXCell
			ssnUnavailable += r.SSNUnavailable
			noCurrentHour += r.NoCurrentHour
			queueFull += r.QueueFull
			notRunning += r.NotRunning
			disabled += r.Disabled
			unavailable += r.Unavailable
			closed += r.Closed
			aligned += r.Aligned
			sparseUpgrade += r.SparseUpgrade
			openRELPass += r.OpenRELPass
			openRELFail += r.OpenRELFail
			notClosed += r.NotClosed
			relMissing += r.RELMissing
			relBelowFloor += r.RELBelowFloor
			relMultiTier += r.RELMultiTier
		}
		count := float64(len(rows))
		summary = append(summary, sparseP50VOACAPHour{
			Hour:                       fmt.Sprintf("%02d:00", h),
			Samples:                    len(rows),
			AvgTotal:                   float64(total) / count,
			AvgNoP50:                   float64(noP50) / count,
			AvgVeryLowCount:            float64(veryLowCount) / count,
			AvgBeaconRX:                float64(beaconRX) / count,
			AvgNonBeacon:               float64(nonBeacon) / count,
			AvgCacheMissTotal:          float64(cacheMissTotal) / count,
			AvgCacheHit:                float64(cacheHit) / count,
			AvgQueued:                  float64(queued) / count,
			AvgDelayed:                 float64(delayed) / count,
			AvgInflight:                float64(inflight) / count,
			AvgInvalidRequest:          float64(invalidRequest) / count,
			AvgInvalidUnsupportedBand:  float64(invalidUnsupportedBand) / count,
			AvgInvalidEmptyUnknownBand: float64(invalidEmptyUnknownBand) / count,
			AvgInvalidUserGrid:         float64(invalidUserGrid) / count,
			AvgInvalidDXGrid:           float64(invalidDXGrid) / count,
			AvgInvalidUserCell:         float64(invalidUserCell) / count,
			AvgInvalidDXCell:           float64(invalidDXCell) / count,
			AvgSSNUnavailable:          float64(ssnUnavailable) / count,
			AvgNoCurrentHour:           float64(noCurrentHour) / count,
			AvgQueueFull:               float64(queueFull) / count,
			AvgNotRunning:              float64(notRunning) / count,
			AvgDisabled:                float64(disabled) / count,
			AvgUnavailable:             float64(unavailable) / count,
			AvgClosed:                  float64(closed) / count,
			AvgAligned:                 float64(aligned) / count,
			AvgSparseUpgrade:           float64(sparseUpgrade) / count,
			AvgOpenRELPass:             float64(openRELPass) / count,
			AvgOpenRELFail:             float64(openRELFail) / count,
			AvgNotClosed:               float64(notClosed) / count,
			AvgRELMissing:              float64(relMissing) / count,
			AvgRELBelowFloor:           float64(relBelowFloor) / count,
			AvgRELMultiTier:            float64(relMultiTier) / count,
		})
	}
	return summary
}

func parseCapP50ShadowTotals(line string) (capP50ShadowTotals, bool) {
	total := 0
	if m := totalField.FindStringSubmatch(line); len(m) == 2 {
		total = parseInt(m[1])
	}
	matches := capP50ShadowField.FindAllStringSubmatch(line, -1)
	if len(matches) == 0 {
		return capP50ShadowTotals{}, false
	}
	byCap := make(map[uint32]*capP50ShadowCandidateTotals, 3)
	var caps []uint32
	for _, match := range matches {
		if len(match) != 4 {
			continue
		}
		capValue64, err := strconv.ParseUint(match[1], 10, 32)
		if err != nil || capValue64 == 0 {
			continue
		}
		capValue := uint32(capValue64)
		candidate := byCap[capValue]
		if candidate == nil {
			candidate = &capP50ShadowCandidateTotals{MaxEffectiveCount: capValue}
			byCap[capValue] = candidate
			caps = append(caps, capValue)
		}
		value := parseInt(match[3])
		switch match[2] {
		case "pass_unlikely":
			candidate.PassUnlikely = value
		case "pass_low":
			candidate.PassLow = value
		case "pass_medium":
			candidate.PassMedium = value
		case "pass_high":
			candidate.PassHigh = value
		case "same":
			candidate.Same = value
		case "stronger":
			candidate.Stronger = value
		case "weaker":
			candidate.Weaker = value
		case "to_insufficient":
			candidate.ToInsufficient = value
		}
	}
	if len(caps) == 0 {
		return capP50ShadowTotals{}, false
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i] < caps[j] })
	out := capP50ShadowTotals{
		Total:      total,
		Candidates: make([]capP50ShadowCandidateTotals, 0, len(caps)),
	}
	for _, capValue := range caps {
		out.Candidates = append(out.Candidates, *byCap[capValue])
	}
	return out, true
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
		DefaultHalfLifeSec:                 cfg.DefaultHalfLifeSec,
		BandHalfLifeSec:                    cfg.BandHalfLifeSec,
		StaleAfterSeconds:                  cfg.StaleAfterSeconds,
		StaleAfterHalfLifeMultiplier:       cfg.StaleAfterHalfLifeMultiplier,
		StaleAfterByBand:                   staleByBand,
		MaxPredictionAgeHalfLifeMultiplier: cfg.MaxPredictionAgeHalfLifeMultiplier,
		MaxPredictionAgeByBand:             maxAgeByBand,
		MinEffectiveWeight:                 cfg.MinEffectiveWeight,
		MinObservationCount:                cfg.MinObservationCount,
		BeaconMinObservationCount:          cfg.BeaconMinObservationCount,
		ReceiverContributionMode:           cfg.ReceiverContributionMode,
		ReceiverFineSlots:                  cfg.ReceiverFineSlots,
		ReceiverCoarseSlots:                cfg.ReceiverCoarseSlots,
		ReceiverMaxEffectiveCount:          cfg.ReceiverMaxEffectiveCount,
		ReceiverMaxEffectiveWeight:         cfg.ReceiverMaxEffectiveWeight,
		MinFineWeight:                      cfg.MinFineWeight,
		ReverseHintDiscount:                cfg.ReverseHintDiscount,
		MergeReceiveWeight:                 cfg.MergeReceiveWeight,
		MergeTransmitWeight:                cfg.MergeTransmitWeight,
		NoiseOffsets:                       cloneFloatMap(cfg.NoiseOffsets),
	}
}

func cloneFloatMap(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
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
		logPath = logutil.DailyArchivePath(filepath.Join("data", "logs", "propagation"), date)
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
	predByTS := make(map[string]*predTotals)
	sparseP50VOACAPByTS := make(map[string]*sparseP50VOACAPTotals)
	capShadowByTS := make(map[string]capShadowTotals)
	capP50ShadowByTS := make(map[string]capP50ShadowTotals)
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
				predByTS[ts] = &totals
			}
		}
		if m := sparseP50VOACAPRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			totals, ok := parseSparseP50VOACAPTotals(entry)
			if ok {
				sparseP50VOACAPByTS[ts] = &totals
			}
		}
		if m := capShadowRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			totals, ok := parseCapShadowTotals(entry)
			if ok {
				capShadowByTS[ts] = totals
			}
		}
		if m := capP50ShadowRe.FindStringSubmatch(entry); len(m) == 2 {
			ts := m[1]
			totals, ok := parseCapP50ShadowTotals(entry)
			if ok {
				capP50ShadowByTS[ts] = totals
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

	predHours := make(map[int][]*predTotals)
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
		var total, combined, voacapClosed, voacapAligned, voacapSparseUpgrade, voacapOpen, insufficient, noSample, lowCount, lowReceiver, lowWeight, stale, capLimited, capWouldBlock int
		var beaconRX, beaconRXInsufficient, beaconRXNoSample, beaconRXLowCount, beaconRXLowReceiver, beaconRXLowWeight, beaconRXStale int
		var beaconRXVOACAPClosed, beaconRXVOACAPAligned, beaconRXVOACAPSparseUpgrade, beaconRXVOACAPOpen int
		for _, r := range rows {
			total += r.Total
			combined += r.Combined
			voacapClosed += r.VOACAPClosed
			voacapAligned += r.VOACAPAligned
			voacapSparseUpgrade += r.VOACAPSparseUpgrade
			voacapOpen += r.VOACAPOpen
			beaconRX += r.BeaconRX
			beaconRXInsufficient += r.BeaconRXInsufficient
			beaconRXNoSample += r.BeaconRXNoSample
			beaconRXLowCount += r.BeaconRXLowCount
			beaconRXLowReceiver += r.BeaconRXLowReceiver
			beaconRXLowWeight += r.BeaconRXLowWeight
			beaconRXStale += r.BeaconRXStale
			beaconRXVOACAPClosed += r.BeaconRXVOACAPClosed
			beaconRXVOACAPAligned += r.BeaconRXVOACAPAligned
			beaconRXVOACAPSparseUpgrade += r.BeaconRXVOACAPSparseUpgrade
			beaconRXVOACAPOpen += r.BeaconRXVOACAPOpen
			insufficient += r.Insufficient
			noSample += r.NoSample
			lowCount += r.LowCount
			lowReceiver += r.LowReceiver
			lowWeight += r.LowWeight
			stale += r.Stale
			capLimited += r.CapLimited
			capWouldBlock += r.CapWouldBlock
		}
		count := len(rows)
		predSummary = append(predSummary, predictionHour{
			Hour:                           fmt.Sprintf("%02d:00", h),
			Samples:                        count,
			AvgTotal:                       float64(total) / float64(count),
			AvgCombined:                    float64(combined) / float64(count),
			AvgVOACAPClosed:                float64(voacapClosed) / float64(count),
			AvgVOACAPAligned:               float64(voacapAligned) / float64(count),
			AvgVOACAPSparseUpgrade:         float64(voacapSparseUpgrade) / float64(count),
			AvgVOACAPOpen:                  float64(voacapOpen) / float64(count),
			AvgBeaconRX:                    float64(beaconRX) / float64(count),
			AvgBeaconRXInsufficient:        float64(beaconRXInsufficient) / float64(count),
			AvgBeaconRXNoSample:            float64(beaconRXNoSample) / float64(count),
			AvgBeaconRXLowCount:            float64(beaconRXLowCount) / float64(count),
			AvgBeaconRXLowReceiver:         float64(beaconRXLowReceiver) / float64(count),
			AvgBeaconRXLowWeight:           float64(beaconRXLowWeight) / float64(count),
			AvgBeaconRXStale:               float64(beaconRXStale) / float64(count),
			AvgBeaconRXVOACAPClosed:        float64(beaconRXVOACAPClosed) / float64(count),
			AvgBeaconRXVOACAPAligned:       float64(beaconRXVOACAPAligned) / float64(count),
			AvgBeaconRXVOACAPSparseUpgrade: float64(beaconRXVOACAPSparseUpgrade) / float64(count),
			AvgBeaconRXVOACAPOpen:          float64(beaconRXVOACAPOpen) / float64(count),
			AvgInsufficient:                float64(insufficient) / float64(count),
			AvgNoSample:                    float64(noSample) / float64(count),
			AvgLowCount:                    float64(lowCount) / float64(count),
			AvgLowReceiver:                 float64(lowReceiver) / float64(count),
			AvgLowWeight:                   float64(lowWeight) / float64(count),
			AvgStale:                       float64(stale) / float64(count),
			AvgCapLimited:                  float64(capLimited) / float64(count),
			AvgCapWouldBlock:               float64(capWouldBlock) / float64(count),
		})
	}

	sparseP50VOACAPSummary := buildSparseP50VOACAPSummary(sparseP50VOACAPByTS)

	capShadowHours := make(map[int][]capShadowTotals)
	for ts, totals := range capShadowByTS {
		tsTime, err := time.Parse("2006/01/02 15:04:05", ts)
		if err != nil {
			continue
		}
		hour := tsTime.Hour()
		capShadowHours[hour] = append(capShadowHours[hour], totals)
	}

	capShadowSummary := make([]capShadowHour, 0, len(capShadowHours))
	var capShadowHourKeys []int
	for h := range capShadowHours {
		capShadowHourKeys = append(capShadowHourKeys, h)
	}
	sort.Ints(capShadowHourKeys)
	for _, h := range capShadowHourKeys {
		rows := capShadowHours[h]
		if len(rows) == 0 {
			continue
		}
		byCap := make(map[uint32]*capShadowCandidateTotals)
		for _, row := range rows {
			for _, candidate := range row.Candidates {
				accum := byCap[candidate.MaxEffectiveCount]
				if accum == nil {
					accum = &capShadowCandidateTotals{MaxEffectiveCount: candidate.MaxEffectiveCount}
					byCap[candidate.MaxEffectiveCount] = accum
				}
				accum.Pass += candidate.Pass
				accum.LowCount += candidate.LowCount
				accum.LowReceiver += candidate.LowReceiver
				accum.LowWeight += candidate.LowWeight
				accum.Block += candidate.Block
			}
		}
		var caps []uint32
		for capValue := range byCap {
			caps = append(caps, capValue)
		}
		sort.Slice(caps, func(i, j int) bool { return caps[i] < caps[j] })
		hourSummary := capShadowHour{
			Hour:    fmt.Sprintf("%02d:00", h),
			Samples: len(rows),
		}
		for _, capValue := range caps {
			candidate := byCap[capValue]
			hourSummary.Candidates = append(hourSummary.Candidates, capShadowCandidateHour{
				MaxEffectiveCount: capValue,
				AvgPass:           float64(candidate.Pass) / float64(len(rows)),
				AvgLowCount:       float64(candidate.LowCount) / float64(len(rows)),
				AvgLowReceiver:    float64(candidate.LowReceiver) / float64(len(rows)),
				AvgLowWeight:      float64(candidate.LowWeight) / float64(len(rows)),
				AvgBlock:          float64(candidate.Block) / float64(len(rows)),
			})
		}
		capShadowSummary = append(capShadowSummary, hourSummary)
	}

	capP50ShadowHours := make(map[int][]capP50ShadowTotals)
	for ts, totals := range capP50ShadowByTS {
		tsTime, err := time.Parse("2006/01/02 15:04:05", ts)
		if err != nil {
			continue
		}
		hour := tsTime.Hour()
		capP50ShadowHours[hour] = append(capP50ShadowHours[hour], totals)
	}

	capP50ShadowSummary := make([]capP50ShadowHour, 0, len(capP50ShadowHours))
	var capP50ShadowHourKeys []int
	for h := range capP50ShadowHours {
		capP50ShadowHourKeys = append(capP50ShadowHourKeys, h)
	}
	sort.Ints(capP50ShadowHourKeys)
	for _, h := range capP50ShadowHourKeys {
		rows := capP50ShadowHours[h]
		if len(rows) == 0 {
			continue
		}
		byCap := make(map[uint32]*capP50ShadowCandidateTotals)
		for _, row := range rows {
			for _, candidate := range row.Candidates {
				accum := byCap[candidate.MaxEffectiveCount]
				if accum == nil {
					accum = &capP50ShadowCandidateTotals{MaxEffectiveCount: candidate.MaxEffectiveCount}
					byCap[candidate.MaxEffectiveCount] = accum
				}
				accum.PassUnlikely += candidate.PassUnlikely
				accum.PassLow += candidate.PassLow
				accum.PassMedium += candidate.PassMedium
				accum.PassHigh += candidate.PassHigh
				accum.Same += candidate.Same
				accum.Stronger += candidate.Stronger
				accum.Weaker += candidate.Weaker
				accum.ToInsufficient += candidate.ToInsufficient
			}
		}
		var caps []uint32
		for capValue := range byCap {
			caps = append(caps, capValue)
		}
		sort.Slice(caps, func(i, j int) bool { return caps[i] < caps[j] })
		hourSummary := capP50ShadowHour{
			Hour:    fmt.Sprintf("%02d:00", h),
			Samples: len(rows),
		}
		for _, capValue := range caps {
			candidate := byCap[capValue]
			hourSummary.Candidates = append(hourSummary.Candidates, capP50ShadowCandidateHour{
				MaxEffectiveCount: capValue,
				AvgPassUnlikely:   float64(candidate.PassUnlikely) / float64(len(rows)),
				AvgPassLow:        float64(candidate.PassLow) / float64(len(rows)),
				AvgPassMedium:     float64(candidate.PassMedium) / float64(len(rows)),
				AvgPassHigh:       float64(candidate.PassHigh) / float64(len(rows)),
				AvgSame:           float64(candidate.Same) / float64(len(rows)),
				AvgStronger:       float64(candidate.Stronger) / float64(len(rows)),
				AvgWeaker:         float64(candidate.Weaker) / float64(len(rows)),
				AvgToInsufficient: float64(candidate.ToInsufficient) / float64(len(rows)),
			})
		}
		capP50ShadowSummary = append(capP50ShadowSummary, hourSummary)
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
		DateUTC:               date.Format("2006-01-02"),
		LogFile:               logPath,
		Timezone:              "UTC",
		ModelContext:          buildModelContext(pathCfg, bands),
		Bands:                 summaries,
		BandGroups:            filteredGroups,
		CoverageMedians:       coverageMedians,
		PredictionsByHour:     predSummary,
		SparseP50VOACAPByHour: sparseP50VOACAPSummary,
		CapShadowByHour:       capShadowSummary,
		CapP50ShadowByHour:    capP50ShadowSummary,
		SourceMixByHour:       sourceMixSummary,
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
	if len(summary.SparseP50VOACAPByHour) > 0 {
		b.WriteString("Sparse p50/no-p50 VOACAP by hour\n\n")
		b.WriteString(sparseP50VOACAPActivitySummary(summary.SparseP50VOACAPByHour))
		b.WriteString("\n\n")
	}
	if len(summary.CapShadowByHour) > 0 {
		b.WriteString("Receiver cap shadow by hour\n\n")
		b.WriteString(capShadowActivitySummary(summary.CapShadowByHour))
		b.WriteString("\n\n")
	}
	if len(summary.CapP50ShadowByHour) > 0 {
		b.WriteString("Receiver cap p50 shadow by hour\n\n")
		b.WriteString(capP50ShadowActivitySummary(summary.CapP50ShadowByHour))
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
	fmt.Fprintf(b, "Default half-life: %ds. Stale after: %ds or %.2fx half-life per band.\n",
		ctx.DefaultHalfLifeSec, ctx.StaleAfterSeconds, ctx.StaleAfterHalfLifeMultiplier)
	fmt.Fprintf(b, "Min effective weight: %.2f. Min observations: %d. Min fine weight: %.2f. Reverse hint discount: %.2f.\n",
		ctx.MinEffectiveWeight, ctx.MinObservationCount, ctx.MinFineWeight, ctx.ReverseHintDiscount)
	fmt.Fprintf(b, "Beacon RX-only min observations: %d.\n", ctx.BeaconMinObservationCount)
	fmt.Fprintf(b, "Receiver contribution caps: mode=%s fine_slots=%d coarse_slots=%d max_count=%d max_weight=%.2f.\n",
		ctx.ReceiverContributionMode, ctx.ReceiverFineSlots, ctx.ReceiverCoarseSlots, ctx.ReceiverMaxEffectiveCount, ctx.ReceiverMaxEffectiveWeight)
	fmt.Fprintf(b, "Merge weights: receive %.2f / transmit %.2f.\n", ctx.MergeReceiveWeight, ctx.MergeTransmitWeight)
	if ctx.MaxPredictionAgeHalfLifeMultiplier > 0 {
		fmt.Fprintf(b, "Prediction freshness gate: %.2fx half-life; older selected evidence is treated as insufficient.\n", ctx.MaxPredictionAgeHalfLifeMultiplier)
	} else {
		b.WriteString("Prediction freshness gate: disabled.\n")
	}
	if len(ctx.NoiseOffsets) > 0 {
		b.WriteString("Noise offsets by class (dB): ")
		b.WriteString(formatNoiseOffsets(ctx.NoiseOffsets))
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

func formatNoiseOffsets(offsets map[string]float64) string {
	classes := make([]string, 0, len(offsets))
	for class := range offsets {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	parts := make([]string, 0, len(classes))
	for _, class := range classes {
		parts = append(parts, fmt.Sprintf("%s=%g", class, offsets[class]))
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
	var lowReceiverSample []string
	var lowWeightSample []string
	var staleSample []string
	var voacapClosedSample []string
	var voacapAlignedSample []string
	var voacapSparseUpgradeSample []string
	var voacapOpenSample []string
	var beaconRXSample []string
	var beaconRXInsufficientSample []string
	var capWouldBlockSample []string
	for i := range hours {
		h := &hours[i]
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
		if h.AvgLowReceiver > h.AvgLowCount && h.AvgLowReceiver > h.AvgLowWeight && h.AvgLowReceiver > 0 {
			lowReceiverSample = append(lowReceiverSample, h.Hour)
		}
		if h.AvgLowWeight > h.AvgLowCount && h.AvgLowWeight > 0 {
			lowWeightSample = append(lowWeightSample, h.Hour)
		}
		if h.AvgStale > 0 {
			staleSample = append(staleSample, h.Hour)
		}
		if h.AvgVOACAPClosed > 0 {
			voacapClosedSample = append(voacapClosedSample, h.Hour)
		}
		if h.AvgVOACAPAligned > 0 {
			voacapAlignedSample = append(voacapAlignedSample, h.Hour)
		}
		if h.AvgVOACAPSparseUpgrade > 0 {
			voacapSparseUpgradeSample = append(voacapSparseUpgradeSample, h.Hour)
		}
		if h.AvgVOACAPOpen > 0 {
			voacapOpenSample = append(voacapOpenSample, h.Hour)
		}
		if h.AvgBeaconRX > 0 ||
			h.AvgBeaconRXVOACAPClosed > 0 ||
			h.AvgBeaconRXVOACAPAligned > 0 ||
			h.AvgBeaconRXVOACAPSparseUpgrade > 0 ||
			h.AvgBeaconRXVOACAPOpen > 0 {
			beaconRXSample = append(beaconRXSample, h.Hour)
		}
		if h.AvgBeaconRXInsufficient > 0 {
			beaconRXInsufficientSample = append(beaconRXInsufficientSample, h.Hour)
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
	if len(lowReceiverSample) > 0 {
		s += fmt.Sprintf(" Hours mostly receiver-limited: %s.", strings.Join(lowReceiverSample, ", "))
	}
	if len(lowWeightSample) > 0 {
		s += fmt.Sprintf(" Hours mostly weight-limited: %s.", strings.Join(lowWeightSample, ", "))
	}
	if len(staleSample) > 0 {
		s += fmt.Sprintf(" Hours with stale selected evidence: %s.", strings.Join(staleSample, ", "))
	}
	if len(voacapClosedSample) > 0 {
		s += fmt.Sprintf(" Hours with VOACAP closed fallback predictions: %s.", strings.Join(voacapClosedSample, ", "))
	}
	if len(voacapAlignedSample) > 0 {
		s += fmt.Sprintf(" Hours with VOACAP-aligned sparse p50 predictions: %s.", strings.Join(voacapAlignedSample, ", "))
	}
	if len(voacapSparseUpgradeSample) > 0 {
		s += fmt.Sprintf(" Hours with REL-gated VOACAP sparse p50 upgrades: %s.", strings.Join(voacapSparseUpgradeSample, ", "))
	}
	if len(voacapOpenSample) > 0 {
		s += fmt.Sprintf(" Hours with REL-gated VOACAP no-p50 open predictions: %s.", strings.Join(voacapOpenSample, ", "))
	}
	if len(beaconRXSample) > 0 {
		s += fmt.Sprintf(" Hours with beacon RX-only path predictions: %s.", strings.Join(beaconRXSample, ", "))
	}
	if len(beaconRXInsufficientSample) > 0 {
		s += fmt.Sprintf(" Hours with insufficient beacon RX-only p50 evidence: %s.", strings.Join(beaconRXInsufficientSample, ", "))
	}
	if len(capWouldBlockSample) > 0 {
		s += fmt.Sprintf(" Hours where receiver caps would block shadow predictions: %s.", strings.Join(capWouldBlockSample, ", "))
	}
	return s
}

func sparseP50VOACAPActivitySummary(hours []sparseP50VOACAPHour) string {
	if len(hours) == 0 {
		return "No sparse p50 VOACAP diagnostic lines were present."
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Hour < hours[j].Hour })
	maxTotal := 0.0
	var maxHour string
	var noP50Sample []string
	var lowCountSample []string
	var cacheMissSample []string
	var cacheWaitSample []string
	var invalidSample []string
	var invalidReasonSample []string
	var notRunningSample []string
	var closedSample []string
	var openSample []string
	var relFailSample []string
	var notClosedSample []string
	var beaconSample []string
	for i := range hours {
		h := &hours[i]
		if h.AvgTotal > maxTotal {
			maxTotal = h.AvgTotal
			maxHour = h.Hour
		}
		if h.AvgNoP50 > 0 {
			noP50Sample = append(noP50Sample, h.Hour)
		}
		if h.AvgVeryLowCount > 0 {
			lowCountSample = append(lowCountSample, h.Hour)
		}
		if h.AvgCacheMissTotal > 0 {
			cacheMissSample = append(cacheMissSample, h.Hour)
		}
		if h.AvgQueued > 0 || h.AvgDelayed > 0 || h.AvgInflight > 0 || h.AvgNoCurrentHour > 0 {
			cacheWaitSample = append(cacheWaitSample, h.Hour)
		}
		if h.AvgInvalidRequest > 0 || h.AvgSSNUnavailable > 0 {
			invalidSample = append(invalidSample, h.Hour)
		}
		if h.AvgInvalidUnsupportedBand > 0 ||
			h.AvgInvalidEmptyUnknownBand > 0 ||
			h.AvgInvalidUserGrid > 0 ||
			h.AvgInvalidDXGrid > 0 ||
			h.AvgInvalidUserCell > 0 ||
			h.AvgInvalidDXCell > 0 {
			invalidReasonSample = append(invalidReasonSample, h.Hour)
		}
		if h.AvgQueueFull > 0 || h.AvgNotRunning > 0 || h.AvgDisabled > 0 || h.AvgUnavailable > 0 {
			notRunningSample = append(notRunningSample, h.Hour)
		}
		if h.AvgClosed > 0 {
			closedSample = append(closedSample, h.Hour)
		}
		if h.AvgAligned > 0 || h.AvgSparseUpgrade > 0 || h.AvgOpenRELPass > 0 {
			openSample = append(openSample, h.Hour)
		}
		if h.AvgOpenRELFail > 0 || h.AvgRELMissing > 0 || h.AvgRELBelowFloor > 0 || h.AvgRELMultiTier > 0 {
			relFailSample = append(relFailSample, h.Hour)
		}
		if h.AvgNotClosed > 0 {
			notClosedSample = append(notClosedSample, h.Hour)
		}
		if h.AvgBeaconRX > 0 {
			beaconSample = append(beaconSample, h.Hour)
		}
	}
	if maxHour == "" {
		return "Sparse p50 VOACAP diagnostics were present, but no activity parsed."
	}
	s := fmt.Sprintf("Peak sparse/no-p50 diagnostic volume occurs around %s (avg_total %.1f).", maxHour, maxTotal)
	if len(noP50Sample) > 0 {
		s += fmt.Sprintf(" Hours with no usable p50 candidates: %s.", strings.Join(noP50Sample, ", "))
	}
	if len(lowCountSample) > 0 {
		s += fmt.Sprintf(" Hours with very-low-count p50 candidates: %s.", strings.Join(lowCountSample, ", "))
	}
	if len(cacheMissSample) > 0 {
		s += fmt.Sprintf(" Hours where sparse candidates lacked a usable current-hour VOACAP cache hit: %s.", strings.Join(cacheMissSample, ", "))
	}
	if len(cacheWaitSample) > 0 {
		s += fmt.Sprintf(" Hours with queued, delayed, inflight, or stale-hour VOACAP work: %s.", strings.Join(cacheWaitSample, ", "))
	}
	if len(invalidSample) > 0 {
		s += fmt.Sprintf(" Hours blocked by invalid requests or missing SSN: %s.", strings.Join(invalidSample, ", "))
	}
	if len(invalidReasonSample) > 0 {
		s += fmt.Sprintf(" Hours with split invalid request reasons: %s.", strings.Join(invalidReasonSample, ", "))
	}
	if len(notRunningSample) > 0 {
		s += fmt.Sprintf(" Hours blocked by worker, queue, disabled, or unavailable VOACAP states: %s.", strings.Join(notRunningSample, ", "))
	}
	if len(closedSample) > 0 {
		s += fmt.Sprintf(" Hours where VOACAP labeled sparse candidates closed: %s.", strings.Join(closedSample, ", "))
	}
	if len(openSample) > 0 {
		s += fmt.Sprintf(" Hours where VOACAP supported open sparse candidates: %s.", strings.Join(openSample, ", "))
	}
	if len(relFailSample) > 0 {
		s += fmt.Sprintf(" Hours where VOACAP open support failed REL or tier guards: %s.", strings.Join(relFailSample, ", "))
	}
	if len(notClosedSample) > 0 {
		s += fmt.Sprintf(" Hours with usable VOACAP that did not classify candidates closed: %s.", strings.Join(notClosedSample, ", "))
	}
	if len(beaconSample) > 0 {
		s += fmt.Sprintf(" Hours including beacon RX-only sparse diagnostics: %s.", strings.Join(beaconSample, ", "))
	}
	return s
}

func capShadowActivitySummary(hours []capShadowHour) string {
	if len(hours) == 0 {
		return "No receiver cap shadow lines were present."
	}
	type capRollup struct {
		samples int
		pass    float64
		lowCnt  float64
		lowRx   float64
		lowWgt  float64
		block   float64
	}
	rollups := make(map[uint32]*capRollup)
	for _, hour := range hours {
		for _, candidate := range hour.Candidates {
			rollup := rollups[candidate.MaxEffectiveCount]
			if rollup == nil {
				rollup = &capRollup{}
				rollups[candidate.MaxEffectiveCount] = rollup
			}
			rollup.samples += hour.Samples
			rollup.pass += candidate.AvgPass * float64(hour.Samples)
			rollup.lowCnt += candidate.AvgLowCount * float64(hour.Samples)
			rollup.lowRx += candidate.AvgLowReceiver * float64(hour.Samples)
			rollup.lowWgt += candidate.AvgLowWeight * float64(hour.Samples)
			rollup.block += candidate.AvgBlock * float64(hour.Samples)
		}
	}
	var caps []uint32
	for capValue := range rollups {
		caps = append(caps, capValue)
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i] < caps[j] })
	parts := make([]string, 0, len(caps))
	for _, capValue := range caps {
		rollup := rollups[capValue]
		if rollup == nil || rollup.samples == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("cap%d avg pass %.1f, low_count %.1f, low_receiver %.1f, low_weight %.1f, would_block %.1f",
			capValue,
			rollup.pass/float64(rollup.samples),
			rollup.lowCnt/float64(rollup.samples),
			rollup.lowRx/float64(rollup.samples),
			rollup.lowWgt/float64(rollup.samples),
			rollup.block/float64(rollup.samples),
		))
	}
	if len(parts) == 0 {
		return "Receiver cap shadow lines were present, but no candidate fields parsed."
	}
	return strings.Join(parts, ". ") + "."
}

func capP50ShadowActivitySummary(hours []capP50ShadowHour) string {
	if len(hours) == 0 {
		return "No receiver cap p50 shadow lines were present."
	}
	type capRollup struct {
		samples        int
		passUnlikely   float64
		passLow        float64
		passMedium     float64
		passHigh       float64
		same           float64
		stronger       float64
		weaker         float64
		toInsufficient float64
	}
	rollups := make(map[uint32]*capRollup)
	for _, hour := range hours {
		for _, candidate := range hour.Candidates {
			rollup := rollups[candidate.MaxEffectiveCount]
			if rollup == nil {
				rollup = &capRollup{}
				rollups[candidate.MaxEffectiveCount] = rollup
			}
			rollup.samples += hour.Samples
			rollup.passUnlikely += candidate.AvgPassUnlikely * float64(hour.Samples)
			rollup.passLow += candidate.AvgPassLow * float64(hour.Samples)
			rollup.passMedium += candidate.AvgPassMedium * float64(hour.Samples)
			rollup.passHigh += candidate.AvgPassHigh * float64(hour.Samples)
			rollup.same += candidate.AvgSame * float64(hour.Samples)
			rollup.stronger += candidate.AvgStronger * float64(hour.Samples)
			rollup.weaker += candidate.AvgWeaker * float64(hour.Samples)
			rollup.toInsufficient += candidate.AvgToInsufficient * float64(hour.Samples)
		}
	}
	var caps []uint32
	for capValue := range rollups {
		caps = append(caps, capValue)
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i] < caps[j] })
	parts := make([]string, 0, len(caps))
	for _, capValue := range caps {
		rollup := rollups[capValue]
		if rollup == nil || rollup.samples == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("cap%d p50 avg pass unlikely %.1f, low %.1f, medium %.1f, high %.1f; same %.1f, stronger %.1f, weaker %.1f, to_insufficient %.1f",
			capValue,
			rollup.passUnlikely/float64(rollup.samples),
			rollup.passLow/float64(rollup.samples),
			rollup.passMedium/float64(rollup.samples),
			rollup.passHigh/float64(rollup.samples),
			rollup.same/float64(rollup.samples),
			rollup.stronger/float64(rollup.samples),
			rollup.weaker/float64(rollup.samples),
			rollup.toInsufficient/float64(rollup.samples),
		))
	}
	if len(parts) == 0 {
		return "Receiver cap p50 shadow lines were present, but no candidate fields parsed."
	}
	return strings.Join(parts, ". ") + "."
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
