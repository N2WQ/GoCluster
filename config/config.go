// Package config loads the cluster's YAML configuration, normalizes defaults,
// and exposes a strongly typed struct other packages rely on at startup.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete cluster configuration. The struct maps
// directly to `config.yaml` and is enriched with defaults during Load so
// downstream packages can assume sane, non-zero values.
type Config struct {
	Server          ServerConfig         `yaml:"server"`
	Telnet          TelnetConfig         `yaml:"telnet"`
	RBN             RBNConfig            `yaml:"rbn"`
	RBNDigital      RBNConfig            `yaml:"rbn_digital"`
	PSKReporter     PSKReporterConfig    `yaml:"pskreporter"`
	Dedup           DedupConfig          `yaml:"dedup"`
	Filter          FilterConfig         `yaml:"filter"`
	Stats           StatsConfig          `yaml:"stats"`
	CallCorrection  CallCorrectionConfig `yaml:"call_correction"`
	CallCache       CallCacheConfig      `yaml:"call_cache"`
	Harmonics       HarmonicConfig       `yaml:"harmonics"`
	SpotPolicy      SpotPolicy           `yaml:"spot_policy"`
	CTY             CTYConfig            `yaml:"cty"`
	Buffer          BufferConfig         `yaml:"buffer"`
	Skew            SkewConfig           `yaml:"skew"`
	FCCULS          FCCULSConfig         `yaml:"fcc_uls"`
	KnownCalls      KnownCallsConfig     `yaml:"known_calls"`
	GridDBPath      string               `yaml:"grid_db"`
	GridFlushSec    int                  `yaml:"grid_flush_seconds"`
	GridCacheSize   int                  `yaml:"grid_cache_size"`
	GridCacheTTLSec int                  `yaml:"grid_cache_ttl_seconds"`
	GridTTLDays     int                  `yaml:"grid_ttl_days"`
}

// ServerConfig contains general server settings
type ServerConfig struct {
	Name   string `yaml:"name"`
	NodeID string `yaml:"node_id"`
}

// TelnetConfig contains telnet server settings
type TelnetConfig struct {
	Port              int    `yaml:"port"`
	TLSEnabled        bool   `yaml:"tls_enabled"`
	MaxConnections    int    `yaml:"max_connections"`
	WelcomeMessage    string `yaml:"welcome_message"`
	DuplicateLoginMsg string `yaml:"duplicate_login_message"`
	LoginGreeting     string `yaml:"login_greeting"` // Supports <CALL> and <CLUSTER> substitution
	BroadcastWorkers  int    `yaml:"broadcast_workers"`
	BroadcastQueue    int    `yaml:"broadcast_queue_size"`
	WorkerQueue       int    `yaml:"worker_queue_size"`
	ClientBuffer      int    `yaml:"client_buffer_size"`
	SkipHandshake     bool   `yaml:"skip_handshake"`
}

// RBNConfig contains Reverse Beacon Network settings
type RBNConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Callsign       string `yaml:"callsign"`
	Name           string `yaml:"name"`
	KeepSSIDSuffix bool   `yaml:"keep_ssid_suffix"` // when true, retain -# SSIDs for dedup/call-correction
	SlotBuffer     int    `yaml:"slot_buffer"`      // size of ingest slot buffer between telnet reader and pipeline
}

// PSKReporterConfig contains PSKReporter MQTT settings
type PSKReporterConfig struct {
	Enabled bool   `yaml:"enabled"`
	Broker  string `yaml:"broker"`
	Port    int    `yaml:"port"`
	Topic   string `yaml:"topic"`
	Name    string `yaml:"name"`
	Workers int    `yaml:"workers"`
	// SpotChannelSize controls the buffered ingest channel between MQTT client and processing.
	SpotChannelSize int      `yaml:"spot_channel_size"`
	Modes           []string `yaml:"modes"`
	// AppendSpotterSSID, when true, appends "-#" to receiver callsigns that
	// lack an SSID so deduplication treats each PSK skimmer uniquely.
	AppendSpotterSSID bool `yaml:"append_spotter_ssid"`
}

const defaultPSKReporterTopic = "pskr/filter/v2/+/+/#"

// SubscriptionTopics returns the MQTT topics to subscribe to based on the configured modes.
// If no modes are specified, it falls back to `Topic` or the default `pskr/filter/v2/+/+/#`.
func (c *PSKReporterConfig) SubscriptionTopics() []string {
	topics := make([]string, 0, len(c.Modes))
	for _, mode := range c.Modes {
		mode = strings.TrimSpace(strings.ToUpper(mode))
		if mode == "" {
			continue
		}
		topics = append(topics, fmt.Sprintf("pskr/filter/v2/+/%s/#", mode))
	}
	if len(topics) == 0 {
		if c.Topic != "" {
			return []string{c.Topic}
		}
		return []string{defaultPSKReporterTopic}
	}
	return topics
}

// DedupConfig contains deduplication settings. The cluster-wide window controls how
// aggressively we suppress duplicates:
//   - A positive window enables deduplication for that many seconds.
//   - A zero or negative window effectively disables dedup (spots pass through immediately).
//
// The SNR policy governs how we handle duplicates from the same DX/spotter/frequency
// bucket—when enabled we keep the strongest SNR representative.
type DedupConfig struct {
	ClusterWindowSeconds   int  `yaml:"cluster_window_seconds"`        // <=0 disables primary dedup
	SecondaryWindowSeconds int  `yaml:"secondary_window_seconds"`      // <=0 uses default (60s)
	PreferStrongerSNR      bool `yaml:"prefer_stronger_snr"`           // keep max SNR when dropping duplicates
	SecondaryPreferStrong  bool `yaml:"secondary_prefer_stronger_snr"` // keep max SNR in secondary dedupe buckets
	OutputBufferSize       int  `yaml:"output_buffer_size"`            // channel capacity for dedup output
}

// FilterConfig holds default filter behavior for new users.
type FilterConfig struct {
	DefaultModes []string `yaml:"default_modes"`
}

// StatsConfig controls periodic runtime reporting.
type StatsConfig struct {
	DisplayIntervalSeconds int `yaml:"display_interval_seconds"`
}

// CallCacheConfig controls the normalization cache used for callsigns and spotters.
type CallCacheConfig struct {
	Size       int `yaml:"size"`        // max entries retained
	TTLSeconds int `yaml:"ttl_seconds"` // >0 expires cached entries after this many seconds
}

// CallCorrectionConfig controls consensus-based DX call corrections.
type CallCorrectionConfig struct {
	Enabled bool `yaml:"enabled"`
	// MinConsensusReports defines how many other unique spotters
	// must agree on an alternate callsign before we consider correcting it.
	MinConsensusReports int `yaml:"min_consensus_reports"`
	// MinAdvantage defines how many more corroborators the alternate call
	// must have compared to the original before a correction can happen.
	MinAdvantage int `yaml:"min_advantage"`
	// MinConfidencePercent defines the minimum percentage (0-100) of total
	// unique spotters on that frequency that must agree with the alternate call.
	MinConfidencePercent int `yaml:"min_confidence_percent"`
	// RecencySeconds defines how old the supporting spots can be for all modes when no override is set.
	RecencySeconds int `yaml:"recency_seconds"`
	// RecencySecondsCW/RecencySecondsRTTY override the base recency for those modes when >0.
	RecencySecondsCW   int `yaml:"recency_seconds_cw"`
	RecencySecondsRTTY int `yaml:"recency_seconds_rtty"`
	// MaxEditDistance bounds how different the alternate call can be from the
	// original (Levenshtein distance). Prevents wildly different corrections.
	MaxEditDistance int `yaml:"max_edit_distance"`
	// FrequencyToleranceHz defines how close two frequencies must be to be considered
	// the same signal when running consensus.
	FrequencyToleranceHz float64 `yaml:"frequency_tolerance_hz"`
	// DebugLog, when true, records call-correction decisions to an asynchronous SQLite log.
	DebugLog bool `yaml:"debug_log"`
	// DebugLogFile optionally overrides the decision log location/prefix; when blank a daily
	// callcorr_YYYY-MM-DD.db file is written under data/analysis.
	DebugLogFile string `yaml:"debug_log_file"`
	// Quality-based anchors: optional per-frequency-bin confidence store.
	QualityBinHz            int `yaml:"quality_bin_hz"`
	QualityGoodThreshold    int `yaml:"quality_good_threshold"`
	QualityNewCallIncrement int `yaml:"quality_newcall_increment"`
	QualityBustedDecrement  int `yaml:"quality_busted_decrement"`
	// Strategy selects how consensus is computed:
	//   - "majority": pick the most-reported call on-frequency (unique spotters); distance is only a safety cap.
	//     Other values are accepted for compatibility but currently coerced to majority.
	Strategy string `yaml:"strategy"`
	// MinSNRCW/RTTY allow discarding marginal decodes from the corroborator set.
	MinSNRCW   int `yaml:"min_snr_cw"`
	MinSNRRTTY int `yaml:"min_snr_rtty"`
	// DistanceModel controls how string distance is measured. Supported values:
	//   - Deprecated: "distance_model" applies to both CW/RTTY when per-mode toggles unset
	//   - "distance_model_cw"/"distance_model_rtty" override per mode:
	//       * "plain" (default) uses rune-based Levenshtein
	//       * "morse" (CW only) switches to Morse-aware distance
	//       * "baudot" (RTTY only) switches to Baudot/ITA2-aware distance
	DistanceModel     string `yaml:"distance_model"`
	DistanceModelCW   string `yaml:"distance_model_cw"`
	DistanceModelRTTY string `yaml:"distance_model_rtty"`
	// InvalidAction controls what to do when consensus suggests a callsign that
	// fails CTY validation. Supported values:
	//   - "broadcast": keep the original spot (default)
	//   - "suppress": drop the spot entirely
	InvalidAction string `yaml:"invalid_action"`
	// Distance3Extra* tighten consensus requirements for distance-3 edits (compared
	// to the subject call). These are additive to the base thresholds above. Set to
	// zero to disable extra requirements for distance-3 corrections.
	Distance3ExtraReports    int `yaml:"distance3_extra_reports"`    // additional unique reporters required
	Distance3ExtraAdvantage  int `yaml:"distance3_extra_advantage"`  // additional advantage over subject required
	Distance3ExtraConfidence int `yaml:"distance3_extra_confidence"` // additional confidence percentage points required
	// DistanceCache* control memoization of string distance calculations across the
	// consensus window. A small cache reduces CPU churn when the same candidates
	// are compared repeatedly during bursts.
	DistanceCacheSize       int `yaml:"distance_cache_size"`
	DistanceCacheTTLSeconds int `yaml:"distance_cache_ttl_seconds"`
	// Frequency guard: skip corrections when a strong runner-up exists at a different frequency.
	FreqGuardMinSeparationKHz float64 `yaml:"freq_guard_min_separation_khz"`
	// Ratio (0-1): runner-up supporters must be at least this fraction of winner supporters to trigger the guard.
	FreqGuardRunnerUpRatio float64 `yaml:"freq_guard_runnerup_ratio"`
	// MorseWeights tunes the dot/dash edit weights for CW distance calculations.
	MorseWeights MorseWeightConfig `yaml:"morse_weights"`
	// BaudotWeights tunes the ITA2 edit weights for RTTY distance calculations.
	BaudotWeights BaudotWeightConfig `yaml:"baudot_weights"`
	// AdaptiveRefresh tunes the activity-based refresh cadence for trust sets.
	AdaptiveRefresh AdaptiveRefreshConfig `yaml:"adaptive_refresh"`
	// AdaptiveMinReports tunes min_reports dynamically by band group based on recent activity.
	AdaptiveMinReports AdaptiveMinReportsConfig `yaml:"adaptive_min_reports"`
	// AdaptiveRefreshByBand drives trust/quality refresh cadence from band activity.
	AdaptiveRefreshByBand AdaptiveRefreshByBandConfig `yaml:"adaptive_refresh_by_band"`
	// Optional prior quality map loaded at startup to seed anchors before runtime learning.
	QualityPriorsFile string `yaml:"quality_priors_file"`
	// Optional spotter reliability weights (0-1). Reporters below MinSpotterReliability are ignored.
	SpotterReliabilityFile string  `yaml:"spotter_reliability_file"`
	MinSpotterReliability  float64 `yaml:"min_spotter_reliability"`
}

// MorseWeightConfig tunes the Morse-aware edit costs used for CW distance.
// Insert/delete and substitution costs apply to dot/dash edits at the pattern
// level; Scale multiplies the normalized score before rounding to an int.
type MorseWeightConfig struct {
	Insert int `yaml:"insert"`
	Delete int `yaml:"delete"`
	Sub    int `yaml:"sub"`
	Scale  int `yaml:"scale"`
}

// BaudotWeightConfig tunes the ITA2-aware edit costs used for RTTY distance.
// Insert/delete and substitution costs apply to letter/figure bits; Scale
// multiplies the normalized score before rounding to an int.
type BaudotWeightConfig struct {
	Insert int `yaml:"insert"`
	Delete int `yaml:"delete"`
	Sub    int `yaml:"sub"`
	Scale  int `yaml:"scale"`
}

// AdaptiveRefreshConfig controls how often trust/quality sets should be rebuilt based on activity.
type AdaptiveRefreshConfig struct {
	BusyThresholdPerMin      int `yaml:"busy_threshold_per_min"`
	QuietThresholdPerMin     int `yaml:"quiet_threshold_per_min"`
	QuietConsecutiveWindows  int `yaml:"quiet_consecutive_windows"`
	BusyIntervalMinutes      int `yaml:"busy_interval_minutes"`
	QuietIntervalMinutes     int `yaml:"quiet_interval_minutes"`
	MinSpotsSinceLastRefresh int `yaml:"min_spots_since_last_refresh"`
	WindowMinutesForRate     int `yaml:"window_minutes_for_rate"`
	EvaluationPeriodSeconds  int `yaml:"evaluation_period_seconds"`
}

// AdaptiveMinReportsConfig controls per-band-group min_reports thresholds driven by spot activity.
type AdaptiveMinReportsConfig struct {
	Enabled                 bool                      `yaml:"enabled"`
	WindowMinutes           int                       `yaml:"window_minutes"`
	EvaluationPeriodSeconds int                       `yaml:"evaluation_period_seconds"`
	HysteresisWindows       int                       `yaml:"hysteresis_windows"`
	Groups                  []AdaptiveMinReportsGroup `yaml:"groups"`
}

// AdaptiveRefreshByBandConfig defines refresh intervals keyed off adaptive band states.
type AdaptiveRefreshByBandConfig struct {
	Enabled                  bool `yaml:"enabled"`
	QuietRefreshMinutes      int  `yaml:"quiet_refresh_minutes"`
	NormalRefreshMinutes     int  `yaml:"normal_refresh_minutes"`
	BusyRefreshMinutes       int  `yaml:"busy_refresh_minutes"`
	MinSpotsSinceLastRefresh int  `yaml:"min_spots_since_last_refresh"`
}

// AdaptiveMinReportsGroup defines thresholds and min_reports values for a band group.
type AdaptiveMinReportsGroup struct {
	Name             string   `yaml:"name"`
	Bands            []string `yaml:"bands"`
	QuietBelow       int      `yaml:"quiet_below"`
	BusyAbove        int      `yaml:"busy_above"`
	QuietMinReports  int      `yaml:"quiet_min_reports"`
	NormalMinReports int      `yaml:"normal_min_reports"`
	BusyMinReports   int      `yaml:"busy_min_reports"`
}

// HarmonicConfig controls detection and suppression of harmonic spots.
type HarmonicConfig struct {
	Enabled              bool    `yaml:"enabled"`
	RecencySeconds       int     `yaml:"recency_seconds"` // look-back window for harmonic correlation
	MaxHarmonicMultiple  int     `yaml:"max_harmonic_multiple"`
	FrequencyToleranceHz float64 `yaml:"frequency_tolerance_hz"`
	MinReportDelta       int     `yaml:"min_report_delta"`
	MinReportDeltaStep   float64 `yaml:"min_report_delta_step"`
}

// SpotPolicy controls generic spot handling rules.
type SpotPolicy struct {
	MaxAgeSeconds int `yaml:"max_age_seconds"`
	// FrequencyAveragingSeconds controls the look-back window for CW/RTTY
	// frequency averaging.
	FrequencyAveragingSeconds int `yaml:"frequency_averaging_seconds"`
	// FrequencyAveragingToleranceHz is the maximum deviation allowed between
	// reports when averaging (in Hz).
	FrequencyAveragingToleranceHz float64 `yaml:"frequency_averaging_tolerance_hz"`
	// FrequencyAveragingMinReports is the minimum number of corroborating
	// reports required before applying an averaged frequency.
	FrequencyAveragingMinReports int `yaml:"frequency_averaging_min_reports"`
}

// BufferConfig controls the ring buffer that holds recent spots.
type BufferConfig struct {
	Capacity int `yaml:"capacity"`
}

// SkewConfig controls how the RBN skew table is fetched and applied.
type SkewConfig struct {
	Enabled    bool   `yaml:"enabled"`
	URL        string `yaml:"url"`
	File       string `yaml:"file"`
	MinSpots   int    `yaml:"min_spots"`
	RefreshUTC string `yaml:"refresh_utc"`
}

// KnownCallsConfig controls downloading of the known callsign list.
type KnownCallsConfig struct {
	Enabled    bool   `yaml:"enabled"`
	URL        string `yaml:"url"`
	File       string `yaml:"file"`
	RefreshUTC string `yaml:"refresh_utc"`
}

// FCCULSConfig controls downloading of the FCC ULS database archive.
type FCCULSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	URL        string `yaml:"url"`
	Archive    string `yaml:"archive_path"`
	DBPath     string `yaml:"db_path"`
	TempDir    string `yaml:"temp_dir"`
	RefreshUTC string `yaml:"refresh_utc"`
}

// CTYConfig allows overriding the CTY prefix database path.
type CTYConfig struct {
	File string `yaml:"file"`
}

// Load reads configuration from a YAML file, applies defaults, and validates
// key fields so the rest of the cluster can rely on a consistent baseline.
func Load(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// RBN ingest buffers should be sized to absorb decode bursts; fall back to
	// generous defaults when omitted.
	if cfg.RBN.SlotBuffer <= 0 {
		cfg.RBN.SlotBuffer = 4000
	}
	if cfg.RBNDigital.SlotBuffer <= 0 {
		cfg.RBNDigital.SlotBuffer = cfg.RBN.SlotBuffer
	}

	if cfg.PSKReporter.Workers < 0 {
		cfg.PSKReporter.Workers = 0
	}
	if cfg.PSKReporter.SpotChannelSize <= 0 {
		cfg.PSKReporter.SpotChannelSize = 25000
	}

	if cfg.Stats.DisplayIntervalSeconds <= 0 {
		cfg.Stats.DisplayIntervalSeconds = 30
	}
	// Call-correction defaults keep consensus strict unless the operator opts in
	// to looser thresholds.
	if cfg.CallCorrection.MinConsensusReports <= 0 {
		cfg.CallCorrection.MinConsensusReports = 4
	}
	if cfg.CallCorrection.MinAdvantage <= 0 {
		cfg.CallCorrection.MinAdvantage = 1
	}
	if cfg.CallCorrection.MinConfidencePercent <= 0 {
		cfg.CallCorrection.MinConfidencePercent = 70
	}
	if cfg.CallCorrection.MaxEditDistance <= 0 {
		cfg.CallCorrection.MaxEditDistance = 2
	}
	if cfg.CallCorrection.RecencySeconds <= 0 {
		cfg.CallCorrection.RecencySeconds = 45
	}
	if cfg.CallCorrection.RecencySecondsCW <= 0 {
		cfg.CallCorrection.RecencySecondsCW = cfg.CallCorrection.RecencySeconds
	}
	if cfg.CallCorrection.RecencySecondsRTTY <= 0 {
		cfg.CallCorrection.RecencySecondsRTTY = cfg.CallCorrection.RecencySeconds
	}
	if cfg.CallCorrection.FrequencyToleranceHz <= 0 {
		cfg.CallCorrection.FrequencyToleranceHz = 0.5
	}
	if cfg.CallCorrection.FreqGuardMinSeparationKHz <= 0 {
		cfg.CallCorrection.FreqGuardMinSeparationKHz = 0.1
	}
	if cfg.CallCorrection.FreqGuardRunnerUpRatio <= 0 {
		cfg.CallCorrection.FreqGuardRunnerUpRatio = 0.5
	}
	if cfg.CallCorrection.QualityBinHz <= 0 {
		cfg.CallCorrection.QualityBinHz = 1000
	}
	if cfg.CallCorrection.QualityGoodThreshold <= 0 {
		cfg.CallCorrection.QualityGoodThreshold = 2
	}
	if cfg.CallCorrection.QualityNewCallIncrement == 0 {
		cfg.CallCorrection.QualityNewCallIncrement = 1
	}
	if cfg.CallCorrection.QualityBustedDecrement == 0 {
		cfg.CallCorrection.QualityBustedDecrement = 1
	}
	if strings.TrimSpace(cfg.CallCorrection.Strategy) == "" {
		cfg.CallCorrection.Strategy = "center"
	} else {
		strategy := strings.ToLower(strings.TrimSpace(cfg.CallCorrection.Strategy))
		switch strategy {
		case "center", "classic", "majority":
			cfg.CallCorrection.Strategy = strategy
		default:
			cfg.CallCorrection.Strategy = "center"
		}
	}
	if cfg.CallCorrection.InvalidAction == "" {
		cfg.CallCorrection.InvalidAction = "broadcast"
	}
	// Normalize the distance model, inheriting the legacy default when mode-
	// specific overrides are absent.
	defaultDistance := strings.TrimSpace(cfg.CallCorrection.DistanceModel)
	if defaultDistance == "" {
		defaultDistance = "plain"
	}
	if strings.TrimSpace(cfg.CallCorrection.DistanceModelCW) == "" {
		cfg.CallCorrection.DistanceModelCW = defaultDistance
	}
	if strings.TrimSpace(cfg.CallCorrection.DistanceModelRTTY) == "" {
		cfg.CallCorrection.DistanceModelRTTY = defaultDistance
	}
	if strings.TrimSpace(cfg.CallCorrection.DistanceModelCW) == "" {
		cfg.CallCorrection.DistanceModelCW = "plain"
	}
	if strings.TrimSpace(cfg.CallCorrection.DistanceModelRTTY) == "" {
		cfg.CallCorrection.DistanceModelRTTY = "plain"
	}
	if cfg.CallCorrection.MinSNRCW < 0 {
		cfg.CallCorrection.MinSNRCW = 0
	}
	if cfg.CallCorrection.MinSNRRTTY < 0 {
		cfg.CallCorrection.MinSNRRTTY = 0
	}
	if cfg.CallCorrection.Distance3ExtraReports < 0 {
		cfg.CallCorrection.Distance3ExtraReports = 0
	}
	if cfg.CallCorrection.Distance3ExtraAdvantage < 0 {
		cfg.CallCorrection.Distance3ExtraAdvantage = 0
	}
	if cfg.CallCorrection.Distance3ExtraConfidence < 0 {
		cfg.CallCorrection.Distance3ExtraConfidence = 0
	}
	if cfg.CallCorrection.MinSpotterReliability < 0 {
		cfg.CallCorrection.MinSpotterReliability = 0
	}
	if cfg.CallCorrection.DistanceCacheSize <= 0 {
		cfg.CallCorrection.DistanceCacheSize = 5000
	}
	if cfg.CallCorrection.MorseWeights.Insert <= 0 {
		cfg.CallCorrection.MorseWeights.Insert = 1
	}
	if cfg.CallCorrection.MorseWeights.Delete <= 0 {
		cfg.CallCorrection.MorseWeights.Delete = 1
	}
	if cfg.CallCorrection.MorseWeights.Sub <= 0 {
		cfg.CallCorrection.MorseWeights.Sub = 2
	}
	if cfg.CallCorrection.MorseWeights.Scale <= 0 {
		cfg.CallCorrection.MorseWeights.Scale = 2
	}
	if cfg.CallCorrection.AdaptiveRefresh.WindowMinutesForRate <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.WindowMinutesForRate = 10
	}
	if cfg.CallCorrection.AdaptiveRefresh.EvaluationPeriodSeconds <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.EvaluationPeriodSeconds = 30
	}
	if cfg.CallCorrection.AdaptiveRefresh.BusyThresholdPerMin <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.BusyThresholdPerMin = 500
	}
	if cfg.CallCorrection.AdaptiveRefresh.QuietThresholdPerMin <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.QuietThresholdPerMin = 300
	}
	if cfg.CallCorrection.AdaptiveRefresh.QuietConsecutiveWindows <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.QuietConsecutiveWindows = 2
	}
	if cfg.CallCorrection.AdaptiveRefresh.BusyIntervalMinutes <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.BusyIntervalMinutes = 15
	}
	if cfg.CallCorrection.AdaptiveRefresh.QuietIntervalMinutes <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.QuietIntervalMinutes = 45
	}
	if cfg.CallCorrection.AdaptiveRefresh.MinSpotsSinceLastRefresh <= 0 {
		cfg.CallCorrection.AdaptiveRefresh.MinSpotsSinceLastRefresh = 1000
	}
	// Adaptive min_reports defaults (per band-group activity)
	if cfg.CallCorrection.AdaptiveMinReports.WindowMinutes <= 0 {
		cfg.CallCorrection.AdaptiveMinReports.WindowMinutes = 10
	}
	if cfg.CallCorrection.AdaptiveMinReports.EvaluationPeriodSeconds <= 0 {
		cfg.CallCorrection.AdaptiveMinReports.EvaluationPeriodSeconds = 60
	}
	if cfg.CallCorrection.AdaptiveMinReports.HysteresisWindows <= 0 {
		cfg.CallCorrection.AdaptiveMinReports.HysteresisWindows = 2
	}
	if !cfg.CallCorrection.AdaptiveMinReports.Enabled {
		cfg.CallCorrection.AdaptiveMinReports.Enabled = true
	}
	if len(cfg.CallCorrection.AdaptiveMinReports.Groups) == 0 {
		cfg.CallCorrection.AdaptiveMinReports.Groups = []AdaptiveMinReportsGroup{
			{Name: "lowbands", Bands: []string{"160m", "80m"}, QuietBelow: 30, BusyAbove: 75, QuietMinReports: 2, NormalMinReports: 3, BusyMinReports: 4},
			{Name: "midbands", Bands: []string{"40m", "20m"}, QuietBelow: 80, BusyAbove: 130, QuietMinReports: 2, NormalMinReports: 3, BusyMinReports: 4},
			{Name: "highbands", Bands: []string{"15m", "10m"}, QuietBelow: 35, BusyAbove: 85, QuietMinReports: 2, NormalMinReports: 3, BusyMinReports: 4},
			{Name: "others", Bands: []string{"30m", "17m", "12m", "60m", "6m"}, QuietBelow: 20, BusyAbove: 55, QuietMinReports: 2, NormalMinReports: 3, BusyMinReports: 4},
		}
	}
	if cfg.CallCorrection.AdaptiveRefreshByBand.QuietRefreshMinutes <= 0 {
		cfg.CallCorrection.AdaptiveRefreshByBand.QuietRefreshMinutes = 30
	}
	if cfg.CallCorrection.AdaptiveRefreshByBand.NormalRefreshMinutes <= 0 {
		cfg.CallCorrection.AdaptiveRefreshByBand.NormalRefreshMinutes = 20
	}
	if cfg.CallCorrection.AdaptiveRefreshByBand.BusyRefreshMinutes <= 0 {
		cfg.CallCorrection.AdaptiveRefreshByBand.BusyRefreshMinutes = 10
	}
	if cfg.CallCorrection.AdaptiveRefreshByBand.MinSpotsSinceLastRefresh <= 0 {
		cfg.CallCorrection.AdaptiveRefreshByBand.MinSpotsSinceLastRefresh = 500
	}
	if cfg.CallCorrection.BaudotWeights.Insert <= 0 {
		cfg.CallCorrection.BaudotWeights.Insert = 1
	}
	if cfg.CallCorrection.BaudotWeights.Delete <= 0 {
		cfg.CallCorrection.BaudotWeights.Delete = 1
	}
	if cfg.CallCorrection.BaudotWeights.Sub <= 0 {
		cfg.CallCorrection.BaudotWeights.Sub = 2
	}
	if cfg.CallCorrection.BaudotWeights.Scale <= 0 {
		cfg.CallCorrection.BaudotWeights.Scale = 2
	}
	if cfg.CallCorrection.DistanceCacheTTLSeconds <= 0 {
		cfg.CallCorrection.DistanceCacheTTLSeconds = cfg.CallCorrection.RecencySeconds
		if cfg.CallCorrection.DistanceCacheTTLSeconds <= 0 {
			cfg.CallCorrection.DistanceCacheTTLSeconds = 120
		}
	}
	if cfg.CallCache.Size <= 0 {
		cfg.CallCache.Size = 4096
	}
	if cfg.CallCache.TTLSeconds <= 0 {
		cfg.CallCache.TTLSeconds = 600
	}
	if cfg.Telnet.BroadcastQueue <= 0 {
		cfg.Telnet.BroadcastQueue = 2048
	}
	if cfg.Telnet.WorkerQueue <= 0 {
		cfg.Telnet.WorkerQueue = 128
	}
	if cfg.Telnet.ClientBuffer <= 0 {
		cfg.Telnet.ClientBuffer = 128
	}
	// Provide operator-facing telnet prompts even when omitted from YAML.
	if strings.TrimSpace(cfg.Telnet.DuplicateLoginMsg) == "" {
		cfg.Telnet.DuplicateLoginMsg = "Another login for your callsign connected. This session is being closed (multiple logins are not allowed)."
	}
	if strings.TrimSpace(cfg.Telnet.LoginGreeting) == "" {
		cfg.Telnet.LoginGreeting = "Hello <CALL>, you are now connected to <CLUSTER>."
	}

	// Harmonic guardrails ensure suppression logic runs with bounded windows and tolerances.
	if cfg.Harmonics.RecencySeconds <= 0 {
		cfg.Harmonics.RecencySeconds = 120
	}
	if cfg.Harmonics.MaxHarmonicMultiple < 2 {
		cfg.Harmonics.MaxHarmonicMultiple = 4
	}
	if cfg.Harmonics.FrequencyToleranceHz <= 0 {
		cfg.Harmonics.FrequencyToleranceHz = 20
	}
	if cfg.Harmonics.MinReportDelta <= 0 {
		cfg.Harmonics.MinReportDelta = 6
	}
	if cfg.Harmonics.MinReportDeltaStep < 0 {
		cfg.Harmonics.MinReportDeltaStep = 0
	}

	// Spot policy defaults avoid unbounded averaging or delivery of stale spots.
	if cfg.SpotPolicy.MaxAgeSeconds <= 0 {
		cfg.SpotPolicy.MaxAgeSeconds = 120
	}
	if cfg.SpotPolicy.FrequencyAveragingSeconds <= 0 {
		cfg.SpotPolicy.FrequencyAveragingSeconds = 45
	}
	if cfg.SpotPolicy.FrequencyAveragingToleranceHz <= 0 {
		cfg.SpotPolicy.FrequencyAveragingToleranceHz = 300
	}
	if cfg.SpotPolicy.FrequencyAveragingMinReports <= 0 {
		cfg.SpotPolicy.FrequencyAveragingMinReports = 4
	}

	if strings.TrimSpace(cfg.KnownCalls.File) == "" {
		cfg.KnownCalls.File = "data/scp/MASTER.SCP"
	}
	if cfg.KnownCalls.RefreshUTC == "" {
		cfg.KnownCalls.RefreshUTC = "01:00"
	}
	// ULS fetch defaults keep the downloader pointed at the official FCC archive
	// and provide safe on-disk locations when omitted.
	if strings.TrimSpace(cfg.FCCULS.URL) == "" {
		cfg.FCCULS.URL = "https://data.fcc.gov/download/pub/uls/complete/l_amat.zip"
	}
	if strings.TrimSpace(cfg.FCCULS.Archive) == "" {
		cfg.FCCULS.Archive = "data/fcc/l_amat.zip"
	}
	if strings.TrimSpace(cfg.FCCULS.DBPath) == "" {
		cfg.FCCULS.DBPath = "data/fcc/fcc_uls.db"
	}
	if strings.TrimSpace(cfg.FCCULS.TempDir) == "" {
		cfg.FCCULS.TempDir = filepath.Dir(cfg.FCCULS.DBPath)
	}
	if strings.TrimSpace(cfg.FCCULS.RefreshUTC) == "" {
		cfg.FCCULS.RefreshUTC = "02:15"
	}
	if _, err := time.Parse("15:04", cfg.FCCULS.RefreshUTC); err != nil {
		return nil, fmt.Errorf("invalid FCC ULS refresh time %q: %w", cfg.FCCULS.RefreshUTC, err)
	}
	// Grid store defaults keep the local cache warm and bound persistence churn.
	if strings.TrimSpace(cfg.GridDBPath) == "" {
		cfg.GridDBPath = "data/grids/calls.db"
	}
	if cfg.GridFlushSec <= 0 {
		cfg.GridFlushSec = 60
	}
	if cfg.GridCacheSize <= 0 {
		cfg.GridCacheSize = 100000
	}
	if cfg.GridCacheTTLSec < 0 {
		cfg.GridCacheTTLSec = 0
	}
	if cfg.GridTTLDays < 0 {
		cfg.GridTTLDays = 0
	}

	// Normalize dedup settings so the window drives behavior.
	if cfg.Dedup.ClusterWindowSeconds < 0 {
		cfg.Dedup.ClusterWindowSeconds = 0
	}
	if cfg.Dedup.SecondaryWindowSeconds < 0 {
		cfg.Dedup.SecondaryWindowSeconds = 0
	}
	if cfg.Dedup.SecondaryWindowSeconds == 0 {
		cfg.Dedup.SecondaryWindowSeconds = 60
	}
	// Default secondary SNR preference to match primary unless specified.
	if !cfg.Dedup.SecondaryPreferStrong {
		cfg.Dedup.SecondaryPreferStrong = cfg.Dedup.PreferStrongerSNR
	}
	if cfg.Dedup.OutputBufferSize <= 0 {
		cfg.Dedup.OutputBufferSize = 1000
	}
	if strings.TrimSpace(cfg.CTY.File) == "" {
		cfg.CTY.File = "data/cty/cty.plist"
	}
	if cfg.Buffer.Capacity <= 0 {
		cfg.Buffer.Capacity = 300000
	}
	// Skew fetch defaults keep the daily scheduler pointed at SM7IUN's published list.
	if strings.TrimSpace(cfg.Skew.URL) == "" {
		cfg.Skew.URL = "https://sm7iun.se/rbnskew.csv"
	}
	if strings.TrimSpace(cfg.Skew.File) == "" {
		cfg.Skew.File = "data/skm_correction/rbnskew.json"
	}
	if cfg.Skew.MinSpots < 0 {
		cfg.Skew.MinSpots = 0
	}
	if strings.TrimSpace(cfg.Skew.RefreshUTC) == "" {
		cfg.Skew.RefreshUTC = "00:30"
	}
	if _, err := time.Parse("15:04", cfg.Skew.RefreshUTC); err != nil {
		return nil, fmt.Errorf("invalid skew refresh time %q: %w", cfg.Skew.RefreshUTC, err)
	}
	return &cfg, nil
}

// Print displays a concise, human-readable summary of the loaded configuration,
// primarily for startup logs and diagnostics.
func (c *Config) Print() {
	fmt.Printf("Server: %s (%s)\n", c.Server.Name, c.Server.NodeID)
	workerDesc := "auto"
	if c.Telnet.BroadcastWorkers > 0 {
		workerDesc = fmt.Sprintf("%d", c.Telnet.BroadcastWorkers)
	}
	fmt.Printf("Telnet: port %d (broadcast workers=%s queue=%d worker_queue=%d client_buffer=%d skip_handshake=%t)\n",
		c.Telnet.Port,
		workerDesc,
		c.Telnet.BroadcastQueue,
		c.Telnet.WorkerQueue,
		c.Telnet.ClientBuffer,
		c.Telnet.SkipHandshake)
	if c.RBN.Enabled {
		fmt.Printf("RBN CW/RTTY: %s:%d (as %s, slot_buffer=%d)\n", c.RBN.Host, c.RBN.Port, c.RBN.Callsign, c.RBN.SlotBuffer)
	}
	if c.RBNDigital.Enabled {
		fmt.Printf("RBN Digital (FT4/FT8): %s:%d (as %s, slot_buffer=%d)\n", c.RBNDigital.Host, c.RBNDigital.Port, c.RBNDigital.Callsign, c.RBNDigital.SlotBuffer)
	}
	if c.PSKReporter.Enabled {
		workerDesc := "auto"
		if c.PSKReporter.Workers > 0 {
			workerDesc = fmt.Sprintf("%d", c.PSKReporter.Workers)
		}
		fmt.Printf("PSKReporter: %s:%d (topic: %s buffer=%d workers=%s)\n",
			c.PSKReporter.Broker,
			c.PSKReporter.Port,
			c.PSKReporter.Topic,
			c.PSKReporter.SpotChannelSize,
			workerDesc)
	}
	clusterWindow := "disabled"
	if c.Dedup.ClusterWindowSeconds > 0 {
		clusterWindow = fmt.Sprintf("%ds", c.Dedup.ClusterWindowSeconds)
	}
	secondaryWindow := "disabled"
	if c.Dedup.SecondaryWindowSeconds > 0 {
		secondaryWindow = fmt.Sprintf("%ds", c.Dedup.SecondaryWindowSeconds)
	}
	fmt.Printf("Dedup: cluster=%s (prefer_stronger=%t) secondary=%s (prefer_stronger=%t)\n", clusterWindow, c.Dedup.PreferStrongerSNR, secondaryWindow, c.Dedup.SecondaryPreferStrong)
	if len(c.Filter.DefaultModes) > 0 {
		fmt.Printf("Default modes: %s\n", strings.Join(c.Filter.DefaultModes, ", "))
	}
	fmt.Printf("Stats interval: %ds\n", c.Stats.DisplayIntervalSeconds)
	status := "disabled"
	if c.CallCorrection.Enabled {
		status = "enabled"
	}
	fmt.Printf("Call correction: %s (min_reports=%d advantage>%d confidence>=%d%% recency=%ds max_edit=%d tol=%.1fHz distance_cw=%s distance_rtty=%s invalid_action=%s d3_extra:+%d/+%d/+%d%%)\n",
		status,
		c.CallCorrection.MinConsensusReports,
		c.CallCorrection.MinAdvantage,
		c.CallCorrection.MinConfidencePercent,
		c.CallCorrection.RecencySeconds,
		c.CallCorrection.MaxEditDistance,
		c.CallCorrection.FrequencyToleranceHz,
		c.CallCorrection.DistanceModelCW,
		c.CallCorrection.DistanceModelRTTY,
		c.CallCorrection.InvalidAction,
		c.CallCorrection.Distance3ExtraReports,
		c.CallCorrection.Distance3ExtraAdvantage,
		c.CallCorrection.Distance3ExtraConfidence)
	fmt.Printf("Call correction cache: size=%d ttl=%ds\n",
		c.CallCorrection.DistanceCacheSize,
		c.CallCorrection.DistanceCacheTTLSeconds)

	harmonicStatus := "disabled"
	if c.Harmonics.Enabled {
		harmonicStatus = "enabled"
	}
	fmt.Printf("Harmonics: %s (recency=%ds max_multiple=%d tolerance=%.1fHz min_report_delta=%ddB)\n",
		harmonicStatus,
		c.Harmonics.RecencySeconds,
		c.Harmonics.MaxHarmonicMultiple,
		c.Harmonics.FrequencyToleranceHz,
		c.Harmonics.MinReportDelta)

	fmt.Printf("Spot policy: max_age=%ds\n", c.SpotPolicy.MaxAgeSeconds)
	if c.CTY.File != "" {
		fmt.Printf("CTY database: %s\n", c.CTY.File)
	}
	if c.KnownCalls.Enabled && c.KnownCalls.URL != "" {
		fmt.Printf("Known calls refresh: %s UTC (source=%s)\n", c.KnownCalls.RefreshUTC, c.KnownCalls.URL)
	}
	if c.FCCULS.Enabled && c.FCCULS.URL != "" {
		fmt.Printf("FCC ULS: refresh %s UTC (source=%s archive=%s db=%s)\n", c.FCCULS.RefreshUTC, c.FCCULS.URL, c.FCCULS.Archive, c.FCCULS.DBPath)
	}
	if strings.TrimSpace(c.GridDBPath) != "" {
		fmt.Printf("Grid/known DB: %s (flush=%ds cache=%d ttl=%dd)\n", c.GridDBPath, c.GridFlushSec, c.GridCacheSize, c.GridTTLDays)
	}
	if c.Skew.Enabled {
		fmt.Printf("Skew: refresh %s UTC (min_spots=%d source=%s)\n", c.Skew.RefreshUTC, c.Skew.MinSpots, c.Skew.URL)
	}
	fmt.Printf("Ring buffer capacity: %d spots\n", c.Buffer.Capacity)
}
