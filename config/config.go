package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the complete cluster configuration
type Config struct {
	Server         ServerConfig         `yaml:"server"`
	Telnet         TelnetConfig         `yaml:"telnet"`
	RBN            RBNConfig            `yaml:"rbn"`
	RBNDigital     RBNConfig            `yaml:"rbn_digital"`
	PSKReporter    PSKReporterConfig    `yaml:"pskreporter"`
	Dedup          DedupConfig          `yaml:"dedup"`
	Filter         FilterConfig         `yaml:"filter"`
	Admin          AdminConfig          `yaml:"admin"`
	Logging        LoggingConfig        `yaml:"logging"`
	Stats          StatsConfig          `yaml:"stats"`
	CallCorrection CallCorrectionConfig `yaml:"call_correction"`
	Harmonics      HarmonicConfig       `yaml:"harmonics"`
	SpotPolicy     SpotPolicy           `yaml:"spot_policy"`
	Confidence     ConfidenceConfig     `yaml:"confidence"`
	CTY            CTYConfig            `yaml:"cty"`
	Buffer         BufferConfig         `yaml:"buffer"`
}

// ServerConfig contains general server settings
type ServerConfig struct {
	Name   string `yaml:"name"`
	NodeID string `yaml:"node_id"`
}

// TelnetConfig contains telnet server settings
type TelnetConfig struct {
	Port             int    `yaml:"port"`
	TLSEnabled       bool   `yaml:"tls_enabled"`
	MaxConnections   int    `yaml:"max_connections"`
	WelcomeMessage   string `yaml:"welcome_message"`
	BroadcastWorkers int    `yaml:"broadcast_workers"`
}

// RBNConfig contains Reverse Beacon Network settings
type RBNConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Callsign string `yaml:"callsign"`
	Name     string `yaml:"name"`
}

// PSKReporterConfig contains PSKReporter MQTT settings
type PSKReporterConfig struct {
	Enabled bool     `yaml:"enabled"`
	Broker  string   `yaml:"broker"`
	Port    int      `yaml:"port"`
	Topic   string   `yaml:"topic"`
	Name    string   `yaml:"name"`
	Workers int      `yaml:"workers"`
	Modes   []string `yaml:"modes"`
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

// DedupConfig contains deduplication settings
type DedupConfig struct {
	Enabled              bool `yaml:"enabled"`
	ClusterWindowSeconds int  `yaml:"cluster_window_seconds"`
	UserWindowSeconds    int  `yaml:"user_window_seconds"`
}

// AdminConfig contains admin interface settings
type AdminConfig struct {
	HTTPPort    int    `yaml:"http_port"`
	BindAddress string `yaml:"bind_address"`
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// FilterConfig holds default filter behavior for new users.
type FilterConfig struct {
	DefaultModes []string `yaml:"default_modes"`
}

// StatsConfig controls periodic runtime reporting.
type StatsConfig struct {
	DisplayIntervalSeconds int `yaml:"display_interval_seconds"`
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
	// RecencySeconds defines how old the supporting spots can be.
	RecencySeconds int `yaml:"recency_seconds"`
	// MaxEditDistance bounds how different the alternate call can be from the
	// original (Levenshtein distance). Prevents wildly different corrections.
	MaxEditDistance int `yaml:"max_edit_distance"`
	// InvalidAction controls what to do when consensus suggests a callsign that
	// fails CTY validation. Supported values:
	//   - "broadcast": keep the original spot (default)
	//   - "suppress": drop the spot entirely
	InvalidAction string `yaml:"invalid_action"`
}

// HarmonicConfig controls detection and suppression of harmonic spots.
type HarmonicConfig struct {
	Enabled              bool    `yaml:"enabled"`
	RecencySeconds       int     `yaml:"recency_seconds"`
	MaxHarmonicMultiple  int     `yaml:"max_harmonic_multiple"`
	FrequencyToleranceHz float64 `yaml:"frequency_tolerance_hz"`
	MinReportDelta       int     `yaml:"min_report_delta"`
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

// ConfidenceConfig controls external data for adjusting confidence.
type ConfidenceConfig struct {
	KnownCallsignsFile string `yaml:"known_callsigns_file"`
}

// CTYConfig allows overriding the CTY prefix database path.
type CTYConfig struct {
	File string `yaml:"file"`
}

// Load loads configuration from a YAML file
func Load(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if cfg.Stats.DisplayIntervalSeconds <= 0 {
		cfg.Stats.DisplayIntervalSeconds = 30
	}
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
	if cfg.CallCorrection.InvalidAction == "" {
		cfg.CallCorrection.InvalidAction = "broadcast"
	}

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
	if strings.TrimSpace(cfg.CTY.File) == "" {
		cfg.CTY.File = "data/cty/cty.plist"
	}
	if cfg.Buffer.Capacity <= 0 {
		cfg.Buffer.Capacity = 300000
	}
	return &cfg, nil
}

// Print displays the configuration
func (c *Config) Print() {
	fmt.Printf("Server: %s (%s)\n", c.Server.Name, c.Server.NodeID)
	workerDesc := "auto"
	if c.Telnet.BroadcastWorkers > 0 {
		workerDesc = fmt.Sprintf("%d", c.Telnet.BroadcastWorkers)
	}
	fmt.Printf("Telnet: port %d (broadcast workers=%s)\n", c.Telnet.Port, workerDesc)
	if c.RBN.Enabled {
		fmt.Printf("RBN CW/RTTY: %s:%d (as %s)\n", c.RBN.Host, c.RBN.Port, c.RBN.Callsign)
	}
	if c.RBNDigital.Enabled {
		fmt.Printf("RBN Digital (FT4/FT8): %s:%d (as %s)\n", c.RBNDigital.Host, c.RBNDigital.Port, c.RBNDigital.Callsign)
	}
	if c.PSKReporter.Enabled {
		fmt.Printf("PSKReporter: %s:%d (topic: %s)\n", c.PSKReporter.Broker, c.PSKReporter.Port, c.PSKReporter.Topic)
	}
	if c.Dedup.Enabled {
		fmt.Printf("Dedup: cluster=%ds, user=%ds\n", c.Dedup.ClusterWindowSeconds, c.Dedup.UserWindowSeconds)
	}
	if len(c.Filter.DefaultModes) > 0 {
		fmt.Printf("Default modes: %s\n", strings.Join(c.Filter.DefaultModes, ", "))
	}
	fmt.Printf("Stats interval: %ds\n", c.Stats.DisplayIntervalSeconds)
	status := "disabled"
	if c.CallCorrection.Enabled {
		status = "enabled"
	}
	fmt.Printf("Call correction: %s (min_reports=%d advantage>%d confidence>=%d%% recency=%ds max_edit=%d invalid_action=%s)\n",
		status,
		c.CallCorrection.MinConsensusReports,
		c.CallCorrection.MinAdvantage,
		c.CallCorrection.MinConfidencePercent,
		c.CallCorrection.RecencySeconds,
		c.CallCorrection.MaxEditDistance,
		c.CallCorrection.InvalidAction)

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
	if c.Confidence.KnownCallsignsFile != "" {
		fmt.Printf("Confidence: known_calls=%s\n", c.Confidence.KnownCallsignsFile)
	}
	fmt.Printf("Ring buffer capacity: %d spots\n", c.Buffer.Capacity)
}
