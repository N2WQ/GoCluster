// Package config loads the cluster's YAML configuration, normalizes defaults,
// and exposes a strongly typed struct other packages rely on at startup.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dxcluster/strutil"

	"gopkg.in/yaml.v3"
)

const (
	// TelnetTransportNative uses the built-in telnet/IAC handling.
	TelnetTransportNative = "native"
	// TelnetTransportZiutek uses the external ziutek/telnet transport for IAC handling.
	TelnetTransportZiutek = "ziutek"
	// TelnetEchoServer enables server-side echo (telnet clients disable local echo).
	TelnetEchoServer = "server"
	// TelnetEchoLocal requests local echo on the client (server does not echo).
	TelnetEchoLocal = "local"
	// TelnetEchoOff disables server echo and requests client echo off (best-effort).
	TelnetEchoOff = "off"
)

// Purpose: Normalize and validate the telnet transport setting.
// Key aspects: Defaults to "native"; returns ok=false on invalid values.
// Upstream: Load config normalization.
// Downstream: TelnetTransport constants.
func normalizeTelnetTransport(value string) (string, bool) {
	trimmed := strutil.NormalizeLower(value)
	if trimmed == "" {
		return TelnetTransportNative, true
	}
	switch trimmed {
	case TelnetTransportNative, TelnetTransportZiutek:
		return trimmed, true
	default:
		return "", false
	}
}

// Purpose: Normalize and validate the telnet echo mode setting.
// Key aspects: Defaults to "server"; returns ok=false on invalid values.
// Upstream: Load config normalization.
// Downstream: TelnetEcho* constants.
func normalizeTelnetEchoMode(value string) (string, bool) {
	trimmed := strutil.NormalizeLower(value)
	if trimmed == "" {
		return TelnetEchoServer, true
	}
	switch trimmed {
	case TelnetEchoServer, TelnetEchoLocal, TelnetEchoOff:
		return trimmed, true
	default:
		return "", false
	}
}

// Config represents the complete cluster configuration. The struct maps
// directly to the YAML files on disk (merged from a config directory) and is
// enriched with defaults during Load so downstream packages can assume sane,
// non-zero values.
type Config struct {
	Server              ServerConfig         `yaml:"server"`
	Telnet              TelnetConfig         `yaml:"telnet"`
	UI                  UIConfig             `yaml:"ui"`
	Logging             LoggingConfig        `yaml:"logging"`
	PropReport          PropReportConfig     `yaml:"prop_report"`
	RBN                 RBNConfig            `yaml:"rbn"`
	RBNDigital          RBNConfig            `yaml:"rbn_digital"`
	HumanTelnet         RBNConfig            `yaml:"human_telnet"`
	PSKReporter         PSKReporterConfig    `yaml:"pskreporter"`
	Archive             ArchiveConfig        `yaml:"archive"`
	Dedup               DedupConfig          `yaml:"dedup"`
	Filter              FilterConfig         `yaml:"filter"`
	Stats               StatsConfig          `yaml:"stats"`
	CallCorrection      CallCorrectionConfig `yaml:"call_correction"`
	CallCache           CallCacheConfig      `yaml:"call_cache"`
	Harmonics           HarmonicConfig       `yaml:"harmonics"`
	SpotPolicy          SpotPolicy           `yaml:"spot_policy"`
	ModeInference       ModeInferenceConfig  `yaml:"mode_inference"`
	CTY                 CTYConfig            `yaml:"cty"`
	Buffer              BufferConfig         `yaml:"buffer"`
	Skew                SkewConfig           `yaml:"skew"`
	FCCULS              FCCULSConfig         `yaml:"fcc_uls"`
	KnownCalls          KnownCallsConfig     `yaml:"known_calls"`
	Peering             PeeringConfig        `yaml:"peering"`
	Reputation          ReputationConfig     `yaml:"reputation"`
	GridDBPath          string               `yaml:"grid_db"`
	GridFlushSec        int                  `yaml:"grid_flush_seconds"`
	GridCacheSize       int                  `yaml:"grid_cache_size"`
	GridCacheTTLSec     int                  `yaml:"grid_cache_ttl_seconds"`
	GridBlockCacheMB    int                  `yaml:"grid_block_cache_mb"`
	GridBloomFilterBits int                  `yaml:"grid_bloom_filter_bits"`
	GridMemTableSizeMB  int                  `yaml:"grid_memtable_size_mb"`
	GridL0Compaction    int                  `yaml:"grid_l0_compaction_threshold"`
	GridL0StopWrites    int                  `yaml:"grid_l0_stop_writes_threshold"`
	GridWriteQueueDepth int                  `yaml:"grid_write_queue_depth"`
	H3TablePath         string               `yaml:"h3_table_path"`
	// GridDBCheckOnMiss controls whether grid updates consult Pebble on cache miss
	// to avoid redundant writes. When nil, Load defaults it to true to preserve
	// historical behavior.
	GridDBCheckOnMiss *bool `yaml:"grid_db_check_on_miss"`
	GridTTLDays       int   `yaml:"grid_ttl_days"`
	// GridPreflightTimeoutMS is ignored for the Pebble grid store (retained for compatibility).
	GridPreflightTimeoutMS int `yaml:"grid_preflight_timeout_ms"`
	// LoadedFrom is populated by Load with the path or directory used to build
	// this configuration. It is not driven by YAML.
	LoadedFrom string `yaml:"-"`
}

// ServerConfig contains general server settings
type ServerConfig struct {
	Name   string `yaml:"name"`
	NodeID string `yaml:"node_id"`
}

// TelnetConfig contains telnet server settings
type TelnetConfig struct {
	Port           int  `yaml:"port"`
	TLSEnabled     bool `yaml:"tls_enabled"`
	MaxConnections int  `yaml:"max_connections"`
	// WelcomeMessage is sent before login; supports <CALL>, <CLUSTER>, <DATE>, <TIME>, <DATETIME>, <UPTIME>,
	// <USER_COUNT>, <LAST_LOGIN>, <LAST_IP>, <DIALECT>, <DIALECT_SOURCE>, <DIALECT_DEFAULT>, <GRID>, and <NOISE>.
	WelcomeMessage    string `yaml:"welcome_message"`
	DuplicateLoginMsg string `yaml:"duplicate_login_message"`
	// LoginGreeting is sent after successful login; supports <CALL>, <CLUSTER>, <DATE>, <TIME>, <DATETIME>, <UPTIME>,
	// <USER_COUNT>, <LAST_LOGIN>, <LAST_IP>, <DIALECT>, <DIALECT_SOURCE>, <DIALECT_DEFAULT>, <GRID>, and <NOISE>.
	LoginGreeting string `yaml:"login_greeting"`
	// LoginPrompt is sent before reading the callsign; supports <DATE>, <TIME>, <DATETIME>, <UPTIME>, and <USER_COUNT>.
	LoginPrompt string `yaml:"login_prompt"`
	// LoginEmptyMessage is sent when the callsign is blank; supports <DATE>, <TIME>, <DATETIME>, <UPTIME>, and <USER_COUNT>.
	LoginEmptyMessage string `yaml:"login_empty_message"`
	// LoginInvalidMessage is sent when the callsign fails validation; supports <DATE>, <TIME>, <DATETIME>, <UPTIME>, and <USER_COUNT>.
	LoginInvalidMessage string `yaml:"login_invalid_message"`
	// InputTooLongMessage is sent when input exceeds LoginLineLimit/CommandLineLimit; supports <CONTEXT>, <MAX_LEN>, and <ALLOWED>.
	InputTooLongMessage string `yaml:"input_too_long_message"`
	// InputInvalidCharMessage is sent when input includes forbidden bytes; supports <CONTEXT>, <MAX_LEN>, and <ALLOWED>.
	InputInvalidCharMessage string `yaml:"input_invalid_char_message"`
	// DialectWelcomeMessage is sent after login to describe the active dialect; supports <DIALECT>, <DIALECT_SOURCE>, and <DIALECT_DEFAULT>.
	DialectWelcomeMessage string `yaml:"dialect_welcome_message"`
	// DialectSourceDefaultLabel labels a default dialect in DialectWelcomeMessage.
	DialectSourceDefaultLabel string `yaml:"dialect_source_default_label"`
	// DialectSourcePersistedLabel labels a persisted dialect in DialectWelcomeMessage.
	DialectSourcePersistedLabel string `yaml:"dialect_source_persisted_label"`
	// PathStatusMessage is sent after login when path reliability display is enabled; supports <GRID> and <NOISE>.
	PathStatusMessage string `yaml:"path_status_message"`
	// NearbyLoginWarning is appended to the login greeting when NEARBY is active.
	NearbyLoginWarning string `yaml:"nearby_login_warning"`
	// Transport selects the telnet parser/negotiation backend ("native" or "ziutek").
	Transport string `yaml:"transport"`
	// EchoMode controls whether the server echoes input or requests local echo.
	// Supported values: "server" (default), "local", "off".
	EchoMode         string `yaml:"echo_mode"`
	BroadcastWorkers int    `yaml:"broadcast_workers"`
	BroadcastQueue   int    `yaml:"broadcast_queue_size"`
	WorkerQueue      int    `yaml:"worker_queue_size"`
	ClientBuffer     int    `yaml:"client_buffer_size"`
	// ControlQueueSize bounds per-client control output (bulletins, prompts, keepalives).
	ControlQueueSize int  `yaml:"control_queue_size"`
	SkipHandshake    bool `yaml:"skip_handshake"`
	// BroadcastBatchIntervalMS controls telnet broadcast micro-batching. 0 disables batching.
	BroadcastBatchIntervalMS int `yaml:"broadcast_batch_interval_ms"`
	// KeepaliveSeconds, when >0, emits a periodic CRLF to all connected clients to keep idle
	// network devices from timing out otherwise quiet sessions.
	KeepaliveSeconds int `yaml:"keepalive_seconds"`
	// ReadIdleTimeoutSeconds sets the read deadline for logged-in sessions; timeouts do not disconnect.
	ReadIdleTimeoutSeconds int `yaml:"read_idle_timeout_seconds"`
	// LoginTimeoutSeconds sets the maximum time to complete pre-login callsign entry.
	LoginTimeoutSeconds int `yaml:"login_timeout_seconds"`
	// LoginLineLimit bounds how many bytes are accepted for the initial callsign
	// prompt. Keep this tight to prevent DoS via huge login banners.
	LoginLineLimit int `yaml:"login_line_limit"`
	// CommandLineLimit bounds how many bytes post-login commands may include.
	// Raising this can help workflows that need larger filter strings.
	CommandLineLimit int `yaml:"command_line_limit"`
	// OutputLineLength controls the DX-cluster output line length (no CRLF).
	// Length uses 1-based columns and must be >= 65.
	OutputLineLength int `yaml:"output_line_length"`
	// DropExtremeRate disconnects lenient clients when their spot drop rate exceeds this threshold.
	DropExtremeRate float64 `yaml:"drop_extreme_rate"`
	// DropExtremeWindowSeconds is the sliding window used for extreme drop evaluation.
	DropExtremeWindowSeconds int `yaml:"drop_extreme_window_seconds"`
	// DropExtremeMinAttempts gates extreme drop checks until this many attempts accrue.
	DropExtremeMinAttempts int `yaml:"drop_extreme_min_attempts"`
}

// ReputationConfig controls the passwordless telnet reputation gate.
type ReputationConfig struct {
	Enabled bool `yaml:"enabled"`

	// IPinfo Lite snapshot paths and download settings (optional; can be disabled when
	// using live API only).
	IPInfoSnapshotPath         string `yaml:"ipinfo_snapshot_path"`
	IPInfoDownloadPath         string `yaml:"ipinfo_download_path"`
	IPInfoDownloadURL          string `yaml:"ipinfo_download_url"`
	IPInfoDownloadToken        string `yaml:"ipinfo_download_token"`
	IPInfoRefreshUTC           string `yaml:"ipinfo_refresh_utc"`
	IPInfoDownloadTimeoutMS    int    `yaml:"ipinfo_download_timeout_ms"`
	IPInfoImportTimeoutMS      int    `yaml:"ipinfo_import_timeout_ms"`
	SnapshotMaxAgeSeconds      int    `yaml:"snapshot_max_age_seconds"`
	IPInfoDownloadEnabled      bool   `yaml:"ipinfo_download_enabled"`
	IPInfoDeleteCSVAfterImport bool   `yaml:"ipinfo_delete_csv_after_import"`
	IPInfoKeepGzip             bool   `yaml:"ipinfo_keep_gzip"`

	// IPinfo Pebble store (on-disk index).
	IPInfoPebblePath     string `yaml:"ipinfo_pebble_path"`
	IPInfoPebbleCacheMB  int    `yaml:"ipinfo_pebble_cache_mb"`
	IPInfoPebbleLoadIPv4 bool   `yaml:"ipinfo_pebble_load_ipv4"`
	IPInfoPebbleCleanup  bool   `yaml:"ipinfo_pebble_cleanup"`
	IPInfoPebbleCompact  bool   `yaml:"ipinfo_pebble_compact"`

	// IPinfo live API fallback.
	IPInfoAPIEnabled   bool   `yaml:"ipinfo_api_enabled"`
	IPInfoAPIToken     string `yaml:"ipinfo_api_token"`
	IPInfoAPIBaseURL   string `yaml:"ipinfo_api_base_url"`
	IPInfoAPITimeoutMS int    `yaml:"ipinfo_api_timeout_ms"`

	// Cymru DNS fallback settings.
	FallbackTeamCymru       bool `yaml:"fallback_team_cymru"`
	CymruLookupTimeoutMS    int  `yaml:"cymru_lookup_timeout_ms"`
	CymruCacheTTLSeconds    int  `yaml:"cymru_cache_ttl_seconds"`
	CymruNegativeTTLSeconds int  `yaml:"cymru_negative_ttl_seconds"`
	CymruWorkers            int  `yaml:"cymru_workers"`

	// Reputation gate thresholds.
	InitialWaitSeconds              int    `yaml:"initial_wait_seconds"`
	RampWindowSeconds               int    `yaml:"ramp_window_seconds"`
	PerBandStart                    int    `yaml:"per_band_start"`
	PerBandCap                      int    `yaml:"per_band_cap"`
	TotalCapStart                   int    `yaml:"total_cap_start"`
	TotalCapPostRamp                int    `yaml:"total_cap_post_ramp"`
	TotalCapRampDelaySeconds        int    `yaml:"total_cap_ramp_delay_seconds"`
	CountryMismatchExtraWaitSeconds int    `yaml:"country_mismatch_extra_wait_seconds"`
	DisagreementPenaltySeconds      int    `yaml:"disagreement_penalty_seconds"`
	UnknownPenaltySeconds           int    `yaml:"unknown_penalty_seconds"`
	DisagreementResetOnNew          bool   `yaml:"disagreement_reset_on_new"`
	ResetOnNewASN                   bool   `yaml:"reset_on_new_asn"`
	CountryFlipScope                string `yaml:"country_flip_scope"`

	// Bounded state and cache settings.
	MaxASNHistory         int `yaml:"max_asn_history"`
	MaxCountryHistory     int `yaml:"max_country_history"`
	StateTTLSeconds       int `yaml:"state_ttl_seconds"`
	StateMaxEntries       int `yaml:"state_max_entries"`
	PrefixTTLSeconds      int `yaml:"prefix_ttl_seconds"`
	PrefixMaxEntries      int `yaml:"prefix_max_entries"`
	LookupCacheTTLSeconds int `yaml:"lookup_cache_ttl_seconds"`
	LookupCacheMaxEntries int `yaml:"lookup_cache_max_entries"`

	// Prefix token bucket limits.
	IPv4BucketSize         int `yaml:"ipv4_bucket_size"`
	IPv4BucketRefillPerSec int `yaml:"ipv4_bucket_refill_per_sec"`
	IPv6BucketSize         int `yaml:"ipv6_bucket_size"`
	IPv6BucketRefillPerSec int `yaml:"ipv6_bucket_refill_per_sec"`

	// Observability and storage.
	ConsoleDropDisplay bool    `yaml:"console_drop_display"`
	DropLogSampleRate  float64 `yaml:"drop_log_sample_rate"`
	ReputationDir      string  `yaml:"reputation_dir"`
}

// UIConfig controls the optional local console UI. The legacy TUI uses tview;
// the lean ANSI mode uses fixed buffers and ANSI escape codes. Mode must be
// one of ansi, tview, tview-v2, or headless (disables the local UI).
type UIConfig struct {
	// Mode selects the UI renderer: "ansi", "tview", "tview-v2", or "headless".
	Mode string `yaml:"mode"`
	// RefreshMS controls the ANSI render cadence; ignored by tview. 0 disables
	// periodic renders (events will still be buffered).
	RefreshMS int `yaml:"refresh_ms"`
	// Color enables simple ANSI coloring for marked-up lines; when false the
	// markup tokens are stripped.
	Color bool `yaml:"color"`
	// ClearScreen is ignored by the ANSI renderer (kept for compatibility).
	ClearScreen bool `yaml:"clear_screen"`
	// PaneLines sets tview pane heights (ANSI uses a fixed layout).
	PaneLines UIPaneLines `yaml:"pane_lines"`
	// V2 configures the page-based tview renderer when ui.mode = tview-v2.
	V2 UIV2Config `yaml:"v2"`
}

// LoggingConfig controls optional system log duplication to disk.
type LoggingConfig struct {
	Enabled                 bool   `yaml:"enabled"`
	DropDedupeWindowSeconds int    `yaml:"drop_dedupe_window_seconds"`
	Dir                     string `yaml:"dir"`
	RetentionDays           int    `yaml:"retention_days"`
}

// PropReportConfig controls automatic propagation report generation on log rotation.
type PropReportConfig struct {
	Enabled    bool   `yaml:"enabled"`
	RefreshUTC string `yaml:"refresh_utc"`
}

// UIPaneLines bounds history depth for ANSI and visible pane heights for tview.
type UIPaneLines struct {
	Stats      int `yaml:"stats"`
	Calls      int `yaml:"calls"`
	Unlicensed int `yaml:"unlicensed"`
	Harmonics  int `yaml:"harmonics"`
	System     int `yaml:"system"`
}

// UIV2Config controls the page-based tview UI.
type UIV2Config struct {
	Pages       []string         `yaml:"pages"`
	EventBuffer UIV2BufferConfig `yaml:"event_buffer"`
	DebugBuffer UIV2BufferConfig `yaml:"debug_buffer"`
	TargetFPS   int              `yaml:"target_fps"`
	EnableMouse bool             `yaml:"enable_mouse"`
	Keybindings UIV2Keybindings  `yaml:"keybindings"`
}

// UIV2BufferConfig bounds event storage for a single page.
type UIV2BufferConfig struct {
	MaxEvents        int  `yaml:"max_events"`
	MaxBytesMB       int  `yaml:"max_bytes_mb"`
	MaxMessageBytes  int  `yaml:"max_message_bytes"`
	EvictOnByteLimit bool `yaml:"evict_on_byte_limit"`
	LogDrops         bool `yaml:"log_drops"`
}

// UIV2Keybindings controls optional alternative navigation bindings.
type UIV2Keybindings struct {
	UseAlternatives bool `yaml:"use_alternatives"`
}

var defaultUIV2Pages = []string{"overview", "ingest", "pipeline", "events"}

func normalizeUIV2(cfg *UIConfig, raw map[string]any) error {
	if cfg == nil {
		return nil
	}

	applyBufferDefaults := func(buf *UIV2BufferConfig, maxEvents, maxBytesMB int, logDrops bool, key string) {
		if buf.MaxEvents <= 0 {
			buf.MaxEvents = maxEvents
		}
		if buf.MaxBytesMB <= 0 {
			buf.MaxBytesMB = maxBytesMB
		}
		if buf.MaxMessageBytes <= 0 {
			buf.MaxMessageBytes = 4096
		}
		if !yamlKeyPresent(raw, "ui", "v2", key, "evict_on_byte_limit") {
			buf.EvictOnByteLimit = true
		}
		if !yamlKeyPresent(raw, "ui", "v2", key, "log_drops") {
			buf.LogDrops = logDrops
		}
	}

	applyBufferDefaults(&cfg.V2.EventBuffer, 1000, 1, true, "event_buffer")
	applyBufferDefaults(&cfg.V2.DebugBuffer, 5000, 2, false, "debug_buffer")

	if cfg.V2.TargetFPS <= 0 {
		cfg.V2.TargetFPS = 30
	}
	if !yamlKeyPresent(raw, "ui", "v2", "keybindings", "use_alternatives") {
		cfg.V2.Keybindings.UseAlternatives = true
	}

	pages := cfg.V2.Pages
	if len(pages) == 0 {
		cfg.V2.Pages = append([]string{}, defaultUIV2Pages...)
		return nil
	}

	allowed := map[string]struct{}{
		"overview": {},
		"ingest":   {},
		"pipeline": {},
		"events":   {},
	}
	seen := make(map[string]struct{}, len(pages))
	normalized := make([]string, 0, len(pages))
	for _, page := range pages {
		name := strings.ToLower(strings.TrimSpace(page))
		if name == "" {
			continue
		}
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("invalid ui.v2.pages entry %q: must be one of overview, ingest, pipeline, events", page)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("invalid ui.v2.pages: duplicate entry %q", page)
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	if len(normalized) == 0 {
		cfg.V2.Pages = append([]string{}, defaultUIV2Pages...)
		return nil
	}
	cfg.V2.Pages = normalized
	return nil
}

// RBNConfig contains Reverse Beacon Network settings
type RBNConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Callsign string `yaml:"callsign"`
	Name     string `yaml:"name"`
	// TelnetTransport selects the telnet parser/negotiation backend ("native" or "ziutek").
	TelnetTransport string `yaml:"telnet_transport"`
	KeepSSIDSuffix  bool   `yaml:"keep_ssid_suffix"`  // when false, compute stripped DE calls for telnet/archive/filters
	SlotBuffer      int    `yaml:"slot_buffer"`       // size of ingest slot buffer between telnet reader and pipeline
	KeepaliveSec    int    `yaml:"keepalive_seconds"` // optional periodic CRLF to keep idle sessions alive (0 disables)
}

// PSKReporterConfig contains PSKReporter MQTT settings
type PSKReporterConfig struct {
	Enabled bool   `yaml:"enabled"`
	Broker  string `yaml:"broker"`
	Port    int    `yaml:"port"`
	Topic   string `yaml:"topic"`
	Name    string `yaml:"name"`
	Workers int    `yaml:"workers"`
	// MQTTInboundWorkers controls Paho's inbound publish dispatch worker pool (0 = auto).
	MQTTInboundWorkers int `yaml:"mqtt_inbound_workers"`
	// MQTTInboundQueueDepth bounds the inbound publish queue from Paho to handlers (0 = auto).
	MQTTInboundQueueDepth int `yaml:"mqtt_inbound_queue_depth"`
	// MQTTQoS12EnqueueTimeoutMS bounds how long QoS1/2 publishes wait when the queue is full.
	MQTTQoS12EnqueueTimeoutMS int `yaml:"mqtt_qos12_enqueue_timeout_ms"`
	// SpotChannelSize controls the buffered ingest channel between MQTT client and processing.
	SpotChannelSize int      `yaml:"spot_channel_size"`
	Modes           []string `yaml:"modes"`
	// PathOnlyModes are ingested for path prediction only; they never reach telnet/dedup/archive.
	PathOnlyModes []string `yaml:"path_only_modes"`
	// AppendSpotterSSID, when true, appends "-#" to receiver callsigns that
	// lack an SSID so deduplication treats each PSK skimmer uniquely.
	AppendSpotterSSID bool `yaml:"append_spotter_ssid"`
	// CTYCacheSize is deprecated; unified call metadata cache uses grid_cache_size.
	CTYCacheSize int `yaml:"cty_cache_size"`
	// CTYCacheTTLSeconds is deprecated; unified cache clears on CTY/SCP reload.
	CTYCacheTTLSeconds int `yaml:"cty_cache_ttl_seconds"`
	// MaxPayloadBytes caps incoming MQTT payload sizes to guard against abuse.
	MaxPayloadBytes int `yaml:"max_payload_bytes"`
}

const (
	defaultPSKReporterTopic                 = "pskr/filter/v2/+/+/#"
	defaultPSKReporterQoS12EnqueueTimeoutMS = 250
)

// ArchiveConfig controls optional Pebble archival of broadcasted spots.
type ArchiveConfig struct {
	Enabled bool `yaml:"enabled"`
	// DBPath is the Pebble archive directory path.
	DBPath                 string `yaml:"db_path"`
	QueueSize              int    `yaml:"queue_size"`
	BatchSize              int    `yaml:"batch_size"`
	BatchIntervalMS        int    `yaml:"batch_interval_ms"`
	CleanupIntervalSeconds int    `yaml:"cleanup_interval_seconds"`
	// CleanupBatchSize limits how many rows are deleted per cleanup batch to keep locks short.
	CleanupBatchSize int `yaml:"cleanup_batch_size"`
	// CleanupBatchYieldMS sleeps between cleanup batches to reduce contention. 0 disables the yield.
	CleanupBatchYieldMS     int `yaml:"cleanup_batch_yield_ms"`
	RetentionFTSeconds      int `yaml:"retention_ft_seconds"`      // FT8/FT4 retention
	RetentionDefaultSeconds int `yaml:"retention_default_seconds"` // All other modes
	// BusyTimeoutMS is ignored for the Pebble archive (retained for compatibility).
	BusyTimeoutMS int `yaml:"busy_timeout_ms"`
	// Synchronous controls archive durability: off disables fsync; normal/full/extra enable sync.
	Synchronous string `yaml:"synchronous"`
	// AutoDeleteCorruptDB removes the archive DB on startup if corruption is detected.
	AutoDeleteCorruptDB bool `yaml:"auto_delete_corrupt_db"`
	// PreflightTimeoutMS is ignored for the Pebble archive (retained for compatibility).
	PreflightTimeoutMS int `yaml:"preflight_timeout_ms"`
}

// PeeringConfig controls DXSpider node-to-node peering.
type PeeringConfig struct {
	Enabled       bool   `yaml:"enabled"`
	LocalCallsign string `yaml:"local_callsign"`
	ListenPort    int    `yaml:"listen_port"`
	HopCount      int    `yaml:"hop_count"`
	NodeVersion   string `yaml:"node_version"`
	NodeBuild     string `yaml:"node_build"`
	LegacyVersion string `yaml:"legacy_version"`
	PC92Bitmap    int    `yaml:"pc92_bitmap"`
	NodeCount     int    `yaml:"node_count"`
	UserCount     int    `yaml:"user_count"`
	// LogKeepalive controls whether keepalive/PC51 chatter is emitted to logs.
	LogKeepalive bool `yaml:"log_keepalive"`
	// LogLineTooLong controls whether oversized peer lines are logged.
	LogLineTooLong bool `yaml:"log_line_too_long"`
	// TelnetTransport selects the telnet parser/negotiation backend ("native" or "ziutek").
	TelnetTransport  string `yaml:"telnet_transport"`
	KeepaliveSeconds int    `yaml:"keepalive_seconds"`
	// ConfigSeconds drives periodic PC92 C refresh frames; peers drop topology if
	// they miss several config periods. 0 disables.
	ConfigSeconds  int `yaml:"config_seconds"`
	WriteQueueSize int `yaml:"write_queue_size"`
	MaxLineLength  int `yaml:"max_line_length"`
	// PC92MaxBytes caps how much of a PC92 topology frame we will buffer/parse.
	// Set to 0 to use a safe default derived from max_line_length.
	PC92MaxBytes int             `yaml:"pc92_max_bytes"`
	Peers        []PeeringPeer   `yaml:"peers"`
	Timeouts     PeeringTimeouts `yaml:"timeouts"`
	Backoff      PeeringBackoff  `yaml:"backoff"`
	Topology     PeeringTopology `yaml:"topology"`
	ACL          PeeringACL      `yaml:"acl"`
}

type PeeringPeer struct {
	Enabled        bool   `yaml:"enabled"`
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Password       string `yaml:"password"`
	PreferPC9x     bool   `yaml:"prefer_pc9x"`
	LoginCallsign  string `yaml:"login_callsign"`
	RemoteCallsign string `yaml:"remote_callsign"`
}

type PeeringTimeouts struct {
	LoginSeconds int `yaml:"login_seconds"`
	InitSeconds  int `yaml:"init_seconds"`
	IdleSeconds  int `yaml:"idle_seconds"`
}

type PeeringBackoff struct {
	BaseMS int `yaml:"base_ms"`
	MaxMS  int `yaml:"max_ms"`
}

type PeeringTopology struct {
	DBPath                 string `yaml:"db_path"`
	RetentionHours         int    `yaml:"retention_hours"`
	PersistIntervalSeconds int    `yaml:"persist_interval_seconds"`
}

type PeeringACL struct {
	AllowIPs       []string `yaml:"allow_ips"`
	AllowCallsigns []string `yaml:"allow_callsigns"`
}

// Purpose: Build MQTT subscription topics based on configured modes.
// Key aspects: Always uses a single catch-all subscription unless a custom topic is provided.
// Upstream: PSKReporter client setup.
// Downstream: None.
func (c *PSKReporterConfig) SubscriptionTopics() []string {
	if c.Topic != "" {
		return []string{c.Topic}
	}
	// Single catch-all subscription; mode filtering happens downstream.
	return []string{defaultPSKReporterTopic}
}

// DedupConfig contains deduplication settings. The cluster-wide window controls how
// aggressively we suppress duplicates:
//   - A positive window enables deduplication for that many seconds.
//   - A zero or negative window effectively disables dedup (spots pass through immediately).
//
// Secondary dedupe has three policy windows (fast/med/slow) with independent SNR behavior.
// When enabled, stronger SNR updates may replace the cached entry and be forwarded.
type DedupConfig struct {
	ClusterWindowSeconds       int  `yaml:"cluster_window_seconds"`             // <=0 disables primary dedup
	SecondaryFastWindowSeconds int  `yaml:"secondary_fast_window_seconds"`      // <=0 disables fast secondary dedupe
	SecondaryMedWindowSeconds  int  `yaml:"secondary_med_window_seconds"`       // <=0 disables med secondary dedupe
	SecondarySlowWindowSeconds int  `yaml:"secondary_slow_window_seconds"`      // <=0 disables slow secondary dedupe
	PreferStrongerSNR          bool `yaml:"prefer_stronger_snr"`                // keep max SNR when dropping duplicates
	SecondaryFastPreferStrong  bool `yaml:"secondary_fast_prefer_stronger_snr"` // keep max SNR in fast secondary buckets
	SecondaryMedPreferStrong   bool `yaml:"secondary_med_prefer_stronger_snr"`  // keep max SNR in med secondary buckets
	SecondarySlowPreferStrong  bool `yaml:"secondary_slow_prefer_stronger_snr"` // keep max SNR in slow secondary buckets
	OutputBufferSize           int  `yaml:"output_buffer_size"`                 // channel capacity for dedup output
}

// FilterConfig holds default filter behavior for new users.
type FilterConfig struct {
	DefaultModes []string `yaml:"default_modes"`
	// DefaultSources controls the initial SOURCE filter (HUMAN/SKIMMER) applied
	// when a callsign has no saved filter file under data/users/.
	// When empty or omitted, new users accept both categories (SOURCE=ALL).
	DefaultSources []string `yaml:"default_sources"`
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
	// BandStateOverrides allows per-band, per-activity-state tuning of frequency tolerance
	// and quality binning. Values fall back to the global defaults when omitted.
	BandStateOverrides []BandStateOverride `yaml:"band_state_overrides"`
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
	// VoiceFrequencyToleranceHz defines the frequency window for USB/LSB consensus
	// (voice signals are wider than CW/RTTY).
	VoiceFrequencyToleranceHz float64 `yaml:"voice_frequency_tolerance_hz"`
	// DebugLog, when true, records call-correction decisions to an asynchronous SQLite log.
	DebugLog bool `yaml:"debug_log"`
	// DebugLogFile optionally overrides the decision log location/prefix; when blank a daily
	// callcorr_YYYY-MM-DD.db file is written under data/analysis.
	DebugLogFile string `yaml:"debug_log_file"`
	// Quality-based anchors: optional per-frequency-bin confidence store.
	QualityBinHz                      int   `yaml:"quality_bin_hz"`
	QualityGoodThreshold              int   `yaml:"quality_good_threshold"`
	QualityNewCallIncrement           int   `yaml:"quality_newcall_increment"`
	QualityBustedDecrement            int   `yaml:"quality_busted_decrement"`
	CallQualityTTLSeconds             int   `yaml:"call_quality_ttl_seconds"`
	CallQualityMaxEntries             int   `yaml:"call_quality_max_entries"`
	CallQualityCleanupIntervalSeconds int   `yaml:"call_quality_cleanup_interval_seconds"`
	CallQualityPinPriors              *bool `yaml:"call_quality_pin_priors"`
	// Strategy selects how consensus is computed:
	//   - "majority": pick the most-reported call on-frequency (unique spotters); distance is only a safety cap.
	//     Other values are accepted for compatibility but currently coerced to majority.
	Strategy string `yaml:"strategy"`
	// MinSNRCW/RTTY allow discarding marginal decodes from the corroborator set.
	MinSNRCW   int `yaml:"min_snr_cw"`
	MinSNRRTTY int `yaml:"min_snr_rtty"`
	// MinSNRVoice allows discarding low-SNR USB/LSB reports (set 0 to ignore SNR).
	MinSNRVoice int `yaml:"min_snr_voice"`
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
	// Cooldown gate to avoid flipping away from a call that already has recent, diverse
	// support. Applies only to corrections away from the subject; corrections toward it
	// still proceed. Bypass allows a decisively stronger alternate to override the cooldown.
	CooldownEnabled          bool `yaml:"cooldown_enabled"`
	CooldownMinReporters     int  `yaml:"cooldown_min_reporters"`
	CooldownDurationSeconds  int  `yaml:"cooldown_duration_seconds"`
	CooldownTTLSeconds       int  `yaml:"cooldown_ttl_seconds"`
	CooldownBinHz            int  `yaml:"cooldown_bin_hz"`
	CooldownMaxReporters     int  `yaml:"cooldown_max_reporters"`
	CooldownBypassAdvantage  int  `yaml:"cooldown_bypass_advantage"`
	CooldownBypassConfidence int  `yaml:"cooldown_bypass_confidence"`
	// Frequency guard: skip corrections when a strong runner-up exists at a different frequency.
	FreqGuardMinSeparationKHz float64 `yaml:"freq_guard_min_separation_khz"`
	// Ratio (0-1): runner-up supporters must be at least this fraction of winner supporters to trigger the guard.
	FreqGuardRunnerUpRatio float64 `yaml:"freq_guard_runnerup_ratio"`
	// VoiceCandidateWindowKHz controls the correction-index search radius for USB/LSB.
	VoiceCandidateWindowKHz float64 `yaml:"voice_candidate_window_khz"`
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

// BandStateOverride groups bands and per-state overrides for correction tolerances.
type BandStateOverride struct {
	Name   string          `yaml:"name"`
	Bands  []string        `yaml:"bands"`
	Quiet  BandStateParams `yaml:"quiet"`
	Normal BandStateParams `yaml:"normal"`
	Busy   BandStateParams `yaml:"busy"`
}

// BandStateParams holds per-state overrides for a band group.
type BandStateParams struct {
	QualityBinHz         int     `yaml:"quality_bin_hz"`
	FrequencyToleranceHz float64 `yaml:"frequency_tolerance_hz"`
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

// ModeInferenceConfig controls how the cluster assigns modes when the
// comment does not provide an explicit mode token.
type ModeInferenceConfig struct {
	// DXFreqCacheTTLSeconds bounds how long a DX+frequency mode stays in memory.
	DXFreqCacheTTLSeconds int `yaml:"dx_freq_cache_ttl_seconds"`
	// DXFreqCacheSize bounds the DX+frequency mode cache size.
	DXFreqCacheSize int `yaml:"dx_freq_cache_size"`
	// DigitalWindowSeconds is the recency window for counting distinct corroborators.
	DigitalWindowSeconds int `yaml:"digital_window_seconds"`
	// DigitalMinCorroborators is the minimum distinct spotters needed to trust a mode.
	DigitalMinCorroborators int `yaml:"digital_min_corroborators"`
	// DigitalSeedTTLSeconds controls how long seeded frequencies remain valid without refresh.
	DigitalSeedTTLSeconds int `yaml:"digital_seed_ttl_seconds"`
	// DigitalCacheSize bounds the number of frequency buckets tracked in the digital map.
	DigitalCacheSize int `yaml:"digital_cache_size"`
	// DigitalSeeds pre-populates the digital map with known FT4/FT8/JS8 dial frequencies.
	DigitalSeeds []ModeSeed `yaml:"digital_seeds"`
}

// ModeSeed defines a single seeded digital frequency entry.
type ModeSeed struct {
	FrequencyKHz int    `yaml:"frequency_khz"`
	Mode         string `yaml:"mode"`
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
	// AllowlistPath points to a file containing regex patterns of calls to skip ULS checks.
	AllowlistPath string `yaml:"allowlist_path"`
	// CacheTTLSeconds controls how long license lookup decisions remain cached.
	CacheTTLSeconds int `yaml:"cache_ttl_seconds"`
}

// CTYConfig controls downloading of the CTY prefix plist.
type CTYConfig struct {
	Enabled    bool   `yaml:"enabled"`
	URL        string `yaml:"url"`
	File       string `yaml:"file"`
	RefreshUTC string `yaml:"refresh_utc"`
}

// Load reads configuration from a YAML directory (or a single YAML file if a file
// path is explicitly supplied), applies defaults, and validates key fields so the
// rest of the cluster can rely on a consistent baseline.
// Purpose: Load and normalize the cluster configuration from a directory.
// Key aspects: Supports directory merge; applies defaults and validates values.
// Upstream: main.go startup.
// Downstream: loadConfigDir, mergeYAMLMaps, normalize* helpers.
func Load(path string) (*Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat config path %q: %w", path, err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("config path %q must be a directory containing YAML files", path)
	}

	merged, files, err := loadConfigDir(path)
	if err != nil {
		return nil, err
	}
	raw := merged
	data, err := yaml.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("failed to render merged config from %q: %w", path, err)
	}
	// Store the directory we loaded from to aid downstream diagnostics.
	if len(files) > 0 {
		path = filepath.Clean(path)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %q: %w", path, err)
	}
	cfg.LoadedFrom = filepath.Clean(path)

	ctyEnabledSet := yamlKeyPresent(raw, "cty", "enabled")
	hasSecondaryFastPrefer := yamlKeyPresent(raw, "dedup", "secondary_fast_prefer_stronger_snr")
	hasSecondaryMedPrefer := yamlKeyPresent(raw, "dedup", "secondary_med_prefer_stronger_snr")
	hasSecondarySlowPrefer := yamlKeyPresent(raw, "dedup", "secondary_slow_prefer_stronger_snr")
	hasSecondaryFastWindow := yamlKeyPresent(raw, "dedup", "secondary_fast_window_seconds")
	hasSecondaryMedWindow := yamlKeyPresent(raw, "dedup", "secondary_med_window_seconds")
	hasSecondarySlowWindow := yamlKeyPresent(raw, "dedup", "secondary_slow_window_seconds")
	legacySecondaryWindow := yamlKeyPresent(raw, "dedup", "secondary_window_seconds")
	legacySecondaryPrefer := yamlKeyPresent(raw, "dedup", "secondary_prefer_stronger_snr")
	hasAdaptiveMinReportsEnabled := yamlKeyPresent(raw, "call_correction", "adaptive_min_reports", "enabled")
	hasArchiveCleanupYield := yamlKeyPresent(raw, "archive", "cleanup_batch_yield_ms")
	hasPSKRMQTTTimeout := yamlKeyPresent(raw, "pskreporter", "mqtt_qos12_enqueue_timeout_ms")

	if legacySecondaryWindow || legacySecondaryPrefer {
		fmt.Printf("Warning: dedup.secondary_window_seconds and dedup.secondary_prefer_stronger_snr are deprecated and ignored; use secondary_fast_* / secondary_med_* / secondary_slow_* instead.\n")
	}

	// UI defaults favor the lightweight ANSI renderer unless overridden. Mode
	// is normalized to a small, explicit set to keep startup behavior
	// predictable and YAML-driven.
	uiMode := strings.ToLower(strings.TrimSpace(cfg.UI.Mode))
	if uiMode == "" {
		uiMode = "ansi"
	}
	switch uiMode {
	case "ansi", "tview", "tview-v2", "headless":
		cfg.UI.Mode = uiMode
	case "none":
		cfg.UI.Mode = "headless"
	case "auto", "ansi_poc":
		cfg.UI.Mode = "ansi"
	default:
		return nil, fmt.Errorf("invalid ui.mode %q: must be ansi, tview, tview-v2, or headless", cfg.UI.Mode)
	}
	if cfg.UI.RefreshMS <= 0 {
		cfg.UI.RefreshMS = 250
	}
	if cfg.UI.PaneLines.Stats <= 0 {
		cfg.UI.PaneLines.Stats = 8
	}
	if cfg.UI.PaneLines.Calls <= 0 {
		cfg.UI.PaneLines.Calls = 20
	}
	if cfg.UI.PaneLines.Unlicensed <= 0 {
		cfg.UI.PaneLines.Unlicensed = 20
	}
	if cfg.UI.PaneLines.Harmonics <= 0 {
		cfg.UI.PaneLines.Harmonics = 20
	}
	if cfg.UI.PaneLines.System <= 0 {
		cfg.UI.PaneLines.System = 40
	}
	if !yamlKeyPresent(raw, "ui", "color") {
		cfg.UI.Color = true
	}
	if !yamlKeyPresent(raw, "ui", "clear_screen") {
		cfg.UI.ClearScreen = true
	}

	if err := normalizeUIV2(&cfg.UI, raw); err != nil {
		return nil, err
	}
	cfg.Logging.Dir = strings.TrimSpace(cfg.Logging.Dir)
	if !yamlKeyPresent(raw, "logging", "drop_dedupe_window_seconds") {
		cfg.Logging.DropDedupeWindowSeconds = 120
	}
	if cfg.Logging.DropDedupeWindowSeconds < 0 {
		return nil, fmt.Errorf("invalid logging.drop_dedupe_window_seconds %d (must be >= 0)", cfg.Logging.DropDedupeWindowSeconds)
	}
	if cfg.Logging.Enabled {
		if cfg.Logging.Dir == "" {
			cfg.Logging.Dir = "data/logs"
		}
		if cfg.Logging.RetentionDays <= 0 {
			cfg.Logging.RetentionDays = 7
		}
	} else if cfg.Logging.RetentionDays < 0 {
		cfg.Logging.RetentionDays = 0
	}
	if strings.TrimSpace(cfg.PropReport.RefreshUTC) == "" {
		cfg.PropReport.RefreshUTC = "00:05"
	}
	if _, err := time.Parse("15:04", cfg.PropReport.RefreshUTC); err != nil {
		return nil, fmt.Errorf("invalid prop report refresh time %q: %w", cfg.PropReport.RefreshUTC, err)
	}

	// RBN ingest buffers should be sized to absorb decode bursts; fall back to
	// generous defaults when omitted.
	if cfg.RBN.SlotBuffer <= 0 {
		cfg.RBN.SlotBuffer = 4000
	}
	if cfg.RBNDigital.SlotBuffer <= 0 {
		cfg.RBNDigital.SlotBuffer = cfg.RBN.SlotBuffer
	}
	if cfg.HumanTelnet.SlotBuffer <= 0 {
		cfg.HumanTelnet.SlotBuffer = 1000
	}
	if cfg.RBN.KeepaliveSec < 0 {
		cfg.RBN.KeepaliveSec = 0
	}
	if cfg.RBNDigital.KeepaliveSec < 0 {
		cfg.RBNDigital.KeepaliveSec = 0
	}
	if cfg.HumanTelnet.KeepaliveSec < 0 {
		cfg.HumanTelnet.KeepaliveSec = 0
	}
	if cfg.RBN.KeepaliveSec == 0 {
		cfg.RBN.KeepaliveSec = 240
	}
	if cfg.RBNDigital.KeepaliveSec == 0 {
		cfg.RBNDigital.KeepaliveSec = cfg.RBN.KeepaliveSec
	}
	if cfg.HumanTelnet.KeepaliveSec == 0 {
		cfg.HumanTelnet.KeepaliveSec = 240
	}

	if cfg.PSKReporter.Workers < 0 {
		cfg.PSKReporter.Workers = 0
	}
	if cfg.PSKReporter.MQTTInboundWorkers < 0 {
		cfg.PSKReporter.MQTTInboundWorkers = 0
	}
	if cfg.PSKReporter.MQTTInboundQueueDepth < 0 {
		cfg.PSKReporter.MQTTInboundQueueDepth = 0
	}
	if cfg.PSKReporter.MQTTQoS12EnqueueTimeoutMS < 0 {
		cfg.PSKReporter.MQTTQoS12EnqueueTimeoutMS = 0
	}
	if cfg.PSKReporter.MQTTQoS12EnqueueTimeoutMS == 0 && !hasPSKRMQTTTimeout {
		cfg.PSKReporter.MQTTQoS12EnqueueTimeoutMS = defaultPSKReporterQoS12EnqueueTimeoutMS
	}
	if cfg.PSKReporter.SpotChannelSize <= 0 {
		cfg.PSKReporter.SpotChannelSize = 25000
	}
	if cfg.PSKReporter.CTYCacheSize <= 0 {
		cfg.PSKReporter.CTYCacheSize = 50000
	}
	if cfg.PSKReporter.CTYCacheTTLSeconds <= 0 {
		cfg.PSKReporter.CTYCacheTTLSeconds = 300
	}
	if cfg.PSKReporter.MaxPayloadBytes <= 0 {
		cfg.PSKReporter.MaxPayloadBytes = 4096
	}

	// Archive defaults keep the writer lightweight and non-blocking.
	if cfg.Archive.QueueSize <= 0 {
		cfg.Archive.QueueSize = 10000
	}
	if cfg.Archive.BatchSize <= 0 {
		cfg.Archive.BatchSize = 500
	}
	if cfg.Archive.BatchIntervalMS <= 0 {
		cfg.Archive.BatchIntervalMS = 200
	}
	if cfg.Archive.CleanupIntervalSeconds <= 0 {
		cfg.Archive.CleanupIntervalSeconds = 3600 // hourly
	}
	if cfg.Archive.CleanupBatchSize <= 0 {
		cfg.Archive.CleanupBatchSize = 2000
	}
	if cfg.Archive.CleanupBatchYieldMS < 0 {
		cfg.Archive.CleanupBatchYieldMS = 0
	}
	if cfg.Archive.CleanupBatchYieldMS == 0 && !hasArchiveCleanupYield {
		cfg.Archive.CleanupBatchYieldMS = 5
	}
	if cfg.Archive.RetentionFTSeconds <= 0 {
		cfg.Archive.RetentionFTSeconds = 3600 // 1 hour by default for FT modes
	}
	if cfg.Archive.RetentionDefaultSeconds <= 0 {
		cfg.Archive.RetentionDefaultSeconds = 86400 // 1 day for other modes
	}
	if strings.TrimSpace(cfg.Archive.DBPath) == "" {
		cfg.Archive.DBPath = "data/archive/pebble"
	}
	if cfg.Archive.BusyTimeoutMS <= 0 {
		cfg.Archive.BusyTimeoutMS = 1000
	}
	if cfg.Archive.PreflightTimeoutMS <= 0 {
		cfg.Archive.PreflightTimeoutMS = 2000
	}
	syncMode := strings.ToLower(strings.TrimSpace(cfg.Archive.Synchronous))
	if syncMode == "" {
		syncMode = "off"
	}
	switch syncMode {
	case "off", "normal", "full", "extra":
		cfg.Archive.Synchronous = syncMode
	default:
		return nil, fmt.Errorf("invalid archive.synchronous %q: must be off, normal, full, or extra", cfg.Archive.Synchronous)
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
		cfg.CallCorrection.FrequencyToleranceHz = 500
	}
	if cfg.CallCorrection.VoiceFrequencyToleranceHz <= 0 {
		cfg.CallCorrection.VoiceFrequencyToleranceHz = 2000
	}
	if cfg.CallCorrection.FreqGuardMinSeparationKHz <= 0 {
		cfg.CallCorrection.FreqGuardMinSeparationKHz = 0.1
	}
	if cfg.CallCorrection.FreqGuardRunnerUpRatio <= 0 {
		cfg.CallCorrection.FreqGuardRunnerUpRatio = 0.5
	}
	if cfg.CallCorrection.VoiceCandidateWindowKHz <= 0 {
		cfg.CallCorrection.VoiceCandidateWindowKHz = 2
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
	if cfg.CallCorrection.CallQualityTTLSeconds <= 0 {
		cfg.CallCorrection.CallQualityTTLSeconds = 86400
	}
	if cfg.CallCorrection.CallQualityMaxEntries <= 0 {
		cfg.CallCorrection.CallQualityMaxEntries = 200000
	}
	if cfg.CallCorrection.CallQualityCleanupIntervalSeconds <= 0 {
		cfg.CallCorrection.CallQualityCleanupIntervalSeconds = 600
	}
	if cfg.CallCorrection.CallQualityPinPriors == nil {
		v := true
		cfg.CallCorrection.CallQualityPinPriors = &v
	}
	if strings.TrimSpace(cfg.CallCorrection.Strategy) == "" {
		cfg.CallCorrection.Strategy = "majority"
	} else {
		strategy := strings.ToLower(strings.TrimSpace(cfg.CallCorrection.Strategy))
		switch strategy {
		case "center", "classic", "majority":
			cfg.CallCorrection.Strategy = strategy
		default:
			cfg.CallCorrection.Strategy = "majority"
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
	if cfg.CallCorrection.MinSNRVoice < 0 {
		cfg.CallCorrection.MinSNRVoice = 0
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
	if !cfg.CallCorrection.AdaptiveMinReports.Enabled && !hasAdaptiveMinReportsEnabled {
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
	if cfg.CallCorrection.CooldownBinHz <= 0 {
		cfg.CallCorrection.CooldownBinHz = cfg.CallCorrection.QualityBinHz
		if cfg.CallCorrection.CooldownBinHz <= 0 {
			cfg.CallCorrection.CooldownBinHz = 1000
		}
	}
	if cfg.CallCorrection.CooldownMinReporters <= 0 {
		cfg.CallCorrection.CooldownMinReporters = 3
	}
	if cfg.CallCorrection.CooldownDurationSeconds <= 0 {
		cfg.CallCorrection.CooldownDurationSeconds = 180
	}
	if cfg.CallCorrection.CooldownTTLSeconds <= 0 {
		ttl := cfg.CallCorrection.RecencySeconds * 2
		if ttl <= 0 {
			ttl = 120
		}
		cfg.CallCorrection.CooldownTTLSeconds = ttl
	}
	if cfg.CallCorrection.CooldownMaxReporters <= 0 {
		cfg.CallCorrection.CooldownMaxReporters = 16
	}
	if cfg.CallCorrection.CooldownBypassAdvantage < 0 {
		cfg.CallCorrection.CooldownBypassAdvantage = 0
	}
	if cfg.CallCorrection.CooldownBypassConfidence < 0 {
		cfg.CallCorrection.CooldownBypassConfidence = 0
	}
	// Provide protective defaults when cooldown is enabled but bypass knobs are unset.
	if cfg.CallCorrection.CooldownEnabled {
		if cfg.CallCorrection.CooldownBypassAdvantage == 0 {
			cfg.CallCorrection.CooldownBypassAdvantage = 2
		}
		if cfg.CallCorrection.CooldownBypassConfidence == 0 {
			cfg.CallCorrection.CooldownBypassConfidence = 10
		}
	}
	cfg.CallCorrection.BandStateOverrides = normalizeBandStateOverrides(
		cfg.CallCorrection.BandStateOverrides,
		cfg.CallCorrection.QualityBinHz,
		cfg.CallCorrection.FrequencyToleranceHz,
	)
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
	if cfg.Telnet.ControlQueueSize <= 0 {
		cfg.Telnet.ControlQueueSize = 32
	}
	if cfg.Telnet.BroadcastBatchIntervalMS <= 0 {
		cfg.Telnet.BroadcastBatchIntervalMS = 250
	}
	if cfg.Telnet.KeepaliveSeconds < 0 {
		cfg.Telnet.KeepaliveSeconds = 0
	}
	if cfg.Telnet.ReadIdleTimeoutSeconds <= 0 {
		cfg.Telnet.ReadIdleTimeoutSeconds = 24 * 60 * 60
	}
	if cfg.Telnet.LoginTimeoutSeconds <= 0 {
		cfg.Telnet.LoginTimeoutSeconds = 120
	}
	if cfg.Telnet.LoginLineLimit <= 0 {
		cfg.Telnet.LoginLineLimit = 32
	}
	if cfg.Telnet.CommandLineLimit <= 0 {
		cfg.Telnet.CommandLineLimit = 128
	}
	if cfg.Telnet.OutputLineLength <= 0 {
		cfg.Telnet.OutputLineLength = 78
	}
	if cfg.Telnet.DropExtremeRate <= 0 {
		cfg.Telnet.DropExtremeRate = 0.80
	}
	if cfg.Telnet.DropExtremeRate > 1 {
		return nil, fmt.Errorf("invalid telnet.drop_extreme_rate %.2f (must be 0 < rate <= 1)", cfg.Telnet.DropExtremeRate)
	}
	if cfg.Telnet.DropExtremeWindowSeconds <= 0 {
		cfg.Telnet.DropExtremeWindowSeconds = 30
	}
	if cfg.Telnet.DropExtremeMinAttempts <= 0 {
		cfg.Telnet.DropExtremeMinAttempts = 100
	}
	if strings.TrimSpace(cfg.Telnet.NearbyLoginWarning) == "" {
		cfg.Telnet.NearbyLoginWarning = "NEARBY filter is ON. Disable NEARBY if you want to use regular location filters"
	}
	if cfg.Telnet.OutputLineLength < 65 {
		return nil, fmt.Errorf("invalid telnet.output_line_length %d (minimum 65)", cfg.Telnet.OutputLineLength)
	}
	if transport, ok := normalizeTelnetTransport(cfg.Telnet.Transport); ok {
		cfg.Telnet.Transport = transport
	} else {
		return nil, fmt.Errorf("invalid telnet.transport %q (expected %q or %q)", cfg.Telnet.Transport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if echoMode, ok := normalizeTelnetEchoMode(cfg.Telnet.EchoMode); ok {
		cfg.Telnet.EchoMode = echoMode
	} else {
		return nil, fmt.Errorf("invalid telnet.echo_mode %q (expected %q, %q, or %q)", cfg.Telnet.EchoMode, TelnetEchoServer, TelnetEchoLocal, TelnetEchoOff)
	}
	// Provide operator-facing telnet prompts even when omitted from YAML.
	if transport, ok := normalizeTelnetTransport(cfg.RBN.TelnetTransport); ok {
		cfg.RBN.TelnetTransport = transport
	} else {
		return nil, fmt.Errorf("invalid rbn.telnet_transport %q (expected %q or %q)", cfg.RBN.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if transport, ok := normalizeTelnetTransport(cfg.RBNDigital.TelnetTransport); ok {
		cfg.RBNDigital.TelnetTransport = transport
	} else {
		return nil, fmt.Errorf("invalid rbn_digital.telnet_transport %q (expected %q or %q)", cfg.RBNDigital.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if transport, ok := normalizeTelnetTransport(cfg.HumanTelnet.TelnetTransport); ok {
		cfg.HumanTelnet.TelnetTransport = transport
	} else {
		return nil, fmt.Errorf("invalid human_telnet.telnet_transport %q (expected %q or %q)", cfg.HumanTelnet.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if strings.TrimSpace(cfg.Peering.LocalCallsign) == "" {
		cfg.Peering.LocalCallsign = cfg.Server.NodeID
	}
	if cfg.Peering.ListenPort <= 0 {
		cfg.Peering.ListenPort = 7300
	}
	if cfg.Peering.HopCount <= 0 {
		cfg.Peering.HopCount = 99
	}
	if strings.TrimSpace(cfg.Peering.NodeVersion) == "" {
		cfg.Peering.NodeVersion = "5457"
	}
	if strings.TrimSpace(cfg.Peering.LegacyVersion) == "" {
		cfg.Peering.LegacyVersion = "5401"
	}
	if cfg.Peering.PC92Bitmap <= 0 {
		cfg.Peering.PC92Bitmap = 5
	}
	if cfg.Peering.NodeCount <= 0 {
		cfg.Peering.NodeCount = 1
	}
	if cfg.Peering.UserCount < 0 {
		cfg.Peering.UserCount = 0
	}
	if transport, ok := normalizeTelnetTransport(cfg.Peering.TelnetTransport); ok {
		cfg.Peering.TelnetTransport = transport
	} else {
		return nil, fmt.Errorf("invalid peering.telnet_transport %q (expected %q or %q)", cfg.Peering.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if cfg.Peering.KeepaliveSeconds <= 0 {
		// Default to a short heartbeat to keep remote DXSpider peers from idling us out.
		// Applies to both PC92 (pc9x) and PC51 (legacy) keepalives.
		cfg.Peering.KeepaliveSeconds = 30
	}
	if cfg.Peering.ConfigSeconds <= 0 {
		// Periodic PC92 C "config" refresh; DXSpider peers purge config after missing several periods.
		cfg.Peering.ConfigSeconds = 180
	}
	if cfg.Peering.WriteQueueSize <= 0 {
		cfg.Peering.WriteQueueSize = 256
	}
	if cfg.Peering.MaxLineLength <= 0 {
		cfg.Peering.MaxLineLength = 4096
	}
	if cfg.Peering.PC92MaxBytes <= 0 {
		cfg.Peering.PC92MaxBytes = cfg.Peering.MaxLineLength
		if cfg.Peering.PC92MaxBytes > 16384 {
			cfg.Peering.PC92MaxBytes = 16384
		}
	}
	if cfg.Peering.MaxLineLength > 0 && cfg.Peering.PC92MaxBytes > cfg.Peering.MaxLineLength {
		cfg.Peering.PC92MaxBytes = cfg.Peering.MaxLineLength
	}
	if cfg.Peering.Timeouts.LoginSeconds <= 0 {
		cfg.Peering.Timeouts.LoginSeconds = 15
	}
	if cfg.Peering.Timeouts.InitSeconds <= 0 {
		cfg.Peering.Timeouts.InitSeconds = 60
	}
	if cfg.Peering.Timeouts.IdleSeconds <= 0 {
		cfg.Peering.Timeouts.IdleSeconds = 600
	}
	if cfg.Peering.Backoff.BaseMS <= 0 {
		cfg.Peering.Backoff.BaseMS = 2000
	}
	if cfg.Peering.Backoff.MaxMS <= 0 {
		cfg.Peering.Backoff.MaxMS = 300000
	}
	cfg.Peering.Topology.DBPath = strings.TrimSpace(cfg.Peering.Topology.DBPath)
	if cfg.Peering.Topology.RetentionHours <= 0 {
		cfg.Peering.Topology.RetentionHours = 24
	}
	if cfg.Peering.Topology.PersistIntervalSeconds <= 0 {
		cfg.Peering.Topology.PersistIntervalSeconds = 300
	}
	for i := range cfg.Peering.Peers {
		if cfg.Peering.Peers[i].Host != "" && cfg.Peering.Peers[i].Port > 0 && !cfg.Peering.Peers[i].Enabled {
			cfg.Peering.Peers[i].Enabled = true
		}
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

	// Mode inference defaults keep caches bounded and predictable.
	if cfg.ModeInference.DXFreqCacheTTLSeconds <= 0 {
		cfg.ModeInference.DXFreqCacheTTLSeconds = 300
	}
	if cfg.ModeInference.DXFreqCacheSize <= 0 {
		cfg.ModeInference.DXFreqCacheSize = 50000
	}
	if cfg.ModeInference.DigitalWindowSeconds <= 0 {
		cfg.ModeInference.DigitalWindowSeconds = 300
	}
	if cfg.ModeInference.DigitalMinCorroborators <= 0 {
		cfg.ModeInference.DigitalMinCorroborators = 10
	}
	if cfg.ModeInference.DigitalSeedTTLSeconds <= 0 {
		cfg.ModeInference.DigitalSeedTTLSeconds = 21600
	}
	if cfg.ModeInference.DigitalCacheSize <= 0 {
		cfg.ModeInference.DigitalCacheSize = 5000
	}

	if strings.TrimSpace(cfg.CTY.File) == "" {
		cfg.CTY.File = "data/cty/cty.plist"
	}
	if strings.TrimSpace(cfg.CTY.URL) == "" {
		cfg.CTY.URL = "https://www.country-files.com/cty/cty.plist"
	}
	if cfg.CTY.RefreshUTC == "" {
		cfg.CTY.RefreshUTC = "00:45"
	}
	if !ctyEnabledSet {
		cfg.CTY.Enabled = true
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
	if strings.TrimSpace(cfg.FCCULS.AllowlistPath) == "" {
		cfg.FCCULS.AllowlistPath = "data/fcc/allowlist.txt"
	}
	if strings.TrimSpace(cfg.FCCULS.TempDir) == "" {
		cfg.FCCULS.TempDir = filepath.Dir(cfg.FCCULS.DBPath)
	}
	if strings.TrimSpace(cfg.FCCULS.RefreshUTC) == "" {
		cfg.FCCULS.RefreshUTC = "02:15"
	}
	if cfg.FCCULS.CacheTTLSeconds <= 0 {
		cfg.FCCULS.CacheTTLSeconds = 21600
	}
	if _, err := time.Parse("15:04", cfg.FCCULS.RefreshUTC); err != nil {
		return nil, fmt.Errorf("invalid FCC ULS refresh time %q: %w", cfg.FCCULS.RefreshUTC, err)
	}
	// Grid store defaults keep the local cache warm and bound persistence churn.
	if strings.TrimSpace(cfg.GridDBPath) == "" {
		cfg.GridDBPath = "data/grids/pebble"
	}
	if strings.TrimSpace(cfg.H3TablePath) == "" {
		cfg.H3TablePath = "data/h3"
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
	if cfg.GridBlockCacheMB <= 0 {
		cfg.GridBlockCacheMB = 64
	}
	if cfg.GridBloomFilterBits <= 0 {
		cfg.GridBloomFilterBits = 10
	}
	if cfg.GridMemTableSizeMB <= 0 {
		cfg.GridMemTableSizeMB = 32
	}
	if cfg.GridL0Compaction <= 0 {
		cfg.GridL0Compaction = 4
	}
	if cfg.GridL0StopWrites <= 0 {
		cfg.GridL0StopWrites = 16
	}
	if cfg.GridL0StopWrites <= cfg.GridL0Compaction {
		cfg.GridL0StopWrites = cfg.GridL0Compaction + 4
	}
	if cfg.GridWriteQueueDepth <= 0 {
		cfg.GridWriteQueueDepth = 64
	}
	if cfg.GridDBCheckOnMiss == nil {
		v := true
		cfg.GridDBCheckOnMiss = &v
	}
	if cfg.GridTTLDays < 0 {
		cfg.GridTTLDays = 0
	}
	if cfg.GridPreflightTimeoutMS <= 0 {
		cfg.GridPreflightTimeoutMS = 2000
	}

	// Normalize dedup settings so the window drives behavior.
	if cfg.Dedup.ClusterWindowSeconds < 0 {
		cfg.Dedup.ClusterWindowSeconds = 0
	}
	if cfg.Dedup.SecondaryFastWindowSeconds < 0 {
		cfg.Dedup.SecondaryFastWindowSeconds = 0
	}
	if cfg.Dedup.SecondaryMedWindowSeconds < 0 {
		cfg.Dedup.SecondaryMedWindowSeconds = 0
	}
	if cfg.Dedup.SecondarySlowWindowSeconds < 0 {
		cfg.Dedup.SecondarySlowWindowSeconds = 0
	}
	if !hasSecondaryFastWindow && cfg.Dedup.SecondaryFastWindowSeconds == 0 {
		cfg.Dedup.SecondaryFastWindowSeconds = 120
	}
	if !hasSecondaryMedWindow && cfg.Dedup.SecondaryMedWindowSeconds == 0 {
		cfg.Dedup.SecondaryMedWindowSeconds = 300
	}
	if !hasSecondarySlowWindow && cfg.Dedup.SecondarySlowWindowSeconds == 0 {
		cfg.Dedup.SecondarySlowWindowSeconds = 480
	}
	if !cfg.Dedup.SecondaryFastPreferStrong && !hasSecondaryFastPrefer && cfg.Dedup.PreferStrongerSNR {
		cfg.Dedup.SecondaryFastPreferStrong = cfg.Dedup.PreferStrongerSNR
	}
	if !cfg.Dedup.SecondaryMedPreferStrong && !hasSecondaryMedPrefer && cfg.Dedup.PreferStrongerSNR {
		cfg.Dedup.SecondaryMedPreferStrong = cfg.Dedup.PreferStrongerSNR
	}
	if !cfg.Dedup.SecondarySlowPreferStrong && !hasSecondarySlowPrefer && cfg.Dedup.PreferStrongerSNR {
		cfg.Dedup.SecondarySlowPreferStrong = cfg.Dedup.PreferStrongerSNR
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

	if cfg.Reputation.Enabled {
		if strings.TrimSpace(cfg.Reputation.IPInfoSnapshotPath) == "" {
			cfg.Reputation.IPInfoSnapshotPath = "data/ipinfo/location.csv"
		}
		if strings.TrimSpace(cfg.Reputation.IPInfoAPIBaseURL) == "" {
			cfg.Reputation.IPInfoAPIBaseURL = "https://ipinfo.io"
		}
		if strings.TrimSpace(cfg.Reputation.IPInfoDownloadPath) == "" {
			cfg.Reputation.IPInfoDownloadPath = "data/ipinfo/ipinfo_lite.csv.gz"
		}
		if strings.TrimSpace(cfg.Reputation.IPInfoDownloadURL) == "" {
			cfg.Reputation.IPInfoDownloadURL = "https://ipinfo.io/data/ipinfo_lite.csv.gz?token=$TOKEN"
		}
		if strings.TrimSpace(cfg.Reputation.IPInfoDownloadToken) == "" {
			cfg.Reputation.IPInfoDownloadToken = "8a74cd36c1905b"
		}
		if strings.TrimSpace(cfg.Reputation.IPInfoRefreshUTC) == "" {
			cfg.Reputation.IPInfoRefreshUTC = "03:00"
		}
		if cfg.Reputation.IPInfoDownloadTimeoutMS <= 0 {
			cfg.Reputation.IPInfoDownloadTimeoutMS = 15000
		}
		if cfg.Reputation.IPInfoImportTimeoutMS <= 0 {
			cfg.Reputation.IPInfoImportTimeoutMS = 600000
		}
		if cfg.Reputation.SnapshotMaxAgeSeconds <= 0 {
			cfg.Reputation.SnapshotMaxAgeSeconds = 26 * 3600
		}
		if strings.TrimSpace(cfg.Reputation.IPInfoPebblePath) == "" {
			cfg.Reputation.IPInfoPebblePath = "data/ipinfo/pebble"
		}
		if cfg.Reputation.IPInfoPebbleCacheMB <= 0 {
			cfg.Reputation.IPInfoPebbleCacheMB = 64
		}
		if !yamlKeyPresent(raw, "reputation", "ipinfo_pebble_load_ipv4") {
			cfg.Reputation.IPInfoPebbleLoadIPv4 = true
		}
		if !yamlKeyPresent(raw, "reputation", "ipinfo_delete_csv_after_import") {
			cfg.Reputation.IPInfoDeleteCSVAfterImport = true
		}
		if !yamlKeyPresent(raw, "reputation", "ipinfo_keep_gzip") {
			cfg.Reputation.IPInfoKeepGzip = true
		}
		if !yamlKeyPresent(raw, "reputation", "ipinfo_pebble_cleanup") {
			cfg.Reputation.IPInfoPebbleCleanup = true
		}
		if !yamlKeyPresent(raw, "reputation", "ipinfo_pebble_compact") {
			cfg.Reputation.IPInfoPebbleCompact = true
		}
		if cfg.Reputation.IPInfoAPITimeoutMS <= 0 {
			cfg.Reputation.IPInfoAPITimeoutMS = 250
		}
		if cfg.Reputation.CymruLookupTimeoutMS <= 0 {
			cfg.Reputation.CymruLookupTimeoutMS = 250
		}
		if cfg.Reputation.CymruCacheTTLSeconds <= 0 {
			cfg.Reputation.CymruCacheTTLSeconds = 86400
		}
		if cfg.Reputation.CymruNegativeTTLSeconds <= 0 {
			cfg.Reputation.CymruNegativeTTLSeconds = 300
		}
		if cfg.Reputation.CymruWorkers <= 0 {
			cfg.Reputation.CymruWorkers = 2
		}
		if cfg.Reputation.InitialWaitSeconds <= 0 {
			cfg.Reputation.InitialWaitSeconds = 60
		}
		if cfg.Reputation.RampWindowSeconds <= 0 {
			cfg.Reputation.RampWindowSeconds = 60
		}
		if cfg.Reputation.PerBandStart <= 0 {
			cfg.Reputation.PerBandStart = 1
		}
		if cfg.Reputation.PerBandCap <= 0 {
			cfg.Reputation.PerBandCap = 5
		}
		if cfg.Reputation.TotalCapStart <= 0 {
			cfg.Reputation.TotalCapStart = 5
		}
		if cfg.Reputation.TotalCapPostRamp <= 0 {
			cfg.Reputation.TotalCapPostRamp = 10
		}
		if cfg.Reputation.TotalCapRampDelaySeconds < 0 {
			cfg.Reputation.TotalCapRampDelaySeconds = 0
		}
		if cfg.Reputation.CountryMismatchExtraWaitSeconds <= 0 {
			cfg.Reputation.CountryMismatchExtraWaitSeconds = 60
		}
		if cfg.Reputation.DisagreementPenaltySeconds <= 0 {
			cfg.Reputation.DisagreementPenaltySeconds = 60
		}
		if cfg.Reputation.UnknownPenaltySeconds <= 0 {
			cfg.Reputation.UnknownPenaltySeconds = 60
		}
		if !cfg.Reputation.ResetOnNewASN {
			cfg.Reputation.ResetOnNewASN = true
		}
		if !cfg.Reputation.DisagreementResetOnNew {
			cfg.Reputation.DisagreementResetOnNew = true
		}
		if strings.TrimSpace(cfg.Reputation.CountryFlipScope) == "" {
			cfg.Reputation.CountryFlipScope = "country"
		}
		if cfg.Reputation.MaxASNHistory <= 0 {
			cfg.Reputation.MaxASNHistory = 5
		}
		if cfg.Reputation.MaxCountryHistory <= 0 {
			cfg.Reputation.MaxCountryHistory = 5
		}
		if cfg.Reputation.StateTTLSeconds <= 0 {
			cfg.Reputation.StateTTLSeconds = 7200
		}
		if cfg.Reputation.StateMaxEntries <= 0 {
			cfg.Reputation.StateMaxEntries = 100000
		}
		if cfg.Reputation.PrefixTTLSeconds <= 0 {
			cfg.Reputation.PrefixTTLSeconds = 3600
		}
		if cfg.Reputation.PrefixMaxEntries <= 0 {
			cfg.Reputation.PrefixMaxEntries = 200000
		}
		if cfg.Reputation.LookupCacheTTLSeconds <= 0 {
			cfg.Reputation.LookupCacheTTLSeconds = 86400
		}
		if cfg.Reputation.LookupCacheMaxEntries <= 0 {
			cfg.Reputation.LookupCacheMaxEntries = 200000
		}
		if cfg.Reputation.IPv4BucketSize <= 0 {
			cfg.Reputation.IPv4BucketSize = 64
		}
		if cfg.Reputation.IPv4BucketRefillPerSec <= 0 {
			cfg.Reputation.IPv4BucketRefillPerSec = 8
		}
		if cfg.Reputation.IPv6BucketSize <= 0 {
			cfg.Reputation.IPv6BucketSize = 32
		}
		if cfg.Reputation.IPv6BucketRefillPerSec <= 0 {
			cfg.Reputation.IPv6BucketRefillPerSec = 4
		}
		if cfg.Reputation.DropLogSampleRate <= 0 {
			cfg.Reputation.DropLogSampleRate = 1
		} else if cfg.Reputation.DropLogSampleRate > 1 {
			cfg.Reputation.DropLogSampleRate = 1
		}
		if strings.TrimSpace(cfg.Reputation.ReputationDir) == "" {
			cfg.Reputation.ReputationDir = "data/reputation"
		}
	}
	return &cfg, nil
}

// Purpose: Load and merge all YAML files from a config directory.
// Key aspects: Sorted file order provides deterministic overrides.
// Upstream: Load.
// Downstream: yaml.Unmarshal, mergeYAMLMaps.
func loadConfigDir(path string) (map[string]any, []string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read config directory %q: %w", path, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		files = append(files, filepath.Join(path, entry.Name()))
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, nil, fmt.Errorf("no YAML files found in config directory %q", path)
	}

	merged := make(map[string]any)
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read config file %q: %w", file, err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal(data, &doc); err != nil {
			return nil, nil, fmt.Errorf("failed to parse config file %q: %w", file, err)
		}
		merged = mergeYAMLMaps(merged, doc)
	}
	return merged, files, nil
}

// Purpose: Deep-merge nested YAML maps.
// Key aspects: Recurses into sub-maps; src overwrites non-map values.
// Upstream: loadConfigDir.
// Downstream: None.
func mergeYAMLMaps(dst, src map[string]any) map[string]any {
	if dst == nil {
		dst = make(map[string]any)
	}
	for key, val := range src {
		if existing, ok := dst[key]; ok {
			existingMap, okExisting := existing.(map[string]any)
			incomingMap, okIncoming := val.(map[string]any)
			if okExisting && okIncoming {
				dst[key] = mergeYAMLMaps(existingMap, incomingMap)
				continue
			}
		}
		dst[key] = val
	}
	return dst
}

// Purpose: Print a human-readable configuration summary.
// Key aspects: Focuses on operationally relevant fields.
// Upstream: main.go startup logging.
// Downstream: fmt.Printf.
func (c *Config) Print() {
	fmt.Printf("Server: %s (%s)\n", c.Server.Name, c.Server.NodeID)
	workerDesc := "auto"
	if c.Telnet.BroadcastWorkers > 0 {
		workerDesc = fmt.Sprintf("%d", c.Telnet.BroadcastWorkers)
	}
	fmt.Printf("Telnet: port %d (transport=%s echo_mode=%s broadcast workers=%s queue=%d worker_queue=%d client_buffer=%d skip_handshake=%t)\n",
		c.Telnet.Port,
		c.Telnet.Transport,
		c.Telnet.EchoMode,
		workerDesc,
		c.Telnet.BroadcastQueue,
		c.Telnet.WorkerQueue,
		c.Telnet.ClientBuffer,
		c.Telnet.SkipHandshake)
	if c.Reputation.Enabled {
		fmt.Printf("Reputation: enabled (cymru=%t api=%t pebble=%s v4_mem=%t wait=%ds ramp=%ds per_band=%d..%d total=%d..%d prefix4=%d@%d/s prefix6=%d@%d/s)\n",
			c.Reputation.FallbackTeamCymru,
			c.Reputation.IPInfoAPIEnabled,
			c.Reputation.IPInfoPebblePath,
			c.Reputation.IPInfoPebbleLoadIPv4,
			c.Reputation.InitialWaitSeconds,
			c.Reputation.RampWindowSeconds,
			c.Reputation.PerBandStart,
			c.Reputation.PerBandCap,
			c.Reputation.TotalCapStart,
			c.Reputation.TotalCapPostRamp,
			c.Reputation.IPv4BucketSize,
			c.Reputation.IPv4BucketRefillPerSec,
			c.Reputation.IPv6BucketSize,
			c.Reputation.IPv6BucketRefillPerSec)
	}
	fmt.Printf("UI: mode=%s refresh=%dms color=%t clear_screen=%t panes(stats=%d calls=%d unlicensed=%d harm=%d system=%d)\n",
		c.UI.Mode,
		c.UI.RefreshMS,
		c.UI.Color,
		c.UI.ClearScreen,
		c.UI.PaneLines.Stats,
		c.UI.PaneLines.Calls,
		c.UI.PaneLines.Unlicensed,
		c.UI.PaneLines.Harmonics,
		c.UI.PaneLines.System)
	if c.Logging.Enabled {
		fmt.Printf("Logging: enabled (dir=%s retention_days=%d drop_dedupe_window_seconds=%d)\n", c.Logging.Dir, c.Logging.RetentionDays, c.Logging.DropDedupeWindowSeconds)
	} else {
		fmt.Printf("Logging: disabled (drop_dedupe_window_seconds=%d)\n", c.Logging.DropDedupeWindowSeconds)
	}
	if c.PropReport.Enabled {
		fmt.Printf("Prop report: enabled (refresh=%s UTC)\n", c.PropReport.RefreshUTC)
	} else {
		fmt.Printf("Prop report: disabled\n")
	}
	if c.RBN.Enabled {
		fmt.Printf("RBN CW/RTTY: %s:%d (as %s, transport=%s slot_buffer=%d keepalive=%ds)\n",
			c.RBN.Host,
			c.RBN.Port,
			c.RBN.Callsign,
			c.RBN.TelnetTransport,
			c.RBN.SlotBuffer,
			c.RBN.KeepaliveSec)
	}
	if c.RBNDigital.Enabled {
		fmt.Printf("RBN Digital (FT4/FT8): %s:%d (as %s, transport=%s slot_buffer=%d keepalive=%ds)\n",
			c.RBNDigital.Host,
			c.RBNDigital.Port,
			c.RBNDigital.Callsign,
			c.RBNDigital.TelnetTransport,
			c.RBNDigital.SlotBuffer,
			c.RBNDigital.KeepaliveSec)
	}
	if c.HumanTelnet.Enabled {
		fmt.Printf("Human/relay telnet: %s:%d (as %s, transport=%s slot_buffer=%d keepalive=%ds)\n",
			c.HumanTelnet.Host,
			c.HumanTelnet.Port,
			c.HumanTelnet.Callsign,
			c.HumanTelnet.TelnetTransport,
			c.HumanTelnet.SlotBuffer,
			c.HumanTelnet.KeepaliveSec)
	}
	if c.Archive.Enabled {
		fmt.Printf("Archive: %s (queue=%d batch=%d/%dms cleanup=%ds cleanup_batch=%d yield=%dms retain_ft=%ds retain_other=%ds sync=%s auto_delete_corrupt=%t)\n",
			c.Archive.DBPath,
			c.Archive.QueueSize,
			c.Archive.BatchSize,
			c.Archive.BatchIntervalMS,
			c.Archive.CleanupIntervalSeconds,
			c.Archive.CleanupBatchSize,
			c.Archive.CleanupBatchYieldMS,
			c.Archive.RetentionFTSeconds,
			c.Archive.RetentionDefaultSeconds,
			c.Archive.Synchronous,
			c.Archive.AutoDeleteCorruptDB)
	}
	if c.PSKReporter.Enabled {
		workerDesc := "auto"
		if c.PSKReporter.Workers > 0 {
			workerDesc = fmt.Sprintf("%d", c.PSKReporter.Workers)
		}
		mqttWorkerDesc := "auto"
		if c.PSKReporter.MQTTInboundWorkers > 0 {
			mqttWorkerDesc = fmt.Sprintf("%d", c.PSKReporter.MQTTInboundWorkers)
		}
		mqttQueueDesc := "auto"
		if c.PSKReporter.MQTTInboundQueueDepth > 0 {
			mqttQueueDesc = fmt.Sprintf("%d", c.PSKReporter.MQTTInboundQueueDepth)
		}
		pathOnly := "(none)"
		if len(c.PSKReporter.PathOnlyModes) > 0 {
			pathOnly = strings.Join(c.PSKReporter.PathOnlyModes, ",")
		}
		fmt.Printf("PSKReporter: %s:%d (topic: %s buffer=%d workers=%s mqtt_inbound_workers=%s mqtt_inbound_queue=%s qos12_timeout=%dms path_only_modes=%s)\n",
			c.PSKReporter.Broker,
			c.PSKReporter.Port,
			c.PSKReporter.Topic,
			c.PSKReporter.SpotChannelSize,
			workerDesc,
			mqttWorkerDesc,
			mqttQueueDesc,
			c.PSKReporter.MQTTQoS12EnqueueTimeoutMS,
			pathOnly)
	}
	clusterWindow := "disabled"
	if c.Dedup.ClusterWindowSeconds > 0 {
		clusterWindow = fmt.Sprintf("%ds", c.Dedup.ClusterWindowSeconds)
	}
	secondaryFast := "disabled"
	if c.Dedup.SecondaryFastWindowSeconds > 0 {
		secondaryFast = fmt.Sprintf("%ds", c.Dedup.SecondaryFastWindowSeconds)
	}
	secondaryMed := "disabled"
	if c.Dedup.SecondaryMedWindowSeconds > 0 {
		secondaryMed = fmt.Sprintf("%ds", c.Dedup.SecondaryMedWindowSeconds)
	}
	secondarySlow := "disabled"
	if c.Dedup.SecondarySlowWindowSeconds > 0 {
		secondarySlow = fmt.Sprintf("%ds", c.Dedup.SecondarySlowWindowSeconds)
	}
	fmt.Printf("Dedup: cluster=%s (prefer_stronger=%t) secondary_fast=%s (prefer_stronger=%t) secondary_med=%s (prefer_stronger=%t) secondary_slow=%s (prefer_stronger=%t)\n",
		clusterWindow,
		c.Dedup.PreferStrongerSNR,
		secondaryFast,
		c.Dedup.SecondaryFastPreferStrong,
		secondaryMed,
		c.Dedup.SecondaryMedPreferStrong,
		secondarySlow,
		c.Dedup.SecondarySlowPreferStrong)
	if len(c.Filter.DefaultModes) > 0 {
		fmt.Printf("Default modes: %s\n", strings.Join(c.Filter.DefaultModes, ", "))
	}
	if len(c.Filter.DefaultSources) > 0 {
		fmt.Printf("Default sources: %s\n", strings.Join(c.Filter.DefaultSources, ", "))
	}
	if c.Peering.Enabled {
		fmt.Printf("Peering: listen_port=%d peers=%d hop=%d transport=%s keepalive=%ds config=%ds topology=%s retention=%dh\n",
			c.Peering.ListenPort,
			len(c.Peering.Peers),
			c.Peering.HopCount,
			c.Peering.TelnetTransport,
			c.Peering.KeepaliveSeconds,
			c.Peering.ConfigSeconds,
			c.Peering.Topology.DBPath,
			c.Peering.Topology.RetentionHours)
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
	fmt.Printf("Call correction voice: tol=%.0fHz search=%.1fkHz min_snr=%d\n",
		c.CallCorrection.VoiceFrequencyToleranceHz,
		c.CallCorrection.VoiceCandidateWindowKHz,
		c.CallCorrection.MinSNRVoice)
	pinPriors := true
	if c.CallCorrection.CallQualityPinPriors != nil {
		pinPriors = *c.CallCorrection.CallQualityPinPriors
	}
	fmt.Printf("Call correction quality: ttl=%ds max_entries=%d cleanup=%ds pin_priors=%t\n",
		c.CallCorrection.CallQualityTTLSeconds,
		c.CallCorrection.CallQualityMaxEntries,
		c.CallCorrection.CallQualityCleanupIntervalSeconds,
		pinPriors)

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
	if c.CTY.Enabled && c.CTY.URL != "" {
		fmt.Printf("CTY refresh: %s UTC (source=%s)\n", c.CTY.RefreshUTC, c.CTY.URL)
	}
	if c.KnownCalls.Enabled && c.KnownCalls.URL != "" {
		fmt.Printf("Known calls refresh: %s UTC (source=%s)\n", c.KnownCalls.RefreshUTC, c.KnownCalls.URL)
	}
	if c.FCCULS.Enabled && c.FCCULS.URL != "" {
		fmt.Printf("FCC ULS: refresh %s UTC (source=%s archive=%s db=%s allowlist=%s cache_ttl=%ds)\n",
			c.FCCULS.RefreshUTC,
			c.FCCULS.URL,
			c.FCCULS.Archive,
			c.FCCULS.DBPath,
			c.FCCULS.AllowlistPath,
			c.FCCULS.CacheTTLSeconds)
	}
	if strings.TrimSpace(c.GridDBPath) != "" {
		dbCheckOnMiss := true
		if c.GridDBCheckOnMiss != nil {
			dbCheckOnMiss = *c.GridDBCheckOnMiss
		}
		fmt.Printf("Grid/known DB: %s (flush=%ds cache=%d cache_ttl=%ds db_check_on_miss=%t ttl=%dd block_cache=%dMB bloom_bits=%d memtable=%dMB l0_compact=%d l0_stop=%d write_queue=%d)\n",
			c.GridDBPath,
			c.GridFlushSec,
			c.GridCacheSize,
			c.GridCacheTTLSec,
			dbCheckOnMiss,
			c.GridTTLDays,
			c.GridBlockCacheMB,
			c.GridBloomFilterBits,
			c.GridMemTableSizeMB,
			c.GridL0Compaction,
			c.GridL0StopWrites,
			c.GridWriteQueueDepth)
	}
	if c.Skew.Enabled {
		fmt.Printf("Skew: refresh %s UTC (min_spots=%d source=%s)\n", c.Skew.RefreshUTC, c.Skew.MinSpots, c.Skew.URL)
	}
	fmt.Printf("Ring buffer capacity: %d spots\n", c.Buffer.Capacity)
}

// Purpose: Normalize per-band-state overrides for correction settings.
// Key aspects: Applies default bins/tolerances when overrides are missing.
// Upstream: Load config normalization.
// Downstream: None.
func normalizeBandStateOverrides(overrides []BandStateOverride, defaultBin int, defaultTol float64) []BandStateOverride {
	if len(overrides) == 0 {
		return overrides
	}
	out := make([]BandStateOverride, 0, len(overrides))
	for _, o := range overrides {
		normalized := o
		if normalized.Quiet.QualityBinHz <= 0 {
			normalized.Quiet.QualityBinHz = defaultBin
		}
		if normalized.Normal.QualityBinHz <= 0 {
			normalized.Normal.QualityBinHz = defaultBin
		}
		if normalized.Busy.QualityBinHz <= 0 {
			normalized.Busy.QualityBinHz = defaultBin
		}
		if normalized.Quiet.FrequencyToleranceHz <= 0 {
			normalized.Quiet.FrequencyToleranceHz = defaultTol
		}
		if normalized.Normal.FrequencyToleranceHz <= 0 {
			normalized.Normal.FrequencyToleranceHz = defaultTol
		}
		if normalized.Busy.FrequencyToleranceHz <= 0 {
			normalized.Busy.FrequencyToleranceHz = defaultTol
		}
		out = append(out, normalized)
	}
	return out
}

// Purpose: Check whether a nested YAML key path exists.
// Key aspects: Walks decoded maps using YAML field names.
// Upstream: Load config normalization (detecting explicit settings).
// Downstream: None.
func yamlKeyPresent(raw map[string]any, path ...string) bool {
	if len(path) == 0 || raw == nil {
		return false
	}
	current := raw
	for i, key := range path {
		val, ok := current[key]
		if !ok {
			return false
		}
		if i == len(path)-1 {
			return true
		}
		next, ok := val.(map[string]any)
		if !ok {
			return false
		}
		current = next
	}
	return false
}
