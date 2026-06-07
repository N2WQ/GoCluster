// Package config loads the cluster's YAML configuration, validates required
// settings, and exposes a strongly typed struct other packages rely on at
// startup.
package config

import (
	"bytes"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dxcluster/internal/yamlconfig"
	"dxcluster/pathreliability"
	"dxcluster/solarweather"
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
	// TelnetHandshakeFull preserves the historical full telnet option negotiation.
	TelnetHandshakeFull = "full"
	// TelnetHandshakeMinimal emits a reduced IAC prelude intended to avoid clients
	// that render raw DO/DONT bytes while still advertising the core options.
	TelnetHandshakeMinimal = "minimal"
	// TelnetHandshakeNone disables all telnet option negotiation.
	TelnetHandshakeNone = "none"
	// PeeringPeerFamilyDXSpider is the default DXSpider-compatible peer family.
	PeeringPeerFamilyDXSpider = "dxspider"
	// PeeringPeerFamilyCCluster marks a CC Cluster peer.
	PeeringPeerFamilyCCluster = "ccluster"
	// PeeringPeerDirectionOutbound dials the remote peer only.
	PeeringPeerDirectionOutbound = "outbound"
	// PeeringPeerDirectionInbound waits for the remote peer to connect.
	PeeringPeerDirectionInbound = "inbound"
	// PeeringPeerDirectionBoth permits inbound and outbound connections for a peer.
	PeeringPeerDirectionBoth = "both"
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

// TelnetHandshakeMode controls the raw IAC prelude sent to inbound telnet
// clients. The YAML key remains `skip_handshake` for backward compatibility:
// legacy booleans still load and map to explicit modes during normalization.
type TelnetHandshakeMode string

func (m *TelnetHandshakeMode) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		*m = ""
		return nil
	}
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("telnet.skip_handshake must be a scalar")
	}
	*m = TelnetHandshakeMode(strutil.NormalizeLower(strings.TrimSpace(node.Value)))
	return nil
}

// Purpose: Normalize and validate the inbound telnet handshake mode.
// Key aspects: Defaults omitted values to minimal; accepts legacy bool strings.
// Upstream: Load config normalization.
// Downstream: telnet server option wiring and startup logging.
func normalizeTelnetHandshakeMode(value TelnetHandshakeMode) (TelnetHandshakeMode, bool) {
	trimmed := strutil.NormalizeLower(string(value))
	switch trimmed {
	case "":
		return TelnetHandshakeMode(TelnetHandshakeMinimal), true
	case TelnetHandshakeFull, "false":
		return TelnetHandshakeMode(TelnetHandshakeFull), true
	case TelnetHandshakeMinimal:
		return TelnetHandshakeMode(TelnetHandshakeMinimal), true
	case TelnetHandshakeNone, "true":
		return TelnetHandshakeMode(TelnetHandshakeNone), true
	default:
		return "", false
	}
}

// Config represents the complete startup configuration built from data/config.
// Merged runtime files populate the main YAML tree; feature-root files populate
// typed fields below without changing their file-local YAML shapes.
type Config struct {
	Server              ServerConfig         `yaml:"server"`
	Telnet              TelnetConfig         `yaml:"telnet"`
	WhoSpotsMe          WhoSpotsMeConfig     `yaml:"who_spots_me"`
	UI                  UIConfig             `yaml:"ui"`
	GoRuntime           GoRuntimeConfig      `yaml:"go_runtime"`
	Logging             LoggingConfig        `yaml:"logging"`
	PropReport          PropReportConfig     `yaml:"prop_report"`
	RBN                 RBNConfig            `yaml:"rbn"`
	RBNDigital          RBNConfig            `yaml:"rbn_digital"`
	HumanTelnet         RBNConfig            `yaml:"human_telnet"`
	PSKReporter         PSKReporterConfig    `yaml:"pskreporter"`
	DXSummit            DXSummitConfig       `yaml:"dxsummit"`
	Archive             ArchiveConfig        `yaml:"archive"`
	Dedup               DedupConfig          `yaml:"dedup"`
	FloodControl        FloodControlConfig   `yaml:"flood_control"`
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
	GridPreflightTimeoutMS int                    `yaml:"grid_preflight_timeout_ms"`
	PathReliability        pathreliability.Config `yaml:"-"`
	SolarWeather           solarweather.Config    `yaml:"-"`
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
	// MaxPreloginSessions bounds unauthenticated sessions so socket floods do not
	// consume unbounded resources before callsign login completes.
	MaxPreloginSessions int `yaml:"max_prelogin_sessions"`
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
	ControlQueueSize int `yaml:"control_queue_size"`
	// BulletinDedupeWindowSeconds suppresses repeated WWV/WCY/announcement lines across all bulletin sources.
	// Set to 0 to disable bulletin dedupe.
	BulletinDedupeWindowSeconds int `yaml:"bulletin_dedupe_window_seconds"`
	// BulletinDedupeMaxEntries bounds retained bulletin dedupe keys when bulletin dedupe is enabled.
	BulletinDedupeMaxEntries int `yaml:"bulletin_dedupe_max_entries"`
	// SkipHandshake retains the historical YAML key name while now accepting
	// `full`, `minimal`, or `none`. Legacy booleans still map to `full`/`none`.
	SkipHandshake TelnetHandshakeMode `yaml:"skip_handshake"`
	// RejectWorkers configures asynchronous pre-auth reject writers.
	RejectWorkers int `yaml:"reject_workers"`
	// RejectQueueSize bounds queued reject jobs so accept never blocks on reject I/O.
	RejectQueueSize int `yaml:"reject_queue_size"`
	// RejectWriteDeadlineMS caps how long reject-banner writes may block.
	RejectWriteDeadlineMS int `yaml:"reject_write_deadline_ms"`
	// WriterBatchMaxBytes bounds per-flush payload size for client writer micro-batching.
	WriterBatchMaxBytes int `yaml:"writer_batch_max_bytes"`
	// WriterBatchWaitMS sets max wait before flushing a partial writer batch.
	WriterBatchWaitMS int `yaml:"writer_batch_wait_ms"`
	// BroadcastBatchIntervalMS controls telnet broadcast micro-batching. 0 disables batching.
	BroadcastBatchIntervalMS int `yaml:"broadcast_batch_interval_ms"`
	// KeepaliveSeconds, when >0, emits a periodic CRLF to all connected clients to keep idle
	// network devices from timing out otherwise quiet sessions.
	KeepaliveSeconds int `yaml:"keepalive_seconds"`
	// ReadIdleTimeoutSeconds sets the read deadline for logged-in sessions; timeouts do not disconnect.
	ReadIdleTimeoutSeconds int `yaml:"read_idle_timeout_seconds"`
	// LoginTimeoutSeconds is the legacy pre-login timeout knob kept for
	// compatibility. Tier-A admission uses prelogin_timeout_seconds.
	LoginTimeoutSeconds int `yaml:"login_timeout_seconds"`
	// PreloginTimeoutSeconds caps total time from accept to successful callsign entry.
	// This hard-bounds unauthenticated socket lifetime.
	PreloginTimeoutSeconds int `yaml:"prelogin_timeout_seconds"`
	// AcceptRatePerIP limits accepted pre-login connections per source IP per second.
	AcceptRatePerIP float64 `yaml:"accept_rate_per_ip"`
	// AcceptBurstPerIP sets token bucket burst capacity for pre-login admissions.
	AcceptBurstPerIP int `yaml:"accept_burst_per_ip"`
	// AcceptRatePerSubnet limits accepted pre-login connections per subnet per second.
	AcceptRatePerSubnet float64 `yaml:"accept_rate_per_subnet"`
	// AcceptBurstPerSubnet sets token bucket burst capacity for subnet admissions.
	AcceptBurstPerSubnet int `yaml:"accept_burst_per_subnet"`
	// AcceptRateGlobal limits accepted pre-login connections globally per second.
	AcceptRateGlobal float64 `yaml:"accept_rate_global"`
	// AcceptBurstGlobal sets token bucket burst capacity for global admissions.
	AcceptBurstGlobal int `yaml:"accept_burst_global"`
	// AcceptRatePerASN limits accepted pre-login connections per ASN per second.
	AcceptRatePerASN float64 `yaml:"accept_rate_per_asn"`
	// AcceptBurstPerASN sets token bucket burst capacity per ASN.
	AcceptBurstPerASN int `yaml:"accept_burst_per_asn"`
	// AcceptRatePerCountry limits accepted pre-login connections per country per second.
	AcceptRatePerCountry float64 `yaml:"accept_rate_per_country"`
	// AcceptBurstPerCountry sets token bucket burst capacity per country.
	AcceptBurstPerCountry int `yaml:"accept_burst_per_country"`
	// PreloginConcurrencyPerIP bounds simultaneous unauthenticated sessions per IP.
	PreloginConcurrencyPerIP int `yaml:"prelogin_concurrency_per_ip"`
	// AdmissionLogIntervalSeconds controls aggregated pre-login reject log cadence.
	AdmissionLogIntervalSeconds int `yaml:"admission_log_interval_seconds"`
	// AdmissionLogSampleRate controls sampled per-event reject logs [0,1].
	AdmissionLogSampleRate float64 `yaml:"admission_log_sample_rate"`
	// AdmissionLogMaxReasonLinesPerInterval caps sampled log lines each interval.
	AdmissionLogMaxReasonLinesPerInterval int `yaml:"admission_log_max_reason_lines_per_interval"`
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

// WhoSpotsMeConfig controls the rolling observation window used by the
// WHOSPOTSME telnet command.
type WhoSpotsMeConfig struct {
	WindowMinutes int `yaml:"window_minutes"`
}

// GoRuntimeConfig controls process-wide Go runtime tuning.
type GoRuntimeConfig struct {
	// MemoryLimitMiB mirrors GOMEMLIMIT in MiB. Zero leaves the runtime/env value unchanged.
	MemoryLimitMiB int `yaml:"memory_limit_mib"`
	// GCPercent mirrors GOGC. Zero leaves the runtime/env value unchanged.
	GCPercent int `yaml:"gc_percent"`
	// MaxProcs mirrors GOMAXPROCS. Zero leaves the runtime/env value unchanged.
	MaxProcs int `yaml:"max_procs"`
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
	Enabled                 bool                     `yaml:"enabled"`
	DropDedupeWindowSeconds int                      `yaml:"drop_dedupe_window_seconds"`
	Dir                     string                   `yaml:"dir"`
	RetentionDays           int                      `yaml:"retention_days"`
	DroppedCalls            DroppedCallLoggingConfig `yaml:"dropped_calls"`
	LoginAttempts           EventFileLoggingConfig   `yaml:"login_attempts"`
	ReputationDrops         EventFileLoggingConfig   `yaml:"reputation_drops"`
	TelnetConnections       EventFileLoggingConfig   `yaml:"telnet_connections"`
	IngestConnections       EventFileLoggingConfig   `yaml:"ingest_connections"`
	PeerConnections         EventFileLoggingConfig   `yaml:"peer_connections"`
	Propagation             PropagationLoggingConfig `yaml:"propagation"`
}

// DroppedCallLoggingConfig controls optional per-category dropped-call files.
type DroppedCallLoggingConfig struct {
	Enabled             bool   `yaml:"enabled"`
	Dir                 string `yaml:"dir"`
	RetentionDays       int    `yaml:"retention_days"`
	DedupeWindowSeconds int    `yaml:"dedupe_window_seconds"`
	BadDEDX             bool   `yaml:"bad_de_dx"`
	NoLicense           bool   `yaml:"no_license"`
	Harmonics           bool   `yaml:"harmonics"`
}

// EventFileLoggingConfig controls optional file-only event logs.
type EventFileLoggingConfig struct {
	Enabled             bool   `yaml:"enabled"`
	Dir                 string `yaml:"dir"`
	RetentionDays       int    `yaml:"retention_days"`
	DedupeWindowSeconds int    `yaml:"dedupe_window_seconds"`
}

// PropagationLoggingConfig controls file-only path/propagation aggregate logs.
type PropagationLoggingConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Dir           string `yaml:"dir"`
	RetentionDays int    `yaml:"retention_days"`
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
	SpotChannelSize int `yaml:"spot_channel_size"`
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

// DXSummitConfig contains DXSummit HTTP polling settings.
type DXSummitConfig struct {
	Enabled                bool     `yaml:"enabled"`
	Name                   string   `yaml:"name"`
	BaseURL                string   `yaml:"base_url"`
	PollIntervalSeconds    int      `yaml:"poll_interval_seconds"`
	MaxRecordsPerPoll      int      `yaml:"max_records_per_poll"`
	RequestTimeoutMS       int      `yaml:"request_timeout_ms"`
	LookbackSeconds        int      `yaml:"lookback_seconds"`
	StartupBackfillSeconds int      `yaml:"startup_backfill_seconds"`
	IncludeBands           []string `yaml:"include_bands"`
	SpotChannelSize        int      `yaml:"spot_channel_size"`
	MaxResponseBytes       int64    `yaml:"max_response_bytes"`
}

const (
	defaultPSKReporterTopic                 = "pskr/filter/v2/+/+/#"
	defaultPSKReporterQoS12EnqueueTimeoutMS = 250
	defaultDXSummitName                     = "DXSUMMIT"
	defaultDXSummitBaseURL                  = "http://www.dxsummit.fi/api/v1/spots"
	defaultDXSummitPollIntervalSeconds      = 30
	defaultDXSummitMaxRecordsPerPoll        = 500
	defaultDXSummitRequestTimeoutMS         = 10000
	defaultDXSummitLookbackSeconds          = 300
	defaultDXSummitSpotChannelSize          = 1000
	defaultDXSummitMaxResponseBytes         = 1048576
)

// DefaultArchiveRetentionSeconds is the archive retention fallback for direct
// ArchiveConfig construction outside the required YAML runtime config path.
const DefaultArchiveRetentionSeconds = 86400

// ArchiveConfig controls optional Pebble archival of broadcasted spots.
type ArchiveConfig struct {
	Enabled bool `yaml:"enabled"`
	// DBPath is the Pebble archive directory path.
	DBPath                 string `yaml:"db_path"`
	QueueSize              int    `yaml:"queue_size"`
	BatchSize              int    `yaml:"batch_size"`
	BatchIntervalMS        int    `yaml:"batch_interval_ms"`
	CleanupIntervalSeconds int    `yaml:"cleanup_interval_seconds"`
	// RetentionSeconds applies one archive retention window to every mode.
	RetentionSeconds int `yaml:"retention_seconds"`
	// Synchronous controls archive durability: off disables fsync; normal/full/extra enable sync.
	Synchronous string `yaml:"synchronous"`
	// AutoDeleteCorruptDB removes the archive DB on startup if corruption is detected.
	AutoDeleteCorruptDB bool `yaml:"auto_delete_corrupt_db"`
}

// PeeringConfig controls DXSpider node-to-node peering.
type PeeringConfig struct {
	Enabled bool `yaml:"enabled"`
	// ForwardSpots controls peer data-plane forwarding of spot-bearing frames.
	// Omitted or false disables transit relay; local DX command spots are still published.
	ForwardSpots  bool   `yaml:"forward_spots"`
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
	Enabled        bool     `yaml:"enabled"`
	Host           string   `yaml:"host"`
	Port           int      `yaml:"port"`
	Password       string   `yaml:"password"`
	PreferPC9x     bool     `yaml:"prefer_pc9x"`
	LoginCallsign  string   `yaml:"login_callsign"`
	RemoteCallsign string   `yaml:"remote_callsign"`
	Family         string   `yaml:"family"`
	Direction      string   `yaml:"direction"`
	AllowIPs       []string `yaml:"allow_ips"`
}

func (p PeeringPeer) AllowsOutbound() bool {
	return p.Direction == "" || p.Direction == PeeringPeerDirectionOutbound || p.Direction == PeeringPeerDirectionBoth
}

func (p PeeringPeer) AllowsInbound() bool {
	return p.Direction == PeeringPeerDirectionInbound || p.Direction == PeeringPeerDirectionBoth
}

func normalizePeeringPeerFamily(value string) (string, bool) {
	trimmed := strutil.NormalizeLower(value)
	if trimmed == "" {
		return PeeringPeerFamilyDXSpider, true
	}
	switch trimmed {
	case PeeringPeerFamilyDXSpider, PeeringPeerFamilyCCluster:
		return trimmed, true
	default:
		return "", false
	}
}

func normalizePeeringPeerDirection(value string) (string, bool) {
	trimmed := strutil.NormalizeLower(value)
	if trimmed == "" {
		return PeeringPeerDirectionOutbound, true
	}
	switch trimmed {
	case PeeringPeerDirectionOutbound, PeeringPeerDirectionInbound, PeeringPeerDirectionBoth:
		return trimmed, true
	default:
		return "", false
	}
}

func validatePeeringAllowIPs(path string, entries []string) error {
	for idx, raw := range entries {
		cidr := strings.TrimSpace(raw)
		if cidr == "" {
			continue
		}
		if !strings.Contains(cidr, "/") {
			if strings.Contains(cidr, ":") {
				cidr += "/128"
			} else {
				cidr += "/32"
			}
		}
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("invalid %s[%d]: %w", path, idx, err)
		}
	}
	return nil
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

// SubscriptionTopics builds MQTT subscription topics based on configured modes.
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
	ClusterWindowSeconds       int    `yaml:"cluster_window_seconds"`             // <=0 disables primary dedup
	SecondaryFastWindowSeconds int    `yaml:"secondary_fast_window_seconds"`      // <=0 disables fast secondary dedupe
	SecondaryMedWindowSeconds  int    `yaml:"secondary_med_window_seconds"`       // <=0 disables med secondary dedupe
	SecondarySlowWindowSeconds int    `yaml:"secondary_slow_window_seconds"`      // <=0 disables slow secondary dedupe
	DefaultPolicy              string `yaml:"default_policy"`                     // default telnet policy for new user records
	PreferStrongerSNR          bool   `yaml:"prefer_stronger_snr"`                // keep max SNR when dropping duplicates
	SecondaryFastPreferStrong  bool   `yaml:"secondary_fast_prefer_stronger_snr"` // keep max SNR in fast secondary buckets
	SecondaryMedPreferStrong   bool   `yaml:"secondary_med_prefer_stronger_snr"`  // keep max SNR in med secondary buckets
	SecondarySlowPreferStrong  bool   `yaml:"secondary_slow_prefer_stronger_snr"` // keep max SNR in slow secondary buckets
	OutputBufferSize           int    `yaml:"output_buffer_size"`                 // channel capacity for dedup output
	LegacySecondaryWindow      int    `yaml:"secondary_window_seconds"`           // deprecated: accepted only so Load can warn and ignore it
	LegacySecondaryPreferSNR   bool   `yaml:"secondary_prefer_stronger_snr"`      // deprecated: accepted only so Load can warn and ignore it
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

const (
	DefaultCallCorrectionFTPMinUniqueSpotters = 2
	DefaultCallCorrectionFTVMinUniqueSpotters = 3
	DefaultCallCorrectionFT8QuietGapSeconds   = 6
	DefaultCallCorrectionFT8HardCapSeconds    = 12
	DefaultCallCorrectionFT4QuietGapSeconds   = 5
	DefaultCallCorrectionFT4HardCapSeconds    = 10
	DefaultCallCorrectionFT2QuietGapSeconds   = 3
	DefaultCallCorrectionFT2HardCapSeconds    = 6
)

// CallCorrectionConfig controls resolver-primary DX call correction behavior.
type CallCorrectionConfig struct {
	Enabled bool `yaml:"enabled"`
	// BandStateOverrides allows per-band, per-activity-state tuning of frequency tolerance.
	// Values fall back to the global defaults when omitted.
	BandStateOverrides []BandStateOverride `yaml:"band_state_overrides"`
	// FamilyPolicy groups slash/truncation family precedence behavior and
	// telnet output suppression bounds.
	FamilyPolicy                    CallCorrectionFamilyPolicyConfig `yaml:"family_policy"`
	LegacySlashPrecedenceMinReports int                              `yaml:"slash_precedence_min_reports"` // deprecated: family_policy.slash_precedence_min_reports wins
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
	// the same signal for resolver evidence grouping.
	FrequencyToleranceHz float64 `yaml:"frequency_tolerance_hz"`
	// VoiceFrequencyToleranceHz defines the frequency window for USB/LSB resolver evidence
	// (voice signals are wider than CW/RTTY).
	VoiceFrequencyToleranceHz float64 `yaml:"voice_frequency_tolerance_hz"`
	DistanceModelCW           string  `yaml:"distance_model_cw"`
	DistanceModelRTTY         string  `yaml:"distance_model_rtty"`
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
	// AdaptiveRefreshByBand drives trust refresh cadence from band activity.
	AdaptiveRefreshByBand AdaptiveRefreshByBandConfig `yaml:"adaptive_refresh_by_band"`
	// Optional confusion model (RBN analytics confusion_model.json) used only to
	// rank tied top-support correction candidates. Hard gates remain unchanged.
	ConfusionModelEnabled bool    `yaml:"confusion_model_enabled"`
	ConfusionModelFile    string  `yaml:"confusion_model_file"`
	ConfusionModelWeight  float64 `yaml:"confusion_model_weight"`
	// Optional recent-on-band corroboration store used by resolver gating,
	// confidence floors, and stabilizer rails.
	RecentBandBonusEnabled            bool `yaml:"recent_band_bonus_enabled"`
	RecentBandWindowSeconds           int  `yaml:"recent_band_window_seconds"`
	RecentBandRecordMinUniqueSpotters int  `yaml:"recent_band_record_min_unique_spotters"`
	// FT corroboration uses separate bounded burst clustering in the main output
	// pipeline. These knobs control only FT ?/S/P/V assignment and do not enable
	// resolver mutation for FT modes.
	PMinUniqueSpotters int `yaml:"p_min_unique_spotters"`
	VMinUniqueSpotters int `yaml:"v_min_unique_spotters"`
	FT8QuietGapSeconds int `yaml:"ft8_quiet_gap_seconds"`
	FT8HardCapSeconds  int `yaml:"ft8_hard_cap_seconds"`
	FT4QuietGapSeconds int `yaml:"ft4_quiet_gap_seconds"`
	FT4HardCapSeconds  int `yaml:"ft4_hard_cap_seconds"`
	FT2QuietGapSeconds int `yaml:"ft2_quiet_gap_seconds"`
	FT2HardCapSeconds  int `yaml:"ft2_hard_cap_seconds"`
	// CustomSCP controls the replacement runtime-learned SCP evidence database.
	CustomSCP CallCorrectionCustomSCPConfig `yaml:"custom_scp"`
	// Optional telnet stabilizer: delay spots that have not been heard recently
	// on the same band+mode. This gate is used only for telnet broadcast and
	// does not alter archive/peer output behavior.
	StabilizerEnabled                 bool   `yaml:"stabilizer_enabled"`
	StabilizerDelaySeconds            int    `yaml:"stabilizer_delay_seconds"`
	StabilizerMaxChecks               int    `yaml:"stabilizer_max_checks"`     // includes first delayed check; 1 preserves legacy behavior
	StabilizerTimeoutAction           string `yaml:"stabilizer_timeout_action"` // release | suppress
	StabilizerMaxPending              int    `yaml:"stabilizer_max_pending"`
	StabilizerPDelayConfidencePercent int    `yaml:"stabilizer_p_delay_confidence_percent"` // 0 disables; delay P when confidence is below this percentage
	StabilizerPDelayMaxChecks         int    `yaml:"stabilizer_p_delay_max_checks"`         // includes first delayed check; 0 disables P-delay gate
	StabilizerAmbiguousMaxChecks      int    `yaml:"stabilizer_ambiguous_max_checks"`       // includes first delayed check; 0 falls back to stabilizer_max_checks
	StabilizerEditNeighborEnabled     bool   `yaml:"stabilizer_edit_neighbor_enabled"`      // feature-gated contested edit-neighbor delay policy
	StabilizerEditNeighborMaxChecks   int    `yaml:"stabilizer_edit_neighbor_max_checks"`   // includes first delayed check; 0 falls back to stabilizer_max_checks
	StabilizerEditNeighborMinSpotters int    `yaml:"stabilizer_edit_neighbor_min_spotters"` // minimum unique spotters for edit-neighbor recent support
	// ResolverNeighborhood* enable cross-bucket winner competition to reduce
	// rounded-frequency forking near bucket boundaries.
	ResolverNeighborhoodEnabled      bool `yaml:"resolver_neighborhood_enabled"`
	ResolverNeighborhoodBucketRadius int  `yaml:"resolver_neighborhood_bucket_radius"`
	// ResolverNeighborhoodMaxDistance bounds neighborhood winner comparability
	// for non-family calls (mode-aware distance). Values <=0 default to 1.
	ResolverNeighborhoodMaxDistance int `yaml:"resolver_neighborhood_max_distance"`
	// ResolverNeighborhoodAllowTruncation toggles truncation-family
	// comparability admission in neighborhood arbitration.
	ResolverNeighborhoodAllowTruncation bool `yaml:"resolver_neighborhood_allow_truncation_family"`
	// ResolverRecentPlus1* controls a conservative resolver-primary-only
	// min_reports corroboration rail using recent-on-band support.
	ResolverRecentPlus1Enabled              bool `yaml:"resolver_recent_plus1_enabled"`
	ResolverRecentPlus1MinUniqueWinner      int  `yaml:"resolver_recent_plus1_min_unique_winner"`
	ResolverRecentPlus1RequireSubjectWeaker bool `yaml:"resolver_recent_plus1_require_subject_weaker"`
	ResolverRecentPlus1MaxDistance          int  `yaml:"resolver_recent_plus1_max_distance"`
	ResolverRecentPlus1AllowTruncation      bool `yaml:"resolver_recent_plus1_allow_truncation_family"`
	// BayesBonus applies a conservative, capped prior bonus for distance-1/2
	// resolver-primary near-threshold cases. The rail is disabled by default.
	BayesBonus CallCorrectionBayesBonusConfig `yaml:"bayes_bonus"`
	// TemporalDecoder enables fixed-lag sequence decoding for resolver-primary
	// winner selection, with bounded pending state and deterministic fallback.
	TemporalDecoder CallCorrectionTemporalDecoderConfig `yaml:"temporal_decoder"`
	// Optional spotter reliability weights (0-1). Reporters below MinSpotterReliability are ignored.
	SpotterReliabilityFile     string  `yaml:"spotter_reliability_file"`
	SpotterReliabilityFileCW   string  `yaml:"spotter_reliability_file_cw"`
	SpotterReliabilityFileRTTY string  `yaml:"spotter_reliability_file_rtty"`
	MinSpotterReliability      float64 `yaml:"min_spotter_reliability"`
}

// CallCorrectionCustomSCPConfig controls runtime-learned SCP membership and
// long-horizon recency evidence.
type CallCorrectionCustomSCPConfig struct {
	Enabled bool `yaml:"enabled"`
	// Path is the Pebble directory used for custom SCP persistence.
	Path string `yaml:"path"`
	// HistoryHorizonDays bounds observation retention.
	HistoryHorizonDays int `yaml:"history_horizon_days"`
	// StaticHorizonDays bounds static-membership retention.
	StaticHorizonDays int `yaml:"static_horizon_days"`

	// >0 enables CW/RTTY SNR admission gates; <=0 disables the gate for that mode.
	MinSNRDBCW   int `yaml:"min_snr_db_cw"`
	MinSNRDBRTTY int `yaml:"min_snr_db_rtty"`

	ResolverMinScore          int `yaml:"resolver_min_score"`
	ResolverMinUniqueSpotters int `yaml:"resolver_min_unique_spotters"`
	ResolverMinUniqueH3Cells  int `yaml:"resolver_min_unique_h3_cells"`

	StabilizerMinScore          int `yaml:"stabilizer_min_score"`
	StabilizerMinUniqueSpotters int `yaml:"stabilizer_min_unique_spotters"`
	StabilizerMinUniqueH3Cells  int `yaml:"stabilizer_min_unique_h3_cells"`

	SFloorMinScore                int `yaml:"s_floor_min_score"`
	SFloorMinUniqueSpottersExact  int `yaml:"s_floor_min_unique_spotters_exact"`
	SFloorMinUniqueH3CellsExact   int `yaml:"s_floor_min_unique_h3_cells_exact"`
	SFloorMinUniqueSpottersFamily int `yaml:"s_floor_min_unique_spotters_family"`
	SFloorMinUniqueH3CellsFamily  int `yaml:"s_floor_min_unique_h3_cells_family"`

	MaxKeys           int   `yaml:"max_keys"`
	MaxSpottersPerKey int   `yaml:"max_spotters_per_key"`
	MaxDBBytes        int64 `yaml:"max_db_bytes"`

	CleanupIntervalSeconds int `yaml:"cleanup_interval_seconds"`

	BlockCacheMB          int `yaml:"block_cache_mb"`
	BloomFilterBits       int `yaml:"bloom_filter_bits"`
	MemTableSizeMB        int `yaml:"memtable_size_mb"`
	L0CompactionThreshold int `yaml:"l0_compaction_threshold"`
	L0StopWritesThreshold int `yaml:"l0_stop_writes_threshold"`
}

// CallCorrectionTemporalDecoderConfig controls fixed-lag temporal decoding for
// resolver-primary winner selection.
type CallCorrectionTemporalDecoderConfig struct {
	// Enabled toggles temporal decoding.
	Enabled bool `yaml:"enabled"`
	// Scope controls which observations are routed through lagged decoding.
	// Supported values: uncertain_only | all_correction_candidates.
	Scope string `yaml:"scope"`
	// LagSeconds is the primary look-ahead delay before the first decision.
	LagSeconds int `yaml:"lag_seconds"`
	// MaxWaitSeconds is the maximum delay budget before forced fallback.
	MaxWaitSeconds int `yaml:"max_wait_seconds"`
	// BeamSize bounds Viterbi/beam hypotheses per step.
	BeamSize int `yaml:"beam_size"`
	// MaxObsCandidates bounds per-observation candidate universe size.
	MaxObsCandidates int `yaml:"max_obs_candidates"`
	// StayBonus rewards state continuity between adjacent observations.
	StayBonus int `yaml:"stay_bonus"`
	// SwitchPenalty penalizes call changes between adjacent observations.
	SwitchPenalty int `yaml:"switch_penalty"`
	// FamilySwitchPenalty applies to slash/truncation-family switches.
	FamilySwitchPenalty int `yaml:"family_switch_penalty"`
	// Edit1SwitchPenalty applies to non-family edit-distance-1 switches.
	Edit1SwitchPenalty int `yaml:"edit1_switch_penalty"`
	// MinScore is the minimum best-path score required for commit.
	MinScore int `yaml:"min_score"`
	// MinMarginScore is the minimum score margin over runner-up required for commit.
	MinMarginScore int `yaml:"min_margin_score"`
	// OverflowAction controls commit fallback when confidence gates fail at max wait
	// or temporal state overflows. Supported values:
	//   - fallback_resolver: use current resolver-primary path
	//   - abstain: skip correction and preserve subject call
	//   - bypass: bypass temporal path and use immediate resolver behavior
	OverflowAction string `yaml:"overflow_action"`
	// MaxPending bounds concurrent temporal requests.
	MaxPending int `yaml:"max_pending"`
	// MaxActiveKeys bounds temporal decoder key-state cardinality.
	MaxActiveKeys int `yaml:"max_active_keys"`
	// MaxEventsPerKey bounds retained observations per key.
	MaxEventsPerKey int `yaml:"max_events_per_key"`
}

// CallCorrectionBayesBonusConfig controls a conservative Bayesian-style
// resolver-primary gate bonus for distance-1/2 candidates.
type CallCorrectionBayesBonusConfig struct {
	Enabled bool `yaml:"enabled"`

	// Distance-specific prior scaling (permille). Example: 350 => 0.35.
	WeightDistance1Milli int `yaml:"weight_distance1_milli"`
	WeightDistance2Milli int `yaml:"weight_distance2_milli"`

	// Smoothing constants for weighted support and recent evidence terms.
	WeightedSmoothingMilli int `yaml:"weighted_smoothing_milli"`
	RecentSmoothing        int `yaml:"recent_smoothing"`

	// Term caps (milli-natural-log units).
	ObsLogCapMilli   int `yaml:"obs_log_cap_milli"`
	PriorLogMinMilli int `yaml:"prior_log_min_milli"`
	PriorLogMaxMilli int `yaml:"prior_log_max_milli"`

	// Report-gate threshold by distance (milli-natural-log units).
	ReportThresholdDistance1Milli int `yaml:"report_threshold_distance1_milli"`
	ReportThresholdDistance2Milli int `yaml:"report_threshold_distance2_milli"`

	// Advantage-gate threshold by distance (milli-natural-log units).
	AdvantageThresholdDistance1Milli int `yaml:"advantage_threshold_distance1_milli"`
	AdvantageThresholdDistance2Milli int `yaml:"advantage_threshold_distance2_milli"`

	// Minimum weighted-support deltas (milli) required to apply advantage bonus.
	AdvantageMinWeightedDeltaDistance1Milli int `yaml:"advantage_min_weighted_delta_distance1_milli"`
	AdvantageMinWeightedDeltaDistance2Milli int `yaml:"advantage_min_weighted_delta_distance2_milli"`

	// Additional confidence points required for advantage bonus by distance.
	AdvantageExtraConfidenceDistance1 int `yaml:"advantage_extra_confidence_distance1"`
	AdvantageExtraConfidenceDistance2 int `yaml:"advantage_extra_confidence_distance2"`

	// Additional conservative rails.
	RequireCandidateValidated          bool `yaml:"require_candidate_validated"`
	RequireSubjectUnvalidatedDistance2 bool `yaml:"require_subject_unvalidated_distance2"`
}

// CallCorrectionFamilyPolicyConfig controls family-specific behavior for call
// precedence and telnet output suppression.
type CallCorrectionFamilyPolicyConfig struct {
	// SlashPrecedenceMinReports defines how many unique reporters are required
	// for a slash-explicit variant to suppress the bare call in the same base
	// family during correction ranking.
	SlashPrecedenceMinReports int `yaml:"slash_precedence_min_reports"`
	// Truncation defines one-character containment-family matching and gate rails.
	Truncation CallCorrectionTruncationFamilyConfig `yaml:"truncation"`
	// TelnetSuppression defines output-only suppression behavior for family
	// variants after correction decisions are made.
	TelnetSuppression CallCorrectionTelnetFamilySuppressionConfig `yaml:"telnet_suppression"`
}

// CallCorrectionTruncationFamilyConfig controls truncation-family detection and
// optional advantage relaxation behavior.
type CallCorrectionTruncationFamilyConfig struct {
	// Enabled toggles truncation-family detection entirely.
	Enabled bool `yaml:"enabled"`
	// MaxLengthDelta bounds |len(longer)-len(shorter)| for containment-family matches.
	MaxLengthDelta int `yaml:"max_length_delta"`
	// MinShorterLength prevents tiny strings from matching as truncation families.
	MinShorterLength int `yaml:"min_shorter_length"`
	// AllowPrefixMatch enables shorter-as-prefix matching (for example W1AB/W1ABC).
	AllowPrefixMatch bool `yaml:"allow_prefix_match"`
	// AllowSuffixMatch enables shorter-as-suffix matching (for example A1ABC/WA1ABC).
	AllowSuffixMatch bool `yaml:"allow_suffix_match"`
	// RelaxAdvantage defines when truncation family candidates can reduce min_advantage.
	RelaxAdvantage CallCorrectionTruncationAdvantageConfig `yaml:"relax_advantage"`
	// LengthBonus defines optional capped bonus support for truncation families.
	// The runtime applies this bonus only to the min_reports gate.
	LengthBonus CallCorrectionTruncationLengthBonusConfig `yaml:"length_bonus"`
	// Delta2Rails defines stricter gates for truncation matches where length delta is 2.
	Delta2Rails CallCorrectionTruncationDelta2RailsConfig `yaml:"delta2_rails"`
}

// CallCorrectionTruncationAdvantageConfig controls truncation-family
// min_advantage relaxation behavior.
type CallCorrectionTruncationAdvantageConfig struct {
	// Enabled toggles truncation-family min_advantage relaxation.
	Enabled bool `yaml:"enabled"`
	// MinAdvantage sets the effective min_advantage when relaxation applies.
	MinAdvantage int `yaml:"min_advantage"`
	// RequireCandidateValidated requires SCP/recent-band validation for the more-specific candidate.
	RequireCandidateValidated bool `yaml:"require_candidate_validated"`
	// RequireSubjectUnvalidated requires the less-specific subject to lack SCP/recent-band validation.
	RequireSubjectUnvalidated bool `yaml:"require_subject_unvalidated"`
}

// CallCorrectionTruncationLengthBonusConfig controls truncation-family bonus
// support that can apply only to the min_reports gate.
type CallCorrectionTruncationLengthBonusConfig struct {
	// Enabled toggles truncation-family length bonus.
	Enabled bool `yaml:"enabled"`
	// Max caps the added support derived from length delta.
	Max int `yaml:"max"`
	// RequireCandidateValidated requires SCP/recent-band validation for bonus use.
	RequireCandidateValidated bool `yaml:"require_candidate_validated"`
	// RequireSubjectUnvalidated requires the less-specific subject to be unvalidated.
	RequireSubjectUnvalidated bool `yaml:"require_subject_unvalidated"`
}

// CallCorrectionTruncationDelta2RailsConfig controls stricter gating for
// truncation relations where length delta is exactly 2.
type CallCorrectionTruncationDelta2RailsConfig struct {
	// Enabled toggles delta-2 strict rails.
	Enabled bool `yaml:"enabled"`
	// ExtraConfidencePercent increases the candidate confidence requirement.
	ExtraConfidencePercent int `yaml:"extra_confidence_percent"`
	// RequireCandidateValidated requires SCP/recent-band validation.
	RequireCandidateValidated bool `yaml:"require_candidate_validated"`
	// RequireSubjectUnvalidated optionally requires the subject to be unvalidated.
	RequireSubjectUnvalidated bool `yaml:"require_subject_unvalidated"`
}

// CallCorrectionTelnetFamilySuppressionConfig controls telnet-only family
// suppression runtime bounds.
type CallCorrectionTelnetFamilySuppressionConfig struct {
	// Enabled toggles telnet-only family suppression.
	Enabled bool `yaml:"enabled"`
	// EditNeighborEnabled toggles resolver-contested edit-neighbor suppression
	// for late-arriving variants in the same mode/frequency neighborhood.
	EditNeighborEnabled bool `yaml:"edit_neighbor_enabled"`
	// WindowSeconds is the recency window for family suppression cache entries.
	WindowSeconds int `yaml:"window_seconds"`
	// MaxEntries bounds total cache entries across all mode/frequency buckets.
	MaxEntries int `yaml:"max_entries"`
	// FrequencyToleranceFallbackHz applies when per-mode tolerance values are invalid/missing.
	FrequencyToleranceFallbackHz float64 `yaml:"frequency_tolerance_fallback_hz"`
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
	Enabled    bool    `yaml:"enabled"`
	URL        string  `yaml:"url"`
	File       string  `yaml:"file"`
	MinAbsSkew float64 `yaml:"min_abs_skew"`
	RefreshUTC string  `yaml:"refresh_utc"`
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

type loadRawPresence struct {
	ctyEnabledSet                                   bool
	hasSecondaryFastPrefer                          bool
	hasSecondaryMedPrefer                           bool
	hasSecondarySlowPrefer                          bool
	hasSecondaryFastWindow                          bool
	hasSecondaryMedWindow                           bool
	hasSecondarySlowWindow                          bool
	legacySecondaryWindow                           bool
	legacySecondaryPrefer                           bool
	hasAdaptiveMinReportsEnabled                    bool
	hasPSKRMQTTTimeout                              bool
	hasDXSummitBaseURL                              bool
	hasDXSummitPollInterval                         bool
	hasDXSummitMaxRecords                           bool
	hasDXSummitRequestTimeout                       bool
	hasDXSummitLookback                             bool
	hasDXSummitIncludeBands                         bool
	hasDXSummitSpotChannel                          bool
	hasDXSummitMaxResponse                          bool
	hasFamilyTruncationEnabled                      bool
	hasFamilyTruncationPrefix                       bool
	hasFamilyTruncationSuffix                       bool
	hasFamilyTruncationRelaxEnabled                 bool
	hasFamilyTruncationRelaxCandidate               bool
	hasFamilyTruncationRelaxSubject                 bool
	hasFamilyTruncationLengthBonusCandidate         bool
	hasFamilyTruncationLengthBonusSubject           bool
	hasFamilyTruncationDelta2Candidate              bool
	hasFamilyTruncationDelta2Subject                bool
	hasFamilyTelnetSuppressionEnabled               bool
	hasResolverNeighborhoodAllowTruncation          bool
	hasResolverRecentPlus1Enabled                   bool
	hasResolverRecentPlus1RequireSubjectWeaker      bool
	hasResolverRecentPlus1AllowTruncation           bool
	hasBayesBonusWeightDistance1Milli               bool
	hasBayesBonusWeightDistance2Milli               bool
	hasBayesBonusPriorLogMinMilli                   bool
	hasBayesBonusAdvantageMinWeightedDeltaD1        bool
	hasBayesBonusAdvantageMinWeightedDeltaD2        bool
	hasBayesBonusAdvantageExtraConfidenceD1         bool
	hasBayesBonusAdvantageExtraConfidenceD2         bool
	hasBayesBonusRequireCandidateValidated          bool
	hasBayesBonusRequireSubjectUnvalidatedDistance2 bool
	hasFTPMinUniqueSpotters                         bool
	hasFTVMinUniqueSpotters                         bool
	hasFT8QuietGapSeconds                           bool
	hasFT8HardCapSeconds                            bool
	hasFT4QuietGapSeconds                           bool
	hasFT4HardCapSeconds                            bool
	hasFT2QuietGapSeconds                           bool
	hasFT2HardCapSeconds                            bool
	hasUIColor                                      bool
	hasUIClearScreen                                bool
	hasTelnetBulletinDedupeWindow                   bool
	hasTelnetBulletinDedupeMaxEntries               bool
	hasLoggingDropDedupeWindow                      bool
	hasDroppedCallsDedupeWindow                     bool
	hasLoginAttemptsDedupeWindow                    bool
	hasReputationDropsDedupeWindow                  bool
	hasTelnetConnectionsDedupeWindow                bool
	hasIngestConnectionsDedupeWindow                bool
	hasPeerConnectionsDedupeWindow                  bool
	hasPropagationLoggingEnabled                    bool
	hasDroppedCallsBadDEDX                          bool
	hasDroppedCallsNoLicense                        bool
	hasDroppedCallsHarmonics                        bool
	hasReputationIPInfoPebbleLoadIPv4               bool
	hasReputationIPInfoDeleteCSVAfterImport         bool
	hasReputationIPInfoKeepGzip                     bool
	hasReputationIPInfoPebbleCleanup                bool
	hasReputationIPInfoPebbleCompact                bool
}

func captureLoadRawPresence(raw map[string]any) loadRawPresence {
	return loadRawPresence{
		ctyEnabledSet:                                   yamlKeyPresent(raw, "cty", "enabled"),
		hasSecondaryFastPrefer:                          yamlKeyPresent(raw, "dedup", "secondary_fast_prefer_stronger_snr"),
		hasSecondaryMedPrefer:                           yamlKeyPresent(raw, "dedup", "secondary_med_prefer_stronger_snr"),
		hasSecondarySlowPrefer:                          yamlKeyPresent(raw, "dedup", "secondary_slow_prefer_stronger_snr"),
		hasSecondaryFastWindow:                          yamlKeyPresent(raw, "dedup", "secondary_fast_window_seconds"),
		hasSecondaryMedWindow:                           yamlKeyPresent(raw, "dedup", "secondary_med_window_seconds"),
		hasSecondarySlowWindow:                          yamlKeyPresent(raw, "dedup", "secondary_slow_window_seconds"),
		legacySecondaryWindow:                           yamlKeyPresent(raw, "dedup", "secondary_window_seconds"),
		legacySecondaryPrefer:                           yamlKeyPresent(raw, "dedup", "secondary_prefer_stronger_snr"),
		hasAdaptiveMinReportsEnabled:                    yamlKeyPresent(raw, "call_correction", "adaptive_min_reports", "enabled"),
		hasPSKRMQTTTimeout:                              yamlKeyPresent(raw, "pskreporter", "mqtt_qos12_enqueue_timeout_ms"),
		hasDXSummitBaseURL:                              yamlKeyPresent(raw, "dxsummit", "base_url"),
		hasDXSummitPollInterval:                         yamlKeyPresent(raw, "dxsummit", "poll_interval_seconds"),
		hasDXSummitMaxRecords:                           yamlKeyPresent(raw, "dxsummit", "max_records_per_poll"),
		hasDXSummitRequestTimeout:                       yamlKeyPresent(raw, "dxsummit", "request_timeout_ms"),
		hasDXSummitLookback:                             yamlKeyPresent(raw, "dxsummit", "lookback_seconds"),
		hasDXSummitIncludeBands:                         yamlKeyPresent(raw, "dxsummit", "include_bands"),
		hasDXSummitSpotChannel:                          yamlKeyPresent(raw, "dxsummit", "spot_channel_size"),
		hasDXSummitMaxResponse:                          yamlKeyPresent(raw, "dxsummit", "max_response_bytes"),
		hasFamilyTruncationEnabled:                      yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "enabled"),
		hasFamilyTruncationPrefix:                       yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "allow_prefix_match"),
		hasFamilyTruncationSuffix:                       yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "allow_suffix_match"),
		hasFamilyTruncationRelaxEnabled:                 yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "relax_advantage", "enabled"),
		hasFamilyTruncationRelaxCandidate:               yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "relax_advantage", "require_candidate_validated"),
		hasFamilyTruncationRelaxSubject:                 yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "relax_advantage", "require_subject_unvalidated"),
		hasFamilyTruncationLengthBonusCandidate:         yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "length_bonus", "require_candidate_validated"),
		hasFamilyTruncationLengthBonusSubject:           yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "length_bonus", "require_subject_unvalidated"),
		hasFamilyTruncationDelta2Candidate:              yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "delta2_rails", "require_candidate_validated"),
		hasFamilyTruncationDelta2Subject:                yamlKeyPresent(raw, "call_correction", "family_policy", "truncation", "delta2_rails", "require_subject_unvalidated"),
		hasFamilyTelnetSuppressionEnabled:               yamlKeyPresent(raw, "call_correction", "family_policy", "telnet_suppression", "enabled"),
		hasResolverNeighborhoodAllowTruncation:          yamlKeyPresent(raw, "call_correction", "resolver_neighborhood_allow_truncation_family"),
		hasResolverRecentPlus1Enabled:                   yamlKeyPresent(raw, "call_correction", "resolver_recent_plus1_enabled"),
		hasResolverRecentPlus1RequireSubjectWeaker:      yamlKeyPresent(raw, "call_correction", "resolver_recent_plus1_require_subject_weaker"),
		hasResolverRecentPlus1AllowTruncation:           yamlKeyPresent(raw, "call_correction", "resolver_recent_plus1_allow_truncation_family"),
		hasBayesBonusWeightDistance1Milli:               yamlKeyPresent(raw, "call_correction", "bayes_bonus", "weight_distance1_milli"),
		hasBayesBonusWeightDistance2Milli:               yamlKeyPresent(raw, "call_correction", "bayes_bonus", "weight_distance2_milli"),
		hasBayesBonusPriorLogMinMilli:                   yamlKeyPresent(raw, "call_correction", "bayes_bonus", "prior_log_min_milli"),
		hasBayesBonusAdvantageMinWeightedDeltaD1:        yamlKeyPresent(raw, "call_correction", "bayes_bonus", "advantage_min_weighted_delta_distance1_milli"),
		hasBayesBonusAdvantageMinWeightedDeltaD2:        yamlKeyPresent(raw, "call_correction", "bayes_bonus", "advantage_min_weighted_delta_distance2_milli"),
		hasBayesBonusAdvantageExtraConfidenceD1:         yamlKeyPresent(raw, "call_correction", "bayes_bonus", "advantage_extra_confidence_distance1"),
		hasBayesBonusAdvantageExtraConfidenceD2:         yamlKeyPresent(raw, "call_correction", "bayes_bonus", "advantage_extra_confidence_distance2"),
		hasBayesBonusRequireCandidateValidated:          yamlKeyPresent(raw, "call_correction", "bayes_bonus", "require_candidate_validated"),
		hasBayesBonusRequireSubjectUnvalidatedDistance2: yamlKeyPresent(raw, "call_correction", "bayes_bonus", "require_subject_unvalidated_distance2"),
		hasFTPMinUniqueSpotters:                         yamlKeyPresent(raw, "call_correction", "p_min_unique_spotters"),
		hasFTVMinUniqueSpotters:                         yamlKeyPresent(raw, "call_correction", "v_min_unique_spotters"),
		hasFT8QuietGapSeconds:                           yamlKeyPresent(raw, "call_correction", "ft8_quiet_gap_seconds"),
		hasFT8HardCapSeconds:                            yamlKeyPresent(raw, "call_correction", "ft8_hard_cap_seconds"),
		hasFT4QuietGapSeconds:                           yamlKeyPresent(raw, "call_correction", "ft4_quiet_gap_seconds"),
		hasFT4HardCapSeconds:                            yamlKeyPresent(raw, "call_correction", "ft4_hard_cap_seconds"),
		hasFT2QuietGapSeconds:                           yamlKeyPresent(raw, "call_correction", "ft2_quiet_gap_seconds"),
		hasFT2HardCapSeconds:                            yamlKeyPresent(raw, "call_correction", "ft2_hard_cap_seconds"),
		hasUIColor:                                      yamlKeyPresent(raw, "ui", "color"),
		hasUIClearScreen:                                yamlKeyPresent(raw, "ui", "clear_screen"),
		hasTelnetBulletinDedupeWindow:                   yamlKeyPresent(raw, "telnet", "bulletin_dedupe_window_seconds"),
		hasTelnetBulletinDedupeMaxEntries:               yamlKeyPresent(raw, "telnet", "bulletin_dedupe_max_entries"),
		hasLoggingDropDedupeWindow:                      yamlKeyPresent(raw, "logging", "drop_dedupe_window_seconds"),
		hasDroppedCallsDedupeWindow:                     yamlKeyPresent(raw, "logging", "dropped_calls", "dedupe_window_seconds"),
		hasLoginAttemptsDedupeWindow:                    yamlKeyPresent(raw, "logging", "login_attempts", "dedupe_window_seconds"),
		hasReputationDropsDedupeWindow:                  yamlKeyPresent(raw, "logging", "reputation_drops", "dedupe_window_seconds"),
		hasTelnetConnectionsDedupeWindow:                yamlKeyPresent(raw, "logging", "telnet_connections", "dedupe_window_seconds"),
		hasIngestConnectionsDedupeWindow:                yamlKeyPresent(raw, "logging", "ingest_connections", "dedupe_window_seconds"),
		hasPeerConnectionsDedupeWindow:                  yamlKeyPresent(raw, "logging", "peer_connections", "dedupe_window_seconds"),
		hasPropagationLoggingEnabled:                    yamlKeyPresent(raw, "logging", "propagation", "enabled"),
		hasDroppedCallsBadDEDX:                          yamlKeyPresent(raw, "logging", "dropped_calls", "bad_de_dx"),
		hasDroppedCallsNoLicense:                        yamlKeyPresent(raw, "logging", "dropped_calls", "no_license"),
		hasDroppedCallsHarmonics:                        yamlKeyPresent(raw, "logging", "dropped_calls", "harmonics"),
		hasReputationIPInfoPebbleLoadIPv4:               yamlKeyPresent(raw, "reputation", "ipinfo_pebble_load_ipv4"),
		hasReputationIPInfoDeleteCSVAfterImport:         yamlKeyPresent(raw, "reputation", "ipinfo_delete_csv_after_import"),
		hasReputationIPInfoKeepGzip:                     yamlKeyPresent(raw, "reputation", "ipinfo_keep_gzip"),
		hasReputationIPInfoPebbleCleanup:                yamlKeyPresent(raw, "reputation", "ipinfo_pebble_cleanup"),
		hasReputationIPInfoPebbleCompact:                yamlKeyPresent(raw, "reputation", "ipinfo_pebble_compact"),
	}
}

// Load reads configuration from a YAML directory, validates required settings,
// and normalizes derived relationships so the rest of the cluster can rely on a
// consistent baseline.
// Purpose: Load and normalize the cluster configuration from a directory.
// Key aspects: Uses the filename registry; rejects unknown/missing settings and
// invalid values before startup.
// Upstream: main.go startup.
// Downstream: loadConfigDir, mergeYAMLMaps, normalize* helpers.
func Load(path string) (*Config, error) {
	cfg, _, err := LoadWithDiagnostics(path)
	return cfg, err
}

func LoadWithDiagnostics(path string) (*Config, LoadDiagnostics, error) {
	diagnostics := LoadDiagnostics{}
	info, err := os.Stat(path)
	if err != nil {
		return nil, diagnostics, fmt.Errorf("failed to stat config path %q: %w", path, err)
	}

	if !info.IsDir() {
		return nil, diagnostics, fmt.Errorf("config path %q must be a directory containing YAML files", path)
	}

	loaded, loadDiagnostics, err := loadConfigDir(path)
	diagnostics = loadDiagnostics
	if err != nil {
		return nil, diagnostics, err
	}
	for _, errText := range requiredConfigFileDiagnostics(loaded) {
		diagnostics.addError(errText)
	}
	raw := loaded.merged
	for _, errText := range removedRuntimeKeyDiagnostics(raw) {
		diagnostics.addError(errText)
	}
	for _, errText := range mergedRuntimePresenceDiagnostics(raw) {
		diagnostics.addError(errText)
	}

	var pathCfg pathreliability.Config
	var solarCfg solarweather.Config
	if pathFile, ok := loaded.pathsByBase[pathReliabilityConfigFile]; ok {
		var pathWarnings []string
		var pathErrors []string
		pathCfg, pathWarnings, pathErrors, err = pathreliability.LoadFileWithDiagnostics(pathFile)
		for _, warning := range pathWarnings {
			diagnostics.addWarning(warning)
		}
		if err != nil {
			diagnostics.addError(fmt.Sprintf("failed to load %s: %v", pathReliabilityConfigFile, err))
		}
		for _, errText := range pathErrors {
			diagnostics.addError(errText)
		}
	}
	if solarFile, ok := loaded.pathsByBase[solarWeatherConfigFile]; ok {
		var solarWarnings []string
		var solarErrors []string
		solarCfg, solarWarnings, solarErrors, err = solarweather.LoadFileWithDiagnostics(solarFile)
		for _, warning := range solarWarnings {
			diagnostics.addWarning(warning)
		}
		if err != nil {
			diagnostics.addError(fmt.Sprintf("failed to load %s: %v", solarWeatherConfigFile, err))
		}
		for _, errText := range solarErrors {
			diagnostics.addError(errText)
		}
	}
	diagnostics.sort()
	if diagnostics.HasErrors() {
		return nil, diagnostics, diagnostics.Error()
	}

	data, err := yaml.Marshal(raw)
	if err != nil {
		return nil, diagnostics, fmt.Errorf("failed to render merged config from %q: %w", path, err)
	}
	// Store the directory we loaded from to aid downstream diagnostics.
	if len(loaded.files) > 0 {
		path = filepath.Clean(path)
	}

	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&cfg); err != nil {
		return nil, diagnostics, fmt.Errorf("failed to parse config file %q: %w", path, err)
	}
	cfg.LoadedFrom = filepath.Clean(path)
	if err := validateConfiguredRuntimeValues(cfg); err != nil {
		return nil, diagnostics, err
	}
	cfg.PathReliability = pathCfg
	cfg.SolarWeather = solarCfg

	presence := captureLoadRawPresence(raw)

	if presence.legacySecondaryWindow || presence.legacySecondaryPrefer {
		fmt.Printf("Warning: dedup.secondary_window_seconds and dedup.secondary_prefer_stronger_snr are deprecated and ignored; use secondary_fast_* / secondary_med_* / secondary_slow_* instead.\n")
	}

	if err := normalizeUIConfig(&cfg, raw, presence); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizeLoggingAndPropReportConfig(&cfg, presence); err != nil {
		return nil, diagnostics, err
	}
	normalizeFeedConfig(&cfg, presence)
	if err := normalizeDXSummitConfig(&cfg, presence); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizeArchiveAndStatsConfig(&cfg, presence); err != nil {
		return nil, diagnostics, err
	}
	if err := validateGoRuntimeConfig(&cfg); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizeCallCorrectionConfig(&cfg, presence); err != nil {
		return nil, diagnostics, err
	}
	normalizeCallCacheConfig(&cfg)
	if err := normalizeTelnetConfig(&cfg, presence); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizeFeedTransportConfig(&cfg); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizePeeringConfig(&cfg); err != nil {
		return nil, diagnostics, err
	}
	// Keep outbound peer enablement explicit: omitted or false stays disabled.
	// This avoids accidental dial loops for placeholder entries.
	normalizeSignalPolicyConfig(&cfg)
	if err := normalizeReferenceDataConfig(&cfg, presence); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizeDedupAndBufferConfig(&cfg, presence); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizeFloodControlConfig(&cfg, raw); err != nil {
		return nil, diagnostics, err
	}
	if err := normalizeSkewConfig(&cfg); err != nil {
		return nil, diagnostics, err
	}
	normalizeReputationConfig(&cfg, presence)
	diagnostics.sort()
	return &cfg, diagnostics, nil
}

func normalizeUIConfig(cfg *Config, raw map[string]any, presence loadRawPresence) error {
	// UI defaults stay YAML-driven and deterministic; omitted booleans must not
	// behave like explicit false values.
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
		return fmt.Errorf("invalid ui.mode %q: must be ansi, tview, tview-v2, or headless", cfg.UI.Mode)
	}
	if cfg.UI.RefreshMS < 0 {
		return fmt.Errorf("invalid ui.refresh_ms %d (must be >= 0)", cfg.UI.RefreshMS)
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
	if !presence.hasUIColor {
		cfg.UI.Color = true
	}
	if !presence.hasUIClearScreen {
		cfg.UI.ClearScreen = true
	}
	return normalizeUIV2(&cfg.UI, raw)
}

func normalizeLoggingAndPropReportConfig(cfg *Config, presence loadRawPresence) error {
	cfg.Logging.Dir = strings.TrimSpace(cfg.Logging.Dir)
	if !presence.hasLoggingDropDedupeWindow {
		cfg.Logging.DropDedupeWindowSeconds = 120
	}
	if cfg.Logging.DropDedupeWindowSeconds < 0 {
		return fmt.Errorf("invalid logging.drop_dedupe_window_seconds %d (must be >= 0)", cfg.Logging.DropDedupeWindowSeconds)
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
		return fmt.Errorf("invalid prop report refresh time %q: %w", cfg.PropReport.RefreshUTC, err)
	}
	if err := normalizeDroppedCallLoggingConfig(cfg, presence); err != nil {
		return err
	}
	if err := normalizeEventFileLoggingConfigs(cfg, presence); err != nil {
		return err
	}
	return normalizePropagationLoggingConfig(cfg, presence)
}

func normalizeDroppedCallLoggingConfig(cfg *Config, presence loadRawPresence) error {
	dropped := &cfg.Logging.DroppedCalls
	dropped.Dir = strings.TrimSpace(dropped.Dir)
	if !presence.hasDroppedCallsBadDEDX {
		dropped.BadDEDX = true
	}
	if !presence.hasDroppedCallsNoLicense {
		dropped.NoLicense = true
	}
	if !presence.hasDroppedCallsHarmonics {
		dropped.Harmonics = true
	}
	if !presence.hasDroppedCallsDedupeWindow {
		dropped.DedupeWindowSeconds = cfg.Logging.DropDedupeWindowSeconds
	}
	if dropped.DedupeWindowSeconds < 0 {
		return fmt.Errorf("invalid logging.dropped_calls.dedupe_window_seconds %d (must be >= 0)", dropped.DedupeWindowSeconds)
	}
	if dropped.RetentionDays < 0 {
		return fmt.Errorf("invalid logging.dropped_calls.retention_days %d (must be >= 0)", dropped.RetentionDays)
	}
	if dropped.RetentionDays == 0 {
		dropped.RetentionDays = cfg.Logging.RetentionDays
		if dropped.RetentionDays <= 0 {
			dropped.RetentionDays = 7
		}
	}
	if dropped.Dir == "" {
		baseDir := cfg.Logging.Dir
		if baseDir == "" {
			baseDir = "data/logs"
		}
		dropped.Dir = filepath.Join(baseDir, "dropped_calls")
	}
	return nil
}

func normalizeEventFileLoggingConfigs(cfg *Config, presence loadRawPresence) error {
	baseDir := cfg.Logging.Dir
	if baseDir == "" {
		baseDir = "data/logs"
	}
	configs := []struct {
		name      string
		subdir    string
		cfg       *EventFileLoggingConfig
		hasDedupe bool
	}{
		{name: "login_attempts", subdir: "login_attempts", cfg: &cfg.Logging.LoginAttempts, hasDedupe: presence.hasLoginAttemptsDedupeWindow},
		{name: "reputation_drops", subdir: "reputation_drops", cfg: &cfg.Logging.ReputationDrops, hasDedupe: presence.hasReputationDropsDedupeWindow},
		{name: "telnet_connections", subdir: "telnet_connections", cfg: &cfg.Logging.TelnetConnections, hasDedupe: presence.hasTelnetConnectionsDedupeWindow},
		{name: "ingest_connections", subdir: "ingest_connections", cfg: &cfg.Logging.IngestConnections, hasDedupe: presence.hasIngestConnectionsDedupeWindow},
		{name: "peer_connections", subdir: "peer_connections", cfg: &cfg.Logging.PeerConnections, hasDedupe: presence.hasPeerConnectionsDedupeWindow},
	}
	for _, item := range configs {
		item.cfg.Dir = strings.TrimSpace(item.cfg.Dir)
		if !item.hasDedupe {
			item.cfg.DedupeWindowSeconds = cfg.Logging.DropDedupeWindowSeconds
		}
		if item.cfg.DedupeWindowSeconds < 0 {
			return fmt.Errorf("invalid logging.%s.dedupe_window_seconds %d (must be >= 0)", item.name, item.cfg.DedupeWindowSeconds)
		}
		if item.cfg.RetentionDays < 0 {
			return fmt.Errorf("invalid logging.%s.retention_days %d (must be >= 0)", item.name, item.cfg.RetentionDays)
		}
		if item.cfg.RetentionDays == 0 {
			item.cfg.RetentionDays = cfg.Logging.RetentionDays
			if item.cfg.RetentionDays <= 0 {
				item.cfg.RetentionDays = 7
			}
		}
		if item.cfg.Dir == "" {
			item.cfg.Dir = filepath.Join(baseDir, item.subdir)
		}
	}
	return nil
}

func normalizePropagationLoggingConfig(cfg *Config, presence loadRawPresence) error {
	prop := &cfg.Logging.Propagation
	prop.Dir = strings.TrimSpace(prop.Dir)
	if !presence.hasPropagationLoggingEnabled {
		prop.Enabled = cfg.Logging.Enabled
	}
	if prop.RetentionDays < 0 {
		return fmt.Errorf("invalid logging.propagation.retention_days %d (must be >= 0)", prop.RetentionDays)
	}
	if prop.RetentionDays == 0 {
		prop.RetentionDays = cfg.Logging.RetentionDays
		if prop.RetentionDays <= 0 {
			prop.RetentionDays = 7
		}
	}
	if prop.Dir == "" {
		prop.Dir = filepath.Join("data", "logs", "propagation")
	}
	return nil
}

func normalizeFeedConfig(cfg *Config, presence loadRawPresence) {
	// Feed buffers need deterministic defaults because callers assume these are
	// always normalized after config.Load returns. Keepalives are YAML-owned; an
	// explicit 0 disables them.
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
	if cfg.PSKReporter.MQTTQoS12EnqueueTimeoutMS == 0 && !presence.hasPSKRMQTTTimeout {
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
}

func normalizeDXSummitConfig(cfg *Config, presence loadRawPresence) error {
	dx := &cfg.DXSummit
	dx.Name = strings.TrimSpace(dx.Name)
	if dx.Name == "" {
		dx.Name = defaultDXSummitName
	}
	dx.BaseURL = strings.TrimSpace(dx.BaseURL)
	if dx.BaseURL == "" {
		if presence.hasDXSummitBaseURL {
			return fmt.Errorf("invalid dxsummit.base_url: must not be empty")
		}
		dx.BaseURL = defaultDXSummitBaseURL
	}
	parsed, err := url.Parse(dx.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid dxsummit.base_url %q (must be an absolute http or https URL)", dx.BaseURL)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("invalid dxsummit.base_url %q (scheme must be http or https)", dx.BaseURL)
	}

	if dx.PollIntervalSeconds <= 0 {
		if presence.hasDXSummitPollInterval {
			return fmt.Errorf("invalid dxsummit.poll_interval_seconds %d (must be > 0)", dx.PollIntervalSeconds)
		}
		dx.PollIntervalSeconds = defaultDXSummitPollIntervalSeconds
	}
	if dx.MaxRecordsPerPoll <= 0 {
		if presence.hasDXSummitMaxRecords {
			return fmt.Errorf("invalid dxsummit.max_records_per_poll %d (must be between 1 and 10000)", dx.MaxRecordsPerPoll)
		}
		dx.MaxRecordsPerPoll = defaultDXSummitMaxRecordsPerPoll
	}
	if dx.MaxRecordsPerPoll > 10000 {
		return fmt.Errorf("invalid dxsummit.max_records_per_poll %d (must be between 1 and 10000)", dx.MaxRecordsPerPoll)
	}
	if dx.RequestTimeoutMS <= 0 {
		if presence.hasDXSummitRequestTimeout {
			return fmt.Errorf("invalid dxsummit.request_timeout_ms %d (must be > 0)", dx.RequestTimeoutMS)
		}
		dx.RequestTimeoutMS = defaultDXSummitRequestTimeoutMS
	}
	if dx.RequestTimeoutMS >= dx.PollIntervalSeconds*1000 {
		return fmt.Errorf("invalid dxsummit.request_timeout_ms %d (must be less than poll_interval_seconds * 1000)", dx.RequestTimeoutMS)
	}
	if dx.LookbackSeconds <= 0 {
		if presence.hasDXSummitLookback {
			return fmt.Errorf("invalid dxsummit.lookback_seconds %d (must be >= poll_interval_seconds)", dx.LookbackSeconds)
		}
		dx.LookbackSeconds = defaultDXSummitLookbackSeconds
	}
	if dx.LookbackSeconds < dx.PollIntervalSeconds {
		return fmt.Errorf("invalid dxsummit.lookback_seconds %d (must be >= poll_interval_seconds)", dx.LookbackSeconds)
	}
	if dx.StartupBackfillSeconds < 0 {
		return fmt.Errorf("invalid dxsummit.startup_backfill_seconds %d (must be >= 0)", dx.StartupBackfillSeconds)
	}
	if dx.StartupBackfillSeconds > dx.LookbackSeconds {
		return fmt.Errorf("invalid dxsummit.startup_backfill_seconds %d (must be <= lookback_seconds)", dx.StartupBackfillSeconds)
	}
	if len(dx.IncludeBands) == 0 {
		if presence.hasDXSummitIncludeBands {
			return fmt.Errorf("invalid dxsummit.include_bands: must include at least one of HF, VHF, UHF")
		}
		dx.IncludeBands = []string{"HF", "VHF", "UHF"}
	}
	for i, band := range dx.IncludeBands {
		normalized := strings.ToUpper(strings.TrimSpace(band))
		switch normalized {
		case "HF", "VHF", "UHF":
			dx.IncludeBands[i] = normalized
		default:
			return fmt.Errorf("invalid dxsummit.include_bands[%d] %q (expected HF, VHF, or UHF)", i, band)
		}
	}
	if dx.SpotChannelSize <= 0 {
		if presence.hasDXSummitSpotChannel {
			return fmt.Errorf("invalid dxsummit.spot_channel_size %d (must be > 0)", dx.SpotChannelSize)
		}
		dx.SpotChannelSize = defaultDXSummitSpotChannelSize
	}
	if dx.MaxResponseBytes <= 0 {
		if presence.hasDXSummitMaxResponse {
			return fmt.Errorf("invalid dxsummit.max_response_bytes %d (must be > 0)", dx.MaxResponseBytes)
		}
		dx.MaxResponseBytes = defaultDXSummitMaxResponseBytes
	}
	return nil
}

func normalizeArchiveAndStatsConfig(cfg *Config, _ loadRawPresence) error {
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
		cfg.Archive.CleanupIntervalSeconds = 3600
	}
	if cfg.Archive.RetentionSeconds <= 0 {
		cfg.Archive.RetentionSeconds = DefaultArchiveRetentionSeconds
	}
	if strings.TrimSpace(cfg.Archive.DBPath) == "" {
		cfg.Archive.DBPath = "data/archive/pebble"
	}
	syncMode := strings.ToLower(strings.TrimSpace(cfg.Archive.Synchronous))
	if syncMode == "" {
		syncMode = "off"
	}
	switch syncMode {
	case "off", "normal", "full", "extra":
		cfg.Archive.Synchronous = syncMode
	default:
		return fmt.Errorf("invalid archive.synchronous %q: must be off, normal, full, or extra", cfg.Archive.Synchronous)
	}
	if cfg.Stats.DisplayIntervalSeconds <= 0 {
		cfg.Stats.DisplayIntervalSeconds = 30
	}
	return nil
}

func validateGoRuntimeConfig(cfg *Config) error {
	if cfg.GoRuntime.MemoryLimitMiB > 0 && cfg.GoRuntime.MemoryLimitMiB < 64 {
		return fmt.Errorf("invalid go_runtime.memory_limit_mib %d (must be 0 or >= 64)", cfg.GoRuntime.MemoryLimitMiB)
	}
	if cfg.GoRuntime.MaxProcs < 0 {
		return fmt.Errorf("invalid go_runtime.max_procs %d (must be >= 0)", cfg.GoRuntime.MaxProcs)
	}
	return nil
}

func normalizeCallCorrectionConfig(cfg *Config, presence loadRawPresence) error {
	normalizeCallCorrectionCoreConfig(cfg)
	if err := normalizeCallCorrectionFTCorroborationConfig(cfg, presence); err != nil {
		return err
	}
	normalizeCallCorrectionResolverConfig(cfg, presence)
	normalizeCallCorrectionBayesDefaultsConfig(cfg, presence)
	if err := normalizeCallCorrectionTemporalConfig(cfg); err != nil {
		return err
	}
	normalizeCallCorrectionFamilyPolicyConfig(cfg, presence)
	if err := validateCallCorrectionStabilizerTimeoutAction(cfg); err != nil {
		return err
	}
	normalizeCallCorrectionWeightsAndAdaptiveConfig(cfg, presence)
	return nil
}

func normalizeCallCorrectionCoreConfig(cfg *Config) {
	if cfg.CallCorrection.MinConsensusReports <= 0 {
		cfg.CallCorrection.MinConsensusReports = 4
	}
	if cfg.CallCorrection.FamilyPolicy.SlashPrecedenceMinReports <= 0 {
		cfg.CallCorrection.FamilyPolicy.SlashPrecedenceMinReports = 2
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
	if cfg.CallCorrection.InvalidAction == "" {
		cfg.CallCorrection.InvalidAction = "broadcast"
	}
	if strings.TrimSpace(cfg.CallCorrection.DistanceModelCW) == "" {
		cfg.CallCorrection.DistanceModelCW = "plain"
	}
	if strings.TrimSpace(cfg.CallCorrection.DistanceModelRTTY) == "" {
		cfg.CallCorrection.DistanceModelRTTY = "plain"
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
	if cfg.CallCorrection.ConfusionModelWeight < 0 {
		cfg.CallCorrection.ConfusionModelWeight = 0
	}
	if cfg.CallCorrection.RecentBandWindowSeconds <= 0 {
		cfg.CallCorrection.RecentBandWindowSeconds = 12 * 60 * 60
	}
	if cfg.CallCorrection.RecentBandRecordMinUniqueSpotters <= 0 {
		cfg.CallCorrection.RecentBandRecordMinUniqueSpotters = 2
	}
	if strings.TrimSpace(cfg.CallCorrection.CustomSCP.Path) == "" {
		cfg.CallCorrection.CustomSCP.Path = filepath.Join("data", "scp")
	}
	if cfg.CallCorrection.CustomSCP.HistoryHorizonDays <= 0 {
		cfg.CallCorrection.CustomSCP.HistoryHorizonDays = 60
	}
	if cfg.CallCorrection.CustomSCP.StaticHorizonDays <= 0 {
		cfg.CallCorrection.CustomSCP.StaticHorizonDays = 395
	}
	if cfg.CallCorrection.CustomSCP.ResolverMinScore <= 0 {
		cfg.CallCorrection.CustomSCP.ResolverMinScore = 5
	}
	if cfg.CallCorrection.CustomSCP.ResolverMinUniqueSpotters <= 0 {
		cfg.CallCorrection.CustomSCP.ResolverMinUniqueSpotters = 4
	}
	if cfg.CallCorrection.CustomSCP.ResolverMinUniqueH3Cells <= 0 {
		cfg.CallCorrection.CustomSCP.ResolverMinUniqueH3Cells = 2
	}
	if cfg.CallCorrection.CustomSCP.StabilizerMinScore <= 0 {
		cfg.CallCorrection.CustomSCP.StabilizerMinScore = 5
	}
	if cfg.CallCorrection.CustomSCP.StabilizerMinUniqueSpotters <= 0 {
		cfg.CallCorrection.CustomSCP.StabilizerMinUniqueSpotters = 3
	}
	if cfg.CallCorrection.CustomSCP.StabilizerMinUniqueH3Cells <= 0 {
		cfg.CallCorrection.CustomSCP.StabilizerMinUniqueH3Cells = 2
	}
	if cfg.CallCorrection.CustomSCP.SFloorMinScore <= 0 {
		cfg.CallCorrection.CustomSCP.SFloorMinScore = 3
	}
	if cfg.CallCorrection.CustomSCP.SFloorMinUniqueSpottersExact <= 0 {
		cfg.CallCorrection.CustomSCP.SFloorMinUniqueSpottersExact = 3
	}
	if cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsExact <= 0 {
		cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsExact = 2
	}
	if cfg.CallCorrection.CustomSCP.SFloorMinUniqueSpottersFamily <= 0 {
		cfg.CallCorrection.CustomSCP.SFloorMinUniqueSpottersFamily = 5
	}
	if cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsFamily <= 0 {
		cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsFamily = 3
	}
	if cfg.CallCorrection.CustomSCP.MaxKeys <= 0 {
		cfg.CallCorrection.CustomSCP.MaxKeys = 500000
	}
	if cfg.CallCorrection.CustomSCP.MaxSpottersPerKey <= 0 {
		cfg.CallCorrection.CustomSCP.MaxSpottersPerKey = 64
	}
	if cfg.CallCorrection.CustomSCP.MaxDBBytes <= 0 {
		cfg.CallCorrection.CustomSCP.MaxDBBytes = 8 * 1024 * 1024 * 1024
	}
	if cfg.CallCorrection.CustomSCP.CleanupIntervalSeconds <= 0 {
		cfg.CallCorrection.CustomSCP.CleanupIntervalSeconds = 600
	}
	if cfg.CallCorrection.CustomSCP.BlockCacheMB <= 0 {
		cfg.CallCorrection.CustomSCP.BlockCacheMB = 64
	}
	if cfg.CallCorrection.CustomSCP.BloomFilterBits <= 0 {
		cfg.CallCorrection.CustomSCP.BloomFilterBits = 10
	}
	if cfg.CallCorrection.CustomSCP.MemTableSizeMB <= 0 {
		cfg.CallCorrection.CustomSCP.MemTableSizeMB = 32
	}
	if cfg.CallCorrection.CustomSCP.L0CompactionThreshold <= 0 {
		cfg.CallCorrection.CustomSCP.L0CompactionThreshold = 4
	}
	if cfg.CallCorrection.CustomSCP.L0StopWritesThreshold <= cfg.CallCorrection.CustomSCP.L0CompactionThreshold {
		cfg.CallCorrection.CustomSCP.L0StopWritesThreshold = cfg.CallCorrection.CustomSCP.L0CompactionThreshold + 4
	}
	if cfg.CallCorrection.CustomSCP.Enabled {
		if cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner < cfg.CallCorrection.CustomSCP.ResolverMinUniqueSpotters {
			cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner = cfg.CallCorrection.CustomSCP.ResolverMinUniqueSpotters
		}
		if cfg.CallCorrection.RecentBandRecordMinUniqueSpotters < cfg.CallCorrection.CustomSCP.StabilizerMinUniqueSpotters {
			cfg.CallCorrection.RecentBandRecordMinUniqueSpotters = cfg.CallCorrection.CustomSCP.StabilizerMinUniqueSpotters
		}
	}
	if cfg.CallCorrection.StabilizerDelaySeconds <= 0 {
		cfg.CallCorrection.StabilizerDelaySeconds = 5
	}
	if cfg.CallCorrection.StabilizerMaxChecks <= 0 {
		cfg.CallCorrection.StabilizerMaxChecks = 1
	}
	if cfg.CallCorrection.StabilizerMaxPending <= 0 {
		cfg.CallCorrection.StabilizerMaxPending = 20000
	}
}

func normalizeCallCorrectionFTCorroborationConfig(cfg *Config, presence loadRawPresence) error {
	if !presence.hasFTPMinUniqueSpotters {
		cfg.CallCorrection.PMinUniqueSpotters = DefaultCallCorrectionFTPMinUniqueSpotters
	}
	if !presence.hasFTVMinUniqueSpotters {
		cfg.CallCorrection.VMinUniqueSpotters = DefaultCallCorrectionFTVMinUniqueSpotters
	}
	if !presence.hasFT8QuietGapSeconds {
		cfg.CallCorrection.FT8QuietGapSeconds = DefaultCallCorrectionFT8QuietGapSeconds
	}
	if !presence.hasFT8HardCapSeconds {
		cfg.CallCorrection.FT8HardCapSeconds = DefaultCallCorrectionFT8HardCapSeconds
	}
	if !presence.hasFT4QuietGapSeconds {
		cfg.CallCorrection.FT4QuietGapSeconds = DefaultCallCorrectionFT4QuietGapSeconds
	}
	if !presence.hasFT4HardCapSeconds {
		cfg.CallCorrection.FT4HardCapSeconds = DefaultCallCorrectionFT4HardCapSeconds
	}
	if !presence.hasFT2QuietGapSeconds {
		cfg.CallCorrection.FT2QuietGapSeconds = DefaultCallCorrectionFT2QuietGapSeconds
	}
	if !presence.hasFT2HardCapSeconds {
		cfg.CallCorrection.FT2HardCapSeconds = DefaultCallCorrectionFT2HardCapSeconds
	}
	if cfg.CallCorrection.PMinUniqueSpotters < 2 {
		return fmt.Errorf("invalid call_correction.p_min_unique_spotters %d (expected >= 2)", cfg.CallCorrection.PMinUniqueSpotters)
	}
	if cfg.CallCorrection.VMinUniqueSpotters <= cfg.CallCorrection.PMinUniqueSpotters {
		return fmt.Errorf("invalid call_correction.v_min_unique_spotters %d (expected > p_min_unique_spotters=%d)", cfg.CallCorrection.VMinUniqueSpotters, cfg.CallCorrection.PMinUniqueSpotters)
	}
	if cfg.CallCorrection.FT8QuietGapSeconds <= 0 {
		return fmt.Errorf("invalid call_correction.ft8_quiet_gap_seconds %d (expected > 0)", cfg.CallCorrection.FT8QuietGapSeconds)
	}
	if cfg.CallCorrection.FT8HardCapSeconds < cfg.CallCorrection.FT8QuietGapSeconds {
		return fmt.Errorf("invalid call_correction.ft8_hard_cap_seconds %d (expected >= ft8_quiet_gap_seconds=%d)", cfg.CallCorrection.FT8HardCapSeconds, cfg.CallCorrection.FT8QuietGapSeconds)
	}
	if cfg.CallCorrection.FT4QuietGapSeconds <= 0 {
		return fmt.Errorf("invalid call_correction.ft4_quiet_gap_seconds %d (expected > 0)", cfg.CallCorrection.FT4QuietGapSeconds)
	}
	if cfg.CallCorrection.FT4HardCapSeconds < cfg.CallCorrection.FT4QuietGapSeconds {
		return fmt.Errorf("invalid call_correction.ft4_hard_cap_seconds %d (expected >= ft4_quiet_gap_seconds=%d)", cfg.CallCorrection.FT4HardCapSeconds, cfg.CallCorrection.FT4QuietGapSeconds)
	}
	if cfg.CallCorrection.FT2QuietGapSeconds <= 0 {
		return fmt.Errorf("invalid call_correction.ft2_quiet_gap_seconds %d (expected > 0)", cfg.CallCorrection.FT2QuietGapSeconds)
	}
	if cfg.CallCorrection.FT2HardCapSeconds < cfg.CallCorrection.FT2QuietGapSeconds {
		return fmt.Errorf("invalid call_correction.ft2_hard_cap_seconds %d (expected >= ft2_quiet_gap_seconds=%d)", cfg.CallCorrection.FT2HardCapSeconds, cfg.CallCorrection.FT2QuietGapSeconds)
	}
	return nil
}

func normalizeCallCorrectionResolverConfig(cfg *Config, presence loadRawPresence) {
	if cfg.CallCorrection.StabilizerPDelayConfidencePercent < 0 {
		cfg.CallCorrection.StabilizerPDelayConfidencePercent = 0
	}
	if cfg.CallCorrection.StabilizerPDelayConfidencePercent > 100 {
		cfg.CallCorrection.StabilizerPDelayConfidencePercent = 100
	}
	if cfg.CallCorrection.StabilizerPDelayMaxChecks < 0 {
		cfg.CallCorrection.StabilizerPDelayMaxChecks = 0
	}
	if cfg.CallCorrection.StabilizerAmbiguousMaxChecks < 0 {
		cfg.CallCorrection.StabilizerAmbiguousMaxChecks = 0
	}
	if cfg.CallCorrection.StabilizerEditNeighborMaxChecks < 0 {
		cfg.CallCorrection.StabilizerEditNeighborMaxChecks = 0
	}
	if cfg.CallCorrection.StabilizerEditNeighborMinSpotters < 0 {
		cfg.CallCorrection.StabilizerEditNeighborMinSpotters = 0
	}
	if cfg.CallCorrection.StabilizerEditNeighborEnabled && cfg.CallCorrection.StabilizerEditNeighborMinSpotters <= 0 {
		cfg.CallCorrection.StabilizerEditNeighborMinSpotters = cfg.CallCorrection.RecentBandRecordMinUniqueSpotters
		if cfg.CallCorrection.StabilizerEditNeighborMinSpotters <= 0 {
			cfg.CallCorrection.StabilizerEditNeighborMinSpotters = 2
		}
	}
	if cfg.CallCorrection.ResolverNeighborhoodBucketRadius < 0 {
		cfg.CallCorrection.ResolverNeighborhoodBucketRadius = 0
	}
	if cfg.CallCorrection.ResolverNeighborhoodEnabled && cfg.CallCorrection.ResolverNeighborhoodBucketRadius <= 0 {
		cfg.CallCorrection.ResolverNeighborhoodBucketRadius = 1
	}
	if cfg.CallCorrection.ResolverNeighborhoodBucketRadius > 2 {
		cfg.CallCorrection.ResolverNeighborhoodBucketRadius = 2
	}
	if cfg.CallCorrection.ResolverNeighborhoodMaxDistance <= 0 {
		cfg.CallCorrection.ResolverNeighborhoodMaxDistance = 1
	}
	if !presence.hasResolverNeighborhoodAllowTruncation {
		cfg.CallCorrection.ResolverNeighborhoodAllowTruncation = true
	}
	if !presence.hasResolverRecentPlus1Enabled {
		cfg.CallCorrection.ResolverRecentPlus1Enabled = true
	}
	if cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner <= 0 {
		cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner = 3
	}
	if !presence.hasResolverRecentPlus1RequireSubjectWeaker {
		cfg.CallCorrection.ResolverRecentPlus1RequireSubjectWeaker = true
	}
	if cfg.CallCorrection.ResolverRecentPlus1MaxDistance <= 0 {
		cfg.CallCorrection.ResolverRecentPlus1MaxDistance = 1
	}
	if !presence.hasResolverRecentPlus1AllowTruncation {
		cfg.CallCorrection.ResolverRecentPlus1AllowTruncation = true
	}
}

func normalizeCallCorrectionBayesDefaultsConfig(cfg *Config, presence loadRawPresence) {
	if cfg.CallCorrection.BayesBonus.WeightDistance1Milli < 0 {
		cfg.CallCorrection.BayesBonus.WeightDistance1Milli = 0
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance1Milli == 0 && !presence.hasBayesBonusWeightDistance1Milli {
		cfg.CallCorrection.BayesBonus.WeightDistance1Milli = 350
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance2Milli < 0 {
		cfg.CallCorrection.BayesBonus.WeightDistance2Milli = 0
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance2Milli == 0 && !presence.hasBayesBonusWeightDistance2Milli {
		cfg.CallCorrection.BayesBonus.WeightDistance2Milli = 200
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance1Milli > 1000 {
		cfg.CallCorrection.BayesBonus.WeightDistance1Milli = 1000
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance2Milli > 1000 {
		cfg.CallCorrection.BayesBonus.WeightDistance2Milli = 1000
	}
	if cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli <= 0 {
		cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli = 1000
	}
	if cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli > 100000 {
		cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli = 100000
	}
	if cfg.CallCorrection.BayesBonus.RecentSmoothing <= 0 {
		cfg.CallCorrection.BayesBonus.RecentSmoothing = 2
	}
	if cfg.CallCorrection.BayesBonus.RecentSmoothing > 100 {
		cfg.CallCorrection.BayesBonus.RecentSmoothing = 100
	}
	if cfg.CallCorrection.BayesBonus.ObsLogCapMilli <= 0 {
		cfg.CallCorrection.BayesBonus.ObsLogCapMilli = 350
	}
	if cfg.CallCorrection.BayesBonus.ObsLogCapMilli > 2000 {
		cfg.CallCorrection.BayesBonus.ObsLogCapMilli = 2000
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMinMilli == 0 && !presence.hasBayesBonusPriorLogMinMilli {
		cfg.CallCorrection.BayesBonus.PriorLogMinMilli = -200
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMaxMilli == 0 {
		cfg.CallCorrection.BayesBonus.PriorLogMaxMilli = 600
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMinMilli > 0 {
		cfg.CallCorrection.BayesBonus.PriorLogMinMilli = 0
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMinMilli < -2000 {
		cfg.CallCorrection.BayesBonus.PriorLogMinMilli = -2000
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMaxMilli < 10 {
		cfg.CallCorrection.BayesBonus.PriorLogMaxMilli = 10
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMaxMilli > 3000 {
		cfg.CallCorrection.BayesBonus.PriorLogMaxMilli = 3000
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMinMilli >= cfg.CallCorrection.BayesBonus.PriorLogMaxMilli {
		cfg.CallCorrection.BayesBonus.PriorLogMinMilli = -200
		cfg.CallCorrection.BayesBonus.PriorLogMaxMilli = 600
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli <= 0 {
		cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli = 450
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli <= 0 {
		cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli = 650
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli > 4000 {
		cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli = 4000
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli > 4000 {
		cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli = 4000
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli < cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli {
		cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli = cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli
	}
	if cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli <= 0 {
		cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli = 700
	}
	if cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli <= 0 {
		cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli = 950
	}
	if cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli > 4000 {
		cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli = 4000
	}
	if cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli > 4000 {
		cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli = 4000
	}
	if cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli < cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli {
		cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli = cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli < 0 {
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli = 0
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli < 0 {
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli = 0
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli == 0 && !presence.hasBayesBonusAdvantageMinWeightedDeltaD1 {
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli = 200
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli == 0 && !presence.hasBayesBonusAdvantageMinWeightedDeltaD2 {
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli = 300
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli > 5000 {
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli = 5000
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli > 5000 {
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli = 5000
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli < cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli {
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli = cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 < 0 {
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 = 0
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 < 0 {
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 = 0
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 == 0 && !presence.hasBayesBonusAdvantageExtraConfidenceD1 {
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 = 3
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 == 0 && !presence.hasBayesBonusAdvantageExtraConfidenceD2 {
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 = 5
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 > 50 {
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 = 50
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 > 50 {
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 = 50
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 < cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 {
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 = cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1
	}
	if !presence.hasBayesBonusRequireCandidateValidated {
		cfg.CallCorrection.BayesBonus.RequireCandidateValidated = true
	}
	if !presence.hasBayesBonusRequireSubjectUnvalidatedDistance2 {
		cfg.CallCorrection.BayesBonus.RequireSubjectUnvalidatedDistance2 = true
	}
}

func normalizeCallCorrectionTemporalConfig(cfg *Config) error {
	cfg.CallCorrection.TemporalDecoder.Scope = strings.ToLower(strings.TrimSpace(cfg.CallCorrection.TemporalDecoder.Scope))
	if cfg.CallCorrection.TemporalDecoder.Scope == "" {
		cfg.CallCorrection.TemporalDecoder.Scope = "uncertain_only"
	}
	switch cfg.CallCorrection.TemporalDecoder.Scope {
	case "uncertain_only", "all_correction_candidates":
	default:
		return fmt.Errorf("invalid call_correction.temporal_decoder.scope %q (expected uncertain_only or all_correction_candidates)", cfg.CallCorrection.TemporalDecoder.Scope)
	}
	if cfg.CallCorrection.TemporalDecoder.LagSeconds <= 0 {
		cfg.CallCorrection.TemporalDecoder.LagSeconds = 2
	}
	if cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds <= 0 {
		cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds = 6
	}
	if cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds < cfg.CallCorrection.TemporalDecoder.LagSeconds {
		cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds = cfg.CallCorrection.TemporalDecoder.LagSeconds
	}
	if cfg.CallCorrection.TemporalDecoder.BeamSize <= 0 {
		cfg.CallCorrection.TemporalDecoder.BeamSize = 8
	}
	if cfg.CallCorrection.TemporalDecoder.MaxObsCandidates <= 0 {
		cfg.CallCorrection.TemporalDecoder.MaxObsCandidates = 8
	}
	if cfg.CallCorrection.TemporalDecoder.StayBonus == 0 {
		cfg.CallCorrection.TemporalDecoder.StayBonus = 120
	}
	if cfg.CallCorrection.TemporalDecoder.StayBonus < 0 {
		cfg.CallCorrection.TemporalDecoder.StayBonus = 0
	}
	if cfg.CallCorrection.TemporalDecoder.SwitchPenalty <= 0 {
		cfg.CallCorrection.TemporalDecoder.SwitchPenalty = 160
	}
	if cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty <= 0 {
		cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty = 60
	}
	if cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty <= 0 {
		cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty = 90
	}
	if cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty > cfg.CallCorrection.TemporalDecoder.SwitchPenalty {
		cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty = cfg.CallCorrection.TemporalDecoder.SwitchPenalty
	}
	if cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty > cfg.CallCorrection.TemporalDecoder.SwitchPenalty {
		cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty = cfg.CallCorrection.TemporalDecoder.SwitchPenalty
	}
	if cfg.CallCorrection.TemporalDecoder.MinScore < 0 {
		cfg.CallCorrection.TemporalDecoder.MinScore = 0
	}
	if cfg.CallCorrection.TemporalDecoder.MinMarginScore <= 0 {
		cfg.CallCorrection.TemporalDecoder.MinMarginScore = 80
	}
	cfg.CallCorrection.TemporalDecoder.OverflowAction = strings.ToLower(strings.TrimSpace(cfg.CallCorrection.TemporalDecoder.OverflowAction))
	if cfg.CallCorrection.TemporalDecoder.OverflowAction == "" {
		cfg.CallCorrection.TemporalDecoder.OverflowAction = "fallback_resolver"
	}
	switch cfg.CallCorrection.TemporalDecoder.OverflowAction {
	case "fallback_resolver", "abstain", "bypass":
	default:
		return fmt.Errorf("invalid call_correction.temporal_decoder.overflow_action %q (expected fallback_resolver, abstain, or bypass)", cfg.CallCorrection.TemporalDecoder.OverflowAction)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxPending <= 0 {
		cfg.CallCorrection.TemporalDecoder.MaxPending = 25000
	}
	if cfg.CallCorrection.TemporalDecoder.MaxActiveKeys <= 0 {
		cfg.CallCorrection.TemporalDecoder.MaxActiveKeys = 6000
	}
	if cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey <= 0 {
		cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey = 32
	}
	if cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey > 256 {
		cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey = 256
	}
	return nil
}

func normalizeCallCorrectionFamilyPolicyConfig(cfg *Config, presence loadRawPresence) {
	if !presence.hasFamilyTruncationEnabled {
		cfg.CallCorrection.FamilyPolicy.Truncation.Enabled = true
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta <= 0 {
		cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta = 1
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength <= 0 {
		cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength = 3
	}
	if !presence.hasFamilyTruncationPrefix {
		cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch = true
	}
	if !presence.hasFamilyTruncationSuffix {
		cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch = true
	}
	if !presence.hasFamilyTruncationRelaxEnabled {
		cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.Enabled = true
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage < 0 {
		cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage = 0
	}
	if !presence.hasFamilyTruncationRelaxCandidate {
		cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated = true
	}
	if !presence.hasFamilyTruncationRelaxSubject {
		cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated = true
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max < 0 {
		cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max = 0
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Enabled {
		if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max <= 0 {
			cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max = 1
		}
		if !presence.hasFamilyTruncationLengthBonusCandidate {
			cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.RequireCandidateValidated = true
		}
		if !presence.hasFamilyTruncationLengthBonusSubject {
			cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.RequireSubjectUnvalidated = true
		}
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent < 0 {
		cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent = 0
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.Enabled {
		if !presence.hasFamilyTruncationDelta2Candidate {
			cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.RequireCandidateValidated = true
		}
		if !presence.hasFamilyTruncationDelta2Subject {
			cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.RequireSubjectUnvalidated = false
		}
	}
	if !presence.hasFamilyTelnetSuppressionEnabled {
		cfg.CallCorrection.FamilyPolicy.TelnetSuppression.Enabled = true
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.WindowSeconds <= 0 {
		windowSeconds := cfg.CallCorrection.RecencySeconds
		if cfg.CallCorrection.RecencySecondsCW > windowSeconds {
			windowSeconds = cfg.CallCorrection.RecencySecondsCW
		}
		if cfg.CallCorrection.RecencySecondsRTTY > windowSeconds {
			windowSeconds = cfg.CallCorrection.RecencySecondsRTTY
		}
		cfg.CallCorrection.FamilyPolicy.TelnetSuppression.WindowSeconds = windowSeconds
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.MaxEntries <= 0 {
		cfg.CallCorrection.FamilyPolicy.TelnetSuppression.MaxEntries = cfg.CallCorrection.StabilizerMaxPending
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz <= 0 {
		cfg.CallCorrection.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz = cfg.CallCorrection.FrequencyToleranceHz
	}
	cfg.CallCorrection.StabilizerTimeoutAction = strings.ToLower(strings.TrimSpace(cfg.CallCorrection.StabilizerTimeoutAction))
	if cfg.CallCorrection.StabilizerTimeoutAction == "" {
		cfg.CallCorrection.StabilizerTimeoutAction = "release"
	}
}

func normalizeCallCorrectionWeightsAndAdaptiveConfig(cfg *Config, presence loadRawPresence) {
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
	if cfg.CallCorrection.AdaptiveMinReports.WindowMinutes <= 0 {
		cfg.CallCorrection.AdaptiveMinReports.WindowMinutes = 10
	}
	if cfg.CallCorrection.AdaptiveMinReports.EvaluationPeriodSeconds <= 0 {
		cfg.CallCorrection.AdaptiveMinReports.EvaluationPeriodSeconds = 60
	}
	if cfg.CallCorrection.AdaptiveMinReports.HysteresisWindows <= 0 {
		cfg.CallCorrection.AdaptiveMinReports.HysteresisWindows = 2
	}
	if !cfg.CallCorrection.AdaptiveMinReports.Enabled && !presence.hasAdaptiveMinReportsEnabled {
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
	cfg.CallCorrection.BandStateOverrides = normalizeBandStateOverrides(
		cfg.CallCorrection.BandStateOverrides,
		cfg.CallCorrection.FrequencyToleranceHz,
	)
}

func validateCallCorrectionStabilizerTimeoutAction(cfg *Config) error {
	switch cfg.CallCorrection.StabilizerTimeoutAction {
	case "release", "suppress":
		return nil
	default:
		return fmt.Errorf("invalid call_correction.stabilizer_timeout_action %q (expected release or suppress)", cfg.CallCorrection.StabilizerTimeoutAction)
	}
}

func normalizeCallCacheConfig(cfg *Config) {
	if cfg.CallCache.Size <= 0 {
		cfg.CallCache.Size = 4096
	}
	if cfg.CallCache.TTLSeconds <= 0 {
		cfg.CallCache.TTLSeconds = 600
	}
}

func normalizeTelnetConfig(cfg *Config, presence loadRawPresence) error {
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
	if cfg.Telnet.BulletinDedupeWindowSeconds < 0 {
		return fmt.Errorf("invalid telnet.bulletin_dedupe_window_seconds %d (must be >= 0)", cfg.Telnet.BulletinDedupeWindowSeconds)
	}
	if cfg.Telnet.BulletinDedupeMaxEntries < 0 {
		return fmt.Errorf("invalid telnet.bulletin_dedupe_max_entries %d (must be >= 0)", cfg.Telnet.BulletinDedupeMaxEntries)
	}
	if !presence.hasTelnetBulletinDedupeWindow {
		cfg.Telnet.BulletinDedupeWindowSeconds = 600
	}
	if cfg.Telnet.BulletinDedupeWindowSeconds > 0 {
		if !presence.hasTelnetBulletinDedupeMaxEntries {
			cfg.Telnet.BulletinDedupeMaxEntries = 4096
		}
		if cfg.Telnet.BulletinDedupeMaxEntries < 1 {
			return fmt.Errorf("invalid telnet.bulletin_dedupe_max_entries %d (must be >= 1 when bulletin dedupe is enabled)", cfg.Telnet.BulletinDedupeMaxEntries)
		}
	}
	if cfg.Telnet.RejectWorkers <= 0 {
		cfg.Telnet.RejectWorkers = 2
	}
	if cfg.Telnet.RejectQueueSize <= 0 {
		cfg.Telnet.RejectQueueSize = 1024
	}
	if cfg.Telnet.RejectWriteDeadlineMS <= 0 {
		cfg.Telnet.RejectWriteDeadlineMS = 500
	}
	if cfg.Telnet.WriterBatchMaxBytes <= 0 {
		cfg.Telnet.WriterBatchMaxBytes = 16384
	}
	if cfg.Telnet.WriterBatchWaitMS <= 0 {
		cfg.Telnet.WriterBatchWaitMS = 5
	}
	if cfg.Telnet.BroadcastBatchIntervalMS < 0 {
		return fmt.Errorf("invalid telnet.broadcast_batch_interval_ms %d (must be >= 0)", cfg.Telnet.BroadcastBatchIntervalMS)
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
	if cfg.Telnet.MaxPreloginSessions <= 0 {
		cfg.Telnet.MaxPreloginSessions = 256
	}
	if cfg.Telnet.PreloginTimeoutSeconds <= 0 {
		cfg.Telnet.PreloginTimeoutSeconds = 15
	}
	if cfg.Telnet.AcceptRatePerIP <= 0 {
		cfg.Telnet.AcceptRatePerIP = 3
	}
	if cfg.Telnet.AcceptBurstPerIP <= 0 {
		cfg.Telnet.AcceptBurstPerIP = 6
	}
	if cfg.Telnet.AcceptRatePerSubnet <= 0 {
		cfg.Telnet.AcceptRatePerSubnet = 24
	}
	if cfg.Telnet.AcceptBurstPerSubnet <= 0 {
		cfg.Telnet.AcceptBurstPerSubnet = 48
	}
	if cfg.Telnet.AcceptRateGlobal <= 0 {
		cfg.Telnet.AcceptRateGlobal = 300
	}
	if cfg.Telnet.AcceptBurstGlobal <= 0 {
		cfg.Telnet.AcceptBurstGlobal = 600
	}
	if cfg.Telnet.AcceptRatePerASN <= 0 {
		cfg.Telnet.AcceptRatePerASN = 40
	}
	if cfg.Telnet.AcceptBurstPerASN <= 0 {
		cfg.Telnet.AcceptBurstPerASN = 80
	}
	if cfg.Telnet.AcceptRatePerCountry <= 0 {
		cfg.Telnet.AcceptRatePerCountry = 120
	}
	if cfg.Telnet.AcceptBurstPerCountry <= 0 {
		cfg.Telnet.AcceptBurstPerCountry = 240
	}
	if cfg.Telnet.PreloginConcurrencyPerIP <= 0 {
		cfg.Telnet.PreloginConcurrencyPerIP = 3
	}
	if cfg.Telnet.AdmissionLogIntervalSeconds <= 0 {
		cfg.Telnet.AdmissionLogIntervalSeconds = 10
	}
	if cfg.Telnet.AdmissionLogMaxReasonLinesPerInterval <= 0 {
		cfg.Telnet.AdmissionLogMaxReasonLinesPerInterval = 20
	}
	if cfg.Telnet.PreloginConcurrencyPerIP > cfg.Telnet.MaxPreloginSessions {
		cfg.Telnet.PreloginConcurrencyPerIP = cfg.Telnet.MaxPreloginSessions
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
		return fmt.Errorf("invalid telnet.drop_extreme_rate %.2f (must be 0 < rate <= 1)", cfg.Telnet.DropExtremeRate)
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
		return fmt.Errorf("invalid telnet.output_line_length %d (minimum 65)", cfg.Telnet.OutputLineLength)
	}
	if cfg.WhoSpotsMe.WindowMinutes < 1 || cfg.WhoSpotsMe.WindowMinutes > 60 {
		return fmt.Errorf("invalid who_spots_me.window_minutes %d (must be between 1 and 60)", cfg.WhoSpotsMe.WindowMinutes)
	}
	if transport, ok := normalizeTelnetTransport(cfg.Telnet.Transport); ok {
		cfg.Telnet.Transport = transport
	} else {
		return fmt.Errorf("invalid telnet.transport %q (expected %q or %q)", cfg.Telnet.Transport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if echoMode, ok := normalizeTelnetEchoMode(cfg.Telnet.EchoMode); ok {
		cfg.Telnet.EchoMode = echoMode
	} else {
		return fmt.Errorf("invalid telnet.echo_mode %q (expected %q, %q, or %q)", cfg.Telnet.EchoMode, TelnetEchoServer, TelnetEchoLocal, TelnetEchoOff)
	}
	if handshakeMode, ok := normalizeTelnetHandshakeMode(cfg.Telnet.SkipHandshake); ok {
		cfg.Telnet.SkipHandshake = handshakeMode
	} else {
		return fmt.Errorf("invalid telnet.skip_handshake %q (expected %q, %q, or %q; legacy booleans false/true are also accepted)", cfg.Telnet.SkipHandshake, TelnetHandshakeFull, TelnetHandshakeMinimal, TelnetHandshakeNone)
	}
	return nil
}

func normalizeFeedTransportConfig(cfg *Config) error {
	// Provide operator-facing telnet prompts even when omitted from YAML.
	if transport, ok := normalizeTelnetTransport(cfg.RBN.TelnetTransport); ok {
		cfg.RBN.TelnetTransport = transport
	} else {
		return fmt.Errorf("invalid rbn.telnet_transport %q (expected %q or %q)", cfg.RBN.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if transport, ok := normalizeTelnetTransport(cfg.RBNDigital.TelnetTransport); ok {
		cfg.RBNDigital.TelnetTransport = transport
	} else {
		return fmt.Errorf("invalid rbn_digital.telnet_transport %q (expected %q or %q)", cfg.RBNDigital.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if transport, ok := normalizeTelnetTransport(cfg.HumanTelnet.TelnetTransport); ok {
		cfg.HumanTelnet.TelnetTransport = transport
	} else {
		return fmt.Errorf("invalid human_telnet.telnet_transport %q (expected %q or %q)", cfg.HumanTelnet.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	return nil
}

func normalizePeeringConfig(cfg *Config) error {
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
		return fmt.Errorf("invalid peering.telnet_transport %q (expected %q or %q)", cfg.Peering.TelnetTransport, TelnetTransportNative, TelnetTransportZiutek)
	}
	if cfg.Peering.KeepaliveSeconds < 0 {
		return fmt.Errorf("invalid peering.keepalive_seconds %d (must be >= 0)", cfg.Peering.KeepaliveSeconds)
	}
	if cfg.Peering.ConfigSeconds < 0 {
		return fmt.Errorf("invalid peering.config_seconds %d (must be >= 0)", cfg.Peering.ConfigSeconds)
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
	seenRemoteCalls := make(map[string]int)
	for i := range cfg.Peering.Peers {
		peer := &cfg.Peering.Peers[i]
		peer.Host = strings.TrimSpace(peer.Host)
		peer.Password = strings.TrimSpace(peer.Password)
		peer.LoginCallsign = strutil.NormalizeUpper(peer.LoginCallsign)
		peer.RemoteCallsign = strutil.NormalizeUpper(peer.RemoteCallsign)
		family, ok := normalizePeeringPeerFamily(peer.Family)
		if !ok {
			return fmt.Errorf("invalid peering.peers[%d].family %q (expected %q or %q)", i, peer.Family, PeeringPeerFamilyDXSpider, PeeringPeerFamilyCCluster)
		}
		peer.Family = family
		direction, ok := normalizePeeringPeerDirection(peer.Direction)
		if !ok {
			return fmt.Errorf("invalid peering.peers[%d].direction %q (expected %q, %q, or %q)", i, peer.Direction, PeeringPeerDirectionOutbound, PeeringPeerDirectionInbound, PeeringPeerDirectionBoth)
		}
		peer.Direction = direction
		switch peer.Direction {
		case PeeringPeerDirectionInbound:
			if peer.RemoteCallsign == "" {
				return fmt.Errorf("invalid peering.peers[%d].remote_callsign: required for inbound peers", i)
			}
		case PeeringPeerDirectionOutbound:
			if peer.Host == "" {
				return fmt.Errorf("invalid peering.peers[%d].host: required for outbound peers", i)
			}
			if peer.Port <= 0 {
				return fmt.Errorf("invalid peering.peers[%d].port: required for outbound peers", i)
			}
		case PeeringPeerDirectionBoth:
			if peer.RemoteCallsign == "" {
				return fmt.Errorf("invalid peering.peers[%d].remote_callsign: required for both-direction peers", i)
			}
			if peer.Host == "" {
				return fmt.Errorf("invalid peering.peers[%d].host: required for both-direction peers", i)
			}
			if peer.Port <= 0 {
				return fmt.Errorf("invalid peering.peers[%d].port: required for both-direction peers", i)
			}
		}
		if err := validatePeeringAllowIPs(fmt.Sprintf("peering.peers[%d].allow_ips", i), peer.AllowIPs); err != nil {
			return err
		}
		if peer.Enabled && peer.RemoteCallsign != "" {
			if prev, exists := seenRemoteCalls[peer.RemoteCallsign]; exists {
				return fmt.Errorf("invalid peering.peers[%d].remote_callsign: duplicate enabled peer callsign also used by peering.peers[%d]", i, prev)
			}
			seenRemoteCalls[peer.RemoteCallsign] = i
		}
	}
	return nil
}

func normalizeSignalPolicyConfig(cfg *Config) {
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
}

func normalizeReferenceDataConfig(cfg *Config, presence loadRawPresence) error {
	normalizeCTYConfig(cfg, presence)
	if err := normalizeFCCULSConfig(cfg); err != nil {
		return err
	}
	normalizeGridConfig(cfg)
	return nil
}

func normalizeCTYConfig(cfg *Config, presence loadRawPresence) {
	if strings.TrimSpace(cfg.CTY.File) == "" {
		cfg.CTY.File = "data/cty/cty.plist"
	}
	if strings.TrimSpace(cfg.CTY.URL) == "" {
		cfg.CTY.URL = "https://www.country-files.com/cty/cty.plist"
	}
	if cfg.CTY.RefreshUTC == "" {
		cfg.CTY.RefreshUTC = "00:45"
	}
	if !presence.ctyEnabledSet {
		cfg.CTY.Enabled = true
	}
}

func normalizeFCCULSConfig(cfg *Config) error {
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
		return fmt.Errorf("invalid FCC ULS refresh time %q: %w", cfg.FCCULS.RefreshUTC, err)
	}
	return nil
}

func normalizeGridConfig(cfg *Config) {
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
}

func normalizeDedupAndBufferConfig(cfg *Config, presence loadRawPresence) error {
	// Normalize dedup settings so the window drives behavior.
	defaultPolicy := strutil.NormalizeUpper(cfg.Dedup.DefaultPolicy)
	switch defaultPolicy {
	case "FAST", "MED", "SLOW":
		cfg.Dedup.DefaultPolicy = defaultPolicy
	default:
		return fmt.Errorf("invalid dedup.default_policy %q: must be FAST, MED, or SLOW", cfg.Dedup.DefaultPolicy)
	}
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
	if !presence.hasSecondaryFastWindow && cfg.Dedup.SecondaryFastWindowSeconds == 0 {
		cfg.Dedup.SecondaryFastWindowSeconds = 120
	}
	if !presence.hasSecondaryMedWindow && cfg.Dedup.SecondaryMedWindowSeconds == 0 {
		cfg.Dedup.SecondaryMedWindowSeconds = 300
	}
	if !presence.hasSecondarySlowWindow && cfg.Dedup.SecondarySlowWindowSeconds == 0 {
		cfg.Dedup.SecondarySlowWindowSeconds = 480
	}
	if !cfg.Dedup.SecondaryFastPreferStrong && !presence.hasSecondaryFastPrefer && cfg.Dedup.PreferStrongerSNR {
		cfg.Dedup.SecondaryFastPreferStrong = cfg.Dedup.PreferStrongerSNR
	}
	if !cfg.Dedup.SecondaryMedPreferStrong && !presence.hasSecondaryMedPrefer && cfg.Dedup.PreferStrongerSNR {
		cfg.Dedup.SecondaryMedPreferStrong = cfg.Dedup.PreferStrongerSNR
	}
	if !cfg.Dedup.SecondarySlowPreferStrong && !presence.hasSecondarySlowPrefer && cfg.Dedup.PreferStrongerSNR {
		cfg.Dedup.SecondarySlowPreferStrong = cfg.Dedup.PreferStrongerSNR
	}
	if cfg.Dedup.OutputBufferSize <= 0 {
		cfg.Dedup.OutputBufferSize = 1000
	}
	if cfg.Buffer.Capacity <= 0 {
		cfg.Buffer.Capacity = 300000
	}
	return nil
}

func normalizeSkewConfig(cfg *Config) error {
	// Skew fetch defaults keep the daily scheduler pointed at SM7IUN's published list.
	if strings.TrimSpace(cfg.Skew.URL) == "" {
		cfg.Skew.URL = "https://sm7iun.se/rbnskew.csv"
	}
	if strings.TrimSpace(cfg.Skew.File) == "" {
		cfg.Skew.File = "data/skm_correction/rbnskew.json"
	}
	if cfg.Skew.MinAbsSkew <= 0 {
		cfg.Skew.MinAbsSkew = 1
	}
	if strings.TrimSpace(cfg.Skew.RefreshUTC) == "" {
		cfg.Skew.RefreshUTC = "00:30"
	}
	if _, err := time.Parse("15:04", cfg.Skew.RefreshUTC); err != nil {
		return fmt.Errorf("invalid skew refresh time %q: %w", cfg.Skew.RefreshUTC, err)
	}
	return nil
}

func normalizeReputationConfig(cfg *Config, presence loadRawPresence) {
	if !cfg.Reputation.Enabled {
		return
	}
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
	if !presence.hasReputationIPInfoPebbleLoadIPv4 {
		cfg.Reputation.IPInfoPebbleLoadIPv4 = true
	}
	if !presence.hasReputationIPInfoDeleteCSVAfterImport {
		cfg.Reputation.IPInfoDeleteCSVAfterImport = true
	}
	if !presence.hasReputationIPInfoKeepGzip {
		cfg.Reputation.IPInfoKeepGzip = true
	}
	if !presence.hasReputationIPInfoPebbleCleanup {
		cfg.Reputation.IPInfoPebbleCleanup = true
	}
	if !presence.hasReputationIPInfoPebbleCompact {
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

// Purpose: Load and merge all YAML files from a config directory.
// Key aspects: Sorted file order provides deterministic overrides.
// Upstream: Load.
// Downstream: yaml.Unmarshal, mergeYAMLMaps.
func loadConfigDir(path string) (loadedConfigDir, LoadDiagnostics, error) {
	diagnostics := LoadDiagnostics{}
	entries, err := os.ReadDir(path)
	if err != nil {
		return loadedConfigDir{}, diagnostics, fmt.Errorf("failed to read config directory %q: %w", path, err)
	}

	files := make([]string, 0, len(entries))
	pathsByBase := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		base := strings.ToLower(entry.Name())
		if _, ok := configFileRegistry[base]; !ok {
			return loadedConfigDir{}, diagnostics, fmt.Errorf("unrecognized config file %q in %q", entry.Name(), path)
		}
		if _, exists := pathsByBase[base]; exists {
			return loadedConfigDir{}, diagnostics, fmt.Errorf("duplicate config file %q in %q", entry.Name(), path)
		}
		file := filepath.Join(path, entry.Name())
		files = append(files, file)
		pathsByBase[base] = file
	}
	sort.Strings(files)
	if len(files) == 0 {
		return loadedConfigDir{}, diagnostics, fmt.Errorf("no YAML files found in config directory %q", path)
	}

	merged := make(map[string]any)
	for _, file := range files {
		base := strings.ToLower(filepath.Base(file))
		spec := configFileRegistry[base]
		if spec.class != configFileMergedRuntime {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return loadedConfigDir{}, diagnostics, fmt.Errorf("failed to read config file %q: %w", file, err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal(data, &doc); err != nil {
			return loadedConfigDir{}, diagnostics, fmt.Errorf("failed to parse config file %q: %w", file, err)
		}
		unknown, err := yamlconfig.UnknownFieldDiagnostics(file, data, Config{})
		if err != nil {
			return loadedConfigDir{}, diagnostics, fmt.Errorf("failed to inspect config file %q: %w", file, err)
		}
		diagnostics.appendYAMLDiagnostics(unknown)
		merged = mergeYAMLMaps(merged, doc)
	}
	return loadedConfigDir{
		merged:      merged,
		files:       files,
		pathsByBase: pathsByBase,
	}, diagnostics, nil
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

// Print prints a human-readable configuration summary.
// Key aspects: Focuses on operationally relevant fields.
// Upstream: main.go startup logging.
// Downstream: fmt.Printf.
func (c *Config) Print() {
	fmt.Printf("Server: %s (%s)\n", c.Server.Name, c.Server.NodeID)
	workerDesc := "auto"
	if c.Telnet.BroadcastWorkers > 0 {
		workerDesc = fmt.Sprintf("%d", c.Telnet.BroadcastWorkers)
	}
	fmt.Printf("Telnet: port %d (transport=%s echo_mode=%s broadcast workers=%s queue=%d worker_queue=%d client_buffer=%d skip_handshake=%s)\n",
		c.Telnet.Port,
		c.Telnet.Transport,
		c.Telnet.EchoMode,
		workerDesc,
		c.Telnet.BroadcastQueue,
		c.Telnet.WorkerQueue,
		c.Telnet.ClientBuffer,
		c.Telnet.SkipHandshake)
	fmt.Printf("Telnet writer/reject: writer_batch=%dB/%dms reject_workers=%d reject_queue=%d reject_deadline=%dms\n",
		c.Telnet.WriterBatchMaxBytes,
		c.Telnet.WriterBatchWaitMS,
		c.Telnet.RejectWorkers,
		c.Telnet.RejectQueueSize,
		c.Telnet.RejectWriteDeadlineMS)
	fmt.Printf("Telnet bulletins: dedupe_window=%ds dedupe_max_entries=%d\n",
		c.Telnet.BulletinDedupeWindowSeconds,
		c.Telnet.BulletinDedupeMaxEntries)
	fmt.Printf("Go runtime: memory_limit_mib=%d gc_percent=%d max_procs=%d\n",
		c.GoRuntime.MemoryLimitMiB,
		c.GoRuntime.GCPercent,
		c.GoRuntime.MaxProcs)
	fmt.Printf("Who spots me: window=%dm\n", c.WhoSpotsMe.WindowMinutes)
	fmt.Printf("Telnet Tier-A: prelogin_max=%d prelogin_timeout=%ds ip_rate=%.2f/s ip_burst=%d subnet_rate=%.2f/s subnet_burst=%d global_rate=%.2f/s global_burst=%d ip_concurrency=%d\n",
		c.Telnet.MaxPreloginSessions,
		c.Telnet.PreloginTimeoutSeconds,
		c.Telnet.AcceptRatePerIP,
		c.Telnet.AcceptBurstPerIP,
		c.Telnet.AcceptRatePerSubnet,
		c.Telnet.AcceptBurstPerSubnet,
		c.Telnet.AcceptRateGlobal,
		c.Telnet.AcceptBurstGlobal,
		c.Telnet.PreloginConcurrencyPerIP)
	fmt.Printf("Telnet Tier-A geo: asn_rate=%.2f/s asn_burst=%d country_rate=%.2f/s country_burst=%d log_interval=%ds log_sample=%.2f max_lines=%d\n",
		c.Telnet.AcceptRatePerASN,
		c.Telnet.AcceptBurstPerASN,
		c.Telnet.AcceptRatePerCountry,
		c.Telnet.AcceptBurstPerCountry,
		c.Telnet.AdmissionLogIntervalSeconds,
		c.Telnet.AdmissionLogSampleRate,
		c.Telnet.AdmissionLogMaxReasonLinesPerInterval)
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
	fmt.Printf("Dropped-call logs: enabled=%t dir=%s retention_days=%d dedupe_window_seconds=%d files(bad_de_dx=%t no_license=%t harmonics=%t)\n",
		c.Logging.DroppedCalls.Enabled,
		c.Logging.DroppedCalls.Dir,
		c.Logging.DroppedCalls.RetentionDays,
		c.Logging.DroppedCalls.DedupeWindowSeconds,
		c.Logging.DroppedCalls.BadDEDX,
		c.Logging.DroppedCalls.NoLicense,
		c.Logging.DroppedCalls.Harmonics)
	fmt.Printf("File-only event logs: login=%t reputation=%t telnet=%t ingest=%t peer=%t\n",
		c.Logging.LoginAttempts.Enabled,
		c.Logging.ReputationDrops.Enabled,
		c.Logging.TelnetConnections.Enabled,
		c.Logging.IngestConnections.Enabled,
		c.Logging.PeerConnections.Enabled)
	fmt.Printf("Propagation log: enabled=%t dir=%s retention_days=%d\n",
		c.Logging.Propagation.Enabled,
		c.Logging.Propagation.Dir,
		c.Logging.Propagation.RetentionDays)
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
	if c.DXSummit.Enabled {
		fmt.Printf("DXSummit: %s (poll=%ds max_records=%d lookback=%ds bands=%s buffer=%d timeout=%dms)\n",
			c.DXSummit.BaseURL,
			c.DXSummit.PollIntervalSeconds,
			c.DXSummit.MaxRecordsPerPoll,
			c.DXSummit.LookbackSeconds,
			strings.Join(c.DXSummit.IncludeBands, ","),
			c.DXSummit.SpotChannelSize,
			c.DXSummit.RequestTimeoutMS)
	}
	if c.Archive.Enabled {
		fmt.Printf("Archive: %s (queue=%d batch=%d/%dms cleanup=%ds retention=%ds sync=%s auto_delete_corrupt=%t)\n",
			c.Archive.DBPath,
			c.Archive.QueueSize,
			c.Archive.BatchSize,
			c.Archive.BatchIntervalMS,
			c.Archive.CleanupIntervalSeconds,
			c.Archive.RetentionSeconds,
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
		fmt.Printf("PSKReporter: %s:%d (topic: %s buffer=%d workers=%s mqtt_inbound_workers=%s mqtt_inbound_queue=%s qos12_timeout=%dms)\n",
			c.PSKReporter.Broker,
			c.PSKReporter.Port,
			c.PSKReporter.Topic,
			c.PSKReporter.SpotChannelSize,
			workerDesc,
			mqttWorkerDesc,
			mqttQueueDesc,
			c.PSKReporter.MQTTQoS12EnqueueTimeoutMS)
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
	fmt.Printf("Dedup: cluster=%s (prefer_stronger=%t) secondary_fast=%s (prefer_stronger=%t) secondary_med=%s (prefer_stronger=%t) secondary_slow=%s (prefer_stronger=%t) default_policy=%s\n",
		clusterWindow,
		c.Dedup.PreferStrongerSNR,
		secondaryFast,
		c.Dedup.SecondaryFastPreferStrong,
		secondaryMed,
		c.Dedup.SecondaryMedPreferStrong,
		secondarySlow,
		c.Dedup.SecondarySlowPreferStrong,
		c.Dedup.DefaultPolicy)
	fmt.Printf("Flood control: enabled=%t partition=%s log_interval=%ds rails=%s\n",
		c.FloodControl.Enabled,
		c.FloodControl.PartitionMode,
		c.FloodControl.LogIntervalSeconds,
		c.FloodControl.Rails.summary())
	if len(c.Filter.DefaultModes) > 0 {
		fmt.Printf("Default modes: %s\n", strings.Join(c.Filter.DefaultModes, ", "))
	}
	if len(c.Filter.DefaultSources) > 0 {
		fmt.Printf("Default sources: %s\n", strings.Join(c.Filter.DefaultSources, ", "))
	}
	if c.Peering.Enabled {
		fmt.Printf("Peering: listen_port=%d peers=%d hop=%d transport=%s keepalive=%ds config=%ds forward_spots=%t topology=%s retention=%dh\n",
			c.Peering.ListenPort,
			len(c.Peering.Peers),
			c.Peering.HopCount,
			c.Peering.TelnetTransport,
			c.Peering.KeepaliveSeconds,
			c.Peering.ConfigSeconds,
			c.Peering.ForwardSpots,
			c.Peering.Topology.DBPath,
			c.Peering.Topology.RetentionHours)
	}
	fmt.Printf("Stats interval: %ds\n", c.Stats.DisplayIntervalSeconds)
	status := "disabled"
	if c.CallCorrection.Enabled {
		status = "enabled"
	}
	fmt.Printf("Call correction: %s (min_reports=%d slash_min_reports=%d advantage>%d confidence>=%d%% recency=%ds max_edit=%d tol=%.1fHz distance_cw=%s distance_rtty=%s invalid_action=%s d3_extra:+%d/+%d/+%d%%)\n",
		status,
		c.CallCorrection.MinConsensusReports,
		c.CallCorrection.FamilyPolicy.SlashPrecedenceMinReports,
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
	fmt.Printf("Call correction family policy: truncation(enabled=%t delta<=%d shorter_len>=%d prefix=%t suffix=%t relax_advantage=%t->%d validated(longer=%t shorter_unvalidated=%t) length_bonus=%t(max=%d validated=%t shorter_unvalidated=%t) delta2_rails=%t(extra_conf=%d validated=%t shorter_unvalidated=%t)) telnet_suppression(enabled=%t edit_neighbor=%t window=%ds max_entries=%d fallback_tol=%.1fHz)\n",
		c.CallCorrection.FamilyPolicy.Truncation.Enabled,
		c.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta,
		c.CallCorrection.FamilyPolicy.Truncation.MinShorterLength,
		c.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch,
		c.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch,
		c.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.Enabled,
		c.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage,
		c.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated,
		c.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated,
		c.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Enabled,
		c.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max,
		c.CallCorrection.FamilyPolicy.Truncation.LengthBonus.RequireCandidateValidated,
		c.CallCorrection.FamilyPolicy.Truncation.LengthBonus.RequireSubjectUnvalidated,
		c.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.Enabled,
		c.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent,
		c.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.RequireCandidateValidated,
		c.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.RequireSubjectUnvalidated,
		c.CallCorrection.FamilyPolicy.TelnetSuppression.Enabled,
		c.CallCorrection.FamilyPolicy.TelnetSuppression.EditNeighborEnabled,
		c.CallCorrection.FamilyPolicy.TelnetSuppression.WindowSeconds,
		c.CallCorrection.FamilyPolicy.TelnetSuppression.MaxEntries,
		c.CallCorrection.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz)
	fmt.Printf("Call correction voice: tol=%.0fHz\n",
		c.CallCorrection.VoiceFrequencyToleranceHz)
	stabilizerStatus := "disabled"
	if c.CallCorrection.StabilizerEnabled {
		stabilizerStatus = "enabled"
	}
	fmt.Printf("Call correction stabilizer: %s (delay=%ds max_checks=%d timeout_action=%s max_pending=%d p_delay_pct=%d p_delay_checks=%d ambiguous_checks=%d edit_neighbor=%t edit_neighbor_checks=%d edit_neighbor_min_spotters=%d resolver_neighborhood=%t resolver_neighborhood_radius=%d resolver_neighborhood_max_distance=%d resolver_neighborhood_allow_trunc=%t resolver_recent_plus1=%t min_unique=%d subject_weaker=%t max_distance=%d allow_trunc=%t)\n",
		stabilizerStatus,
		c.CallCorrection.StabilizerDelaySeconds,
		c.CallCorrection.StabilizerMaxChecks,
		c.CallCorrection.StabilizerTimeoutAction,
		c.CallCorrection.StabilizerMaxPending,
		c.CallCorrection.StabilizerPDelayConfidencePercent,
		c.CallCorrection.StabilizerPDelayMaxChecks,
		c.CallCorrection.StabilizerAmbiguousMaxChecks,
		c.CallCorrection.StabilizerEditNeighborEnabled,
		c.CallCorrection.StabilizerEditNeighborMaxChecks,
		c.CallCorrection.StabilizerEditNeighborMinSpotters,
		c.CallCorrection.ResolverNeighborhoodEnabled,
		c.CallCorrection.ResolverNeighborhoodBucketRadius,
		c.CallCorrection.ResolverNeighborhoodMaxDistance,
		c.CallCorrection.ResolverNeighborhoodAllowTruncation,
		c.CallCorrection.ResolverRecentPlus1Enabled,
		c.CallCorrection.ResolverRecentPlus1MinUniqueWinner,
		c.CallCorrection.ResolverRecentPlus1RequireSubjectWeaker,
		c.CallCorrection.ResolverRecentPlus1MaxDistance,
		c.CallCorrection.ResolverRecentPlus1AllowTruncation)
	bayesStatus := "disabled"
	if c.CallCorrection.BayesBonus.Enabled {
		bayesStatus = "enabled"
	}
	fmt.Printf("Call correction bayes bonus: %s (w_d1=%d w_d2=%d tau_w=%d alpha_recent=%d obs_cap=%d prior=[%d,%d] report_thr=[%d,%d] adv_thr=[%d,%d] adv_delta_w=[%d,%d] adv_conf=[%d,%d] require_candidate_validated=%t require_subject_unvalidated_d2=%t)\n",
		bayesStatus,
		c.CallCorrection.BayesBonus.WeightDistance1Milli,
		c.CallCorrection.BayesBonus.WeightDistance2Milli,
		c.CallCorrection.BayesBonus.WeightedSmoothingMilli,
		c.CallCorrection.BayesBonus.RecentSmoothing,
		c.CallCorrection.BayesBonus.ObsLogCapMilli,
		c.CallCorrection.BayesBonus.PriorLogMinMilli,
		c.CallCorrection.BayesBonus.PriorLogMaxMilli,
		c.CallCorrection.BayesBonus.ReportThresholdDistance1Milli,
		c.CallCorrection.BayesBonus.ReportThresholdDistance2Milli,
		c.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli,
		c.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli,
		c.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli,
		c.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli,
		c.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1,
		c.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2,
		c.CallCorrection.BayesBonus.RequireCandidateValidated,
		c.CallCorrection.BayesBonus.RequireSubjectUnvalidatedDistance2)
	temporalStatus := "disabled"
	if c.CallCorrection.TemporalDecoder.Enabled {
		temporalStatus = "enabled"
	}
	fmt.Printf("Call correction temporal decoder: %s (scope=%s lag=%ds max_wait=%ds beam=%d max_obs=%d stay_bonus=%d switch_penalty=%d family_penalty=%d edit1_penalty=%d min_score=%d min_margin=%d overflow_action=%s max_pending=%d max_active_keys=%d max_events_per_key=%d)\n",
		temporalStatus,
		c.CallCorrection.TemporalDecoder.Scope,
		c.CallCorrection.TemporalDecoder.LagSeconds,
		c.CallCorrection.TemporalDecoder.MaxWaitSeconds,
		c.CallCorrection.TemporalDecoder.BeamSize,
		c.CallCorrection.TemporalDecoder.MaxObsCandidates,
		c.CallCorrection.TemporalDecoder.StayBonus,
		c.CallCorrection.TemporalDecoder.SwitchPenalty,
		c.CallCorrection.TemporalDecoder.FamilySwitchPenalty,
		c.CallCorrection.TemporalDecoder.Edit1SwitchPenalty,
		c.CallCorrection.TemporalDecoder.MinScore,
		c.CallCorrection.TemporalDecoder.MinMarginScore,
		c.CallCorrection.TemporalDecoder.OverflowAction,
		c.CallCorrection.TemporalDecoder.MaxPending,
		c.CallCorrection.TemporalDecoder.MaxActiveKeys,
		c.CallCorrection.TemporalDecoder.MaxEventsPerKey)
	fmt.Printf("Call correction FT corroboration: p_unique=%d v_unique=%d ft8=%ds/%ds ft4=%ds/%ds ft2=%ds/%ds\n",
		c.CallCorrection.PMinUniqueSpotters,
		c.CallCorrection.VMinUniqueSpotters,
		c.CallCorrection.FT8QuietGapSeconds,
		c.CallCorrection.FT8HardCapSeconds,
		c.CallCorrection.FT4QuietGapSeconds,
		c.CallCorrection.FT4HardCapSeconds,
		c.CallCorrection.FT2QuietGapSeconds,
		c.CallCorrection.FT2HardCapSeconds)

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
		fmt.Printf("Skew: refresh %s UTC (min_abs_skew=%g source=%s)\n", c.Skew.RefreshUTC, c.Skew.MinAbsSkew, c.Skew.URL)
	}
	fmt.Printf("Ring buffer capacity: %d spots\n", c.Buffer.Capacity)
}

// Purpose: Normalize per-band-state overrides for correction settings.
// Key aspects: Applies default frequency tolerances when overrides are missing.
// Upstream: Load config normalization.
// Downstream: None.
func normalizeBandStateOverrides(overrides []BandStateOverride, defaultTol float64) []BandStateOverride {
	if len(overrides) == 0 {
		return overrides
	}
	out := make([]BandStateOverride, 0, len(overrides))
	for _, o := range overrides {
		normalized := o
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
	_, ok := yamlValueAt(raw, path...)
	return ok
}

func yamlValueAt(raw map[string]any, path ...string) (any, bool) {
	if len(path) == 0 || raw == nil {
		return nil, false
	}
	current := raw
	for i, key := range path {
		val, ok := current[key]
		if !ok {
			return nil, false
		}
		if i == len(path)-1 {
			return val, true
		}
		next, ok := val.(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return nil, false
}
