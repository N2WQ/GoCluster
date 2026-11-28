package main

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"dxcluster/config"
	"dxcluster/internal/toxicity"
	"gopkg.in/yaml.v3"
)

// Public YAML can spell a new filename as simply "stations". Requiring a
// classification for each string setting avoids guessing from the spelling
// of either the key or its value. Wildcards occupy ONE map-key component and
// are limited to reference tables whose keys are data (regions, bands, ADIFs).
// This inventory describes the release boundary; actual settings, defaults
// and enablement still belong to config.Load and each feature's own loader.
var publicTextSettings = map[string]string{
	"app.yaml":     `server.name server.node_id ui.mode ui.v2.pages`,
	"archive.yaml": `archive.synchronous`,
	"data.yaml": `cty.url cty.refresh_utc fcc_uls.url fcc_uls.refresh_utc
		ised.url ised.special_url ised.refresh_utc skew.url skew.refresh_utc`,
	"dedupe.yaml": `dedup.default_policy dedup.secondary_frequency_changes.state`,
	"floodcontrol.yaml": `flood_control.partition_mode flood_control.rails.decall.action
		flood_control.rails.dxcall.action flood_control.rails.dxcall.active_mode
		flood_control.rails.source_node.action flood_control.rails.spotter_ip.action`,
	"iaru_mode_inference.yaml": `regions.*.*.class unknown_region_cw_safe.*.class voice_mode_by_band.*`,
	"iaru_regions.yaml":        `adif_overrides.* defaults_by_continent.*`,
	"ingest.yaml": `dxsummit.base_url dxsummit.include_bands dxsummit.name human_telnet.callsign
		human_telnet.host human_telnet.name human_telnet.telnet_transport pskreporter.broker pskreporter.name
		pskreporter.topic rbn.callsign rbn.host rbn.name rbn.telnet_transport rbn_digital.callsign
		rbn_digital.host rbn_digital.name rbn_digital.telnet_transport`,
	"mode_seeds.yaml": `mode_inference.digital_seeds.mode`,
	"path_reliability.yaml": `allowed_bands glyph_symbols.closed glyph_symbols.high glyph_symbols.insufficient
		glyph_symbols.low glyph_symbols.medium glyph_symbols.unlikely prop_prior_feeds.dst_fallback_url
		prop_prior_feeds.dst_primary_url prop_prior_feeds.hp30_fallback_url prop_prior_feeds.hp30_primary_url
		prop_prior_feeds.rtsw_mag_url prop_prior_feeds.rtsw_wind_url prop_prior_feeds.sfi_fallback_url
		prop_prior_feeds.sfi_primary_url prop_prior_feeds.user_agent receiver_contribution_mode
		voacap_fallback.output_name_prefix`,
	"peering.yaml": `peering.legacy_version peering.local_callsign peering.node_build peering.node_version
		peering.peers.direction peering.peers.family peering.peers.host peering.peers.login_callsign
		peering.peers.password peering.peers.remote_callsign peering.telnet_transport`,
	"pipeline.yaml": `call_correction.adaptive_min_reports.groups.bands call_correction.adaptive_min_reports.groups.name
		call_correction.distance_model_cw call_correction.distance_model_rtty call_correction.invalid_action
		call_correction.stabilizer_timeout_action call_correction.temporal_decoder.overflow_action
		call_correction.temporal_decoder.scope`,
	"prop_report.yaml": `prop_report.refresh_utc`,
	"reputation.yaml": `reputation.country_flip_scope reputation.ipinfo_api_base_url
		reputation.ipinfo_download_enabled reputation.ipinfo_download_url reputation.ipinfo_refresh_utc`,
	"runtime.yaml": `filter.default_modes filter.default_sources telnet.dialect_source_default_label
		telnet.dialect_source_persisted_label telnet.dialect_welcome_message telnet.duplicate_login_message
		telnet.echo_mode telnet.input_invalid_char_message telnet.input_too_long_message telnet.login_empty_message
		telnet.login_greeting telnet.login_invalid_message telnet.login_prompt telnet.path_status_message
		telnet.skip_handshake telnet.transport telnet.welcome_message web.hostname web.listen_address`,
	"solarweather.yaml": `g_levels.bands g_levels.name glyphs.g glyphs.r goes.energy_band goes.url
		kp.url r_levels.bands r_levels.name`,
	"spot_taxonomy.yaml": `events.display events.name events.reference_prefixes events.reference_suffix
		events.standalone_tokens modes.call_correction_profile modes.comment_tokens modes.custom_scp_bucket
		modes.display modes.ft_confidence.hard_cap_seconds_key modes.ft_confidence.quiet_gap_seconds_key
		modes.name modes.path_reliability_bucket modes.path_reliability_offset_mode modes.pskreporter_route
		modes.report_format modes.variants`,
	"toxicity.yaml":           `bearer_token_env endpoint`,
	"toxicity_safe_gate.yaml": `event_prefixes safe_tokens`,
	"voacap_experiment.yaml":  `output_name_prefix`,
}

// These node-local stores, downloaded snapshots, credentials and external
// installations may be named in public YAML but never authorize copying them.
var localSettings = map[string]string{
	"app.yaml": `logging.dir logging.dropped_calls.dir logging.ingest_connections.dir logging.login_attempts.dir
		logging.peer_connections.dir logging.propagation.dir logging.reputation_drops.dir logging.telnet_connections.dir`,
	"archive.yaml": `archive.db_path bulletin_archive.path`,
	"data.yaml": `fcc_uls.archive_path fcc_uls.db_path fcc_uls.temp_dir grid_db
		ised.archive_path ised.db_path ised.special_archive_path ised.temp_dir`,
	"path_reliability.yaml": `voacap_fallback.forecast_cache_db_path voacap_fallback.ssn_state_path voacap_fallback.voacap_home`,
	"pipeline.yaml":         `call_correction.custom_scp.path`,
	"reputation.yaml": `reputation.ipinfo_api_token reputation.ipinfo_download_token reputation.ipinfo_download_path
		reputation.ipinfo_pebble_path reputation.ipinfo_snapshot_path reputation.reputation_dir`,
	"voacap_experiment.yaml": `state_path voacap_home`,
}

type input struct {
	file, setting, value string
}

func startupInputs(cfg *config.Config, tc toxicity.Config) []input {
	cc := cfg.CallCorrection
	// Keep even disabled settings classified, but only ship what their
	// runtime consumers would load.
	ctyFile, confusion, gate, skew := "", "", "", ""
	if cfg.Skew.Enabled {
		skew = cfg.Skew.File
	}
	if cfg.CTY.Enabled {
		ctyFile = cfg.CTY.File
	}
	if cc.ConfusionModelEnabled {
		confusion = cc.ConfusionModelFile
	}
	if tc.Enabled {
		gate = tc.SafeGateFile
	}
	return []input{
		{"data.yaml", "h3_table_path", cfg.H3TablePath},
		{"data.yaml", "fcc_uls.allowlist_path", cfg.FCCULS.AllowlistPath},
		{"data.yaml", "cty.file", ctyFile},
		{"data.yaml", "skew.file", skew},
		{"peering.yaml", "peering.topology.db_path", cfg.Peering.Topology.DBPath},
		{"pipeline.yaml", "call_correction.spotter_reliability_file", cc.SpotterReliabilityFile},
		{"pipeline.yaml", "call_correction.spotter_reliability_file_cw", cc.SpotterReliabilityFileCW},
		{"pipeline.yaml", "call_correction.spotter_reliability_file_rtty", cc.SpotterReliabilityFileRTTY},
		{"pipeline.yaml", "call_correction.confusion_model_file", confusion},
		{"toxicity.yaml", "safe_gate_file", gate},
	}
}

// Accept portable, literal filenames only. A classification is permission
// for one role in one public tree, not permission to copy arbitrary Git data.
var (
	ctyName      = regexp.MustCompile(`^data/cty/[A-Za-z0-9_-]+\.plist$`)
	priorsTXT    = regexp.MustCompile(`^data/rbn/priors/[A-Za-z0-9_-]+\.txt$`)
	priorsJSON   = regexp.MustCompile(`^data/rbn/priors/[A-Za-z0-9_-]+\.json$`)
	h3Dir        = regexp.MustCompile(`^data/h3(/[A-Za-z0-9_-]+)*$`)
	gateName     = regexp.MustCompile(`^[A-Za-z0-9_-]+\.ya?ml$`)
	topologyName = regexp.MustCompile(`^data/peers/[A-Za-z0-9_-]+\.db$`)
	skewName     = regexp.MustCompile(`^data/skm_correction/[A-Za-z0-9_-]+\.json$`)
)

func (in input) paths() ([]string, error) {
	name := strings.ReplaceAll(strings.TrimSpace(in.value), `\`, "/")
	if name == "" {
		return nil, nil
	}
	if filepath.IsLocal(filepath.FromSlash(name)) {
		switch in.setting {
		case "h3_table_path":
			if h3Dir.MatchString(name) {
				return []string{name + "/res1.bin", name + "/res2.bin"}, nil
			}
		case "cty.file":
			if ctyName.MatchString(name) {
				return []string{name}, nil
			}
		case "fcc_uls.allowlist_path":
			if name == "data/fcc/allowlist.txt" {
				return []string{name}, nil
			}
		case "call_correction.confusion_model_file":
			if priorsJSON.MatchString(name) {
				return []string{name}, nil
			}
		case "call_correction.spotter_reliability_file", "call_correction.spotter_reliability_file_cw", "call_correction.spotter_reliability_file_rtty":
			if priorsTXT.MatchString(name) {
				return []string{name}, nil
			}
		case "safe_gate_file":
			if gateName.MatchString(name) && name != "openai.yaml" {
				return []string{path.Join("data/config", name)}, nil
			}
		case "peering.topology.db_path":
			if topologyName.MatchString(name) {
				return []string{name}, nil
			}
		case "skew.file":
			if skewName.MatchString(name) {
				return []string{name}, nil
			}
		}
	}
	return nil, fmt.Errorf("%s: %s selects disallowed public asset %q", in.file, in.setting, in.value)
}

func checkClassification(file string, data []byte, inputs []input, smoke bool) error {
	textKeys, ok := publicTextSettings[file]
	if !ok {
		return fmt.Errorf("unclassified public configuration file %s", file)
	}
	// Classify the effective document. Resolving the same YAML aliases and
	// merge keys as the configuration loader prevents them hiding new inputs.
	var effective map[string]any
	if err := yaml.Unmarshal(data, &effective); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	var document yaml.Node
	if err := document.Encode(effective); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	localPaths := strings.Fields(localSettings[file])
	for _, in := range inputs {
		if in.file == file {
			localPaths = append(localPaths, in.setting)
		}
	}
	return checkSettings(file, &document, nil, append(strings.Fields(textKeys), localPaths...), localPaths, smoke)
}

func checkSettings(file string, n *yaml.Node, keys, patterns, localPaths []string, smoke bool) error {
	// yaml.v3 can decode a bare number into a typed string. A path field
	// with a numeric, null or empty-sequence value still needs a classification.
	if len(keys) != 0 && (n.ShortTag() == "!!str" || fileSetting.MatchString(keys[len(keys)-1])) {
		if !slices.ContainsFunc(patterns, func(pattern string) bool {
			return slices.EqualFunc(strings.Split(pattern, "."), keys, func(p, key string) bool { return p == "*" || p == key })
		}) {
			return fmt.Errorf("%s: unclassified public setting %s; classify its release inputs", file, strings.Join(keys, "."))
		}
	}
	switch n.Kind {
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, child := range n.Content {
			if err := checkSettings(file, child, keys, patterns, localPaths, smoke); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		for i := 0; i < len(n.Content); i += 2 {
			if err := checkSettings(file, n.Content[i+1], append(keys, n.Content[i].Value), patterns, localPaths, smoke); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		key := strings.Join(keys, ".")
		if slices.Contains(localPaths, key) {
			if err := checkLocalSetting(key, n.Value, smoke); err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
		}
	}
	return nil
}

var fileSetting = regexp.MustCompile(`(?:^|_)(?:file|filename|path|dir|directory|database)(?:_cw|_rtty)?$`)

// The public example runs inside a fresh ZIP extraction. Local state and
// inactive file settings must stay there too, even when the builder runs on
// Linux for a Windows target. The two operator-owned interfaces below have
// separate runtime resolution rules and are neutralized by the smoke config.
func checkLocalSetting(key, value string, smoke bool) error {
	switch key {
	case "reputation.ipinfo_download_token":
		// The runtime requires a nonempty config value for the optional
		// download credential. Resolve only the smoke fixture's local reference.
		if smoke && value != "file:smoke-unused-token" {
			return fmt.Errorf("%s must name the local dummy reference in the smoke configuration", key)
		}
		return nil
	case "reputation.ipinfo_api_token":
		if smoke && value != "" {
			return fmt.Errorf("%s must be empty in the smoke configuration", key)
		}
		return nil
	case "voacap_home", "voacap_fallback.voacap_home":
		if !smoke {
			return nil
		}
	}
	name := strings.ReplaceAll(strings.TrimSpace(value), `\`, "/")
	if name == "" {
		return nil
	}
	for part := range strings.SplitSeq(name, "/") {
		if part == "" || part == "." || part == ".." || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") ||
			strings.ContainsAny(part, `:*?"<>|`+"\x00") || windowsDevice.MatchString(part) {
			return fmt.Errorf("%s is not a portable path inside the public extraction", key)
		}
	}
	return nil
}

var windowsDevice = regexp.MustCompile(`(?i)^(CON|PRN|AUX|NUL|CONIN\$|CONOUT\$|(?:COM|LPT)[1-9¹²³])(?:\.|$)`)
