package config

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"dxcluster/internal/yamlconfig"
)

const (
	pathReliabilityConfigFile = "path_reliability.yaml"
	solarWeatherConfigFile    = "solarweather.yaml"
	iaruRegionsConfigFile     = "iaru_regions.yaml"
	iaruModeInferenceFile     = "iaru_mode_inference.yaml"
	spotTaxonomyConfigFile    = "spot_taxonomy.yaml"
	openAIConfigFile          = "openai.yaml"
	toxicityConfigFile        = "toxicity.yaml"
	toxicitySafeGateFile      = "toxicity_safe_gate.yaml"
)

type configFileClass string

const (
	configFileMergedRuntime configFileClass = "merged_runtime"
	configFileFeatureRoot   configFileClass = "feature_root"
	configFileReference     configFileClass = "reference_table"
	configFileOptionalTool  configFileClass = "optional_tool"
)

type configFileSpec struct {
	class    configFileClass
	required bool
}

// configFileRegistry is a fixed startup-owned allowlist for YAML files under
// data/config. It prevents accidental runtime settings from entering the
// process through unregistered files or hidden merge behavior.
var configFileRegistry = map[string]configFileSpec{
	"app.yaml":                {class: configFileMergedRuntime, required: true},
	"archive.yaml":            {class: configFileMergedRuntime, required: true},
	"data.yaml":               {class: configFileMergedRuntime, required: true},
	"dedupe.yaml":             {class: configFileMergedRuntime, required: true},
	"floodcontrol.yaml":       {class: configFileMergedRuntime, required: true},
	"ingest.yaml":             {class: configFileMergedRuntime, required: true},
	"mode_seeds.yaml":         {class: configFileMergedRuntime, required: true},
	"peering.yaml":            {class: configFileMergedRuntime, required: true},
	"pipeline.yaml":           {class: configFileMergedRuntime, required: true},
	"prop_report.yaml":        {class: configFileMergedRuntime, required: true},
	"reputation.yaml":         {class: configFileMergedRuntime, required: true},
	"runtime.yaml":            {class: configFileMergedRuntime, required: true},
	pathReliabilityConfigFile: {class: configFileFeatureRoot, required: true},
	solarWeatherConfigFile:    {class: configFileFeatureRoot, required: true},
	iaruRegionsConfigFile:     {class: configFileReference, required: true},
	iaruModeInferenceFile:     {class: configFileReference, required: true},
	spotTaxonomyConfigFile:    {class: configFileReference, required: true},
	openAIConfigFile:          {class: configFileOptionalTool, required: false},
	toxicityConfigFile:        {class: configFileOptionalTool, required: false},
	toxicitySafeGateFile:      {class: configFileReference, required: false},
}

type loadedConfigDir struct {
	merged      map[string]any
	files       []string
	pathsByBase map[string]string
}

type LoadDiagnostics struct {
	Warnings []string
	Errors   []string
}

func (d *LoadDiagnostics) addWarning(message string) {
	if strings.TrimSpace(message) == "" {
		return
	}
	d.Warnings = append(d.Warnings, message)
}

func (d *LoadDiagnostics) addError(message string) {
	if strings.TrimSpace(message) == "" {
		return
	}
	d.Errors = append(d.Errors, message)
}

func (d *LoadDiagnostics) appendYAMLDiagnostics(diags []yamlconfig.Diagnostic) {
	for _, diag := range diags {
		message := diag.Error()
		switch diag.Severity {
		case yamlconfig.DiagnosticWarning:
			d.addWarning(message)
		default:
			d.addError(message)
		}
	}
}

func (d *LoadDiagnostics) sort() {
	sort.Strings(d.Warnings)
	sort.Strings(d.Errors)
}

func (d LoadDiagnostics) HasErrors() bool {
	return len(d.Errors) > 0
}

func (d LoadDiagnostics) Error() error {
	if !d.HasErrors() {
		return nil
	}
	return DiagnosticError{Diagnostics: d}
}

type DiagnosticError struct {
	Diagnostics LoadDiagnostics
}

func (e DiagnosticError) Error() string {
	switch len(e.Diagnostics.Errors) {
	case 0:
		return "config diagnostics failed"
	case 1:
		return e.Diagnostics.Errors[0]
	default:
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "config diagnostics failed with %d error(s):", len(e.Diagnostics.Errors))
		for _, err := range e.Diagnostics.Errors {
			fmt.Fprintf(&buf, "\n- %s", err)
		}
		return buf.String()
	}
}

func IsDiagnosticError(err error) bool {
	var diagErr DiagnosticError
	return errors.As(err, &diagErr)
}

func requiredConfigFileDiagnostics(loaded loadedConfigDir) []string {
	missing := make([]string, 0)
	for base, spec := range configFileRegistry {
		if !spec.required {
			continue
		}
		if _, ok := loaded.pathsByBase[base]; !ok {
			missing = append(missing, fmt.Sprintf("required config file %q not found in config directory", base))
		}
	}
	sort.Strings(missing)
	return missing
}

func removedRuntimeKeyDiagnostics(raw map[string]any) []string {
	removed := []string{
		"archive.retention_ft_seconds",
		"archive.retention_default_seconds",
		"archive.cleanup_batch_size",
		"archive.cleanup_batch_yield_ms",
		"archive.busy_timeout_ms",
		"archive.preflight_timeout_ms",
		"pskreporter.modes",
		"pskreporter.path_only_modes",
	}
	diagnostics := make([]string, 0)
	for _, path := range removed {
		parts := strings.Split(path, ".")
		if yamlKeyPresent(raw, parts...) {
			diagnostics = append(diagnostics, removedRuntimeKeyMessage(path))
		}
	}
	return diagnostics
}

func removedRuntimeKeyMessage(path string) string {
	switch path {
	case "archive.retention_ft_seconds", "archive.retention_default_seconds":
		return fmt.Sprintf("removed YAML setting %q: use archive.retention_seconds", path)
	case "archive.cleanup_batch_size", "archive.cleanup_batch_yield_ms":
		return fmt.Sprintf("removed YAML setting %q: archive cleanup now uses timestamp range deletion; remove this key", path)
	case "archive.busy_timeout_ms", "archive.preflight_timeout_ms":
		return fmt.Sprintf("removed YAML setting %q: Pebble archive compatibility fields are no longer supported; remove this key", path)
	default:
		return fmt.Sprintf("removed YAML setting %q: define PSKReporter mode routing in spot_taxonomy.yaml using pskreporter_route", path)
	}
}

var runtimePresenceExemptPaths = map[string]struct{}{
	// These fields are shared by the flood rail struct but are intentionally
	// absent from simple rails that are validated by normalizeFloodControlConfig.
	"flood_control.rails.decall.active_mode":                {},
	"flood_control.rails.decall.thresholds_by_mode":         {},
	"flood_control.rails.source_node.active_mode":           {},
	"flood_control.rails.source_node.thresholds_by_mode":    {},
	"flood_control.rails.spotter_ip.active_mode":            {},
	"flood_control.rails.spotter_ip.thresholds_by_mode":     {},
	"flood_control.rails.dxcall.thresholds_per_source_type": {},
	"dedup.secondary_window_seconds":                        {},
	"dedup.secondary_prefer_stronger_snr":                   {},
	"call_correction.slash_precedence_min_reports":          {},
}

var runtimeAllowEmptySettings = map[string]struct{}{
	"telnet.welcome_message":         {},
	"telnet.dialect_welcome_message": {},
	"telnet.path_status_message":     {},
	"peering.topology.db_path":       {},
	"reputation.ipinfo_api_token":    {},
}

var runtimeAllowZeroSettings = map[string]struct{}{
	"ui.refresh_ms":                                                            {},
	"go_runtime.memory_limit_mib":                                              {},
	"go_runtime.gc_percent":                                                    {},
	"go_runtime.max_procs":                                                     {},
	"rbn.keepalive_seconds":                                                    {},
	"rbn_digital.keepalive_seconds":                                            {},
	"human_telnet.keepalive_seconds":                                           {},
	"telnet.broadcast_workers":                                                 {},
	"telnet.broadcast_batch_interval_ms":                                       {},
	"telnet.keepalive_seconds":                                                 {},
	"telnet.admission_log_sample_rate":                                         {},
	"telnet.bulletin_dedupe_window_seconds":                                    {},
	"logging.drop_dedupe_window_seconds":                                       {},
	"logging.dropped_calls.dedupe_window_seconds":                              {},
	"logging.login_attempts.retention_days":                                    {},
	"logging.login_attempts.dedupe_window_seconds":                             {},
	"logging.reputation_drops.retention_days":                                  {},
	"logging.reputation_drops.dedupe_window_seconds":                           {},
	"logging.telnet_connections.retention_days":                                {},
	"logging.telnet_connections.dedupe_window_seconds":                         {},
	"logging.ingest_connections.retention_days":                                {},
	"logging.ingest_connections.dedupe_window_seconds":                         {},
	"logging.peer_connections.retention_days":                                  {},
	"logging.peer_connections.dedupe_window_seconds":                           {},
	"dedup.cluster_window_seconds":                                             {},
	"dedup.secondary_fast_window_seconds":                                      {},
	"dedup.secondary_med_window_seconds":                                       {},
	"dedup.secondary_slow_window_seconds":                                      {},
	"pskreporter.workers":                                                      {},
	"pskreporter.mqtt_inbound_workers":                                         {},
	"pskreporter.mqtt_inbound_queue_depth":                                     {},
	"pskreporter.mqtt_qos12_enqueue_timeout_ms":                                {},
	"dxsummit.startup_backfill_seconds":                                        {},
	"grid_cache_ttl_seconds":                                                   {},
	"grid_ttl_days":                                                            {},
	"peering.keepalive_seconds":                                                {},
	"peering.config_seconds":                                                   {},
	"call_correction.recency_seconds_cw":                                       {},
	"call_correction.recency_seconds_rtty":                                     {},
	"call_correction.distance3_extra_reports":                                  {},
	"call_correction.distance3_extra_advantage":                                {},
	"call_correction.confusion_model_weight":                                   {},
	"call_correction.min_spotter_reliability":                                  {},
	"call_correction.stabilizer_p_delay_confidence_percent":                    {},
	"call_correction.stabilizer_p_delay_max_checks":                            {},
	"call_correction.stabilizer_ambiguous_max_checks":                          {},
	"call_correction.stabilizer_edit_neighbor_max_checks":                      {},
	"call_correction.family_policy.truncation.relax_advantage.min_advantage":   {},
	"call_correction.bayes_bonus.weight_distance1_milli":                       {},
	"call_correction.bayes_bonus.weight_distance2_milli":                       {},
	"call_correction.bayes_bonus.prior_log_min_milli":                          {},
	"call_correction.bayes_bonus.advantage_min_weighted_delta_distance1_milli": {},
	"call_correction.bayes_bonus.advantage_min_weighted_delta_distance2_milli": {},
	"call_correction.bayes_bonus.advantage_extra_confidence_distance1":         {},
	"call_correction.bayes_bonus.advantage_extra_confidence_distance2":         {},
	"call_correction.temporal_decoder.min_score":                               {},
}

var runtimeAllowNegativeSettings = map[string]struct{}{
	"call_correction.bayes_bonus.prior_log_min_milli": {},
}

func mergedRuntimePresenceDiagnostics(raw map[string]any) []string {
	diagnostics := make([]string, 0)
	collectStructPresenceDiagnostics(raw, reflect.TypeOf(Config{}), nil, &diagnostics)
	sort.Strings(diagnostics)
	return diagnostics
}

func collectStructPresenceDiagnostics(raw map[string]any, t reflect.Type, prefix []string, diagnostics *[]string) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		key, ok := yamlFieldName(field)
		if !ok {
			continue
		}
		path := append(append([]string(nil), prefix...), key)
		joined := strings.Join(path, ".")
		if _, exempt := runtimePresenceExemptPaths[joined]; exempt {
			continue
		}
		value, present := yamlValueAt(raw, path...)
		if !present {
			*diagnostics = append(*diagnostics, fmt.Sprintf("required YAML setting %q is missing", joined))
			continue
		}
		if value == nil {
			*diagnostics = append(*diagnostics, fmt.Sprintf("required YAML setting %q must not be null", joined))
			continue
		}
		fieldType := field.Type
		for fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Struct {
			collectStructPresenceDiagnostics(raw, fieldType, path, diagnostics)
		}
	}
}

func validateConfiguredRuntimeValues(cfg Config) error {
	if err := validateValueTree(reflect.ValueOf(cfg), reflect.TypeOf(cfg), nil); err != nil {
		return err
	}
	if cfg.Telnet.AdmissionLogSampleRate < 0 || cfg.Telnet.AdmissionLogSampleRate > 1 {
		return fmt.Errorf("invalid YAML setting %q: must be between 0 and 1", "telnet.admission_log_sample_rate")
	}
	return nil
}

func validateValueTree(value reflect.Value, t reflect.Type, prefix []string) error {
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
		t = value.Type()
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		key, ok := yamlFieldName(field)
		if !ok {
			continue
		}
		path := append(append([]string(nil), prefix...), key)
		joined := strings.Join(path, ".")
		if _, exempt := runtimePresenceExemptPaths[joined]; exempt {
			continue
		}
		fieldValue := value.Field(i)
		fieldType := field.Type
		for fieldType.Kind() == reflect.Pointer {
			if fieldValue.IsNil() {
				return fmt.Errorf("required YAML setting %q must not be null", joined)
			}
			fieldValue = fieldValue.Elem()
			fieldType = fieldType.Elem()
		}
		switch fieldType.Kind() {
		case reflect.Struct:
			if err := validateValueTree(fieldValue, fieldType, path); err != nil {
				return err
			}
		case reflect.String:
			if strings.TrimSpace(fieldValue.String()) == "" {
				if _, ok := runtimeAllowEmptySettings[joined]; !ok {
					return fmt.Errorf("invalid YAML setting %q: must not be empty", joined)
				}
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v := fieldValue.Int()
			if v < 0 {
				if _, ok := runtimeAllowNegativeSettings[joined]; !ok {
					return fmt.Errorf("invalid YAML setting %q: must be >= 0", joined)
				}
			} else if v == 0 {
				if _, ok := runtimeAllowZeroSettings[joined]; !ok {
					return fmt.Errorf("invalid YAML setting %q: must be > 0", joined)
				}
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if fieldValue.Uint() == 0 {
				if _, ok := runtimeAllowZeroSettings[joined]; !ok {
					return fmt.Errorf("invalid YAML setting %q: must be > 0", joined)
				}
			}
		case reflect.Float32, reflect.Float64:
			f := fieldValue.Float()
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return fmt.Errorf("invalid YAML setting %q: must be finite", joined)
			}
			if f < 0 {
				if _, ok := runtimeAllowNegativeSettings[joined]; !ok {
					return fmt.Errorf("invalid YAML setting %q: must be >= 0", joined)
				}
			} else if f == 0 {
				if _, ok := runtimeAllowZeroSettings[joined]; !ok {
					return fmt.Errorf("invalid YAML setting %q: must be > 0", joined)
				}
			}
		}
	}
	return nil
}

func yamlFieldName(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get("yaml")
	name := strings.Split(tag, ",")[0]
	switch {
	case name == "-":
		return "", false
	case name != "":
		return name, true
	default:
		return field.Name, true
	}
}
