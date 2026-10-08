// File role: Builds coherent, bounded human and machine configuration readbacks.
// Callers own the full-callsign stripe. Runtime status is captured before borrowing
// filter/path state, avoiding recursive read locks and exposing no preset baseline.
package telnet

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"dxcluster/filter"
)

type readbackPresetStatus struct {
	Associated bool   `yaml:"associated"`
	Name       string `yaml:"name"`
	Modified   bool   `yaml:"modified"`
}

type readbackEffectiveStatus struct {
	Dialect                 string `yaml:"dialect"`
	Grid                    string `yaml:"grid"`
	GridDerived             bool   `yaml:"grid_derived"`
	NoiseClass              string `yaml:"noise_class"`
	DedupePolicy            string `yaml:"dedupe_policy"`
	PathMinObservationCount int    `yaml:"path_min_observation_count"`
	SolarSummaryMinutes     int    `yaml:"solar_summary_minutes"`
	NearbyActive            bool   `yaml:"nearby_active"`
}

type readbackSessionStatus struct {
	Callsign             string `yaml:"callsign"`
	DiagnosticComments   string `yaml:"diagnostic_comments"`
	PauseActive          bool   `yaml:"pause_active"`
	PausePendingDelivery bool   `yaml:"pause_pending_delivery"`
	PauseRemaining       int    `yaml:"pause_remaining_seconds"`
	SuppressedSpots      uint64 `yaml:"suppressed_spots"`
	TemporaryDefaults    bool   `yaml:"temporary_defaults"`
}

type readbackServerStatus struct {
	DefaultDialect          string `yaml:"default_dialect"`
	DefaultDedupePolicy     string `yaml:"default_dedupe_policy"`
	DefaultNoiseClass       string `yaml:"default_noise_class"`
	PathMinObservationCount int    `yaml:"path_min_observation_count"`
	AutoReadPauseMinRows    int    `yaml:"auto_read_pause_min_rows"`
	AutoReadPauseSeconds    int    `yaml:"auto_read_pause_seconds"`
}

type configurationReadbackStatus struct {
	Configured filter.SettingsConfiguration `yaml:"configured"`
	Effective  readbackEffectiveStatus      `yaml:"effective"`
	Session    readbackSessionStatus        `yaml:"session"`
	Server     readbackServerStatus         `yaml:"server"`
	Preset     readbackPresetStatus         `yaml:"preset"`
	MinSNR     *readbackMinSNRStatus        `yaml:"min_snr,omitempty"`
	Comments   *readbackCommentStatus       `yaml:"comments,omitempty"`
}

type readbackCommentStatus struct {
	PassCount   int `yaml:"pass_count"`
	RejectCount int `yaml:"reject_count"`
}

type readbackMinSNRStatus struct {
	ConfiguredCount int      `yaml:"configured_count"`
	InactiveCount   int      `yaml:"inactive_count"`
	InactiveModes   []string `yaml:"inactive_modes"`
}

// Minimum activity and phrase counts come from exact saved rules. The
// schema gate preserves old status shapes; validation bounds the list before
// construction and the complete envelope budget includes its repeated keys.
func attachReadbackFilterStatus(status configurationReadbackStatus, cfg filter.Configuration, version int) (configurationReadbackStatus, error) {
	if version < 3 {
		return status, nil
	}
	if err := cfg.ValidateMinSNRRules(); err != nil {
		return status, err
	}
	activity := &readbackMinSNRStatus{ConfiguredCount: len(cfg.Filters.MinSNR), InactiveModes: make([]string, 0)}
	for key := range cfg.Filters.MinSNR {
		if !filter.IsActiveMinSNRMode(key) {
			activity.InactiveModes = append(activity.InactiveModes, key)
		}
	}
	sort.Strings(activity.InactiveModes)
	activity.InactiveCount = len(activity.InactiveModes)
	status.MinSNR = activity
	if version >= 4 {
		if err := cfg.ValidateCommentRules(); err != nil {
			return status, err
		}
		status.Comments = &readbackCommentStatus{PassCount: len(cfg.Filters.Comments), RejectCount: len(cfg.Filters.BlockComments)}
	}
	return status, nil
}

type yamlReadbackEnvelope struct {
	SchemaVersion int                         `yaml:"schema_version"`
	RequestID     string                      `yaml:"request_id"`
	Resource      string                      `yaml:"resource"`
	Revision      string                      `yaml:"revision"`
	Configuration any                         `yaml:"configuration"`
	Status        configurationReadbackStatus `yaml:"status"`
}

func readbackDiagnosticName(mode diagMode) string {
	switch mode {
	case diagModeDedupe:
		return "DEDUPE"
	case diagModeSource:
		return "SOURCE"
	case diagModeConfidence:
		return "CONF"
	case diagModePath:
		return "PATH"
	case diagModeMode:
		return "MODE"
	default:
		return "OFF"
	}
}

// Prepared runtime status belongs to the candidate; session controls always
// belong to the real client. This prevents admission from describing old GRID,
// NEARBY, or dedupe state alongside a proposed configuration.
func (s *Server) captureReadbackStatus(c, prepared *Client) configurationReadbackStatus {
	if prepared == nil {
		prepared = c
	}
	state := prepared.pathSnapshot()
	now := s.now().UnixNano()
	c.readPauseMu.Lock()
	until, pending := c.readPauseUntilUnixNano.Load(), c.readPausePending.Load()
	var remaining time.Duration
	if until > now {
		remaining = time.Duration(until - now)
	}
	active, suppressed := pending || remaining > 0, c.readPauseSuppressed.Load()
	c.readPauseMu.Unlock()
	status := configurationReadbackStatus{
		Effective: readbackEffectiveStatus{
			Dialect: string(prepared.dialect), Grid: state.grid, GridDerived: state.gridDerived,
			NoiseClass: state.noiseClass, DedupePolicy: s.effectiveDedupePolicyForClient(prepared).label(),
			PathMinObservationCount: max(state.pathMinObservationCount, s.pathPredictorMinObservationCount()),
			SolarSummaryMinutes:     prepared.getSolarSummaryMinutes(), NearbyActive: prepared.nearbyFilterInEffect(),
		},
		Session: readbackSessionStatus{
			Callsign: c.callsign, DiagnosticComments: readbackDiagnosticName(c.getDiagMode()),
			PauseActive: active, PausePendingDelivery: pending,
			PauseRemaining: durationCeilSeconds(remaining), SuppressedSpots: suppressed,
			TemporaryDefaults: c.recordProtected,
		},
		Server: readbackServerStatus{
			DefaultDialect: string(s.configurationDefaultDialect()), DefaultDedupePolicy: s.effectiveDefaultDedupePolicy().label(),
			DefaultNoiseClass: "QUIET", PathMinObservationCount: s.pathPredictorMinObservationCount(),
		},
	}
	if s != nil {
		status.Server.AutoReadPauseMinRows = s.autoReadPauseMinRows
		status.Server.AutoReadPauseSeconds = durationCeilSeconds(s.autoReadPauseDuration)
	}
	return status
}

func attachReadbackConfiguration(status configurationReadbackStatus, c *Client, cfg filter.Configuration) configurationReadbackStatus {
	status.Configured = cfg.Settings
	if c.presetReference != nil {
		status.Preset.Associated, status.Preset.Name = true, c.presetReference.Name
		// Equal checks collection lengths before allocating its bounded pattern
		// multiset. The immutable preset baseline bounds any equal-length input.
		status.Preset.Modified = !cfg.Equal(filter.ConfigurationFromPreset(c.presetReference.Baseline))
	}
	return status
}

func readbackResourceConfiguration(cfg filter.Configuration, resource string) (filter.Configuration, any, error) {
	switch resource {
	case "FILTER":
		return filter.Configuration{Filters: cfg.Filters}, cfg.Filters, nil
	case "SETTINGS":
		return filter.Configuration{Settings: cfg.Settings}, cfg.Settings, nil
	case "CONFIG":
		return cfg, cfg, nil
	default:
		return filter.Configuration{}, nil, fmt.Errorf("unknown YAML resource %q", resource)
	}
}

func readbackMetadataFits(status configurationReadbackStatus, requestID, revision string) bool {
	remaining := maxYAMLBytes
	if status.MinSNR != nil {
		for _, key := range status.MinSNR.InactiveModes {
			if len(key) > remaining {
				return false
			}
			remaining -= len(key)
		}
	}
	for _, value := range []string{
		requestID, revision, status.Preset.Name, status.Session.Callsign, status.Session.DiagnosticComments,
		status.Configured.Dialect, status.Configured.Grid, status.Configured.NoiseClass, status.Configured.DedupePolicy,
		status.Effective.Dialect, status.Effective.Grid, status.Effective.NoiseClass, status.Effective.DedupePolicy,
		status.Server.DefaultDialect, status.Server.DefaultDedupePolicy, status.Server.DefaultNoiseClass,
	} {
		if len(value) > remaining {
			return false
		}
		remaining -= len(value)
	}
	return true
}

// renderYAMLReadback is called under transaction ownership. Preflight guards
// borrowed state before the encoder can sort maps or build its node tree.
func (s *Server) renderYAMLReadback(c *Client, resource, requestID, revision string) (string, error) {
	return s.renderYAMLReadbackVersion(c, resource, requestID, revision, 1)
}

func (s *Server) renderYAMLReadbackVersion(c *Client, resource, requestID, revision string, version int) (string, error) {
	version = machineSchemaVersion(version)
	resource = strings.ToUpper(resource)
	if c == nil {
		return "", fmt.Errorf("configuration unavailable")
	}
	status := s.captureReadbackStatus(c, nil)
	var response string
	err := c.withBorrowedConfiguration(func(cfg filter.Configuration) error {
		if err := cfg.ValidateCommentRules(); err != nil {
			return err
		}
		status = attachReadbackConfiguration(status, c, cfg)
		var err error
		status, err = attachReadbackFilterStatus(status, cfg, version)
		if err != nil {
			return err
		}
		if !readbackMetadataFits(status, requestID, revision) {
			return errReadbackTooLarge
		}
		var data any
		if resource == "CAPABILITIES" {
			data = s.readbackCapabilitiesVersion(version)
		} else {
			bounded, selected, err := readbackResourceConfigurationVersion(cfg, resource, version)
			if err != nil {
				return err
			}
			if err := bounded.ValidateMinSNRRules(); err != nil {
				return err
			}
			if !bounded.MinimumSizeFits(maxYAMLBytes) {
				return errReadbackTooLarge
			}
			data = selected
		}
		response, err = encodeBoundedYAML(yamlReadbackEnvelope{
			SchemaVersion: version, RequestID: requestID, Resource: resource, Revision: revision, Configuration: data, Status: status,
		})
		return err
	})
	return response, err
}

// Worst-case status is encoded with the actual candidate and the widest valid
// v1 envelope scalars. The exact byte check includes quoted numeric identifiers,
// all headers, document markers and CRLF, rather than a guessed overhead margin.
func (s *Server) configurationReadbackFits(c *Client, cfg filter.Configuration, prepared *Client) error {
	return s.configurationReadbackFitsVersion(c, cfg, prepared, 1)
}

func (s *Server) configurationReadbackFitsVersion(c *Client, cfg filter.Configuration, prepared *Client, version int) error {
	version = machineSchemaVersion(version)
	if err := machineConfigurationFits(cfg, version); err != nil {
		return err
	}
	status := attachReadbackConfiguration(s.captureReadbackStatus(c, prepared), c, cfg)
	var err error
	status, err = attachReadbackFilterStatus(status, cfg, version)
	if err != nil {
		return err
	}
	if !readbackMetadataFits(status, "", "") {
		return errReadbackTooLarge
	}
	grid := status.Effective.Grid
	if len(grid) < 6 {
		grid = "AA00AA"
	}
	status.Effective = readbackEffectiveStatus{
		Dialect: "go", Grid: grid, NoiseClass: "INDUSTRIAL", DedupePolicy: "SLOW",
		PathMinObservationCount: math.MaxInt, SolarSummaryMinutes: math.MaxInt,
	}
	status.Session = readbackSessionStatus{
		Callsign: strings.Repeat("1", 32), DiagnosticComments: "DEDUPE",
		PauseRemaining: math.MaxInt, SuppressedSpots: math.MaxUint64,
	}
	status.Server = readbackServerStatus{
		DefaultDialect: "go", DefaultDedupePolicy: "SLOW", DefaultNoiseClass: "INDUSTRIAL",
		PathMinObservationCount: math.MaxInt, AutoReadPauseMinRows: math.MaxInt, AutoReadPauseSeconds: math.MaxInt,
	}
	status.Preset = readbackPresetStatus{Name: strings.Repeat("1", 32)}
	_, document, err := readbackResourceConfigurationVersion(cfg, "CONFIG", version)
	if err != nil {
		return err
	}
	_, err = encodeBoundedYAML(yamlReadbackEnvelope{
		SchemaVersion: version, RequestID: strings.Repeat("1", 32), Resource: "CONFIG", Revision: strings.Repeat("\"", 128),
		Configuration: document, Status: status,
	})
	return err
}

func renderYAMLCommandError(resource, requestID, revision, code, message string) string {
	return renderYAMLCommandErrorVersion(1, resource, requestID, revision, code, message)
}

func renderYAMLCommandErrorVersion(version int, resource, requestID, revision, code, message string) string {
	version = machineSchemaVersion(version)
	type documentError struct {
		Code    string `yaml:"code"`
		Message string `yaml:"message"`
	}
	type errorEnvelope struct {
		SchemaVersion int           `yaml:"schema_version"`
		RequestID     string        `yaml:"request_id"`
		Resource      string        `yaml:"resource"`
		Revision      string        `yaml:"revision,omitempty"`
		Error         documentError `yaml:"error"`
	}
	if len(resource)+len(requestID)+len(revision)+len(code)+len(message) <= maxYAMLBytes {
		response, err := encodeBoundedYAML(errorEnvelope{version, requestID, resource, revision, documentError{code, message}})
		if err == nil {
			return response
		}
	}
	return fmt.Sprintf("---\r\nschema_version: %d\r\nrequest_id: \"\"\r\nresource: \"\"\r\nerror:\r\n  code: response_too_large\r\n  message: Error details exceed the response limit.\r\n...\r\n", version)
}

func renderYAMLCommandSuccess(resource, requestID, revision, operation string, applied, persisted bool) (string, error) {
	return renderYAMLCommandSuccessVersion(1, resource, requestID, revision, operation, applied, persisted)
}

func renderYAMLCommandSuccessVersion(version int, resource, requestID, revision, operation string, applied, persisted bool) (string, error) {
	version = machineSchemaVersion(version)
	if len(resource)+len(requestID)+len(revision)+len(operation) > maxYAMLBytes {
		return "", errReadbackTooLarge
	}
	type commandResult struct {
		Operation string `yaml:"operation"`
		Valid     bool   `yaml:"valid"`
		Applied   bool   `yaml:"applied"`
		Persisted bool   `yaml:"persisted"`
	}
	return encodeBoundedYAML(struct {
		SchemaVersion int           `yaml:"schema_version"`
		RequestID     string        `yaml:"request_id"`
		Resource      string        `yaml:"resource"`
		Revision      string        `yaml:"revision"`
		Result        commandResult `yaml:"result"`
	}{version, requestID, resource, revision, commandResult{operation, true, applied, persisted}})
}

// Human readbacks establish suppression before waiting for transaction ownership.
// Responses (including size errors) enter the queue once with fixed metadata.
func (s *Server) handleHumanReadback(c *Client, line string) bool {
	resource, category, recognized := parseHumanReadback(line)
	if !recognized {
		return false
	}
	if c == nil {
		return true
	}
	duration := defaultReadPauseDuration
	if s != nil && s.autoReadPauseDuration > 0 {
		duration = s.autoReadPauseDuration
	}
	completion := c.beginHumanReadback(s.now(), duration)
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	var response string
	if err == nil {
		response, err = s.renderHumanReadback(c, resource, category, duration)
		release()
	}
	if err != nil {
		response = humanReadbackError(err, duration)
	}
	if err := c.enqueueControl(controlMessage{raw: []byte(response), readback: completion}); err != nil && !isExpectedClientSendErr(err) {
		log.Printf("Readback delivery failed for %s: %v", c.identity(), err)
	}
	return true
}

func parseHumanReadback(line string) (resource, category string, recognized bool) {
	fields := strings.Fields(strings.ToUpper(line))
	if len(fields) == 0 {
		return "", "", false
	}
	var start int
	switch fields[0] {
	case "SHOW/FILTER", "SH/FILTER":
		resource, start = "FILTER", 1
	case "SHOW":
		if len(fields) < 2 || (fields[1] != "FILTER" && fields[1] != "SETTINGS") {
			return "", "", false
		}
		resource, start = fields[1], 2
	default:
		return "", "", false
	}
	if len(fields) == start {
		return resource, "", true
	}
	if len(fields) != start+1 || resource == "SETTINGS" {
		return resource, "INVALID", true
	}
	return resource, fields[start], true
}
