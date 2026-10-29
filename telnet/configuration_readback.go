// File role: Builds coherent, bounded human and machine configuration readbacks.
// Callers own the full-callsign stripe. Runtime status is captured before borrowing
// filter/path state, avoiding recursive read locks and exposing no preset baseline.
package telnet

import (
	"log"
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
// or dedupe state alongside a proposed configuration.
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
	solarMinutes := prepared.getSolarSummaryMinutes()
	if !s.solarWeather.Enabled() {
		solarMinutes = 0
	}
	status := configurationReadbackStatus{
		Effective: readbackEffectiveStatus{
			Dialect: string(prepared.dialect), Grid: state.grid, GridDerived: state.gridDerived,
			NoiseClass: state.noiseClass, DedupePolicy: s.effectiveDedupePolicyForClient(prepared).label(),
			PathMinObservationCount: max(state.pathMinObservationCount, s.pathPredictorMinObservationCount()),
			SolarSummaryMinutes:     solarMinutes,
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

func readbackMetadataFits(status configurationReadbackStatus, requestID, revision string) bool {
	remaining := maxYAMLBytes
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

func renderYAMLCommandError(resource, requestID, revision, code, message string) string {
	return renderV6YAMLCommandError(resource, requestID, revision, code, message)
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
