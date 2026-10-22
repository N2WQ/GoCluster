// File role: Renders exact configuration through a hard final CRLF byte budget.
// Preflight happens before map sorting or YAML's intermediate node allocation.
// Human previews are bounded; exact detail preflights before sorting borrowed keys.
package telnet

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

type boundedResponse struct {
	data []byte
	err  error
}

// Write measures converted bytes before appending. Explicit capacity growth
// never retains a buffer larger than the final response limit.
func (b *boundedResponse) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	size := len(p)
	previousCR := len(b.data) != 0 && b.data[len(b.data)-1] == '\r'
	for _, value := range p {
		if value == '\n' && !previousCR {
			size++
		}
		previousCR = value == '\r'
	}
	if size > maxYAMLBytes-len(b.data) {
		b.err = errReadbackTooLarge
		return 0, b.err
	}
	if needed := len(b.data) + size; needed > cap(b.data) {
		capacity := min(maxYAMLBytes, max(needed, max(1024, cap(b.data)*2)))
		grown := make([]byte, len(b.data), capacity)
		copy(grown, b.data)
		b.data = grown
	}
	for _, value := range p {
		if value == '\n' && (len(b.data) == 0 || b.data[len(b.data)-1] != '\r') {
			b.data = append(b.data, '\r')
		}
		b.data = append(b.data, value)
	}
	return len(p), nil
}

func encodeBoundedYAML(document any) (string, error) {
	var response boundedResponse
	if _, err := response.Write([]byte("---\n")); err != nil {
		return "", err
	}
	encoder := yaml.NewEncoder(&response)
	encoder.SetIndent(2)
	if err := encoder.Encode(document); err != nil {
		_ = encoder.Close()
		if response.err != nil {
			return "", response.err
		}
		return "", err
	}
	if err := encoder.Close(); err != nil {
		if response.err != nil {
			return "", response.err
		}
		return "", err
	}
	if _, err := response.Write([]byte("...\n")); err != nil {
		return "", err
	}
	return string(response.data), nil
}

func humanReadbackFooter(duration time.Duration) string {
	if duration <= 0 {
		duration = defaultReadPauseDuration
	}
	return fmt.Sprintf("Live spots paused during delivery and for at least %ds afterward.\nType RESUME when ready. Missed spots are not replayed.\n", durationCeilSeconds(duration))
}

func humanReadbackError(err error, duration time.Duration) string {
	var h humanResponse
	message := err.Error()
	if errors.Is(err, errReadbackTooLarge) {
		message = "response exceeds 65,536 bytes."
	}
	if h.prose("Readback failed: ", "", message) == nil && writeHumanFooter(&h, duration) == nil {
		return string(h.data)
	}
	return "Readback failed: response exceeds 65,536 bytes.\r\n\r\n" + strings.ReplaceAll(humanReadbackFooter(duration), "\n", "\r\n")
}

func writeHumanFooter(h *humanResponse, duration time.Duration) error {
	if err := h.line(""); err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSuffix(humanReadbackFooter(duration), "\n"), "\n") {
		if err := h.line(line); err != nil {
			return err
		}
	}
	return nil
}

// Callers own the full-callsign transaction. Capture runtime before borrowing
// filter/path locks, then use the same snapshot for preflight and generation.
// Machine status/envelopes and writer/pause ownership are deliberately unchanged.
func (s *Server) renderHumanReadback(c *Client, resource, category string, duration time.Duration) (string, error) {
	status := s.captureReadbackStatus(c, nil)
	runtime := s.captureHumanRuntime(c)
	var response string
	err := c.withBorrowedConfiguration(func(cfg filter.Configuration) error {
		status = attachReadbackConfiguration(status, c, cfg)
		if !readbackMetadataFits(status, "", "") {
			return errReadbackTooLarge
		}
		if resource == "SETTINGS" && category != "" {
			return fmt.Errorf("usage: SHOW SETTINGS")
		}
		if resource == "FILTER" && category != "" && category != "RULE" && category != "FULL" {
			return fmt.Errorf("usage: SHOW FILTER [RULE|FULL]; use HELP RULE")
		}
		if resource == "SETTINGS" && !(filter.Configuration{Settings: cfg.Settings}).MinimumSizeFits(maxYAMLBytes) {
			return errReadbackTooLarge
		}
		for _, countOnly := range []bool{true, false} {
			h := humanResponse{countOnly: countOnly}
			if err := writeHumanHeader(&h, c, status); err != nil {
				return err
			}
			switch resource {
			case "SETTINGS":
				if err := writeHumanSettings(&h, cfg.Settings, status, runtime, duration); err != nil {
					return err
				}
			default:
				var err error
				if category == "FULL" {
					err = writeBooleanFilterSnapshot(&h, c.filter)
				} else {
					err = writeBooleanSettingsAndRules(&h, c, cfg, status, runtime, false)
					if err == nil {
						err = h.line("")
					}
					if err == nil {
						err = writeBooleanEditGuidance(&h)
					}
				}
				if err != nil {
					return err
				}
			}
			if err := writeHumanFooter(&h, duration); err != nil {
				return err
			}
			if !countOnly {
				response = string(h.data)
			}
		}
		return nil
	})
	return response, err
}

type humanRuntimeStatus struct {
	stationFloor, beaconFloor                  int
	stationMinimum, beaconMinimum, userMinimum int
	prediction                                 string
	dedupeEnabled, fastEnabled, solarEnabled   bool
}

func (s *Server) captureHumanRuntime(c *Client) humanRuntimeStatus {
	state := c.pathSnapshot()
	runtime := humanRuntimeStatus{prediction: "unavailable", userMinimum: state.pathMinObservationCount}
	if s == nil {
		return runtime
	}
	runtime.dedupeEnabled = s.dedupeFastEnabled || s.dedupeMedEnabled || s.dedupeSlowEnabled
	runtime.fastEnabled = s.dedupeFastEnabled
	runtime.solarEnabled = s.solarWeather.Enabled()
	if s.pathPredictor != nil {
		cfg := s.pathPredictor.Config()
		runtime.stationFloor, runtime.beaconFloor = cfg.MinObservationCount, cfg.BeaconMinObservationCount
		runtime.stationMinimum = effectivePathMinObservationCount(state, cfg)
		runtime.beaconMinimum = effectiveBeaconPathMinObservationCount(state, cfg)
		runtime.prediction = ""
		if !cfg.Enabled {
			runtime.prediction = "disabled"
		}
	}
	return runtime
}

func humanSettingToken(label, value string) string {
	// Only the known dialect labels use their familiar human spelling.
	// Other saved values, including unusual mixed-case strings, stay exact.
	if label == "Dialect" && (strings.EqualFold(value, "go") || strings.EqualFold(value, "cc")) {
		return strings.ToUpper(value)
	}
	return value
}

func writeHumanSetting(h *humanResponse, label, configured, effective string) error {
	if configured == "" {
		display := humanSettingToken(label, effective)
		if simpleHumanValue(display) && len(display)+19 <= humanValueWidth {
			return h.row(label, "DEFAULT; effective "+display)
		}
		if err := h.row(label, "DEFAULT; effective"); err != nil {
			return err
		}
		return h.quoted(humanPrefix(""), effective, "")
	}
	configuredDisplay, effectiveDisplay := humanSettingToken(label, configured), humanSettingToken(label, effective)
	if strings.EqualFold(configured, effective) {
		return h.valueRow(label, configuredDisplay, "")
	}
	if err := h.valueRow(label, configuredDisplay, ""); err != nil {
		return err
	}
	return h.valueRow("", effectiveDisplay, " (effective)")
}

func writeHumanPathSettings(h *humanResponse, configured int, runtime humanRuntimeStatus) error {
	selection := "DEFAULT"
	if configured != 0 {
		selection = strconv.Itoa(configured) + " configured"
	}
	if runtime.prediction != "" {
		return h.row("Path samples", selection+"; prediction "+runtime.prediction)
	}
	floors := fmt.Sprintf("stations %d, beacons %d", runtime.stationMinimum, runtime.beaconMinimum)
	if configured == 0 {
		return h.row("Path samples", "DEFAULT; "+floors)
	}
	if runtime.userMinimum == 0 {
		if err := h.row("Path samples", selection+"; effective "+floors); err != nil {
			return err
		}
		return h.row("", fmt.Sprintf("Override inactive: not above station minimum %d", runtime.stationFloor))
	}
	if err := h.row("Path samples", fmt.Sprintf("%d (user minimum); %s", configured, floors)); err != nil {
		return err
	}
	return h.row("", fmt.Sprintf("Cluster minimums: stations %d, beacons %d", runtime.stationFloor, runtime.beaconFloor))
}

func writeHumanDedupe(h *humanResponse, configured string, status configurationReadbackStatus, runtime humanRuntimeStatus) error {
	choice := configured
	if choice == "" {
		choice = "DEFAULT"
	}
	if !runtime.dedupeEnabled {
		return h.valueRow("Dedupe", choice, "; secondary duplicate suppression disabled")
	}
	requested, known := parseDedupePolicyToken(configured)
	suffix := ""
	if configured == "" || !known || requested.label() != status.Effective.DedupePolicy {
		suffix = "; effective " + status.Effective.DedupePolicy
		if status.Effective.NearbyActive {
			suffix += " while NEARBY is active"
		}
	}
	if err := h.valueRow("Dedupe", choice, suffix); err != nil {
		return err
	}
	if configured != "" && !known {
		if err := h.row("", "Configured choice is unrecognized"); err != nil {
			return err
		}
	}
	if status.Effective.NearbyActive && !runtime.fastEnabled {
		return h.row("", "FAST is unavailable on this server")
	}
	if known && !status.Effective.NearbyActive && requested.label() != status.Effective.DedupePolicy {
		return h.row("", "Configured choice is unavailable on this server")
	}
	return nil
}

func writeHumanSettings(h *humanResponse, cfg filter.SettingsConfiguration, status configurationReadbackStatus, runtime humanRuntimeStatus, duration time.Duration) error {
	if err := writeHumanSetting(h, "Dialect", cfg.Dialect, status.Effective.Dialect); err != nil {
		return err
	}
	if cfg.Grid == "" {
		text := "DEFAULT; no grid available"
		if status.Effective.Grid != "" && simpleHumanValue(status.Effective.Grid) && len(status.Effective.Grid)+41 <= humanValueWidth {
			text = "DEFAULT; using " + status.Effective.Grid + " from callsign lookup"
		} else if status.Effective.Grid != "" {
			if err := h.row("Grid", "DEFAULT; using grid from callsign lookup"); err != nil {
				return err
			}
			if err := h.quoted(humanPrefix(""), status.Effective.Grid, ""); err != nil {
				return err
			}
			text = ""
		}
		if text != "" {
			if err := h.row("Grid", text); err != nil {
				return err
			}
		}
	} else if err := writeHumanSetting(h, "Grid", cfg.Grid, status.Effective.Grid); err != nil {
		return err
	}
	if err := writeHumanSetting(h, "Noise", cfg.NoiseClass, status.Effective.NoiseClass); err != nil {
		return err
	}
	if err := writeHumanDedupe(h, cfg.DedupePolicy, status, runtime); err != nil {
		return err
	}
	if err := writeHumanPathSettings(h, cfg.PathMinObservationCount, runtime); err != nil {
		return err
	}
	solar := "Off"
	if cfg.SolarSummaryMinutes != 0 {
		solar = fmt.Sprintf("%d minutes; unavailable on this server", cfg.SolarSummaryMinutes)
		if runtime.solarEnabled {
			solar = fmt.Sprintf("Every %d minutes when conditions are active", cfg.SolarSummaryMinutes)
		}
	}
	if err := h.row("Solar", solar); err != nil {
		return err
	}
	if err := h.line(""); err != nil {
		return err
	}
	if err := h.line("Session only"); err != nil {
		return err
	}
	diagnostics := status.Session.DiagnosticComments
	if diagnostics == "OFF" {
		diagnostics = "Off"
	}
	if err := h.row("Diagnostics", diagnostics); err != nil {
		return err
	}
	if status.Session.TemporaryDefaults {
		if err := h.row("Persistence", "Temporary defaults; changes will not be saved"); err != nil {
			return err
		}
	}
	if status.Session.PausePendingDelivery {
		if duration <= 0 {
			duration = defaultReadPauseDuration
		}
		if err := h.row("Live spots", fmt.Sprintf("Paused for reading; at least %ds after delivery", durationCeilSeconds(duration))); err != nil {
			return err
		}
		if err := h.row("", fmt.Sprintf("%d spots suppressed", status.Session.SuppressedSpots)); err != nil {
			return err
		}
		if status.Session.PauseRemaining > durationCeilSeconds(duration) {
			return h.row("", fmt.Sprintf("Existing pause: %ds remaining", status.Session.PauseRemaining))
		}
		return nil
	}
	if status.Session.PauseActive {
		return h.row("Live spots", fmt.Sprintf("Paused: %ds remaining; %d suppressed", status.Session.PauseRemaining, status.Session.SuppressedSpots))
	}
	return h.row("Live spots", "Flowing")
}
