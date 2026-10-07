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

	"dxcluster/cty"
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

type readbackCategory struct {
	name         string
	rules        any
	allow, block []string
	toggle       filter.DefaultBool
	kind         byte
}

func readbackCategories(f filter.FilterConfiguration) [25]readbackCategory {
	return [25]readbackCategory{
		{name: "BAND", rules: f.Bands}, {name: "MODE", rules: f.Modes},
		{name: "SOURCE", rules: f.Sources}, {name: "EVENT", rules: f.Events},
		{name: "CONFIDENCE", rules: f.Confidence}, {name: "PATH", rules: f.PathClasses},
		{name: "DXCONT", rules: f.DXContinents}, {name: "DECONT", rules: f.DEContinents},
		{name: "DXZONE", rules: f.DXZones}, {name: "DEZONE", rules: f.DEZones},
		{name: "DXGRID2", rules: f.DXGrid2}, {name: "DEGRID2", rules: f.DEGrid2},
		{name: "DXDXCC", rules: f.DXDXCC}, {name: "DEDXCC", rules: f.DEDXCC},
		{name: "DXSTATE", rules: f.DXStates}, {name: "DESTATE", rules: f.DEStates},
		{name: "DXCALL", allow: f.DXCallsigns, block: f.BlockDXCallsigns, kind: 'p'},
		{name: "DECALL", allow: f.DECallsigns, block: f.BlockDECallsigns, kind: 'p'},
		{name: "BEACON", toggle: f.IncludeBeacons, kind: 't'}, {name: "WWV", toggle: f.AllowWWV, kind: 't'},
		{name: "WCY", toggle: f.AllowWCY, kind: 't'}, {name: "ANNOUNCE", toggle: f.AllowAnnounce, kind: 't'},
		{name: "SELF", toggle: f.AllowSelf, kind: 't'}, {name: "TOXIC", toggle: f.AllowToxic, kind: 't'},
		{name: "NEARBY", kind: 'n'},
	}
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
	// Build the request index before borrowing broadcast-visible filter/path
	// locks. One snapshot serves both preflight and generation after refresh.
	var dxcc *cty.DXCCIndex
	if resource == "FILTER" && (category == "" || category == "FULL" || category == "DXDXCC" || category == "DEDXCC") {
		var db *cty.CTYDatabase
		if s != nil && s.ctyLookup != nil {
			db = s.ctyLookup()
		}
		dxcc = cty.NewDXCCIndex(db)
	}
	var response string
	err := c.withBorrowedConfiguration(func(cfg filter.Configuration) error {
		status = attachReadbackConfiguration(status, c, cfg)
		if !readbackMetadataFits(status, "", "") {
			return errReadbackTooLarge
		}
		switch category {
		case "CONF":
			category = "CONFIDENCE"
		case "PC93":
			category = "ANNOUNCE"
		}
		if resource == "SETTINGS" && category != "" {
			return fmt.Errorf("usage: SHOW SETTINGS")
		}
		if resource == "SETTINGS" && !(filter.Configuration{Settings: cfg.Settings}).MinimumSizeFits(maxYAMLBytes) {
			return errReadbackTooLarge
		}
		for _, countOnly := range []bool{true, false} {
			h := humanResponse{countOnly: countOnly, dxcc: dxcc}
			if err := writeHumanHeader(&h, c, status); err != nil {
				return err
			}
			switch {
			case resource == "SETTINGS":
				if err := writeHumanSettings(&h, cfg.Settings, status, runtime, duration); err != nil {
					return err
				}
			case category == "":
				if err := writeHumanOverview(&h, cfg.Filters, status); err != nil {
					return err
				}
			default:
				found := false
				for _, item := range readbackCategories(cfg.Filters) {
					if category != "FULL" && category != item.name {
						continue
					}
					if found {
						if err := h.line(""); err != nil {
							return err
						}
					}
					found = true
					if err := writeHumanEffectiveCategory(&h, item, cfg.Filters, status); err != nil {
						return err
					}
				}
				if !found {
					return fmt.Errorf("usage: SHOW FILTER [FULL|category]; use HELP SHOW FILTER")
				}
				if h.joined {
					if err := h.line(""); err != nil {
						return err
					}
					if err := h.line("A + joins quoted pieces of one value; no characters are added."); err != nil {
						return err
					}
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
	dedupeEnabled, fastEnabled                 bool
}

func (s *Server) captureHumanRuntime(c *Client) humanRuntimeStatus {
	state := c.pathSnapshot()
	runtime := humanRuntimeStatus{prediction: "unavailable", userMinimum: state.pathMinObservationCount}
	if s == nil {
		return runtime
	}
	runtime.dedupeEnabled = s.dedupeFastEnabled || s.dedupeMedEnabled || s.dedupeSlowEnabled
	runtime.fastEnabled = s.dedupeFastEnabled
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
	suffix := ""
	if configured == "" || !strings.EqualFold(configured, status.Effective.DedupePolicy) {
		suffix = "; effective " + status.Effective.DedupePolicy
		if status.Effective.NearbyActive {
			suffix += " while NEARBY is active"
		}
	}
	if err := h.valueRow("Dedupe", choice, suffix); err != nil {
		return err
	}
	if status.Effective.NearbyActive && !runtime.fastEnabled {
		return h.row("", "FAST is unavailable on this server")
	}
	if configured != "" && !status.Effective.NearbyActive && !strings.EqualFold(configured, status.Effective.DedupePolicy) {
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
		solar = fmt.Sprintf("Every %d minutes", cfg.SolarSummaryMinutes)
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
