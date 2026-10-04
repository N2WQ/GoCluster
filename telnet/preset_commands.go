// File role: Owns dialect-independent SAVE/LIST/LOAD/DELETE PRESET commands.
// LOAD prepares detached runtime state and commits the SSID default before
// changing the live client. Disk I/O never holds client write locks.
// Related docs: telnet/README.md, commands/README.md.
package telnet

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type presetCommand struct{ verb, name string }

func presetUsage(verb string) string {
	if verb == "LIST" {
		return "Usage: LIST PRESET\n"
	}
	return fmt.Sprintf("Usage: %s PRESET <name>\n", verb)
}

func parsePresetCommand(line string) (presetCommand, bool, string) {
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return presetCommand{}, false, ""
	}
	verb := strings.ToUpper(tokens[0])
	switch verb {
	case "SAVE", "LIST", "LOAD", "DELETE":
	default:
		return presetCommand{}, false, ""
	}
	if len(tokens) > 1 && !strings.EqualFold(tokens[1], "PRESET") {
		return presetCommand{}, false, ""
	}
	command := presetCommand{verb: verb}
	expected := 3
	if verb == "LIST" {
		expected = 2
	}
	if len(tokens) != expected {
		return command, true, presetUsage(verb)
	}
	if verb != "LIST" {
		name, err := filter.NormalizePresetName(tokens[2])
		if err != nil {
			return command, true, err.Error() + "\n" + presetUsage(verb)
		}
		command.name = name
	}
	return command, true, ""
}

func (s *Server) handlePresetCommand(client *Client, line string) (string, bool) {
	command, handled, usage := parsePresetCommand(line)
	if !handled || usage != "" {
		return usage, handled
	}
	if s == nil || client == nil || client.filter == nil || strings.TrimSpace(client.callsign) == "" {
		return "Preset settings unavailable.\n", true
	}
	switch command.verb {
	case "SAVE":
		set, err := client.presetSnapshot()
		if err == nil {
			err = filter.SavePreset(client.callsign, command.name, set)
		}
		if err != nil {
			return presetError(command.verb, command.name, err), true
		}
		return fmt.Sprintf("Saved preset %s for %s.\n", command.name, spot.NormalizeOwnCallsign(client.callsign)), true
	case "LIST":
		names, err := filter.ListPresets(client.callsign)
		if err != nil {
			return presetError(command.verb, "", err), true
		}
		header := fmt.Sprintf("Saved presets for %s (%d/%d):\n", spot.NormalizeOwnCallsign(client.callsign), len(names), filter.MaxPresets)
		if len(names) == 0 {
			return header + "(none)\n", true
		}
		return header + strings.Join(names, "\n") + "\n", true
	case "LOAD":
		return s.loadPreset(client, command.name, filter.SaveUserPreferences), true
	case "DELETE":
		if err := filter.DeletePreset(client.callsign, command.name); err != nil {
			return presetError(command.verb, command.name, err), true
		}
		return fmt.Sprintf("Deleted preset %s.\n", command.name), true
	}
	return "", false
}

func presetError(verb, name string, err error) string {
	if name != "" && errors.Is(err, filter.ErrPresetNotFound) {
		return fmt.Sprintf("Saved preset %s not found.\n", name)
	}
	return fmt.Sprintf("%s PRESET failed: %v\n", verb, err)
}

// The session goroutine owns preference mutations. Broadcast readers share
// filter/path/solar state through the existing locks; Clone detaches every map,
// slice and toggle before persistence can outlive those read locks.
func (c *Client) presetSnapshot() (*filter.SavedPreset, error) {
	state := c.pathSnapshot()
	solarMinutes := c.getSolarSummaryMinutes()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	set := &filter.SavedPreset{
		Filter: *c.filter, Dialect: string(c.dialect), DedupePolicy: c.getDedupePolicy().label(),
		Grid: state.grid, NoiseClass: state.noiseClass,
		PathMinObservationCount: state.pathMinObservationCount, SolarSummaryMinutes: solarMinutes,
	}
	return set.Clone()
}

func (s *Server) preparePreset(client *Client, set *filter.SavedPreset, now time.Time) (*Client, string) {
	prepared := &Client{filter: &set.Filter, dialect: normalizeDialectName(set.Dialect)}
	requested := parseDedupePolicy(set.DedupePolicy)
	policy := s.resolveDedupePolicy(requested)
	prepared.setDedupePolicy(policy)
	prepared.grid = strutil.NormalizeUpper(set.Grid)
	if prepared.grid == "" && s.gridLookup != nil {
		if grid, derived, ok := s.gridLookup(client.callsign); ok {
			prepared.grid = strutil.NormalizeUpper(grid)
			prepared.gridDerived = derived
		}
	}
	prepared.gridCell = pathreliability.EncodeCell(prepared.grid)
	prepared.gridCoarseCell = pathreliability.EncodeCoarseCell(prepared.grid)
	prepared.noiseClass = strutil.NormalizeUpper(set.NoiseClass)
	if prepared.noiseClass == "" {
		prepared.noiseClass = "QUIET"
	}
	if set.PathMinObservationCount > s.pathPredictorMinObservationCount() {
		prepared.pathMinObservationCount = set.PathMinObservationCount
	}
	prepared.setSolarSummaryMinutes(set.SolarSummaryMinutes, now)
	warning, _ := applyNearbyLoginState(prepared, s.nearbyLoginWarning)
	if policy != requested {
		warning += fmt.Sprintf("Note: dedupe %s unavailable; using %s.\n", requested.label(), policy.label())
	}
	return prepared, warning
}

func (s *Server) loadPreset(client *Client, name string, persist func(string, *filter.SavedPreset, []string) error) string {
	set, err := filter.LoadPreset(client.callsign, name)
	if err != nil {
		return presetError("LOAD", name, err)
	}
	now := time.Now().UTC()
	prepared, warning := s.preparePreset(client, set, now)
	preferences := &filter.SavedPreset{
		Filter: *prepared.filter, Dialect: string(prepared.dialect), DedupePolicy: prepared.getDedupePolicy().label(),
		Grid: prepared.grid, NoiseClass: prepared.noiseClass,
		PathMinObservationCount: prepared.pathMinObservationCount, SolarSummaryMinutes: prepared.getSolarSummaryMinutes(),
	}
	if err := persist(client.callsign, preferences, client.recentIPs); err != nil {
		return presetError("LOAD", name, err)
	}
	// Keep the Filter pointer stable: fan-out/history readers retain it between
	// read-lock sections. All fallible preparation and disk work is already done.
	client.pathMu.Lock()
	client.filterMu.Lock()
	*client.filter = *prepared.filter
	client.grid = prepared.grid
	client.gridDerived = prepared.gridDerived
	client.gridCell = prepared.gridCell
	client.gridCoarseCell = prepared.gridCoarseCell
	client.noiseClass = prepared.noiseClass
	client.pathMinObservationCount = prepared.pathMinObservationCount
	client.dialect = prepared.dialect
	client.setDedupePolicy(prepared.getDedupePolicy())
	// Schedule from publication time so slow disk I/O cannot install a past tick.
	client.setSolarSummaryMinutes(prepared.getSolarSummaryMinutes(), time.Now().UTC())
	client.filterMu.Unlock()
	client.pathMu.Unlock()
	return fmt.Sprintf("Loaded preset %s; defaults saved for %s.\n", name, client.callsign) + warning
}
