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
	"dxcluster/spot"
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
	release, err := s.acquireConfiguration(client, false, false, time.Time{})
	if err != nil {
		return presetError(command.verb, command.name, err), true
	}
	defer release()
	switch command.verb {
	case "SAVE":
		if client.recordProtected {
			return presetError(command.verb, command.name, errProtectedRecord), true
		}
		set, err := client.presetSnapshot()
		if err == nil {
			err = filter.SavePreset(client.callsign, command.name, set)
		}
		if err != nil {
			return presetError(command.verb, command.name, err), true
		}
		ref := &filter.PresetReference{Name: command.name, Baseline: set}
		if err := s.persistConfiguration(client, filter.ConfigurationFromPreset(set), ref); err != nil {
			return fmt.Sprintf("Saved preset %s, but could not persist its association for %s.\nPrevious preset association and baseline retained.\n", command.name, client.callsign), true
		}
		client.presetReference = ref
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
		return s.loadPresetOwned(client, command.name), true
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

// The caller owns the transaction stripe. Capture preflights the independent
// preset budget before detaching maps/lists; configured defaults remain exact.
func (c *Client) presetSnapshot() (*filter.SavedPreset, error) {
	cfg, err := c.captureConfiguration(filter.MaxPresetBytes)
	if err != nil {
		return nil, fmt.Errorf("preset exceeds %d KiB", filter.MaxPresetBytes/1024)
	}
	return cfg.Preset()
}

func (s *Server) preparePreset(client *Client, set *filter.SavedPreset, now time.Time) (*Client, string) {
	prepared, warning := s.prepareConfiguration(client, filter.ConfigurationFromPreset(set), now)
	warning = strings.ReplaceAll(warning, "; effective dedupe is ", "; using ")
	return prepared, warning
}

func (s *Server) loadPresetOwned(client *Client, name string) string {
	if client.recordProtected {
		return presetError("LOAD", name, errProtectedRecord)
	}
	set, err := filter.LoadPreset(client.callsign, name)
	if err != nil {
		return presetError("LOAD", name, err)
	}
	now := time.Now().UTC()
	prepared, warning := s.preparePreset(client, set, now)
	applied := filter.ConfigurationFromPreset(set)
	if applied.Settings.DedupePolicy != "" && applied.Settings.DedupePolicy != prepared.getDedupePolicy().label() {
		if !strings.Contains(warning, "Note: dedupe ") {
			warning += fmt.Sprintf("Note: stored dedupe choice unavailable; using %s.\n", prepared.getDedupePolicy().label())
		}
		applied.Settings.DedupePolicy = prepared.getDedupePolicy().label()
	}
	if applied.Settings.Dialect != "" && applied.Settings.Dialect != string(prepared.dialect) {
		warning += fmt.Sprintf("Note: stored dialect applied as %s.\n", strings.ToUpper(string(prepared.dialect)))
		applied.Settings.Dialect = string(prepared.dialect)
	}
	if applied.Settings.PathMinObservationCount != prepared.pathMinObservationCount {
		warning += fmt.Sprintf("Note: PATHSAMPLES %d unavailable; using the cluster default.\n", applied.Settings.PathMinObservationCount)
		applied.Settings.PathMinObservationCount = prepared.pathMinObservationCount
	}
	acknowledgement := fmt.Sprintf("Loaded preset %s; defaults saved for %s.\n", name, client.callsign)
	if !presetAcknowledgementFits(acknowledgement, warning) {
		return presetError("LOAD", name, errReadbackTooLarge)
	}
	baseline, err := applied.Preset()
	if err != nil {
		return presetError("LOAD", name, err)
	}
	ref := &filter.PresetReference{Name: name, Baseline: baseline}
	if err := s.persistConfiguration(client, applied, ref); err != nil {
		return presetError("LOAD", name, err)
	}
	s.publishPreparedConfiguration(client, applied, prepared, ref, time.Now().UTC(), true)
	return acknowledgement + warning
}

// LOAD admits the acknowledgement, independently of a complete CONFIG readback.
// Count the writer's CRLF expansion before concatenating a success response.
func presetAcknowledgementFits(parts ...string) bool {
	remaining := maxYAMLBytes
	previousCR := false
	for _, part := range parts {
		if len(part) > remaining {
			return false
		}
		remaining -= len(part)
		for i := range len(part) {
			if part[i] == '\n' && !previousCR {
				if remaining == 0 {
					return false
				}
				remaining--
			}
			previousCR = part[i] == '\r'
		}
	}
	return true
}
