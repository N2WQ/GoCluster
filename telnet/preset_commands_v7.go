// File role: Tagged native presets under the existing full-SSID transaction.
// Library-first SAVE may succeed independently; associations publish only after
// profile commit. LOAD commits both saved banks before activating its tagged one.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

func (s *Server) handleV7PresetOwned(c *Client, command presetCommand) string {
	// Every verb can decode the complete shared library, including LIST/DELETE.
	// Reserve before storage access rather than only before active-bank capture.
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return presetError(command.verb, command.name, err)
	}
	defer reservation.Release()
	if command.verb == "LIST" {
		rows, err := filter.ListV7PresetSummaries(c.callsign)
		if err != nil {
			return presetError(command.verb, "", err)
		}
		var out strings.Builder
		fmt.Fprintf(&out, "Saved presets for %s (%d/%d):\n", spot.NormalizeOwnCallsign(c.callsign), len(rows), filter.MaxPresets)
		if len(rows) == 0 {
			out.WriteString("(none)\n")
		}
		for _, row := range rows {
			fmt.Fprintf(&out, "%s (%s)\n", row.Name, strings.ToUpper(row.Dialect))
		}
		return out.String()
	}
	if command.verb == "DELETE" {
		if err := filter.DeleteV7Preset(c.callsign, command.name); err != nil {
			return presetError(command.verb, command.name, err)
		}
		return fmt.Sprintf("Deleted preset %s.\n", command.name)
	}
	if c.recordProtected {
		return presetError(command.verb, command.name, errProtectedRecord)
	}
	before, err := c.captureV7Configuration()
	if err != nil {
		return presetError(command.verb, command.name, err)
	}
	refs := c.v7PresetReferences // Immutable admitted references; replace only the named bank.
	if command.verb == "SAVE" {
		set, err := filter.V7PresetFromConfiguration(before, string(c.dialect))
		if err != nil {
			return presetError(command.verb, command.name, err)
		}
		ref := &filter.V7PresetReference{Name: command.name, Baseline: set}
		if set.Dialect == "cc" {
			refs.CC = ref
		} else {
			refs.GO = ref
		}
		if err := s.v7ConfigurationReadbackFits(c, before, c, refs); err != nil {
			return presetError(command.verb, command.name, err)
		}
		saved, err := filter.SaveV7PresetAndRecord(c.callsign, command.name, set, func() (*filter.PreparedUserRecord, error) {
			return s.prepareV7ConfigurationRecord(c, before, refs)
		}, s.commitConfigurationRecord)
		if err != nil && !saved {
			return presetError(command.verb, command.name, err)
		}
		if err != nil {
			return fmt.Sprintf("Saved preset %s, but could not persist its association for %s.\nPrevious preset associations and baselines retained.\n", command.name, c.callsign)
		}
		c.v7PresetReferences, c.configurationUnsaved = refs, false
		return fmt.Sprintf("Saved preset %s (%s) for %s.\n", command.name, strings.ToUpper(set.Dialect), spot.NormalizeOwnCallsign(c.callsign))
	}
	set, err := filter.LoadV7Preset(c.callsign, command.name)
	if err != nil {
		return presetError(command.verb, command.name, err)
	}
	next, err := filter.ApplyV7Preset(before, set)
	if err != nil {
		return presetError(command.verb, command.name, err)
	}
	ref := &filter.V7PresetReference{Name: command.name, Baseline: set}
	if set.Dialect == "cc" {
		refs.CC = ref
	} else {
		refs.GO = ref
	}
	now := s.now()
	prepared, err := s.prepareV7ConfigurationUpdate(c, before, next, now)
	if err != nil {
		return presetError(command.verb, command.name, err)
	}
	if err := s.v7ConfigurationReadbackFits(c, next, prepared, refs); err != nil {
		return presetError(command.verb, command.name, err)
	}
	record, err := s.prepareV7ConfigurationRecord(c, next, refs)
	if err == nil {
		err = s.commitConfigurationRecord(record)
	}
	if err != nil {
		return presetError(command.verb, command.name, err)
	}
	prepared.v7PresetReferences = refs
	s.publishConfigurationState(c, next.Settings, prepared, nil, now, true)
	c.refreshV7ConfigurationRevision()
	c.configurationUnsaved = false
	message := fmt.Sprintf("Loaded preset %s (%s); defaults saved for %s.\n", command.name, strings.ToUpper(set.Dialect), c.callsign)
	if string(c.dialect) != string(s.effectiveV7Dialect(before.Settings)) {
		message += fmt.Sprintf("Dialect switched to %s; its saved filter is active.\n", strings.ToUpper(set.Dialect))
	}
	return message
}
