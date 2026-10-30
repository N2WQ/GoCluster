// File role: Concise native CC login inspection, without a reading pause.
package telnet

import (
	"fmt"
	"strings"
	"time"

	"dxcluster/filter"
)

func (s *Server) renderV7CCLoginBlock(c *Client, first bool) (string, error) {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return "", err
	}
	defer release()
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return "", err
	}
	defer reservation.Release()
	configuration, err := c.captureV7Configuration()
	if err != nil {
		return "", err
	}
	return s.renderV7CCLoginOwned(c, configuration, first)
}

func (s *Server) renderV7CCLoginOwned(c *Client, configuration filter.V7Configuration, first bool) (string, error) {
	return renderNativeCCBody(maxYAMLBytes, func(h *humanResponse) error {
		title := "Current settings and native CC filters:"
		if first {
			title = "Default settings and native CC filters:"
		}
		if err := h.line(title); err != nil {
			return err
		}
		if err := h.row("Commands:", "CC; independent saved filter bank"); err != nil {
			return err
		}
		status, runtime := s.captureReadbackStatus(c, nil), s.captureHumanRuntime(c)
		grid := status.Effective.Grid
		if grid == "" {
			grid = "Not set (SET GRID <grid>)"
		} else if status.Effective.GridDerived {
			grid += " (callsign lookup)"
		}
		if err := h.row("Grid:", grid); err != nil {
			return err
		}
		if err := h.row("Noise:", status.Effective.NoiseClass); err != nil {
			return err
		}
		if err := writeHumanDedupe(h, configuration.Settings.DedupePolicy, status, runtime); err != nil {
			return err
		}
		if err := h.row("Modes:", compactV7NoticeSelection(configuration.Filters.CC.Modes)); err != nil {
			return err
		}
		if err := h.row("Sources:", compactV7NoticeSelection(configuration.Filters.CC.Sources)); err != nil {
			return err
		}
		if err := h.prose("", "", "Mode selection applies to human and skimmer reports. Source selection does not activate feeds."); err != nil {
			return err
		}
		for _, name := range []string{"DOC", "DOS", "DXCTY", "DXSTATE"} {
			entry := configuration.Filters.CC.Geography[name]
			if err := h.row(name+":", fmt.Sprintf("global %s; %d band overrides", strings.ToUpper(string(entry.Global.Action)), len(entry.Bands))); err != nil {
				return err
			}
		}
		dxbm := "OFF (unrestricted)"
		if configuration.Filters.CC.DXBM.Active {
			base := "DENY"
			if configuration.Filters.CC.DXBM.BaseAllow {
				base = "ALLOW"
			}
			dxbm = fmt.Sprintf("active; base %s; %d slot overrides", base, len(configuration.Filters.CC.DXBM.Slots))
		}
		if err := h.row("DXBM:", dxbm); err != nil {
			return err
		}
		if err := writeNativeCCControls(h, configuration.Filters.CC, ""); err != nil {
			return err
		}
		if c.recordProtected {
			if err := h.prose("", "", "Saving is unavailable; temporary preferences are not saved."); err != nil {
				return err
			}
		}
		if c.configurationUnsaved {
			if err := h.prose("", "", "Active preferences include unsaved changes; they may be lost on reconnect."); err != nil {
				return err
			}
		}
		for _, line := range []string{"SHOW/FILTER - Inspect all current native restrictions", "SHOW SETTINGS - Inspect your shared preferences", "HELP SET/FILTER - Native commands and examples"} {
			if err := h.line(line); err != nil {
				return err
			}
		}
		return h.prose("", "", "Filter/settings displays pause live spots while you read. RESUME restarts delivery. Missed spots are not replayed.")
	})
}
