// File role: Concise login/readback views over coherent prepared configuration.
package telnet

import (
	"fmt"
	"strings"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

func writeBooleanSettingsAndRules(h *humanResponse, c *Client, cfg filter.Configuration, status configurationReadbackStatus, runtime humanRuntimeStatus, first bool) error {
	title := "Current settings and filters:"
	if first {
		title = "Default settings and filters:"
	}
	if err := h.line(title); err != nil {
		return err
	}
	grid := status.Effective.Grid
	if grid == "" {
		grid = "Not set (SET GRID <grid>)"
	} else if status.Effective.GridDerived {
		grid += " (callsign lookup)"
	}
	if err := h.valueRow("Commands:", strings.ToUpper(status.Effective.Dialect), ""); err != nil {
		return err
	}
	if status.Effective.Grid == "" {
		if err := h.row("Grid:", grid); err != nil {
			return err
		}
	} else if err := h.valueRow("Grid:", status.Effective.Grid, strings.TrimPrefix(grid, status.Effective.Grid)); err != nil {
		return err
	}
	if err := h.valueRow("Noise:", status.Effective.NoiseClass, ""); err != nil {
		return err
	}
	if err := writeHumanDedupe(h, cfg.Settings.DedupePolicy, status, runtime); err != nil {
		return err
	}
	if status.Session.TemporaryDefaults {
		if err := h.row("Saving:", "Unavailable; changes will not be saved."); err != nil {
			return err
		}
	}
	if err := h.line(""); err != nil {
		return err
	}
	if err := writeBooleanRuleRows(h, c.filter); err != nil {
		return err
	}
	nearby := "OFF"
	if c.filter != nil && c.filter.NearbyEnabled {
		nearby = "ON"
		if c.filter.NearbyUserFine == pathreliability.InvalidCell &&
			c.filter.NearbyUserCoarse == pathreliability.InvalidCell {
			nearby += "; location unavailable"
		} else if c.filter.NearbyUserFine == pathreliability.InvalidCell ||
			c.filter.NearbyUserCoarse == pathreliability.InvalidCell {
			nearby += "; location partly available"
		}
	}
	if err := h.row("Nearby:", nearby); err != nil {
		return err
	}
	for _, row := range []struct {
		label string
		on    bool
	}{
		{"BEACON:", c.filter.BeaconsEnabled()}, {"WWV:", c.filter.WWVEnabled()},
		{"WCY:", c.filter.WCYEnabled()}, {"ANNOUNCE:", c.filter.AnnounceEnabled()},
		{"SELF:", c.filter.SelfEnabled()}, {"TOXIC:", c.filter.ToxicEnabled()},
	} {
		value := "OFF"
		if row.on {
			value = "ON"
		}
		if err := h.row(row.label, value); err != nil {
			return err
		}
	}
	if c.filter.SelfEnabled() {
		return h.prose("", "", "SELF ON: spots of your call bypass rules and NEARBY; TOXIC still applies.")
	}
	return nil
}

func writeBooleanEditGuidance(h *humanResponse) error {
	for _, text := range []string{
		"To change rule 0, create a new rule 0.",
		"A different PASS number adds another selection.",
		"Any PASS can match. A matching REJECT blocks the spot.",
		"PASS without SOURCE allows humans and skimmers.",
		"Add AND SOURCE=HUMAN to select human reports only.",
	} {
		if err := h.prose("", "", text); err != nil {
			return err
		}
	}
	return nil
}

// Rendering owns the same configuration lease as command readback. It does not
// start a reading pause or publish defaults while describing restored state.
func (s *Server) renderBooleanLoginBlock(c *Client, first, reset bool) (string, error) {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return "", err
	}
	defer release()
	status, runtime := s.captureReadbackStatus(c, nil), s.captureHumanRuntime(c)
	var response string
	err = c.withBorrowedConfiguration(func(cfg filter.Configuration) error {
		if !readbackMetadataFits(status, "", "") {
			return errReadbackTooLarge
		}
		var renderErr error
		response, renderErr = renderBooleanHumanBody(func(h *humanResponse) error {
			if err := writeBooleanSettingsAndRules(h, c, cfg, status, runtime, first); err != nil {
				return err
			}
			if first || reset {
				// Only generated starting rules have this exact shape. Do not
				// infer source eligibility by flattening arbitrary saved rules.
				rules := cfg.V5().Filters.Rules
				if len(rules) == 1 && rules[0].Action == filter.RulePass &&
					strings.HasPrefix(rules[0].Expr, "MODE=") &&
					strings.HasSuffix(rules[0].Expr, " AND SOURCE=HUMAN") {
					if err := h.line("Report sources: HUMAN only"); err != nil {
						return err
					}
				}
				if err := h.line(""); err != nil {
					return err
				}
				if err := writeBooleanEditGuidance(h); err != nil {
					return err
				}
				for _, text := range []string{
					"To receive CW and FT8 from humans and skimmers:",
					"  PASS RULE 0 MODE=CW,FT8",
				} {
					if err := h.line(text); err != nil {
						return err
					}
				}
			}
			for _, text := range []string{
				"", "SHOW FILTER - See your rules", "SHOW SETTINGS - See your settings",
				"HELP RULE - More examples", "HELP - All commands",
			} {
				if err := h.line(text); err != nil {
					return err
				}
			}
			return nil
		})
		return renderErr
	})
	if err != nil {
		return "", fmt.Errorf("settings/filter display: %w", err)
	}
	return response, nil
}
