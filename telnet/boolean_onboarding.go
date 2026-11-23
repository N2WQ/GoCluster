// File role: Concise login/readback views over coherent prepared configuration.
package telnet

import (
	"fmt"
	"strings"
	"time"

	"dxcluster/filter"
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
		return h.prose("", "", "SELF ON: spots of your call bypass personal rules; TOXIC still applies.")
	}
	return nil
}

const firstGOFilterExample = "  PASS RULE BAND=20M AND SOURCE=HUMAN"
const firstGOFilterReplacement = "This replaces all of rule 0. Include every condition you want to keep."

var firstGOFilterRecipe = [...]string{
	"For human reports on 20m, try the PASS RULE shortcut.",
	firstGOFilterExample,
	firstGOFilterReplacement,
	"Coming from AR-Cluster? See HELP FILTERS.",
}

const loginSummaryToken = "<LOGIN_SUMMARY>"
const settingsFiltersToken = "<SETTINGS_FILTERS>"

// The bounds cover just the generated recipe, leaving every other line in
// place when this block is expanded a second time.
type loginGreetingBlock struct {
	text                   string
	recipeStart, recipeEnd int
}

type loginGreeting struct {
	data                    templateData
	recipeToken, recipeText string
}

func (g *loginGreeting) blockValue(tmpl, token, prefix string, block loginGreetingBlock) string {
	if block.recipeEnd == block.recipeStart || strings.Count(tmpl, token) < 2 {
		return prefix + block.text
	}
	g.recipeToken, g.recipeText = token, prefix+block.text
	return prefix + block.text[:block.recipeStart] + block.text[block.recipeEnd:]
}

// Each fragment belongs to the same login and is formatted in display order.
// Expand the selected occurrence directly so its generated text is never
// treated as part of the operator's template.
func (g *loginGreeting) format(tmpl string) string {
	if g.recipeToken != "" {
		if before, after, found := strings.Cut(tmpl, g.recipeToken); found {
			g.recipeToken = ""
			return formatGreeting(before, g.data) + g.recipeText + formatGreeting(after, g.data)
		}
	}
	return formatGreeting(tmpl, g.data)
}

func writeBooleanEditGuidance(h *humanResponse) error {
	for _, text := range []string{
		"PASS RULE without a number sets rule 0. Include every condition to keep.",
		"Use PASS RULE 0 with your expression to replace a REJECT in rule 0.",
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

func loginSummary(data templateData, restored restoredLogin, showRecipe bool) loginGreetingBlock {
	var b strings.Builder
	var block loginGreetingBlock
	unit := "users"
	if data.userCount == 1 {
		unit = "user"
	}
	fmt.Fprintf(&b, "Cluster: %d %s | Uptime: %s\n", data.userCount, unit, formatUptime(data.now, data.startTime))
	if !data.lastLogin.IsZero() {
		fmt.Fprintf(&b, "Last login: %s", data.lastLogin.UTC().Format("02-Jan-2006 15:04:05 UTC"))
		if ip := strings.TrimSpace(data.lastIP); ip != "" {
			fmt.Fprintf(&b, " from %s", ip)
		}
		b.WriteByte('\n')
	}
	if restored.loadError == nil {
		switch {
		case restored.created:
			fmt.Fprintf(&b, "Starting filters are active (%s).\n", data.dialect)
			if showRecipe {
				block.recipeStart = b.Len()
				for _, line := range firstGOFilterRecipe {
					b.WriteString(line + "\n")
				}
				block.recipeEnd = b.Len()
			}
		case !restored.profileReset:
			fmt.Fprintf(&b, "Saved settings loaded (%s).\n", data.dialect)
		}
	}
	if data.grid == "unset" {
		b.WriteString("Grid not set. Type SET GRID <grid>.\n")
	}
	b.WriteString(loginFilterCommand(data.dialect) + ": your filters | SHOW SETTINGS: your settings | HELP: commands")
	block.text = b.String()
	return block
}

func loginFilterCommand(dialect string) string {
	if strings.EqualFold(dialect, string(DialectCC)) {
		return "SHOW/FILTER"
	}
	return "SHOW FILTER"
}

// Rendering owns the same configuration lease as command readback. It does not
// start a reading pause or publish defaults while describing restored state.
func (s *Server) renderBooleanLoginBlock(c *Client, first, reset, showRecipe bool) (loginGreetingBlock, error) {
	if c != nil && c.filterBanks != nil && c.dialect == DialectCC {
		text, err := s.renderV7CCLoginBlock(c, first)
		return loginGreetingBlock{text: text}, err
	}
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return loginGreetingBlock{}, err
	}
	defer release()
	status, runtime := s.captureReadbackStatus(c, nil), s.captureHumanRuntime(c)
	var block loginGreetingBlock
	err = c.withBorrowedConfiguration(func(cfg filter.Configuration) error {
		if !readbackMetadataFits(status, "", "") {
			return errReadbackTooLarge
		}
		var renderErr error
		block.text, renderErr = renderBooleanHumanBody(func(h *humanResponse) error {
			if err := writeBooleanSettingsAndRules(h, c, cfg, status, runtime, first); err != nil {
				return err
			}
			if first || reset {
				// Only generated starting rules have this exact shape. Do not
				// infer source eligibility by flattening arbitrary saved rules.
				rules := cfg.V6().Filters.Rules
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
				if showRecipe {
					block.recipeStart = h.size
					for _, text := range firstGOFilterRecipe {
						if err := h.line(text); err != nil {
							return err
						}
					}
					block.recipeEnd = h.size
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
		return loginGreetingBlock{}, fmt.Errorf("settings/filter display: %w", err)
	}
	return block, nil
}
