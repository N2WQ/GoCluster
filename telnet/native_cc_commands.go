// File role: Stages native CC edits on detached preference candidates. The
// enclosing human transaction owns admission, publication and persistence.
package telnet

import (
	"fmt"
	"strconv"
	"strings"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func (e *filterCommandEngine) stageNativeCCFilterCommand(c *Client, line string) humanCommandOutcome {
	if e == nil || c == nil || c.dialect != DialectCC {
		return humanCommandOutcome{}
	}
	limit := defaultCommandLineLimit
	if c.server != nil && c.server.commandLineLimit > 0 {
		limit = c.server.commandLineLimit
	}
	if len(line) > limit {
		return nativeCCRefusal("Command exceeds the configured line limit. Use native schema-seven YAML for a longer replacing list; do not split it into successive replacements.")
	}
	command, handled, refusal := parseNativeCCCommand(line)
	if !handled {
		return humanCommandOutcome{}
	}
	if refusal != "" {
		return nativeCCRefusal(refusal)
	}
	if command.edit == nativeCCControl {
		action := actionBlock
		if command.enabled {
			action = actionAllow
		}
		response, changed := e.execute(c, parsedFilterCommand{action: action, domain: command.category})
		return humanCommandOutcome{response: response, handled: true, publish: changed, warning: humanWarningFilter}
	}
	c.filterMu.Lock()
	defer c.filterMu.Unlock()
	configuration, exists := c.filter.NativeCCConfiguration()
	if !exists {
		return nativeCCRefusal("Native CC filters are unavailable for this candidate.")
	}
	next, err := command.apply(c, configuration)
	if err != nil {
		return nativeCCRefusal("Invalid native CC selection. " + boundedNativeCCError(err))
	}
	canonical, prepared, err := filter.NewCCFilter(next)
	if err != nil {
		return nativeCCRefusal("Cannot prepare native CC filters. " + boundedNativeCCError(err))
	}
	response, err := command.acceptedNativeResponse(canonical)
	if err != nil {
		return nativeCCRefusal("The complete acknowledgement exceeds the response limit.")
	}
	*c.filter = prepared
	return humanCommandOutcome{response: response, handled: true, publish: true, warning: humanWarningFilter}
}

func (command nativeCCCommand) apply(c *Client, current filter.CCConfiguration) (filter.CCConfiguration, error) {
	switch command.edit {
	case nativeCCMode:
		return current.WithMode(command.category, command.enabled)
	case nativeCCSource:
		return current.WithSource(command.category, command.enabled)
	case nativeCCDXBM:
		return current.WithDXBM(command.action, command.values)
	case nativeCCDefaults:
		defaults, err := filter.CurrentDefaultCCConfiguration()
		if err != nil {
			return filter.CCConfiguration{}, err
		}
		defaults.IncludeBeacons, defaults.AllowWWV, defaults.AllowWCY = current.IncludeBeacons, current.AllowWWV, current.AllowWCY
		defaults.AllowAnnounce, defaults.AllowSelf, defaults.AllowToxic = current.AllowAnnounce, current.AllowSelf, current.AllowToxic
		return defaults, nil
	default:
		category := filter.CCCategory{Action: command.action}
		if command.action != filter.CCOff {
			if command.category == "DOC" || command.category == "DXCTY" {
				if len(command.values) > filter.MaxCCCountries {
					return filter.CCConfiguration{}, fmt.Errorf("country list exceeds %d input values", filter.MaxCCCountries)
				}
				var lookup func() *cty.CTYDatabase
				if c.server != nil {
					lookup = c.server.ctyLookup
				}
				resolve := newRuleDXCCResolver(lookup)
				for _, value := range command.values {
					country, err := nativeCCCountry(value, resolve)
					if err != nil {
						return filter.CCConfiguration{}, err
					}
					category.Countries = append(category.Countries, country)
				}
			} else {
				if len(command.values) > filter.MaxCCStates {
					return filter.CCConfiguration{}, fmt.Errorf("state list exceeds %d input values", filter.MaxCCStates)
				}
				for _, value := range command.values {
					state := strings.ToUpper(value)
					if !spot.IsState(state) {
						return filter.CCConfiguration{}, fmt.Errorf("unsupported state/province code")
					}
					category.States = append(category.States, state)
				}
			}
		}
		return current.WithGeography(command.category, "", category)
	}
}

func nativeCCCountry(value string, resolve func(string) (int, error)) (int, error) {
	number, err := strconv.Atoi(value)
	if err == nil {
		if number <= 0 {
			return 0, fmt.Errorf("ADIF must be a positive integer")
		}
		return number, nil
	}
	integer := value
	if integer != "" && (integer[0] == '+' || integer[0] == '-') {
		integer = integer[1:]
	}
	numeric := integer != ""
	for i := range len(integer) {
		numeric = numeric && integer[i] >= '0' && integer[i] <= '9'
	}
	if numeric {
		return 0, fmt.Errorf("ADIF integer exceeds the supported range")
	}
	return resolve(value)
}

func boundedNativeCCError(err error) string {
	text := err.Error()
	if len(text) > 160 {
		return "The supplied values are invalid or exceed native filter limits."
	}
	return text
}

func nativeCCRefusal(message string) humanCommandOutcome {
	response, err := renderNativeCCBody(maxYAMLBytes, func(h *humanResponse) error {
		if err := h.prose("", "", message); err != nil {
			return err
		}
		return h.line("No changes applied.")
	})
	if err != nil {
		response = "Native CC command refused. Type HELP SET/FILTER.\r\nNo changes applied.\r\n"
	}
	return humanHandled(response)
}

func (command nativeCCCommand) acceptedNativeResponse(configuration filter.CCConfiguration) (string, error) {
	return renderNativeCCBody(maxYAMLBytes, func(h *humanResponse) error {
		switch command.edit {
		case nativeCCMode:
			state := "disabled"
			if command.enabled {
				state = "enabled"
			}
			if err := h.prose("", "", "CC mode "+command.category+" "+state+"; current source selection is unchanged."); err != nil {
				return err
			}
		case nativeCCSource:
			state := "disabled"
			if command.enabled {
				state = "enabled"
			}
			if err := h.line("CC source " + command.category + " " + state + "; mode selection unchanged."); err != nil {
				return err
			}
		case nativeCCDefaults:
			if err := h.prose("", "", "CC filters restored to current operator mode/source defaults. Geography and DXBM restrictions cleared; six switches kept."); err != nil {
				return err
			}
		case nativeCCDXBM:
			if err := writeNativeCCDXBM(h, configuration.DXBM); err != nil {
				return err
			}
		default:
			if err := h.line("CC " + command.category + " " + strings.ToUpper(string(command.action)) + " selection replaced on all bands."); err != nil {
				return err
			}
		}
		return h.line("Use SHOW/FILTER to inspect the complete active CC selection.")
	})
}
