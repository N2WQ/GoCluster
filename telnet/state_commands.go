// File role: State commands share the US/Canadian mailing-address vocabulary with imported metadata.
// Parse the whole list before mutating preferences so invalid input is atomic.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

func parseStateList(value string) (states, invalid []string) {
	seen := make(map[string]bool, 73)
	for _, token := range strings.FieldsFunc(strings.ToUpper(value), func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		if !spot.IsState(token) {
			invalid = append(invalid, token)
			continue
		}
		if !seen[token] {
			states = append(states, token)
			seen[token] = true
		}
	}
	return states, invalid
}

func newStateHandler(name string) *domainHandler {
	return &domainHandler{name: name, apply: func(c *Client, action filterAction, args []string) (string, bool) {
		if action != actionAllow && action != actionBlock {
			return invalidFilterCommandMsg, false
		}
		verb := "PASS"
		if action == actionBlock {
			verb = "REJECT"
		}
		value := strings.TrimSpace(strings.Join(args, " "))
		all := strings.EqualFold(value, "ALL")
		states, invalid := parseStateList(value)
		if !all && len(invalid) != 0 {
			return "Invalid state selection: " + strings.Join(invalid, ", ") + "\n", false
		}
		if !all && len(states) == 0 {
			return fmt.Sprintf("Usage: %s %s <state>[,<state>...] (US state/Canadian province codes, or ALL)\nType HELP for usage.\n", verb, name), false
		}
		c.updateFilter(func(f *filter.Filter) {
			if all {
				if name == "DXSTATE" {
					f.ResetDXStates()
					f.BlockAllDXStates, f.AllDXStates = action == actionBlock, action == actionAllow
				} else {
					f.ResetDEStates()
					f.BlockAllDEStates, f.AllDEStates = action == actionBlock, action == actionAllow
				}
				return
			}
			for _, state := range states {
				if name == "DXSTATE" {
					f.SetDXState(state, action == actionAllow)
				} else {
					f.SetDEState(state, action == actionAllow)
				}
			}
		})
		return fmt.Sprintf("%s %s %s\n", verb, name, strings.ToUpper(value)), true
	}}
}
