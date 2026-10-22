// File role: Owns the user-facing telnet filter command engine.
// Crawler notes: Start here for numbered-rule and separate-control dispatch,
// dialect alias mapping, NEARBY control behavior, and
// per-client filter persistence side effects.
// Related docs: telnet/README.md, commands/README.md, data/config/README.md.
// Related tests: telnet/*filter*_test.go, telnet/path_settings_test.go.
package telnet

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

// DialectName identifies the command dialect a client is using.
type DialectName string

const (
	// DialectGo is the default PASS/REJECT/SHOW FILTER syntax.
	DialectGo DialectName = "go"
	// DialectCC models a subset of CC Cluster aliases mapped onto existing filter atoms.
	DialectCC DialectName = "cc"
)

type filterAction int

const (
	actionAllow filterAction = iota
	actionBlock
	actionSummary
	actionResetDefaults
)

type parsedFilterCommand struct {
	action filterAction
	domain string
	args   []string
}

type dialectSpec struct {
	name    DialectName
	aliases []string
	parse   func(tokens []string, upper []string) (parsedFilterCommand, bool, string)
}

type domainHandler struct {
	name    string
	aliases []string
	apply   func(c *Client, action filterAction, args []string) (string, bool)
}

type filterCommandEngine struct {
	dialects       map[DialectName]*dialectSpec
	dialectAliases map[string]DialectName
	domains        map[string]*domainHandler
	defaultDialect DialectName
}

const nearbyUsageMsg = "Usage: PASS NEARBY ON|OFF\nType HELP for usage.\n"
const nearbyMissingGridMsg = "Grid not set. Use SET GRID <4 or 6 character Maidenhead locator>.\n"
const nearbyUnavailableMsg = "Unable to enable NEARBY: invalid grid or H3 tables unavailable.\n"

func newFilterCommandEngine() *filterCommandEngine {
	engine := &filterCommandEngine{
		dialects:       make(map[DialectName]*dialectSpec),
		dialectAliases: make(map[string]DialectName),
		domains:        make(map[string]*domainHandler),
		defaultDialect: DialectGo,
	}
	engine.registerDialects()
	engine.registerDomains()
	return engine
}

func (e *filterCommandEngine) registerDialects() {
	goDialect := &dialectSpec{
		name:    DialectGo,
		aliases: []string{"go", "gocluster", "classic", "default"},
		parse:   parseClassicDialect,
	}
	cc := &dialectSpec{
		name:    DialectCC,
		aliases: []string{"cc"},
		parse:   parseCCDialect,
	}
	for _, d := range []*dialectSpec{goDialect, cc} {
		e.dialects[d.name] = d
		for _, alias := range d.aliases {
			e.dialectAliases[strings.ToLower(alias)] = d.name
		}
	}
}

func (e *filterCommandEngine) registerDomains() {
	handlers := []*domainHandler{
		newNearbyHandler(),
		newFeatureToggleHandler("BEACON", func(f *filter.Filter, enabled bool) { f.SetBeaconEnabled(enabled) }),
		newFeatureToggleHandler("WWV", func(f *filter.Filter, enabled bool) { f.SetWWVEnabled(enabled) }),
		newFeatureToggleHandler("WCY", func(f *filter.Filter, enabled bool) { f.SetWCYEnabled(enabled) }),
		newFeatureToggleHandler("ANNOUNCE", func(f *filter.Filter, enabled bool) { f.SetAnnounceEnabled(enabled) }, "PC93"),
		newFeatureToggleHandler("SELF", func(f *filter.Filter, enabled bool) { f.SetSelfEnabled(enabled) }),
		newFeatureToggleHandler("TOXIC", func(f *filter.Filter, enabled bool) { f.SetToxicEnabled(enabled) }),
	}
	for _, h := range handlers {
		e.registerDomain(h)
	}
}

func (e *filterCommandEngine) registerDomain(handler *domainHandler) {
	e.domains[strings.ToUpper(handler.name)] = handler
	for _, alias := range handler.aliases {
		e.domains[strings.ToUpper(alias)] = handler
	}
}

func (e *filterCommandEngine) availableDialectNames() []string {
	if e == nil {
		return nil
	}
	names := make([]string, 0, len(e.dialects))
	for name := range e.dialects {
		names = append(names, string(name))
	}
	sort.Strings(names)
	return names
}

func normalizeDialectName(name string) DialectName {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "cc":
		return DialectCC
	case "classic", "default", "gocluster", "go", "":
		return DialectGo
	default:
		return DialectGo
	}
}

// Handle parses and executes a filter command for the active dialect.
// Returns the user-facing response and whether the command was handled.
func (e *filterCommandEngine) Handle(client *Client, line string) (string, bool) {
	if client == nil || e == nil {
		return "", false
	}
	release, err := client.server.acquireConfiguration(client, false, false, time.Time{})
	if err != nil {
		return fmt.Sprintf("Filter command failed: %v\n", err), true
	}
	defer release()
	client.refreshConfigurationRevision()
	outcome, needsStage := e.preflightHumanFilterOwned(client, line)
	response, handled := outcome.response, outcome.handled
	if needsStage {
		response, handled = client.server.runHumanPreferenceOwned(client, line, e.stageOwned)
	}
	client.refreshConfigurationRevision()
	return response, handled
}

func (e *filterCommandEngine) stageOwned(client *Client, line string) humanCommandOutcome {
	if outcome := e.stageBooleanFilterCommand(client, line); outcome.handled {
		return outcome
	}
	tokens := strings.Fields(strings.TrimSpace(line))
	if len(tokens) == 0 {
		return humanCommandOutcome{}
	}
	upper := make([]string, len(tokens))
	for i, t := range tokens {
		upper[i] = strings.ToUpper(t)
	}

	active := client.dialect
	parsed, ok, errText := e.parse(tokens, upper, active)
	if ok {
		if errText != "" {
			return humanHandled(errText)
		}
		resp, mutated := e.execute(client, parsed)
		return humanCommandOutcome{response: resp, handled: true, publish: mutated, warning: humanWarningFilter}
	}

	if hint := e.hintForOtherDialect(tokens, upper, active); hint != "" {
		return humanHandled(hint)
	}

	return humanCommandOutcome{}
}

func (e *filterCommandEngine) parse(tokens, upper []string, dialect DialectName) (parsedFilterCommand, bool, string) {
	spec, ok := e.dialects[dialect]
	if !ok || spec == nil {
		return parsedFilterCommand{}, false, ""
	}
	parsed, matched, errText := spec.parse(tokens, upper)
	if !matched && dialect != DialectGo {
		// Try go/classic as a safe fallback to preserve current behavior when the
		// client switches to an unknown dialect.
		if gc, ok := e.dialects[DialectGo]; ok && gc != nil {
			return gc.parse(tokens, upper)
		}
	}
	return parsed, matched, errText
}

func (e *filterCommandEngine) hintForOtherDialect(tokens, upper []string, active DialectName) string {
	for name, spec := range e.dialects {
		if name == active || spec == nil {
			continue
		}
		_, matched, _ := spec.parse(tokens, upper)
		if matched {
			return fmt.Sprintf("Command matches the %s dialect. Switch with DIALECT %s or use the %s syntax.\n", spec.name, strings.ToUpper(string(spec.name)), strings.ToUpper(string(active)))
		}
	}
	return ""
}

func (e *filterCommandEngine) execute(client *Client, cmd parsedFilterCommand) (string, bool) {
	switch cmd.action {
	case actionSummary:
		client.filterMu.RLock()
		resp, err := formatBooleanFilterSnapshot(client.filter)
		client.filterMu.RUnlock()
		if err != nil {
			return fmt.Sprintf("Readback failed: %v\n", err), false
		}
		return resp, false
	case actionResetDefaults:
		return booleanRuleRefusal(booleanRuleResetUsage).response, false
	}

	if !booleanSeparateDomain(cmd.domain) {
		return booleanRuleRefusal(booleanRuleDiscovery).response, false
	}
	handler := e.domains[strutil.NormalizeUpper(cmd.domain)]
	if handler == nil {
		return booleanRuleRefusal(booleanRuleDiscovery).response, false
	}

	resp, mutated := handler.apply(client, cmd.action, cmd.args)
	return resp, mutated
}

func parseClassicDialect(tokens, upper []string) (parsedFilterCommand, bool, string) {
	if len(upper) == 0 {
		return parsedFilterCommand{}, false, ""
	}
	switch upper[0] {
	case "RESET":
		if len(upper) == 2 && upper[1] == "FILTER" {
			return parsedFilterCommand{action: actionResetDefaults}, true, ""
		}
		return parsedFilterCommand{}, true, "Usage: RESET FILTER\nType HELP for usage.\n"
	case "PASS":
		if len(upper) < 2 {
			return parsedFilterCommand{}, true, passFilterUsageMsg()
		}
		return parsedFilterCommand{action: actionAllow, domain: upper[1], args: tokens[2:]}, true, ""
	case "REJECT":
		if len(upper) < 2 {
			return parsedFilterCommand{}, true, rejectFilterUsageMsg()
		}
		return parsedFilterCommand{action: actionBlock, domain: upper[1], args: tokens[2:]}, true, ""
	case "SHOW":
		if len(upper) < 2 || upper[1] != "FILTER" {
			return parsedFilterCommand{}, false, ""
		}
		return parsedFilterCommand{action: actionSummary}, true, ""
	default:
		return parsedFilterCommand{}, false, ""
	}
}

func parseCCDialect(tokens, upper []string) (parsedFilterCommand, bool, string) {
	if len(upper) == 0 {
		return parsedFilterCommand{}, false, ""
	}

	if upper[0] == "SET/FILTER" && len(upper) >= 2 {
		// DXBM/PASS or DXBM/REJECT inline or separated.
		if strings.HasPrefix(strings.ToUpper(upper[1]), "DXBM/") {
			mode := strings.TrimPrefix(strings.ToUpper(upper[1]), "DXBM/")
			switch mode {
			case "PASS":
				return parsedFilterCommand{action: actionAllow, domain: "DXBM", args: tokens[2:]}, true, ""
			case "REJECT":
				return parsedFilterCommand{action: actionBlock, domain: "DXBM", args: tokens[2:]}, true, ""
			}
		} else if strings.EqualFold(upper[1], "DXBM") && len(upper) >= 3 {
			if strings.EqualFold(upper[2], "PASS") {
				return parsedFilterCommand{action: actionAllow, domain: "DXBM", args: tokens[3:]}, true, ""
			}
			if strings.EqualFold(upper[2], "REJECT") {
				return parsedFilterCommand{action: actionBlock, domain: "DXBM", args: tokens[3:]}, true, ""
			}
		}
	}

	switch upper[0] {
	case "RESET":
		if len(upper) == 2 && upper[1] == "FILTER" {
			return parsedFilterCommand{action: actionResetDefaults}, true, ""
		}
		return parsedFilterCommand{}, true, "Usage: RESET FILTER\nType HELP for usage.\n"
	case "SET/ANN":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "ANNOUNCE"})
	case "SET/NOANN":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionBlock, domain: "ANNOUNCE"})
	case "SET/BEACON":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "BEACON"})
	case "SET/NOBEACON":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionBlock, domain: "BEACON"})
	case "SET/WWV":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "WWV"})
	case "SET/NOWWV":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionBlock, domain: "WWV"})
	case "SET/WCY":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "WCY"})
	case "SET/NOWCY":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionBlock, domain: "WCY"})
	case "SET/SELF":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "SELF"})
	case "SET/NOSELF":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionBlock, domain: "SELF"})
	case "SET/SKIMMER":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "SOURCE", args: []string{"SKIMMER"}})
	case "SET/NOSKIMMER":
		return parseCCShortcut(upper, parsedFilterCommand{action: actionBlock, domain: "SOURCE", args: []string{"SKIMMER"}})
	case "SET/NOFILTER":
		// CC "no filter" means allow everything.
		return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "NOFILTER"})
	case "SET/FILTER":
		if len(upper) < 2 {
			return parsedFilterCommand{}, true, passFilterUsageMsg()
		}
		domain := upper[1]
		args := tokens[2:]
		// Recognize inline /OFF to disable a domain quickly.
		if strings.HasSuffix(domain, "/OFF") {
			if len(args) != 0 {
				return parsedFilterCommand{}, true, fmt.Sprintf("Usage: SET/FILTER %s\nNo changes applied.\n", domain)
			}
			domain = strings.TrimSuffix(domain, "/OFF")
			args = []string{"ALL"}
			return parsedFilterCommand{action: actionBlock, domain: domain, args: args}, true, ""
		}
		if strings.HasSuffix(domain, "/ON") {
			if len(args) != 0 {
				return parsedFilterCommand{}, true, fmt.Sprintf("Usage: SET/FILTER %s\nNo changes applied.\n", domain)
			}
			domain = strings.TrimSuffix(domain, "/ON")
			args = []string{"ALL"}
			return parsedFilterCommand{action: actionAllow, domain: domain, args: args}, true, ""
		}
		return parsedFilterCommand{action: actionAllow, domain: domain, args: args}, true, ""
	case "UNSET/FILTER":
		if len(upper) < 2 {
			return parsedFilterCommand{}, true, rejectFilterUsageMsg()
		}
		return parsedFilterCommand{action: actionBlock, domain: upper[1], args: tokens[2:]}, true, ""
	case "SHOW/FILTER", "SH/FILTER":
		return parsedFilterCommand{action: actionSummary}, true, ""
	default:
		// Mode shortcuts: SET/FT8, SET/NOFT8, etc.
		if strings.HasPrefix(upper[0], "SET/NO") {
			mode := strings.TrimPrefix(upper[0], "SET/NO")
			if isCCMode(mode) {
				return parseCCShortcut(upper, parsedFilterCommand{action: actionBlock, domain: "MODE", args: []string{mode}})
			}
		}
		if strings.HasPrefix(upper[0], "SET/") && !strings.Contains(upper[0], "/NO") {
			mode := strings.TrimPrefix(upper[0], "SET/")
			if isCCMode(mode) {
				return parseCCShortcut(upper, parsedFilterCommand{action: actionAllow, domain: "MODE", args: []string{mode}})
			}
		}
		return parsedFilterCommand{}, false, ""
	}
}

// Check the original shortcut before accepting its normalized value; otherwise its
// ignored tail can acknowledge an unintended change under the full command.
func parseCCShortcut(upper []string, command parsedFilterCommand) (parsedFilterCommand, bool, string) {
	if len(upper) != 1 {
		return parsedFilterCommand{}, true, fmt.Sprintf("Usage: %s\nNo changes applied.\n", upper[0])
	}
	return command, true, ""
}

func newNearbyHandler() *domainHandler {
	return &domainHandler{
		name: "NEARBY",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			if c == nil || c.filter == nil {
				return "Filter unavailable.\n", false
			}
			if len(args) != 1 {
				return nearbyUsageMsg, false
			}
			mode := strutil.NormalizeUpper(args[0])
			switch mode {
			case "ON":
				state := c.pathSnapshot()
				grid := strings.TrimSpace(state.grid)
				if grid == "" {
					return nearbyMissingGridMsg, false
				}
				userFine := state.gridCell
				if userFine == pathreliability.InvalidCell {
					userFine = pathreliability.EncodeCell(grid)
				}
				userCoarse := state.gridCoarseCell
				if userCoarse == pathreliability.InvalidCell {
					userCoarse = pathreliability.EncodeCoarseCell(grid)
				}
				if userFine == pathreliability.InvalidCell || userCoarse == pathreliability.InvalidCell {
					return nearbyUnavailableMsg, false
				}
				var enableErr error
				c.updateFilter(func(f *filter.Filter) {
					enableErr = f.EnableNearby(userFine, userCoarse)
				})
				if enableErr != nil {
					log.Printf("Warning: failed to enable nearby filter for %s: %v", c.callsign, enableErr)
					return nearbyUnavailableMsg, false
				}
				return "Nearby filter enabled.\n", true
			case "OFF":
				c.updateFilter(func(f *filter.Filter) {
					f.DisableNearby()
				})
				return "Nearby filter disabled.\n", true
			default:
				return nearbyUsageMsg, false
			}
		},
	}
}

func newFeatureToggleHandler(name string, setter func(*filter.Filter, bool), aliases ...string) *domainHandler {
	enableLabel, disableLabel, _ := featureLabels(name)
	return &domainHandler{
		name:    name,
		aliases: aliases,
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			if len(args) != 0 && (len(args) != 1 || !strings.EqualFold(args[0], "ALL")) {
				return fmt.Sprintf("%s accepts no value or ALL. No changes applied.\n", name), false
			}
			switch action {
			case actionAllow:
				c.updateFilter(func(f *filter.Filter) { setter(f, true) })
				return fmt.Sprintf("%s enabled\n", enableLabel), true
			case actionBlock:
				c.updateFilter(func(f *filter.Filter) { setter(f, false) })
				return fmt.Sprintf("%s disabled\n", disableLabel), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func titleCase(name string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	if lower == "" {
		return ""
	}
	return strings.ToUpper(lower[:1]) + lower[1:]
}

func featureLabels(name string) (enable string, disable string, status string) {
	switch strings.ToUpper(name) {
	case "BEACON":
		return "Beacon spots", "Beacon spots", "Beacon spots"
	case "WWV":
		return "WWV bulletins", "WWV bulletins", "WWV bulletins"
	case "WCY":
		return "WCY bulletins", "WCY bulletins", "WCY bulletins"
	case "ANNOUNCE":
		return "Announcements", "Announcements", "Announcements"
	case "SELF":
		return "Self spots", "Self spots", "Self spots"
	case "TOXIC":
		return "Toxic spots", "Toxic spots", "Toxic spots"
	default:
		tc := titleCase(name)
		return tc, tc, tc
	}
}

func isCCMode(mode string) bool {
	return spot.IsCCShortcutMode(strutil.NormalizeUpper(mode))
}
