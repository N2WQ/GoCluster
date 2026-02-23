package telnet

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"dxcluster/cty"
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
	note   string
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
	ctyLookup      func() *cty.CTYDatabase
}

const nearbyLocationFilterWarning = "This filter is disabled when NEARBY ON. Disable NEARBY if you want to use regular location filters\n"
const nearbyUsageMsg = "Usage: PASS NEARBY ON|OFF\nType HELP for usage.\n"
const nearbyMissingGridMsg = "Grid not set. Use SET GRID <4-6 char maidenhead>.\n"
const nearbyUnavailableMsg = "Unable to enable NEARBY: invalid grid or H3 tables unavailable.\n"

var locationFilterDomains = map[string]bool{
	"DXGRID2": true,
	"DEGRID2": true,
	"DXCONT":  true,
	"DECONT":  true,
	"DXZONE":  true,
	"DEZONE":  true,
	"DXDXCC":  true,
	"DEDXCC":  true,
}

func newFilterCommandEngine() *filterCommandEngine {
	return newFilterCommandEngineWithCTY(nil)
}

func newFilterCommandEngineWithCTY(ctyLookup func() *cty.CTYDatabase) *filterCommandEngine {
	engine := &filterCommandEngine{
		dialects:       make(map[DialectName]*dialectSpec),
		dialectAliases: make(map[string]DialectName),
		domains:        make(map[string]*domainHandler),
		defaultDialect: DialectGo,
		ctyLookup:      ctyLookup,
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
		newBandHandler(),
		newDXBMHandler(),
		newModeHandler(),
		newSourceHandler(),
		newNoFilterHandler(),
		newCallPatternHandler("DXCALL"),
		newCallPatternHandler("DECALL"),
		newConfidenceHandler(),
		newPathHandler(),
		newNearbyHandler(),
		newContinentHandler("DXCONT", func(f *filter.Filter, value string, allowed bool) { f.SetDXContinent(value, allowed) }),
		newContinentHandler("DECONT", func(f *filter.Filter, value string, allowed bool) { f.SetDEContinent(value, allowed) }),
		newZoneHandler("DXZONE", func(f *filter.Filter, value int, allowed bool) { f.SetDXZone(value, allowed) }),
		newZoneHandler("DEZONE", func(f *filter.Filter, value int, allowed bool) { f.SetDEZone(value, allowed) }),
		newDXCCHandler("DXDXCC", func(f *filter.Filter, code int, allowed bool) { f.SetDXDXCC(code, allowed) }),
		newDXCCHandler("DEDXCC", func(f *filter.Filter, code int, allowed bool) { f.SetDEDXCC(code, allowed) }),
		newGrid2Handler("DXGRID2", func(f *filter.Filter, value string, allowed bool) { f.SetDXGrid2Prefix(value, allowed) }),
		newGrid2Handler("DEGRID2", func(f *filter.Filter, value string, allowed bool) { f.SetDEGrid2Prefix(value, allowed) }),
		newFeatureToggleHandler("BEACON", func(f *filter.Filter, enabled bool) { f.SetBeaconEnabled(enabled) }),
		newFeatureToggleHandler("WWV", func(f *filter.Filter, enabled bool) { f.SetWWVEnabled(enabled) }),
		newFeatureToggleHandler("WCY", func(f *filter.Filter, enabled bool) { f.SetWCYEnabled(enabled) }),
		newFeatureToggleHandler("ANNOUNCE", func(f *filter.Filter, enabled bool) { f.SetAnnounceEnabled(enabled) }, "PC93"),
		newFeatureToggleHandler("SELF", func(f *filter.Filter, enabled bool) { f.SetSelfEnabled(enabled) }),
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
	tokens := strings.Fields(strings.TrimSpace(line))
	if len(tokens) == 0 {
		return "", false
	}
	upper := make([]string, len(tokens))
	for i, t := range tokens {
		upper[i] = strings.ToUpper(t)
	}

	active := client.dialect
	parsed, ok, errText := e.parse(tokens, upper, active)
	if ok {
		if errText != "" {
			return errText, true
		}
		resp, mutated := e.execute(client, parsed)
		if mutated {
			if err := client.saveFilter(); err != nil {
				log.Printf("Warning: failed to persist filter for %s: %v", client.callsign, err)
			}
		}
		return resp, true
	}

	if hint := e.hintForOtherDialect(tokens, upper, active); hint != "" {
		return hint, true
	}

	return "", false
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
		resp := formatFilterSnapshot(client.filter, e.ctyLookup)
		client.filterMu.RUnlock()
		if cmd.note != "" {
			resp += cmd.note
		}
		return resp, false
	case actionResetDefaults:
		client.updateFilter(func(f *filter.Filter) { f.ResetToDefaults() })
		return "Filters reset to defaults\n", true
	}

	handler := e.domains[strutil.NormalizeUpper(cmd.domain)]
	if handler == nil {
		switch cmd.action {
		case actionAllow:
			if strings.EqualFold(cmd.domain, "DXBM") {
				return "Unknown DXBM band. Supported: 160, 80, 40, 30, 20, 17, 15, 12, 10, 6, 2 (and 1 if enabled). Modes ignored; use SET/NO<MODE>.\n", false
			}
			return unknownPassTypeMsg, false
		case actionBlock:
			if strings.EqualFold(cmd.domain, "DXBM") {
				return "Unknown DXBM band. Supported: 160, 80, 40, 30, 20, 17, 15, 12, 10, 6, 2 (and 1 if enabled). Modes ignored; use SET/NO<MODE>.\n", false
			}
			return unknownRejectTypeMsg, false
		default:
			return invalidFilterCommandMsg, false
		}
	}

	if cmd.action == actionAllow || cmd.action == actionBlock {
		domainKey := strutil.NormalizeUpper(cmd.domain)
		if locationFilterDomains[domainKey] {
			client.filterMu.RLock()
			nearby := client.filter != nil && client.filter.NearbyActive()
			client.filterMu.RUnlock()
			if nearby {
				return nearbyLocationFilterWarning, false
			}
		}
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
			return parsedFilterCommand{}, true, passFilterUsageMsg
		}
		return parsedFilterCommand{action: actionAllow, domain: upper[1], args: tokens[2:]}, true, ""
	case "REJECT":
		if len(upper) < 2 {
			return parsedFilterCommand{}, true, rejectFilterUsageMsg
		}
		return parsedFilterCommand{action: actionBlock, domain: upper[1], args: tokens[2:]}, true, ""
	case "SHOW":
		if len(upper) < 2 || upper[1] != "FILTER" {
			return parsedFilterCommand{}, false, ""
		}
		cmd := parsedFilterCommand{action: actionSummary}
		if len(upper) > 2 {
			cmd.note = showFilterDeprecationNote(upper, DialectGo)
		}
		return cmd, true, ""
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
		return parsedFilterCommand{action: actionAllow, domain: "ANNOUNCE"}, true, ""
	case "SET/NOANN":
		return parsedFilterCommand{action: actionBlock, domain: "ANNOUNCE"}, true, ""
	case "SET/BEACON":
		return parsedFilterCommand{action: actionAllow, domain: "BEACON"}, true, ""
	case "SET/NOBEACON":
		return parsedFilterCommand{action: actionBlock, domain: "BEACON"}, true, ""
	case "SET/WWV":
		return parsedFilterCommand{action: actionAllow, domain: "WWV"}, true, ""
	case "SET/NOWWV":
		return parsedFilterCommand{action: actionBlock, domain: "WWV"}, true, ""
	case "SET/WCY":
		return parsedFilterCommand{action: actionAllow, domain: "WCY"}, true, ""
	case "SET/NOWCY":
		return parsedFilterCommand{action: actionBlock, domain: "WCY"}, true, ""
	case "SET/SELF":
		return parsedFilterCommand{action: actionAllow, domain: "SELF"}, true, ""
	case "SET/NOSELF":
		return parsedFilterCommand{action: actionBlock, domain: "SELF"}, true, ""
	case "SET/SKIMMER":
		return parsedFilterCommand{action: actionAllow, domain: "SOURCE", args: []string{"SKIMMER"}}, true, ""
	case "SET/NOSKIMMER":
		return parsedFilterCommand{action: actionBlock, domain: "SOURCE", args: []string{"SKIMMER"}}, true, ""
	case "SET/NOFILTER":
		// CC "no filter" means allow everything.
		return parsedFilterCommand{action: actionAllow, domain: "NOFILTER"}, true, ""
	case "SET/FILTER":
		if len(upper) < 2 {
			return parsedFilterCommand{}, true, passFilterUsageMsg
		}
		domain := upper[1]
		args := tokens[2:]
		// Recognize inline /OFF to disable a domain quickly.
		if strings.HasSuffix(domain, "/OFF") {
			domain = strings.TrimSuffix(domain, "/OFF")
			args = []string{"ALL"}
			return parsedFilterCommand{action: actionBlock, domain: domain, args: args}, true, ""
		}
		if strings.HasSuffix(domain, "/ON") {
			domain = strings.TrimSuffix(domain, "/ON")
			args = []string{"ALL"}
			return parsedFilterCommand{action: actionAllow, domain: domain, args: args}, true, ""
		}
		return parsedFilterCommand{action: actionAllow, domain: domain, args: args}, true, ""
	case "UNSET/FILTER":
		if len(upper) < 2 {
			return parsedFilterCommand{}, true, rejectFilterUsageMsg
		}
		return parsedFilterCommand{action: actionBlock, domain: upper[1], args: tokens[2:]}, true, ""
	case "SHOW/FILTER", "SH/FILTER":
		cmd := parsedFilterCommand{action: actionSummary}
		if len(upper) > 1 {
			cmd.note = showFilterDeprecationNote(upper, DialectCC)
		}
		return cmd, true, ""
	default:
		// Mode shortcuts: SET/FT8, SET/NOFT8, etc. (CC supports CW, FT4, FT8, RTTY)
		if len(upper) == 1 && strings.HasPrefix(upper[0], "SET/NO") {
			mode := strings.TrimPrefix(upper[0], "SET/NO")
			if isCCMode(mode) {
				return parsedFilterCommand{action: actionBlock, domain: "MODE", args: []string{mode}}, true, ""
			}
		}
		if len(upper) == 1 && strings.HasPrefix(upper[0], "SET/") && !strings.Contains(upper[0], "/NO") {
			mode := strings.TrimPrefix(upper[0], "SET/")
			if isCCMode(mode) {
				return parsedFilterCommand{action: actionAllow, domain: "MODE", args: []string{mode}}, true, ""
			}
		}
		return parsedFilterCommand{}, false, ""
	}
}

func newBandHandler() *domainHandler {
	return &domainHandler{
		name: "BAND",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			switch action {
			case actionAllow:
				value := strings.TrimSpace(strings.Join(args, " "))
				if value == "" {
					return passFilterUsageMsg, false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) { f.ResetBands() })
					return "All bands enabled\n", true
				}
				rawBands := parseBandList(value)
				if len(rawBands) == 0 {
					return passFilterUsageMsg, false
				}
				normalizedBands, invalid := normalizeBands(rawBands)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown band: %s\nSupported bands: %s\n", strings.Join(invalid, ", "), strings.Join(spot.SupportedBandNames(), ", ")), false
				}
				if len(normalizedBands) == 0 {
					return passFilterUsageMsg, false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, band := range normalizedBands {
						f.SetBand(band, true)
					}
				})
				if len(normalizedBands) == 1 {
					return fmt.Sprintf("Filter set: Band %s\n", normalizedBands[0]), true
				}
				return fmt.Sprintf("Filter set: Bands %s\n", strings.Join(normalizedBands, ", ")), true
			case actionBlock:
				value := strings.TrimSpace(strings.Join(args, " "))
				if value == "" {
					return rejectFilterUsageMsg, false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						f.ResetBands()
						f.BlockAllBands = true
						f.AllBands = false
					})
					return "All bands blocked\n", true
				}
				rawBands := parseBandList(value)
				if len(rawBands) == 0 {
					return rejectFilterUsageMsg, false
				}
				normalizedBands, invalid := normalizeBands(rawBands)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown band: %s\nSupported bands: %s\n", strings.Join(invalid, ", "), strings.Join(spot.SupportedBandNames(), ", ")), false
				}
				if len(normalizedBands) == 0 {
					return rejectFilterUsageMsg, false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, band := range normalizedBands {
						f.SetBand(band, false)
					}
				})
				return fmt.Sprintf("Band filters disabled: %s\n", strings.Join(normalizedBands, ", ")), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func newDXBMHandler() *domainHandler {
	return &domainHandler{
		name: "DXBM",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			return applyDXBMBands(c, action, args)
		},
	}
}

func newModeHandler() *domainHandler {
	return &domainHandler{
		name: "MODE",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			switch action {
			case actionAllow:
				modeArgs := strings.TrimSpace(strings.Join(args, " "))
				if strings.EqualFold(modeArgs, "ALL") {
					c.updateFilter(func(f *filter.Filter) { f.ResetModes() })
					return "All modes enabled\n", true
				}
				modes := parseModeList(modeArgs)
				if len(modes) == 0 {
					return "Usage: PASS MODE <mode>[,<mode>...] (comma or space separated)\nType HELP for usage.\n", false
				}
				invalid := collectInvalidModes(modes)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown mode: %s\nSupported modes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedModes, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, mode := range modes {
						f.SetMode(mode, true)
					}
				})
				return fmt.Sprintf("Filter set: Modes %s\n", strings.Join(modes, ", ")), true
			case actionBlock:
				modeArgs := strings.TrimSpace(strings.Join(args, " "))
				if modeArgs == "" {
					return "Usage: REJECT MODE <mode>[,<mode>...] (comma or space separated, or ALL)\nType HELP for usage.\n", false
				}
				if strings.EqualFold(modeArgs, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						f.ResetModes()
						f.BlockAllModes = true
						f.AllModes = false
					})
					return "All modes blocked\n", true
				}
				modes := parseModeList(modeArgs)
				if len(modes) == 0 {
					return "Usage: REJECT MODE <mode>[,<mode>...] (comma or space separated, or ALL)\nType HELP for usage.\n", false
				}
				invalid := collectInvalidModes(modes)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown mode: %s\nSupported modes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedModes, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, mode := range modes {
						f.SetMode(mode, false)
					}
				})
				return fmt.Sprintf("Mode filters disabled: %s\n", strings.Join(modes, ", ")), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func newSourceHandler() *domainHandler {
	return &domainHandler{
		name: "SOURCE",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			value := strutil.NormalizeUpper(strings.Join(args, " "))
			switch action {
			case actionAllow:
				if value == "" {
					return "Usage: PASS SOURCE <HUMAN|SKIMMER|ALL>\nType HELP for usage.\n", false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) { f.ResetSources() })
					return "Source filtering disabled\n", true
				}
				if !filter.IsSupportedSource(value) {
					return fmt.Sprintf("Unknown source: %s\nValid sources: %s\n", value, strings.Join(filter.SupportedSources, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) { f.SetSource(value, true) })
				return fmt.Sprintf("Filter set: Source %s\n", value), true
			case actionBlock:
				if value == "" {
					return "Usage: REJECT SOURCE <HUMAN|SKIMMER|ALL>\nType HELP for usage.\n", false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						f.ResetSources()
						f.BlockAllSources = true
						f.AllSources = false
					})
					return "All sources blocked\n", true
				}
				if !filter.IsSupportedSource(value) {
					return fmt.Sprintf("Unknown source: %s\nValid sources: %s\n", value, strings.Join(filter.SupportedSources, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) { f.SetSource(value, false) })
				return fmt.Sprintf("Source filters disabled: %s\n", value), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func newNoFilterHandler() *domainHandler {
	return &domainHandler{
		name: "NOFILTER",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			if action != actionAllow {
				return invalidFilterCommandMsg, false
			}
			c.updateFilter(func(f *filter.Filter) { f.Reset() })
			return "All filters disabled\n", true
		},
	}
}
func newCallPatternHandler(name string) *domainHandler {
	return &domainHandler{
		name: name,
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			switch action {
			case actionAllow:
				value := strings.TrimSpace(strings.Join(args, " "))
				if value == "" {
					return passFilterUsageMsg, false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXCALL" {
							f.ClearDXCallsignPatterns()
							f.ClearDXCallsignBlockPatterns()
						} else {
							f.ClearDECallsignPatterns()
							f.ClearDECallsignBlockPatterns()
						}
					})
					if name == "DXCALL" {
						return "All DX callsigns enabled\n", true
					}
					return "All DE callsigns enabled\n", true
				}
				patterns := splitListValues(value)
				if len(patterns) == 0 {
					return passFilterUsageMsg, false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, pattern := range patterns {
						if name == "DXCALL" {
							f.AddDXCallsignPattern(pattern)
						} else {
							f.AddDECallsignPattern(pattern)
						}
					}
				})
				if name == "DXCALL" {
					if len(patterns) == 1 {
						return fmt.Sprintf("Filter set: DX callsign %s\n", strings.ToUpper(patterns[0])), true
					}
					return fmt.Sprintf("Filter set: DX callsigns %s\n", strings.Join(stringsUpper(patterns), ", ")), true
				}
				if len(patterns) == 1 {
					return fmt.Sprintf("Filter set: DE callsign %s\n", strings.ToUpper(patterns[0])), true
				}
				return fmt.Sprintf("Filter set: DE callsigns %s\n", strings.Join(stringsUpper(patterns), ", ")), true
			case actionBlock:
				value := strings.TrimSpace(strings.Join(args, " "))
				if value == "" {
					return rejectFilterUsageMsg, false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXCALL" {
							f.ClearDXCallsignPatterns()
							f.ClearDXCallsignBlockPatterns()
							f.AddBlockDXCallsignPattern("*")
						} else {
							f.ClearDECallsignPatterns()
							f.ClearDECallsignBlockPatterns()
							f.AddBlockDECallsignPattern("*")
						}
					})
					if name == "DXCALL" {
						return "All DX callsigns blocked\n", true
					}
					return "All DE callsigns blocked\n", true
				}
				patterns := splitListValues(value)
				if len(patterns) == 0 {
					return rejectFilterUsageMsg, false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, pattern := range patterns {
						if name == "DXCALL" {
							f.AddBlockDXCallsignPattern(pattern)
						} else {
							f.AddBlockDECallsignPattern(pattern)
						}
					}
				})
				if name == "DXCALL" {
					if len(patterns) == 1 {
						return fmt.Sprintf("DX callsign blocklist: %s\n", strings.ToUpper(patterns[0])), true
					}
					return fmt.Sprintf("DX callsign blocklist: %s\n", strings.Join(stringsUpper(patterns), ", ")), true
				}
				if len(patterns) == 1 {
					return fmt.Sprintf("DE callsign blocklist: %s\n", strings.ToUpper(patterns[0])), true
				}
				return fmt.Sprintf("DE callsign blocklist: %s\n", strings.Join(stringsUpper(patterns), ", ")), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func stringsUpper(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, strutil.NormalizeUpper(value))
	}
	return out
}

func newConfidenceHandler() *domainHandler {
	return &domainHandler{
		name: "CONFIDENCE",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			value := strings.TrimSpace(strings.Join(args, " "))
			switch action {
			case actionAllow:
				if value == "" {
					return "Usage: PASS CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL)\nType HELP for usage.\n", false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) { f.ResetConfidence() })
					return "All confidence symbols enabled\n", true
				}
				symbols := parseConfidenceList(value)
				if len(symbols) == 0 {
					return "Usage: PASS CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL)\nType HELP for usage.\n", false
				}
				invalid := collectInvalidConfidenceSymbols(symbols)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown confidence symbol: %s\nSupported symbols: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedConfidenceSymbols, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, symbol := range symbols {
						f.SetConfidenceSymbol(symbol, true)
					}
				})
				return fmt.Sprintf("Confidence symbols enabled: %s\n", strings.Join(symbols, ", ")), true
			case actionBlock:
				if value == "" {
					return "Usage: REJECT CONFIDENCE <symbol>[,<symbol>...] (comma or space separated, or ALL)\nType HELP for usage.\n", false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						f.ResetConfidence()
						f.BlockAllConfidence = true
						f.AllConfidence = false
					})
					return "All confidence symbols blocked (non-exempt modes)\n", true
				}
				symbols := parseConfidenceList(value)
				if len(symbols) == 0 {
					return "Usage: REJECT CONFIDENCE <symbol>[,<symbol>...] (comma or space separated, or ALL)\nType HELP for usage.\n", false
				}
				invalid := collectInvalidConfidenceSymbols(symbols)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown confidence symbol: %s\nSupported symbols: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedConfidenceSymbols, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, symbol := range symbols {
						f.SetConfidenceSymbol(symbol, false)
					}
				})
				return fmt.Sprintf("Confidence symbols disabled: %s\n", strings.Join(symbols, ", ")), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func pathFilteringAvailable(c *Client) bool {
	if c == nil || c.server == nil || c.server.pathPredictor == nil {
		return false
	}
	cfg := c.server.pathPredictor.Config()
	return cfg.Enabled
}

func newPathHandler() *domainHandler {
	return &domainHandler{
		name: "PATH",
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			if !pathFilteringAvailable(c) {
				return "PATH filtering unavailable (path predictor disabled). Command ignored.\n", false
			}
			value := strings.TrimSpace(strings.Join(args, " "))
			switch action {
			case actionAllow:
				if value == "" {
					return "Usage: PASS PATH <class>[,<class>...] (HIGH, MEDIUM, LOW, UNLIKELY, INSUFFICIENT, or ALL)\nType HELP for usage.\n", false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) { f.ResetPathClasses() })
					return "All path classes enabled\n", true
				}
				classes := parsePathClassList(value)
				if len(classes) == 0 {
					return "Usage: PASS PATH <class>[,<class>...] (HIGH, MEDIUM, LOW, UNLIKELY, INSUFFICIENT, or ALL)\nType HELP for usage.\n", false
				}
				invalid := collectInvalidPathClasses(classes)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown path class: %s\nSupported classes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedPathClasses, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, class := range classes {
						f.SetPathClass(class, true)
					}
				})
				return fmt.Sprintf("Path classes enabled: %s\n", strings.Join(classes, ", ")), true
			case actionBlock:
				if value == "" {
					return "Usage: REJECT PATH <class>[,<class>...] (comma or space separated, or ALL)\nType HELP for usage.\n", false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						f.ResetPathClasses()
						f.BlockAllPathClasses = true
						f.AllPathClasses = false
					})
					return "All path classes blocked\n", true
				}
				classes := parsePathClassList(value)
				if len(classes) == 0 {
					return "Usage: REJECT PATH <class>[,<class>...] (comma or space separated, or ALL)\nType HELP for usage.\n", false
				}
				invalid := collectInvalidPathClasses(classes)
				if len(invalid) > 0 {
					return fmt.Sprintf("Unknown path class: %s\nSupported classes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedPathClasses, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, class := range classes {
						f.SetPathClass(class, false)
					}
				})
				return fmt.Sprintf("Path classes disabled: %s\n", strings.Join(classes, ", ")), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
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

func newContinentHandler(name string, setter func(*filter.Filter, string, bool)) *domainHandler {
	return &domainHandler{
		name: name,
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			value := strings.TrimSpace(strings.Join(args, " "))
			switch action {
			case actionAllow:
				if value == "" {
					return fmt.Sprintf("Usage: PASS %s <cont>[,<cont>...] (continents: AF, AN, AS, EU, NA, OC, SA, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXCONT" {
							f.ResetDXContinents()
						} else {
							f.ResetDEContinents()
						}
					})
					return fmt.Sprintf("All %s continents enabled\n", strings.ToLower(name[:2])), true
				}
				continents := parseContinentList(value)
				if len(continents) == 0 {
					return fmt.Sprintf("Usage: PASS %s <cont>[,<cont>...] (continents: AF, AN, AS, EU, NA, OC, SA, or ALL)\nType HELP for usage.\n", name), false
				}
				if invalid := collectInvalidContinents(continents); len(invalid) > 0 {
					return fmt.Sprintf("Unknown continent: %s\nSupported continents: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedContinents, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, cont := range continents {
						setter(f, cont, true)
					}
				})
				return fmt.Sprintf("Filter set: %s continents %s\n", name[:2], strings.Join(continents, ", ")), true
			case actionBlock:
				if value == "" {
					return fmt.Sprintf("Usage: REJECT %s <cont>[,<cont>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXCONT" {
							f.ResetDXContinents()
							f.BlockAllDXContinents = true
							f.AllDXContinents = false
						} else {
							f.ResetDEContinents()
							f.BlockAllDEContinents = true
							f.AllDEContinents = false
						}
					})
					return fmt.Sprintf("All %s continents blocked\n", strings.ToLower(name[:2])), true
				}
				continents := parseContinentList(value)
				if len(continents) == 0 {
					return fmt.Sprintf("Usage: REJECT %s <cont>[,<cont>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if invalid := collectInvalidContinents(continents); len(invalid) > 0 {
					return fmt.Sprintf("Unknown continent: %s\nSupported continents: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedContinents, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, cont := range continents {
						setter(f, cont, false)
					}
				})
				return fmt.Sprintf("%s continent filters disabled: %s\n", name[:2], strings.Join(continents, ", ")), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func newZoneHandler(name string, setter func(*filter.Filter, int, bool)) *domainHandler {
	return &domainHandler{
		name: name,
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			value := strings.TrimSpace(strings.Join(args, " "))
			switch action {
			case actionAllow:
				if value == "" {
					return fmt.Sprintf("Usage: PASS %s <zone>[,<zone>...] (1-40, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXZONE" {
							f.ResetDXZones()
						} else {
							f.ResetDEZones()
						}
					})
					return fmt.Sprintf("All %s zones enabled\n", strings.ToLower(name[:2])), true
				}
				zones, invalidTokens := parseZoneList(value)
				if len(invalidTokens) > 0 {
					return fmt.Sprintf("Unknown CQ zone: %s\nValid zones: %d-%d\n", strings.Join(invalidTokens, ", "), filter.MinCQZone(), filter.MaxCQZone()), false
				}
				if len(zones) == 0 {
					return fmt.Sprintf("Usage: PASS %s <zone>[,<zone>...] (1-40, or ALL)\nType HELP for usage.\n", name), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, zone := range zones {
						setter(f, zone, true)
					}
				})
				return fmt.Sprintf("Filter set: %s zones %s\n", name[:2], joinZones(zones)), true
			case actionBlock:
				if value == "" {
					return fmt.Sprintf("Usage: REJECT %s <zone>[,<zone>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXZONE" {
							f.ResetDXZones()
							f.BlockAllDXZones = true
							f.AllDXZones = false
						} else {
							f.ResetDEZones()
							f.BlockAllDEZones = true
							f.AllDEZones = false
						}
					})
					return fmt.Sprintf("All %s zones blocked\n", strings.ToLower(name[:2])), true
				}
				zones, invalidTokens := parseZoneList(value)
				if len(invalidTokens) > 0 {
					return fmt.Sprintf("Unknown CQ zone: %s\nValid zones: %d-%d\n", strings.Join(invalidTokens, ", "), filter.MinCQZone(), filter.MaxCQZone()), false
				}
				if len(zones) == 0 {
					return fmt.Sprintf("Usage: REJECT %s <zone>[,<zone>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, zone := range zones {
						setter(f, zone, false)
					}
				})
				return fmt.Sprintf("%s zone filters disabled: %s\n", name[:2], joinZones(zones)), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func newDXCCHandler(name string, setter func(*filter.Filter, int, bool)) *domainHandler {
	return &domainHandler{
		name: name,
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			value := strings.TrimSpace(strings.Join(args, " "))
			switch action {
			case actionAllow:
				if value == "" {
					return fmt.Sprintf("Usage: PASS %s <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXDXCC" {
							f.ResetDXDXCC()
						} else {
							f.ResetDEDXCC()
						}
					})
					return fmt.Sprintf("All %s DXCCs enabled\n", strings.ToLower(name[:2])), true
				}
				codes, invalid := parseDXCCList(value)
				if len(codes) == 0 {
					return fmt.Sprintf("Usage: PASS %s <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if len(invalid) > 0 {
					return fmt.Sprintf("Invalid DXCC code: %s\n", strings.Join(invalid, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, code := range codes {
						setter(f, code, true)
					}
				})
				return fmt.Sprintf("Filter set: %s %s\n", name[:2], joinZones(codes)), true
			case actionBlock:
				if value == "" {
					return fmt.Sprintf("Usage: REJECT %s <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXDXCC" {
							f.ResetDXDXCC()
							f.BlockAllDXDXCC = true
							f.AllDXDXCC = false
						} else {
							f.ResetDEDXCC()
							f.BlockAllDEDXCC = true
							f.AllDEDXCC = false
						}
					})
					return fmt.Sprintf("All %s DXCCs blocked\n", strings.ToLower(name[:2])), true
				}
				codes, invalid := parseDXCCList(value)
				if len(codes) == 0 {
					return fmt.Sprintf("Usage: REJECT %s <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if len(invalid) > 0 {
					return fmt.Sprintf("Invalid DXCC code: %s\n", strings.Join(invalid, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, code := range codes {
						setter(f, code, false)
					}
				})
				return fmt.Sprintf("%s DXCC filters disabled: %s\n", name[:2], joinZones(codes)), true
			default:
				return invalidFilterCommandMsg, false
			}
		},
	}
}

func newGrid2Handler(name string, setter func(*filter.Filter, string, bool)) *domainHandler {
	return &domainHandler{
		name: name,
		apply: func(c *Client, action filterAction, args []string) (string, bool) {
			value := strings.TrimSpace(strings.Join(args, " "))
			switch action {
			case actionAllow:
				if value == "" {
					return fmt.Sprintf("Usage: PASS %s <grid>[,<grid>...] (two characters, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXGRID2" {
							f.ResetDXGrid2()
						} else {
							f.ResetDEGrid2()
						}
					})
					return fmt.Sprintf("All %s 2-character grids enabled\n", strings.ToLower(name[:2])), true
				}
				gridList, invalidTokens := parseGrid2List(value)
				if len(gridList) == 0 {
					return fmt.Sprintf("Usage: PASS %s <grid>[,<grid>...] (two characters, or ALL)\nType HELP for usage.\n", name), false
				}
				if len(invalidTokens) > 0 {
					return fmt.Sprintf("Unknown 2-character grid: %s\n", strings.Join(invalidTokens, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, grid := range gridList {
						setter(f, grid, true)
					}
				})
				return fmt.Sprintf("Filter set: %s 2-character grids %s\n", name[:2], strings.Join(gridList, ", ")), true
			case actionBlock:
				if value == "" {
					return fmt.Sprintf("Usage: REJECT %s <grid>[,<grid>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if strings.EqualFold(value, "ALL") {
					c.updateFilter(func(f *filter.Filter) {
						if name == "DXGRID2" {
							f.ResetDXGrid2()
							f.BlockAllDXGrid2 = true
							f.AllDXGrid2 = false
						} else {
							f.ResetDEGrid2()
							f.BlockAllDEGrid2 = true
							f.AllDEGrid2 = false
						}
					})
					return fmt.Sprintf("All %s 2-character grids blocked\n", strings.ToLower(name[:2])), true
				}
				gridList, invalidTokens := parseGrid2List(value)
				if len(gridList) == 0 {
					return fmt.Sprintf("Usage: REJECT %s <grid>[,<grid>...] (comma or space separated, or ALL)\nType HELP for usage.\n", name), false
				}
				if len(invalidTokens) > 0 {
					return fmt.Sprintf("Unknown 2-character grid: %s\n", strings.Join(invalidTokens, ", ")), false
				}
				c.updateFilter(func(f *filter.Filter) {
					for _, grid := range gridList {
						setter(f, grid, false)
					}
				})
				return fmt.Sprintf("%s 2-character grid filters disabled: %s\n", name[:2], strings.Join(gridList, ", ")), true
			default:
				return invalidFilterCommandMsg, false
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

func applyDXBMBands(c *Client, action filterAction, args []string) (string, bool) {
	if c == nil {
		return invalidFilterCommandMsg, false
	}
	values := splitListValues(strings.Join(args, " "))
	if len(values) == 0 {
		return "Usage: SET/FILTER DXBM/PASS|REJECT <band>[,<band>...]\nType HELP for usage.\n", false
	}
	bands, invalid := normalizeDXBMBands(values)
	if len(invalid) > 0 {
		return fmt.Sprintf("Unknown DXBM band: %s\nSupported: 160, 80, 40, 30, 20, 17, 15, 12, 10, 6, 2 (and 1 if enabled). Modes ignored; use SET/NO<MODE>.\n", strings.Join(invalid, ", ")), false
	}
	if len(bands) == 0 {
		return "No valid DXBM bands provided.\n", false
	}

	switch action {
	case actionAllow:
		// Pass wins: apply allow set first and ignore block set overlaps.
		c.updateFilter(func(f *filter.Filter) {
			for _, band := range bands {
				f.SetBand(band, true)
			}
		})
		return fmt.Sprintf("DXBM mapped: allow bands %s\n", strings.Join(bands, ", ")), true
	case actionBlock:
		// Remove any overlap with allow; pass wins.
		c.updateFilter(func(f *filter.Filter) {
			for _, band := range bands {
				f.SetBand(band, false)
			}
		})
		return fmt.Sprintf("DXBM mapped: block bands %s\n", strings.Join(bands, ", ")), true
	default:
		return invalidFilterCommandMsg, false
	}
}

func normalizeDXBMBands(values []string) (bands []string, invalid []string) {
	seen := make(map[string]bool)
	for _, v := range values {
		raw := strutil.NormalizeUpper(v)
		if raw == "" {
			continue
		}
		switch {
		case strings.HasPrefix(raw, "160"):
			addBand("160M", &bands, seen)
		case strings.HasPrefix(raw, "80"):
			addBand("80M", &bands, seen)
		case strings.HasPrefix(raw, "40"):
			addBand("40M", &bands, seen)
		case strings.HasPrefix(raw, "30"):
			addBand("30M", &bands, seen)
		case strings.HasPrefix(raw, "20"):
			addBand("20M", &bands, seen)
		case strings.HasPrefix(raw, "17"):
			addBand("17M", &bands, seen)
		case strings.HasPrefix(raw, "15"):
			addBand("15M", &bands, seen)
		case strings.HasPrefix(raw, "12"):
			addBand("12M", &bands, seen)
		case strings.HasPrefix(raw, "10"):
			addBand("10M", &bands, seen)
		case strings.HasPrefix(raw, "6"):
			addBand("6M", &bands, seen)
		case strings.HasPrefix(raw, "2"):
			addBand("2M", &bands, seen)
		case strings.HasPrefix(raw, "1"):
			addBand("1.25M", &bands, seen)
		default:
			invalid = append(invalid, v)
		}
	}
	return bands, invalid
}

func addBand(name string, out *[]string, seen map[string]bool) {
	if seen[name] {
		return
	}
	*out = append(*out, name)
	seen[name] = true
}

func normalizeBands(values []string) ([]string, []string) {
	normalized := make([]string, 0, len(values))
	seen := make(map[string]bool)
	invalid := make([]string, 0)
	for _, candidate := range values {
		norm := spot.NormalizeBand(candidate)
		if norm == "" || !spot.IsValidBand(norm) {
			invalid = append(invalid, candidate)
			continue
		}
		if !seen[norm] {
			normalized = append(normalized, norm)
			seen[norm] = true
		}
	}
	return normalized, invalid
}

func showFilterDeprecationNote(tokens []string, dialect DialectName) string {
	if len(tokens) == 0 {
		return ""
	}
	cmd := strings.Join(tokens, " ")
	switch dialect {
	case DialectCC:
		return fmt.Sprintf("Note: %s is deprecated. Use SHOW/FILTER or SH/FILTER.\n", cmd)
	default:
		return fmt.Sprintf("Note: %s is deprecated. Use SHOW FILTER.\n", cmd)
	}
}

type allowBlockSnapshot struct {
	allow      string
	block      string
	effective  string
	allowAll   bool
	blockAll   bool
	allowList  string
	blockList  string
	allowCount int
	blockCount int
}

// formatFilterSnapshot renders a stable, multi-line view of current filter state.
// Caller must hold a read lock; output uses display-only normalization plus effective labels.
func formatFilterSnapshot(f *filter.Filter, ctyLookup func() *cty.CTYDatabase) string {
	if f == nil {
		return "Current filters: unavailable\n"
	}

	var ctyDB *cty.CTYDatabase
	if ctyLookup != nil {
		ctyDB = ctyLookup()
	}
	dxccSupported := supportedDXCCCodes(ctyDB)

	bands := snapshotAllowBlockStrings(f.AllBands, f.BlockAllBands, f.Bands, f.BlockBands, spot.SupportedBandNames())
	modes := snapshotAllowBlockStrings(f.AllModes, f.BlockAllModes, f.Modes, f.BlockModes, filter.SupportedModes)
	sources := snapshotAllowBlockStrings(f.AllSources, f.BlockAllSources, f.Sources, f.BlockSources, filter.SupportedSources)
	confidence := snapshotAllowBlockStrings(f.AllConfidence, f.BlockAllConfidence, f.Confidence, f.BlockConfidence, filter.SupportedConfidenceSymbols)
	pathClasses := snapshotAllowBlockStrings(f.AllPathClasses, f.BlockAllPathClasses, f.PathClasses, f.BlockPathClasses, filter.SupportedPathClasses)
	dxCont := snapshotAllowBlockStrings(f.AllDXContinents, f.BlockAllDXContinents, f.DXContinents, f.BlockDXContinents, filter.SupportedContinents)
	deCont := snapshotAllowBlockStrings(f.AllDEContinents, f.BlockAllDEContinents, f.DEContinents, f.BlockDEContinents, filter.SupportedContinents)
	dxZone := snapshotAllowBlockIntsRange(f.AllDXZones, f.BlockAllDXZones, f.DXZones, f.BlockDXZones, filter.MinCQZone(), filter.MaxCQZone())
	deZone := snapshotAllowBlockIntsRange(f.AllDEZones, f.BlockAllDEZones, f.DEZones, f.BlockDEZones, filter.MinCQZone(), filter.MaxCQZone())
	dxDXCC := snapshotAllowBlockIntsSupported(f.AllDXDXCC, f.BlockAllDXDXCC, f.DXDXCC, f.BlockDXDXCC, dxccSupported)
	deDXCC := snapshotAllowBlockIntsSupported(f.AllDEDXCC, f.BlockAllDEDXCC, f.DEDXCC, f.BlockDEDXCC, dxccSupported)
	dxGrid2 := snapshotAllowBlockStrings(f.AllDXGrid2, f.BlockAllDXGrid2, f.DXGrid2Prefixes, f.BlockDXGrid2, nil)
	deGrid2 := snapshotAllowBlockStrings(f.AllDEGrid2, f.BlockAllDEGrid2, f.DEGrid2Prefixes, f.BlockDEGrid2, nil)

	dxCallSummary, dxCallLine := callsignSnapshot("DXCALL", f.DXCallsigns, f.BlockDXCallsigns)
	deCallSummary, deCallLine := callsignSnapshot("DECALL", f.DECallsigns, f.BlockDECallsigns)

	beaconSummary, beaconLine := toggleSnapshot("BEACON", f.BeaconsEnabled(), f.IncludeBeacons)
	wwvSummary, wwvLine := toggleSnapshot("WWV", f.WWVEnabled(), f.AllowWWV)
	wcySummary, wcyLine := toggleSnapshot("WCY", f.WCYEnabled(), f.AllowWCY)
	announceSummary, announceLine := toggleSnapshot("ANNOUNCE", f.AnnounceEnabled(), f.AllowAnnounce)
	selfSummary, selfLine := toggleSnapshot("SELF", f.SelfEnabled(), f.AllowSelf)
	nearbySummary := "NEARBY=OFF"
	nearbyLine := "NEARBY: OFF\n"
	if f.NearbyActive() {
		nearbySummary = "NEARBY=ON"
		nearbyLine = "NEARBY: ON (location filters suspended)\n"
	}

	var b strings.Builder
	summaryPrefix := "Current filters: "
	summaryIndent := strings.Repeat(" ", len(summaryPrefix))
	const summaryMaxWidth = 78
	maxFieldLen := summaryMaxWidth - len(summaryIndent)
	if maxFieldLen < 1 {
		maxFieldLen = summaryMaxWidth
	}
	summaryParts := []string{
		summaryAllowBlockField("BAND", bands, maxFieldLen),
		summaryAllowBlockField("MODE", modes, maxFieldLen),
		summaryAllowBlockField("SOURCE", sources, maxFieldLen),
		clampSummaryField(dxCallSummary, maxFieldLen),
		clampSummaryField(deCallSummary, maxFieldLen),
		summaryAllowBlockField("CONFIDENCE", confidence, maxFieldLen),
		summaryAllowBlockField("PATH", pathClasses, maxFieldLen),
		summaryAllowBlockField("DXCONT", dxCont, maxFieldLen),
		summaryAllowBlockField("DECONT", deCont, maxFieldLen),
		summaryAllowBlockField("DXZONE", dxZone, maxFieldLen),
		summaryAllowBlockField("DEZONE", deZone, maxFieldLen),
		summaryAllowBlockField("DXDXCC", dxDXCC, maxFieldLen),
		summaryAllowBlockField("DEDXCC", deDXCC, maxFieldLen),
		summaryAllowBlockField("DXGRID2", dxGrid2, maxFieldLen),
		summaryAllowBlockField("DEGRID2", deGrid2, maxFieldLen),
		clampSummaryField(nearbySummary, maxFieldLen),
		clampSummaryField(beaconSummary, maxFieldLen),
		clampSummaryField(wwvSummary, maxFieldLen),
		clampSummaryField(wcySummary, maxFieldLen),
		clampSummaryField(announceSummary, maxFieldLen),
		clampSummaryField(selfSummary, maxFieldLen),
	}
	for _, line := range wrapSummaryLines(summaryPrefix, summaryIndent, summaryParts, summaryMaxWidth) {
		b.WriteString(line)
		b.WriteString("\n")
	}
	b.WriteString(formatAllowBlockLine("BAND", bands))
	b.WriteString(formatAllowBlockLine("MODE", modes))
	b.WriteString(formatAllowBlockLine("SOURCE", sources))
	b.WriteString(dxCallLine)
	b.WriteString(deCallLine)
	b.WriteString(formatAllowBlockLine("CONFIDENCE", confidence))
	b.WriteString(formatAllowBlockLine("PATH", pathClasses))
	b.WriteString(formatAllowBlockLine("DXCONT", dxCont))
	b.WriteString(formatAllowBlockLine("DECONT", deCont))
	b.WriteString(formatAllowBlockLine("DXZONE", dxZone))
	b.WriteString(formatAllowBlockLine("DEZONE", deZone))
	b.WriteString(formatAllowBlockLine("DXDXCC", dxDXCC))
	b.WriteString(formatAllowBlockLine("DEDXCC", deDXCC))
	b.WriteString(formatAllowBlockLine("DXGRID2", dxGrid2))
	b.WriteString(formatAllowBlockLine("DEGRID2", deGrid2))
	b.WriteString(nearbyLine)
	b.WriteString(beaconLine)
	b.WriteString(wwvLine)
	b.WriteString(wcyLine)
	b.WriteString(announceLine)
	b.WriteString(selfLine)
	return b.String()
}

func snapshotAllowBlockStrings(allowAll, blockAll bool, allow, block map[string]bool, supported []string) allowBlockSnapshot {
	allowValues := orderedStringValues(allow, supported)
	blockValues := orderedStringValues(block, supported)
	allowListLabel := strings.Join(allowValues, ", ")
	blockListLabel := strings.Join(blockValues, ", ")
	allowAllNormalized := allowAll || coversAllSupported(allow, supported)
	return buildAllowBlockSnapshot(allowAllNormalized, blockAll, allowListLabel, blockListLabel, len(allowValues), len(blockValues))
}

func snapshotAllowBlockIntsRange(allowAll, blockAll bool, allow, block map[int]bool, min, max int) allowBlockSnapshot {
	allowValues := orderedIntValues(allow)
	blockValues := orderedIntValues(block)
	allowList := joinIntValues(allowValues)
	blockList := joinIntValues(blockValues)
	allowAllNormalized := allowAll || coversAllRange(allow, min, max)
	return buildAllowBlockSnapshot(allowAllNormalized, blockAll, allowList, blockList, len(allowValues), len(blockValues))
}

func snapshotAllowBlockIntsSupported(allowAll, blockAll bool, allow, block map[int]bool, supported []int) allowBlockSnapshot {
	allowValues := orderedIntValues(allow)
	blockValues := orderedIntValues(block)
	allowList := joinIntValues(allowValues)
	blockList := joinIntValues(blockValues)
	allowAllNormalized := allowAll || coversAllSupportedInts(allow, supported)
	return buildAllowBlockSnapshot(allowAllNormalized, blockAll, allowList, blockList, len(allowValues), len(blockValues))
}

func buildAllowBlockSnapshot(allowAll, blockAll bool, allowList, blockList string, allowCount, blockCount int) allowBlockSnapshot {
	allowLabel := "NONE"
	if allowAll {
		allowLabel = "ALL"
	} else if allowList != "" {
		allowLabel = allowList
	}

	blockLabel := "NONE"
	if blockAll {
		blockLabel = "ALL"
	} else if blockList != "" {
		blockLabel = blockList
	}

	return allowBlockSnapshot{
		allow:      allowLabel,
		block:      blockLabel,
		effective:  effectiveAllowBlockLabel(allowAll, blockAll, allowList, blockList),
		allowAll:   allowAll,
		blockAll:   blockAll,
		allowList:  allowList,
		blockList:  blockList,
		allowCount: allowCount,
		blockCount: blockCount,
	}
}

func effectiveAllowBlockLabel(allowAll, blockAll bool, allowList, blockList string) string {
	if blockAll {
		return "all blocked"
	}
	if allowAll {
		if blockList == "" {
			return "all pass"
		}
		return "all except: " + blockList
	}
	if allowList == "" {
		return "none pass"
	}
	return "only: " + allowList
}

func formatAllowBlockLine(name string, snapshot allowBlockSnapshot) string {
	return fmt.Sprintf("%s: allow=%s block=%s (effective: %s)\n", name, snapshot.allow, snapshot.block, snapshot.effective)
}

func summaryAllowBlockField(name string, snapshot allowBlockSnapshot, maxFieldLen int) string {
	if maxFieldLen <= len(name)+1 {
		return name
	}
	maxLabelLen := maxFieldLen - len(name) - 1
	label := summaryEffectiveLabel(snapshot, maxLabelLen)
	field := fmt.Sprintf("%s=%s", name, label)
	return clampSummaryField(field, maxFieldLen)
}

func summaryEffectiveLabel(snapshot allowBlockSnapshot, maxLen int) string {
	label := snapshot.effective
	if len(label) <= maxLen {
		return label
	}
	if snapshot.blockAll {
		return label
	}
	if snapshot.allowAll {
		if snapshot.blockList != "" {
			label = fmt.Sprintf("all except: %d items", snapshot.blockCount)
		}
		return clampSummaryField(label, maxLen)
	}
	if snapshot.allowList != "" {
		label = fmt.Sprintf("only: %d items", snapshot.allowCount)
	}
	return clampSummaryField(label, maxLen)
}

func clampSummaryField(field string, maxLen int) string {
	if maxLen <= 0 || len(field) <= maxLen {
		return field
	}
	if maxLen <= 3 {
		return field[:maxLen]
	}
	return field[:maxLen-3] + "..."
}

func wrapSummaryLines(prefix, indent string, fields []string, maxWidth int) []string {
	if maxWidth <= 0 {
		return []string{prefix + strings.Join(fields, " | ")}
	}
	lines := make([]string, 0, len(fields))
	linePrefix := prefix
	line := linePrefix
	lineLen := len(line)
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		sep := ""
		if lineLen > len(linePrefix) {
			sep = " | "
		}
		candidate := line + sep + field
		if len(candidate) > maxWidth && lineLen > len(linePrefix) {
			lines = append(lines, line)
			linePrefix = indent
			line = linePrefix + field
			lineLen = len(line)
			continue
		}
		line = candidate
		lineLen = len(line)
	}
	if strings.TrimSpace(line) != "" {
		lines = append(lines, line)
	}
	return lines
}

func orderedStringValues(values map[string]bool, supported []string) []string {
	if len(values) == 0 {
		return nil
	}
	if len(supported) == 0 {
		return keysString(values)
	}
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, entry := range supported {
		if values[entry] {
			out = append(out, entry)
			seen[entry] = true
		}
	}
	extras := make([]string, 0, len(values))
	for entry := range values {
		if !seen[entry] {
			extras = append(extras, entry)
		}
	}
	sort.Strings(extras)
	out = append(out, extras...)
	return out
}

func orderedIntValues(values map[int]bool) []int {
	if len(values) == 0 {
		return nil
	}
	out := make([]int, 0, len(values))
	for entry := range values {
		out = append(out, entry)
	}
	sort.Ints(out)
	return out
}

func joinIntValues(values []int) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, strconv.Itoa(v))
	}
	return strings.Join(parts, ", ")
}

func coversAllSupported(values map[string]bool, supported []string) bool {
	if len(supported) == 0 {
		return false
	}
	for _, entry := range supported {
		if !values[entry] {
			return false
		}
	}
	return true
}

func coversAllRange(values map[int]bool, min, max int) bool {
	if min <= 0 || max < min {
		return false
	}
	for i := min; i <= max; i++ {
		if !values[i] {
			return false
		}
	}
	return true
}

func coversAllSupportedInts(values map[int]bool, supported []int) bool {
	if len(supported) == 0 {
		return false
	}
	for _, entry := range supported {
		if !values[entry] {
			return false
		}
	}
	return true
}

func supportedDXCCCodes(db *cty.CTYDatabase) []int {
	if db == nil || len(db.Data) == 0 {
		return nil
	}
	seen := make(map[int]bool)
	codes := make([]int, 0, len(db.Data))
	for _, entry := range db.Data {
		if entry.ADIF <= 0 {
			continue
		}
		if seen[entry.ADIF] {
			continue
		}
		seen[entry.ADIF] = true
		codes = append(codes, entry.ADIF)
	}
	sort.Ints(codes)
	return codes
}

func callsignSnapshot(name string, allow, block []string) (summary string, line string) {
	allowLabel := "ALL"
	blockLabel := "NONE"
	if len(allow) > 0 {
		allowLabel = strings.Join(allow, ", ")
	}
	if len(block) > 0 {
		blockLabel = strings.Join(block, ", ")
	}
	if patternListHasAll(block) {
		blockLabel = "ALL"
	}

	switch {
	case len(allow) == 0 && len(block) == 0:
		summary = fmt.Sprintf("%s=all pass", name)
	case len(allow) == 0:
		summary = fmt.Sprintf("%s=blocklist(%d)", name, len(block))
	case len(block) == 0:
		summary = fmt.Sprintf("%s=allowlist(%d)", name, len(allow))
	default:
		summary = fmt.Sprintf("%s=allow(%d) block(%d)", name, len(allow), len(block))
	}

	effective := "all pass"
	if len(allow) > 0 {
		effective = "allowlist"
	}
	if patternListHasAll(block) {
		effective = "none pass"
	} else if len(block) > 0 && len(allow) == 0 {
		effective = "all except blocklist"
	} else if len(block) > 0 && len(allow) > 0 {
		effective = "allowlist minus blocklist"
	}

	line = fmt.Sprintf("%s: allow=%s block=%s (effective: %s)\n", name, allowLabel, blockLabel, effective)
	return summary, line
}

func patternListHasAll(patterns []string) bool {
	for _, pattern := range patterns {
		if strings.TrimSpace(pattern) == "*" {
			return true
		}
	}
	return false
}

func toggleSnapshot(name string, enabled bool, raw *bool) (summary string, line string) {
	status := "OFF"
	if enabled {
		status = "ON"
	}
	summary = fmt.Sprintf("%s=%s", name, status)
	if raw == nil {
		line = fmt.Sprintf("%s: %s (default)\n", name, status)
		return summary, line
	}
	line = fmt.Sprintf("%s: %s\n", name, status)
	return summary, line
}

func splitListValues(arg string) []string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil
	}
	cleaned := strings.ReplaceAll(arg, ",", " ")
	values := strings.Fields(cleaned)
	if len(values) == 0 {
		return nil
	}
	return values
}

func parseBandList(arg string) []string {
	return splitListValues(arg)
}

func parseModeList(arg string) []string {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	modes := make([]string, 0, len(values))
	for _, value := range values {
		mode := strutil.NormalizeUpper(value)
		if mode == "" {
			continue
		}
		modes = append(modes, mode)
	}
	return modes
}

func parseConfidenceList(arg string) []string {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	symbols := make([]string, 0, len(values))
	for _, value := range values {
		symbol := strutil.NormalizeUpper(value)
		if symbol == "" {
			continue
		}
		symbols = append(symbols, symbol)
	}
	return symbols
}

func parsePathClassList(arg string) []string {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	classes := make([]string, 0, len(values))
	for _, value := range values {
		class := strutil.NormalizeUpper(value)
		if class == "" || seen[class] {
			continue
		}
		classes = append(classes, class)
		seen[class] = true
	}
	return classes
}

func parseContinentList(arg string) []string {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	continents := make([]string, 0, len(values))
	for _, value := range values {
		cont := strutil.NormalizeUpper(value)
		if cont == "" || seen[cont] {
			continue
		}
		continents = append(continents, cont)
		seen[cont] = true
	}
	return continents
}

func parseZoneList(arg string) ([]int, []string) {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[int]bool)
	zones := make([]int, 0, len(values))
	invalid := make([]string, 0)
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		zone, err := strconv.Atoi(v)
		if err != nil {
			invalid = append(invalid, value)
			continue
		}
		if !filter.IsSupportedZone(zone) {
			invalid = append(invalid, value)
			continue
		}
		if seen[zone] {
			continue
		}
		zones = append(zones, zone)
		seen[zone] = true
	}
	return zones, invalid
}

func parseDXCCList(arg string) ([]int, []string) {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[int]bool)
	codes := make([]int, 0, len(values))
	invalid := make([]string, 0)
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		code, err := strconv.Atoi(v)
		if err != nil || code <= 0 {
			invalid = append(invalid, value)
			continue
		}
		if seen[code] {
			continue
		}
		codes = append(codes, code)
		seen[code] = true
	}
	return codes, invalid
}

func parseGrid2List(arg string) ([]string, []string) {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[string]bool)
	grids := make([]string, 0, len(values))
	invalid := make([]string, 0)
	for _, value := range values {
		raw := strutil.NormalizeUpper(value)
		if raw == "" {
			continue
		}
		if len(raw) > 2 {
			raw = raw[:2]
		}
		if len(raw) != 2 {
			invalid = append(invalid, value)
			continue
		}
		grid := raw
		if seen[grid] {
			continue
		}
		grids = append(grids, grid)
		seen[grid] = true
	}
	return grids, invalid
}

func collectInvalidConfidenceSymbols(symbols []string) []string {
	invalid := make([]string, 0)
	for _, symbol := range symbols {
		if !filter.IsSupportedConfidenceSymbol(symbol) {
			invalid = append(invalid, symbol)
		}
	}
	return invalid
}

func collectInvalidPathClasses(classes []string) []string {
	invalid := make([]string, 0)
	for _, class := range classes {
		if !filter.IsSupportedPathClass(class) {
			invalid = append(invalid, class)
		}
	}
	return invalid
}

func collectInvalidContinents(continents []string) []string {
	invalid := make([]string, 0)
	for _, cont := range continents {
		if !filter.IsSupportedContinent(cont) {
			invalid = append(invalid, cont)
		}
	}
	return invalid
}

func collectInvalidModes(modes []string) []string {
	invalid := make([]string, 0)
	for _, mode := range modes {
		if !filter.IsSupportedMode(mode) {
			invalid = append(invalid, mode)
		}
	}
	return invalid
}

func keysString(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func joinZones(zones []int) string {
	if len(zones) == 0 {
		return ""
	}
	cp := append([]int(nil), zones...)
	sort.Ints(cp)
	parts := make([]string, 0, len(cp))
	for _, z := range cp {
		parts = append(parts, fmt.Sprintf("%d", z))
	}
	return strings.Join(parts, ", ")
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
	default:
		tc := titleCase(name)
		return tc, tc, tc
	}
}

func isCCMode(mode string) bool {
	mode = strutil.NormalizeUpper(mode)
	switch mode {
	case "CW", "FT4", "FT8", "RTTY":
		return true
	default:
		return false
	}
}
