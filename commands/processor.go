// Package commands implements the minimal command processor used by telnet
// sessions. It focuses on HELP/SHOW/DX/SHOW MYDX and defers filter manipulation
// to the telnet package so both layers stay small and easy to reason about.
package commands

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"dxcluster/buffer"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/reputation"
	"dxcluster/spot"
)

// archiveReader is the minimal interface the archive layer exposes for read paths.
type archiveReader interface {
	Recent(limit int) ([]*spot.Spot, error)
	RecentFiltered(limit int, match func(*spot.Spot) bool) ([]*spot.Spot, error)
}

// Processor handles telnet command parsing and replies that rely on shared state
// (recent spots in the archive).
type Processor struct {
	spotBuffer *buffer.RingBuffer
	archive    archiveReader
	spotInput  chan<- *spot.Spot
	ctyLookup  func() *cty.CTYDatabase
	prefixIdx  *prefixIndex
	repGate    *reputation.Gate
	repReport  func(reputation.DropEvent)
}

// NewProcessor constructs a command processor bound to shared spot state.
// Key aspects: SHOW/DX uses archive history; DX commands can enqueue spots.
// Upstream: Telnet server initialization.
// Downstream: Processor methods (ProcessCommand, handleShowDX, handleDX).
func NewProcessor(buf *buffer.RingBuffer, archive archiveReader, spotInput chan<- *spot.Spot, ctyLookup func() *cty.CTYDatabase, repGate *reputation.Gate, repReport func(reputation.DropEvent)) *Processor {
	return &Processor{
		spotBuffer: buf,
		archive:    archive,
		spotInput:  spotInput,
		ctyLookup:  ctyLookup,
		prefixIdx:  &prefixIndex{},
		repGate:    repGate,
		repReport:  repReport,
	}
}

// ProcessCommand parses a command and returns the response text.
// Key aspects: "BYE" signals the caller to close the session.
// Upstream: Telnet client command loop.
// Downstream: ProcessCommandForClient.
func (p *Processor) ProcessCommand(cmd string) string {
	return p.ProcessCommandForClient(cmd, "", "", nil, "go")
}

// ProcessCommandForClient parses a command with client context for DX posting and filtered history.
// Key aspects: Routes DX commands, SHOW/DX, and SHOW/MYDX with optional filter.
// Upstream: Telnet client command loop with callsign context.
// Downstream: handleDX, handleHelp, handleShow.
func (p *Processor) ProcessCommandForClient(cmd string, spotter string, spotterIP string, filterFn func(*spot.Spot) bool, dialect string) string {
	cmd = strings.TrimSpace(cmd)

	// Empty command
	if cmd == "" {
		return ""
	}
	if strings.TrimSpace(spotter) == "" {
		return noLoggedUserMsg
	}

	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return ""
	}
	if strings.EqualFold(fields[0], "DX") {
		return p.handleDX(fields, spotter, spotterIP)
	}

	// Split into parts
	parts := strings.Fields(strings.ToUpper(cmd))
	command := parts[0]
	dialect = normalizeDialectString(dialect)

	if dialect == "cc" {
		switch command {
		case "SHOW/DX", "SH/DX":
			return p.handleShow(append([]string{"DX"}, parts[1:]...), filterFn, dialect)
		case "SHOW", "SH":
			if len(parts) >= 2 && parts[1] == "DX" {
				return "Use SHOW/DX or SH/DX for DX history.\n"
			}
		}
	} else {
		switch command {
		case "SHOW/DX", "SH/DX":
			return "Use SHOW DX or SH DX for DX history.\n"
		}
	}

	switch command {
	case "HELP", "H":
		topic := ""
		if len(parts) > 1 {
			topic = strings.Join(parts[1:], " ")
		}
		return p.handleHelp(dialect, topic)
	case "SH", "SHOW":
		if len(parts) < 2 {
			return showDXUsage(dialect)
		}
		return p.handleShow(parts[1:], filterFn, dialect)
	case "BYE", "QUIT", "EXIT":
		return "BYE"
	default:
		return fmt.Sprintf("Unknown command: %s\nType HELP for available commands.\n", command)
	}
}

// Purpose: Render the HELP text for users.
// Key aspects: Supports HELP <command>; tailored per dialect; honors width cap.
// Upstream: ProcessCommandForClient (HELP/H).
// Downstream: filter.SupportedModes, spot.SupportedBandNames.
func (p *Processor) handleHelp(dialect string, topic string) string {
	dialect = normalizeDialectString(dialect)
	catalog := buildHelpCatalog(dialect)
	normalized := normalizeHelpTopic(dialect, topic)
	if normalized != "" {
		if entry, ok := catalog.lookup(normalized); ok {
			return strings.Join(entry.lines, "\n") + "\n"
		}
		return fmt.Sprintf("Unknown help topic: %s\nType HELP for available commands.\n", normalized)
	}

	lines := []string{
		"Available commands:",
	}
	for _, key := range catalog.order {
		entry := catalog.entries[key]
		lines = append(lines, entry.summary)
	}
	lines = append(lines, "Type HELP <command> for details.")
	lines = append(lines, filterHelpLines(dialect)...)
	lines = append(lines, "")
	lines = append(lines, "List types:")
	lines = append(lines, wrapListLines(filterListTypes())...)
	lines = append(lines, "")
	lines = append(lines, "Supported modes:")
	lines = append(lines, wrapListLines(filter.SupportedModes)...)
	lines = append(lines, "")
	lines = append(lines, "Supported bands:")
	lines = append(lines, wrapListLines(spot.SupportedBandNames())...)
	return strings.Join(lines, "\n") + "\n"
}

type helpEntry struct {
	summary string
	lines   []string
}

type helpCatalog struct {
	entries map[string]helpEntry
	aliases map[string]string
	order   []string
}

func (c helpCatalog) lookup(topic string) (helpEntry, bool) {
	if c.entries == nil {
		return helpEntry{}, false
	}
	if canonical, ok := c.aliases[topic]; ok {
		topic = canonical
	}
	entry, ok := c.entries[topic]
	return entry, ok
}

func buildHelpCatalog(dialect string) helpCatalog {
	catalog := helpCatalog{
		entries: make(map[string]helpEntry),
		aliases: make(map[string]string),
	}
	add := func(key, summary string, lines []string, aliases ...string) {
		catalog.entries[key] = helpEntry{summary: summary, lines: lines}
		for _, alias := range aliases {
			catalog.aliases[alias] = key
		}
	}

	helpLines := helpEntryLines(
		"HELP - Show command list or command-specific help.",
		[]string{"HELP [command]"},
		nil,
		[]string{
			"Without arguments, lists commands for the active dialect.",
			"With a command, shows detailed usage.",
		},
	)
	add("HELP", "HELP - Show command list or command-specific help.", helpLines)

	dxLines := helpEntryLines(
		"DX - Post a spot (human entry).",
		[]string{"DX <freq_khz> <callsign> [comment]"},
		nil,
		[]string{
			"Frequency is in kHz (e.g., 7001.0).",
			"Comment is free text; mode/report may be parsed from it.",
			"Rejects invalid callsigns and CTY-unknown DX when CTY is enabled.",
		},
	)
	add("DX", "DX - Post a spot (human entry).", dxLines)

	showMYDXLines := helpEntryLines(
		"SHOW MYDX - Show filtered spot history.",
		[]string{"SHOW MYDX [count]"},
		nil,
		[]string{
			"History is pulled from stored spots (not live buffer).",
			"Count range is 1-250 (default 50).",
			"Respects your filters; self-spots always pass.",
		},
	)
	add("SHOW MYDX", "SHOW MYDX - Show filtered spot history.", showMYDXLines)

	showDXCCLines := helpEntryLines(
		"SHOW DXCC - Look up DXCC/ADIF and zones.",
		[]string{"SHOW DXCC <prefix|callsign>"},
		nil,
		[]string{
			"Uses the CTY database to resolve ADIF, country, and zones.",
			"Returns other prefixes for the same country when available.",
		},
	)
	add("SHOW DXCC", "SHOW DXCC - Look up DXCC/ADIF and zones.", showDXCCLines)

	showDedupeLines := helpEntryLines(
		"SHOW DEDUPE - Show your broadcast dedupe policy.",
		[]string{"SHOW DEDUPE"},
		nil,
		[]string{
			"FAST = short window; MED = medium window; SLOW = long window.",
			"FAST/MED use 2-character grid squares; SLOW uses CQ zones.",
			"Shows if a policy is disabled server-side.",
		},
	)
	add("SHOW DEDUPE", "SHOW DEDUPE - Show dedupe policy.", showDedupeLines)

	setDedupeLines := helpEntryLines(
		"SET DEDUPE - Select broadcast dedupe policy.",
		[]string{"SET DEDUPE <FAST|MED|SLOW>"},
		nil,
		[]string{
			"FAST = short window; MED = medium window; SLOW = long window.",
			"FAST/MED use 2-character grid squares; SLOW uses CQ zones.",
			"If a policy is disabled, the nearest available is chosen.",
		},
	)
	add("SET DEDUPE", "SET DEDUPE - Select dedupe policy.", setDedupeLines)

	setDiagLines := helpEntryLines(
		"SET DIAG - Toggle diagnostic comments.",
		[]string{"SET DIAG <ON|OFF>"},
		nil,
		[]string{
			"ON replaces the comment field with a diagnostic tag.",
		},
	)
	add("SET DIAG", "SET DIAG - Toggle diagnostic comments.", setDiagLines)

	setGridLines := helpEntryLines(
		"SET GRID - Set your grid for path reliability glyphs.",
		[]string{"SET GRID <4-6 char maidenhead>"},
		nil,
		[]string{
			"Example: SET GRID FN31.",
		},
	)
	add("SET GRID", "SET GRID - Set your grid (4-6 chars).", setGridLines)

	setNoiseLines := helpEntryLines(
		"SET NOISE - Set your noise class for glyphs.",
		[]string{"SET NOISE <QUIET|RURAL|SUBURBAN|URBAN>"},
		nil,
		[]string{
			"Default is QUIET when unset.",
		},
	)
	add("SET NOISE", "SET NOISE - Set noise class.", setNoiseLines)

	passNearbyLines := helpEntryLines(
		"PASS NEARBY - Toggle nearby filtering.",
		[]string{"PASS NEARBY ON|OFF"},
		nil,
		[]string{
			"Requires SET GRID. Uses H3 L1 on 160/80/60m and L2 on all other bands.",
			"Location filters are suspended while NEARBY is ON.",
			"Setting location filters while NEARBY is ON is rejected with a warning.",
			"NEARBY state persists across sessions and shows a login warning when active.",
		},
	)
	add("PASS NEARBY", "PASS NEARBY - Toggle nearby filtering.", passNearbyLines)

	setSolarLines := helpEntryLines(
		"SET SOLAR - Opt into solar summaries.",
		[]string{"SET SOLAR <15|30|60|OFF>"},
		nil,
		[]string{
			"Summaries are wall-clock aligned and start at the next tick.",
			"OFF disables summaries.",
		},
	)
	add("SET SOLAR", "SET SOLAR - Solar summary cadence.", setSolarLines)

	resetFilterLines := helpEntryLines(
		"RESET FILTER - Reset filters to configured defaults.",
		[]string{"RESET FILTER"},
		nil,
		nil,
	)
	add("RESET FILTER", "RESET FILTER - Reset filters to defaults.", resetFilterLines)

	dialectLines := helpEntryLines(
		"DIALECT - Show or switch filter command dialect.",
		[]string{"DIALECT", "DIALECT LIST", "DIALECT <go|cc>"},
		nil,
		[]string{
			"Dialect selection is persisted per callsign.",
		},
	)
	add("DIALECT", "DIALECT - Show or switch dialect.", dialectLines)

	byeLines := helpEntryLines(
		"BYE - Disconnect from the cluster.",
		[]string{"BYE"},
		[]string{"QUIT", "EXIT"},
		nil,
	)
	add("BYE", "BYE - Disconnect.", byeLines, "QUIT", "EXIT")

	if dialect == "cc" {
		showLines := helpEntryLines(
			"SHOW - See SHOW subcommands.",
			[]string{"SHOW MYDX [count]", "SHOW DXCC <prefix|callsign>"},
			nil,
			[]string{
				"Use HELP SHOW/DX for the history alias.",
			},
		)
		add("SHOW", "SHOW - See SHOW subcommands.", showLines)

		showDXLines := helpEntryLines(
			"SHOW/DX - Alias of SHOW MYDX (stored history).",
			[]string{"SHOW/DX [count]"},
			[]string{"SH/DX"},
			[]string{
				"Count range is 1-250 (default 50).",
			},
		)
		add("SHOW/DX", "SHOW/DX - Alias of SHOW MYDX.", showDXLines, "SH/DX")
		shDXLines := helpEntryLines(
			"SH/DX - Alias of SHOW/DX.",
			[]string{"SH/DX [count]"},
			[]string{"SHOW/DX"},
			nil,
		)
		add("SH/DX", "SH/DX - Alias of SHOW/DX.", shDXLines)

		showFilterCCLines := helpEntryLines(
			"SHOW/FILTER - Display current filter state.",
			[]string{"SHOW/FILTER"},
			[]string{"SH/FILTER"},
			[]string{
				"Alias of SHOW FILTER in the CC dialect.",
			},
		)
		add("SHOW/FILTER", "SHOW/FILTER - Display filter state.", showFilterCCLines, "SH/FILTER")
		shFilterLines := helpEntryLines(
			"SH/FILTER - Alias of SHOW/FILTER.",
			[]string{"SH/FILTER"},
			[]string{"SHOW/FILTER"},
			nil,
		)
		add("SH/FILTER", "SH/FILTER - Alias of SHOW/FILTER.", shFilterLines)

		setFilterLines := helpEntryLines(
			"SET/FILTER - Allow list-based filters (CC dialect).",
			[]string{"SET/FILTER <type> <list>"},
			nil,
			[]string{
				"Same semantics as PASS.",
				"Use /ON or /OFF to allow or block all for a type.",
				"Example: SET/FILTER BAND/ON",
			},
		)
		setFilterLines = appendListSection(setFilterLines, "Types:", append([]string{"DXBM"}, filterListTypes()...))
		setFilterLines = appendNotes(setFilterLines, []string{
			"DXBM maps CC band codes to BAND filters.",
		}, helpMaxWidth)
		setFilterLines = appendNotes(setFilterLines, []string{
			"DXBM bands: 160, 80, 40, 30, 20, 17, 15, 12, 10, 6, 2 (1 if enabled).",
		}, helpMaxWidth)
		add("SET/FILTER", "SET/FILTER - Allow list-based filters.", setFilterLines)

		unsetFilterLines := helpEntryLines(
			"UNSET/FILTER - Block list-based filters (CC dialect).",
			[]string{"UNSET/FILTER <type> <list>"},
			nil,
			[]string{
				"Same semantics as REJECT.",
			},
		)
		unsetFilterLines = appendListSection(unsetFilterLines, "Types:", append([]string{"DXBM"}, filterListTypes()...))
		add("UNSET/FILTER", "UNSET/FILTER - Block list-based filters.", unsetFilterLines)

		setNoFilterLines := helpEntryLines(
			"SET/NOFILTER - Allow everything (CC dialect).",
			[]string{"SET/NOFILTER"},
			nil,
			[]string{
				"Resets filters to a fully permissive state.",
			},
		)
		add("SET/NOFILTER", "SET/NOFILTER - Allow everything.", setNoFilterLines)

		addCCToggle := func(cmd, summary, alias string) {
			lines := helpEntryLines(
				fmt.Sprintf("%s - %s", cmd, summary),
				[]string{cmd},
				nil,
				[]string{
					fmt.Sprintf("Alias of %s.", alias),
				},
			)
			add(cmd, fmt.Sprintf("%s - %s", cmd, summary), lines)
		}
		addCCToggle("SET/ANN", "Enable announcements.", "PASS ANNOUNCE")
		addCCToggle("SET/NOANN", "Disable announcements.", "REJECT ANNOUNCE")
		addCCToggle("SET/BEACON", "Enable beacon spots.", "PASS BEACON")
		addCCToggle("SET/NOBEACON", "Disable beacon spots.", "REJECT BEACON")
		addCCToggle("SET/WWV", "Enable WWV bulletins.", "PASS WWV")
		addCCToggle("SET/NOWWV", "Disable WWV bulletins.", "REJECT WWV")
		addCCToggle("SET/WCY", "Enable WCY bulletins.", "PASS WCY")
		addCCToggle("SET/NOWCY", "Disable WCY bulletins.", "REJECT WCY")
		addCCToggle("SET/SELF", "Enable self spots.", "PASS SELF")
		addCCToggle("SET/NOSELF", "Disable self spots.", "REJECT SELF")
		addCCToggle("SET/SKIMMER", "Allow skimmer spots.", "PASS SOURCE SKIMMER")
		addCCToggle("SET/NOSKIMMER", "Block skimmer spots.", "REJECT SOURCE SKIMMER")

		setModeLines := helpEntryLines(
			"SET/<MODE> - Allow a mode (CC dialect).",
			[]string{"SET/<MODE>"},
			nil,
			[]string{
				"Alias of PASS MODE <MODE>.",
			},
		)
		setModeLines = appendListSection(setModeLines, "Modes:", []string{"CW", "FT4", "FT8", "RTTY"})
		add("SET/<MODE>", "SET/<MODE> - Allow a mode.", setModeLines)

		setNoModeLines := helpEntryLines(
			"SET/NO<MODE> - Block a mode (CC dialect).",
			[]string{"SET/NO<MODE>"},
			nil,
			[]string{
				"Alias of REJECT MODE <MODE>.",
			},
		)
		setNoModeLines = appendListSection(setNoModeLines, "Modes:", []string{"CW", "FT4", "FT8", "RTTY"})
		add("SET/NO<MODE>", "SET/NO<MODE> - Block a mode.", setNoModeLines)

		catalog.order = []string{
			"HELP",
			"DX",
			"SHOW/DX",
			"SH/DX",
			"SHOW MYDX",
			"SHOW DXCC",
			"SHOW DEDUPE",
			"SET DEDUPE",
			"SET DIAG",
			"SET GRID",
			"SET NOISE",
			"PASS NEARBY",
			"SHOW/FILTER",
			"SH/FILTER",
			"RESET FILTER",
			"SET/FILTER",
			"UNSET/FILTER",
			"SET/NOFILTER",
			"SET/ANN",
			"SET/NOANN",
			"SET/BEACON",
			"SET/NOBEACON",
			"SET/WWV",
			"SET/NOWWV",
			"SET/WCY",
			"SET/NOWCY",
			"SET/SELF",
			"SET/NOSELF",
			"SET/SKIMMER",
			"SET/NOSKIMMER",
			"SET/<MODE>",
			"SET/NO<MODE>",
			"DIALECT",
			"BYE",
		}
	} else {
		showLines := helpEntryLines(
			"SHOW - See SHOW subcommands.",
			[]string{"SHOW DX [count]", "SHOW MYDX [count]", "SHOW DXCC <prefix|callsign>"},
			nil,
			[]string{
				"Use HELP SHOW DX for the history alias.",
			},
		)
		add("SHOW", "SHOW - See SHOW subcommands.", showLines)

		showFilterLines := helpEntryLines(
			"SHOW FILTER - Display current filter state.",
			[]string{"SHOW FILTER"},
			nil,
			[]string{
				"Shows effective allow/block state plus per-type lists.",
			},
		)
		add("SHOW FILTER", "SHOW FILTER - Display filter state.", showFilterLines)

		passLines := helpEntryLines(
			"PASS - Allow filter matches.",
			[]string{"PASS <type> <list>"},
			nil,
			[]string{
				"Adds to allowlist and removes from blocklist.",
				"List is comma or space separated; use ALL to allow all.",
			},
		)
		passLines = appendListSection(passLines, "Types:", filterListTypes())
		passLines = appendListSection(passLines, "Feature toggles:", []string{
			"PASS BEACON",
			"PASS WWV",
			"PASS WCY",
			"PASS ANNOUNCE",
			"PASS SELF",
		})
		add("PASS", "PASS - Allow filter matches.", passLines)

		rejectLines := helpEntryLines(
			"REJECT - Block filter matches.",
			[]string{"REJECT <type> <list>"},
			nil,
			[]string{
				"Adds to blocklist and removes from allowlist.",
				"List is comma or space separated; use ALL to block all.",
			},
		)
		rejectLines = appendListSection(rejectLines, "Types:", filterListTypes())
		rejectLines = appendListSection(rejectLines, "Feature toggles:", []string{
			"REJECT BEACON",
			"REJECT WWV",
			"REJECT WCY",
			"REJECT ANNOUNCE",
			"REJECT SELF",
		})
		add("REJECT", "REJECT - Block filter matches.", rejectLines)

		showDXLines := helpEntryLines(
			"SHOW DX - Alias of SHOW MYDX (stored history).",
			[]string{"SHOW DX [count]"},
			[]string{"SH DX"},
			[]string{
				"Count range is 1-250 (default 50).",
			},
		)
		add("SHOW DX", "SHOW DX - Alias of SHOW MYDX.", showDXLines, "SH DX")
		shDXLines := helpEntryLines(
			"SH DX - Alias of SHOW DX.",
			[]string{"SH DX [count]"},
			[]string{"SHOW DX"},
			nil,
		)
		add("SH DX", "SH DX - Alias of SHOW DX.", shDXLines)

		catalog.order = []string{
			"HELP",
			"DX",
			"SHOW DX",
			"SH DX",
			"SHOW MYDX",
			"SHOW DXCC",
			"SHOW DEDUPE",
			"SET DEDUPE",
			"SET DIAG",
			"SET GRID",
			"SET NOISE",
			"PASS NEARBY",
			"SHOW FILTER",
			"PASS",
			"REJECT",
			"RESET FILTER",
			"DIALECT",
			"BYE",
		}
	}

	return catalog
}

func normalizeHelpTopic(dialect string, topic string) string {
	upper := strings.ToUpper(strings.TrimSpace(topic))
	if upper == "" {
		return ""
	}
	upper = strings.Join(strings.Fields(upper), " ")

	switch {
	case strings.HasPrefix(upper, "PASS NEARBY"):
		return "PASS NEARBY"
	case strings.HasPrefix(upper, "SHOW DXCC"):
		return "SHOW DXCC"
	case strings.HasPrefix(upper, "SHOW MYDX"):
		return "SHOW MYDX"
	case strings.HasPrefix(upper, "SHOW DEDUPE"):
		return "SHOW DEDUPE"
	case strings.HasPrefix(upper, "SET DEDUPE"):
		return "SET DEDUPE"
	case strings.HasPrefix(upper, "SET DIAG"):
		return "SET DIAG"
	case strings.HasPrefix(upper, "SET GRID"):
		return "SET GRID"
	case strings.HasPrefix(upper, "SET NOISE"):
		return "SET NOISE"
	case strings.HasPrefix(upper, "RESET FILTER"):
		return "RESET FILTER"
	case strings.HasPrefix(upper, "DIALECT"):
		return "DIALECT"
	case upper == "BYE" || upper == "QUIT" || upper == "EXIT":
		return "BYE"
	case upper == "H":
		return "HELP"
	case upper == "HELP":
		return "HELP"
	case upper == "DX":
		return "DX"
	case upper == "SHOW":
		return "SHOW"
	case upper == "NEARBY":
		return "PASS NEARBY"
	}

	if dialect == "cc" {
		if strings.HasPrefix(upper, "SHOW FILTER") {
			return "SHOW/FILTER"
		}
		if strings.HasPrefix(upper, "SHOW/DX") || upper == "SH/DX" {
			return "SHOW/DX"
		}
		if strings.HasPrefix(upper, "SHOW/FILTER") || upper == "SH/FILTER" {
			return "SHOW/FILTER"
		}
		if strings.HasPrefix(upper, "SET/FILTER DXBM") {
			return "SET/FILTER"
		}
		if strings.HasPrefix(upper, "SET/FILTER ") || upper == "SET/FILTER" {
			return "SET/FILTER"
		}
		if strings.HasPrefix(upper, "UNSET/FILTER") {
			return "UNSET/FILTER"
		}
		if strings.HasPrefix(upper, "SET/NOFILTER") {
			return "SET/NOFILTER"
		}
		if strings.HasPrefix(upper, "SET/NO") {
			mode := strings.TrimPrefix(upper, "SET/NO")
			if isCCHelpMode(mode) {
				return "SET/NO<MODE>"
			}
		}
		if strings.HasPrefix(upper, "SET/") && !strings.Contains(upper, "/NO") {
			mode := strings.TrimPrefix(upper, "SET/")
			if isCCHelpMode(mode) {
				return "SET/<MODE>"
			}
		}
		return upper
	}

	if strings.HasPrefix(upper, "PASS ") {
		return "PASS"
	}
	if upper == "PASS" {
		return "PASS"
	}
	if strings.HasPrefix(upper, "REJECT ") {
		return "REJECT"
	}
	if upper == "REJECT" {
		return "REJECT"
	}
	if strings.HasPrefix(upper, "SHOW FILTER") {
		return "SHOW FILTER"
	}
	if strings.HasPrefix(upper, "SHOW DX") {
		return "SHOW DX"
	}
	if upper == "SH DX" {
		return "SHOW DX"
	}

	return upper
}

func isCCHelpMode(mode string) bool {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "CW", "FT4", "FT8", "RTTY":
		return true
	default:
		return false
	}
}

func helpEntryLines(summary string, usage []string, aliases []string, notes []string) []string {
	lines := []string{summary}
	lines = appendUsageLines(lines, usage)
	lines = appendAliases(lines, aliases, helpMaxWidth)
	lines = appendNotes(lines, notes, helpMaxWidth)
	return lines
}

func appendUsageLines(lines []string, usage []string) []string {
	if len(usage) == 0 {
		return lines
	}
	for i, entry := range usage {
		prefix := "Usage: "
		if i > 0 {
			prefix = "       "
		}
		lines = append(lines, prefix+entry)
	}
	return lines
}

func appendAliases(lines []string, aliases []string, width int) []string {
	if len(aliases) == 0 {
		return lines
	}
	lines = append(lines, wrapLabelList("Aliases:", aliases, width)...)
	return lines
}

func appendNotes(lines []string, notes []string, width int) []string {
	if len(notes) == 0 {
		return lines
	}
	lines = append(lines, "Notes:")
	for _, note := range notes {
		lines = append(lines, wrapTextLines(note, width, "  ", "  ")...)
	}
	return lines
}

func appendListSection(lines []string, title string, items []string) []string {
	if len(items) == 0 {
		return lines
	}
	lines = append(lines, title)
	lines = append(lines, wrapListLines(items)...)
	return lines
}

func wrapLabelList(label string, items []string, width int) []string {
	if len(items) == 0 {
		return nil
	}
	prefix := label + " "
	indent := strings.Repeat(" ", len(prefix))
	line := prefix
	lines := []string{}
	for _, item := range items {
		candidate := line + item
		if line != prefix {
			candidate = line + ", " + item
		}
		if len(candidate) > width && line != prefix {
			lines = append(lines, line)
			line = indent + item
			continue
		}
		line = candidate
	}
	if strings.TrimSpace(line) != "" {
		lines = append(lines, line)
	}
	return lines
}

func wrapTextLines(text string, width int, indentFirst string, indentNext string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if width <= 0 {
		return []string{indentFirst + text}
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}
	lines := []string{}
	line := indentFirst
	limit := width
	for _, word := range words {
		if line == indentFirst || strings.TrimSpace(line) == "" {
			if len(line)+len(word) > limit {
				lines = append(lines, line)
				line = indentNext + word
				continue
			}
			line += word
			continue
		}
		candidate := line + " " + word
		if len(candidate) > limit {
			lines = append(lines, line)
			line = indentNext + word
			continue
		}
		line = candidate
	}
	if strings.TrimSpace(line) != "" {
		lines = append(lines, line)
	}
	return lines
}

func filterHelpLines(dialect string) []string {
	lines := []string{
		"",
		"Filter core rules:",
		"PASS <type> <list> adds to allowlist and removes from blocklist.",
		"REJECT <type> <list> adds to blocklist and removes from allowlist.",
		"If an item appears in both lists, block wins.",
		"",
		"ALL keyword (type-scoped):",
		"PASS <type> ALL - allow everything for that type",
		"REJECT <type> ALL - block everything for that type",
		"RESET FILTER resets all filters to configured defaults for new users.",
		"",
		"Feature toggles (not list-based):",
		"PASS BEACON | REJECT BEACON",
		"PASS WWV | REJECT WWV",
		"PASS WCY | REJECT WCY",
		"PASS ANNOUNCE | REJECT ANNOUNCE",
		"PASS SELF | REJECT SELF",
		"PASS NEARBY ON|OFF",
	}
	if strings.EqualFold(strings.TrimSpace(dialect), "cc") {
		lines = append(lines,
			"",
			"CC shortcuts:",
			"SET/ANN | SET/NOANN",
			"SET/BEACON | SET/NOBEACON",
			"SET/WWV | SET/NOWWV",
			"SET/WCY | SET/NOWCY",
			"SET/SKIMMER | SET/NOSKIMMER",
			"SET/<MODE> | SET/NO<MODE> (CW, FT4, FT8, RTTY)",
			"SET/NOFILTER",
			"SET/FILTER <type> <list>",
			"UNSET/FILTER <type> <list>",
			"SET/FILTER <type>/ON  -> PASS <type> ALL",
			"SET/FILTER <type>/OFF -> REJECT <type> ALL",
			"SHOW/FILTER | SH/FILTER",
		)
	}
	return lines
}

func normalizeDialectString(dialect string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "cc":
		return "cc"
	default:
		return "go"
	}
}

func showDXUsage(dialect string) string {
	if normalizeDialectString(dialect) == "cc" {
		return "Usage: SHOW/DX [count 1-250]\n"
	}
	return "Usage: SHOW DX [count 1-250]\n"
}

const (
	helpMaxWidth              = 78
	showDXDefaultCount        = 50
	showDXMaxCount            = 250
	noLoggedUserMsg           = "No logged user found. Command ignored.\n"
	testCallCTYUnavailableMsg = "Test calls require CTY-valid prefix; CTY database unavailable.\n"
	testCallCTYInvalidMsg     = "Test calls require CTY-valid prefix; unknown test callsign.\n"
)

func filterListTypes() []string {
	return []string{
		"BAND", "MODE", "SOURCE", "DXCALL", "DECALL", "DXGRID2",
		"DEGRID2", "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC",
		"DEDXCC", "CONFIDENCE", "PATH",
	}
}

func wrapListLines(items []string) []string {
	lines := []string{}
	if len(items) == 0 {
		return lines
	}
	indent := "  "
	width := helpMaxWidth
	line := indent
	for _, item := range items {
		candidate := indent + item
		if line != indent {
			candidate = line + ", " + item
		}
		if len(candidate) > width && line != indent {
			lines = append(lines, line)
			line = indent + item
			continue
		}
		line = candidate
	}
	if strings.TrimSpace(line) != "" {
		lines = append(lines, line)
	}
	return lines
}

// Purpose: Identify test spotter calls and return the base call for CTY lookup.
// Key aspects: Requires no "/" segments, suffix TEST, and optional numeric SSID.
// Upstream: handleDX test-spotter gating.
// Downstream: CTY validation for the base call.
func testSpotterBaseCall(call string) (string, bool) {
	call = strings.TrimSpace(call)
	if call == "" || strings.Contains(call, "/") {
		return "", false
	}
	if strings.HasSuffix(call, "TEST") {
		return call, true
	}
	idx := strings.LastIndexByte(call, '-')
	if idx <= 0 || idx >= len(call)-1 {
		return "", false
	}
	suffix := call[idx+1:]
	for i := 0; i < len(suffix); i++ {
		if suffix[i] < '0' || suffix[i] > '9' {
			return "", false
		}
	}
	base := call[:idx]
	if strings.HasSuffix(base, "TEST") {
		return base, true
	}
	return "", false
}

// Purpose: Handle the DX command and enqueue a human spot.
// Key aspects: Validates callsign/frequency; parses comment for mode/report.
// Upstream: ProcessCommandForClient (DX).
// Downstream: spot.ParseSpotComment, spot.NewSpot, spotInput channel.
func (p *Processor) handleDX(fields []string, spotter string, spotterIP string) string {
	spotterRaw := strings.TrimSpace(spotter)
	if spotterRaw == "" {
		return "DX command requires a logged-in callsign.\n"
	}
	spotterNorm := spot.NormalizeCallsign(spotterRaw)
	if !spot.IsValidNormalizedCallsign(spotterNorm) {
		return "DX command requires a valid callsign.\n"
	}
	if len(fields) < 3 {
		return "Usage: DX <frequency> <callsign> [comment]\n"
	}
	freq, err := strconv.ParseFloat(fields[1], 64)
	if err != nil || freq <= 0 {
		return "Invalid frequency. Use a kHz value like 7001.0.\n"
	}
	dxRaw := strings.TrimSpace(fields[2])
	dx := spot.NormalizeCallsign(dxRaw)
	if !spot.IsValidNormalizedCallsign(dx) {
		return "Invalid DX callsign.\n"
	}
	if p.ctyLookup != nil {
		if db := p.ctyLookup(); db != nil {
			if _, ok := db.LookupCallsignPortable(dx); !ok {
				return "Unknown DX callsign (not in CTY database).\n"
			}
		}
	}
	testBaseCall, isTestSpotter := testSpotterBaseCall(spotterNorm)
	if isTestSpotter {
		if p.ctyLookup == nil {
			return testCallCTYUnavailableMsg
		}
		db := p.ctyLookup()
		if db == nil {
			return testCallCTYUnavailableMsg
		}
		if _, ok := db.LookupCallsignPortable(testBaseCall); !ok {
			return testCallCTYInvalidMsg
		}
	}
	if p.repGate != nil {
		now := time.Now().UTC()
		band := spot.FreqToBand(freq)
		decision := p.repGate.Check(reputation.Request{
			Call: spotterNorm,
			Band: band,
			IP:   spotterIP,
			Now:  now,
		})
		if decision.Drop {
			if p.repReport != nil {
				p.repReport(reputation.DropEvent{
					Call:        spotterNorm,
					Band:        band,
					IP:          spotterIP,
					Prefix:      decision.Prefix,
					Reason:      decision.Reason,
					Flags:       decision.Flags,
					ASN:         decision.ASN,
					CountryCode: decision.CountryCode,
					CountryName: decision.CountryName,
					Source:      decision.Source,
					When:        now,
				})
			}
			return ""
		}
	}
	comment := ""
	if len(fields) > 3 {
		comment = strings.TrimSpace(strings.Join(fields[3:], " "))
	}
	parsed := spot.ParseSpotComment(comment, freq)
	s := spot.NewSpotNormalized(dx, spotterNorm, freq, parsed.Mode)
	s.Comment = parsed.Comment
	s.Report = parsed.Report
	s.HasReport = parsed.HasReport
	s.SourceNode = spotterNorm
	s.SpotterIP = strings.TrimSpace(spotterIP)
	s.IsTestSpotter = isTestSpotter

	if p.spotInput == nil {
		return "Spot input is not configured on this cluster.\n"
	}
	select {
	case p.spotInput <- s:
		return "Spot queued.\n"
	default:
		log.Printf("DX command: dedup input full, dropping spot from %s", spotter)
		return "Spot queue full; try again.\n"
	}
}

// Purpose: Route SHOW subcommands with optional filter predicate.
// Key aspects: Supports SHOW/DX, SHOW/MYDX, and SHOW DXCC lookups.
// Upstream: ProcessCommandForClient (SHOW/SH).
// Downstream: handleShowMYDX, handleShowDXCC.
func (p *Processor) handleShow(args []string, filterFn func(*spot.Spot) bool, dialect string) string {
	if len(args) == 0 {
		return showDXUsage(dialect)
	}

	subCmd := args[0]

	switch subCmd {
	case "DX":
		return p.handleShowMYDX(args[1:], filterFn)
	case "MYDX":
		return p.handleShowMYDX(args[1:], filterFn)
	case "DXCC":
		return p.handleShowDXCC(args[1:])
	default:
		return fmt.Sprintf("Unknown SHOW subcommand: %s\n", subCmd)
	}
}

// Purpose: Render the most recent N spots that match the provided filter.
// Key aspects: Archive-only history; outputs oldest-first.
// Upstream: handleShow (SHOW MYDX).
// Downstream: archive.RecentFiltered.
func (p *Processor) handleShowMYDX(args []string, filterFn func(*spot.Spot) bool) string {
	if filterFn == nil {
		return noLoggedUserMsg
	}
	count := showDXDefaultCount // Default count

	if len(args) > 0 {
		var err error
		_, err = fmt.Sscanf(args[0], "%d", &count)
		if err != nil || count < 1 || count > showDXMaxCount {
			return "Invalid count. Use 1-250.\n"
		}
	}

	var spots []*spot.Spot
	if p.archive == nil {
		return "No spots available.\n"
	}
	if rows, err := p.archive.RecentFiltered(count, filterFn); err != nil {
		log.Printf("SHOW MYDX: archive query failed: %v", err)
	} else {
		spots = rows
	}
	if len(spots) == 0 {
		return "No spots available.\n"
	}

	reverseSpotsInPlace(spots)

	var result strings.Builder
	for _, spot := range spots {
		result.WriteString(spot.FormatDXCluster())
		result.WriteString("\r\n")
	}

	return result.String()
}

// Purpose: Reverse a slice of spots in place.
// Key aspects: Used to present chronological output.
// Upstream: handleShowDX.
// Downstream: None.
func reverseSpotsInPlace(spots []*spot.Spot) {
	for i, j := 0, len(spots)-1; i < j; i, j = i+1, j-1 {
		spots[i], spots[j] = spots[j], spots[i]
	}
}

// Purpose: Resolve CTY metadata for a prefix or callsign and render DXCC details.
// Key aspects: Uses CTY portable lookup, reports ADIF/country/zones, and lists sibling prefixes for the same ADIF.
// Upstream: handleShow (SHOW DXCC).
// Downstream: CTY lookup, prefixIdx for sibling retrieval.
func (p *Processor) handleShowDXCC(args []string) string {
	if len(args) == 0 {
		return "Usage: SHOW DXCC <prefix|callsign>\n"
	}
	if p.ctyLookup == nil {
		return "CTY database is not available.\n"
	}
	db := p.ctyLookup()
	if db == nil {
		return "CTY database is not loaded.\n"
	}

	queryRaw := strings.TrimSpace(args[0])
	if queryRaw == "" {
		return "Usage: SHOW DXCC <prefix|callsign>\n"
	}
	lookup := spot.NormalizeCallsign(queryRaw)
	if lookup == "" {
		return "Unknown DXCC/prefix.\n"
	}

	info, ok := db.LookupCallsignPortable(lookup)
	if !ok || info == nil {
		return "Unknown DXCC/prefix.\n"
	}

	prefix := strings.ToUpper(strings.TrimSpace(info.Prefix))
	country := strings.TrimSpace(info.Country)
	continent := strings.ToUpper(strings.TrimSpace(info.Continent))
	others := p.prefixIdx.siblings(db, info.ADIF, prefix)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%s -> ADIF %d | %s (%s) | Prefix: %s | CQ %d | ITU %d",
		lookup, info.ADIF, country, continent, prefix, info.CQZone, info.ITUZone))
	if len(others) > 0 {
		b.WriteString(" | Other: ")
		b.WriteString(strings.Join(others, ", "))
	}
	b.WriteByte('\n')
	return b.String()
}

// prefixIndex caches ADIF->prefix list mappings for the current CTY DB pointer.
// It builds a fresh map when the DB pointer changes (e.g., after a CTY refresh).
type prefixIndex struct {
	mu             sync.Mutex
	db             *cty.CTYDatabase
	adifToPrefixes map[int][]string
}

func (p *prefixIndex) siblings(db *cty.CTYDatabase, adif int, current string) []string {
	if p == nil || db == nil {
		return nil
	}
	p.mu.Lock()
	if p.db != db {
		p.adifToPrefixes = buildPrefixMap(db)
		p.db = db
	}
	prefixes := p.adifToPrefixes[adif]
	p.mu.Unlock()

	current = strings.ToUpper(strings.TrimSpace(current))
	out := make([]string, 0, len(prefixes))
	for _, pref := range prefixes {
		if pref == "" || pref == current {
			continue
		}
		out = append(out, pref)
	}
	return out
}

func buildPrefixMap(db *cty.CTYDatabase) map[int][]string {
	if db == nil {
		return nil
	}
	tmp := make(map[int]map[string]struct{}, len(db.Data))
	for _, info := range db.Data {
		pref := strings.ToUpper(strings.TrimSpace(info.Prefix))
		if pref == "" {
			continue
		}
		set, ok := tmp[info.ADIF]
		if !ok {
			set = make(map[string]struct{})
			tmp[info.ADIF] = set
		}
		set[pref] = struct{}{}
	}
	result := make(map[int][]string, len(tmp))
	for adif, set := range tmp {
		prefixes := make([]string, 0, len(set))
		for pref := range set {
			prefixes = append(prefixes, pref)
		}
		sort.Strings(prefixes)
		result[adif] = prefixes
	}
	return result
}
