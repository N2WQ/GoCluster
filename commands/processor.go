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
	"dxcluster/strutil"
)

// archiveReader is the minimal interface the archive layer exposes for read paths.
type archiveReader interface {
	Recent(limit int) ([]*spot.Spot, error)
	RecentFiltered(limit int, match func(*spot.Spot) bool) ([]*spot.Spot, error)
}

type whoSpotsMeQuerier interface {
	Window() time.Duration
	CountryCountsByContinent(call, band string, now time.Time) map[string][]spot.WhoSpotsMeCountryCount
}

// PathGlyphHelpConfig carries the configured telnet path glyph display mapping
// into HELP rendering. It is a startup snapshot, so Processor never performs
// config I/O or depends on runtime reload behavior.
type PathGlyphHelpConfig struct {
	Enabled      bool
	High         string
	Medium       string
	Low          string
	Unlikely     string
	Insufficient string
}

// DedupeHelpConfig carries the effective secondary dedupe policy windows into
// HELP rendering. It is a startup snapshot so HELP remains deterministic and
// does not depend on runtime config access.
type DedupeHelpConfig struct {
	Configured        bool
	FastWindowSeconds int
	MedWindowSeconds  int
	SlowWindowSeconds int
}

// WhoSpotsMeHelpConfig carries the configured WHOSPOTSME rolling window into
// HELP rendering.
type WhoSpotsMeHelpConfig struct {
	Configured    bool
	WindowMinutes int
}

// BuildInfo carries the startup-resolved binary identity into SHOW BUILD.
type BuildInfo struct {
	Version     string
	Commit      string
	BuildTime   string
	VCSModified string
	GoVersion   string
}

// ProcessorOption customizes Processor construction without widening the core
// command-call surface.
type ProcessorOption func(*Processor)

// BadCallReporter receives callsign-validation drops from manually entered DX
// commands. The callback should be short because it runs in the session command path.
type BadCallReporter func(source, role, reason, call, deCall, dxCall, mode, detail string)

// Processor handles telnet command parsing and replies that rely on shared state
// (recent spots in the archive).
type Processor struct {
	spotBuffer     *buffer.RingBuffer
	archive        archiveReader
	spotInput      chan<- *spot.Spot
	ctyLookup      func() *cty.CTYDatabase
	prefixIdx      *prefixIndex
	repGate        *reputation.Gate
	repReport      func(reputation.DropEvent)
	badCallReport  BadCallReporter
	pathGlyphHelp  PathGlyphHelpConfig
	dedupeHelp     DedupeHelpConfig
	whoSpotsMe     whoSpotsMeQuerier
	whoSpotsMeHelp WhoSpotsMeHelpConfig
	buildInfo      BuildInfo
}

// NewProcessor constructs a command processor bound to shared spot state.
// Key aspects: SHOW/DX uses archive history; DX commands can enqueue spots.
// Upstream: Telnet server initialization.
// Downstream: Processor methods (ProcessCommand, handleShowDX, handleDX).
func NewProcessor(buf *buffer.RingBuffer, archive archiveReader, spotInput chan<- *spot.Spot, ctyLookup func() *cty.CTYDatabase, repGate *reputation.Gate, repReport func(reputation.DropEvent), opts ...ProcessorOption) *Processor {
	p := &Processor{
		spotBuffer: buf,
		archive:    archive,
		spotInput:  spotInput,
		ctyLookup:  ctyLookup,
		prefixIdx:  &prefixIndex{},
		repGate:    repGate,
		repReport:  repReport,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

// WithPathGlyphHelp configures top-level HELP to describe the actual glyphs
// shown in telnet output for path reliability.
func WithPathGlyphHelp(cfg PathGlyphHelpConfig) ProcessorOption {
	return func(p *Processor) {
		if p == nil {
			return
		}
		p.pathGlyphHelp = cfg
	}
}

// WithDedupeHelp configures HELP text for SHOW/SET DEDUPE using the effective
// runtime window values from dedupe.yaml.
func WithDedupeHelp(cfg DedupeHelpConfig) ProcessorOption {
	return func(p *Processor) {
		if p == nil {
			return
		}
		p.dedupeHelp = cfg
	}
}

// WithWhoSpotsMe installs the WHOSPOTSME query backend.
func WithWhoSpotsMe(store whoSpotsMeQuerier) ProcessorOption {
	return func(p *Processor) {
		if p == nil {
			return
		}
		p.whoSpotsMe = store
	}
}

// WithWhoSpotsMeHelp configures HELP text for WHOSPOTSME using the effective
// runtime window from YAML.
func WithWhoSpotsMeHelp(cfg WhoSpotsMeHelpConfig) ProcessorOption {
	return func(p *Processor) {
		if p == nil {
			return
		}
		p.whoSpotsMeHelp = cfg
	}
}

// WithBuildInfo installs the startup-resolved binary identity for SHOW BUILD.
func WithBuildInfo(info BuildInfo) ProcessorOption {
	return func(p *Processor) {
		if p == nil {
			return
		}
		p.buildInfo = normalizeBuildInfo(info)
	}
}

// WithBadCallReporter installs an optional callback for callsign-validation
// drops in manually entered DX commands.
func WithBadCallReporter(reporter BadCallReporter) ProcessorOption {
	return func(p *Processor) {
		if p == nil {
			return
		}
		p.badCallReport = reporter
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
	if strings.EqualFold(fields[0], "WHOSPOTSME") {
		return p.handleWhoSpotsMe(fields[1:], spotter)
	}

	// Split into parts
	parts := strings.Fields(strings.ToUpper(cmd))
	command := parts[0]
	dialect = normalizeDialectString(dialect)

	if dialect == "cc" {
		switch command {
		case "SHOW/DX", "SH/DX":
			return p.handleShow(append([]string{"DX"}, parts[1:]...), filterFn, dialect, spotter)
		case "SHOW", "SH":
			if len(parts) >= 2 && parts[1] == "DX" {
				return p.handleShow(append([]string{"DX"}, parts[2:]...), filterFn, dialect, spotter)
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
		return p.handleShow(parts[1:], filterFn, dialect, spotter)
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
	catalog := buildHelpCatalog(dialect, p.dedupeHelp, p.whoSpotsMeHelp)
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
	lines = append(lines, pathGlyphHelpLines(p.pathGlyphHelp)...)
	lines = append(lines, "")
	lines = append(lines, "List types:")
	lines = append(lines, wrapListLines(filterListTypes())...)
	lines = append(lines, "")
	lines = append(lines, "Supported modes:")
	lines = append(lines, wrapListLines(filter.SupportedModes())...)
	lines = append(lines, "")
	lines = append(lines, "Supported events:")
	lines = append(lines, wrapListLines(filter.SupportedEvents())...)
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

func buildHelpCatalog(dialect string, dedupeHelp DedupeHelpConfig, whoSpotsMeHelp WhoSpotsMeHelpConfig) helpCatalog {
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
		[]string{
			"DX <freq_khz> <callsign> [comment]",
			"DX <callsign> <freq_khz> [comment]",
		},
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
		[]string{
			"SHOW MYDX [count]",
			"SHOW MYDX <prefix|callsign> [count]",
			"SHOW MYDX [count] <prefix|callsign>",
		},
		nil,
		[]string{
			"History is pulled from stored spots (not live buffer).",
			"Count range is 1-250 (default 50).",
			"Respects your filters; self-spots always pass.",
			"When prefix/call is present, only matching DXCC (ADIF) spots are shown.",
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

	showBuildLines := helpEntryLines(
		"SHOW BUILD - Show binary build metadata.",
		[]string{"SHOW BUILD"},
		nil,
		[]string{
			"Shows version, commit, build time, dirty flag, and Go version.",
		},
	)
	add("SHOW BUILD", "SHOW BUILD - Show binary build metadata.", showBuildLines)

	showOwnLines := helpEntryLines(
		"SHOW OWN - Show your login and baseline own call.",
		[]string{"SHOW OWN"},
		nil,
		[]string{
			"Numeric SSIDs are ignored for own-call features.",
		},
	)
	add("SHOW OWN", "SHOW OWN - Show own-call identity.", showOwnLines)

	whoSpotsMeLines := helpEntryLines(
		"WHOSPOTSME - Show recent spotter countries for your call.",
		[]string{"WHOSPOTSME [band]"},
		nil,
		whoSpotsMeHelpNotes(whoSpotsMeHelp),
	)
	add("WHOSPOTSME", "WHOSPOTSME - Show recent spotter countries.", whoSpotsMeLines)

	showDedupeLines := helpEntryLines(
		"SHOW DEDUPE - Show your broadcast dedupe policy.",
		[]string{"SHOW DEDUPE"},
		nil,
		dedupeHelpNotes(dedupeHelp, false),
	)
	add("SHOW DEDUPE", "SHOW DEDUPE - Show dedupe policy.", showDedupeLines)

	setDedupeLines := helpEntryLines(
		"SET DEDUPE - Select broadcast dedupe policy.",
		[]string{"SET DEDUPE <FAST|MED|SLOW>"},
		nil,
		dedupeHelpNotes(dedupeHelp, true),
	)
	add("SET DEDUPE", "SET DEDUPE - Select dedupe policy.", setDedupeLines)

	setDiagLines := helpEntryLines(
		"SET DIAG - Select diagnostic comments.",
		[]string{"SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>"},
		nil,
		[]string{
			"DEDUPE shows <DE-DXCC>|<DE-key>|<src>|<policy>.",
			"SOURCE shows compact ingest provenance.",
			"CONF shows pipeline confidence percent when available.",
			"PATH shows observation count, weight, and age.",
			"MODE shows <mode>|<provenance>.",
		},
	)
	add("SET DIAG", "SET DIAG - Select diagnostic comments.", setDiagLines)

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
		[]string{"SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>"},
		nil,
		[]string{
			"Default is QUIET when unset.",
		},
	)
	add("SET NOISE", "SET NOISE - Set noise class.", setNoiseLines)

	setPathSamplesLines := helpEntryLines(
		"SET PATHSAMPLES - Set your path sample floor.",
		[]string{"SET PATHSAMPLES <count|DEFAULT>"},
		nil,
		[]string{
			"Count must be higher than the cluster default.",
			"DEFAULT clears your personal override.",
		},
	)
	add("SET PATHSAMPLES", "SET PATHSAMPLES - Set path sample floor.", setPathSamplesLines)

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
			[]string{"SHOW MYDX [count|selector]", "SHOW DXCC <prefix|callsign>", "SHOW BUILD", "SHOW OWN"},
			nil,
			[]string{
				"Use HELP SHOW/DX for the history alias.",
			},
		)
		add("SHOW", "SHOW - See SHOW subcommands.", showLines)

		showDXLines := helpEntryLines(
			"SHOW/DX - Alias of SHOW MYDX (stored history).",
			[]string{
				"SHOW/DX [count]",
				"SHOW/DX <prefix|callsign> [count]",
				"SHOW/DX [count] <prefix|callsign>",
			},
			[]string{"SH/DX"},
			[]string{
				"Count range is 1-250 (default 50).",
				"When prefix/call is present, only matching DXCC (ADIF) spots are shown.",
			},
		)
		add("SHOW/DX", "SHOW/DX - Alias of SHOW MYDX.", showDXLines, "SH/DX")
		shDXLines := helpEntryLines(
			"SH/DX - Alias of SHOW/DX.",
			[]string{
				"SH/DX [count]",
				"SH/DX <prefix|callsign> [count]",
				"SH/DX [count] <prefix|callsign>",
			},
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
		setModeLines = appendListSection(setModeLines, "Modes:", spot.CCShortcutModes())
		add("SET/<MODE>", "SET/<MODE> - Allow a mode.", setModeLines)

		setNoModeLines := helpEntryLines(
			"SET/NO<MODE> - Block a mode (CC dialect).",
			[]string{"SET/NO<MODE>"},
			nil,
			[]string{
				"Alias of REJECT MODE <MODE>.",
			},
		)
		setNoModeLines = appendListSection(setNoModeLines, "Modes:", spot.CCShortcutModes())
		add("SET/NO<MODE>", "SET/NO<MODE> - Block a mode.", setNoModeLines)

		catalog.order = []string{
			"HELP",
			"DX",
			"SHOW/DX",
			"SH/DX",
			"SHOW MYDX",
			"SHOW DXCC",
			"SHOW BUILD",
			"SHOW OWN",
			"WHOSPOTSME",
			"SHOW DEDUPE",
			"SET DEDUPE",
			"SET DIAG",
			"SET SOLAR",
			"SET GRID",
			"SET NOISE",
			"SET PATHSAMPLES",
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
			[]string{"SHOW DX [count|selector]", "SHOW MYDX [count|selector]", "SHOW DXCC <prefix|callsign>", "SHOW BUILD", "SHOW OWN"},
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
				"For MODE, list entries are enabled without changing modes not listed.",
				"Use PASS MODE UNKNOWN or PASS MODE ALL to restore blank-mode spots.",
			},
		)
		passLines = appendListSection(passLines, "Types:", filterListTypes())
		passLines = appendListSection(passLines, "Feature toggles:", []string{
			"PASS BEACON",
			"PASS WWV",
			"PASS WCY",
			"PASS ANNOUNCE",
			"PASS SELF",
			"PASS TOXIC",
		})
		add("PASS", "PASS - Allow filter matches.", passLines)

		rejectLines := helpEntryLines(
			"REJECT - Block filter matches.",
			[]string{"REJECT <type> <list>"},
			nil,
			[]string{
				"Adds to blocklist and removes from allowlist.",
				"List is comma or space separated; use ALL to block all.",
				"For MODE, list entries are rejected without changing modes not listed.",
				"UNKNOWN is the MODE token for blank-mode spots.",
			},
		)
		rejectLines = appendListSection(rejectLines, "Types:", filterListTypes())
		rejectLines = appendListSection(rejectLines, "Feature toggles:", []string{
			"REJECT BEACON",
			"REJECT WWV",
			"REJECT WCY",
			"REJECT ANNOUNCE",
			"REJECT SELF",
			"REJECT TOXIC",
		})
		add("REJECT", "REJECT - Block filter matches.", rejectLines)

		showDXLines := helpEntryLines(
			"SHOW DX - Alias of SHOW MYDX (stored history).",
			[]string{
				"SHOW DX [count]",
				"SHOW DX <prefix|callsign> [count]",
				"SHOW DX [count] <prefix|callsign>",
			},
			[]string{"SH DX"},
			[]string{
				"Count range is 1-250 (default 50).",
				"When prefix/call is present, only matching DXCC (ADIF) spots are shown.",
			},
		)
		add("SHOW DX", "SHOW DX - Alias of SHOW MYDX.", showDXLines, "SH DX")
		shDXLines := helpEntryLines(
			"SH DX - Alias of SHOW DX.",
			[]string{
				"SH DX [count]",
				"SH DX <prefix|callsign> [count]",
				"SH DX [count] <prefix|callsign>",
			},
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
			"SHOW BUILD",
			"SHOW OWN",
			"WHOSPOTSME",
			"SHOW DEDUPE",
			"SET DEDUPE",
			"SET DIAG",
			"SET SOLAR",
			"SET GRID",
			"SET NOISE",
			"SET PATHSAMPLES",
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
	upper := strutil.NormalizeUpper(topic)
	if upper == "" {
		return ""
	}
	upper = strings.Join(strings.Fields(upper), " ")

	switch {
	case strings.HasPrefix(upper, "PASS NEARBY"):
		return "PASS NEARBY"
	case strings.HasPrefix(upper, "SHOW BUILD"):
		return "SHOW BUILD"
	case strings.HasPrefix(upper, "SHOW OWN"):
		return "SHOW OWN"
	case strings.HasPrefix(upper, "SHOW DXCC"):
		return "SHOW DXCC"
	case strings.HasPrefix(upper, "WHOSPOTSME"):
		return "WHOSPOTSME"
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
	case strings.HasPrefix(upper, "SET PATHSAMPLES"):
		return "SET PATHSAMPLES"
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
		if strings.HasPrefix(upper, "SHOW DX") || upper == "SH DX" {
			return "SHOW/DX"
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
	return spot.IsCCShortcutMode(strutil.NormalizeUpper(mode))
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
		lines = append(lines, wrapTextLines(note, width, "  ")...)
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

func wrapTextLines(text string, width int, indentNext string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	const indentFirst = "  "
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
		"PASS/REJECT MODE <list> are deltas; modes not listed are unchanged.",
		"UNKNOWN is the MODE token for blank-mode spots.",
		"If an item appears in both lists, block wins.",
		"",
		"ALL keyword (type-scoped):",
		"PASS <type> ALL - allow everything for that type",
		"REJECT <type> ALL - block everything for most types",
		"REJECT EVENT ALL - block only tagged EVENT spots",
		"RESET FILTER resets all filters to configured defaults for new users.",
		"",
		"Feature toggles (not list-based):",
		"PASS BEACON | REJECT BEACON",
		"PASS WWV | REJECT WWV",
		"PASS WCY | REJECT WCY",
		"PASS ANNOUNCE | REJECT ANNOUNCE",
		"PASS SELF | REJECT SELF",
		"PASS TOXIC | REJECT TOXIC",
		"PASS NEARBY ON|OFF",
	}
	lines = append(lines, "", "Confidence glyphs:")
	for _, note := range []string{
		"? - One reporter only; no prior/static support promoted it to S.",
		"S - One reporter only, but the call has static or recent on-band support.",
		"P - Resolver modes: lower-confidence multi-spotter support. FT modes: corroboration burst support at or above the configured P threshold but below the configured V threshold.",
		"V - Resolver modes: higher-confidence multi-spotter support. FT modes: corroboration burst support at or above the configured V threshold.",
		"C - The call was corrected.",
		"B - A correction was attempted, but base-call or CTY validation failed, so the original call was kept.",
	} {
		lines = append(lines, wrapTextLines(note, helpMaxWidth, "    ")...)
	}
	lines = append(lines, "", "Event filters:")
	lines = append(lines, wrapTextLines("EVENT recognizes the taxonomy EVENT families as standalone comment tokens or acronym-prefixed references such as POTA-1234. Only the event family is filtered; the reference remains in the comment.", helpMaxWidth, "    ")...)
	lines = append(lines, wrapTextLines("Spots with no recognized EVENT tag are not affected by EVENT filters, including REJECT EVENT ALL.", helpMaxWidth, "    ")...)
	if strings.EqualFold(strings.TrimSpace(dialect), "cc") {
		ccModeLine := "SET/<MODE> | SET/NO<MODE> (" + strings.Join(spot.CCShortcutModes(), ", ") + ")"
		lines = append(lines,
			"",
			"CC shortcuts:",
			"SET/ANN | SET/NOANN",
			"SET/BEACON | SET/NOBEACON",
			"SET/WWV | SET/NOWWV",
			"SET/WCY | SET/NOWCY",
			"SET/SKIMMER | SET/NOSKIMMER",
			ccModeLine,
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

func pathGlyphHelpLines(cfg PathGlyphHelpConfig) []string {
	if !cfg.Enabled {
		return nil
	}
	if cfg.High == "" || cfg.Medium == "" || cfg.Low == "" || cfg.Unlikely == "" || cfg.Insufficient == "" {
		return nil
	}
	lines := []string{
		"",
		"Path reliability glyphs:",
	}
	for _, note := range []string{
		fmt.Sprintf("%s - HIGH: favorable path.", quoteHelpGlyph(cfg.High)),
		fmt.Sprintf("%s - MEDIUM: workable path.", quoteHelpGlyph(cfg.Medium)),
		fmt.Sprintf("%s - LOW: weak or marginal path.", quoteHelpGlyph(cfg.Low)),
		fmt.Sprintf("%s - UNLIKELY: poor path.", quoteHelpGlyph(cfg.Unlikely)),
		fmt.Sprintf("%s - INSUFFICIENT: not enough recent evidence.", quoteHelpGlyph(cfg.Insufficient)),
		"PATH filters use HIGH, MEDIUM, LOW, UNLIKELY, INSUFFICIENT.",
	} {
		lines = append(lines, wrapTextLines(note, helpMaxWidth, "    ")...)
	}
	return lines
}

func dedupeHelpNotes(cfg DedupeHelpConfig, includeFallbackNote bool) []string {
	if !cfg.Configured {
		notes := []string{
			"FAST = short window; MED = medium window; SLOW = long window.",
			"FAST/MED use 2-character grid squares; SLOW uses CQ zones.",
		}
		if includeFallbackNote {
			notes = append(notes, "If a policy is disabled, the nearest available is chosen.")
		} else {
			notes = append(notes, "Shows if a policy is disabled server-side.")
		}
		return notes
	}

	notes := []string{
		formatDedupePolicyNote("FAST", cfg.FastWindowSeconds, "DE grid2"),
		formatDedupePolicyNote("MED", cfg.MedWindowSeconds, "DE grid2"),
		formatDedupePolicyNote("SLOW", cfg.SlowWindowSeconds, "DE CQ zone"),
	}
	if includeFallbackNote {
		notes = append(notes, "If a policy is disabled, the nearest available is chosen.")
	} else {
		notes = append(notes, "Shows if a policy is disabled server-side.")
	}
	return notes
}

func formatDedupePolicyNote(name string, windowSeconds int, locationKey string) string {
	if windowSeconds <= 0 {
		return fmt.Sprintf("%s - disabled server-side. Key: band + DE DXCC (ADIF) + %s + DX call.", name, locationKey)
	}
	return fmt.Sprintf("%s - %ds window. Key: band + DE DXCC (ADIF) + %s + DX call.", name, windowSeconds, locationKey)
}

func quoteHelpGlyph(glyph string) string {
	return strconv.Quote(glyph)
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
		return showHistoryUsage("SHOW/DX")
	}
	return showHistoryUsage("SHOW DX")
}

func showHistoryUsage(command string) string {
	return fmt.Sprintf("Usage: %s [count 1-250] | %s <prefix|callsign> [count 1-250]\n", command, command)
}

func whoSpotsMeUsage() string {
	return "Usage: WHOSPOTSME [band]\n"
}

const (
	helpMaxWidth              = 78
	showDXDefaultCount        = 50
	showDXMaxCount            = 250
	noLoggedUserMsg           = "No logged user found. Command ignored.\n"
	testCallCTYUnavailableMsg = "Test calls require CTY-valid prefix; CTY database unavailable.\n"
	testCallCTYInvalidMsg     = "Test calls require CTY-valid prefix; unknown test callsign.\n"
)

type showHistoryRequest struct {
	count    int
	selector string
}

func parseShowHistoryCount(token string) (int, bool, string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, false, ""
	}
	count, err := strconv.Atoi(token)
	if err != nil {
		return 0, false, ""
	}
	if count < 1 || count > showDXMaxCount {
		return 0, true, "Invalid count. Use 1-250.\n"
	}
	return count, true, ""
}

func parseShowHistoryRequest(args []string, commandLabel string) (showHistoryRequest, string) {
	req := showHistoryRequest{count: showDXDefaultCount}
	switch len(args) {
	case 0:
		return req, ""
	case 1:
		if count, isCount, errText := parseShowHistoryCount(args[0]); isCount {
			if errText != "" {
				return showHistoryRequest{}, errText
			}
			req.count = count
			return req, ""
		}
		req.selector = strings.TrimSpace(args[0])
		if req.selector == "" {
			return showHistoryRequest{}, showHistoryUsage(commandLabel)
		}
		return req, ""
	case 2:
		countA, isCountA, errTextA := parseShowHistoryCount(args[0])
		countB, isCountB, errTextB := parseShowHistoryCount(args[1])
		if errTextA != "" {
			return showHistoryRequest{}, errTextA
		}
		if errTextB != "" {
			return showHistoryRequest{}, errTextB
		}
		switch {
		case isCountA && isCountB:
			return showHistoryRequest{}, showHistoryUsage(commandLabel)
		case isCountA && !isCountB:
			req.count = countA
			req.selector = strings.TrimSpace(args[1])
		case !isCountA && isCountB:
			req.count = countB
			req.selector = strings.TrimSpace(args[0])
		default:
			return showHistoryRequest{}, showHistoryUsage(commandLabel)
		}
		if req.selector == "" {
			return showHistoryRequest{}, showHistoryUsage(commandLabel)
		}
		return req, ""
	default:
		return showHistoryRequest{}, showHistoryUsage(commandLabel)
	}
}

func filterListTypes() []string {
	return []string{
		"BAND", "MODE", "SOURCE", "EVENT", "DXCALL", "DECALL", "DXGRID2",
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

// parseDXCommandArgs resolves the DX command's frequency and DX tokens.
// Canonical order stays preferred when the first argument is a plain kHz value.
// Logger-style order is accepted only when arg1 looks like a real callsign and
// arg2 is the plain kHz token.
func parseDXCommandArgs(fields []string) (freq float64, dxRaw string, ok bool) {
	if len(fields) < 3 {
		return 0, "", false
	}
	if parsedFreq, freqOK := parseDXFrequencyToken(fields[1]); freqOK {
		return parsedFreq, strings.TrimSpace(fields[2]), true
	}
	if !isPlausibleDXCallToken(fields[1]) {
		return 0, "", false
	}
	parsedFreq, freqOK := parseDXFrequencyToken(fields[2])
	if !freqOK {
		return 0, "", false
	}
	return parsedFreq, strings.TrimSpace(fields[1]), true
}

// parseDXFrequencyToken accepts only plain decimal kHz values so command-order
// routing does not confuse float-like callsigns (for example, 5E7) with a
// frequency token.
func parseDXFrequencyToken(token string) (float64, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, false
	}
	digitCount := 0
	dotCount := 0
	for i := 0; i < len(token); i++ {
		switch ch := token[i]; {
		case ch >= '0' && ch <= '9':
			digitCount++
		case ch == '.':
			dotCount++
			if dotCount > 1 {
				return 0, false
			}
		default:
			return 0, false
		}
	}
	if digitCount == 0 {
		return 0, false
	}
	freq, err := strconv.ParseFloat(token, 64)
	if err != nil || freq <= 0 {
		return 0, false
	}
	return freq, true
}

func isPlausibleDXCallToken(token string) bool {
	normalized := spot.NormalizeCallsign(strings.TrimSpace(token))
	return spot.IsValidNormalizedCallsign(normalized) && containsASCIILetter(normalized)
}

func containsASCIILetter(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return true
		}
	}
	return false
}

func (p *Processor) reportBadCall(source, role, reason, call, deCall, dxCall, mode, detail string) {
	if p == nil || p.badCallReport == nil {
		return
	}
	p.badCallReport(source, role, reason, call, deCall, dxCall, mode, detail)
}

func manualDXMode(fields []string, freq float64) string {
	if len(fields) <= 3 {
		return ""
	}
	comment := strings.TrimSpace(strings.Join(fields[3:], " "))
	if comment == "" {
		return ""
	}
	return spot.ParseSpotComment(comment, freq).Mode
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
	spotterNorm := spot.NormalizeOwnCallsign(spotterRaw)
	if !spot.IsValidNormalizedCallsign(spotterNorm) {
		p.reportBadCall("manual:"+spotterRaw, "DE", "invalid_callsign", spotterRaw, spotterRaw, "", "", "manual_dx")
		return "DX command requires a valid callsign.\n"
	}
	if len(fields) < 3 {
		return "Usage: DX <freq_khz> <callsign> [comment]\n" +
			"       DX <callsign> <freq_khz> [comment]\n"
	}
	freq, dxRaw, ok := parseDXCommandArgs(fields)
	if !ok {
		return "Invalid frequency. Use a kHz value like 7001.0.\n"
	}
	dx := spot.NormalizeSpotDXCallsign(dxRaw)
	if !spot.IsValidNormalizedCallsign(dx) {
		p.reportBadCall("manual:"+spotterNorm, "DX", "invalid_callsign", dxRaw, spotterNorm, dxRaw, manualDXMode(fields, freq), "manual_dx")
		return "Invalid DX callsign.\n"
	}
	if p.ctyLookup != nil {
		if db := p.ctyLookup(); db != nil {
			if _, ok := db.LookupCallsignPortable(dx); !ok {
				p.reportBadCall("manual:"+spotterNorm, "DX", "cty_unknown", dx, spotterNorm, dx, manualDXMode(fields, freq), "manual_dx")
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
			p.reportBadCall("manual:"+spotterNorm, "DE", "cty_unknown", testBaseCall, spotterNorm, dx, manualDXMode(fields, freq), "test_spotter_base")
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
	if parsed.Mode != "" {
		s.ModeProvenance = spot.ModeProvenanceCommentExplicit
	}
	s.Comment = parsed.Comment
	s.Events = parsed.Events
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
func (p *Processor) handleShow(args []string, filterFn func(*spot.Spot) bool, dialect string, spotter string) string {
	if len(args) == 0 {
		return showDXUsage(dialect)
	}

	subCmd := args[0]

	switch subCmd {
	case "DX":
		commandLabel := "SHOW DX"
		if normalizeDialectString(dialect) == "cc" {
			commandLabel = "SHOW/DX"
		}
		return p.handleShowMYDX(args[1:], filterFn, commandLabel)
	case "MYDX":
		return p.handleShowMYDX(args[1:], filterFn, "SHOW MYDX")
	case "DXCC":
		return p.handleShowDXCC(args[1:])
	case "BUILD":
		if len(args) != 1 {
			return "Usage: SHOW BUILD\n"
		}
		return p.handleShowBuild()
	case "OWN":
		if len(args) != 1 {
			return "Usage: SHOW OWN\n"
		}
		return p.handleShowOwn(spotter)
	default:
		return fmt.Sprintf("Unknown SHOW subcommand: %s\n", subCmd)
	}
}

func normalizeBuildInfo(info BuildInfo) BuildInfo {
	info.Version = strings.TrimSpace(info.Version)
	info.Commit = strings.TrimSpace(info.Commit)
	info.BuildTime = strings.TrimSpace(info.BuildTime)
	info.VCSModified = strings.TrimSpace(info.VCSModified)
	info.GoVersion = strings.TrimSpace(info.GoVersion)
	if info.Version == "" {
		info.Version = "dev"
	}
	if info.Commit == "" {
		info.Commit = "unknown"
	}
	if info.BuildTime == "" {
		info.BuildTime = "unknown"
	}
	return info
}

func (p *Processor) handleShowBuild() string {
	info := normalizeBuildInfo(p.buildInfo)
	lines := []string{
		"Build version: " + info.Version,
	}
	if info.GoVersion != "" {
		lines = append(lines, "Go: "+info.GoVersion)
	}
	return strings.Join(lines, "\n") + "\n"
}

func (p *Processor) handleShowOwn(spotter string) string {
	loginCall := strings.TrimSpace(spotter)
	ownCall := spot.NormalizeOwnCallsign(loginCall)
	if ownCall == "" {
		return noLoggedUserMsg
	}
	return fmt.Sprintf("Own call: %s\nLogin call: %s\nSSID handling: numeric SSIDs are ignored for own-call features.\n", ownCall, loginCall)
}

// Purpose: Render recent stored spots filtered by client rules and optional DXCC selector.
// Key aspects: Archive-only history; outputs oldest-first; optional selector narrows by DX ADIF.
// Upstream: handleShow (SHOW MYDX).
// Downstream: archive.RecentFiltered.
func (p *Processor) handleShowMYDX(args []string, filterFn func(*spot.Spot) bool, commandLabel string) string {
	if filterFn == nil {
		return noLoggedUserMsg
	}

	request, errText := parseShowHistoryRequest(args, commandLabel)
	if errText != "" {
		return errText
	}

	matchFn := filterFn
	if request.selector != "" {
		_, _, info, lookupErr := p.lookupPortableDXCC(request.selector)
		if lookupErr != "" {
			return lookupErr
		}
		dxADIF := info.ADIF
		baseMatch := matchFn
		matchFn = func(s *spot.Spot) bool {
			if s == nil || s.DXMetadata.ADIF != dxADIF {
				return false
			}
			return baseMatch(s)
		}
	}

	var spots []*spot.Spot
	if p.archive == nil {
		return "No spots available.\n"
	}
	if rows, err := p.archive.RecentFiltered(request.count, matchFn); err != nil {
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
	queryRaw := strings.TrimSpace(args[0])
	if queryRaw == "" {
		return "Usage: SHOW DXCC <prefix|callsign>\n"
	}

	db, lookup, info, errText := p.lookupPortableDXCC(queryRaw)
	if errText != "" {
		return errText
	}

	prefix := strutil.NormalizeUpper(info.Prefix)
	country := strings.TrimSpace(info.Country)
	continent := strutil.NormalizeUpper(info.Continent)
	others := p.prefixIdx.siblings(db, info.ADIF, prefix)

	var b strings.Builder
	fmt.Fprintf(&b, "%s -> ADIF %d | %s (%s) | Prefix: %s | CQ %d | ITU %d",
		lookup, info.ADIF, country, continent, prefix, info.CQZone, info.ITUZone)
	if len(others) > 0 {
		b.WriteString(" | Other: ")
		b.WriteString(strings.Join(others, ", "))
	}
	b.WriteByte('\n')
	return b.String()
}

func (p *Processor) handleWhoSpotsMe(args []string, spotter string) string {
	if len(args) > 1 {
		return whoSpotsMeUsage()
	}
	call := spot.NormalizeOwnCallsign(strings.TrimSpace(spotter))
	if call == "" {
		return noLoggedUserMsg
	}
	band := ""
	if len(args) == 1 {
		band = spot.NormalizeBand(strings.TrimSpace(args[0]))
		if band == "" || band == "???" || !spot.IsValidBand(band) {
			return whoSpotsMeUsage()
		}
	}
	if p.whoSpotsMe == nil {
		return "WHOSPOTSME is not available.\n"
	}

	now := time.Now().UTC()
	windowMinutes := p.whoSpotsMeWindowMinutes()
	db := p.whoSpotsMeDB()

	if band != "" {
		counts := p.whoSpotsMe.CountryCountsByContinent(call, band, now)
		return p.renderWhoSpotsMeBand(db, band, counts, windowMinutes)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "WHOSPOTSME")
	p.writeWhoSpotsMeWindow(&b, windowMinutes)
	b.WriteString(":\n")
	bandsRendered := 0
	for _, band := range spot.SupportedBandNames() {
		counts := p.whoSpotsMe.CountryCountsByContinent(call, band, now)
		if !p.hasWhoSpotsMeCounts(db, counts) {
			continue
		}
		if bandsRendered > 0 {
			b.WriteByte('\n')
		}
		p.writeWhoSpotsMeBand(&b, db, band, counts)
		bandsRendered++
	}
	if bandsRendered == 0 {
		var empty strings.Builder
		fmt.Fprintf(&empty, "WHOSPOTSME")
		p.writeWhoSpotsMeWindow(&empty, windowMinutes)
		empty.WriteString(": no data\n")
		return empty.String()
	}
	return b.String()
}

func (p *Processor) whoSpotsMeWindowMinutes() int {
	windowMinutes := p.whoSpotsMeHelp.WindowMinutes
	if windowMinutes <= 0 {
		if window := p.whoSpotsMe.Window(); window > 0 {
			windowMinutes = int(window / time.Minute)
		}
	}
	return windowMinutes
}

func (p *Processor) whoSpotsMeDB() *cty.CTYDatabase {
	if p.ctyLookup != nil {
		return p.ctyLookup()
	}
	return nil
}

func (p *Processor) renderWhoSpotsMeBand(db *cty.CTYDatabase, band string, counts map[string][]spot.WhoSpotsMeCountryCount, windowMinutes int) string {
	if !p.hasWhoSpotsMeCounts(db, counts) {
		var empty strings.Builder
		fmt.Fprintf(&empty, "WHOSPOTSME %s", strings.ToUpper(band))
		p.writeWhoSpotsMeWindow(&empty, windowMinutes)
		empty.WriteString(": no data\n")
		return empty.String()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "WHOSPOTSME %s", strings.ToUpper(band))
	p.writeWhoSpotsMeWindow(&b, windowMinutes)
	b.WriteString(":\n")
	p.writeWhoSpotsMeRows(&b, db, counts)
	return b.String()
}

func (p *Processor) writeWhoSpotsMeBand(b *strings.Builder, db *cty.CTYDatabase, band string, counts map[string][]spot.WhoSpotsMeCountryCount) {
	fmt.Fprintf(b, "%s:\n", strings.ToUpper(band))
	p.writeWhoSpotsMeRows(b, db, counts)
}

func (p *Processor) writeWhoSpotsMeRows(b *strings.Builder, db *cty.CTYDatabase, counts map[string][]spot.WhoSpotsMeCountryCount) {
	for _, continent := range filter.SupportedContinents {
		display := p.formatWhoSpotsMeCountries(db, counts[continent])
		if len(display) == 0 {
			continue
		}
		b.WriteString("  ")
		b.WriteString(continent)
		b.WriteString(":  ")
		for i, item := range display {
			if i > 0 {
				b.WriteByte(' ')
			}
			fmt.Fprintf(b, "%s(%d)", item.label, item.count)
		}
		b.WriteByte('\n')
	}
}

func (p *Processor) hasWhoSpotsMeCounts(db *cty.CTYDatabase, counts map[string][]spot.WhoSpotsMeCountryCount) bool {
	for _, continent := range filter.SupportedContinents {
		if len(p.formatWhoSpotsMeCountries(db, counts[continent])) > 0 {
			return true
		}
	}
	return false
}

func (p *Processor) writeWhoSpotsMeWindow(b *strings.Builder, windowMinutes int) {
	if windowMinutes > 0 {
		fmt.Fprintf(b, " (last %dm)", windowMinutes)
	}
}

type whoSpotsMeDisplayCountry struct {
	label string
	count int
	adif  int
}

func (p *Processor) formatWhoSpotsMeCountries(db *cty.CTYDatabase, counts []spot.WhoSpotsMeCountryCount) []whoSpotsMeDisplayCountry {
	if len(counts) == 0 {
		return nil
	}
	display := make([]whoSpotsMeDisplayCountry, 0, len(counts))
	for _, item := range counts {
		if item.Count <= 0 {
			continue
		}
		display = append(display, whoSpotsMeDisplayCountry{
			label: p.whoSpotsMeCountryLabel(db, item.ADIF),
			count: item.Count,
			adif:  item.ADIF,
		})
	}
	sort.Slice(display, func(i, j int) bool {
		if display[i].count != display[j].count {
			return display[i].count > display[j].count
		}
		if display[i].label != display[j].label {
			return display[i].label < display[j].label
		}
		return display[i].adif < display[j].adif
	})
	if len(display) > 5 {
		display = display[:5]
	}
	return display
}

func (p *Processor) whoSpotsMeCountryLabel(db *cty.CTYDatabase, adif int) string {
	if adif <= 0 {
		return "ADIF0"
	}
	if prefix := p.prefixIdx.primary(db, adif); prefix != "" {
		return prefix
	}
	return fmt.Sprintf("ADIF%d", adif)
}

func (p *Processor) lookupPortableDXCC(queryRaw string) (*cty.CTYDatabase, string, *cty.PrefixInfo, string) {
	if p.ctyLookup == nil {
		return nil, "", nil, "CTY database is not available.\n"
	}
	db := p.ctyLookup()
	if db == nil {
		return nil, "", nil, "CTY database is not loaded.\n"
	}
	lookup := spot.NormalizeCallsign(strings.TrimSpace(queryRaw))
	if lookup == "" {
		return nil, "", nil, "Unknown DXCC/prefix.\n"
	}
	info, ok := db.LookupCallsignPortable(lookup)
	if !ok || info == nil {
		return nil, "", nil, "Unknown DXCC/prefix.\n"
	}
	return db, lookup, info, ""
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

	current = strutil.NormalizeUpper(current)
	out := make([]string, 0, len(prefixes))
	for _, pref := range prefixes {
		if pref == "" || pref == current {
			continue
		}
		out = append(out, pref)
	}
	return out
}

func (p *prefixIndex) primary(db *cty.CTYDatabase, adif int) string {
	if p == nil || db == nil || adif <= 0 {
		return ""
	}
	p.mu.Lock()
	if p.db != db {
		p.adifToPrefixes = buildPrefixMap(db)
		p.db = db
	}
	prefixes := p.adifToPrefixes[adif]
	p.mu.Unlock()
	if len(prefixes) == 0 {
		return ""
	}
	return prefixes[0]
}

func whoSpotsMeHelpNotes(cfg WhoSpotsMeHelpConfig) []string {
	windowNote := "Uses the configured rolling window of accepted observations."
	if cfg.Configured && cfg.WindowMinutes > 0 {
		windowNote = fmt.Sprintf("Uses the last %d minutes of accepted observations.", cfg.WindowMinutes)
	}
	return []string{
		windowNote,
		"Omit band to show bands with rolling-window data.",
		"Counts accepted observations before secondary broadcast dedupe.",
		"Counts are totals, not unique spotters.",
		"Shows up to 5 DXCC prefixes per continent for your baseline call.",
	}
}

func buildPrefixMap(db *cty.CTYDatabase) map[int][]string {
	if db == nil {
		return nil
	}
	tmp := make(map[int]map[string]struct{}, len(db.Data))
	for _, info := range db.Data {
		pref := strutil.NormalizeUpper(info.Prefix)
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
