// Native CC HELP describes implemented commands and their qualified extensions.
package commands

import (
	"dxcluster/filter"
	"dxcluster/spot"
)

func ccFilterHelpLines(verb string) []string {
	usage := []string{verb + " <switch> [ALL]"}
	notes := []string{
		"Switches accept no value or ALL. PC93 aliases ANNOUNCE.",
		"SELF controls spots of your own call in GoCluster, not other users' self-spots.",
		"OWN, BOB, DX/NODX, TALK, WX and announcement/weather geography are unavailable.",
	}
	if verb == "SET/FILTER" {
		usage = append(usage,
			"SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/<PASS|REJECT> <list>",
			"SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/OFF",
			"SET/FILTER <country-list>/PASS or /REJECT",
			"SET/FILTER DXBM/PASS <band-or-slot-list>",
			"SET/FILTER DXBM/REJECT <band-or-slot-list>", "SET/FILTER DXBM/OFF")
		notes = append(notes,
			"DOC/DOS select spotter country/state; DXCTY/DXSTATE select the DX country/state.",
			"Lists accept commas or spaces. Countries use exact canonical CTY labels or positive ADIF integers; states/provinces use the existing US/Canadian codes.",
			"Each geography edit replaces that category's PASS/REJECT list. Global edits clear its band exceptions. Other categories remain unchanged.",
			"Values in one list use OR; enabled categories use AND. Missing/invalid metadata fails PASS and survives REJECT.",
			"OFF disables only that category. SET/NOFILTER restores current operator mode/source defaults and clears geography/DXBM, preserving six switches.",
			"DXBM names frequency slots, regardless of decoded mode; decoded-mode selection also applies. First PASS after OFF starts a whitelist; first REJECT starts a blacklist. Later edits keep other slots.",
			"The higher slot owns shared endpoints; each band's final upper endpoint is included.",
			"SET/FILTER <switch>/ON or /OFF changes a listed switch, without extra arguments.",
			"Per-band geography is a GoCluster schema-seven YAML extension. Its terminal grammar and CC User integration are unverified.",
			"The configured command-line limit still applies. Use YAML for long replacing lists; do not split them across replacing commands.",
			"Examples: SET/FILTER DOC/PASS K,VE; SET/FILTER DXBM/PASS 20-CW,40; SET/FILTER BEACON/OFF.")
	} else {
		notes = append(notes,
			"UNSET/FILTER does not accept inline /ON or /OFF or geography lists.",
			"Exact UNSET/<mode>, UNSET/SKIMMER and UNSET/<switch> shortcuts disable their selection and accept no arguments.",
			"Use SET/FILTER category/PASS, /REJECT or /OFF for native categories.")
	}
	lines := helpEntryLines(verb+" - Change native CC selections or switches.", usage, nil, notes)
	lines = appendListSection(lines, "Switches:", []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"})
	if verb == "SET/FILTER" {
		lines = appendListSection(lines, "Native bands:", filter.CCSupportedBands())
		nativeSlots := filter.CCSupportedSlots()
		slots := make([]string, 0, len(nativeSlots))
		for _, slot := range nativeSlots {
			slots = append(slots, slot.Name)
		}
		lines = appendListSection(lines, "Native slots:", slots)
		lines = appendListSection(lines, "State/province codes:", spot.StateCodes())
	}
	return lines
}

func nativeCCShortcutHelpLines(topic string) []string {
	notes := []string{"No additional arguments are accepted. Use SHOW/FILTER to inspect the result."}
	switch topic {
	case "SET/NOFILTER":
		notes = append(notes, "Restores current operator mode/source defaults and clears geography, band exceptions and DXBM. Keeps six switches, inactive GO bank and shared preferences.")
	case "SET/SKIMMER", "SET/NOSKIMMER", "UNSET/SKIMMER":
		notes = append(notes, "Changes only the SKIMMER source selection; modes stay unchanged and no feed is activated.")
	default:
		notes = append(notes,
			"Only modes with an enabled operator cc_shortcut flag are accepted; configured aliases normalize to their canonical mode.",
			"The mode selection applies to HUMAN and SKIMMER. Source selection is unchanged.",
			"SET/<MODE> enables; SET/NO<MODE> or UNSET/<MODE> disables. Empty mode/source selections admit no reports.")
	}
	return helpEntryLines(topic+" - Change a native CC selection.", []string{topic}, nil, notes)
}

func installNativeCCHelpBoundary(catalog *helpCatalog) {
	for _, name := range []string{"ANN", "ANNOUNCE", "BEACON", "WWV", "WCY", "SELF", "TOXIC"} {
		for _, prefix := range []string{"SET/", "SET/NO", "UNSET/"} {
			topic := prefix + name
			if _, exists := catalog.entries[topic]; exists {
				continue
			}
			catalog.entries[topic] = helpEntry{summary: topic + " - Change a separate switch.", lines: helpEntryLines(topic+" - Change a separate switch.", []string{topic}, nil, []string{"SET enables; SET/NO and UNSET disable the named switch.", "No additional arguments are accepted. Use SHOW/FILTER to inspect the result.", "SELF means spots of your own call in GoCluster."})}
		}
	}
	for _, topic := range []string{"RULE", "PASS RULE", "REJECT RULE", "RESET FILTER", "RESET FILTER RULE", "NEARBY", "PASS COMMENT", "REJECT COMMENT", "REMOVE PASS COMMENT", "REMOVE REJECT COMMENT", "RESET FILTER COMMENT", "SHOW FILTER COMMENT", "PASS MINSNR", "REJECT MINSNR"} {
		catalog.entries[topic] = helpEntry{summary: topic + " - GO dialect only.", lines: helpEntryLines(topic+" - GO dialect only.", []string{"DIALECT GO"}, nil, []string{
			"CC uses native mode/source, geography and DXBM selections; Boolean mutations are refused here.",
			"Switching banks preserves the other bank. Use HELP SET/FILTER or HELP FILTERS for native CC commands.",
		})}
	}
	for _, verb := range []string{"PASS", "REJECT"} {
		catalog.entries[verb] = helpEntry{summary: verb + " - Change separate delivery switches.", lines: helpEntryLines(verb+" - Change separate delivery switches.", []string{verb + " <BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC> [ALL]"}, nil, []string{
			"PASS enables; REJECT disables. PC93 aliases ANNOUNCE. Switches accept no value or ALL.",
			"SELF controls spots of your own call. Native geography uses SET/FILTER category/PASS or /REJECT.",
			"Boolean rule edits require GO; HELP SET/FILTER describes native CC selection.",
		})}
	}
	for _, topic := range []string{"SHOW/FILTER", "SH/FILTER"} {
		aliases := []string{"SHOW FILTER", "SH/FILTER"}
		if topic == "SH/FILTER" {
			aliases = []string{"SHOW/FILTER", "SHOW FILTER"}
		}
		catalog.entries[topic] = helpEntry{summary: topic + " - Inspect native CC state.", lines: helpEntryLines(topic+" - Inspect native CC state.", []string{topic + " [DOC|DOS|DXCTY|DXSTATE|DXBM|MODE|SOURCE|FULL]"}, aliases, []string{
			"Shows exact native selections and six controls. Band INHERIT differs from explicit OFF; DXBM OFF differs from active allow-all.",
			"Readbacks pause live spots through delivery and reading. RESUME ends the pause; missed spots are not replayed.",
			"GET YAML CONFIG SCHEMA 7 provides a complete both-bank backup. Schema 6 does not.",
		})}
	}
}

func nativeCCFilterReferenceLines() []string {
	return append([]string{"Native CC filter reference:", "  HELP SET/FILTER describes geography, frequency slots and six switches.", "  HELP SET/NOFILTER describes current operator defaults.", "  Boolean edits require DIALECT GO; switching preserves the other bank.", ""}, ccFilterHelpLines("SET/FILTER")...)
}
