// File role: Documents human configuration readbacks and framed YAML commands.
// Execution remains in telnet; HELP uses the existing bounded-width formatter.
package commands

import (
	"slices"
	"strings"
)

func installConfigurationHelp(catalog *helpCatalog, dialect string) {
	filterTopic := "SHOW FILTER"
	filterAliases := []string{"SHOW/FILTER", "SH/FILTER"}
	if dialect == "cc" {
		filterTopic = "SHOW/FILTER"
		filterAliases = []string{"SHOW FILTER", "SH/FILTER"}
	}
	filterNotes := []string{
		"Shows canonical numbered rules and separate controls.",
		"RULE matches SHOW FILTER; FULL shows canonical rules, controls and program counts.",
		"PASS rules are ORed; REJECT vetoes them. Slot IDs are not priorities.",
		"No rules means no personal restriction. Default slot 0 is editable.",
		"NEARBY is an ordinary predicate; geography and other conditions retain their meaning.",
		"Human lines use at most 78 ASCII characters; exact values use escapes.",
		"Display quotes/escapes are not expression syntax; joined + pieces concatenate exactly.",
		"Separate controls show ON/OFF or DEFAULT; RESET FILTER does not reset them.",
		"GET YAML FILTER SCHEMA 6 is the active GO legacy view; schema 7 uses a tagged active bank.",
		"GET YAML CONFIG SCHEMA 7 is the complete both-bank backup; schema 6 is not.",
		"Preset (modified) means preferences differ from the applied snapshot.",
		"These readbacks always pause live spots, even if automatic pause is disabled.",
		"The reading interval starts after server delivery; RESUME ends it now.",
		"Responses are complete within 65,536 bytes or return an explicit size error.",
	}
	catalog.entries[filterTopic] = helpEntry{
		summary: filterTopic + " - Display rules and separate controls.",
		lines: helpEntryLines(filterTopic+" - Display current filter state.",
			[]string{filterTopic, filterTopic + " RULE", filterTopic + " FULL"}, filterAliases, filterNotes),
	}
	if dialect == "cc" {
		catalog.entries["SH/FILTER"] = helpEntry{summary: "SH/FILTER - Alias of SHOW/FILTER.", lines: helpEntryLines("SH/FILTER - Display current filter state.", []string{"SH/FILTER [RULE|FULL]"}, []string{"SHOW/FILTER", "SHOW FILTER"}, filterNotes)}
	}
	add := func(topic, summary string, usage, notes []string) {
		catalog.entries[topic] = helpEntry{summary: topic + " - " + summary, lines: helpEntryLines(topic+" - "+summary, usage, nil, notes)}
		catalog.order = append(catalog.order, topic)
	}
	add("SHOW SETTINGS", "Display preferences and session behavior.", []string{"SHOW SETTINGS"}, []string{
		"Shows configured settings separately from effective choices and session state.",
		"Always pauses live spots during preparation/delivery and for reading afterward.",
		"Uses the positive configured pause duration, or 30 seconds when it is zero.",
		"The complete response, including its pause footer, is limited to 65,536 bytes.",
	})
	settingsEntry := catalog.entries["SHOW SETTINGS"]
	settingsEntry.lines = slices.Insert(settingsEntry.lines, 2, "Aliases: SHOW/SETTINGS, SH/SETTINGS")
	catalog.entries["SHOW SETTINGS"] = settingsEntry
	for _, verb := range []string{"PASS", "REJECT"} {
		topic := verb + " MINSNR"
		catalog.entries[topic] = helpEntry{
			summary: topic + " - Replaced by Boolean SNR expressions.",
			lines: helpEntryLines(topic+" - Replaced by Boolean SNR expressions.", []string{"HELP RULE"}, nil, []string{
				"MINSNR category commands are refused, not translated.",
				"Use SNR comparisons in numbered rules. Reports from human spots also count.",
				"To retain the former human exemption, include SOURCE=SKIMMER in the rule.",
				"Example: REJECT RULE 9 SOURCE=SKIMMER AND MODE=CW AND SNR<10",
				"Missing SNR makes comparisons false; NOT reverses that result.",
			}),
		}
	}
	for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"} {
		topic := "GET YAML " + resource
		notes := []string{
			"Returns one framed YAML document; machine commands never change pause state.",
			"Optional ID uses 1-32 ASCII letters, digits or hyphens and preserves case.",
			"Responses include schema version, request ID and an opaque revision.",
			"Choose explicit SCHEMA 6 or SCHEMA 7. Schema 6 is a legacy view, not a complete backup.",
			"Schema 7 FILTER describes the tagged active bank; CONFIG includes both banks, controls, shared settings and selection.",
			"Schema 6 FILTER/CONFIG is available only with GO active. SETTINGS and CAPABILITIES work in either dialect.",
			"GET YAML CONFIG SCHEMA 7 provides a complete both-bank backup.",
			"Final framed limits: schema 6 is 65,536 bytes; schema 7 is 262,144 bytes.",
		}
		if resource != "CAPABILITIES" {
			notes = append(notes, "Edit the writable configuration section; status is read-only.")
		}
		add(topic, "Read "+strings.ToLower(resource)+" for clients.", []string{topic + " SCHEMA 6 [ID <id>]", topic + " SCHEMA 7 [ID <id>]"}, notes)
	}
	for _, verb := range []string{"PUT", "PATCH"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG"} {
			topic := verb + " YAML " + resource
			notes := []string{
				"Send standalone --- and ... marker lines around one plain YAML document.",
				"Legacy three-token upload headers select schema 6 and its 65,536-byte physical limit.",
				"New uploads require SCHEMA 7 in the header and matching schema_version: 7 in the body. Include request_id, if_revision and configuration.",
				"GET again after reconnect or a revision conflict before retrying a write.",
				"Validation or persistence failure leaves live and saved configuration unchanged.",
				"Unavailable choices are rejected; named preset baselines are preserved.",
				"Schema-seven body limit: 262,144 bytes; total upload deadline: 30 seconds. Large transfers also use fixed process budgets.",
				"Oversized, expired or unreliable framing closes the connection.",
				"A complete invalid document gets a YAML error and keeps the connection open.",
				"Schema 6 FILTER/CONFIG writes require GO active; CONFIG must also leave GO selected. Schema 6 SETTINGS may switch either bank.",
				"Schema 7 FILTER edits only the tagged active bank. SETTINGS owns shared settings and selection; full CONFIG restores both banks.",
				"Ordinary PATCH filter edits cannot also change bank selection or inactive-bank filters. PUT CONFIG is the complete restore boundary.",
				"Supplied GO rules replace the list; [] clears it. Native empty mode/source lists admit no reports.",
				"Native global geography PATCH replaces its list and clears band exceptions. Bands-only edits change named rows; INHERIT removes one override. Do not combine global and bands in one category PATCH.",
				"DXBM PATCH supplies complete active/base/slot state. Mode/source lists are replacements, not incremental patches.",
				"No aliases, anchors, merge keys, custom tags, nulls or extra documents.",
			}
			if verb == "PUT" {
				notes = append(notes, "PUT requires every writable field of the resource; missing fields are errors.")
			} else {
				notes = append(notes, "PATCH preserves omitted fields. Supplied lists replace those selections; native band rows follow the geography rules above.")
			}
			add(topic, "Write "+strings.ToLower(resource)+" for clients.", []string{topic, topic + " SCHEMA 7"}, notes)
		}
	}
	add("VALIDATE YAML CONFIG", "Check a complete client proposal.", []string{"VALIDATE YAML CONFIG", "VALIDATE YAML CONFIG SCHEMA 7"}, []string{
		"Uses the same framed upload and plain YAML rules as PUT.",
		"Include schema_version, request_id and the complete configuration.",
		"The explicit SCHEMA 7 header must match body version 7; the legacy header selects version 6 and its smaller physical limit.",
		"Schema 6 validation requires effective GO. Schema 7 validates the complete both-bank restore.",
		"Checks choices and complete CONFIG response size without applying or saving.",
		"Does not change revisions, pause, diagnostics or scheduling.",
	})
	for _, topic := range []string{"SAVE PRESET", "LOAD PRESET"} {
		entry := catalog.entries[topic]
		notes := []string{"Presets contain one tagged filter bank, its six controls and shared preferences.", "Success establishes that bank's applied snapshot as its preset baseline.", "Ordinary preference changes preserve the baseline; reversing them clears modified."}
		if topic == "SAVE PRESET" {
			notes = append(notes, "Temporary-defaults sessions cannot SAVE; the preset library stays unchanged.", "If snapshot save succeeds but association save fails, the old association is retained.")
		} else {
			notes = append(notes, "LOAD selects the tagged dialect, announces it and preserves the other filter bank. It commits before publishing.", "Rules/native categories and separate controls use the same bounded validation as direct edits.")
		}
		entry.lines = appendNotes(entry.lines, notes)
		catalog.entries[topic] = entry
	}
}
