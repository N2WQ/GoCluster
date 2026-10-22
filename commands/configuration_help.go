// File role: Documents human configuration readbacks and framed YAML commands.
// Execution remains in telnet; HELP uses the existing bounded-width formatter.
package commands

import "strings"

func installConfigurationHelp(catalog *helpCatalog, dialect string) {
	filterTopic := "SHOW FILTER"
	filterAliases := []string{"SHOW/FILTER", "SH/FILTER"}
	if dialect == "cc" {
		filterTopic = "SHOW/FILTER"
		filterAliases = []string{"SHOW FILTER", "SH/FILTER"}
	}
	filterNotes := []string{
		"Shows numbered saved rules, effective NEARBY expressions and separate controls.",
		"RULE and FULL request the same complete bounded filter state.",
		"PASS rules are ORed; REJECT vetoes them. Slot IDs are not priorities.",
		"No rules means no personal restriction. Default slot 0 is editable.",
		"NEARBY omits geography leaves only; emptied rules are ignored and full saved rules stay intact.",
		"Human lines use at most 78 ASCII characters; exact values use escapes.",
		"Display quotes/escapes are not expression syntax; joined + pieces concatenate exactly.",
		"Separate controls show ON/OFF or DEFAULT; RESET FILTER does not reset them.",
		"Use GET YAML FILTER SCHEMA 5 for machine-readable rule expressions.",
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
			"Explicit SCHEMA 5 is required; omitted and older schemas are refused.",
			"Schema 5 carries numbered rules and separate controls, not category maps.",
			"No old machine-schema migration or command translation is provided.",
			"The final CRLF response is limited to 65,536 bytes, including framing.",
		}
		if resource != "CAPABILITIES" {
			notes = append(notes, "Edit the writable configuration section; status is read-only.")
		}
		add(topic, "Read "+strings.ToLower(resource)+" for clients.", []string{topic + " SCHEMA 5 [ID <id>]"}, notes)
	}
	for _, verb := range []string{"PUT", "PATCH"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG"} {
			topic := verb + " YAML " + resource
			notes := []string{
				"Send standalone --- and ... marker lines around one plain YAML document.",
				"Include schema_version: 5, request_id, if_revision and configuration.",
				"GET again after reconnect or a revision conflict before retrying a write.",
				"Validation or persistence failure leaves live and saved configuration unchanged.",
				"Unavailable choices are rejected; named preset baselines are preserved.",
				"Body limit: 65,536 bytes; total upload deadline: 30 seconds.",
				"Oversized, expired or unreliable framing closes the connection.",
				"A complete invalid document gets a YAML error and keeps the connection open.",
				"Schema 5 uses numbered rules; legacy category fields and older schemas are refused.",
				"Supplied rules replace the complete rule list; [] clears it.",
				"PATCH omission preserves rules. No old machine-schema migration is provided.",
				"No aliases, anchors, merge keys, custom tags, nulls or extra documents.",
			}
			if verb == "PUT" {
				notes = append(notes, "PUT requires every writable field of the resource; missing fields are errors.")
			} else {
				notes = append(notes, "PATCH preserves omitted fields. Supplied lists and maps replace those collections.")
			}
			add(topic, "Write "+strings.ToLower(resource)+" for clients.", []string{topic}, notes)
		}
	}
	add("VALIDATE YAML CONFIG", "Check a complete client proposal.", []string{"VALIDATE YAML CONFIG"}, []string{
		"Uses the same framed upload and plain YAML rules as PUT.",
		"Include schema_version, request_id and the complete configuration.",
		"Checks choices and complete CONFIG response size without applying or saving.",
		"Does not change revisions, pause, diagnostics or scheduling.",
	})
	for _, topic := range []string{"SAVE PRESET", "LOAD PRESET"} {
		entry := catalog.entries[topic]
		notes := []string{"Success establishes the named applied snapshot as the preset baseline.", "Ordinary preference changes preserve the baseline; reversing them clears modified."}
		if topic == "SAVE PRESET" {
			notes = append(notes, "Temporary-defaults sessions cannot SAVE; the preset library stays unchanged.", "If snapshot save succeeds but association save fails, the old association is retained.")
		} else {
			notes = append(notes, "Rules and separate controls use the same bounded validation as direct edits.")
		}
		entry.lines = appendNotes(entry.lines, notes)
		catalog.entries[topic] = entry
	}
}
