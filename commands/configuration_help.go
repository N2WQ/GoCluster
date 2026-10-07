// File role: Documents human configuration readbacks and framed YAML commands.
// Execution remains in telnet; HELP uses the existing bounded-width formatter.
package commands

import "strings"

func installConfigurationHelp(catalog *helpCatalog, dialect string) {
	filterTopic := "SHOW FILTER"
	if dialect == "cc" {
		filterTopic = "SHOW/FILTER"
	}
	filterNotes := []string{
		"The overview wraps finite selections and useful explicit exclusions.",
		"Long callsign, DXCC, grid and zone lists use counts.",
		"DXCC entries show all unambiguous canonical CTY prefixes per entity.",
		"Conflicting labels are omitted; valid alternatives remain.",
		"FULL/category show effective PASS/REJECT selections, using ALL or NONE.",
		"Unresolved rules show Unknown DXCC followed by their stored number.",
		"Human lines use at most 78 ASCII characters; exact values use escapes.",
		"Switches show ON/OFF; REJECT takes precedence over PASS patterns.",
		"GET YAML FILTER preserves stored flags, false entries and numeric ADIF keys.",
		"DESTATE/DXSTATE show FCC mailing-address codes; empty means unknown.",
		"Preset (modified) means preferences differ from the applied snapshot.",
		"These readbacks always pause live spots, even if automatic pause is disabled.",
		"The reading interval starts after server delivery; RESUME ends it now.",
		"Responses are complete within 65,536 bytes or return an explicit size error.",
	}
	catalog.entries[filterTopic] = helpEntry{
		summary: filterTopic + " - Display filters and selections.",
		lines: helpEntryLines(filterTopic+" - Display current filter state.",
			[]string{filterTopic, filterTopic + " FULL", filterTopic + " <category>"}, nil, filterNotes),
	}
	if dialect == "cc" {
		catalog.entries["SH/FILTER"] = helpEntry{summary: "SH/FILTER - Alias of SHOW/FILTER.", lines: helpEntryLines("SH/FILTER - Display current filter state.", []string{"SH/FILTER [FULL|category]"}, []string{"SHOW/FILTER", "SHOW FILTER"}, filterNotes)}
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
	for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"} {
		topic := "GET YAML " + resource
		notes := []string{
			"Returns one framed YAML document; machine commands never change pause state.",
			"Optional ID uses 1-32 ASCII letters, digits or hyphens and preserves case.",
			"Responses include schema version, request ID and an opaque revision.",
			"Default schema 1 is unchanged. SCHEMA 2 includes DESTATE/DXSTATE rules.",
			"The final CRLF response is limited to 65,536 bytes, including framing.",
		}
		if resource != "CAPABILITIES" {
			notes = append(notes, "Edit the writable configuration section; status is read-only.")
		}
		add(topic, "Read "+strings.ToLower(resource)+" for clients.", []string{topic + " [SCHEMA 2] [ID <id>]"}, notes)
	}
	for _, verb := range []string{"PUT", "PATCH"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG"} {
			topic := verb + " YAML " + resource
			notes := []string{
				"Send standalone --- and ... marker lines around one plain YAML document.",
				"Include schema_version: 1 or 2, request_id, if_revision and configuration.",
				"GET again after reconnect or a revision conflict before retrying a write.",
				"Validation or persistence failure leaves live and saved configuration unchanged.",
				"Unavailable choices are rejected; named preset baselines are preserved.",
				"Body limit: 65,536 bytes; total upload deadline: 30 seconds.",
				"Oversized, expired or unreliable framing closes the connection.",
				"A complete invalid document gets a YAML error and keeps the connection open.",
				"Schema 1 preserves hidden state rules and bounds its CONFIG projection.",
				"Schema 2 includes state rules; full CONFIG must fit 65,536 bytes.",
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
			notes = append(notes, "A valid large preset may LOAD even when FULL or YAML readback exceeds 65,536 bytes.")
		}
		entry.lines = appendNotes(entry.lines, notes)
		catalog.entries[topic] = entry
	}
}
