// File role: Describes exact configured DEDUPE separately from its effective lane.
// Callers hold the existing full-SSID lease. No setting, revision or file is
// changed; bounded counting and quoting admit the complete response first.
package telnet

import "fmt"

// sendCommandResponse may append its existing footer after this formatter.
// Reserve CRLF bytes for the longest signed integer seconds and for at most
// 65,536 rows. This bounds the final message without extending a pause here.
const dedupeReadPauseFooterBytes = len("Live spots paused for -9223372036854775808s after 65536 output rows. Type RESUME to resume now.\r\nMissed spots are not replayed.\r\n")

func (s *Server) formatDedupeStatus(client *Client) string {
	if client == nil || s == nil {
		return ""
	}
	configured := client.getDedupePolicy().label()
	if client.configurationInitialized {
		configured = client.configuredSettings.DedupePolicy
	}
	// Marked records can contain a long exact string. Refuse before token
	// normalization or quoting can allocate an intermediate of that size.
	if len(configured) > maxYAMLBytes {
		return "Readback failed: response exceeds 65,536 bytes.\r\n"
	}
	effective := s.effectiveDedupePolicyForClient(client)
	nearby := client.nearbyFilterInEffect()
	enabled := s.dedupeFastEnabled || s.dedupeMedEnabled || s.dedupeSlowEnabled
	onOff := func(value bool) string {
		if value {
			return "on"
		}
		return "off"
	}
	availability := fmt.Sprintf("(fast=%s med=%s slow=%s)", onOff(s.dedupeFastEnabled), onOff(s.dedupeMedEnabled), onOff(s.dedupeSlowEnabled))
	for _, countOnly := range []bool{true, false} {
		h := humanResponse{countOnly: countOnly}
		if err := writeDedupeStatus(&h, configured, effective, nearby, enabled, s.dedupeFastEnabled, availability); err != nil {
			return "Readback failed: response exceeds 65,536 bytes.\r\n"
		}
		if !countOnly {
			response := string(h.data)
			if s.autoReadPauseEnabled() && len(response) > maxYAMLBytes-dedupeReadPauseFooterBytes && renderedOutputRows(response) >= s.autoReadPauseMinRows {
				return "Readback failed: response exceeds 65,536 bytes.\r\n"
			}
			return response
		}
	}
	return ""
}

func writeDedupeStatus(h *humanResponse, configured string, effective dedupePolicy, nearby, enabled, fastEnabled bool, availability string) error {
	choice := configured
	if choice == "" {
		choice = "DEFAULT"
	}
	requested, known := parseDedupePolicyToken(configured)
	suffix := "; secondary duplicate suppression disabled"
	compact := enabled && known && requested == effective
	if compact {
		suffix = fmt.Sprintf(" (%s) %s", dedupeKeyLabel(effective), availability)
	} else if enabled {
		prefix := "; effective "
		if nearby {
			prefix = "; NEARBY effective "
		}
		suffix = fmt.Sprintf("%s%s (%s)", prefix, effective.label(), dedupeKeyLabel(effective))
	}
	if simpleHumanValue(choice) && len(choice)+len(suffix)+8 <= humanReadbackWidth {
		if err := h.line("Dedupe: " + choice + suffix); err != nil {
			return err
		}
	} else if err := h.quoted("Dedupe: ", choice, suffix); err != nil {
		return err
	}
	if !enabled || compact {
		return nil
	}
	if err := h.line("        " + availability); err != nil {
		return err
	}
	if configured != "" && !known {
		if err := h.line("        Configured choice is unrecognized"); err != nil {
			return err
		}
	} else if configured != "" && !nearby {
		return h.line("        Configured choice is unavailable on this server")
	}
	if nearby && !fastEnabled {
		return h.line("        FAST is unavailable on this server")
	}
	return nil
}
