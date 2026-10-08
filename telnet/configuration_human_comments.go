// File role: Shows exact bounded COMMENT phrases without interpreting literal
// punctuation as wildcards or display sentinels. Every phrase is quoted so
// leading spaces, repeated spaces, ALL/NONE and duplicate entries stay visible.
package telnet

import (
	"fmt"

	"dxcluster/filter"
)

func writeHumanComments(h *humanResponse, allow, block []string, overview bool) error {
	cfg := filter.Configuration{Filters: filter.FilterConfiguration{Comments: allow, BlockComments: block}}
	if err := cfg.ValidateCommentRules(); err != nil {
		return err
	}
	if overview {
		if len(allow) == 0 && len(block) == 0 {
			return h.row("Comments", "All")
		}
		return h.row("Comments", fmt.Sprintf("%d PASS phrases; %d REJECT phrases", len(allow), len(block)))
	}
	if err := h.line("Comments"); err != nil {
		return err
	}
	if err := h.line("  Case-insensitive literal substring; REJECT takes precedence."); err != nil {
		return err
	}
	if err := h.line("  Limits: 32 phrases per list; 1-64 printable ASCII bytes per phrase."); err != nil {
		return err
	}
	for i, values := range [][]string{allow, block} {
		label := "PASS"
		empty := "ALL (no comment allowlist)"
		if i == 1 {
			label, empty = "REJECT", "NONE"
		}
		if len(values) == 0 {
			if err := h.line("  " + label + ": " + empty); err != nil {
				return err
			}
			continue
		}
		if err := h.line(fmt.Sprintf("  %s: %d phrases", label, len(values))); err != nil {
			return err
		}
		for _, value := range values {
			if err := h.quoted("    ", value, ""); err != nil {
				return err
			}
		}
	}
	return nil
}
