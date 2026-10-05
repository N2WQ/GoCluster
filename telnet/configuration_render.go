// File role: Renders exact configuration through a hard final CRLF byte budget.
// Preflight happens before map sorting or YAML's intermediate node allocation.
// Compact counts borrow collections without building detailed rule strings.
package telnet

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

type boundedResponse struct {
	data []byte
	err  error
}

// Write measures converted bytes before appending. Explicit capacity growth
// never retains a buffer larger than the final response limit.
func (b *boundedResponse) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	size := len(p)
	previousCR := len(b.data) != 0 && b.data[len(b.data)-1] == '\r'
	for _, value := range p {
		if value == '\n' && !previousCR {
			size++
		}
		previousCR = value == '\r'
	}
	if size > maxYAMLBytes-len(b.data) {
		b.err = errReadbackTooLarge
		return 0, b.err
	}
	if needed := len(b.data) + size; needed > cap(b.data) {
		capacity := min(maxYAMLBytes, max(needed, max(1024, cap(b.data)*2)))
		grown := make([]byte, len(b.data), capacity)
		copy(grown, b.data)
		b.data = grown
	}
	for _, value := range p {
		if value == '\n' && (len(b.data) == 0 || b.data[len(b.data)-1] != '\r') {
			b.data = append(b.data, '\r')
		}
		b.data = append(b.data, value)
	}
	return len(p), nil
}

func encodeBoundedYAML(document any) (string, error) {
	var response boundedResponse
	if _, err := response.Write([]byte("---\n")); err != nil {
		return "", err
	}
	encoder := yaml.NewEncoder(&response)
	encoder.SetIndent(2)
	if err := encoder.Encode(document); err != nil {
		_ = encoder.Close()
		if response.err != nil {
			return "", response.err
		}
		return "", err
	}
	if err := encoder.Close(); err != nil {
		if response.err != nil {
			return "", response.err
		}
		return "", err
	}
	if _, err := response.Write([]byte("...\n")); err != nil {
		return "", err
	}
	return string(response.data), nil
}

type readbackCategory struct {
	name         string
	rules        any
	allow, block []string
	toggle       filter.DefaultBool
	kind         byte
}

func readbackCategories(f filter.FilterConfiguration) [23]readbackCategory {
	return [23]readbackCategory{
		{name: "BAND", rules: f.Bands}, {name: "MODE", rules: f.Modes},
		{name: "SOURCE", rules: f.Sources}, {name: "EVENT", rules: f.Events},
		{name: "CONFIDENCE", rules: f.Confidence}, {name: "PATH", rules: f.PathClasses},
		{name: "DXCONT", rules: f.DXContinents}, {name: "DECONT", rules: f.DEContinents},
		{name: "DXZONE", rules: f.DXZones}, {name: "DEZONE", rules: f.DEZones},
		{name: "DXGRID2", rules: f.DXGrid2}, {name: "DEGRID2", rules: f.DEGrid2},
		{name: "DXDXCC", rules: f.DXDXCC}, {name: "DEDXCC", rules: f.DEDXCC},
		{name: "DXCALL", allow: f.DXCallsigns, block: f.BlockDXCallsigns, kind: 'p'},
		{name: "DECALL", allow: f.DECallsigns, block: f.BlockDECallsigns, kind: 'p'},
		{name: "BEACON", toggle: f.IncludeBeacons, kind: 't'}, {name: "WWV", toggle: f.AllowWWV, kind: 't'},
		{name: "WCY", toggle: f.AllowWCY, kind: 't'}, {name: "ANNOUNCE", toggle: f.AllowAnnounce, kind: 't'},
		{name: "SELF", toggle: f.AllowSelf, kind: 't'}, {name: "TOXIC", toggle: f.AllowToxic, kind: 't'},
		{name: "NEARBY", kind: 'n'},
	}
}

func categoryMinimumFits(category readbackCategory) bool {
	var f filter.FilterConfiguration
	switch rules := category.rules.(type) {
	case filter.StringRules:
		f.Bands = rules
	case filter.IntRules:
		f.DXZones = rules
	default:
		f.DXCallsigns, f.BlockDXCallsigns = category.allow, category.block
	}
	return (filter.Configuration{Filters: f}).MinimumSizeFits(maxYAMLBytes)
}

func enabledRules[K string | int](entries map[K]bool) int {
	count := 0
	for _, enabled := range entries {
		if enabled {
			count++
		}
	}
	return count
}

func writeExactRules[K string | int](b *boundedResponse, name string, rules filter.RuleSet[K]) error {
	if _, err := fmt.Fprintf(b, "%s:\n  allow_all: %t\n  block_all: %t\n", name, rules.AllowAll, rules.BlockAll); err != nil {
		return err
	}
	for i, entries := range []map[K]bool{rules.Allow, rules.Block} {
		label := "allow"
		if i == 1 {
			label = "block"
		}
		if _, err := fmt.Fprintf(b, "  %s: {", label); err != nil {
			return err
		}
		keys := make([]K, 0, len(entries))
		for key := range entries {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for index, key := range keys {
			if index > 0 {
				if _, err := b.Write([]byte(", ")); err != nil {
					return err
				}
			}
			var text string
			switch value := any(key).(type) {
			case string:
				text = strconv.Quote(value)
			case int:
				text = strconv.Itoa(value)
			}
			if _, err := fmt.Fprintf(b, "%s: %t", text, entries[key]); err != nil {
				return err
			}
		}
		if _, err := b.Write([]byte("}\n")); err != nil {
			return err
		}
	}
	return nil
}

func writePatternList(b *boundedResponse, label string, patterns []string) error {
	if _, err := fmt.Fprintf(b, "  %s: [", label); err != nil {
		return err
	}
	for index, pattern := range patterns {
		if index > 0 {
			if _, err := b.Write([]byte(", ")); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(b, "%q", pattern); err != nil {
			return err
		}
	}
	_, err := b.Write([]byte("]\n"))
	return err
}

func writeReadbackCategory(b *boundedResponse, category readbackCategory, compact bool, nearby bool, active bool) error {
	switch rules := category.rules.(type) {
	case filter.StringRules:
		if !compact {
			if err := writeExactRules(b, category.name, rules); err != nil {
				return err
			}
		} else {
			countLabel := "enabled"
			if category.name == "EVENT" {
				countLabel = "true"
			}
			if _, err := fmt.Fprintf(b, "%s: allow_all=%t block_all=%t; allow=%d/%d %s; block=%d/%d %s\n",
				category.name, rules.AllowAll, rules.BlockAll, enabledRules(rules.Allow), len(rules.Allow), countLabel, enabledRules(rules.Block), len(rules.Block), countLabel); err != nil {
				return err
			}
		}
		if category.name == "EVENT" {
			_, err := b.Write([]byte("  note: EVENT rules use key presence; false does not remove a rule.\n"))
			return err
		}
		return nil
	case filter.IntRules:
		if !compact {
			return writeExactRules(b, category.name, rules)
		}
		_, err := fmt.Fprintf(b, "%s: allow_all=%t block_all=%t; allow=%d/%d enabled; block=%d/%d enabled\n",
			category.name, rules.AllowAll, rules.BlockAll, enabledRules(rules.Allow), len(rules.Allow), enabledRules(rules.Block), len(rules.Block))
		return err
	}
	switch category.kind {
	case 'p':
		if compact {
			_, err := fmt.Fprintf(b, "%s: allow=%d patterns; block=%d patterns\n", category.name, len(category.allow), len(category.block))
			return err
		}
		if _, err := fmt.Fprintf(b, "%s:\n", category.name); err != nil {
			return err
		}
		if err := writePatternList(b, "allow", category.allow); err != nil {
			return err
		}
		return writePatternList(b, "block", category.block)
	case 't':
		selection := "DEFAULT (effective true)"
		switch category.toggle {
		case filter.DefaultBoolFalse:
			selection = "false"
		case filter.DefaultBoolTrue:
			selection = "true"
		}
		_, err := fmt.Fprintf(b, "%s: %s\n", category.name, selection)
		return err
	default:
		_, err := fmt.Fprintf(b, "NEARBY: %t (effective active=%t; location rules suspended only while active)\n", nearby, active)
		return err
	}
}

func humanReadbackFooter(duration time.Duration) string {
	if duration <= 0 {
		duration = defaultReadPauseDuration
	}
	return fmt.Sprintf("Live spots paused during delivery and for at least %ds after delivery. Type RESUME to resume now.\nMissed spots are not replayed.\n", durationCeilSeconds(duration))
}

func humanReadbackError(err error, duration time.Duration) string {
	var b boundedResponse
	if _, writeErr := fmt.Fprintf(&b, "Readback failed: %v\n%s", err, humanReadbackFooter(duration)); writeErr == nil {
		return string(b.data)
	}
	return "Readback failed: response exceeds 65,536 bytes.\r\n" + strings.ReplaceAll(humanReadbackFooter(duration), "\n", "\r\n")
}

func writeReadbackHeader(b *boundedResponse, c *Client, status configurationReadbackStatus, resource string) error {
	if _, err := fmt.Fprintf(b, "%s for %s\nPreset: ", resource, c.callsign); err != nil {
		return err
	}
	name := "(none)"
	if status.Preset.Associated {
		name = status.Preset.Name
		if status.Preset.Modified {
			name += " (modified)"
		}
	}
	_, err := fmt.Fprintf(b, "%s\n", name)
	return err
}

func (s *Server) renderHumanReadback(c *Client, resource, category string, duration time.Duration) (string, error) {
	status := s.captureReadbackStatus(c, nil)
	var b boundedResponse
	err := c.withBorrowedConfiguration(func(cfg filter.Configuration) error {
		status = attachReadbackConfiguration(status, c, cfg)
		if !readbackMetadataFits(status, "", "") {
			return errReadbackTooLarge
		}
		if err := writeReadbackHeader(&b, c, status, resource); err != nil {
			return err
		}
		if resource == "SETTINGS" {
			if category != "" {
				return fmt.Errorf("usage: SHOW SETTINGS")
			}
			if !(filter.Configuration{Settings: cfg.Settings}).MinimumSizeFits(maxYAMLBytes) {
				return errReadbackTooLarge
			}
			return writeHumanSettings(&b, cfg.Settings, status)
		}
		switch category {
		case "CONF":
			category = "CONFIDENCE"
		case "PC93":
			category = "ANNOUNCE"
		}
		categories := readbackCategories(cfg.Filters)
		if category == "FULL" && !(filter.Configuration{Filters: cfg.Filters}).MinimumSizeFits(maxYAMLBytes) {
			return errReadbackTooLarge
		}
		found := category == "" || category == "FULL"
		for _, item := range categories {
			if category != "" && category != "FULL" && category != item.name {
				continue
			}
			found = true
			if category != "" && !categoryMinimumFits(item) {
				return errReadbackTooLarge
			}
			if err := writeReadbackCategory(&b, item, category == "", cfg.Filters.NearbyEnabled, status.Effective.NearbyActive); err != nil {
				return err
			}
		}
		if !found {
			return fmt.Errorf("usage: SHOW FILTER [FULL|BAND|MODE|SOURCE|EVENT|CONFIDENCE|PATH|DXCONT|DECONT|DXZONE|DEZONE|DXGRID2|DEGRID2|DXDXCC|DEDXCC|DXCALL|DECALL|BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC|NEARBY]")
		}
		if category == "" {
			_, err := b.Write([]byte("Use SHOW FILTER FULL or SHOW FILTER <category> for every exact rule.\n"))
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if _, err := b.Write([]byte(humanReadbackFooter(duration))); err != nil {
		return "", err
	}
	return string(b.data), nil
}

func writeHumanSettings(b *boundedResponse, cfg filter.SettingsConfiguration, status configurationReadbackStatus) error {
	for _, item := range []struct{ label, configured, effective string }{
		{"DIALECT", cfg.Dialect, status.Effective.Dialect}, {"GRID", cfg.Grid, status.Effective.Grid},
		{"NOISE", cfg.NoiseClass, status.Effective.NoiseClass}, {"DEDUPE", cfg.DedupePolicy, status.Effective.DedupePolicy},
	} {
		configured := strconv.Quote(item.configured)
		if item.configured == "" {
			configured = "DEFAULT (empty)"
		}
		if _, err := fmt.Fprintf(b, "%s: %s (effective %q)\n", item.label, configured, item.effective); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(b, "GRID source: derived=%t\nPATHSAMPLES: %d (0=cluster default; effective %d)\nSOLAR: %d minutes (0=OFF)\nDIAG: %s (session only)\nLive spots: paused=%t; delivery pending=%t; remaining=%ds; suppressed=%d\nPersistence: temporary defaults=%t\nServer defaults: dialect=%s dedupe=%s noise=%s PATHSAMPLES=%d\n",
		status.Effective.GridDerived, cfg.PathMinObservationCount, status.Effective.PathMinObservationCount,
		cfg.SolarSummaryMinutes, status.Session.DiagnosticComments, status.Session.PauseActive,
		status.Session.PausePendingDelivery, status.Session.PauseRemaining, status.Session.SuppressedSpots,
		status.Session.TemporaryDefaults, status.Server.DefaultDialect, status.Server.DefaultDedupePolicy,
		status.Server.DefaultNoiseClass, status.Server.PathMinObservationCount)
	return err
}
