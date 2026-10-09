// File role: Stages human preference commands within the existing SSID lease.
// A fresh minimal adapter owns writable preferences; only accepted outcomes
// publish. Human saves retain their live changes when persistence fails.
package telnet

import (
	"fmt"
	"log"
	"strings"

	"dxcluster/filter"
)

type humanPersistenceWarning uint8

const (
	humanWarningInline humanPersistenceWarning = iota
	humanWarningFilter
	humanWarningDialect
)

// Intent comes from the handler's accepted branch, never response text or
// equality. Repeated ordinary PASS still saves; unchanged COMMENT/DIALECT do not.
type humanCommandOutcome struct {
	response      string
	handled       bool
	publish       bool
	forceSolar    bool
	warning       humanPersistenceWarning
	failureFormat string
	failureArgs   []any
}

func humanHandled(response string) humanCommandOutcome {
	return humanCommandOutcome{response: response, handled: true}
}

// Format is a literal owned by the handler. Arguments contain only captured
// scalar/string values; user text is never interpreted as another format.
func humanMutation(response, failureFormat string, failureArgs ...any) humanCommandOutcome {
	return humanCommandOutcome{response: response, handled: true, publish: true, failureFormat: failureFormat, failureArgs: failureArgs}
}

func (outcome humanCommandOutcome) persistenceFailure(c *Client, err error) string {
	if outcome.warning == humanWarningInline {
		args := make([]any, len(outcome.failureArgs)+1)
		copy(args, outcome.failureArgs)
		args[len(outcome.failureArgs)] = err
		return fmt.Sprintf(outcome.failureFormat, args...)
	}
	label := "filter"
	if outcome.warning == humanWarningDialect {
		label = "dialect"
	}
	log.Printf("Warning: failed to persist %s for %s: %v", label, c.callsign, err)
	return outcome.response + fmt.Sprintf("warning: failed to persist; changes are active but were not saved: %v\n", err)
}

// newHumanPreferenceCandidate requires the enclosing configuration lease. It
// deliberately avoids capture/revision helpers: legacy raw callers must not be
// initialized as a side effect of constructing or discarding a candidate.
func newHumanPreferenceCandidate(c *Client) *Client {
	if c == nil {
		return nil
	}
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	settings := c.configuredSettings
	minutes := c.getSolarSummaryMinutes()
	if !c.configurationInitialized {
		grid := c.grid
		if c.gridDerived {
			grid = ""
		}
		settings = filter.SettingsConfiguration{
			Dialect: string(c.dialect), Grid: grid, NoiseClass: c.noiseClass,
			DedupePolicy: c.getDedupePolicy().label(), PathMinObservationCount: c.pathMinObservationCount,
			SolarSummaryMinutes: minutes,
		}
	}
	staged := &Client{
		callsign: c.callsign, server: c.server, dialect: c.dialect,
		grid: c.grid, gridDerived: c.gridDerived, gridCell: c.gridCell, gridCoarseCell: c.gridCoarseCell,
		noiseClass: c.noiseClass, pathMinObservationCount: c.pathMinObservationCount,
		configuredSettings: settings, configurationInitialized: true, solarSummaryMinutes: minutes,
	}
	staged.setDedupePolicy(c.getDedupePolicy())
	if c.filter != nil {
		value := filter.ConfigurationFromFilter(c.filter, settings).Clone().FilterValue()
		value.LegacyMinConfidence = c.filter.LegacyMinConfidence
		value.NearbyUserFine, value.NearbyUserCoarse = c.filter.NearbyUserFine, c.filter.NearbyUserCoarse
		// DisableNearby copies maps from this immutable runtime snapshot. It must
		// remain available for OFF even after an unrelated human setting changes.
		value.NearbySnapshot = c.filter.NearbySnapshot
		staged.filter = &value
	}
	return staged
}

func (s *Server) runHumanPreferenceOwned(c *Client, line string, stage func(*Client, string) humanCommandOutcome) (string, bool) {
	staged := newHumanPreferenceCandidate(c)
	outcome := stage(staged, line)
	if !outcome.publish {
		return outcome.response, outcome.handled
	}
	s.publishHumanPreference(c, staged, outcome.forceSolar)
	if err := c.saveFilterOwned(); err != nil {
		return outcome.persistenceFailure(c, err), outcome.handled
	}
	return outcome.response, outcome.handled
}

// preflightHumanFilterOwned keeps unrecognized input, static parser errors and
// potentially large live summaries out of the detached candidate path. It uses
// only the existing parsers; domain validation still belongs to staged execution.
func (e *filterCommandEngine) preflightHumanFilterOwned(c *Client, line string) (humanCommandOutcome, bool) {
	if _, handled, valid := parseCommentFilterCommand(line); handled {
		if !valid {
			return humanHandled(commentCommandUsage), false
		}
		return humanCommandOutcome{}, true
	}
	tokens := strings.Fields(strings.TrimSpace(line))
	upper := make([]string, len(tokens))
	for i, token := range tokens {
		upper[i] = strings.ToUpper(token)
	}
	parsed, matched, errText := e.parse(tokens, upper, c.dialect)
	if matched {
		if errText != "" {
			return humanHandled(errText), false
		}
		if parsed.action == actionSummary {
			response, _ := e.execute(c, parsed)
			return humanHandled(response), false
		}
		return humanCommandOutcome{}, true
	}
	if hint := e.hintForOtherDialect(tokens, upper, c.dialect); hint != "" {
		return humanHandled(hint), false
	}
	return humanCommandOutcome{}, false
}
