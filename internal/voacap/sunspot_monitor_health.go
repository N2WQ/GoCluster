package voacap

import (
	"fmt"
	"strings"
	"time"
)

// finishPoll runs on success, unchanged input and every fetch/parse/save error.
// Observation timestamps, rather than fetch activity, determine stale entry and
// recovery. One scalar alarm state bounds noise to one entry and recovery;
// missing components have no invented age and no startup timer.
func (m *SunspotMonitor) finishPoll(now time.Time, err error) {
	m.mu.Lock()
	m.lastAttemptAtUTC = now
	m.lastError = ""
	if err != nil {
		message := err.Error()
		if len(message) > maxSolarErrorBytes {
			message = message[:maxSolarErrorBytes]
		}
		m.lastError = strings.Clone(message)
	}
	recentStale := m.state.EWMAInitialized && now.Sub(m.state.LastObservedAtUTC) >= m.cfg.StaleAfter
	message := solarHealthTransition("reported-group index", now, m.state.LastObservedAtUTC, recentStale, m.recentStale, &m.recentWarningAtUTC, &m.recentRecoveryAtUTC)
	m.recentStale = recentStale
	m.mu.Unlock()
	// A logger may block; keep it outside the state mutex used by forecasts.
	if m.cfg.Logger != nil && message != "" {
		m.cfg.Logger.Printf("%s", message)
	}
}

func solarHealthTransition(component string, now, observed time.Time, stale, wasStale bool, warningAt, recoveryAt *time.Time) string {
	if stale == wasStale {
		return ""
	}
	if stale {
		*warningAt = now
		return fmt.Sprintf("Warning: VOACAP solar %s observation is stale (observed=%s age=%s); continuing last good solar input when available", component, observed.Format(time.RFC3339), now.Sub(observed).Round(time.Second))
	}
	*recoveryAt = now
	return fmt.Sprintf("VOACAP solar %s observation recovered (observed=%s)", component, observed.Format(time.RFC3339))
}
