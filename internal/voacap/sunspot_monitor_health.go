package voacap

import (
	"fmt"
	"strings"
	"time"
)

// finishPoll runs on success, unchanged input and every fetch/parse/save error.
// Observation timestamps, rather than fetch activity, determine stale entry and
// recovery. Two scalar alarm states bound noise to one entry and recovery each;
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
	recentStale := m.cfg.RecentWeight > 0 && m.state.EWMAInitialized && now.Sub(m.state.LastObservedAtUTC) >= m.cfg.StaleAfter
	meanStale := m.cfg.RecentWeight < 1 && m.state.MeanInitialized && now.Sub(m.state.MeanObservedAtUTC) >= m.cfg.StaleAfter
	var messages [2]string
	messages[0] = solarHealthTransition("Noon flux", now, m.state.LastObservedAtUTC, recentStale, m.recentStale, &m.recentWarningAtUTC, &m.recentRecoveryAtUTC)
	messages[1] = solarHealthTransition("published 90-day mean", now, m.state.MeanObservedAtUTC, meanStale, m.meanStale, &m.meanWarningAtUTC, &m.meanRecoveryAtUTC)
	m.recentStale, m.meanStale = recentStale, meanStale
	m.mu.Unlock()
	// A logger may block; keep it outside the state mutex used by forecasts.
	if m.cfg.Logger != nil {
		for _, message := range messages {
			if message != "" {
				m.cfg.Logger.Printf("%s", message)
			}
		}
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
