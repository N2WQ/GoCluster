// File role: Restores exact preferences and fences the complete reconnect handoff.
// The full-callsign stripe remains owned from disk read through registration.
// Optional eviction reporting happens after release; teardown retains its final
// owner lease through membership removal. Protected records and terminal machine
// rejection skip autosave; eligible ordinary disconnects save before removal.
package telnet

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

type restoredLogin struct {
	created       bool
	previousLogin time.Time
	previousIP    string
	loadError     error
	warning       string
	evicted       *Client
	total         int
}

func (s *Server) restoreAndRegisterClient(c *Client, loginTime, deadline time.Time) (restoredLogin, error) {
	release, err := s.acquireConfiguration(c, false, true, deadline)
	if err != nil {
		return restoredLogin{}, err
	}
	result := s.restoreClientRecordOwned(c, loginTime)
	select {
	case <-c.done:
		release()
		return result, errClientClosed
	case <-s.shutdown:
		release()
		return result, errClientClosed
	default:
	}
	if !time.Now().Before(deadline) {
		release()
		return result, fmt.Errorf("configuration handoff exceeded login deadline")
	}
	evicted, total := s.registerClientOwned(c)
	release()
	result.evicted, result.total = evicted, total
	return result, nil
}

func (s *Server) restoreClientRecordOwned(c *Client, loginTime time.Time) restoredLogin {
	result := restoredLogin{}
	record, err := filter.LoadUserRecord(c.callsign)
	if errors.Is(err, os.ErrNotExist) {
		record = &filter.UserRecord{
			Filter: *filter.NewFilter(), Dialect: "go",
			DedupePolicy: s.effectiveDefaultDedupePolicy().label(),
		}
		result.created, err = true, nil
	}
	if err != nil {
		c.recordProtected = true
		result.loadError = err
		result.warning = "Your saved user record could not be read. Using temporary defaults; changes in this session will not be saved. SAVE PRESET is unavailable.\n"
		record = &filter.UserRecord{
			Filter: *filter.NewFilter(), Dialect: string(s.configurationDefaultDialect()),
			DedupePolicy: s.effectiveDefaultDedupePolicy().label(),
		}
	} else {
		result.previousLogin = record.LastLoginUTC
		if len(record.RecentIPs) != 0 {
			result.previousIP = record.RecentIPs[0]
		}
		record.RecentIPs = filter.UpdateRecentIPs(record.RecentIPs, spotterIP(c.address))
		record.LastLoginUTC = loginTime
		persist := filter.SaveUserRecord
		if s.saveLoginRecordFn != nil {
			persist = s.saveLoginRecordFn
		}
		if err := persist(c.callsign, record); err != nil {
			result.warning = "Restored your saved configuration, but could not save this login's timestamp/IP.\n"
			log.Printf("Warning: failed to save login metadata for %s: %v", c.callsign, err)
		}
	}
	c.recentIPs = record.RecentIPs
	c.configuredSettings = filter.SettingsConfiguration{
		Dialect: record.Dialect, Grid: record.Grid, NoiseClass: record.NoiseClass,
		DedupePolicy: record.DedupePolicy, PathMinObservationCount: record.PathMinObservationCount,
		SolarSummaryMinutes: record.SolarSummaryMinutes,
	}
	c.configurationInitialized = true
	c.presetReference = record.Preset
	cfg := filter.ConfigurationFromFilter(&record.Filter, c.configuredSettings)
	prepared, warning := s.prepareConfiguration(c, cfg, loginTime)
	// No broadcast reader can observe this unregistered client yet. Keep the
	// initial Filter pointer stable for the entire connection's lifetime.
	*c.filter = *prepared.filter
	c.dialect = prepared.dialect
	c.setDedupePolicy(prepared.getDedupePolicy())
	c.grid, c.gridDerived = prepared.grid, prepared.gridDerived
	c.gridCell, c.gridCoarseCell = prepared.gridCell, prepared.gridCoarseCell
	c.noiseClass, c.pathMinObservationCount = prepared.noiseClass, prepared.pathMinObservationCount
	c.setSolarSummaryMinutes(c.configuredSettings.SolarSummaryMinutes, loginTime)
	c.refreshConfigurationRevision()
	result.warning += warning
	return result
}

func (s *Server) configurationDefaultDialect() DialectName {
	if s == nil || s.filterEngine == nil {
		return DialectGo
	}
	return s.filterEngine.defaultDialect
}

// prepareConfiguration builds runtime state without altering the caller. Stored
// empty GRID/noise/default choices stay in configuration; effective fallbacks
// and NEARBY cells belong solely to the prepared runtime client.
func (s *Server) prepareConfiguration(c *Client, cfg filter.Configuration, now time.Time) (*Client, string) {
	f := cfg.FilterValue()
	prepared := &Client{filter: &f, dialect: normalizeDialectName(cfg.Settings.Dialect)}
	if cfg.Settings.Dialect == "" {
		prepared.dialect = s.configurationDefaultDialect()
	}
	requested := parseDedupePolicy(cfg.Settings.DedupePolicy)
	if cfg.Settings.DedupePolicy == "" {
		requested = s.effectiveDefaultDedupePolicy()
	}
	policy := s.resolveDedupePolicy(requested)
	prepared.setDedupePolicy(policy)
	prepared.grid = cfg.Settings.Grid
	if prepared.grid == "" && s.gridLookup != nil {
		if grid, derived, ok := s.gridLookup(c.callsign); ok {
			prepared.grid, prepared.gridDerived = strings.ToUpper(strings.TrimSpace(grid)), derived
		}
	}
	prepared.gridCell = pathreliability.EncodeCell(prepared.grid)
	prepared.gridCoarseCell = pathreliability.EncodeCoarseCell(prepared.grid)
	prepared.noiseClass = cfg.Settings.NoiseClass
	if prepared.noiseClass == "" {
		prepared.noiseClass = "QUIET"
	}
	if cfg.Settings.PathMinObservationCount > s.pathPredictorMinObservationCount() {
		prepared.pathMinObservationCount = cfg.Settings.PathMinObservationCount
	}
	prepared.setSolarSummaryMinutes(cfg.Settings.SolarSummaryMinutes, now)
	warning, _ := applyNearbyLoginState(prepared, s.nearbyLoginWarning)
	if policy != requested {
		warning += fmt.Sprintf("Note: dedupe %s unavailable; effective dedupe is %s.\n", requested.label(), policy.label())
	}
	return prepared, warning
}

func (s *Server) registerClientOwned(c *Client) (*Client, int) {
	s.clientsMutex.Lock()
	if s.clients == nil {
		s.clients = make(map[string]*Client)
	}
	evicted := s.clients[c.callsign]
	if evicted != nil && evicted != c {
		evicted.invalidateHumanReadback()
	}
	s.peerSessionID++
	c.peerSessionID = s.peerSessionID
	s.clients[c.callsign] = c
	s.peerMembershipRevision++
	total := len(s.clients)
	s.shardsDirty.Store(true)
	s.clientsMutex.Unlock()
	return evicted, total
}

func (s *Server) finishClientRegistration(c, evicted *Client, total int) {
	s.notifyPeerMembershipChange()
	s.notifyClientListChange()
	if evicted != nil && evicted != c {
		// Preserve the configured duplicate-login notice and existing serialized
		// close-after-control policy. Ownership already fences every retired save.
		message := strings.TrimSpace(s.duplicateLoginMsg)
		if message != "" {
			_ = evicted.SendAndClose(message + "\n")
		} else {
			evicted.close("duplicate login")
		}
		s.reportConnection("evict", "duplicate_login", evicted.callsign, evicted.address)
		log.Printf("Evicted existing session for %s due to duplicate login", c.callsign)
	}
	log.Printf("Registered client: %s (total: %d)", c.callsign, total)
}

func (s *Server) registerClient(c *Client) {
	release, err := s.acquireConfiguration(c, false, true, time.Time{})
	if err != nil {
		return
	}
	evicted, total := s.registerClientOwned(c)
	release()
	s.finishClientRegistration(c, evicted, total)
}

func (s *Server) unregisterClient(c *Client) {
	if c == nil {
		return
	}
	release, err := s.acquireConfiguration(c, true, false, time.Time{})
	if err != nil {
		return // A retired session may neither save nor remove its replacement.
	}
	// A rejected machine upload must not repair an earlier failed human save.
	// Keep the final owner fence and registry removal even when autosave is skipped.
	if !c.preserveRecordOnExit.Load() {
		if err := c.saveFilterOwned(); err != nil {
			log.Printf("Warning: failed to persist filter for %s during unregister: %v", c.callsign, err)
		}
	}
	s.clientsMutex.Lock()
	removed := s.clients[c.callsign] == c
	if removed {
		delete(s.clients, c.callsign)
		s.peerMembershipRevision++
		s.shardsDirty.Store(true)
	}
	total := len(s.clients)
	s.clientsMutex.Unlock()
	release()
	if removed {
		s.notifyPeerMembershipChange()
		s.notifyClientListChange()
	}
	log.Printf("Unregistered client: %s (total: %d)", c.callsign, total)
}
