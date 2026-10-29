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
	"net"
	"net/netip"
	"strings"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

type restoredLogin struct {
	created       bool
	profileReset  bool
	previousLogin time.Time
	previousIP    string
	loadError     error
	warning       string
	evicted       *Client
	total         int
}

var (
	errSessionFull      = errors.New("maximum registered sessions reached")
	errBaseSessionFull  = errors.New("maximum sessions for station reached")
	errIPSessionFull    = errors.New("maximum sessions for IP reached")
	errInvalidSessionIP = errors.New("connection peer is not an IP address")
)

// Entries exist only while a station has registered sessions or reservations.
// All counts and membership transitions are protected by clientsMutex.
type baseSessionCount struct {
	registered int
	pending    int
}

// IP entries are retained only while a registered session or reservation owns
// them; the client registry mutex protects all count transitions.
type ipSessionCount struct {
	registered int
	pending    int
}

// A new identity reserves global/station capacity; a cross-IP replacement
// reserves only its destination IP. Empty fields mean no reservation.
type loginReservation struct {
	identity bool
	ip       string
}

func (r loginReservation) active() bool { return r.identity || r.ip != "" }

// sessionIP derives the quota key from the observed peer, never a stored or
// forwarded address. A zone remains part of a scoped IPv6 identity.
func sessionIP(address string) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = strings.TrimSpace(address)
	}
	parsed, err := netip.ParseAddr(strings.TrimSpace(host))
	if err != nil {
		return "", errInvalidSessionIP
	}
	return parsed.Unmap().String(), nil
}

func (s *Server) setIPSessionCountLocked(ip string, count ipSessionCount) {
	if count.registered == 0 && count.pending == 0 {
		delete(s.ipSessionCounts, ip)
	} else {
		if s.ipSessionCounts == nil {
			s.ipSessionCounts = make(map[string]ipSessionCount)
		}
		s.ipSessionCounts[ip] = count
	}
	// Readers must not reacquire the registry lock during reporting or close.
	s.metrics.trackedSessionIPs.Store(int64(len(s.ipSessionCounts)))
}

func (s *Server) setBaseSessionCountLocked(base string, count baseSessionCount) {
	if count.registered == 0 && count.pending == 0 {
		delete(s.baseSessionCounts, base)
	} else {
		if s.baseSessionCounts == nil {
			s.baseSessionCounts = make(map[string]baseSessionCount)
		}
		s.baseSessionCounts[base] = count
	}
	// Publish diagnostic cardinality without making readers acquire the registry
	// lock. Admission decisions continue to use the map while holding that lock.
	s.metrics.trackedSessionBases.Store(int64(len(s.baseSessionCounts)))
}

// A registered exact identity is a replacement, not a new occupied slot.
// Global registered-full precedence preserves the existing server-full notice.
func (s *Server) loginCapacityErrorLocked(c *Client, base, ip string) error {
	owner := s.clients[c.callsign]
	if owner == nil {
		if s.maxConnections > 0 && len(s.clients) >= s.maxConnections {
			return errSessionFull
		}
		if s.maxSessionsPerBaseCall > 0 && s.baseSessionCounts[base].registered >= s.maxSessionsPerBaseCall {
			return errBaseSessionFull
		}
	}
	if s.maxSessionsPerIP > 0 {
		if owner != nil {
			if owner.ipSessionKey == "" {
				return errInvalidSessionIP
			}
			if owner.ipSessionKey == ip {
				return nil
			}
		}
		if s.ipSessionCounts[ip].registered >= s.maxSessionsPerIP {
			return errIPSessionFull
		}
	}
	return nil
}

// loginCapacityError makes only an early rejection decision. An unavailable lock
// is not evidence of capacity; admission always rechecks while owning the stripe.
func (s *Server) loginCapacityError(c *Client, base, ip string) error {
	checked := s.clientsMutex.TryRLock()
	var err error
	if checked {
		err = s.loginCapacityErrorLocked(c, base, ip)
		s.clientsMutex.RUnlock()
	}
	if s.testLoginCapacityProbe != nil {
		s.testLoginCapacityProbe(c, checked, err != nil)
	}
	return err
}

// acquireLoginCapacity owns the callsign stripe when it returns. New identities
// reserve a slot before profile I/O; a current owner can be replaced in place.
// Never wait for capacity with the stripe held: another callsign on the same
// stripe may need to finish its final save before it can leave the registry.
func (s *Server) acquireLoginCapacity(c *Client, deadline time.Time) (release func(), reserved loginReservation, err error) {
	base := spot.NormalizeOwnCallsign(c.callsign)
	ip := ""
	if s.maxSessionsPerIP > 0 {
		ip, err = sessionIP(c.address)
		if err != nil {
			return nil, loginReservation{}, err
		}
	}
	for {
		// A full registry can reject a different identity immediately, even if
		// its stripe is busy with an unrelated save. A possible replacement must
		// still acquire the stripe and recheck before any profile work.
		if err := s.loginCapacityError(c, base, ip); err != nil {
			return nil, loginReservation{}, err
		}
		var recheck func() error
		if s.maxConnections > 0 || s.maxSessionsPerBaseCall > 0 || s.maxSessionsPerIP > 0 {
			recheck = func() error { return s.loginCapacityError(c, base, ip) }
		}
		release, err = s.acquireConfigurationWithRecheck(c, false, true, deadline, recheck)
		if err != nil {
			return nil, loginReservation{}, err
		}
		s.clientsMutex.Lock()
		if err = s.loginCapacityErrorLocked(c, base, ip); err != nil {
			s.clientsMutex.Unlock()
			release()
			return nil, loginReservation{}, err
		}
		owner := s.clients[c.callsign]
		newIdentity := owner == nil
		needsIP := s.maxSessionsPerIP > 0
		if needsIP && owner != nil {
			needsIP = owner.ipSessionKey != ip
		}
		globalAvailable := !newIdentity || s.maxConnections <= 0 || len(s.clients)+s.sessionReservations < s.maxConnections
		baseCount := s.baseSessionCounts[base]
		baseAvailable := !newIdentity || s.maxSessionsPerBaseCall <= 0 || baseCount.registered+baseCount.pending < s.maxSessionsPerBaseCall
		ipCount := s.ipSessionCounts[ip]
		ipAvailable := !needsIP || ipCount.registered+ipCount.pending < s.maxSessionsPerIP
		if globalAvailable && baseAvailable && ipAvailable {
			if newIdentity && (s.maxConnections > 0 || s.maxSessionsPerBaseCall > 0 || s.maxSessionsPerIP > 0) {
				s.sessionReservations++
				reserved.identity = true
			}
			if reserved.identity && s.maxSessionsPerBaseCall > 0 {
				baseCount.pending++
				s.setBaseSessionCountLocked(base, baseCount)
			}
			if needsIP {
				ipCount.pending++
				s.setIPSessionCountLocked(ip, ipCount)
				reserved.ip = ip
			}
			s.clientsMutex.Unlock()
			return release, reserved, nil
		}
		if s.sessionCapacityChanged == nil {
			s.sessionCapacityChanged = make(chan struct{})
		}
		changed := s.sessionCapacityChanged
		s.clientsMutex.Unlock()
		release()
		if s.testLoginCapacityWait != nil {
			s.testLoginCapacityWait(c)
		}
		if err = s.waitLoginCapacity(c, changed, deadline); err != nil {
			return nil, loginReservation{}, err
		}
	}
}

func (s *Server) waitLoginCapacity(c *Client, changed <-chan struct{}, deadline time.Time) error {
	var timer *time.Timer
	var expired <-chan time.Time
	if !deadline.IsZero() {
		timer = time.NewTimer(time.Until(deadline))
		expired = timer.C
		defer timer.Stop()
	}
	select {
	case <-changed:
		return nil
	case <-c.done:
		return errClientClosed
	case <-s.shutdown:
		return errClientClosed
	case <-expired:
		return errLoginDeadline
	}
}

// signalSessionCapacityLocked wakes bounded prelogin waiters after an occupancy
// transition. The condition and notification are both protected by clientsMutex.
func (s *Server) signalSessionCapacityLocked() {
	if s.sessionCapacityChanged != nil {
		close(s.sessionCapacityChanged)
		s.sessionCapacityChanged = nil
	}
}

func (s *Server) releaseSessionReservation(c *Client, reserved loginReservation) {
	base := spot.NormalizeOwnCallsign(c.callsign)
	s.clientsMutex.Lock()
	if reserved.identity {
		s.sessionReservations--
	}
	if reserved.identity && s.maxSessionsPerBaseCall > 0 {
		count := s.baseSessionCounts[base]
		count.pending--
		s.setBaseSessionCountLocked(base, count)
	}
	if reserved.ip != "" {
		count := s.ipSessionCounts[reserved.ip]
		count.pending--
		s.setIPSessionCountLocked(reserved.ip, count)
	}
	s.signalSessionCapacityLocked()
	s.clientsMutex.Unlock()
}

func (s *Server) restoreAndRegisterClient(c *Client, loginTime, deadline time.Time) (restoredLogin, error) {
	release, reserved, err := s.acquireLoginCapacity(c, deadline)
	if err != nil {
		return restoredLogin{}, err
	}
	defer release()
	defer func() {
		if reserved.active() {
			s.releaseSessionReservation(c, reserved)
		}
	}()
	select {
	case <-c.done:
		return restoredLogin{}, errClientClosed
	case <-s.shutdown:
		return restoredLogin{}, errClientClosed
	default:
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return restoredLogin{}, errLoginDeadline
	}
	result := s.restoreClientRecordOwned(c, loginTime)
	select {
	case <-c.done:
		return result, errClientClosed
	case <-s.shutdown:
		return result, errClientClosed
	default:
	}
	if !time.Now().Before(deadline) {
		return result, errLoginDeadline
	}
	evicted, total, err := s.registerClientOwned(c, reserved, deadline)
	if err != nil {
		return result, err
	}
	reserved = loginReservation{}
	result.evicted, result.total = evicted, total
	return result, nil
}

func (s *Server) restoreClientRecordOwned(c *Client, loginTime time.Time) restoredLogin {
	result := restoredLogin{}
	record, upgrade, err := filter.UpgradeUserRecordForLoginWithOutcome(c.callsign, s.commitLoginRecord)
	result.profileReset = upgrade.ProfileReset
	if upgrade.ProfileReset {
		result.warning = "Your old filters were reset. Your settings were kept.\n"
	} else if upgrade.PresetLibraryCleaned && record != nil {
		result.warning = "Old shared presets were reset; your Boolean rules were kept.\n"
	} else if upgrade.PresetLibraryCleaned {
		result.warning = "Old shared presets were reset, but your profile could not be upgraded.\n"
	}
	if record != nil && errors.Is(err, filter.ErrLoginUpgradeLibrary) {
		result.warning += "Your saved profile was restored, but old preset cleanup failed. Preset commands may remain unavailable; other preferences can still be saved.\n"
		log.Printf("Warning: failed old preset cleanup for %s: %v", c.callsign, err)
		err = nil
	}
	if errors.Is(err, filter.ErrLoginUpgradeProfileMissing) {
		record = &filter.UserRecord{
			Filter: *filter.NewFilter(), Dialect: "go",
			DedupePolicy: s.effectiveDefaultDedupePolicy().label(),
		}
		result.created, err = true, nil
	}
	if err != nil {
		c.recordProtected = true
		result.loadError = err
		result.warning += "Your saved user record could not be read or upgraded. Using temporary defaults; changes in this session will not be saved. SAVE PRESET is unavailable.\n"
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
		prepared, saveErr := filter.PrepareUserRecord(c.callsign, record)
		if saveErr == nil {
			saveErr = s.commitLoginRecord(prepared)
		}
		if saveErr != nil {
			if result.created {
				result.warning += "Your starting settings are active but could not be saved. They may be lost when you reconnect.\n"
			} else {
				result.warning += "Restored your saved configuration, but could not save this login's timestamp/IP.\n"
			}
			log.Printf("Warning: failed to save login metadata for %s: %v", c.callsign, saveErr)
		} else if result.created {
			// A new SSID can inherit an old base-call library. Only inspect it
			// after the new profile exists; never replace its new-user defaults.
			_, cleanup, cleanupErr := filter.UpgradeUserRecordForLoginWithOutcome(c.callsign, nil)
			if cleanupErr != nil {
				result.warning += "Your new profile was saved, but shared preset cleanup could not finish. Preset commands may remain unavailable.\n"
				log.Printf("Warning: new-SSID preset cleanup failed for %s: %v", c.callsign, cleanupErr)
			} else if cleanup.PresetLibraryCleaned {
				result.warning += "Old shared presets were reset; your new-user filter defaults are unchanged.\n"
			}
		}
	}
	c.recentIPs = boundedSessionIPs(record.RecentIPs)
	c.lastLoginUTC = record.LastLoginUTC
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

func (s *Server) commitLoginRecord(prepared *filter.PreparedUserRecord) error {
	if s.commitLoginRecordFn != nil {
		return s.commitLoginRecordFn(prepared)
	}
	return prepared.Commit()
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

func (s *Server) registerClientOwned(c *Client, reserved loginReservation, deadline time.Time) (*Client, int, error) {
	base := spot.NormalizeOwnCallsign(c.callsign)
	ip := ""
	if s.maxSessionsPerIP > 0 {
		var err error
		ip, err = sessionIP(c.address)
		if err != nil {
			return nil, 0, err
		}
	}
	s.clientsMutex.Lock()
	// Stop closes shutdown before taking this lock. Checking under the same
	// lock ensures Stop either sees this registration or prevents it entirely.
	select {
	case <-c.done:
		s.clientsMutex.Unlock()
		return nil, 0, errClientClosed
	case <-s.shutdown:
		s.clientsMutex.Unlock()
		return nil, 0, errClientClosed
	default:
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		s.clientsMutex.Unlock()
		return nil, 0, errLoginDeadline
	}
	if s.clients == nil {
		s.clients = make(map[string]*Client)
	}
	evicted := s.clients[c.callsign]
	oldIP := ""
	if s.maxSessionsPerIP > 0 && evicted != nil {
		oldIP = evicted.ipSessionKey
		if oldIP == "" {
			s.clientsMutex.Unlock()
			return nil, 0, errInvalidSessionIP
		}
	}
	replacing := evicted != nil && evicted != c
	if replacing {
		evicted.readPauseMu.Lock()
	}
	if c.preloginBudget != nil {
		if err := c.preloginBudget.finish(); err != nil {
			if replacing {
				evicted.readPauseMu.Unlock()
			}
			s.clientsMutex.Unlock()
			return nil, 0, err
		}
		c.preloginBudget = nil
	}
	if replacing {
		evicted.invalidateHumanReadbackLocked()
		evicted.readPauseMu.Unlock()
	}
	s.peerSessionID++
	c.peerSessionID = s.peerSessionID
	c.ipSessionKey = ip
	s.clients[c.callsign] = c
	if evicted != nil && evicted != c && evicted.retired != nil {
		// Retirement and registry ownership change share this lock. The writer
		// still owns notice delivery and socket closure after replacement.
		select {
		case <-evicted.retired:
		default:
			close(evicted.retired)
		}
	}
	if reserved.identity {
		s.sessionReservations--
	}
	if s.maxSessionsPerBaseCall > 0 {
		count := s.baseSessionCounts[base]
		if reserved.identity {
			count.pending--
		}
		if evicted == nil {
			count.registered++
		}
		s.setBaseSessionCountLocked(base, count)
	}
	if s.maxSessionsPerIP > 0 {
		if evicted != nil && oldIP != ip {
			oldCount := s.ipSessionCounts[oldIP]
			oldCount.registered--
			s.setIPSessionCountLocked(oldIP, oldCount)
		}
		if evicted == nil || oldIP != ip {
			count := s.ipSessionCounts[ip]
			if reserved.ip != "" {
				count.pending--
			}
			count.registered++
			s.setIPSessionCountLocked(ip, count)
		}
	}
	s.signalSessionCapacityLocked()
	s.peerMembershipRevision++
	total := len(s.clients)
	s.shardsDirty.Store(true)
	s.clientsMutex.Unlock()
	return evicted, total, nil
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
	release, reserved, err := s.acquireLoginCapacity(c, time.Time{})
	if err != nil {
		return
	}
	evicted, total, err := s.registerClientOwned(c, reserved, time.Time{})
	if err != nil && reserved.active() {
		s.releaseSessionReservation(c, reserved)
	}
	release()
	if err != nil {
		return
	}
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
	base := spot.NormalizeOwnCallsign(c.callsign)
	s.clientsMutex.Lock()
	removed := s.clients[c.callsign] == c
	if removed {
		delete(s.clients, c.callsign)
		if s.maxSessionsPerBaseCall > 0 {
			count := s.baseSessionCounts[base]
			count.registered--
			s.setBaseSessionCountLocked(base, count)
		}
		if s.maxSessionsPerIP > 0 {
			ip := c.ipSessionKey
			count := s.ipSessionCounts[ip]
			count.registered--
			s.setIPSessionCountLocked(ip, count)
		}
		s.signalSessionCapacityLocked()
		s.peerMembershipRevision++
		s.shardsDirty.Store(true)
	}
	total := len(s.clients)
	s.clientsMutex.Unlock()
	if removed && s.testAfterClientRemoval != nil {
		s.testAfterClientRemoval(c)
	}
	release()
	if removed {
		s.notifyPeerMembershipChange()
		s.notifyClientListChange()
	}
	log.Printf("Unregistered client: %s (total: %d)", c.callsign, total)
}
