// File role: Owns per-SSID configuration transactions, exact captures and revisions.
// Fixed stripes serialize saves with reconnect handoff without retaining historical
// callsigns. Acquire a stripe before registry/path/filter/writer locks; disk I/O
// runs after releasing those locks, while the stripe retains transaction ownership.
package telnet

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"dxcluster/filter"
)

var (
	errRetiredConfiguration = errors.New("session no longer owns this callsign")
	errProtectedRecord      = errors.New("saved user record is protected; changes in this session are temporary")
	errReadbackTooLarge     = errors.New("response exceeds 65,536 bytes; request a smaller filter category or reduce the configuration")
)

func configurationStripe(callsign string) uint64 {
	h := uint64(14695981039346656037)
	for i := range len(callsign) {
		b := callsign[i]
		if b >= 'a' && b <= 'z' {
			b -= 'a' - 'A'
		}
		h = (h ^ uint64(b)) * 1099511628211
	}
	return h % 64
}

// acquireConfiguration is cancellable while waiting. Final current-owner saves
// intentionally survive done/shutdown: teardown closes the socket before saving.
// New handoffs may enter without membership, but must retain this lease through
// authoritative record loading, preparation, metadata persistence and registration.
func (s *Server) acquireConfiguration(c *Client, final, handoff bool, deadline time.Time) (func(), error) {
	if s == nil {
		return func() {}, nil
	}
	s.configurationTxnOnce.Do(func() {
		for i := range s.configurationTxnStripes {
			s.configurationTxnStripes[i] = make(chan struct{}, 1)
		}
	})
	stripe := s.configurationTxnStripes[configurationStripe(c.callsign)]
	var timer *time.Timer
	var expired <-chan time.Time
	if !deadline.IsZero() {
		timer = time.NewTimer(time.Until(deadline))
		expired = timer.C
		defer timer.Stop()
	}
	done, shutdown := c.done, s.shutdown
	if final {
		done, shutdown = nil, nil
	}
	select {
	case stripe <- struct{}{}:
	case <-done:
		return nil, errClientClosed
	case <-shutdown:
		return nil, errClientClosed
	case <-expired:
		return nil, fmt.Errorf("configuration handoff exceeded login deadline")
	}
	release := func() { <-stripe }
	if !final {
		select {
		case <-done:
			release()
			return nil, errClientClosed
		case <-shutdown:
			release()
			return nil, errClientClosed
		default:
		}
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		release()
		return nil, fmt.Errorf("configuration handoff exceeded login deadline")
	}
	if !handoff {
		s.clientsMutex.RLock()
		current := s.clients[c.callsign]
		retired := current != c && (current != nil || c.peerSessionID != 0)
		s.clientsMutex.RUnlock()
		if retired {
			release()
			return nil, errRetiredConfiguration
		}
	}
	return release, nil
}

// initializeConfiguredSettings supports legacy in-memory callers as well as
// freshly restored sessions. Production login supplies the stored preferences
// explicitly so lookup-derived GRID and effective fallback choices stay separate.
func (c *Client) initializeConfiguredSettings() {
	if c.configurationInitialized {
		return
	}
	c.pathMu.RLock()
	grid := c.grid
	if c.gridDerived {
		grid = ""
	}
	c.configuredSettings = filter.SettingsConfiguration{
		Dialect: string(c.dialect), Grid: grid, NoiseClass: c.noiseClass,
		DedupePolicy:            c.getDedupePolicy().label(),
		PathMinObservationCount: c.pathMinObservationCount,
		SolarSummaryMinutes:     c.getSolarSummaryMinutes(),
	}
	c.pathMu.RUnlock()
	c.configurationInitialized = true
}

// withBorrowedConfiguration never lets references escape its lock section.
// Callers preflight before detaching a snapshot or producing detailed output.
func (c *Client) withBorrowedConfiguration(fn func(filter.Configuration) error) error {
	c.initializeConfiguredSettings()
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	return fn(filter.ConfigurationFromFilter(c.filter, c.configuredSettings))
}

func (c *Client) captureConfiguration(limit int) (filter.Configuration, error) {
	var result filter.Configuration
	err := c.withBorrowedConfiguration(func(cfg filter.Configuration) error {
		if limit > 0 && !cfg.MinimumSizeFits(limit) {
			return errReadbackTooLarge
		}
		result = cfg.Clone()
		return nil
	})
	return result, err
}

func (c *Client) refreshConfigurationRevision() {
	c.initializeConfiguredSettings()
	c.pathMu.RLock()
	c.filterMu.RLock()
	digest := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Fingerprint()
	c.filterMu.RUnlock()
	c.pathMu.RUnlock()
	if c.configurationDigestSet && digest != c.configurationDigest {
		c.configurationRevision++
	}
	c.configurationDigest, c.configurationDigestSet = digest, true
}

func (c *Client) configurationRevisionToken() (string, error) {
	if c.configurationEpoch == "" {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return "", fmt.Errorf("create configuration revision: %w", err)
		}
		c.configurationEpoch = hex.EncodeToString(nonce[:])
	}
	c.refreshConfigurationRevision()
	return fmt.Sprintf("%s-%d", c.configurationEpoch, c.configurationRevision), nil
}

func (s *Server) persistConfiguration(c *Client, cfg filter.Configuration, ref *filter.PresetReference) error {
	if c.recordProtected {
		return errProtectedRecord
	}
	persist := filter.SaveConfiguration
	if s != nil && s.saveConfigurationFn != nil {
		persist = s.saveConfigurationFn
	}
	return persist(c.callsign, cfg, ref, c.recentIPs)
}

// saveFilterOwned runs under a lease acquired by the enclosing command or final
// teardown. The exact snapshot detaches maps before disk encoding, keeping disk
// waits outside broadcast locks and preserving the session's preset reference.
func (c *Client) saveFilterOwned() error {
	if c == nil || c.filter == nil || strings.TrimSpace(c.callsign) == "" {
		return nil
	}
	if c.recordProtected {
		return errProtectedRecord
	}
	cfg, err := c.captureConfiguration(0)
	if err != nil {
		return err
	}
	c.refreshConfigurationRevision()
	return c.server.persistConfiguration(c, cfg, c.presetReference)
}

func (s *Server) runPreferenceCommand(c *Client, line string, handle func(*Client, string) (string, bool)) (string, bool) {
	if c == nil {
		return handle(c, line)
	}
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return fmt.Sprintf("Configuration command failed: %v\n", err), true
	}
	defer release()
	c.refreshConfigurationRevision()
	response, handled := handle(c, line)
	c.refreshConfigurationRevision()
	return response, handled
}

func (s *Server) handleDialectCommand(c *Client, line string) (string, bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "DIALECT") {
		return "", false
	}
	return s.runPreferenceCommand(c, line, s.handleDialectCommandOwned)
}

func (s *Server) handlePathSettingsCommand(c *Client, line string) (string, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SET") {
		return "", false
	}
	switch strings.ToUpper(fields[1]) {
	case "GRID", "NOISE", "PATHSAMPLES":
		return s.runPreferenceCommand(c, line, s.handlePathSettingsCommandOwned)
	default:
		return "", false
	}
}

func (s *Server) handleSolarCommand(c *Client, line string) (string, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SET") || !strings.EqualFold(fields[1], "SOLAR") {
		return "", false
	}
	return s.runPreferenceCommand(c, line, s.handleSolarCommandOwned)
}

func (s *Server) handleDedupeCommand(c *Client, line string) (string, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 || !strings.EqualFold(fields[1], "DEDUPE") || (!strings.EqualFold(fields[0], "SET") && !strings.EqualFold(fields[0], "SHOW")) {
		return "", false
	}
	return s.runPreferenceCommand(c, line, s.handleDedupeCommandOwned)
}
