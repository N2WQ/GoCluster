// File role: Owns coherent both-bank capture, preparation and publication.
// The existing full-SSID lease orders preferences and metadata; immutable
// captures are expanded outside path/filter locks and never grant write authority.
package telnet

import (
	"errors"
	"fmt"
	"os"
	"time"

	"dxcluster/filter"
)

// Early routing precedes the transaction lease. Publication changes both fields
// under filterMu, so even a presence/selection probe must take that lock.
func (c *Client) configurationBankSelection() (bool, DialectName) {
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	return c.filterBanks != nil, c.dialect
}

func (c *Client) captureV7Values() (filter.Configuration, filter.V7FilterBanks, error) {
	if c == nil {
		return filter.Configuration{}, filter.V7FilterBanks{}, errors.New("native configuration unavailable")
	}
	c.initializeConfiguredSettings()
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	if c.filterBanks == nil || c.filter == nil {
		return filter.Configuration{}, filter.V7FilterBanks{}, errors.New("both-bank configuration unavailable")
	}
	return filter.ConfigurationFromFilter(c.filter, c.configuredSettings), *c.filterBanks, nil
}

func (c *Client) captureV7Configuration() (filter.V7Configuration, error) {
	active, banks, err := c.captureV7Values()
	if err != nil {
		return filter.V7Configuration{}, err
	}
	return active.CaptureV7(banks), nil
}

func (c *Client) refreshV7ConfigurationRevision() {
	active, banks, err := c.captureV7Values()
	if err != nil {
		return
	}
	c.observeConfigurationDigest(active.FingerprintV7(banks))
	c.observeHistoryDigest(c.historyFilterDigest())
}

func (c *Client) v7ConfigurationRevisionToken() (string, error) {
	if err := c.ensureConfigurationEpoch(); err != nil {
		return "", err
	}
	if _, _, err := c.captureV7Values(); err != nil {
		return "", err
	}
	c.refreshV7ConfigurationRevision()
	return fmt.Sprintf("%s-%d", c.configurationEpoch, c.configurationRevision), nil
}

func (s *Server) effectiveV7Dialect(settings filter.SettingsConfiguration) DialectName {
	if settings.Dialect == "" {
		return s.configurationDefaultDialect()
	}
	return normalizeDialectName(settings.Dialect)
}

func (s *Server) prepareV7ConfigurationUpdate(c *Client, before, next filter.V7Configuration, now time.Time) (*Client, error) {
	canonical, goFilter, ccFilter, err := filter.PrepareV7Configuration(next)
	if err != nil {
		return nil, err
	}
	selected := s.effectiveV7Dialect(canonical.Settings)
	value := goFilter
	if selected == DialectCC {
		value = ccFilter
	}
	c.filterMu.RLock()
	previous := filter.ConfigurationFromFilter(c.filter, before.Settings)
	c.filterMu.RUnlock()
	prepared, _ := s.prepareConfigurationUpdate(c, previous, filter.ConfigurationFromFilter(&value, canonical.Settings), now)
	prepared.dialect, prepared.filterBanks = selected, &canonical.Filters
	prepared.configuredSettings, prepared.configurationInitialized = canonical.Settings, true
	prepared.v7PresetReferences, prepared.v7Notice = c.v7PresetReferences, c.v7Notice
	prepared.v7LibraryResetGeneration = c.v7LibraryResetGeneration
	return prepared, nil
}

func (s *Server) prepareV7ConfigurationRecord(c *Client, cfg filter.V7Configuration, refs filter.V7PresetReferences) (*filter.PreparedUserRecord, error) {
	if c.recordProtected {
		return nil, errProtectedRecord
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %w", filter.ErrCurrentUserRecord, err)
	}
	if record == nil {
		// A current owner may repair an absent destination with its complete live
		// state. This is not the login upgrader and installs no new-user defaults.
		record = &filter.V7UserRecord{LastLoginUTC: c.lastLoginUTC, Notice: c.v7Notice}
	}
	record.ConfigurationVersion = filter.V7ConfigurationVersion
	record.Configuration, record.Presets = cfg, refs
	record.RecentIPs = filter.MergeRecentIPs(c.recentIPs, record.RecentIPs)
	return filter.PrepareV7UserRecord(c.callsign, record)
}

func (s *Server) publishV7Configuration(c *Client, next filter.V7Configuration, prepared *Client, refs filter.V7PresetReferences, now time.Time) {
	prepared.v7PresetReferences = refs
	s.publishConfigurationState(c, next.Settings, prepared, nil, now, false)
	c.refreshV7ConfigurationRevision()
	c.configurationUnsaved = false
}

func (c *Client) v7PresetStatuses(cfg filter.V7Configuration) map[string]readbackPresetStatus {
	return v7ReadbackPresetStatuses(cfg, c.v7PresetReferences)
}

func (c *Client) admitTemporaryV7Configuration(cfg filter.V7Configuration) error {
	return filter.AdmitV7UserRecord(&filter.V7UserRecord{
		ConfigurationVersion: filter.V7ConfigurationVersion,
		Configuration:        cfg, Presets: c.v7PresetReferences,
		RecentIPs: boundedSessionIPs(c.recentIPs), LastLoginUTC: c.lastLoginUTC,
		Notice: c.v7Notice,
	})
}

// Admission checks the complete native view even while GO is selected, so an
// accepted inactive CC bank will remain inspectable after a later switch.
// The enclosing operation already owns its bounded preparation reservation.
func (s *Server) v7HumanReadbackFits(c *Client, cfg filter.V7Configuration, prepared *Client, refs filter.V7PresetReferences) error {
	_, _, ccFilter, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		return err
	}
	view := newHumanPreferenceCandidate(prepared)
	view.callsign, view.server, view.filter = c.callsign, s, &ccFilter
	view.dialect, view.filterBanks = DialectCC, &cfg.Filters
	view.configuredSettings, view.configurationInitialized = cfg.Settings, true
	view.v7PresetReferences = refs
	_, err = s.nativeCCReadback(view, "CONFIG", "", s.autoReadPauseDuration)
	return err
}
