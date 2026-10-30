// File role: Login-only generation-seven reset/restore and detached admission.
// The caller retains the full-SSID login lease through registration. A failure
// protects the original; historical reset notices are delivered separately.
package telnet

import (
	"errors"
	"fmt"
	"time"

	"dxcluster/filter"
)

func (s *Server) prepareV7LoginRuntime(c *Client, record *filter.V7UserRecord, now time.Time) (*Client, string, error) {
	canonical, goFilter, ccFilter, err := filter.PrepareV7Configuration(record.Configuration)
	if err != nil {
		return nil, "", err
	}
	value := goFilter
	selected := s.effectiveV7Dialect(canonical.Settings)
	if selected == DialectCC {
		value = ccFilter
	}
	prepared, warning := s.prepareConfigurationRuntime(c, filter.ConfigurationFromFilter(&value, canonical.Settings), now, false)
	prepared.callsign, prepared.address, prepared.server = c.callsign, c.address, s
	prepared.filterBanks = &canonical.Filters
	prepared.configuredSettings = canonical.Settings
	prepared.configurationInitialized = true
	prepared.v7PresetReferences, prepared.v7Notice = record.Presets, record.Notice
	prepared.v7LibraryResetGeneration = c.v7LibraryResetGeneration
	prepared.lastLoginUTC, prepared.recentIPs = record.LastLoginUTC, boundedSessionIPs(record.RecentIPs)
	return prepared, warning, nil
}

// The enclosing restore owns the process preparation reservation. Both actual
// wrapped views and the concise login/notice must fit before a reset is durable.
func (s *Server) admitV7LoginRecord(c *Client, record *filter.V7UserRecord, now time.Time) error {
	prepared, _, err := s.prepareV7LoginRuntime(c, record, now)
	if err != nil {
		return err
	}
	prepared.v7LibraryResetGeneration = filter.V7ConfigurationVersion // Admit the maximal pending status.
	if err := s.v7ConfigurationReadbackFits(prepared, record.Configuration, prepared, record.Presets); err != nil {
		return err
	}
	if _, err := s.renderV7CCLoginOwned(prepared, record.Configuration, false); err != nil {
		return err
	}
	_, err = renderV7ResetNotice(record.Configuration, prepared.dialect, v7ResetNoticeEvents{profileGeneration: 7, libraryGeneration: 7}, false)
	return err
}

func (s *Server) newV7LoginRecord(dialect DialectName) (*filter.V7UserRecord, error) {
	configuration, err := filter.DefaultV7Configuration(filter.SettingsConfiguration{Dialect: string(dialect), DedupePolicy: s.effectiveDefaultDedupePolicy().label()})
	if err != nil {
		return nil, err
	}
	return &filter.V7UserRecord{ConfigurationVersion: filter.V7ConfigurationVersion, Configuration: configuration}, nil
}

func (s *Server) restoreV7ClientRecordOwned(c *Client, loginTime time.Time) restoredLogin {
	result := restoredLogin{}
	reservation, reserveErr := c.reserveConfigurationOutput()
	if reserveErr != nil {
		result.loadError = reserveErr
		result.warning = "Configuration preparation is busy; reconnect to retry. Your saved files are unchanged.\n"
		c.preserveRecordOnExit.Store(true)
		c.interrupt()
		return result
	}
	defer reservation.Release()
	var record *filter.V7UserRecord
	var outcome filter.V7UpgradeOutcome
	record, outcome, err := filter.UpgradeV7UserRecordForLoginWithAdmission(c.callsign, s.commitLoginRecord, func(candidate *filter.V7UserRecord) error { return s.admitV7LoginRecord(c, candidate, loginTime) })
	result.profileReset = outcome.ProfileReset
	c.v7LibraryResetGeneration = outcome.LibraryResetGeneration
	if outcome.LibraryError != nil {
		result.warning = "The shared preset reset is pending; preset commands are unavailable until it succeeds.\n"
	}
	if errors.Is(err, filter.ErrLoginUpgradeProfileMissing) {
		record, err = s.newV7LoginRecord(DialectGo)
		result.created = err == nil
		if err == nil {
			err = s.admitV7LoginRecord(c, record, loginTime)
		}
	}
	if err == nil {
		// Current records also need view admission before ordinary login metadata
		// is written. This path does not recreate defaults or reset a v7 record.
		err = s.admitV7LoginRecord(c, record, loginTime)
	}
	if err != nil {
		c.recordProtected = true
		c.preserveRecordOnExit.Store(true)
		result.loadError = err
		result.warning += "Your saved user record could not be read, admitted or reset. Using temporary defaults; changes in this session will not be saved. SAVE PRESET is unavailable.\n"
		var factoryErr error
		record, factoryErr = s.newV7LoginRecord(s.configurationDefaultDialect())
		if factoryErr != nil {
			result.loadError = errors.Join(err, factoryErr)
			c.interrupt()
			return result
		}
	} else {
		result.previousLogin = record.LastLoginUTC
		if len(record.RecentIPs) > 0 {
			result.previousIP = record.RecentIPs[0]
		}
		record.RecentIPs = filter.UpdateRecentIPs(record.RecentIPs, spotterIP(c.address))
		record.LastLoginUTC = loginTime
		payload, saveErr := filter.PrepareV7UserRecord(c.callsign, record)
		if saveErr == nil {
			saveErr = s.commitLoginRecord(payload)
		}
		if saveErr != nil {
			if result.created {
				c.configurationUnsaved = true
				result.warning += "Your starting settings are active but could not be saved. They may be lost when you reconnect.\n"
			} else {
				result.warning += "Restored your saved configuration, but could not save this login's timestamp/IP.\n"
			}
		} else if result.created {
			library, libraryErr := filter.ResetV7PresetLibraryForLogin(c.callsign)
			if libraryErr != nil {
				result.warning += "Your new profile was saved, but the shared preset reset is pending; preset commands are unavailable.\n"
			} else {
				c.v7LibraryResetGeneration = library.ResetGeneration
			}
		}
	}
	prepared, warning, prepareErr := s.prepareV7LoginRuntime(c, record, loginTime)
	if prepareErr != nil {
		result.loadError = fmt.Errorf("prepare temporary login: %w", prepareErr)
		c.interrupt()
		return result
	}
	// This client is not registered yet; retain its existing Filter identity.
	if c.filter == nil {
		c.filter = &filter.Filter{}
	}
	*c.filter = *prepared.filter
	c.filterBanks, c.configuredSettings, c.configurationInitialized = prepared.filterBanks, record.Configuration.Settings, true
	c.v7PresetReferences, c.v7Notice = record.Presets, record.Notice
	c.recentIPs, c.lastLoginUTC = boundedSessionIPs(record.RecentIPs), record.LastLoginUTC
	c.dialect = prepared.dialect
	c.setDedupePolicy(prepared.getDedupePolicy())
	c.grid, c.gridDerived = prepared.grid, prepared.gridDerived
	c.gridCell, c.gridCoarseCell = prepared.gridCell, prepared.gridCoarseCell
	c.noiseClass, c.pathMinObservationCount = prepared.noiseClass, prepared.pathMinObservationCount
	c.setSolarSummaryMinutes(record.Configuration.Settings.SolarSummaryMinutes, loginTime)
	c.refreshV7ConfigurationRevision()
	result.warning += warning
	return result
}
