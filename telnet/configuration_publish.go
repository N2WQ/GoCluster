// File role: Prepares exact configuration updates and publishes after disk commit.
// The caller retains the full-callsign transaction stripe throughout preparation,
// persistence and publication. Runtime caches never become writable preferences.
package telnet

import (
	"strings"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

func (s *Server) prepareConfigurationUpdate(c *Client, before, next filter.Configuration, now time.Time) (*Client, string) {
	f := next.FilterValue()
	prepared := &Client{filter: &f, dialect: c.dialect}
	prepared.setDedupePolicy(c.getDedupePolicy())
	state := c.pathSnapshot()
	prepared.grid, prepared.gridDerived = state.grid, state.gridDerived
	prepared.gridCell, prepared.gridCoarseCell = state.gridCell, state.gridCoarseCell
	prepared.noiseClass, prepared.pathMinObservationCount = state.noiseClass, state.pathMinObservationCount
	if before.Settings.Dialect != next.Settings.Dialect {
		prepared.dialect = normalizeDialectName(next.Settings.Dialect)
		if next.Settings.Dialect == "" {
			prepared.dialect = s.configurationDefaultDialect()
		}
	}
	if before.Settings.DedupePolicy != next.Settings.DedupePolicy {
		requested := parseDedupePolicy(next.Settings.DedupePolicy)
		if next.Settings.DedupePolicy == "" {
			requested = s.effectiveDefaultDedupePolicy()
		}
		prepared.setDedupePolicy(s.resolveDedupePolicy(requested))
	}
	if before.Settings.Grid != next.Settings.Grid {
		s.prepareUpdatedGrid(c, prepared, next.Settings.Grid)
	}
	if before.Settings.NoiseClass != next.Settings.NoiseClass {
		prepared.noiseClass = next.Settings.NoiseClass
		if prepared.noiseClass == "" {
			prepared.noiseClass = "QUIET"
		}
	}
	if before.Settings.PathMinObservationCount != next.Settings.PathMinObservationCount {
		prepared.pathMinObservationCount = 0
		if next.Settings.PathMinObservationCount > s.pathPredictorMinObservationCount() {
			prepared.pathMinObservationCount = next.Settings.PathMinObservationCount
		}
	}
	prepared.setSolarSummaryMinutes(next.Settings.SolarSummaryMinutes, now)
	return prepared, ""
}

func (s *Server) prepareUpdatedGrid(c, prepared *Client, configured string) {
	prepared.grid, prepared.gridDerived = configured, false
	if configured == "" && s.gridLookup != nil {
		if grid, derived, ok := s.gridLookup(c.callsign); ok {
			prepared.grid, prepared.gridDerived = strings.ToUpper(strings.TrimSpace(grid)), derived
		}
	}
	prepared.gridCell = pathreliability.EncodeCell(prepared.grid)
	prepared.gridCoarseCell = pathreliability.EncodeCoarseCell(prepared.grid)
}

// publishConfiguration has no fallible work. Stable Filter identity and the
// path→filter lock order protect fan-out readers, while the transaction stripe
// protects configuration metadata. Unchanged cadence retains the live solar
// clock, including any tick advanced by the broadcaster during disk persistence.
func (s *Server) publishConfiguration(c *Client, next filter.Configuration, prepared *Client, ref *filter.PresetReference, now time.Time) {
	s.publishPreparedConfiguration(c, next, prepared, ref, now, false)
}

// LOAD starts a fresh solar schedule even when the stored cadence is unchanged.
// Machine updates use the same publication path with resetSolar=false so an
// unchanged write preserves broadcaster progress made during disk persistence.
func (s *Server) publishPreparedConfiguration(c *Client, next filter.Configuration, prepared *Client, ref *filter.PresetReference, now time.Time, resetSolar bool) {
	s.publishConfigurationState(c, next.Settings, prepared, ref, now, resetSolar)
	c.refreshConfigurationRevision()
}

// State transfer is shared by versioned coordinators. The caller owns its
// transaction lease and refreshes the matching configuration/history digest.
func (s *Server) publishConfigurationState(c *Client, settings filter.SettingsConfiguration, prepared *Client, ref *filter.PresetReference, now time.Time, resetSolar bool) {
	c.initializeConfiguredSettings()
	solarChanged := c.configuredSettings.SolarSummaryMinutes != settings.SolarSummaryMinutes
	c.pathMu.Lock()
	c.filterMu.Lock()
	*c.filter = *prepared.filter
	c.grid, c.gridDerived = prepared.grid, prepared.gridDerived
	c.gridCell, c.gridCoarseCell = prepared.gridCell, prepared.gridCoarseCell
	c.noiseClass, c.pathMinObservationCount = prepared.noiseClass, prepared.pathMinObservationCount
	c.dialect = prepared.dialect
	c.setDedupePolicy(prepared.getDedupePolicy())
	if solarChanged || resetSolar {
		c.setSolarSummaryMinutes(settings.SolarSummaryMinutes, now)
	}
	c.configuredSettings = settings
	c.configurationInitialized = true
	c.presetReference = ref
	c.filterMu.Unlock()
	c.pathMu.Unlock()
}

// Human commands publish their staged runtime Filter directly, preserving
// the GRID-owned location rather than reconstructing unrelated runtime state.
// The SSID lease covers publication/save. Fan-out sees stable Filter identity
// under path→filter locks; unrelated edits never replace a broadcaster's tick.
// Publication transfers the staged collections to the live client. The command
// must discard the adapter and never mutate those collections after this call.
func (s *Server) publishHumanPreference(c, staged *Client, forceSolar bool) {
	s.publishHumanPreferenceState(c, staged, forceSolar)
	c.refreshConfigurationRevision()
}

func (s *Server) publishHumanPreferenceState(c, staged *Client, forceSolar bool) {
	c.pathMu.Lock()
	c.filterMu.Lock()
	if c.filter != nil && staged.filter != nil {
		*c.filter = *staged.filter
	}
	c.grid, c.gridDerived = staged.grid, staged.gridDerived
	c.gridCell, c.gridCoarseCell = staged.gridCell, staged.gridCoarseCell
	c.noiseClass, c.pathMinObservationCount = staged.noiseClass, staged.pathMinObservationCount
	c.dialect = staged.dialect
	c.setDedupePolicy(staged.getDedupePolicy())
	if forceSolar {
		c.setSolarSummaryMinutes(staged.configuredSettings.SolarSummaryMinutes, s.now())
	}
	c.configuredSettings, c.configurationInitialized = staged.configuredSettings, true
	c.filterMu.Unlock()
	c.pathMu.Unlock()
}
