// File role: Prepares exact configuration updates and publishes after disk commit.
// The caller retains the full-callsign transaction stripe throughout preparation,
// persistence and publication. Runtime caches never become writable preferences.
package telnet

import (
	"maps"
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
	return prepared, prepareUpdatedNearby(c, prepared, before, next)
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

func prepareUpdatedNearby(c, prepared *Client, before, next filter.Configuration) string {
	locationsEqual := sameLocationRules(before.Filters, next.Filters)
	gridEqual := before.Settings.Grid == next.Settings.Grid
	c.filterMu.RLock()
	priorSnapshot := c.filter.NearbySnapshot
	priorFine, priorCoarse := c.filter.NearbyUserFine, c.filter.NearbyUserCoarse
	c.filterMu.RUnlock()
	if before.Filters.NearbyEnabled == next.Filters.NearbyEnabled && locationsEqual && gridEqual {
		// Location snapshots are immutable until replacement. DisableNearby
		// copies their maps, so sharing this runtime pointer retains restoration.
		prepared.filter.NearbySnapshot = priorSnapshot
		prepared.filter.NearbyUserFine, prepared.filter.NearbyUserCoarse = priorFine, priorCoarse
		return ""
	}
	if !next.Filters.NearbyEnabled {
		return ""
	}
	if before.Filters.NearbyEnabled && locationsEqual {
		prepared.filter.NearbySnapshot = priorSnapshot
	}
	fine, coarse := prepared.gridCell, prepared.gridCoarseCell
	if before.Filters.NearbyEnabled && gridEqual {
		fine, coarse = priorFine, priorCoarse
	}
	if fine == pathreliability.InvalidCell || coarse == pathreliability.InvalidCell {
		return nearbyLoginInactiveMsg
	}
	if err := prepared.filter.EnableNearby(fine, coarse); err != nil {
		return nearbyLoginInactiveMsg
	}
	return ""
}

func sameRules[K string | int](a, b filter.RuleSet[K]) bool {
	return a.AllowAll == b.AllowAll && a.BlockAll == b.BlockAll && maps.Equal(a.Allow, b.Allow) && maps.Equal(a.Block, b.Block)
}

func sameLocationRules(a, b filter.FilterConfiguration) bool {
	return sameRules(a.DXContinents, b.DXContinents) && sameRules(a.DEContinents, b.DEContinents) &&
		sameRules(a.DXZones, b.DXZones) && sameRules(a.DEZones, b.DEZones) &&
		sameRules(a.DXGrid2, b.DXGrid2) && sameRules(a.DEGrid2, b.DEGrid2) &&
		sameRules(a.DXDXCC, b.DXDXCC) && sameRules(a.DEDXCC, b.DEDXCC) &&
		sameRules(a.DXStates, b.DXStates) && sameRules(a.DEStates, b.DEStates)
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
	c.initializeConfiguredSettings()
	solarChanged := c.configuredSettings.SolarSummaryMinutes != next.Settings.SolarSummaryMinutes
	c.pathMu.Lock()
	c.filterMu.Lock()
	*c.filter = *prepared.filter
	c.grid, c.gridDerived = prepared.grid, prepared.gridDerived
	c.gridCell, c.gridCoarseCell = prepared.gridCell, prepared.gridCoarseCell
	c.noiseClass, c.pathMinObservationCount = prepared.noiseClass, prepared.pathMinObservationCount
	c.dialect = prepared.dialect
	c.setDedupePolicy(prepared.getDedupePolicy())
	if solarChanged || resetSolar {
		c.setSolarSummaryMinutes(next.Settings.SolarSummaryMinutes, now)
	}
	c.configuredSettings = next.Settings
	c.configurationInitialized = true
	c.presetReference = ref
	c.filterMu.Unlock()
	c.pathMu.Unlock()
	c.refreshConfigurationRevision()
}
