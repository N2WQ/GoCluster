// File role: Request-owned history matching state. Configuration transactions
// capture it once per page; archive scans never borrow mutable client rules.
package telnet

import (
	"crypto/sha256"
	"encoding/binary"

	"dxcluster/filter"
	"dxcluster/spot"
)

// historyFilterSnapshot owns detached rules and path settings only for one page.
// Its minimal client is newly constructed, so no live mutex or atomic is copied.
// Propagation observations belong to the server and remain current per candidate.
type historyFilterSnapshot struct {
	digest   [32]byte
	client   *Client
	matching clientMatchingSnapshot
}

// captureHistoryFilter requires the configuration transaction stripe. Lock order
// matches configuration publication: path, then filter. No borrowed rule escapes.
func (c *Client) captureHistoryFilter() historyFilterSnapshot {
	c.pathMu.RLock()
	c.filterMu.RLock()
	owned := &Client{
		callsign: c.callsign, grid: c.grid, gridDerived: c.gridDerived,
		gridCell: c.gridCell, gridCoarseCell: c.gridCoarseCell,
		noiseClass: c.noiseClass, pathMinObservationCount: c.pathMinObservationCount,
	}
	if c.filter != nil {
		value := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone().FilterValue()
		owned.filter = &value
	}
	c.filterMu.RUnlock()
	c.pathMu.RUnlock()
	matching := owned.captureMatchingSnapshot()
	owned.gridCell, owned.gridCoarseCell = matching.path.gridCell, matching.path.gridCoarseCell
	return historyFilterSnapshot{digest: owned.historyFilterDigest(), client: owned, matching: matching}
}

// historyFilterDigest requires the configuration transaction stripe. It borrows
// maps only while locked and retains only a fixed-size matching fingerprint.
func (c *Client) historyFilterDigest() [32]byte {
	c.pathMu.RLock()
	c.filterMu.RLock()
	cfg := filter.V6ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{
		Grid: c.grid, NoiseClass: c.noiseClass, PathMinObservationCount: c.pathMinObservationCount,
	})
	matching := c.filter.MatchSnapshot()
	state := pathState{grid: c.grid, gridDerived: c.gridDerived, gridCell: c.gridCell, gridCoarseCell: c.gridCoarseCell}
	valid := c.filter != nil
	c.filterMu.RUnlock()
	c.pathMu.RUnlock()
	geometry := matching.NearbyFilterActive() || matching.PathFilterActive()
	if geometry {
		state = resolveMatchingLocation(state)
	}
	// Self and bulletin controls do not change history's personal spot matches.
	cfg.Filters.AllowWWV, cfg.Filters.AllowWCY = filter.DefaultBoolDefault, filter.DefaultBoolDefault
	cfg.Filters.AllowAnnounce, cfg.Filters.AllowSelf = filter.DefaultBoolDefault, filter.DefaultBoolDefault
	configured := cfg.Fingerprint()
	var payload [38]byte
	copy(payload[:32], configured[:])
	if valid {
		payload[32] = 1
	}
	// Irrelevant lazy cache fills must not invalidate location-free searches.
	// Geometry users hash the same snapshot-local effective cells they evaluate.
	if geometry {
		binary.LittleEndian.PutUint16(payload[33:35], uint16(state.gridCell))
		binary.LittleEndian.PutUint16(payload[35:37], uint16(state.gridCoarseCell))
	}
	if state.gridDerived {
		payload[37] = 1
	}
	return sha256.Sum256(payload[:])
}

func (snapshot historyFilterSnapshot) matches(s *Server, sp *spot.Spot) bool {
	if sp == nil || snapshot.client == nil {
		return false
	}
	f := snapshot.client.filter
	if isSelfMatch(sp, snapshot.client.callsign) {
		return f.AllowsToxicity(sp)
	}
	if f == nil {
		return false
	}
	matched, _, _ := s.matchSnapshot(snapshot.matching, sp)
	return matched
}
