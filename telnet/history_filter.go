// File role: Request-owned history matching state. Configuration transactions
// capture it once per page; archive scans never borrow mutable client rules.
package telnet

import (
	"crypto/sha256"
	"encoding/binary"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// historyFilterSnapshot owns detached rules and path settings only for one page.
// Its minimal client is newly constructed, so no live mutex or atomic is copied.
// Propagation observations belong to the server and remain current per candidate.
type historyFilterSnapshot struct {
	digest [32]byte
	client *Client
}

// captureHistoryFilter requires the configuration transaction stripe. Lock order
// matches configuration publication: path, then filter. No borrowed rule escapes.
func (c *Client) captureHistoryFilter() historyFilterSnapshot {
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	owned := &Client{
		callsign: c.callsign, grid: c.grid, gridDerived: c.gridDerived,
		gridCell: historyEffectiveGridCell(c), gridCoarseCell: c.gridCoarseCell,
		noiseClass: c.noiseClass, pathMinObservationCount: c.pathMinObservationCount,
	}
	if c.filter != nil {
		value := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone().FilterValue()
		value.NearbyUserFine = c.filter.NearbyUserFine
		value.NearbyUserCoarse = c.filter.NearbyUserCoarse
		// NearbySnapshot restores settings on a later command; matching never uses
		// it, so the request need not retain that extra set of location maps.
		owned.filter = &value
	}
	return historyFilterSnapshot{digest: c.historyFilterDigestLocked(), client: owned}
}

// historyFilterDigest requires the configuration transaction stripe. It borrows
// maps only while locked and retains only a fixed-size matching fingerprint.
func (c *Client) historyFilterDigest() [32]byte {
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	return c.historyFilterDigestLocked()
}

func (c *Client) historyFilterDigestLocked() [32]byte {
	cfg := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{
		Grid: c.grid, NoiseClass: c.noiseClass, PathMinObservationCount: c.pathMinObservationCount,
	})
	// History's existing self-match exception bypasses the self toggle. Bulletin
	// controls and presentation preferences likewise cannot change spot matches.
	cfg.Filters.AllowWWV, cfg.Filters.AllowWCY = filter.DefaultBoolDefault, filter.DefaultBoolDefault
	cfg.Filters.AllowAnnounce, cfg.Filters.AllowSelf = filter.DefaultBoolDefault, filter.DefaultBoolDefault
	configured := cfg.Fingerprint()
	var payload [42]byte
	copy(payload[:32], configured[:])
	if c.filter != nil {
		payload[32] = 1
		binary.LittleEndian.PutUint16(payload[33:35], uint16(c.filter.NearbyUserFine))
		binary.LittleEndian.PutUint16(payload[35:37], uint16(c.filter.NearbyUserCoarse))
	}
	binary.LittleEndian.PutUint16(payload[37:39], uint16(historyEffectiveGridCell(c)))
	binary.LittleEndian.PutUint16(payload[39:41], uint16(c.gridCoarseCell))
	if c.gridDerived {
		payload[41] = 1
	}
	return sha256.Sum256(payload[:])
}

// Resolving the lazy cache locally avoids invalidating a search merely because
// another path read filled the cache without changing the effective location.
func historyEffectiveGridCell(c *Client) pathreliability.CellID {
	if c.gridCell != pathreliability.InvalidCell {
		return c.gridCell
	}
	return pathreliability.EncodeCell(c.grid)
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
	pathClass := filter.PathClassInsufficient
	if f.PathFilterActive() && !f.BlockAllPathClasses {
		pathClass = s.pathClassForClient(snapshot.client, sp)
	}
	return f.MatchesWithPath(sp, pathClass)
}
