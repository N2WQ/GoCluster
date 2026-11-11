// File role: Prepares output location without observing or replaying path data.
// A corrected call can retain an attached grid on a lookup miss, but that
// inherited value must not become a reported cache observation for the new call.
// Related docs: spot/README.md#live-stabilizer-output, docs/decisions/ADR-0341-stabilizer-live-output.md.
// Related tests: internal/cluster/corrected_grid_delivery_test.go.
package cluster

import (
	"strings"

	"dxcluster/spot"
)

// resetCorrectedDXClassification drops facts belonging to the replaced call.
// Grid retention follows the owner-selected lookup rule independently of CTY
// classification. DE facts remain intact; cached cells are rebuilt before use.
func resetCorrectedDXClassification(s *spot.Spot) {
	grid, derived := s.DXMetadata.Grid, s.DXMetadata.GridDerived
	s.DXMetadata = spot.CallMetadata{Grid: grid, GridDerived: derived}
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
}

// prepareOutputLocation reuses bounded lookup ownership without path learning.
// Release consumers use only the cache-first asynchronous hook: cache misses
// offer background work and return, never waiting for storage or worker replies.
// Existing cache/admission mutex contention is not a hard elapsed-time bound.
func (p *outputPipeline) prepareOutputLocation(ctx *outputSpotContext, delayed bool) {
	s := ctx.spot
	syncLookup := p.gridLookupSync
	if delayed {
		syncLookup = nil
	}
	changed := false
	if ctx.dxIdentityChanged || strings.TrimSpace(s.DXMetadata.Grid) == "" {
		call := normalizeCallForMetadata(normalizedDXCall(s))
		if grid, derived, ok := lookupGridUnified(call, syncLookup, p.gridLookup); ok &&
			!strings.EqualFold(strings.TrimSpace(s.DXMetadata.Grid), strings.TrimSpace(grid)) {
			s.DXMetadata.Grid, s.DXMetadata.GridDerived = grid, derived
			changed = true
		}
	}
	if strings.TrimSpace(s.DEMetadata.Grid) == "" {
		call := s.DECallNorm
		if call == "" {
			call = s.DECall
		}
		if grid, derived, ok := lookupGridUnified(normalizeCallForMetadata(call), syncLookup, p.gridLookup); ok {
			s.DEMetadata.Grid, s.DEMetadata.GridDerived = grid, derived
			changed = true
		}
	}
	if changed {
		s.InvalidateMetadataCache()
		s.EnsureNormalized()
	}
	if ctx.dxIdentityChanged {
		// A license/CTY refresh can invalidate cells even when grid text stays
		// equal. Rehydrate both endpoints, without repeating predictor updates.
		hydrateSpotPathCells(s)
	}
}
