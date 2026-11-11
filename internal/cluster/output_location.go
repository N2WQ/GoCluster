// File role: Prepares output location without observing or replaying path data.
// A corrected call requires a completed lookup before selecting its own grid;
// inherited grids never become observations for the replacement identity.
// Related docs: spot/README.md#live-stabilizer-output, docs/decisions/ADR-0343-corrected-grid-precedence.md.
// Related tests: internal/cluster/corrected_grid_delivery_test.go.
package cluster

import (
	"strings"

	"dxcluster/cty"
	"dxcluster/spot"
)

// resetCorrectedDXClassification drops facts belonging to the replaced call.
// Location selection follows separately using the captured CTY image. The old
// grid remains only until that stage; DE facts and cells are rebuilt before use.
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
	if ctx.dxIdentityChanged {
		call := normalizeCallForMetadata(normalizedDXCall(s))
		grid, derived, ok := lookupGridUnified(call, syncLookup, p.gridLookup)
		if !ok {
			// Pending/failed lookups cannot establish whether a reported grid
			// exists. Neither the old identity's grid nor CTY may substitute.
			grid, derived = "", false
		} else if derived {
			// Stored estimates are not reported evidence. Recompute from the
			// immutable CTY image that validated this correction, not a reload.
			grid, ok = cty.Grid4FromPrefixInfo(effectivePrefixInfo(ctx.ctyDB, p.metaCache, call))
			derived = ok
		}
		if s.DXMetadata.Grid != grid || s.DXMetadata.GridDerived != derived {
			s.DXMetadata.Grid, s.DXMetadata.GridDerived = grid, derived
			changed = true
		}
	} else if strings.TrimSpace(s.DXMetadata.Grid) == "" {
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
