// File role: Coherent, allocation-free personal matching captures for fan-out.
// Immutable programs and scalar controls/path inputs outlive the short locks;
// prediction and comment matching never run under broadcast-visible locks.
package telnet

import (
	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

type clientMatchingSnapshot struct {
	filter filter.MatchSnapshot
	path   pathState
}

func (c *Client) captureMatchingSnapshot() clientMatchingSnapshot {
	if c == nil {
		return clientMatchingSnapshot{}
	}
	c.pathMu.RLock()
	c.filterMu.RLock()
	snapshot := clientMatchingSnapshot{
		filter: c.filter.MatchSnapshot(),
		path: pathState{
			grid: c.grid, gridDerived: c.gridDerived,
			gridCell: c.gridCell, gridCoarseCell: c.gridCoarseCell,
			noiseClass: c.noiseClass, pathMinObservationCount: c.pathMinObservationCount,
		},
	}
	c.filterMu.RUnlock()
	c.pathMu.RUnlock()
	if snapshot.filter.PathFilterActive() {
		snapshot.path = c.resolvePathSnapshot(snapshot.path)
	}
	if snapshot.filter.NearbyFilterActive() || snapshot.filter.PathFilterActive() {
		snapshot.path = resolveMatchingLocation(snapshot.path)
	}
	return snapshot
}

// Resolve only captured GRID values, after releasing publication locks. Each
// resolution is independent; an unavailable coarse cell cannot erase fine
// geometry. NEARBY never publishes caches or depends on the PATH predictor.
func resolveMatchingLocation(state pathState) pathState {
	if state.gridCell == pathreliability.InvalidCell {
		state.gridCell = pathreliability.EncodeCell(state.grid)
	}
	if state.gridCoarseCell == pathreliability.InvalidCell {
		state.gridCoarseCell = pathreliability.EncodeCoarseCell(state.grid)
	}
	return state
}

func (s *Server) matchSnapshot(snapshot clientMatchingSnapshot, sp *spot.Spot) (bool, pathPrediction, bool) {
	pathClass := filter.PathClassInsufficient
	var prediction pathPrediction
	var predicted bool
	if sp != nil && snapshot.filter.PathFilterActive() {
		prediction, predicted = s.computePathPredictionForState(snapshot.path, sp)
		if predicted {
			pathClass = pathClassFromPrediction(prediction)
		}
	}
	return snapshot.filter.MatchesWithContext(sp, filter.EvaluationContext{
		PathClass: pathClass, FineCell: snapshot.path.gridCell, CoarseCell: snapshot.path.gridCoarseCell,
	}), prediction, predicted
}
