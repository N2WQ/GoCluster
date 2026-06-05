// File role: Hydrates per-spot H3 path cells for the output and PSKReporter
// path-only ingest paths before they update path reliability state.
// Crawler notes: Start here when path metrics and predictor updates appear to
// duplicate grid-to-H3 work or when spot-owned path cell cache behavior changes.
// Related docs: pathreliability/README.md, docs/decisions/ADR-0146-h3-path-cell-duplicate-work-removal.md.
// Related tests: internal/cluster/path_cells_test.go, internal/cluster/path_report_metrics_test.go.
package cluster

import (
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

type spotPathCells struct {
	dxFine   pathreliability.CellID
	deFine   pathreliability.CellID
	dxCoarse pathreliability.CellID
	deCoarse pathreliability.CellID
}

// hydrateSpotFinePathCells preserves the existing output-pipeline contract that
// fine path cells are available on fanout spots when path prediction is enabled.
func hydrateSpotFinePathCells(s *spot.Spot) spotPathCells {
	if s == nil {
		return spotPathCells{}
	}
	s.EnsureNormalized()
	if s.DXCellID == 0 {
		s.DXCellID = uint16(pathreliability.EncodeCell(s.DXGridNorm))
	}
	if s.DECellID == 0 {
		s.DECellID = uint16(pathreliability.EncodeCell(s.DEGridNorm))
	}
	return spotPathCells{
		dxFine: pathreliability.CellID(s.DXCellID),
		deFine: pathreliability.CellID(s.DECellID),
	}
}

// hydrateSpotPathCells computes the fine and coarse path cells at most once for
// a mutable pipeline spot. The cache is spot-owned and is cleared by
// Spot.InvalidateMetadataCache when metadata grids change.
func hydrateSpotPathCells(s *spot.Spot) spotPathCells {
	cells := hydrateSpotFinePathCells(s)
	if s == nil {
		return cells
	}
	if s.DXCoarseCellID == 0 {
		s.DXCoarseCellID = uint16(pathreliability.EncodeCoarseCell(s.DXGridNorm))
	}
	if s.DECoarseCellID == 0 {
		s.DECoarseCellID = uint16(pathreliability.EncodeCoarseCell(s.DEGridNorm))
	}
	cells.dxCoarse = pathreliability.CellID(s.DXCoarseCellID)
	cells.deCoarse = pathreliability.CellID(s.DECoarseCellID)
	return cells
}
