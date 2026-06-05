package cluster

import (
	"testing"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestHydrateSpotPathCellsCachesFineAndCoarseCells(t *testing.T) {
	requirePathReportH3Mappings(t)
	s := &spot.Spot{
		DXMetadata: spot.CallMetadata{Grid: "EM12"},
		DEMetadata: spot.CallMetadata{Grid: "FN20"},
	}

	cells := hydrateSpotPathCells(s)
	if cells.dxFine != pathreliability.EncodeCell("EM12") || cells.deFine != pathreliability.EncodeCell("FN20") {
		t.Fatalf("fine cells dx=%d de=%d, want dx=%d de=%d", cells.dxFine, cells.deFine, pathreliability.EncodeCell("EM12"), pathreliability.EncodeCell("FN20"))
	}
	if cells.dxCoarse != pathreliability.EncodeCoarseCell("EM12") || cells.deCoarse != pathreliability.EncodeCoarseCell("FN20") {
		t.Fatalf("coarse cells dx=%d de=%d, want dx=%d de=%d", cells.dxCoarse, cells.deCoarse, pathreliability.EncodeCoarseCell("EM12"), pathreliability.EncodeCoarseCell("FN20"))
	}
	if s.DXCellID != uint16(cells.dxFine) || s.DECellID != uint16(cells.deFine) || s.DXCoarseCellID != uint16(cells.dxCoarse) || s.DECoarseCellID != uint16(cells.deCoarse) {
		t.Fatalf("spot cache mismatch dx=%d de=%d dxCoarse=%d deCoarse=%d cells=%+v", s.DXCellID, s.DECellID, s.DXCoarseCellID, s.DECoarseCellID, cells)
	}
}

func TestHydrateSpotFinePathCellsDoesNotPopulateCoarseCells(t *testing.T) {
	requirePathReportH3Mappings(t)
	s := &spot.Spot{
		DXMetadata: spot.CallMetadata{Grid: "EM12"},
		DEMetadata: spot.CallMetadata{Grid: "FN20"},
	}

	cells := hydrateSpotFinePathCells(s)
	if cells.dxFine == pathreliability.InvalidCell || cells.deFine == pathreliability.InvalidCell {
		t.Fatalf("expected fine cells to be populated, got %+v", cells)
	}
	if cells.dxCoarse != pathreliability.InvalidCell || cells.deCoarse != pathreliability.InvalidCell {
		t.Fatalf("expected returned coarse cells to remain unset, got %+v", cells)
	}
	if s.DXCoarseCellID != 0 || s.DECoarseCellID != 0 {
		t.Fatalf("expected spot coarse caches to remain empty, got dxCoarse=%d deCoarse=%d", s.DXCoarseCellID, s.DECoarseCellID)
	}
}

func TestHydrateSpotPathCellsRecomputesAfterMetadataInvalidation(t *testing.T) {
	requirePathReportH3Mappings(t)
	s := &spot.Spot{
		DXMetadata: spot.CallMetadata{Grid: "EM12"},
		DEMetadata: spot.CallMetadata{Grid: "FN20"},
	}

	first := hydrateSpotPathCells(s)
	s.DXMetadata.Grid = "QF56"
	s.DEMetadata.Grid = "JN58"
	s.InvalidateMetadataCache()
	second := hydrateSpotPathCells(s)

	if first.dxFine == second.dxFine || first.deFine == second.deFine || first.dxCoarse == second.dxCoarse || first.deCoarse == second.deCoarse {
		t.Fatalf("expected path cells to change after invalidation, first=%+v second=%+v", first, second)
	}
	if second.dxFine != pathreliability.EncodeCell("QF56") || second.deFine != pathreliability.EncodeCell("JN58") {
		t.Fatalf("recomputed fine cells=%+v", second)
	}
	if second.dxCoarse != pathreliability.EncodeCoarseCell("QF56") || second.deCoarse != pathreliability.EncodeCoarseCell("JN58") {
		t.Fatalf("recomputed coarse cells=%+v", second)
	}
}
