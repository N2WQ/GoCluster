package pathreliability

import (
	"dxcluster/internal/testsupport"
	"testing"
)

func requireH3Mappings(t *testing.T) {
	t.Helper()
	if err := InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatalf("InitH3Mappings failed: %v", err)
	}
}

func requireDistinctPathCells(t *testing.T, userGrid string, dxGrid string) (CellID, CellID, CellID, CellID) {
	t.Helper()
	requireH3Mappings(t)
	userCell := EncodeCell(userGrid)
	dxCell := EncodeCell(dxGrid)
	userCoarse := EncodeCoarseCell(userGrid)
	dxCoarse := EncodeCoarseCell(dxGrid)
	if userCell == InvalidCell || dxCell == InvalidCell || userCoarse == InvalidCell || dxCoarse == InvalidCell {
		t.Fatalf("invalid H3 test cells for user=%s dx=%s: user=%d dx=%d userCoarse=%d dxCoarse=%d",
			userGrid, dxGrid, userCell, dxCell, userCoarse, dxCoarse)
	}
	if userCell == dxCell {
		t.Fatalf("test grids %s and %s collapse to the same fine H3 cell %d", userGrid, dxGrid, userCell)
	}
	if userCoarse == dxCoarse {
		t.Fatalf("test grids %s and %s collapse to the same coarse H3 cell %d", userGrid, dxGrid, userCoarse)
	}
	return userCell, dxCell, userCoarse, dxCoarse
}
