//go:build !windows && cgo

package pathreliability

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"dxcluster/internal/fsutil"

	"github.com/uber/h3-go/v4"
)

// H3Mapper provides deterministic uint16 proxy IDs for a resolution's H3 cells.
type H3Mapper struct {
	Res  int
	ToID map[uint64]CellID
	ToH3 map[CellID]uint64
}

var (
	h3InitOnce   sync.Once
	h3InitErr    error
	fineMapper   *H3Mapper
	coarseMapper *H3Mapper
)

// InitH3Mappings builds deterministic H3 mappings for fine and coarse resolutions.
// It must be called at startup before any Encode* functions are used.
func InitH3Mappings() error {
	return InitH3MappingsFromDir("data/h3")
}

// InitH3MappingsFromDir loads H3 tables from dir or regenerates them when missing.
func InitH3MappingsFromDir(dir string) error {
	h3InitOnce.Do(func() {
		trimmed := strings.TrimSpace(dir)
		if trimmed == "" {
			trimmed = "data/h3"
		}
		fineMapper, coarseMapper, h3InitErr = loadOrGenerateMappings(trimmed)
	})
	return h3InitErr
}

// NewH3Mapper returns a deterministic mapping for all cells at a resolution.
func NewH3Mapper(res int) (*H3Mapper, error) {
	if res < 0 || res > 15 {
		return nil, fmt.Errorf("h3map: invalid resolution %d", res)
	}
	res0, err := h3.Res0Cells()
	if err != nil {
		return nil, fmt.Errorf("h3map: res0 cells: %w", err)
	}
	var allCells []h3.Cell
	for _, base := range res0 {
		children, err := base.Children(res)
		if err != nil {
			return nil, fmt.Errorf("h3map: children for base cell %d: %w", base, err)
		}
		allCells = append(allCells, children...)
	}
	sort.Slice(allCells, func(i, j int) bool { return allCells[i] < allCells[j] })
	switch res {
	case 1:
		if len(allCells) != 842 {
			return nil, fmt.Errorf("h3map: unexpected res-1 cell count %d (want 842)", len(allCells))
		}
	case 2:
		if len(allCells) != 5882 {
			return nil, fmt.Errorf("h3map: unexpected res-2 cell count %d (want 5882)", len(allCells))
		}
	}
	cells := make([]uint64, len(allCells))
	for i, cell := range allCells {
		cells[i] = uint64(cell)
	}
	return buildMapperFromCells(res, cells)
}

func encodeH3Cell(grid string, mapper *H3Mapper) CellID {
	if mapper == nil {
		return InvalidCell
	}
	lat, lon, ok := gridCenterLatLon(grid)
	if !ok {
		return InvalidCell
	}
	cell, err := h3.LatLngToCell(h3.NewLatLng(lat, lon), mapper.Res)
	if err != nil {
		return InvalidCell
	}
	if id := mapper.ToID[uint64(cell)]; id != 0 {
		return id
	}
	return InvalidCell
}

func loadOrGenerateMappings(dir string) (*H3Mapper, *H3Mapper, error) {
	fineCells, err := loadH3Table(dir, fineResolution, h3Res2Count)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("h3map: load res-2 table: %w", err)
		}
		fineCells, err = generateH3Cells(fineResolution, h3Res2Count)
		if err != nil {
			return nil, nil, err
		}
		if err := writeH3Table(dir, fineResolution, fineCells); err != nil {
			return nil, nil, err
		}
	}
	coarseCells, err := loadH3Table(dir, coarseResolution, h3Res1Count)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("h3map: load res-1 table: %w", err)
		}
		coarseCells, err = generateH3Cells(coarseResolution, h3Res1Count)
		if err != nil {
			return nil, nil, err
		}
		if err := writeH3Table(dir, coarseResolution, coarseCells); err != nil {
			return nil, nil, err
		}
	}
	fine, err := buildMapperFromCells(fineResolution, fineCells)
	if err != nil {
		return nil, nil, err
	}
	coarse, err := buildMapperFromCells(coarseResolution, coarseCells)
	if err != nil {
		return nil, nil, err
	}
	return fine, coarse, nil
}

func generateH3Cells(res int, wantCount int) ([]uint64, error) {
	res0, err := h3.Res0Cells()
	if err != nil {
		return nil, fmt.Errorf("h3map: res0 cells: %w", err)
	}
	var all []h3.Cell
	for _, base := range res0 {
		children, err := base.Children(res)
		if err != nil {
			return nil, fmt.Errorf("h3map: children for base cell %d: %w", base, err)
		}
		all = append(all, children...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	if wantCount > 0 && len(all) != wantCount {
		return nil, fmt.Errorf("h3map: unexpected res-%d cell count %d (want %d)", res, len(all), wantCount)
	}
	cells := make([]uint64, len(all))
	for i, cell := range all {
		cells[i] = uint64(cell)
	}
	return cells, nil
}

func writeH3Table(dir string, res int, cells []uint64) error {
	if err := fsutil.EnsureParentDir(h3TablePath(dir, res), "h3map: create table dir"); err != nil {
		return err
	}
	path := h3TablePath(dir, res)
	tmp, err := os.CreateTemp(dir, fmt.Sprintf("res%d-*.bin", res))
	if err != nil {
		return fmt.Errorf("h3map: create temp table: %w", err)
	}
	defer func() {
		_ = os.Remove(tmp.Name())
	}()
	buf := make([]byte, 8)
	for _, cell := range cells {
		binary.LittleEndian.PutUint64(buf, cell)
		if _, err := tmp.Write(buf); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("h3map: write temp table: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("h3map: sync temp table: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("h3map: close temp table: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("h3map: rename table: %w", err)
	}
	return nil
}
