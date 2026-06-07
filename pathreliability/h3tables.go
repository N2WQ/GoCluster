// File role: Loads and validates precomputed H3 mapping tables for path
// reliability startup and platform fallback mappers.
// Crawler notes: Start here when H3 table files, expected cell counts, or
// startup validation of path prediction cell coverage changes.
// Related docs: pathreliability/README.md, data/h3/README.md,
// docs/decisions/ADR-0153-startup-config-diagnostics-and-gridstore-logging.md.
// Related tests: pathreliability/h3map_test.go.
package pathreliability

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	h3Res1Count = 842
	h3Res2Count = 5882
)

func h3TablePath(dir string, res int) string {
	return filepath.Join(dir, fmt.Sprintf("res%d.bin", res))
}

func loadH3Table(dir string, res int, wantCount int) ([]uint64, error) {
	path := h3TablePath(dir, res)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("h3 table %s has invalid length %d", path, len(data))
	}
	count := len(data) / 8
	if wantCount > 0 && count != wantCount {
		return nil, fmt.Errorf("h3 table %s has %d entries (want %d)", path, count, wantCount)
	}
	cells := make([]uint64, count)
	for i := 0; i < count; i++ {
		offset := i * 8
		cells[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
	}
	return cells, nil
}

func ValidateH3Tables(dir string) []error {
	trimmed := strings.TrimSpace(dir)
	if trimmed == "" {
		trimmed = "data/h3"
	}
	checks := []struct {
		res       int
		wantCount int
	}{
		{res: coarseResolution, wantCount: h3Res1Count},
		{res: fineResolution, wantCount: h3Res2Count},
	}
	errs := make([]error, 0, len(checks))
	for _, check := range checks {
		if _, err := loadH3Table(trimmed, check.res, check.wantCount); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", h3TablePath(trimmed, check.res), err))
		}
	}
	return errs
}

func buildMapperFromCells(res int, cells []uint64) (*H3Mapper, error) {
	if res < 0 || res > 15 {
		return nil, fmt.Errorf("h3map: invalid resolution %d", res)
	}
	m := &H3Mapper{
		Res:  res,
		ToID: make(map[uint64]CellID, len(cells)),
		ToH3: make(map[CellID]uint64, len(cells)),
	}
	for i, cell := range cells {
		id := CellID(i + 1) // 1-based; 0 reserved for invalid
		m.ToID[cell] = id
		m.ToH3[id] = cell
	}
	return m, nil
}
