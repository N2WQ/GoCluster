// File role: Loads required shared geography without generating or writing tables.
// Startup owns initialization before any encoding consumer runs. Validation is
// per invocation; the existing process-wide mappings still initialize only once.
// Related docs: docs/decisions/ADR-0344-required-shared-geography.md.
package pathreliability

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// InitRequiredH3Mappings validates the explicitly configured tables and installs
// their captured cells on the first successful initialization. It never writes
// files or searches another directory. Relative paths retain caller-CWD meaning.
// Every invocation validates both inputs, even when mappings already exist;
// validation failures do not consume the legacy once-owned initialization.
func InitRequiredH3Mappings(dir string) error {
	return initRequiredH3Mappings(dir, func(path string) (io.ReadCloser, error) { return os.Open(path) })
}

// Keep the complete initialization path behind a request-local opener: cold
// process fault tests must cover publication from captured bytes, not only the
// decoder. No mutable global hook or alternate runtime initialization exists.
func initRequiredH3Mappings(dir string, open func(string) (io.ReadCloser, error)) error {
	fineCells, coarseCells, err := loadRequiredH3Tables(dir, open)
	if err != nil {
		return err
	}
	h3InitOnce.Do(func() {
		fine, buildErr := buildMapperFromCells(fineResolution, fineCells)
		if buildErr != nil {
			h3InitErr = buildErr
			return
		}
		coarse, buildErr := buildMapperFromCells(coarseResolution, coarseCells)
		if buildErr != nil {
			h3InitErr = buildErr
			return
		}
		fineMapper, coarseMapper = fine, coarse
	})
	return h3InitErr
}

// The opener is request-local so fault tests exercise the real bounded reader
// without replacing a process global. Each read consumes at most expected+1 bytes;
// the extra byte detects oversized or growing input without trusting Stat.
func loadRequiredH3Tables(dir string, open func(string) (io.ReadCloser, error)) (fine, coarse []uint64, err error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil, errors.New("h3_table_path must not be empty")
	}
	var errs []error
	for _, table := range []struct {
		res, count int
		cells      *[]uint64
	}{{coarseResolution, h3Res1Count, &coarse}, {fineResolution, h3Res2Count, &fine}} {
		path := h3TablePath(dir, table.res)
		cells, readErr := readRequiredH3Table(path, table.count, open)
		if readErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", path, readErr))
			continue
		}
		*table.cells = cells
	}
	if err := errors.Join(errs...); err != nil {
		return nil, nil, err
	}
	return fine, coarse, nil
}

func readRequiredH3Table(path string, wantCount int, open func(string) (io.ReadCloser, error)) (cells []uint64, err error) {
	file, err := open(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	data, err := io.ReadAll(io.LimitReader(file, int64(wantCount)*8+1))
	if err != nil {
		return nil, err
	}
	if len(data) > wantCount*8 {
		return nil, fmt.Errorf("h3 table %s exceeds %d bytes", path, wantCount*8)
	}
	return decodeH3Table(path, data, wantCount)
}
