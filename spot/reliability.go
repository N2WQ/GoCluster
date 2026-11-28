// File role: Loads immutable receiver reliability tables for correction evidence.
// Crawler notes: Invalid numerical assets must fail their startup/replay owner;
// finite clamps and unrelated malformed-line skipping preserve the text contract.
// Related docs: spot/README.md, docs/decisions/ADR-0290-correction-numerical-asset-integrity.md.
// Related tests: spot/numerical_integrity*_test.go.
package spot

import (
	"bufio"
	"bytes"
	"dxcluster/strutil"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

// SpotterReliability holds per-spotter weights (0..1). Reporters below the configured
// minimum are ignored by call correction consensus. Callers must not mutate a
// borrowed table while a resolver uses it; loaded tables are read-only assets.
type SpotterReliability map[string]float64

// Both correction formats are distributed through Git LFS. Check before
// parsing: the permissive weights parser otherwise accepts "size N" as a row.
func rejectCorrectionLFSPointer(path string, data []byte) error {
	const header = "version https://git-lfs.github.com/spec/v1"
	rest, found := bytes.CutPrefix(data, []byte(header))
	if found && (len(rest) == 0 || rest[0] == '\n' || bytes.HasPrefix(rest, []byte("\r\n"))) {
		return fmt.Errorf("%s is a Git LFS pointer; use GitHub's Download raw file button on the matching release tag to get the data", path)
	}
	return nil
}

// LoadSpotterReliability loads per-spotter reliability weights from a text file.
// Key aspects: Parses SPOTTER WEIGHT lines and clamps to [0,1].
// Upstream: main startup when reliability file configured.
// Downstream: NormalizeCallsign and map assignment.
// LoadSpotterReliability loads spotter weights from a text file to down-weight
// noisy reporters. Format per line:
//
//	SPOTTER WEIGHT
//
// Lines starting with # are ignored. Returns the populated map and count applied.
func LoadSpotterReliability(path string) (SpotterReliability, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	out := make(SpotterReliability)
	applied := 0
	scanner := bufio.NewScanner(f)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		if lineNumber == 1 {
			if err := rejectCorrectionLFSPointer(path, scanner.Bytes()); err != nil {
				return nil, 0, err
			}
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		spotter := NormalizeCallsign(fields[0])
		if spotter == "" {
			continue
		}
		weight, err := strconv.ParseFloat(fields[1], 64)
		// ParseFloat range overflow returns infinity together with ErrRange.
		// Refuse it before the unrelated malformed-token skip or finite clamps.
		if math.IsNaN(weight) || math.IsInf(weight, 0) {
			return nil, 0, fmt.Errorf("reliability line %d (%s): weight must be finite", lineNumber, spotter)
		}
		if err != nil {
			continue
		}
		if weight < 0 {
			weight = 0
		}
		if weight > 1 {
			weight = 1
		}
		out[spotter] = weight
		applied++
	}
	if err := scanner.Err(); err != nil {
		return out, applied, fmt.Errorf("reading reliability: %w", err)
	}
	return out, applied, nil
}

// reliabilityFor applies the same exact and marker-free lookup to the global table.
func reliabilityFor(r SpotterReliability, reporter string) float64 {
	return reliabilityForTables(nil, r, reporter)
}

// reliabilityForMode returns the weight for the reporter in the given mode.
// For each spelling, the mode-specific table takes precedence over the global table.
func reliabilityForMode(global, cw, rtty SpotterReliability, mode, reporter string) float64 {
	switch strutil.NormalizeUpper(mode) {
	case "CW":
		return reliabilityForTables(cw, global, reporter)
	case "RTTY":
		return reliabilityForTables(rtty, global, reporter)
	default:
		return reliabilityFor(global, reporter)
	}
}

func reliabilityForTables(mode, global SpotterReliability, reporter string) float64 {
	if len(mode) == 0 && len(global) == 0 {
		return 1.0
	}
	reporter = NormalizeCallsign(reporter)
	if reporter == "" {
		return 1.0
	}
	// An exact entry in either table wins over any alias. The fallback
	// changes only the lookup key, so numeric IDs and evidence identity survive.
	if w, ok := reliabilityLookupExact(mode, global, reporter); ok {
		return w
	}
	if bare, tagged := strings.CutSuffix(reporter, "-#"); tagged && bare != "" && !strings.HasSuffix(bare, "-#") {
		if w, ok := reliabilityLookupExact(mode, global, bare); ok {
			return w
		}
	}
	return 1.0
}

func reliabilityLookupExact(mode, global SpotterReliability, reporter string) (float64, bool) {
	if w, ok := mode[reporter]; ok {
		return w, true
	}
	w, ok := global[reporter]
	return w, ok
}
