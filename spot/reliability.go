package spot

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// SpotterReliability holds per-spotter weights (0..1). Reporters below the configured
// minimum are ignored by call correction consensus.
type SpotterReliability map[string]float64

// Purpose: Load per-spotter reliability weights from a text file.
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
	for scanner.Scan() {
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

// Purpose: Return a reporter reliability weight with default 1.0.
// Key aspects: Normalizes reporter and falls back when absent.
// Upstream: call correction weighting logic.
// Downstream: NormalizeCallsign.
// reliabilityFor returns the weight for a reporter (defaults to 1.0).
func reliabilityFor(r SpotterReliability, reporter string) float64 {
	if r == nil {
		return 1.0
	}
	reporter = NormalizeCallsign(reporter)
	if reporter == "" {
		return 1.0
	}
	if w, ok := r[reporter]; ok {
		return w
	}
	return 1.0
}
