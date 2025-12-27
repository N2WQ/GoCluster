package spot

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Purpose: Load quality priors for known-good callsigns.
// Key aspects: Parses text file and applies score deltas to the quality store.
// Upstream: main startup when priors file configured.
// Downstream: callQuality.Add and NormalizeCallsign.
// LoadCallQualityPriors seeds the quality store from a text file so
// known-good calls start with a positive bias. Expected format:
//
//	CALL SCORE [FREQ_KHZ]
//	- CALL: callsign (case-insensitive; normalized internally)
//	- SCORE: integer delta applied to the quality store
//	- FREQ_KHZ (optional): bin to apply the score to; omit/<=0 to apply globally
//
// Lines beginning with # are treated as comments. Returns the number of priors applied.
func LoadCallQualityPriors(path string, binSizeHz int) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	applied := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		call := NormalizeCallsign(fields[0])
		if call == "" {
			continue
		}
		score, err := strconv.Atoi(fields[1])
		if err != nil || score == 0 {
			continue
		}
		freqHz := 0.0
		if len(fields) >= 3 {
			if v, err := strconv.ParseFloat(fields[2], 64); err == nil {
				freqHz = v * 1000.0
			}
		}
		callQuality.Add(call, freqHz, binSizeHz, score)
		applied++
	}
	if err := scanner.Err(); err != nil {
		return applied, fmt.Errorf("reading priors: %w", err)
	}
	return applied, nil
}
