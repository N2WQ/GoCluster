package voacap

import (
	"bytes"
	"fmt"
	"math"
	"time"
)

func BuildExperimentDeck(cfg ExperimentConfig, smoothedSSN float64, now time.Time) ([]byte, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if math.IsNaN(smoothedSSN) || math.IsInf(smoothedSSN, 0) || smoothedSSN < 0 {
		return nil, fmt.Errorf("smoothed SSN must be finite and >= 0")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()

	var buf bytes.Buffer
	fmt.Fprintln(&buf, "COMMENT    Ham FT8/CW baseline VOACAP deck: Boston to Warsaw.")
	fmt.Fprintln(&buf, "LINEMAX      55       number of lines-per-page")
	fmt.Fprintln(&buf, "COEFFS    CCIR")
	fmt.Fprintf(&buf, "TIME          1%5d    1    1\n", cfg.ForecastHours)
	fmt.Fprintf(&buf, "MONTH      %04d %d.00\n", now.Year(), int(now.Month()))
	fmt.Fprintf(&buf, "SUNSPOT    %.0f.\n", math.Round(smoothedSSN))
	fmt.Fprintln(&buf, "LABEL     BOSTON              WARSAW")
	fmt.Fprintln(&buf, "CIRCUIT   42.36N    71.06W    52.23N    21.01E  S     0")
	fmt.Fprintln(&buf, "SYSTEM       1. 145. 0.10  90. 10.0 3.00 0.10")
	fmt.Fprintln(&buf, "FPROB      1.00 1.00 1.00 0.00")
	fmt.Fprintln(&buf, "ANTENNA       1    1    2   30     0.000[default\\Isotrope     ]  0.0    0.1000")
	fmt.Fprintln(&buf, "ANTENNA       2    2    2   30     0.000[default\\Isotrope     ]  0.0    0.0000")
	fmt.Fprintf(&buf, "FREQUENCY %s\n", formatVOACAPFrequencySlots(cfg.CenterFrequenciesMHz))
	fmt.Fprintln(&buf, "METHOD       30    0")
	fmt.Fprintln(&buf, "EXECUTE")
	fmt.Fprintln(&buf, "QUIT")
	return buf.Bytes(), nil
}

func formatVOACAPFrequencySlots(centerFrequencies []float64) string {
	var buf bytes.Buffer
	for _, freq := range centerFrequencies {
		fmt.Fprintf(&buf, "%5.2f", freq)
	}
	for i := len(centerFrequencies); i < maxExperimentCenterFrequencies; i++ {
		fmt.Fprintf(&buf, "%5.2f", 0.0)
	}
	return buf.String()
}
