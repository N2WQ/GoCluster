package voacap

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"time"
)

// DeckEndpoint is one VOACAP circuit endpoint. Latitude and longitude are
// decimal degrees; labels are rendered only in the human-readable LABEL card.
type DeckEndpoint struct {
	Label     string
	Latitude  float64
	Longitude float64
}

// PathDeckRequest describes one VOACAP method-30 path deck.
type PathDeckRequest struct {
	Comment              string
	Transmit             DeckEndpoint
	Receive              DeckEndpoint
	SSN                  int
	Now                  time.Time
	ForecastHours        int
	CenterFrequenciesMHz []float64
}

func BuildExperimentDeck(cfg ExperimentConfig, smoothedSSN float64, now time.Time) ([]byte, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if math.IsNaN(smoothedSSN) || math.IsInf(smoothedSSN, 0) || smoothedSSN < 0 {
		return nil, fmt.Errorf("smoothed SSN must be finite and >= 0")
	}
	return BuildPathDeck(PathDeckRequest{
		Comment:              "Ham FT8/CW baseline VOACAP deck: Boston to Warsaw.",
		Transmit:             DeckEndpoint{Label: "BOSTON", Latitude: 42.36, Longitude: -71.06},
		Receive:              DeckEndpoint{Label: "WARSAW", Latitude: 52.23, Longitude: 21.01},
		SSN:                  int(math.Round(smoothedSSN)),
		Now:                  now,
		ForecastHours:        cfg.ForecastHours,
		CenterFrequenciesMHz: cfg.CenterFrequenciesMHz,
	})
}

// BuildPathDeck builds a fixed-format VOACAP method-30 deck for a directed
// path. The CIRCUIT card mirrors the working PowerShell deck generator:
// coordinates are formatted before interpolation, then written without extra
// field-width padding because the Windows VOACAP engine is column-sensitive.
func BuildPathDeck(req PathDeckRequest) ([]byte, error) {
	if req.SSN < 0 {
		return nil, fmt.Errorf("SSN must be >= 0")
	}
	if req.ForecastHours <= 0 || req.ForecastHours > 24 {
		return nil, fmt.Errorf("forecast hours must be between 1 and 24")
	}
	if len(req.CenterFrequenciesMHz) == 0 {
		return nil, fmt.Errorf("center frequencies must not be empty")
	}
	if len(req.CenterFrequenciesMHz) > maxExperimentCenterFrequencies {
		return nil, fmt.Errorf("center frequencies must contain at most ten frequencies")
	}
	for i, freq := range req.CenterFrequenciesMHz {
		if math.IsNaN(freq) || math.IsInf(freq, 0) || freq <= 0 {
			return nil, fmt.Errorf("center frequency %d must be finite and > 0", i)
		}
	}
	if !validEndpoint(req.Transmit) {
		return nil, fmt.Errorf("transmit endpoint must have valid latitude/longitude")
	}
	if !validEndpoint(req.Receive) {
		return nil, fmt.Errorf("receive endpoint must have valid latitude/longitude")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	now := req.Now.UTC()
	comment := strings.TrimSpace(req.Comment)
	if comment == "" {
		comment = "GoCluster VOACAP path deck."
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "COMMENT    %s\n", comment)
	fmt.Fprintln(&buf, "LINEMAX      55       number of lines-per-page")
	fmt.Fprintln(&buf, "COEFFS    CCIR")
	fmt.Fprintf(&buf, "TIME          1%5d    1    1\n", req.ForecastHours)
	fmt.Fprintf(&buf, "MONTH      %04d %d.00\n", now.Year(), int(now.Month()))
	fmt.Fprintf(&buf, "SUNSPOT    %d.\n", req.SSN)
	fmt.Fprintf(&buf, "LABEL     %-18s %-18s\n", voacapLabel(req.Transmit.Label), voacapLabel(req.Receive.Label))
	fmt.Fprintf(&buf, "CIRCUIT   %s   %s    %s    %s  S     0\n",
		formatVOACAPLatitude(req.Transmit.Latitude),
		formatVOACAPLongitude(req.Transmit.Longitude),
		formatVOACAPLatitude(req.Receive.Latitude),
		formatVOACAPLongitude(req.Receive.Longitude))
	fmt.Fprintln(&buf, "SYSTEM       1. 145. 0.10  90. 10.0 3.00 0.10")
	fmt.Fprintln(&buf, "FPROB      1.00 1.00 1.00 0.00")
	fmt.Fprintln(&buf, "ANTENNA       1    1    2   30     0.000[default\\Isotrope     ]  0.0    0.1000")
	fmt.Fprintln(&buf, "ANTENNA       2    2    2   30     0.000[default\\Isotrope     ]  0.0    0.0000")
	fmt.Fprintf(&buf, "FREQUENCY %s\n", formatVOACAPFrequencySlots(req.CenterFrequenciesMHz))
	fmt.Fprintln(&buf, "METHOD       30    0")
	fmt.Fprintln(&buf, "EXECUTE")
	fmt.Fprintln(&buf, "QUIT")
	return buf.Bytes(), nil
}

func validEndpoint(endpoint DeckEndpoint) bool {
	return !math.IsNaN(endpoint.Latitude) &&
		!math.IsInf(endpoint.Latitude, 0) &&
		!math.IsNaN(endpoint.Longitude) &&
		!math.IsInf(endpoint.Longitude, 0) &&
		endpoint.Latitude >= -90 &&
		endpoint.Latitude <= 90 &&
		endpoint.Longitude >= -180 &&
		endpoint.Longitude <= 180
}

func voacapLabel(label string) string {
	label = strings.ToUpper(strings.TrimSpace(label))
	if label == "" {
		return "GOCLUSTER"
	}
	if len(label) > 18 {
		return label[:18]
	}
	return label
}

func formatVOACAPLatitude(lat float64) string {
	hemi := "N"
	if lat < 0 {
		hemi = "S"
		lat = -lat
	}
	return fmt.Sprintf("%05.2f%s", lat, hemi)
}

func formatVOACAPLongitude(lon float64) string {
	hemi := "E"
	if lon < 0 {
		hemi = "W"
		lon = -lon
	}
	return fmt.Sprintf("%06.2f%s", lon, hemi)
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
