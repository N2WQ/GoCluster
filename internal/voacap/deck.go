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

// PathMethod is the VOACAP prediction method written to the METHOD card.
type PathMethod int

const (
	PathMethodCompleteSystem         PathMethod = 20
	PathMethodShortLongPathSmoothing PathMethod = 30
	pathMethodDistanceThresholdKM               = 7000.0
	pathMethodDistanceEpsilonKM                 = 1e-9
	earthMeanRadiusKM                           = 6371.0088
)

// PathDeckRequest describes one VOACAP path deck.
type PathDeckRequest struct {
	Comment       string
	Transmit      DeckEndpoint
	Receive       DeckEndpoint
	SSN           int
	Now           time.Time
	ForecastHours int
	// StartVOACAPHour is the optional first TIME-card hour in VOACAP's
	// 1..24 notation. Zero keeps the legacy default start hour of 1.
	StartVOACAPHour      int
	CenterFrequenciesMHz []float64
	// Method is the VOACAP METHOD card value. Zero preserves the historical
	// Method-30 default for experiment decks and direct callers.
	Method PathMethod
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
		Method:               PathMethodShortLongPathSmoothing,
	})
}

// BuildPathDeck builds a fixed-format VOACAP deck for a directed path. The
// CIRCUIT card mirrors the working PowerShell deck generator:
// coordinates are formatted before interpolation, then written without extra
// field-width padding because the Windows VOACAP engine is column-sensitive.
func BuildPathDeck(req PathDeckRequest) ([]byte, error) {
	if req.SSN < 0 {
		return nil, fmt.Errorf("SSN must be >= 0")
	}
	if req.ForecastHours <= 0 || req.ForecastHours > 24 {
		return nil, fmt.Errorf("forecast hours must be between 1 and 24")
	}
	startHour := req.StartVOACAPHour
	if startHour == 0 {
		startHour = 1
	}
	if startHour < 1 || startHour > 24 {
		return nil, fmt.Errorf("start VOACAP hour must be between 1 and 24")
	}
	endHour := endVOACAPHour(startHour, req.ForecastHours)
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
	method := req.Method
	if method == 0 {
		method = PathMethodShortLongPathSmoothing
	}
	if !validPathMethod(method) {
		return nil, fmt.Errorf("unsupported VOACAP method %d", method)
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
	fmt.Fprintf(&buf, "TIME      %5d%5d    1    1\n", startHour, endHour)
	fmt.Fprintf(&buf, "MONTH      %04d %d.00\n", now.Year(), int(now.Month()))
	fmt.Fprintf(&buf, "SUNSPOT    %d.\n", req.SSN)
	fmt.Fprintf(&buf, "LABEL     %-18s %-18s\n", voacapLabel(req.Transmit.Label), voacapLabel(req.Receive.Label))
	fmt.Fprintf(&buf, "CIRCUIT   %s   %s    %s    %s  S     0\n",
		formatVOACAPLatitude(req.Transmit.Latitude),
		formatVOACAPLongitude(req.Transmit.Longitude),
		formatVOACAPLatitude(req.Receive.Latitude),
		formatVOACAPLongitude(req.Receive.Longitude))
	fmt.Fprintln(&buf, "SYSTEM       1. 153. 3.00  50. 10.0 3.00 0.10")
	fmt.Fprintln(&buf, "FPROB      1.00 1.00 1.00 0.00")
	fmt.Fprintln(&buf, "ANTENNA       1    1    2   30     0.000[default\\Isotrope     ]  0.0    0.1000")
	fmt.Fprintln(&buf, "ANTENNA       2    2    2   30     0.000[default\\Isotrope     ]  0.0    0.0000")
	fmt.Fprintf(&buf, "FREQUENCY %s\n", formatVOACAPFrequencySlots(req.CenterFrequenciesMHz))
	fmt.Fprintf(&buf, "METHOD %8d    0\n", method)
	fmt.Fprintln(&buf, "EXECUTE")
	fmt.Fprintln(&buf, "QUIT")
	return buf.Bytes(), nil
}

// RecommendedPathMethod applies VOACAP's distance recommendation to the same
// directed endpoints that will be written to the CIRCUIT card.
func RecommendedPathMethod(transmit, receive DeckEndpoint) (PathMethod, float64, error) {
	distanceKM, err := GreatCircleDistanceKM(transmit, receive)
	if err != nil {
		return 0, 0, err
	}
	if distanceKM+pathMethodDistanceEpsilonKM >= pathMethodDistanceThresholdKM {
		return PathMethodShortLongPathSmoothing, distanceKM, nil
	}
	return PathMethodCompleteSystem, distanceKM, nil
}

// GreatCircleDistanceKM returns the spherical great-circle distance between
// two deck endpoints in kilometers.
func GreatCircleDistanceKM(a, b DeckEndpoint) (float64, error) {
	if !validEndpoint(a) {
		return 0, fmt.Errorf("first endpoint must have valid latitude/longitude")
	}
	if !validEndpoint(b) {
		return 0, fmt.Errorf("second endpoint must have valid latitude/longitude")
	}
	lat1 := degreesToRadians(a.Latitude)
	lat2 := degreesToRadians(b.Latitude)
	dLat := degreesToRadians(b.Latitude - a.Latitude)
	dLon := degreesToRadians(b.Longitude - a.Longitude)
	sinLat := math.Sin(dLat / 2)
	sinLon := math.Sin(dLon / 2)
	h := sinLat*sinLat + math.Cos(lat1)*math.Cos(lat2)*sinLon*sinLon
	if h > 1 {
		h = 1
	}
	return 2 * earthMeanRadiusKM * math.Atan2(math.Sqrt(h), math.Sqrt(1-h)), nil
}

// HourForUTC maps a UTC instant to VOACAP's 1..24 hour notation.
// VOACAP emits midnight as hour 24, while Go's time.Hour uses 0.
func HourForUTC(t time.Time) int {
	hour := t.UTC().Hour()
	if hour == 0 {
		return 24
	}
	return hour
}

func endVOACAPHour(startHour, forecastHours int) int {
	endHour := startHour + forecastHours - 1
	for endHour > 24 {
		endHour -= 24
	}
	return endHour
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

func validPathMethod(method PathMethod) bool {
	return method == PathMethodCompleteSystem || method == PathMethodShortLongPathSmoothing
}

func degreesToRadians(degrees float64) float64 {
	return degrees * math.Pi / 180
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
