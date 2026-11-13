package voacap

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Native DECRED.FOR format1506 fixes the numeric and hemisphere columns.
// Decode those columns independently of the Go formatter so an echoed, but
// misplaced, hemisphere cannot pass this regression check.
func decodeCircuitCoordinates(card string) ([4]float64, error) {
	var coordinates [4]float64
	if len(card) != 55 || card[48] != 'S' || strings.TrimSpace(card[50:55]) != "0" {
		return coordinates, fmt.Errorf("invalid CIRCUIT length or trailing path fields: %q", card)
	}
	for i, field := range [][3]int{{10, 15, 15}, {16, 25, 25}, {26, 35, 35}, {36, 45, 45}} {
		value, err := strconv.ParseFloat(strings.TrimSpace(card[field[0]:field[1]]), 64)
		if err != nil {
			return coordinates, err
		}
		positive, negative := byte('N'), byte('S')
		if i%2 == 1 {
			positive, negative = 'E', 'W'
		}
		switch card[field[2]] {
		case positive:
		case negative:
			value = -value
		default:
			return coordinates, fmt.Errorf("invalid hemisphere in column %d", field[2]+1)
		}
		coordinates[i] = value
	}
	return coordinates, nil
}

func TestBuildPathDeckNativeCircuitColumns(t *testing.T) {
	cases := []struct {
		name   string
		tx, rx DeckEndpoint
		want   [4]float64
	}{
		{"east_to_west", DeckEndpoint{Latitude: 52.23, Longitude: 21.01}, DeckEndpoint{Latitude: 42.36, Longitude: -71.06}, [4]float64{52.23, 21.01, 42.36, -71.06}},
		{"west_to_east", DeckEndpoint{Latitude: 42.36, Longitude: -71.06}, DeckEndpoint{Latitude: 52.23, Longitude: 21.01}, [4]float64{42.36, -71.06, 52.23, 21.01}},
		{"south_east", DeckEndpoint{Latitude: -33.87, Longitude: 151.21}, DeckEndpoint{Latitude: 42.36, Longitude: -71.06}, [4]float64{-33.87, 151.21, 42.36, -71.06}},
		{"south_west", DeckEndpoint{Latitude: -33.87, Longitude: -151.21}, DeckEndpoint{Latitude: -42.36, Longitude: 71.06}, [4]float64{-33.87, -151.21, -42.36, 71.06}},
		{"zero", DeckEndpoint{}, DeckEndpoint{Latitude: -12.25, Longitude: -0.01}, [4]float64{0, 0, -12.25, -0.01}},
		{"extents", DeckEndpoint{Latitude: 90, Longitude: -180}, DeckEndpoint{Latitude: -90, Longitude: 180}, [4]float64{90, -180, -90, 180}},
		{"rounding", DeckEndpoint{Latitude: 42.3649, Longitude: -71.0649}, DeckEndpoint{Latitude: 52.2349, Longitude: 21.0149}, [4]float64{42.36, -71.06, 52.23, 21.01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deck, err := BuildPathDeck(PathDeckRequest{Transmit: tc.tx, Receive: tc.rx, SSN: 0, Now: time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC), ForecastHours: 24, CenterFrequenciesMHz: []float64{14.18}, Method: PathMethodCompleteSystem})
			if err != nil {
				t.Fatal(err)
			}
			var card string
			for _, line := range strings.Split(string(deck), "\n") {
				if strings.HasPrefix(line, "CIRCUIT") {
					card = line
					break
				}
			}
			got, err := decodeCircuitCoordinates(card)
			if err != nil || got != tc.want {
				t.Fatalf("native columns = %v, err=%v; want %v; card=%q", got, err, tc.want, card)
			}
		})
	}
}

func TestNativeCircuitOracleRejectsShiftedHemisphere(t *testing.T) {
	// The former card places W at47 and the path flag at50, rather than46/49.
	bad := "CIRCUIT   52.23N   021.01E    42.36N    071.06W  S     0"
	if _, err := decodeCircuitCoordinates(bad); err == nil {
		t.Fatal("native-column oracle accepted the former shifted card")
	}
}
