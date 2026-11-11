package cty

import (
	"math"
	"testing"
)

func TestGrid4FromLatLon(t *testing.T) {
	tests := []struct {
		name   string
		lat    float64
		lon    float64
		want   string
		wantOK bool
	}{
		{name: "origin", lat: 0, lon: 0, want: "JJ00", wantOK: true},
		{name: "max_edge", lat: 89.9999, lon: 179.9999, want: "RR99", wantOK: true},
		{name: "north_pole_clamp", lat: 90, lon: 180, want: "RR99", wantOK: true},
		{name: "invalid_nan", lat: math.NaN(), lon: 0, want: "", wantOK: false},
		{name: "invalid_out_of_range", lat: 95, lon: 0, want: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Grid4FromLatLon(tt.lat, tt.lon)
			if ok != tt.wantOK {
				t.Fatalf("ok=%v want %v (grid=%q)", ok, tt.wantOK, got)
			}
			if ok && got != tt.want {
				t.Fatalf("grid=%q want %q", got, tt.want)
			}
		})
	}
}

func TestGrid4FromPrefixInfoShippedCoordinates(t *testing.T) {
	db := loadShippedCTY(t)
	for _, tc := range []struct{ key, grid string }{
		{"K1", "FN42"}, {"W6", "DM05"}, {"JA1", "PM95"},
		{"VE3", "FN03"}, {"VK", "PG66"}, {"VK2", "QF56"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			info, exists := db.Data[tc.key]
			if !exists {
				t.Fatal("required shipped coordinate missing")
			}
			grid, ok := Grid4FromPrefixInfo(&info)
			if !ok || grid != tc.grid || info != db.Data[tc.key] {
				t.Fatalf("%s = %s/%v; raw coordinate changed=%v", tc.key, grid, ok, info != db.Data[tc.key])
			}
			if tc.key == "VE3" && grid[:2] != "FN" {
				t.Fatal("VE3 field must be FN")
			}
		})
	}
}

func TestGrid4FromPrefixInfoBoundaryAndInvalid(t *testing.T) {
	for _, tc := range []struct {
		info *PrefixInfo
		grid string
	}{
		{nil, ""}, {&PrefixInfo{Latitude: math.NaN()}, ""},
		{&PrefixInfo{Longitude: math.Inf(1)}, ""}, {&PrefixInfo{Latitude: 91}, ""},
		{&PrefixInfo{}, "JJ00"},
		{&PrefixInfo{Latitude: -34.000001, Longitude: -151}, "QF55"},
		{&PrefixInfo{Latitude: -34, Longitude: -151}, "QF56"},
		{&PrefixInfo{Latitude: -33.999999, Longitude: -151}, "QF56"},
	} {
		grid, ok := Grid4FromPrefixInfo(tc.info)
		if grid != tc.grid || ok != (tc.grid != "") {
			t.Fatalf("%+v = %s/%v, want %s", tc.info, grid, ok, tc.grid)
		}
	}
}
