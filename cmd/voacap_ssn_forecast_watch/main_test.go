package main

import (
	"os"
	"path/filepath"
	"testing"

	"dxcluster/internal/voacap"
)

func TestStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	want := voacap.ForecastState{
		ETag:                       `"abc"`,
		LastRawSSN:                 112,
		EWMA:                       108.5,
		EWMAInitialized:            true,
		LastForecastSSN:            108.5,
		LastForecastSSNInitialized: true,
	}
	if err := saveState(path, want); err != nil {
		t.Fatalf("saveState() error: %v", err)
	}
	got, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState() error: %v", err)
	}
	if got.ETag != want.ETag || got.LastRawSSN != want.LastRawSSN || got.EWMA != want.EWMA || !got.LastForecastSSNInitialized {
		t.Fatalf("state round trip = %#v, want %#v", got, want)
	}
}

func TestForecastOutputSizeMissingIsZero(t *testing.T) {
	if got := forecastOutputSize(filepath.Join(t.TempDir(), "missing.out")); got != 0 {
		t.Fatalf("forecastOutputSize missing = %d, want 0", got)
	}
}

func TestForecastOutputSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.dat")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write output: %v", err)
	}
	if got := forecastOutputSize(path); got != 3 {
		t.Fatalf("forecastOutputSize = %d, want 3", got)
	}
}
