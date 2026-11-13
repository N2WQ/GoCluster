package voacap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	sunspotMonitorStateVersion  = 3
	maxSunspotMonitorStateBytes = 64 * 1024
	solarCountStateSource       = "NOAA SWPC sunspot_report.json reported groups"
	solarCountStateUnits        = "local reported-group index"
	solarCountStateConversion   = "coherent station+observatory+Obsid 10G+S; elapsed-time EWMA v1"
)

type solarMonitorStateIdentity struct {
	Source              string `json:"source"`
	URL                 string `json:"url"`
	Units               string `json:"units"`
	Conversion          string `json:"conversion"`
	EWMAHalfLifeSeconds int64  `json:"ewma_half_life_seconds"`
}

func solarStateIdentity(cfg SunspotMonitorConfig) solarMonitorStateIdentity {
	return solarMonitorStateIdentity{
		Source: solarCountStateSource, URL: cfg.URL, Units: solarCountStateUnits,
		Conversion: solarCountStateConversion, EWMAHalfLifeSeconds: int64(cfg.EWMAHalfLife / time.Second),
	}
}

// sunspotMonitorState is one bounded restart-continuity record, not a history.
// Its watermark prevents replaying an accepted observation, including corrections.
// Identity mismatches reject all data and validators. Errors and alarm entry/
// recovery state remain live diagnostics, so a restart rechecks source health.
type sunspotMonitorState struct {
	Version            int                       `json:"version"`
	Identity           solarMonitorStateIdentity `json:"identity"`
	ETag               string                    `json:"etag,omitempty"`
	LastModified       string                    `json:"last_modified,omitempty"`
	LastFetchAtUTC     time.Time                 `json:"last_fetch_at_utc,omitempty"`
	LastObservedAtUTC  time.Time                 `json:"last_observed_at_utc,omitempty"`
	LastRawCountIndex  float64                   `json:"last_raw_count_index,omitempty"`
	SmoothedCountIndex float64                   `json:"smoothed_count_index,omitempty"`
	EWMAInitialized    bool                      `json:"ewma_initialized,omitempty"`
	Input              SolarInputSnapshot        `json:"input"`
	InputInitialized   bool                      `json:"input_initialized,omitempty"`
	LastForecastAtUTC  time.Time                 `json:"last_forecast_at_utc,omitempty"`
	LastForecastReason string                    `json:"last_forecast_reason,omitempty"`
}

// LoadState is called before background polling. Missing state is a cold start;
// malformed, old Wolf, or incompatible state returns a warning-worthy error and
// leaves the monitor's current state untouched, including its HTTP validators.
func (m *SunspotMonitor) LoadState() (bool, error) {
	if m == nil {
		return false, errors.New("nil sunspot monitor")
	}
	if m.cfg.StatePath == "" {
		return false, nil
	}
	m.pollMu.Lock()
	defer m.pollMu.Unlock()
	state, found, err := loadSunspotMonitorState(m.cfg.StatePath)
	if err != nil || !found {
		return found, err
	}
	if state.Identity != solarStateIdentity(m.cfg) {
		return true, errors.New("VOACAP solar state source, units or parameters differ from current configuration")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	m.lastError = ""
	return true, nil
}

func (m *SunspotMonitor) saveState() error {
	if m.cfg.StatePath == "" {
		return nil
	}
	if err := saveSunspotMonitorState(m.cfg.StatePath, m.stateSnapshot()); err != nil {
		if m.cfg.Logger != nil {
			m.cfg.Logger.Printf("Warning: VOACAP SSN state save failed (%s): %v", m.cfg.StatePath, err)
		}
		return fmt.Errorf("persist VOACAP SSN state: %w", err)
	}
	return nil
}

func (m *SunspotMonitor) stateSnapshot() sunspotMonitorState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func loadSunspotMonitorState(path string) (sunspotMonitorState, bool, error) {
	data, err := readBoundedStateFile(path, maxSunspotMonitorStateBytes)
	if errors.Is(err, os.ErrNotExist) {
		return sunspotMonitorState{}, false, nil
	}
	if err != nil {
		return sunspotMonitorState{}, false, err
	}
	var state sunspotMonitorState
	if err := json.Unmarshal(data, &state); err != nil {
		return sunspotMonitorState{}, true, fmt.Errorf("parse state JSON: %w", err)
	}
	if err := validateSunspotMonitorState(state); err != nil {
		return sunspotMonitorState{}, true, err
	}
	return state, true, nil
}

func saveSunspotMonitorState(path string, state sunspotMonitorState) error {
	if err := validateSunspotMonitorState(state); err != nil {
		return err
	}
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if len(body) > maxSunspotMonitorStateBytes {
		return fmt.Errorf("state JSON exceeds %d bytes", maxSunspotMonitorStateBytes)
	}
	return writeFileAtomic(path, body, 0o644)
}

func validateSunspotMonitorState(s sunspotMonitorState) error {
	if s.Version != sunspotMonitorStateVersion {
		return fmt.Errorf("unsupported VOACAP SSN state version %d", s.Version)
	}
	id := s.Identity
	if id.Source != solarCountStateSource || id.Units != solarCountStateUnits || id.Conversion != solarCountStateConversion ||
		id.URL == "" || len(id.URL) > maxSolarSourceURLBytes || id.EWMAHalfLifeSeconds <= 0 {
		return errors.New("invalid VOACAP solar state identity")
	}
	if len(s.ETag) > maxSolarValidatorBytes || len(s.LastModified) > maxSolarValidatorBytes {
		return errors.New("VOACAP solar state HTTP validator exceeds its byte limit")
	}
	if !finiteNonnegativeCount(s.LastRawCountIndex) || !finiteNonnegativeCount(s.SmoothedCountIndex) || (s.EWMAInitialized && (!validRawCount(s.LastRawCountIndex) || s.SmoothedCountIndex < 10)) {
		return errors.New("VOACAP solar state count index must be finite and >= 0")
	}
	if s.EWMAInitialized == s.LastObservedAtUTC.IsZero() {
		return errors.New("VOACAP solar state component timestamp and initialization disagree")
	}
	if !s.EWMAInitialized && (s.SmoothedCountIndex != 0 || s.LastRawCountIndex != 0) {
		return errors.New("VOACAP solar state has an uninitialized component value")
	}
	if !s.InputInitialized {
		if s.Input != (SolarInputSnapshot{}) || !s.LastForecastAtUTC.IsZero() || s.LastForecastReason != "" {
			return errors.New("VOACAP solar state has uninitialized forecast metadata")
		}
		_, ready, err := solarInputFromState(s, time.Time{})
		if err != nil || ready {
			return errors.New("VOACAP solar state omits an available complete input")
		}
		return nil
	}
	if s.LastObservedAtUTC.Unix() > s.Input.Provenance.InputUpdatedUnix || (!s.LastFetchAtUTC.IsZero() && s.LastObservedAtUTC.After(s.LastFetchAtUTC)) || s.LastForecastAtUTC.Unix() > s.Input.Provenance.InputUpdatedUnix {
		return errors.New("VOACAP solar state violates observation/acceptance ordering")
	}
	if s.LastForecastAtUTC.IsZero() || (s.LastForecastReason != "initial local sunspot index" && s.LastForecastReason != "rounded local sunspot index changed") {
		return errors.New("VOACAP solar state forecast metadata is invalid")
	}
	expected, ready, err := solarInputFromState(s, time.Unix(s.Input.Provenance.InputUpdatedUnix, 0).UTC())
	if err != nil || !ready || expected != s.Input {
		return errors.New("VOACAP solar state accepted input does not match its components")
	}
	return nil
}

func readBoundedStateFile(path string, maxBytes int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("state file exceeds %d bytes", maxBytes)
	}
	return data, nil
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceFile(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}
