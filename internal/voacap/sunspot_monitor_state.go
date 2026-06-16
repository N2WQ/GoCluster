package voacap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	sunspotMonitorStateVersion  = 1
	maxSunspotMonitorStateBytes = 64 * 1024
)

// sunspotMonitorState is the durable restart-continuity subset of
// SunspotMonitorSnapshot. It deliberately excludes LastError because errors are
// live diagnostics, not state that should survive a clean restart.
type sunspotMonitorState struct {
	Version             int       `json:"version"`
	ETag                string    `json:"etag,omitempty"`
	LastModified        string    `json:"last_modified,omitempty"`
	LastFetchAtUTC      time.Time `json:"last_fetch_at_utc,omitempty"`
	LastObservedAtUTC   time.Time `json:"last_observed_at_utc,omitempty"`
	LastRawSSN          int       `json:"last_raw_ssn,omitempty"`
	EWMA                float64   `json:"ewma,omitempty"`
	EWMAInitialized     bool      `json:"ewma_initialized,omitempty"`
	ForecastSSN         int       `json:"forecast_ssn,omitempty"`
	ForecastInitialized bool      `json:"forecast_initialized,omitempty"`
	LastRecomputeDelta  float64   `json:"last_recompute_delta,omitempty"`
	LastForecastAtUTC   time.Time `json:"last_forecast_at_utc,omitempty"`
	LastForecastReason  string    `json:"last_forecast_reason,omitempty"`
}

// LoadState restores persisted SSN monitor continuity state from the configured
// StatePath. A missing file is a cold start, while malformed or unreadable state
// returns an error so the runtime can warn and continue cold.
func (m *SunspotMonitor) LoadState() (bool, error) {
	if m == nil {
		return false, errors.New("nil sunspot monitor")
	}
	path := strings.TrimSpace(m.cfg.StatePath)
	if path == "" {
		return false, nil
	}
	state, found, err := loadSunspotMonitorState(path)
	if err != nil || !found {
		return found, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.etag = state.ETag
	m.lastModified = state.LastModified
	m.lastFetchAtUTC = state.LastFetchAtUTC.UTC()
	m.lastObservedAtUTC = state.LastObservedAtUTC.UTC()
	m.lastRawSSN = state.LastRawSSN
	m.ewma = state.EWMA
	m.ewmaInitialized = state.EWMAInitialized
	m.forecastSSN = state.ForecastSSN
	m.forecastInitialized = state.ForecastInitialized
	m.lastRecomputeDelta = state.LastRecomputeDelta
	m.lastForecastAtUTC = state.LastForecastAtUTC.UTC()
	m.lastForecastReason = state.LastForecastReason
	m.lastError = ""
	return true, nil
}

func (m *SunspotMonitor) saveState() error {
	if m == nil {
		return nil
	}
	path := strings.TrimSpace(m.cfg.StatePath)
	if path == "" {
		return nil
	}
	state := m.stateSnapshot()
	if err := saveSunspotMonitorState(path, state); err != nil {
		if m.cfg.Logger != nil {
			m.cfg.Logger.Printf("Warning: VOACAP SSN state save failed (%s): %v", path, err)
		}
		return fmt.Errorf("persist VOACAP SSN state: %w", err)
	}
	return nil
}

func (m *SunspotMonitor) stateSnapshot() sunspotMonitorState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return sunspotMonitorState{
		Version:             sunspotMonitorStateVersion,
		ETag:                m.etag,
		LastModified:        m.lastModified,
		LastFetchAtUTC:      m.lastFetchAtUTC,
		LastObservedAtUTC:   m.lastObservedAtUTC,
		LastRawSSN:          m.lastRawSSN,
		EWMA:                m.ewma,
		EWMAInitialized:     m.ewmaInitialized,
		ForecastSSN:         m.forecastSSN,
		ForecastInitialized: m.forecastInitialized,
		LastRecomputeDelta:  m.lastRecomputeDelta,
		LastForecastAtUTC:   m.lastForecastAtUTC,
		LastForecastReason:  m.lastForecastReason,
	}
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

func validateSunspotMonitorState(state sunspotMonitorState) error {
	if state.Version != sunspotMonitorStateVersion {
		return fmt.Errorf("unsupported VOACAP SSN state version %d", state.Version)
	}
	if state.LastRawSSN < 0 {
		return errors.New("VOACAP SSN state last_raw_ssn must be >= 0")
	}
	if math.IsNaN(state.EWMA) || math.IsInf(state.EWMA, 0) || state.EWMA < 0 {
		return errors.New("VOACAP SSN state ewma must be finite and >= 0")
	}
	if !state.EWMAInitialized && state.EWMA != 0 {
		return errors.New("VOACAP SSN state ewma is set without ewma_initialized")
	}
	if state.EWMAInitialized && state.LastObservedAtUTC.IsZero() {
		return errors.New("VOACAP SSN state last_observed_at_utc is required when ewma_initialized")
	}
	if state.ForecastSSN < 0 {
		return errors.New("VOACAP SSN state forecast_ssn must be >= 0")
	}
	if state.ForecastInitialized && !state.EWMAInitialized {
		return errors.New("VOACAP SSN state forecast is initialized without EWMA")
	}
	if state.ForecastInitialized && state.LastForecastAtUTC.IsZero() {
		return errors.New("VOACAP SSN state last_forecast_at_utc is required when forecast_initialized")
	}
	if math.IsNaN(state.LastRecomputeDelta) || math.IsInf(state.LastRecomputeDelta, 0) || state.LastRecomputeDelta < 0 {
		return errors.New("VOACAP SSN state last_recompute_delta must be finite and >= 0")
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
