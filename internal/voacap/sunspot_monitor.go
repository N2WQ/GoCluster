package voacap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"
)

const defaultMaxSunspotReportBytes = 1 << 20

type sunspotHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type sunspotMonitorLogger interface {
	Printf(format string, args ...any)
}

// SunspotMonitorConfig owns the runtime NOAA SSN polling cadence and the EWMA
// generation threshold used by path-reliability VOACAP fallback keys.
type SunspotMonitorConfig struct {
	URL                string
	FetchInterval      time.Duration
	RequestTimeout     time.Duration
	StatePath          string
	EWMAHalfLife       time.Duration
	RecomputeThreshold float64
	MaxResponseBytes   int64
	HTTPClient         sunspotHTTPClient
	Logger             sunspotMonitorLogger
}

// SunspotMonitorSnapshot is a point-in-time view of the monitor's retained
// state for tests and operator diagnostics.
type SunspotMonitorSnapshot struct {
	LastFetchAtUTC      time.Time
	LastObservedAtUTC   time.Time
	LastRawSSN          int
	EWMA                float64
	EWMAInitialized     bool
	ForecastSSN         int
	ForecastInitialized bool
	LastRecomputeDelta  float64
	LastForecastAtUTC   time.Time
	LastForecastReason  string
	LastError           string
	ETag                string
	LastModified        string
}

// SunspotMonitor polls NOAA SWPC sunspot_report.json and exposes a rounded
// integer EWMA SSN generation. It does not invoke VOACAP itself.
type SunspotMonitor struct {
	cfg SunspotMonitorConfig

	mu                  sync.RWMutex
	etag                string
	lastModified        string
	lastFetchAtUTC      time.Time
	lastObservedAtUTC   time.Time
	lastRawSSN          int
	ewma                float64
	ewmaInitialized     bool
	forecastSSN         int
	forecastInitialized bool
	lastRecomputeDelta  float64
	lastForecastAtUTC   time.Time
	lastForecastReason  string
	lastError           string

	startOnce sync.Once
	wg        sync.WaitGroup
}

func NewSunspotMonitor(cfg SunspotMonitorConfig) (*SunspotMonitor, error) {
	if cfg.URL == "" {
		cfg.URL = NOAASunspotReportURL
	}
	if cfg.FetchInterval <= 0 {
		return nil, errors.New("sunspot fetch interval must be positive")
	}
	if cfg.RequestTimeout <= 0 {
		return nil, errors.New("sunspot request timeout must be positive")
	}
	if cfg.EWMAHalfLife <= 0 {
		return nil, errors.New("sunspot EWMA half-life must be positive")
	}
	if math.IsNaN(cfg.RecomputeThreshold) || math.IsInf(cfg.RecomputeThreshold, 0) || cfg.RecomputeThreshold <= 0 {
		return nil, errors.New("sunspot recompute threshold must be finite and positive")
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = defaultMaxSunspotReportBytes
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	return &SunspotMonitor{cfg: cfg}, nil
}

// Start launches one cancellable polling goroutine. The first poll runs
// immediately so an enabled VOACAP fallback gets a generation as soon as the
// NOAA endpoint responds.
func (m *SunspotMonitor) Start(ctx context.Context) {
	if m == nil || ctx == nil {
		return
	}
	m.startOnce.Do(func() {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.pollWithTimeout(ctx, time.Now().UTC())
			ticker := time.NewTicker(m.cfg.FetchInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-ticker.C:
					m.pollWithTimeout(ctx, now.UTC())
				}
			}
		}()
	})
}

func (m *SunspotMonitor) Wait() {
	if m == nil {
		return
	}
	m.wg.Wait()
}

func (m *SunspotMonitor) pollWithTimeout(parent context.Context, now time.Time) {
	ctx, cancel := context.WithTimeout(parent, m.cfg.RequestTimeout)
	defer cancel()
	if err := m.Poll(ctx, now); err != nil {
		m.setLastError(err)
	}
}

// Poll performs one fetch and updates EWMA state only for fresh observation
// timestamps. The forecast SSN generation advances only when the rounded EWMA
// delta crosses the configured recompute threshold.
func (m *SunspotMonitor) Poll(ctx context.Context, now time.Time) error {
	if m == nil {
		return errors.New("nil sunspot monitor")
	}
	if ctx == nil {
		return errors.New("nil context")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()

	etag, lastModified := m.fetchValidators()
	result, err := fetchSunspotReport(ctx, m.cfg.HTTPClient, m.cfg.URL, etag, lastModified, m.cfg.MaxResponseBytes)
	if err != nil {
		return err
	}
	if result.notModified {
		m.storeFetchHeaders(now, result)
		if err := m.saveState(); err != nil {
			return err
		}
		m.clearLastError()
		return nil
	}
	series, err := ParseNOAASunspotReport(result.body)
	if err != nil {
		return err
	}
	if len(series) == 0 {
		return errors.New("no usable NOAA sunspot observations")
	}
	if err := m.applyObservation(series[len(series)-1], now); err != nil {
		return err
	}
	m.storeFetchHeaders(now, result)
	if err := m.saveState(); err != nil {
		return err
	}
	return nil
}

func (m *SunspotMonitor) CurrentSSN(time.Time) (int, bool) {
	if m == nil {
		return 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.forecastInitialized {
		return 0, false
	}
	return m.forecastSSN, true
}

func (m *SunspotMonitor) Snapshot() SunspotMonitorSnapshot {
	if m == nil {
		return SunspotMonitorSnapshot{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return SunspotMonitorSnapshot{
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
		LastError:           m.lastError,
		ETag:                m.etag,
		LastModified:        m.lastModified,
	}
}

func (m *SunspotMonitor) applyObservation(observation SunspotObservation, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ewmaInitialized && !observation.ObservedAtUTC.After(m.lastObservedAtUTC) {
		m.lastError = ""
		return nil
	}
	ewma, err := UpdateSunspotEWMA(m.ewma, m.lastObservedAtUTC, m.ewmaInitialized, observation, m.cfg.EWMAHalfLife)
	if err != nil {
		return err
	}
	rounded, err := RoundedSunspotSSN(ewma.Average)
	if err != nil {
		return err
	}
	trigger, delta, err := ShouldRecomputeVOACAP(float64(rounded), float64(m.forecastSSN), m.forecastInitialized, m.cfg.RecomputeThreshold)
	if err != nil {
		return err
	}

	m.lastObservedAtUTC = observation.ObservedAtUTC
	m.lastRawSSN = observation.RawWolfEstimate
	m.ewma = ewma.Average
	m.ewmaInitialized = ewma.Initialized
	m.lastRecomputeDelta = delta
	if trigger {
		m.forecastSSN = rounded
		m.forecastInitialized = true
		m.lastForecastAtUTC = now
		m.lastForecastReason = "initial forecast SSN generation"
		if delta > 0 {
			m.lastForecastReason = "EWMA delta reached recompute threshold"
		}
	}
	m.lastError = ""
	return nil
}

func (m *SunspotMonitor) fetchValidators() (string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.etag, m.lastModified
}

func (m *SunspotMonitor) storeFetchHeaders(now time.Time, result sunspotFetchResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastFetchAtUTC = now
	if result.etag != "" {
		m.etag = result.etag
	}
	if result.lastModified != "" {
		m.lastModified = result.lastModified
	}
}

func (m *SunspotMonitor) setLastError(err error) {
	if m == nil || err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastError = err.Error()
}

func (m *SunspotMonitor) clearLastError() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastError = ""
}

// RoundedSunspotSSN converts an EWMA SSN into the integer value used by VOACAP
// deck generation and cache keys.
func RoundedSunspotSSN(value float64) (int, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, errors.New("sunspot SSN must be finite and >= 0")
	}
	return int(math.Round(value)), nil
}

type sunspotFetchResult struct {
	body         []byte
	notModified  bool
	etag         string
	lastModified string
}

func fetchSunspotReport(ctx context.Context, client sunspotHTTPClient, url string, etag string, lastModified string, maxBytes int64) (sunspotFetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return sunspotFetchResult{}, err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastModified != "" {
		req.Header.Set("If-Modified-Since", lastModified)
	}
	resp, err := client.Do(req)
	if err != nil {
		return sunspotFetchResult{}, err
	}
	defer resp.Body.Close()

	result := sunspotFetchResult{
		etag:         resp.Header.Get("ETag"),
		lastModified: resp.Header.Get("Last-Modified"),
	}
	if resp.StatusCode == http.StatusNotModified {
		result.notModified = true
		return result, nil
	}
	if resp.StatusCode != http.StatusOK {
		return sunspotFetchResult{}, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return sunspotFetchResult{}, err
	}
	if int64(len(body)) > maxBytes {
		return sunspotFetchResult{}, fmt.Errorf("response from %s exceeds %d bytes", url, maxBytes)
	}
	result.body = body
	return result, nil
}
