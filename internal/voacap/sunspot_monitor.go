package voacap

// The production monitor owns one polling loop and one bounded continuity
// record. Spot/forecast consumers read immutable copies; they never fetch NOAA
// or wait for persistence. Manual Wolf-estimate experiments remain in sunspot.go.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultMaxSunspotReportBytes = 1 << 20
	maxSolarValidatorBytes       = 4096
	maxSolarErrorBytes           = 4096
	maxSolarSourceURLBytes       = 4096
)

type sunspotHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type sunspotMonitorLogger interface {
	Printf(format string, args ...any)
}

// SunspotMonitorConfig owns production NOAA flux polling and solar-input policy.
// RecentWeight explicitly supports zero and one; none of the policy values is
// defaulted here. URL, client and response-byte limit are transport/test seams.
type SunspotMonitorConfig struct {
	URL              string
	FetchInterval    time.Duration
	RequestTimeout   time.Duration
	StatePath        string
	EWMAHalfLife     time.Duration
	RecentWeight     float64
	StaleAfter       time.Duration
	MaxResponseBytes int64
	HTTPClient       sunspotHTTPClient
	Logger           sunspotMonitorLogger
}

// SunspotMonitorSnapshot separates latest accepted source components from the
// last complete input. Missing flags do not invent an observation age. Stale
// flags and warning/recovery times reflect the most recent poll, even if that
// poll failed or returned 304. Warnings apply only to contributing components.
type SunspotMonitorSnapshot struct {
	LastAttemptAtUTC    time.Time
	LastFetchAtUTC      time.Time
	LastObservedAtUTC   time.Time
	LastNoonFluxSFU     float64
	RecentFluxEWMA      float64
	EWMAInitialized     bool
	MeanObservedAtUTC   time.Time
	PublishedMeanSFU    float64
	MeanInitialized     bool
	ForecastSSN         int
	ForecastInitialized bool
	LastForecastAtUTC   time.Time
	LastForecastReason  string
	Input               SolarInputSnapshot
	RecentMissing       bool
	MeanMissing         bool
	RecentStale         bool
	MeanStale           bool
	RecentWarningAtUTC  time.Time
	MeanWarningAtUTC    time.Time
	RecentRecoveryAtUTC time.Time
	MeanRecoveryAtUTC   time.Time
	LastError           string
	ETag                string
	LastModified        string
}

// SunspotMonitor exposes a daily flux-derived integer SSN, not an official SSN.
// pollMu serializes polling/restoration and their disk saves; mu protects only
// short state publication/read sections. There is no retained observation list.
type SunspotMonitor struct {
	cfg SunspotMonitorConfig

	pollMu sync.Mutex
	mu     sync.RWMutex
	state  sunspotMonitorState

	lastAttemptAtUTC    time.Time
	lastError           string
	recentStale         bool
	meanStale           bool
	recentWarningAtUTC  time.Time
	meanWarningAtUTC    time.Time
	recentRecoveryAtUTC time.Time
	meanRecoveryAtUTC   time.Time

	startOnce sync.Once
	wg        sync.WaitGroup
}

func NewSunspotMonitor(cfg SunspotMonitorConfig) (*SunspotMonitor, error) {
	if cfg.URL == "" {
		cfg.URL = NOAANoonF107URL
	}
	if len(cfg.URL) > maxSolarSourceURLBytes {
		return nil, errors.New("solar source URL exceeds its byte limit")
	}
	if cfg.FetchInterval <= 0 || cfg.RequestTimeout <= 0 {
		return nil, errors.New("solar fetch interval and request timeout must be positive")
	}
	if cfg.EWMAHalfLife <= 0 || cfg.EWMAHalfLife%time.Second != 0 {
		return nil, errors.New("solar EWMA half-life must be a positive whole number of seconds")
	}
	if !validRecentWeight(cfg.RecentWeight) {
		return nil, errors.New("solar recent weight must be finite and between 0 and 1")
	}
	if cfg.StaleAfter <= 0 {
		return nil, errors.New("solar stale warning age must be positive")
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = defaultMaxSunspotReportBytes
	}
	if cfg.MaxResponseBytes == math.MaxInt64 {
		return nil, errors.New("solar response-byte limit cannot equal MaxInt64")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	cfg.StatePath = strings.TrimSpace(cfg.StatePath)
	m := &SunspotMonitor{cfg: cfg}
	m.state = sunspotMonitorState{Version: sunspotMonitorStateVersion, Identity: solarStateIdentity(cfg)}
	return m, nil
}

func validRecentWeight(weight float64) bool {
	return !math.IsNaN(weight) && !math.IsInf(weight, 0) && weight >= 0 && weight <= 1
}

// Start owns exactly one cancellable polling goroutine. Its request context,
// stopped ticker and WaitGroup also cover cancellation during an active fetch.
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
	if m != nil {
		m.wg.Wait()
	}
}

func (m *SunspotMonitor) pollWithTimeout(parent context.Context, now time.Time) {
	ctx, cancel := context.WithTimeout(parent, m.cfg.RequestTimeout)
	defer cancel()
	_ = m.Poll(ctx, now) //nolint:errcheck // Poll publishes errors and component health; this owned loop retries on cadence.
}

// Poll replays all newer available observations, retaining independent component
// watermarks. It never revises accepted values, invents samples across gaps, or
// smooths an already accepted Noon again when its first mean arrives later.
func (m *SunspotMonitor) Poll(ctx context.Context, now time.Time) (err error) {
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
	m.pollMu.Lock()
	defer m.pollMu.Unlock()
	defer func() { m.finishPoll(now, err) }()

	m.mu.RLock()
	etag, lastModified := m.state.ETag, m.state.LastModified
	m.mu.RUnlock()
	result, err := fetchSunspotReport(ctx, m.cfg.HTTPClient, m.cfg.URL, etag, lastModified, m.cfg.MaxResponseBytes)
	if err != nil {
		return err
	}
	if !result.notModified {
		series, parseErr := parseNOAANoonFlux(result.body, now)
		if parseErr != nil {
			return parseErr
		}
		if len(series) == 0 {
			return errors.New("no usable NOAA Noon solar flux components")
		}
		if err := m.applyFluxSeries(series, now); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.state.LastFetchAtUTC = now
	// A new representation without validators must not retain validators from
	// an older body. A 304 may omit them, so keep its existing validators.
	if !result.notModified || result.etag != "" {
		m.state.ETag = result.etag
	}
	if !result.notModified || result.lastModified != "" {
		m.state.LastModified = result.lastModified
	}
	m.mu.Unlock()
	return m.saveState()
}

func (m *SunspotMonitor) applyFluxSeries(series []solarFluxObservation, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.state
	changed := false
	for _, row := range series {
		if row.hasFlux && (!next.EWMAInitialized || row.at.After(next.LastObservedAtUTC)) {
			if next.EWMAInitialized {
				next.RecentFluxEWMA = advanceFluxEWMA(next.RecentFluxEWMA, row.flux, row.at.Sub(next.LastObservedAtUTC), m.cfg.EWMAHalfLife)
			} else {
				next.RecentFluxEWMA = row.flux
			}
			next.EWMAInitialized, next.LastObservedAtUTC, next.LastNoonFluxSFU = true, row.at, row.flux
			changed = true
		}
		if row.hasMean && (!next.MeanInitialized || row.at.After(next.MeanObservedAtUTC)) {
			next.MeanInitialized, next.MeanObservedAtUTC, next.PublishedMeanSFU = true, row.at, row.mean
			changed = true
		}
	}
	if changed {
		input, ready, err := solarInputFromState(next, now)
		if err != nil {
			return err // Publish no partially advanced batch after arithmetic failure.
		}
		if ready {
			if !next.InputInitialized || input.SSN != next.Input.SSN {
				next.LastForecastAtUTC = now
				next.LastForecastReason = "rounded solar flux SSN changed"
				if !next.InputInitialized {
					next.LastForecastReason = "initial solar flux SSN"
				}
			}
			next.Input, next.InputInitialized = input, true
		}
		m.state = next
	}
	return nil
}

// CurrentSolarInput returns SSN and its last accepted provenance in one read.
// Fetch failure and component age never withdraw a previously valid input.
func (m *SunspotMonitor) CurrentSolarInput(time.Time) (SolarInputSnapshot, bool) {
	if m == nil {
		return SolarInputSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.Input, m.state.InputInitialized
}

func (m *SunspotMonitor) CurrentSSN(now time.Time) (int, bool) {
	input, ok := m.CurrentSolarInput(now)
	return input.SSN, ok
}

func (m *SunspotMonitor) Snapshot() SunspotMonitorSnapshot {
	if m == nil {
		return SunspotMonitorSnapshot{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := m.state
	return SunspotMonitorSnapshot{
		LastAttemptAtUTC: m.lastAttemptAtUTC, LastFetchAtUTC: s.LastFetchAtUTC,
		LastObservedAtUTC: s.LastObservedAtUTC, LastNoonFluxSFU: s.LastNoonFluxSFU,
		RecentFluxEWMA: s.RecentFluxEWMA, EWMAInitialized: s.EWMAInitialized,
		MeanObservedAtUTC: s.MeanObservedAtUTC, PublishedMeanSFU: s.PublishedMeanSFU,
		MeanInitialized: s.MeanInitialized, ForecastSSN: s.Input.SSN,
		ForecastInitialized: s.InputInitialized, Input: s.Input,
		LastForecastAtUTC: s.LastForecastAtUTC, LastForecastReason: s.LastForecastReason,
		RecentMissing: !s.EWMAInitialized, MeanMissing: !s.MeanInitialized,
		RecentStale: m.recentStale, MeanStale: m.meanStale,
		RecentWarningAtUTC: m.recentWarningAtUTC, MeanWarningAtUTC: m.meanWarningAtUTC,
		RecentRecoveryAtUTC: m.recentRecoveryAtUTC, MeanRecoveryAtUTC: m.meanRecoveryAtUTC,
		LastError: m.lastError, ETag: s.ETag, LastModified: s.LastModified,
	}
}

// RoundedSunspotSSN converts an EWMA SSN into the integer value used by VOACAP
// deck generation and cache keys. The manual Wolf experiment still uses it.
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

func fetchSunspotReport(ctx context.Context, client sunspotHTTPClient, url, etag, lastModified string, maxBytes int64) (sunspotFetchResult, error) {
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
	result := sunspotFetchResult{etag: resp.Header.Get("ETag"), lastModified: resp.Header.Get("Last-Modified")}
	if len(result.etag) > maxSolarValidatorBytes || len(result.lastModified) > maxSolarValidatorBytes {
		return sunspotFetchResult{}, errors.New("solar HTTP validator exceeds its byte limit")
	}
	result.etag, result.lastModified = strings.Clone(result.etag), strings.Clone(result.lastModified)
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
