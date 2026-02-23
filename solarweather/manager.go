package solarweather

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"dxcluster/internal/logutil"
)

type Manager struct {
	cfg       Config
	dipole    Vec3
	logger    *log.Logger
	client    *http.Client
	goesFetch *conditionalFetcher
	kpFetch   *conditionalFetcher

	mu    sync.RWMutex
	state State

	sunMu    sync.Mutex
	sunCache sunCache

	gateCache *GateCache
}

type sunCache struct {
	vec Vec3
	at  time.Time
}

type State struct {
	GOESFlux      float64
	GOESTime      time.Time
	GOESUpdatedAt time.Time
	Kp            float64
	KpTime        time.Time
	KpUpdatedAt   time.Time
	RActiveUntil  []time.Time
	RSeeded       bool
}

type Snapshot struct {
	Now          time.Time
	SunVec       Vec3
	DipoleVec    Vec3
	ActiveRLevel int
	ActiveGLevel int
	GOESFlux     float64
	GOESTime     time.Time
	Kp           float64
	KpTime       time.Time
	GOESStale    bool
	KpStale      bool
}

type OverrideKind uint8

const (
	OverrideNone OverrideKind = iota
	OverrideR
	OverrideG
)

func NewManager(cfg Config, logger *log.Logger) *Manager {
	cfg.normalize()
	client := &http.Client{Timeout: time.Duration(cfg.RequestTimeoutSec) * time.Second}
	m := &Manager{
		cfg:    cfg,
		dipole: DipoleAxisECEF(time.Now().UTC()),
		logger: logger,
		client: client,
		goesFetch: &conditionalFetcher{
			url:    cfg.GOES.URL,
			client: client,
		},
		kpFetch: &conditionalFetcher{
			url:    cfg.Kp.URL,
			client: client,
		},
		gateCache: NewGateCache(cfg.GateCache),
	}
	m.state.RActiveUntil = make([]time.Time, len(cfg.rLevels))
	return m
}

func (m *Manager) Start(ctx context.Context) {
	if m == nil || !m.cfg.Enabled {
		return
	}
	go func() {
		m.poll(ctx)
		ticker := time.NewTicker(time.Duration(m.cfg.FetchIntervalSec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.poll(ctx)
			}
		}
	}()
}

func (m *Manager) poll(ctx context.Context) {
	if m == nil {
		return
	}
	now := time.Now().UTC()
	reqTimeout := time.Duration(m.cfg.RequestTimeoutSec) * time.Second

	if m.goesFetch != nil {
		reqCtx, cancel := context.WithTimeout(ctx, reqTimeout)
		body, updated, err := m.goesFetch.Fetch(reqCtx)
		cancel()
		if err != nil {
			m.logf("solarweather: GOES fetch failed: %v", err)
		} else if updated {
			if samples, ok := parseGOES(body, m.cfg.GOES.EnergyBand); ok {
				m.updateGOES(now, samples)
			}
		}
	}

	if m.kpFetch != nil {
		reqCtx, cancel := context.WithTimeout(ctx, reqTimeout)
		body, updated, err := m.kpFetch.Fetch(reqCtx)
		cancel()
		if err != nil {
			m.logf("solarweather: Kp fetch failed: %v", err)
		} else if updated {
			if kp, t, ok := parseKp(body); ok {
				m.updateKp(now, kp, t)
			}
		}
	}
}

func (m *Manager) updateGOES(now time.Time, samples []goesSample) {
	if len(samples) == 0 {
		return
	}
	latest := samples[len(samples)-1]
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.state.RActiveUntil) != len(m.cfg.rLevels) {
		m.state.RActiveUntil = make([]time.Time, len(m.cfg.rLevels))
		m.state.RSeeded = false
	}
	seed := !m.state.RSeeded
	if !seed && m.cfg.rMaxHold > 0 && !m.state.GOESUpdatedAt.IsZero() {
		if now.Sub(m.state.GOESUpdatedAt) > m.cfg.rMaxHold {
			seed = true
		}
	}
	m.state.GOESFlux = latest.Flux
	m.state.GOESTime = latest.Time
	m.state.GOESUpdatedAt = now
	if seed {
		m.state.RSeeded = m.seedRLevelsLocked(now, samples)
	}
	m.applyRSampleLocked(latest)
}

func (m *Manager) updateKp(now time.Time, kp float64, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Kp = kp
	m.state.KpTime = t
	m.state.KpUpdatedAt = now
}

func (m *Manager) Snapshot(now time.Time) Snapshot {
	if m == nil {
		return Snapshot{}
	}
	sun := m.sunVector(now)

	m.mu.RLock()
	state := m.state
	goesStale := state.GOESTime.IsZero() || now.Sub(state.GOESTime) > time.Duration(m.cfg.GOES.MaxAgeSec)*time.Second
	kpStale := state.KpTime.IsZero() || now.Sub(state.KpTime) > time.Duration(m.cfg.Kp.MaxAgeSec)*time.Second

	activeR := -1
	if !goesStale && len(state.RActiveUntil) == len(m.cfg.rLevels) {
		activeR = activeRLevelIndex(now, state.RActiveUntil)
	}
	activeG := -1
	if !kpStale {
		activeG = activeGLevelIndex(state.Kp, m.cfg.gLevels)
	}
	goesFlux := state.GOESFlux
	goesTime := state.GOESTime
	kpVal := state.Kp
	kpTime := state.KpTime
	m.mu.RUnlock()

	return Snapshot{
		Now:          now,
		SunVec:       sun,
		DipoleVec:    m.dipole,
		ActiveRLevel: activeR,
		ActiveGLevel: activeG,
		GOESFlux:     goesFlux,
		GOESTime:     goesTime,
		Kp:           kpVal,
		KpTime:       kpTime,
		GOESStale:    goesStale,
		KpStale:      kpStale,
	}
}

func (m *Manager) OverrideGlyph(now time.Time, input PathInput) (string, OverrideKind) {
	if m == nil || !m.cfg.Enabled {
		return "", OverrideNone
	}
	band, ok := normalizeBand(input.Band)
	if !ok {
		return "", OverrideNone
	}
	input.Band = band
	snap := m.Snapshot(now)
	if snap.ActiveRLevel < 0 && snap.ActiveGLevel < 0 {
		return "", OverrideNone
	}
	kpValid := !snap.KpStale
	gate := EvaluateGates(now, m.cfg, snap.DipoleVec, snap.SunVec, snap.Kp, kpValid, input, m.gateCache)
	return selectOverride(m.cfg, band, gate, snap.ActiveRLevel, snap.ActiveGLevel)
}

// Summary returns a single-line solar summary or empty string when inactive or disabled.
func (m *Manager) Summary(now time.Time) string {
	if m == nil || !m.cfg.Enabled {
		return ""
	}
	snap := m.Snapshot(now)
	if snap.ActiveRLevel < 0 && snap.ActiveGLevel < 0 {
		return ""
	}
	var mask bandMask
	parts := []string{"SOLAR"}
	if snap.ActiveRLevel >= 0 && snap.ActiveRLevel < len(m.cfg.rLevels) {
		parts = append(parts, m.cfg.rLevels[snap.ActiveRLevel].Name)
		mask |= m.cfg.rLevels[snap.ActiveRLevel].BandMask
	}
	if snap.ActiveGLevel >= 0 && snap.ActiveGLevel < len(m.cfg.gLevels) {
		parts = append(parts, m.cfg.gLevels[snap.ActiveGLevel].Name)
		mask |= m.cfg.gLevels[snap.ActiveGLevel].BandMask
	}
	bands := bandListString(mask)
	if bands == "" {
		return ""
	}
	return strings.Join(parts, " ") + " impacting " + bands
}

func activeRLevelIndex(now time.Time, activeUntil []time.Time) int {
	for i := len(activeUntil) - 1; i >= 0; i-- {
		until := activeUntil[i]
		if !until.IsZero() && !now.After(until) {
			return i
		}
	}
	return -1
}

func activeGLevelIndex(kp float64, levels []gLevel) int {
	for i := len(levels) - 1; i >= 0; i-- {
		if kp >= levels[i].MinKp {
			return i
		}
	}
	return -1
}

func selectOverride(cfg Config, band string, gate GateDecision, rLevelIdx, gLevelIdx int) (string, OverrideKind) {
	normalized, ok := normalizeBand(band)
	if !ok {
		return "", OverrideNone
	}
	if gate.DaylightUnknown {
		return "", OverrideNone
	}
	if rLevelIdx >= 0 && rLevelIdx < len(cfg.rLevels) && gate.Daylight {
		if bandAllowed(cfg.rLevels[rLevelIdx].BandMask, normalized) {
			return cfg.Glyphs.R, OverrideR
		}
	}
	if gLevelIdx >= 0 && gLevelIdx < len(cfg.gLevels) && gate.HighLat {
		if bandAllowed(cfg.gLevels[gLevelIdx].BandMask, normalized) {
			return cfg.Glyphs.G, OverrideG
		}
	}
	return "", OverrideNone
}

// seedRLevelsLocked initializes R-level hold-downs from the recent window. Caller must hold m.mu.
func (m *Manager) seedRLevelsLocked(now time.Time, samples []goesSample) bool {
	if m == nil || len(samples) == 0 || len(m.cfg.rLevels) == 0 {
		return false
	}
	for i := range m.state.RActiveUntil {
		m.state.RActiveUntil[i] = time.Time{}
	}
	cutoff := now.Add(-m.cfg.rMaxHold)
	maxFlux := -1.0
	var maxTime time.Time
	for _, sample := range samples {
		if sample.Time.Before(cutoff) {
			continue
		}
		if sample.Flux > maxFlux || (sample.Flux == maxFlux && sample.Time.After(maxTime)) {
			maxFlux = sample.Flux
			maxTime = sample.Time
		}
	}
	if maxTime.IsZero() {
		return true
	}
	levelIdx := rLevelIndex(maxFlux, m.cfg.rLevels)
	if levelIdx < 0 {
		return true
	}
	until := maxTime.Add(m.cfg.rLevels[levelIdx].Hold)
	if until.After(m.state.RActiveUntil[levelIdx]) {
		m.state.RActiveUntil[levelIdx] = until
	}
	return true
}

// applyRSampleLocked updates the active-until window for the highest matching level. Caller must hold m.mu.
func (m *Manager) applyRSampleLocked(sample goesSample) {
	if m == nil || len(m.cfg.rLevels) == 0 {
		return
	}
	levelIdx := rLevelIndex(sample.Flux, m.cfg.rLevels)
	if levelIdx < 0 {
		return
	}
	until := sample.Time.Add(m.cfg.rLevels[levelIdx].Hold)
	if levelIdx >= len(m.state.RActiveUntil) {
		return
	}
	if until.After(m.state.RActiveUntil[levelIdx]) {
		m.state.RActiveUntil[levelIdx] = until
	}
}

func rLevelIndex(flux float64, levels []rLevel) int {
	for i := len(levels) - 1; i >= 0; i-- {
		if flux >= levels[i].MinFlux {
			return i
		}
	}
	return -1
}

func (m *Manager) sunVector(now time.Time) Vec3 {
	m.sunMu.Lock()
	defer m.sunMu.Unlock()
	cacheAge := time.Duration(m.cfg.Sun.CacheSeconds) * time.Second
	if cacheAge <= 0 {
		cacheAge = time.Minute
	}
	if !m.sunCache.at.IsZero() && now.Sub(m.sunCache.at) < cacheAge {
		return m.sunCache.vec
	}
	vec := SunVectorECEF(now)
	m.sunCache = sunCache{vec: vec, at: now}
	return vec
}

func (m *Manager) logf(format string, args ...any) {
	if m == nil {
		return
	}
	logutil.SafePrintf(m.logger, format, args...)
}
