package spot

import (
	"strings"
	"sync"
	"time"

	"dxcluster/config"
)

// AdaptiveMinReports adjusts the min_reports threshold per band group based on
// recent unique reporter activity. It is designed to be cheap to update from
// the hot path (one Observe per spot) and evaluated periodically to avoid
// thrashing thresholds.
type AdaptiveMinReports struct {
	cfg        config.AdaptiveMinReportsConfig
	fallback   int
	window     time.Duration
	evalPeriod time.Duration

	mu           sync.Mutex
	lastEval     time.Time
	reporterSeen map[string]map[string]time.Time // band -> reporter -> lastSeen
	groups       map[string]*groupState          // group name -> state
	bandToGroup  map[string]string               // band -> group name
}

type groupState struct {
	config AdaptiveGroupThresholds

	state          string
	pending        string
	pendingStreak  int
	minReports     int
	lastTransition time.Time
}

// AdaptiveGroupThresholds flattens group-level thresholds for easier reuse.
type AdaptiveGroupThresholds struct {
	Name             string
	Bands            []string
	QuietBelow       int
	BusyAbove        int
	QuietMinReports  int
	NormalMinReports int
	BusyMinReports   int
}

// NewAdaptiveMinReports builds the controller from config. When disabled, or
// when no groups are defined, it returns nil so callers can skip all overhead.
func NewAdaptiveMinReports(cfg config.CallCorrectionConfig) *AdaptiveMinReports {
	groups := normalizeAdaptiveGroups(cfg.AdaptiveMinReports.Groups)
	if !cfg.AdaptiveMinReports.Enabled || len(groups) == 0 {
		return nil
	}

	window := time.Duration(cfg.AdaptiveMinReports.WindowMinutes) * time.Minute
	if window <= 0 {
		window = 10 * time.Minute
	}
	eval := time.Duration(cfg.AdaptiveMinReports.EvaluationPeriodSeconds) * time.Second
	if eval <= 0 {
		eval = time.Minute
	}

	bandToGroup := make(map[string]string, 16)
	groupStates := make(map[string]*groupState, len(groups))
	for _, g := range groups {
		state := &groupState{
			config:         g,
			state:          "normal",
			minReports:     g.NormalMinReports,
			lastTransition: time.Now(),
		}
		groupStates[g.Name] = state
		for _, b := range g.Bands {
			band := strings.ToLower(strings.TrimSpace(b))
			if band != "" {
				bandToGroup[band] = g.Name
			}
		}
	}

	return &AdaptiveMinReports{
		cfg:          cfg.AdaptiveMinReports,
		fallback:     cfg.MinConsensusReports,
		window:       window,
		evalPeriod:   eval,
		reporterSeen: make(map[string]map[string]time.Time),
		groups:       groupStates,
		bandToGroup:  bandToGroup,
	}
}

// Observe records that a reporter was active on a band at the given time.
// It is safe to call from hot paths.
func (a *AdaptiveMinReports) Observe(band, reporter string, now time.Time) {
	if a == nil {
		return
	}
	b := strings.ToLower(strings.TrimSpace(band))
	r := strings.ToUpper(strings.TrimSpace(reporter))
	if b == "" || r == "" {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.reporterSeen[b]; !ok {
		a.reporterSeen[b] = make(map[string]time.Time)
	}
	a.reporterSeen[b][r] = now
}

// MinReportsForBand returns the current min_reports for the provided band,
// falling back to the static configuration when adaptive control is disabled
// or when the band is not mapped to any group.
func (a *AdaptiveMinReports) MinReportsForBand(band string, now time.Time) int {
	if a == nil {
		return a.fallback
	}
	b := strings.ToLower(strings.TrimSpace(band))

	a.mu.Lock()
	defer a.mu.Unlock()

	if now.Sub(a.lastEval) >= a.evalPeriod {
		a.evaluate(now)
		a.lastEval = now
	}

	groupName, ok := a.bandToGroup[b]
	if !ok {
		return a.fallback
	}
	gs, ok := a.groups[groupName]
	if !ok {
		return a.fallback
	}
	if gs.minReports <= 0 {
		return a.fallback
	}
	return gs.minReports
}

func (a *AdaptiveMinReports) evaluate(now time.Time) {
	// Drop stale reporters outside the window.
	for band, reporters := range a.reporterSeen {
		for call, seenAt := range reporters {
			if now.Sub(seenAt) > a.window {
				delete(reporters, call)
			}
		}
		if len(reporters) == 0 {
			delete(a.reporterSeen, band)
		}
	}

	for name, gs := range a.groups {
		count := a.averageReporters(gs.config.Bands)
		candidate := stateForCount(count, gs.config.QuietBelow, gs.config.BusyAbove)

		if candidate != gs.state {
			if gs.pending == candidate {
				gs.pendingStreak++
			} else {
				gs.pending = candidate
				gs.pendingStreak = 1
			}
			if gs.pendingStreak >= a.cfg.HysteresisWindows {
				gs.state = candidate
				gs.pending = ""
				gs.pendingStreak = 0
				gs.lastTransition = now
			}
		} else {
			gs.pending = ""
			gs.pendingStreak = 0
		}

		switch gs.state {
		case "quiet":
			gs.minReports = gs.config.QuietMinReports
		case "busy":
			gs.minReports = gs.config.BusyMinReports
		default:
			gs.minReports = gs.config.NormalMinReports
		}
		a.groups[name] = gs
	}
}

func (a *AdaptiveMinReports) averageReporters(bands []string) float64 {
	if len(bands) == 0 {
		return 0
	}
	var total float64
	for _, b := range bands {
		band := strings.ToLower(strings.TrimSpace(b))
		if band == "" {
			continue
		}
		reporters := a.reporterSeen[band]
		total += float64(len(reporters))
	}
	return total / float64(len(bands))
}

// HighestState returns the busiest state across all groups ("busy" > "normal" > "quiet").
// When no groups are present it defaults to "normal".
func (a *AdaptiveMinReports) HighestState() string {
	if a == nil {
		return "normal"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	hasNormal := false
	for _, gs := range a.groups {
		switch gs.state {
		case "busy":
			return "busy"
		case "normal":
			hasNormal = true
		}
	}
	if hasNormal {
		return "normal"
	}
	return "quiet"
}

func stateForCount(count float64, quietBelow, busyAbove int) string {
	if count < float64(quietBelow) {
		return "quiet"
	}
	if count > float64(busyAbove) {
		return "busy"
	}
	return "normal"
}

func normalizeAdaptiveGroups(groups []config.AdaptiveMinReportsGroup) []AdaptiveGroupThresholds {
	out := make([]AdaptiveGroupThresholds, 0, len(groups))
	for _, g := range groups {
		if len(g.Bands) == 0 {
			continue
		}
		entry := AdaptiveGroupThresholds{
			Name:             g.Name,
			Bands:            g.Bands,
			QuietBelow:       g.QuietBelow,
			BusyAbove:        g.BusyAbove,
			QuietMinReports:  g.QuietMinReports,
			NormalMinReports: g.NormalMinReports,
			BusyMinReports:   g.BusyMinReports,
		}
		if entry.QuietMinReports <= 0 {
			entry.QuietMinReports = 2
		}
		if entry.NormalMinReports <= 0 {
			entry.NormalMinReports = 3
		}
		if entry.BusyMinReports <= 0 {
			entry.BusyMinReports = entry.NormalMinReports
		}
		if entry.QuietBelow <= 0 {
			entry.QuietBelow = 10
		}
		if entry.BusyAbove <= 0 {
			entry.BusyAbove = entry.QuietBelow * 2
		}
		if strings.TrimSpace(entry.Name) == "" {
			entry.Name = strings.Join(entry.Bands, ",")
		}
		out = append(out, entry)
	}
	return out
}
