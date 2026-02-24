package spot

import (
	"dxcluster/strutil"
	"sync"
	"time"
)

// CallCooldownConfig drives the cooldown gate applied before flipping away from a call.
type CallCooldownConfig struct {
	Enabled          bool
	MinReporters     int
	Duration         time.Duration
	TTL              time.Duration
	BinHz            int
	MaxReporters     int
	BypassAdvantage  int
	BypassConfidence int
}

type callCooldownKey struct {
	call string
	bin  int
}

type callCooldownEntry struct {
	reporters     map[string]time.Time
	cooldownUntil time.Time
	lastSeen      time.Time
}

// CallCooldown tracks recent reporter diversity per call/frequency bin so we can
// temporarily refuse to flip away from a call that already has support.
// It is safe for concurrent use.
type CallCooldown struct {
	mu      sync.Mutex
	cfg     CallCooldownConfig
	entries map[callCooldownKey]*callCooldownEntry
	quit    chan struct{}
}

// NewCallCooldown constructs the tracker when enabled; returns nil when disabled.
func NewCallCooldown(cfg CallCooldownConfig) *CallCooldown {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MinReporters <= 0 {
		cfg.MinReporters = 3
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 3 * time.Minute
	}
	if cfg.TTL <= 0 {
		cfg.TTL = cfg.Duration * 2
	}
	if cfg.BinHz <= 0 {
		cfg.BinHz = 1000
	}
	if cfg.MaxReporters <= 0 {
		cfg.MaxReporters = 16
	}
	if cfg.BypassAdvantage < 0 {
		cfg.BypassAdvantage = 0
	}
	if cfg.BypassConfidence < 0 {
		cfg.BypassConfidence = 0
	}
	return &CallCooldown{
		cfg:     cfg,
		entries: make(map[callCooldownKey]*callCooldownEntry),
	}
}

// StartCleanup launches a background ticker that evicts stale entries so memory
// stays bounded even when keys go cold.
func (c *CallCooldown) StartCleanup(interval time.Duration) {
	if c == nil {
		return
	}
	if interval <= 0 {
		interval = c.cfg.TTL
		if interval <= 0 {
			interval = time.Minute
		}
	}
	startPeriodicCleanup(&c.mu, &c.quit, interval, func() {
		c.cleanup(time.Now().UTC())
	})
}

// StopCleanup stops the background cleanup ticker.
func (c *CallCooldown) StopCleanup() {
	if c == nil {
		return
	}
	stopPeriodicCleanup(&c.mu, &c.quit)
}

// Record ingests the current set of reporters for a call at a frequency. It prunes
// stale reporters (older than recency) and starts/refreshes cooldown when the unique
// reporter count crosses the configured threshold. minOverride allows band-adaptive
// thresholds; values <=0 fall back to the configured minimum.
func (c *CallCooldown) Record(call string, freqHz float64, reporters map[string]struct{}, minOverride int, recency time.Duration, now time.Time) {
	if c == nil {
		return
	}
	call = strutil.NormalizeUpper(call)
	if call == "" {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if recency <= 0 {
		recency = 45 * time.Second
	}

	key := callCooldownKey{call: call, bin: c.binFor(freqHz)}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.entries[key]
	if entry != nil && c.cfg.TTL > 0 && entry.lastSeen.Add(c.cfg.TTL).Before(now) {
		delete(c.entries, key)
		entry = nil
	}
	if entry == nil {
		entry = &callCooldownEntry{
			reporters: make(map[string]time.Time, len(reporters)),
		}
		c.entries[key] = entry
	}

	c.pruneEntry(entry, recency, now)

	for reporter := range reporters {
		rep := strutil.NormalizeUpper(reporter)
		if rep == "" {
			continue
		}
		entry.reporters[rep] = now
		if len(entry.reporters) > c.cfg.MaxReporters {
			c.evictOldest(entry)
		}
	}

	entry.lastSeen = now
	effectiveMin := c.cfg.MinReporters
	if minOverride > 0 {
		effectiveMin = minOverride
	}
	if len(entry.reporters) >= effectiveMin {
		entry.cooldownUntil = now.Add(c.cfg.Duration)
	}
}

// ShouldBlock returns true when the call is under cooldown and should not be flipped
// away from. Bypass allows a decisively stronger alternate to proceed. minOverride
// allows band-adaptive thresholds; values <=0 fall back to configured minimum.
func (c *CallCooldown) ShouldBlock(call string, freqHz float64, minOverride int, recency time.Duration, subjectSupport, subjectConfidence, winnerSupport, winnerConfidence int, now time.Time) (bool, int) {
	if c == nil {
		return false, 0
	}
	call = strutil.NormalizeUpper(call)
	if call == "" {
		return false, 0
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if recency <= 0 {
		recency = 45 * time.Second
	}

	key := callCooldownKey{call: call, bin: c.binFor(freqHz)}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return false, 0
	}
	if c.cfg.TTL > 0 && entry.lastSeen.Add(c.cfg.TTL).Before(now) {
		delete(c.entries, key)
		return false, 0
	}

	c.pruneEntry(entry, recency, now)

	reporterCount := len(entry.reporters)
	effectiveMin := c.cfg.MinReporters
	if minOverride > 0 {
		effectiveMin = minOverride
	}
	if reporterCount < effectiveMin {
		return false, reporterCount
	}
	if entry.cooldownUntil.IsZero() || now.After(entry.cooldownUntil) {
		return false, reporterCount
	}

	// Bypass when the alternate is clearly stronger than the protected subject.
	advOK := c.cfg.BypassAdvantage > 0 && winnerSupport >= subjectSupport+c.cfg.BypassAdvantage
	confOK := c.cfg.BypassConfidence > 0 && winnerConfidence >= subjectConfidence+c.cfg.BypassConfidence
	if c.cfg.BypassAdvantage > 0 && c.cfg.BypassConfidence > 0 {
		if advOK && confOK {
			return false, reporterCount
		}
	} else {
		if advOK || confOK {
			return false, reporterCount
		}
	}

	return true, reporterCount
}

func (c *CallCooldown) binFor(freqHz float64) int {
	if c.cfg.BinHz <= 0 {
		return 0
	}
	return int(freqHz) / c.cfg.BinHz
}

func (c *CallCooldown) pruneEntry(entry *callCooldownEntry, recency time.Duration, now time.Time) {
	if entry == nil {
		return
	}
	if recency <= 0 {
		return
	}
	cutoff := now.Add(-recency)
	for rep, ts := range entry.reporters {
		if ts.Before(cutoff) {
			delete(entry.reporters, rep)
		}
	}
}

// cleanup removes entries that have exceeded TTL without activity.
func (c *CallCooldown) cleanup(now time.Time) {
	if c == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) == 0 || c.cfg.TTL <= 0 {
		return
	}
	for k, v := range c.entries {
		if v == nil {
			delete(c.entries, k)
			continue
		}
		if v.lastSeen.IsZero() {
			continue
		}
		if v.lastSeen.Add(c.cfg.TTL).Before(now) {
			delete(c.entries, k)
		}
	}
}

// Cleanup evicts entries that have exceeded TTL without activity as of now.
// It is exposed for deterministic callers (e.g., replay) that do not want a
// wall-clock-driven background goroutine.
func (c *CallCooldown) Cleanup(now time.Time) {
	c.cleanup(now)
}

func (c *CallCooldown) evictOldest(entry *callCooldownEntry) {
	if entry == nil || len(entry.reporters) <= c.cfg.MaxReporters {
		return
	}
	var oldestRep string
	oldestTime := time.Time{}
	for rep, ts := range entry.reporters {
		if oldestTime.IsZero() || ts.Before(oldestTime) {
			oldestTime = ts
			oldestRep = rep
		}
	}
	if oldestRep != "" {
		delete(entry.reporters, oldestRep)
	}
}
