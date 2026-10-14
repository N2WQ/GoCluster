// Package floodcontrol applies shared-ingest actor flood policy before primary dedupe.
package floodcontrol

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/internal/ratelimit"
	"dxcluster/spot"
	"dxcluster/stats"
)

const (
	actionRankNone = iota
	actionRankObserve
	actionRankSuppress
	actionRankDrop
)

type actorWindow struct {
	times []time.Time
}

type railState struct {
	name                   string
	reason                 string
	action                 string
	window                 time.Duration
	maxEntriesPerPartition int
	threshold              func(string) int
	partitions             map[string]map[string]*actorWindow
}

// Controller owns bounded per-rail actor windows and forwards only spots that
// survive shared-ingest flood policy. Invariant: overflow for unseen actor keys
// fails open so state stays bounded and ingest never blocks on map growth.
type Controller struct {
	mu              sync.Mutex
	stopOnce        sync.Once
	enabled         bool
	input           chan *spot.Spot
	downstream      chan<- *spot.Spot
	admitPrimary    func(*spot.Spot) bool // Installed before Start; owns successful primary admission.
	shutdown        chan struct{}
	cleanupInterval time.Duration
	logInterval     time.Duration
	tracker         *stats.Tracker
	dropReporter    func(string)
	downstreamDrop  ratelimit.Counter
	logCounters     map[string]*ratelimit.Counter
	rails           []*railState

	processed  atomic.Uint64
	observed   atomic.Uint64
	suppressed atomic.Uint64
	dropped    atomic.Uint64
	overflow   atomic.Uint64
}

// New constructs a controller that forwards surviving spots into the supplied
// downstream channel. The input channel is sized to match the downstream buffer
// when possible so the shared-ingest topology remains bounded and predictable.
func New(cfg config.FloodControlConfig, downstream chan<- *spot.Spot, tracker *stats.Tracker, dropReporter func(string)) *Controller {
	inputBuffer := cap(downstream)
	if inputBuffer <= 0 {
		inputBuffer = 1
	}
	logInterval := time.Duration(cfg.LogIntervalSeconds) * time.Second
	return &Controller{
		enabled:         cfg.Enabled,
		input:           make(chan *spot.Spot, inputBuffer),
		downstream:      downstream,
		shutdown:        make(chan struct{}),
		cleanupInterval: cleanupInterval(cfg),
		logInterval:     logInterval,
		tracker:         tracker,
		dropReporter:    dropReporter,
		downstreamDrop:  ratelimit.NewCounterWithRetry(logInterval),
		logCounters:     make(map[string]*ratelimit.Counter),
		rails:           buildRails(cfg),
	}
}

// Input returns the controller ingress channel.
func (c *Controller) Input() chan<- *spot.Spot {
	if c == nil {
		return nil
	}
	return c.input
}

// SetAdmissionSink installs the primary admission boundary before Start.
// It replaces only the channel send; policy decisions and pressure reporting
// remain owned here. The sink returns the result of a nonblocking channel send;
// admission bookkeeping may first wait for its own mutex. Flood locks are not
// held during the call.
func (c *Controller) SetAdmissionSink(admit func(*spot.Spot) bool) {
	c.admitPrimary = admit
}

// Start launches the processing loop and derived cleanup ticker.
func (c *Controller) Start() {
	if c == nil {
		return
	}
	go c.run()
}

// Stop terminates the run loop.
func (c *Controller) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		close(c.shutdown)
	})
}

// GetStats returns current counters plus the total actor-state cardinality.
func (c *Controller) GetStats() (processed, observed, suppressed, dropped, overflow uint64, cacheSize int) {
	if c == nil {
		return 0, 0, 0, 0, 0, 0
	}
	processed = c.processed.Load()
	observed = c.observed.Load()
	suppressed = c.suppressed.Load()
	dropped = c.dropped.Load()
	overflow = c.overflow.Load()

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rail := range c.rails {
		for _, actors := range rail.partitions {
			cacheSize += len(actors)
		}
	}
	return processed, observed, suppressed, dropped, overflow, cacheSize
}

func (c *Controller) run() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.shutdown:
			return
		case <-ticker.C:
			c.cleanup(time.Now().UTC())
		case s := <-c.input:
			if s == nil {
				continue
			}
			c.process(time.Now().UTC(), s)
		}
	}
}

func (c *Controller) process(now time.Time, s *spot.Spot) {
	c.processed.Add(1)
	if !c.enabled {
		c.forward(s)
		return
	}

	sourceType := normalizeSourceType(s)
	if sourceType == "" {
		c.forward(s)
		return
	}

	bestRank := actionRankNone
	bestAction := ""
	bestReason := ""
	overflowReasons := make([]string, 0, 1)

	c.mu.Lock()
	for _, rail := range c.rails {
		action, reason, overflow := rail.observe(sourceType, actorKey(rail.name, s), now)
		if overflow {
			overflowReasons = append(overflowReasons, reason)
		}
		rank := actionRank(action)
		if rank > bestRank {
			bestRank = rank
			bestAction = action
			bestReason = reason
		}
	}
	c.mu.Unlock()

	for _, reason := range overflowReasons {
		c.recordOverflow(reason)
	}

	switch bestAction {
	case config.FloodActionObserve:
		c.recordDecision(bestAction, bestReason)
		c.forward(s)
	case config.FloodActionSuppress, config.FloodActionDrop:
		c.recordDecision(bestAction, bestReason)
	default:
		c.forward(s)
	}
}

func (c *Controller) forward(s *spot.Spot) {
	if c.tryForward(s) {
		return
	}
	if count, ok := c.downstreamDrop.Inc(); ok {
		line := fmt.Sprintf("Flood control: downstream full, dropping spot (total=%d)", count)
		if c.dropReporter != nil {
			c.dropReporter(line)
		} else {
			log.Print(line)
		}
	}
}

func (c *Controller) tryForward(s *spot.Spot) bool {
	if c.admitPrimary != nil {
		return c.admitPrimary(s)
	}
	select {
	case c.downstream <- s:
		return true
	default:
		return false
	}
}

func (c *Controller) recordDecision(action, reason string) {
	switch action {
	case config.FloodActionObserve:
		c.observed.Add(1)
	case config.FloodActionSuppress:
		c.suppressed.Add(1)
	case config.FloodActionDrop:
		c.dropped.Add(1)
	default:
		return
	}
	if c.tracker != nil {
		c.tracker.ObserveFloodDecision(action, reason)
	}
	count, ok := c.logCounter(action + "|" + reason).Inc()
	if !ok {
		return
	}
	line := fmt.Sprintf("Flood control: action=%s reason=%s total=%d", action, reason, count)
	if action == config.FloodActionObserve || c.dropReporter == nil {
		log.Print(line)
		return
	}
	c.dropReporter(line)
}

func (c *Controller) recordOverflow(reason string) {
	c.overflow.Add(1)
	if c.tracker != nil {
		c.tracker.ObserveFloodOverflow(reason)
	}
	count, ok := c.logCounter("overflow|" + reason).Inc()
	if !ok {
		return
	}
	log.Printf("Flood control: overflow reason=%s total=%d (fail-open for unseen actor keys)", reason, count)
}

func (c *Controller) logCounter(key string) *ratelimit.Counter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if counter, ok := c.logCounters[key]; ok {
		return counter
	}
	counter := &ratelimit.Counter{}
	*counter = ratelimit.NewCounterWithRetry(c.logInterval)
	c.logCounters[key] = counter
	return counter
}

func (c *Controller) cleanup(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rail := range c.rails {
		for partition, actors := range rail.partitions {
			for actor, window := range actors {
				pruneWindow(window, now, rail.window, 1)
				if len(window.times) == 0 {
					delete(actors, actor)
				}
			}
			if len(actors) == 0 {
				delete(rail.partitions, partition)
			}
		}
	}
}

func (r *railState) observe(sourceType, actor string, now time.Time) (action string, reason string, overflow bool) {
	if r == nil || actor == "" {
		return "", r.reason, false
	}
	threshold := r.threshold(sourceType)
	if threshold <= 0 {
		return "", r.reason, false
	}
	actors := r.partitions[sourceType]
	if actors == nil {
		actors = make(map[string]*actorWindow)
		r.partitions[sourceType] = actors
	}

	window, exists := actors[actor]
	if !exists {
		if len(actors) >= r.maxEntriesPerPartition {
			return "", r.reason, true
		}
		window = &actorWindow{}
		actors[actor] = window
	}
	pruneWindow(window, now, r.window, threshold+1)
	window.times = append(window.times, now)
	if len(window.times) > threshold+1 {
		window.times = window.times[len(window.times)-(threshold+1):]
	}
	if len(window.times) > threshold {
		return r.action, r.reason, false
	}
	return "", r.reason, false
}

func pruneWindow(window *actorWindow, now time.Time, horizon time.Duration, maxRetained int) {
	if window == nil {
		return
	}
	keep := window.times[:0]
	for _, ts := range window.times {
		if now.Sub(ts) <= horizon {
			keep = append(keep, ts)
		}
	}
	if maxRetained > 0 && len(keep) > maxRetained {
		keep = keep[len(keep)-maxRetained:]
	}
	window.times = keep
}

func buildRails(cfg config.FloodControlConfig) []*railState {
	return []*railState{
		newSimpleRail("decall", "flood_decall", cfg.Rails.DECall),
		newSimpleRail("source_node", "flood_source_node", cfg.Rails.SourceNode),
		newSimpleRail("spotter_ip", "flood_spotter_ip", cfg.Rails.SpotterIP),
		newDXCallRail(cfg.Rails.DXCall),
	}
}

func newSimpleRail(name, reason string, cfg config.FloodRailConfig) *railState {
	window := time.Duration(cfg.WindowSeconds) * time.Second
	if !cfg.Enabled {
		window = 0
	}
	return &railState{
		name:                   name,
		reason:                 reason,
		action:                 strings.ToLower(strings.TrimSpace(cfg.Action)),
		window:                 window,
		maxEntriesPerPartition: cfg.MaxEntriesPerPartition,
		partitions:             make(map[string]map[string]*actorWindow),
		threshold: func(sourceType string) int {
			if !cfg.Enabled {
				return 0
			}
			return cfg.ThresholdsPerSourceType[sourceType]
		},
	}
}

func newDXCallRail(cfg config.FloodRailConfig) *railState {
	activeMode := strings.ToLower(strings.TrimSpace(cfg.ActiveMode))
	window := time.Duration(cfg.WindowSeconds) * time.Second
	if !cfg.Enabled {
		window = 0
	}
	return &railState{
		name:                   "dxcall",
		reason:                 "flood_dxcall",
		action:                 strings.ToLower(strings.TrimSpace(cfg.Action)),
		window:                 window,
		maxEntriesPerPartition: cfg.MaxEntriesPerPartition,
		partitions:             make(map[string]map[string]*actorWindow),
		threshold: func(sourceType string) int {
			if !cfg.Enabled {
				return 0
			}
			switch activeMode {
			case "moderate":
				return cfg.ThresholdsByMode.Moderate[sourceType]
			case "aggressive":
				return cfg.ThresholdsByMode.Aggressive[sourceType]
			default:
				return cfg.ThresholdsByMode.Conservative[sourceType]
			}
		},
	}
}

func cleanupInterval(cfg config.FloodControlConfig) time.Duration {
	minWindow := time.Duration(cfg.LogIntervalSeconds) * time.Second
	for _, rail := range buildRails(cfg) {
		if rail.window <= 0 {
			continue
		}
		if minWindow <= 0 || rail.window < minWindow {
			minWindow = rail.window
		}
	}
	if minWindow <= 0 {
		return time.Second
	}
	return minWindow
}

func normalizeSourceType(s *spot.Spot) string {
	if s == nil {
		return ""
	}
	sourceType := strings.ToUpper(strings.TrimSpace(string(s.SourceType)))
	switch sourceType {
	case "MANUAL", "UPSTREAM", "PEER", "RBN", "FT8", "FT4", "PSKREPORTER":
		return sourceType
	default:
		return ""
	}
}

func actorKey(railName string, s *spot.Spot) string {
	if s == nil {
		return ""
	}
	switch railName {
	case "decall":
		if key := strings.ToUpper(strings.TrimSpace(s.DECallNorm)); key != "" {
			return key
		}
		return strings.ToUpper(strings.TrimSpace(s.DECall))
	case "source_node":
		return strings.ToUpper(strings.TrimSpace(s.SourceNode))
	case "spotter_ip":
		return strings.ToUpper(strings.TrimSpace(s.SpotterIP))
	case "dxcall":
		if key := strings.ToUpper(strings.TrimSpace(s.DXCallNorm)); key != "" {
			return key
		}
		return strings.ToUpper(strings.TrimSpace(s.DXCall))
	default:
		return ""
	}
}

func actionRank(action string) int {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case config.FloodActionObserve:
		return actionRankObserve
	case config.FloodActionSuppress:
		return actionRankSuppress
	case config.FloodActionDrop:
		return actionRankDrop
	default:
		return actionRankNone
	}
}
