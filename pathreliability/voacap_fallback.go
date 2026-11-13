// File role: Owns the optional nonblocking VOACAP closed-band fallback.
// Crawler notes: Start here for SSN-gated fallback delay/cache behavior,
// mode-owned closed-threshold evaluation, VOACAP deck launch, and closed glyph
// result construction.
// Related docs: pathreliability/README.md,
// data/config/PATH_PREDICTIONS.md, docs/decisions/ADR-0161-voacap-closed-only-path-fallback.md,
// docs/decisions/ADR-0162-voacap-hourly-forecast-window-cache.md,
// docs/decisions/ADR-0163-voacap-aligned-sparse-p50-fallback.md,
// docs/decisions/ADR-0169-bidirectional-noise-aware-voacap-fallback.md.
// Related tests: pathreliability/voacap_fallback_test.go,
// telnet/path_settings_test.go.
package pathreliability

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/internal/voacap"
)

// ClosedFallback is the nonblocking path-reliability hook for an optional
// VOACAP fallback. Implementations return cached current-hour forecasts when
// available; lookup may enqueue background work but must not run VOACAP in the
// telnet display/filter path.
type ClosedFallback interface {
	CheckForecast(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecast, bool)
}

// DetailedForecastProvider exposes the same nonblocking lookup with the reason
// a current-hour forecast was not returned.
type DetailedForecastProvider interface {
	CheckForecastDetailed(req VOACAPClosedRequest, now time.Time) VOACAPForecastCheck
}

// CachedForecastProvider exposes an observation-only cache lookup for
// comparison diagnostics. Implementations must not enqueue VOACAP work, update
// delay windows, or mutate fallback stage counters from this method.
type CachedForecastProvider interface {
	CheckCachedForecast(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecast, bool)
}

// VOACAPForecastWindowProvider exposes the cached rolling forecast horizon for
// on-demand operator queries. Implementations may enqueue delayed background
// work on a miss, but must not run VOACAP synchronously in the telnet command
// path.
type VOACAPForecastWindowProvider interface {
	CheckForecastWindow(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecastWindow, bool)
}

// VOACAPForecastWindowWaitProvider exposes a command-triggered refresh path for
// on-demand operator queries. Implementations still use their owned worker and
// cache; the caller may wait briefly for the worker to finish.
type VOACAPForecastWindowWaitProvider interface {
	CheckForecastWindowWait(req VOACAPClosedRequest, now time.Time, wait time.Duration) (VOACAPCachedForecastWindow, VOACAPForecastWindowStatus)
}

// ClosedFallbackStatsProvider exposes reset-on-snapshot stage counters for
// optional propagation-log diagnostics without changing the lookup interface.
type ClosedFallbackStatsProvider interface {
	StatsSnapshot() VOACAPFallbackStats
}

type VOACAPSSNProvider interface {
	CurrentSSN(now time.Time) (int, bool)
}

type coherentSolarInputProvider interface {
	CurrentSolarInput(time.Time) (voacap.SolarInputSnapshot, bool)
}

type VOACAPClosedForecaster interface {
	ForecastClosed(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error)
}

type VOACAPFallbackLogger interface {
	Printf(format string, args ...any)
}

type VOACAPClosedRequest struct {
	UserCell              CellID
	DXCell                CellID
	UserGrid              string
	DXGrid                string
	Band                  string
	Mode                  string
	ReceiveNoisePenaltyDB float64
}

type VOACAPClosedJob struct {
	Request        VOACAPClosedRequest
	SSN            int
	WindowStartUTC time.Time
	FrequencyMHz   float64
	SolarInput     voacap.SolarInputSnapshot

	key      voacapCacheKey
	delayKey voacapDelayKey
	origin   time.Time
	work     *voacapPendingWork
}

// One pending owner is shared by the queue, worker and bounded command waits.
// Closing published broadcasts memory publication; persistence still owns work.
type voacapPendingWork struct {
	request    VOACAPClosedRequest
	key        voacapCacheKey
	enqueued   time.Time
	sequence   uint64
	started    bool
	ssn        int
	inputKnown bool
	published  chan struct{}
	err        error
}

type VOACAPClosedForecast struct {
	WindowStartUTC time.Time
	Records        []VOACAPHourlyForecast
	OutputPath     string
	Elapsed        time.Duration
}

type VOACAPHourlyForecast struct {
	FT8SNRDB                        int
	VOACAPSNRDBHz                   int
	HourUTC                         int
	FrequencyMHz                    float64
	ReqSNRReliability               float64
	HasReqSNRReliability            bool
	ReceiveFT8SNRDB                 int
	TransmitFT8SNRDB                int
	ReceiveVOACAPSNRDBHz            int
	TransmitVOACAPSNRDBHz           int
	ReceiveReqSNRReliability        float64
	TransmitReqSNRReliability       float64
	HasDirectionalReqSNRReliability bool
	HasDirectionalSNR               bool
}

// VOACAPCachedForecast carries one cached current-hour VOACAP record after
// request-specific receive-noise adjustment. Raw directional SNRs stay in
// Record so one bounded cache entry can serve users with different noise
// classes without multiplying retained state.
type VOACAPCachedForecast struct {
	Record                VOACAPHourlyForecast
	EffectiveFT8SNRDB     float64
	HasEffectiveFT8SNRDB  bool
	ReceiveNoisePenaltyDB float64
	SSN                   int
	AgeSec                int64
}

func (f VOACAPCachedForecast) EffectiveDB() float64 {
	if f.HasEffectiveFT8SNRDB {
		return f.EffectiveFT8SNRDB
	}
	return float64(f.Record.FT8SNRDB)
}

// ReqSNRReliability returns VOACAP REL for the request SNR contract. For
// bidirectional records, both directed decks must have REL and the conservative
// effective value is the lower directional reliability.
func (f VOACAPCachedForecast) ReqSNRReliability() (float64, bool) {
	if f.Record.HasDirectionalReqSNRReliability {
		return math.Min(f.Record.ReceiveReqSNRReliability, f.Record.TransmitReqSNRReliability), true
	}
	if f.Record.HasReqSNRReliability {
		return f.Record.ReqSNRReliability, true
	}
	return 0, false
}

// ReceiveReqSNRReliability returns VOACAP REL for the receive deck. Legacy
// non-directional records fall back to the record-level request-SNR REL.
func (f VOACAPCachedForecast) ReceiveReqSNRReliability() (float64, bool) {
	if f.Record.HasDirectionalReqSNRReliability {
		return f.Record.ReceiveReqSNRReliability, true
	}
	return f.ReqSNRReliability()
}

// ReceiveDB returns the target-to-user receive leg after the request-specific
// receive-noise penalty. Non-directional legacy records fall back to EffectiveDB.
func (f VOACAPCachedForecast) ReceiveDB() float64 {
	if !f.Record.HasDirectionalSNR {
		return f.EffectiveDB()
	}
	return float64(f.Record.ReceiveFT8SNRDB) - f.ReceiveNoisePenaltyDB
}

// TransmitDB returns the user-to-target transmit leg. Non-directional legacy
// records fall back to EffectiveDB.
func (f VOACAPCachedForecast) TransmitDB() float64 {
	if !f.Record.HasDirectionalSNR {
		return f.EffectiveDB()
	}
	return float64(f.Record.TransmitFT8SNRDB)
}

type VOACAPReliabilityGateReason uint8

const (
	VOACAPReliabilityGateOK VOACAPReliabilityGateReason = iota
	VOACAPReliabilityGateMissing
	VOACAPReliabilityGateBelowThreshold
)

func (r VOACAPReliabilityGateReason) String() string {
	switch r {
	case VOACAPReliabilityGateOK:
		return "ok"
	case VOACAPReliabilityGateMissing:
		return "missing"
	case VOACAPReliabilityGateBelowThreshold:
		return "below_threshold"
	default:
		return "unknown"
	}
}

// VOACAPReliabilityThresholdForClass returns the YAML-owned minimum REL gate
// for an ordinary open VOACAP class.
func VOACAPReliabilityThresholdForClass(class string, cfg Config) (float64, bool) {
	switch class {
	case classHigh:
		return cfg.VOACAPFallback.ReliabilityMinHigh, true
	case classMedium:
		return cfg.VOACAPFallback.ReliabilityMinMedium, true
	case classLow:
		return cfg.VOACAPFallback.ReliabilityMinLow, true
	case classUnlikely:
		return cfg.VOACAPFallback.ReliabilityMinUnlikely, true
	default:
		return 0, false
	}
}

// VOACAPReqSNRReliabilityGate evaluates VOACAP REL as reliability of the
// request-SNR contract, not as probability of a HIGH/MEDIUM/LOW class.
func VOACAPReqSNRReliabilityGate(forecast VOACAPCachedForecast, class string, cfg Config) (rel float64, threshold float64, reason VOACAPReliabilityGateReason) {
	threshold, ok := VOACAPReliabilityThresholdForClass(class, cfg)
	if !ok {
		return 0, 0, VOACAPReliabilityGateMissing
	}
	rel, ok = forecast.ReqSNRReliability()
	if !ok {
		return 0, threshold, VOACAPReliabilityGateMissing
	}
	if rel < threshold {
		return rel, threshold, VOACAPReliabilityGateBelowThreshold
	}
	return rel, threshold, VOACAPReliabilityGateOK
}

// VOACAPReceiveReqSNRReliabilityGate evaluates REL on the receive leg for
// beacon RX-only fallback semantics.
func VOACAPReceiveReqSNRReliabilityGate(forecast VOACAPCachedForecast, class string, cfg Config) (rel float64, threshold float64, reason VOACAPReliabilityGateReason) {
	threshold, ok := VOACAPReliabilityThresholdForClass(class, cfg)
	if !ok {
		return 0, 0, VOACAPReliabilityGateMissing
	}
	rel, ok = forecast.ReceiveReqSNRReliability()
	if !ok {
		return 0, threshold, VOACAPReliabilityGateMissing
	}
	if rel < threshold {
		return rel, threshold, VOACAPReliabilityGateBelowThreshold
	}
	return rel, threshold, VOACAPReliabilityGateOK
}

// VOACAPCachedForecastWindow carries cached hourly VOACAP rows from the current
// UTC hour through the configured forecast horizon. Missing rows mean callers
// should either report a bounded refresh state or enqueue command-triggered
// work through the owning fallback worker.
type VOACAPCachedForecastWindow struct {
	WindowStartUTC time.Time
	Records        []VOACAPCachedForecast
}

// VOACAPForecastWindowStatus reports whether a window is ready, still being
// refreshed by the fallback worker, or blocked by a bounded runtime condition.
type VOACAPForecastWindowStatus int

const (
	VOACAPForecastWindowUnavailable VOACAPForecastWindowStatus = iota
	VOACAPForecastWindowReady
	VOACAPForecastWindowRefreshing
	VOACAPForecastWindowBusy
	VOACAPForecastWindowFailed
)

type VOACAPForecastCheckStatus uint8

const (
	VOACAPForecastCheckDisabled VOACAPForecastCheckStatus = iota
	VOACAPForecastCheckReady
	VOACAPForecastCheckInvalidRequest
	VOACAPForecastCheckSSNUnavailable
	VOACAPForecastCheckNoCurrentHour
	VOACAPForecastCheckInflight
	VOACAPForecastCheckDelayWait
	VOACAPForecastCheckNotRunning
	VOACAPForecastCheckQueueFull
	VOACAPForecastCheckQueued
	VOACAPForecastCheckUnavailable
	VOACAPForecastCheckStatusCount
)

type VOACAPInvalidRequestReason uint8

const (
	VOACAPInvalidRequestReasonNone VOACAPInvalidRequestReason = iota
	VOACAPInvalidUnsupportedBand
	VOACAPInvalidEmptyUnknownBand
	VOACAPInvalidUserGrid
	VOACAPInvalidDXGrid
	VOACAPInvalidUserCell
	VOACAPInvalidDXCell
	VOACAPInvalidRequestReasonCount
)

func (r VOACAPInvalidRequestReason) String() string {
	switch r {
	case VOACAPInvalidUnsupportedBand:
		return "invalid_unsupported_band"
	case VOACAPInvalidEmptyUnknownBand:
		return "invalid_empty_unknown_band"
	case VOACAPInvalidUserGrid:
		return "invalid_user_grid"
	case VOACAPInvalidDXGrid:
		return "invalid_dx_grid"
	case VOACAPInvalidUserCell:
		return "invalid_user_cell"
	case VOACAPInvalidDXCell:
		return "invalid_dx_cell"
	default:
		return ""
	}
}

func (s VOACAPForecastCheckStatus) String() string {
	switch s {
	case VOACAPForecastCheckDisabled:
		return "disabled"
	case VOACAPForecastCheckReady:
		return "ready"
	case VOACAPForecastCheckInvalidRequest:
		return "invalid_request"
	case VOACAPForecastCheckSSNUnavailable:
		return "ssn_unavailable"
	case VOACAPForecastCheckNoCurrentHour:
		return "no_current_hour"
	case VOACAPForecastCheckInflight:
		return "inflight"
	case VOACAPForecastCheckDelayWait:
		return "delay_wait"
	case VOACAPForecastCheckNotRunning:
		return "not_running"
	case VOACAPForecastCheckQueueFull:
		return "queue_full"
	case VOACAPForecastCheckQueued:
		return "queued"
	default:
		return "unavailable"
	}
}

type VOACAPForecastCheck struct {
	Forecast      VOACAPCachedForecast
	Status        VOACAPForecastCheckStatus
	InvalidReason VOACAPInvalidRequestReason
	CacheMiss     bool
	NoCurrentHour bool
}

type VOACAPClosedFallbackSnapshot struct {
	CacheEntries        int
	DelayEntries        int
	InflightEntries     int
	QueueDepth          int
	ForecastCacheIndex  VOACAPForecastCacheIndexSnapshot
	UsableCacheEntries  int
	DifferentSSNEntries int
	SSNComparisonKnown  bool
	QueueOldestAge      time.Duration
	QueueOldestAgeKnown bool
	WorkerBusy          bool
	WorkerBusyFor       time.Duration
	WorkerDurationCount uint64
	WorkerDurationSum   time.Duration
	WorkerDurationMax   time.Duration
}

// VOACAPFallbackStats explains why fallback work did or did not emit a glyph
// during one propagation-log interval. Final emitted glyph counts stay in the
// telnet path prediction counters.
type VOACAPFallbackStats struct {
	InvalidRequest          int64
	InvalidUnsupportedBand  int64
	InvalidEmptyUnknownBand int64
	InvalidUserGrid         int64
	InvalidDXGrid           int64
	InvalidUserCell         int64
	InvalidDXCell           int64
	SSNUnavailable          int64
	CacheHit                int64
	NoCurrentHour           int64
	DelayWait               int64
	Inflight                int64
	NotRunning              int64
	QueueFull               int64
	Queued                  int64
	RunSuccess              int64
	RunFailure              int64
}

func (s VOACAPFallbackStats) HasActivity() bool {
	return s.InvalidRequest != 0 ||
		s.SSNUnavailable != 0 ||
		s.CacheHit != 0 ||
		s.NoCurrentHour != 0 ||
		s.DelayWait != 0 ||
		s.Inflight != 0 ||
		s.NotRunning != 0 ||
		s.QueueFull != 0 ||
		s.Queued != 0 ||
		s.RunSuccess != 0 ||
		s.RunFailure != 0
}

type VOACAPClosedFallback struct {
	cfg      Config
	fallback VOACAPFallbackConfig

	forecaster  VOACAPClosedForecaster
	ssnProvider VOACAPSSNProvider
	logger      VOACAPFallbackLogger
	queue       chan *voacapPendingWork

	mu                  sync.Mutex
	cache               map[voacapCacheKey]voacapCacheEntry
	delays              map[voacapDelayKey]voacapDelayEntry
	inflight            map[voacapCacheKey]*voacapPendingWork
	forecastCacheStore  *VOACAPForecastCacheStore
	lastPrune           time.Time
	startOnce           sync.Once
	wg                  sync.WaitGroup
	running             atomic.Bool
	workerBusy          bool
	workerStartedAt     time.Time
	workSequence        uint64
	workerDurationCount uint64
	workerDurationSum   time.Duration
	workerDurationMax   time.Duration
	now                 func() time.Time

	statsInvalidRequest atomic.Int64
	statsSSNUnavailable atomic.Int64
	statsCacheHit       atomic.Int64
	statsNoCurrentHour  atomic.Int64
	statsDelayWait      atomic.Int64
	statsInflight       atomic.Int64
	statsNotRunning     atomic.Int64
	statsQueueFull      atomic.Int64
	statsQueued         atomic.Int64
	statsInvalidReasons [VOACAPInvalidRequestReasonCount]atomic.Int64
	statsRunSuccess     atomic.Int64
	statsRunFailure     atomic.Int64
}

type voacapCacheKey struct {
	userCell     CellID
	dxCell       CellID
	direction    voacapDirectionID
	band         string
	frequencyKHz int
}

type voacapDelayKey struct {
	userCell     CellID
	dxCell       CellID
	direction    voacapDirectionID
	band         string
	frequencyKHz int
}

// Cache and delay owners admit only canonical bidirectional paths. A compact
// private ID keeps both keys 32 bytes; wire keys retain their original string.
type voacapDirectionID uint8

const voacapDirectionBidirectionalID voacapDirectionID = 1

type voacapCacheEntry struct {
	forecast    VOACAPClosedForecast
	storedAt    time.Time
	metadata    *voacapForecastMetadata
	windowEndNS int64
	expiresNS   int64
	hourMask    uint32
	flags       uint32
}

// Immutable input and validity; no reference to mutable monitor state escapes.
// This pointer-free object is 96 bytes. Its pointer, inline validity bounds,
// hour mask and flags add 32 bytes to the original entry: 128 additional bytes
// in total. Expiry scans read inline bounds in a 128-byte map element.
type voacapForecastMetadata struct {
	solar    voacap.SolarInputSnapshot
	originNS int64
}

const voacapMetadataInitialized uint32 = 1

type voacapDelayKind uint8

const (
	voacapDelayWarmup voacapDelayKind = iota
	voacapDelayFailure
	voacapDelayRetired
)

type voacapDelayEntry struct {
	firstSeen time.Time
	lastSeen  time.Time
	kind      voacapDelayKind
}

const (
	voacapDirectionBidirectional = "bidirectional"
	voacapDeckReceive            = "receive"
	voacapDeckTransmit           = "transmit"
)

// ErrVOACAPFallbackDisabled marks an explicit disabled-constructor call.
var ErrVOACAPFallbackDisabled = fmt.Errorf("voacap fallback disabled")

func NewVOACAPClosedFallback(cfg Config, forecaster VOACAPClosedForecaster, ssnProvider VOACAPSSNProvider, logger VOACAPFallbackLogger) (*VOACAPClosedFallback, error) {
	fallback := cfg.VOACAPFallback
	if err := fallback.finalize(); err != nil {
		return nil, err
	}
	if !fallback.Enabled {
		return nil, ErrVOACAPFallbackDisabled
	}
	if cfg.GlyphSymbols.Closed == "" {
		return nil, fmt.Errorf("glyph_symbols.closed is required when VOACAP fallback is enabled")
	}
	if forecaster == nil {
		return nil, fmt.Errorf("VOACAP closed forecaster is required when fallback is enabled")
	}
	if ssnProvider == nil {
		return nil, fmt.Errorf("VOACAP SSN provider is required when fallback is enabled")
	}
	return &VOACAPClosedFallback{
		cfg:         cfg,
		fallback:    fallback,
		forecaster:  forecaster,
		ssnProvider: ssnProvider,
		logger:      logger,
		queue:       make(chan *voacapPendingWork, fallback.MaxQueueDepth),
		cache:       make(map[voacapCacheKey]voacapCacheEntry),
		delays:      make(map[voacapDelayKey]voacapDelayEntry),
		inflight:    make(map[voacapCacheKey]*voacapPendingWork),
		now:         time.Now,
	}, nil
}

func (f *VOACAPClosedFallback) Start(ctx context.Context) {
	if f == nil || ctx == nil {
		return
	}
	f.startOnce.Do(func() {
		f.running.Store(true)
		for i := 0; i < f.fallback.WorkerCount; i++ {
			f.wg.Add(1)
			go f.worker(ctx)
		}
	})
}

func (f *VOACAPClosedFallback) Wait() {
	if f == nil {
		return
	}
	f.wg.Wait()
}

// AttachForecastCacheStore hydrates current persisted forecasts into the owned
// memory cache. Future lookups still read memory only; the store is used by
// worker completions and closed after Wait during runtime shutdown.
func (f *VOACAPClosedFallback) AttachForecastCacheStore(store *VOACAPForecastCacheStore, now time.Time) (VOACAPForecastCacheRestoreStats, error) {
	var stats VOACAPForecastCacheRestoreStats
	if f == nil || store == nil {
		return stats, nil
	}
	now = normalizeVOACAPForecastCacheNow(now)
	f.mu.Lock()
	f.forecastCacheStore = store
	f.mu.Unlock()
	loaded, stats, err := store.loadCurrentRetiring(now, f.fallback, func(key voacapCacheKey) {
		f.mu.Lock()
		f.addDelayLocked(f.delayKeyForCacheKey(key), voacapDelayEntry{firstSeen: now, lastSeen: now, kind: voacapDelayRetired})
		f.mu.Unlock()
	})
	if err != nil {
		return stats, err
	}
	f.mu.Lock()
	for i := range loaded {
		item := &loaded[i]
		f.addCacheLocked(item.key, item.entry)
		dk := f.delayKeyForCacheKey(item.key)
		if d, ok := f.delays[dk]; ok && d.kind == voacapDelayRetired {
			delete(f.delays, dk)
		}
	}
	f.mu.Unlock()
	return stats, nil
}

func (f *VOACAPClosedFallback) CloseForecastCacheStore() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	store := f.forecastCacheStore
	f.forecastCacheStore = nil
	f.mu.Unlock()
	if store == nil {
		return nil
	}
	return store.Close()
}

func (f *VOACAPClosedFallback) Snapshot() VOACAPClosedFallbackSnapshot {
	if f == nil {
		return VOACAPClosedFallbackSnapshot{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshotLocked(f.clockNow())
}

func (f *VOACAPClosedFallback) snapshotLocked(now time.Time) VOACAPClosedFallbackSnapshot {
	out := VOACAPClosedFallbackSnapshot{CacheEntries: len(f.cache), DelayEntries: len(f.delays), InflightEntries: len(f.inflight), QueueDepth: len(f.queue), WorkerBusy: f.workerBusy, WorkerDurationCount: f.workerDurationCount, WorkerDurationSum: f.workerDurationSum, WorkerDurationMax: f.workerDurationMax}
	if f.workerBusy && now.After(f.workerStartedAt) {
		out.WorkerBusyFor = now.Sub(f.workerStartedAt)
	}
	if f.forecastCacheStore != nil {
		out.ForecastCacheIndex = f.forecastCacheStore.IndexSnapshot()
	}
	return out
}

// DedicatedDiagnosticSnapshot performs one bounded, allocation-free inventory.
// Call once per overview, using the same input/time for every rendered consumer.
func (f *VOACAPClosedFallback) DedicatedDiagnosticSnapshot(now time.Time, currentSSN int, currentSSNKnown bool) VOACAPClosedFallbackSnapshot {
	if f == nil {
		return VOACAPClosedFallbackSnapshot{}
	}
	now = normalizeVOACAPForecastCacheNow(now)
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.snapshotLocked(now)
	out.SSNComparisonKnown = currentSSNKnown
	nowNS := now.UnixNano()
	hourMask := uint32(1) << uint(now.Hour())
	for _, entry := range f.cache { //nolint:gocritic // One inline map-value copy avoids a second hash lookup or per-owner pointer allocation.
		if expiredVOACAPEntry(&entry, nowNS) || now.Before(entry.forecast.WindowStartUTC) || entry.hourMask&hourMask == 0 {
			continue
		}
		out.UsableCacheEntries++
		if currentSSNKnown && entry.metadata.solar.SSN != currentSSN {
			out.DifferentSSNEntries++
		}
	}
	// The worker can receive just before acquiring mu. Find two oldest pending
	// queue owners, then exclude the received front if len(channel) has advanced.
	var oldest, second *voacapPendingWork
	waiting := 0
	for _, work := range f.inflight {
		if work == nil || work.started {
			continue
		}
		waiting++
		if oldest == nil || int64(work.sequence-oldest.sequence) < 0 {
			second = oldest
			oldest = work
		} else if second == nil || int64(work.sequence-second.sequence) < 0 {
			second = work
		}
	}
	out.QueueDepth = len(f.queue)
	if waiting == out.QueueDepth+1 {
		oldest = second
		waiting--
	}
	out.QueueOldestAgeKnown = waiting == out.QueueDepth
	if out.QueueDepth > 0 && oldest != nil && out.QueueOldestAgeKnown && now.After(oldest.enqueued) {
		out.QueueOldestAge = now.Sub(oldest.enqueued)
	}
	return out
}

func (f *VOACAPClosedFallback) StatsSnapshot() VOACAPFallbackStats {
	if f == nil {
		return VOACAPFallbackStats{}
	}
	invalidReason := func(reason VOACAPInvalidRequestReason) int64 {
		idx := int(reason)
		if idx <= int(VOACAPInvalidRequestReasonNone) || idx >= len(f.statsInvalidReasons) {
			return 0
		}
		return f.statsInvalidReasons[idx].Swap(0)
	}
	return VOACAPFallbackStats{
		InvalidRequest:          f.statsInvalidRequest.Swap(0),
		InvalidUnsupportedBand:  invalidReason(VOACAPInvalidUnsupportedBand),
		InvalidEmptyUnknownBand: invalidReason(VOACAPInvalidEmptyUnknownBand),
		InvalidUserGrid:         invalidReason(VOACAPInvalidUserGrid),
		InvalidDXGrid:           invalidReason(VOACAPInvalidDXGrid),
		InvalidUserCell:         invalidReason(VOACAPInvalidUserCell),
		InvalidDXCell:           invalidReason(VOACAPInvalidDXCell),
		SSNUnavailable:          f.statsSSNUnavailable.Swap(0),
		CacheHit:                f.statsCacheHit.Swap(0),
		NoCurrentHour:           f.statsNoCurrentHour.Swap(0),
		DelayWait:               f.statsDelayWait.Swap(0),
		Inflight:                f.statsInflight.Swap(0),
		NotRunning:              f.statsNotRunning.Swap(0),
		QueueFull:               f.statsQueueFull.Swap(0),
		Queued:                  f.statsQueued.Swap(0),
		RunSuccess:              f.statsRunSuccess.Swap(0),
		RunFailure:              f.statsRunFailure.Swap(0),
	}
}

func (f *VOACAPClosedFallback) recordInvalidRequest(reason VOACAPInvalidRequestReason) {
	f.statsInvalidRequest.Add(1)
	idx := int(reason)
	if idx > int(VOACAPInvalidRequestReasonNone) && idx < len(f.statsInvalidReasons) {
		f.statsInvalidReasons[idx].Add(1)
	}
}

func (f *VOACAPClosedFallback) CheckClosed(req VOACAPClosedRequest, now time.Time) (Result, bool) {
	forecast, ok := f.CheckForecast(req, now)
	if !ok || !f.forecastClosedForMode(forecast, req.Mode) {
		return Result{}, false
	}
	return f.closedResultFromForecast(forecast), true
}

func (f *VOACAPClosedFallback) CheckForecast(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecast, bool) {
	check := f.CheckForecastDetailed(req, now)
	if check.Status != VOACAPForecastCheckReady {
		return VOACAPCachedForecast{}, false
	}
	return check.Forecast, true
}

func (f *VOACAPClosedFallback) CheckForecastDetailed(req VOACAPClosedRequest, now time.Time) VOACAPForecastCheck {
	if f == nil || !f.fallback.Enabled {
		return VOACAPForecastCheck{Status: VOACAPForecastCheckDisabled}
	}
	now = normalizeVOACAPForecastCacheNow(now)
	prepared, reason, ok := f.prepareRequest(req)
	if !ok {
		f.recordInvalidRequest(reason)
		return VOACAPForecastCheck{Status: VOACAPForecastCheckInvalidRequest, InvalidReason: reason}
	}
	input, known := f.currentSolarInput(now)
	key := f.cacheKey(prepared)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneLocked(now)
	noHour := false
	if entry, exists := f.cache[key]; exists {
		if f.cacheExpired(entry, now) {
			delete(f.cache, key)
		} else if record, usable := f.recordForEntry(entry, now); usable {
			f.statsCacheHit.Add(1)
			remaining := f.remaining(entry, now)
			renew := remaining > 0 && remaining <= time.Hour
			if known && (entry.metadata.solar.SSN != input.SSN || renew) {
				f.queueLocked(prepared, key, now, renew, false, true, input.SSN)
			}
			return VOACAPForecastCheck{Status: VOACAPForecastCheckReady, Forecast: f.cachedForecastFromCache(entry, record, prepared, now)}
		} else {
			noHour = true
			f.statsNoCurrentHour.Add(1)
		}
	}
	status, _ := f.queueLocked(prepared, key, now, false, false, known, input.SSN)
	return VOACAPForecastCheck{Status: status, CacheMiss: known, NoCurrentHour: noHour}
}

func (f *VOACAPClosedFallback) CheckForecastWindow(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecastWindow, bool) {
	if f == nil || !f.fallback.Enabled {
		return VOACAPCachedForecastWindow{}, false
	}
	f.CheckForecastDetailed(req, now)
	now = normalizeVOACAPForecastCacheNow(now)
	prepared, _, ok := f.prepareRequest(req)
	if !ok {
		return VOACAPCachedForecastWindow{}, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	window, ok := f.cachedForecastWindowLocked(f.cacheKey(prepared), prepared, now)
	return window, ok
}

// Command refresh bypasses both initial warmup and retry for empty/partial
// windows. Coalesced callers wait on the same publication, never a waiter list.
func (f *VOACAPClosedFallback) CheckForecastWindowWait(req VOACAPClosedRequest, now time.Time, wait time.Duration) (VOACAPCachedForecastWindow, VOACAPForecastWindowStatus) {
	if f == nil || !f.fallback.Enabled {
		return VOACAPCachedForecastWindow{}, VOACAPForecastWindowUnavailable
	}
	now = normalizeVOACAPForecastCacheNow(now)
	prepared, reason, valid := f.prepareRequest(req)
	if !valid {
		f.recordInvalidRequest(reason)
		return VOACAPCachedForecastWindow{}, VOACAPForecastWindowUnavailable
	}
	input, known := f.currentSolarInput(now)
	key := f.cacheKey(prepared)
	f.mu.Lock()
	f.pruneLocked(now)
	window, cached := f.cachedForecastWindowLocked(key, prepared, now)
	complete := cached && len(window.Records) >= f.fallback.ForecastHours
	if complete && (!known || f.cache[key].metadata.solar.SSN == input.SSN) {
		f.statsCacheHit.Add(1)
		f.mu.Unlock()
		return window, VOACAPForecastWindowReady
	}
	status, work := f.queueLocked(prepared, key, now, !complete, !complete, known, input.SSN)
	f.mu.Unlock()
	if work == nil {
		switch status {
		case VOACAPForecastCheckQueueFull:
			return window, VOACAPForecastWindowBusy
		case VOACAPForecastCheckDelayWait:
			return window, VOACAPForecastWindowRefreshing
		default:
			return window, VOACAPForecastWindowUnavailable
		}
	}
	if wait <= 0 {
		return window, VOACAPForecastWindowRefreshing
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-work.published:
		if work.err != nil {
			return window, VOACAPForecastWindowFailed
		}
		refreshNow := f.clockNow().UTC()
		if refreshNow.Before(now) {
			refreshNow = now
		}
		refreshed, ok := f.checkCachedForecastWindowNoStats(prepared, key, refreshNow)
		if !ok {
			return window, VOACAPForecastWindowFailed
		}
		if len(refreshed.Records) < f.fallback.ForecastHours || (known && len(refreshed.Records) > 0 && refreshed.Records[0].SSN != input.SSN) {
			return refreshed, VOACAPForecastWindowRefreshing
		}
		return refreshed, VOACAPForecastWindowReady
	case <-timer.C:
		return window, VOACAPForecastWindowRefreshing
	}
}

func (f *VOACAPClosedFallback) CheckCachedForecast(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecast, bool) {
	if f == nil || !f.fallback.Enabled {
		return VOACAPCachedForecast{}, false
	}
	now = normalizeVOACAPForecastCacheNow(now)
	prepared, _, ok := f.prepareRequest(req)
	if !ok {
		return VOACAPCachedForecast{}, false
	}
	key := f.cacheKey(prepared)
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.cache[key]
	if !ok || f.cacheExpired(entry, now) {
		return VOACAPCachedForecast{}, false
	}
	record, ok := f.recordForEntry(entry, now)
	if !ok {
		return VOACAPCachedForecast{}, false
	}
	return f.cachedForecastFromCache(entry, record, prepared, now), true
}

func (f *VOACAPClosedFallback) checkCachedForecastWindowNoStats(req VOACAPClosedRequest, key voacapCacheKey, now time.Time) (VOACAPCachedForecastWindow, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	window, ok := f.cachedForecastWindowLocked(key, req, now)
	return window, ok
}

func (f *VOACAPClosedFallback) cachedForecastWindowLocked(key voacapCacheKey, req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecastWindow, bool) {
	entry, ok := f.cache[key]
	if !ok || f.cacheExpired(entry, now) {
		return VOACAPCachedForecastWindow{}, false
	}
	records := f.recordsForEntry(entry, now)
	if len(records) == 0 {
		return VOACAPCachedForecastWindow{}, false
	}
	return f.cachedForecastWindowFromCache(entry, records, req, now), true
}

func (f *VOACAPClosedFallback) currentSolarInput(now time.Time) (voacap.SolarInputSnapshot, bool) {
	if provider, ok := f.ssnProvider.(coherentSolarInputProvider); ok {
		return provider.CurrentSolarInput(now)
	}
	ssn, ok := f.ssnProvider.CurrentSSN(now)
	return voacap.SolarInputSnapshot{SSN: ssn}, ok
}

func (f *VOACAPClosedFallback) queueLocked(req VOACAPClosedRequest, key voacapCacheKey, now time.Time, bypassWarmup, bypassFailure, known bool, desiredSSN int) (VOACAPForecastCheckStatus, *voacapPendingWork) {
	if work, ok := f.inflight[key]; ok {
		if work != nil && work.started && work.inputKnown && known && work.ssn != desiredSSN && !bypassWarmup && !bypassFailure {
			dk := f.delayKeyForCacheKey(key)
			if d, exists := f.delays[dk]; !exists || d.kind != voacapDelayFailure {
				f.delayElapsedLocked(dk, now)
			}
		}
		f.statsInflight.Add(1)
		return VOACAPForecastCheckInflight, work
	}
	if !known {
		f.statsSSNUnavailable.Add(1)
		return VOACAPForecastCheckSSNUnavailable, nil
	}
	dk := f.delayKeyForCacheKey(key)
	if !bypassFailure {
		if d, ok := f.delays[dk]; ok && d.kind == voacapDelayFailure && now.Before(d.firstSeen.Add(time.Duration(f.fallback.DelaySeconds)*time.Second)) {
			f.statsDelayWait.Add(1)
			return VOACAPForecastCheckDelayWait, nil
		}
	}
	if !bypassWarmup && !f.delayElapsedLocked(dk, now) {
		f.statsDelayWait.Add(1)
		return VOACAPForecastCheckDelayWait, nil
	}
	if !f.running.Load() {
		f.statsNotRunning.Add(1)
		return VOACAPForecastCheckNotRunning, nil
	}
	if len(f.queue) >= cap(f.queue) {
		f.statsQueueFull.Add(1)
		return VOACAPForecastCheckQueueFull, nil
	}
	f.workSequence++
	work := &voacapPendingWork{request: req, key: key, enqueued: now, sequence: f.workSequence, published: make(chan struct{})}
	f.inflight[key] = work
	delete(f.delays, dk)
	f.queue <- work
	f.statsQueued.Add(1)
	return VOACAPForecastCheckQueued, work
}

func (f *VOACAPClosedFallback) prepareRequest(req VOACAPClosedRequest) (VOACAPClosedRequest, VOACAPInvalidRequestReason, bool) {
	req.Band = normalizeBand(req.Band)
	if req.Band == "" || req.Band == "???" || req.Band == "unknown" {
		return VOACAPClosedRequest{}, VOACAPInvalidEmptyUnknownBand, false
	}
	if _, ok := centerFrequencyKHzForBand(req.Band, f.fallback.CenterFrequenciesMHz); !ok {
		return VOACAPClosedRequest{}, VOACAPInvalidUnsupportedBand, false
	}
	if _, _, ok := GridCenterLatLon(req.UserGrid); !ok {
		return VOACAPClosedRequest{}, VOACAPInvalidUserGrid, false
	}
	if _, _, ok := GridCenterLatLon(req.DXGrid); !ok {
		return VOACAPClosedRequest{}, VOACAPInvalidDXGrid, false
	}
	if req.UserCell == InvalidCell {
		return VOACAPClosedRequest{}, VOACAPInvalidUserCell, false
	}
	if req.DXCell == InvalidCell {
		return VOACAPClosedRequest{}, VOACAPInvalidDXCell, false
	}
	return req, VOACAPInvalidRequestReasonNone, true
}

func (f *VOACAPClosedFallback) forecastClosedForMode(forecast VOACAPCachedForecast, mode string) bool {
	return ClosedForDB(forecast.EffectiveDB(), mode, f.cfg)
}

func (f *VOACAPClosedFallback) cacheKey(req VOACAPClosedRequest) voacapCacheKey {
	khz, _ := centerFrequencyKHzForBand(req.Band, f.fallback.CenterFrequenciesMHz)
	return voacapCacheKey{userCell: req.UserCell, dxCell: req.DXCell, band: req.Band, frequencyKHz: khz, direction: voacapDirectionBidirectionalID}
}

func (f *VOACAPClosedFallback) delayKey(req VOACAPClosedRequest) voacapDelayKey {
	return f.delayKeyForCacheKey(f.cacheKey(req))
}

func (f *VOACAPClosedFallback) delayKeyForCacheKey(key voacapCacheKey) voacapDelayKey {
	if key.direction != voacapDirectionBidirectionalID {
		panic("noncanonical VOACAP delay direction")
	}
	return voacapDelayKey{userCell: key.userCell, dxCell: key.dxCell, direction: voacapDirectionBidirectionalID, band: key.band, frequencyKHz: key.frequencyKHz}
}

func (f *VOACAPClosedFallback) delayElapsedLocked(key voacapDelayKey, now time.Time) bool {
	delay := time.Duration(f.fallback.DelaySeconds) * time.Second
	if delay <= 0 {
		return true
	}
	entry, ok := f.delays[key]
	if !ok {
		f.addDelayLocked(key, voacapDelayEntry{firstSeen: now, lastSeen: now})
		return false
	}
	if entry.kind == voacapDelayRetired {
		return true
	}
	entry.lastSeen = now
	f.delays[key] = entry
	return !now.Before(entry.firstSeen.Add(delay))
}

func (f *VOACAPClosedFallback) clockNow() time.Time {
	if f.now != nil {
		return f.now()
	}
	return time.Now()
}

func (f *VOACAPClosedFallback) worker(ctx context.Context) {
	defer f.wg.Done()
	defer func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		// Admission uses this same lock: no requester can enqueue after the
		// final drain while still observing a running worker.
		f.running.Store(false)
		for {
			select {
			case work := <-f.queue:
				delete(f.inflight, work.key)
				work.err = ctx.Err()
				close(work.published)
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case work := <-f.queue:
			origin := f.clockNow()
			f.mu.Lock()
			work.started = true
			f.workerBusy = true
			f.workerStartedAt = origin
			f.mu.Unlock()
			input, known := f.currentSolarInput(origin.UTC())
			f.mu.Lock()
			work.ssn = input.SSN
			work.inputKnown = known
			f.mu.Unlock()
			job := VOACAPClosedJob{Request: work.request, SSN: input.SSN, SolarInput: input, WindowStartUTC: forecastWindowStart(origin), FrequencyMHz: float64(work.key.frequencyKHz) / 1000, key: work.key, delayKey: f.delayKeyForCacheKey(work.key), origin: origin, work: work}
			if !known {
				f.completeJob(ctx, job, VOACAPClosedForecast{}, fmt.Errorf("VOACAP solar input unavailable at dequeue"))
				continue
			}
			f.runJob(ctx, job)
		}
	}
}

func (f *VOACAPClosedFallback) runJob(ctx context.Context, job VOACAPClosedJob) {
	if job.origin.IsZero() {
		job.origin = f.clockNow()
		job.SolarInput.SSN = job.SSN
	}
	forecast, err := f.forecaster.ForecastClosed(ctx, job)
	f.completeJob(ctx, job, forecast, err)
}

func (f *VOACAPClosedFallback) completeJob(ctx context.Context, job VOACAPClosedJob, forecast VOACAPClosedForecast, err error) {
	defer func() {
		elapsed := f.clockNow().Sub(job.origin)
		if elapsed < 0 {
			elapsed = 0
		}
		f.mu.Lock()
		if job.work == nil || f.inflight[job.key] == job.work {
			delete(f.inflight, job.key)
		}
		f.workerBusy = false
		f.workerDurationCount++
		if elapsed > time.Duration(math.MaxInt64)-f.workerDurationSum {
			f.workerDurationSum = time.Duration(math.MaxInt64)
		} else {
			f.workerDurationSum += elapsed
		}
		if elapsed > f.workerDurationMax {
			f.workerDurationMax = elapsed
		}
		f.mu.Unlock()
	}()
	now := f.clockNow().UTC()
	if err == nil {
		err = f.validateCompletedForecast(job, forecast, now)
	}
	var entry voacapCacheEntry
	if err == nil {
		entry = f.completedCacheEntry(job, forecast, now)
	}
	f.mu.Lock()
	if err != nil {
		if ctx.Err() == nil {
			f.statsRunFailure.Add(1)
			if f.logger != nil {
				f.logger.Printf("VOACAP closed fallback failed user_cell=%d dx_cell=%d band=%s ssn=%d: %v", job.Request.UserCell, job.Request.DXCell, job.Request.Band, job.SSN, err)
			}
			f.addDelayLocked(job.delayKey, voacapDelayEntry{firstSeen: now, lastSeen: now, kind: voacapDelayFailure})
		}
		f.publishJobLocked(job, err)
		f.mu.Unlock()
		return
	}
	f.statsRunSuccess.Add(1)
	// One worker and one pending owner per path prevent an older in-flight
	// completion from overwriting a later owner. Disk also compares actual origin.
	if job.work != nil && f.inflight[job.key] != job.work {
		f.publishJobLocked(job, nil)
		f.mu.Unlock()
		return
	}
	if previous, ok := f.cache[job.key]; ok && previous.metadata != nil && previous.metadata.originNS > job.origin.UnixNano() {
		f.publishJobLocked(job, nil)
		f.mu.Unlock()
		return
	}
	f.addCacheLocked(job.key, entry)
	store := f.forecastCacheStore
	f.publishJobLocked(job, nil)
	f.mu.Unlock()
	if store != nil {
		if persistErr := store.storeForecast(job.key, entry, f.fallback, now); persistErr != nil && f.logger != nil {
			f.logger.Printf("Warning: VOACAP forecast cache persist failed user_cell=%d dx_cell=%d band=%s ssn=%d: %v", job.Request.UserCell, job.Request.DXCell, job.Request.Band, job.SSN, persistErr)
		}
	}
}

// Allocate compact immutable owners before acquiring the delivery mutex.
func (f *VOACAPClosedFallback) completedCacheEntry(job VOACAPClosedJob, forecast VOACAPClosedForecast, now time.Time) voacapCacheEntry {
	records := make([]VOACAPHourlyForecast, len(forecast.Records))
	copy(records, forecast.Records)
	forecast.Records = records
	forecast.OutputPath = ""
	end := job.WindowStartUTC.Add(time.Duration(f.fallback.ForecastHours) * time.Hour)
	expiry := now.Add(time.Duration(f.fallback.CacheTTLSeconds) * time.Second)
	if end.Before(expiry) {
		expiry = end
	}
	return voacapCacheEntry{forecast: forecast, storedAt: now, metadata: &voacapForecastMetadata{solar: job.SolarInput, originNS: job.origin.UnixNano()}, windowEndNS: end.UnixNano(), expiresNS: expiry.UnixNano(), hourMask: forecastCacheHourMask(records), flags: voacapMetadataInitialized}
}

func (f *VOACAPClosedFallback) validateCompletedForecast(job VOACAPClosedJob, forecast VOACAPClosedForecast, now time.Time) error {
	hours := f.fallback.ForecastHours
	if !forecast.WindowStartUTC.Equal(job.WindowStartUTC) || hours < 1 || hours > 24 || len(forecast.Records) != hours {
		return fmt.Errorf("VOACAP replacement does not contain the requested complete window")
	}
	var actual, want uint32
	for i := 0; i < hours; i++ {
		want |= 1 << uint(job.WindowStartUTC.Add(time.Duration(i)*time.Hour).Hour())
	}
	for _, row := range forecast.Records {
		if row.HourUTC < 0 || row.HourUTC > 23 || actual&(1<<uint(row.HourUTC)) != 0 || !finiteVOACAPValue(row.FrequencyMHz) || !finiteVOACAPValue(row.ReqSNRReliability) || !finiteVOACAPValue(row.ReceiveReqSNRReliability) || !finiteVOACAPValue(row.TransmitReqSNRReliability) || bandForMHz(row.FrequencyMHz) != job.Request.Band {
			return fmt.Errorf("VOACAP replacement contains malformed hourly records")
		}
		actual |= 1 << uint(row.HourUTC)
	}
	p := job.SolarInput.Provenance
	if job.SSN < 0 || job.SolarInput.SSN != job.SSN || !finiteVOACAPValue(p.LatestNoonFluxSFU) || !finiteVOACAPValue(p.RecentEWMAFluxSFU) || !finiteVOACAPValue(p.PublishedMeanFluxSFU) || !finiteVOACAPValue(p.BlendedFluxSFU) || !finiteVOACAPValue(p.RecentWeight) {
		return fmt.Errorf("VOACAP replacement contains malformed solar input")
	}
	end := job.WindowStartUTC.Add(time.Duration(hours) * time.Hour)
	expiry := now.Add(time.Duration(f.fallback.CacheTTLSeconds) * time.Second)
	if actual != want || now.Before(job.origin) || expiry.Before(now) || now.Before(job.WindowStartUTC) || !now.Before(end) || end.UnixNano() <= job.WindowStartUTC.UnixNano() || expiry.UnixNano() <= 0 {
		return fmt.Errorf("VOACAP replacement is outside its original window")
	}
	return nil
}

func finiteVOACAPValue(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func (f *VOACAPClosedFallback) publishJobLocked(job VOACAPClosedJob, err error) {
	if job.work != nil {
		select {
		case <-job.work.published:
		default:
			job.work.err = err
			close(job.work.published)
		}
	}
}

func (f *VOACAPClosedFallback) cachedForecastFromCache(entry voacapCacheEntry, record VOACAPHourlyForecast, req VOACAPClosedRequest, now time.Time) VOACAPCachedForecast {
	ageSec := int64(0)
	if now.After(entry.storedAt) {
		ageSec = int64(now.Sub(entry.storedAt).Seconds())
	}
	effective := effectiveVOACAPFT8SNRDB(record, req.ReceiveNoisePenaltyDB, f.cfg)
	record.FT8SNRDB = int(math.Round(effective))
	return VOACAPCachedForecast{
		Record:                record,
		EffectiveFT8SNRDB:     effective,
		HasEffectiveFT8SNRDB:  true,
		ReceiveNoisePenaltyDB: req.ReceiveNoisePenaltyDB,
		SSN:                   entry.metadata.solar.SSN,
		AgeSec:                ageSec,
	}
}

func (f *VOACAPClosedFallback) cachedForecastWindowFromCache(entry voacapCacheEntry, records []VOACAPHourlyForecast, req VOACAPClosedRequest, now time.Time) VOACAPCachedForecastWindow {
	out := VOACAPCachedForecastWindow{
		WindowStartUTC: entry.forecast.WindowStartUTC.UTC(),
		Records:        make([]VOACAPCachedForecast, 0, len(records)),
	}
	for _, record := range records {
		out.Records = append(out.Records, f.cachedForecastFromCache(entry, record, req, now))
	}
	return out
}

func (f *VOACAPClosedFallback) closedResultFromForecast(forecast VOACAPCachedForecast) Result {
	return VOACAPClosedResult(f.cfg, forecast)
}

// VOACAPClosedResult maps a cached VOACAP forecast to the closed fallback result.
func VOACAPClosedResult(cfg Config, forecast VOACAPCachedForecast) Result {
	res := Result{
		Glyph:              cfg.GlyphSymbols.Closed,
		Class:              classUnlikely,
		P50DB:              forecast.EffectiveDB(),
		HasP50:             true,
		P50Glyph:           cfg.GlyphSymbols.Closed,
		AgeSec:             forecast.AgeSec,
		Source:             SourceVOACAPClosed,
		VOACAPFT8SNRDB:     forecast.Record.FT8SNRDB,
		VOACAPSSN:          forecast.SSN,
		VOACAPAgeSec:       forecast.AgeSec,
		VOACAPHourUTC:      forecast.Record.HourUTC,
		VOACAPFrequencyMHz: forecast.Record.FrequencyMHz,
	}
	attachVOACAPDiagnostics(&res, forecast)
	return res
}

// VOACAPBeaconClosedResult maps a receive-leg VOACAP forecast to the closed
// fallback result for beacon RX-only paths.
func VOACAPBeaconClosedResult(cfg Config, forecast VOACAPCachedForecast) Result {
	res := VOACAPClosedResult(cfg, forecast)
	res.BeaconRX = true
	attachReceiveVOACAPDiagnostics(&res, forecast)
	res.P50DB = forecast.ReceiveDB()
	return res
}

// VOACAPAlignedResult maps sparse p50 evidence corroborated by VOACAP to a
// normal path glyph while retaining both bucket and VOACAP diagnostics.
func VOACAPAlignedResult(base Result, cfg Config, mode string, forecast VOACAPCachedForecast) Result {
	class := ClassForDB(base.P50DB, mode, cfg)
	base.Glyph = glyphForClass(class, cfg)
	base.Class = class
	base.P50Glyph = base.Glyph
	base.Source = SourceVOACAPAligned
	base.InsufficientReason = InsufficientNone
	attachVOACAPDiagnostics(&base, forecast)
	return base
}

// VOACAPBeaconAlignedResult maps sparse beacon RX-only p50 evidence
// corroborated by receive-leg VOACAP to a normal path glyph.
func VOACAPBeaconAlignedResult(base Result, cfg Config, mode string, forecast VOACAPCachedForecast) Result {
	base = VOACAPAlignedResult(base, cfg, mode, forecast)
	base.BeaconRX = true
	attachReceiveVOACAPDiagnostics(&base, forecast)
	return base
}

// VOACAPSparseUpgradeResult maps sparse p50 evidence to a one-tier stronger
// VOACAP class after the request-SNR REL gate has passed.
func VOACAPSparseUpgradeResult(base Result, cfg Config, mode string, forecast VOACAPCachedForecast) Result {
	class := ClassForDB(forecast.EffectiveDB(), mode, cfg)
	base.Glyph = glyphForClass(class, cfg)
	base.Class = class
	base.Source = SourceVOACAPSparseUpgrade
	base.InsufficientReason = InsufficientNone
	attachVOACAPDiagnostics(&base, forecast)
	return base
}

// VOACAPBeaconSparseUpgradeResult maps sparse beacon p50 evidence to a one-tier
// stronger receive-leg VOACAP class after the receive REL gate has passed.
func VOACAPBeaconSparseUpgradeResult(base Result, cfg Config, mode string, forecast VOACAPCachedForecast) Result {
	class := ClassForDB(forecast.ReceiveDB(), mode, cfg)
	base.Glyph = glyphForClass(class, cfg)
	base.Class = class
	base.Source = SourceVOACAPSparseUpgrade
	base.BeaconRX = true
	base.InsufficientReason = InsufficientNone
	attachReceiveVOACAPDiagnostics(&base, forecast)
	return base
}

// VOACAPOpenResult maps a no-p50 insufficient result to an ordinary open
// VOACAP class after the request-SNR REL gate has passed.
func VOACAPOpenResult(cfg Config, mode string, forecast VOACAPCachedForecast) Result {
	class := ClassForDB(forecast.EffectiveDB(), mode, cfg)
	res := Result{
		Glyph:  glyphForClass(class, cfg),
		Class:  class,
		P50DB:  forecast.EffectiveDB(),
		HasP50: false,
		AgeSec: forecast.AgeSec,
		Source: SourceVOACAPOpen,
	}
	attachVOACAPDiagnostics(&res, forecast)
	return res
}

// VOACAPBeaconOpenResult maps a no-p50 insufficient beacon result to a
// receive-leg VOACAP class after the receive REL gate has passed.
func VOACAPBeaconOpenResult(cfg Config, mode string, forecast VOACAPCachedForecast) Result {
	class := ClassForDB(forecast.ReceiveDB(), mode, cfg)
	res := Result{
		Glyph:    glyphForClass(class, cfg),
		Class:    class,
		P50DB:    forecast.ReceiveDB(),
		HasP50:   false,
		AgeSec:   forecast.AgeSec,
		Source:   SourceVOACAPOpen,
		BeaconRX: true,
	}
	attachReceiveVOACAPDiagnostics(&res, forecast)
	return res
}

func attachVOACAPDiagnostics(res *Result, forecast VOACAPCachedForecast) {
	if res == nil {
		return
	}
	res.VOACAPFT8SNRDB = forecast.Record.FT8SNRDB
	res.VOACAPSSN = forecast.SSN
	res.VOACAPAgeSec = forecast.AgeSec
	res.VOACAPHourUTC = forecast.Record.HourUTC
	res.VOACAPFrequencyMHz = forecast.Record.FrequencyMHz
	if rel, ok := forecast.ReqSNRReliability(); ok {
		res.VOACAPReqSNRReliability = rel
		res.VOACAPHasReqSNRReliability = true
	}
}

func attachReceiveVOACAPDiagnostics(res *Result, forecast VOACAPCachedForecast) {
	if res == nil {
		return
	}
	res.VOACAPFT8SNRDB = int(math.Round(forecast.ReceiveDB()))
	res.VOACAPSSN = forecast.SSN
	res.VOACAPAgeSec = forecast.AgeSec
	res.VOACAPHourUTC = forecast.Record.HourUTC
	res.VOACAPFrequencyMHz = forecast.Record.FrequencyMHz
	if rel, ok := forecast.ReceiveReqSNRReliability(); ok {
		res.VOACAPReqSNRReliability = rel
		res.VOACAPHasReqSNRReliability = true
	}
}

func effectiveVOACAPFT8SNRDB(record VOACAPHourlyForecast, receiveNoisePenaltyDB float64, cfg Config) float64 {
	if !record.HasDirectionalSNR {
		return float64(record.FT8SNRDB)
	}
	receive := float64(record.ReceiveFT8SNRDB)
	if receiveNoisePenaltyDB > 0 {
		receive -= receiveNoisePenaltyDB
	}
	return cfg.MergeReceiveWeight*receive + cfg.MergeTransmitWeight*float64(record.TransmitFT8SNRDB)
}

func (f *VOACAPClosedFallback) pruneLocked(now time.Time) {
	if !f.lastPrune.IsZero() && now.Sub(f.lastPrune) < time.Minute {
		return
	}
	f.lastPrune = now
	nowNS := now.UnixNano()
	for key, entry := range f.cache { //nolint:gocritic // Inline map owners require one copy; a second lookup or per-owner pointer allocation adds hot-path cost.
		if expiredVOACAPEntry(&entry, nowNS) {
			delete(f.cache, key)
		}
	}
	delayTTL := time.Duration(f.fallback.CacheTTLSeconds+f.fallback.DelaySeconds) * time.Second
	for key, entry := range f.delays {
		if now.Sub(entry.lastSeen) > delayTTL {
			delete(f.delays, key)
		}
	}
}

func (f *VOACAPClosedFallback) cacheExpired(entry voacapCacheEntry, now time.Time) bool {
	return expiredVOACAPEntry(&entry, now.UnixNano())
}

func expiredVOACAPEntry(entry *voacapCacheEntry, nowNS int64) bool {
	return entry.metadata == nil || entry.flags&voacapMetadataInitialized == 0 || nowNS > entry.expiresNS || nowNS >= entry.windowEndNS
}

func (f *VOACAPClosedFallback) remaining(entry voacapCacheEntry, now time.Time) time.Duration {
	end := min(entry.windowEndNS, entry.expiresNS)
	return time.Unix(0, end).Sub(now)
}

func (f *VOACAPClosedFallback) recordForEntry(entry voacapCacheEntry, now time.Time) (VOACAPHourlyForecast, bool) {
	if entry.metadata == nil || entry.flags&voacapMetadataInitialized == 0 {
		return VOACAPHourlyForecast{}, false
	}
	if now.Before(entry.forecast.WindowStartUTC) || f.cacheExpired(entry, now) || entry.hourMask&(1<<uint(now.Hour())) == 0 {
		return VOACAPHourlyForecast{}, false
	}
	for _, row := range entry.forecast.Records {
		if row.HourUTC == now.Hour() {
			return row, true
		}
	}
	return VOACAPHourlyForecast{}, false
}

func (f *VOACAPClosedFallback) recordsForEntry(entry voacapCacheEntry, now time.Time) []VOACAPHourlyForecast {
	if entry.metadata == nil || entry.flags&voacapMetadataInitialized == 0 {
		return nil
	}
	if now.Before(entry.forecast.WindowStartUTC) || f.cacheExpired(entry, now) {
		return nil
	}
	out := make([]VOACAPHourlyForecast, 0, len(entry.forecast.Records))
	for at := now.Truncate(time.Hour); at.UnixNano() < entry.windowEndNS; at = at.Add(time.Hour) {
		for _, row := range entry.forecast.Records {
			if row.HourUTC == at.Hour() {
				out = append(out, row)
				break
			}
		}
	}
	return out
}

func (f *VOACAPClosedFallback) addCacheLocked(key voacapCacheKey, entry voacapCacheEntry) {
	if len(f.cache) >= f.fallback.MaxCacheEntries {
		// At capacity, replacing an owner must not evict another key.
		if _, exists := f.cache[key]; exists {
			f.cache[key] = entry
			return
		}
		var oldestKey voacapCacheKey
		var oldest time.Time
		first := true
		for candidate, existing := range f.cache { //nolint:gocritic // Keep one range copy instead of rehashing each key or allocating pointer-owned entries.
			order := existing.storedAt.Compare(oldest)
			if first || order < 0 || (order == 0 && compareLiveCacheKey(&candidate, &oldestKey) > 0) {
				oldestKey = candidate
				oldest = existing.storedAt
				first = false
			}
		}
		delete(f.cache, oldestKey)
	}
	f.cache[key] = entry
}

func (f *VOACAPClosedFallback) addDelayLocked(key voacapDelayKey, entry voacapDelayEntry) {
	if len(f.delays) >= f.fallback.MaxDelayEntries {
		if _, exists := f.delays[key]; exists {
			f.delays[key] = entry
			return
		}
		var oldestKey voacapDelayKey
		var oldest time.Time
		first := true
		for candidate, existing := range f.delays {
			order := existing.lastSeen.Compare(oldest)
			if first || order < 0 || (order == 0 && compareLiveDelayKey(&candidate, &oldestKey) > 0) {
				oldestKey = candidate
				oldest = existing.lastSeen
				first = false
			}
		}
		delete(f.delays, oldestKey)
	}
	f.delays[key] = entry
}

func forecastWindowStart(now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.UTC().Truncate(time.Hour)
}

func centerFrequencyKHzForBand(band string, freqs []float64) (int, bool) {
	band = normalizeBand(band)
	for _, freq := range freqs {
		if bandForMHz(freq) == band {
			return int(math.Round(freq * 1000)), true
		}
	}
	return 0, false
}

func VOACAPFallbackBands(cfg VOACAPFallbackConfig) []string {
	seen := make(map[string]struct{}, len(cfg.CenterFrequenciesMHz))
	bands := make([]string, 0, len(cfg.CenterFrequenciesMHz))
	for _, freq := range cfg.CenterFrequenciesMHz {
		band := bandForMHz(freq)
		if band == "" {
			continue
		}
		if _, ok := seen[band]; ok {
			continue
		}
		seen[band] = struct{}{}
		bands = append(bands, band)
	}
	return bands
}

func VOACAPFallbackCenterFrequencyMHz(cfg VOACAPFallbackConfig, band string) (float64, bool) {
	khz, ok := centerFrequencyKHzForBand(band, cfg.CenterFrequenciesMHz)
	if !ok {
		return 0, false
	}
	return float64(khz) / 1000, true
}

func bandForMHz(freq float64) string {
	switch {
	case freq >= 1.8 && freq < 2.0:
		return "160m"
	case freq >= 3.5 && freq < 4.0:
		return "80m"
	case freq >= 5.0 && freq < 5.5:
		return "60m"
	case freq >= 7.0 && freq < 7.4:
		return "40m"
	case freq >= 10.0 && freq < 10.2:
		return "30m"
	case freq >= 14.0 && freq < 14.4:
		return "20m"
	case freq >= 18.0 && freq < 18.3:
		return "17m"
	case freq >= 21.0 && freq < 21.6:
		return "15m"
	case freq >= 24.8 && freq < 25.1:
		return "12m"
	case freq >= 28.0 && freq < 30.0:
		return "10m"
	case freq >= 50.0 && freq < 54.0:
		return "6m"
	default:
		return ""
	}
}
