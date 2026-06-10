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

// ClosedFallbackStatsProvider exposes reset-on-snapshot stage counters for
// optional propagation-log diagnostics without changing the lookup interface.
type ClosedFallbackStatsProvider interface {
	StatsSnapshot() VOACAPFallbackStats
}

type VOACAPSSNProvider interface {
	CurrentSSN(now time.Time) (int, bool)
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

	key      voacapCacheKey
	delayKey voacapDelayKey
}

type VOACAPClosedForecast struct {
	WindowStartUTC time.Time
	Records        []VOACAPHourlyForecast
	OutputPath     string
	Elapsed        time.Duration
}

type VOACAPHourlyForecast struct {
	FT8SNRDB              int
	VOACAPSNRDBHz         int
	HourUTC               int
	FrequencyMHz          float64
	ReceiveFT8SNRDB       int
	TransmitFT8SNRDB      int
	ReceiveVOACAPSNRDBHz  int
	TransmitVOACAPSNRDBHz int
	HasDirectionalSNR     bool
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

// VOACAPCachedForecastWindow carries cached hourly VOACAP rows from the current
// UTC hour through the configured forecast horizon. Missing rows mean the caller
// should report a nonblocking cold miss rather than run VOACAP synchronously.
type VOACAPCachedForecastWindow struct {
	WindowStartUTC time.Time
	Records        []VOACAPCachedForecast
}

type VOACAPClosedFallbackSnapshot struct {
	CacheEntries    int
	DelayEntries    int
	InflightEntries int
	QueueDepth      int
}

// VOACAPFallbackStats explains why fallback work did or did not emit a glyph
// during one propagation-log interval. Final emitted glyph counts stay in the
// telnet path prediction counters.
type VOACAPFallbackStats struct {
	InvalidRequest int64
	SSNUnavailable int64
	CacheHit       int64
	NoCurrentHour  int64
	DelayWait      int64
	Inflight       int64
	NotRunning     int64
	QueueFull      int64
	Queued         int64
	RunSuccess     int64
	RunFailure     int64
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
	queue       chan VOACAPClosedJob

	mu        sync.Mutex
	cache     map[voacapCacheKey]voacapCacheEntry
	delays    map[voacapDelayKey]voacapDelayEntry
	inflight  map[voacapCacheKey]struct{}
	lastPrune time.Time
	startOnce sync.Once
	wg        sync.WaitGroup
	running   atomic.Bool

	statsInvalidRequest atomic.Int64
	statsSSNUnavailable atomic.Int64
	statsCacheHit       atomic.Int64
	statsNoCurrentHour  atomic.Int64
	statsDelayWait      atomic.Int64
	statsInflight       atomic.Int64
	statsNotRunning     atomic.Int64
	statsQueueFull      atomic.Int64
	statsQueued         atomic.Int64
	statsRunSuccess     atomic.Int64
	statsRunFailure     atomic.Int64
}

type voacapCacheKey struct {
	userCell     CellID
	dxCell       CellID
	band         string
	frequencyKHz int
	year         int
	month        int
	ssn          int
	direction    string
}

type voacapDelayKey struct {
	userCell     CellID
	dxCell       CellID
	band         string
	frequencyKHz int
	direction    string
}

type voacapCacheEntry struct {
	forecast VOACAPClosedForecast
	storedAt time.Time
}

type voacapDelayEntry struct {
	firstSeen time.Time
	lastSeen  time.Time
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
		queue:       make(chan VOACAPClosedJob, fallback.MaxQueueDepth),
		cache:       make(map[voacapCacheKey]voacapCacheEntry),
		delays:      make(map[voacapDelayKey]voacapDelayEntry),
		inflight:    make(map[voacapCacheKey]struct{}),
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

func (f *VOACAPClosedFallback) Snapshot() VOACAPClosedFallbackSnapshot {
	if f == nil {
		return VOACAPClosedFallbackSnapshot{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return VOACAPClosedFallbackSnapshot{
		CacheEntries:    len(f.cache),
		DelayEntries:    len(f.delays),
		InflightEntries: len(f.inflight),
		QueueDepth:      len(f.queue),
	}
}

func (f *VOACAPClosedFallback) StatsSnapshot() VOACAPFallbackStats {
	if f == nil {
		return VOACAPFallbackStats{}
	}
	return VOACAPFallbackStats{
		InvalidRequest: f.statsInvalidRequest.Swap(0),
		SSNUnavailable: f.statsSSNUnavailable.Swap(0),
		CacheHit:       f.statsCacheHit.Swap(0),
		NoCurrentHour:  f.statsNoCurrentHour.Swap(0),
		DelayWait:      f.statsDelayWait.Swap(0),
		Inflight:       f.statsInflight.Swap(0),
		NotRunning:     f.statsNotRunning.Swap(0),
		QueueFull:      f.statsQueueFull.Swap(0),
		Queued:         f.statsQueued.Swap(0),
		RunSuccess:     f.statsRunSuccess.Swap(0),
		RunFailure:     f.statsRunFailure.Swap(0),
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
	if f == nil || !f.fallback.Enabled {
		return VOACAPCachedForecast{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	prepared, ok := f.prepareRequest(req)
	if !ok {
		f.statsInvalidRequest.Add(1)
		return VOACAPCachedForecast{}, false
	}
	ssn, ok := f.ssnProvider.CurrentSSN(now)
	if !ok {
		f.statsSSNUnavailable.Add(1)
		return VOACAPCachedForecast{}, false
	}
	key := f.cacheKey(prepared, ssn, now)
	delayKey := f.delayKey(prepared)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneLocked(now)
	if entry, ok := f.cache[key]; ok {
		if f.cacheExpired(entry, now) {
			delete(f.cache, key)
		} else {
			record, ok := forecastRecordForHour(entry.forecast, now, f.fallback.ForecastHours)
			if !ok {
				f.statsNoCurrentHour.Add(1)
				delete(f.cache, key)
			} else {
				f.statsCacheHit.Add(1)
				return f.cachedForecastFromCache(key, entry, record, prepared, now), true
			}
		}
	}
	if _, ok := f.inflight[key]; ok {
		f.statsInflight.Add(1)
		return VOACAPCachedForecast{}, false
	}
	if !f.delayElapsedLocked(delayKey, now) {
		f.statsDelayWait.Add(1)
		return VOACAPCachedForecast{}, false
	}
	if !f.running.Load() {
		f.statsNotRunning.Add(1)
		return VOACAPCachedForecast{}, false
	}
	if len(f.queue) >= cap(f.queue) {
		f.statsQueueFull.Add(1)
		return VOACAPCachedForecast{}, false
	}
	job := VOACAPClosedJob{
		Request:        prepared,
		SSN:            ssn,
		WindowStartUTC: forecastWindowStart(now),
		FrequencyMHz:   float64(key.frequencyKHz) / 1000,
		key:            key,
		delayKey:       delayKey,
	}
	f.inflight[key] = struct{}{}
	delete(f.delays, delayKey)
	f.statsQueued.Add(1)
	f.queue <- job
	return VOACAPCachedForecast{}, false
}

// CheckForecastWindow returns the cached rolling forecast horizon for req, or
// queues/delays fallback work and returns false on a cache miss.
func (f *VOACAPClosedFallback) CheckForecastWindow(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecastWindow, bool) {
	if f == nil || !f.fallback.Enabled {
		return VOACAPCachedForecastWindow{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	prepared, ok := f.prepareRequest(req)
	if !ok {
		f.statsInvalidRequest.Add(1)
		return VOACAPCachedForecastWindow{}, false
	}
	ssn, ok := f.ssnProvider.CurrentSSN(now)
	if !ok {
		f.statsSSNUnavailable.Add(1)
		return VOACAPCachedForecastWindow{}, false
	}
	key := f.cacheKey(prepared, ssn, now)
	delayKey := f.delayKey(prepared)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneLocked(now)
	if entry, ok := f.cache[key]; ok {
		if f.cacheExpired(entry, now) {
			delete(f.cache, key)
		} else {
			records := forecastRecordsForWindow(entry.forecast, now, f.fallback.ForecastHours)
			if len(records) == 0 {
				f.statsNoCurrentHour.Add(1)
				delete(f.cache, key)
			} else {
				f.statsCacheHit.Add(1)
				return f.cachedForecastWindowFromCache(key, entry, records, prepared, now), true
			}
		}
	}
	if _, ok := f.inflight[key]; ok {
		f.statsInflight.Add(1)
		return VOACAPCachedForecastWindow{}, false
	}
	if !f.delayElapsedLocked(delayKey, now) {
		f.statsDelayWait.Add(1)
		return VOACAPCachedForecastWindow{}, false
	}
	if !f.running.Load() {
		f.statsNotRunning.Add(1)
		return VOACAPCachedForecastWindow{}, false
	}
	if len(f.queue) >= cap(f.queue) {
		f.statsQueueFull.Add(1)
		return VOACAPCachedForecastWindow{}, false
	}
	job := VOACAPClosedJob{
		Request:        prepared,
		SSN:            ssn,
		WindowStartUTC: forecastWindowStart(now),
		FrequencyMHz:   float64(key.frequencyKHz) / 1000,
		key:            key,
		delayKey:       delayKey,
	}
	f.inflight[key] = struct{}{}
	delete(f.delays, delayKey)
	f.statsQueued.Add(1)
	f.queue <- job
	return VOACAPCachedForecastWindow{}, false
}

func (f *VOACAPClosedFallback) CheckCachedForecast(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecast, bool) {
	if f == nil || !f.fallback.Enabled {
		return VOACAPCachedForecast{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	prepared, ok := f.prepareRequest(req)
	if !ok {
		return VOACAPCachedForecast{}, false
	}
	ssn, ok := f.ssnProvider.CurrentSSN(now)
	if !ok {
		return VOACAPCachedForecast{}, false
	}
	key := f.cacheKey(prepared, ssn, now)

	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.cache[key]
	if !ok || f.cacheExpired(entry, now) {
		return VOACAPCachedForecast{}, false
	}
	record, ok := forecastRecordForHour(entry.forecast, now, f.fallback.ForecastHours)
	if !ok {
		return VOACAPCachedForecast{}, false
	}
	return f.cachedForecastFromCache(key, entry, record, prepared, now), true
}

func (f *VOACAPClosedFallback) prepareRequest(req VOACAPClosedRequest) (VOACAPClosedRequest, bool) {
	if req.UserCell == InvalidCell || req.DXCell == InvalidCell {
		return VOACAPClosedRequest{}, false
	}
	req.Band = normalizeBand(req.Band)
	if req.Band == "" {
		return VOACAPClosedRequest{}, false
	}
	if _, ok := centerFrequencyKHzForBand(req.Band, f.fallback.CenterFrequenciesMHz); !ok {
		return VOACAPClosedRequest{}, false
	}
	if _, _, ok := GridCenterLatLon(req.UserGrid); !ok {
		return VOACAPClosedRequest{}, false
	}
	if _, _, ok := GridCenterLatLon(req.DXGrid); !ok {
		return VOACAPClosedRequest{}, false
	}
	return req, true
}

func (f *VOACAPClosedFallback) forecastClosedForMode(forecast VOACAPCachedForecast, mode string) bool {
	return ClosedForDB(forecast.EffectiveDB(), mode, f.cfg)
}

func (f *VOACAPClosedFallback) cacheKey(req VOACAPClosedRequest, ssn int, now time.Time) voacapCacheKey {
	frequencyKHz, _ := centerFrequencyKHzForBand(req.Band, f.fallback.CenterFrequenciesMHz)
	window := forecastWindowStart(now)
	return voacapCacheKey{
		userCell:     req.UserCell,
		dxCell:       req.DXCell,
		band:         req.Band,
		frequencyKHz: frequencyKHz,
		year:         window.Year(),
		month:        int(window.Month()),
		ssn:          ssn,
		direction:    voacapDirectionBidirectional,
	}
}

func (f *VOACAPClosedFallback) delayKey(req VOACAPClosedRequest) voacapDelayKey {
	frequencyKHz, _ := centerFrequencyKHzForBand(req.Band, f.fallback.CenterFrequenciesMHz)
	return voacapDelayKey{
		userCell:     req.UserCell,
		dxCell:       req.DXCell,
		band:         req.Band,
		frequencyKHz: frequencyKHz,
		direction:    voacapDirectionBidirectional,
	}
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
	entry.lastSeen = now
	f.delays[key] = entry
	return !now.Before(entry.firstSeen.Add(delay))
}

func (f *VOACAPClosedFallback) worker(ctx context.Context) {
	defer f.wg.Done()
	defer f.running.Store(false)
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-f.queue:
			f.runJob(ctx, job)
		}
	}
}

func (f *VOACAPClosedFallback) runJob(ctx context.Context, job VOACAPClosedJob) {
	forecast, err := f.forecaster.ForecastClosed(ctx, job)
	now := time.Now().UTC()
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.inflight, job.key)
	if err != nil {
		if ctx.Err() == nil {
			if f.logger != nil {
				f.logger.Printf("VOACAP closed fallback failed user_cell=%d dx_cell=%d band=%s ssn=%d: %v", job.Request.UserCell, job.Request.DXCell, job.Request.Band, job.SSN, err)
			}
			f.statsRunFailure.Add(1)
		}
		f.addDelayLocked(job.delayKey, voacapDelayEntry{firstSeen: now, lastSeen: now})
		return
	}
	f.statsRunSuccess.Add(1)
	f.addCacheLocked(job.key, voacapCacheEntry{forecast: forecast, storedAt: now})
}

func (f *VOACAPClosedFallback) cachedForecastFromCache(key voacapCacheKey, entry voacapCacheEntry, record VOACAPHourlyForecast, req VOACAPClosedRequest, now time.Time) VOACAPCachedForecast {
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
		SSN:                   key.ssn,
		AgeSec:                ageSec,
	}
}

func (f *VOACAPClosedFallback) cachedForecastWindowFromCache(key voacapCacheKey, entry voacapCacheEntry, records []VOACAPHourlyForecast, req VOACAPClosedRequest, now time.Time) VOACAPCachedForecastWindow {
	out := VOACAPCachedForecastWindow{
		WindowStartUTC: entry.forecast.WindowStartUTC.UTC(),
		Records:        make([]VOACAPCachedForecast, 0, len(records)),
	}
	for _, record := range records {
		out.Records = append(out.Records, f.cachedForecastFromCache(key, entry, record, req, now))
	}
	return out
}

func (f *VOACAPClosedFallback) closedResultFromForecast(forecast VOACAPCachedForecast) Result {
	return VOACAPClosedResult(f.cfg, forecast)
}

// VOACAPClosedResult maps a cached VOACAP forecast to the closed fallback result.
func VOACAPClosedResult(cfg Config, forecast VOACAPCachedForecast) Result {
	return Result{
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
	base.VOACAPFT8SNRDB = forecast.Record.FT8SNRDB
	base.VOACAPSSN = forecast.SSN
	base.VOACAPAgeSec = forecast.AgeSec
	base.VOACAPHourUTC = forecast.Record.HourUTC
	base.VOACAPFrequencyMHz = forecast.Record.FrequencyMHz
	return base
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
	for key, entry := range f.cache {
		if f.cacheExpired(entry, now) {
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
	return now.Sub(entry.storedAt) > time.Duration(f.fallback.CacheTTLSeconds)*time.Second
}

func (f *VOACAPClosedFallback) addCacheLocked(key voacapCacheKey, entry voacapCacheEntry) {
	if len(f.cache) >= f.fallback.MaxCacheEntries {
		var oldestKey voacapCacheKey
		var oldest time.Time
		first := true
		for candidate, existing := range f.cache {
			if first || existing.storedAt.Before(oldest) {
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
		var oldestKey voacapDelayKey
		var oldest time.Time
		first := true
		for candidate, existing := range f.delays {
			if first || existing.lastSeen.Before(oldest) {
				oldestKey = candidate
				oldest = existing.lastSeen
				first = false
			}
		}
		delete(f.delays, oldestKey)
	}
	f.delays[key] = entry
}

type VOACAPRunnerClosedForecaster struct {
	cfg    VOACAPFallbackConfig
	runner voacap.Runner
}

func NewVOACAPRunnerClosedForecaster(cfg VOACAPFallbackConfig) VOACAPRunnerClosedForecaster {
	return VOACAPRunnerClosedForecaster{
		cfg:    cfg,
		runner: voacap.NewRunner(cfg.VOACAPHome),
	}
}

func (f VOACAPRunnerClosedForecaster) ForecastClosed(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
	receiveForecast, receiveResult, err := f.runDirectionalForecast(ctx, job, voacapDeckReceive)
	if err != nil {
		return VOACAPClosedForecast{}, err
	}
	transmitForecast, transmitResult, err := f.runDirectionalForecast(ctx, job, voacapDeckTransmit)
	if err != nil {
		return VOACAPClosedForecast{}, err
	}
	forecast, err := combineDirectionalForecasts(receiveForecast, transmitForecast, job.Request.Band)
	if err != nil {
		return VOACAPClosedForecast{}, err
	}
	forecast.WindowStartUTC = job.WindowStartUTC.UTC()
	forecast.OutputPath = receiveResult.OutputPath + ";" + transmitResult.OutputPath
	forecast.Elapsed = receiveResult.Elapsed + transmitResult.Elapsed
	return forecast, nil
}

func (f VOACAPRunnerClosedForecaster) runDirectionalForecast(ctx context.Context, job VOACAPClosedJob, direction string) (VOACAPClosedForecast, voacap.RunResult, error) {
	deck, err := f.buildDeck(job, direction)
	if err != nil {
		return VOACAPClosedForecast{}, voacap.RunResult{}, err
	}
	outputName := fmt.Sprintf("%s_%s_%d_%d_%s_%d_%s.out",
		f.cfg.OutputNamePrefix,
		job.WindowStartUTC.Format("010215"),
		job.Request.UserCell,
		job.Request.DXCell,
		job.Request.Band,
		job.SSN,
		direction)
	result, err := f.runner.Run(ctx, voacap.RunRequest{
		Deck:       deck,
		OutputName: outputName,
		Timeout:    time.Duration(f.cfg.VOACAPTimeoutSeconds) * time.Second,
	})
	if err != nil {
		return VOACAPClosedForecast{}, result, err
	}
	records, err := voacap.ParseMethod30Predictions(result.Output)
	if err != nil {
		return VOACAPClosedForecast{}, result, err
	}
	forecast, err := closedForecastFromRecords(records, job.Request.Band)
	if err != nil {
		return VOACAPClosedForecast{}, result, err
	}
	return forecast, result, nil
}

func (f VOACAPRunnerClosedForecaster) buildDeck(job VOACAPClosedJob, direction string) ([]byte, error) {
	userLat, userLon, ok := GridCenterLatLon(job.Request.UserGrid)
	if !ok {
		return nil, fmt.Errorf("invalid user grid %q", job.Request.UserGrid)
	}
	dxLat, dxLon, ok := GridCenterLatLon(job.Request.DXGrid)
	if !ok {
		return nil, fmt.Errorf("invalid DX grid %q", job.Request.DXGrid)
	}
	transmit := voacap.DeckEndpoint{
		Label:     "TRANSMITTER",
		Latitude:  userLat,
		Longitude: userLon,
	}
	receive := voacap.DeckEndpoint{
		Label:     "RECEIVER",
		Latitude:  dxLat,
		Longitude: dxLon,
	}
	switch direction {
	case voacapDeckReceive:
		transmit.Latitude = dxLat
		transmit.Longitude = dxLon
		receive.Latitude = userLat
		receive.Longitude = userLon
	case voacapDeckTransmit:
	default:
		return nil, fmt.Errorf("unknown VOACAP deck direction %q", direction)
	}
	return voacap.BuildPathDeck(voacap.PathDeckRequest{
		Comment:              fmt.Sprintf("GoCluster VOACAP closed fallback %s %d", job.Request.Band, job.SSN),
		Transmit:             transmit,
		Receive:              receive,
		SSN:                  job.SSN,
		Now:                  job.WindowStartUTC,
		ForecastHours:        f.cfg.ForecastHours,
		StartVOACAPHour:      voacap.HourForUTC(job.WindowStartUTC),
		CenterFrequenciesMHz: f.cfg.CenterFrequenciesMHz,
	})
}

func combineDirectionalForecasts(receiveForecast, transmitForecast VOACAPClosedForecast, band string) (VOACAPClosedForecast, error) {
	transmitByHour := make(map[int]VOACAPHourlyForecast, len(transmitForecast.Records))
	for _, record := range transmitForecast.Records {
		transmitByHour[record.HourUTC] = record
	}
	combined := VOACAPClosedForecast{
		Records: make([]VOACAPHourlyForecast, 0, len(receiveForecast.Records)),
	}
	for _, receive := range receiveForecast.Records {
		transmit, ok := transmitByHour[receive.HourUTC]
		if !ok {
			continue
		}
		combined.Records = append(combined.Records, VOACAPHourlyForecast{
			FT8SNRDB:              receive.FT8SNRDB,
			VOACAPSNRDBHz:         receive.VOACAPSNRDBHz,
			HourUTC:               receive.HourUTC,
			FrequencyMHz:          receive.FrequencyMHz,
			ReceiveFT8SNRDB:       receive.FT8SNRDB,
			TransmitFT8SNRDB:      transmit.FT8SNRDB,
			ReceiveVOACAPSNRDBHz:  receive.VOACAPSNRDBHz,
			TransmitVOACAPSNRDBHz: transmit.VOACAPSNRDBHz,
			HasDirectionalSNR:     true,
		})
	}
	if len(combined.Records) == 0 {
		return VOACAPClosedForecast{}, fmt.Errorf("VOACAP output has no common bidirectional prediction records for band %s", normalizeBand(band))
	}
	return combined, nil
}

func closedForecastFromRecords(records []voacap.PredictionRecord, band string) (VOACAPClosedForecast, error) {
	band = normalizeBand(band)
	byHour := make(map[int]VOACAPHourlyForecast)
	hourOrder := make([]int, 0)
	for _, record := range records {
		if bandForMHz(record.FrequencyMHz) != band {
			continue
		}
		hourly := VOACAPHourlyForecast{
			FT8SNRDB:      record.FT8SNRDB,
			VOACAPSNRDBHz: record.VOACAPSNRDBHz,
			HourUTC:       record.HourUTC,
			FrequencyMHz:  record.FrequencyMHz,
		}
		existing, ok := byHour[record.HourUTC]
		if !ok {
			hourOrder = append(hourOrder, record.HourUTC)
			byHour[record.HourUTC] = hourly
			continue
		}
		if record.FT8SNRDB > existing.FT8SNRDB {
			byHour[record.HourUTC] = hourly
		}
	}
	if len(hourOrder) == 0 {
		return VOACAPClosedForecast{}, fmt.Errorf("VOACAP output has no prediction records for band %s", band)
	}
	forecast := VOACAPClosedForecast{
		Records: make([]VOACAPHourlyForecast, 0, len(hourOrder)),
	}
	for _, hour := range hourOrder {
		forecast.Records = append(forecast.Records, byHour[hour])
	}
	return forecast, nil
}

func forecastRecordForHour(forecast VOACAPClosedForecast, now time.Time, horizonHours int) (VOACAPHourlyForecast, bool) {
	now = now.UTC()
	if !forecast.WindowStartUTC.IsZero() && horizonHours > 0 {
		start := forecast.WindowStartUTC.UTC()
		end := start.Add(time.Duration(horizonHours) * time.Hour)
		if now.Before(start) || !now.Before(end) {
			return VOACAPHourlyForecast{}, false
		}
	}
	hour := now.Hour()
	for _, record := range forecast.Records {
		if record.HourUTC == hour {
			return record, true
		}
	}
	return VOACAPHourlyForecast{}, false
}

func forecastRecordsForWindow(forecast VOACAPClosedForecast, now time.Time, horizonHours int) []VOACAPHourlyForecast {
	now = now.UTC()
	start := now.Truncate(time.Hour)
	end := start.Add(time.Duration(horizonHours) * time.Hour)
	if !forecast.WindowStartUTC.IsZero() && horizonHours > 0 {
		windowStart := forecast.WindowStartUTC.UTC()
		windowEnd := windowStart.Add(time.Duration(horizonHours) * time.Hour)
		if now.Before(windowStart) || !now.Before(windowEnd) {
			return nil
		}
		end = windowEnd
	}
	if horizonHours <= 0 {
		end = start.Add(time.Hour)
	}
	byHour := make(map[int]VOACAPHourlyForecast, len(forecast.Records))
	for _, record := range forecast.Records {
		byHour[record.HourUTC] = record
	}
	out := make([]VOACAPHourlyForecast, 0, len(forecast.Records))
	for at := start; at.Before(end); at = at.Add(time.Hour) {
		if record, ok := byHour[at.Hour()]; ok {
			out = append(out, record)
		}
	}
	return out
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
