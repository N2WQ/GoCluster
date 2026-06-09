// File role: Owns the optional nonblocking VOACAP closed-band fallback.
// Crawler notes: Start here for SSN-gated fallback delay/cache behavior,
// mode-owned closed-threshold evaluation, VOACAP deck launch, and closed glyph
// result construction.
// Related docs: pathreliability/README.md,
// data/config/PATH_PREDICTIONS.md, docs/decisions/ADR-0161-voacap-closed-only-path-fallback.md,
// docs/decisions/ADR-0162-voacap-hourly-forecast-window-cache.md.
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
// VOACAP fallback. Implementations return a result only when a closed verdict
// is already cached; lookup may enqueue background work but must not run VOACAP
// in the telnet display/filter path.
type ClosedFallback interface {
	CheckClosed(req VOACAPClosedRequest, now time.Time) (Result, bool)
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
	UserCell CellID
	DXCell   CellID
	UserGrid string
	DXGrid   string
	Band     string
	Mode     string
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
	FT8SNRDB      int
	VOACAPSNRDBHz int
	HourUTC       int
	FrequencyMHz  float64
}

type VOACAPClosedFallbackSnapshot struct {
	CacheEntries    int
	DelayEntries    int
	InflightEntries int
	QueueDepth      int
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

const voacapDirectionUserToDX = "user_to_dx"

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

func (f *VOACAPClosedFallback) CheckClosed(req VOACAPClosedRequest, now time.Time) (Result, bool) {
	if f == nil || !f.fallback.Enabled {
		return Result{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	prepared, ok := f.prepareRequest(req)
	if !ok {
		return Result{}, false
	}
	ssn, ok := f.ssnProvider.CurrentSSN(now)
	if !ok {
		return Result{}, false
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
				delete(f.cache, key)
			} else if f.forecastRecordClosedForMode(record, prepared.Mode) {
				return f.resultFromCache(key, entry, record, now), true
			} else {
				return Result{}, false
			}
		}
	}
	if _, ok := f.inflight[key]; ok {
		return Result{}, false
	}
	if !f.delayElapsedLocked(delayKey, now) {
		return Result{}, false
	}
	if !f.running.Load() || len(f.queue) >= cap(f.queue) {
		return Result{}, false
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
	f.queue <- job
	return Result{}, false
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

func (f *VOACAPClosedFallback) closedThresholdForMode(mode string) float64 {
	if f == nil {
		return 0
	}
	return thresholdsForModeDB(mode, f.cfg).Closed
}

func (f *VOACAPClosedFallback) forecastRecordClosedForMode(record VOACAPHourlyForecast, mode string) bool {
	return float64(record.FT8SNRDB) <= f.closedThresholdForMode(mode)
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
		direction:    voacapDirectionUserToDX,
	}
}

func (f *VOACAPClosedFallback) delayKey(req VOACAPClosedRequest) voacapDelayKey {
	frequencyKHz, _ := centerFrequencyKHzForBand(req.Band, f.fallback.CenterFrequenciesMHz)
	return voacapDelayKey{
		userCell:     req.UserCell,
		dxCell:       req.DXCell,
		band:         req.Band,
		frequencyKHz: frequencyKHz,
		direction:    voacapDirectionUserToDX,
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
		if ctx.Err() == nil && f.logger != nil {
			f.logger.Printf("VOACAP closed fallback failed user_cell=%d dx_cell=%d band=%s ssn=%d: %v", job.Request.UserCell, job.Request.DXCell, job.Request.Band, job.SSN, err)
		}
		f.addDelayLocked(job.delayKey, voacapDelayEntry{firstSeen: now, lastSeen: now})
		return
	}
	f.addCacheLocked(job.key, voacapCacheEntry{forecast: forecast, storedAt: now})
}

func (f *VOACAPClosedFallback) resultFromCache(key voacapCacheKey, entry voacapCacheEntry, record VOACAPHourlyForecast, now time.Time) Result {
	ageSec := int64(0)
	if now.After(entry.storedAt) {
		ageSec = int64(now.Sub(entry.storedAt).Seconds())
	}
	return Result{
		Glyph:              f.cfg.GlyphSymbols.Closed,
		Class:              classUnlikely,
		P50DB:              float64(record.FT8SNRDB),
		HasP50:             true,
		P50Glyph:           f.cfg.GlyphSymbols.Closed,
		AgeSec:             ageSec,
		Source:             SourceVOACAPClosed,
		VOACAPFT8SNRDB:     record.FT8SNRDB,
		VOACAPSSN:          key.ssn,
		VOACAPAgeSec:       ageSec,
		VOACAPHourUTC:      record.HourUTC,
		VOACAPFrequencyMHz: record.FrequencyMHz,
	}
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
	deck, err := f.buildDeck(job)
	if err != nil {
		return VOACAPClosedForecast{}, err
	}
	outputName := fmt.Sprintf("%s_%s_%d_%d_%s_%d.out",
		f.cfg.OutputNamePrefix,
		job.WindowStartUTC.Format("010215"),
		job.Request.UserCell,
		job.Request.DXCell,
		job.Request.Band,
		job.SSN)
	result, err := f.runner.Run(ctx, voacap.RunRequest{
		Deck:       deck,
		OutputName: outputName,
		Timeout:    time.Duration(f.cfg.VOACAPTimeoutSeconds) * time.Second,
	})
	if err != nil {
		return VOACAPClosedForecast{}, err
	}
	records, err := voacap.ParseMethod30Predictions(result.Output)
	if err != nil {
		return VOACAPClosedForecast{}, err
	}
	forecast, err := closedForecastFromRecords(records, job.Request.Band)
	if err != nil {
		return VOACAPClosedForecast{}, err
	}
	forecast.WindowStartUTC = job.WindowStartUTC.UTC()
	forecast.OutputPath = result.OutputPath
	forecast.Elapsed = result.Elapsed
	return forecast, nil
}

func (f VOACAPRunnerClosedForecaster) buildDeck(job VOACAPClosedJob) ([]byte, error) {
	userLat, userLon, ok := GridCenterLatLon(job.Request.UserGrid)
	if !ok {
		return nil, fmt.Errorf("invalid user grid %q", job.Request.UserGrid)
	}
	dxLat, dxLon, ok := GridCenterLatLon(job.Request.DXGrid)
	if !ok {
		return nil, fmt.Errorf("invalid DX grid %q", job.Request.DXGrid)
	}
	return voacap.BuildPathDeck(voacap.PathDeckRequest{
		Comment: fmt.Sprintf("GoCluster VOACAP closed fallback %s %d", job.Request.Band, job.SSN),
		Transmit: voacap.DeckEndpoint{
			Label:     "TRANSMITTER",
			Latitude:  userLat,
			Longitude: userLon,
		},
		Receive: voacap.DeckEndpoint{
			Label:     "RECEIVER",
			Latitude:  dxLat,
			Longitude: dxLon,
		},
		SSN:                  job.SSN,
		Now:                  job.WindowStartUTC,
		ForecastHours:        f.cfg.ForecastHours,
		CenterFrequenciesMHz: f.cfg.CenterFrequenciesMHz,
	})
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
