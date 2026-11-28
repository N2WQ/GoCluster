// File role: Owns the pure-Go internal/propprior sparse-path HF fallback engine
// and strictly bounded fixed-capacity reciprocal prediction cache.
// Crawler notes: Start here for zero-allocation reciprocal cache lookups,
// SpaceWeatherStore generation invalidation, SET NOISE receive-leg alignment,
// and instant 24-hour SHOW PROP multi-band outlooks.
package pathreliability

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/internal/propprior"
)

const (
	propPriorBandCount   = 9
	propPriorCacheShards = 64
	propPriorCacheWays   = 4
)

// PropPriorFallbackSnapshot captures bounded runtime telemetry for the Overview
// panel and operator diagnostics.
type PropPriorFallbackSnapshot struct {
	CacheEntries    int
	CacheCapacity   int
	CacheHits       uint64
	CacheMisses     uint64
	CacheEvictions  uint64
	InvalidRequests uint64
	NonFiniteErrors uint64
	SpaceWeatherGen uint64
	SpaceWeather    propprior.SpaceWeatherSnapshot
}

type propPriorCacheKey struct {
	loCell     CellID
	hiCell     CellID
	bandIdx    uint8
	_          [3]byte
	timeBucket int64
}

type propPriorCacheSlot struct {
	key         propPriorCacheKey
	swGen       uint64
	lastTouch   uint64
	snrTotal    float32
	sigmaTotal  float32
	lpEvaluated bool
	valid       bool
}

type propPriorCacheShard struct {
	mu      sync.Mutex
	touch   uint64
	entries int
	slots   []propPriorCacheSlot
}

type propPriorReciprocalCache struct {
	capacity     int
	setsPerShard uint64
	setMask      uint64
	shards       [propPriorCacheShards]propPriorCacheShard
	totalEntries atomic.Int64
	evictions    atomic.Uint64
}

func newPropPriorReciprocalCache(capacity int) *propPriorReciprocalCache {
	setsTotal := capacity / propPriorCacheWays
	setsPerShard := setsTotal / propPriorCacheShards
	if setsPerShard < 1 {
		setsPerShard = 1
	}
	c := &propPriorReciprocalCache{
		capacity:     setsPerShard * propPriorCacheShards * propPriorCacheWays,
		setsPerShard: uint64(setsPerShard),
		setMask:      uint64(setsPerShard - 1),
	}
	slotsPerShard := setsPerShard * propPriorCacheWays
	// Allocate one contiguous value-only backing slab (zero pointers -> zero GC scan).
	backing := make([]propPriorCacheSlot, c.capacity)
	for i := range propPriorCacheShards {
		start := i * slotsPerShard
		c.shards[i].slots = backing[start : start+slotsPerShard]
	}
	return c
}

func hashPropPriorCacheKey(k propPriorCacheKey) uint64 {
	h := (uint64(k.loCell) << 16) | uint64(k.hiCell)
	h ^= uint64(k.bandIdx) << 33
	h ^= uint64(k.timeBucket) * 0x9e3779b97f4a7c15
	// SplitMix64 finalizer for uniform shard and set distribution.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

func (c *propPriorReciprocalCache) lookup(key propPriorCacheKey, swGen uint64) (snrTotal, sigmaTotal float64, ok bool) {
	h := hashPropPriorCacheKey(key)
	shard := &c.shards[h&(propPriorCacheShards-1)]
	base := int(((h >> 6) & c.setMask) * propPriorCacheWays)

	shard.mu.Lock()
	set := shard.slots[base : base+propPriorCacheWays]
	for i := range set {
		slot := &set[i]
		if slot.valid && slot.key == key && slot.swGen == swGen {
			shard.touch++
			slot.lastTouch = shard.touch
			snr := float64(slot.snrTotal)
			sig := float64(slot.sigmaTotal)
			shard.mu.Unlock()
			return snr, sig, true
		}
	}
	shard.mu.Unlock()
	return 0, 0, false
}

// lookupReadOnly probes the cache without mutating touch metadata or counters.
func (c *propPriorReciprocalCache) lookupReadOnly(key propPriorCacheKey, swGen uint64) (snrTotal, sigmaTotal float64, ok bool) {
	h := hashPropPriorCacheKey(key)
	shard := &c.shards[h&(propPriorCacheShards-1)]
	base := int(((h >> 6) & c.setMask) * propPriorCacheWays)

	shard.mu.Lock()
	set := shard.slots[base : base+propPriorCacheWays]
	for i := range set {
		slot := &set[i]
		if slot.valid && slot.key == key && slot.swGen == swGen {
			snr := float64(slot.snrTotal)
			sig := float64(slot.sigmaTotal)
			shard.mu.Unlock()
			return snr, sig, true
		}
	}
	shard.mu.Unlock()
	return 0, 0, false
}

func (c *propPriorReciprocalCache) store(key propPriorCacheKey, swGen uint64, snrTotal, sigmaTotal float64, lpEvaluated bool) {
	h := hashPropPriorCacheKey(key)
	shard := &c.shards[h&(propPriorCacheShards-1)]
	base := int(((h >> 6) & c.setMask) * propPriorCacheWays)

	shard.mu.Lock()
	set := shard.slots[base : base+propPriorCacheWays]
	shard.touch++
	touch := shard.touch

	emptyIdx := -1
	staleIdx := -1
	lruIdx := 0
	lruTouch := set[0].lastTouch

	for i := range set {
		slot := &set[i]
		if slot.valid && slot.key == key {
			slot.swGen = swGen
			slot.lastTouch = touch
			slot.snrTotal = float32(snrTotal)
			slot.sigmaTotal = float32(sigmaTotal)
			slot.lpEvaluated = lpEvaluated
			shard.mu.Unlock()
			return
		}
		if !slot.valid {
			if emptyIdx < 0 {
				emptyIdx = i
			}
			continue
		}
		if staleIdx < 0 && (slot.swGen != swGen || slot.key.timeBucket != key.timeBucket) {
			staleIdx = i
		}
		if slot.lastTouch < lruTouch {
			lruTouch = slot.lastTouch
			lruIdx = i
		}
	}

	var victim int
	var evicted bool
	switch {
	case emptyIdx >= 0:
		victim = emptyIdx
		shard.entries++
		c.totalEntries.Add(1)
	case staleIdx >= 0:
		victim = staleIdx
		evicted = true
	default:
		victim = lruIdx
		evicted = true
	}

	set[victim] = propPriorCacheSlot{
		key:         key,
		swGen:       swGen,
		lastTouch:   touch,
		snrTotal:    float32(snrTotal),
		sigmaTotal:  float32(sigmaTotal),
		lpEvaluated: lpEvaluated,
		valid:       true,
	}
	shard.mu.Unlock()
	if evicted {
		c.evictions.Add(1)
	}
}

// PropPriorFallback implements GoCluster's sparse-path HF fallback and SHOW PROP
// outlook interfaces using the embedded pure-Go internal/propprior MLP backed by
// live SpaceWeatherStore snapshots and a fixed-capacity reciprocal cache.
type PropPriorFallback struct {
	cfg      Config
	fallback PropPriorFallbackConfig
	model    *propprior.Model
	weather  *propprior.SpaceWeatherStore
	cache    *propPriorReciprocalCache

	bandEnabled [propPriorBandCount]bool
	bandFreqMHz [propPriorBandCount]float64
	bucketSec   int64

	// Reset-on-snapshot propagation log counters (ClosedFallbackStatsProvider).
	statsInvalidRequest atomic.Int64
	statsCacheHit       atomic.Int64
	statsRunSuccess     atomic.Int64
	statsRunFailure     atomic.Int64
	statsInvalidReasons [VOACAPInvalidRequestReasonCount]atomic.Int64

	// Cumulative runtime counters (Overview / Snapshot).
	totalHits       atomic.Uint64
	totalMisses     atomic.Uint64
	totalInvalid    atomic.Uint64
	totalNonFinite  atomic.Uint64
}

// NewPropPriorFallback builds a pure-Go PropPriorFallback engine. Callers pass
// a preloaded *propprior.Model (typically propprior.LoadEmbedded()) and a shared
// *propprior.SpaceWeatherStore (if nil, a fresh quiet-defaults store is created).
func NewPropPriorFallback(cfg Config, model *propprior.Model, weather *propprior.SpaceWeatherStore) (*PropPriorFallback, error) {
	if model == nil {
		return nil, errors.New("prop_prior_fallback: model must not be nil")
	}
	if err := cfg.finalize(); err != nil {
		return nil, err
	}
	fbCfg := DefaultPropPriorFallbackConfig()
	if cfg.PropPriorFallback != nil {
		fbCfg = *cfg.PropPriorFallback
	}
	if err := fbCfg.finalize(); err != nil {
		return nil, err
	}
	if weather == nil {
		weather = propprior.NewSpaceWeatherStore()
	}

	f := &PropPriorFallback{
		cfg:       cfg,
		fallback:  fbCfg,
		model:     model,
		weather:   weather,
		cache:     newPropPriorReciprocalCache(fbCfg.CacheCapacity),
		bucketSec: int64(fbCfg.CacheBucketSeconds),
	}
	for _, freq := range fbCfg.CenterFrequenciesMHz {
		idx, ok := propPriorBandIndex(bandForMHz(freq))
		if !ok {
			return nil, fmt.Errorf("prop_prior_fallback: unsupported center frequency %.3f MHz", freq)
		}
		f.bandEnabled[idx] = true
		f.bandFreqMHz[idx] = freq
	}
	return f, nil
}

// CheckForecast satisfies ClosedFallback.
func (f *PropPriorFallback) CheckForecast(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecast, bool) {
	check := f.CheckForecastDetailed(req, now)
	if check.Status != VOACAPForecastCheckReady {
		return VOACAPCachedForecast{}, false
	}
	return check.Forecast, true
}

// CheckForecastDetailed satisfies DetailedForecastProvider with zero steady-state
// allocations on both cache hits and cache misses.
func (f *PropPriorFallback) CheckForecastDetailed(req VOACAPClosedRequest, now time.Time) VOACAPForecastCheck {
	if f == nil || f.model == nil {
		return VOACAPForecastCheck{Status: VOACAPForecastCheckDisabled}
	}
	bandIdx, freqMHz, txLat, txLon, rxLat, rxLon, reason := f.validateRequest(req)
	if reason != VOACAPInvalidRequestReasonNone {
		f.recordInvalid(reason)
		return VOACAPForecastCheck{
			Status:        VOACAPForecastCheckInvalidRequest,
			InvalidReason: reason,
		}
	}

	utc := now.UTC()
	if utc.IsZero() {
		utc = time.Now().UTC()
	}
	key := makePropPriorCacheKey(req.UserCell, req.DXCell, bandIdx, utc, f.bucketSec)
	gen := f.weather.Generation()
	if snr, sigma, hit := f.cache.lookup(key, gen); hit {
		f.statsCacheHit.Add(1)
		f.totalHits.Add(1)
		return VOACAPForecastCheck{
			Forecast: f.buildForecast(snr, sigma, freqMHz, req.ReceiveNoisePenaltyDB, utc),
			Status:   VOACAPForecastCheckReady,
		}
	}

	sw := f.weather.Evaluate(utc)
	pred, err := f.model.Predict(propprior.PathRequest{
		TXLat:                txLat,
		TXLon:                txLon,
		RXLat:                rxLat,
		RXLon:                rxLon,
		FreqMHz:              freqMHz,
		Time:                 utc,
		SpaceWeatherFeatures: sw.Features,
	})
	if err != nil {
		f.statsRunFailure.Add(1)
		f.totalNonFinite.Add(1)
		return VOACAPForecastCheck{
			Status:    VOACAPForecastCheckUnavailable,
			CacheMiss: true,
		}
	}
	f.cache.store(key, sw.Generation, pred.SnrTotal, pred.SigmaTotal, pred.LPEvaluated)
	f.statsRunSuccess.Add(1)
	f.totalMisses.Add(1)
	return VOACAPForecastCheck{
		Forecast:  f.buildForecast(pred.SnrTotal, pred.SigmaTotal, freqMHz, req.ReceiveNoisePenaltyDB, utc),
		Status:    VOACAPForecastCheckReady,
		CacheMiss: true,
	}
}

// CheckCachedForecast satisfies CachedForecastProvider as a strictly
// observation-only cache probe: it never evaluates the model, inserts into the
// cache, or mutates fallback stage counters.
func (f *PropPriorFallback) CheckCachedForecast(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecast, bool) {
	if f == nil || f.model == nil {
		return VOACAPCachedForecast{}, false
	}
	bandIdx, freqMHz, _, _, _, _, reason := f.validateRequest(req)
	if reason != VOACAPInvalidRequestReasonNone {
		return VOACAPCachedForecast{}, false
	}
	utc := now.UTC()
	if utc.IsZero() {
		utc = time.Now().UTC()
	}
	key := makePropPriorCacheKey(req.UserCell, req.DXCell, bandIdx, utc, f.bucketSec)
	gen := f.weather.Generation()
	snr, sigma, hit := f.cache.lookupReadOnly(key, gen)
	if !hit {
		return VOACAPCachedForecast{}, false
	}
	return f.buildForecast(snr, sigma, freqMHz, req.ReceiveNoisePenaltyDB, utc), true
}

// CheckForecastWindow satisfies VOACAPForecastWindowProvider for SHOW PROP,
// immediately evaluating the configured rolling hourly horizon in pure Go.
func (f *PropPriorFallback) CheckForecastWindow(req VOACAPClosedRequest, now time.Time) (VOACAPCachedForecastWindow, bool) {
	window, status := f.CheckForecastWindowWait(req, now, 0)
	return window, status == VOACAPForecastWindowReady
}

// CheckForecastWindowWait satisfies VOACAPForecastWindowWaitProvider. Because
// pure-Go propprior evaluates 24 hourly points in ~1 ms, no background worker or
// wait delay is needed.
func (f *PropPriorFallback) CheckForecastWindowWait(req VOACAPClosedRequest, now time.Time, _ time.Duration) (VOACAPCachedForecastWindow, VOACAPForecastWindowStatus) {
	if f == nil || f.model == nil {
		return VOACAPCachedForecastWindow{}, VOACAPForecastWindowUnavailable
	}
	bandIdx, freqMHz, txLat, txLon, rxLat, rxLon, reason := f.validateRequest(req)
	if reason != VOACAPInvalidRequestReasonNone {
		return VOACAPCachedForecastWindow{}, VOACAPForecastWindowUnavailable
	}
	utc := now.UTC()
	if utc.IsZero() {
		utc = time.Now().UTC()
	}
	windowStart := forecastWindowStart(utc)
	hours := f.fallback.ForecastHours
	if hours <= 0 || hours > 24 {
		hours = 24
	}
	// Evaluate current causal space-weather state once for the forecast outlook
	// (future hours project current solar/geomagnetic conditions across diurnal
	// geometry, matching how VOACAP holds current SSN across the 24h horizon).
	sw := f.weather.Evaluate(utc)
	records := make([]VOACAPCachedForecast, 0, hours)
	for h := range hours {
		at := windowStart.Add(time.Duration(h) * time.Hour)
		key := makePropPriorCacheKey(req.UserCell, req.DXCell, bandIdx, at, f.bucketSec)
		snr, sigma, hit := f.cache.lookup(key, sw.Generation)
		if !hit {
			pred, err := f.model.Predict(propprior.PathRequest{
				TXLat:                txLat,
				TXLon:                txLon,
				RXLat:                rxLat,
				RXLon:                rxLon,
				FreqMHz:              freqMHz,
				Time:                 at,
				SpaceWeatherFeatures: sw.Features,
			})
			if err != nil {
				f.totalNonFinite.Add(1)
				return VOACAPCachedForecastWindow{}, VOACAPForecastWindowFailed
			}
			f.cache.store(key, sw.Generation, pred.SnrTotal, pred.SigmaTotal, pred.LPEvaluated)
			snr = pred.SnrTotal
			sigma = pred.SigmaTotal
		}
		records = append(records, f.buildForecast(snr, sigma, freqMHz, req.ReceiveNoisePenaltyDB, at))
	}
	return VOACAPCachedForecastWindow{
		WindowStartUTC: windowStart,
		Records:        records,
	}, VOACAPForecastWindowReady
}

// StatsSnapshot satisfies ClosedFallbackStatsProvider (reset-on-snapshot).
func (f *PropPriorFallback) StatsSnapshot() VOACAPFallbackStats {
	if f == nil {
		return VOACAPFallbackStats{}
	}
	return VOACAPFallbackStats{
		InvalidRequest:          f.statsInvalidRequest.Swap(0),
		InvalidUnsupportedBand:  f.statsInvalidReasons[VOACAPInvalidUnsupportedBand].Swap(0),
		InvalidEmptyUnknownBand: f.statsInvalidReasons[VOACAPInvalidEmptyUnknownBand].Swap(0),
		InvalidUserGrid:         f.statsInvalidReasons[VOACAPInvalidUserGrid].Swap(0),
		InvalidDXGrid:           f.statsInvalidReasons[VOACAPInvalidDXGrid].Swap(0),
		InvalidUserCell:         f.statsInvalidReasons[VOACAPInvalidUserCell].Swap(0),
		InvalidDXCell:           f.statsInvalidReasons[VOACAPInvalidDXCell].Swap(0),
		CacheHit:                f.statsCacheHit.Swap(0),
		RunSuccess:              f.statsRunSuccess.Swap(0),
		RunFailure:              f.statsRunFailure.Swap(0),
	}
}

// Snapshot returns cumulative bounded telemetry for the Overview panel.
func (f *PropPriorFallback) Snapshot(now time.Time) PropPriorFallbackSnapshot {
	if f == nil {
		return PropPriorFallbackSnapshot{}
	}
	utc := now.UTC()
	if utc.IsZero() {
		utc = time.Now().UTC()
	}
	sw := f.weather.Evaluate(utc)
	return PropPriorFallbackSnapshot{
		CacheEntries:    int(f.cache.totalEntries.Load()),
		CacheCapacity:   f.cache.capacity,
		CacheHits:       f.totalHits.Load(),
		CacheMisses:     f.totalMisses.Load(),
		CacheEvictions:  f.cache.evictions.Load(),
		InvalidRequests: f.totalInvalid.Load(),
		NonFiniteErrors: f.totalNonFinite.Load(),
		SpaceWeatherGen: sw.Generation,
		SpaceWeather:    sw,
	}
}

func (f *PropPriorFallback) recordInvalid(reason VOACAPInvalidRequestReason) {
	f.statsInvalidRequest.Add(1)
	f.totalInvalid.Add(1)
	if reason > VOACAPInvalidRequestReasonNone && reason < VOACAPInvalidRequestReasonCount {
		f.statsInvalidReasons[reason].Add(1)
	}
}

func (f *PropPriorFallback) validateRequest(req VOACAPClosedRequest) (bandIdx uint8, freqMHz, txLat, txLon, rxLat, rxLon float64, reason VOACAPInvalidRequestReason) {
	if isTrimEmptyASCII(req.Band) {
		return 0, 0, 0, 0, 0, 0, VOACAPInvalidEmptyUnknownBand
	}
	idx, ok := propPriorBandIndex(req.Band)
	if !ok {
		if isKnownNonPropPriorBand(req.Band) {
			return 0, 0, 0, 0, 0, 0, VOACAPInvalidUnsupportedBand
		}
		return 0, 0, 0, 0, 0, 0, VOACAPInvalidEmptyUnknownBand
	}
	if !f.bandEnabled[idx] {
		return 0, 0, 0, 0, 0, 0, VOACAPInvalidUnsupportedBand
	}
	if req.UserCell == InvalidCell {
		return 0, 0, 0, 0, 0, 0, VOACAPInvalidUserCell
	}
	if req.DXCell == InvalidCell {
		return 0, 0, 0, 0, 0, 0, VOACAPInvalidDXCell
	}
	rxLat, rxLon, ok = parseGridCenterLatLonFast(req.UserGrid)
	if !ok {
		return 0, 0, 0, 0, 0, 0, VOACAPInvalidUserGrid
	}
	txLat, txLon, ok = parseGridCenterLatLonFast(req.DXGrid)
	if !ok {
		return 0, 0, 0, 0, 0, 0, VOACAPInvalidDXGrid
	}
	return idx, f.bandFreqMHz[idx], txLat, txLon, rxLat, rxLon, VOACAPInvalidRequestReasonNone
}

func (f *PropPriorFallback) buildForecast(snrTotal, sigmaTotal, freqMHz, receiveNoisePenaltyDB float64, at time.Time) VOACAPCachedForecast {
	snrAdj := snrTotal + f.fallback.SNROffsetDB
	rel := (propprior.Prediction{
		SnrTotal:   snrAdj,
		SigmaTotal: sigmaTotal,
	}).Rel(f.fallback.ReqSNRDB)
	penalty := receiveNoisePenaltyDB
	if penalty < 0 || math.IsNaN(penalty) || math.IsInf(penalty, 0) {
		penalty = 0
	}
	rxDB := snrAdj - penalty
	txDB := snrAdj
	effDB := f.cfg.MergeReceiveWeight*rxDB + f.cfg.MergeTransmitWeight*txDB
	roundedEff := int(math.Round(effDB))
	roundedBase := int(math.Round(snrAdj))
	return VOACAPCachedForecast{
		Record: VOACAPHourlyForecast{
			FT8SNRDB:                        roundedEff,
			HourUTC:                         at.UTC().Hour(),
			FrequencyMHz:                    freqMHz,
			ReqSNRReliability:               rel,
			HasReqSNRReliability:            true,
			ReceiveFT8SNRDB:                 roundedBase,
			TransmitFT8SNRDB:                roundedBase,
			ReceiveReqSNRReliability:        rel,
			TransmitReqSNRReliability:       rel,
			HasDirectionalReqSNRReliability: true,
			HasDirectionalSNR:               true,
		},
		EffectiveFT8SNRDB:     effDB,
		HasEffectiveFT8SNRDB:  true,
		ExactReceiveFT8SNRDB:  rxDB,
		ExactTransmitFT8SNRDB: txDB,
		HasExactDirectionalDB: true,
		ReceiveNoisePenaltyDB: penalty,
		PropPrior:             true,
	}
}

func makePropPriorCacheKey(userCell, dxCell CellID, bandIdx uint8, utc time.Time, bucketSec int64) propPriorCacheKey {
	lo, hi := userCell, dxCell
	if lo > hi {
		lo, hi = hi, lo
	}
	return propPriorCacheKey{
		loCell:     lo,
		hiCell:     hi,
		bandIdx:    bandIdx,
		timeBucket: propPriorFloorDiv(utc.Unix(), bucketSec),
	}
}

func propPriorFloorDiv(a, b int64) int64 {
	q := a / b
	if (a%b) != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func trimASCIIBounds(s string) (int, int) {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\r' || s[end-1] == '\n') {
		end--
	}
	return start, end
}

func isTrimEmptyASCII(s string) bool {
	start, end := trimASCIIBounds(s)
	return start == end
}

func asciiLower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func asciiUpper(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
}

// propPriorBandIndex maps the 9 supported HF skywave bands (80m-10m) to dense
// indices 0..8 without allocating.
func propPriorBandIndex(raw string) (uint8, bool) {
	start, end := trimASCIIBounds(raw)
	n := end - start
	if n != 3 || asciiLower(raw[end-1]) != 'm' {
		return 0, false
	}
	a, b := raw[start], raw[start+1]
	switch {
	case a == '8' && b == '0':
		return 0, true
	case a == '6' && b == '0':
		return 1, true
	case a == '4' && b == '0':
		return 2, true
	case a == '3' && b == '0':
		return 3, true
	case a == '2' && b == '0':
		return 4, true
	case a == '1' && b == '7':
		return 5, true
	case a == '1' && b == '5':
		return 6, true
	case a == '1' && b == '2':
		return 7, true
	case a == '1' && b == '0':
		return 8, true
	default:
		return 0, false
	}
}

func isKnownNonPropPriorBand(raw string) bool {
	start, end := trimASCIIBounds(raw)
	n := end - start
	if n < 2 || n > 5 {
		return false
	}
	last := asciiLower(raw[end-1])
	if last == 'm' {
		if n == 4 && raw[start] == '1' && raw[start+1] == '6' && raw[start+2] == '0' {
			return true
		}
		if n == 2 && (raw[start] == '6' || raw[start] == '4' || raw[start] == '2') {
			return true
		}
		if n == 3 && raw[start] == '1' && raw[start+1] == '.' {
			return false
		}
		if n == 5 && raw[start] == '1' && raw[start+1] == '.' && raw[start+2] == '2' && raw[start+3] == '5' {
			return true
		}
		if n == 4 && raw[start] == '6' && raw[start+1] == '3' && raw[start+2] == '0' {
			return true
		}
		if n == 5 && raw[start] == '2' && raw[start+1] == '2' && raw[start+2] == '0' && raw[start+3] == '0' {
			return true
		}
		if n == 4 && raw[start] == '7' && raw[start+1] == '0' && asciiLower(raw[start+2]) == 'c' {
			return true
		}
		if n == 4 && raw[start] == '3' && raw[start+1] == '3' && asciiLower(raw[start+2]) == 'c' {
			return true
		}
		if n == 4 && raw[start] == '2' && raw[start+1] == '3' && asciiLower(raw[start+2]) == 'c' {
			return true
		}
		if n == 4 && raw[start] == '1' && raw[start+1] == '3' && asciiLower(raw[start+2]) == 'c' {
			return true
		}
	}
	return false
}

// parseGridCenterLatLonFast parses a 4- or 6-character Maidenhead locator into
// its center latitude/longitude with zero heap allocations.
func parseGridCenterLatLonFast(grid string) (lat, lon float64, ok bool) {
	start, end := trimASCIIBounds(grid)
	n := end - start
	if n < 4 || n == 5 {
		return 0, 0, false
	}
	a := asciiUpper(grid[start])
	b := asciiUpper(grid[start+1])
	if a < 'A' || a > 'R' || b < 'A' || b > 'R' {
		return 0, 0, false
	}
	d0, d1 := grid[start+2], grid[start+3]
	if d0 < '0' || d0 > '9' || d1 < '0' || d1 > '9' {
		return 0, 0, false
	}
	lon = -180.0 + float64(a-'A')*fieldLonSize + float64(d0-'0')*squareLonSize
	lat = -90.0 + float64(b-'A')*fieldLatSize + float64(d1-'0')*squareLatSize
	if n >= 6 {
		s0 := asciiUpper(grid[start+4])
		s1 := asciiUpper(grid[start+5])
		if s0 < 'A' || s0 > 'X' || s1 < 'A' || s1 > 'X' {
			return 0, 0, false
		}
		lon += float64(s0-'A')*subLonSize + subCenterLon
		lat += float64(s1-'A')*subLatSize + subCenterLat
		return lat, lon, true
	}
	lon += squareCenterLon
	lat += squareCenterLat
	return lat, lon, true
}
