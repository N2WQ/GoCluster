// File role: Multi-stream background space-weather poller with primary/fallback
// non-overwrite hysteresis, atomic SpaceWeatherStore hydration, and lock-free
// health diagnostics.
// Crawler notes: Owns 4 independent stream goroutines (hp30, sfi, rtsw, dst).
// Inside rtsw, mag and wind poll and back off independently. Start is a no-op
// when Config.Enabled is false; Stop cancels requests, closes idle transport
// connections, and joins all workers.
package feed

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/internal/logutil"
	"dxcluster/internal/propprior"
)

const (
	gfzHp30Lookback  = 36 * time.Hour
	gfzFobsLookback  = 14 * 24 * time.Hour
	maxHealthErrLen  = 256
	errorLogCooldown = 15 * time.Minute
)

// StreamState classifies the live ingestion health of one space-weather stream.
type StreamState string

const (
	StateCold            StreamState = "cold"
	StateHealthyPrimary  StreamState = "healthy_primary"
	StateDegradedPrimary StreamState = "degraded_primary"
	StateFallbackActive  StreamState = "fallback_active"
	StateStale           StreamState = "stale"
	StateError           StreamState = "error"
)

// StreamHealth reports bounded operational diagnostics for one stream.
type StreamHealth struct {
	State             StreamState
	ActiveSource      string
	UsingFallback     bool
	LastHTTPStatus    int
	ConsecutiveErrors int
	LastAttemptUTC    time.Time
	LastSuccessUTC    time.Time
	NewestBinUTC      time.Time
	LastError         string
}

// HealthSnapshot is the immutable lock-free health view returned by Poller.Health.
type HealthSnapshot struct {
	UpdatedAtUTC time.Time
	Hp30         StreamHealth
	SFI          StreamHealth
	RTSWMag      StreamHealth
	RTSWWind     StreamHealth
	Dst          StreamHealth
}

// Poller fetches live space-weather streams and publishes atomic batches into
// a SpaceWeatherStore without blocking readers.
type Poller struct {
	cfg    Config
	store  *propprior.SpaceWeatherStore
	logger *log.Logger
	client *boundedHTTPClient
	nowFn  func() time.Time

	lifeMu  sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	healthMu sync.Mutex
	health   atomic.Pointer[HealthSnapshot]
}

// NewPoller validates cfg and constructs a Poller targeting store.
func NewPoller(cfg Config, store *propprior.SpaceWeatherStore, logger *log.Logger) (*Poller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("propprior/feed: store must not be nil")
	}
	p := &Poller{
		cfg:    cfg,
		store:  store,
		logger: logger,
		client: newBoundedHTTPClient(cfg.RequestTimeout, cfg.UserAgent),
		nowFn:  func() time.Time { return time.Now().UTC() },
	}
	initSnap := &HealthSnapshot{
		Hp30:     StreamHealth{State: StateCold},
		SFI:      StreamHealth{State: StateCold},
		RTSWMag:  StreamHealth{State: StateCold},
		RTSWWind: StreamHealth{State: StateCold},
		Dst:      StreamHealth{State: StateCold},
	}
	p.health.Store(initSnap)
	return p, nil
}

// Start launches the 4 independent stream worker goroutines. When Config.Enabled
// is false, ctx is nil, or Start was already called, Start is a no-op and spawns
// 0 goroutines.
func (p *Poller) Start(ctx context.Context) {
	if p == nil || !p.cfg.Enabled || ctx == nil {
		return
	}
	p.lifeMu.Lock()
	if p.started || p.stopped {
		p.lifeMu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.started = true
	p.wg.Add(4)
	p.lifeMu.Unlock()

	go p.runHp30(runCtx)
	go p.runSFI(runCtx)
	go p.runRTSW(runCtx)
	go p.runDst(runCtx)
}

// Stop cancels all in-flight fetches and backoff timers, closes idle HTTP
// connections, and waits for all worker goroutines to exit. Safe to call
// multiple times or when never started.
func (p *Poller) Stop() {
	if p == nil {
		return
	}
	p.lifeMu.Lock()
	cancel := p.cancel
	p.stopped = true
	p.lifeMu.Unlock()
	if cancel != nil {
		cancel()
	}
	p.client.closeIdle()
	p.wg.Wait()
}

// Health returns a lock-free snapshot of all stream health states.
func (p *Poller) Health() HealthSnapshot {
	if p == nil {
		return HealthSnapshot{}
	}
	if snap := p.health.Load(); snap != nil {
		return *snap
	}
	return HealthSnapshot{}
}

type streamTracker struct {
	name                string
	stepSec             int64
	staleAfter          time.Duration
	primarySource       string
	fallbackSource      string
	primaryEP           conditionalEndpoint
	fallbackEP          conditionalEndpoint
	hasPrimaryBin       bool
	lastValidPrimaryBin int64
	hasAnyBin           bool
	newestBinUTC        time.Time
	consecutiveErrors   int
	usingFallback       bool
	lastSuccessUTC      time.Time
	lastErrorLogUTC     time.Time
}

func (p *Poller) runHp30(ctx context.Context) {
	defer p.wg.Done()
	rng := newJitterRNG(uint64(p.nowFn().UnixNano()) ^ 0x11)
	tr := &streamTracker{
		name:          "hp30",
		stepSec:       hp30StepSec,
		staleAfter:    p.cfg.Hp30StaleAfter,
		primarySource: "gfz_hp30",
	}
	for {
		delay := p.pollPrimaryFallback(
			ctx, tr, p.cfg.Hp30PollInterval, p.cfg.MaxBodyBytesStandard, rng,
			func(now time.Time) (string, error) {
				return BuildGFZURL(p.cfg.Hp30PrimaryURL, "Hp30", now.Add(-gfzHp30Lookback), now)
			},
			func(body []byte, now time.Time) ([]propprior.Sample, string, error) {
				s, err := ParseGFZHp30(body, now)
				return s, "gfz_hp30", err
			},
			func(_ time.Time) (string, error) { return p.cfg.Hp30FallbackURL, nil },
			func(body []byte, now time.Time) ([]propprior.Sample, string, error) {
				s, err := ParseNOAAKp(body, now)
				return s, "noaa_kp_fallback", err
			},
			p.cfg.MaxBodyBytesStandard,
			p.store.RecordHp30Batch,
			func(s *HealthSnapshot, h StreamHealth) { s.Hp30 = h },
		)
		if !waitContext(ctx, delay) {
			return
		}
	}
}

func (p *Poller) runSFI(ctx context.Context) {
	defer p.wg.Done()
	rng := newJitterRNG(uint64(p.nowFn().UnixNano()) ^ 0x22)
	tr := &streamTracker{
		name:          "sfi",
		stepSec:       sfiStepSec,
		staleAfter:    p.cfg.SFIStaleAfter,
		primarySource: "noaa_f107",
	}
	for {
		delay := p.pollPrimaryFallback(
			ctx, tr, p.cfg.SFIPollInterval, p.cfg.MaxBodyBytesStandard, rng,
			func(_ time.Time) (string, error) { return p.cfg.SFIPrimaryURL, nil },
			func(body []byte, now time.Time) ([]propprior.Sample, string, error) {
				s, err := ParseNOAAF107(body, now)
				return s, "noaa_f107", err
			},
			func(now time.Time) (string, error) {
				if isGFZBaseURL(p.cfg.SFIFallbackURL) {
					return BuildGFZURL(p.cfg.SFIFallbackURL, "Fobs", now.Add(-gfzFobsLookback), now)
				}
				return p.cfg.SFIFallbackURL, nil
			},
			ParseSFIFallback,
			p.cfg.MaxBodyBytesStandard,
			p.store.RecordSFIBatch,
			func(s *HealthSnapshot, h StreamHealth) { s.SFI = h },
		)
		if !waitContext(ctx, delay) {
			return
		}
	}
}

func (p *Poller) runDst(ctx context.Context) {
	defer p.wg.Done()
	rng := newJitterRNG(uint64(p.nowFn().UnixNano()) ^ 0x33)
	tr := &streamTracker{
		name:          "dst",
		stepSec:       omniStepSec,
		staleAfter:    p.cfg.DstStaleAfter,
		primarySource: "noaa_kyoto_dst",
	}
	for {
		delay := p.pollPrimaryFallback(
			ctx, tr, p.cfg.DstPollInterval, p.cfg.MaxBodyBytesStandard, rng,
			func(_ time.Time) (string, error) { return p.cfg.DstPrimaryURL, nil },
			func(body []byte, now time.Time) ([]propprior.Sample, string, error) {
				s, err := ParseKyotoDst(body, now)
				return s, "noaa_kyoto_dst", err
			},
			func(_ time.Time) (string, error) { return p.cfg.DstFallbackURL, nil },
			func(body []byte, now time.Time) ([]propprior.Sample, string, error) {
				s, err := ParseGeospaceDst(body, now)
				return s, "noaa_geospace_dst_fallback", err
			},
			p.cfg.MaxBodyBytesGeospace,
			p.store.RecordDstBatch,
			func(s *HealthSnapshot, h StreamHealth) { s.Dst = h },
		)
		if !waitContext(ctx, delay) {
			return
		}
	}
}

func (p *Poller) runRTSW(ctx context.Context) {
	defer p.wg.Done()
	rng := newJitterRNG(uint64(p.nowFn().UnixNano()) ^ 0x44)
	magTr := &streamTracker{name: "rtsw_mag", stepSec: omniStepSec, primarySource: "noaa_rtsw_mag"}
	windTr := &streamTracker{name: "rtsw_wind", stepSec: omniStepSec, primarySource: "noaa_rtsw_wind"}

	nextMag := p.nowFn()
	nextWind := nextMag
	for {
		now := p.nowFn()
		if !now.Before(nextMag) {
			delay := p.pollSingleStream(
				ctx, magTr, p.cfg.RTSWMagURL, p.cfg.RTSWPollInterval, p.cfg.MaxBodyBytesRTSW, rng,
				ParseRTSWMag, p.store.RecordBzGSMBatch,
				func(s *HealthSnapshot, h StreamHealth) { s.RTSWMag = h },
			)
			nextMag = p.nowFn().Add(delay)
		}
		if ctx.Err() != nil {
			return
		}
		now = p.nowFn()
		if !now.Before(nextWind) {
			delay := p.pollSingleStream(
				ctx, windTr, p.cfg.RTSWWindURL, p.cfg.RTSWPollInterval, p.cfg.MaxBodyBytesRTSW, rng,
				ParseRTSWWind, p.store.RecordVswBatch,
				func(s *HealthSnapshot, h StreamHealth) { s.RTSWWind = h },
			)
			nextWind = p.nowFn().Add(delay)
		}
		nextWake := nextMag
		if nextWind.Before(nextWake) {
			nextWake = nextWind
		}
		sleep := nextWake.Sub(p.nowFn())
		if !waitContext(ctx, sleep) {
			return
		}
	}
}

func (p *Poller) pollSingleStream(
	ctx context.Context,
	tr *streamTracker,
	targetURL string,
	pollInterval time.Duration,
	maxBytes int64,
	rng *jitterRNG,
	parse func([]byte, time.Time) ([]propprior.Sample, error),
	ingest func([]propprior.Sample, string) int,
	setHealth func(*HealthSnapshot, StreamHealth),
) time.Duration {
	now := p.nowFn()
	res, err := p.client.fetch(ctx, &tr.primaryEP, targetURL, maxBytes, now)
	if err != nil {
		return p.recordSingleError(tr, now, res.statusCode, res.retryAfter, err, pollInterval, rng, setHealth)
	}
	if res.modified {
		samples, parseErr := parse(res.body, now)
		if parseErr != nil {
			return p.recordSingleError(tr, now, res.statusCode, 0, parseErr, pollInterval, rng, setHealth)
		}
		if len(samples) > 0 {
			if accepted := ingest(samples, tr.primarySource); accepted > 0 {
				newest := samples[len(samples)-1].BinStartUTC
				if !tr.hasAnyBin || newest.After(tr.newestBinUTC) {
					tr.newestBinUTC = newest
					tr.hasAnyBin = true
				}
			}
		}
	}
	if tr.consecutiveErrors > 0 {
		p.logf("propprior/feed: %s recovered (source=%s)", tr.name, tr.primarySource)
	}
	tr.consecutiveErrors = 0
	tr.lastSuccessUTC = now
	state := StateHealthyPrimary
	if !tr.hasAnyBin {
		state = StateCold
	}
	p.publishHealth(now, setHealth, StreamHealth{
		State:          state,
		ActiveSource:   tr.primarySource,
		LastHTTPStatus: res.statusCode,
		LastAttemptUTC: now,
		LastSuccessUTC: tr.lastSuccessUTC,
		NewestBinUTC:   tr.newestBinUTC,
	})
	return jitterDuration(pollInterval, rng)
}

func (p *Poller) recordSingleError(
	tr *streamTracker,
	now time.Time,
	statusCode int,
	retryAfter time.Duration,
	err error,
	pollInterval time.Duration,
	rng *jitterRNG,
	setHealth func(*HealthSnapshot, StreamHealth),
) time.Duration {
	tr.consecutiveErrors++
	p.logErrorThrottled(tr, now, err)
	state := StateDegradedPrimary
	if tr.consecutiveErrors >= p.cfg.FailoverConsecutiveErrors {
		state = StateError
	}
	p.publishHealth(now, setHealth, StreamHealth{
		State:             state,
		ActiveSource:      tr.primarySource,
		LastHTTPStatus:    statusCode,
		ConsecutiveErrors: tr.consecutiveErrors,
		LastAttemptUTC:    now,
		LastSuccessUTC:    tr.lastSuccessUTC,
		NewestBinUTC:      tr.newestBinUTC,
		LastError:         truncateErr(err),
	})
	return computeBackoff(pollInterval, tr.consecutiveErrors, retryAfter, rng)
}

func (p *Poller) pollPrimaryFallback(
	ctx context.Context,
	tr *streamTracker,
	pollInterval time.Duration,
	primaryMaxBytes int64,
	rng *jitterRNG,
	buildPrimaryURL func(time.Time) (string, error),
	parsePrimary func([]byte, time.Time) ([]propprior.Sample, string, error),
	buildFallbackURL func(time.Time) (string, error),
	parseFallback func([]byte, time.Time) ([]propprior.Sample, string, error),
	fallbackMaxBytes int64,
	ingest func([]propprior.Sample, string) int,
	setHealth func(*HealthSnapshot, StreamHealth),
) time.Duration {
	now := p.nowFn()
	primaryURL, err := buildPrimaryURL(now)
	var primaryRes fetchResult
	if err == nil {
		primaryRes, err = p.client.fetch(ctx, &tr.primaryEP, primaryURL, primaryMaxBytes, now)
	}

	var primaryErr error
	if err != nil {
		primaryErr = err
		tr.consecutiveErrors++
		p.logErrorThrottled(tr, now, err)
	} else if primaryRes.modified {
		samples, src, parseErr := parsePrimary(primaryRes.body, now)
		if parseErr != nil {
			primaryErr = parseErr
			tr.consecutiveErrors++
			p.logErrorThrottled(tr, now, parseErr)
		} else {
			if len(samples) > 0 {
				if accepted := ingest(samples, src); accepted > 0 {
					newest := samples[len(samples)-1].BinStartUTC
					newestBin := floorDiv(newest.Unix(), tr.stepSec)
					if !tr.hasPrimaryBin || newestBin > tr.lastValidPrimaryBin {
						tr.lastValidPrimaryBin = newestBin
						tr.hasPrimaryBin = true
					}
					if !tr.hasAnyBin || newest.After(tr.newestBinUTC) {
						tr.newestBinUTC = newest
						tr.hasAnyBin = true
					}
				}
			}
			tr.consecutiveErrors = 0
			tr.lastSuccessUTC = now
		}
	} else {
		// 304 Not Modified: HTTP path is healthy, though data freshness is still checked below.
		tr.consecutiveErrors = 0
		tr.lastSuccessUTC = now
	}

	primaryStale := tr.isPrimaryStale(now)
	needFallback := tr.consecutiveErrors >= p.cfg.FailoverConsecutiveErrors || primaryStale

	if !needFallback {
		if tr.usingFallback {
			p.logf("propprior/feed: %s returned to primary source=%s", tr.name, tr.primarySource)
			tr.usingFallback = false
		}
		state := StateHealthyPrimary
		if primaryErr != nil {
			state = StateDegradedPrimary
		} else if !tr.hasAnyBin {
			state = StateCold
		}
		p.publishHealth(now, setHealth, StreamHealth{
			State:             state,
			ActiveSource:      tr.primarySource,
			UsingFallback:     false,
			LastHTTPStatus:    primaryRes.statusCode,
			ConsecutiveErrors: tr.consecutiveErrors,
			LastAttemptUTC:    now,
			LastSuccessUTC:    tr.lastSuccessUTC,
			NewestBinUTC:      tr.newestBinUTC,
			LastError:         truncateErr(primaryErr),
		})
		if primaryErr != nil {
			return computeBackoff(pollInterval, tr.consecutiveErrors, primaryRes.retryAfter, rng)
		}
		return jitterDuration(pollInterval, rng)
	}

	// Primary is stale or exceeded error threshold: invoke fallback with strict
	// non-overwrite gating (> lastValidPrimaryBin unless cold start).
	fbURL, fbErr := buildFallbackURL(now)
	var fbRes fetchResult
	var fbSource string
	if fbErr == nil {
		fbRes, fbErr = p.client.fetch(ctx, &tr.fallbackEP, fbURL, fallbackMaxBytes, now)
	}
	if fbErr == nil && fbRes.modified {
		fbSamples, src, parseErr := parseFallback(fbRes.body, now)
		if parseErr != nil {
			fbErr = parseErr
		} else {
			fbSource = src
			if src != "" {
				tr.fallbackSource = src
			}
			gated := filterNewerThanPrimary(fbSamples, tr.stepSec, tr.hasPrimaryBin, tr.lastValidPrimaryBin)
			if len(gated) > 0 {
				if accepted := ingest(gated, src); accepted > 0 {
					newest := gated[len(gated)-1].BinStartUTC
					if !tr.hasAnyBin || newest.After(tr.newestBinUTC) {
						tr.newestBinUTC = newest
						tr.hasAnyBin = true
					}
				}
			}
		}
	}

	if fbErr == nil {
		activeFallback := nonEmpty(fbSource, tr.fallbackSource)
		if !tr.usingFallback {
			p.logf("propprior/feed: %s activated fallback source=%s (primary_errors=%d stale=%v)",
				tr.name, nonEmpty(activeFallback, "fallback"), tr.consecutiveErrors, primaryStale)
			tr.usingFallback = true
		}
		tr.lastSuccessUTC = now
		p.publishHealth(now, setHealth, StreamHealth{
			State:             StateFallbackActive,
			ActiveSource:      nonEmpty(activeFallback, tr.primarySource),
			UsingFallback:     true,
			LastHTTPStatus:    fbRes.statusCode,
			ConsecutiveErrors: tr.consecutiveErrors,
			LastAttemptUTC:    now,
			LastSuccessUTC:    tr.lastSuccessUTC,
			NewestBinUTC:      tr.newestBinUTC,
			LastError:         truncateErr(primaryErr),
		})
		if primaryErr != nil {
			return computeBackoff(pollInterval, tr.consecutiveErrors, primaryRes.retryAfter, rng)
		}
		return jitterDuration(pollInterval, rng)
	}

	// Both primary and fallback failed or remain stale.
	state := StateError
	if primaryErr == nil && primaryStale {
		state = StateStale
	}
	p.publishHealth(now, setHealth, StreamHealth{
		State:             state,
		ActiveSource:      tr.primarySource,
		UsingFallback:     false,
		LastHTTPStatus:    fbRes.statusCode,
		ConsecutiveErrors: tr.consecutiveErrors,
		LastAttemptUTC:    now,
		LastSuccessUTC:    tr.lastSuccessUTC,
		NewestBinUTC:      tr.newestBinUTC,
		LastError:         truncateErr(fbErr),
	})
	retry := primaryRes.retryAfter
	if fbRes.retryAfter > retry {
		retry = fbRes.retryAfter
	}
	errs := tr.consecutiveErrors
	if errs < 1 {
		errs = 1
	}
	return computeBackoff(pollInterval, errs, retry, rng)
}

func (tr *streamTracker) isPrimaryStale(now time.Time) bool {
	if tr.staleAfter <= 0 || !tr.hasPrimaryBin {
		return false
	}
	// Measure staleness from the end of the newest accepted completed primary
	// bin so a full-width bin (such as daily F10.7) keeps the entire staleAfter
	// grace window after UTC rollover.
	lastEnd := time.Unix((tr.lastValidPrimaryBin+1)*tr.stepSec, 0).UTC()
	return now.Sub(lastEnd) > tr.staleAfter
}

func filterNewerThanPrimary(samples []propprior.Sample, stepSec int64, hasPrimary bool, lastPrimaryBin int64) []propprior.Sample {
	if !hasPrimary || len(samples) == 0 {
		return samples
	}
	out := make([]propprior.Sample, 0, len(samples))
	for _, s := range samples {
		bin := floorDiv(s.BinStartUTC.UTC().Unix(), stepSec)
		if bin > lastPrimaryBin {
			out = append(out, s)
		}
	}
	return out
}

func isGFZBaseURL(rawURL string) bool {
	lower := strings.ToLower(rawURL)
	return strings.Contains(lower, "kp.gfz") || strings.Contains(lower, "/app/json")
}

func (p *Poller) publishHealth(now time.Time, setHealth func(*HealthSnapshot, StreamHealth), h StreamHealth) {
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	cur := p.health.Load()
	var next HealthSnapshot
	if cur != nil {
		next = *cur
	}
	next.UpdatedAtUTC = now
	setHealth(&next, h)
	p.health.Store(&next)
}

func (p *Poller) logErrorThrottled(tr *streamTracker, now time.Time, err error) {
	if err == nil {
		return
	}
	if tr.consecutiveErrors == 1 || tr.consecutiveErrors == p.cfg.FailoverConsecutiveErrors || now.Sub(tr.lastErrorLogUTC) >= errorLogCooldown {
		tr.lastErrorLogUTC = now
		p.logf("propprior/feed: %s fetch/parse failed (consecutive=%d): %v", tr.name, tr.consecutiveErrors, err)
	}
}

func (p *Poller) logf(format string, args ...any) {
	if p == nil || p.logger == nil {
		return
	}
	logutil.SafePrintf(p.logger, format, args...)
}

func truncateErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > maxHealthErrLen {
		return s[:maxHealthErrLen]
	}
	return s
}

func nonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
