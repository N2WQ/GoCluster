package spot

import (
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/strutil"
)

const (
	defaultSignalResolverQueueSize              = 8192
	defaultSignalResolverMaxActiveKeys          = 6000
	defaultSignalResolverMaxCandidatesPerKey    = 16
	defaultSignalResolverMaxReportersPerCand    = 64
	defaultSignalResolverInactiveTTL            = 10 * time.Minute
	defaultSignalResolverEvalMinInterval        = 500 * time.Millisecond
	defaultSignalResolverSweepInterval          = 1 * time.Second
	defaultSignalResolverHysteresisWindows      = 2
	defaultSignalResolverRecencyWindow          = 45 * time.Second
	defaultSignalResolverStopDrainDeadline      = 250 * time.Millisecond
	defaultSignalResolverFreqGuardRunnerUpRatio = 0.6

	resolverStateConfidentMinPercent = 51
	resolverStateProbableMinPercent  = 25
)

// ResolverState is the signal-level shadow verdict emitted by the resolver.
type ResolverState string

const (
	ResolverStateConfident ResolverState = "confident"
	ResolverStateProbable  ResolverState = "probable"
	ResolverStateUncertain ResolverState = "uncertain"
	ResolverStateSplit     ResolverState = "split"
)

// ResolverSignalKey identifies one signal-resolution bucket.
//
// Invariants:
//  1. Band/Mode are normalized (band lowercase canonical, mode uppercase).
//  2. ToleranceHz is positive and participates in bucket partitioning.
//  3. Bucket is computed with half-up rounding on frequency/tolerance.
type ResolverSignalKey struct {
	Band        string
	Mode        string
	ToleranceHz int
	Bucket      int64
}

func (k ResolverSignalKey) String() string {
	return k.Band + "|" + k.Mode + "|" + strconvItoa(k.ToleranceHz) + "|" + strconvFormatInt(k.Bucket)
}

// NewResolverSignalKey normalizes a key for a (band, mode, frequency, tolerance) tuple.
func NewResolverSignalKey(freqKHz float64, band, mode string, toleranceHz float64) ResolverSignalKey {
	bandNorm := NormalizeBand(band)
	modeNorm := strutil.NormalizeUpper(mode)
	tolHz := int(math.Round(toleranceHz))
	if tolHz <= 0 {
		tolHz = int(math.Round(500))
	}
	stepKHz := float64(tolHz) / 1000.0
	if stepKHz <= 0 {
		stepKHz = 0.5
	}
	bucket := int64(math.Floor((freqKHz / stepKHz) + 0.5))
	return ResolverSignalKey{
		Band:        bandNorm,
		Mode:        modeNorm,
		ToleranceHz: tolHz,
		Bucket:      bucket,
	}
}

// ResolverEvidence is one immutable pre-correction observation consumed by the resolver.
type ResolverEvidence struct {
	ObservedAt    time.Time
	Key           ResolverSignalKey
	DXCall        string
	Spotter       string
	FrequencyKHz  float64
	RecencyWindow time.Duration
}

// ResolverSnapshot is the latest shadow verdict for a ResolverSignalKey.
type ResolverSnapshot struct {
	Key            ResolverSignalKey
	EvaluatedAt    time.Time
	State          ResolverState
	Winner         string
	RunnerUp       string
	WinnerSupport  int
	RunnerSupport  int
	Margin         int
	TotalReporters int
}

// SignalResolverMetrics exposes resolver ingest and disagreement observability.
type SignalResolverMetrics struct {
	ActiveKeys int
	QueueDepth int

	Accepted              uint64
	Processed             uint64
	DropQueueFull         uint64
	DropMaxKeys           uint64
	DropMaxCandidates     uint64
	DropMaxReporters      uint64
	CapPressureCandidates uint64
	CapPressureReporters  uint64
	EvictedCandidates     uint64
	EvictedReporters      uint64
	HighWaterCandidates   uint64
	HighWaterReporters    uint64

	StateConfident uint64
	StateProbable  uint64
	StateUncertain uint64
	StateSplit     uint64

	DecisionsObserved                uint64
	DecisionsComparable              uint64
	DecisionAgreement                uint64
	DecisionDisagreement             uint64
	DisagreeSplitCorrected           uint64
	DisagreeConfidentDifferentWinner uint64
	DisagreeUncertainCorrected       uint64
}

// SignalResolverConfig controls bounded resolver behavior.
//
// All bounds are hard caps. Enqueue is always fail-open/non-blocking.
type SignalResolverConfig struct {
	QueueSize           int
	MaxActiveKeys       int
	MaxCandidatesPerKey int
	MaxReportersPerCand int
	InactiveTTL         time.Duration
	EvalMinInterval     time.Duration
	SweepInterval       time.Duration
	HysteresisWindows   int

	FreqGuardMinSeparationKHz float64
	FreqGuardRunnerUpRatio    float64
	MaxEditDistance           int
	DistanceModelCW           string
	DistanceModelRTTY         string
	FamilyPolicy              CorrectionFamilyPolicy
}

// SignalResolver performs signal-level shadow resolution from pre-correction evidence.
//
// Concurrency contract:
//  1. A single owner goroutine mutates all per-key/per-candidate resolver state.
//  2. Producers only interact through non-blocking Enqueue.
//  3. Snapshots are published through sync.Map for lock-free read-side lookups.
type SignalResolver struct {
	cfg SignalResolverConfig

	input  chan ResolverEvidence
	stopCh chan struct{}
	doneCh chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool

	snapshots sync.Map // ResolverSignalKey -> ResolverSnapshot

	activeKeys atomic.Int64

	accepted              atomic.Uint64
	processed             atomic.Uint64
	dropQueueFull         atomic.Uint64
	dropMaxKeys           atomic.Uint64
	dropMaxCandidates     atomic.Uint64
	dropMaxReporters      atomic.Uint64
	capPressureCandidates atomic.Uint64
	capPressureReporters  atomic.Uint64
	evictedCandidates     atomic.Uint64
	evictedReporters      atomic.Uint64
	highWaterCandidates   atomic.Uint64
	highWaterReporters    atomic.Uint64

	decisionsObserved                atomic.Uint64
	decisionsComparable              atomic.Uint64
	decisionAgreement                atomic.Uint64
	decisionDisagreement             atomic.Uint64
	disagreeSplitCorrected           atomic.Uint64
	disagreeConfidentDifferentWinner atomic.Uint64
	disagreeUncertainCorrected       atomic.Uint64
}

type resolverCandidate struct {
	lastSeen    time.Time
	lastFreqKHz float64
	reporters   map[string]time.Time
}

type resolverKeyState struct {
	key ResolverSignalKey

	recencyWindow time.Duration
	candidates    map[string]*resolverCandidate
	// reporterRefs tracks how many active candidates currently reference each
	// reporter for this key. Invariant: count is always >=1 when present.
	reporterRefs map[string]int
	lastSeen     time.Time

	dirty      bool
	nextEvalAt time.Time
	lastEvalAt time.Time

	stableWinner  string
	pendingWinner string
	pendingWins   int
}

type rankedResolverCandidate struct {
	call        string
	support     int
	lastSeen    time.Time
	lastFreqKHz float64
	reporters   map[string]time.Time
}

// NewSignalResolver builds a resolver. Call Start to activate the owner goroutine.
func NewSignalResolver(cfg SignalResolverConfig) *SignalResolver {
	cfg = normalizeSignalResolverConfig(cfg)
	return &SignalResolver{
		cfg:    cfg,
		input:  make(chan ResolverEvidence, cfg.QueueSize),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the single owner goroutine exactly once.
func (r *SignalResolver) Start() {
	if r == nil {
		return
	}
	r.startOnce.Do(func() {
		r.started.Store(true)
		go r.run()
	})
}

// Stop requests shutdown and waits for bounded drain completion.
func (r *SignalResolver) Stop() {
	if r == nil || !r.started.Load() {
		return
	}
	r.stopOnce.Do(func() {
		close(r.stopCh)
		<-r.doneCh
	})
}

// Enqueue submits one evidence item without blocking.
//
// Drop policy: when input queue is full, evidence is dropped and accounted.
func (r *SignalResolver) Enqueue(e ResolverEvidence) bool {
	if r == nil {
		return false
	}
	ev, ok := normalizeResolverEvidence(e)
	if !ok {
		return false
	}
	select {
	case r.input <- ev:
		r.accepted.Add(1)
		return true
	default:
		r.dropQueueFull.Add(1)
		return false
	}
}

// Lookup returns the latest snapshot for key when present.
func (r *SignalResolver) Lookup(key ResolverSignalKey) (ResolverSnapshot, bool) {
	if r == nil {
		return ResolverSnapshot{}, false
	}
	if value, ok := r.snapshots.Load(key); ok {
		if snap, valid := value.(ResolverSnapshot); valid {
			return snap, true
		}
	}
	return ResolverSnapshot{}, false
}

// ObserveCurrentDecision compares current-path outcome with resolver shadow state.
func (r *SignalResolver) ObserveCurrentDecision(key ResolverSignalKey, finalCall string, corrected bool) {
	if r == nil {
		return
	}
	finalCall = NormalizeCallsign(finalCall)
	if finalCall == "" {
		return
	}
	r.decisionsObserved.Add(1)

	snap, ok := r.Lookup(key)
	if !ok {
		return
	}

	if corrected {
		switch snap.State {
		case ResolverStateSplit:
			r.disagreeSplitCorrected.Add(1)
		case ResolverStateUncertain:
			r.disagreeUncertainCorrected.Add(1)
		case ResolverStateConfident:
			if snap.Winner != "" && !strings.EqualFold(snap.Winner, finalCall) {
				r.disagreeConfidentDifferentWinner.Add(1)
			}
		}
	}

	if snap.State != ResolverStateConfident && snap.State != ResolverStateProbable {
		return
	}
	r.decisionsComparable.Add(1)
	if snap.Winner != "" && strings.EqualFold(snap.Winner, finalCall) {
		r.decisionAgreement.Add(1)
		return
	}
	r.decisionDisagreement.Add(1)
}

// MetricsSnapshot returns a point-in-time snapshot of resolver observability.
func (r *SignalResolver) MetricsSnapshot() SignalResolverMetrics {
	if r == nil {
		return SignalResolverMetrics{}
	}
	metrics := SignalResolverMetrics{
		ActiveKeys: int(r.activeKeys.Load()),
		QueueDepth: len(r.input),

		Accepted:              r.accepted.Load(),
		Processed:             r.processed.Load(),
		DropQueueFull:         r.dropQueueFull.Load(),
		DropMaxKeys:           r.dropMaxKeys.Load(),
		DropMaxCandidates:     r.dropMaxCandidates.Load(),
		DropMaxReporters:      r.dropMaxReporters.Load(),
		CapPressureCandidates: r.capPressureCandidates.Load(),
		CapPressureReporters:  r.capPressureReporters.Load(),
		EvictedCandidates:     r.evictedCandidates.Load(),
		EvictedReporters:      r.evictedReporters.Load(),
		HighWaterCandidates:   r.highWaterCandidates.Load(),
		HighWaterReporters:    r.highWaterReporters.Load(),

		DecisionsObserved:                r.decisionsObserved.Load(),
		DecisionsComparable:              r.decisionsComparable.Load(),
		DecisionAgreement:                r.decisionAgreement.Load(),
		DecisionDisagreement:             r.decisionDisagreement.Load(),
		DisagreeSplitCorrected:           r.disagreeSplitCorrected.Load(),
		DisagreeConfidentDifferentWinner: r.disagreeConfidentDifferentWinner.Load(),
		DisagreeUncertainCorrected:       r.disagreeUncertainCorrected.Load(),
	}
	r.snapshots.Range(func(_, value any) bool {
		snap, ok := value.(ResolverSnapshot)
		if !ok {
			return true
		}
		switch snap.State {
		case ResolverStateConfident:
			metrics.StateConfident++
		case ResolverStateProbable:
			metrics.StateProbable++
		case ResolverStateUncertain:
			metrics.StateUncertain++
		case ResolverStateSplit:
			metrics.StateSplit++
		}
		return true
	})
	return metrics
}

func (r *SignalResolver) run() {
	defer close(r.doneCh)
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("signal resolver panic: %v", recovered)
		}
	}()

	states := make(map[ResolverSignalKey]*resolverKeyState, r.cfg.MaxActiveKeys/2)
	ticker := time.NewTicker(r.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			r.drain(states, time.Now().UTC().Add(defaultSignalResolverStopDrainDeadline))
			r.activeKeys.Store(int64(len(states)))
			return
		case ev := <-r.input:
			r.processed.Add(1)
			r.applyEvidence(states, ev)
		case now := <-ticker.C:
			r.sweep(states, now.UTC())
		}
	}
}

func (r *SignalResolver) drain(states map[ResolverSignalKey]*resolverKeyState, deadline time.Time) {
	for time.Now().UTC().Before(deadline) {
		select {
		case ev := <-r.input:
			r.processed.Add(1)
			r.applyEvidence(states, ev)
		default:
			return
		}
	}
}

func (r *SignalResolver) applyEvidence(states map[ResolverSignalKey]*resolverKeyState, ev ResolverEvidence) {
	st := states[ev.Key]
	if st == nil {
		if len(states) >= r.cfg.MaxActiveKeys {
			r.dropMaxKeys.Add(1)
			return
		}
		st = &resolverKeyState{
			key:           ev.Key,
			recencyWindow: ev.RecencyWindow,
			candidates:    make(map[string]*resolverCandidate, r.cfg.MaxCandidatesPerKey),
			reporterRefs:  make(map[string]int, r.cfg.MaxReportersPerCand),
			lastSeen:      ev.ObservedAt,
		}
		states[ev.Key] = st
	}

	if ev.RecencyWindow > 0 {
		st.recencyWindow = ev.RecencyWindow
	}
	st.lastSeen = ev.ObservedAt

	r.pruneKeyState(st, ev.ObservedAt)

	candidate := st.candidates[ev.DXCall]
	if candidate == nil {
		if len(st.candidates) >= r.cfg.MaxCandidatesPerKey {
			r.capPressureCandidates.Add(1)
			evictCall, ok := chooseResolverCandidateEviction(st.candidates)
			if !ok {
				r.dropMaxCandidates.Add(1)
				r.activeKeys.Store(int64(len(states)))
				return
			}
			removeResolverCandidate(st, evictCall)
			r.evictedCandidates.Add(1)
		}
		candidate = &resolverCandidate{
			reporters: make(map[string]time.Time, r.cfg.MaxReportersPerCand),
		}
		st.candidates[ev.DXCall] = candidate
	}
	updateAtomicMax(&r.highWaterCandidates, uint64(len(st.candidates)))
	candidate.lastSeen = ev.ObservedAt
	candidate.lastFreqKHz = ev.FrequencyKHz

	if _, exists := candidate.reporters[ev.Spotter]; !exists && len(candidate.reporters) >= r.cfg.MaxReportersPerCand {
		r.capPressureReporters.Add(1)
		evictReporter, ok := chooseResolverReporterEviction(candidate.reporters)
		if !ok {
			r.dropMaxReporters.Add(1)
			r.activeKeys.Store(int64(len(states)))
			return
		}
		removeResolverReporter(st, candidate, evictReporter)
		r.evictedReporters.Add(1)
	}
	upsertResolverReporter(st, candidate, ev.Spotter, ev.ObservedAt)
	updateAtomicMax(&r.highWaterReporters, uint64(len(candidate.reporters)))

	st.dirty = true
	if st.nextEvalAt.IsZero() || !ev.ObservedAt.Before(st.nextEvalAt) {
		r.evaluateKey(st, ev.ObservedAt)
		st.nextEvalAt = ev.ObservedAt.Add(r.cfg.EvalMinInterval)
		st.dirty = false
	}

	r.activeKeys.Store(int64(len(states)))
}

func (r *SignalResolver) sweep(states map[ResolverSignalKey]*resolverKeyState, now time.Time) {
	for key, st := range states {
		if st == nil {
			delete(states, key)
			r.snapshots.Delete(key)
			continue
		}

		if now.Sub(st.lastSeen) > r.cfg.InactiveTTL {
			delete(states, key)
			r.snapshots.Delete(key)
			continue
		}

		r.pruneKeyState(st, now)
		if len(st.candidates) == 0 {
			delete(states, key)
			r.snapshots.Delete(key)
			continue
		}

		if !now.Before(st.nextEvalAt) {
			if st.dirty || now.Sub(st.lastEvalAt) >= r.cfg.SweepInterval {
				r.evaluateKey(st, now)
				st.nextEvalAt = now.Add(r.cfg.EvalMinInterval)
				st.dirty = false
			}
		}
	}
	r.activeKeys.Store(int64(len(states)))
}

func upsertResolverReporter(st *resolverKeyState, candidate *resolverCandidate, reporter string, seenAt time.Time) {
	if st == nil || candidate == nil || reporter == "" {
		return
	}
	if candidate.reporters == nil {
		candidate.reporters = make(map[string]time.Time, 1)
	}
	if st.reporterRefs == nil {
		st.reporterRefs = make(map[string]int, 1)
	}
	if _, exists := candidate.reporters[reporter]; !exists {
		st.reporterRefs[reporter]++
	}
	candidate.reporters[reporter] = seenAt
}

func removeResolverReporter(st *resolverKeyState, candidate *resolverCandidate, reporter string) {
	if st == nil || candidate == nil || reporter == "" {
		return
	}
	if candidate.reporters == nil {
		return
	}
	if _, exists := candidate.reporters[reporter]; !exists {
		return
	}
	delete(candidate.reporters, reporter)
	if st.reporterRefs == nil {
		return
	}
	count, exists := st.reporterRefs[reporter]
	if !exists || count <= 1 {
		delete(st.reporterRefs, reporter)
		return
	}
	st.reporterRefs[reporter] = count - 1
}

func removeResolverCandidate(st *resolverKeyState, call string) {
	if st == nil {
		return
	}
	candidate, exists := st.candidates[call]
	if !exists {
		return
	}
	if candidate != nil {
		for reporter := range candidate.reporters {
			removeResolverReporter(st, candidate, reporter)
		}
	}
	delete(st.candidates, call)
}

func (r *SignalResolver) pruneKeyState(st *resolverKeyState, now time.Time) {
	if st == nil {
		return
	}
	window := st.recencyWindow
	if window <= 0 {
		window = defaultSignalResolverRecencyWindow
	}
	cutoff := now.Add(-window)
	for call, candidate := range st.candidates {
		if candidate == nil {
			removeResolverCandidate(st, call)
			continue
		}
		for reporter, seenAt := range candidate.reporters {
			if seenAt.Before(cutoff) {
				removeResolverReporter(st, candidate, reporter)
			}
		}
		if len(candidate.reporters) == 0 || candidate.lastSeen.Before(cutoff) {
			removeResolverCandidate(st, call)
		}
	}
}

func (r *SignalResolver) evaluateKey(st *resolverKeyState, now time.Time) {
	if st == nil {
		return
	}
	r.pruneKeyState(st, now)
	st.lastEvalAt = now

	ranked := make([]rankedResolverCandidate, 0, len(st.candidates))
	top := rankedResolverCandidate{}
	runner := rankedResolverCandidate{}
	hasTop := false
	hasRunner := false
	for call, candidate := range st.candidates {
		if candidate == nil {
			continue
		}
		support := len(candidate.reporters)
		if support <= 0 {
			continue
		}
		entry := rankedResolverCandidate{
			call:        call,
			support:     support,
			lastSeen:    candidate.lastSeen,
			lastFreqKHz: candidate.lastFreqKHz,
			reporters:   candidate.reporters,
		}
		ranked = append(ranked, entry)
		if !hasTop || resolverCandidateRanksAhead(entry, top) {
			if hasTop {
				runner = top
				hasRunner = true
			}
			top = entry
			hasTop = true
			continue
		}
		if !hasRunner || resolverCandidateRanksAhead(entry, runner) {
			runner = entry
			hasRunner = true
		}
	}

	snapshot := ResolverSnapshot{
		Key:         st.key,
		EvaluatedAt: now,
		State:       ResolverStateUncertain,
	}
	if !hasTop {
		st.stableWinner = ""
		st.pendingWinner = ""
		st.pendingWins = 0
		r.snapshots.Store(st.key, snapshot)
		return
	}

	totalReporters := len(st.reporterRefs)
	if totalReporters <= 0 {
		// Defensive fallback for externally-constructed test states that do not
		// initialize reporterRefs.
		totalReporters = totalUniqueResolverReporters(ranked)
	}
	provisionalState := classifyResolverState(top.support, totalReporters)
	provisionalWinner := top.call
	margin := top.support
	if hasRunner {
		margin = top.support - runner.support
	}

	split := false
	if hasRunner {
		winnerIdentity := normalizeCorrectionCallIdentity(top.call)
		runnerIdentity := normalizeCorrectionCallIdentity(runner.call)
		related := false
		if _, ok := detectCorrectionFamilyByIdentity(winnerIdentity, runnerIdentity, r.cfg.FamilyPolicy); ok {
			related = true
		}
		distance := correctionDistance(winnerIdentity, runnerIdentity, st.key.Mode, r.cfg.DistanceModelCW, r.cfg.DistanceModelRTTY)
		overlap := timedReporterSetOverlapCount(top.reporters, runner.reporters)
		split = shouldRejectAsAmbiguousMultiSignal(
			top.support,
			runner.support,
			top.lastFreqKHz,
			runner.lastFreqKHz,
			r.cfg.FreqGuardMinSeparationKHz,
			r.cfg.FreqGuardRunnerUpRatio,
			overlap,
			distance,
			r.cfg.MaxEditDistance,
			related,
		)
	}

	if split {
		st.pendingWinner = ""
		st.pendingWins = 0
		snapshot.State = ResolverStateSplit
		snapshot.Winner = ""
		snapshot.RunnerUp = runner.call
		snapshot.WinnerSupport = top.support
		snapshot.RunnerSupport = runner.support
		snapshot.Margin = margin
		snapshot.TotalReporters = totalReporters
		r.snapshots.Store(st.key, snapshot)
		return
	}

	publishedWinner := provisionalWinner
	publishedState := provisionalState
	if st.stableWinner == "" {
		st.stableWinner = provisionalWinner
		st.pendingWinner = ""
		st.pendingWins = 0
	} else if provisionalWinner == st.stableWinner {
		st.pendingWinner = ""
		st.pendingWins = 0
	} else if margin > 0 {
		if st.pendingWinner == provisionalWinner {
			st.pendingWins++
		} else {
			st.pendingWinner = provisionalWinner
			st.pendingWins = 1
		}
		if st.pendingWins >= r.cfg.HysteresisWindows {
			st.stableWinner = provisionalWinner
			st.pendingWinner = ""
			st.pendingWins = 0
		} else {
			publishedWinner = st.stableWinner
			publishedState = ResolverStateUncertain
		}
	} else {
		publishedWinner = st.stableWinner
		publishedState = ResolverStateUncertain
		st.pendingWinner = ""
		st.pendingWins = 0
	}

	winnerSupport := supportForResolverCall(ranked, publishedWinner)
	runnerCall, runnerSupport := runnerForResolverCall(ranked, publishedWinner)
	if winnerSupport == 0 {
		winnerSupport = top.support
		publishedWinner = top.call
	}

	snapshot.State = publishedState
	snapshot.Winner = publishedWinner
	snapshot.RunnerUp = runnerCall
	snapshot.WinnerSupport = winnerSupport
	snapshot.RunnerSupport = runnerSupport
	snapshot.Margin = winnerSupport - runnerSupport
	snapshot.TotalReporters = totalReporters
	r.snapshots.Store(st.key, snapshot)
}

func normalizeSignalResolverConfig(cfg SignalResolverConfig) SignalResolverConfig {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultSignalResolverQueueSize
	}
	if cfg.MaxActiveKeys <= 0 {
		cfg.MaxActiveKeys = defaultSignalResolverMaxActiveKeys
	}
	if cfg.MaxCandidatesPerKey <= 0 {
		cfg.MaxCandidatesPerKey = defaultSignalResolverMaxCandidatesPerKey
	}
	if cfg.MaxReportersPerCand <= 0 {
		cfg.MaxReportersPerCand = defaultSignalResolverMaxReportersPerCand
	}
	if cfg.InactiveTTL <= 0 {
		cfg.InactiveTTL = defaultSignalResolverInactiveTTL
	}
	if cfg.EvalMinInterval <= 0 {
		cfg.EvalMinInterval = defaultSignalResolverEvalMinInterval
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = defaultSignalResolverSweepInterval
	}
	if cfg.HysteresisWindows <= 0 {
		cfg.HysteresisWindows = defaultSignalResolverHysteresisWindows
	}
	if cfg.FreqGuardRunnerUpRatio <= 0 {
		cfg.FreqGuardRunnerUpRatio = defaultSignalResolverFreqGuardRunnerUpRatio
	}
	cfg.FamilyPolicy = normalizeCorrectionFamilyPolicy(cfg.FamilyPolicy)
	return cfg
}

func normalizeResolverEvidence(e ResolverEvidence) (ResolverEvidence, bool) {
	call := NormalizeCallsign(e.DXCall)
	spotter := strutil.NormalizeUpper(e.Spotter)
	if call == "" || spotter == "" {
		return ResolverEvidence{}, false
	}
	key := NewResolverSignalKey(
		e.FrequencyKHz,
		e.Key.Band,
		e.Key.Mode,
		float64(e.Key.ToleranceHz),
	)
	if key.Band == "" {
		return ResolverEvidence{}, false
	}
	if !IsCallCorrectionCandidate(key.Mode) {
		return ResolverEvidence{}, false
	}
	observedAt := e.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	window := e.RecencyWindow
	if window <= 0 {
		window = defaultSignalResolverRecencyWindow
	}
	return ResolverEvidence{
		ObservedAt:    observedAt,
		Key:           key,
		DXCall:        call,
		Spotter:       spotter,
		FrequencyKHz:  e.FrequencyKHz,
		RecencyWindow: window,
	}, true
}

func classifyResolverState(support int, totalReporters int) ResolverState {
	if support <= 0 || totalReporters <= 0 {
		return ResolverStateUncertain
	}
	confidence := support * 100 / totalReporters
	switch {
	case confidence >= resolverStateConfidentMinPercent:
		return ResolverStateConfident
	case confidence >= resolverStateProbableMinPercent:
		return ResolverStateProbable
	default:
		return ResolverStateUncertain
	}
}

func totalUniqueResolverReporters(candidates []rankedResolverCandidate) int {
	if len(candidates) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, 64)
	for _, candidate := range candidates {
		for reporter := range candidate.reporters {
			seen[reporter] = struct{}{}
		}
	}
	return len(seen)
}

func resolverCandidateRanksAhead(left, right rankedResolverCandidate) bool {
	if left.support != right.support {
		return left.support > right.support
	}
	if !left.lastSeen.Equal(right.lastSeen) {
		return left.lastSeen.After(right.lastSeen)
	}
	return left.call < right.call
}

func supportForResolverCall(candidates []rankedResolverCandidate, call string) int {
	for _, candidate := range candidates {
		if strings.EqualFold(candidate.call, call) {
			return candidate.support
		}
	}
	return 0
}

func runnerForResolverCall(candidates []rankedResolverCandidate, winner string) (string, int) {
	var (
		runner rankedResolverCandidate
		found  bool
	)
	for _, candidate := range candidates {
		if strings.EqualFold(candidate.call, winner) {
			continue
		}
		if !found || resolverCandidateRanksAhead(candidate, runner) {
			runner = candidate
			found = true
		}
	}
	if found {
		return runner.call, runner.support
	}
	return "", 0
}

func timedReporterSetOverlapCount(left, right map[string]time.Time) int {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	if len(left) > len(right) {
		left, right = right, left
	}
	overlap := 0
	for reporter := range left {
		if _, ok := right[reporter]; ok {
			overlap++
		}
	}
	return overlap
}

func chooseResolverCandidateEviction(candidates map[string]*resolverCandidate) (string, bool) {
	if len(candidates) == 0 {
		return "", false
	}
	var (
		selectedCall     string
		selectedSupport  int
		selectedLastSeen time.Time
		initialized      bool
	)
	for call, candidate := range candidates {
		support := 0
		lastSeen := time.Time{}
		if candidate != nil {
			support = len(candidate.reporters)
			lastSeen = candidate.lastSeen
		}
		if !initialized ||
			support < selectedSupport ||
			(support == selectedSupport && lastSeen.Before(selectedLastSeen)) ||
			(support == selectedSupport && lastSeen.Equal(selectedLastSeen) && call < selectedCall) {
			selectedCall = call
			selectedSupport = support
			selectedLastSeen = lastSeen
			initialized = true
		}
	}
	return selectedCall, initialized
}

func chooseResolverReporterEviction(reporters map[string]time.Time) (string, bool) {
	if len(reporters) == 0 {
		return "", false
	}
	var (
		selectedReporter string
		selectedSeenAt   time.Time
		initialized      bool
	)
	for reporter, seenAt := range reporters {
		if !initialized ||
			seenAt.Before(selectedSeenAt) ||
			(seenAt.Equal(selectedSeenAt) && reporter < selectedReporter) {
			selectedReporter = reporter
			selectedSeenAt = seenAt
			initialized = true
		}
	}
	return selectedReporter, initialized
}

func updateAtomicMax(dst *atomic.Uint64, value uint64) {
	for {
		current := dst.Load()
		if value <= current {
			return
		}
		if dst.CompareAndSwap(current, value) {
			return
		}
	}
}

func strconvItoa(v int) string {
	return strconvFormatInt(int64(v))
}

func strconvFormatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}
