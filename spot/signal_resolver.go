// File role: Owns the retained signal resolver used by call-correction support.
// Crawler notes: Start here for resolver bucket keys, bounded candidate/report
// retention, background evaluation cadence, hysteresis, and decision states
// that feed runtime output and replay diagnostics.
// Related docs: spot/README.md, docs/decision-log.md.
// Related tests: spot/signal_resolver*_test.go, spot/*resolver*_bench_test.go.
package spot

import (
	"log"
	"math"
	"sort"
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
	resolverReliabilityScale                    = 1000

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
	Report        int
	FrequencyKHz  float64
	RecencyWindow time.Duration
}

// ResolverSnapshot is the latest shadow verdict for a ResolverSignalKey.
type ResolverSnapshot struct {
	Key                        ResolverSignalKey
	EvaluatedAt                time.Time
	State                      ResolverState
	Winner                     string
	RunnerUp                   string
	WinnerSupport              int
	RunnerSupport              int
	Margin                     int
	TotalReporters             int
	WinnerWeightedSupportMilli int
	RunnerWeightedSupportMilli int
	TotalWeightedSupportMilli  int
	CandidateRanks             []ResolverCandidateSupport
}

// ResolverCandidateSupport captures one candidate support tally from the latest
// resolver evaluation, sorted by resolver ranking.
type ResolverCandidateSupport struct {
	Call                 string
	Support              int
	WeightedSupportMilli int
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
	DropReliability       uint64
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

	FreqGuardMinSeparationKHz  float64
	FreqGuardRunnerUpRatio     float64
	MaxEditDistance            int
	DistanceModelCW            string
	DistanceModelRTTY          string
	FamilyPolicy               CorrectionFamilyPolicy
	SpotterReliability         SpotterReliability
	SpotterReliabilityCW       SpotterReliability
	SpotterReliabilityRTTY     SpotterReliability
	MinSpotterReliability      float64
	ConfusionModel             *ConfusionModel
	ConfusionWeight            float64
	minSpotterReliabilityMilli int
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

	snapshots          sync.Map // ResolverSignalKey -> *resolverSnapshotCell
	confusionWorkspace confusionAlignmentWorkspace

	activeKeys atomic.Int64

	accepted              atomic.Uint64
	processed             atomic.Uint64
	dropQueueFull         atomic.Uint64
	dropMaxKeys           atomic.Uint64
	dropMaxCandidates     atomic.Uint64
	dropMaxReporters      atomic.Uint64
	dropReliability       atomic.Uint64
	capPressureCandidates atomic.Uint64
	capPressureReporters  atomic.Uint64
	evictedCandidates     atomic.Uint64
	evictedReporters      atomic.Uint64
	highWaterCandidates   atomic.Uint64
	highWaterReporters    atomic.Uint64
}

type resolverCandidate struct {
	lastSeen    time.Time
	lastReport  int
	lastFreqKHz float64
	reporters   map[string]time.Time
	identity    correctionCallIdentity
	callRunes   []rune
}

type resolverKeyState struct {
	key ResolverSignalKey

	recencyWindow time.Duration
	candidates    map[string]*resolverCandidate
	// reporterRefs tracks how many active candidates currently reference each
	// reporter for this key. Invariant: refCount is always >=1 when present.
	reporterRefs map[string]resolverReporterRef
	lastSeen     time.Time

	dirty      bool
	nextEvalAt time.Time
	lastEvalAt time.Time

	stableWinner  string
	pendingWinner string
	pendingWins   int

	rankedScratch           []rankedResolverCandidate
	candidateRanksScratch   []ResolverCandidateSupport
	publishedCandidateRanks []ResolverCandidateSupport
}

type rankedResolverCandidate struct {
	call                 string
	support              int
	weightedSupportMilli int
	confusionScore       float64
	lastSeen             time.Time
	lastReport           int
	lastFreqKHz          float64
	reporters            map[string]time.Time
	identity             correctionCallIdentity
	callRunes            []rune
}

type resolverReporterRef struct {
	refCount    int
	weightMilli int
}

type resolverSnapshotCell struct {
	snapshot atomic.Pointer[ResolverSnapshot]
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
		if cell, valid := value.(*resolverSnapshotCell); valid && cell != nil {
			if snap := cell.snapshot.Load(); snap != nil {
				return *snap, true
			}
		}
	}
	return ResolverSnapshot{}, false
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
		DropReliability:       r.dropReliability.Load(),
		CapPressureCandidates: r.capPressureCandidates.Load(),
		CapPressureReporters:  r.capPressureReporters.Load(),
		EvictedCandidates:     r.evictedCandidates.Load(),
		EvictedReporters:      r.evictedReporters.Load(),
		HighWaterCandidates:   r.highWaterCandidates.Load(),
		HighWaterReporters:    r.highWaterReporters.Load(),
	}
	r.snapshots.Range(func(_, value any) bool {
		cell, ok := value.(*resolverSnapshotCell)
		if !ok || cell == nil {
			return true
		}
		snap := cell.snapshot.Load()
		if snap == nil {
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
	reporterWeightMilli, accepted := resolverSpotterWeightMilli(r.cfg, ev.Key.Mode, ev.Spotter)
	if !accepted {
		r.dropReliability.Add(1)
		return
	}
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
			reporterRefs:  make(map[string]resolverReporterRef, r.cfg.MaxReportersPerCand),
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
			identity:  normalizeCorrectionCallIdentity(ev.DXCall),
			callRunes: []rune(ev.DXCall),
		}
		st.candidates[ev.DXCall] = candidate
	}
	updateAtomicMax(&r.highWaterCandidates, uint64(len(st.candidates)))
	candidate.lastSeen = ev.ObservedAt
	candidate.lastReport = ev.Report
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
	upsertResolverReporter(st, candidate, ev.Spotter, ev.ObservedAt, reporterWeightMilli)
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

func upsertResolverReporter(st *resolverKeyState, candidate *resolverCandidate, reporter string, seenAt time.Time, reporterWeightMilli int) {
	if st == nil || candidate == nil || reporter == "" {
		return
	}
	if candidate.reporters == nil {
		candidate.reporters = make(map[string]time.Time, 1)
	}
	if st.reporterRefs == nil {
		st.reporterRefs = make(map[string]resolverReporterRef, 1)
	}
	if _, exists := candidate.reporters[reporter]; !exists {
		ref, exists := st.reporterRefs[reporter]
		if !exists {
			st.reporterRefs[reporter] = resolverReporterRef{
				refCount:    1,
				weightMilli: reporterWeightMilli,
			}
		} else {
			ref.refCount++
			st.reporterRefs[reporter] = ref
		}
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
	ref, exists := st.reporterRefs[reporter]
	if !exists || ref.refCount <= 1 {
		delete(st.reporterRefs, reporter)
		return
	}
	ref.refCount--
	st.reporterRefs[reporter] = ref
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
	invalidateResolverWinnerState(st, call)
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

// invalidateResolverWinnerState keeps hysteresis state coupled to retained
// candidates. A removed stable winner cannot remain authoritative across later
// evaluations; any pending challenger against that old winner is stale too.
func invalidateResolverWinnerState(st *resolverKeyState, call string) {
	if st == nil || call == "" {
		return
	}
	if st.stableWinner == call {
		st.stableWinner = ""
		st.pendingWinner = ""
		st.pendingWins = 0
		return
	}
	if st.pendingWinner == call {
		st.pendingWinner = ""
		st.pendingWins = 0
	}
}

func resolverRankedContainsCall(candidates []rankedResolverCandidate, call string) bool {
	if call == "" {
		return false
	}
	for i := range candidates {
		if candidates[i].call == call {
			return true
		}
	}
	return false
}

func (r *SignalResolver) evaluateKey(st *resolverKeyState, now time.Time) {
	if st == nil {
		return
	}
	r.pruneKeyState(st, now)
	st.lastEvalAt = now

	ranked := st.rankedScratch[:0]
	for call, candidate := range st.candidates {
		if candidate == nil {
			continue
		}
		support := len(candidate.reporters)
		if support <= 0 {
			continue
		}
		entry := rankedResolverCandidate{
			call:                 call,
			support:              support,
			weightedSupportMilli: weightedSupportForResolverCandidate(candidate, st.reporterRefs),
			lastSeen:             candidate.lastSeen,
			lastReport:           candidate.lastReport,
			lastFreqKHz:          candidate.lastFreqKHz,
			reporters:            candidate.reporters,
			identity:             candidate.identity,
			callRunes:            candidate.callRunes,
		}
		ranked = append(ranked, entry)
	}

	snapshot := ResolverSnapshot{
		Key:         st.key,
		EvaluatedAt: now,
		State:       ResolverStateUncertain,
	}
	if len(ranked) == 0 {
		st.stableWinner = ""
		st.pendingWinner = ""
		st.pendingWins = 0
		st.rankedScratch = ranked[:0]
		r.publishSnapshot(st.key, snapshot)
		return
	}
	if st.stableWinner != "" && !resolverRankedContainsCall(ranked, st.stableWinner) {
		st.stableWinner = ""
		st.pendingWinner = ""
		st.pendingWins = 0
	} else if st.pendingWinner != "" && !resolverRankedContainsCall(ranked, st.pendingWinner) {
		st.pendingWinner = ""
		st.pendingWins = 0
	}

	sort.Slice(ranked, func(i, j int) bool {
		return resolverCandidateRanksAhead(ranked[i], ranked[j])
	})
	applyResolverConfusionTieBreak(ranked, st.key.Mode, r.cfg.ConfusionModel, r.cfg.ConfusionWeight, &r.confusionWorkspace)

	top := ranked[0]
	hasRunner := len(ranked) > 1
	runner := rankedResolverCandidate{}
	if hasRunner {
		runner = ranked[1]
	}

	totalReporters := len(st.reporterRefs)
	if totalReporters <= 0 {
		// Defensive fallback for externally-constructed test states that do not
		// initialize reporterRefs.
		totalReporters = totalUniqueResolverReporters(ranked)
	}
	totalWeightedSupportMilli := totalResolverReporterWeightMilli(st.reporterRefs)
	if totalWeightedSupportMilli <= 0 {
		totalWeightedSupportMilli = totalUniqueResolverReporterWeightMilli(ranked)
	}
	provisionalState := classifyResolverState(top.weightedSupportMilli, totalWeightedSupportMilli)
	provisionalWinner := top.call
	margin := top.support
	weightedMargin := top.weightedSupportMilli
	confusionTieMargin := 0.0
	if hasRunner {
		margin = top.support - runner.support
		weightedMargin = top.weightedSupportMilli - runner.weightedSupportMilli
		if weightedMargin == 0 && r.cfg.ConfusionWeight > 0 && top.confusionScore > runner.confusionScore {
			confusionTieMargin = top.confusionScore - runner.confusionScore
		}
	}
	candidateRanksScratch := st.candidateRanksScratch[:0]
	for i := range ranked {
		candidate := &ranked[i]
		candidateRanksScratch = append(candidateRanksScratch, ResolverCandidateSupport{
			Call:                 candidate.call,
			Support:              candidate.support,
			WeightedSupportMilli: candidate.weightedSupportMilli,
		})
	}
	candidateRanks := st.publishedCandidateRanks
	if !resolverCandidateSupportSlicesEqual(candidateRanks, candidateRanksScratch) {
		candidateRanks = append(make([]ResolverCandidateSupport, 0, len(candidateRanksScratch)), candidateRanksScratch...)
		st.publishedCandidateRanks = candidateRanks
	}
	st.candidateRanksScratch = candidateRanksScratch[:0]

	split := false
	if hasRunner {
		winnerIdentity := top.identity
		runnerIdentity := runner.identity
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
		snapshot.WinnerWeightedSupportMilli = top.weightedSupportMilli
		snapshot.RunnerWeightedSupportMilli = runner.weightedSupportMilli
		snapshot.TotalWeightedSupportMilli = totalWeightedSupportMilli
		snapshot.CandidateRanks = candidateRanks
		st.rankedScratch = ranked[:0]
		r.publishSnapshot(st.key, snapshot)
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
	} else if weightedMargin > 0 || confusionTieMargin > 0 {
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
	winnerWeightedSupportMilli := weightedSupportForResolverCall(ranked, publishedWinner)
	runnerCall, runnerSupport, runnerWeightedSupportMilli := runnerForResolverCall(ranked, publishedWinner)
	if winnerSupport == 0 {
		winnerSupport = top.support
		winnerWeightedSupportMilli = top.weightedSupportMilli
		publishedWinner = top.call
		publishedState = provisionalState
		st.stableWinner = top.call
		st.pendingWinner = ""
		st.pendingWins = 0
		runnerCall, runnerSupport, runnerWeightedSupportMilli = runnerForResolverCall(ranked, publishedWinner)
	}

	snapshot.State = publishedState
	snapshot.Winner = publishedWinner
	snapshot.RunnerUp = runnerCall
	snapshot.WinnerSupport = winnerSupport
	snapshot.RunnerSupport = runnerSupport
	snapshot.Margin = winnerSupport - runnerSupport
	snapshot.TotalReporters = totalReporters
	snapshot.WinnerWeightedSupportMilli = winnerWeightedSupportMilli
	snapshot.RunnerWeightedSupportMilli = runnerWeightedSupportMilli
	snapshot.TotalWeightedSupportMilli = totalWeightedSupportMilli
	snapshot.CandidateRanks = candidateRanks
	st.rankedScratch = ranked[:0]
	r.publishSnapshot(st.key, snapshot)
}

func (r *SignalResolver) publishSnapshot(key ResolverSignalKey, snapshot ResolverSnapshot) {
	if r == nil {
		return
	}
	cell := r.snapshotCell(key)
	published := new(ResolverSnapshot)
	*published = snapshot
	cell.snapshot.Store(published)
}

func (r *SignalResolver) snapshotCell(key ResolverSignalKey) *resolverSnapshotCell {
	if r == nil {
		return nil
	}
	if value, ok := r.snapshots.Load(key); ok {
		if cell, valid := value.(*resolverSnapshotCell); valid && cell != nil {
			return cell
		}
	}
	cell := &resolverSnapshotCell{}
	actual, _ := r.snapshots.LoadOrStore(key, cell)
	if existing, ok := actual.(*resolverSnapshotCell); ok && existing != nil {
		return existing
	}
	return cell
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
	if cfg.MinSpotterReliability < 0 {
		cfg.MinSpotterReliability = 0
	}
	if cfg.MinSpotterReliability > 1 {
		cfg.MinSpotterReliability = 1
	}
	if cfg.ConfusionWeight < 0 {
		cfg.ConfusionWeight = 0
	}
	cfg.minSpotterReliabilityMilli = reliabilityToMilli(cfg.MinSpotterReliability)
	cfg.FamilyPolicy = normalizeCorrectionFamilyPolicy(cfg.FamilyPolicy)
	return cfg
}

func normalizeResolverEvidence(e ResolverEvidence) (ResolverEvidence, bool) {
	call := NormalizeSpotDXCallsign(e.DXCall)
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
		Report:        e.Report,
		FrequencyKHz:  e.FrequencyKHz,
		RecencyWindow: window,
	}, true
}

func applyResolverConfusionTieBreak(ranked []rankedResolverCandidate, mode string, model *ConfusionModel, weight float64, workspace *confusionAlignmentWorkspace) {
	if len(ranked) < 2 || model == nil || weight <= 0 {
		return
	}
	tieEnd := 1
	top := ranked[0]
	for tieEnd < len(ranked) {
		next := ranked[tieEnd]
		if next.weightedSupportMilli != top.weightedSupportMilli || next.support != top.support {
			break
		}
		tieEnd++
	}
	if tieEnd <= 1 {
		return
	}
	cohort := ranked[:tieEnd]
	for i := range cohort {
		score := resolverConfusionAggregateScore(cohort[i], cohort, mode, model, workspace)
		if math.IsNaN(score) || math.IsInf(score, 0) {
			score = 0
		}
		cohort[i].confusionScore = score
	}
	sort.Slice(cohort, func(i, j int) bool {
		leftScore := float64(cohort[i].weightedSupportMilli) + weight*cohort[i].confusionScore
		rightScore := float64(cohort[j].weightedSupportMilli) + weight*cohort[j].confusionScore
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		if cohort[i].confusionScore != cohort[j].confusionScore {
			return cohort[i].confusionScore > cohort[j].confusionScore
		}
		return resolverCandidateRanksAhead(cohort[i], cohort[j])
	})
}

func resolverConfusionAggregateScore(
	candidate rankedResolverCandidate,
	cohort []rankedResolverCandidate,
	mode string,
	model *ConfusionModel,
	workspace *confusionAlignmentWorkspace,
) float64 {
	if model == nil || len(cohort) <= 1 {
		return 0
	}
	score := 0.0
	for i := range cohort {
		observed := &cohort[i]
		if strings.EqualFold(observed.call, candidate.call) {
			continue
		}
		weight := float64(observed.weightedSupportMilli)
		if weight <= 0 {
			weight = 1
		}
		score += model.scorePreparedCandidate(observed.callRunes, candidate.callRunes, mode, float64(observed.lastReport), workspace) * weight
	}
	return score
}

func classifyResolverState(weightedSupportMilli int, totalWeightedSupportMilli int) ResolverState {
	if weightedSupportMilli <= 0 || totalWeightedSupportMilli <= 0 {
		return ResolverStateUncertain
	}
	confidence := weightedSupportMilli * 100 / totalWeightedSupportMilli
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
	for i := range candidates {
		candidate := &candidates[i]
		for reporter := range candidate.reporters {
			seen[reporter] = struct{}{}
		}
	}
	return len(seen)
}

func resolverCandidateSupportSlicesEqual(existing []ResolverCandidateSupport, ranked []ResolverCandidateSupport) bool {
	if len(existing) != len(ranked) {
		return false
	}
	for i := range existing {
		if existing[i] != ranked[i] {
			return false
		}
	}
	return true
}

func resolverCandidateRanksAhead(left, right rankedResolverCandidate) bool {
	if left.weightedSupportMilli != right.weightedSupportMilli {
		return left.weightedSupportMilli > right.weightedSupportMilli
	}
	if left.support != right.support {
		return left.support > right.support
	}
	if !left.lastSeen.Equal(right.lastSeen) {
		return left.lastSeen.After(right.lastSeen)
	}
	return left.call < right.call
}

func supportForResolverCall(candidates []rankedResolverCandidate, call string) int {
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.call == call {
			return candidate.support
		}
	}
	return 0
}

func weightedSupportForResolverCall(candidates []rankedResolverCandidate, call string) int {
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.call == call {
			return candidate.weightedSupportMilli
		}
	}
	return 0
}

func runnerForResolverCall(candidates []rankedResolverCandidate, winner string) (string, int, int) {
	var (
		runner rankedResolverCandidate
		found  bool
	)
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.call == winner {
			continue
		}
		if !found || resolverCandidateRanksAhead(*candidate, runner) {
			runner = *candidate
			found = true
		}
	}
	if found {
		return runner.call, runner.support, runner.weightedSupportMilli
	}
	return "", 0, 0
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

func resolverSpotterWeightMilli(cfg SignalResolverConfig, mode string, reporter string) (int, bool) {
	weight := reliabilityForMode(cfg.SpotterReliability, cfg.SpotterReliabilityCW, cfg.SpotterReliabilityRTTY, mode, reporter)
	weightMilli := reliabilityToMilli(weight)
	if weightMilli < cfg.minSpotterReliabilityMilli {
		return 0, false
	}
	return weightMilli, true
}

func reliabilityToMilli(weight float64) int {
	switch {
	case weight <= 0:
		return 0
	case weight >= 1:
		return resolverReliabilityScale
	default:
		return int(math.Round(weight * float64(resolverReliabilityScale)))
	}
}

func totalResolverReporterWeightMilli(refs map[string]resolverReporterRef) int {
	if len(refs) == 0 {
		return 0
	}
	total := 0
	for _, ref := range refs {
		if ref.refCount <= 0 {
			continue
		}
		if ref.weightMilli <= 0 {
			continue
		}
		total += ref.weightMilli
	}
	return total
}

func totalUniqueResolverReporterWeightMilli(candidates []rankedResolverCandidate) int {
	if len(candidates) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, 64)
	total := 0
	for i := range candidates {
		candidate := &candidates[i]
		for reporter := range candidate.reporters {
			if _, ok := seen[reporter]; ok {
				continue
			}
			seen[reporter] = struct{}{}
			total += resolverReliabilityScale
		}
	}
	return total
}

func weightedSupportForResolverCandidate(candidate *resolverCandidate, refs map[string]resolverReporterRef) int {
	if candidate == nil || len(candidate.reporters) == 0 {
		return 0
	}
	total := 0
	for reporter := range candidate.reporters {
		ref, ok := refs[reporter]
		if ok {
			total += ref.weightMilli
			continue
		}
		total += resolverReliabilityScale
	}
	return total
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
