// File role: Pure causal space-weather ring/decay state and lock-free snapshot
// store evaluated at query time now.
// Crawler notes: Owns fixed-capacity completed-bin ring buffers for Hp30 (30m),
// OMNI2/RTSW hourly Bz_gsm/Vsw/Dst (1h), and daily observed SFI (24h). Writers
// serialize under sync.Mutex with copy-on-write publication into atomic.Pointer;
// readers perform zero locks and zero allocations.
package propprior

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	hp30StepSec  int64 = 1800
	omniStepSec  int64 = 3600
	sfiStepSec   int64 = 86400
	hp30RingSize       = 48 // 24 hours of 30-min completed bins.
	omniRingSize       = 24 // 24 hours of 1-hour completed bins.
	sfiRingSize        = 4  // 4 daily UTC bins (covers 3-day trailing window + 1-day lag).

	hp30FFillBins int64 = 12 // 6.0 hours of 30-min bins.
	omniFFillBins int64 = 6  // 6.0 hours of 1-hour bins.

	// Quiet defaults matching hf-prop-lab/src/config.py verbatim.
	QuietHp30    = 2.0
	QuietBzGSMNT = 0.0
	QuietVswKMS  = 400.0
	QuietDstNT   = 0.0

	// Default SFI outage policy for live serving beyond training's 72h forward-fill.
	DefaultSFIQuiet           = 120.0
	DefaultSFIForwardFillDays = 3
	DefaultSFIDecayHalfLifeD  = 7.0
)

var (
	hp30StepDecay = math.Pow(0.5, 0.5/6.0)
	omniStepDecay = math.Pow(0.5, 1.0/6.0)

	// ErrInvalidSFIPolicy is returned when SFIPolicy parameters are invalid.
	ErrInvalidSFIPolicy = errors.New("propprior: invalid SFI outage policy")
)

// SFIPolicy defines the daily observed F10.7_obs quiet baseline, forward-fill
// horizon (days), and post-forward-fill exponential decay half-life (days).
type SFIPolicy struct {
	QuietDefault      float64
	ForwardFillDays   int
	DecayHalfLifeDays float64
}

// DefaultSFIPolicy returns the default daily SFI outage policy (120 SFU quiet
// baseline, 3-day / 72h forward-fill, 7-day / 168h slow decay half-life).
func DefaultSFIPolicy() SFIPolicy {
	return SFIPolicy{
		QuietDefault:      DefaultSFIQuiet,
		ForwardFillDays:   DefaultSFIForwardFillDays,
		DecayHalfLifeDays: DefaultSFIDecayHalfLifeD,
	}
}

// Validate checks that SFIPolicy parameters are finite and in range.
func (p SFIPolicy) Validate() error {
	if !isFinite64(p.QuietDefault) || p.QuietDefault <= 0 {
		return ErrInvalidSFIPolicy
	}
	if p.ForwardFillDays < 0 {
		return ErrInvalidSFIPolicy
	}
	if !isFinite64(p.DecayHalfLifeDays) || p.DecayHalfLifeDays <= 0 {
		return ErrInvalidSFIPolicy
	}
	return nil
}

// FeedDiagnostics reports per-feed causal freshness metadata (not fed to the MLP).
type FeedDiagnostics struct {
	HasValid     bool
	LastValidUTC time.Time
	GapHours     float64
	Source       string
}

// SpaceWeatherSnapshot is the immutable, strictly causal feature bundle
// evaluated at a specific query timestamp now.
type SpaceWeatherSnapshot struct {
	EvaluatedAtUTC time.Time
	Generation     uint64
	Features       SpaceWeatherFeatures
	SFIDelta       float64
	DstRecovery    float64
	SFI            FeedDiagnostics
	Hp30           FeedDiagnostics
	Bz             FeedDiagnostics
	Vsw            FeedDiagnostics
	Dst            FeedDiagnostics
}

type ring48 struct {
	initialized  bool
	headBin      int64
	lastValidBin int64
	anchorHasObs bool
	anchorVal    float64
	anchorGap    int64
	source       string
	raw          [hp30RingSize]float64
	valid        [hp30RingSize]bool
}

type ring24 struct {
	initialized  bool
	headBin      int64
	lastValidBin int64
	anchorHasObs bool
	anchorVal    float64
	anchorGap    int64
	source       string
	raw          [omniRingSize]float64
	valid        [omniRingSize]bool
}

type ring4 struct {
	initialized  bool
	headBin      int64
	lastValidBin int64
	anchorHasObs bool
	anchorVal    float64
	anchorGap    int64
	source       string
	raw          [sfiRingSize]float64
	valid        [sfiRingSize]bool
}

// weatherState is the fixed-size immutable snapshot published into Store.state.
// Its retained memory is strictly bounded (~1.5 KB, zero maps or slices).
type weatherState struct {
	gen       uint64
	sfiPolicy SFIPolicy
	sfiDecay  float64
	hp30      ring48
	bz        ring24
	vsw       ring24
	dst       ring24
	sfi       ring4
}

// SpaceWeatherStore holds concurrent-safe, bounded causal space-weather ring
// state. Writers serialize under mu and publish copy-on-write snapshots into
// atomic.Pointer; Evaluate reads lock-free with zero allocations.
type SpaceWeatherStore struct {
	mu    sync.Mutex
	state atomic.Pointer[weatherState]
}

// NewSpaceWeatherStore constructs a SpaceWeatherStore using DefaultSFIPolicy.
func NewSpaceWeatherStore() *SpaceWeatherStore {
	return newSpaceWeatherStore(DefaultSFIPolicy())
}

// NewSpaceWeatherStoreWithPolicy constructs a SpaceWeatherStore with an
// explicit SFI outage policy.
func NewSpaceWeatherStoreWithPolicy(policy SFIPolicy) (*SpaceWeatherStore, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return newSpaceWeatherStore(policy), nil
}

func newSpaceWeatherStore(policy SFIPolicy) *SpaceWeatherStore {
	st := &SpaceWeatherStore{}
	initState := &weatherState{
		sfiPolicy: policy,
		sfiDecay:  math.Pow(0.5, 1.0/policy.DecayHalfLifeDays),
	}
	initState.hp30.resetAnchor(QuietHp30, hp30FFillBins)
	initState.bz.resetAnchor(QuietBzGSMNT)
	initState.vsw.resetAnchor(QuietVswKMS)
	initState.dst.resetAnchor(QuietDstNT)
	initState.sfi.resetAnchor(policy.QuietDefault, int64(policy.ForwardFillDays))
	st.state.Store(initState)
	return st
}

// Sample represents one bin-aligned space-weather observation for atomic batch
// ingestion into SpaceWeatherStore.
type Sample struct {
	BinStartUTC time.Time
	Value       float64
}

func validHp30Sample(ts time.Time, v float64) bool {
	return !ts.IsZero() && isFinite64(v) && v >= 0 && v < 99.0
}

func validBzGSMSample(ts time.Time, v float64) bool {
	return !ts.IsZero() && isFinite64(v) && math.Abs(v) < 900.0
}

func validVswSample(ts time.Time, v float64) bool {
	return !ts.IsZero() && isFinite64(v) && v >= 100.0 && v < 9000.0
}

func validDstSample(ts time.Time, v float64) bool {
	return !ts.IsZero() && isFinite64(v) && math.Abs(v) < 9000.0
}

func validSFISample(ts time.Time, v float64) bool {
	return !ts.IsZero() && isFinite64(v) && v > 0
}

// RecordHp30 ingests one 30-minute Hp30 observation at binStartUTC. Invalid or
// fill values (outside [0, 99) or non-finite) or observations older than the
// retained 24-hour ring window are ignored and return false.
func (s *SpaceWeatherStore) RecordHp30(binStartUTC time.Time, hp30 float64, source string) bool {
	if s == nil || !validHp30Sample(binStartUTC, hp30) {
		return false
	}
	bin := floorDiv(binStartUTC.UTC().Unix(), hp30StepSec)
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	if !next.hp30.record(bin, hp30, source, QuietHp30, hp30FFillBins, hp30StepDecay) {
		return false
	}
	s.publishStateLocked(next)
	return true
}

// RecordHp30Batch atomically ingests a slice of 30-minute Hp30 observations in
// order, publishing at most one copy-on-write snapshot. Returns accepted count.
func (s *SpaceWeatherStore) RecordHp30Batch(samples []Sample, source string) int {
	if s == nil || len(samples) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	accepted := 0
	for _, sample := range samples {
		if !validHp30Sample(sample.BinStartUTC, sample.Value) {
			continue
		}
		bin := floorDiv(sample.BinStartUTC.UTC().Unix(), hp30StepSec)
		if next.hp30.record(bin, sample.Value, source, QuietHp30, hp30FFillBins, hp30StepDecay) {
			accepted++
		}
	}
	if accepted > 0 {
		s.publishStateLocked(next)
	}
	return accepted
}

// RecordBzGSM ingests one hourly signed Bz_gsm (nT) observation at binStartUTC.
// Fill values (|bz| >= 900 or non-finite) are ignored and return false.
func (s *SpaceWeatherStore) RecordBzGSM(binStartUTC time.Time, bzGSM float64, source string) bool {
	if s == nil || !validBzGSMSample(binStartUTC, bzGSM) {
		return false
	}
	bin := floorDiv(binStartUTC.UTC().Unix(), omniStepSec)
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	if !next.bz.record(bin, bzGSM, source, QuietBzGSMNT) {
		return false
	}
	s.publishStateLocked(next)
	return true
}

// RecordBzGSMBatch atomically ingests a slice of hourly signed Bz_gsm
// observations in order, publishing at most one copy-on-write snapshot.
func (s *SpaceWeatherStore) RecordBzGSMBatch(samples []Sample, source string) int {
	if s == nil || len(samples) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	accepted := 0
	for _, sample := range samples {
		if !validBzGSMSample(sample.BinStartUTC, sample.Value) {
			continue
		}
		bin := floorDiv(sample.BinStartUTC.UTC().Unix(), omniStepSec)
		if next.bz.record(bin, sample.Value, source, QuietBzGSMNT) {
			accepted++
		}
	}
	if accepted > 0 {
		s.publishStateLocked(next)
	}
	return accepted
}

// RecordVsw ingests one hourly solar-wind speed (km/s) observation at binStartUTC.
// Fill values (outside [100, 9000) or non-finite) are ignored and return false.
func (s *SpaceWeatherStore) RecordVsw(binStartUTC time.Time, vswKMS float64, source string) bool {
	if s == nil || !validVswSample(binStartUTC, vswKMS) {
		return false
	}
	bin := floorDiv(binStartUTC.UTC().Unix(), omniStepSec)
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	if !next.vsw.record(bin, vswKMS, source, QuietVswKMS) {
		return false
	}
	s.publishStateLocked(next)
	return true
}

// RecordVswBatch atomically ingests a slice of hourly solar-wind speed
// observations in order, publishing at most one copy-on-write snapshot.
func (s *SpaceWeatherStore) RecordVswBatch(samples []Sample, source string) int {
	if s == nil || len(samples) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	accepted := 0
	for _, sample := range samples {
		if !validVswSample(sample.BinStartUTC, sample.Value) {
			continue
		}
		bin := floorDiv(sample.BinStartUTC.UTC().Unix(), omniStepSec)
		if next.vsw.record(bin, sample.Value, source, QuietVswKMS) {
			accepted++
		}
	}
	if accepted > 0 {
		s.publishStateLocked(next)
	}
	return accepted
}

// RecordDst ingests one hourly Dst (nT) observation at binStartUTC. Fill values
// (|dst| >= 9000 or non-finite) are ignored and return false.
func (s *SpaceWeatherStore) RecordDst(binStartUTC time.Time, dstNT float64, source string) bool {
	if s == nil || !validDstSample(binStartUTC, dstNT) {
		return false
	}
	bin := floorDiv(binStartUTC.UTC().Unix(), omniStepSec)
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	if !next.dst.record(bin, dstNT, source, QuietDstNT) {
		return false
	}
	s.publishStateLocked(next)
	return true
}

// RecordDstBatch atomically ingests a slice of hourly Dst observations in
// order, publishing at most one copy-on-write snapshot.
func (s *SpaceWeatherStore) RecordDstBatch(samples []Sample, source string) int {
	if s == nil || len(samples) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	accepted := 0
	for _, sample := range samples {
		if !validDstSample(sample.BinStartUTC, sample.Value) {
			continue
		}
		bin := floorDiv(sample.BinStartUTC.UTC().Unix(), omniStepSec)
		if next.dst.record(bin, sample.Value, source, QuietDstNT) {
			accepted++
		}
	}
	if accepted > 0 {
		s.publishStateLocked(next)
	}
	return accepted
}

// RecordSFI ingests one daily observed F10.7_obs (SFU) value for the UTC day
// containing dayUTC. Non-positive or non-finite values are ignored and return false.
func (s *SpaceWeatherStore) RecordSFI(dayUTC time.Time, f107Obs float64, source string) bool {
	if s == nil || !validSFISample(dayUTC, f107Obs) {
		return false
	}
	bin := floorDiv(dayUTC.UTC().Unix(), sfiStepSec)
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	if !next.sfi.record(bin, f107Obs, source, next.sfiPolicy.QuietDefault, int64(next.sfiPolicy.ForwardFillDays), next.sfiDecay) {
		return false
	}
	s.publishStateLocked(next)
	return true
}

// RecordSFIBatch atomically ingests a slice of daily observed F10.7_obs values
// in order, publishing at most one copy-on-write snapshot.
func (s *SpaceWeatherStore) RecordSFIBatch(samples []Sample, source string) int {
	if s == nil || len(samples) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cloneStateLocked()
	accepted := 0
	for _, sample := range samples {
		if !validSFISample(sample.BinStartUTC, sample.Value) {
			continue
		}
		bin := floorDiv(sample.BinStartUTC.UTC().Unix(), sfiStepSec)
		if next.sfi.record(bin, sample.Value, source, next.sfiPolicy.QuietDefault, int64(next.sfiPolicy.ForwardFillDays), next.sfiDecay) {
			accepted++
		}
	}
	if accepted > 0 {
		s.publishStateLocked(next)
	}
	return accepted
}

// Generation returns the current monotonic state publication counter. It is
// lock-free, zero-allocation, and advances by 1 on every accepted Record* or
// Record*Batch swap.
func (s *SpaceWeatherStore) Generation() uint64 {
	if s == nil {
		return 0
	}
	st := s.state.Load()
	if st == nil {
		return 0
	}
	return st.gen
}

// Evaluate computes the strictly causal SpaceWeatherSnapshot at now using only
// completed bins (< floor(now / step)). Lock-free, O(1) bounded work regardless
// of outage duration, and zero heap allocations.
func (s *SpaceWeatherStore) Evaluate(now time.Time) SpaceWeatherSnapshot {
	utc := now.UTC()
	var st *weatherState
	if s != nil {
		st = s.state.Load()
	}
	if st == nil {
		return quietSnapshot(utc, DefaultSFIPolicy())
	}

	unixSec := utc.Unix()
	hpLag1, hp12Max, hp24Mean, hpDiag := evalHp30(&st.hp30, floorDiv(unixSec, hp30StepSec)-1)
	bzSouth3h, bzDiag := evalBzSouth3h(&st.bz, floorDiv(unixSec, omniStepSec)-1)
	vsw3h, vswDiag := evalVsw3h(&st.vsw, floorDiv(unixSec, omniStepSec)-1)
	dstNow, dst24Min, dstDiag := evalDst(&st.dst, floorDiv(unixSec, omniStepSec)-1)
	sfiNow, sfi72Mean, sfiDiag := evalSFI(&st.sfi, floorDiv(unixSec, sfiStepSec)-1, st.sfiPolicy, st.sfiDecay)

	dstRecovery := dstNow - dst24Min
	if dstRecovery < 0 {
		dstRecovery = 0
	}

	return SpaceWeatherSnapshot{
		EvaluatedAtUTC: utc,
		Generation:     st.gen,
		Features: SpaceWeatherFeatures{
			SFINow:      sfiNow,
			SFI72hMean:  sfi72Mean,
			Hp30Lag1:    hpLag1,
			Hp3012hMax:  hp12Max,
			Hp3024hMean: hp24Mean,
			BzSouth3h:   bzSouth3h,
			Vsw3h:       vsw3h,
			DstNow:      dstNow,
			Dst24hMin:   dst24Min,
		},
		SFIDelta:    sfiNow - sfi72Mean,
		DstRecovery: dstRecovery,
		SFI:         sfiDiag,
		Hp30:        hpDiag,
		Bz:          bzDiag,
		Vsw:         vswDiag,
		Dst:         dstDiag,
	}
}

func (s *SpaceWeatherStore) publishStateLocked(next *weatherState) {
	next.gen++
	s.state.Store(next)
}

func (s *SpaceWeatherStore) cloneStateLocked() *weatherState {
	cur := s.state.Load()
	if cur == nil {
		policy := DefaultSFIPolicy()
		cp := &weatherState{
			sfiPolicy: policy,
			sfiDecay:  math.Pow(0.5, 1.0/policy.DecayHalfLifeDays),
		}
		cp.hp30.resetAnchor(QuietHp30, hp30FFillBins)
		cp.bz.resetAnchor(QuietBzGSMNT)
		cp.vsw.resetAnchor(QuietVswKMS)
		cp.dst.resetAnchor(QuietDstNT)
		cp.sfi.resetAnchor(policy.QuietDefault, int64(policy.ForwardFillDays))
		return cp
	}
	cp := *cur
	return &cp
}

func quietSnapshot(utc time.Time, policy SFIPolicy) SpaceWeatherSnapshot {
	return SpaceWeatherSnapshot{
		EvaluatedAtUTC: utc,
		Features: SpaceWeatherFeatures{
			SFINow:      policy.QuietDefault,
			SFI72hMean:  policy.QuietDefault,
			Hp30Lag1:    QuietHp30,
			Hp3012hMax:  QuietHp30,
			Hp3024hMean: QuietHp30,
			BzSouth3h:   0,
			Vsw3h:       QuietVswKMS,
			DstNow:      QuietDstNT,
			Dst24hMin:   QuietDstNT,
		},
	}
}

func evalHp30(r *ring48, targetEnd int64) (lag1, max12h, mean24h float64, diag FeedDiagnostics) {
	var vals [hp30RingSize]float64
	var obs [hp30RingSize]bool
	evalWindow(
		r.initialized, r.headBin, r.lastValidBin, r.anchorHasObs, r.anchorVal, r.anchorGap, r.source,
		r.raw[:], r.valid[:], targetEnd, hp30StepSec, QuietHp30, hp30FFillBins, hp30StepDecay,
		vals[:], obs[:], &diag,
	)
	lag1 = vals[hp30RingSize-1]
	max12h = vals[hp30RingSize-24]
	sum24 := 0.0
	for i := range hp30RingSize {
		sum24 += vals[i]
		if i >= hp30RingSize-24 && vals[i] > max12h {
			max12h = vals[i]
		}
	}
	mean24h = sum24 / float64(hp30RingSize)
	return lag1, max12h, mean24h, diag
}

func evalBzSouth3h(r *ring24, targetEnd int64) (float64, FeedDiagnostics) {
	var vals [3]float64
	var obs [3]bool
	var diag FeedDiagnostics
	evalWindow(
		r.initialized, r.headBin, r.lastValidBin, r.anchorHasObs, r.anchorVal, r.anchorGap, r.source,
		r.raw[:], r.valid[:], targetEnd, omniStepSec, QuietBzGSMNT, omniFFillBins, omniStepDecay,
		vals[:], obs[:], &diag,
	)
	sum := 0.0
	for i := range 3 {
		// Mirror 01_fetch_space_weather_v4.py: gap-fill signed Bz_gsm first, then rectify.
		if vals[i] < 0 {
			sum += -vals[i]
		}
	}
	return sum / 3.0, diag
}

func evalVsw3h(r *ring24, targetEnd int64) (float64, FeedDiagnostics) {
	var vals [3]float64
	var obs [3]bool
	var diag FeedDiagnostics
	evalWindow(
		r.initialized, r.headBin, r.lastValidBin, r.anchorHasObs, r.anchorVal, r.anchorGap, r.source,
		r.raw[:], r.valid[:], targetEnd, omniStepSec, QuietVswKMS, omniFFillBins, omniStepDecay,
		vals[:], obs[:], &diag,
	)
	return (vals[0] + vals[1] + vals[2]) / 3.0, diag
}

func evalDst(r *ring24, targetEnd int64) (dstNow, dst24Min float64, diag FeedDiagnostics) {
	var vals [omniRingSize]float64
	var obs [omniRingSize]bool
	evalWindow(
		r.initialized, r.headBin, r.lastValidBin, r.anchorHasObs, r.anchorVal, r.anchorGap, r.source,
		r.raw[:], r.valid[:], targetEnd, omniStepSec, QuietDstNT, omniFFillBins, omniStepDecay,
		vals[:], obs[:], &diag,
	)
	dstNow = vals[omniRingSize-1]
	dst24Min = vals[0]
	for i := 1; i < omniRingSize; i++ {
		if vals[i] < dst24Min {
			dst24Min = vals[i]
		}
	}
	return dstNow, dst24Min, diag
}

func evalSFI(r *ring4, targetEnd int64, policy SFIPolicy, stepDecay float64) (sfiNow, sfi72Mean float64, diag FeedDiagnostics) {
	var vals [3]float64
	var obs [3]bool
	evalWindow(
		r.initialized, r.headBin, r.lastValidBin, r.anchorHasObs, r.anchorVal, r.anchorGap, r.source,
		r.raw[:], r.valid[:], targetEnd, sfiStepSec, policy.QuietDefault, int64(policy.ForwardFillDays), stepDecay,
		vals[:], obs[:], &diag,
	)
	// Match 01_fetch_data.py cold-start rolling_mean(window=3, min_samples=1):
	// never pad unobserved leading days with 120.0 (which would synthesize a flare delta).
	sum := 0.0
	count := 0
	for i := range 3 {
		if obs[i] {
			sum += vals[i]
			count++
		}
	}
	if count == 0 {
		return policy.QuietDefault, policy.QuietDefault, diag
	}
	return vals[2], sum / float64(count), diag
}

func evalWindow(
	initialized bool,
	headBin, lastValidBin int64,
	anchorHasObs bool,
	anchorVal float64,
	anchorGap int64,
	source string,
	raw []float64,
	valid []bool,
	targetEnd, stepSec int64,
	quietDefault float64,
	ffillBins int64,
	stepDecay float64,
	outVals []float64,
	outObs []bool,
	diag *FeedDiagnostics,
) {
	w := int64(len(raw))
	k := int64(len(outVals))
	for i := range outVals {
		outVals[i] = quietDefault
		outObs[i] = false
	}
	if !initialized {
		return
	}

	baseBin := headBin - w + 1
	// Guard historical queries strictly older than the retained ring window so
	// slot indexing can never alias newer bins.
	if targetEnd < baseBin {
		return
	}

	winStart := targetEnd - k + 1
	val := anchorVal
	gap := anchorGap
	hasObs := anchorHasObs

	ringLimit := headBin
	if targetEnd < ringLimit {
		ringLimit = targetEnd
	}
	lastSeenValid := int64(0)
	hasSeenValid := false
	if anchorHasObs {
		lastSeenValid = baseBin - 1 - anchorGap
		hasSeenValid = true
	}

	for b := baseBin; b <= ringLimit; b++ {
		slot := ringSlot(b, w)
		if valid[slot] {
			val = raw[slot]
			gap = 0
			hasObs = true
			lastSeenValid = b
			hasSeenValid = true
		} else if hasObs {
			val, gap = advanceGap(val, gap, 1, quietDefault, ffillBins, stepDecay)
		}
		if b >= winStart && b <= targetEnd {
			idx := int(b - winStart)
			outVals[idx] = val
			outObs[idx] = hasObs
		}
	}

	if targetEnd > headBin {
		extra := targetEnd - headBin
		stepStart := headBin + 1
		if extra > k {
			skip := extra - k
			if hasObs {
				val, gap = advanceGap(val, gap, skip, quietDefault, ffillBins, stepDecay)
			}
			stepStart = winStart
		}
		for b := stepStart; b <= targetEnd; b++ {
			if hasObs {
				val, gap = advanceGap(val, gap, 1, quietDefault, ffillBins, stepDecay)
			}
			idx := int(b - winStart)
			outVals[idx] = val
			outObs[idx] = hasObs
		}
	}

	if hasSeenValid {
		// Use lastValidBin when it is not ahead of targetEnd; otherwise use the
		// causal last valid bin <= targetEnd.
		effLast := lastSeenValid
		if targetEnd >= lastValidBin && lastValidBin >= baseBin {
			effLast = lastValidBin
		}
		gapBins := targetEnd - effLast
		if gapBins < 0 {
			gapBins = 0
		}
		*diag = FeedDiagnostics{
			HasValid:     true,
			LastValidUTC: time.Unix(effLast*stepSec, 0).UTC(),
			GapHours:     float64(gapBins*stepSec) / 3600.0,
			Source:       source,
		}
	}
}

func (r *ring48) resetAnchor(quietDefault float64, ffillBins int64) {
	r.anchorHasObs = false
	r.anchorVal = quietDefault
	r.anchorGap = ffillBins + 1
}

func (r *ring24) resetAnchor(quietDefault float64) {
	r.anchorHasObs = false
	r.anchorVal = quietDefault
	r.anchorGap = omniFFillBins + 1
}

func (r *ring4) resetAnchor(quietDefault float64, ffillBins int64) {
	r.anchorHasObs = false
	r.anchorVal = quietDefault
	r.anchorGap = ffillBins + 1
}

func (r *ring48) record(bin int64, val float64, source string, quietDefault float64, ffillBins int64, stepDecay float64) bool {
	return recordRing(
		&r.initialized, &r.headBin, &r.lastValidBin, &r.anchorHasObs, &r.anchorVal, &r.anchorGap, &r.source,
		r.raw[:], r.valid[:], bin, val, source, quietDefault, ffillBins, stepDecay,
	)
}

func (r *ring24) record(bin int64, val float64, source string, quietDefault float64) bool {
	return recordRing(
		&r.initialized, &r.headBin, &r.lastValidBin, &r.anchorHasObs, &r.anchorVal, &r.anchorGap, &r.source,
		r.raw[:], r.valid[:], bin, val, source, quietDefault, omniFFillBins, omniStepDecay,
	)
}

func (r *ring4) record(bin int64, val float64, source string, quietDefault float64, ffillBins int64, stepDecay float64) bool {
	return recordRing(
		&r.initialized, &r.headBin, &r.lastValidBin, &r.anchorHasObs, &r.anchorVal, &r.anchorGap, &r.source,
		r.raw[:], r.valid[:], bin, val, source, quietDefault, ffillBins, stepDecay,
	)
}

func recordRing(
	initialized *bool,
	headBin, lastValidBin *int64,
	anchorHasObs *bool,
	anchorVal *float64,
	anchorGap *int64,
	storedSource *string,
	raw []float64,
	valid []bool,
	bin int64,
	val float64,
	source string,
	quietDefault float64,
	ffillBins int64,
	stepDecay float64,
) bool {
	w := int64(len(raw))
	if !*initialized {
		clear(valid)
		slot := ringSlot(bin, w)
		raw[slot] = val
		valid[slot] = true
		*initialized = true
		*headBin = bin
		*lastValidBin = bin
		*anchorHasObs = false
		*anchorVal = quietDefault
		*anchorGap = ffillBins + 1
		if source != "" {
			*storedSource = source
		}
		return true
	}

	if bin < *headBin-w+1 {
		return false
	}

	if bin > *headBin {
		shift := bin - *headBin
		oldBase := *headBin - w + 1
		if shift >= w {
			// Fold all currently retained bins into anchor, then jump O(1) across missing gap.
			for b := oldBase; b <= *headBin; b++ {
				slot := ringSlot(b, w)
				if valid[slot] {
					*anchorVal = raw[slot]
					*anchorGap = 0
					*anchorHasObs = true
				} else if *anchorHasObs {
					*anchorVal, *anchorGap = advanceGap(*anchorVal, *anchorGap, 1, quietDefault, ffillBins, stepDecay)
				}
			}
			missed := shift - w
			if missed > 0 && *anchorHasObs {
				*anchorVal, *anchorGap = advanceGap(*anchorVal, *anchorGap, missed, quietDefault, ffillBins, stepDecay)
			}
			clear(valid)
		} else {
			for i := range shift {
				oldBin := oldBase + i
				slot := ringSlot(oldBin, w)
				if valid[slot] {
					*anchorVal = raw[slot]
					*anchorGap = 0
					*anchorHasObs = true
				} else if *anchorHasObs {
					*anchorVal, *anchorGap = advanceGap(*anchorVal, *anchorGap, 1, quietDefault, ffillBins, stepDecay)
				}
				valid[slot] = false
			}
		}
		*headBin = bin
	}

	slot := ringSlot(bin, w)
	raw[slot] = val
	valid[slot] = true
	if bin >= *lastValidBin {
		*lastValidBin = bin
		if source != "" {
			*storedSource = source
		}
	}
	return true
}

func advanceGap(val float64, gap, count int64, quietDefault float64, ffillBins int64, stepDecay float64) (float64, int64) {
	if count <= 0 {
		return val, gap
	}
	var decaySteps int64
	if gap >= ffillBins {
		decaySteps = count
	} else if gap+count > ffillBins {
		decaySteps = gap + count - ffillBins
	}
	gap += count
	if decaySteps > 0 {
		val = quietDefault + math.Pow(stepDecay, float64(decaySteps))*(val-quietDefault)
	}
	return val, gap
}

func ringSlot(bin, size int64) int {
	m := bin % size
	if m < 0 {
		m += size
	}
	return int(m)
}

func floorDiv(a, b int64) int64 {
	q := a / b
	r := a % b
	if r != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
