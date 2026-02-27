package correctionflow

import (
	"sort"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

const temporalEmissionScale = 10000

// TemporalDecisionAction describes the decoder commit action for one request.
type TemporalDecisionAction string

const (
	TemporalDecisionActionDefer            TemporalDecisionAction = "defer"
	TemporalDecisionActionApply            TemporalDecisionAction = "apply"
	TemporalDecisionActionFallbackResolver TemporalDecisionAction = "fallback_resolver"
	TemporalDecisionActionAbstain          TemporalDecisionAction = "abstain"
	TemporalDecisionActionBypass           TemporalDecisionAction = "bypass"
)

// TemporalObservation captures one resolver-primary observation routed through
// fixed-lag temporal decoding.
type TemporalObservation struct {
	ID          uint64
	ObservedAt  time.Time
	Key         spot.ResolverSignalKey
	SubjectCall string
	Selection   ResolverPrimarySelection
}

// TemporalDecision is the decoder verdict for one observation id.
type TemporalDecision struct {
	ID             uint64
	Action         TemporalDecisionAction
	Winner         string
	Selection      ResolverPrimarySelection
	BestScore      int
	MarginScore    int
	CandidateCount int
	Reason         string
	PathSwitched   bool
	CommitLatency  time.Duration
}

type temporalRequest struct {
	id         uint64
	key        spot.ResolverSignalKey
	observedAt time.Time
	subject    string
	evicted    bool
}

type temporalEvent struct {
	id         uint64
	observedAt time.Time
	subject    string
	selection  ResolverPrimarySelection
}

type temporalKeyState struct {
	events []temporalEvent
}

// TemporalDecoder implements bounded fixed-lag winner decoding over resolver
// snapshots with deterministic beam search.
type TemporalDecoder struct {
	cfg               config.CallCorrectionTemporalDecoderConfig
	distanceModelCW   string
	distanceModelRTTY string
	familyPolicy      spot.CorrectionFamilyPolicy

	mu            sync.Mutex
	active        bool
	requests      map[uint64]temporalRequest
	keyStates     map[spot.ResolverSignalKey]*temporalKeyState
	lastCommitted map[spot.ResolverSignalKey]string
}

// NewTemporalDecoder builds a temporal decoder from call-correction config.
func NewTemporalDecoder(cfg config.CallCorrectionConfig) *TemporalDecoder {
	temporalCfg := cfg.TemporalDecoder
	return &TemporalDecoder{
		cfg:               temporalCfg,
		distanceModelCW:   cfg.DistanceModelCW,
		distanceModelRTTY: cfg.DistanceModelRTTY,
		familyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          cfg.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
		active:        temporalCfg.Enabled,
		requests:      make(map[uint64]temporalRequest, 64),
		keyStates:     make(map[spot.ResolverSignalKey]*temporalKeyState, 64),
		lastCommitted: make(map[spot.ResolverSignalKey]string, 64),
	}
}

// Enabled reports whether temporal decoding is active.
func (d *TemporalDecoder) Enabled() bool {
	if d == nil {
		return false
	}
	return d.active
}

// Scope returns temporal decoder routing scope.
func (d *TemporalDecoder) Scope() string {
	if d == nil {
		return ""
	}
	return d.cfg.Scope
}

// LagDuration returns primary lag duration.
func (d *TemporalDecoder) LagDuration() time.Duration {
	if d == nil {
		return 0
	}
	return time.Duration(d.cfg.LagSeconds) * time.Second
}

// MaxWaitDuration returns max wait duration.
func (d *TemporalDecoder) MaxWaitDuration() time.Duration {
	if d == nil {
		return 0
	}
	return time.Duration(d.cfg.MaxWaitSeconds) * time.Second
}

// PendingCount returns currently tracked temporal requests.
func (d *TemporalDecoder) PendingCount() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.requests)
}

// ShouldHoldSelection reports whether a selection should be routed through lag
// decoding under the configured scope.
func (d *TemporalDecoder) ShouldHoldSelection(selection ResolverPrimarySelection) bool {
	if d == nil || !d.active {
		return false
	}
	switch d.cfg.Scope {
	case "all_correction_candidates":
		return true
	default:
		if !selection.SnapshotOK {
			return false
		}
		if selection.NeighborhoodSplit {
			return true
		}
		switch selection.Snapshot.State {
		case spot.ResolverStateSplit, spot.ResolverStateUncertain:
			return true
		default:
			return false
		}
	}
}

// Observe records one temporal observation request. Return values are:
//   - accepted: whether request was stored
//   - reason: empty on success; otherwise short reject reason
func (d *TemporalDecoder) Observe(obs TemporalObservation) (bool, string) {
	if d == nil || !d.active {
		return false, "disabled"
	}
	if obs.ID == 0 {
		return false, "invalid_id"
	}
	subject := spot.NormalizeCallsign(obs.SubjectCall)
	if subject == "" {
		return false, "missing_subject"
	}
	key := obs.Key
	if key.Band == "" || key.Mode == "" {
		key = obs.Selection.Snapshot.Key
	}
	if key.Band == "" || key.Mode == "" {
		return false, "missing_key"
	}
	observedAt := obs.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.requests[obs.ID]; exists {
		return false, "duplicate_id"
	}
	if len(d.requests) >= d.cfg.MaxPending {
		return false, "pending_full"
	}
	st := d.keyStates[key]
	if st == nil {
		if len(d.keyStates) >= d.cfg.MaxActiveKeys {
			return false, "max_active_keys"
		}
		st = &temporalKeyState{
			events: make([]temporalEvent, 0, d.cfg.MaxEventsPerKey),
		}
		d.keyStates[key] = st
	}
	st.events = append(st.events, temporalEvent{
		id:         obs.ID,
		observedAt: observedAt,
		subject:    subject,
		selection:  obs.Selection,
	})
	d.requests[obs.ID] = temporalRequest{
		id:         obs.ID,
		key:        key,
		observedAt: observedAt,
		subject:    subject,
	}
	d.trimKeyEventsLocked(key, st)
	return true, ""
}

// Evaluate computes one temporal decision for request id at time now. When
// force=true, confidence gates no longer defer and fallback is applied.
func (d *TemporalDecoder) Evaluate(id uint64, now time.Time, force bool) TemporalDecision {
	decision := TemporalDecision{
		ID:     id,
		Action: TemporalDecisionActionBypass,
		Reason: "decoder_disabled",
	}
	if d == nil || !d.active {
		return decision
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	req, ok := d.requests[id]
	if !ok {
		decision.Reason = "missing_request"
		return decision
	}
	decision.CommitLatency = clampDurationNonNegative(now.Sub(req.observedAt))

	lagAt := req.observedAt.Add(d.LagDuration())
	maxAt := req.observedAt.Add(d.MaxWaitDuration())
	if !force && now.Before(lagAt) {
		decision.Action = TemporalDecisionActionDefer
		decision.Reason = "lag_pending"
		return decision
	}

	st := d.keyStates[req.key]
	if st == nil {
		decision.Action = d.fallbackActionLocked(req.key)
		decision.Reason = "missing_key_state"
		delete(d.requests, req.id)
		return decision
	}
	startIdx := findTemporalEventIndex(st.events, req.id)
	if startIdx < 0 {
		decision.Action = d.fallbackActionLocked(req.key)
		decision.Reason = "event_evicted"
		delete(d.requests, req.id)
		return decision
	}

	endTime := now
	if endTime.After(maxAt) {
		endTime = maxAt
	}
	endIdx := startIdx
	for i := startIdx + 1; i < len(st.events); i++ {
		if st.events[i].observedAt.After(endTime) {
			break
		}
		endIdx = i
	}

	window := make([]temporalEvent, 0, endIdx-startIdx+1)
	for i := startIdx; i <= endIdx; i++ {
		window = append(window, st.events[i])
	}
	winner, bestScore, marginScore, candidateCount := d.decodeWindowLocked(window, req.subject, req.key.Mode)
	decision.BestScore = bestScore
	decision.MarginScore = marginScore
	decision.CandidateCount = candidateCount
	decision.Winner = winner
	decision.Selection = selectionForTemporalWinner(window[0].selection, winner)

	thresholdPass := winner != "" && bestScore >= d.cfg.MinScore && marginScore >= d.cfg.MinMarginScore
	timedOut := force || !now.Before(maxAt)
	if !thresholdPass && !timedOut {
		decision.Action = TemporalDecisionActionDefer
		decision.Reason = temporalGateReason(winner, bestScore, marginScore, d.cfg.MinScore, d.cfg.MinMarginScore)
		return decision
	}
	if thresholdPass {
		decision.Action = TemporalDecisionActionApply
		decision.Reason = "committed"
		last := strings.TrimSpace(d.lastCommitted[req.key])
		if last != "" && !strings.EqualFold(last, winner) {
			decision.PathSwitched = true
		}
		d.lastCommitted[req.key] = winner
		delete(d.requests, req.id)
		if !d.hasPendingKeyLocked(req.key) {
			delete(d.keyStates, req.key)
		}
		return decision
	}
	decision.Action = d.fallbackActionLocked(req.key)
	decision.Reason = temporalGateReason(winner, bestScore, marginScore, d.cfg.MinScore, d.cfg.MinMarginScore)
	delete(d.requests, req.id)
	if !d.hasPendingKeyLocked(req.key) {
		delete(d.keyStates, req.key)
	}
	return decision
}

func (d *TemporalDecoder) fallbackActionLocked(key spot.ResolverSignalKey) TemporalDecisionAction {
	switch d.cfg.OverflowAction {
	case "abstain":
		return TemporalDecisionActionAbstain
	case "bypass":
		return TemporalDecisionActionBypass
	default:
		return TemporalDecisionActionFallbackResolver
	}
}

func (d *TemporalDecoder) hasPendingKeyLocked(key spot.ResolverSignalKey) bool {
	for _, req := range d.requests {
		if req.key == key {
			return true
		}
	}
	return false
}

func (d *TemporalDecoder) trimKeyEventsLocked(key spot.ResolverSignalKey, st *temporalKeyState) {
	if st == nil {
		return
	}
	maxEvents := d.cfg.MaxEventsPerKey
	if maxEvents <= 0 {
		maxEvents = 32
	}
	for len(st.events) > maxEvents {
		ev := st.events[0]
		st.events = st.events[1:]
		if req, ok := d.requests[ev.id]; ok {
			req.evicted = true
			d.requests[ev.id] = req
		}
	}
	if len(st.events) == 0 {
		delete(d.keyStates, key)
	}
}

func findTemporalEventIndex(events []temporalEvent, id uint64) int {
	for i := range events {
		if events[i].id == id {
			return i
		}
	}
	return -1
}

func clampDurationNonNegative(v time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	return v
}

func temporalGateReason(winner string, bestScore int, marginScore int, minScore int, minMargin int) string {
	if strings.TrimSpace(winner) == "" {
		return "winner_missing"
	}
	if bestScore < minScore {
		return "low_score"
	}
	if marginScore < minMargin {
		return "low_margin"
	}
	return "gate_reject"
}

func selectionForTemporalWinner(base ResolverPrimarySelection, winner string) ResolverPrimarySelection {
	if !base.SnapshotOK {
		return base
	}
	winner = spot.NormalizeCallsign(winner)
	if winner == "" {
		return base
	}
	snap := base.Snapshot
	snap.Winner = winner
	snap.WinnerSupport = ResolverSupportForCall(snap, winner)
	snap.WinnerWeightedSupportMilli = ResolverWeightedSupportForCall(snap, winner)
	runnerCall, runnerSupport, runnerWeighted := runnerForTemporalSnapshot(snap, winner)
	snap.RunnerUp = runnerCall
	snap.RunnerSupport = runnerSupport
	snap.RunnerWeightedSupportMilli = runnerWeighted
	snap.Margin = runnerSupport
	if snap.WinnerSupport > 0 {
		snap.Margin = snap.WinnerSupport - runnerSupport
	}
	snap.State = classifyTemporalSnapshotState(snap)
	base.Snapshot = snap
	return base
}

func runnerForTemporalSnapshot(snap spot.ResolverSnapshot, winner string) (string, int, int) {
	bestCall := ""
	bestSupport := 0
	bestWeighted := 0
	for _, candidate := range snap.CandidateRanks {
		call := spot.NormalizeCallsign(candidate.Call)
		if call == "" || strings.EqualFold(call, winner) {
			continue
		}
		if candidate.WeightedSupportMilli > bestWeighted {
			bestCall = call
			bestSupport = candidate.Support
			bestWeighted = candidate.WeightedSupportMilli
			continue
		}
		if candidate.WeightedSupportMilli == bestWeighted && candidate.Support > bestSupport {
			bestCall = call
			bestSupport = candidate.Support
			bestWeighted = candidate.WeightedSupportMilli
			continue
		}
		if candidate.WeightedSupportMilli == bestWeighted && candidate.Support == bestSupport && (bestCall == "" || call < bestCall) {
			bestCall = call
			bestSupport = candidate.Support
			bestWeighted = candidate.WeightedSupportMilli
		}
	}
	return bestCall, bestSupport, bestWeighted
}

func classifyTemporalSnapshotState(snap spot.ResolverSnapshot) spot.ResolverState {
	if snap.TotalWeightedSupportMilli > 0 && snap.WinnerWeightedSupportMilli > 0 {
		confidence := snap.WinnerWeightedSupportMilli * 100 / snap.TotalWeightedSupportMilli
		switch {
		case confidence >= 51:
			return spot.ResolverStateConfident
		case confidence >= 25:
			return spot.ResolverStateProbable
		default:
			return spot.ResolverStateUncertain
		}
	}
	if snap.TotalReporters > 0 && snap.WinnerSupport > 0 {
		confidence := snap.WinnerSupport * 100 / snap.TotalReporters
		switch {
		case confidence >= 51:
			return spot.ResolverStateConfident
		case confidence >= 25:
			return spot.ResolverStateProbable
		default:
			return spot.ResolverStateUncertain
		}
	}
	return spot.ResolverStateUncertain
}

func (d *TemporalDecoder) decodeWindowLocked(events []temporalEvent, fallbackSubject string, mode string) (string, int, int, int) {
	if len(events) == 0 {
		return "", 0, 0, 0
	}
	candidateSets := make([][]string, 0, len(events))
	for _, ev := range events {
		candidateSets = append(candidateSets, d.candidatesForEvent(ev, fallbackSubject))
	}
	startCandidates := candidateSets[0]
	if len(startCandidates) == 0 {
		return "", 0, 0, 0
	}
	perStart := make([]temporalScore, 0, len(startCandidates))
	for _, start := range startCandidates {
		initial := d.emissionScore(events[0], start, len(candidateSets[0]))
		prev := map[string]int{start: initial}
		for step := 1; step < len(events); step++ {
			universe := temporalUniverse(candidateSets[step], prev)
			nextScores := make(map[string]int, len(universe))
			prevCalls := sortedTemporalScoreKeys(prev)
			for _, call := range universe {
				best := -1 << 30
				for _, prevCall := range prevCalls {
					score := prev[prevCall]
					score += d.transitionScore(prevCall, call, mode)
					score += d.emissionScore(events[step], call, len(candidateSets[step]))
					if score > best {
						best = score
					}
				}
				nextScores[call] = best
			}
			prev = topTemporalScores(nextScores, d.cfg.BeamSize)
			if len(prev) == 0 {
				break
			}
		}
		perStart = append(perStart, temporalScore{
			call:  start,
			score: maxTemporalScore(prev),
		})
	}
	sort.Slice(perStart, func(i, j int) bool {
		if perStart[i].score != perStart[j].score {
			return perStart[i].score > perStart[j].score
		}
		return perStart[i].call < perStart[j].call
	})
	if len(perStart) == 0 {
		return "", 0, 0, 0
	}
	best := perStart[0]
	margin := best.score
	if len(perStart) > 1 {
		margin = best.score - perStart[1].score
	}
	return best.call, best.score, margin, len(startCandidates)
}

func (d *TemporalDecoder) candidatesForEvent(event temporalEvent, fallbackSubject string) []string {
	set := make(map[string]struct{}, d.cfg.MaxObsCandidates+2)
	out := make([]string, 0, d.cfg.MaxObsCandidates+2)

	appendCall := func(value string) {
		call := spot.NormalizeCallsign(value)
		if call == "" {
			return
		}
		if _, exists := set[call]; exists {
			return
		}
		set[call] = struct{}{}
		out = append(out, call)
	}

	if event.selection.SnapshotOK {
		for idx, candidate := range event.selection.Snapshot.CandidateRanks {
			if idx >= d.cfg.MaxObsCandidates {
				break
			}
			appendCall(candidate.Call)
		}
		appendCall(event.selection.Snapshot.Winner)
	}
	appendCall(event.subject)
	appendCall(fallbackSubject)
	return out
}

func (d *TemporalDecoder) emissionScore(event temporalEvent, call string, candidateCount int) int {
	call = spot.NormalizeCallsign(call)
	if call == "" {
		return 0
	}
	if candidateCount <= 0 {
		candidateCount = 1
	}
	if event.selection.SnapshotOK {
		snapshot := event.selection.Snapshot
		if snapshot.TotalWeightedSupportMilli > 0 {
			support := ResolverWeightedSupportForCall(snapshot, call)
			denom := snapshot.TotalWeightedSupportMilli + candidateCount
			if denom <= 0 {
				return 0
			}
			return (support + 1) * temporalEmissionScale / denom
		}
		if snapshot.TotalReporters > 0 {
			support := ResolverSupportForCall(snapshot, call)
			denom := snapshot.TotalReporters + candidateCount
			if denom <= 0 {
				return 0
			}
			return (support + 1) * temporalEmissionScale / denom
		}
	}
	if strings.EqualFold(call, event.subject) {
		return 1
	}
	return 0
}

func (d *TemporalDecoder) transitionScore(prevCall, nextCall, mode string) int {
	prev := spot.NormalizeCallsign(prevCall)
	next := spot.NormalizeCallsign(nextCall)
	if prev == "" || next == "" {
		return -d.cfg.SwitchPenalty
	}
	if strings.EqualFold(prev, next) {
		return d.cfg.StayBonus
	}
	penalty := d.cfg.SwitchPenalty
	if relatedByTemporalFamily(prev, next, d.familyPolicy) {
		penalty = d.cfg.FamilySwitchPenalty
	} else if spot.CallDistance(prev, next, mode, d.distanceModelCW, d.distanceModelRTTY) <= 1 {
		penalty = d.cfg.Edit1SwitchPenalty
	}
	return -penalty
}

func relatedByTemporalFamily(left, right string, policy spot.CorrectionFamilyPolicy) bool {
	left = spot.CorrectionVoteKey(left)
	right = spot.CorrectionVoteKey(right)
	if left == "" || right == "" {
		return false
	}
	if strings.EqualFold(left, right) {
		return true
	}
	_, ok := spot.DetectCorrectionFamilyWithPolicy(left, right, policy)
	return ok
}

type temporalScore struct {
	call  string
	score int
}

func temporalUniverse(observed []string, prev map[string]int) []string {
	set := make(map[string]struct{}, len(observed)+len(prev))
	out := make([]string, 0, len(observed)+len(prev))
	appendCall := func(value string) {
		call := spot.NormalizeCallsign(value)
		if call == "" {
			return
		}
		if _, exists := set[call]; exists {
			return
		}
		set[call] = struct{}{}
		out = append(out, call)
	}
	for _, call := range observed {
		appendCall(call)
	}
	for call := range prev {
		appendCall(call)
	}
	sort.Strings(out)
	return out
}

func sortedTemporalScoreKeys(scores map[string]int) []string {
	out := make([]string, 0, len(scores))
	for call := range scores {
		out = append(out, call)
	}
	sort.Strings(out)
	return out
}

func maxTemporalScore(scores map[string]int) int {
	if len(scores) == 0 {
		return -1 << 30
	}
	best := -1 << 30
	for _, value := range scores {
		if value > best {
			best = value
		}
	}
	return best
}

func topTemporalScores(scores map[string]int, beam int) map[string]int {
	if beam <= 0 {
		beam = 1
	}
	ranked := make([]temporalScore, 0, len(scores))
	for call, score := range scores {
		ranked = append(ranked, temporalScore{call: call, score: score})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].call < ranked[j].call
	})
	if len(ranked) > beam {
		ranked = ranked[:beam]
	}
	out := make(map[string]int, len(ranked))
	for _, row := range ranked {
		out[row.call] = row.score
	}
	return out
}
