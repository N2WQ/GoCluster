package correctionflow

import (
	"sort"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type RuntimeSettings struct {
	Window          time.Duration
	MinReports      int
	FreqToleranceHz float64
}

type bandStateParams struct {
	FrequencyToleranceHz float64
}

type BuildSettingsInput struct {
	Cfg             config.CallCorrectionConfig
	MinReports      int
	Window          time.Duration
	FreqToleranceHz float64
	RecentBandStore spot.RecentSupportStore
}

type ResolverPrimarySelection struct {
	Snapshot          spot.ResolverSnapshot
	SnapshotOK        bool
	UsedNeighborhood  bool
	WinnerOverride    bool
	NeighborhoodSplit bool
	CandidateCount    int
	// replacementCalls is immutable selection-local eligibility. Nil keeps exact
	// snapshot behavior; synthesized neighborhoods constrain replacements while
	// CandidateRanks still carry every admitted dissenting receiver's tally.
	replacementCalls []string
	exactWinner      string // Same-generation witness for the final neighborhood rail.
	// Each nonzero slot witnesses a contributing key lifecycle at offsets -2..2.
	// Temporal retention copies these scalars rather than receiver rows/cells.
	generationWitnesses [5]uint64
	generationConflict  bool
	// NeighborhoodExcluded* count neighborhood winner candidates excluded from
	// competition due to comparability rails.
	NeighborhoodExcludedUnrelated int
	NeighborhoodExcludedDistance  int
	// NeighborhoodExcludedAnchorMissing is set when neighborhood arbitration is
	// enabled but no anchor call is available; selection fails closed to exact.
	NeighborhoodExcludedAnchorMissing int
}

// FormatConfidence converts subject confidence percent into the public glyph.
// Contract: callers provide subject confidence (not corrected-candidate confidence).
// Mapping is intentionally coarse and stable: unknown ("?") for <=1 reporter,
// then P (<=50) or V (>=51). C/B are set by apply/validation rails, not here.
func FormatConfidence(percent int, totalReporters int) string {
	if totalReporters <= 1 {
		return "?"
	}

	value := percent
	if value < 0 {
		value = 0
	}
	if value > 100 {
		value = 100
	}

	switch {
	case value >= 51:
		return "V"
	default:
		return "P"
	}
}

// ResolverSupportForCall returns reporter support for one call in snapshot.
// It prefers candidate rank support and falls back to winner/runner fields when
// rank lists are unavailable in synthetic snapshots.
func ResolverSupportForCall(snapshot spot.ResolverSnapshot, call string) int {
	call = spot.NormalizeSpotDXCallsign(call)
	if call == "" {
		return 0
	}
	for _, candidate := range snapshot.CandidateRanks {
		if candidate.Call == call {
			if candidate.Support > 0 {
				return candidate.Support
			}
			return 0
		}
	}
	if snapshot.WinnerSupport > 0 && snapshot.Winner == call {
		return snapshot.WinnerSupport
	}
	if snapshot.RunnerSupport > 0 && snapshot.RunnerUp == call {
		return snapshot.RunnerSupport
	}
	return 0
}

// ResolverWeightedSupportForCall returns weighted support (milli-units) for one
// call in snapshot. It mirrors ResolverSupportForCall fallback behavior.
func ResolverWeightedSupportForCall(snapshot spot.ResolverSnapshot, call string) int {
	call = spot.NormalizeSpotDXCallsign(call)
	if call == "" {
		return 0
	}
	for _, candidate := range snapshot.CandidateRanks {
		if candidate.Call == call {
			if candidate.WeightedSupportMilli > 0 {
				return candidate.WeightedSupportMilli
			}
			return 0
		}
	}
	if snapshot.WinnerWeightedSupportMilli > 0 && snapshot.Winner == call {
		return snapshot.WinnerWeightedSupportMilli
	}
	if snapshot.RunnerWeightedSupportMilli > 0 && snapshot.RunnerUp == call {
		return snapshot.RunnerWeightedSupportMilli
	}
	return 0
}

// ResolverCallConfidencePercent computes call-specific confidence percentage.
// Eligible call evidence is required first; its declared denominator basis does
// not change when a known weighted numerator is zero or rounds below one percent.
func ResolverCallConfidencePercent(snapshot spot.ResolverSnapshot, call string) (int, bool) {
	if snapshot.EvidenceLoss || snapshot.TotalReporters <= 0 {
		return 0, false
	}
	call = spot.NormalizeSpotDXCallsign(call)
	if call == "" {
		return 0, false
	}
	support, weighted := 0, 0
	if len(snapshot.CandidateRanks) > 0 {
		// A published rank list is the eligible call population. Its absent or
		// zero row must not regain support from stale winner/runner headers.
		for _, candidate := range snapshot.CandidateRanks {
			if candidate.Call == call {
				support, weighted = max(0, candidate.Support), max(0, candidate.WeightedSupportMilli)
				break
			}
		}
	} else {
		switch call {
		case snapshot.Winner:
			support, weighted = max(0, snapshot.WinnerSupport), max(0, snapshot.WinnerWeightedSupportMilli)
		case snapshot.RunnerUp:
			support, weighted = max(0, snapshot.RunnerSupport), max(0, snapshot.RunnerWeightedSupportMilli)
		}
	}
	if support <= 0 {
		return 0, false
	}
	switch snapshot.EffectiveSupportBasis() {
	case spot.ResolverSupportBasisWeighted:
		if snapshot.TotalWeightedSupportMilli > 0 {
			return clampPercent(weighted * 100 / snapshot.TotalWeightedSupportMilli), true
		}
	case spot.ResolverSupportBasisReceivers:
		return clampPercent(support * 100 / snapshot.TotalReporters), true
	}
	return 0, false
}

// ResolverWinnerConfidence computes winner confidence percentage from snapshot.
func ResolverWinnerConfidence(snapshot spot.ResolverSnapshot) int {
	percent, ok := ResolverCallConfidencePercent(snapshot, snapshot.Winner)
	if !ok {
		return 0
	}
	return percent
}

// ResolverConfidenceGlyphForCall maps resolver snapshot evidence to output glyph
// semantics for the call that will actually be emitted.
func ResolverConfidenceGlyphForCall(snapshot spot.ResolverSnapshot, snapshotOK bool, emittedCall string) string {
	if !snapshotOK || snapshot.EvidenceLoss {
		return "?"
	}
	if snapshot.State == spot.ResolverStateSplit || snapshot.State == spot.ResolverStateUncertain {
		if snapshot.TotalReporters <= 1 {
			return "?"
		}
		// Keep contested resolver outcomes conservative so conflicting variants
		// do not both present as strong-verified ("V").
		return "P"
	}

	percent, ok := ResolverCallConfidencePercent(snapshot, emittedCall)
	if !ok {
		return "?"
	}
	return FormatConfidence(percent, snapshot.TotalReporters)
}

// SelectResolverPrimarySnapshot returns the resolver snapshot used by
// resolver-primary correction decisions. When neighborhood mode is enabled, it
// competes winners across adjacent resolver buckets to reduce boundary forks.
func SelectResolverPrimarySnapshot(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig) ResolverPrimarySelection {
	return SelectResolverPrimarySnapshotForCall(resolver, key, cfg, "")
}

// SelectResolverPrimarySnapshotForCall is the call-anchored variant of
// SelectResolverPrimarySnapshot.
//
// Anchor contract:
//   - When anchorCall is provided (typically pre-correction DX call), only
//     neighborhood winners comparable to that anchor are admitted.
//   - When anchorCall is blank, exact snapshot winner is used as fallback
//     anchor when available.
//   - If no anchor can be established, selection fails closed to exact snapshot.
func SelectResolverPrimarySnapshotForCall(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, anchorCall string) ResolverPrimarySelection {
	return selectResolverPrimarySnapshotForCall(resolver, key, cfg, anchorCall, time.Time{})
}

// SelectResolverPrimarySnapshotForCallAt uses only rows eligible at release.
// It does not wait for ingestion or advance resolver hysteresis.
func SelectResolverPrimarySnapshotForCallAt(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, anchorCall string, now time.Time) ResolverPrimarySelection {
	return selectResolverPrimarySnapshotForCall(resolver, key, cfg, anchorCall, now)
}

func selectResolverPrimarySnapshotForCall(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, anchorCall string, now time.Time) ResolverPrimarySelection {
	if resolver == nil {
		return ResolverPrimarySelection{}
	}
	lookup := func(k spot.ResolverSignalKey) (spot.ResolverSnapshot, bool) {
		if now.IsZero() {
			return resolver.Lookup(k)
		}
		return resolver.LookupEligible(k, now)
	}
	return selectResolverPrimarySnapshotFromLookup(key, cfg, anchorCall, lookup)
}

// selectResolverPrimarySnapshotFromLookup keeps the production arbitration in
// one bounded pass. The private boundary also permits explicit raw populations
// to exercise the same rules without introducing a runtime unweighted switch.
func selectResolverPrimarySnapshotFromLookup(key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, anchorCall string, lookup func(spot.ResolverSignalKey) (spot.ResolverSnapshot, bool)) ResolverPrimarySelection {
	selection := ResolverPrimarySelection{}
	exact, exactOK := lookup(key)
	selection.Snapshot = exact
	selection.SnapshotOK = exactOK
	selection.exactWinner = exact.Winner
	if exactOK {
		selection.generationWitnesses[2] = exact.Generation
	}
	// Read the bounded neighborhood once. Known loss protects the declared
	// population before state/support/replacement comparability can hide it.
	// Raw reads preserve owner-clock semantics; release reads use their clock.
	radius := 0
	if cfg.ResolverNeighborhoodEnabled {
		radius = min(2, max(1, cfg.ResolverNeighborhoodBucketRadius))
	}
	var snapshots [5]spot.ResolverSnapshot
	var present [5]bool
	snapshots[2], present[2] = exact, exactOK
	lost := exactOK && exact.EvidenceLoss
	for offset := -radius; offset <= radius; offset++ {
		if offset == 0 {
			continue
		}
		neighborKey := key
		neighborKey.Bucket += int64(offset)
		snap, ok := lookup(neighborKey)
		snapshots[offset+2], present[offset+2] = snap, ok
		if ok && snap.EvidenceLoss {
			lost = true
			selection.generationWitnesses[offset+2] = snap.Generation
		}
	}
	if lost {
		selection.Snapshot.Key = key
		selection.Snapshot.EvidenceLoss = true
		selection.Snapshot.State = spot.ResolverStateUncertain
		selection.Snapshot.Winner = ""
		selection.SnapshotOK = true
		return selection
	}
	// A qualifying exact split protects its own population. Neighboring bucket
	// winners cannot discard that dissent; a later owner publication reevaluates
	// protection when exact eligible evidence changes, so this is not a latch.
	if exactOK && exact.State == spot.ResolverStateSplit {
		return selection
	}
	if !cfg.ResolverNeighborhoodEnabled {
		return selection
	}
	anchor := spot.NormalizeSpotDXCallsign(anchorCall)
	if anchor == "" && exactOK {
		anchor = spot.NormalizeSpotDXCallsign(exact.Winner)
	}
	if anchor == "" {
		selection.NeighborhoodExcludedAnchorMissing = 1
		return selection
	}

	aggregates := make(map[string]*neighborhoodCandidate, (radius*2)+1)
	receivers := make(map[string]int)
	var admitted [5]bool
	basis := spot.ResolverSupportBasisUnspecified
	for offset := -radius; offset <= radius; offset++ {
		snap, ok := snapshots[offset+2], present[offset+2]
		if !ok {
			continue
		}
		settled := snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable
		winner := snap.Winner
		if incumbent, _, pending := snap.PendingTransition(); winner == "" && pending {
			winner = incumbent
		}
		if snap.ReceiverEvidence().Len() == 0 {
			continue
		}

		if offset != 0 {
			selection.UsedNeighborhood = true
		}
		// The exact population belongs to this request even while contested. A
		// neighboring population still needs its published authority/owner incumbent
		// to pass the existing whole-bucket comparability rail.
		comparable, rejectReason := resolverNeighborhoodComparable(anchor, winner, key.Mode, cfg)
		if offset == 0 {
			comparable = true
		}
		if !comparable {
			switch rejectReason {
			case "distance":
				selection.NeighborhoodExcludedDistance++
			default:
				selection.NeighborhoodExcludedUnrelated++
			}
			continue
		}
		selection.generationWitnesses[offset+2] = snap.Generation
		admitted[offset+2] = true
		// Only admitted populations contribute. Unavailable or mixed bases are
		// absorbing; later weighted buckets must not restore numerical authority.
		incomingBasis := snap.EffectiveSupportBasis()
		if basis == spot.ResolverSupportBasisUnspecified {
			basis = incomingBasis
		} else if basis != incomingBasis {
			basis = spot.ResolverSupportBasisUnavailable
		}
		addNeighborhoodEvidence(aggregates, receivers, snap, settled)
	}

	if !selection.UsedNeighborhood && exactOK {
		return selection
	}
	if len(aggregates) == 0 {
		if exactOK {
			return selection
		}
		return ResolverPrimarySelection{}
	}

	ranked := make([]neighborhoodCandidate, 0, len(aggregates))
	allCandidates := make([]spot.ResolverCandidateSupport, 0, len(aggregates))
	for _, group := range aggregates {
		if group == nil {
			continue
		}
		allCandidates = append(allCandidates, spot.ResolverCandidateSupport{Call: group.call,
			Support: group.support, WeightedSupportMilli: group.weighted})
		if comparable, _ := resolverNeighborhoodComparable(anchor, group.call, key.Mode, cfg); comparable && group.nominee {
			ranked = append(ranked, *group)
		}
	}
	sort.Slice(allCandidates, func(i, j int) bool {
		left, right := allCandidates[i], allCandidates[j]
		if left.WeightedSupportMilli != right.WeightedSupportMilli {
			return left.WeightedSupportMilli > right.WeightedSupportMilli
		}
		if left.Support != right.Support {
			return left.Support > right.Support
		}
		return left.Call < right.Call
	})
	if len(ranked) == 0 {
		// No settled population supplied a replacement nominee. Keep the admitted
		// union visible for confidence without inventing settled authority.
		selection.Snapshot = spot.ResolverSnapshot{Key: key, State: spot.ResolverStateUncertain,
			SupportBasis: basis, TotalReporters: len(receivers), CandidateRanks: allCandidates}
		for _, weight := range receivers {
			selection.Snapshot.TotalWeightedSupportMilli += weight
		}
		selection.SnapshotOK, selection.CandidateCount = true, len(allCandidates)
		selection.replacementCalls = []string{}
		return selection
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].weighted != ranked[j].weighted {
			return ranked[i].weighted > ranked[j].weighted
		}
		if ranked[i].support != ranked[j].support {
			return ranked[i].support > ranked[j].support
		}
		leftRank := resolverStateRank(ranked[i].best.State)
		rightRank := resolverStateRank(ranked[j].best.State)
		if leftRank != rightRank {
			return leftRank > rightRank
		}
		return ranked[i].call < ranked[j].call
	})
	selection.CandidateCount = len(allCandidates)
	top := ranked[0]

	// Synthesized tallies cover several buckets, so never label one bucket's
	// receiver view as their provenance. Fresh selection rereads publications.
	synth := spot.ResolverSnapshot{EvaluatedAt: top.best.EvaluatedAt}
	synth.Key = key
	synth.Generation = 0 // Several keys contributed; their five witnesses own provenance.
	synth.Winner = top.call
	synth.WinnerSupport = top.support
	synth.WinnerWeightedSupportMilli = top.weighted
	synth.TotalReporters = len(receivers)
	synth.SupportBasis = basis
	synth.TotalWeightedSupportMilli = 0
	for _, weight := range receivers {
		synth.TotalWeightedSupportMilli += weight
	}
	synth.CandidateRanks = allCandidates
	synth.State = classifyTemporalSnapshotState(synth)

	if len(ranked) > 1 {
		runner := ranked[1]
		synth.RunnerUp = runner.call
		synth.RunnerSupport = runner.support
		synth.RunnerWeightedSupportMilli = runner.weighted
		synth.Margin = synth.WinnerSupport - synth.RunnerSupport

		comparable, rejectReason := resolverNeighborhoodComparable(top.call, runner.call, key.Mode, cfg)
		if !comparable {
			switch rejectReason {
			case "distance":
				selection.NeighborhoodExcludedDistance++
			default:
				selection.NeighborhoodExcludedUnrelated++
			}
		} else {
			ratioThreshold := cfg.FreqGuardRunnerUpRatio
			if ratioThreshold <= 0 {
				ratioThreshold = 0.5
			}
			conflict := false
			switch {
			case synth.WinnerWeightedSupportMilli > 0 && synth.RunnerWeightedSupportMilli > 0:
				conflict = float64(synth.RunnerWeightedSupportMilli) >= ratioThreshold*float64(synth.WinnerWeightedSupportMilli)
			case synth.WinnerSupport > 0 && synth.RunnerSupport > 0:
				conflict = float64(synth.RunnerSupport) >= ratioThreshold*float64(synth.WinnerSupport)
			}
			if conflict {
				synth.State = spot.ResolverStateSplit
				synth.Winner = ""
				synth.Margin = 0
				selection.NeighborhoodSplit = true
			}
		}
	} else {
		synth.RunnerUp = ""
		synth.RunnerSupport = 0
		synth.RunnerWeightedSupportMilli = 0
		synth.Margin = synth.WinnerSupport
	}

	selection.Snapshot = synth
	selection.SnapshotOK = true
	// Eligibility is proposal-specific: a decoded candidate may differ from the
	// ordinary aggregate winner. Compact temporal events retain only these bounded
	// identities and tallies, never the source receiver publications.
	selection.replacementCalls = make([]string, 0, len(ranked))
	for i := range ranked {
		candidate := &ranked[i]
		pending, tied := neighborhoodProposalBlocked(candidate.call, synth, aggregates, snapshots, admitted, cfg)
		if !pending && !tied {
			selection.replacementCalls = append(selection.replacementCalls, candidate.call)
		}
		if candidate.call == top.call && !selection.NeighborhoodSplit {
			switch {
			case tied:
				selection.Snapshot.State, selection.Snapshot.Winner = spot.ResolverStateSplit, ""
				selection.NeighborhoodSplit = true
			case pending:
				selection.Snapshot.State = spot.ResolverStateUncertain
			}
		}
	}
	if !selection.NeighborhoodSplit {
		exactWinner := ""
		if exactOK {
			exactWinner = spot.NormalizeSpotDXCallsign(exact.Winner)
		}
		if exactWinner != "" && exactWinner != synth.Winner {
			comparable, rejectReason := resolverNeighborhoodComparable(exactWinner, synth.Winner, key.Mode, cfg)
			if !comparable {
				switch rejectReason {
				case "distance":
					selection.NeighborhoodExcludedDistance++
				default:
					selection.NeighborhoodExcludedUnrelated++
				}
				selection.Snapshot = exact
				selection.SnapshotOK = exactOK
				selection.replacementCalls = nil
				selection.WinnerOverride = false
				return selection
			}
			selection.WinnerOverride = true
		}
	}
	return selection
}

// SelectTemporalReleaseSnapshot treats the decoded identity as a proposal.
// Current eligible, settled evidence supplies all counts and ambiguity gates;
// an unsupported proposal falls back to the current ordinary selection.
func SelectTemporalReleaseSnapshot(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, subject string, decision TemporalDecision, now time.Time) ResolverPrimarySelection {
	selection := SelectResolverPrimarySnapshotForCallAt(resolver, key, cfg, subject, now)
	if decision.Action != TemporalDecisionActionApply || !selection.SnapshotOK || selection.Snapshot.EvidenceLoss || selection.NeighborhoodSplit ||
		(selection.Snapshot.State != spot.ResolverStateConfident && selection.Snapshot.State != spot.ResolverStateProbable) {
		return selection
	}
	if !resolverGenerationWitnessesCurrent(resolver, key, decision.Selection) {
		return selection
	}
	winner := spot.NormalizeSpotDXCallsign(decision.Winner)
	if winner == "" || !selection.allowsReplacement(winner) || ResolverSupportForCall(selection.Snapshot, winner) == 0 {
		return selection
	}
	if selection.replacementCalls == nil && resolver.ProposalHasAmbiguousRawTie(selection.Snapshot, winner) {
		return selection
	}
	if selection.replacementCalls != nil && selection.exactWinner != "" && selection.exactWinner != winner {
		if comparable, _ := resolverNeighborhoodComparable(selection.exactWinner, winner, key.Mode, cfg); !comparable {
			return selection
		}
	}
	return selectionForTemporalWinner(selection, winner)
}

func resolverNeighborhoodComparable(anchorCall, candidateCall, mode string, cfg config.CallCorrectionConfig) (bool, string) {
	anchor := spot.CorrectionVoteKey(anchorCall)
	candidate := spot.CorrectionVoteKey(candidateCall)
	if anchor == "" || candidate == "" {
		return false, "unrelated"
	}
	if strings.EqualFold(anchor, candidate) {
		return true, ""
	}
	familyPolicy := spot.CorrectionFamilyPolicy{
		Configured:                 true,
		TruncationEnabled:          cfg.FamilyPolicy.Truncation.Enabled,
		TruncationMaxLengthDelta:   cfg.FamilyPolicy.Truncation.MaxLengthDelta,
		TruncationMinShorterLength: cfg.FamilyPolicy.Truncation.MinShorterLength,
		TruncationAllowPrefix:      cfg.FamilyPolicy.Truncation.AllowPrefixMatch,
		TruncationAllowSuffix:      cfg.FamilyPolicy.Truncation.AllowSuffixMatch,
	}
	if relation, ok := spot.DetectCorrectionFamilyWithPolicy(anchor, candidate, familyPolicy); ok {
		switch relation.Kind {
		case spot.CorrectionFamilySlash:
			return true, ""
		case spot.CorrectionFamilyTruncation:
			if cfg.ResolverNeighborhoodAllowTruncation {
				return true, ""
			}
			return false, "distance"
		default:
			return false, "unrelated"
		}
	}

	maxDistance := cfg.ResolverNeighborhoodMaxDistance
	if maxDistance <= 0 {
		maxDistance = 1
	}
	distance := spot.CallDistance(anchor, candidate, mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
	if distance <= maxDistance {
		return true, ""
	}
	if distance <= maxDistance+1 {
		return false, "distance"
	}
	return false, "unrelated"
}

const editNeighborAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// EditDistanceOneSubstitutionVariants returns deterministic distance-1
// substitution variants for a correction vote key.
func EditDistanceOneSubstitutionVariants(call string) []string {
	key, ok := editDistanceOneSubstitutionKey(call)
	if !ok {
		return nil
	}
	seed := []byte(key)
	out := make([]string, 0, len(seed)*(len(editNeighborAlphabet)-1))
	for idx := range seed {
		original := seed[idx]
		for i := 0; i < len(editNeighborAlphabet); i++ {
			replacement := editNeighborAlphabet[i]
			if replacement == original {
				continue
			}
			seed[idx] = replacement
			out = append(out, string(seed))
		}
		seed[idx] = original
	}
	return out
}

// ForEachEditDistanceOneSubstitutionVariant streams deterministic distance-1
// substitution variants and stops when visit returns false. It is used by hot
// lookup paths that need early exit without retaining every generated variant.
func ForEachEditDistanceOneSubstitutionVariant(call string, visit func(string) bool) {
	if visit == nil {
		return
	}
	key, ok := editDistanceOneSubstitutionKey(call)
	if !ok {
		return
	}
	seed := []byte(key)
	for idx := range seed {
		original := seed[idx]
		for i := 0; i < len(editNeighborAlphabet); i++ {
			replacement := editNeighborAlphabet[i]
			if replacement == original {
				continue
			}
			seed[idx] = replacement
			if !visit(string(seed)) {
				seed[idx] = original
				return
			}
		}
		seed[idx] = original
	}
}

func editDistanceOneSubstitutionKey(call string) (string, bool) {
	key := spot.CorrectionVoteKey(call)
	if key == "" || strings.Contains(key, "/") {
		return "", false
	}
	if len(key) < 3 || len(key) > 12 {
		return "", false
	}
	return key, true
}

func resolverSnapshotBeats(left, right spot.ResolverSnapshot) bool {
	if left.WinnerWeightedSupportMilli != right.WinnerWeightedSupportMilli {
		return left.WinnerWeightedSupportMilli > right.WinnerWeightedSupportMilli
	}
	if left.WinnerSupport != right.WinnerSupport {
		return left.WinnerSupport > right.WinnerSupport
	}
	leftRank := resolverStateRank(left.State)
	rightRank := resolverStateRank(right.State)
	if leftRank != rightRank {
		return leftRank > rightRank
	}
	return left.EvaluatedAt.After(right.EvaluatedAt)
}

func resolverStateRank(state spot.ResolverState) int {
	switch state {
	case spot.ResolverStateConfident:
		return 2
	case spot.ResolverStateProbable:
		return 1
	default:
		return 0
	}
}

// CallCorrectionWindowForMode returns the recency window used by correction.
// Mode-specific overrides are only for CW/RTTY; other modes use base recency.
func CallCorrectionWindowForMode(cfg config.CallCorrectionConfig, mode string) time.Duration {
	baseSeconds := cfg.RecencySeconds
	if baseSeconds <= 0 {
		baseSeconds = 45
	}
	switch strutil.NormalizeUpper(mode) {
	case "CW":
		if cfg.RecencySecondsCW > 0 {
			baseSeconds = cfg.RecencySecondsCW
		}
	case "RTTY":
		if cfg.RecencySecondsRTTY > 0 {
			baseSeconds = cfg.RecencySecondsRTTY
		}
	}
	return time.Duration(baseSeconds) * time.Second
}

// ResolveRuntimeSettings computes resolver runtime parameters from config.
// Invariants:
//   - taxonomy voice correction modes use voice tolerance.
//   - adaptive min-reports can override correction min-report gate.
//
// Side effect: when observeAdaptive is true, source-skew-corrected observations are recorded before
// querying adaptive state so the current spot can influence near-term thresholds.
func ResolveRuntimeSettings(cfg config.CallCorrectionConfig, spotEntry *spot.Spot, adaptive *spot.AdaptiveMinReports, now time.Time, observeAdaptive bool) RuntimeSettings {
	mode := ""
	band := ""
	if spotEntry != nil {
		mode = spotEntry.Mode
		band = spotEntry.Band
	}
	modeUpper := strutil.NormalizeUpper(mode)
	if band == "" && spotEntry != nil {
		band = spot.FreqToBand(spotEntry.Frequency)
	}
	isVoice := spot.CallCorrectionProfileForMode(modeUpper) == spot.CallCorrectionProfileVoice
	adaptiveMode := spot.SourceSkewCorrectedMode(modeUpper)
	window := CallCorrectionWindowForMode(cfg, modeUpper)

	if observeAdaptive && adaptive != nil && adaptiveMode && spotEntry != nil {
		reporter := spotEntry.DECallNorm
		if reporter == "" {
			reporter = spotEntry.DECall
		}
		adaptive.Observe(band, reporter, now)
	}

	minReports := cfg.MinConsensusReports
	if adaptive != nil && adaptiveMode {
		if dyn := adaptive.MinReportsForBand(band, now); dyn > 0 {
			minReports = dyn
		}
	}

	state := "normal"
	if adaptive != nil {
		state = adaptive.StateForBand(band, now)
	}

	freqToleranceHz := cfg.FrequencyToleranceHz
	if isVoice {
		freqToleranceHz = cfg.VoiceFrequencyToleranceHz
	} else if params, ok := resolveBandStateParams(cfg.BandStateOverrides, band, state); ok {
		if params.FrequencyToleranceHz > 0 {
			freqToleranceHz = params.FrequencyToleranceHz
		}
	}

	return RuntimeSettings{
		Window:          window,
		MinReports:      minReports,
		FreqToleranceHz: freqToleranceHz,
	}
}

// BuildCorrectionSettings maps config/runtime inputs into resolver gate settings.
// Ownership: this is a pure mapper; it does not mutate shared state.
func BuildCorrectionSettings(in BuildSettingsInput) spot.CorrectionSettings {
	cfg := in.Cfg
	minUnique := cfg.RecentBandRecordMinUniqueSpotters
	if cfg.CustomSCP.Enabled {
		minUnique = max(minUnique, cfg.CustomSCP.ResolverMinUniqueSpotters)
	}
	plus1MinUnique := cfg.ResolverRecentPlus1MinUniqueWinner
	if cfg.CustomSCP.Enabled {
		plus1MinUnique = max(plus1MinUnique, minUnique)
	}
	return spot.CorrectionSettings{
		MinConsensusReports: in.MinReports,
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          cfg.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
		SlashPrecedenceMinReports: cfg.FamilyPolicy.SlashPrecedenceMinReports,
		MinAdvantage:              cfg.MinAdvantage,
		TruncationAdvantagePolicy: spot.CorrectionTruncationAdvantagePolicy{
			Configured:                true,
			Enabled:                   cfg.FamilyPolicy.Truncation.RelaxAdvantage.Enabled,
			MinAdvantage:              cfg.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage,
			RequireCandidateValidated: cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated,
			RequireSubjectUnvalidated: cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated,
		},
		MinConfidencePercent:              cfg.MinConfidencePercent,
		MaxEditDistance:                   cfg.MaxEditDistance,
		RecencyWindow:                     in.Window,
		DistanceModelCW:                   cfg.DistanceModelCW,
		DistanceModelRTTY:                 cfg.DistanceModelRTTY,
		Distance3ExtraReports:             cfg.Distance3ExtraReports,
		Distance3ExtraAdvantage:           cfg.Distance3ExtraAdvantage,
		Distance3ExtraConfidence:          cfg.Distance3ExtraConfidence,
		FreqGuardMinSeparationKHz:         cfg.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:            cfg.FreqGuardRunnerUpRatio,
		FrequencyToleranceHz:              in.FreqToleranceHz,
		RecentBandRecordMinUniqueSpotters: minUnique,
		RecentBandStore:                   in.RecentBandStore,
		RecentBandCustomSCPEnabled:        cfg.CustomSCP.Enabled,
		RecentBandCustomSCPRequirements: spot.CustomSCPRequirements{
			MinScore: cfg.CustomSCP.ResolverMinScore, MinH3Cells: cfg.CustomSCP.ResolverMinUniqueH3Cells,
		},
		ResolverRecentPlus1Enabled:              cfg.ResolverRecentPlus1Enabled,
		ResolverRecentPlus1MinUniqueWinner:      plus1MinUnique,
		ResolverRecentPlus1RequireSubjectWeaker: cfg.ResolverRecentPlus1RequireSubjectWeaker,
		ResolverRecentPlus1MaxDistance:          cfg.ResolverRecentPlus1MaxDistance,
		ResolverRecentPlus1AllowTruncation:      cfg.ResolverRecentPlus1AllowTruncation,
		BayesBonusPolicy: spot.CorrectionBayesBonusPolicy{
			Configured:                              true,
			Enabled:                                 cfg.BayesBonus.Enabled,
			WeightDistance1Milli:                    cfg.BayesBonus.WeightDistance1Milli,
			WeightDistance2Milli:                    cfg.BayesBonus.WeightDistance2Milli,
			WeightedSmoothingMilli:                  cfg.BayesBonus.WeightedSmoothingMilli,
			RecentSmoothing:                         cfg.BayesBonus.RecentSmoothing,
			ObsLogCapMilli:                          cfg.BayesBonus.ObsLogCapMilli,
			PriorLogMinMilli:                        cfg.BayesBonus.PriorLogMinMilli,
			PriorLogMaxMilli:                        cfg.BayesBonus.PriorLogMaxMilli,
			ReportThresholdDistance1Milli:           cfg.BayesBonus.ReportThresholdDistance1Milli,
			ReportThresholdDistance2Milli:           cfg.BayesBonus.ReportThresholdDistance2Milli,
			AdvantageThresholdDistance1Milli:        cfg.BayesBonus.AdvantageThresholdDistance1Milli,
			AdvantageThresholdDistance2Milli:        cfg.BayesBonus.AdvantageThresholdDistance2Milli,
			AdvantageMinWeightedDeltaDistance1Milli: cfg.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli,
			AdvantageMinWeightedDeltaDistance2Milli: cfg.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli,
			AdvantageExtraConfidenceDistance1:       cfg.BayesBonus.AdvantageExtraConfidenceDistance1,
			AdvantageExtraConfidenceDistance2:       cfg.BayesBonus.AdvantageExtraConfidenceDistance2,
			RequireCandidateValidated:               cfg.BayesBonus.RequireCandidateValidated,
			RequireSubjectUnvalidatedDistance2:      cfg.BayesBonus.RequireSubjectUnvalidatedDistance2,
		},
		TruncationLengthBonusEnabled:                   cfg.FamilyPolicy.Truncation.LengthBonus.Enabled,
		TruncationLengthBonusMax:                       cfg.FamilyPolicy.Truncation.LengthBonus.Max,
		TruncationLengthBonusRequireCandidateValidated: cfg.FamilyPolicy.Truncation.LengthBonus.RequireCandidateValidated,
		TruncationLengthBonusRequireSubjectUnvalidated: cfg.FamilyPolicy.Truncation.LengthBonus.RequireSubjectUnvalidated,
		TruncationDelta2RailsEnabled:                   cfg.FamilyPolicy.Truncation.Delta2Rails.Enabled,
		TruncationDelta2ExtraConfidence:                cfg.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent,
		TruncationDelta2RequireCandidateValidated:      cfg.FamilyPolicy.Truncation.Delta2Rails.RequireCandidateValidated,
		TruncationDelta2RequireSubjectUnvalidated:      cfg.FamilyPolicy.Truncation.Delta2Rails.RequireSubjectUnvalidated,
	}
}

// NormalizedDXCall returns normalized DX call from DXCallNorm or DXCall fallback.
func NormalizedDXCall(s *spot.Spot) string {
	if s == nil {
		return ""
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	return spot.NormalizeSpotDXCallsign(call)
}

// BuildResolverEvidenceSnapshot creates immutable resolver evidence from one spot.
// It only emits evidence for correction-candidate modes with valid normalized call,
// reporter, and band identity.
func BuildResolverEvidenceSnapshot(spotEntry *spot.Spot, cfg config.CallCorrectionConfig, adaptive *spot.AdaptiveMinReports, now time.Time) (spot.ResolverEvidence, bool) {
	if spotEntry == nil || !cfg.Enabled {
		return spot.ResolverEvidence{}, false
	}
	mode := spotEntry.ModeNorm
	if mode == "" {
		mode = spotEntry.Mode
	}
	mode = strutil.NormalizeUpper(mode)
	if !spot.IsCallCorrectionCandidate(mode) {
		return spot.ResolverEvidence{}, false
	}

	call := NormalizedDXCall(spotEntry)
	reporter := spotEntry.DECallNorm
	if reporter == "" {
		reporter = spotEntry.DECall
	}
	reporter = strutil.NormalizeUpper(reporter)
	if call == "" || reporter == "" {
		return spot.ResolverEvidence{}, false
	}

	band := spotEntry.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(spotEntry.Frequency)
	}
	band = spot.NormalizeBand(band)
	if band == "" || band == "???" {
		return spot.ResolverEvidence{}, false
	}

	runtime := ResolveRuntimeSettings(cfg, spotEntry, adaptive, now, false)
	key := spot.NewResolverSignalKey(spotEntry.Frequency, band, mode, runtime.FreqToleranceHz)

	return spot.ResolverEvidence{
		ObservedAt:    now,
		Key:           key,
		DXCall:        call,
		Spotter:       reporter,
		Report:        spotEntry.Report,
		HasReport:     spotEntry.HasReport,
		FrequencyKHz:  spotEntry.Frequency,
		RecencyWindow: runtime.Window,
	}, true
}

// resolveBandStateParams selects per-band state override values.
// Matching is case-insensitive on band labels and returns false when no override matches.
func resolveBandStateParams(overrides []config.BandStateOverride, band, state string) (bandStateParams, bool) {
	b := strings.ToLower(strings.TrimSpace(band))
	if b == "" || len(overrides) == 0 {
		return bandStateParams{}, false
	}
	stateKey := strings.ToLower(strings.TrimSpace(state))
	for _, o := range overrides {
		for _, candidate := range o.Bands {
			if strings.ToLower(strings.TrimSpace(candidate)) != b {
				continue
			}
			switch stateKey {
			case "quiet":
				return bandStateParams{
					FrequencyToleranceHz: o.Quiet.FrequencyToleranceHz,
				}, true
			case "busy":
				return bandStateParams{
					FrequencyToleranceHz: o.Busy.FrequencyToleranceHz,
				}, true
			default:
				return bandStateParams{
					FrequencyToleranceHz: o.Normal.FrequencyToleranceHz,
				}, true
			}
		}
	}
	return bandStateParams{}, false
}

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}
