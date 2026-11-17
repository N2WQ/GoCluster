// File role: Scores only the resolver's top raw-and-weighted tied cohort.
// Receiver weights are reduced in integer SNR bins once per observation, and
// call alignment is reused across bins. All scratch is evaluation-owned.
package spot

import (
	"math"
	"sort"
	"strings"

	"dxcluster/strutil"
)

func applyResolverConfusionTieBreak(ranked []rankedResolverCandidate, mode string, model *ConfusionModel, weight float64, workspace *confusionAlignmentWorkspace) {
	if len(ranked) < 2 || model == nil || weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		return
	}
	tieEnd := 1
	top := ranked[0]
	for tieEnd < len(ranked) {
		next := ranked[tieEnd]
		if top.replacementBlocked || next.replacementBlocked || next.weightedSupportMilli != top.weightedSupportMilli || next.support != top.support {
			break
		}
		tieEnd++
	}
	if tieEnd <= 1 {
		return
	}
	cohort := ranked[:tieEnd]
	if workspace == nil {
		workspace = &confusionAlignmentWorkspace{}
	}
	modeIndex, weights, ok := prepareResolverConfusionBins(cohort, mode, model, workspace)
	if !ok {
		return
	}
	for i := range cohort {
		score := scoreResolverConfusionCohort(cohort[i], cohort, model, modeIndex, weights, workspace)
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

func prepareResolverConfusionBins(cohort []rankedResolverCandidate, mode string, model *ConfusionModel, workspace *confusionAlignmentWorkspace) (int, []int64, bool) {
	modeIndex, ok := model.modeIndex[strutil.NormalizeUpper(mode)]
	if !ok {
		return 0, nil, false
	}
	bins := len(model.snrEdges) - 1
	count := len(cohort) * bins
	if cap(workspace.binWeights) < count {
		workspace.binWeights = make([]int64, count)
	} else {
		workspace.binWeights = workspace.binWeights[:count]
		clear(workspace.binWeights)
	}
	for i := range cohort {
		for _, row := range cohort[i].receivers {
			if !row.HasReport || row.SNRConflict || row.WeightMilli <= 0 {
				continue
			}
			if bin := model.snrBandIndex(float64(row.Report)); bin >= 0 {
				workspace.binWeights[i*bins+bin] += int64(row.WeightMilli)
			}
		}
	}
	return modeIndex, workspace.binWeights, true
}

func resolverConfusionAggregateScore(candidate rankedResolverCandidate, cohort []rankedResolverCandidate, mode string, model *ConfusionModel, workspace *confusionAlignmentWorkspace) float64 {
	if model == nil || len(cohort) <= 1 {
		return 0
	}
	if workspace == nil {
		workspace = &confusionAlignmentWorkspace{}
	}
	modeIndex, weights, ok := prepareResolverConfusionBins(cohort, mode, model, workspace)
	if !ok {
		return 0
	}
	return scoreResolverConfusionCohort(candidate, cohort, model, modeIndex, weights, workspace)
}

func scoreResolverConfusionCohort(candidate rankedResolverCandidate, cohort []rankedResolverCandidate, model *ConfusionModel, modeIndex int, weights []int64, workspace *confusionAlignmentWorkspace) float64 {
	if len(candidate.callRunes) == 0 {
		return 0
	}
	bins := len(model.snrEdges) - 1
	score := 0.0
	for i := range cohort {
		observed := &cohort[i]
		if len(observed.callRunes) == 0 || strings.EqualFold(resolverRankIdentity(*observed), resolverRankIdentity(candidate)) {
			continue
		}
		rowWeights := weights[i*bins : (i+1)*bins]
		anyMeasured := false
		for _, weight := range rowWeights {
			if weight > 0 {
				anyMeasured = true
				break
			}
		}
		if !anyMeasured {
			continue
		}
		trueRunes, observedRunes := candidate.callRunes, observed.callRunes
		if candidate.identity.VoteKey == "" && strings.Contains(candidate.call, "/") {
			trueRunes = []rune(resolverRankIdentity(candidate))
		}
		if observed.identity.VoteKey == "" && strings.Contains(observed.call, "/") {
			observedRunes = []rune(resolverRankIdentity(*observed))
		}
		ops := alignConfusionCallsWithWorkspace(trueRunes, observedRunes, workspace)
		// Ascending bin reduction is deterministic, including multiple SNRs on
		// the same call. Own matches contribute zero; zero weights stay zero.
		for bin, weight := range rowWeights {
			if weight > 0 {
				score += model.scoreAlignedConfusion(ops, modeIndex, bin) * float64(weight)
			}
		}
	}
	return score
}
