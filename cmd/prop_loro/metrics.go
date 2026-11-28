// File role: Bounded O(1)-memory streaming metric accumulators for head-to-head
// SNR accuracy, per-mode T_mode residual bias, glyph-tier agreement, uncertainty
// calibration, path-hour empirical p50 aggregates, and active-pair Rel ROC-AUC/Brier.
package main

import (
	"math"
	"sort"

	"dxcluster/pathreliability"
)

const (
	// Fixed 0.5 dB histogram over [-100.0, +100.0] dB (401 bins) for bounded O(1)
	// median and p90 quantile computation across tens of millions of spots.
	histMinDB  = -100.0
	histMaxDB  = 100.0
	histStepDB = 0.5
	histBins   = 401

	relAUCBins   = 100
	relCalibBins = 10
)

type dbHist struct {
	bins  [histBins]uint64
	total uint64
}

func (h *dbHist) add(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	idx := int(math.Round((v - histMinDB) / histStepDB))
	if idx < 0 {
		idx = 0
	} else if idx >= histBins {
		idx = histBins - 1
	}
	h.bins[idx]++
	h.total++
}

func (h *dbHist) quantile(q float64) float64 {
	if h.total == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(h.total)))
	if target < 1 {
		target = 1
	}
	var acc uint64
	for i, c := range h.bins {
		acc += c
		if acc >= target {
			return histMinDB + float64(i)*histStepDB
		}
	}
	return histMaxDB
}

type modelStatsSummary struct {
	Count            uint64  `json:"count"`
	MAE              float64 `json:"mae_db"`
	RMSE             float64 `json:"rmse_db"`
	MeanBias         float64 `json:"mean_bias_db"`
	MedianBias       float64 `json:"median_bias_db"`
	P90AbsErr        float64 `json:"p90_abs_err_db"`
	RawMeanResidual  float64 `json:"raw_minus_pred_mean_db"`
	RawMedResidual   float64 `json:"raw_minus_pred_median_db"`
	FalseClosedRate  float64 `json:"false_closed_rate"`
	FalseClosedCount uint64  `json:"false_closed_count"`
	ExactTierAcc     float64 `json:"exact_tier_acc"`
	Within1TierAcc   float64 `json:"within_1_tier_acc"`
	EmittedRate      float64 `json:"emitted_rate"`
	MeanSigma        float64 `json:"mean_sigma_db,omitempty"`
	ZAbsLE1Rate      float64 `json:"z_abs_le_1_rate,omitempty"`
	ZAbsLE2Rate      float64 `json:"z_abs_le_2_rate,omitempty"`
}

type modelAccum struct {
	count           uint64
	sumAbsErr       float64
	sumSqErr        float64
	sumBias         float64
	sumRawResidual  float64
	biasHist        dbHist
	absErrHist      dbHist
	rawResidualHist dbHist
	falseClosed     uint64
	emittedCount    uint64
	exactTier       uint64
	within1Tier     uint64
	sigmaCount      uint64
	sumSigma        float64
	zLE1            uint64
	zLE2            uint64
}

func (a *modelAccum) add(
	obsFT8DB float64,
	obsRawSNRDB float64,
	obsClass string,
	predRxFT8DB float64,
	predClass string,
	predClosed bool,
	predEmitted bool,
	sigmaDB float64,
) {
	a.count++
	errDB := obsFT8DB - predRxFT8DB
	absErr := math.Abs(errDB)
	a.sumAbsErr += absErr
	a.sumSqErr += errDB * errDB
	a.sumBias += errDB
	a.biasHist.add(errDB)
	a.absErrHist.add(absErr)

	rawRes := obsRawSNRDB - predRxFT8DB
	a.sumRawResidual += rawRes
	a.rawResidualHist.add(rawRes)

	if predClosed {
		a.falseClosed++
	}
	if predEmitted {
		a.emittedCount++
		obsRank := pathreliability.ClassStrengthRank(obsClass)
		predRank := pathreliability.ClassStrengthRank(predClass)
		if obsRank >= 0 && predRank >= 0 {
			diff := obsRank - predRank
			if diff < 0 {
				diff = -diff
			}
			if diff == 0 {
				a.exactTier++
				a.within1Tier++
			} else if diff <= 1 {
				a.within1Tier++
			}
		}
	}
	if sigmaDB > 0 && !math.IsNaN(sigmaDB) && !math.IsInf(sigmaDB, 0) {
		a.sigmaCount++
		a.sumSigma += sigmaDB
		z := absErr / sigmaDB
		if z <= 1.0 {
			a.zLE1++
		}
		if z <= 2.0 {
			a.zLE2++
		}
	}
}

func (a *modelAccum) summary() modelStatsSummary {
	if a == nil || a.count == 0 {
		return modelStatsSummary{}
	}
	n := float64(a.count)
	out := modelStatsSummary{
		Count:            a.count,
		MAE:              round3(a.sumAbsErr / n),
		RMSE:             round3(math.Sqrt(a.sumSqErr / n)),
		MeanBias:         round3(a.sumBias / n),
		MedianBias:       round3(a.biasHist.quantile(0.50)),
		P90AbsErr:        round3(a.absErrHist.quantile(0.90)),
		RawMeanResidual:  round3(a.sumRawResidual / n),
		RawMedResidual:   round3(a.rawResidualHist.quantile(0.50)),
		FalseClosedRate:  round4(float64(a.falseClosed) / n),
		FalseClosedCount: a.falseClosed,
		EmittedRate:      round4(float64(a.emittedCount) / n),
	}
	if a.emittedCount > 0 {
		en := float64(a.emittedCount)
		out.ExactTierAcc = round4(float64(a.exactTier) / en)
		out.Within1TierAcc = round4(float64(a.within1Tier) / en)
	}
	if a.sigmaCount > 0 {
		sn := float64(a.sigmaCount)
		out.MeanSigma = round3(a.sumSigma / sn)
		out.ZAbsLE1Rate = round4(float64(a.zLE1) / sn)
		out.ZAbsLE2Rate = round4(float64(a.zLE2) / sn)
	}
	return out
}

type headToHeadSummary struct {
	MatchedCount uint64            `json:"matched_count"`
	PropPrior    modelStatsSummary `json:"propprior"`
	VOACAP       modelStatsSummary `json:"voacap"`
	DeltaMAE     float64           `json:"delta_mae_pp_minus_voacap_db"`
	DeltaRMSE    float64           `json:"delta_rmse_pp_minus_voacap_db"`
	DeltaFC      float64           `json:"delta_false_closed_pp_minus_voacap"`
}

type headToHeadAccum struct {
	pp     modelAccum
	voacap modelAccum
}

func (h *headToHeadAccum) summary() headToHeadSummary {
	if h == nil || h.pp.count == 0 {
		return headToHeadSummary{}
	}
	ps := h.pp.summary()
	vs := h.voacap.summary()
	return headToHeadSummary{
		MatchedCount: ps.Count,
		PropPrior:    ps,
		VOACAP:       vs,
		DeltaMAE:     round3(ps.MAE - vs.MAE),
		DeltaRMSE:    round3(ps.RMSE - vs.RMSE),
		DeltaFC:      round4(ps.FalseClosedRate - vs.FalseClosedRate),
	}
}

type fallbackOutcomeCounts struct {
	Total         uint64 `json:"total"`
	Open          uint64 `json:"open"`
	Closed        uint64 `json:"closed"`
	Aligned       uint64 `json:"aligned"`
	SparseUpgrade uint64 `json:"sparse_upgrade"`
	Insufficient  uint64 `json:"insufficient"`
}

func (c *fallbackOutcomeCounts) record(src pathreliability.PredictionSource) {
	c.Total++
	switch src {
	case pathreliability.SourceVOACAPOpen:
		c.Open++
	case pathreliability.SourceVOACAPClosed:
		c.Closed++
	case pathreliability.SourceVOACAPAligned:
		c.Aligned++
	case pathreliability.SourceVOACAPSparseUpgrade:
		c.SparseUpgrade++
	default:
		c.Insufficient++
	}
}

type relCalibBinSummary struct {
	BinLow       float64 `json:"bin_low"`
	BinHigh      float64 `json:"bin_high"`
	Count        uint64  `json:"count"`
	MeanPredRel  float64 `json:"mean_pred_rel"`
	ObservedRate float64 `json:"observed_heard_rate"`
}

type relCalibSummary struct {
	Positives       uint64               `json:"positives"`
	Negatives       uint64               `json:"negatives"`
	ROCAUC          float64              `json:"roc_auc"`
	BrierScore      float64              `json:"brier_score"`
	MeanRelPos      float64              `json:"mean_rel_pos"`
	MeanRelNeg      float64              `json:"mean_rel_neg"`
	FalseOpenGE50   float64              `json:"false_open_rel_ge_050_rate"`
	FalseOpenGE70   float64              `json:"false_open_rel_ge_070_rate"`
	CalibrationBins []relCalibBinSummary `json:"calibration_bins"`
}

type relCalibAccum struct {
	posBins        [relAUCBins + 1]uint64
	negBins        [relAUCBins + 1]uint64
	posCount       uint64
	negCount       uint64
	sumPosRel      float64
	sumNegRel      float64
	sumBrier       float64
	negGE50        uint64
	negGE70        uint64
	calibCount     [relCalibBins]uint64
	calibPos       [relCalibBins]uint64
	calibSumPred   [relCalibBins]float64
}

func (r *relCalibAccum) add(rel float64, positive bool) {
	if math.IsNaN(rel) || math.IsInf(rel, 0) {
		return
	}
	if rel < 0 {
		rel = 0
	} else if rel > 1 {
		rel = 1
	}
	aucIdx := int(math.Round(rel * float64(relAUCBins)))
	if aucIdx < 0 {
		aucIdx = 0
	} else if aucIdx > relAUCBins {
		aucIdx = relAUCBins
	}
	y := 0.0
	if positive {
		y = 1.0
		r.posCount++
		r.posBins[aucIdx]++
		r.sumPosRel += rel
	} else {
		r.negCount++
		r.negBins[aucIdx]++
		r.sumNegRel += rel
		if rel >= 0.50 {
			r.negGE50++
		}
		if rel >= 0.70 {
			r.negGE70++
		}
	}
	diff := rel - y
	r.sumBrier += diff * diff

	cIdx := int(math.Floor(rel * float64(relCalibBins)))
	if cIdx >= relCalibBins {
		cIdx = relCalibBins - 1
	}
	if cIdx < 0 {
		cIdx = 0
	}
	r.calibCount[cIdx]++
	if positive {
		r.calibPos[cIdx]++
	}
	r.calibSumPred[cIdx] += rel
}

func (r *relCalibAccum) summary() relCalibSummary {
	total := r.posCount + r.negCount
	if total == 0 {
		return relCalibSummary{}
	}
	out := relCalibSummary{
		Positives:  r.posCount,
		Negatives:  r.negCount,
		BrierScore: round4(r.sumBrier / float64(total)),
	}
	if r.posCount > 0 {
		out.MeanRelPos = round4(r.sumPosRel / float64(r.posCount))
	}
	if r.negCount > 0 {
		out.MeanRelNeg = round4(r.sumNegRel / float64(r.negCount))
		out.FalseOpenGE50 = round4(float64(r.negGE50) / float64(r.negCount))
		out.FalseOpenGE70 = round4(float64(r.negGE70) / float64(r.negCount))
	}
	if r.posCount > 0 && r.negCount > 0 {
		// Exact Mann-Whitney U over the 101-bin discretized probability score.
		var cumNegBelow uint64
		var concordant float64
		for i := 0; i <= relAUCBins; i++ {
			if r.posBins[i] > 0 {
				concordant += float64(r.posBins[i]) * (float64(cumNegBelow) + 0.5*float64(r.negBins[i]))
			}
			cumNegBelow += r.negBins[i]
		}
		out.ROCAUC = round4(concordant / (float64(r.posCount) * float64(r.negCount)))
	}
	bins := make([]relCalibBinSummary, 0, relCalibBins)
	for i := range relCalibBins {
		if r.calibCount[i] == 0 {
			continue
		}
		cn := float64(r.calibCount[i])
		bins = append(bins, relCalibBinSummary{
			BinLow:       round2(float64(i) / float64(relCalibBins)),
			BinHigh:      round2(float64(i+1) / float64(relCalibBins)),
			Count:        r.calibCount[i],
			MeanPredRel:  round4(r.calibSumPred[i] / cn),
			ObservedRate: round4(float64(r.calibPos[i]) / cn),
		})
	}
	out.CalibrationBins = bins
	return out
}

type pathHourKey struct {
	deCell pathreliability.CellID
	dxCell pathreliability.CellID
	band   string
}

type pathHourGroup struct {
	obsFT8      []float64
	ppRxSum     float64
	ppSigmaSum  float64
	voacapRxSum float64
	count       int
	mode        string
}

type native160Summary struct {
	TotalHeldOut uint64 `json:"total_held_out"`
	LiveCombined uint64 `json:"live_combined"`
	EmittedLow   uint64 `json:"native160_low"`
	EmittedUnlk  uint64 `json:"native160_unlikely"`
	EmittedClose uint64 `json:"native160_closed_false_closed"`
	Insufficient uint64 `json:"insufficient"`
}

func medianFloat64(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := make([]float64, len(vals))
	copy(cp, vals)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return 0.5 * (cp[mid-1] + cp[mid])
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
func round4(v float64) float64 { return math.Round(v*10000) / 10000 }
