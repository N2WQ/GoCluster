// File role: Single-pass dual-prior LORO engine scoring v4.3 internal/propprior
// head-to-head against VOACAP over one captured weekend stream across Regime A
// (pure prior) and Regime B (exact production live Predictor + fallback tree).
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"dxcluster/internal/propprior"
	"dxcluster/pathreliability"
)

const (
	loroPurgeIntervalSec   int64 = 300
	loroMinPathHourSamples int   = 3
	loroMaxNegPerBandHour  int   = 500
)

type loroConfig struct {
	HoldoutUnit     string // "receiver" or "fine_cell"
	Folds           int
	Fold            int
	WarmupSeconds   int64
	ExcludeColdSW   bool
	VOACAPSSNFixed  int
	MaxNegBandHour  int
	ProgressEvery   time.Duration
}

type loroManifest struct {
	CaptureDir        string          `json:"capture_dir"`
	SpotFiles         []string        `json:"spot_files"`
	SpaceWeatherFiles []string        `json:"spaceweather_files"`
	HoldoutUnit       string          `json:"holdout_unit"`
	Folds             int             `json:"folds"`
	Fold              int             `json:"fold"`
	WarmupSeconds     int64           `json:"warmup_seconds"`
	ExcludeColdSW     bool            `json:"exclude_cold_sw"`
	VOACAPSSNFixed    int             `json:"voacap_ssn_override,omitempty"`
	FirstObservedUTC  string          `json:"first_observed_utc,omitempty"`
	LastObservedUTC   string          `json:"last_observed_utc,omitempty"`
	TotalRowsRead     int64           `json:"total_rows_read"`
	BadRowsSkipped    int64           `json:"bad_rows_skipped"`
	WarmupSkipped     int64           `json:"warmup_skipped"`
	ColdSWSkipped     int64           `json:"cold_sw_skipped"`
	NoSWSkipped       int64           `json:"no_sw_skipped"`
	LiveSWScored      int64           `json:"live_sw_scored"`
	TrainUpdates      int64           `json:"train_predictor_updates"`
	HeldOutTotal      int64           `json:"held_out_total"`
	HeldOutHFMatched  int64           `json:"held_out_hf_matched"`
	HeldOutUnmatched  int64           `json:"held_out_hf_unmatched"`
	PropPriorCRC32    string          `json:"propprior_weights_crc32"`
	VOACAPStats       voacapEvalStats `json:"voacap_stats"`
	ElapsedSeconds    float64         `json:"elapsed_seconds"`
}

type loroScorecard struct {
	Manifest             loroManifest                   `json:"manifest"`
	RegimeAOverall       headToHeadSummary              `json:"regime_a_pure_prior_overall"`
	RegimeAExactGrid     headToHeadSummary              `json:"regime_a_exact_grid"`
	RegimeADerivedGrid   headToHeadSummary              `json:"regime_a_derived_grid"`
	RegimeAPathHourP50   headToHeadSummary              `json:"regime_a_path_hour_p50_ge3"`
	RegimeBCombined      headToHeadSummary              `json:"regime_b_live_combined"`
	RegimeBFallbackAll   headToHeadSummary              `json:"regime_b_fallback_all"`
	RegimeBFallbackNoP50 headToHeadSummary              `json:"regime_b_fallback_no_p50"`
	RegimeBFallbackSpP50 headToHeadSummary              `json:"regime_b_fallback_sparse_p50"`
	RegimeBPPOutcomes    fallbackOutcomeCounts          `json:"regime_b_propprior_fallback_outcomes"`
	RegimeBVOOutcomes    fallbackOutcomeCounts          `json:"regime_b_voacap_fallback_outcomes"`
	ByMode                 map[string]headToHeadSummary `json:"by_mode"`
	ByModeFallbackOnly     map[string]headToHeadSummary `json:"by_mode_fallback_only"`
	ByModeNonPolar         map[string]headToHeadSummary `json:"by_mode_non_polar"`
	ByModeNonPolarNAEU     map[string]headToHeadSummary `json:"by_mode_non_polar_na_eu"`
	ByBand                 map[string]headToHeadSummary `json:"by_band"`
	ByBandDayNight         map[string]headToHeadSummary `json:"by_band_day_night"`
	LongHaulPolar          map[string]headToHeadSummary `json:"long_haul_and_polar"`
	LongHaulPolarFBOnly    map[string]headToHeadSummary `json:"long_haul_and_polar_fallback_only"`
	GeomagCliff            map[string]headToHeadSummary `json:"geomag_cliff"`
	GeomagCliff20m         map[string]headToHeadSummary `json:"geomag_cliff_20m"`
	NASACAHighBand         map[string]headToHeadSummary `json:"na_sa_ca_high_band"`
	Native160m             native160Summary             `json:"native_160m"`
	RelCalibrationPP       relCalibSummary              `json:"rel_calibration_propprior"`
	RelCalibrationVO       relCalibSummary              `json:"rel_calibration_voacap"`
	RelCalibrationByBandPP map[string]relCalibSummary   `json:"rel_calibration_by_band_propprior"`
	RelCalibrationByBandVO map[string]relCalibSummary   `json:"rel_calibration_by_band_voacap"`
	RelCalibrationNASACAPP map[string]relCalibSummary   `json:"rel_calibration_na_sa_ca_propprior"`
	RelCalibrationNASACAVO map[string]relCalibSummary   `json:"rel_calibration_na_sa_ca_voacap"`
}

type ppCacheKey struct {
	loCell     pathreliability.CellID
	hiCell     pathreliability.CellID
	bandIdx    uint8
	timeBucket int64
	swGen      uint64
}

type ppCacheVal struct {
	snrTotal   float64
	sigmaTotal float64
}

type activeEndpoint struct {
	cell pathreliability.CellID
	lat  float64
	lon  float64
}

type bandHourActivity struct {
	rxByCell map[pathreliability.CellID]activeEndpoint
	txByCell map[pathreliability.CellID]activeEndpoint
	heard    map[uint64]struct{}
	lastSW   swSnapshot
	hasSW    bool
	lastUnix int64
}

type loroEngine struct {
	cfg           loroConfig
	pathCfg       pathreliability.Config
	ppCfg         pathreliability.Config
	voacapCfg     pathreliability.Config
	ppFB          pathreliability.PropPriorFallbackConfig
	ppBandFreq    [hfBandCount]float64
	ppModel       *propprior.Model
	voacapEval    voacapEvaluator
	timeline      *swTimeline
	predictor     *pathreliability.Predictor
	allowedBands  map[string]struct{}

	startedWall   time.Time
	lastProgress  time.Time
	firstObsUnix  int64
	lastObsUnix   int64
	lastPurgeUnix int64
	currentHour   int64
	hasCurrentHr  bool

	// Per-hour bounded caches flushed at UTC hour rollover.
	ppCache     map[ppCacheKey]ppCacheVal
	pathHourAgg map[pathHourKey]*pathHourGroup
	activity    map[string]*bandHourActivity

	manifest           loroManifest
	regimeAAll         headToHeadAccum
	regimeAExact       headToHeadAccum
	regimeADerived     headToHeadAccum
	regimeAP50         headToHeadAccum
	regimeBCombined    headToHeadAccum
	regimeBFBAll       headToHeadAccum
	regimeBFBNoP50     headToHeadAccum
	regimeBFBSparseP50 headToHeadAccum
	regimeBPPOut       fallbackOutcomeCounts
	regimeBVOOut       fallbackOutcomeCounts
	byMode             map[string]*headToHeadAccum
	byModeFB           map[string]*headToHeadAccum
	byModeNonPolar     map[string]*headToHeadAccum
	byModeNonPolarNAEU map[string]*headToHeadAccum
	byBand             map[string]*headToHeadAccum
	byBandSolar        map[string]*headToHeadAccum
	longHaulPolar      map[string]*headToHeadAccum
	longHaulPolarFB    map[string]*headToHeadAccum
	geomagCliff        map[string]*headToHeadAccum
	geomagCliff20m     map[string]*headToHeadAccum
	naSACAHighBand     map[string]*headToHeadAccum
	native160          native160Summary
	relPP              relCalibAccum
	relVO              relCalibAccum
	relByBandPP        map[string]*relCalibAccum
	relByBandVO        map[string]*relCalibAccum
	relNASACAPP        map[string]*relCalibAccum
	relNASACAVO        map[string]*relCalibAccum
}

func newLOROEngine(
	cfg loroConfig,
	basePathCfg pathreliability.Config,
	model *propprior.Model,
	voacapEval voacapEvaluator,
	timeline *swTimeline,
) (*loroEngine, error) {
	if cfg.Folds <= 0 {
		cfg.Folds = 5
	}
	if cfg.Fold < 0 || cfg.Fold >= cfg.Folds {
		return nil, fmt.Errorf("invalid fold %d for k=%d", cfg.Fold, cfg.Folds)
	}
	if cfg.HoldoutUnit == "" {
		cfg.HoldoutUnit = "receiver"
	}
	if cfg.HoldoutUnit != "receiver" && cfg.HoldoutUnit != "fine_cell" {
		return nil, fmt.Errorf("unsupported holdout-unit %q (want receiver or fine_cell)", cfg.HoldoutUnit)
	}
	if cfg.MaxNegBandHour <= 0 {
		cfg.MaxNegBandHour = loroMaxNegPerBandHour
	}

	// Build two exact production-calibrated config views from the loaded YAML:
	// ppCfg exercises prop_prior_fallback REL thresholds (0.70/0.50/0.35/0.25);
	// voacapCfg exercises voacap_fallback REL thresholds (0.85/0.75/0.60/0.50).
	ppCfg := basePathCfg
	ppFB := pathreliability.DefaultPropPriorFallbackConfig()
	if basePathCfg.PropPriorFallback != nil {
		ppFB = *basePathCfg.PropPriorFallback
	}
	ppFB.Enabled = true
	ppCfg.PropPriorFallback = &ppFB
	ppCfg.VOACAPFallback.Enabled = false

	voacapCfg := basePathCfg
	voacapCfg.PropPriorFallback = nil
	voacapCfg.VOACAPFallback.Enabled = true

	var bandFreq [hfBandCount]float64
	for _, f := range ppFB.CenterFrequenciesMHz {
		if idx, ok := bandForCenterMHz(f); ok {
			bandFreq[idx] = f
		}
	}
	allowed := make(map[string]struct{}, len(basePathCfg.AllowedBands))
	for _, b := range basePathCfg.AllowedBands {
		allowed[b] = struct{}{}
	}

	return &loroEngine{
		cfg:             cfg,
		pathCfg:         basePathCfg,
		ppCfg:           ppCfg,
		voacapCfg:       voacapCfg,
		ppFB:            ppFB,
		ppBandFreq:      bandFreq,
		ppModel:         model,
		voacapEval:      voacapEval,
		timeline:        timeline,
		predictor:       pathreliability.NewPredictor(basePathCfg, basePathCfg.AllowedBands),
		allowedBands:    allowed,
		startedWall:     time.Now(),
		lastProgress:    time.Now(),
		ppCache:         make(map[ppCacheKey]ppCacheVal, 4096),
		pathHourAgg:     make(map[pathHourKey]*pathHourGroup, 4096),
		activity:        make(map[string]*bandHourActivity, hfBandCount),
		byMode:             make(map[string]*headToHeadAccum),
		byModeFB:           make(map[string]*headToHeadAccum),
		byModeNonPolar:     make(map[string]*headToHeadAccum),
		byModeNonPolarNAEU: make(map[string]*headToHeadAccum),
		byBand:             make(map[string]*headToHeadAccum),
		byBandSolar:        make(map[string]*headToHeadAccum),
		longHaulPolar:      make(map[string]*headToHeadAccum),
		longHaulPolarFB:    make(map[string]*headToHeadAccum),
		geomagCliff:        make(map[string]*headToHeadAccum),
		geomagCliff20m:     make(map[string]*headToHeadAccum),
		naSACAHighBand:     make(map[string]*headToHeadAccum),
		relByBandPP:        make(map[string]*relCalibAccum),
		relByBandVO:        make(map[string]*relCalibAccum),
		relNASACAPP:        make(map[string]*relCalibAccum),
		relNASACAVO:        make(map[string]*relCalibAccum),
		manifest: loroManifest{
			HoldoutUnit:    cfg.HoldoutUnit,
			Folds:          cfg.Folds,
			Fold:           cfg.Fold,
			WarmupSeconds:  cfg.WarmupSeconds,
			ExcludeColdSW:  cfg.ExcludeColdSW,
			VOACAPSSNFixed: cfg.VOACAPSSNFixed,
			PropPriorCRC32: fmt.Sprintf("0x%08x", propprior.EmbeddedWeightsCRC32),
		},
	}, nil
}

func (e *loroEngine) processSpot(ctx context.Context, rec capturedSpot) error {
	if _, ok := e.allowedBands[rec.band]; !ok {
		return nil
	}
	deFine := pathreliability.EncodeCell(rec.deGrid)
	dxFine := pathreliability.EncodeCell(rec.dxGrid)
	deCoarse := pathreliability.EncodeCoarseCell(rec.deGrid)
	dxCoarse := pathreliability.EncodeCoarseCell(rec.dxGrid)
	if (deFine == pathreliability.InvalidCell || dxFine == pathreliability.InvalidCell) &&
		(deCoarse == pathreliability.InvalidCell || dxCoarse == pathreliability.InvalidCell) {
		return nil
	}

	if e.firstObsUnix == 0 {
		e.firstObsUnix = rec.observedUnix
		e.lastPurgeUnix = rec.observedUnix
	}
	if rec.observedUnix > e.lastObsUnix {
		e.lastObsUnix = rec.observedUnix
	}

	obsTime := time.Unix(rec.observedUnix, 0).UTC()
	arrTime := time.Unix(rec.arrivedUnix, 0).UTC()
	hourBucket := rec.observedUnix / 3600
	if !e.hasCurrentHr {
		e.currentHour = hourBucket
		e.hasCurrentHr = true
	} else if hourBucket > e.currentHour {
		if err := e.flushHour(ctx); err != nil {
			return err
		}
		e.currentHour = hourBucket
	}

	if rec.observedUnix-e.lastPurgeUnix >= loroPurgeIntervalSec {
		e.predictor.PurgeStale(obsTime)
		e.lastPurgeUnix = rec.observedUnix
	}

	heldOut := e.isHeldOut(rec.deHash, deFine)
	if !heldOut {
		// Train fold: feed production Predictor identically to live GoCluster.
		bucket := pathreliability.BucketForIngest(rec.mode)
		if bucket != pathreliability.BucketNone {
			adm := e.predictor.UpdateWithReceiverHash(
				bucket, deFine, dxFine, deCoarse, dxCoarse, rec.band, rec.ft8DB, 1.0,
				pathreliability.ObservationTimes{ObservedAt: obsTime, ArrivedAt: arrTime},
				rec.isBeacon, rec.deHash,
			)
			if adm.Accepted() {
				e.manifest.TrainUpdates++
			}
		}
		return nil
	}

	// Held-out receiver: NEVER inserted into e.predictor (strict LORO isolation).
	if e.cfg.WarmupSeconds > 0 && rec.observedUnix-e.firstObsUnix < e.cfg.WarmupSeconds {
		e.manifest.WarmupSkipped++
		return nil
	}
	e.manifest.HeldOutTotal++

	if rec.band == "160m" {
		e.score160m(rec, deFine, dxFine, deCoarse, dxCoarse, obsTime)
		return nil
	}

	bandIdx, ok := hfBandIndex(rec.band)
	if !ok || deFine == pathreliability.InvalidCell || dxFine == pathreliability.InvalidCell {
		return nil
	}

	sw, hasSW := e.timeline.lookupCausal(rec.observedUnix)
	if !hasSW {
		e.manifest.NoSWSkipped++
		return nil
	}
	if e.cfg.ExcludeColdSW && sw.Gen == 0 {
		e.manifest.ColdSWSkipped++
		return nil
	}
	e.manifest.LiveSWScored++

	userLat, userLon, okU := pathreliability.GridCenterLatLon(rec.deGrid)
	dxLat, dxLon, okD := pathreliability.GridCenterLatLon(rec.dxGrid)
	if !okU || !okD {
		return nil
	}

	ssn := sw.VOACAPSSN.SSN
	if e.cfg.VOACAPSSNFixed > 0 {
		ssn = e.cfg.VOACAPSSNFixed
	}

	ppForecast, ppSigma, ppOK := e.evalPropPrior(deFine, dxFine, userLat, userLon, dxLat, dxLon, bandIdx, obsTime, sw)
	voForecast, voOK, err := e.voacapEval.Forecast(ctx, deFine, dxFine, userLat, userLon, dxLat, dxLon, rec.band, obsTime, ssn, e.voacapCfg)
	if err != nil {
		// Individual VOACAP failure is tracked inside voacapEval.Stats(); keep scoring matched spots.
		voOK = false
	}
	if !ppOK || !voOK {
		e.manifest.HeldOutUnmatched++
		return nil
	}
	e.manifest.HeldOutHFMatched++

	// Record hourly endpoint activity for active-TX x active-RX negative sampling.
	e.recordActivity(rec.band, deFine, userLat, userLon, dxFine, dxLat, dxLon, sw, rec.observedUnix)

	obsClass := pathreliability.ClassForDB(rec.ft8DB, rec.mode, e.pathCfg)
	pf := computePathFeatures(userLat, userLon, dxLat, dxLon)
	solar := classifyMidpointSolar(pf.midLatDeg, pf.midLonDeg, obsTime)

	// Regime A: Pure Prior (calibrated production FT8-equiv receive-leg SNR + REL + class).
	ppRxDB := ppForecast.ReceiveDB()
	ppEffDB := ppForecast.EffectiveDB()
	if rec.isBeacon {
		ppEffDB = ppRxDB
	}
	ppClosedA := pathreliability.ClosedForDB(ppEffDB, rec.mode, e.ppCfg)
	ppClassA := pathreliability.ClassForDB(ppEffDB, rec.mode, e.ppCfg)
	ppRel, _ := ppForecast.ReceiveReqSNRReliability()

	voRxDB := voForecast.ReceiveDB()
	voEffDB := voForecast.EffectiveDB()
	if rec.isBeacon {
		voEffDB = voRxDB
	}
	voClosedA := pathreliability.ClosedForDB(voEffDB, rec.mode, e.voacapCfg)
	voClassA := pathreliability.ClassForDB(voEffDB, rec.mode, e.voacapCfg)
	voRel, _ := voForecast.ReceiveReqSNRReliability()

	addMatched := func(acc *headToHeadAccum) {
		acc.pp.add(rec.ft8DB, float64(rec.rawSNRDB), obsClass, ppRxDB, ppClassA, ppClosedA, true, ppSigma)
		acc.voacap.add(rec.ft8DB, float64(rec.rawSNRDB), obsClass, voRxDB, voClassA, voClosedA, true, 0)
	}

	addMatched(&e.regimeAAll)
	if !rec.deGridDerived && !rec.dxGridDerived {
		addMatched(&e.regimeAExact)
	} else {
		addMatched(&e.regimeADerived)
	}
	addMatched(getOrInitAccum(e.byMode, rec.mode))
	if !pf.isPolar {
		addMatched(getOrInitAccum(e.byModeNonPolar, rec.mode))
		if pf.isNAEU {
			addMatched(getOrInitAccum(e.byModeNonPolarNAEU, rec.mode))
		}
	}
	addMatched(getOrInitAccum(e.byBand, rec.band))
	addMatched(getOrInitAccum(e.byBandSolar, rec.band+"_"+string(solar)))

	addMatched(getOrInitAccum(e.geomagCliff, pf.geomagBin))
	if rec.band == "20m" {
		addMatched(getOrInitAccum(e.geomagCliff20m, pf.geomagBin))
	}
	if pf.isNorthNAEEU {
		addMatched(getOrInitAccum(e.geomagCliff, "north_na_eeu"))
		if rec.band == "20m" {
			addMatched(getOrInitAccum(e.geomagCliff20m, "north_na_eeu"))
		}
	}
	if pf.isMidNAEEU {
		addMatched(getOrInitAccum(e.geomagCliff, "mid_na_eeu"))
		if rec.band == "20m" {
			addMatched(getOrInitAccum(e.geomagCliff20m, "mid_na_eeu"))
		}
	}
	if pf.isNASACA && isHighBand(rec.band) {
		addMatched(getOrInitAccum(e.naSACAHighBand, rec.band))
	}

	if pf.isLongHaul {
		addMatched(getOrInitAccum(e.longHaulPolar, "dist_ge_6000km"))
		if rec.band == "20m" {
			addMatched(getOrInitAccum(e.longHaulPolar, "20m_dist_ge_6000km"))
		}
	}
	if pf.isPolar {
		addMatched(getOrInitAccum(e.longHaulPolar, "polar_all"))
		if rec.band == "20m" {
			addMatched(getOrInitAccum(e.longHaulPolar, "20m_polar"))
		}
	}
	if rec.band == "20m" && pf.isNAJA {
		addMatched(getOrInitAccum(e.longHaulPolar, "20m_na_ja"))
	}

	// Accumulate Rel positives (negatives are sampled on UTC hour rollover).
	e.relPP.add(ppRel, true)
	e.relVO.add(voRel, true)
	getOrInitRelAccum(e.relByBandPP, rec.band).add(ppRel, true)
	getOrInitRelAccum(e.relByBandVO, rec.band).add(voRel, true)
	if pf.isNASACA && isHighBand(rec.band) {
		getOrInitRelAccum(e.relNASACAPP, rec.band).add(ppRel, true)
		getOrInitRelAccum(e.relNASACAVO, rec.band).add(voRel, true)
	}

	// Path-hour empirical p50 aggregator.
	phKey := pathHourKey{deCell: deFine, dxCell: dxFine, band: rec.band}
	grp := e.pathHourAgg[phKey]
	if grp == nil {
		grp = &pathHourGroup{mode: rec.mode}
		e.pathHourAgg[phKey] = grp
	}
	grp.obsFT8 = append(grp.obsFT8, rec.ft8DB)
	grp.ppRxSum += ppRxDB
	grp.ppSigmaSum += ppSigma
	grp.voacapRxSum += voRxDB
	grp.count++

	// Regime B: Production LORO live Predictor + exact fallback decision tree.
	var baseRes pathreliability.Result
	if rec.isBeacon {
		baseRes = e.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(
			deFine, dxFine, deCoarse, dxCoarse, rec.band, rec.mode, 0, e.pathCfg.BeaconMinObservationCount, obsTime,
		)
	} else {
		baseRes = e.predictor.PredictWithMinObservationCount(
			deFine, dxFine, deCoarse, dxCoarse, rec.band, rec.mode, 0, e.pathCfg.MinObservationCount, obsTime,
		)
	}
	switch baseRes.Source {
	case pathreliability.SourceCombined:
		// Live buckets already sufficient: record priors side-by-side on the live-dominated slice.
		addMatched(&e.regimeBCombined)
	case pathreliability.SourceInsufficient:
		ppRes := applyFallbackDecision(baseRes, rec.mode, rec.isBeacon, ppForecast, e.ppCfg)
		voRes := applyFallbackDecision(baseRes, rec.mode, rec.isBeacon, voForecast, e.voacapCfg)
		e.regimeBPPOut.record(ppRes.Source)
		e.regimeBVOOut.record(voRes.Source)

		addRegimeB := func(acc *headToHeadAccum) {
			ppClosedB := ppRes.Source == pathreliability.SourceVOACAPClosed
			ppEmittedB := ppRes.Source != pathreliability.SourceInsufficient && !ppClosedB
			voClosedB := voRes.Source == pathreliability.SourceVOACAPClosed
			voEmittedB := voRes.Source != pathreliability.SourceInsufficient && !voClosedB
			acc.pp.add(rec.ft8DB, float64(rec.rawSNRDB), obsClass, ppRxDB, ppRes.Class, ppClosedB, ppEmittedB, ppSigma)
			acc.voacap.add(rec.ft8DB, float64(rec.rawSNRDB), obsClass, voRxDB, voRes.Class, voClosedB, voEmittedB, 0)
		}
		addRegimeB(&e.regimeBFBAll)
		if !baseRes.HasP50 {
			addRegimeB(&e.regimeBFBNoP50)
		} else {
			addRegimeB(&e.regimeBFBSparseP50)
		}
		addRegimeB(getOrInitAccum(e.byModeFB, rec.mode))
		if pf.isLongHaul {
			addRegimeB(getOrInitAccum(e.longHaulPolarFB, "dist_ge_6000km"))
			if rec.band == "20m" {
				addRegimeB(getOrInitAccum(e.longHaulPolarFB, "20m_dist_ge_6000km"))
			}
		}
		if pf.isPolar {
			addRegimeB(getOrInitAccum(e.longHaulPolarFB, "polar_all"))
			if rec.band == "20m" {
				addRegimeB(getOrInitAccum(e.longHaulPolarFB, "20m_polar"))
			}
		}
		if rec.band == "20m" && pf.isNAJA {
			addRegimeB(getOrInitAccum(e.longHaulPolarFB, "20m_na_ja"))
		}
	}

	if e.cfg.ProgressEvery > 0 && time.Since(e.lastProgress) >= e.cfg.ProgressEvery {
		vs := e.voacapEval.Stats()
		log.Printf("prop_loro progress: utc=%s train=%d heldout_matched=%d voacap_calls=%d cache_hits=%d pairs=%d elapsed=%s",
			obsTime.Format(time.RFC3339), e.manifest.TrainUpdates, e.manifest.HeldOutHFMatched,
			vs.RunnerCalls, vs.CacheHits, vs.UniquePairsEvaluated, time.Since(e.startedWall).Round(time.Second))
		e.lastProgress = time.Now()
	}
	return nil
}

func (e *loroEngine) score160m(rec capturedSpot, deFine, dxFine, deCoarse, dxCoarse pathreliability.CellID, obsTime time.Time) {
	e.native160.TotalHeldOut++
	base := e.predictor.PredictWithMinObservationCount(
		deFine, dxFine, deCoarse, dxCoarse, "160m", rec.mode, 0, e.pathCfg.MinObservationCount, obsTime,
	)
	if base.Source == pathreliability.SourceCombined {
		e.native160.LiveCombined++
		return
	}
	res := pathreliability.Native160FallbackResult(base, e.pathCfg, pathreliability.VOACAPClosedRequest{
		UserCell: deFine,
		DXCell:   dxFine,
		UserGrid: rec.deGrid,
		DXGrid:   rec.dxGrid,
		Band:     "160m",
		Mode:     rec.mode,
	}, obsTime)
	if res.Source != pathreliability.SourceNative160 {
		e.native160.Insufficient++
		return
	}
	switch res.Class {
	case "LOW":
		e.native160.EmittedLow++
	case "UNLIKELY":
		e.native160.EmittedUnlk++
	case "CLOSED":
		e.native160.EmittedClose++
	default:
		e.native160.Insufficient++
	}
}

func (e *loroEngine) isHeldOut(deHash uint64, deFine pathreliability.CellID) bool {
	if e.cfg.HoldoutUnit == "fine_cell" {
		return int(uint64(deFine)%uint64(e.cfg.Folds)) == e.cfg.Fold
	}
	return int(deHash%uint64(e.cfg.Folds)) == e.cfg.Fold
}

func (e *loroEngine) evalPropPrior(
	userCell, dxCell pathreliability.CellID,
	userLat, userLon, dxLat, dxLon float64,
	bandIdx int,
	utc time.Time,
	sw swSnapshot,
) (pathreliability.VOACAPCachedForecast, float64, bool) {
	freqMHz := e.ppBandFreq[bandIdx]
	if freqMHz <= 0 || e.ppModel == nil {
		return pathreliability.VOACAPCachedForecast{}, 0, false
	}
	lo, hi := userCell, dxCell
	if lo > hi {
		lo, hi = hi, lo
	}
	bucketSec := int64(e.ppFB.CacheBucketSeconds)
	if bucketSec <= 0 {
		bucketSec = 300
	}
	ck := ppCacheKey{
		loCell:     lo,
		hiCell:     hi,
		bandIdx:    uint8(bandIdx),
		timeBucket: utc.Unix() / bucketSec,
		swGen:      sw.Gen,
	}
	val, ok := e.ppCache[ck]
	if !ok {
		pred, err := e.ppModel.Predict(propprior.PathRequest{
			TXLat:                dxLat,
			TXLon:                dxLon,
			RXLat:                userLat,
			RXLon:                userLon,
			FreqMHz:              freqMHz,
			Time:                 utc,
			SpaceWeatherFeatures: sw.features(),
		})
		if err != nil {
			return pathreliability.VOACAPCachedForecast{}, 0, false
		}
		val = ppCacheVal{snrTotal: pred.SnrTotal, sigmaTotal: pred.SigmaTotal}
		e.ppCache[ck] = val
	}

	// Exact production calibration from pathreliability/prop_prior_fallback.go:buildForecast.
	snrAdj := val.snrTotal + e.ppFB.SNROffsetDB
	rel := (propprior.Prediction{SnrTotal: snrAdj, SigmaTotal: val.sigmaTotal}).Rel(e.ppFB.ReqSNRDB)
	rounded := int(math.Round(snrAdj))
	fc := pathreliability.VOACAPCachedForecast{
		Record: pathreliability.VOACAPHourlyForecast{
			FT8SNRDB:                        rounded,
			HourUTC:                         utc.Hour(),
			FrequencyMHz:                    freqMHz,
			ReqSNRReliability:               rel,
			HasReqSNRReliability:            true,
			ReceiveFT8SNRDB:                 rounded,
			TransmitFT8SNRDB:                rounded,
			ReceiveReqSNRReliability:        rel,
			TransmitReqSNRReliability:       rel,
			HasDirectionalReqSNRReliability: true,
			HasDirectionalSNR:               true,
		},
		EffectiveFT8SNRDB:     snrAdj,
		HasEffectiveFT8SNRDB:  true,
		ExactReceiveFT8SNRDB:  snrAdj,
		ExactTransmitFT8SNRDB: snrAdj,
		HasExactDirectionalDB: true,
		ReceiveNoisePenaltyDB: 0,
		PropPrior:             true,
	}
	return fc, val.sigmaTotal, true
}

// applyFallbackDecision reproduces telnet/server.go:pathResultWithClosedFallbackTrace
// identically for both v4.3 propprior (ppCfg) and VOACAP (voacapCfg).
func applyFallbackDecision(
	base pathreliability.Result,
	mode string,
	isBeacon bool,
	forecast pathreliability.VOACAPCachedForecast,
	cfg pathreliability.Config,
) pathreliability.Result {
	if base.Source != pathreliability.SourceInsufficient {
		return base
	}
	if isBeacon {
		rxDB := forecast.ReceiveDB()
		cls := pathreliability.ClassForDB(rxDB, mode, cfg)
		if pathreliability.ClosedForDB(rxDB, mode, cfg) {
			return pathreliability.VOACAPBeaconClosedResult(cfg, forecast)
		}
		if !base.HasP50 {
			if cfg.FallbackReliabilityGatedOpenEnabled() {
				if _, _, reason := pathreliability.VOACAPReceiveReqSNRReliabilityGate(forecast, cls, cfg); reason == pathreliability.VOACAPReliabilityGateOK {
					return pathreliability.VOACAPBeaconOpenResult(cfg, mode, forecast)
				}
			}
			return base
		}
		p50Class := pathreliability.ClassForDB(base.P50DB, mode, cfg)
		if p50Class == cls {
			return pathreliability.VOACAPBeaconAlignedResult(base, cfg, mode, forecast)
		}
		if pathreliability.ClassUpgradeSteps(p50Class, cls) == 1 && cfg.FallbackReliabilitySparseUpgradeEnabled() {
			if _, _, reason := pathreliability.VOACAPReceiveReqSNRReliabilityGate(forecast, cls, cfg); reason == pathreliability.VOACAPReliabilityGateOK {
				return pathreliability.VOACAPBeaconSparseUpgradeResult(base, cfg, mode, forecast)
			}
		}
		return base
	}

	effDB := forecast.EffectiveDB()
	cls := pathreliability.ClassForDB(effDB, mode, cfg)
	if pathreliability.ClosedForDB(effDB, mode, cfg) {
		return pathreliability.VOACAPClosedResult(cfg, forecast)
	}
	if !base.HasP50 {
		if cfg.FallbackReliabilityGatedOpenEnabled() {
			if _, _, reason := pathreliability.VOACAPReqSNRReliabilityGate(forecast, cls, cfg); reason == pathreliability.VOACAPReliabilityGateOK {
				return pathreliability.VOACAPOpenResult(cfg, mode, forecast)
			}
		}
		return base
	}
	p50Class := pathreliability.ClassForDB(base.P50DB, mode, cfg)
	if p50Class == cls {
		return pathreliability.VOACAPAlignedResult(base, cfg, mode, forecast)
	}
	if pathreliability.ClassUpgradeSteps(p50Class, cls) == 1 && cfg.FallbackReliabilitySparseUpgradeEnabled() {
		if _, _, reason := pathreliability.VOACAPReqSNRReliabilityGate(forecast, cls, cfg); reason == pathreliability.VOACAPReliabilityGateOK {
			return pathreliability.VOACAPSparseUpgradeResult(base, cfg, mode, forecast)
		}
	}
	return base
}

func (e *loroEngine) recordActivity(
	band string,
	rxCell pathreliability.CellID, rxLat, rxLon float64,
	txCell pathreliability.CellID, txLat, txLon float64,
	sw swSnapshot,
	obsUnix int64,
) {
	act := e.activity[band]
	if act == nil {
		act = &bandHourActivity{
			rxByCell: make(map[pathreliability.CellID]activeEndpoint),
			txByCell: make(map[pathreliability.CellID]activeEndpoint),
			heard:    make(map[uint64]struct{}),
		}
		e.activity[band] = act
	}
	act.rxByCell[rxCell] = activeEndpoint{cell: rxCell, lat: rxLat, lon: rxLon}
	act.txByCell[txCell] = activeEndpoint{cell: txCell, lat: txLat, lon: txLon}
	act.heard[uint64(rxCell)<<32|uint64(txCell)] = struct{}{}
	act.lastSW = sw
	act.hasSW = true
	act.lastUnix = obsUnix
}

func (e *loroEngine) flushHour(ctx context.Context) error {
	// 1. Path-hour empirical p50 aggregates (n >= 3).
	for _, grp := range e.pathHourAgg {
		if grp.count < loroMinPathHourSamples {
			continue
		}
		empP50 := medianFloat64(grp.obsFT8)
		obsClass := pathreliability.ClassForDB(empP50, grp.mode, e.pathCfg)
		ppMean := grp.ppRxSum / float64(grp.count)
		ppSig := grp.ppSigmaSum / float64(grp.count)
		voMean := grp.voacapRxSum / float64(grp.count)

		ppClosed := pathreliability.ClosedForDB(ppMean, grp.mode, e.ppCfg)
		ppClass := pathreliability.ClassForDB(ppMean, grp.mode, e.ppCfg)
		voClosed := pathreliability.ClosedForDB(voMean, grp.mode, e.voacapCfg)
		voClass := pathreliability.ClassForDB(voMean, grp.mode, e.voacapCfg)

		e.regimeAP50.pp.add(empP50, empP50, obsClass, ppMean, ppClass, ppClosed, true, ppSig)
		e.regimeAP50.voacap.add(empP50, empP50, obsClass, voMean, voClass, voClosed, true, 0)
	}
	clear(e.pathHourAgg)

	// 2. Deterministic matched active-TX x active-RX negative sampling per band-hour.
	for _, band := range hfBandOrder {
		act := e.activity[band]
		if act == nil || !act.hasSW || len(act.rxByCell) < 2 || len(act.txByCell) < 2 {
			continue
		}
		bIdx, ok := hfBandIndex(band)
		if !ok {
			continue
		}
		rxs := sortedEndpoints(act.rxByCell)
		txs := sortedEndpoints(act.txByCell)
		target := len(act.heard)
		if target > e.cfg.MaxNegBandHour {
			target = e.cfg.MaxNegBandHour
		}
		at := time.Unix(act.lastUnix, 0).UTC()
		ssn := act.lastSW.VOACAPSSN.SSN
		if e.cfg.VOACAPSSNFixed > 0 {
			ssn = e.cfg.VOACAPSSNFixed
		}
		// Deterministic SplitMix64 sampling over active RX x active TX non-heard pairs.
		state := uint64(e.currentHour)*0x9e3779b97f4a7c15 ^ uint64(bIdx+1)*0xbf58476d1ce4e5b9
		sampled := 0
		attempts := 0
		maxAttempts := target * 8
		for sampled < target && attempts < maxAttempts {
			attempts++
			state = splitMix64(state)
			rx := rxs[state%uint64(len(rxs))]
			state = splitMix64(state)
			tx := txs[state%uint64(len(txs))]
			if rx.cell == tx.cell {
				continue
			}
			if _, wasHeard := act.heard[uint64(rx.cell)<<32|uint64(tx.cell)]; wasHeard {
				continue
			}
			ppFC, _, ppOK := e.evalPropPrior(rx.cell, tx.cell, rx.lat, rx.lon, tx.lat, tx.lon, bIdx, at, act.lastSW)
			voFC, voOK, voErr := e.voacapEval.Forecast(ctx, rx.cell, tx.cell, rx.lat, rx.lon, tx.lat, tx.lon, band, at, ssn, e.voacapCfg)
			if voErr != nil || !ppOK || !voOK {
				continue
			}
			ppRel, _ := ppFC.ReceiveReqSNRReliability()
			voRel, _ := voFC.ReceiveReqSNRReliability()
			e.relPP.add(ppRel, false)
			e.relVO.add(voRel, false)
			getOrInitRelAccum(e.relByBandPP, band).add(ppRel, false)
			getOrInitRelAccum(e.relByBandVO, band).add(voRel, false)
			if isHighBand(band) && isNASACACorridor(rx.lat, rx.lon, tx.lat, tx.lon) {
				getOrInitRelAccum(e.relNASACAPP, band).add(ppRel, false)
				getOrInitRelAccum(e.relNASACAVO, band).add(voRel, false)
			}
			sampled++
		}
	}
	clear(e.activity)
	clear(e.ppCache)
	return nil
}

func (e *loroEngine) finalize(ctx context.Context, captureDir string, spotFiles, swFiles []string, readStats spotReadStats) (loroScorecard, error) {
	if e.hasCurrentHr {
		if err := e.flushHour(ctx); err != nil {
			return loroScorecard{}, err
		}
	}
	e.manifest.CaptureDir = captureDir
	e.manifest.SpotFiles = spotFiles
	e.manifest.SpaceWeatherFiles = swFiles
	e.manifest.TotalRowsRead = readStats.RowsRead
	e.manifest.BadRowsSkipped = readStats.RowsBad
	if e.firstObsUnix > 0 {
		e.manifest.FirstObservedUTC = time.Unix(e.firstObsUnix, 0).UTC().Format(time.RFC3339)
	}
	if e.lastObsUnix > 0 {
		e.manifest.LastObservedUTC = time.Unix(e.lastObsUnix, 0).UTC().Format(time.RFC3339)
	}
	e.manifest.VOACAPStats = e.voacapEval.Stats()
	e.manifest.ElapsedSeconds = round2(time.Since(e.startedWall).Seconds())

	return loroScorecard{
		Manifest:             e.manifest,
		RegimeAOverall:       e.regimeAAll.summary(),
		RegimeAExactGrid:     e.regimeAExact.summary(),
		RegimeADerivedGrid:   e.regimeADerived.summary(),
		RegimeAPathHourP50:   e.regimeAP50.summary(),
		RegimeBCombined:      e.regimeBCombined.summary(),
		RegimeBFallbackAll:   e.regimeBFBAll.summary(),
		RegimeBFallbackNoP50: e.regimeBFBNoP50.summary(),
		RegimeBFallbackSpP50: e.regimeBFBSparseP50.summary(),
		RegimeBPPOutcomes:    e.regimeBPPOut,
		RegimeBVOOutcomes:    e.regimeBVOOut,
		ByMode:                 summarizeMap(e.byMode),
		ByModeFallbackOnly:     summarizeMap(e.byModeFB),
		ByModeNonPolar:         summarizeMap(e.byModeNonPolar),
		ByModeNonPolarNAEU:     summarizeMap(e.byModeNonPolarNAEU),
		ByBand:                 summarizeMap(e.byBand),
		ByBandDayNight:         summarizeMap(e.byBandSolar),
		LongHaulPolar:          summarizeMap(e.longHaulPolar),
		LongHaulPolarFBOnly:    summarizeMap(e.longHaulPolarFB),
		GeomagCliff:            summarizeMap(e.geomagCliff),
		GeomagCliff20m:         summarizeMap(e.geomagCliff20m),
		NASACAHighBand:         summarizeMap(e.naSACAHighBand),
		Native160m:             e.native160,
		RelCalibrationPP:       e.relPP.summary(),
		RelCalibrationVO:       e.relVO.summary(),
		RelCalibrationByBandPP: summarizeRelMap(e.relByBandPP),
		RelCalibrationByBandVO: summarizeRelMap(e.relByBandVO),
		RelCalibrationNASACAPP: summarizeRelMap(e.relNASACAPP),
		RelCalibrationNASACAVO: summarizeRelMap(e.relNASACAVO),
	}, nil
}

func getOrInitAccum(m map[string]*headToHeadAccum, key string) *headToHeadAccum {
	acc := m[key]
	if acc == nil {
		acc = &headToHeadAccum{}
		m[key] = acc
	}
	return acc
}

func getOrInitRelAccum(m map[string]*relCalibAccum, key string) *relCalibAccum {
	acc := m[key]
	if acc == nil {
		acc = &relCalibAccum{}
		m[key] = acc
	}
	return acc
}

func summarizeMap(m map[string]*headToHeadAccum) map[string]headToHeadSummary {
	out := make(map[string]headToHeadSummary, len(m))
	for k, v := range m {
		if v.pp.count > 0 {
			out[k] = v.summary()
		}
	}
	return out
}

func summarizeRelMap(m map[string]*relCalibAccum) map[string]relCalibSummary {
	out := make(map[string]relCalibSummary, len(m))
	for k, v := range m {
		if v.posCount+v.negCount > 0 {
			out[k] = v.summary()
		}
	}
	return out
}

func sortedEndpoints(m map[pathreliability.CellID]activeEndpoint) []activeEndpoint {
	out := make([]activeEndpoint, 0, len(m))
	for _, ep := range m {
		out = append(out, ep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].cell < out[j].cell })
	return out
}

func splitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
