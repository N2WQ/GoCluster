package main

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/strutil"
	"dxcluster/uls"
)

const (
	defaultEvalNegatives = 10000
	defaultFreqBucketHz  = 100

	resolverQueueSize           = 8192
	resolverMaxActiveKeys       = 6000
	resolverMaxCandidatesPerKey = 16
	resolverMaxReportersPerCand = 64
	resolverInactiveTTL         = 10 * time.Minute
	resolverEvalMinInterval     = 500 * time.Millisecond
	resolverSweepInterval       = 1 * time.Second
	resolverHysteresisWindows   = 2
)

var errMissingRBNColumns = errors.New("missing required RBN CSV columns")

type interner struct {
	ids map[string]uint32
	arr []string
}

func newInterner() *interner {
	return &interner{ids: map[string]uint32{}, arr: []string{""}}
}

func (i *interner) intern(raw string) uint32 {
	call := normalizeCall(raw)
	if call == "" {
		return 0
	}
	if id, ok := i.ids[call]; ok {
		return id
	}
	id := uint32(len(i.arr))
	i.ids[call] = id
	i.arr = append(i.arr, call)
	return id
}

func (i *interner) str(id uint32) string {
	if int(id) >= 0 && int(id) < len(i.arr) {
		return i.arr[id]
	}
	return ""
}

type labelKey struct {
	MinuteUnix int64
	FreqKey    int64
	SpotterID  uint32
	SubjectID  uint32
}

type predictedEvent struct {
	WinnerID uint32
	Reason   string
}

type otherLoadStats struct {
	Lines            int64 `json:"lines"`
	Parsed           int64 `json:"parsed"`
	SkippedHarmonic  int64 `json:"skipped_harmonic"`
	SkippedInvalid   int64 `json:"skipped_invalid"`
	SkippedNoDB      int64 `json:"skipped_no_db"`
	ConflictingLabel int64 `json:"conflicting_label"`
}

type replayStats struct {
	RBNRows       int64 `json:"rbn_rows"`
	CandidateRows int64 `json:"candidate_rows"`
	SkippedBadRow int64 `json:"skipped_bad_row"`
	SkippedSchema int64 `json:"skipped_schema"`
}

type primaryCounters struct {
	DecisionTotal int64 `json:"decision_total"`
	Applied       int64 `json:"applied"`
	Rejected      int64 `json:"rejected"`
	Suppressed    int64 `json:"suppressed"`

	AppliedResolver                     int64            `json:"applied_resolver"`
	AppliedResolverNeighborOverride     int64            `json:"applied_resolver_neighbor_override"`
	AppliedResolverRecentPlus1          int64            `json:"applied_resolver_recent_plus1"`
	AppliedResolverNeighborRecentPlus1  int64            `json:"applied_resolver_neighbor_recent_plus1"`
	RejectedNoSnapshot                  int64            `json:"rejected_no_snapshot"`
	RejectedNeighborhoodConflict        int64            `json:"rejected_neighborhood_conflict"`
	RejectedStateSplit                  int64            `json:"rejected_state_split"`
	RejectedStateUncertain              int64            `json:"rejected_state_uncertain"`
	RejectedStateUnknown                int64            `json:"rejected_state_unknown"`
	RejectedPrecallMissing              int64            `json:"rejected_precall_missing"`
	RejectedWinnerMissing               int64            `json:"rejected_winner_missing"`
	RejectedSameCall                    int64            `json:"rejected_same_call"`
	RejectedInvalidBase                 int64            `json:"rejected_invalid_base"`
	RejectedCTYMiss                     int64            `json:"rejected_cty_miss"`
	RejectedNonCandidate                int64            `json:"rejected_non_candidate_mode"`
	ResolverEnqueueFailed               int64            `json:"resolver_enqueue_failed"`
	OutOfOrderRows                      int64            `json:"out_of_order_rows"`
	NeighborhoodUsed                    int64            `json:"neighborhood_used"`
	NeighborhoodWinnerOverride          int64            `json:"neighborhood_winner_override"`
	NeighborhoodConflictSplit           int64            `json:"neighborhood_conflict_split"`
	NeighborhoodExcludedUnrelated       int64            `json:"neighborhood_excluded_unrelated"`
	NeighborhoodExcludedDistance        int64            `json:"neighborhood_excluded_distance"`
	NeighborhoodExcludedAnchorMissing   int64            `json:"neighborhood_excluded_anchor_missing"`
	RecentPlus1Considered               int64            `json:"recent_plus1_considered"`
	RecentPlus1Applied                  int64            `json:"recent_plus1_applied"`
	RecentPlus1Rejected                 int64            `json:"recent_plus1_rejected"`
	RecentPlus1RejectEditNeighbor       int64            `json:"recent_plus1_reject_edit_neighbor_contested"`
	RecentPlus1RejectDistanceOrFamily   int64            `json:"recent_plus1_reject_distance_or_family"`
	RecentPlus1RejectWinnerInsufficient int64            `json:"recent_plus1_reject_winner_recent_insufficient"`
	RecentPlus1RejectSubjectNotWeaker   int64            `json:"recent_plus1_reject_subject_not_weaker"`
	RecentPlus1RejectOther              int64            `json:"recent_plus1_reject_other"`
	AppliedReasonCounts                 map[string]int64 `json:"applied_reason_counts"`
	RejectedReasonCounts                map[string]int64 `json:"rejected_reason_counts"`
	GateRejectReasonCounts              map[string]int64 `json:"gate_reject_reason_counts"`
}

type mismatch struct {
	TsUnix         int64  `json:"ts_unix"`
	FreqHz         int64  `json:"freq_hz"`
	Spotter        string `json:"spotter"`
	Subject        string `json:"subject"`
	Expected       string `json:"expected"`
	Predicted      string `json:"predicted"`
	DecisionReason string `json:"decision_reason,omitempty"`
}

type inferredMethod struct {
	ResolverMode     string                      `json:"resolver_mode"`
	ConfigLoadedFrom string                      `json:"config_loaded_from"`
	CallCorrection   config.CallCorrectionConfig `json:"call_correction"`
}

type trainingReport struct {
	Mode             string `json:"mode"`
	ResolverMode     string `json:"resolver_mode"`
	ConfigLoadedFrom string `json:"config_loaded_from"`
	ExamplesTotal    int64  `json:"examples_total"`
	Positives        int64  `json:"positives"`
	Negatives        int64  `json:"negatives"`
}

type evalReport struct {
	ResolverMode              string          `json:"resolver_mode"`
	RBNRows                   int64           `json:"rbn_rows"`
	PositivesTotal            int64           `json:"positives_total"`
	CorrectPositive           int64           `json:"correct_positive"`
	MissedPositive            int64           `json:"missed_positive"`
	PredictedExtraOnPositives int64           `json:"predicted_extra_on_positives"`
	NegativesSeen             int64           `json:"negatives_seen"`
	NegativesSampled          int64           `json:"negatives_sampled"`
	FalsePositive             int64           `json:"false_positive"`
	TrueNegative              int64           `json:"true_negative"`
	PredictedAppliedTotal     int64           `json:"predicted_applied_total"`
	PredictedOnUnlabeled      int64           `json:"predicted_on_unlabeled"`
	RecallPercent             float64         `json:"recall_percent"`
	PrecisionPercentSampled   float64         `json:"precision_percent_sampled"`
	Mismatches                []mismatch      `json:"mismatches,omitempty"`
	Replay                    replayStats     `json:"replay"`
	Labels                    otherLoadStats  `json:"labels"`
	Counters                  primaryCounters `json:"resolver_primary_counters"`
}

type runState struct {
	cfg             *config.Config
	in              *interner
	freqBucketHz    int64
	expectedByKey   map[labelKey][]uint32
	predictedByKey  map[labelKey][]predictedEvent
	negSeen         int64
	negSamples      []bool
	rng             *xorshift64
	replay          replayStats
	counters        primaryCounters
	predOnUnlabeled int64
	ctyDB           *cty.CTYDatabase
	knownCallset    *spot.KnownCallsigns
	recentBand      *spot.RecentBandStore
	adaptive        *spot.AdaptiveMinReports
	resolver        *spot.SignalResolver
	driver          *spot.SignalResolverDriver
	lastReplayTS    time.Time
}

type rbnHeader struct {
	callsign int
	freq     int
	band     int
	dx       int
	db       int
	date     int
	txMode   int
}

type rbnRow struct {
	Time    time.Time
	FreqKHz float64
	Band    string
	DXCall  string
	Spotter string
	Mode    string
	Report  int
}

type xorshift64 struct {
	state uint64
}

func (x *xorshift64) next() uint64 {
	if x.state == 0 {
		x.state = 1
	}
	s := x.state
	s ^= s << 13
	s ^= s >> 7
	s ^= s << 17
	x.state = s
	return s
}

func main() {
	var (
		rbnDir       = flag.String("rbn-dir", "archive data", "RBN .zip/.csv directory")
		otherDir     = flag.String("other-dir", "archive data", "Other cluster .txt/.log directory")
		configDir    = flag.String("config-dir", filepath.Join("data", "config"), "Cluster config directory")
		outDir       = flag.String("out", filepath.Join("data", "analysis", "callcorr_reveng_rebuilt"), "Output directory")
		evalNeg      = flag.Int("eval-negatives", defaultEvalNegatives, "Reservoir-sampled negatives")
		seed         = flag.Uint64("seed", 1, "Negative sampling seed")
		freqBucketHz = flag.Int64("freq-bucket-hz", defaultFreqBucketHz, "Keying bucket for frequency matching")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.LUTC)
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}

	cfg, err := config.Load(*configDir)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.CallCorrection.ResolverMode != config.CallCorrectionResolverModePrimary {
		log.Fatalf("strict resolver-primary mode required; effective call_correction.resolver_mode=%q (config=%s)", cfg.CallCorrection.ResolverMode, cfg.LoadedFrom)
	}
	spot.ConfigureMorseWeights(cfg.CallCorrection.MorseWeights.Insert, cfg.CallCorrection.MorseWeights.Delete, cfg.CallCorrection.MorseWeights.Sub, cfg.CallCorrection.MorseWeights.Scale)
	spot.ConfigureBaudotWeights(cfg.CallCorrection.BaudotWeights.Insert, cfg.CallCorrection.BaudotWeights.Delete, cfg.CallCorrection.BaudotWeights.Sub, cfg.CallCorrection.BaudotWeights.Scale)
	if err := configureULS(cfg); err != nil {
		log.Fatal(err)
	}
	ctyDB, err := loadCTY(cfg)
	if err != nil {
		log.Fatal(err)
	}
	knownCallset, err := loadKnownCallset(cfg.KnownCalls.File)
	if err != nil {
		log.Fatal(err)
	}

	rbnFiles, err := discover(*rbnDir, []string{".zip", ".csv"})
	if err != nil {
		log.Fatal(err)
	}
	otherFiles, err := discover(*otherDir, []string{".txt", ".log"})
	if err != nil {
		log.Fatal(err)
	}

	in := newInterner()
	expectedByKey, labelStats, err := loadOtherLabels(otherFiles, in, *freqBucketHz)
	if err != nil {
		log.Fatal(err)
	}

	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:                 resolverQueueSize,
		MaxActiveKeys:             resolverMaxActiveKeys,
		MaxCandidatesPerKey:       resolverMaxCandidatesPerKey,
		MaxReportersPerCand:       resolverMaxReportersPerCand,
		InactiveTTL:               resolverInactiveTTL,
		EvalMinInterval:           resolverEvalMinInterval,
		SweepInterval:             resolverSweepInterval,
		HysteresisWindows:         resolverHysteresisWindows,
		FreqGuardMinSeparationKHz: cfg.CallCorrection.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:    cfg.CallCorrection.FreqGuardRunnerUpRatio,
		MaxEditDistance:           cfg.CallCorrection.MaxEditDistance,
		DistanceModelCW:           cfg.CallCorrection.DistanceModelCW,
		DistanceModelRTTY:         cfg.CallCorrection.DistanceModelRTTY,
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          cfg.CallCorrection.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
	})
	driver, err := spot.NewSignalResolverDriver(resolver)
	if err != nil {
		log.Fatal(err)
	}

	var recentBandStore *spot.RecentBandStore
	if cfg.CallCorrection.Enabled && (cfg.CallCorrection.RecentBandBonusEnabled || cfg.CallCorrection.StabilizerEnabled) {
		recentBandStore = spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
	}

	state := &runState{
		cfg:            cfg,
		in:             in,
		freqBucketHz:   *freqBucketHz,
		expectedByKey:  expectedByKey,
		predictedByKey: make(map[labelKey][]predictedEvent, len(expectedByKey)),
		negSamples:     make([]bool, 0, maxInt(*evalNeg, 0)),
		rng:            &xorshift64{state: *seed},
		ctyDB:          ctyDB,
		knownCallset:   knownCallset,
		recentBand:     recentBandStore,
		adaptive:       spot.NewAdaptiveMinReports(cfg.CallCorrection),
		resolver:       resolver,
		driver:         driver,
		counters: primaryCounters{
			AppliedReasonCounts:    map[string]int64{},
			RejectedReasonCounts:   map[string]int64{},
			GateRejectReasonCounts: map[string]int64{},
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	for _, path := range rbnFiles {
		if err := ctx.Err(); err != nil {
			log.Fatal(err)
		}
		rows, badRows, err := replayRBNFile(ctx, path, state, *evalNeg)
		if err != nil {
			if errors.Is(err, errMissingRBNColumns) {
				state.replay.SkippedSchema++
				log.Printf("Skipping RBN input %q: %v", path, err)
				continue
			}
			log.Fatal(err)
		}
		state.replay.RBNRows += rows
		state.replay.SkippedBadRow += badRows
	}

	report := buildEvalReport(state, labelStats)
	method := inferredMethod{
		ResolverMode:     cfg.CallCorrection.ResolverMode,
		ConfigLoadedFrom: cfg.LoadedFrom,
		CallCorrection:   cfg.CallCorrection,
	}
	train := trainingReport{
		Mode:             "resolver_primary_config_mapping",
		ResolverMode:     cfg.CallCorrection.ResolverMode,
		ConfigLoadedFrom: cfg.LoadedFrom,
		ExamplesTotal:    report.PositivesTotal + report.NegativesSampled,
		Positives:        report.PositivesTotal,
		Negatives:        report.NegativesSampled,
	}

	mustWrite(filepath.Join(*outDir, "inferred_method.json"), method)
	mustWrite(filepath.Join(*outDir, "training_report.json"), train)
	mustWrite(filepath.Join(*outDir, "eval_report.json"), report)
	writeSummary(filepath.Join(*outDir, "summary.txt"), report, method)
	log.Printf("Wrote outputs to %s", *outDir)
}

func replayRBNFile(ctx context.Context, path string, st *runState, evalNeg int) (int64, int64, error) {
	reader, closeFn, err := openRBNReader(path)
	if err != nil {
		return 0, 0, err
	}
	defer closeFn()

	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	csvReader.ReuseRecord = true

	headerRow, err := csvReader.Read()
	if err != nil {
		return 0, 0, err
	}
	header, err := parseRBNHeader(headerRow)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %v", errMissingRBNColumns, err)
	}

	var rows int64
	var badRows int64
	for {
		if err := ctx.Err(); err != nil {
			return rows, badRows, err
		}
		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return rows, badRows, err
		}
		row, ok := parseRBNRecord(record, header)
		if !ok {
			badRows++
			continue
		}
		if !spot.IsCallCorrectionCandidate(row.Mode) {
			continue
		}
		rows++
		st.replay.CandidateRows++
		processRBNRow(st, row, evalNeg)
	}
	return rows, badRows, nil
}

func processRBNRow(st *runState, row rbnRow, evalNeg int) {
	now := row.Time.UTC()
	if !st.lastReplayTS.IsZero() && now.Before(st.lastReplayTS) {
		st.counters.OutOfOrderRows++
	}
	if now.After(st.lastReplayTS) {
		st.lastReplayTS = now
	}

	st.driver.Step(now)

	spotEntry := spot.NewSpotNormalized(row.DXCall, row.Spotter, row.FreqKHz, row.Mode)
	spotEntry.Time = now
	spotEntry.Report = row.Report
	spotEntry.HasReport = true
	spotEntry.SourceType = spot.SourceRBN
	spotEntry.SourceNode = "RBN-HISTORY"
	spotEntry.Band = row.Band
	spotEntry.EnsureNormalized()
	spotEntry.RefreshBeaconFlag()
	if spotEntry.IsBeacon {
		st.driver.Step(now)
		return
	}

	evidence, hasEvidence := correctionflow.BuildResolverEvidenceSnapshot(spotEntry, st.cfg.CallCorrection, st.adaptive, now)
	if hasEvidence {
		if accepted := st.resolver.Enqueue(evidence); !accepted {
			st.counters.ResolverEnqueueFailed++
			st.driver.Step(now)
			return
		}
	}

	preCorrectionCall := correctionflow.NormalizedDXCall(spotEntry)
	k := labelKey{
		MinuteUnix: minuteUnix(now),
		FreqKey:    freqKeyFromKHz(row.FreqKHz, st.freqBucketHz),
		SpotterID:  st.in.intern(row.Spotter),
		SubjectID:  st.in.intern(preCorrectionCall),
	}

	decision := evaluateResolverPrimaryDecision(st, spotEntry, evidence, hasEvidence, preCorrectionCall, now)
	if decision.Applied {
		st.predictedByKey[k] = append(st.predictedByKey[k], predictedEvent{
			WinnerID: st.in.intern(decision.Winner),
			Reason:   decision.Reason,
		})
	}

	_, positiveKey := st.expectedByKey[k]
	if !positiveKey {
		st.negSeen++
		if decision.Applied {
			st.predOnUnlabeled++
		}
		recordNegativeSample(st, evalNeg, decision.Applied)
	}

	if !decision.Suppress {
		recordRecentBandObservation(spotEntry, st.recentBand, st.cfg.CallCorrection)
	}
	st.driver.Step(now)
}

type resolverDecision struct {
	Applied  bool
	Suppress bool
	Winner   string
	Reason   string
}

func evaluateResolverPrimaryDecision(
	st *runState,
	spotEntry *spot.Spot,
	evidence spot.ResolverEvidence,
	hasEvidence bool,
	preCorrectionCall string,
	now time.Time,
) resolverDecision {
	st.counters.DecisionTotal++
	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		st.counters.Rejected++
		st.counters.RejectedNonCandidate++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_non_candidate_mode")
		return resolverDecision{Reason: "resolver_non_candidate_mode"}
	}

	preCall := spot.NormalizeCallsign(preCorrectionCall)
	if preCall == "" {
		st.counters.Rejected++
		st.counters.RejectedPrecallMissing++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_precall_missing")
		return resolverDecision{Reason: "resolver_precall_missing"}
	}

	selection := correctionflow.ResolverPrimarySelection{}
	snapshot := spot.ResolverSnapshot{}
	snapshotOK := false
	if hasEvidence {
		selection = correctionflow.SelectResolverPrimarySnapshotForCall(st.resolver, evidence.Key, st.cfg.CallCorrection, preCall)
		snapshot = selection.Snapshot
		snapshotOK = selection.SnapshotOK
		st.counters.NeighborhoodExcludedUnrelated += int64(selection.NeighborhoodExcludedUnrelated)
		st.counters.NeighborhoodExcludedDistance += int64(selection.NeighborhoodExcludedDistance)
		st.counters.NeighborhoodExcludedAnchorMissing += int64(selection.NeighborhoodExcludedAnchorMissing)
		if selection.UsedNeighborhood {
			st.counters.NeighborhoodUsed++
		}
		if selection.WinnerOverride {
			st.counters.NeighborhoodWinnerOverride++
		}
		if selection.NeighborhoodSplit {
			st.counters.NeighborhoodConflictSplit++
		}
	}

	if !snapshotOK {
		st.counters.Rejected++
		st.counters.RejectedNoSnapshot++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_no_snapshot")
		return resolverDecision{Reason: "resolver_no_snapshot"}
	}
	if selection.NeighborhoodSplit {
		st.counters.Rejected++
		st.counters.RejectedNeighborhoodConflict++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_neighbor_conflict")
		return resolverDecision{Reason: "resolver_neighbor_conflict"}
	}

	switch snapshot.State {
	case spot.ResolverStateConfident, spot.ResolverStateProbable:
	case spot.ResolverStateSplit:
		st.counters.Rejected++
		st.counters.RejectedStateSplit++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_state_split")
		return resolverDecision{Reason: "resolver_state_split"}
	case spot.ResolverStateUncertain:
		st.counters.Rejected++
		st.counters.RejectedStateUncertain++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_state_uncertain")
		return resolverDecision{Reason: "resolver_state_uncertain"}
	default:
		st.counters.Rejected++
		st.counters.RejectedStateUnknown++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_state_unknown")
		return resolverDecision{Reason: "resolver_state_unknown"}
	}

	winnerCall := spot.NormalizeCallsign(snapshot.Winner)
	if winnerCall == "" {
		st.counters.Rejected++
		st.counters.RejectedWinnerMissing++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_winner_missing")
		return resolverDecision{Reason: "resolver_winner_missing"}
	}
	if strings.EqualFold(winnerCall, preCall) {
		st.counters.Rejected++
		st.counters.RejectedSameCall++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_same_call")
		return resolverDecision{Reason: "resolver_same_call"}
	}

	subjectSupport := correctionflow.ResolverSupportForCall(snapshot, preCall)
	winnerSupport := correctionflow.ResolverSupportForCall(snapshot, winnerCall)
	winnerConfidence := correctionflow.ResolverWinnerConfidence(snapshot)
	subjectMode := spotEntry.ModeNorm
	if subjectMode == "" {
		subjectMode = spotEntry.Mode
	}
	subjectBand := spotEntry.BandNorm
	if subjectBand == "" || subjectBand == "???" {
		subjectBand = spot.FreqToBand(spotEntry.Frequency)
	}

	runtime := correctionflow.ResolveRuntimeSettings(st.cfg.CallCorrection, spotEntry, st.adaptive, now, true)
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{
		Cfg:                st.cfg.CallCorrection,
		MinReports:         runtime.MinReports,
		CooldownMinReports: runtime.CooldownMinReports,
		Window:             runtime.Window,
		FreqToleranceHz:    runtime.FreqToleranceHz,
		QualityBinHz:       runtime.QualityBinHz,
		RecentBandStore:    st.recentBand,
		KnownCallset:       st.knownCallset,
	})
	gateOptions := spot.ResolverPrimaryGateOptions{}
	if st.cfg.CallCorrection.ResolverRecentPlus1Enabled {
		if spot.ResolverSnapshotHasComparableEditNeighbor(snapshot, winnerCall, subjectMode, st.cfg.CallCorrection.DistanceModelCW, st.cfg.CallCorrection.DistanceModelRTTY) {
			gateOptions.RecentPlus1DisallowReason = resolverRecentPlus1DisallowEditNeighborGate
		}
	}
	gate := spot.EvaluateResolverPrimaryGates(
		preCall,
		winnerCall,
		subjectBand,
		subjectMode,
		subjectSupport,
		winnerSupport,
		winnerConfidence,
		settings,
		now,
		gateOptions,
	)

	if gate.RecentPlus1Considered {
		st.counters.RecentPlus1Considered++
		if gate.RecentPlus1Applied {
			st.counters.RecentPlus1Applied++
		} else {
			st.counters.RecentPlus1Rejected++
			reject := strings.ToLower(strings.TrimSpace(gate.RecentPlus1Reject))
			switch reject {
			case "edit_neighbor_contested":
				st.counters.RecentPlus1RejectEditNeighbor++
			case "distance_or_family":
				st.counters.RecentPlus1RejectDistanceOrFamily++
			case "winner_recent_insufficient":
				st.counters.RecentPlus1RejectWinnerInsufficient++
			case "subject_not_weaker":
				st.counters.RecentPlus1RejectSubjectNotWeaker++
			default:
				st.counters.RecentPlus1RejectOther++
			}
		}
	}

	if !gate.Allow {
		st.counters.Rejected++
		reason := resolverGateDecisionReason(gate.Reason)
		if plusReason, ok := resolverRecentPlus1DecisionReason(gate); ok {
			reason = plusReason
		}
		incrementCount(st.counters.RejectedReasonCounts, reason)
		incrementCount(st.counters.GateRejectReasonCounts, reason)
		return resolverDecision{Reason: reason}
	}

	if shouldRejectCTYCall(winnerCall) {
		st.counters.Rejected++
		st.counters.RejectedInvalidBase++
		incrementCount(st.counters.RejectedReasonCounts, "resolver_invalid_base")
		if strings.EqualFold(st.cfg.CallCorrection.InvalidAction, "suppress") {
			st.counters.Suppressed++
			return resolverDecision{Reason: "resolver_invalid_base", Suppress: true}
		}
		spotEntry.Confidence = "B"
		return resolverDecision{Reason: "resolver_invalid_base"}
	}
	if st.ctyDB != nil {
		if _, ok := st.ctyDB.LookupCallsignPortable(winnerCall); !ok {
			st.counters.Rejected++
			st.counters.RejectedCTYMiss++
			incrementCount(st.counters.RejectedReasonCounts, "resolver_cty_miss")
			if strings.EqualFold(st.cfg.CallCorrection.InvalidAction, "suppress") {
				st.counters.Suppressed++
				return resolverDecision{Reason: "resolver_cty_miss", Suppress: true}
			}
			spotEntry.Confidence = "B"
			return resolverDecision{Reason: "resolver_cty_miss"}
		}
	}

	spotEntry.DXCall = winnerCall
	spotEntry.DXCallNorm = winnerCall
	spotEntry.Confidence = "C"

	st.counters.Applied++
	reason := resolverDecisionApplied
	if gate.RecentPlus1Applied && selection.WinnerOverride {
		reason = resolverDecisionAppliedNeighborRecentPlus1
		st.counters.AppliedResolverNeighborRecentPlus1++
	} else if gate.RecentPlus1Applied {
		reason = resolverDecisionAppliedRecentPlus1
		st.counters.AppliedResolverRecentPlus1++
	} else if selection.WinnerOverride {
		reason = resolverDecisionAppliedNeighbor
		st.counters.AppliedResolverNeighborOverride++
	} else {
		st.counters.AppliedResolver++
	}
	incrementCount(st.counters.AppliedReasonCounts, reason)
	return resolverDecision{Applied: true, Winner: winnerCall, Reason: reason}
}

func buildEvalReport(st *runState, labels otherLoadStats) evalReport {
	report := evalReport{
		ResolverMode: st.cfg.CallCorrection.ResolverMode,
		Replay:       st.replay,
		Labels:       labels,
		Counters:     st.counters,
	}

	report.PredictedAppliedTotal = countPredictedTotal(st.predictedByKey)
	report.PredictedOnUnlabeled = st.predOnUnlabeled
	report.NegativesSeen = st.negSeen
	report.NegativesSampled = int64(len(st.negSamples))
	for _, applied := range st.negSamples {
		if applied {
			report.FalsePositive++
		} else {
			report.TrueNegative++
		}
	}

	correct, missed, extra, mismatches := scorePositives(st.expectedByKey, st.predictedByKey, st.in, st.freqBucketHz)
	report.PositivesTotal = countExpectedTotal(st.expectedByKey)
	report.CorrectPositive = correct
	report.MissedPositive = missed
	report.PredictedExtraOnPositives = extra
	report.Mismatches = mismatches

	if report.PositivesTotal > 0 {
		report.RecallPercent = round2(float64(report.CorrectPositive) * 100 / float64(report.PositivesTotal))
	}
	if report.CorrectPositive+report.FalsePositive > 0 {
		report.PrecisionPercentSampled = round2(float64(report.CorrectPositive) * 100 / float64(report.CorrectPositive+report.FalsePositive))
	}
	report.RBNRows = st.replay.RBNRows
	return report
}

func scorePositives(
	expectedByKey map[labelKey][]uint32,
	predictedByKey map[labelKey][]predictedEvent,
	in *interner,
	freqBucketHz int64,
) (int64, int64, int64, []mismatch) {
	keys := make([]labelKey, 0, len(expectedByKey))
	for key := range expectedByKey {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].MinuteUnix != keys[j].MinuteUnix {
			return keys[i].MinuteUnix < keys[j].MinuteUnix
		}
		if keys[i].FreqKey != keys[j].FreqKey {
			return keys[i].FreqKey < keys[j].FreqKey
		}
		if keys[i].SpotterID != keys[j].SpotterID {
			return keys[i].SpotterID < keys[j].SpotterID
		}
		return keys[i].SubjectID < keys[j].SubjectID
	})

	var correct int64
	var missed int64
	var extra int64
	mismatches := make([]mismatch, 0, 25)

	for _, key := range keys {
		expected := expectedByKey[key]
		predicted := predictedByKey[key]
		used := make([]bool, len(predicted))

		for _, wantWinner := range expected {
			matchIdx := -1
			for i, got := range predicted {
				if used[i] {
					continue
				}
				if got.WinnerID == wantWinner {
					matchIdx = i
					break
				}
			}
			if matchIdx >= 0 {
				used[matchIdx] = true
				correct++
				continue
			}

			missed++
			predID := uint32(0)
			predReason := ""
			for i, got := range predicted {
				if used[i] {
					continue
				}
				used[i] = true
				predID = got.WinnerID
				predReason = got.Reason
				break
			}
			if len(mismatches) < cap(mismatches) {
				mismatches = append(mismatches, mismatch{
					TsUnix:         key.MinuteUnix,
					FreqHz:         key.FreqKey * freqBucketHz,
					Spotter:        in.str(key.SpotterID),
					Subject:        in.str(key.SubjectID),
					Expected:       in.str(wantWinner),
					Predicted:      in.str(predID),
					DecisionReason: predReason,
				})
			}
		}
		for i := range predicted {
			if !used[i] {
				extra++
			}
		}
	}
	return correct, missed, extra, mismatches
}

func countExpectedTotal(expectedByKey map[labelKey][]uint32) int64 {
	var total int64
	for _, winners := range expectedByKey {
		total += int64(len(winners))
	}
	return total
}

func countPredictedTotal(predictedByKey map[labelKey][]predictedEvent) int64 {
	var total int64
	for _, winners := range predictedByKey {
		total += int64(len(winners))
	}
	return total
}

func recordNegativeSample(st *runState, limit int, applied bool) {
	if limit <= 0 {
		return
	}
	if len(st.negSamples) < limit {
		st.negSamples = append(st.negSamples, applied)
		return
	}
	if st.negSeen <= 0 {
		return
	}
	if int64(st.rng.next()%uint64(st.negSeen)) < int64(limit) {
		idx := int(st.rng.next() % uint64(limit))
		st.negSamples[idx] = applied
	}
}

func recordRecentBandObservation(s *spot.Spot, store *spot.RecentBandStore, cfg config.CallCorrectionConfig) {
	if s == nil || store == nil || s.IsBeacon {
		return
	}
	if !cfg.RecentBandBonusEnabled && !cfg.StabilizerEnabled {
		return
	}
	mode := s.ModeNorm
	if strings.TrimSpace(mode) == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return
	}
	call := correctionflow.NormalizedDXCall(s)
	if call == "" {
		return
	}
	band := s.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	band = spot.NormalizeBand(band)
	if band == "" || band == "???" {
		return
	}
	reporter := s.DECallNorm
	if reporter == "" {
		reporter = s.DECall
	}
	store.Record(call, band, mode, reporter, s.Time)
}

func loadOtherLabels(files []string, in *interner, freqBucketHz int64) (map[labelKey][]uint32, otherLoadStats, error) {
	labels := map[labelKey][]uint32{}
	stats := otherLoadStats{}
	for _, path := range files {
		year, ok := inferYear(filepath.Base(path))
		if !ok {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, stats, err
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			stats.Lines++
			parts := strings.Fields(line)
			if len(parts) < 8 || strings.ToUpper(parts[len(parts)-1]) != "CW" {
				continue
			}
			dbIdx := -1
			for i := len(parts) - 3; i >= 4; i-- {
				if _, err := strconv.Atoi(parts[i]); err == nil {
					dbIdx = i
					break
				}
			}
			if dbIdx < 0 {
				stats.SkippedNoDB++
				continue
			}
			corrected := strings.Join(parts[4:dbIdx], " ")
			if corrected == "?" || corrected == "" || strings.Contains(corrected, "Harmonic") || strings.HasPrefix(corrected, "*") {
				stats.SkippedHarmonic++
				continue
			}
			corrected = strings.TrimPrefix(corrected, "?")
			ts, err := time.Parse("02-Jan-2006 1504Z", fmt.Sprintf("%s-%04d %s", parts[0], year, parts[1]))
			if err != nil {
				stats.SkippedInvalid++
				continue
			}
			spotterID := in.intern(parts[len(parts)-2])
			subjectID := in.intern(parts[2])
			winnerID := in.intern(corrected)
			if spotterID == 0 || subjectID == 0 || winnerID == 0 || subjectID == winnerID {
				stats.SkippedInvalid++
				continue
			}
			if !spot.IsValidNormalizedCallsign(in.str(subjectID)) || !spot.IsValidNormalizedCallsign(in.str(winnerID)) {
				stats.SkippedInvalid++
				continue
			}
			key := labelKey{
				MinuteUnix: minuteUnix(ts.UTC()),
				FreqKey:    freqKeyFromKHzString(parts[3], freqBucketHz),
				SpotterID:  spotterID,
				SubjectID:  subjectID,
			}
			existing := labels[key]
			if len(existing) > 0 {
				last := existing[len(existing)-1]
				if last != winnerID {
					stats.ConflictingLabel++
				}
			}
			labels[key] = append(labels[key], winnerID)
			stats.Parsed++
		}
	}
	return labels, stats, nil
}

func discover(root string, exts []string) ([]string, error) {
	set := map[string]struct{}{}
	for _, ext := range exts {
		set[strings.ToLower(ext)] = struct{}{}
	}
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if _, ok := set[strings.ToLower(filepath.Ext(d.Name()))]; ok {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func openRBNReader(path string) (io.Reader, func() error, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		return f, f.Close, nil
	case ".zip":
		zr, err := zip.OpenReader(path)
		if err != nil {
			return nil, nil, err
		}
		for _, member := range zr.File {
			if strings.HasSuffix(strings.ToLower(member.Name), ".csv") {
				rc, err := member.Open()
				if err != nil {
					_ = zr.Close()
					return nil, nil, err
				}
				return rc, func() error {
					_ = rc.Close()
					return zr.Close()
				}, nil
			}
		}
		_ = zr.Close()
		return nil, nil, fmt.Errorf("zip %q: no .csv member found", path)
	default:
		return nil, nil, fmt.Errorf("unsupported extension %q", filepath.Ext(path))
	}
}

func parseRBNHeader(row []string) (rbnHeader, error) {
	h := rbnHeader{callsign: -1, freq: -1, band: -1, dx: -1, db: -1, date: -1, txMode: -1}
	for i, raw := range row {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "callsign":
			h.callsign = i
		case "freq":
			h.freq = i
		case "band":
			h.band = i
		case "dx":
			h.dx = i
		case "db":
			h.db = i
		case "date":
			h.date = i
		case "tx_mode":
			h.txMode = i
		}
	}
	missing := make([]string, 0, 7)
	if h.callsign < 0 {
		missing = append(missing, "callsign")
	}
	if h.freq < 0 {
		missing = append(missing, "freq")
	}
	if h.dx < 0 {
		missing = append(missing, "dx")
	}
	if h.db < 0 {
		missing = append(missing, "db")
	}
	if h.date < 0 {
		missing = append(missing, "date")
	}
	if h.txMode < 0 {
		missing = append(missing, "tx_mode")
	}
	if len(missing) > 0 {
		return h, fmt.Errorf("%s", strings.Join(missing, ","))
	}
	return h, nil
}

func parseRBNRecord(record []string, h rbnHeader) (rbnRow, bool) {
	get := func(idx int) string {
		if idx < 0 || idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}
	mode := strings.ToUpper(get(h.txMode))
	if !spot.IsCallCorrectionCandidate(mode) {
		return rbnRow{}, false
	}
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", get(h.date), time.UTC)
	if err != nil {
		return rbnRow{}, false
	}
	freqKHz, err := strconv.ParseFloat(get(h.freq), 64)
	if err != nil || freqKHz <= 0 {
		return rbnRow{}, false
	}
	dxCall := spot.NormalizeCallsign(get(h.dx))
	spotter := spot.NormalizeCallsign(get(h.callsign))
	if dxCall == "" || spotter == "" {
		return rbnRow{}, false
	}
	report := 0
	dbRaw := get(h.db)
	if v, err := strconv.Atoi(dbRaw); err == nil {
		report = v
	} else if v, err := strconv.ParseFloat(dbRaw, 64); err == nil {
		report = int(v + 0.5)
	} else {
		return rbnRow{}, false
	}
	band := spot.NormalizeBand(get(h.band))
	if band == "" {
		band = spot.NormalizeBand(spot.FreqToBand(freqKHz))
	}
	return rbnRow{Time: ts.UTC(), FreqKHz: freqKHz, Band: band, DXCall: dxCall, Spotter: spotter, Mode: mode, Report: report}, true
}

func configureULS(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("nil config")
	}
	uls.SetLicenseChecksEnabled(cfg.FCCULS.Enabled)
	uls.SetLicenseDBPath(strings.TrimSpace(cfg.FCCULS.DBPath))
	allowlistPath := strings.TrimSpace(cfg.FCCULS.AllowlistPath)
	if allowlistPath == "" {
		uls.SetAllowlistPath("")
		return nil
	}
	if _, err := os.Stat(allowlistPath); err != nil {
		return fmt.Errorf("fcc_uls.allowlist_path missing/unreadable %s: %w", allowlistPath, err)
	}
	uls.SetAllowlistPath(allowlistPath)
	return nil
}

func loadCTY(cfg *config.Config) (*cty.CTYDatabase, error) {
	if cfg == nil || !cfg.CTY.Enabled {
		return nil, nil
	}
	path := strings.TrimSpace(cfg.CTY.File)
	if path == "" {
		return nil, errors.New("cty.enabled=true but cty.file is empty")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("cty.file missing/unreadable %s: %w", path, err)
	}
	return cty.LoadCTYDatabase(path)
}

func loadKnownCallset(path string) (*spot.KnownCallsigns, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("known_calls.file missing/unreadable %s: %w", path, err)
	}
	return spot.LoadKnownCallsigns(path)
}

func shouldRejectCTYCall(call string) bool {
	base := strings.TrimSpace(uls.NormalizeForLicense(call))
	if base == "" {
		return false
	}
	if uls.AllowlistMatchAny(base) {
		return false
	}
	return hasLeadingLettersBeforeDigit(base, 3)
}

func hasLeadingLettersBeforeDigit(call string, threshold int) bool {
	if threshold <= 0 {
		return false
	}
	call = strutil.NormalizeUpper(call)
	if call == "" {
		return false
	}
	count := 0
	for i := 0; i < len(call); i++ {
		ch := call[i]
		if ch >= '0' && ch <= '9' {
			return count >= threshold
		}
		if ch >= 'A' && ch <= 'Z' {
			count++
			if count >= threshold {
				return true
			}
			continue
		}
		break
	}
	return count >= threshold
}

func writeSummary(path string, report evalReport, method inferredMethod) {
	text := fmt.Sprintf(
		"callcorr_reveng summary\n======================\n\nMethod source:\n  config_loaded_from=%s\n  resolver_mode=%s\n\nOther logs:\n  lines=%d parsed=%d skipped_harmonic=%d skipped_invalid=%d skipped_no_db=%d conflicting_label=%d\n\nReplay:\n  rbn_rows=%d candidate_rows=%d skipped_bad_row=%d skipped_schema=%d\n\nPrimary counters:\n  decision_total=%d applied=%d rejected=%d suppressed=%d\n  no_snapshot=%d neighborhood_conflict=%d state_split=%d state_uncertain=%d state_unknown=%d\n  invalid_base=%d cty_miss=%d same_call=%d winner_missing=%d\n  neighborhood_used=%d override=%d split=%d excluded_unrelated=%d excluded_distance=%d excluded_anchor_missing=%d\n  recent_plus1_considered=%d applied=%d rejected=%d\n\nEvaluation:\n  positives_total=%d\n  correct_positive=%d\n  missed_positive=%d\n  predicted_extra_on_positives=%d\n  negatives_seen=%d\n  negatives_sampled=%d\n  false_positive=%d\n  true_negative=%d\n  predicted_applied_total=%d\n  predicted_on_unlabeled=%d\n  recall_percent=%.2f\n  precision_percent_sampled=%.2f\n",
		method.ConfigLoadedFrom, method.ResolverMode,
		report.Labels.Lines, report.Labels.Parsed, report.Labels.SkippedHarmonic, report.Labels.SkippedInvalid, report.Labels.SkippedNoDB, report.Labels.ConflictingLabel,
		report.Replay.RBNRows, report.Replay.CandidateRows, report.Replay.SkippedBadRow, report.Replay.SkippedSchema,
		report.Counters.DecisionTotal, report.Counters.Applied, report.Counters.Rejected, report.Counters.Suppressed,
		report.Counters.RejectedNoSnapshot, report.Counters.RejectedNeighborhoodConflict, report.Counters.RejectedStateSplit, report.Counters.RejectedStateUncertain, report.Counters.RejectedStateUnknown,
		report.Counters.RejectedInvalidBase, report.Counters.RejectedCTYMiss, report.Counters.RejectedSameCall, report.Counters.RejectedWinnerMissing,
		report.Counters.NeighborhoodUsed, report.Counters.NeighborhoodWinnerOverride, report.Counters.NeighborhoodConflictSplit, report.Counters.NeighborhoodExcludedUnrelated, report.Counters.NeighborhoodExcludedDistance, report.Counters.NeighborhoodExcludedAnchorMissing,
		report.Counters.RecentPlus1Considered, report.Counters.RecentPlus1Applied, report.Counters.RecentPlus1Rejected,
		report.PositivesTotal, report.CorrectPositive, report.MissedPositive, report.PredictedExtraOnPositives,
		report.NegativesSeen, report.NegativesSampled, report.FalsePositive, report.TrueNegative, report.PredictedAppliedTotal, report.PredictedOnUnlabeled,
		report.RecallPercent, report.PrecisionPercentSampled,
	)
	_ = os.WriteFile(path, []byte(text), 0o644)
}

func mustWrite(path string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Printf("marshal %s: %v", path, err)
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("write %s: %v", path, err)
	}
}

func incrementCount(m map[string]int64, key string) {
	if m == nil {
		return
	}
	m[key]++
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minuteUnix(ts time.Time) int64 {
	u := ts.UTC().Unix()
	return u - (u % 60)
}

func normalizeCall(raw string) string {
	call := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(raw, ".", "/")))
	call = strings.TrimSuffix(call, "/")
	return call
}

func inferYear(base string) (int, bool) {
	parts := strings.Split(base, "-")
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.Split(parts[i], ".")[0]
		if len(part) != 4 {
			continue
		}
		year, err := strconv.Atoi(part)
		if err != nil {
			continue
		}
		if year >= 1980 && year <= 2100 {
			return year, true
		}
	}
	return 0, false
}

func freqKeyFromKHzString(raw string, bucketHz int64) int64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	return freqKeyFromKHz(v, bucketHz)
}

func freqKeyFromKHz(freqKHz float64, bucketHz int64) int64 {
	if bucketHz <= 0 {
		bucketHz = defaultFreqBucketHz
	}
	hz := int64(math.Round(freqKHz * 1000))
	return (hz + bucketHz/2) / bucketHz
}

func resolverGateDecisionReason(reason string) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	if reason == "" {
		reason = "unknown"
	}
	return resolverDecisionGatePrefix + reason
}

func resolverRecentPlus1DecisionReason(gate spot.ResolverPrimaryGateResult) (string, bool) {
	if !gate.RecentPlus1Considered || gate.RecentPlus1Applied {
		return "", false
	}
	reject := strings.ToLower(strings.TrimSpace(gate.RecentPlus1Reject))
	if reject == "" {
		return "", false
	}
	return resolverDecisionRecentPlus1RejectPrefix + reject, true
}

const (
	resolverDecisionApplied                     = "resolver_applied"
	resolverDecisionAppliedNeighbor             = "resolver_applied_neighbor_override"
	resolverDecisionAppliedRecentPlus1          = "resolver_applied_recent_plus1"
	resolverDecisionAppliedNeighborRecentPlus1  = "resolver_applied_neighbor_recent_plus1"
	resolverDecisionGatePrefix                  = "resolver_gate_"
	resolverDecisionRecentPlus1RejectPrefix     = "resolver_recent_plus1_reject_"
	resolverRecentPlus1DisallowEditNeighborGate = "edit_neighbor_contested"
)
