// File role: Evaluate complete named experimental outcomes using existing raw
// population, immutable-ID join, follow-on scorer and episode reference floors.
// One day owns at most three streams; results never select a global grid winner.
package main

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
)

type studyMethodEvalDay struct {
	DateUTC   string                            `json:"date_utc"`
	RawRows   int                               `json:"archive_rows"`
	FocalRows int                               `json:"focal_rows"`
	Counts    studyMethodEvalCounts             `json:"counts"`
	Modes     map[string]*studyMethodEvalCounts `json:"modes"`
	Episodes  studyMethodEvalEpisodeChanges     `json:"baseline_episode_changes"`
	Cells     map[string]studyCellDate          `json:"all_observed_cells"`
}
type studyMethodEvalVariantResult struct {
	Candidate           studyMethodCandidate              `json:"candidate"`
	Counts              studyMethodEvalCounts             `json:"pooled_counts"`
	Modes               map[string]*studyMethodEvalCounts `json:"pooled_modes"`
	Days                []studyMethodEvalDay              `json:"days"`
	Episodes            studyMethodEvalEpisodeChanges     `json:"baseline_episode_changes"`
	Cells               []studyCellVerdict                `json:"active_cell_reference_floors"`
	ObjectivePass       bool                              `json:"complete_population_objective_pass"`
	Eligible            bool                              `json:"eligible"`
	EligibilityAssessed bool                              `json:"eligibility_assessed"`
	AllCellRenamePass   bool                              `json:"all_cells_strong_disagreeing_rename_gate_pass"`
}
type studyMethodEvaluationResult struct {
	SchemaVersion     int                               `json:"schema_version"`
	Experimental      bool                              `json:"experimental"`
	Complete          bool                              `json:"complete"`
	Phase             string                            `json:"phase"`
	EvaluationPlan    studyFile                         `json:"evaluation_plan"`
	Inputs            studyMethodEvaluationPlan         `json:"frozen_input_bindings"`
	RoleOrder         []string                          `json:"role_order"`
	CurrentStability  replayStabilityConfig             `json:"current_stability"`
	Variants          []studyMethodEvalVariantResult    `json:"variants"`
	OrdinalVsGeometry *studyMethodEvalVariantResult     `json:"ordinal_vs_geometry,omitempty"`
	Paired            studyFile                         `json:"full_paired_union"`
	Receipts          studyFile                         `json:"applied_receipts"`
	Outputs           []studyDiagnosticOutputFile       `json:"closed_prior_outputs"`
	Accounting        studyDiagnosticArtifactAccounting `json:"accounting"`
	ClaimLimits       []string                          `json:"claim_limits"`
}

type studyMethodEvalTerminal struct {
	Receipt         studyAuditReceipt `json:"receipt"`
	Reason          string            `json:"reason"`
	Held            bool              `json:"held"`
	TemporalAction  string            `json:"temporal_action,omitempty"`
	AfterCutoff     bool              `json:"decision_after_cutoff"`
	StabilizerDelay bool              `json:"stabilizer_proxy_delay"`
}
type studyMethodEvalPairRecord struct {
	DateUTC         string                    `json:"date_utc"`
	RawID           string                    `json:"raw_id"`
	RawSHA256       string                    `json:"raw_sha256"`
	EpisodeID       string                    `json:"episode_id"`
	Mode            string                    `json:"mode"`
	SubjectIdentity string                    `json:"subject_identity"`
	R1              studyReference            `json:"r1"`
	R2Frozen        studyReference            `json:"r2_frozen"`
	Outcomes        []studyMethodEvalTerminal `json:"outcomes_in_role_order"`
}

type studyMethodEvalAppliedRecord struct {
	DateUTC string                  `json:"date_utc"`
	RawID   string                  `json:"raw_id"`
	Mode    string                  `json:"mode"`
	Role    string                  `json:"role"`
	Outcome studyMethodEvalTerminal `json:"outcome"`
}

func newStudyMethodEvaluationResult(p *studyMethodEvalPrepared) studyMethodEvaluationResult {
	r := studyMethodEvaluationResult{SchemaVersion: 1, Experimental: true, Complete: true, Phase: p.Plan.Phase, EvaluationPlan: p.File, Inputs: p.Plan, RoleOrder: []string{"control"}, CurrentStability: replayStabilityConfig{WindowMinutes: 15, MinFollowOn: 3, FreqToleranceHz: 500},
		ClaimLimits: []string{
			"Experimental complete-candidate comparison; no global grid winner is selected here",
			"Archive reference agreement and follow-on stability are not RF truth or calibrated probability",
			"Exposed tuning dates, including former holdout dates, are not prospective confirmation",
			"R1 and R2Frozen are shared immutable row facts; actual R2 is decision_after_cutoff unknown when the corresponding outcome flag is true",
			"The reference gate retains the inherited new-target rename predicate; newly assessable disagreement on the same delivered identity is descriptive and reported separately",
			"All loss/retarget/retime overlaps and affected-ID unions are reported; no tie-break policy is chosen",
			"Closed prior-output inventory/accounting excludes aggregates.json and COMPLETE.json themselves; both are charged before COMPLETE-last",
			"Independent predictor states are required by frozen captures; this evaluator reads immutable outcomes only",
		}}
	for _, candidate := range p.Variants {
		r.RoleOrder = append(r.RoleOrder, candidate.ID)
		r.Variants = append(r.Variants, studyMethodEvalVariantResult{Candidate: candidate, Counts: studyMethodEvalNewCounts(), Modes: make(map[string]*studyMethodEvalCounts)})
	}
	if len(p.Variants) == 2 {
		r.OrdinalVsGeometry = &studyMethodEvalVariantResult{Candidate: p.Variants[1], Counts: studyMethodEvalNewCounts(), Modes: make(map[string]*studyMethodEvalCounts)}
	}
	return r
}

func studyMethodEvaluateDay(p *studyMethodEvalPrepared, loaded studyMethodEvalLoadedGroup, aliases map[string]string, result *studyMethodEvaluationResult, paired, receipts *studyMethodEvalWriter) error {
	day, err := loadStudyRawDay(loaded.Input, aliases, p.Capture.WarmupSeconds)
	if err != nil {
		return err
	}
	day.Index = nil
	spots, accepted, bad, skipped, focal := studyAuditRawPopulation(day)
	for role, report := range loaded.Reports {
		manifest := loaded.Manifests[role]
		if report.RawRows != len(day.Rows) || report.FocalRows != focal || report.TerminalRows != focal || manifest.CSV.RecordsTotal != int64(len(day.Rows)) || manifest.CSV.RecordsProcessed != int64(accepted) || manifest.CSV.RecordsSkippedBad != int64(bad) || manifest.CSV.RecordsSkippedMode != int64(skipped) {
			return errors.New("evaluation archive/report population does not reconcile")
		}
	}
	pairs, err := studyReadDayPairs(loaded.Input, loaded.Reports)
	if err != nil {
		return err
	}
	if err := studyAuditArchiveBindings(day, pairs); err != nil {
		return err
	}
	controlIdentities := make(studyEpisodeCorrections)
	var geometryIdentities studyEpisodeCorrections
	if len(p.Variants) == 2 {
		geometryIdentities = make(studyEpisodeCorrections)
	}
	for _, pair := range pairs {
		if d := pair.Arms[0]; studyDeliveredCorrection(d) {
			controlIdentities[studyEpisodeIdentity{d.EpisodeID, d.Raw.Mode, d.FinalIdentity}] = struct{}{}
		}
		if geometryIdentities != nil {
			if d := pair.Arms[1]; studyDeliveredCorrection(d) {
				geometryIdentities[studyEpisodeIdentity{d.EpisodeID, d.Raw.Mode, d.FinalIdentity}] = struct{}{}
			}
		}
	}
	accumulators := make([]studyMethodEvalDayAccumulator, len(p.Variants))
	for i, candidate := range p.Variants {
		accumulators[i] = newStudyMethodEvalDayAccumulator(day, candidate)
	}
	var attribution *studyMethodEvalDayAccumulator
	if len(p.Variants) == 2 {
		value := newStudyMethodEvalDayAccumulator(day, p.Variants[1])
		attribution = &value
	}
	legacy := replayStabilityConfig{WindowMinutes: 60, MinFollowOn: 2, FreqToleranceHz: 1000}
	var legacyFractions [3]studyAuditFraction
	for index := range day.Rows {
		pair := pairs[int64(index+1)]
		if pair == nil {
			continue
		}
		first := pair.Arms[0]
		record := studyMethodEvalPairRecord{DateUTC: loaded.Input.DateUTC, RawID: first.RawID, RawSHA256: first.RawSHA256, EpisodeID: first.EpisodeID, Mode: first.Raw.Mode, SubjectIdentity: first.SubjectIdentity, R1: first.R1, R2Frozen: first.R2Frozen}
		for role := range loaded.Bindings {
			d := pair.Arms[role]
			if err := studyMethodEvalR2Binding(d); err != nil {
				return err
			}
			r := studyAuditMakeReceipt(d, false)
			if r.ActualApplied {
				r.CurrentStable = studyAuditStable(d, spots, result.CurrentStability)
				r.LegacyStable = studyAuditStable(d, spots, legacy)
				legacyFractions[role].Applied++
				if r.LegacyStable {
					legacyFractions[role].Stable++
				}
			}
			terminal := studyMethodEvalTerminal{Receipt: r, Reason: d.Reason, Held: d.Held, TemporalAction: d.TemporalAction, AfterCutoff: d.DecisionAfterCutoff, StabilizerDelay: d.StabilizerProxyDelay}
			record.Outcomes = append(record.Outcomes, terminal)
			if r.ActualApplied {
				if err := receipts.record(studyMethodEvalAppliedRecord{loaded.Input.DateUTC, d.RawID, d.Raw.Mode, result.RoleOrder[role], terminal}); err != nil {
					return err
				}
			}
		}
		if err := paired.record(record); err != nil {
			return err
		}
		for i := range accumulators {
			if err := accumulators[i].account(first, pair.Arms[i+1], record.Outcomes[0].Receipt, record.Outcomes[i+1].Receipt, controlIdentities); err != nil {
				return err
			}
		}
		if attribution != nil {
			if err := attribution.account(pair.Arms[1], pair.Arms[2], record.Outcomes[1].Receipt, record.Outcomes[2].Receipt, geometryIdentities); err != nil {
				return err
			}
		}
	}
	for role := range loaded.Bindings {
		want := loaded.Manifests[role].Results.Stability
		if legacyFractions[role].Applied != int64(want.TotalApplied) || legacyFractions[role].Stable != int64(want.StableApplied) {
			return errors.New("evaluation unchanged legacy scorer fails manifest reconciliation")
		}
	}
	if !reflect.DeepEqual(loaded.Reports[0].References, accumulators[0].day.Counts.ControlReferences) {
		return errors.New("control reference/unknown counts do not reconcile with all terminal IDs")
	}
	for i := range accumulators {
		if !reflect.DeepEqual(loaded.Reports[i+1].References, accumulators[i].day.Counts.VariantReferences) {
			return errors.New("variant reference/unknown counts do not reconcile with all terminal IDs")
		}
	}
	for i := range accumulators {
		if err := studyMethodEvalAppendDay(&result.Variants[i], accumulators[i].finish()); err != nil {
			return err
		}
	}
	if attribution != nil {
		if err := studyMethodEvalAppendDay(result.OrdinalVsGeometry, attribution.finish()); err != nil {
			return err
		}
	}
	return nil
}

func studyMethodEvalR2Binding(d *studyDecision) error {
	expected := d.R2Frozen
	if d.DecisionAfterCutoff {
		expected = studyReference{Reason: "decision_after_cutoff"}
	}
	if d.R2 != expected {
		return errors.New("actual R2 does not follow its frozen reference and decision cutoff")
	}
	return nil
}

type studyMethodEvalDayAccumulator struct {
	day            studyMethodEvalDay
	active         map[string]bool
	cells          map[string]map[string]*studyEpisodeCell
	changes        map[string]uint8
	bound, entries int
}

func newStudyMethodEvalDayAccumulator(day *studyRawDay, candidate studyMethodCandidate) studyMethodEvalDayAccumulator {
	a := studyMethodEvalDayAccumulator{day: studyMethodEvalDay{DateUTC: day.Start.Format("2006-01-02"), RawRows: len(day.Rows), Counts: studyMethodEvalNewCounts(), Modes: make(map[string]*studyMethodEvalCounts)}, active: make(map[string]bool, 2), cells: make(map[string]map[string]*studyEpisodeCell), changes: make(map[string]uint8), bound: 2 * len(day.Rows)}
	for _, distance := range candidate.CWDistances {
		cell := fmt.Sprintf("CW/%d", distance)
		a.active[cell] = true
		a.cells[cell] = make(map[string]*studyEpisodeCell)
	}
	return a
}

func (a *studyMethodEvalDayAccumulator) account(control, variant *studyDecision, x, y studyAuditReceipt, identities studyEpisodeCorrections) error {
	if err := studyMethodEvalAccount(&a.day.Counts, control, variant, x, y, a.active); err != nil {
		return err
	}
	mode := variant.Raw.Mode
	if a.day.Modes[mode] == nil {
		if len(a.day.Modes) >= 2 {
			return errors.New("evaluation focal mode inventory exceeds CW/RTTY")
		}
		counts := studyMethodEvalNewCounts()
		a.day.Modes[mode] = &counts
	}
	if err := studyMethodEvalAccount(a.day.Modes[mode], control, variant, x, y, a.active); err != nil {
		return err
	}
	mask := studyMethodEvalEpisodeMask(x, y)
	if mask != 0 {
		if _, ok := a.changes[variant.EpisodeID]; !ok && len(a.changes) >= a.bound {
			return errors.New("evaluation episode changes exceed day bound")
		}
		a.changes[variant.EpisodeID] |= mask
	}
	var cells [2]string
	count := 0
	for _, d := range []*studyDecision{control, variant} {
		if !studyDeliveredCorrection(d) {
			continue
		}
		cell := fmt.Sprintf("%s/%d", d.Raw.Mode, d.ConfiguredDistance)
		if count == 1 && cells[0] == cell {
			continue
		}
		cells[count] = cell
		count++
	}
	for _, cell := range cells[:count] {
		if a.cells[cell] == nil {
			if len(a.cells) >= 256 {
				return errors.New("evaluation mode/distance cells exceed 256")
			}
			a.cells[cell] = make(map[string]*studyEpisodeCell)
		}
		ep := a.cells[cell][variant.EpisodeID]
		if ep == nil {
			if a.entries >= a.bound {
				return errors.New("evaluation episode/cell entries exceed twice the raw rows")
			}
			ep = &studyEpisodeCell{CoveredR1: true, CoveredR2: true, Joint: true}
			a.cells[cell][variant.EpisodeID] = ep
			a.entries++
		}
		studyAccountEpisode(ep, control, variant, cell, identities)
	}
	return nil
}
func (a *studyMethodEvalDayAccumulator) finish() studyMethodEvalDay {
	a.day.FocalRows = int(a.day.Counts.RawIDs)
	a.day.Cells = studySummarizeDate(a.day.DateUTC, a.cells)
	a.day.Episodes = studyMethodEvalEpisodeTotals(a.changes)
	return a.day
}

func studyMethodEvalAppendDay(result *studyMethodEvalVariantResult, day studyMethodEvalDay) error {
	result.Days = append(result.Days, day)
	if err := studyMethodEvalMerge(&result.Counts, day.Counts); err != nil {
		return err
	}
	studyMethodEvalMergeEpisodes(&result.Episodes, day.Episodes)
	for mode, counts := range day.Modes {
		if result.Modes[mode] == nil {
			value := studyMethodEvalNewCounts()
			result.Modes[mode] = &value
		}
		if err := studyMethodEvalMerge(result.Modes[mode], *counts); err != nil {
			return err
		}
	}
	return nil
}

func studyMethodEvalFinish(result *studyMethodEvaluationResult, phase string) {
	for i := range result.Variants {
		variant := &result.Variants[i]
		variant.EligibilityAssessed = true
		variant.ObjectivePass = studyMethodEvalObjective(variant.Counts)
		variant.AllCellRenamePass = variant.Counts.NewWrongR1 == 0 && variant.Counts.NewWrongR2 == 0
		variant.Eligible = variant.ObjectivePass && variant.AllCellRenamePass
		for _, distance := range variant.Candidate.CWDistances {
			cell := fmt.Sprintf("CW/%d", distance)
			dates := make([]studyCellDate, 0, len(variant.Days))
			for dayIndex := range variant.Days {
				day := &variant.Days[dayIndex]
				value := day.Cells[cell]
				value.DateUTC = day.DateUTC
				dates = append(dates, value)
			}
			sort.Slice(dates, func(i, j int) bool { return dates[i].DateUTC < dates[j].DateUTC })
			verdict := studyQualifyCell(cell, dates)
			variant.Cells = append(variant.Cells, verdict)
			if phase == "holdout" {
				variant.Eligible = variant.Eligible && verdict.Pass
			}
		}
	}
	if result.OrdinalVsGeometry != nil {
		result.OrdinalVsGeometry.ObjectivePass = studyMethodEvalObjective(result.OrdinalVsGeometry.Counts)
		result.OrdinalVsGeometry.AllCellRenamePass = result.OrdinalVsGeometry.Counts.NewWrongR1 == 0 && result.OrdinalVsGeometry.Counts.NewWrongR2 == 0
	}
}
