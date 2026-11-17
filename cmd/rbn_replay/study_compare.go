// Paired study accounting uses the full raw-ID union. Every changed output is
// streamed; qualification collapses repeats only at the frozen episode/cell.
// Per-day maps have the plan's enforced raw-row bound and are released between
// dates. No whole-study raw registry or intersection-only denominator is kept.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"dxcluster/spot"
)

type studyPair struct {
	Arms [3]*studyDecision
}

type studyEpisodeCell struct {
	Incremental, CoveredR1, CoveredR2, Joint bool
	NewWrongR1, NewWrongR2                   bool
	OnSupportedR1, OffSupportedR1            bool
	OnSupportedR2, OffSupportedR2            bool
}

type studyCellDate struct {
	DateUTC        string `json:"date_utc"`
	Incremental    int    `json:"all_incremental_episode_ids"`
	Joint          int    `json:"jointly_strong_incremental_episodes"`
	CoveredR1      int    `json:"r1_covered_incremental_episodes"`
	CoveredR2      int    `json:"r2_covered_incremental_episodes"`
	NewWrongR1     int    `json:"r1_new_strong_contradictions"`
	NewWrongR2     int    `json:"r2_new_strong_contradictions"`
	NetSupportedR1 int    `json:"r1_net_supported_correction_episodes"`
	NetSupportedR2 int    `json:"r2_net_supported_correction_episodes"`
}

type studyCellVerdict struct {
	Cell       string          `json:"cell"`
	Pass       bool            `json:"pass"`
	Dates      []studyCellDate `json:"dates"`
	Joint      int             `json:"jointly_strong_incremental_episodes"`
	CoverageR1 float64         `json:"r1_coverage_percent"`
	CoverageR2 float64         `json:"r2_coverage_percent"`
	Reasons    []string        `json:"reasons"`
}

type studyMissed struct {
	RawID, EpisodeID, DateUTC, Mode            string
	Subject, Reference, Reason                 string
	PlainDistance, ConfiguredDistance, Support int
	FocalSNR                                   int
}

type studyComparison struct {
	SchemaVersion             int                `json:"schema_version"`
	Complete                  bool               `json:"complete"`
	PlanSHA256                string             `json:"plan_sha256"`
	CandidateSHA256           string             `json:"candidate_sha256"`
	Phase                     string             `json:"phase"`
	Verdict                   string             `json:"verdict"`
	AllActiveCellsPass        bool               `json:"all_active_cells_pass"`
	Cells                     []studyCellVerdict `json:"cells"`
	RawUnion                  int64              `json:"raw_id_union"`
	UnpairedRawIDs            int64              `json:"unpaired_raw_ids"`
	BayesOutcomes             map[string]int64   `json:"bayes_paired_outcomes"`
	RepairOutcomes            map[string]int64   `json:"repair_vs_shipped_paired_outcomes"`
	CrossCellEpisodes         int64              `json:"episodes_changed_in_multiple_cells"`
	Spillover                 []studySpillover   `json:"outside_active_cells"`
	NewStrongContradictionsR1 int                `json:"all_cells_new_r1_strong_contradictions"`
	NewStrongContradictionsR2 int                `json:"all_cells_new_r2_strong_contradictions"`
	Missed                    []studyMissed      `json:"ranked_missed_examples"`
	Paired                    studyFile          `json:"paired_outcomes"`
	ClaimLimits               []string           `json:"claim_limits"`
}

type studySpillover struct {
	Cell   string        `json:"cell"`
	Counts studyCellDate `json:"counts"`
}

func compareCCV2Study(p studyPlan, digest string, req studyCLIRequest) (err error) {
	candidate, err := studySelectCandidate(p, req)
	if err != nil {
		return err
	}
	reports, err := studyReadReports(p, digest, req, candidate)
	if err != nil {
		return err
	}
	if err := os.Mkdir(req.Output, 0o700); err != nil {
		return fmt.Errorf("study comparison output must be new: %w", err)
	}
	pairedPath := filepath.Join(req.Output, "paired.jsonl")
	file, err := os.OpenFile(pairedPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, file.Close())
		}
	}()
	encoder := json.NewEncoder(file)
	result := studyComparison{SchemaVersion: 1, PlanSHA256: digest, CandidateSHA256: hashAnyStable(candidate), Phase: req.Phase,
		BayesOutcomes: make(map[string]int64), RepairOutcomes: make(map[string]int64),
		ClaimLimits: []string{"Reduced-input empty-A-history archive population; no RF-truth or calibrated probability claim",
			"Physical receiver/site/decoder independence and production steady-state history are unverified",
			"Largest-ZIP selection, date/receiver/episode dependence and training exposure can bias results",
			"Replay stabilization is a proxy, not a network delivery or paced-capacity qualification",
			"Every holdout active cell must pass in this exact complete combination; no cell-winner selection"}}
	cellDates := make(map[string][]studyCellDate)
	for _, in := range p.Inputs {
		if in.Phase != req.Phase {
			continue
		}
		pairs, err := studyReadDayPairs(in, reports[in.DateUTC])
		if err != nil {
			return err
		}
		dateCells, err := studyAccountDay(in, pairs, candidate, &result, encoder, p.MaxExamples)
		if err != nil {
			return err
		}
		for cell, count := range dateCells {
			cellDates[cell] = append(cellDates[cell], count)
			result.NewStrongContradictionsR1 += count.NewWrongR1
			result.NewStrongContradictionsR2 += count.NewWrongR2
		}
	}
	for _, spillover := range result.Spillover {
		result.NewStrongContradictionsR1 += spillover.Counts.NewWrongR1
		result.NewStrongContradictionsR2 += spillover.Counts.NewWrongR2
	}
	result.AllActiveCellsPass = len(studySortedCells(candidate.ModeDistances)) > 0 && req.Phase == "holdout" &&
		result.UnpairedRawIDs == 0 && result.NewStrongContradictionsR1 == 0 && result.NewStrongContradictionsR2 == 0
	for _, cell := range studySortedCells(candidate.ModeDistances) {
		verdict := studyQualifyCell(cell, cellDates[cell])
		result.Cells = append(result.Cells, verdict)
		result.AllActiveCellsPass = result.AllActiveCellsPass && verdict.Pass
	}
	result.Verdict = "whole_bayes_off"
	if result.AllActiveCellsPass {
		result.Verdict = "archive_reference_floors_passed"
	}
	if req.Phase == "tuning" {
		result.Verdict = "tuning_only_not_holdout_qualified"
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		closed = true
		return err
	}
	closed = true
	sum, err := studyHashFile(pairedPath)
	if err != nil {
		return err
	}
	result.Paired, result.Complete = studyFile{Path: pairedPath, SHA256: sum}, true
	return writeJSONAtomic(filepath.Join(req.Output, "comparison.json"), result)
}

func studyArmIndex(name string) int {
	switch name {
	case "shipped":
		return 0
	case "repaired_off":
		return 1
	case "repaired_on":
		return 2
	default:
		return -1
	}
}

func studyReadReports(p studyPlan, digest string, req studyCLIRequest, candidate studyCandidate) (map[string][3]*studyReport, error) {
	if err := studyValidateArms(p.Arms); err != nil {
		return nil, err
	}
	paths := strings.Split(req.Compare, ",")
	if len(paths) > 96 {
		return nil, errors.New("study comparison exceeds 32 days/96 arm reports")
	}
	reports := make(map[string][3]*studyReport)
	inputs := make(map[string]studyInput)
	arms := make(map[string]studyArm)
	for _, arm := range p.Arms {
		arms[arm.Name] = arm
	}
	for _, in := range p.Inputs {
		if in.Phase == req.Phase {
			inputs[in.DateUTC] = in
		}
	}
	if len(inputs) == 0 || len(paths) != 3*len(inputs) {
		return nil, errors.New("study comparison requires every frozen date and all three arms")
	}
	for _, path := range paths {
		data, err := studyReadBounded(strings.TrimSpace(path), studyMaxJSONBytes)
		if err != nil {
			return nil, err
		}
		var report studyReport
		if err := studyDecodeJSON(data, &report); err != nil {
			return nil, err
		}
		index := studyArmIndex(report.Arm)
		arm := arms[report.Arm]
		in, known := inputs[report.DateUTC]
		if !known || index < 0 || report.SchemaVersion != 1 || !report.Complete || report.PlanSHA256 != digest ||
			report.ReferenceVersion != p.ReferenceVersion || report.Phase != req.Phase || report.CandidateSHA256 != hashAnyStable(candidate) ||
			report.SourceRevision != arm.SourceRevision || !strings.EqualFold(report.ExecutableSHA256, arm.ExecutableSHA256) ||
			report.AssetsSHA256 != studyAssetDigest(arm.Assets) || report.ConfigFilesSHA256 != hashAnyStable(arm.ConfigFiles) ||
			report.FocalRows != report.TerminalRows || report.RawRows > in.MaxRows || report.RawRows < 1 ||
			report.MaxRows != in.MaxRows || report.MaxCSVBytes != in.MaxCSVBytes || !studyValidFile(report.Decisions) {
			return nil, errors.New("study comparison refused incomplete, mismatched or unfrozen report")
		}
		group := reports[report.DateUTC]
		if group[index] != nil {
			return nil, errors.New("study comparison contains a duplicate date/arm")
		}
		group[index] = &report
		reports[report.DateUTC] = group
	}
	for _, group := range reports {
		if group[0] == nil || group[1] == nil || group[2] == nil || group[1].AssetsSHA256 != group[2].AssetsSHA256 ||
			group[1].PipelineWithoutBayesSHA256 == "" || group[1].PipelineWithoutBayesSHA256 != group[2].PipelineWithoutBayesSHA256 ||
			group[1].SourceRevision != group[2].SourceRevision || !strings.EqualFold(group[1].ExecutableSHA256, group[2].ExecutableSHA256) {
			return nil, errors.New("study requires identical repaired pipeline/assets with Bayes off/on")
		}
	}
	return reports, nil
}

func studyReadDayPairs(in studyInput, reports [3]*studyReport) (map[int64]*studyPair, error) {
	pairs := make(map[int64]*studyPair)
	for arm, report := range reports {
		if err := studyCheckFile(report.Decisions); err != nil {
			return nil, err
		}
		file, err := os.Open(report.Decisions.Path)
		if err != nil {
			return nil, err
		}
		err = studyReadDecisionStream(file, in, arm, report.TerminalRows, pairs)
		if err = errors.Join(err, file.Close()); err != nil {
			return nil, err
		}
	}
	for _, pair := range pairs {
		var first *studyDecision
		for _, decision := range pair.Arms {
			if decision == nil {
				continue // Full union remains visible; never filter to intersection.
			}
			if first != nil && (decision.RawSHA256 != first.RawSHA256 || decision.EpisodeID != first.EpisodeID ||
				decision.R1 != first.R1 || decision.R2Frozen != first.R2Frozen) {
				return nil, errors.New("paired raw row/SNR, frozen episode or reference changed between arms")
			}
			first = decision
		}
	}
	return pairs, nil
}

func studyReadDecisionStream(reader io.Reader, in studyInput, arm, expected int, pairs map[int64]*studyPair) error {
	scanner := bufio.NewScanner(io.LimitReader(reader, int64(in.MaxRows)*16384+1))
	scanner.Buffer(make([]byte, 4096), 16384)
	count := 0
	for scanner.Scan() {
		var decision studyDecision
		if err := studyDecodeJSON(scanner.Bytes(), &decision); err != nil {
			return err
		}
		if decision.Ordinal < 1 || decision.Ordinal > int64(in.MaxRows) || !studyDecisionBinding(decision, in.Archive.SHA256) ||
			decision.EpisodeID == "" || !studyCohortA(decision.ReceiverFamily) || decision.DecisionAtUTC.Before(decision.Raw.Time) {
			return errors.New("invalid study decision provenance, receiver cohort or decision clock")
		}
		pair := pairs[decision.Ordinal]
		if pair == nil {
			if len(pairs) >= in.MaxRows {
				return errors.New("study paired union exceeds frozen day row bound")
			}
			pair = &studyPair{}
			pairs[decision.Ordinal] = pair
		}
		if pair.Arms[arm] != nil {
			return errors.New("study decision stream duplicates an original raw ID")
		}
		pair.Arms[arm] = &decision
		count++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if count != expected {
		return errors.New("study terminal artifact is truncated or contains extra decisions")
	}
	return nil
}

func studyDeliveredCorrection(d *studyDecision) bool {
	return d != nil && d.Status == "output" && d.IdentityCorrection && d.FinalIdentity != "" && d.FinalIdentity != d.SubjectIdentity
}

func studyPairedClass(before, after *studyDecision) string {
	if before == nil || after == nil {
		return "eligibility"
	}
	if before.Status == after.Status && before.FinalIdentity == after.FinalIdentity {
		if before.FinalObserved != after.FinalObserved {
			return "display_only"
		}
		if before.Held != after.Held || before.StabilizerProxyDelay != after.StabilizerProxyDelay || !before.DecisionAtUTC.Equal(after.DecisionAtUTC) {
			return "hold"
		}
		if before.GateEvaluated != after.GateEvaluated || before.GateAllow != after.GateAllow {
			return "eligibility"
		}
		return "unchanged"
	}
	if after.Status != "output" {
		return "suppress"
	}
	if !studyDeliveredCorrection(after) {
		return "retention"
	}
	if after.R1.Strong && after.R1.Identity != after.FinalIdentity || after.R2.Strong && after.R2.Identity != after.FinalIdentity {
		return "wrong_rename"
	}
	if after.R1.Strong && after.R2.Strong {
		return "helpful"
	}
	return "unknown"
}

func studyAccountDay(in studyInput, pairs map[int64]*studyPair, candidate studyCandidate, result *studyComparison,
	encoder *json.Encoder, maxExamples int) (map[string]studyCellDate, error) {
	active := make(map[string]bool)
	episodes := make(map[string]map[string]*studyEpisodeCell)
	for _, cell := range studySortedCells(candidate.ModeDistances) {
		active[cell] = true
		episodes[cell] = make(map[string]*studyEpisodeCell)
	}
	ordinals := make([]int64, 0, len(pairs))
	for ordinal := range pairs {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	crossCells := make(map[string]map[string]bool)
	spillovers := make(map[string]map[string]*studyEpisodeCell)
	offIdentities := studyOffEpisodeCorrections(pairs)
	for _, ordinal := range ordinals {
		pair := pairs[ordinal]
		result.RawUnion++
		if pair.Arms[0] == nil || pair.Arms[1] == nil || pair.Arms[2] == nil {
			result.UnpairedRawIDs++
		}
		repairClass := studyPairedClass(pair.Arms[0], pair.Arms[1])
		bayesClass := studyPairedClass(pair.Arms[1], pair.Arms[2])
		result.RepairOutcomes[repairClass]++
		result.BayesOutcomes[bayesClass]++
		if repairClass != "unchanged" || bayesClass != "unchanged" {
			if err := encoder.Encode(struct {
				DateUTC       string `json:"date_utc"`
				Repair, Bayes string
				Arms          [3]*studyDecision `json:"arms"`
			}{in.DateUTC, repairClass, bayesClass, pair.Arms}); err != nil {
				return nil, err
			}
		}
		studyAddMissed(in.DateUTC, pair, result, maxExamples)
		off, on := pair.Arms[1], pair.Arms[2]
		if studyDeliveredCorrection(on) && (!studyDeliveredCorrection(off) || off.FinalIdentity != on.FinalIdentity) {
			cell := fmt.Sprintf("%s/%d", on.Raw.Mode, on.ConfiguredDistance)
			if !active[cell] {
				if spillovers[cell] == nil {
					spillovers[cell] = make(map[string]*studyEpisodeCell)
				}
				ep := spillovers[cell][on.EpisodeID]
				if ep == nil {
					ep = &studyEpisodeCell{CoveredR1: true, CoveredR2: true, Joint: true}
					spillovers[cell][on.EpisodeID] = ep
				}
				studyAccountEpisode(ep, off, on, cell, offIdentities)
			}
		}
		cells := make(map[string]bool, 2)
		for _, d := range []*studyDecision{off, on} {
			if studyDeliveredCorrection(d) {
				cell := fmt.Sprintf("%s/%d", d.Raw.Mode, d.ConfiguredDistance)
				if active[cell] {
					cells[cell] = true
				}
			}
		}
		for cell := range cells {
			anchor := on
			if anchor == nil {
				anchor = off
			}
			ep := episodes[cell][anchor.EpisodeID]
			if ep == nil {
				ep = &studyEpisodeCell{CoveredR1: true, CoveredR2: true, Joint: true}
				episodes[cell][anchor.EpisodeID] = ep
			}
			studyAccountEpisode(ep, off, on, cell, offIdentities)
			if bayesClass != "unchanged" && bayesClass != "hold" {
				if crossCells[anchor.EpisodeID] == nil {
					crossCells[anchor.EpisodeID] = make(map[string]bool, 2)
				}
				crossCells[anchor.EpisodeID][cell] = true
			}
		}
	}
	for _, cells := range crossCells {
		if len(cells) > 1 {
			result.CrossCellEpisodes++
		}
	}
	for cell, counts := range studySummarizeDate(in.DateUTC, spillovers) {
		result.Spillover = append(result.Spillover, studySpillover{Cell: cell, Counts: counts})
	}
	sort.Slice(result.Spillover, func(i, j int) bool {
		if result.Spillover[i].Counts.DateUTC != result.Spillover[j].Counts.DateUTC {
			return result.Spillover[i].Counts.DateUTC < result.Spillover[j].Counts.DateUTC
		}
		return result.Spillover[i].Cell < result.Spillover[j].Cell
	})
	return studySummarizeDate(in.DateUTC, episodes), nil
}

func studyAddMissed(date string, pair *studyPair, result *studyComparison, maxExamples int) {
	d := pair.Arms[2]
	if d == nil || !d.R1.Strong || !d.R2.Strong || d.R1.Identity != d.R2.Identity || d.R1.Identity == d.SubjectIdentity {
		return
	}
	for _, arm := range pair.Arms {
		if studyDeliveredCorrection(arm) && arm.FinalIdentity == d.R1.Identity {
			return
		}
	}
	missed := studyMissed{RawID: d.RawID, EpisodeID: d.EpisodeID, DateUTC: date, Mode: d.Raw.Mode,
		Subject: d.Raw.DXCall, Reference: d.R1.Identity, Reason: d.Reason,
		PlainDistance:      spot.CallDistance(d.SubjectIdentity, d.R1.Identity, d.Raw.Mode, "plain", "plain"),
		ConfiguredDistance: d.ConfiguredDistance, Support: min(d.R1.Families, d.R2.Families), FocalSNR: d.Raw.ReportDB}
	// A missed correction has final distance zero. Recover its configured
	// reference distance from the same mode model used by that candidate's arm.
	// The vector records the actual selected model on every decision below.
	missed.ConfiguredDistance = spot.CallDistance(d.SubjectIdentity, d.R1.Identity, d.Raw.Mode, d.DistanceModelCW, d.DistanceModelRTTY)
	for i, existing := range result.Missed {
		if existing.EpisodeID == missed.EpisodeID && existing.Mode == missed.Mode && existing.ConfiguredDistance == missed.ConfiguredDistance {
			if existing.Support >= missed.Support {
				return
			}
			result.Missed = append(result.Missed[:i], result.Missed[i+1:]...)
			break
		}
	}
	result.Missed = append(result.Missed, missed)
	sort.Slice(result.Missed, func(i, j int) bool {
		if result.Missed[i].Support != result.Missed[j].Support {
			return result.Missed[i].Support > result.Missed[j].Support
		}
		return result.Missed[i].RawID < result.Missed[j].RawID
	})
	if len(result.Missed) > maxExamples {
		result.Missed = result.Missed[:maxExamples]
	}
}
