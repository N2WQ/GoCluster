// File role: Compare the two frozen CW fallback captures. Admission proves
// that the only effective change is the bounded private reliability asset.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"

	"dxcluster/config"
	"dxcluster/spot"
)

type studyWeightCapture struct {
	Plan    studyFile                 `json:"plan"`
	Reports [2]studyMethodEvalBinding `json:"reports"`
}

type studyWeightEvaluationPlan struct {
	Method   studyFile             `json:"disabled_method"`
	Captures [2]studyWeightCapture `json:"control_then_candidate"`
}

var studyWeightDates = [...]string{"2026-11-07", "2026-11-14"}
var studyWeightArchiveHashes = [...]string{
	"6e93a5249a41f026dd19de15a013045f6e73e9ff3f6e67f2f0ccfcdd621451d1",
	"a921a0d7d96bfc7d4020f9b461eeb35cb36d935755744ea53ea3a5c1724eb041",
}

type studyWeightDay struct {
	Date          string                    `json:"date_utc"`
	Counts        studyMethodEvalCounts     `json:"counts"`
	CW            studyMethodEvalCounts     `json:"cw_counts"`
	Paired        studyPairedStability      `json:"paired_follow_on"`
	CWPaired      studyPairedStability      `json:"cw_paired_follow_on"`
	Confidence    [2]replayConfidenceCounts `json:"control_then_candidate_output_confidence"`
	NewlySplit    int                       `json:"newly_split"`
	NoLongerSplit int                       `json:"no_longer_split"`
}

var studyWeightStability = replayStabilityConfig{WindowMinutes: 15, MinFollowOn: 3, FreqToleranceHz: 500}

func runStudyWeightEvaluation(file studyFile, out string) (err error) {
	var plan studyWeightEvaluationPlan
	if err := studyMethodEvalReadJSON(file, &plan); err != nil {
		return err
	}
	method, _, err := loadStudyMethodPlan(plan.Method.Path, plan.Method.SHA256)
	if err != nil {
		return err
	}
	if method.Enabled {
		return errors.New("weight comparison requires the disabled method")
	}
	var captures [2]studyMethodEvalPrepared
	var settings [2]*config.Config
	for i := range plan.Captures {
		captures[i], settings[i], err = studyLoadWeightCapture(plan.Captures[i].Plan, plan.Method, method)
		if err != nil {
			return err
		}
	}
	if err := studyCompareWeightCapture(captures, settings); err != nil {
		return err
	}
	original, patched, err := studyLoadWeightTables(settings)
	if err != nil {
		return err
	}
	aliases, err := loadStudyReceiverAliases(captures[0].Capture.ReceiverMetadata)
	if err != nil {
		return err
	}
	if err := studyAuditNewDirectory(out); err != nil {
		return err
	}
	owner, err := newStudyDiagnosticArtifacts(out)
	if err != nil {
		return err
	}
	changes, err := newStudyMethodEvalWriter(owner, "changed_decisions.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, changes.close()) }()
	var results [2]studyWeightDay
	for i, date := range studyWeightDates {
		results[i], err = studyEvaluateWeightDay(plan, captures, i, date, aliases, original, patched, changes)
		if err != nil {
			return err
		}
	}
	if len(patched) != 0 {
		return errors.New("CW asset contains fallback rows absent from eligible archive inputs")
	}
	if err := errors.Join(studyCheckFile(file), studyCheckFile(plan.Method), studyCheckFile(method.Model),
		studyCheckFile(captures[0].Capture.ReceiverMetadata)); err != nil {
		return err
	}
	for i := range captures {
		arm := captures[i].Arm
		if err := errors.Join(studyCheckFile(plan.Captures[i].Plan), studyCheckConfigFiles(arm), studyCheckAssets(arm, settings[i])); err != nil {
			return err
		}
	}
	if err := changes.close(); err != nil {
		return err
	}
	changeFile, err := changes.binding()
	if err != nil {
		return err
	}
	result := struct {
		Plan    studyFile         `json:"plan"`
		Changes studyFile         `json:"changed_decisions"`
		Days    [2]studyWeightDay `json:"days"`
	}{file, changeFile, results}
	if err := owner.atomicJSON("comparison.json", result); err != nil {
		return err
	}
	name := filepath.Join(owner.root, "comparison.json")
	sum, err := studyHashFile(name)
	if err != nil {
		return err
	}
	return owner.atomicJSON("COMPLETE.json", studyFile{Path: name, SHA256: sum})
}

func studyLoadWeightCapture(file, methodFile studyFile, method studyMethodPlan) (studyMethodEvalPrepared, *config.Config, error) {
	capture, _, err := loadStudyPlan(file.Path, file.SHA256)
	if err != nil {
		return studyMethodEvalPrepared{}, nil, err
	}
	if len(capture.Inputs) != len(studyWeightDates) {
		return studyMethodEvalPrepared{}, nil, errors.New("weight capture requires exactly the two exposed dates")
	}
	for i, input := range capture.Inputs {
		if input.DateUTC != studyWeightDates[i] || input.Phase != "tuning" || input.Archive.SHA256 != studyWeightArchiveHashes[i] {
			return studyMethodEvalPrepared{}, nil, errors.New("weight capture date or phase is outside the experiment")
		}
	}
	var arm studyArm
	for _, entry := range capture.Arms {
		if entry.Name == "repaired_off" {
			arm = entry
		}
	}
	p := studyMethodEvalPrepared{Capture: capture, Arm: arm, Method: method, Control: method,
		Plan: studyMethodEvaluationPlan{Phase: "tuning", CapturePlan: file, ControlPlan: methodFile}}
	p.Candidate, err = studySelectCandidate(capture, studyCLIRequest{Phase: "tuning"})
	if err != nil {
		return p, nil, err
	}
	if arm.SourceRevision != method.SourceRevision {
		return p, nil, errors.New("weight capture and method source revisions differ")
	}
	if err := studyCheckConfigFiles(arm); err != nil {
		return p, nil, err
	}
	settings, err := config.Load(arm.ConfigDir)
	if err != nil {
		return p, nil, err
	}
	if err := studyApplyCandidate(settings, arm, p.Candidate); err != nil {
		return p, nil, err
	}
	if _, err := studyValidateRuntimeConfig(settings, arm, p.Candidate, capture); err != nil {
		return p, nil, err
	}
	return p, settings, studyCheckAssets(arm, settings)
}

func studyCompareWeightCapture(captures [2]studyMethodEvalPrepared, settings [2]*config.Config) error {
	left, right := captures[0].Capture, captures[1].Capture
	left.Arms, right.Arms = nil, nil
	a, b := captures[0].Arm, captures[1].Arm
	if !reflect.DeepEqual(left, right) || a.ExecutableSHA256 != b.ExecutableSHA256 || a.SourceRevision != b.SourceRevision {
		return errors.New("weight captures differ in executable, source, or frozen replay inputs")
	}
	ac, bc := settings[0].CallCorrection, settings[1].CallCorrection
	if ac.MinSpotterReliability != .95 || bc.MinSpotterReliability != .95 || studyAuditSamePath(ac.SpotterReliabilityFileCW, bc.SpotterReliabilityFileCW) {
		return errors.New("weight captures require floor 0.950 and distinct CW table paths")
	}
	candidate := *settings[1]
	candidate.LoadedFrom = settings[0].LoadedFrom
	candidate.CallCorrection.SpotterReliabilityFileCW = ac.SpotterReliabilityFileCW
	if !reflect.DeepEqual(*settings[0], candidate) {
		return errors.New("effective weight capture settings differ beyond the CW table path")
	}
	if !studyEqualWeightInputs(a, b, ac.SpotterReliabilityFileCW, bc.SpotterReliabilityFileCW) {
		return errors.New("weight capture changes another frozen asset or config file")
	}
	first, err := studyReadBounded(filepath.Join(a.ConfigDir, "pipeline.yaml"), studyMaxJSONBytes)
	if err != nil {
		return err
	}
	second, err := studyReadBounded(filepath.Join(b.ConfigDir, "pipeline.yaml"), studyMaxJSONBytes)
	if err != nil {
		return err
	}
	oldPath, newPath := []byte(filepath.ToSlash(ac.SpotterReliabilityFileCW)), []byte(filepath.ToSlash(bc.SpotterReliabilityFileCW))
	if bytes.Count(first, oldPath) != 1 || !bytes.Equal(bytes.Replace(first, oldPath, newPath, 1), second) {
		return errors.New("private pipeline files may differ only at the CW asset path")
	}
	return nil
}

func studyEqualWeightInputs(left, right studyArm, leftCW, rightCW string) bool {
	if len(left.Assets) != len(right.Assets) || len(left.ConfigFiles) != len(right.ConfigFiles) {
		return false
	}
	for name, hash := range left.ConfigFiles {
		if other, ok := right.ConfigFiles[name]; !ok || (name != "pipeline.yaml" && hash != other) {
			return false
		}
	}
	count := 0
	for i, a := range left.Assets {
		b := right.Assets[i]
		if studyAuditSamePath(a.Path, leftCW) && studyAuditSamePath(b.Path, rightCW) {
			count++
			continue
		}
		if !studyMethodEvalSameFile(a, b) {
			return false
		}
	}
	return count == 1
}

func studyLoadWeightTables(settings [2]*config.Config) (spot.SpotterReliability, spot.SpotterReliability, error) {
	regular, cw, _, err := loadSpotterReliability(settings[0].CallCorrection)
	if err != nil {
		return nil, nil, err
	}
	if regular == nil {
		regular = make(spot.SpotterReliability)
	}
	_, patched, _, err := loadSpotterReliability(settings[1].CallCorrection)
	if err != nil {
		return nil, nil, err
	}
	for receiver, expected := range cw {
		if actual, ok := patched[receiver]; !ok || actual != expected {
			return nil, nil, fmt.Errorf("weight asset modified measured CW entry %q", receiver)
		}
		delete(patched, receiver)
		regular[receiver] = expected
	}
	for receiver, weight := range patched {
		if _, known := regular[receiver]; known || weight != .95 {
			return nil, nil, fmt.Errorf("weight asset introduced an invalid fallback entry %q", receiver)
		}
	}
	err = studyCheckWeightTableBytes(settings[0].CallCorrection.SpotterReliabilityFileCW, settings[1].CallCorrection.SpotterReliabilityFileCW, patched)
	return regular, patched, err
}

func studyCheckWeightTableBytes(control, candidate string, added spot.SpotterReliability) error {
	before, err := studyReadBounded(control, studyMaxJSONBytes)
	if err != nil {
		return err
	}
	after, err := studyReadBounded(candidate, studyMaxJSONBytes)
	if err != nil {
		return err
	}
	receivers := make([]string, 0, len(added))
	for receiver := range added {
		receivers = append(receivers, receiver)
	}
	sort.Strings(receivers)
	tail := bytes.NewBuffer(nil)
	if len(receivers) != 0 && len(before) != 0 && before[len(before)-1] != '\n' {
		tail.WriteByte('\n')
	}
	for _, receiver := range receivers {
		fmt.Fprintf(tail, "%s 0.950\n", receiver)
	}
	remaining, ok := bytes.CutPrefix(after, before)
	if !ok || !bytes.Equal(remaining, tail.Bytes()) {
		return errors.New("private CW asset must preserve all original bytes and append exactly the sorted fallback rows")
	}
	return nil
}

func studyEvaluateWeightDay(plan studyWeightEvaluationPlan, captures [2]studyMethodEvalPrepared, index int, date string,
	aliases map[string]string, original, patched spot.SpotterReliability, changes *studyMethodEvalWriter) (studyWeightDay, error) {
	result := studyWeightDay{Date: date, Counts: studyMethodEvalNewCounts(), CW: studyMethodEvalNewCounts()}
	input := captures[0].Capture.Inputs[index]
	var reports [2]*studyReport
	var manifests [2]replayManifest
	for i := range captures {
		capture := &captures[i]
		var err error
		reports[i], manifests[i], err = capture.readReport(plan.Captures[i].Reports[index], input, capture.Control, false, "")
		if err != nil {
			return result, err
		}
		result.Confidence[i] = manifests[i].Results.ABMetrics.Output.ConfidenceCounts
	}
	day, err := loadStudyRawDay(input, aliases, captures[0].Capture.WarmupSeconds)
	if err != nil {
		return result, err
	}
	day.Index = nil
	if err := studyCheckWeightReceivers(day, original, patched); err != nil {
		return result, err
	}
	spots, accepted, bad, skipped, focal := studyAuditRawPopulation(day)
	for i, report := range reports {
		csv := manifests[i].CSV
		if report.RawRows != len(day.Rows) || report.FocalRows != focal || report.TerminalRows != focal ||
			csv.RecordsTotal != int64(len(day.Rows)) || csv.RecordsProcessed != int64(accepted) ||
			csv.RecordsSkippedBad != int64(bad) || csv.RecordsSkippedMode != int64(skipped) {
			return result, errors.New("weight capture archive populations do not reconcile")
		}
	}
	pairs, err := studyReadDayPairsForReports(input, reports[:])
	if err != nil {
		return result, err
	}
	if err := studyAuditArchiveBindingsForArms(day, pairs, len(reports)); err != nil {
		return result, err
	}
	for i := range day.Rows {
		pair := pairs[int64(i+1)]
		if pair == nil {
			continue
		}
		a, b := pair.Arms[0], pair.Arms[1]
		for _, decision := range []*studyDecision{a, b} {
			if err := studyMethodEvalR2Binding(decision); err != nil {
				return result, err
			}
		}
		x, y := studyAuditMakeReceipt(a, false), studyAuditMakeReceipt(b, false)
		x.CurrentStable, y.CurrentStable = studyAuditStable(a, spots, studyWeightStability), studyAuditStable(b, spots, studyWeightStability)
		if err := studyRecordWeightChange(changes, date, a, b, x, y); err != nil {
			return result, err
		}
		if err := studyMethodEvalAccount(&result.Counts, a, b, x, y, nil); err != nil {
			return result, err
		}
		result.Paired.observe(a, b, spots, studyWeightStability)
		if a.Raw.Mode == "CW" {
			if err := studyMethodEvalAccount(&result.CW, a, b, x, y, nil); err != nil {
				return result, err
			}
			result.CWPaired.observe(a, b, spots, studyWeightStability)
		}
		ordinarySplit := studyWeightSplit(a.Reason)
		candidateSplit := studyWeightSplit(b.Reason)
		if !ordinarySplit && candidateSplit {
			result.NewlySplit++
		}
		if ordinarySplit && !candidateSplit {
			result.NoLongerSplit++
		}
	}
	if !reflect.DeepEqual(reports[0].References, result.Counts.ControlReferences) ||
		!reflect.DeepEqual(reports[1].References, result.Counts.VariantReferences) {
		return result, errors.New("weight capture reference accounting does not reconcile")
	}
	if err := studyCheckFile(input.Archive); err != nil {
		return result, err
	}
	for i, report := range reports {
		binding := plan.Captures[i].Reports[index]
		if err := errors.Join(studyCheckFile(binding.Complete), studyCheckFile(binding.Manifest), studyCheckFile(report.Decisions)); err != nil {
			return result, err
		}
	}
	return result, nil
}

func studyRecordWeightChange(changes *studyMethodEvalWriter, date string, a, b *studyDecision, x, y studyAuditReceipt) error {
	if x == y && a.Reason == b.Reason {
		return nil
	}
	return changes.record(studyMethodEvalPairRecord{
		DateUTC: date, RawID: a.RawID, RawSHA256: a.RawSHA256, EpisodeID: a.EpisodeID, Mode: a.Raw.Mode,
		SubjectIdentity: a.SubjectIdentity, R1: a.R1, R2Frozen: a.R2Frozen,
		Outcomes: []studyMethodEvalTerminal{
			{Receipt: x, Reason: a.Reason, Held: a.Held, TemporalAction: a.TemporalAction, AfterCutoff: a.DecisionAfterCutoff, StabilizerDelay: a.StabilizerProxyDelay},
			{Receipt: y, Reason: b.Reason, Held: b.Held, TemporalAction: b.TemporalAction, AfterCutoff: b.DecisionAfterCutoff, StabilizerDelay: b.StabilizerProxyDelay},
		},
	})
}

func studyCheckWeightReceivers(day *studyRawDay, original, patched spot.SpotterReliability) error {
	for i := range day.Rows {
		raw := &day.Rows[i]
		if !raw.Valid || raw.Row.Mode != "CW" {
			continue
		}
		receiver := raw.Row.Spotter
		if _, known := original[receiver]; known {
			continue
		}
		if _, covered := patched[receiver]; !covered {
			return fmt.Errorf("weight asset omitted missing CW reporter %q", receiver)
		}
		delete(patched, receiver)
		original[receiver] = .95
	}
	return nil
}

func studyWeightSplit(reason string) bool {
	switch reason {
	case "resolver_state_split", "license_rejected:resolver_state_split",
		"resolver_neighbor_conflict", "license_rejected:resolver_neighbor_conflict":
		return true
	default:
		return false
	}
}
