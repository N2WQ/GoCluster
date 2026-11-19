// File role: Admit a finite experimental evaluation slice before reading archives.
// V1 tuning owns one geometry/ordinal pair on four dates; holdout owns the frozen
// selection. V2 owns C/B on November 7/14 plus all four disabled parity streams.
// Neither format selects a winner or grants live activation authority.
package main

import (
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"time"
)

type studyMethodEvalBinding struct {
	Complete studyFile `json:"complete"`
	Manifest studyFile `json:"manifest"`
}
type studyMethodEvalVariant struct {
	CandidateID string    `json:"candidate_id"`
	Complete    studyFile `json:"complete"`
	Manifest    studyFile `json:"manifest"`
}
type studyMethodEvalGroup struct {
	DateUTC  string                   `json:"date_utc"`
	Control  studyMethodEvalBinding   `json:"control"`
	Variants []studyMethodEvalVariant `json:"variants"`
}
type studyMethodEvalParity struct {
	DateUTC           string    `json:"date_utc"`
	OriginalDecisions studyFile `json:"original_decisions"`
	ControlDecisions  studyFile `json:"control_decisions"`
	Rows              int       `json:"rows"`
	ControlComplete   studyFile `json:"control_complete"`
	ControlManifest   studyFile `json:"control_manifest"`
}
type studyMethodEvaluationPlan struct {
	SchemaVersion     int                     `json:"schema_version"`
	Experimental      bool                    `json:"experimental"`
	Phase             string                  `json:"phase"`
	CapturePlan       studyFile               `json:"capture_plan"`
	MethodPlan        studyFile               `json:"method_plan"`
	ControlPlan       studyFile               `json:"control_plan"`
	ParityCapturePlan studyFile               `json:"parity_capture_plan"`
	ParityControlPlan studyFile               `json:"parity_control_plan"`
	Parity            []studyMethodEvalParity `json:"parity"`
	Groups            []studyMethodEvalGroup  `json:"groups"`
}
type studyMethodEvalPrepared struct {
	Plan            studyMethodEvaluationPlan
	File            studyFile
	Capture         studyPlan
	Method, Control studyMethodPlan
	Arm             studyArm
	Candidate       studyCandidate
	Variants        []studyMethodCandidate
	ParityGroups    []studyMethodEvalLoadedGroup
}

func prepareStudyMethodEvaluation(path, frozenSHA string) (*studyMethodEvalPrepared, error) {
	return prepareStudyMethodEvaluationWithAuthority(path, frozenSHA, studyMethodEvalOriginalAuthority())
}

type studyMethodEvalAuthority struct {
	DateUTC string
	Rows    int
	SHA256  string
}

// These are the independently verified E1 original repaired-OFF full streams,
// not caller-selected "originals". Only private synthetic tests supply a separate
// authority table; neither JSON nor CLI has an override.
func studyMethodEvalOriginalAuthority() [4]studyMethodEvalAuthority {
	return [4]studyMethodEvalAuthority{
		{"2026-11-07", 458495, "2a78ec300acc2cfe29048201af4e971cad933db01c6f36b567ac7443b57ccd34"},
		{"2026-11-14", 882021, "ad0c8f5a942bab03da052cbe91c2087e59ff566cbd48aa2be8be43b1140bff49"},
		{"2026-11-15", 793630, "537ef50ac1893afb9f2b440316f4cab9c0863da0071a81451c94cebdf54519ce"},
		{"2026-11-16", 230784, "923e9556f375c126e364c9b84ee3eae8bb2985cc95df944262362779d3d5ef22"},
	}
}

func prepareStudyMethodEvaluationWithAuthority(path, frozenSHA string, authority [4]studyMethodEvalAuthority) (*studyMethodEvalPrepared, error) {
	file := studyFile{Path: path, SHA256: frozenSHA}
	var plan studyMethodEvaluationPlan
	if err := studyMethodEvalReadJSON(file, &plan); err != nil {
		return nil, err
	}
	if err := validateStudyMethodEvaluationPlan(plan); err != nil {
		return nil, err
	}
	for parityIndex := range plan.Parity {
		parity := &plan.Parity[parityIndex]
		found := false
		for _, original := range authority {
			if parity.DateUTC == original.DateUTC && parity.Rows == original.Rows && parity.OriginalDecisions.SHA256 == original.SHA256 {
				found = true
			}
		}
		if !found {
			return nil, errors.New("disabled parity does not name the authoritative original full stream/count")
		}
	}
	capture, _, err := loadStudyPlan(plan.CapturePlan.Path, plan.CapturePlan.SHA256)
	if err != nil {
		return nil, err
	}
	method, _, err := loadStudyMethodPlan(plan.MethodPlan.Path, plan.MethodPlan.SHA256)
	if err != nil {
		return nil, err
	}
	control, _, err := loadStudyMethodPlan(plan.ControlPlan.Path, plan.ControlPlan.SHA256)
	if err != nil {
		return nil, err
	}
	if !method.Enabled || control.Enabled {
		return nil, errors.New("evaluation requires enabled method and explicit disabled control")
	}
	if method.SchemaVersion != plan.SchemaVersion || control.SchemaVersion != plan.SchemaVersion {
		return nil, errors.New("evaluation and method schema versions differ")
	}
	a, b := method, control
	a.Enabled, b.Enabled, a.SelectedCandidate, b.SelectedCandidate = false, false, "", ""
	if !reflect.DeepEqual(a, b) {
		return nil, errors.New("control/method grid, source, reference or model differs")
	}
	var arm studyArm
	for _, entry := range capture.Arms {
		if entry.Name == "repaired_off" {
			arm = entry
		}
	}
	if arm.SourceRevision != method.SourceRevision || capture.ReferenceVersion != method.ReferenceVersion {
		return nil, errors.New("evaluation source/reference differs from capture arm")
	}
	if _, err := studyLoadOrdinalModel(method.Model); err != nil {
		return nil, err
	}
	candidate, err := studySelectCandidate(capture, studyCLIRequest{Phase: plan.Phase})
	if err != nil {
		return nil, err
	}
	prepared := &studyMethodEvalPrepared{Plan: plan, File: file, Capture: capture, Method: method, Control: control, Arm: arm, Candidate: candidate}
	for _, variant := range plan.Groups[0].Variants {
		entry, ok := prepared.methodCandidate(variant.CandidateID)
		if !ok {
			return nil, errors.New("evaluation candidate is outside the frozen grid")
		}
		prepared.Variants = append(prepared.Variants, entry)
	}
	sort.Slice(prepared.Variants, func(i, j int) bool {
		if plan.SchemaVersion == 2 {
			return prepared.Variants[i].Ranking == "selective" && prepared.Variants[j].Ranking != "selective"
		}
		return prepared.Variants[i].Ranking < prepared.Variants[j].Ranking
	})
	if plan.Phase == "tuning" {
		a, b := prepared.Variants[0], prepared.Variants[1]
		left, right := "geometry", "ordinal"
		if plan.SchemaVersion == 2 {
			left, right = "selective", "receiver_compatibility"
		}
		if a.Ranking != left || b.Ranking != right || a.GapHz != b.GapHz || a.MaxWitnessSeconds != b.MaxWitnessSeconds || !reflect.DeepEqual(a.CWDistances, b.CWDistances) {
			return nil, errors.New("evaluation requires one otherwise identical geometry/ordinal pair")
		}
	} else if method.SelectedCandidate == "" || control.SelectedCandidate != method.SelectedCandidate || prepared.Variants[0].ID != method.SelectedCandidate {
		return nil, errors.New("holdout requires the single frozen method/control selection")
	}
	for _, group := range plan.Groups {
		if _, err := prepared.input(group.DateUTC); err != nil {
			return nil, err
		}
		for _, selected := range prepared.Variants {
			found := false
			for _, variant := range group.Variants {
				found = found || variant.CandidateID == selected.ID
			}
			if !found {
				return nil, errors.New("evaluation changes the complete candidate across dates")
			}
		}
	}
	declared := 0
	for _, input := range capture.Inputs {
		if input.Phase == plan.Phase && (plan.SchemaVersion == 1 || input.DateUTC == "2026-11-07" || input.DateUTC == "2026-11-14") {
			declared++
		}
	}
	if declared != len(plan.Groups) {
		return nil, errors.New("evaluation omits a capture-plan date in its selected phase")
	}
	for parityIndex := range plan.Parity {
		parity := &plan.Parity[parityIndex]
		if err := errors.Join(studyCheckFile(parity.OriginalDecisions), studyCheckFile(parity.ControlDecisions)); err != nil {
			return nil, err
		}
	}
	if err := prepared.prepareParity(); err != nil {
		return nil, err
	}
	return prepared, nil
}

func validateStudyMethodEvaluationPlan(p studyMethodEvaluationPlan) error {
	if (p.SchemaVersion != 1 && p.SchemaVersion != 2) || !p.Experimental || (p.Phase != "tuning" && p.Phase != "holdout") {
		return errors.New("evaluation requires experimental schema 1/2 and tuning/holdout phase")
	}
	if p.SchemaVersion == 2 && p.Phase != "tuning" {
		return errors.New("evaluation v2 is development-only and cannot claim confirmation")
	}
	for _, file := range []studyFile{p.CapturePlan, p.MethodPlan, p.ControlPlan, p.ParityCapturePlan, p.ParityControlPlan} {
		if err := studyAuditFile(file); err != nil {
			return err
		}
	}
	exposed := map[string]bool{"2026-11-07": false, "2026-11-14": false, "2026-11-15": false, "2026-11-16": false}
	if len(p.Parity) != 4 {
		return errors.New("evaluation requires all four exposed disabled-parity bindings")
	}
	for parityIndex := range p.Parity {
		parity := &p.Parity[parityIndex]
		seen, ok := exposed[parity.DateUTC]
		if !ok || seen || parity.Rows < 1 || parity.Rows > studyMaxRows || parity.OriginalDecisions.SHA256 != parity.ControlDecisions.SHA256 {
			return errors.New("invalid, duplicate or unequal disabled parity")
		}
		for _, file := range []studyFile{parity.OriginalDecisions, parity.ControlDecisions} {
			if err := studyAuditFile(file); err != nil {
				return err
			}
		}
		if err := studyMethodEvalCheckBinding(studyMethodEvalBinding{parity.ControlComplete, parity.ControlManifest}); err != nil {
			return err
		}
		exposed[parity.DateUTC] = true
	}
	wantGroups, wantVariants := 4, 2
	if p.SchemaVersion == 2 {
		wantGroups = 2
	}
	if p.Phase == "holdout" {
		wantGroups, wantVariants = 2, 1
	}
	if len(p.Groups) != wantGroups {
		return errors.New("evaluation has incomplete date coverage")
	}
	dates := make(map[string]bool, wantGroups)
	for _, group := range p.Groups {
		day, _, err := parseUTCDate(group.DateUTC)
		if err != nil || day.Format("2006-01-02") != group.DateUTC || dates[group.DateUTC] || len(group.Variants) != wantVariants {
			return errors.New("invalid evaluation date or variant coverage")
		}
		if p.Phase == "tuning" {
			if !exposed[group.DateUTC] {
				return errors.New("tuning must use exactly the four exposed dates")
			}
			if p.SchemaVersion == 2 && group.DateUTC != "2026-11-07" && group.DateUTC != "2026-11-14" {
				return errors.New("evaluation v2 permits development on November 7/14 only")
			}
		} else if !day.After(time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)) {
			return errors.New("confirmation cannot reuse exposed dates")
		}
		dates[group.DateUTC] = true
		if err := studyMethodEvalCheckBinding(group.Control); err != nil {
			return err
		}
		ids := make(map[string]bool, wantVariants)
		for _, variant := range group.Variants {
			if variant.CandidateID == "" || len(variant.CandidateID) > 64 || ids[variant.CandidateID] {
				return errors.New("duplicate or invalid method candidate")
			}
			ids[variant.CandidateID] = true
			if err := studyMethodEvalCheckBinding(studyMethodEvalBinding{variant.Complete, variant.Manifest}); err != nil {
				return err
			}
		}
	}
	return nil
}

func studyMethodEvalCheckBinding(binding studyMethodEvalBinding) error {
	if err := errors.Join(studyAuditFile(binding.Complete), studyAuditFile(binding.Manifest)); err != nil {
		return err
	}
	if !studyAuditSamePath(binding.Manifest.Path, filepath.Join(filepath.Dir(binding.Complete.Path), "manifest.json")) {
		return errors.New("evaluation manifest must belong to its completion")
	}
	return nil
}
func (p *studyMethodEvalPrepared) methodCandidate(id string) (studyMethodCandidate, bool) {
	for _, c := range p.Method.Candidates {
		if c.ID == id {
			return c, true
		}
	}
	return studyMethodCandidate{}, false
}
func (p *studyMethodEvalPrepared) input(date string) (studyInput, error) {
	for _, in := range p.Capture.Inputs {
		if in.DateUTC == date && in.Phase == p.Plan.Phase {
			return in, nil
		}
	}
	return studyInput{}, errors.New("evaluation date/phase is outside capture plan")
}

func (p *studyMethodEvalPrepared) prepareParity() error {
	capture, _, err := loadStudyPlan(p.Plan.ParityCapturePlan.Path, p.Plan.ParityCapturePlan.SHA256)
	if err != nil {
		return err
	}
	control, _, err := loadStudyMethodPlan(p.Plan.ParityControlPlan.Path, p.Plan.ParityControlPlan.SHA256)
	if err != nil {
		return err
	}
	if control.Enabled {
		return errors.New("parity control plan must explicitly disable the method")
	}
	a, b := p.Control, control
	a.Enabled, b.Enabled, a.SelectedCandidate, b.SelectedCandidate = false, false, "", ""
	if !reflect.DeepEqual(a, b) {
		return errors.New("parity and current method/control contracts differ")
	}
	var arm studyArm
	for _, entry := range capture.Arms {
		if entry.Name == "repaired_off" {
			arm = entry
		}
	}
	if arm.SourceRevision != p.Arm.SourceRevision || arm.ExecutableSHA256 != p.Arm.ExecutableSHA256 || !reflect.DeepEqual(arm.ConfigFiles, p.Arm.ConfigFiles) || studyAssetDigest(arm.Assets) != studyAssetDigest(p.Arm.Assets) || capture.InputScenario != p.Capture.InputScenario || capture.ReferenceVersion != p.Capture.ReferenceVersion || capture.ReferenceContractSHA256 != p.Capture.ReferenceContractSHA256 || !studyMethodEvalSameFile(capture.ReceiverMetadata, p.Capture.ReceiverMetadata) || capture.WarmupSeconds != p.Capture.WarmupSeconds || capture.DecisionCutoffSeconds != p.Capture.DecisionCutoffSeconds {
		return errors.New("parity capture source/executable/config/assets/scenario differs from current capture")
	}
	candidate, err := studySelectCandidate(capture, studyCLIRequest{Phase: "tuning"})
	if err != nil {
		return err
	}
	if hashAnyStable(candidate) != hashAnyStable(p.Candidate) {
		return errors.New("parity ordinary capture configuration differs")
	}
	reader := *p
	reader.Capture, reader.Control, reader.Arm, reader.Candidate = capture, control, arm, candidate
	reader.Plan.Phase = "tuning"
	reader.Plan.CapturePlan = p.Plan.ParityCapturePlan
	reader.Plan.ControlPlan = p.Plan.ParityControlPlan
	for parityIndex := range p.Plan.Parity {
		parity := &p.Plan.Parity[parityIndex]
		input, err := reader.input(parity.DateUTC)
		if err != nil {
			return err
		}
		found := false
		for _, current := range p.Capture.Inputs {
			if current.DateUTC == parity.DateUTC && current.Phase == "tuning" && current.Archive.SHA256 == input.Archive.SHA256 && current.MaxRows == input.MaxRows && current.MaxCSVBytes == input.MaxCSVBytes {
				found = true
			}
		}
		if !found {
			return errors.New("current capture does not preserve the four original exposed archive bindings")
		}
		binding := studyMethodEvalBinding{parity.ControlComplete, parity.ControlManifest}
		report, manifest, err := reader.readReport(binding, input, control, false, "")
		if err != nil {
			return err
		}
		if report.TerminalRows != parity.Rows || !studyMethodEvalSameFile(report.Decisions, parity.ControlDecisions) {
			return errors.New("parity completion does not bind the authoritative disabled stream/count")
		}
		if err := studyMethodEvalParityArchiveBinding(report.Decisions, input.Archive.SHA256); err != nil {
			return err
		}
		if len(p.ParityGroups) > 0 {
			first := p.ParityGroups[0].Reports[0]
			if report.CallCorrectionSHA256 != first.CallCorrectionSHA256 || report.PipelineWithoutBayesSHA256 != first.PipelineWithoutBayesSHA256 {
				return errors.New("disabled parity pipeline fingerprints differ across dates")
			}
		}
		p.ParityGroups = append(p.ParityGroups, studyMethodEvalLoadedGroup{Input: input, Reports: [3]*studyReport{report}, Manifests: [3]replayManifest{manifest}, Bindings: []studyMethodEvalBinding{binding}})
	}
	return nil
}
