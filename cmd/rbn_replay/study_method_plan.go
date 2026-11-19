// File role: Admit the frozen V1 grid or V2 development pair before archive reads.
// Presence is explicit: disabled parity still validates its plan and model. The
// token pass bounds collections before typed allocation and supplies no defaults.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"dxcluster/internal/correctionflow"
)

type studyMethodCandidate struct {
	ID                string `json:"id"`
	GapHz             int    `json:"gap_hz"`
	MaxWitnessSeconds int    `json:"max_witness_seconds"`
	Ranking           string `json:"ranking"`
	CWDistances       []int  `json:"cw_distances"`
}

type studyMethodPlan struct {
	SchemaVersion     int                    `json:"schema_version"`
	Experimental      bool                   `json:"experimental"`
	Enabled           bool                   `json:"enabled"`
	SourceRevision    string                 `json:"source_revision"`
	ReferenceVersion  string                 `json:"reference_version"`
	Model             studyFile              `json:"model"`
	SelectedCandidate string                 `json:"selected_candidate"`
	Candidates        []studyMethodCandidate `json:"candidates"`
}

type studyMethodPrepared struct {
	Plan      studyMethodPlan
	File      studyFile
	Candidate studyMethodCandidate
	Model     *correctionflow.OrdinalModel
}

//nolint:nilnil // Absence alone means ordinary execution; explicit false is a method run.
func prepareStudyMethod(req studyCLIRequest, arm studyArm) (*studyMethodPrepared, error) {
	if req.MethodPlan == "" && req.MethodFreeze == "" && req.MethodCandidate == "" {
		return nil, nil
	}
	if req.MethodPlan == "" || req.MethodFreeze == "" || req.MethodCandidate == "" {
		return nil, errors.New("method plan, freeze and candidate are required together")
	}
	if arm.Name != "repaired_off" || req.Compare != "" || req.DiagnosticSelection != "" || req.DiagnosticFreeze != "" {
		return nil, errors.New("method execution requires repaired_off without comparison or diagnostic capture")
	}
	plan, digest, err := loadStudyMethodPlan(req.MethodPlan, req.MethodFreeze)
	if err != nil {
		return nil, err
	}
	if plan.SourceRevision != arm.SourceRevision {
		return nil, errors.New("method source revision differs from the frozen study arm")
	}
	if req.Phase != "tuning" && req.Phase != "holdout" {
		return nil, errors.New("method phase must be tuning or holdout")
	}
	if plan.SchemaVersion == 2 {
		allowed := req.Date == "2026-11-07" || req.Date == "2026-11-14"
		if !plan.Enabled {
			allowed = allowed || req.Date == "2026-11-15" || req.Date == "2026-11-16"
		}
		if req.Phase != "tuning" || !allowed {
			return nil, errors.New("method v2 permits development on November 7/14 and disabled parity on the four exposed dates only")
		}
	}
	if req.Phase == "holdout" && (plan.SelectedCandidate == "" || req.MethodCandidate != plan.SelectedCandidate) {
		return nil, errors.New("method holdout requires the single frozen selected candidate")
	}
	var candidate studyMethodCandidate
	for _, entry := range plan.Candidates {
		if entry.ID == req.MethodCandidate {
			candidate = entry
		}
	}
	if candidate.ID == "" {
		return nil, errors.New("method candidate is not in the complete frozen grid")
	}
	model, err := studyLoadOrdinalModel(plan.Model)
	if err != nil {
		return nil, err
	}
	path, err := filepath.Abs(req.MethodPlan)
	if err != nil {
		return nil, err
	}
	return &studyMethodPrepared{Plan: plan, File: studyFile{Path: path, SHA256: digest}, Candidate: candidate, Model: model}, nil
}

func loadStudyMethodPlan(path, frozenSHA string) (studyMethodPlan, string, error) {
	var plan studyMethodPlan
	if !studyValidSHA(frozenSHA) || frozenSHA != strings.ToLower(frozenSHA) {
		return plan, "", errors.New("method freeze must be a lowercase SHA256")
	}
	data, err := studyReadBounded(path, studyMaxJSONBytes)
	if err != nil {
		return plan, "", err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if digest != frozenSHA {
		return plan, "", errors.New("method plan differs from its frozen SHA256")
	}
	if err := studyMethodPlanPreflight(data); err != nil {
		return plan, "", err
	}
	if err := json.Unmarshal(data, &plan); err != nil {
		return plan, "", err
	}
	return plan, digest, validateStudyMethodPlan(plan)
}

func validateStudyMethodPlan(plan studyMethodPlan) error {
	revision, err := hex.DecodeString(plan.SourceRevision)
	if (plan.SchemaVersion != 1 && plan.SchemaVersion != 2) || !plan.Experimental || err != nil || len(revision) != 20 || plan.SourceRevision != strings.ToLower(plan.SourceRevision) || plan.ReferenceVersion != studyReferenceVersion {
		return errors.New("method plan requires experimental schema 1/2 and exact source/reference versions")
	}
	if !studyValidFile(plan.Model) || plan.Model.SHA256 != strings.ToLower(plan.Model.SHA256) {
		return errors.New("method plan requires a frozen model")
	}
	if err := studyAuditLocalPath(plan.Model.Path); err != nil {
		return err
	}
	if plan.SchemaVersion == 2 {
		return validateStudyMethodPlanV2(plan)
	}
	if len(plan.Candidates) != 16 {
		return errors.New("method plan requires all 16 complete candidates")
	}
	var seen [16]bool
	selected := plan.SelectedCandidate == ""
	for _, candidate := range plan.Candidates {
		index, err := studyMethodCandidateIndex(candidate)
		if err != nil {
			return err
		}
		if seen[index] {
			return errors.New("method plan repeats a grid candidate")
		}
		seen[index] = true
		selected = selected || candidate.ID == plan.SelectedCandidate
	}
	if !selected {
		return errors.New("selected method candidate is outside the frozen grid")
	}
	return nil
}

// V2 is a two-method development comparison, not an expansion or replacement
// of V1's exact sixteen-cell grid or its confirmation contract.
func validateStudyMethodPlanV2(plan studyMethodPlan) error {
	if len(plan.Candidates) != 2 || plan.SelectedCandidate != "" {
		return errors.New("method v2 requires both development methods and no selected confirmation candidate")
	}
	var seen [2]bool
	for _, candidate := range plan.Candidates {
		index, err := studyMethodCandidateIndexV2(candidate)
		if err != nil {
			return err
		}
		if seen[index] {
			return errors.New("method v2 repeats a development method")
		}
		seen[index] = true
	}
	return nil
}

func studyMethodCandidateIndexV2(candidate studyMethodCandidate) (int, error) {
	if candidate.GapHz != 150 || candidate.MaxWitnessSeconds != 60 ||
		len(candidate.CWDistances) != 3 || candidate.CWDistances[0] != 1 || candidate.CWDistances[1] != 2 || candidate.CWDistances[2] != 3 {
		return 0, errors.New("method v2 requires 150 Hz, 60 seconds and CW distances [1,2,3]")
	}
	switch {
	case candidate.ID == "c_f150_t60_d3" && candidate.Ranking == "selective":
		return 0, nil
	case candidate.ID == "b_f150_t60_d3" && candidate.Ranking == "receiver_compatibility":
		return 1, nil
	default:
		return 0, errors.New("method v2 requires the exact C/B development identities")
	}
}

func studyMethodCandidateIndex(candidate studyMethodCandidate) (int, error) {
	index := 0
	if candidate.GapHz == 250 {
		index += 8
	} else if candidate.GapHz != 150 {
		return 0, errors.New("method gap must be 150 or 250 Hz")
	}
	if candidate.MaxWitnessSeconds == 120 {
		index += 4
	} else if candidate.MaxWitnessSeconds != 60 {
		return 0, errors.New("method witness separation must be 60 or 120 seconds")
	}
	if candidate.Ranking == "ordinal" {
		index += 2
	} else if candidate.Ranking != "geometry" {
		return 0, errors.New("method ranking must be geometry or ordinal")
	}
	if len(candidate.CWDistances) < 1 || len(candidate.CWDistances) > 2 || candidate.CWDistances[0] != 1 || len(candidate.CWDistances) == 2 && candidate.CWDistances[1] != 2 {
		return 0, errors.New("method CW distances must be [1] or [1,2]")
	}
	index += len(candidate.CWDistances) - 1
	if candidate.ID != fmt.Sprintf("f%d_t%d_%s_d%d", candidate.GapHz, candidate.MaxWitnessSeconds, candidate.Ranking, len(candidate.CWDistances)) {
		return 0, errors.New("method candidate ID does not name its complete frozen combination")
	}
	return index, nil
}

type studyMethodWire uint8

type studyMethodWireField struct {
	name  string
	shape studyMethodWire
}

const (
	methodWirePlan studyMethodWire = iota
	methodWireCandidate
	methodWireCandidates
	methodWireDistances
	methodWireText
	methodWireBool
	methodWireInteger
	methodWireFile
)

func studyMethodPlanPreflight(data []byte) error {
	if len(data) > studyMaxJSONBytes || !utf8.Valid(data) {
		return errors.New("method plan exceeds byte bound or has invalid UTF-8")
	}
	// Read only the scalar discriminator before the collection pass. Unknown
	// fields are skipped without building their arrays; the exact token walker
	// below still rejects duplicates, aliases, omissions and unknown fields.
	var header struct {
		Version int `json:"schema_version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}
	if header.Version != 1 && header.Version != 2 {
		return errors.New("unsupported method schema version")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := studyMethodWireValue(decoder, methodWirePlan, header.Version); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("method plan requires exactly one JSON value")
	}
	return nil
}

func studyMethodWireValue(decoder *json.Decoder, shape studyMethodWire, version int) error {
	switch shape {
	case methodWireText:
		return studyDiagnosticWireValue(decoder, diagnosticWireText, 0)
	case methodWireBool:
		return studyDiagnosticWireValue(decoder, diagnosticWireBool, 0)
	case methodWireInteger:
		return studyDiagnosticWireValue(decoder, diagnosticWireInteger, 0)
	case methodWireFile:
		return studyDiagnosticWireValue(decoder, diagnosticWireFile, 0)
	}
	if shape == methodWireCandidates || shape == methodWireDistances {
		child, minimum, maximum := methodWireCandidate, 16, 16
		if version == 2 {
			minimum, maximum = 2, 2
		}
		if shape == methodWireDistances {
			child, minimum, maximum = methodWireInteger, 1, 2
			if version == 2 {
				minimum, maximum = 3, 3
			}
		}
		return studyMethodWireArray(decoder, child, minimum, maximum, version)
	}
	fields := []studyMethodWireField{{"schema_version", methodWireInteger}, {"experimental", methodWireBool}, {"enabled", methodWireBool}, {"source_revision", methodWireText}, {"reference_version", methodWireText}, {"model", methodWireFile}, {"selected_candidate", methodWireText}, {"candidates", methodWireCandidates}}
	if shape == methodWireCandidate {
		fields = []studyMethodWireField{{"id", methodWireText}, {"gap_hz", methodWireInteger}, {"max_witness_seconds", methodWireInteger}, {"ranking", methodWireText}, {"cw_distances", methodWireDistances}}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return errors.Join(err, errors.New("method operative object required"))
	}
	var seen uint16
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		index := -1
		for i, field := range fields {
			if ok && key == field.name {
				index = i
				break
			}
		}
		if index < 0 || seen&(1<<index) != 0 {
			return errors.New("unknown, duplicate or case-aliased method field")
		}
		seen |= 1 << index
		if err := studyMethodWireValue(decoder, fields[index].shape, version); err != nil {
			return err
		}
	}
	if err := studyDiagnosticWireClose(decoder, '}'); err != nil {
		return err
	}
	if seen != (1<<len(fields))-1 {
		return errors.New("required method field is absent")
	}
	return nil
}

func studyMethodWireArray(decoder *json.Decoder, child studyMethodWire, minimum, maximum, version int) error {
	if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
		return errors.Join(err, errors.New("method operative array required"))
	}
	count := 0
	for decoder.More() {
		if count == maximum {
			return errors.New("method array exceeds its structural bound")
		}
		if err := studyMethodWireValue(decoder, child, version); err != nil {
			return err
		}
		count++
	}
	if err := studyDiagnosticWireClose(decoder, ']'); err != nil {
		return err
	}
	if count < minimum {
		return errors.New("method array lacks required members")
	}
	return nil
}
