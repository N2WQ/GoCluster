// File role: Read experimental evidence through explicit frozen bindings.
// Legacy readers remain refusing. Token preflight admits only the finite local
// DTOs below, before slices/maps can grow; no marker is removed or arm renamed.
package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

func studyMethodEvalReadJSON(file studyFile, dst any) error {
	if err := studyAuditFile(file); err != nil {
		return err
	}
	data, err := studyReadOrdinalAsset(file.Path)
	if err != nil {
		return err
	}
	sum := studySHA256Bytes(data)
	if hex.EncodeToString(sum[:]) != strings.ToLower(file.SHA256) {
		return errors.New("evaluation metadata differs from its frozen SHA256")
	}
	allowed := false
	switch dst.(type) {
	case *studyMethodEvaluationPlan, *studyReport, *studyMethodRunReport, *replayManifest:
		allowed = true
	}
	if !allowed || reflect.ValueOf(dst).IsNil() {
		return errors.New("unsupported experimental evaluation metadata")
	}
	if len(data) > studyMaxJSONBytes || !utf8.Valid(data) {
		return errors.New("invalid evaluation metadata byte bound/UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := studyMethodEvalWire(decoder, reflect.TypeOf(dst).Elem(), 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("evaluation metadata requires one JSON value")
	}
	return studyDecodeJSON(data, dst)
}

// This local shape walker has no arbitrary destination or opaque metadata seam.
// Required facts cannot be null. Optional omitted fields retain their existing
// serialization contract, but a present optional field must have the right type.
func studyMethodEvalWire(d *json.Decoder, t reflect.Type, depth int) error {
	if depth > 32 {
		return errors.New("evaluation metadata nesting exceeds 32")
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("evaluation operative fact is null")
	}
	if t == reflect.TypeFor[time.Time]() {
		if _, ok := token.(string); !ok {
			return errors.New("evaluation timestamp must be a string")
		}
		return nil
	}
	switch t.Kind() {
	case reflect.Struct:
		if token != json.Delim('{') || t.NumField() > 64 {
			return errors.New("evaluation object shape is invalid")
		}
		var seen uint64
		for d.More() {
			keyToken, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			index := -1
			for i := 0; i < t.NumField(); i++ {
				name := strings.Split(t.Field(i).Tag.Get("json"), ",")[0]
				if name == "" {
					name = t.Field(i).Name
				}
				if ok && key == name {
					index = i
					break
				}
			}
			if index < 0 || seen&(1<<index) != 0 {
				return errors.New("unknown, duplicate or case-aliased evaluation field")
			}
			seen |= 1 << index
			if err := studyMethodEvalWire(d, t.Field(index).Type, depth+1); err != nil {
				return err
			}
		}
		if err := studyDiagnosticWireClose(d, '}'); err != nil {
			return err
		}
		for i := 0; i < t.NumField(); i++ {
			if !strings.Contains(t.Field(i).Tag.Get("json"), ",omitempty") && seen&(1<<i) == 0 {
				return errors.New("required evaluation field is absent")
			}
		}
	case reflect.Slice, reflect.Array:
		if token != json.Delim('[') {
			return errors.New("evaluation array required")
		}
		limit := 256
		switch t.Elem() {
		case reflect.TypeFor[studyMethodEvalParity](), reflect.TypeFor[studyMethodEvalGroup]():
			limit = 4
		case reflect.TypeFor[studyMethodEvalVariant]():
			limit = 2
		case reflect.TypeFor[studyDiagnosticCopyRecord]():
			limit = 128
		case reflect.TypeFor[int]():
			limit = 2
		}
		if t.Kind() == reflect.Array {
			limit = t.Len()
		}
		count := 0
		for d.More() {
			if count == limit {
				return errors.New("evaluation array exceeds structural bound")
			}
			if err := studyMethodEvalWire(d, t.Elem(), depth+1); err != nil {
				return err
			}
			count++
		}
		if t.Kind() == reflect.Array && count != limit {
			return errors.New("evaluation fixed array is short")
		}
		return studyDiagnosticWireClose(d, ']')
	case reflect.Map:
		if token != json.Delim('{') || t.Key().Kind() != reflect.String {
			return errors.New("evaluation map shape is invalid")
		}
		var keys [32]string
		count := 0
		for d.More() {
			keyToken, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || len(key) > 1024 || count == len(keys) {
				return errors.New("evaluation map exceeds bound")
			}
			for _, old := range keys[:count] {
				if old == key {
					return errors.New("duplicate evaluation map key")
				}
			}
			keys[count] = key
			count++
			if err := studyMethodEvalWire(d, t.Elem(), depth+1); err != nil {
				return err
			}
		}
		return studyDiagnosticWireClose(d, '}')
	case reflect.String:
		value, ok := token.(string)
		if !ok || len(value) > studyAuditLabelBytes {
			return errors.New("evaluation string exceeds bound")
		}
	case reflect.Bool:
		if _, ok := token.(bool); !ok {
			return errors.New("evaluation boolean required")
		}
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Float64:
		if _, ok := token.(json.Number); !ok {
			return errors.New("evaluation number required")
		}
	default:
		return errors.New("unsupported evaluation wire type")
	}
	return nil
}

type studyMethodEvalLoadedGroup struct {
	Input     studyInput
	Reports   [3]*studyReport
	Manifests [3]replayManifest
	Bindings  []studyMethodEvalBinding
}

func (p *studyMethodEvalPrepared) loadGroup(group studyMethodEvalGroup) (studyMethodEvalLoadedGroup, error) {
	var loaded studyMethodEvalLoadedGroup
	input, err := p.input(group.DateUTC)
	if err != nil {
		return loaded, err
	}
	loaded.Input = input
	loaded.Bindings = append(loaded.Bindings, group.Control)
	for _, candidate := range p.Variants {
		for _, variant := range group.Variants {
			if variant.CandidateID == candidate.ID {
				loaded.Bindings = append(loaded.Bindings, studyMethodEvalBinding{variant.Complete, variant.Manifest})
			}
		}
	}
	for role, binding := range loaded.Bindings {
		expected, enabled := p.Control, false
		id := ""
		if p.Plan.Phase == "holdout" {
			id = p.Control.SelectedCandidate
		}
		if role > 0 {
			expected, enabled = p.Method, true
			id = p.Variants[role-1].ID
		}
		report, manifest, err := p.readReport(binding, input, expected, enabled, id)
		if err != nil {
			return loaded, err
		}
		loaded.Reports[role], loaded.Manifests[role] = report, manifest
		if role == 0 && len(p.ParityGroups) > 0 {
			parity := p.ParityGroups[0].Reports[0]
			if report.CallCorrectionSHA256 != parity.CallCorrectionSHA256 || report.PipelineWithoutBayesSHA256 != parity.PipelineWithoutBayesSHA256 {
				return loaded, errors.New("current control pipeline fingerprints differ from disabled parity")
			}
		}
		if role > 0 && (report.CallCorrectionSHA256 != loaded.Reports[0].CallCorrectionSHA256 || report.PipelineWithoutBayesSHA256 != loaded.Reports[0].PipelineWithoutBayesSHA256) {
			return loaded, errors.New("method/control pipeline configuration differs")
		}
	}
	if p.Plan.Phase == "tuning" {
		for parityIndex := range p.Plan.Parity {
			parity := &p.Plan.Parity[parityIndex]
			if parity.DateUTC == group.DateUTC && (parity.Rows != loaded.Reports[0].TerminalRows || !studyMethodEvalSameFile(parity.ControlDecisions, loaded.Reports[0].Decisions)) {
				return loaded, errors.New("tuning control is not the frozen disabled-parity stream")
			}
		}
	}
	// Borrowing the already-admitted second stream fills only the legacy join's
	// fixed-width validation slot. It creates no report, role or predictor state;
	// holdout metrics and outputs still iterate exactly two actual bindings.
	if len(loaded.Bindings) == 2 {
		loaded.Reports[2], loaded.Manifests[2] = loaded.Reports[1], loaded.Manifests[1]
	}
	return loaded, nil
}

func (p *studyMethodEvalPrepared) readReport(binding studyMethodEvalBinding, input studyInput, method studyMethodPlan, enabled bool, id string) (*studyReport, replayManifest, error) {
	var report studyReport
	var manifest replayManifest
	if err := studyMethodEvalReadJSON(binding.Complete, &report); err != nil {
		return nil, manifest, err
	}
	if report.Experimental == nil || report.Diagnostics != nil {
		return nil, manifest, errors.New("evaluation requires an explicit experimental method manifest without diagnostics")
	}
	if err := studyMethodEvalReadJSON(binding.Manifest, &manifest); err != nil {
		return nil, manifest, err
	}
	if report.SchemaVersion != studySchemaVersion || !report.Complete || report.Arm != "repaired_off" || report.DateUTC != input.DateUTC || report.Phase != p.Plan.Phase || report.PlanSHA256 != p.Plan.CapturePlan.SHA256 || report.ReferenceVersion != p.Capture.ReferenceVersion || report.InputScenario != p.Capture.InputScenario || report.SourceRevision != p.Arm.SourceRevision || report.ExecutableSHA256 != p.Arm.ExecutableSHA256 || report.AssetsSHA256 != studyAssetDigest(p.Arm.Assets) || report.ConfigFilesSHA256 != hashAnyStable(p.Arm.ConfigFiles) || report.CandidateSHA256 != hashAnyStable(p.Candidate) || report.FocalRows != report.TerminalRows || report.RawRows < 1 || report.RawRows > input.MaxRows || report.MaxRows != input.MaxRows || report.MaxCSVBytes != input.MaxCSVBytes || !report.InputCoverage.complete(report.FocalRows) {
		return nil, manifest, errors.New("experimental report has incomplete or mismatched frozen provenance")
	}
	if err := studyAuditReportBinding(p.Capture, p.Candidate, report, manifest, studyAuditReportInput{Report: binding.Complete, Manifest: binding.Manifest}); err != nil {
		return nil, manifest, err
	}
	if manifest.InputZipMeta.SHA256 != input.Archive.SHA256 {
		return nil, manifest, errors.New("experimental manifest archive hash differs")
	}
	marker := report.Experimental
	planRef := p.Plan.ControlPlan
	if enabled {
		planRef = p.Plan.MethodPlan
	}
	if marker.SchemaVersion != 1 || !marker.Experimental || marker.Enabled != enabled || marker.Plan.SHA256 != planRef.SHA256 || marker.Model.SHA256 != method.Model.SHA256 || id != "" && marker.CandidateID != id {
		return nil, manifest, errors.New("experimental marker plan/model/candidate binding differs")
	}
	if _, ok := p.methodCandidate(marker.CandidateID); !ok {
		return nil, manifest, errors.New("experimental marker candidate is outside frozen grid")
	}
	if err := p.checkMethodEvidence(binding, report, method, planRef); err != nil {
		return nil, manifest, err
	}
	return &report, manifest, nil
}

func (p *studyMethodEvalPrepared) checkMethodEvidence(binding studyMethodEvalBinding, report studyReport, method studyMethodPlan, planRef studyFile) error {
	marker := report.Experimental
	root := filepath.Dir(binding.Complete.Path)
	for name, file := range map[string]studyFile{"method-plan.json": marker.Plan, "method-model.json": marker.Model, "method-report.json": marker.Report} {
		if !studyAuditSamePath(file.Path, filepath.Join(root, name)) {
			return errors.New("method evidence escaped its fixed run path")
		}
		if err := studyMethodCheckInput(file); err != nil {
			return err
		}
	}
	var evidence studyMethodRunReport
	if err := studyMethodEvalReadJSON(marker.Report, &evidence); err != nil {
		return err
	}
	candidate, ok := p.methodCandidate(marker.CandidateID)
	if !ok || evidence.SchemaVersion != 1 || !evidence.Experimental || evidence.Enabled != marker.Enabled || !reflect.DeepEqual(evidence.Candidate, candidate) || !studyMethodEvalSameFile(evidence.Plan, marker.Plan) || !studyMethodEvalSameFile(evidence.Model, marker.Model) || !studyMethodEvalSameFile(evidence.OriginalPlan, planRef) || !studyMethodEvalSameFile(evidence.OriginalModel, method.Model) {
		return errors.New("method report disagrees with frozen manifest/plan/model")
	}
	return studyMethodEvalCheckInventory(root, binding.Complete, marker.Report, evidence, report.Decisions)
}

func studyMethodEvalCheckInventory(root string, complete, methodReport studyFile, e studyMethodRunReport, decisions studyFile) error {
	account := e.Accounting
	if account.LimitBytes != studyAuditOutputBytes || account.ReservedCapacityBytes != studyAuditReserveBytes || account.ChargedBytes < 0 || account.ChargedBytes > account.LimitBytes || account.Stage != "after_cleanup_and_closed_output_hashes_before_method_report_and_complete" || account.Policy != "cumulative requested writes; no cleanup refunds; rename charged once" || len(e.Outputs) > 256 || len(e.Copies) > 128 || e.DeletedCopyBytes < 0 || e.UnretainedChargedBytes < 0 || e.Scratch.Backend == "not_constructed" {
		return errors.New("invalid method accounting/inventory bounds")
	}
	seen := make(map[string]bool, len(e.Outputs))
	var retained, deleted int64
	foundDecisions := false
	owner, err := newStudyDiagnosticArtifacts(root)
	if err != nil {
		return err
	}
	for _, output := range e.Outputs {
		path, err := owner.destination(output.File.Path)
		if err != nil {
			return err
		}
		key := studyDiagnosticPathKey(path)
		if seen[key] || output.Bytes < 0 || output.Bytes > studyAuditOutputBytes-retained || studyAuditSamePath(path, complete.Path) || studyAuditSamePath(path, methodReport.Path) {
			return errors.New("duplicate, self-referential or oversized method inventory")
		}
		seen[key] = true
		retained += output.Bytes
		info, err := os.Lstat(path)
		if err != nil || info.Size() != output.Bytes {
			return errors.Join(err, errors.New("method output size changed"))
		}
		if err := studyCheckFile(output.File); err != nil {
			return err
		}
		foundDecisions = foundDecisions || output.File == decisions
	}
	copyPaths := make(map[string]bool, len(e.Copies))
	for _, copy := range e.Copies {
		r := copy.Receipt
		key := studyDiagnosticPathKey(r.Path)
		if copyPaths[key] || !copy.VerifiedBeforeCleanup || !studyValidSHA(r.SHA256) || r.BytesCopied < 0 || r.SourceSizeAtStat < 0 {
			return errors.New("invalid method copy receipt")
		}
		copyPaths[key] = true
		if copy.DeletedAfterCleanup {
			parent := filepath.Dir(r.Path)
			if !studyAuditSamePath(filepath.Dir(parent), root) || !strings.HasPrefix(filepath.Base(parent), ".license-inputs-") || r.BytesCopied > studyAuditOutputBytes-deleted {
				return errors.New("deleted copy is not the exact license-owned child")
			}
			if _, err := os.Lstat(r.Path); !os.IsNotExist(err) {
				return errors.New("deleted method copy still exists")
			}
			deleted += r.BytesCopied
		} else {
			if !seen[key] {
				return errors.New("retained copy is absent from closed inventory")
			}
			if err := studyCheckFile(studyFile{Path: r.Path, SHA256: r.SHA256}); err != nil {
				return err
			}
		}
	}
	for _, name := range []string{"method-plan.json", "method-model.json"} {
		key := studyDiagnosticPathKey(filepath.Join(owner.root, name))
		if !seen[key] || !copyPaths[key] {
			return errors.New("method plan/model copy is missing from inventory/receipts")
		}
	}
	for _, name := range []string{"method-receipts.jsonl", "manifest.json"} {
		if !seen[studyDiagnosticPathKey(filepath.Join(owner.root, name))] {
			return errors.New("mandatory method output is absent from inventory")
		}
	}
	if !foundDecisions || deleted != e.DeletedCopyBytes || retained+deleted > account.ChargedBytes || e.UnretainedChargedBytes != account.ChargedBytes-retained-deleted || e.RetainedEvidence.RegularFiles != uint64(len(e.Outputs)) || e.RetainedEvidence.RegularFileBytes != retained || !e.RetainedEvidence.Complete || !strings.Contains(e.InventoryStage, "closed prior outputs only") {
		return errors.New("method prior-output accounting does not reconcile")
	}
	if e.Scratch.Applicable {
		if e.Scratch.Backend != "custom_scp_runtime" || !studyAuditSamePath(e.Scratch.Path, filepath.Join(root, studyDiagnosticScratchChild)) || e.Scratch.MaxKeys < 1 || e.Scratch.MaxSpottersPerKey < 1 || e.Scratch.Measurement == nil {
			return errors.New("invalid separately measured method scratch")
		}
		measurement, err := measureStudyDiagnosticScratch(e.Scratch.Path)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(measurement, *e.Scratch.Measurement) {
			return errors.New("method scratch measurement changed")
		}
	} else if e.Scratch.Path != "" || e.Scratch.Measurement != nil {
		return errors.New("inapplicable method scratch carries a measurement")
	}
	actual, err := measureStudyDiagnosticEvidence(root, e.Scratch.Path)
	if err != nil {
		return err
	}
	var metadataBytes int64
	for _, file := range []studyFile{methodReport, complete} {
		info, err := os.Stat(file.Path)
		if err != nil {
			return err
		}
		metadataBytes += info.Size()
	}
	if actual.RegularFiles != uint64(len(e.Outputs)+2) || actual.RegularFileBytes != retained+metadataBytes || metadataBytes > studyAuditOutputBytes-account.ChargedBytes || e.RetainedEvidence.Limits != actual.Limits {
		return errors.New("method final inventory does not include exactly report and COMPLETE last")
	}
	return nil
}

func studyMethodEvalSameFile(a, b studyFile) bool {
	return a.SHA256 == b.SHA256 && studyAuditSamePath(a.Path, b.Path)
}

// The authoritative full-stream hash already fixes every row. Inspecting its
// first bounded original ID additionally binds the parity capture's declared ZIP
// hash to that immutable stream, rather than trusting matching metadata alone.
func studyMethodEvalParityArchiveBinding(decisions studyFile, archiveSHA string) error {
	file, err := os.Open(decisions.Path)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 16384)
	if !scanner.Scan() {
		return errors.Join(scanner.Err(), file.Close(), errors.New("parity stream has no bounded original row"))
	}
	var decision studyDecision
	decodeErr := studyDecodeJSON(scanner.Bytes(), &decision)
	if err := errors.Join(decodeErr, file.Close()); err != nil {
		return err
	}
	if !studyDecisionBinding(decision, archiveSHA) {
		return errors.New("parity archive hash differs from the authoritative original IDs")
	}
	return nil
}
