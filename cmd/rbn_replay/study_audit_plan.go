// File role: Freeze the inputs to an exposed, read-only stability audit.
// Historical source, executable, configuration and model hashes are provenance
// declarations. This path never reloads or executes those historical inputs.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

const (
	studyAuditSchema             = 1
	studyAuditMaxDays            = 32
	studyAuditLabelBytes         = 1024
	studyAuditRowBytes           = 64 << 10
	studyAuditRecordBytes        = 8 << 20
	studyAuditOutputBytes  int64 = 4 << 30
	studyAuditReserveBytes int64 = 16 << 20
)

type studyAuditReportInput struct {
	Report   studyFile `json:"report"`
	Manifest studyFile `json:"manifest"`
}

type studyAuditGroup struct {
	Plan      studyFile               `json:"plan"`
	Phase     string                  `json:"phase"`
	Candidate string                  `json:"candidate"`
	Reports   []studyAuditReportInput `json:"reports"`
}

type studyAuditPlan struct {
	SchemaVersion int                   `json:"schema_version"`
	Frozen        bool                  `json:"frozen"`
	Exposed       bool                  `json:"exposed"`
	Stability     replayStabilityConfig `json:"stability"`
	Groups        []studyAuditGroup     `json:"groups"`
}

type studyAuditLoadedGroup struct {
	Input      studyAuditGroup
	Plan       studyPlan
	PlanSHA256 string
	Candidate  studyCandidate
	Reports    map[string][3]*studyReport
	Manifests  map[string][3]replayManifest
	Bindings   map[string][3]studyAuditReportInput
}

func loadStudyAuditPlan(path, frozenSHA string) (studyAuditPlan, string, error) {
	var p studyAuditPlan
	if !studyValidSHA(frozenSHA) || studyAuditLocalPath(path) != nil {
		return p, "", errors.New("audit requires a local plan and its externally frozen SHA256")
	}
	data, err := studyReadBounded(path, studyMaxJSONBytes)
	if err != nil {
		return p, "", err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if !strings.EqualFold(digest, frozenSHA) {
		return p, "", errors.New("audit plan differs from frozen SHA256")
	}
	if err := studyDecodeJSON(data, &p); err != nil {
		return p, "", fmt.Errorf("audit plan: %w", err)
	}
	return p, digest, validateStudyAuditPlan(p)
}

func validateStudyAuditPlan(p studyAuditPlan) error {
	if p.SchemaVersion != studyAuditSchema || !p.Frozen || !p.Exposed {
		return errors.New("audit requires schema 1, frozen=true and exposed=true")
	}
	if p.Stability != (replayStabilityConfig{WindowMinutes: 15, MinFollowOn: 3, FreqToleranceHz: 500}) {
		return errors.New("audit requires explicit current stability 15 minutes/3 rows/500 Hz")
	}
	if len(p.Groups) < 1 || len(p.Groups) > studyAuditMaxDays {
		return errors.New("audit requires 1..32 groups")
	}
	if err := studyAuditLabels(reflect.ValueOf(p)); err != nil {
		return err
	}
	seen := make(map[string]bool)
	reports := 0
	for _, group := range p.Groups {
		if err := studyAuditFile(group.Plan); err != nil {
			return err
		}
		if (group.Phase != "tuning" && group.Phase != "holdout") || group.Candidate == "" || len(group.Candidate) > 64 {
			return errors.New("audit groups require explicit phase and candidate")
		}
		key := strings.ToLower(group.Plan.SHA256) + ":" + group.Phase
		if seen[key] {
			return errors.New("audit duplicates an original plan/phase")
		}
		seen[key] = true
		if len(group.Reports) < 3 || len(group.Reports) > 96 || len(group.Reports)%3 != 0 {
			return errors.New("audit requires complete three-arm reports for each group")
		}
		reports += len(group.Reports)
		for _, binding := range group.Reports {
			if err := errors.Join(studyAuditFile(binding.Report), studyAuditFile(binding.Manifest)); err != nil {
				return err
			}
			if !studyAuditSamePath(binding.Manifest.Path, filepath.Join(filepath.Dir(binding.Report.Path), "manifest.json")) {
				return errors.New("audit manifest must be the hash-bound adjacent manifest.json")
			}
		}
	}
	if reports > 3*studyAuditMaxDays {
		return errors.New("audit exceeds 32 total days/96 reports")
	}
	return nil
}

// Inputs are filesystem paths, never download locations or network shares.
func studyAuditLocalPath(path string) error {
	if strings.TrimSpace(path) == "" || len(path) > studyAuditLabelBytes || strings.Contains(path, "://") ||
		strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, "//") || strings.IndexByte(path, 0) >= 0 {
		return errors.New("audit requires a bounded local filesystem path")
	}
	return nil
}

func studyAuditFile(file studyFile) error {
	if !studyValidFile(file) {
		return errors.New("audit input requires a path and SHA256")
	}
	return studyAuditLocalPath(file.Path)
}

func studyAuditSamePath(a, b string) bool {
	left, errA := filepath.Abs(a)
	right, errB := filepath.Abs(b)
	return errA == nil && errB == nil && strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
}

// This walk only visits bounded plan/report metadata or a single flat receipt.
// It does not marshal or traverse a population-sized day registry.
func studyAuditLabels(v reflect.Value) error {
	if !v.IsValid() {
		return nil
	}
	if v.Type() == reflect.TypeFor[time.Time]() {
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		if v.Len() > studyAuditLabelBytes {
			return errors.New("audit label exceeds 1024 bytes")
		}
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			return studyAuditLabels(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := studyAuditLabels(v.Field(i)); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		// RawMessage stores bounded JSON bytes rather than a display label.
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := 0; i < v.Len(); i++ {
			if err := studyAuditLabels(v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			if err := studyAuditLabels(iter.Key()); err != nil {
				return err
			}
			if err := studyAuditLabels(iter.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

func studyAuditLoadGroup(group studyAuditGroup) (studyAuditLoadedGroup, error) {
	loaded := studyAuditLoadedGroup{Input: group, Manifests: make(map[string][3]replayManifest), Bindings: make(map[string][3]studyAuditReportInput)}
	p, digest, err := loadStudyPlan(group.Plan.Path, group.Plan.SHA256)
	if err != nil {
		return loaded, err
	}
	if err := studyAuditLabels(reflect.ValueOf(p)); err != nil {
		return loaded, err
	}
	if err := studyAuditFile(p.ReceiverMetadata); err != nil {
		return loaded, err
	}
	for _, input := range p.Inputs {
		if err := studyAuditFile(input.Archive); err != nil {
			return loaded, err
		}
	}
	candidate, err := studySelectCandidate(p, studyCLIRequest{Phase: group.Phase, Candidate: group.Candidate})
	if err != nil {
		return loaded, err
	}
	paths := make([]string, 0, len(group.Reports))
	for _, binding := range group.Reports {
		if strings.Contains(binding.Report.Path, ",") {
			return loaded, errors.New("audit report path cannot contain comma")
		}
		if err := errors.Join(studyCheckFile(binding.Report), studyCheckFile(binding.Manifest)); err != nil {
			return loaded, err
		}
		paths = append(paths, binding.Report.Path)
	}
	reports, err := studyReadReports(p, digest, studyCLIRequest{Phase: group.Phase, Compare: strings.Join(paths, ",")}, candidate)
	if err != nil {
		return loaded, err
	}
	for _, binding := range group.Reports {
		var report studyReport
		if err := studyAuditReadJSON(binding.Report, &report); err != nil {
			return loaded, err
		}
		var manifest replayManifest
		if err := studyAuditReadJSON(binding.Manifest, &manifest); err != nil {
			return loaded, err
		}
		if err := studyAuditReportBinding(p, candidate, report, manifest, binding); err != nil {
			return loaded, err
		}
		i := studyArmIndex(report.Arm)
		ms, bs := loaded.Manifests[report.DateUTC], loaded.Bindings[report.DateUTC]
		ms[i], bs[i] = manifest, binding
		loaded.Manifests[report.DateUTC], loaded.Bindings[report.DateUTC] = ms, bs
	}
	loaded.Plan, loaded.PlanSHA256, loaded.Candidate, loaded.Reports = p, digest, candidate, reports
	return loaded, nil
}

// Ownership is frozen before any scoring: at most 32 selected day keys and
// archive digests, not a population-sized global raw-ID registry. Distinct plan
// hashes do not authorize counting the same immutable archive twice.
func studyAuditPrepareGroups(p studyAuditPlan) ([]studyAuditLoadedGroup, error) {
	groups := make([]studyAuditLoadedGroup, 0, len(p.Groups))
	archives, instances := make(map[string]bool), make(map[string]bool)
	for _, group := range p.Groups {
		loaded, err := studyAuditLoadAndFreezeGroup(group)
		if err != nil {
			return nil, err
		}
		for _, input := range loaded.Plan.Inputs {
			if input.Phase != group.Phase {
				continue
			}
			archive := strings.ToLower(input.Archive.SHA256)
			instance := input.Phase + "/" + input.DateUTC
			if archives[archive] || instances[instance] {
				return nil, errors.New("audit selected groups overlap an archive or phase/date population")
			}
			if len(archives) >= studyAuditMaxDays {
				return nil, errors.New("audit exceeds 32 selected day populations")
			}
			archives[archive], instances[instance] = true, true
		}
		groups = append(groups, loaded)
	}
	return groups, nil
}

func studyAuditReadJSON(file studyFile, dst any) error {
	if err := studyCheckFile(file); err != nil {
		return err
	}
	data, err := studyReadBounded(file.Path, studyMaxJSONBytes)
	if err != nil {
		return err
	}
	if _, report := dst.(*studyReport); report {
		if err := studyDiagnosticRejectReportMarker(data); err != nil {
			return err
		}
	}
	if err := studyDecodeJSON(data, dst); err != nil {
		return err
	}
	return studyAuditLabels(reflect.ValueOf(dst))
}

func studyAuditReportBinding(p studyPlan, candidate studyCandidate, report studyReport, manifest replayManifest, binding studyAuditReportInput) error {
	if report.Candidate != candidate.ID || report.WarmupSeconds != p.WarmupSeconds || report.FocalRows < 0 || report.TerminalRows < 0 ||
		report.PendingBound < 1 || report.PendingHighWater < 0 || report.PendingHighWater > report.PendingBound ||
		!studyValidSHA(report.CallCorrectionSHA256) || !studyValidSHA(report.PipelineWithoutBayesSHA256) ||
		(report.Arm != "shipped" && !report.MaskSchemaPresent) {
		return errors.New("audit report candidate, mask, pending bound or provenance mismatch")
	}
	if err := studyAuditFile(report.Decisions); err != nil {
		return err
	}
	if !studyAuditSamePath(report.Decisions.Path, filepath.Join(filepath.Dir(binding.Report.Path), "decisions.jsonl")) ||
		!studyAuditSamePath(manifest.Outputs.ManifestJSON, binding.Manifest.Path) || manifest.DateUTC != report.DateUTC ||
		!reflect.DeepEqual(manifest.LicenseInputs, report.LicenseInputs) {
		return errors.New("audit manifest/report path, date or declared license provenance mismatch")
	}
	if manifest.Results.Stability.WindowMinutes != 60 || manifest.Results.Stability.MinFollowOn != 2 || manifest.Results.Stability.FreqToleranceHz != 1000 ||
		manifest.Results.Stability.TotalApplied < 0 || manifest.Results.Stability.StableApplied < 0 ||
		manifest.Results.Stability.StableApplied > manifest.Results.Stability.TotalApplied || manifest.Results.Stability.TotalApplied > report.TerminalRows {
		return errors.New("audit requires a valid frozen legacy 60/2/1000 manifest")
	}
	for _, input := range p.Inputs {
		if input.DateUTC == report.DateUTC && !studyAuditSamePath(manifest.InputZipPath, input.Archive.Path) {
			return errors.New("audit manifest archive differs from the original frozen plan")
		}
	}
	return nil
}

// Reject aliases to output inputs before creating anything. The directory is
// exclusively owned by this run; no existing artifact is overwritten or erased.
func studyAuditNewDirectory(output string) error {
	if err := studyAuditLocalPath(output); err != nil {
		return err
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(err, errors.New("audit output must be a new directory"))
	}
	return os.Mkdir(output, 0o700)
}
