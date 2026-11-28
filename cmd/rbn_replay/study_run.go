// File role: Run the study with the existing replay parser and bounded owners.
// It bypasses only download discovery, requiring frozen local inputs instead.
// Each run starts with empty A-only history; this is a declared experimental
// population, not a reconstruction of a warm production history or delivery.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"

	"dxcluster/config"
	"dxcluster/pathreliability"
)

func studySHA256Bytes(data []byte) [32]byte { return sha256.Sum256(data) }

func runCCV2Study(req studyCLIRequest) error {
	if err := validateOrdinaryGuardRequest(req); err != nil {
		return err
	}
	if (req.MethodPlan != "" || req.MethodFreeze != "" || req.MethodCandidate != "") && (req.Compare != "" || req.DiagnosticSelection != "" || req.DiagnosticFreeze != "") {
		return errors.New("experimental method cannot use legacy comparison or selected diagnostics")
	}
	if err := studyDiagnosticValidateRequest(req); err != nil {
		return err
	}
	p, digest, err := loadStudyPlan(req.Plan, req.Freeze)
	if err != nil {
		return err
	}
	if req.Output == "" || req.Phase != "tuning" && req.Phase != "holdout" {
		return errors.New("study requires -study-out and -study-phase tuning|holdout")
	}
	if req.Compare != "" {
		if req.Arm != "" || req.Date != "" {
			return errors.New("study comparison cannot select an arm or date")
		}
		return compareCCV2Study(p, digest, req)
	}
	var arm *studyArm
	for i := range p.Arms {
		if p.Arms[i].Name == req.Arm {
			arm = &p.Arms[i]
		}
	}
	if arm == nil {
		return errors.New("study requires -study-arm shipped|repaired_off|repaired_on")
	}
	candidate, err := studySelectCandidate(p, req)
	if err != nil {
		return err
	}
	var input *studyInput
	for i := range p.Inputs {
		if p.Inputs[i].DateUTC == req.Date && p.Inputs[i].Phase == req.Phase {
			input = &p.Inputs[i]
		}
	}
	if input == nil {
		return errors.New("study date/population was not predeclared in frozen plan")
	}
	if info, err := os.Stat(input.Archive.Path); err != nil || !info.Mode().IsRegular() {
		return errors.Join(err, errors.New("study archive must be a regular local file"))
	}
	if err := studyCheckExecutable(*arm); err != nil {
		return err
	}
	if err := studyCheckConfigFiles(*arm); err != nil {
		return err
	}
	// Validation precedes input content inspection. In particular, a holdout
	// run cannot select a different complete vector or cell map after freezing.
	cfg, err := config.Load(arm.ConfigDir)
	if err != nil {
		return err
	}
	if err := studyCheckConfigFiles(*arm); err != nil {
		return err
	}
	if err := studyApplyCandidate(cfg, *arm, candidate); err != nil {
		return err
	}
	maskSchema, err := studyValidateRuntimeConfig(cfg, *arm, candidate, p)
	if err != nil {
		return err
	}
	if err := studyCheckAssets(*arm, cfg); err != nil {
		return err
	}
	if err := studyValidateGeographyAssets(p.InputScenario, *arm, cfg); err != nil {
		return err
	}
	prepared, err := prepareStudyDiagnostics(req)
	if err != nil {
		return err
	}
	method, err := prepareStudyMethod(req, *arm)
	if err != nil {
		return err
	}
	if err := studyMethodRuntimeAuthority(cfg, method); err != nil {
		return err
	}
	if prepared != nil || method != nil {
		req.Output, err = filepath.Abs(req.Output)
		if err != nil {
			return err
		}
	}
	if err := os.Mkdir(req.Output, 0o700); err != nil {
		return fmt.Errorf("study output must be a new directory: %w", err)
	}
	return studyRunDayPreparedMethod(p, digest, req, *arm, candidate, *input, cfg, nil, maskSchema, prepared, method)
}

func studySelectCandidate(p studyPlan, req studyCLIRequest) (studyCandidate, error) {
	id := req.Candidate
	if id == "" {
		id = p.SelectedCandidate
	}
	if req.Phase == "holdout" && id != p.SelectedCandidate {
		return studyCandidate{}, errors.New("holdout requires the single frozen selected candidate")
	}
	for _, c := range p.Candidates {
		if c.ID == id {
			return c, nil
		}
	}
	return studyCandidate{}, errors.New("candidate is not in the frozen complete candidate list")
}

func studyCheckExecutable(arm studyArm) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	if err := studyCheckFile(studyFile{Path: executable, SHA256: arm.ExecutableSHA256}); err != nil {
		return fmt.Errorf("study arm executable: %w", err)
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("study executable has no Go build provenance")
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && setting.Value == arm.SourceRevision {
			return nil
		}
	}
	return errors.New("study arm source revision differs from executable build provenance")
}

func studyCheckConfigFiles(arm studyArm) error {
	actual := make(map[string]string)
	err := filepath.WalkDir(arm.ConfigDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("study configuration must not contain symbolic links")
		}
		if entry.IsDir() {
			return nil
		}
		if len(actual) >= 256 {
			return errors.New("study configuration exceeds 256 frozen files")
		}
		name, err := filepath.Rel(arm.ConfigDir, path)
		if err != nil {
			return err
		}
		name = filepath.ToSlash(name)
		sum, err := studyHashFile(path)
		actual[name] = sum
		return err
	})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, arm.ConfigFiles) {
		return errors.New("study complete configuration file set or SHA256 changed")
	}
	return nil
}

func studyCheckAssets(arm studyArm, cfg *config.Config) error {
	files := make(map[string]studyFile, len(arm.Assets))
	for _, file := range arm.Assets {
		abs, err := filepath.Abs(file.Path)
		if err != nil {
			return err
		}
		files[strings.ToLower(abs)] = file
		if err := studyCheckFile(file); err != nil {
			return err
		}
	}
	paths := []string{cfg.CallCorrection.SpotterReliabilityFile, cfg.CallCorrection.SpotterReliabilityFileCW,
		cfg.CallCorrection.SpotterReliabilityFileRTTY, cfg.FCCULS.AllowlistPath}
	if cfg.CTY.Enabled {
		paths = append(paths, cfg.CTY.File)
	}
	if cfg.CallCorrection.ConfusionModelEnabled {
		paths = append(paths, cfg.CallCorrection.ConfusionModelFile)
	}
	// A configured disabled database can still answer factual gate queries.
	// Require its frozen file rather than relying on enforcement alone.
	for _, db := range []string{cfg.FCCULS.DBPath, cfg.ISED.DBPath} {
		if strings.TrimSpace(db) != "" {
			paths = append(paths, db)
		}
	}
	if cfg.CallCorrection.CustomSCP.Enabled {
		paths = append(paths, filepath.Join(cfg.H3TablePath, "res1.bin"), filepath.Join(cfg.H3TablePath, "res2.bin"))
	}
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		if _, ok := files[strings.ToLower(abs)]; !ok {
			return fmt.Errorf("study runtime asset is not frozen in manifest: %s", path)
		}
	}
	return nil
}

func studyRunDay(p studyPlan, digest string, req studyCLIRequest, arm studyArm, candidate studyCandidate,
	in studyInput, cfg *config.Config, day *studyRawDay, maskSchema bool) (err error) {
	prepared, err := prepareStudyDiagnostics(req)
	if err != nil {
		return err
	}
	return studyRunDayPrepared(p, digest, req, arm, candidate, in, cfg, day, maskSchema, prepared)
}

func studyRunDayPrepared(p studyPlan, digest string, req studyCLIRequest, arm studyArm, candidate studyCandidate,
	in studyInput, cfg *config.Config, day *studyRawDay, maskSchema bool, prepared *studyDiagnosticPrepared) (err error) {
	method, err := prepareStudyMethod(req, arm)
	if err != nil {
		return err
	}
	return studyRunDayPreparedMethod(p, digest, req, arm, candidate, in, cfg, day, maskSchema, prepared, method)
}

func studyRunDayPreparedMethod(p studyPlan, digest string, req studyCLIRequest, arm studyArm, candidate studyCandidate,
	in studyInput, cfg *config.Config, day *studyRawDay, maskSchema bool, prepared *studyDiagnosticPrepared, method *studyMethodPrepared) (err error) {
	if err := validateOrdinaryGuardRequest(req); err != nil {
		return err
	}
	if req.OrdinaryGuard && (prepared != nil || method != nil || cfg.CallCorrection.SelectiveRescueEnabled) {
		return errors.New("ordinary guard requires the ordinary resolver with selective rescue and diagnostics off")
	}
	if err := studyMethodRuntimeAuthority(cfg, method); err != nil {
		return err
	}
	if prepared != nil && method != nil {
		return errors.New("method cannot masquerade as selected diagnostic capture")
	}
	if prepared != nil || method != nil {
		req.Output, err = filepath.Abs(req.Output)
		if err != nil {
			return err
		}
	}
	r := newReplayRunner(replayRequest{dateValue: in.DateUTC, frozenStudyResolverLimits: true})
	r.ordinaryGuard = req.OrdinaryGuard
	r.cfg, r.replayCfg, r.outDir = cfg, defaultReplayConfig(), req.Output
	r.archiveDir, r.zipPath = req.Output, in.Archive.Path
	r.zipResult.Meta.SHA256 = in.Archive.SHA256
	if prepared != nil {
		r.diagnostics, err = newStudyDiagnosticRun(req.Output, prepared)
		if err != nil {
			return err
		}
		req.Output = r.diagnostics.owner.root
		r.outDir, r.archiveDir = req.Output, req.Output
	}
	if method != nil {
		r.method, err = newStudyMethodRun(req.Output, method)
		if err != nil {
			return err
		}
		req.Output = r.method.files.owner.root
		r.outDir, r.archiveDir = req.Output, req.Output
	}
	var observer *studyObserver
	defer func() {
		if observer != nil {
			err = errors.Join(err, observer.close())
		}
		err = errors.Join(err, r.closeWithError())
		if r.diagnostics != nil {
			err = errors.Join(err, r.diagnostics.closeStreams())
		}
		if r.method != nil {
			err = errors.Join(err, r.method.closeStreams(), r.method.files.owner.Err())
		}
		if err != nil {
			// A failed run never publishes complete.json. Partial legacy files
			// and decision streams are explicitly unusable for qualification.
			if r.diagnostics != nil {
				err = errors.Join(err, r.diagnostics.failure(err))
			} else if r.method != nil {
				err = errors.Join(err, r.method.failure(err))
			} else {
				err = errors.Join(err, writeJSONAtomic(filepath.Join(req.Output, "FAILED.json"), map[string]string{"error": err.Error()}))
			}
		}
	}()
	if err := r.resolveDay(); err != nil {
		return err
	}
	if err := studyValidateGeographyAssets(p.InputScenario, arm, cfg); err != nil {
		return err
	}
	ccFingerprint := hashAnyStable(cfg.CallCorrection)
	pipelineFingerprint, err := studyPipelineFingerprint(cfg)
	if err != nil {
		return err
	}
	if err := studySnapshotAssets(arm, r); err != nil {
		return err
	}
	if files := r.studyArtifactFiles(); files != nil {
		if err := files.verifyCopies(false); err != nil {
			return err
		}
	}
	if err := pathreliability.InitRequiredH3Mappings(cfg.H3TablePath); err != nil {
		return err
	}
	if err := r.configureExternalDependencies(); err != nil {
		return err
	}
	if err := studyValidateCTYImage(r.ctyDB); err != nil {
		return err
	}
	if err := r.buildReplayRuntime(); err != nil {
		return err
	}
	if files := r.studyArtifactFiles(); files != nil {
		if err := files.verifyCopies(false); err != nil {
			return err
		}
		if err := r.diagnosticBoundary(); err != nil {
			return err
		}
	}
	// Asset/schema/setup failures must not inspect raw archive contents or be
	// converted into per-row unknown locations. Tests may supply a private day;
	// the CLI always parses only after frozen dependencies have loaded.
	if err := studyCheckFile(in.Archive); err != nil {
		return err
	}
	if day == nil {
		aliases, err := loadStudyReceiverAliases(p.ReceiverMetadata)
		if err != nil {
			return err
		}
		day, err = loadStudyRawDay(in, aliases, p.WarmupSeconds)
		if err != nil {
			return err
		}
	}
	if r.diagnostics != nil {
		if err := r.diagnostics.bindDay(in, day, r.temporalNextID); err != nil {
			return err
		}
	}
	if err := r.openReplayOutputs(); err != nil {
		return err
	}
	observer, err = newStudyObserver(day, r, filepath.Join(req.Output, "decisions.jsonl"))
	if err != nil {
		return err
	}
	r.study = observer
	if err := r.replayDay(); err != nil {
		return err
	}
	if err := observer.complete(); err != nil {
		return err
	}
	if err := observer.close(); err != nil {
		return err
	}
	if err := r.finalize(); err != nil {
		return err
	}
	// Recheck content used by the second parser pass and copies. A changed
	// original can invalidate the run, never silently become a different arm.
	if err := studyCheckFile(in.Archive); err != nil {
		return err
	}
	if files := r.studyArtifactFiles(); files != nil {
		if err := files.beforeCleanup(r.licenseInputsDir); err != nil {
			return err
		}
	}
	if err := r.closeWithError(); err != nil {
		return err
	}
	decisionSHA, err := studyHashFile(observer.path)
	if err != nil {
		return err
	}
	report := newStudyReport(p, digest, req, arm, candidate, in, observer, maskSchema)
	report.Decisions = studyFile{Path: observer.path, SHA256: decisionSHA}
	report.CallCorrectionSHA256, report.PipelineWithoutBayesSHA256 = ccFingerprint, pipelineFingerprint
	report.LicenseInputs = r.licenseInputs
	if r.ordinaryGuard {
		report.OrdinaryGuard = true
		report.ClaimLimits = append(report.ClaimLimits, "Ordinary CW safeguard experiment on exposed dates; no activation or RF-accuracy qualification")
	}
	if r.diagnostics != nil {
		report.Diagnostics, err = r.diagnostics.complete(r.temporalNextID)
		if err != nil {
			return err
		}
		report.ClaimLimits = append(report.ClaimLimits, "This exposed diagnostic capture is non-qualification evidence; it cannot authorize activation, model fitting or deployment")
		return r.diagnostics.owner.atomicJSON(filepath.Join(req.Output, "complete.json"), report)
	}
	if r.method != nil {
		report.Experimental, err = r.method.complete()
		if err != nil {
			return err
		}
		report.ClaimLimits = append(report.ClaimLimits, "Experimental method replay; legacy activation readers refuse marker presence, including disabled parity; no live activation or RF-probability claim")
		return r.method.files.owner.atomicJSON(filepath.Join(req.Output, "complete.json"), report)
	}
	return writeJSONAtomic(filepath.Join(req.Output, "complete.json"), report)
}

// Experimental policy is owned by the frozen method plan. A live rescue flag
// would introduce a second policy authority and invalidate disabled parity.
func studyMethodRuntimeAuthority(cfg *config.Config, method *studyMethodPrepared) error {
	if method != nil && cfg != nil && cfg.CallCorrection.SelectiveRescueEnabled {
		return errors.New("experimental method requires call_correction.selective_rescue_enabled false")
	}
	return nil
}

// Stream-copy every frozen asset, verify the copy, then route consumers to
// those owned paths. No production model, license or history file is written.
func studySnapshotAssets(arm studyArm, r *replayRunner) error {
	root := filepath.Join(r.outDir, ".study-assets")
	if err := os.Mkdir(root, 0o700); err != nil {
		return err
	}
	frozen := make(map[string]string, len(arm.Assets))
	for i, asset := range arm.Assets {
		destination := filepath.Join(root, fmt.Sprintf("%02d_%s", i, filepath.Base(asset.Path)))
		input := replayLicenseInput{}
		source, err := os.Open(asset.Path)
		if err != nil {
			return err
		}
		err = errors.Join(r.copyFrozenLicenseFile(source, destination, &input), source.Close())
		if err != nil {
			return err
		}
		if !strings.EqualFold(input.SHA256, asset.SHA256) {
			return errors.New("study asset changed while freezing")
		}
		abs, err := filepath.Abs(asset.Path)
		if err != nil {
			return err
		}
		frozen[strings.ToLower(abs)] = destination
	}
	rewrite := func(path *string) {
		abs, err := filepath.Abs(*path)
		if err == nil && strings.TrimSpace(*path) != "" {
			if destination, ok := frozen[strings.ToLower(abs)]; ok {
				*path = destination
			}
		}
	}
	cfg := r.cfg
	for _, path := range []*string{&cfg.CTY.File, &cfg.CallCorrection.ConfusionModelFile,
		&cfg.CallCorrection.SpotterReliabilityFile, &cfg.CallCorrection.SpotterReliabilityFileCW,
		&cfg.CallCorrection.SpotterReliabilityFileRTTY, &cfg.FCCULS.DBPath, &cfg.ISED.DBPath, &cfg.FCCULS.AllowlistPath} {
		rewrite(path)
	}
	geography := filepath.Join(root, "geography")
	if err := os.Mkdir(geography, 0o700); err != nil {
		return err
	}
	for _, name := range []string{"res1.bin", "res2.bin"} {
		original, err := filepath.Abs(filepath.Join(cfg.H3TablePath, name))
		if err != nil {
			return err
		}
		path, ok := frozen[strings.ToLower(original)]
		if !ok || filepath.Dir(path) != root {
			return errors.New("study geography is not owned by the frozen asset directory")
		}
		destination := filepath.Join(geography, name)
		if files := r.studyArtifactFiles(); files != nil {
			if err := files.renameAsset(path, destination); err != nil {
				return err
			}
		} else {
			if err := os.Rename(path, destination); err != nil {
				return err
			}
		}
	}
	cfg.H3TablePath = geography
	return nil
}

func studyAssetDigest(assets []studyFile) string {
	values := make([]studyFile, 0, len(assets))
	for _, a := range assets {
		values = append(values, studyFile{Path: filepath.ToSlash(filepath.Clean(a.Path)), SHA256: strings.ToLower(a.SHA256)})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Path < values[j].Path })
	return hashAnyStable(values)
}

// Licensing, CTY, geography and every other effective setting are part of the
// paired contract. LoadedFrom is only the source-directory label; source files
// have their own frozen digest. Capture before study-owned asset path rewrites.
func studyPipelineFingerprint(cfg *config.Config) (string, error) {
	if cfg == nil {
		return "", errors.New("study cannot fingerprint nil effective configuration")
	}
	effective := *cfg
	effective.LoadedFrom = ""
	effective.CallCorrection.BayesBonus = config.CallCorrectionBayesBonusConfig{}
	data, err := json.Marshal(effective)
	if err != nil {
		return "", fmt.Errorf("study effective configuration fingerprint: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
