// File role: Validate optional CCv2 study input, never server configuration.
// A caller supplies an externally frozen plan digest. Strict decoding, complete
// vectors, exact cell maps and bounded local inputs prevent silent experiment
// changes. The declarations record exposure; they cannot prove human blinding.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"dxcluster/config"
)

const (
	studySchemaVersion    = 2
	studyReferenceVersion = "ccv2-reference-v1"
	studyMaxRows          = 8_000_000
	studyMaxCSVBytes      = 512 << 20
	studyMaxJSONBytes     = 4 << 20
	studyDecisionSeconds  = 75
	studyEpisodeSeconds   = 195
)

type studyCLIRequest struct {
	Plan, Freeze, Arm, Candidate, Phase, Output, Compare, Date string
	DiagnosticSelection, DiagnosticFreeze                      string
}

type studyFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type studyInput struct {
	DateUTC     string    `json:"date_utc"`
	Phase       string    `json:"phase"`
	Archive     studyFile `json:"archive"`
	MaxRows     int       `json:"max_rows"`
	MaxCSVBytes uint64    `json:"max_csv_bytes"`
}

type studyArm struct {
	Name             string            `json:"name"`
	ExecutableSHA256 string            `json:"executable_sha256"`
	SourceRevision   string            `json:"source_revision"`
	ConfigDir        string            `json:"config_dir"`
	ConfigFiles      map[string]string `json:"config_files"`
	Assets           []studyFile       `json:"assets"`
}

type studyCandidate struct {
	ID            string           `json:"id"`
	Parameters    json.RawMessage  `json:"parameters"`
	ModeDistances map[string][]int `json:"mode_distances"`
}

type studyPlan struct {
	SchemaVersion              int                `json:"schema_version"`
	InputScenario              studyInputScenario `json:"input_scenario"`
	Frozen                     bool               `json:"frozen"`
	ReferenceVersion           string             `json:"reference_version"`
	ReferenceContractSHA256    string             `json:"reference_contract_sha256"`
	FrozenAtUTC                string             `json:"frozen_at_utc"`
	SelectedAtUTC              string             `json:"selected_at_utc"`
	AssetTrainingThroughUTC    string             `json:"asset_training_through_utc"`
	TuningThroughUTC           string             `json:"tuning_through_utc"`
	ReceiverMetadataThroughUTC string             `json:"receiver_metadata_through_utc"`
	ReceiverMetadata           studyFile          `json:"receiver_metadata"`
	HoldoutContentUninspected  bool               `json:"holdout_content_uninspected"`
	SelectedCandidate          string             `json:"selected_candidate"`
	DecisionCutoffSeconds      int                `json:"decision_cutoff_seconds"`
	EmbargoSeconds             int                `json:"embargo_seconds"`
	WarmupSeconds              int                `json:"warmup_seconds"`
	MaxExamples                int                `json:"max_examples"`
	Candidates                 []studyCandidate   `json:"candidates"`
	Arms                       []studyArm         `json:"arms"`
	Inputs                     []studyInput       `json:"inputs"`
}

func loadStudyPlan(path, frozenSHA string) (studyPlan, string, error) {
	var p studyPlan
	if !studyValidSHA(frozenSHA) {
		return p, "", errors.New("study requires -study-freeze SHA256 of the already frozen plan")
	}
	data, err := studyReadBounded(path, studyMaxJSONBytes)
	if err != nil {
		return p, "", err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if !strings.EqualFold(digest, frozenSHA) {
		return p, "", errors.New("study plan SHA256 differs from frozen digest")
	}
	if err := studyDecodeJSON(data, &p); err != nil {
		return p, "", fmt.Errorf("study plan: %w", err)
	}
	return p, digest, validateStudyPlan(p)
}

func validateStudyPlan(p studyPlan) error {
	if p.SchemaVersion != studySchemaVersion || !p.Frozen || !p.HoldoutContentUninspected || p.ReferenceVersion != studyReferenceVersion {
		return errors.New("study requires schema 2, frozen=true, uninspected holdout and ccv2-reference-v1")
	}
	if err := studyValidateInputScenario(p.InputScenario, p.Arms); err != nil {
		return err
	}
	if !strings.EqualFold(p.ReferenceContractSHA256, "879cc458ef9fa94f977051fb71ae0f07a72deede6728604fc7b18d556ed74626") {
		return errors.New("study reference contract fingerprint changed")
	}
	if p.DecisionCutoffSeconds != studyDecisionSeconds || p.EmbargoSeconds < studyEpisodeSeconds || p.EmbargoSeconds > 86400*365 {
		return errors.New("study requires fixed 75s cutoff and embargo >=195s (at most one year)")
	}
	if p.WarmupSeconds < 300 || p.WarmupSeconds > 86400 || p.MaxExamples < 1 || p.MaxExamples > 100 {
		return errors.New("study warmup must be 300..86400 seconds; max_examples must be 1..100")
	}
	frozen, err := time.Parse(time.RFC3339, p.FrozenAtUTC)
	if err != nil {
		return fmt.Errorf("frozen_at_utc: %w", err)
	}
	selected, err := time.Parse(time.RFC3339, p.SelectedAtUTC)
	if err != nil || selected.After(frozen) || frozen.After(time.Now().UTC()) {
		return errors.New("selected_at_utc must be a timestamp at or before freeze")
	}
	training, err := time.Parse(time.RFC3339, p.AssetTrainingThroughUTC)
	if err != nil {
		return fmt.Errorf("asset_training_through_utc: %w", err)
	}
	tuning, err := time.Parse(time.RFC3339, p.TuningThroughUTC)
	if err != nil {
		return fmt.Errorf("tuning_through_utc: %w", err)
	}
	metadata, err := time.Parse(time.RFC3339, p.ReceiverMetadataThroughUTC)
	if err != nil || !metadata.Before(time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)) {
		return errors.New("receiver metadata must be frozen strictly before 2026-11-15 UTC")
	}
	if !studyValidFile(p.ReceiverMetadata) || len(p.Candidates) < 1 || len(p.Candidates) > 16 {
		return errors.New("study requires hashed receiver metadata and 1..16 complete candidates")
	}
	candidateIDs := make(map[string]bool, len(p.Candidates))
	for _, c := range p.Candidates {
		if c.ID == "" || len(c.ID) > 64 || candidateIDs[c.ID] {
			return errors.New("study candidate IDs must be distinct nonempty strings of at most 64 bytes")
		}
		candidateIDs[c.ID] = true
		if err := studyValidateCandidate(c); err != nil {
			return fmt.Errorf("candidate %s: %w", c.ID, err)
		}
	}
	if !candidateIDs[p.SelectedCandidate] {
		return errors.New("selected_candidate must name one complete frozen candidate")
	}
	if err := studyValidateArms(p.Arms); err != nil {
		return err
	}
	if len(p.Inputs) < 1 || len(p.Inputs) > 32 {
		return errors.New("study requires 1..32 predeclared input days")
	}
	dates := make(map[string]bool, len(p.Inputs))
	for _, in := range p.Inputs {
		day, _, err := parseUTCDate(in.DateUTC)
		if err != nil || in.DateUTC != day.Format("2006-01-02") || dates[in.DateUTC] {
			return errors.New("study input dates must be distinct canonical UTC dates")
		}
		dates[in.DateUTC] = true
		if !studyValidFile(in.Archive) || in.MaxRows < 1 || in.MaxRows > studyMaxRows || in.MaxCSVBytes < 1 || in.MaxCSVBytes > studyMaxCSVBytes {
			return errors.New("study input requires SHA256, <=8000000 rows and <=512 MiB uncompressed CSV")
		}
		switch in.Phase {
		case "tuning":
			if day.Add(24*time.Hour - time.Second).After(tuning) {
				return errors.New("tuning input extends beyond frozen tuning exposure")
			}
		case "holdout":
			// TuningThroughUTC declares the last exposed raw/label instant. R2
			// never crosses a daily input boundary, so it adds no hidden next-day
			// label exposure. Historical untouched archives may be frozen later
			// than their event dates; blinding is a recorded owner declaration.
			exposure := tuning
			if training.After(exposure) {
				exposure = training
			}
			if !exposure.Before(day) || exposure.Add(time.Duration(p.EmbargoSeconds)*time.Second).After(day.Add(time.Duration(p.WarmupSeconds)*time.Second)) {
				return errors.New("holdout overlaps training/tuning labels or frozen selection embargo")
			}
		default:
			return errors.New("input phase must be tuning or holdout")
		}
	}
	return nil
}

func studyValidateArms(arms []studyArm) error {
	if len(arms) != 3 {
		return errors.New("study requires shipped, repaired_off and repaired_on arms")
	}
	seen := make(map[string]bool, 3)
	var repairedOff, repairedOn studyArm
	for _, a := range arms {
		if (a.Name != "shipped" && a.Name != "repaired_off" && a.Name != "repaired_on") || seen[a.Name] {
			return errors.New("invalid or duplicate study arm")
		}
		seen[a.Name] = true
		switch a.Name {
		case "repaired_off":
			repairedOff = a
		case "repaired_on":
			repairedOn = a
		}
		if !studyValidSHA(a.ExecutableSHA256) || len(a.SourceRevision) != 40 || a.ConfigDir == "" || len(a.ConfigFiles) < 1 || len(a.ConfigFiles) > 256 || len(a.Assets) > 64 {
			return errors.New("arm requires executable/source/config fingerprints and bounded asset manifest")
		}
		for name, sum := range a.ConfigFiles {
			if filepath.IsAbs(name) || strings.Contains(name, "..") || !studyValidSHA(sum) {
				return errors.New("invalid frozen configuration file entry")
			}
		}
		paths := make(map[string]bool, len(a.Assets))
		for _, asset := range a.Assets {
			key, err := filepath.Abs(asset.Path)
			if err != nil || !studyValidFile(asset) || paths[strings.ToLower(key)] {
				return errors.New("arm assets must have unique paths and SHA256")
			}
			paths[strings.ToLower(key)] = true
		}
	}
	if !strings.EqualFold(repairedOff.ExecutableSHA256, repairedOn.ExecutableSHA256) || repairedOff.SourceRevision != repairedOn.SourceRevision {
		return errors.New("study repaired OFF/ON arms must use identical executable and source revision")
	}
	if studyAssetDigest(repairedOff.Assets) != studyAssetDigest(repairedOn.Assets) {
		return errors.New("study repaired OFF/ON arms must use identical asset path/content bindings")
	}
	return nil
}

func studyValidateCandidate(c studyCandidate) error {
	var fields map[string]json.RawMessage
	if err := studyDecodeJSON(c.Parameters, &fields); err != nil || fields == nil {
		return errors.New("parameters must contain the complete BayesBonus Go JSON object")
	}
	// Reflect only to require every scalar in the complete vector. The typed
	// mode/distance mask is declared separately and is never inferred from it.
	typ := reflect.TypeFor[config.CallCorrectionBayesBonusConfig]()
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if name == "EnabledDistancesByMode" {
			continue
		}
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("missing complete parameter %s", name)
		}
		delete(fields, name)
	}
	if len(fields) != 0 {
		return errors.New("unknown parameter (cell map belongs in mode_distances)")
	}
	var typed config.CallCorrectionBayesBonusConfig
	if err := studyDecodeJSON(c.Parameters, &typed); err != nil || !typed.Enabled {
		return errors.New("complete candidate parameters require typed values and Enabled=true")
	}
	if err := studyValidateBayesScalarDomain(typed); err != nil {
		return err
	}
	if c.ModeDistances == nil {
		return errors.New("mode_distances must be an explicit complete map; {} means no cells")
	}
	for mode, distances := range c.ModeDistances {
		if mode != "CW" && mode != "RTTY" || distances == nil {
			return errors.New("mode_distances accepts CW/RTTY and non-null lists")
		}
		seen := [3]bool{}
		for _, d := range distances {
			if d < 1 || d > 2 || seen[d] {
				return errors.New("mode_distances accepts unique distances 1 and 2")
			}
			seen[d] = true
		}
	}
	return nil
}

func studyValidSHA(s string) bool {
	decoded, err := hex.DecodeString(s)
	return err == nil && len(decoded) == sha256.Size
}

func studyValidFile(f studyFile) bool {
	return strings.TrimSpace(f.Path) != "" && studyValidSHA(f.SHA256)
}

func studyReadBounded(path string, maxBytes int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(io.LimitReader(f, maxBytes+1))
	err = errors.Join(readErr, f.Close())
	if err == nil && int64(len(b)) > maxBytes {
		err = errors.New("study JSON exceeds byte bound")
	}
	return b, err
}

func studyHashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.Join(err, f.Close(), errors.New("study input must be a regular local file"))
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	err = errors.Join(err, f.Close())
	return hex.EncodeToString(h.Sum(nil)), err
}

func studyCheckFile(f studyFile) error {
	sum, err := studyHashFile(f.Path)
	if err != nil {
		return fmt.Errorf("study input %s: %w", f.Path, err)
	}
	if !strings.EqualFold(sum, f.SHA256) {
		return fmt.Errorf("study input SHA256 mismatch: %s", f.Path)
	}
	return nil
}

func studyDecodeJSON(data []byte, dst any) error {
	if err := studyJSONKeys(json.NewDecoder(bytes.NewReader(data)), 0); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("study JSON must contain exactly one value")
	}
	return nil
}

// Reject duplicate object keys rather than allowing JSON's last-value-wins
// behavior to alter the meaning of frozen optional configuration.
func studyJSONKeys(d *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("study JSON nesting exceeds 32")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	seen := make(map[string]bool)
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return errors.New("duplicate or invalid study JSON key")
			}
			seen[name] = true
		}
		if err := studyJSONKeys(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}

func studySortedCells(mask map[string][]int) []string {
	var cells []string
	for mode, ds := range mask {
		for _, d := range ds {
			cells = append(cells, fmt.Sprintf("%s/%d", mode, d))
		}
	}
	sort.Strings(cells)
	return cells
}
