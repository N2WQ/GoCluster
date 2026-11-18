// File role: Admit an externally frozen exposed selection, never model inputs.
// Pure parsing owns at most 4 MiB; file integrity checks stay in the loader.
// Research metadata remains opaque. Raw bindings refer to the caller-owned day.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const studyDiagnosticSelectedLimit = 512

type studyDiagnosticSelection struct {
	SchemaVersion                int                           `json:"schema_version"`
	Exposed                      bool                          `json:"exposed"`
	MaxSelectedPerDate           int                           `json:"max_selected_per_date"`
	Days                         []studyDiagnosticSelectionDay `json:"days"`
	AddedStrata                  json.RawMessage               `json:"added_strata"`
	AddedStratumRule             json.RawMessage               `json:"added_stratum_rule"`
	Authority                    json.RawMessage               `json:"authority"`
	Categories                   json.RawMessage               `json:"categories"`
	ClaimLimit                   json.RawMessage               `json:"claim_limit"`
	FrozenAtUTC                  json.RawMessage               `json:"frozen_at_utc"`
	MandatoryNearMissReceipts    json.RawMessage               `json:"mandatory_near_miss_receipts"`
	ParentSelection              json.RawMessage               `json:"parent_selection"`
	PerCategoryLowestSHA256Limit json.RawMessage               `json:"per_category_lowest_sha256_limit"`
	Seed                         json.RawMessage               `json:"seed"`
}

type studyDiagnosticSelectionDay struct {
	AllFocalRows          int                             `json:"all_focal_rows"`
	DateUTC               string                          `json:"date_utc"`
	Selected              []studyDiagnosticSelectedRecord `json:"selected"`
	SelectedCount         int                             `json:"selected_count"`
	SourceDecisions       studyFile                       `json:"source_decisions"`
	EligibleControlCounts json.RawMessage                 `json:"eligible_control_counts"`
}

type studyDiagnosticSelectedRecord struct {
	Categories []string `json:"categories"`
	Ordinal    int64    `json:"ordinal"`
	RawID      string   `json:"raw_id"`
	RawSHA256  string   `json:"raw_sha256"`
}

type studyDiagnosticRawBinding struct {
	DateUTC   string `json:"date_utc"`
	RawID     string `json:"raw_id"`
	Ordinal   int64  `json:"ordinal"`
	RawSHA256 string `json:"raw_sha256"`
}

func studyDiagnosticCaseDate(studyDate string) (string, error) {
	at, err := time.Parse("2006-01-02", studyDate)
	if err != nil || at.Format("2006-01-02") != studyDate || !studyDiagnosticExposedDate(at.Format("20060102")) {
		return "", errors.New("diagnostics require an exposed November 7/14/15/16 study date in YYYY-MM-DD form")
	}
	return at.Format("20060102"), nil
}

func studyDiagnosticExposedDate(date string) bool {
	switch date {
	case "20261107", "20261114", "20261115", "20261116":
		return true
	}
	return false
}

// Entry validation is independent of CLI routing; integration must call it
// before creating an output or opening archive content.
func studyDiagnosticValidateEntry(path, frozenSHA, date string) error {
	if path == "" || frozenSHA == "" {
		return errors.New("diagnostic selection and frozen SHA256 must be supplied together")
	}
	if err := studyAuditLocalPath(path); err != nil {
		return err
	}
	if !studyValidSHA(frozenSHA) {
		return errors.New("diagnostic selection requires an external SHA256")
	}
	_, err := studyDiagnosticCaseDate(date)
	return err
}

func loadStudyDiagnosticSelection(path, frozenSHA string) (studyDiagnosticSelection, string, error) {
	var empty studyDiagnosticSelection
	if err := studyAuditLocalPath(path); err != nil {
		return empty, "", err
	}
	if !studyValidSHA(frozenSHA) {
		return empty, "", errors.New("diagnostic selection requires an external SHA256")
	}
	data, err := studyReadBounded(path, studyMaxJSONBytes)
	if err != nil {
		return empty, "", err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if !strings.EqualFold(digest, frozenSHA) {
		return empty, "", errors.New("diagnostic selection differs from its frozen SHA256")
	}
	selection, err := parseStudyDiagnosticSelection(data)
	if err != nil {
		return empty, "", err
	}
	for i := range selection.Days {
		if err := studyCheckFile(selection.Days[i].SourceDecisions); err != nil {
			return empty, "", err
		}
	}
	return selection, digest, nil
}

func parseStudyDiagnosticSelection(data []byte) (studyDiagnosticSelection, error) {
	var selection studyDiagnosticSelection
	if len(data) > studyMaxJSONBytes {
		return selection, errors.New("diagnostic selection exceeds 4 MiB")
	}
	if err := studyDiagnosticDecode(data, diagnosticWireSelection, &selection); err != nil {
		return selection, err
	}
	if selection.SchemaVersion != 2 || !selection.Exposed || selection.MaxSelectedPerDate != 512 || len(selection.Days) != 4 {
		return selection, errors.New("diagnostics require schema 2, exposed=true, 512/date and all four exposed dates")
	}
	dates := make(map[string]bool, 4)
	ids := make(map[string]bool)
	for i := range selection.Days {
		day := &selection.Days[i]
		if !studyDiagnosticExposedDate(day.DateUTC) || dates[day.DateUTC] || day.AllFocalRows < 1 || day.AllFocalRows > studyMaxRows || day.Selected == nil || day.SelectedCount != len(day.Selected) || len(day.Selected) > studyDiagnosticSelectedLimit {
			return selection, errors.New("invalid diagnostic day, selected count or duplicate date")
		}
		dates[day.DateUTC] = true
		if err := studyAuditFile(day.SourceDecisions); err != nil {
			return selection, err
		}
		ordinals := make(map[int64]bool, len(day.Selected))
		for j := range day.Selected {
			record := &day.Selected[j]
			if err := studyDiagnosticValidateRawID(record.RawID, record.Ordinal, record.RawSHA256); err != nil {
				return selection, err
			}
			if ids[record.RawID] || ordinals[record.Ordinal] || len(record.Categories) == 0 || len(record.Categories) > 64 {
				return selection, errors.New("duplicate diagnostic ID/ordinal or invalid categories")
			}
			ids[record.RawID], ordinals[record.Ordinal] = true, true
			categories := make(map[string]bool, len(record.Categories))
			for _, category := range record.Categories {
				if strings.TrimSpace(category) == "" || len(category) > 1024 || !utf8.ValidString(category) || categories[category] {
					return selection, errors.New("invalid diagnostic category label")
				}
				categories[category] = true
			}
		}
	}
	return selection, nil
}

func studyDiagnosticValidateRawID(id string, ordinal int64, rawSHA string) error {
	if ordinal < 1 || ordinal > studyMaxRows || !studyValidSHA(rawSHA) || len(id) > 96 {
		return errors.New("invalid diagnostic raw binding")
	}
	prefix, suffix, ok := strings.Cut(id, ":")
	if !ok || !studyValidSHA(prefix) || prefix != strings.ToLower(prefix) || suffix != strconv.FormatInt(ordinal, 10) {
		return errors.New("diagnostic raw ID must contain the exact archive SHA256 and ordinal")
	}
	return nil
}

func (s *studyDiagnosticSelection) bindDay(input studyInput, day *studyRawDay) ([]studyDiagnosticRawBinding, error) {
	if s == nil {
		return nil, errors.New("diagnostics require a loaded selection")
	}
	date, err := studyDiagnosticCaseDate(input.DateUTC)
	if err != nil {
		return nil, err
	}
	if day == nil || !studyValidSHA(input.Archive.SHA256) || !strings.EqualFold(input.Archive.SHA256, day.InputSHA256) || day.Start.Format("20060102") != date || len(day.Rows) > studyMaxRows {
		return nil, errors.New("diagnostic study input and loaded archive day differ")
	}
	for i := range s.Days {
		selected := &s.Days[i]
		if selected.DateUTC != date {
			continue
		}
		focalRows := 0
		for j := range day.Rows {
			if studyFocalRow(day.Rows[j]) {
				focalRows++
			}
		}
		if focalRows != selected.AllFocalRows {
			return nil, errors.New("diagnostic selected day focal count differs from archive")
		}
		out := make([]studyDiagnosticRawBinding, 0, len(selected.Selected))
		for j := range selected.Selected {
			record := &selected.Selected[j]
			binding := studyDiagnosticRawBinding{DateUTC: date, RawID: record.RawID, Ordinal: record.Ordinal, RawSHA256: record.RawSHA256}
			if err := studyDiagnosticCheckRawBinding(day, binding); err != nil {
				return nil, err
			}
			out = append(out, binding)
		}
		return out, nil
	}
	return nil, errors.New("diagnostic selection is missing the requested archive date")
}

func studyDiagnosticCheckRawBinding(day *studyRawDay, binding studyDiagnosticRawBinding) error {
	if err := studyDiagnosticValidateRawID(binding.RawID, binding.Ordinal, binding.RawSHA256); err != nil {
		return err
	}
	if day == nil || binding.Ordinal > int64(len(day.Rows)) || binding.DateUTC != day.Start.Format("20060102") || binding.RawID != fmt.Sprintf("%s:%d", strings.ToLower(day.InputSHA256), binding.Ordinal) {
		return errors.New("diagnostic raw binding differs from archive/date/ordinal")
	}
	raw := &day.Rows[binding.Ordinal-1]
	if raw.Ordinal != binding.Ordinal || !strings.EqualFold(binding.RawSHA256, hashAnyStable(raw.Row)) || !studyFocalRow(*raw) {
		return errors.New("diagnostic raw binding differs from actual archive row or focal eligibility")
	}
	return nil
}
