// File role: Admit finite diagnostic wire shapes before typed slice allocation.
// Operative objects have exact names and required non-null facts. Opaque
// metadata is token-skipped with <=128 active keys/object, <=1024 bytes/key
// and depth32; no metadata tree or population-sized generic map is built.
// Arrays of days/records/categories stop at 4/512/64 before typed growth.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

var errStudyDiagnosticCollectionBound = errors.New("diagnostic wire collection exceeds structural limit")

type studyDiagnosticWire uint8

const (
	diagnosticWireSelection studyDiagnosticWire = iota
	diagnosticWireDay
	diagnosticWireSelected
	diagnosticWireFile
	diagnosticWireAttempt
	diagnosticWireBinding
	diagnosticWireKey
	diagnosticWireDays
	diagnosticWireRecords
	diagnosticWireCategories
	diagnosticWireText
	diagnosticWireID
	diagnosticWireSHA
	diagnosticWireDate
	diagnosticWireBool
	diagnosticWireInteger
	diagnosticWireOpaque
)

type studyDiagnosticWireField struct {
	name     string
	shape    studyDiagnosticWire
	required bool
}

// Only the two input destinations are admitted; this is not a generic decoder
// or a custom-UnmarshalJSON callback seam. Preflight always precedes Decode.
func studyDiagnosticDecode(data []byte, shape studyDiagnosticWire, destination any) error {
	allowed := false
	switch target := destination.(type) {
	case *studyDiagnosticSelection:
		allowed = target != nil && shape == diagnosticWireSelection
	case *studyDiagnosticAttempt:
		allowed = target != nil && shape == diagnosticWireAttempt
	}
	if !allowed {
		return errors.New("unsupported diagnostic wire destination")
	}
	if err := studyDiagnosticWirePreflight(data, shape); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func studyDiagnosticWirePreflight(data []byte, shape studyDiagnosticWire) error {
	limit := studyMaxJSONBytes
	if shape == diagnosticWireAttempt {
		limit = studyAuditRowBytes - 1
	}
	if len(data) > limit || !utf8.Valid(data) {
		return errors.New("diagnostic wire input exceeds byte bound or contains invalid raw UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := studyDiagnosticWireValue(decoder, shape, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("diagnostic wire input must contain exactly one value")
	}
	return nil
}

func studyDiagnosticWireFields(shape studyDiagnosticWire) []studyDiagnosticWireField {
	switch shape {
	case diagnosticWireSelection:
		return []studyDiagnosticWireField{{"schema_version", diagnosticWireInteger, true}, {"exposed", diagnosticWireBool, true}, {"max_selected_per_date", diagnosticWireInteger, true}, {"days", diagnosticWireDays, true},
			{"added_strata", diagnosticWireOpaque, false}, {"added_stratum_rule", diagnosticWireOpaque, false}, {"authority", diagnosticWireOpaque, false}, {"categories", diagnosticWireOpaque, false}, {"claim_limit", diagnosticWireOpaque, false},
			{"frozen_at_utc", diagnosticWireOpaque, false}, {"mandatory_near_miss_receipts", diagnosticWireOpaque, false}, {"parent_selection", diagnosticWireOpaque, false}, {"per_category_lowest_sha256_limit", diagnosticWireOpaque, false}, {"seed", diagnosticWireOpaque, false}}
	case diagnosticWireDay:
		return []studyDiagnosticWireField{{"all_focal_rows", diagnosticWireInteger, true}, {"date_utc", diagnosticWireDate, true}, {"selected", diagnosticWireRecords, true}, {"selected_count", diagnosticWireInteger, true}, {"source_decisions", diagnosticWireFile, true}, {"eligible_control_counts", diagnosticWireOpaque, false}}
	case diagnosticWireSelected:
		return []studyDiagnosticWireField{{"categories", diagnosticWireCategories, true}, {"ordinal", diagnosticWireInteger, true}, {"raw_id", diagnosticWireID, true}, {"raw_sha256", diagnosticWireSHA, true}}
	case diagnosticWireFile:
		return []studyDiagnosticWireField{{"path", diagnosticWireText, true}, {"sha256", diagnosticWireSHA, true}}
	case diagnosticWireAttempt:
		return []studyDiagnosticWireField{{"id", diagnosticWireInteger, true}, {"binding", diagnosticWireBinding, true}, {"key", diagnosticWireKey, true}, {"observed_at", diagnosticWireText, true}, {"accepted", diagnosticWireBool, true}, {"reason", diagnosticWireText, true}}
	case diagnosticWireBinding:
		return []studyDiagnosticWireField{{"date_utc", diagnosticWireDate, true}, {"raw_id", diagnosticWireID, true}, {"ordinal", diagnosticWireInteger, true}, {"raw_sha256", diagnosticWireSHA, true}}
	case diagnosticWireKey:
		return []studyDiagnosticWireField{{"Band", diagnosticWireText, true}, {"Mode", diagnosticWireText, true}, {"Bucket", diagnosticWireInteger, true}, {"ToleranceHz", diagnosticWireInteger, true}}
	}
	return nil
}

func studyDiagnosticWireValue(decoder *json.Decoder, shape studyDiagnosticWire, depth int) error {
	if depth > 32 {
		return errors.New("diagnostic wire nesting exceeds 32")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if shape == diagnosticWireOpaque {
		return studyDiagnosticWireOpaque(decoder, token, depth)
	}
	if token == nil {
		return errors.New("diagnostic operative fact must not be null")
	}
	if fields := studyDiagnosticWireFields(shape); fields != nil {
		if token != json.Delim('{') {
			return errors.New("diagnostic operative object required")
		}
		var seen uint64
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || len(key) > 1024 {
				return errors.New("invalid diagnostic operative field name")
			}
			index := -1
			for i := range fields {
				if fields[i].name == key {
					index = i
					break
				}
			}
			if index < 0 || seen&(uint64(1)<<index) != 0 {
				return errors.New("unknown, case-aliased or duplicate diagnostic operative field")
			}
			seen |= uint64(1) << index
			if err := studyDiagnosticWireValue(decoder, fields[index].shape, depth+1); err != nil {
				return err
			}
		}
		if err := studyDiagnosticWireClose(decoder, '}'); err != nil {
			return err
		}
		for i := range fields {
			if fields[i].required && seen&(uint64(1)<<i) == 0 {
				return errors.New("required diagnostic operative field is absent")
			}
		}
		return nil
	}
	switch shape {
	case diagnosticWireDays, diagnosticWireRecords, diagnosticWireCategories:
		if token != json.Delim('[') {
			return errors.New("diagnostic operative array required")
		}
		limit, child := 4, diagnosticWireDay
		if shape == diagnosticWireRecords {
			limit, child = 512, diagnosticWireSelected
		}
		if shape == diagnosticWireCategories {
			limit, child = 64, diagnosticWireText
		}
		count := 0
		for decoder.More() {
			if count >= limit {
				return errStudyDiagnosticCollectionBound
			}
			count++
			if err := studyDiagnosticWireValue(decoder, child, depth+1); err != nil {
				return err
			}
		}
		return studyDiagnosticWireClose(decoder, ']')
	case diagnosticWireText, diagnosticWireID, diagnosticWireSHA, diagnosticWireDate:
		text, ok := token.(string)
		limit := 1024
		if shape == diagnosticWireID {
			limit = 96
		}
		if shape == diagnosticWireSHA {
			limit = 64
		}
		if shape == diagnosticWireDate {
			limit = 8
		}
		if !ok || len(text) > limit {
			return errors.New("invalid diagnostic operative string")
		}
	case diagnosticWireBool:
		if _, ok := token.(bool); !ok {
			return errors.New("diagnostic operative boolean required")
		}
	case diagnosticWireInteger:
		if _, ok := token.(json.Number); !ok {
			return errors.New("diagnostic operative number required")
		}
	default:
		return errors.New("unsupported diagnostic wire shape")
	}
	return nil
}

func studyDiagnosticWireClose(decoder *json.Decoder, closing byte) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim(closing) {
		return errors.New("invalid diagnostic object/array closing delimiter")
	}
	return nil
}

func studyDiagnosticWireOpaque(decoder *json.Decoder, token json.Token, depth int) error {
	switch token {
	case json.Delim('{'):
		var keys [128]string
		count := 0
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || len(key) > 1024 {
				return errors.New("invalid diagnostic metadata key")
			}
			if count >= len(keys) {
				return errStudyDiagnosticCollectionBound
			}
			for _, previous := range keys[:count] {
				if previous == key {
					return errors.New("duplicate diagnostic metadata key")
				}
			}
			keys[count] = key
			count++
			if err := studyDiagnosticWireValue(decoder, diagnosticWireOpaque, depth+1); err != nil {
				return err
			}
		}
		return studyDiagnosticWireClose(decoder, '}')
	case json.Delim('['):
		for decoder.More() {
			if err := studyDiagnosticWireValue(decoder, diagnosticWireOpaque, depth+1); err != nil {
				return err
			}
		}
		return studyDiagnosticWireClose(decoder, ']')
	case json.Delim('}'), json.Delim(']'):
		return errors.New("unexpected diagnostic metadata closing delimiter")
	}
	return nil
}
