// File role: Isolate exposed diagnostic CLI requests and qualification inputs.
// Validation is pure and runs before study input/output ownership. The report
// marker scan owns only bounded decoder scratch, never a decoded metadata tree.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

var errStudyDiagnosticReportMarker = errors.New("study comparison refuses diagnostic reports")
var errStudyExperimentalReportMarker = errors.New("experimental method report is not a legacy activation input")

// Main must separately retain flag presence: an explicitly supplied empty pair
// is an invalid diagnostic request, while an absent pair preserves ordinary use.
func studyDiagnosticValidateRequest(req studyCLIRequest) error {
	if req.DiagnosticSelection == "" && req.DiagnosticFreeze == "" {
		return nil
	}
	if req.Compare != "" {
		return errors.New("diagnostic capture cannot be used for study comparison")
	}
	return studyDiagnosticValidateEntry(req.DiagnosticSelection, req.DiagnosticFreeze, req.Date)
}

// Inspect only unparsed CLI arguments. Ordinary flag values and positional
// policy remain unchanged unless an actual diagnostic flag name is present.
func studyDiagnosticCLIFlag(argument string) bool {
	if !strings.HasPrefix(argument, "-") {
		return false
	}
	name := strings.TrimPrefix(strings.TrimPrefix(argument, "-"), "-")
	name, _, _ = strings.Cut(name, "=")
	return name == "study-diagnostic-selection" || name == "study-diagnostic-freeze"
}

// A top-level marker is disqualifying by presence, before its value is read.
// In particular null, false, malformed or future diagnostic manifest values
// must never be restored into an ordinary report by typed decoding defaults.
func studyDiagnosticRejectReportMarker(data []byte) error {
	if len(data) > studyMaxJSONBytes {
		return errors.New("study report marker scan exceeds byte bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("study report must be a JSON object")
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("invalid study report field name")
		}
		if strings.EqualFold(key, "diagnostics") {
			return errStudyDiagnosticReportMarker
		}
		if strings.EqualFold(key, "experimental") || strings.EqualFold(key, "ordinary_guard") {
			return errStudyExperimentalReportMarker
		}
		if err := studyDiagnosticSkipReportValue(decoder, 1); err != nil {
			return err
		}
	}
	if err := studyDiagnosticWireClose(decoder, '}'); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("study report must contain exactly one JSON value")
	}
	return nil
}

// Match the existing study JSON depth limit without imposing the diagnostic
// selection metadata's key-count/label rules on ordinary report fields. The
// caller's 4 MiB limit bounds token bytes; recursion retains no object key map.
func studyDiagnosticSkipReportValue(decoder *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("study report marker scan nesting exceeds 32")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	if delimiter != '{' && delimiter != '[' {
		return errors.New("unexpected study report closing delimiter")
	}
	for decoder.More() {
		if delimiter == '{' {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			if _, ok := key.(string); !ok {
				return errors.New("invalid nested study report field name")
			}
		}
		if err := studyDiagnosticSkipReportValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing := byte(']')
	if delimiter == '{' {
		closing = '}'
	}
	return studyDiagnosticWireClose(decoder, closing)
}
