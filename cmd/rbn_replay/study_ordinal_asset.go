// File role: Strict, hash-bound admission of the experimental ordinal asset.
// Fixed wire dimensions are checked before encoding/json can fill/discard array
// entries. This replay-only loader installs nothing in the live configuration.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"dxcluster/internal/correctionflow"
)

func studyLoadOrdinalModel(file studyFile) (*correctionflow.OrdinalModel, error) {
	if !studyValidFile(file) {
		return nil, errors.New("invalid ordinal model reference")
	}
	data, err := studyReadOrdinalAsset(file.Path)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != file.SHA256 {
		return nil, errors.New("ordinal model SHA mismatch")
	}
	var asset correctionflow.OrdinalAsset
	if err := studyDiagnosticDecode(data, diagnosticWireOrdinal, &asset); err != nil {
		return nil, err
	}
	return correctionflow.NewOrdinalModel(&asset)
}

func studyReadOrdinalAsset(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return nil, errors.Join(err, f.Close(), errors.New("ordinal model must be a regular file at most 4 MiB"))
	}
	data, readErr := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	err = errors.Join(readErr, f.Close())
	if err != nil {
		return nil, err
	}
	if len(data) > 4<<20 {
		return nil, errors.New("ordinal model exceeds 4 MiB")
	}
	return data, nil
}

func studyOrdinalWireFields(shape studyDiagnosticWire) []studyDiagnosticWireField {
	var fields []studyDiagnosticWireField
	add := func(kind studyDiagnosticWire, names ...string) {
		for _, name := range names {
			fields = append(fields, studyDiagnosticWireField{name, kind, true})
		}
	}
	switch shape {
	case diagnosticWireOrdinal:
		add(diagnosticWireInteger, "schema_version", "emission_prior_total", "insertion_prior_total", "gap_prior_alpha", "gap_prior_beta")
		add(diagnosticWireBool, "experimental")
		add(diagnosticWireText, "mode", "alphabet", "alignment", "reference_version")
		add(diagnosticWireOrdinalEdges, "snr_edges")
		add(diagnosticWireOrdinalSources, "sources")
		add(diagnosticWireOrdinalCells, "cells")
	case diagnosticWireOrdinalSource:
		add(diagnosticWireDate, "date_utc")
		add(diagnosticWireSHA, "sha256")
		add(diagnosticWireInteger, "focal_rows", "accepted_pairs", "fitted_cw_pairs")
	case diagnosticWireOrdinalCell:
		add(diagnosticWireInteger, "pairs", "exact_pairs", "matches", "substitutions", "deletions", "insertions", "source_characters", "subject_characters", "gap_stops")
		add(diagnosticWireOrdinalSymbols, "opportunities", "insertion_symbols")
		add(diagnosticWireOrdinalEvents, "events")
	}
	return fields
}

func studyOrdinalWireArray(shape studyDiagnosticWire) (int, studyDiagnosticWire, bool) {
	switch shape {
	case diagnosticWireOrdinalSources:
		return 2, diagnosticWireOrdinalSource, true
	case diagnosticWireOrdinalCells:
		return 6, diagnosticWireOrdinalCell, true
	case diagnosticWireOrdinalEdges:
		return 7, diagnosticWireInteger, true
	case diagnosticWireOrdinalSymbols:
		return 37, diagnosticWireInteger, true
	case diagnosticWireOrdinalEvents:
		return 37, diagnosticWireOrdinalOutcomes, true
	case diagnosticWireOrdinalOutcomes:
		return 38, diagnosticWireInteger, true
	}
	return 0, 0, false
}
