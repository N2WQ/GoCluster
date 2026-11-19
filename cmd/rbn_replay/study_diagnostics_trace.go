// File role: Flatten only the three owned shared diagnostic roots after unlock.
// Each struct groups scalar fields; collections preserve nil, length and order.
// No rules, lookups, custom hooks or full-population encodings run here.
package main

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type studyDiagnosticTraceRow struct {
	SchemaVersion int                       `json:"schema_version"`
	Binding       studyDiagnosticRawBinding `json:"binding"`
	Sequence      int                       `json:"sequence"`
	Stage         string                    `json:"stage"`
	Root          string                    `json:"root"`
	Kind          string                    `json:"kind"`
	Path          string                    `json:"path"`
	Scalars       map[string]any            `json:"scalars,omitempty"`
	Length        int                       `json:"length"`
	Nil           bool                      `json:"nil"`
	Rows          int                       `json:"rows,omitempty"`
	Bytes         int                       `json:"bytes,omitempty"`
	Nodes         int                       `json:"nodes,omitempty"`
}

type studyDiagnosticTraceWriter struct {
	owner    *studyDiagnosticArtifacts
	file     *studyDiagnosticArtifactFile
	selected map[string]studyDiagnosticRawBinding
	counts   map[string]int
}

func newStudyDiagnosticTraceWriter(owner *studyDiagnosticArtifacts, file *studyDiagnosticArtifactFile, selected []studyDiagnosticRawBinding) (*studyDiagnosticTraceWriter, error) {
	if owner == nil {
		return nil, errors.New("diagnostic trace requires an artifact owner")
	}
	if file == nil || file.owner != owner || len(selected) > 512 {
		return nil, owner.latch(errors.New("diagnostic trace requires its owned file and <=512 selected bindings"))
	}
	w := &studyDiagnosticTraceWriter{owner: owner, file: file, selected: make(map[string]studyDiagnosticRawBinding, len(selected)), counts: make(map[string]int, len(selected))}
	for _, binding := range selected {
		if err := studyDiagnosticValidateRawID(binding.RawID, binding.Ordinal, binding.RawSHA256); err != nil {
			return nil, owner.latch(err)
		}
		if !studyDiagnosticExposedDate(binding.DateUTC) {
			return nil, owner.latch(errors.New("invalid selected diagnostic trace date"))
		}
		if _, duplicate := w.selected[binding.RawID]; duplicate {
			return nil, owner.latch(errors.New("duplicate selected trace binding"))
		}
		binding.DateUTC, binding.RawID, binding.RawSHA256 = strings.Clone(binding.DateUTC), strings.Clone(binding.RawID), strings.Clone(binding.RawSHA256)
		w.selected[binding.RawID] = binding
	}
	return w, nil
}

// Capture is synchronous. The caller retains its owned DTO; no reference to it
// survives return. A partial failed logical record is evidence, never success.
func (w *studyDiagnosticTraceWriter) Capture(binding studyDiagnosticRawBinding, stage string, root any) error {
	if w.owner.Err() != nil {
		return w.owner.Err()
	}
	selected, ok := w.selected[binding.RawID]
	if !ok || selected != binding {
		return w.owner.latch(errors.New("diagnostic trace binding is not the selected immutable binding"))
	}
	if w.counts[binding.RawID] >= 64 {
		return w.owner.latch(errors.New("diagnostic selected capture limit exceeds 64"))
	}
	if len(stage) > 1024 || !utf8.ValidString(stage) || strings.TrimSpace(stage) == "" {
		return w.owner.latch(errors.New("diagnostic trace requires a stage"))
	}
	value := reflect.ValueOf(root)
	name := ""
	switch capture := root.(type) {
	case *correctionflow.ResolverSelectionDiagnostics:
		if capture != nil {
			name = "ResolverSelectionDiagnostics"
		}
	case *correctionflow.TemporalReleaseDiagnostics:
		if capture != nil {
			name = "TemporalReleaseDiagnostics"
		}
	case *correctionflow.TemporalEvaluateDiagnostics:
		if capture != nil {
			name = "TemporalEvaluateDiagnostics"
		}
	}
	if name == "" {
		return w.owner.latch(errors.New("diagnostic trace requires one of three nonnil typed roots"))
	}
	w.counts[binding.RawID]++
	walk := studyDiagnosticTraceWalk{writer: w, row: studyDiagnosticTraceRow{SchemaVersion: 1, Binding: selected, Sequence: w.counts[binding.RawID], Stage: stage, Root: name}}
	if err := walk.emit("begin", "", nil, 0, false); err != nil {
		return err
	}
	if err := walk.visit(value.Elem(), "", 0); err != nil {
		return w.owner.latch(err)
	}
	return walk.end()
}

type studyDiagnosticTraceWalk struct {
	writer             *studyDiagnosticTraceWriter
	row                studyDiagnosticTraceRow
	bytes, rows, nodes int
}

func (w *studyDiagnosticTraceWalk) emit(kind, path string, scalars map[string]any, length int, nilValue bool) error {
	row := w.row
	row.Kind, row.Path, row.Scalars, row.Length, row.Nil = kind, path, scalars, length, nilValue
	return w.write(row)
}

func (w *studyDiagnosticTraceWalk) write(row studyDiagnosticTraceRow) error {
	data, err := w.writer.owner.jsonBytes(row, studyAuditRowBytes-1)
	if err != nil {
		return err
	}
	if len(data)+1 > studyAuditRecordBytes-w.bytes {
		return w.writer.owner.latch(errors.New("diagnostic framed logical capture exceeds 8 MiB"))
	}
	data = append(data, '\n')
	if _, err := w.writer.file.Write(data); err != nil {
		return err
	}
	w.bytes += len(data)
	w.rows++
	return nil
}

func (w *studyDiagnosticTraceWalk) end() error {
	row := w.row
	row.Kind, row.Rows, row.Nodes = "end", w.rows+1, w.nodes
	// Bytes includes the end row itself. Its decimal width reaches a fixed
	// point within the finite 8 MiB range; no whole-record encoding is needed.
	for range 8 {
		data, err := w.writer.owner.jsonBytes(row, studyAuditRowBytes-1)
		if err != nil {
			return err
		}
		total := w.bytes + len(data) + 1
		if total == row.Bytes {
			return w.write(row)
		}
		row.Bytes = total
	}
	return w.writer.owner.latch(errors.New("diagnostic capture framing did not converge"))
}

func (w *studyDiagnosticTraceWalk) node() error {
	if w.nodes >= 200000 {
		return errors.New("diagnostic trace exceeds 200000 structural nodes")
	}
	w.nodes++
	return nil
}

func (w *studyDiagnosticTraceWalk) visit(value reflect.Value, path string, depth int) error {
	if depth > 16 {
		return errors.New("diagnostic trace exceeds depth 16")
	}
	if err := w.node(); err != nil {
		return err
	}
	if scalar, ok := studyDiagnosticTraceScalar(value); ok {
		return w.emit("scalar", path, map[string]any{"value": scalar}, 0, false)
	}
	switch value.Kind() {
	case reflect.Struct:
		if !studyDiagnosticTraceStruct(value.Type()) || value.NumField() > 64 {
			return errors.New("unsupported diagnostic trace struct")
		}
		scalars := make(map[string]any)
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			if scalar, ok := studyDiagnosticTraceScalar(value.Field(i)); ok {
				scalars[field.Name] = scalar
			}
		}
		if err := w.emit("struct", path, scalars, 0, false); err != nil {
			return err
		}
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			if _, ok := studyDiagnosticTraceScalar(value.Field(i)); ok {
				continue
			}
			if err := w.visit(value.Field(i), path+"/"+field.Name, depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		limit := studyDiagnosticTraceCollectionLimit(value.Type(), path)
		if limit < 0 || value.Len() > limit {
			return errors.New("diagnostic trace collection exceeds its field/type limit")
		}
		kind, isNil := "array", false
		if value.Kind() == reflect.Slice {
			kind, isNil = "slice", value.IsNil()
		}
		if err := w.emit(kind, path, nil, value.Len(), isNil); err != nil {
			return err
		}
		for i := 0; i < value.Len(); i++ {
			if err := w.visit(value.Index(i), path+"/"+strconv.Itoa(i), depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("diagnostic trace rejects pointer/map/interface/custom payloads")
	}
}

func studyDiagnosticTraceScalar(value reflect.Value) (any, bool) {
	if value.Type() == reflect.TypeFor[time.Time]() {
		return value.Interface(), true
	}
	if studyDiagnosticJSONCustomMarshaler(value.Type()) {
		return nil, false
	}
	switch value.Kind() {
	case reflect.String:
		return value.String(), true
	case reflect.Bool:
		return value.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint(), true
	case reflect.Float32:
		return float32(value.Float()), true
	case reflect.Float64:
		return value.Float(), true
	}
	return nil, false
}

func studyDiagnosticTraceStruct(typ reflect.Type) bool {
	switch typ {
	case reflect.TypeFor[correctionflow.ResolverSelectionDiagnostics](), reflect.TypeFor[correctionflow.TemporalReleaseDiagnostics](), reflect.TypeFor[correctionflow.TemporalEvaluateDiagnostics](),
		reflect.TypeFor[correctionflow.DiagnosticKey](), reflect.TypeFor[correctionflow.DiagnosticCandidate](), reflect.TypeFor[correctionflow.DiagnosticReceiver](),
		reflect.TypeFor[correctionflow.DiagnosticSnapshot](), reflect.TypeFor[correctionflow.DiagnosticSelection](), reflect.TypeFor[correctionflow.DiagnosticLookup](),
		reflect.TypeFor[correctionflow.DiagnosticNomination](), reflect.TypeFor[correctionflow.DiagnosticRule](), reflect.TypeFor[correctionflow.DiagnosticGenerationCheck](),
		reflect.TypeFor[correctionflow.DiagnosticTransform](), reflect.TypeFor[correctionflow.DiagnosticTemporalRank](), reflect.TypeFor[correctionflow.DiagnosticTemporalExtra](),
		reflect.TypeFor[correctionflow.DiagnosticTemporalEvent](), reflect.TypeFor[correctionflow.DiagnosticTemporalStartScore](), reflect.TypeFor[spot.ResolverAmbiguityInput]():
		return true
	}
	return false
}

func studyDiagnosticTraceCollectionLimit(typ reflect.Type, path string) int {
	if typ.Kind() == reflect.Array {
		if typ.Len() == 5 {
			return 5
		}
		return -1
	}
	switch typ.Elem() {
	case reflect.TypeFor[correctionflow.DiagnosticRule]():
		return 53056
	case reflect.TypeFor[correctionflow.DiagnosticReceiver]():
		return 5120
	case reflect.TypeFor[correctionflow.DiagnosticCandidate](), reflect.TypeFor[correctionflow.DiagnosticNomination](), reflect.TypeFor[correctionflow.DiagnosticTemporalRank]():
		return 80
	case reflect.TypeFor[correctionflow.DiagnosticTemporalEvent]():
		return 32
	case reflect.TypeFor[correctionflow.DiagnosticTemporalExtra]():
		return 3
	case reflect.TypeFor[correctionflow.DiagnosticTemporalStartScore]():
		return 11
	case reflect.TypeFor[string]():
		if strings.HasSuffix(path, "/Candidates") {
			return 11
		}
		return 80
	}
	return -1
}
