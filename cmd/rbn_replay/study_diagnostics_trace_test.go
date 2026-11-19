package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
)

func diagnosticTraceFixture(t *testing.T) (*studyDiagnosticTraceWriter, studyDiagnosticRawBinding) {
	t.Helper()
	_, bindings := diagnosticFoundationDay()
	owner := diagnosticArtifactOwner(t, 64<<20, 128, studyDiagnosticArtifactFileOps())
	file := diagnosticArtifactOpen(t, owner, "trace.jsonl")
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	writer, err := newStudyDiagnosticTraceWriter(owner, file, bindings[:1])
	if err != nil {
		t.Fatal(err)
	}
	return writer, bindings[0]
}

func diagnosticTracePopulate(value reflect.Value, counter *int, empty bool) {
	if value.Type() == reflect.TypeFor[time.Time]() {
		value.Set(reflect.ValueOf(time.Date(2026, 11, 15, 1, 2, 3, 123456789+*counter, time.UTC)))
		*counter++
		return
	}
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if value.Field(i).CanSet() {
				diagnosticTracePopulate(value.Field(i), counter, empty)
			}
		}
	case reflect.Slice:
		length := 1
		if empty {
			length = 0
		}
		value.Set(reflect.MakeSlice(value.Type(), length, length))
		for i := 0; i < value.Len(); i++ {
			diagnosticTracePopulate(value.Index(i), counter, empty)
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			diagnosticTracePopulate(value.Index(i), counter, empty)
		}
	case reflect.String:
		value.SetString("field-" + strconv.Itoa(*counter))
		*counter++
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int, reflect.Int64:
		value.SetInt(int64(*counter))
		*counter++
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value.Kind() == reflect.Uint64 {
			value.SetUint((1 << 53) + uint64(*counter))
		} else {
			value.SetUint(uint64(*counter%200) + 1)
		}
		*counter++
	case reflect.Float64:
		value.SetFloat(float64(*counter) + 0.125)
		*counter++
	}
}

func diagnosticTraceJSON(t *testing.T, data []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func diagnosticTraceSet(t *testing.T, root *any, path string, value any) {
	t.Helper()
	if path == "" {
		*root = value
		return
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	parent := *root
	for _, part := range parts[:len(parts)-1] {
		switch node := parent.(type) {
		case map[string]any:
			parent = node[part]
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) {
				t.Fatal("invalid trace array path:", path)
			}
			parent = node[index]
		default:
			t.Fatal("trace path has no parent:", path)
		}
	}
	last := parts[len(parts)-1]
	switch node := parent.(type) {
	case map[string]any:
		node[last] = value
	case []any:
		index, err := strconv.Atoi(last)
		if err != nil || index < 0 || index >= len(node) {
			t.Fatal("invalid trace array path:", path)
		}
		node[index] = value
	default:
		t.Fatal("trace path has no final parent:", path)
	}
}

func diagnosticTraceReconstruct(t *testing.T, data []byte) any {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 64<<10)
	var root any
	rows, bytesSeen := 0, 0
	ended := false
	for scanner.Scan() {
		rows++
		bytesSeen += len(scanner.Bytes()) + 1
		row := diagnosticTraceJSON(t, scanner.Bytes()).(map[string]any)
		kind, path := row["kind"].(string), row["path"].(string)
		switch kind {
		case "begin":
			if rows != 1 {
				t.Fatal("begin marker reordered")
			}
		case "struct":
			object := map[string]any{}
			if scalars, ok := row["scalars"].(map[string]any); ok {
				for key, value := range scalars {
					object[key] = value
				}
			}
			diagnosticTraceSet(t, &root, path, object)
		case "slice", "array":
			length, err := strconv.Atoi(string(row["length"].(json.Number)))
			if err != nil {
				t.Fatal(err)
			}
			var value any = make([]any, length)
			if row["nil"].(bool) {
				value = nil
			}
			diagnosticTraceSet(t, &root, path, value)
		case "scalar":
			diagnosticTraceSet(t, &root, path, row["scalars"].(map[string]any)["value"])
		case "end":
			ended = true
			if row["rows"] != json.Number(strconv.Itoa(rows)) || row["bytes"] != json.Number(strconv.Itoa(bytesSeen)) {
				t.Fatal("end framing omitted actual row/newline bytes")
			}
		default:
			t.Fatal("unknown trace row", kind)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !ended || bytesSeen != len(data) || len(data) > 8<<20 {
		t.Fatal("incomplete or oversized logical trace")
	}
	return root
}

func TestDiagnosticTraceThreeRootRawJSONRoundtrip(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[correctionflow.ResolverSelectionDiagnostics](), reflect.TypeFor[correctionflow.TemporalReleaseDiagnostics](), reflect.TypeFor[correctionflow.TemporalEvaluateDiagnostics]()} {
		for _, shape := range []string{"zero_nil", "populated", "empty"} {
			t.Run(typ.Name()+"/"+shape, func(t *testing.T) {
				root := reflect.New(typ)
				if shape != "zero_nil" {
					counter := 1
					diagnosticTracePopulate(root.Elem(), &counter, shape == "empty")
				}
				writer, binding := diagnosticTraceFixture(t)
				if err := writer.Capture(binding, "literal_stage", root.Interface()); err != nil {
					t.Fatal(err)
				}
				if err := writer.file.Close(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(writer.owner.root, "trace.jsonl"))
				if err != nil {
					t.Fatal(err)
				}
				expected, err := json.Marshal(root.Interface())
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(diagnosticTraceReconstruct(t, data), diagnosticTraceJSON(t, expected)) {
					t.Fatal("raw JSON reconstruction lost fields, zero/nil/empty, time or integer precision")
				}
			})
		}
	}
}

func TestDiagnosticTraceDenseRulesAndFramedLimit(t *testing.T) {
	writer, binding := diagnosticTraceFixture(t)
	root := &correctionflow.ResolverSelectionDiagnostics{Rules: make([]correctionflow.DiagnosticRule, 5121)}
	root.Rules[5120].Inputs.Distance = 7
	if err := writer.Capture(binding, "dense", root); err != nil {
		t.Fatal("valid >5120 rules refused:", err)
	}
	if err := writer.file.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(writer.owner.root, "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(diagnosticTraceReconstruct(t, data), diagnosticTraceJSON(t, expected)) {
		t.Fatal("dense rule membership lost")
	}
	writer, binding = diagnosticTraceFixture(t)
	root.Rules = make([]correctionflow.DiagnosticRule, 53056)
	if err := writer.Capture(binding, "dense", root); err == nil || writer.owner.Err() == nil {
		t.Fatal("8MiB logical cap did not include actual flattened framing")
	}
	if err := writer.file.Close(); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(writer.owner.root, "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 8<<20 || bytes.Contains(data, []byte(`"kind":"end"`)) {
		t.Fatal("oversized partial capture claimed complete")
	}
}

// Size one flat row using the standard encoder, independently of preflight.
// At most 64 bounded scalar labels are constructed; no DTO is marshaled here.
func diagnosticTraceSizedRow(t *testing.T, base studyDiagnosticTraceRow, encodedBytes int) studyDiagnosticTraceRow {
	t.Helper()
	base.Kind, base.Path = "struct", "/wire_budget"
	base.Scalars = map[string]any{"padding": ""}
	for count := 0; count < 64; count++ {
		data, err := json.Marshal(base)
		if err != nil {
			t.Fatal(err)
		}
		padding := encodedBytes - len(data)
		if padding >= 0 && padding <= 1024 {
			base.Scalars["padding"] = strings.Repeat("x", padding)
			return base
		}
		base.Scalars["f"+strconv.Itoa(count)] = strings.Repeat("x", 1024)
	}
	t.Fatal("flat-row byte vector cannot be represented within scalar-label bounds")
	return base
}

func TestDiagnosticTraceLiteralPhysicalRowAndRecordBytes(t *testing.T) {
	for _, over := range []bool{false, true} {
		t.Run(map[bool]string{false: "row_exact", true: "row_one_over"}[over], func(t *testing.T) {
			writer, binding := diagnosticTraceFixture(t)
			walk := studyDiagnosticTraceWalk{writer: writer, row: studyDiagnosticTraceRow{SchemaVersion: 1, Binding: binding, Sequence: 1, Stage: "bytes", Root: "ResolverSelectionDiagnostics"}}
			target := 65535
			if over {
				target++
			}
			row := diagnosticTraceSizedRow(t, walk.row, target)
			data, err := json.Marshal(row)
			if err != nil || len(data) != target {
				t.Fatal("independent row sizing changed", err)
			}
			err = walk.write(row)
			if (err != nil) != over {
				t.Fatal("literal row/newline boundary changed:", err)
			}
			if err := writer.file.Close(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(writer.owner.root, "trace.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			want := int64(65536)
			if over {
				want = 0
			}
			if info.Size() != want {
				t.Fatal("oversized row wrote bytes or exact row was refused")
			}
		})
	}
	for _, over := range []bool{false, true} {
		t.Run(map[bool]string{false: "record_exact", true: "record_end_one_over"}[over], func(t *testing.T) {
			writer, binding := diagnosticTraceFixture(t)
			walk := studyDiagnosticTraceWalk{writer: writer, row: studyDiagnosticTraceRow{SchemaVersion: 1, Binding: binding, Sequence: 1, Stage: "bytes", Root: "ResolverSelectionDiagnostics"}}
			row := diagnosticTraceSizedRow(t, walk.row, 65535)
			// Include the real begin row and every later newline in the 8 MiB.
			if err := walk.emit("begin", "", nil, 0, false); err != nil {
				t.Fatal(err)
			}
			for range 126 {
				if err := walk.write(row); err != nil {
					t.Fatal(err)
				}
			}
			end := walk.row
			end.Kind, end.Rows, end.Bytes = "end", walk.rows+3, 8<<20
			endBytes, err := json.Marshal(end)
			if err != nil {
				t.Fatal(err)
			}
			remaining := (8 << 20) - walk.bytes - (len(endBytes) + 1)
			// Two bounded rows fill the remaining bytes before the end marker.
			first := remaining / 2
			second := remaining - first
			if over {
				second++
			}
			if err := walk.write(diagnosticTraceSizedRow(t, walk.row, first-1)); err != nil {
				t.Fatal(err)
			}
			if err := walk.write(diagnosticTraceSizedRow(t, walk.row, second-1)); err != nil {
				t.Fatal(err)
			}
			err = walk.end()
			if (err != nil) != over {
				t.Fatal("actual begin/end framing boundary changed:", err)
			}
			if err := writer.file.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(writer.owner.root, "trace.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			if over {
				if len(data) > 8<<20 || bytes.Contains(data, []byte(`"kind":"end"`)) {
					t.Fatal("over-budget end marker published")
				}
				return
			}
			if len(data) != 8<<20 {
				t.Fatalf("exact physical record bytes=%d", len(data))
			}
			last := bytes.LastIndexByte(data[:len(data)-1], '\n')
			marker := diagnosticTraceJSON(t, data[last+1:]).(map[string]any)
			if marker["bytes"] != json.Number("8388608") {
				t.Fatal("end marker omitted its own bytes")
			}
		})
	}
}

func TestDiagnosticTraceExactCollectionBounds(t *testing.T) {
	for _, variant := range []string{"receivers", "candidates_aliases", "temporal"} {
		t.Run(variant, func(t *testing.T) {
			writer, binding := diagnosticTraceFixture(t)
			var root any
			switch variant {
			case "receivers":
				root = &correctionflow.ResolverSelectionDiagnostics{CanonicalRows: make([]correctionflow.DiagnosticReceiver, 5120)}
			case "candidates_aliases":
				selector := &correctionflow.ResolverSelectionDiagnostics{MergedCandidates: make([]correctionflow.DiagnosticCandidate, 80), Nominations: make([]correctionflow.DiagnosticNomination, 80)}
				selector.MergedCandidates[0].ObservedCalls = make([]string, 80)
				root = selector
			case "temporal":
				capture := &correctionflow.TemporalEvaluateDiagnostics{Window: make([]correctionflow.DiagnosticTemporalEvent, 32), StartScores: make([]correctionflow.DiagnosticTemporalStartScore, 11)}
				capture.Window[0].Ranks = make([]correctionflow.DiagnosticTemporalRank, 80)
				capture.Window[0].Extras = make([]correctionflow.DiagnosticTemporalExtra, 3)
				capture.Window[0].Candidates = make([]string, 11)
				root = capture
			}
			if err := writer.Capture(binding, "exact", root); err != nil {
				t.Fatal("exact collection limit refused:", err)
			}
		})
	}
}

func TestDiagnosticTraceSelectedCountsAndStructuralBounds(t *testing.T) {
	writer, binding := diagnosticTraceFixture(t)
	for range 64 {
		if err := writer.Capture(binding, "selector", &correctionflow.ResolverSelectionDiagnostics{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Capture(binding, "selector", &correctionflow.ResolverSelectionDiagnostics{}); err == nil {
		t.Fatal("65th logical capture accepted")
	}
	for _, variant := range []string{"binding", "root", "nil", "rules", "receivers", "candidates", "aliases", "nominations", "events", "ranks", "event_candidates", "extras", "scores", "label"} {
		t.Run(variant, func(t *testing.T) {
			writer, binding := diagnosticTraceFixture(t)
			selector := &correctionflow.ResolverSelectionDiagnostics{}
			var root any = selector
			switch variant {
			case "binding":
				binding.RawSHA256 = strings.Repeat("c", 64)
			case "root":
				root = &struct{ Ignored bool }{true}
			case "nil":
				root = (*correctionflow.ResolverSelectionDiagnostics)(nil)
			case "rules":
				selector.Rules = make([]correctionflow.DiagnosticRule, 53057)
			case "receivers":
				selector.CanonicalRows = make([]correctionflow.DiagnosticReceiver, 5121)
			case "candidates":
				selector.MergedCandidates = make([]correctionflow.DiagnosticCandidate, 81)
			case "aliases":
				selector.MergedCandidates = []correctionflow.DiagnosticCandidate{{ObservedCalls: make([]string, 81)}}
			case "nominations":
				selector.Nominations = make([]correctionflow.DiagnosticNomination, 81)
			case "events":
				root = &correctionflow.TemporalEvaluateDiagnostics{Window: make([]correctionflow.DiagnosticTemporalEvent, 33)}
			case "ranks":
				root = &correctionflow.TemporalEvaluateDiagnostics{Window: []correctionflow.DiagnosticTemporalEvent{{Ranks: make([]correctionflow.DiagnosticTemporalRank, 81)}}}
			case "event_candidates":
				root = &correctionflow.TemporalEvaluateDiagnostics{Window: []correctionflow.DiagnosticTemporalEvent{{Candidates: make([]string, 12)}}}
			case "extras":
				root = &correctionflow.TemporalEvaluateDiagnostics{Window: []correctionflow.DiagnosticTemporalEvent{{Extras: make([]correctionflow.DiagnosticTemporalExtra, 4)}}}
			case "scores":
				root = &correctionflow.TemporalEvaluateDiagnostics{StartScores: make([]correctionflow.DiagnosticTemporalStartScore, 12)}
			case "label":
				selector.Anchor = strings.Repeat("é", 513)
			}
			if err := writer.Capture(binding, "selector", root); err == nil || writer.owner.Err() == nil {
				t.Fatal("invalid root/binding/type bound accepted")
			}
		})
	}
	walk := studyDiagnosticTraceWalk{nodes: 199999}
	if err := walk.node(); err != nil || walk.nodes != 200000 {
		t.Fatal("exact structural node bound refused")
	}
	if err := walk.node(); err == nil {
		t.Fatal("200001st structural node accepted")
	}
	if err := walk.visit(reflect.ValueOf(correctionflow.DiagnosticKey{}), "", 17); err == nil {
		t.Fatal("depth17 accepted")
	}
	for _, payload := range []any{[6]uint64{}, map[string]int{"a": 1}, new(correctionflow.DiagnosticKey), diagnosticJSONValueByte(1)} {
		walk.nodes = 0
		if err := walk.visit(reflect.ValueOf(payload), "", 0); err == nil {
			t.Fatal("unsupported nested payload accepted")
		}
	}
	writer, binding = diagnosticTraceFixture(t)
	walk = studyDiagnosticTraceWalk{writer: writer, row: studyDiagnosticTraceRow{SchemaVersion: 1, Binding: binding, Sequence: 1, Stage: "node_count", Root: "ResolverSelectionDiagnostics"}}
	if err := walk.visit(reflect.ValueOf(correctionflow.DiagnosticKey{}), "", 16); err != nil || walk.nodes != 1 {
		t.Fatal("scalar struct fields were counted as extra nodes or depth16 refused:", err)
	}
}
