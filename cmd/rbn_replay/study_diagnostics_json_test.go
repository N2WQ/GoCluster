package main

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Intentional malformed/ambiguous tags are finite test data, not static source
// declarations. One scalar field keeps the reflective construction bounded.
func diagnosticJSONTaggedValue(t *testing.T, name, tag string, value any) any {
	t.Helper()
	if name != "Value" && name != "LongFieldName" {
		t.Fatal("unsupported diagnostic JSON fixture field name")
	}
	switch tag {
	case `json:"-,omitempty"`, `json:"value,"`, `json:"\\bad"`, `json:"bad\\name"`, `json:"value,future"`:
	default:
		t.Fatal("unsupported diagnostic JSON fixture tag")
	}
	typ := reflect.TypeOf(value)
	if typ == nil || (typ.Kind() != reflect.Int && typ.Kind() != reflect.Uint8) {
		t.Fatal("diagnostic JSON tag fixture requires a scalar")
	}
	result := reflect.New(reflect.StructOf([]reflect.StructField{{Name: name, Type: typ, Tag: reflect.StructTag(tag)}})).Elem()
	result.Field(0).Set(reflect.ValueOf(value))
	return result.Interface()
}

func TestDiagnosticArtifactJSONLiteralSizing(t *testing.T) {
	for _, fixture := range []struct {
		name    string
		value   any
		encoded string
	}{
		{"nil", nil, `null`}, {"nil_slice", []int(nil), `null`}, {"empty_slice", []int{}, `[]`}, {"nil_map", map[string]int(nil), `null`}, {"empty_map", map[string]int{}, `{}`},
		{"true", true, `true`}, {"false", false, `false`}, {"minimum_int", int64(math.MinInt64), `-9223372036854775808`}, {"maximum_uint", uint64(math.MaxUint64), `18446744073709551615`},
		{"small_float", float64(0.000001), `0.000001`}, {"exponent_float", float64(0.0000001), `1e-7`}, {"negative_zero", math.Copysign(0, -1), `-0`},
		{"escaping", "\x00<&>\u2028é\n", `"\u0000\u003c\u0026\u003e\u2028é\n"`}, {"slash_quote", "\\\"", `"\\\""`},
		{"map", map[string][]int{"a": {1, 2}}, `{"a":[1,2]}`}, {"bytes", []byte{0, 1, 2}, `"AAEC"`}, {"time", time.Date(2026, 11, 7, 12, 34, 56, 0, time.UTC), `"2026-11-07T12:34:56Z"`},
		{"dash_name_with_options", diagnosticJSONTaggedValue(t, "Value", `json:"-,omitempty"`, 7), `{"-":7}`},
		{"empty_option", diagnosticJSONTaggedValue(t, "Value", `json:"value,"`, 7), `{"value":7}`},
		{"omitted", struct {
			Count int    `json:"count"`
			Skip  string `json:"skip,omitempty"`
		}{Count: 7}, `{"count":7}`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			actual, err := json.Marshal(fixture.value)
			if err != nil || string(actual) != fixture.encoded {
				t.Fatalf("literal standard-library JSON vector: %q %v", actual, err)
			}
			if err := studyDiagnosticJSONPreflight(reflect.ValueOf(fixture.value), len(fixture.encoded)); err != nil {
				t.Fatalf("exact preflight refused literal byte length: %v", err)
			}
			if err := studyDiagnosticJSONPreflight(reflect.ValueOf(fixture.value), len(fixture.encoded)-1); err == nil {
				t.Fatal("one-byte-under preflight accepted encoding")
			}
		})
	}
}

type diagnosticJSONCycle struct{ Next *diagnosticJSONCycle }

type diagnosticJSONValueByte uint8

func (diagnosticJSONValueByte) MarshalJSON() ([]byte, error) {
	panic("diagnostic JSON value byte marshaler must never run")
}

type diagnosticJSONPointerByte uint8

func (*diagnosticJSONPointerByte) MarshalJSON() ([]byte, error) {
	panic("diagnostic JSON pointer byte marshaler must never run")
}

type diagnosticTextValueByte uint8

func (diagnosticTextValueByte) MarshalText() ([]byte, error) {
	panic("diagnostic text value byte marshaler must never run")
}

type diagnosticTextPointerByte uint8

func (*diagnosticTextPointerByte) MarshalText() ([]byte, error) {
	panic("diagnostic text pointer byte marshaler must never run")
}

type diagnosticAppendTextValueByte uint8

func (diagnosticAppendTextValueByte) AppendText([]byte) ([]byte, error) {
	panic("diagnostic text value byte appender must never run")
}

type diagnosticAppendTextPointerByte uint8

func (*diagnosticAppendTextPointerByte) AppendText([]byte) ([]byte, error) {
	panic("diagnostic text pointer byte appender must never run")
}

// Even an incompatible signature is deliberately outside the closed subset.
type diagnosticJSONToNameValueByte uint8

func (diagnosticJSONToNameValueByte) MarshalJSONTo() {
	panic("diagnostic JSONTo value method must never run")
}

type diagnosticJSONToNamePointerByte uint8

func (*diagnosticJSONToNamePointerByte) MarshalJSONTo() {
	panic("diagnostic JSONTo pointer method must never run")
}

type diagnosticJSONZeroValue int

func (diagnosticJSONZeroValue) IsZero() bool {
	panic("diagnostic value IsZero must never run")
}

type diagnosticJSONZeroPointer int

func (*diagnosticJSONZeroPointer) IsZero() bool {
	panic("diagnostic pointer IsZero must never run")
}

type diagnosticJSONEmbedded struct{ Value diagnosticJSONValueByte }

func TestDiagnosticArtifactJSONFieldBoundaries(t *testing.T) {
	fallback := diagnosticJSONTaggedValue(t, "LongFieldName", `json:"\\bad"`, 7)
	prefixed := diagnosticJSONTaggedValue(t, "Value", `json:"bad\\name"`, diagnosticJSONValueByte(1))
	// Go 1.27's default v2 backend consumes the identifier prefix of bad\name.
	// A leading backslash is invalid before any name is accepted by either
	// backend. Prove the quoted source tags decode to those intended bytes.
	for _, fixture := range []struct {
		value        any
		raw, decoded string
	}{
		{fallback, `json:"\\bad"`, `\bad`},
		{prefixed, `json:"bad\\name"`, `bad\name`},
	} {
		tag := reflect.TypeOf(fixture.value).Field(0).Tag
		if string(tag) != fixture.raw || tag.Get("json") != fixture.decoded {
			t.Fatalf("invalid-name fixture tag bytes: raw=%x decoded=%x", string(tag), tag.Get("json"))
		}
		t.Logf("invalid-name fixture tag bytes: raw=%x decoded=%x", string(tag), tag.Get("json"))
	}
	encoded, err := json.Marshal(fallback)
	if err != nil || string(encoded) != `{"LongFieldName":7}` {
		t.Fatal("standard-library invalid tag fallback vector changed:", string(encoded), err)
	}
	for _, fixture := range []struct {
		name  string
		value any
		cause string
	}{
		{"dash_with_options", diagnosticJSONTaggedValue(t, "Value", `json:"-,omitempty"`, diagnosticJSONValueByte(1)), "custom marshaler"},
		{"invalid_tag", diagnosticJSONTaggedValue(t, "Value", `json:"\\bad"`, diagnosticJSONValueByte(1)), "invalid tag name"},
		{"invalid_tag_prefix", prefixed, "invalid tag name"},
		{"invalid_tag_fallback", fallback, "invalid tag name"},
		{"unexported_anonymous", struct{ diagnosticJSONEmbedded }{diagnosticJSONEmbedded{Value: 1}}, "embedded-field promotion"},
		{"omitzero_value", struct {
			Value diagnosticJSONZeroValue `json:"value,omitzero"`
		}{Value: 1}, "omitzero tag"},
		{"omitzero_pointer_method", struct {
			Value diagnosticJSONZeroPointer `json:"value,omitzero"`
		}{Value: 1}, "omitzero tag"},
		{"omitzero_nil_pointer", struct {
			Value *diagnosticJSONZeroPointer `json:"value,omitzero"`
		}{}, "omitzero tag"},
		{"omitzero_after_omitted", struct {
			Value diagnosticJSONZeroValue `json:"value,omitempty,omitzero"`
		}{}, "omitzero tag"},
		{"format_array", struct {
			Value []byte `json:"value,format:array"`
		}{Value: []byte{255, 255, 255}}, "tag option"},
		{"embed", struct {
			Value map[string]int `json:"value,embed"`
		}{Value: map[string]int{"a": 1}}, "tag option"},
		{"unknown_option", diagnosticJSONTaggedValue(t, "Value", `json:"value,future"`, 1), "tag option"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			a := diagnosticArtifactOwner(t, 10000, 128, studyDiagnosticArtifactFileOps())
			err := a.atomicJSON("invalid.json", fixture.value)
			if err == nil || !strings.Contains(err.Error(), fixture.cause) || a.Err() == nil || a.quota.used != 0 {
				t.Fatal("unsupported field shape escaped preflight refusal and latch:", err)
			}
		})
	}
	// Only the complete '-' tag ignores a field, including an anonymous one.
	ignored := struct {
		Value                  diagnosticJSONValueByte `json:"-"`
		diagnosticJSONEmbedded `json:"-"`
	}{Value: 1, diagnosticJSONEmbedded: diagnosticJSONEmbedded{Value: 1}}
	a := diagnosticArtifactOwner(t, 10000, 128, studyDiagnosticArtifactFileOps())
	data, err := a.jsonBytes(ignored, 2)
	if err != nil || string(data) != "{}" {
		t.Fatal("exact ignored tag traversed custom marshaler or anonymous field:", err)
	}
}

func TestDiagnosticArtifactJSONCustomByteElements(t *testing.T) {
	for _, fixture := range []struct {
		name string
		elem reflect.Type
	}{
		{"json_value", reflect.TypeFor[diagnosticJSONValueByte]()},
		{"json_pointer", reflect.TypeFor[diagnosticJSONPointerByte]()},
		{"text_value", reflect.TypeFor[diagnosticTextValueByte]()},
		{"text_pointer", reflect.TypeFor[diagnosticTextPointerByte]()},
		{"append_text_value", reflect.TypeFor[diagnosticAppendTextValueByte]()},
		{"append_text_pointer", reflect.TypeFor[diagnosticAppendTextPointerByte]()},
		{"json_to_name_value", reflect.TypeFor[diagnosticJSONToNameValueByte]()},
		{"json_to_name_pointer", reflect.TypeFor[diagnosticJSONToNamePointerByte]()},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			diagnosticJSONAssertCustomByteRefused(t, fixture.elem)
		})
	}
}

func diagnosticJSONAssertCustomByteRefused(t *testing.T, elem reflect.Type) {
	t.Helper()
	for _, value := range []reflect.Value{reflect.Zero(elem), reflect.New(elem)} {
		a := diagnosticArtifactOwner(t, 10000, 128, studyDiagnosticArtifactFileOps())
		err := a.atomicJSON("custom.json", value.Interface())
		if err == nil || !strings.Contains(err.Error(), "custom marshaler") || a.Err() == nil || a.quota.used != 0 {
			t.Fatal("custom scalar hook escaped preflight refusal and latch:", err)
		}
	}
	for _, shape := range []struct {
		name   string
		length int
	}{{"nil", -1}, {"empty", 0}, {"element", 1}} {
		t.Run(shape.name, func(t *testing.T) {
			value := reflect.Zero(reflect.SliceOf(elem))
			if shape.length >= 0 {
				value = reflect.MakeSlice(value.Type(), shape.length, shape.length)
			}
			pointer := reflect.New(value.Type())
			pointer.Elem().Set(value)
			for _, input := range []any{value.Interface(), pointer.Interface()} {
				a := diagnosticArtifactOwner(t, 10000, 128, studyDiagnosticArtifactFileOps())
				err := a.atomicJSON("custom.json", input)
				if err == nil || !strings.Contains(err.Error(), "custom byte-element marshaler") || a.Err() == nil || a.quota.used != 0 {
					t.Fatal("custom byte-element type escaped preflight refusal and latch:", err)
				}
			}
		})
	}
}

type diagnosticTextValueKey string

func (diagnosticTextValueKey) MarshalText() ([]byte, error) {
	panic("diagnostic text value map key marshaler must never run")
}

type diagnosticTextPointerKey string

func (*diagnosticTextPointerKey) MarshalText() ([]byte, error) {
	panic("diagnostic text pointer map key marshaler must never run")
}

type diagnosticAppendTextValueKey string

func (diagnosticAppendTextValueKey) AppendText([]byte) ([]byte, error) {
	panic("diagnostic text value map key appender must never run")
}

type diagnosticAppendTextPointerKey string

func (*diagnosticAppendTextPointerKey) AppendText([]byte) ([]byte, error) {
	panic("diagnostic text pointer map key appender must never run")
}

type diagnosticJSONToNameValueKey string

func (diagnosticJSONToNameValueKey) MarshalJSONTo() {
	panic("diagnostic JSONTo value map key method must never run")
}

type diagnosticJSONToNamePointerKey string

func (*diagnosticJSONToNamePointerKey) MarshalJSONTo() {
	panic("diagnostic JSONTo pointer map key method must never run")
}

func TestDiagnosticArtifactJSONCustomMapKeys(t *testing.T) {
	for _, fixture := range []struct {
		name string
		key  reflect.Type
	}{
		{"text_value", reflect.TypeFor[diagnosticTextValueKey]()},
		{"text_pointer", reflect.TypeFor[diagnosticTextPointerKey]()},
		{"append_text_value", reflect.TypeFor[diagnosticAppendTextValueKey]()},
		{"append_text_pointer", reflect.TypeFor[diagnosticAppendTextPointerKey]()},
		{"json_to_name_value", reflect.TypeFor[diagnosticJSONToNameValueKey]()},
		{"json_to_name_pointer", reflect.TypeFor[diagnosticJSONToNamePointerKey]()},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			diagnosticJSONAssertCustomMapKeyRefused(t, fixture.key)
		})
	}
}

func diagnosticJSONAssertCustomMapKeyRefused(t *testing.T, key reflect.Type) {
	t.Helper()
	for _, shape := range []string{"nil", "empty", "entry"} {
		t.Run(shape, func(t *testing.T) {
			value := reflect.Zero(reflect.MapOf(key, reflect.TypeFor[int]()))
			if shape != "nil" {
				value = reflect.MakeMap(value.Type())
			}
			if shape == "entry" {
				value.SetMapIndex(reflect.ValueOf("key").Convert(key), reflect.ValueOf(7))
			}
			pointer := reflect.New(value.Type())
			pointer.Elem().Set(value)
			for _, input := range []any{value.Interface(), pointer.Interface()} {
				a := diagnosticArtifactOwner(t, 10000, 128, studyDiagnosticArtifactFileOps())
				err := a.atomicJSON("custom.json", input)
				if err == nil || !strings.Contains(err.Error(), "custom map-key marshaler") || a.Err() == nil || a.quota.used != 0 {
					t.Fatal("custom map-key type escaped preflight refusal and latch:", err)
				}
			}
		})
	}
}

func TestDiagnosticArtifactJSONErrorsAndDepth(t *testing.T) {
	for _, value := range []any{math.NaN(), math.Inf(1), math.Inf(-1), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), json.RawMessage(`{}`), make(chan int), func() {}} {
		a := diagnosticArtifactOwner(t, 100000, 128, studyDiagnosticArtifactFileOps())
		if err := a.atomicJSON("invalid.json", value); err == nil || a.Err() == nil || a.quota.used != 0 {
			t.Fatal("unsupported/nonfinite/time value escaped preflight latch")
		}
	}
	var value any = 0
	for depth := 0; depth < 32; depth++ {
		value = []any{value}
	}
	if err := studyDiagnosticJSONPreflight(reflect.ValueOf(value), 10000); err != nil {
		t.Fatal("exact depth32 refused:", err)
	}
	value = []any{value}
	if err := studyDiagnosticJSONPreflight(reflect.ValueOf(value), 10000); err == nil {
		t.Fatal("depth33 accepted")
	}
	cycle := &diagnosticJSONCycle{}
	cycle.Next = cycle
	a := diagnosticArtifactOwner(t, 100000, 128, studyDiagnosticArtifactFileOps())
	if err := a.atomicJSON("cycle.json", cycle); err == nil || a.Err() == nil {
		t.Fatal("cycle was not bounded before encoding")
	}
	// Ignored fields are not encoded and must not trigger an unbounded labels
	// traversal. The generic E0 walker is reused only on individual strings.
	ignored := struct {
		Value   int                  `json:"value"`
		Ignored *diagnosticJSONCycle `json:"-"`
	}{Value: 1, Ignored: cycle}
	if err := studyDiagnosticJSONPreflight(reflect.ValueOf(ignored), 100); err != nil {
		t.Fatal("ignored cycle was traversed:", err)
	}
	a = diagnosticArtifactOwner(t, 16<<20, 128, studyDiagnosticArtifactFileOps())
	if err := a.atomicJSON("large.json", make([]byte, 6<<20)); err == nil || a.Err() == nil || a.quota.used != 0 {
		t.Fatal("8MiB logical-record limit was not enforced before encoding")
	}
}
