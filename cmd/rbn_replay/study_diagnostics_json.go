// File role: Preflight the bounded standard JSON shapes used by diagnostic DTOs.
// This is a restricted artifact boundary, not an encoding/json replacement.
// RawMessage, custom hooks, embedded promotion and tags beyond omitempty are
// refused. The accepted shapes deliberately exclude general encoder features.
package main

import (
	"encoding"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Preflight exact standard JSON lengths without a population-sized encoding.
// Custom marshalers are refused; time.Time is the sole bounded exception.
// Strings are byte bounded before encoding allocation. Unlike E0's conservative
// estimate, exact scalar/string sizes admit the full row limit with framing.
func studyDiagnosticJSONPreflight(value reflect.Value, limit int) error {
	left := limit
	take := func(n int) error {
		if n < 0 || n > left {
			return errors.New("diagnostic JSON preflight exceeds encoded byte limit")
		}
		left -= n
		return nil
	}
	var visit func(reflect.Value, int) error
	goDepth := 0
	visit = func(v reflect.Value, depth int) error {
		goDepth++
		defer func() { goDepth-- }()
		if goDepth > 128 {
			return errors.New("diagnostic JSON pointer/interface chain exceeds bound")
		}
		if depth > 32 {
			return errors.New("diagnostic JSON nesting exceeds 32")
		}
		if !v.IsValid() {
			return take(4)
		}
		if v.Type() == reflect.TypeFor[time.Time]() {
			data, err := json.Marshal(v.Interface())
			if err != nil {
				return err
			}
			return take(len(data))
		}
		if studyDiagnosticJSONCustomMarshaler(v.Type()) {
			return errors.New("diagnostic JSON custom marshaler is not prebounded")
		}
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return take(4)
			}
			return visit(v.Elem(), depth)
		case reflect.String:
			n, err := studyDiagnosticJSONStringSize(v.String())
			if err != nil {
				return err
			}
			return take(n)
		case reflect.Bool:
			if v.Bool() {
				return take(4)
			}
			return take(5)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			data, err := json.Marshal(v.Interface())
			if err != nil {
				return err
			}
			return take(len(data))
		case reflect.Slice, reflect.Array:
			// The standard encoder uses element methods instead of base64 for
			// named byte elements. Refuse that type even when nil or empty.
			if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 && studyDiagnosticJSONCustomMarshaler(v.Type().Elem()) {
				return errors.New("diagnostic JSON custom byte-element marshaler is not prebounded")
			}
			if v.Kind() == reflect.Slice && v.IsNil() {
				return take(4)
			}
			if v.Len() > left {
				return errors.New("diagnostic JSON collection exceeds remaining byte limit")
			}
			if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
				return take(2 + 4*((v.Len()+2)/3))
			}
			if err := take(2 + max(0, v.Len()-1)); err != nil {
				return err
			}
			for i := 0; i < v.Len(); i++ {
				if err := visit(v.Index(i), depth+1); err != nil {
					return err
				}
			}
			return nil
		case reflect.Map:
			if v.Type().Key().Kind() != reflect.String {
				return errors.New("diagnostic JSON requires string map keys")
			}
			if studyDiagnosticJSONCustomMarshaler(v.Type().Key()) {
				return errors.New("diagnostic JSON custom map-key marshaler is not prebounded")
			}
			if v.IsNil() {
				return take(4)
			}
			if err := take(2 + max(0, v.Len()-1)); err != nil {
				return err
			}
			iter := v.MapRange()
			for iter.Next() {
				n, err := studyDiagnosticJSONStringSize(iter.Key().String())
				if err != nil {
					return err
				}
				if err := take(n + 1); err != nil {
					return err
				}
				if err := visit(iter.Value(), depth+1); err != nil {
					return err
				}
			}
			return nil
		case reflect.Struct:
			if err := take(2); err != nil {
				return err
			}
			fields := 0
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				rawTag := field.Tag.Get("json")
				if rawTag == "-" {
					continue
				}
				if field.Anonymous {
					return errors.New("diagnostic JSON embedded-field promotion is not supported")
				}
				if field.PkgPath != "" {
					continue
				}
				tag := strings.Split(rawTag, ",")
				if tag[0] != "" && !studyDiagnosticJSONTagNameValid(tag[0]) {
					return errors.New("diagnostic JSON invalid tag name is not supported")
				}
				omit := false
				for _, option := range tag[1:] {
					switch option {
					case "":
					case "string":
						return errors.New("diagnostic JSON string tag is not supported")
					case "omitzero":
						return errors.New("diagnostic JSON omitzero tag is not supported")
					case "omitempty":
						omit = studyDiagnosticJSONEmpty(v.Field(i))
					default:
						return errors.New("diagnostic JSON tag option is not supported")
					}
				}
				if omit {
					continue
				}
				name := tag[0]
				if name == "" {
					name = field.Name
				}
				n, err := studyDiagnosticJSONStringSize(name)
				if err != nil {
					return err
				}
				if fields > 0 {
					n++
				}
				fields++
				if err := take(n + 1); err != nil {
					return err
				}
				if err := visit(v.Field(i), depth+1); err != nil {
					return err
				}
			}
			return nil
		default:
			return errors.New("unsupported diagnostic JSON value")
		}
	}
	return visit(value, 0)
}

func studyDiagnosticJSONCustomMarshaler(typ reflect.Type) bool {
	for _, candidate := range []reflect.Type{typ, reflect.PointerTo(typ)} {
		if candidate.Implements(reflect.TypeFor[json.Marshaler]()) || candidate.Implements(reflect.TypeFor[encoding.TextMarshaler]()) || candidate.Implements(reflect.TypeFor[encoding.TextAppender]()) {
			return true
		}
		// Go 1.27 enables the v2 encoder by default. A conservative name check
		// refuses its hook without an experiment-gated production dependency.
		if _, ok := candidate.MethodByName("MarshalJSONTo"); ok {
			return true
		}
	}
	return false
}

func studyDiagnosticJSONTagNameValid(name string) bool {
	for _, r := range name {
		if strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", r) || unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		return false
	}
	return true
}

func studyDiagnosticJSONEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

func studyDiagnosticJSONStringSize(value string) (int, error) {
	if err := studyAuditLabels(reflect.ValueOf(value)); err != nil {
		return 0, err
	}
	if !utf8.ValidString(value) {
		return 0, errors.New("diagnostic label requires at most 1024 valid UTF-8 bytes")
	}
	n := 2
	for _, r := range value {
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			n += 2
		case '<', '>', '&', '\u2028', '\u2029':
			n += 6
		default:
			if r < 0x20 {
				n += 6
			} else {
				n += utf8.RuneLen(r)
			}
		}
	}
	return n, nil
}
