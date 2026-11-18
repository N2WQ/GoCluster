//go:build goexperiment.jsonv2

package main

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"reflect"
	"testing"
)

type diagnosticJSONToValueByte uint8

func (diagnosticJSONToValueByte) MarshalJSONTo(*jsontext.Encoder) error {
	panic("diagnostic v2 JSON value byte marshaler must never run")
}

type diagnosticJSONToPointerByte uint8

func (*diagnosticJSONToPointerByte) MarshalJSONTo(*jsontext.Encoder) error {
	panic("diagnostic v2 JSON pointer byte marshaler must never run")
}

type diagnosticJSONToValueKey string

func (diagnosticJSONToValueKey) MarshalJSONTo(*jsontext.Encoder) error {
	panic("diagnostic v2 JSON value map key marshaler must never run")
}

type diagnosticJSONToPointerKey string

func (*diagnosticJSONToPointerKey) MarshalJSONTo(*jsontext.Encoder) error {
	panic("diagnostic v2 JSON pointer map key marshaler must never run")
}

func TestDiagnosticArtifactJSONV2CustomHooks(t *testing.T) {
	var _ jsonv2.MarshalerTo = diagnosticJSONToValueByte(0)
	var _ jsonv2.MarshalerTo = (*diagnosticJSONToPointerByte)(nil)
	var _ jsonv2.MarshalerTo = diagnosticJSONToValueKey("")
	var _ jsonv2.MarshalerTo = (*diagnosticJSONToPointerKey)(nil)
	t.Run("byte_value", func(t *testing.T) {
		diagnosticJSONAssertCustomByteRefused(t, reflect.TypeFor[diagnosticJSONToValueByte]())
	})
	t.Run("byte_pointer", func(t *testing.T) {
		diagnosticJSONAssertCustomByteRefused(t, reflect.TypeFor[diagnosticJSONToPointerByte]())
	})
	t.Run("key_value", func(t *testing.T) {
		diagnosticJSONAssertCustomMapKeyRefused(t, reflect.TypeFor[diagnosticJSONToValueKey]())
	})
	t.Run("key_pointer", func(t *testing.T) {
		diagnosticJSONAssertCustomMapKeyRefused(t, reflect.TypeFor[diagnosticJSONToPointerKey]())
	})
}
