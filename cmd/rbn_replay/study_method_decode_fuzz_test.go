package main

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"unicode/utf8"
)

// Exercise both new bounded wire decoders without opening plan-named files.
// Accepted metadata must survive its canonical encoding without losing facts;
// malformed mutations must not panic or bypass the token allocation bounds.
func FuzzCCV2MethodPlanWire(f *testing.F) {
	method := methodTestPlan(f)
	file := method.Model
	evaluation := studyMethodEvaluationPlan{
		SchemaVersion: 1, Experimental: true, Phase: "tuning",
		CapturePlan: file, MethodPlan: file, ControlPlan: file,
		ParityCapturePlan: file, ParityControlPlan: file,
	}
	for _, original := range studyMethodEvalOriginalAuthority() {
		evaluation.Parity = append(evaluation.Parity, studyMethodEvalParity{
			DateUTC: original.DateUTC, OriginalDecisions: file, ControlDecisions: file,
			Rows: original.Rows, ControlComplete: file, ControlManifest: file,
		})
		evaluation.Groups = append(evaluation.Groups, studyMethodEvalGroup{
			DateUTC: original.DateUTC, Control: studyMethodEvalBinding{file, file},
			Variants: []studyMethodEvalVariant{{"f150_t60_geometry_d1", file, file}, {"f150_t60_ordinal_d1", file, file}},
		})
	}
	for kind, value := range []any{method, evaluation} {
		data, err := json.Marshal(value)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(uint8(kind), data)
		f.Add(uint8(kind), bytes.Replace(data, []byte(`"experimental":true`), []byte(`"experimental":null`), 1))
	}
	v2method := methodTestPlanV2(f)
	v2evaluation := evaluation
	v2evaluation.SchemaVersion = 2
	v2evaluation.Groups = append([]studyMethodEvalGroup(nil), evaluation.Groups[:2]...)
	for i := range v2evaluation.Groups {
		v2evaluation.Groups[i].Variants = []studyMethodEvalVariant{{"c_f150_t60_d3", file, file}, {"b_f150_t60_d3", file, file}}
	}
	for kind, value := range []any{v2method, v2evaluation} {
		data, err := json.Marshal(value)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(uint8(kind), data)
	}
	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		if len(data) > studyMaxJSONBytes || !utf8.Valid(data) {
			return
		}
		var first, second any
		if kind%2 == 0 {
			if studyMethodPlanPreflight(data) != nil {
				return
			}
			a, b := new(studyMethodPlan), new(studyMethodPlan)
			if err := json.Unmarshal(data, a); err != nil {
				return
			}
			count, maxDistance := 16, 2
			if a.SchemaVersion == 2 {
				count, maxDistance = 2, 3
			}
			if len(a.Candidates) != count {
				t.Fatal("wire preflight bypassed the complete grid bound")
			}
			for _, candidate := range a.Candidates {
				if len(candidate.CWDistances) < 1 || len(candidate.CWDistances) > maxDistance || a.SchemaVersion == 2 && len(candidate.CWDistances) != 3 {
					t.Fatal("wire preflight bypassed the distance bound")
				}
			}
			first, second = a, b
		} else {
			a, b := new(studyMethodEvaluationPlan), new(studyMethodEvaluationPlan)
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			if studyMethodEvalWire(decoder, reflect.TypeFor[studyMethodEvaluationPlan](), 0) != nil {
				return
			}
			if _, err := decoder.Token(); err != io.EOF {
				return
			}
			if err := json.Unmarshal(data, a); err != nil {
				return
			}
			if len(a.Parity) > 4 || len(a.Groups) > 4 {
				t.Fatal("wire walker bypassed the date bounds")
			}
			for _, group := range a.Groups {
				if len(group.Variants) > 2 {
					t.Fatal("wire walker bypassed the pair bound")
				}
			}
			first, second = a, b
		}
		encoded, err := json.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(encoded, second); err != nil || !reflect.DeepEqual(first, second) {
			t.Fatal("accepted wire facts did not survive canonical round trip:", err)
		}
	})
}
