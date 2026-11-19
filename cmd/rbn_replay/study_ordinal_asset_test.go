package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"dxcluster/internal/correctionflow"
)

func ordinalWireFixture(t testing.TB) []byte {
	t.Helper()
	a := correctionflow.OrdinalAsset{SchemaVersion: 1, Experimental: true, Mode: "CW", Alphabet: correctionflow.OrdinalAlphabet, Alignment: "unit-suffix-MSDI-v1", ReferenceVersion: "ccv2-reference-v1",
		SNREdges: [7]int{-999, 0, 6, 12, 18, 24, 999}, EmissionPriorTotal: 1, InsertionPriorTotal: 1, GapPriorAlpha: 1, GapPriorBeta: 1,
		Sources: [2]correctionflow.OrdinalTrainingSource{
			{DateUTC: "20261107", SHA256: "2a78ec300acc2cfe29048201af4e971cad933db01c6f36b567ac7443b57ccd34", FocalRows: 458495},
			{DateUTC: "20261114", SHA256: "ad0c8f5a942bab03da052cbe91c2087e59ff566cbd48aa2be8be43b1140bff49", FocalRows: 882021},
		}}
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCCV2F1AssetWireContract(t *testing.T) {
	valid := ordinalWireFixture(t)
	decode := func(b []byte) error {
		var a correctionflow.OrdinalAsset
		return studyDiagnosticDecode(b, diagnosticWireOrdinal, &a)
	}
	if err := decode(valid); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ old, new string }{
		{`"experimental":true`, `"experimental":false`},
		{`"experimental":true,`, ``},
		{`"experimental":true`, `"experimental":null`},
		{`"experimental":true`, `"experimental":true,"experimental":true`},
		{`"experimental":true`, `"Experimental":true`},
		{`"experimental":true`, `"unknown":true,"experimental":true`},
	} {
		b := bytes.Replace(valid, []byte(tc.old), []byte(tc.new), 1)
		err := decode(b)
		if tc.new == `"experimental":false` {
			if err != nil {
				t.Fatal("explicit false is present, not a wire absence")
			}
			var a correctionflow.OrdinalAsset
			if err := studyDiagnosticDecode(b, diagnosticWireOrdinal, &a); err != nil {
				t.Fatal(err)
			}
			if _, err := correctionflow.NewOrdinalModel(&a); err == nil {
				t.Fatal("false marker accepted by model")
			}
		} else if err == nil {
			t.Fatalf("malformed fact accepted: %s", tc.new)
		}
	}
	for _, path := range [][]any{{"snr_edges"}, {"sources"}, {"cells"}, {"cells", 0, "opportunities"}, {"cells", 0, "insertion_symbols"}, {"cells", 0, "events"}, {"cells", 0, "events", 0}} {
		for _, delta := range []int{-1, 1} {
			var root map[string]any
			if err := json.Unmarshal(valid, &root); err != nil {
				t.Fatal(err)
			}
			var owner any = root
			for _, key := range path[:len(path)-1] {
				switch k := key.(type) {
				case string:
					owner = owner.(map[string]any)[k]
				case int:
					owner = owner.([]any)[k]
				}
			}
			key := path[len(path)-1]
			var list []any
			switch k := key.(type) {
			case string:
				list = owner.(map[string]any)[k].([]any)
			case int:
				list = owner.([]any)[k].([]any)
			}
			if delta < 0 {
				list = list[:len(list)-1]
			} else {
				list = append(list, list[len(list)-1])
			}
			switch k := key.(type) {
			case string:
				owner.(map[string]any)[k] = list
			case int:
				owner.([]any)[k] = list
			}
			b, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := decode(b); err == nil {
				t.Fatalf("fixed array dimension changed at %v delta%d", path, delta)
			}
		}
	}
	if err := decode(append(valid, []byte(` {}`)...)); err == nil {
		t.Fatal("extra JSON value accepted")
	}
}

func TestCCV2F1AssetFileIntegrity(t *testing.T) {
	data := ordinalWireFixture(t)
	path := filepath.Join(t.TempDir(), "model.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(data)
	file := studyFile{Path: path, SHA256: hex.EncodeToString(h[:])}
	if model, err := studyLoadOrdinalModel(file); err != nil || model == nil {
		t.Fatalf("valid model unavailable: %v", err)
	}
	file.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := studyLoadOrdinalModel(file); err == nil {
		t.Fatal("tampered model hash accepted")
	}
	if err := os.WriteFile(path, make([]byte, (4<<20)+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := studyLoadOrdinalModel(file); err == nil {
		t.Fatal("oversized model accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := studyLoadOrdinalModel(file); err == nil {
		t.Fatal("missing model accepted")
	}
}

func TestCCV2F1FrozenFittedAsset(t *testing.T) {
	file := studyFile{Path: "testdata/ordinal_fitted_v7.json", SHA256: "968caf263f3bf44c13509ded8b5e48faf0bd39b1d7cc42dcd9d1e6c0a45e5e1a"}
	m, err := studyLoadOrdinalModel(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, snr := range []int{7, 12, 18, 24, 25} {
		score, ok := m.Score("K1ABC", "K1ABD", "CW", snr, true)
		if !ok || math.IsNaN(score) || math.IsInf(score, 0) || score >= 0 {
			t.Fatalf("fitted scoring unreachable for SNR%d: %g/%v", snr, score, ok)
		}
	}
	if _, ok := m.Score("K1ABC", "K1ABD", "CW", 12, false); ok {
		t.Fatal("fitted model scored missing focal SNR")
	}
	data, err := studyReadOrdinalAsset(file.Path)
	if err != nil {
		t.Fatal(err)
	}
	var a correctionflow.OrdinalAsset
	if err := studyDiagnosticDecode(data, diagnosticWireOrdinal, &a); err != nil {
		t.Fatal(err)
	}
	var pairs, characters, gaps uint64
	for _, cell := range a.Cells {
		pairs += cell.Pairs
		characters += cell.SourceCharacters
		gaps += cell.GapStops
	}
	if pairs != 124035 || characters != 610470 || gaps != 734505 {
		t.Fatal("frozen model/corpus reconciliation changed")
	}
}

func FuzzCCV2F1AssetWire(f *testing.F) {
	valid := ordinalWireFixture(f)
	f.Add(valid)
	f.Add(bytes.Replace(valid, []byte(`"opportunities":[0,0,`), []byte(`"opportunities":[0,`), 1))
	f.Add([]byte(`{"schema_version":1}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"experimental":false,"experimental":true}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4<<20 {
			return
		}
		var a correctionflow.OrdinalAsset
		if err := studyDiagnosticDecode(b, diagnosticWireOrdinal, &a); err == nil {
			model, err := correctionflow.NewOrdinalModel(&a)
			if err == nil && model == nil {
				t.Fatal("successful constructor returned no model")
			}
		}
	})
}
