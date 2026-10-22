package filter

import (
	"errors"
	"strings"
	"testing"

	"dxcluster/filter/expr"
	"dxcluster/spot"
)

func TestRuleCompileCanonical(t *testing.T) {
	for _, test := range []struct {
		source, want  string
		atoms, values int
		path          bool
	}{
		{`(mode=psk31 AND band=[20,6m]) OR comment="POTA * ?"`,
			`MODE=PSK AND BAND=20M,6M OR COMMENT="POTA * ?"`, 3, 4, false},
		{`NOT (source="human" OR path=closed)`, `NOT (SOURCE=HUMAN OR PATH=CLOSED)`, 2, 2, true},
		{`comment="  Mixed ""quoted"" * ?  "`, `COMMENT="  Mixed ""quoted"" * ?  "`, 1, 1, false},
		{`comment=POTA`, `COMMENT="POTA"`, 1, 1, false},
		{`comment=PoTa`, `COMMENT="PoTa"`, 1, 1, false},
		{`FREQ=07000.0100,7e3-7.1e3 AND SNR=-00`, `FREQ=7000.01,7000-7100 AND SNR=0`, 2, 3, false},
		{`DXZONE=01,40 AND DXDXCC=00291 AND EVENT=NONE,POTA`,
			`DXZONE=1,40 AND DXDXCC=291 AND EVENT=NONE,POTA`, 3, 5, false},
		{`DXSTATE="ct" AND DEGRID2=em`, `DXSTATE=CT AND DEGRID2=EM`, 2, 2, false},
	} {
		t.Run(test.source, func(t *testing.T) {
			program, err := compileRule(test.source)
			if err != nil {
				t.Fatal(err)
			}
			if program.canonical != test.want {
				t.Fatalf("canonical got %q, want %q", program.canonical, test.want)
			}
			if program.atomCount != test.atoms || program.valueCount != test.values || program.needsPath != test.path {
				t.Fatalf("counts/path got (%d,%d,%v), want (%d,%d,%v)",
					program.atomCount, program.valueCount, program.needsPath, test.atoms, test.values, test.path)
			}
			if len(program.nodes) != len(program.syntax.Nodes) {
				t.Fatal("compiled and syntax indexes differ")
			}
			for i, node := range program.nodes {
				syntax := program.syntax.Nodes[i]
				if node.kind != syntax.Kind || node.left != syntax.Left || node.right != syntax.Right {
					t.Fatalf("node %d changed syntax topology", i)
				}
			}
			again, err := compileRule(program.canonical)
			if err != nil || again.canonical != program.canonical {
				t.Fatalf("canonical recompile got %v, %v", again, err)
			}
		})
	}
}

func TestRuleCompileRejects(t *testing.T) {
	for _, test := range []struct {
		source string
		column int
		hint   string
	}{
		{`AR=EU`, 1, "field"},
		{`BAND=banana`, 6, "BAND"},
		{`MODE=SSB`, 6, "MODE"},
		{`MODE=banana`, 6, "MODE"},
		{`MODE=""`, 6, "MODE"},
		{`SOURCE=RBN`, 8, "SOURCE"},
		{`EVENT=DXPEDITION`, 7, "EVENT"},
		{`DXCONT=ZZ`, 8, "DXCONT"},
		{`DXSTATE=ZZ`, 9, "DXSTATE"},
		{`DEGRID2=SS`, 9, "DEGRID2"},
		{`DXGRID2=FN31`, 9, "DXGRID2"},
		{`CONF=75`, 6, "CONF"},
		{`PATH=GOOD`, 6, "PATH"},
		{`BAND>20m`, 1, "operator"},
		{`MODE>=CW`, 1, "operator"},
		{`DXDXCC>=291`, 1, "operator"},
		{`COMMENT<"POTA"`, 1, "operator"},
		{`SNR>=0,6`, 1, "one value"},
		{`DXZONE<14,15`, 1, "one value"},
		{`FREQ>=7000,7100`, 1, "one value"},
		{`SNR=0.5`, 5, "integral"},
		{`SNR=-5-6`, 5, "integral"},
		{`SNR=1e2`, 5, "integral"},
		{`SNR=999999999999999999999999999`, 5, "integral"},
		{`DXZONE=0`, 8, "1..40"},
		{`DEZONE=41`, 8, "1..40"},
		{`DXZONE=14.0`, 8, "integral"},
		{`DXDXCC=0`, 8, "ADIF"},
		{`DEDXCC=-1`, 8, "ADIF"},
		{`DXDXCC=K`, 8, "integral"},
		{`DXDXCC=291.0`, 8, "integral"},
		{`DXCALL=K*ABC`, 8, "DXCALL"},
		{`DECALL=*ABC*`, 8, "DECALL"},
		{`DXCALL=K1?`, 8, "DXCALL"},
		{`DXCALL=""`, 8, "DXCALL"},
		{`DXCALL="K1 ABC"`, 8, "DXCALL"},
		{`COMMENT=""`, 9, "COMMENT"},
		{`COMMENT="   "`, 9, "COMMENT"},
		{`COMMENT="` + strings.Repeat("x", 65) + `"`, 9, "COMMENT"},
		{`FREQ=-1`, 6, "FREQ"},
		{`FREQ=NaN`, 6, "FREQ"},
		{`FREQ=Inf`, 6, "FREQ"},
		{`FREQ=1e999`, 6, "FREQ"},
		{`FREQ=7100-7000`, 6, "FREQ"},
		{`FREQ=7000-Inf`, 6, "FREQ"},
		{`FREQ>7000-7100`, 6, "FREQ"},
		{`FREQ=7000-7100-7200`, 6, "FREQ"},
		{`MODE=CW AND SNR=bad`, 17, "integral"},
	} {
		t.Run(test.source, func(t *testing.T) {
			program, err := compileRule(test.source)
			if err == nil || program != nil {
				t.Fatalf("invalid rule accepted: %v", program)
			}
			var syntaxError *expr.SyntaxError
			if !errors.As(err, &syntaxError) || syntaxError.Column != test.column {
				t.Fatalf("got %v, want expression column %d", err, test.column)
			}
			if !strings.Contains(err.Error(), test.hint) {
				t.Fatalf("error %q lacks useful hint %q", err, test.hint)
			}
		})
	}
}

func TestRuleCompileBudgets(t *testing.T) {
	// The syntax can fit while domain normalization expands beyond 1024 bytes.
	source := `FREQ=` + strings.Repeat("1e300,", 3) + `1e300`
	if _, err := compileRule(source); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("expanded canonical expression admitted: %v", err)
	}
	phrases := strings.Repeat(`"`+strings.Repeat("x", 64)+`",`, 14) + `"` + strings.Repeat("x", 64) + `"`
	if _, err := compileRule("COMMENT=" + phrases); err != nil {
		t.Fatalf("valid bounded phrase list rejected: %v", err)
	}
	values := strings.Repeat("0,", expr.MaxValuesPerAtom-1) + "0"
	if p, err := compileRule("SNR=" + values); err != nil || p.valueCount != expr.MaxValuesPerAtom {
		t.Fatalf("maximum values rejected: %v", err)
	}
	if _, err := compileRule("SNR=" + values + ",0"); err == nil {
		t.Fatal("oversized value list accepted")
	}
	atoms := strings.Repeat("SNR=0 AND ", expr.MaxAtoms-1) + "SNR=0"
	if p, err := compileRule(atoms); err != nil || p.atomCount != expr.MaxAtoms {
		t.Fatalf("maximum atoms rejected: %v", err)
	}
	if _, err := compileRule(atoms + " AND SNR=0"); err == nil {
		t.Fatal("oversized atom list accepted")
	}
}

func TestRuleFieldProjection(t *testing.T) {
	for _, name := range []string{
		"BAND", "MODE", "SOURCE", "EVENT", "DXCALL", "DECALL", "DXCONT", "DECONT",
		"DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE", "DESTATE",
		"DXGRID2", "DEGRID2", "FREQ", "SNR", "CONF", "PATH", "COMMENT",
	} {
		field, ok := ruleFieldForName(name)
		if !ok {
			t.Fatalf("field %s not recognized", name)
		}
		want := false
		switch name {
		case "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC",
			"DXSTATE", "DESTATE", "DXGRID2", "DEGRID2":
			want = true
		}
		if isGeographicField(field) != want {
			t.Errorf("%s geographic classification got %v, want %v", name, !want, want)
		}
	}
}

func TestRuleValidators(t *testing.T) {
	for _, pattern := range []string{"*", "K1ABC", "k1abc", "W1*", "*ABC", "EA/K1ABC", "N2ABC-9"} {
		if !ValidCallsignPattern(pattern) {
			t.Errorf("valid callsign pattern %q rejected", pattern)
		}
	}
	for _, pattern := range []string{"", " K1ABC", "K1ABC ", "K*ABC", "*ABC*", "**", "A?", "A_B", "A.B"} {
		if ValidCallsignPattern(pattern) {
			t.Errorf("invalid callsign pattern %q accepted", pattern)
		}
	}
	for _, grid := range []string{"AA", "FN", "RR"} {
		if !ValidGrid2Prefix(grid) {
			t.Errorf("valid grid prefix %q rejected", grid)
		}
	}
	for _, grid := range []string{"", "A", "SS", "AR ", "FN31", "fn", "A1"} {
		if ValidGrid2Prefix(grid) {
			t.Errorf("invalid grid prefix %q accepted", grid)
		}
	}
}

func TestRuleCompileTaxonomyAndBands(t *testing.T) {
	for _, band := range spot.SupportedBandNames() {
		if _, err := compileRule("BAND=" + band); err != nil {
			t.Errorf("supported band %q rejected: %v", band, err)
		}
	}
	for _, mode := range SupportedModes() {
		if _, err := compileRule("MODE=" + mode); err != nil {
			t.Errorf("supported mode %q rejected: %v", mode, err)
		}
	}
}
