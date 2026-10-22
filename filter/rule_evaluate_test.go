package filter

import (
	"math"
	"testing"

	"dxcluster/spot"
)

func TestRuleEvaluateTruth(t *testing.T) {
	tests := []struct {
		name, source string
		spot         spot.Spot
		path         string
		want         bool
	}{
		{"mixed-CW-any-band", `MODE=CW OR (MODE=FT8 AND BAND=6m)`, spot.Spot{Mode: "CW", Band: "40m"}, "", true},
		{"mixed-CW-missing-band", `MODE=CW OR (MODE=FT8 AND BAND=6m)`, spot.Spot{Mode: "CW"}, "", true},
		{"mixed-FT8-six", `MODE=CW OR (MODE=FT8 AND BAND=6m)`, spot.Spot{Mode: "FT8", Band: "6m"}, "", true},
		{"mixed-FT8-twenty", `MODE=CW OR (MODE=FT8 AND BAND=6m)`, spot.Spot{Mode: "FT8", Band: "20m"}, "", false},
		{"band-missing-equal", `BAND=20m`, spot.Spot{}, "", false},
		{"band-empty-normalized-fallback", `BAND=20m`, spot.Spot{BandNorm: " ", Band: "20m"}, "", true},
		{"band-missing-unequal", `BAND<>20m`, spot.Spot{}, "", false},
		{"band-missing-not", `NOT BAND=20m`, spot.Spot{}, "", true},
		{"mode-blank", `MODE=UNKNOWN`, spot.Spot{}, "", true},
		{"mode-variant", `MODE=PSK`, spot.Spot{Mode: "PSK31"}, "", true},
		{"mode-not-SSB", `MODE=USB`, spot.Spot{Mode: "LSB"}, "", false},
		{"snr-present-zero", `SNR>=0`, spot.Spot{HasReport: true}, "", true},
		{"snr-missing", `SNR>=0`, spot.Spot{}, "", false},
		{"snr-missing-unequal", `SNR<>0`, spot.Spot{}, "", false},
		{"snr-missing-not", `NOT SNR>=0`, spot.Spot{}, "", true},
		{"snr-human-present-zero", `SNR<6`, spot.Spot{HasReport: true, IsHuman: true}, "", true},
		{"source-human", `SOURCE=HUMAN`, spot.Spot{IsHuman: true}, "", true},
		{"source-skimmer", `SOURCE=SKIMMER`, spot.Spot{}, "", true},
		{"source-explicit-exemption", `SOURCE=HUMAN OR SNR>=6`, spot.Spot{IsHuman: true}, "", true},
		{"event-none", `EVENT=NONE`, spot.Spot{}, "", true},
		{"event-untagged", `EVENT=POTA`, spot.Spot{}, "", false},
		{"event-untagged-unequal", `EVENT<>POTA`, spot.Spot{}, "", true},
		{"event-none-unequal", `EVENT<>NONE`, spot.Spot{}, "", false},
		{"event-multi-equal", `EVENT=POTA`, spot.Spot{Events: spot.EventPOTA | spot.EventSOTA}, "", true},
		{"event-multi-unequal", `EVENT<>POTA`, spot.Spot{Events: spot.EventPOTA | spot.EventSOTA}, "", false},
		{"event-list-equal", `EVENT=NONE,IOTA,SOTA`, spot.Spot{Events: spot.EventPOTA | spot.EventSOTA}, "", true},
		{"event-list-unequal", `EVENT<>IOTA,WWFF`, spot.Spot{Events: spot.EventPOTA | spot.EventSOTA}, "", true},
		{"conf-exempt", `CONF=?`, spot.Spot{Mode: "MSK144"}, "", false},
		{"conf-exempt-unequal", `CONF<>?`, spot.Spot{Mode: "MSK144"}, "", false},
		{"conf-exempt-not", `NOT CONF=?`, spot.Spot{Mode: "MSK144"}, "", true},
		{"conf-blank", `CONF=?`, spot.Spot{Mode: "CW"}, "", true},
		{"conf-invalid", `CONF<>P`, spot.Spot{Mode: "CW", Confidence: "bad"}, "", false},
		{"conf-percentage", `CONF=P`, spot.Spot{Mode: "CW", Confidence: "50%"}, "", true},
		{"path-closed-not-unlikely", `PATH=UNLIKELY`, spot.Spot{}, PathClassClosed, false},
		{"path-closed", `PATH=CLOSED`, spot.Spot{}, PathClassClosed, true},
		{"path-unavailable", `PATH=INSUFFICIENT`, spot.Spot{}, "", true},
		{"path-invalid-unavailable", `PATH=INSUFFICIENT`, spot.Spot{}, "bogus", true},
		{"path-normalized", `PATH=HIGH`, spot.Spot{}, " high ", true},
		{"zone-boundary", `DXZONE>=14`, spot.Spot{DXMetadata: spot.CallMetadata{CQZone: 14}}, "", true},
		{"zone-missing-ne", `DXZONE<>14`, spot.Spot{}, "", false},
		{"zone-out-of-range", `DEZONE<14`, spot.Spot{DEMetadata: spot.CallMetadata{CQZone: 41}}, "", false},
		{"adif-missing-ne", `DXDXCC<>291`, spot.Spot{}, "", false},
		{"adif-positive", `DXDXCC=291`, spot.Spot{DXMetadata: spot.CallMetadata{ADIF: 291}}, "", true},
		{"de-stripped", `DECALL=N2ABC`, spot.Spot{DECall: "N2ABC-9", DECallStripped: "N2ABC"}, "", true},
		{"de-fallback", `DECALL=N2ABC-9`, spot.Spot{DECall: "N2ABC-9"}, "", true},
		{"call-prefix", `DXCALL=K1*`, spot.Spot{DXCall: "k1abc"}, "", true},
		{"call-suffix", `DXCALL=*ABC`, spot.Spot{DXCall: "K1ABC"}, "", true},
		{"call-star", `DXCALL=*`, spot.Spot{DXCall: "K1ABC"}, "", true},
		{"call-missing-star", `DXCALL=*`, spot.Spot{}, "", false},
		{"call-missing-ne", `DECALL<>N2ABC`, spot.Spot{}, "", false},
		{"comment-literal", `COMMENT="POTA * ?"`, spot.Spot{Comment: "CQ pOtA * ? UP"}, "", true},
		{"comment-no-regex", `COMMENT="POTA.*"`, spot.Spot{Comment: "CQ POTA up"}, "", false},
		{"comment-regex-literal", `COMMENT="POTA.*"`, spot.Spot{Comment: "CQ pota.* UP"}, "", true},
		{"comment-list", `COMMENT="SOTA","POTA * ?"`, spot.Spot{Comment: "pota * ?"}, "", true},
		{"comment-list-ne", `COMMENT<>"SOTA","POTA"`, spot.Spot{Comment: "CQ POTA"}, "", false},
		{"comment-missing-ne", `COMMENT<>"POTA"`, spot.Spot{}, "", false},
		{"comment-not-missing", `NOT COMMENT="POTA"`, spot.Spot{}, "", true},
		{"comment-exact-spaces", `COMMENT=" POTA "`, spot.Spot{Comment: "POTA"}, "", false},
		{"comment-non-ASCII-barrier", `COMMENT="POTA"`, spot.Spot{Comment: "POéTA"}, "", false},
		{"frequency-no-epsilon", `FREQ>7000.0099999995`, spot.Spot{Frequency: 7000.01}, "", true},
	}
	for i := range tests {
		test := &tests[i]
		t.Run(test.name, func(t *testing.T) {
			program, err := compileRule(test.source)
			if err != nil {
				t.Fatal(err)
			}
			view := newRuleView(&test.spot, test.path)
			if got := program.evaluate(&view, program.syntax.Root); got != test.want {
				t.Fatalf("%s got %v, want %v", test.source, got, test.want)
			}
		})
	}
}

func TestRuleNumericBoundaries(t *testing.T) {
	for _, test := range []struct {
		source string
		number float64
		want   bool
	}{
		{"FREQ=7000-7100", 7000, true},
		{"FREQ=7000-7100", 7100, true},
		{"FREQ=7000-7100", math.Nextafter(7000, 0), false},
		{"FREQ=7000-7100", math.Nextafter(7100, math.Inf(1)), false},
		{"FREQ<>7000-7100,7200", 7100, false},
		{"FREQ<>7000-7100,7200", 7150, true},
		{"FREQ<7000.01", 7000.01, false},
		{"FREQ<=7000.01", 7000.01, true},
		{"FREQ>7000.01", 7000.01, false},
		{"FREQ>=7000.01", 7000.01, true},
		{"FREQ=7000.01", math.Nextafter(7000.01, math.Inf(1)), false},
		{"FREQ<>0", 0, false},
		{"FREQ<>7000", -1, false},
		{"FREQ<>7000", math.NaN(), false},
		{"FREQ<>7000", math.Inf(1), false},
		{"FREQ<>7000", math.Inf(-1), false},
	} {
		t.Run(test.source, func(t *testing.T) {
			program, err := compileRule(test.source)
			if err != nil {
				t.Fatal(err)
			}
			s := spot.Spot{Frequency: test.number}
			view := newRuleView(&s, "")
			if got := program.evaluate(&view, program.syntax.Root); got != test.want {
				t.Fatalf("%s on %.17g got %v, want %v", test.source, test.number, got, test.want)
			}
		})
	}
	for _, test := range []struct {
		source string
		want   bool
	}{
		{"SNR<6", false}, {"SNR<=6", true}, {"SNR>6", false}, {"SNR>=6", true},
		{"SNR=0,6", true}, {"SNR<>0,6", false}, {"SNR<>0,7", true},
	} {
		t.Run(test.source, func(t *testing.T) {
			program, err := compileRule(test.source)
			if err != nil {
				t.Fatal(err)
			}
			s := spot.Spot{HasReport: true, Report: 6}
			view := newRuleView(&s, "")
			if got := program.evaluate(&view, program.syntax.Root); got != test.want {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}

func TestRuleUnknownMetadata(t *testing.T) {
	for _, field := range []string{"DXCONT", "DECONT", "DXSTATE", "DESTATE", "DXGRID2", "DEGRID2"} {
		operand := "NA"
		switch field {
		case "DXSTATE", "DESTATE":
			operand = "CT"
		case "DXGRID2", "DEGRID2":
			operand = "FN"
		}
		spots := []spot.Spot{
			{},
			{
				DXMetadata: spot.CallMetadata{Continent: "ZZ", State: "ct", Grid: "SS31"},
				DEMetadata: spot.CallMetadata{Continent: "ZZ", State: "ct", Grid: "SS31"},
			},
		}
		for i := range spots {
			s := &spots[i]
			program, err := compileRule(field + "<>" + operand)
			if err != nil {
				t.Fatal(err)
			}
			view := newRuleView(s, "")
			if program.evaluate(&view, program.syntax.Root) {
				t.Fatalf("unknown %s passed <>", field)
			}
		}
	}
}

func TestRuleLazyView(t *testing.T) {
	for _, source := range []string{
		`MODE=CW OR COMMENT="POTA"`,
		`NOT (MODE=CW AND COMMENT="POTA")`,
		`MODE=CW OR MODE=FT8`,
	} {
		program, err := compileRule(source)
		if err != nil {
			t.Fatal(err)
		}
		s := spot.Spot{Mode: "FT8", ModeNorm: "FT8", Comment: "POTA"}
		if source != `NOT (MODE=CW AND COMMENT="POTA")` {
			s.Mode, s.ModeNorm = "CW", "CW"
		}
		view := newRuleView(&s, "")
		if view.loaded != 0 {
			t.Fatal("view normalized fields eagerly")
		}
		if !program.evaluate(&view, program.syntax.Root) {
			t.Fatalf("short-circuit fixture %q must be true", source)
		}
		if view.loaded != uint32(1)<<ruleFieldMode {
			t.Fatalf("short circuit loaded unrelated fields: %b", view.loaded)
		}
		// A repeated field uses this evaluation's normalized value.
		if view.textValue(ruleFieldMode) != s.ModeNorm || view.loaded != uint32(1)<<ruleFieldMode {
			t.Fatal("repeated field did not reuse its local normalized value")
		}
		if canonical, err := program.syntax.Canonical(); err != nil || canonical != program.canonical {
			t.Fatal("evaluation mutated owned syntax")
		}
	}
}

const ruleAllFieldsSource = `BAND=20m AND MODE=FT8 AND SOURCE=SKIMMER AND EVENT=POTA AND ` +
	`DXCALL=K1* AND DXCALL=*ABC AND DECALL=N2ABC AND DXCONT=NA AND DECONT=NA AND ` +
	`DXZONE>=5 AND DEZONE<5 AND DXDXCC=291 AND DEDXCC=1 AND DXSTATE=CT AND DESTATE=ON AND ` +
	`DXGRID2=FN AND DEGRID2=EM AND FREQ=14000-14100 AND SNR>=0 AND CONF=P AND PATH=HIGH AND COMMENT="POTA * ?"`

func normalizedRuleTestSpot() *spot.Spot {
	s := spot.NewSpotNormalized("K1ABC", "N2ABC-9", 14025, "FT8")
	s.IsHuman = false
	s.DECallStripped = "N2ABC"
	s.DXMetadata = spot.CallMetadata{Continent: "NA", CQZone: 5, ADIF: 291, State: "CT", Grid: "FN31"}
	s.DEMetadata = spot.CallMetadata{Continent: "NA", CQZone: 4, ADIF: 1, State: "ON", Grid: "EM10"}
	s.HasReport = true
	s.Report = 0
	s.Events = spot.EventPOTA | spot.EventSOTA
	s.Confidence = "P"
	s.Comment = "CQ pOtA * ? UP"
	s.EnsureNormalized()
	return s
}

func TestRuleEvaluateNormalizedFields(t *testing.T) {
	program, err := compileRule(ruleAllFieldsSource)
	if err != nil {
		t.Fatal(err)
	}
	s := normalizedRuleTestSpot()
	view := newRuleView(s, PathClassHigh)
	if !program.evaluate(&view, program.syntax.Root) {
		t.Fatal("fully populated normalized spot must satisfy every field")
	}
	for _, source := range []string{
		`BAND<>40m,20m`, `MODE<>CW,FT8`, `DXCALL<>W1*,*ABC`, `DECALL<>N2ABC,N3ABC`,
		`DXCONT<>EU,NA`, `DESTATE<>CT,ON`, `DXZONE<>4,5`, `DEDXCC<>291,1`,
		`DXGRID2<>EM,FN`, `CONF<>?,P`, `PATH<>UNLIKELY,HIGH`,
	} {
		program, err := compileRule(source)
		if err != nil {
			t.Fatal(err)
		}
		view := newRuleView(s, PathClassHigh)
		if program.evaluate(&view, program.syntax.Root) {
			t.Errorf("%s must reject a member of its inequality list", source)
		}
	}
}
