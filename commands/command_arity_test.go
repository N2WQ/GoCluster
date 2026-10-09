package commands

import (
	"strings"
	"testing"

	"dxcluster/cty"
)

func TestDXCCQueryArityBeforeLookup(t *testing.T) {
	db := loadTestCTY(t)
	lookups := 0
	p := NewProcessor(nil, nil, nil, func() *cty.CTYDatabase { lookups++; return db }, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, verb := range []string{"SHOW", "SH"} {
			for _, tail := range []string{"", " IT9 BAD", " W6/LZ5VV/M EXTRA MORE"} {
				lookups = 0
				response := p.ProcessCommandForClient(verb+" DXCC"+tail, "N2WQ", "", nil, dialect)
				if response != "Usage: SHOW DXCC <prefix|callsign>\n" || lookups != 0 {
					t.Fatalf("wrong arity reached CTY or lacked usage: %s %q -> %q, lookups=%d", dialect, tail, response, lookups)
				}
			}
			for _, tc := range []struct{ query, result string }{
				{"IT9", "IT9 -> ADIF 248 | Sicily (EU)"},
				{"W6/LZ5VV/M", "W6/LZ5VV -> ADIF 291 | United States (NA)"},
			} {
				lookups = 0
				response := p.ProcessCommandForClient(verb+" DXCC "+tc.query, "N2WQ", "", nil, dialect)
				if !strings.Contains(response, tc.result) || lookups == 0 {
					t.Fatalf("valid lookup changed: %s %s -> %q, lookups=%d", dialect, tc.query, response, lookups)
				}
			}
		}
	}
}

func TestLogoutCommandArity(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, alias := range []string{"BYE", "QUIT", "EXIT"} {
			for _, tail := range []string{" NOW", " ALL MORE", "\tEXTRA"} {
				command := strings.ToLower(alias) + tail
				if response := p.ProcessCommandForClient(command, "N2WQ", "", nil, dialect); response != "Usage: "+alias+"\n" {
					t.Fatalf("invalid logout returned sentinel or lacked usage: %s %q -> %q", dialect, command, response)
				}
			}
			if response := p.ProcessCommandForClient("  "+strings.ToLower(alias)+"  ", "N2WQ", "", nil, dialect); response != "BYE" {
				t.Fatalf("valid logout lost close sentinel: %s %s -> %q", dialect, alias, response)
			}
		}
	}
}
