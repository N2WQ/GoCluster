package uls

import (
	"strings"
	"testing"
)

func TestAllowlistMatchByJurisdiction(t *testing.T) {
	content := `
# comment
US:^N2WQ$
ADIF212:^LZ
W1AW
`
	al, err := LoadAllowlist(strings.NewReader(content))
	if err != nil {
		t.Fatalf("load allowlist: %v", err)
	}
	allowlist.Store(al)
	defer allowlist.Store(nil)

	if !AllowlistMatch(291, "N2WQ") {
		t.Fatalf("expected US allowlist to match N2WQ")
	}
	if AllowlistMatch(291, "K1ABC") {
		t.Fatalf("did not expect allowlist to match K1ABC")
	}
	if !AllowlistMatch(291, "W1AW") {
		t.Fatalf("expected default jurisdiction allowlist to match W1AW")
	}
	if !AllowlistMatch(212, "LZ5VV") {
		t.Fatalf("expected ADIF212 allowlist to match LZ5VV")
	}
}

func TestFCCAllowlistCoverageAndNamedEntityIsolation(t *testing.T) {
	al, err := LoadAllowlist(strings.NewReader("US:K1ABC\nK2ABC\nADIF6:KL1ABC\nADIF291:K3ABC\nADIF105:KG4ABC\n"))
	if err != nil {
		t.Fatal(err)
	}
	previous := allowlist.Load()
	allowlist.Store(al)
	defer allowlist.Store(previous)
	// This literal expected list is independent of the production predicate.
	for _, adif := range []int{6, 9, 20, 43, 103, 110, 123, 138, 166, 174, 182, 197, 202, 285, 291, 297, 515} {
		for _, call := range []string{"K1ABC", "K2ABC"} {
			if !AllowlistMatch(adif, call) {
				t.Fatalf("US/default entry missing for ADIF%d %s", adif, call)
			}
		}
		if AllowlistMatch(adif, "KL1ABC") != (adif == 6) {
			t.Fatalf("ADIF6 entry leaked or missing for ADIF%d", adif)
		}
		if AllowlistMatch(adif, "K3ABC") != (adif == 291) {
			t.Fatalf("ADIF291 entry leaked or missing for ADIF%d", adif)
		}
	}
	for _, adif := range []int{0, 1, 105, 134, 230} {
		for _, call := range []string{"K1ABC", "K2ABC", "KL1ABC", "K3ABC"} {
			if AllowlistMatch(adif, call) {
				t.Fatalf("FCC exception leaked to excluded ADIF%d %s", adif, call)
			}
		}
	}
	if !AllowlistMatch(105, "KG4ABC") || AllowlistMatch(6, "KG4ABC") {
		t.Fatal("explicit non-FCC ADIF exception lost isolation")
	}
}
