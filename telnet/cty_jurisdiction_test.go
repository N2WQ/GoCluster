package telnet

import (
	"path/filepath"
	"testing"

	"dxcluster/cty"
)

func TestShippedExactOnlyLicenseJurisdiction(t *testing.T) {
	db, err := cty.LoadCTYDatabase(filepath.Join("..", "data", "cty", "cty.plist"))
	if err != nil {
		t.Fatal(err)
	}
	for _, licensed := range []bool{false, true} {
		checked := 0
		s := &Server{
			ctyLookup: func() *cty.CTYDatabase { return db },
			usLicenseCheck: func(call string) bool {
				checked++
				if call != "K1DG" {
					t.Fatalf("wrong license identity: %s", call)
				}
				return licensed
			},
		}
		result := s.validateLoginCallsign("K1DG")
		if checked != 1 || result.valid != licensed || result.failOpen || (!licensed && result.reason != loginValidationReasonUSUnlicensed) {
			t.Fatalf("licensed=%v: checks=%d result=%+v", licensed, checked, result)
		}
	}
}
