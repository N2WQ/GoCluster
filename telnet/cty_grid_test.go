package telnet

import (
	"path/filepath"
	"testing"

	"dxcluster/cty"
)

func TestShowPropShippedCTYCoordinates(t *testing.T) {
	requireH3Mappings(t)
	db, err := cty.LoadCTYDatabase(filepath.Join("..", "data", "cty", "cty.plist"))
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ctyLookup: func() *cty.CTYDatabase { return db }}
	for _, tc := range []struct{ input, grid string }{
		{"K1ABC", "FN42"}, {"W6ABC", "DM05"}, {"JA1ABC", "PM95"},
		{"VE3", "FN03"}, {"VK", "PG66"}, {"VK2ZZZ", "QF56"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			target, ok := s.resolveShowPropTarget(tc.input)
			if !ok || target.grid != tc.grid || target.source != "cty-derived" {
				t.Fatalf("%s = %+v/%v, want %s/cty-derived", tc.input, target, ok, tc.grid)
			}
		})
	}
}
