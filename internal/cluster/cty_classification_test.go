package cluster

import (
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestShippedCTYCountryFilterClassification(t *testing.T) {
	db := loadShippedClusterCTY(t)
	cache := newCallMetaCache(64, 0)
	s := spot.NewSpotNormalized("K1DG", "K1D", 14025, "CW")
	s.DXMetadata = metadataFromPrefix(effectivePrefixInfo(db, cache, "K1DG"))
	s.DEMetadata = metadataFromPrefix(effectivePrefixInfo(db, cache, "K1D"))
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{"DXDXCC=291 AND DEDXCC=202", true},
		{"DXDXCC=202", false},
		{"DEDXCC=291", false},
	} {
		rules, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: tc.expr}})
		if err != nil {
			t.Fatal(err)
		}
		if got := rules.Matches(s, filter.EvaluationContext{}); got != tc.want {
			t.Fatalf("%s = %v, want %v; DX=%+v DE=%+v", tc.expr, got, tc.want, s.DXMetadata, s.DEMetadata)
		}
	}
}
