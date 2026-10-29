package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestResetFilterRetainsUserGridAndRetiresNearbyBothDialects(t *testing.T) {
	requireH3Mappings(t)
	fine, coarse := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
	if fine == pathreliability.InvalidCell || coarse == pathreliability.InvalidCell {
		t.Fatal("FN31 test cells unavailable")
	}
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		c := configurationTestClient(presetTestServer(t), "W1ABC-1")
		c.dialect, c.grid, c.gridCell, c.gridCoarseCell = dialect, "FN31", fine, coarse
		c.configuredSettings = filter.SettingsConfiguration{Dialect: string(dialect), Grid: "FN31", DedupePolicy: "FAST"}
		c.configurationInitialized = true
		c.filter = passRuleTestFilter(t, "NEARBY AND DXCONT=EU")
		candidate := spot.NewSpot("K1ABC", "VE3XYZ", 14030, "CW")
		candidate.DXMetadata.Continent, candidate.DEMetadata.Continent = "NA", "NA"
		candidate.DXMetadata.Grid, candidate.DEMetadata.Grid = "EM10", "EM10"
		if c.filter.Matches(candidate) {
			t.Fatal("restrictive before-control did not hide candidate")
		}
		pointer := c.filter
		programs := c.filter.PreparedRules()
		command := "RESET FILTER"
		response, handled := newFilterCommandEngine().Handle(c, command)
		if !handled || !strings.Contains(response, "cleared") || !c.filter.Matches(candidate) ||
			len(c.filter.PreparedRules().Definitions()) != 0 {
			t.Fatalf("%s failed effective reset: %q", command, response)
		}
		if c.filter != pointer || c.grid != "FN31" || c.gridCell != fine || c.gridCoarseCell != coarse ||
			programs.Definitions()[0].Expr != "NEARBY AND DXCONT=EU" {
			t.Fatal("RESET changed ownership/user cells/frozen rules or retained NEARBY runtime state")
		}
		// Inspect command persistence before any disconnect or autosave.
		record, err := filter.LoadUserRecord(c.callsign)
		if err != nil || record.Grid != "FN31" || !record.Matches(candidate) {
			t.Fatalf("command reset not immediately durable: record=%+v err=%v", record, err)
		}
	}
}
