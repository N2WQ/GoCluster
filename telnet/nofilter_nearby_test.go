package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestNoFilterRetainsUserGridAndRetiresNearbyBothDialects(t *testing.T) {
	requireH3Mappings(t)
	fine, coarse := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
	if fine == pathreliability.InvalidCell || coarse == pathreliability.InvalidCell {
		t.Fatal("FN31 test cells unavailable")
	}
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, snapshot := range []bool{false, true} {
			c := configurationTestClient(presetTestServer(t), "W1ABC-1")
			c.dialect, c.grid, c.gridCell, c.gridCoarseCell = dialect, "FN31", fine, coarse
			c.configuredSettings = filter.SettingsConfiguration{Dialect: string(dialect), Grid: "FN31", DedupePolicy: "FAST"}
			c.configurationInitialized = true
			c.filter.SetDXContinent("EU", true)
			candidate := spot.NewSpot("K1ABC", "VE3XYZ", 14030, "CW")
			candidate.DXMetadata.Continent, candidate.DEMetadata.Continent = "NA", "NA"
			candidate.DXMetadata.Grid, candidate.DEMetadata.Grid = "EM10", "EM10"
			if c.filter.Matches(candidate) {
				t.Fatal("restrictive before-control did not hide candidate")
			}
			if err := c.filter.EnableNearby(fine, coarse); err != nil {
				t.Fatal(err)
			}
			if !snapshot {
				c.filter.NearbySnapshot = nil // Loaded active state need not have an ON snapshot.
			}
			pointer := c.filter
			command := "PASS NOFILTER"
			if dialect == DialectCC {
				command = "SET/NOFILTER"
			}
			response, handled := newFilterCommandEngine().Handle(c, command)
			if !handled || !strings.Contains(response, "All filters disabled") || !c.filter.Matches(candidate) {
				t.Fatalf("%s failed effective reset: %q", command, response)
			}
			if c.filter != pointer || c.grid != "FN31" || c.gridCell != fine || c.gridCoarseCell != coarse || c.filter.NearbySnapshot != nil || c.filter.NearbyActive() || c.filter.NearbyUserFine != pathreliability.InvalidCell || c.filter.NearbyUserCoarse != pathreliability.InvalidCell {
				t.Fatal("NOFILTER changed ownership/user cells or retained NEARBY runtime state")
			}
			// Inspect command persistence before any disconnect or autosave.
			record, err := filter.LoadUserRecord(c.callsign)
			if err != nil || record.Grid != "FN31" || record.NearbyEnabled || !record.Matches(candidate) {
				t.Fatalf("command reset not immediately durable: record=%+v err=%v", record, err)
			}
		}
	}
}
