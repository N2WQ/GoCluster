package telnet

import (
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestNativeCCHistorySnapshotOwnsCriteriaControlsAndDialect(t *testing.T) {
	c, _, _ := nativeCCCommandFixture(t)
	configuration := nativeCCConfiguration(t, c)
	configuration.Modes = []string{"CW"}
	configuration.Geography["DOC"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCPass, Countries: []int{291}}}
	configuration.AllowToxic = filter.DefaultBoolTrue
	_, value, err := filter.NewCCFilter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = value
	snapshot := c.captureHistoryFilter()
	sp := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
	sp.DEMetadata.ADIF, sp.ToxicityStatus = 291, spot.ToxicityToxic
	if snapshot.client.dialect != DialectCC || !snapshot.matches(nil, sp) {
		t.Fatal("history capture lost native plan or selected dialect")
	}
	*c.filter.AllowToxic = false
	if !snapshot.matches(nil, sp) || c.captureHistoryFilter().matches(nil, sp) {
		t.Fatal("native history borrowed a mutable six-control pointer")
	}
	configuration.Modes = []string{"FT8"}
	_, value, err = filter.NewCCFilter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = value
	c.dialect = DialectGo
	if !snapshot.matches(nil, sp) || c.captureHistoryFilter().matches(nil, sp) || snapshot.digest == c.historyFilterDigest() {
		t.Fatal("live native mutation changed a captured page or escaped invalidation")
	}
}

func TestNativeCCHistoryDigestExcludesInactiveAndIrrelevantState(t *testing.T) {
	c, _, _ := nativeCCCommandFixture(t)
	before := c.historyFilterDigest()
	no := false
	c.filter.AllowWWV, c.filter.AllowWCY, c.filter.AllowAnnounce, c.filter.AllowSelf = &no, &no, &no, &no
	c.grid, c.gridDerived, c.gridCell, c.gridCoarseCell = "IO91", true, 100, 200
	c.noiseClass, c.pathMinObservationCount = "SUBURBAN", 123
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "cc", Grid: "FN31", DedupePolicy: "SLOW", SolarSummaryMinutes: 60}
	c.setSolarSummaryMinutes(60, time.Unix(1700000000, 0))
	if before != c.historyFilterDigest() || before != c.captureHistoryFilter().digest {
		t.Fatal("native history was invalidated by unused path/shared/self/bulletin state")
	}
	for _, mutation := range []string{"mode", "source", "geography", "frequency", "beacon", "toxic", "dialect"} {
		t.Run(mutation, func(t *testing.T) {
			c, _, _ := nativeCCCommandFixture(t)
			before := c.historyFilterDigest()
			configuration := nativeCCConfiguration(t, c)
			switch mutation {
			case "mode":
				configuration.Modes = []string{"FT8"}
			case "source":
				configuration.Sources = []string{"SKIMMER"}
			case "geography":
				configuration.Geography["DXCTY"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCReject, Countries: []int{291}}}
			case "frequency":
				configuration.DXBM = filter.CCDXBM{Active: true, BaseAllow: false, Slots: map[string]bool{"20-CW": true}}
			case "beacon":
				configuration.IncludeBeacons = filter.DefaultBoolTrue
			case "toxic":
				configuration.AllowToxic = filter.DefaultBoolTrue
			case "dialect":
				c.dialect = DialectGo
			}
			_, value, err := filter.NewCCFilter(configuration)
			if err != nil {
				t.Fatal(err)
			}
			*c.filter = value
			if before == c.historyFilterDigest() {
				t.Fatal("native matching/activation change was omitted from history digest")
			}
		})
	}
}

func TestNativeCCHistoryInactiveRestoreAndBankABA(t *testing.T) {
	s, c := v7MachineTestClient(t)
	c.refreshConfigurationRevision()
	cursor := &historySearch{token: "existing-search"}
	c.historySearch = cursor
	generation := c.historyGeneration
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	next := before.Clone()
	next.Filters.GO.Rules = []filter.RuleDefinition{{Slot: 1, Action: filter.RulePass, Expr: "MODE=FT8"}}
	prepared, err := s.prepareV7ConfigurationUpdate(c, before, next, s.now())
	if err != nil {
		t.Fatal(err)
	}
	s.publishV7Configuration(c, next, prepared, c.v7PresetReferences, s.now())
	if c.historySearch != cursor || c.historyGeneration != generation {
		t.Fatal("inactive GO-only full restore invalidated the active CC history cursor")
	}
	for _, selected := range []string{"go", "cc"} {
		before, err = c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		next = before.Clone()
		next.Settings.Dialect = selected
		prepared, err = s.prepareV7ConfigurationUpdate(c, before, next, s.now())
		if err != nil {
			t.Fatal(err)
		}
		s.publishV7Configuration(c, next, prepared, c.v7PresetReferences, s.now())
		generation++
		if c.historySearch != nil || c.historyGeneration != generation {
			t.Fatal("bank activation/restoration revived an earlier continuation")
		}
	}
}
