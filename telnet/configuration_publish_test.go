package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

func publishTestClient(t *testing.T, s *Server) (*Client, filter.Configuration, time.Time) {
	t.Helper()
	requireH3Mappings(t)
	c := newTestClient()
	c.server, c.callsign = s, "N2WQ-1"
	c.grid, c.gridDerived, c.noiseClass = "FN31", true, "QUIET"
	c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
	c.setDedupePolicy(dedupePolicyMed)
	c.filter = passRuleTestFilter(t, "DXCONT=EU AND DXCALL=K1*,W1* AND NEARBY")
	now := time.Date(2026, 10, 5, 14, 17, 0, 0, time.UTC)
	c.setSolarSummaryMinutes(30, now)
	c.setDiagMode(diagModeSource)
	c.readPauseUntilUnixNano.Store(now.Add(10 * time.Minute).UnixNano())
	c.readPauseDiscardBefore.Store(1234)
	c.readPauseSuppressed.Store(9)
	c.readPausePending.Store(true)
	c.readPauseEpoch = 7
	before, err := c.captureConfiguration(maxYAMLBytes)
	if err != nil {
		t.Fatal(err)
	}
	if before.Settings.Grid != "" {
		t.Fatal("lookup grid became a configured preference")
	}
	if _, err := c.configurationRevisionToken(); err != nil {
		t.Fatal(err)
	}
	return c, before, now
}

func TestPreparePreservesExactGridOwnedCells(t *testing.T) {
	for _, tc := range []struct {
		name         string
		fine, coarse pathreliability.CellID
	}{
		{"both", 11, 22}, {"fine-only", 11, 0}, {"coarse-only", 0, 22}, {"neither", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := presetTestServer(t)
			c, before, now := publishTestClient(t, s)
			c.gridCell, c.gridCoarseCell = tc.fine, tc.coarse
			state, pointer := c.pathSnapshot(), c.filter
			s.gridLookup = func(string) (string, bool, bool) { t.Fatal("unchanged GRID triggered lookup"); return "", false, false }
			prepared, warning := s.prepareConfigurationUpdate(c, before, before.Clone(), now)
			if warning != "" || prepared.pathSnapshot() != state {
				t.Fatal("preparation changed captured location")
			}
			if c.filter != pointer || c.pathSnapshot() != state {
				t.Fatal("preparation mutated live state")
			}
			s.publishConfiguration(c, before.Clone(), prepared, nil, now)
			if c.pathSnapshot() != state || c.filter != pointer {
				t.Fatal("unchanged publication erased partial location")
			}
		})
	}
}

func TestPublishUnchangedConfigurationPreservesRuntime(t *testing.T) {
	s := presetTestServer(t)
	c, before, now := publishTestClient(t, s)
	s.gridLookup = func(string) (string, bool, bool) {
		t.Fatal("unchanged GRID performed a fresh lookup")
		return "", false, false
	}
	baseline, err := before.Preset()
	if err != nil {
		t.Fatal(err)
	}
	ref := &filter.PresetReference{Name: "CONTEST", Baseline: baseline}
	c.presetReference = ref
	pointer, programs, state := c.filter, c.filter.PreparedRules(), c.pathSnapshot()
	revision := c.configurationRevision
	prepared, warning := s.prepareConfigurationUpdate(c, before, before.Clone(), now)
	if warning != "" {
		t.Fatalf("unchanged update emitted a warning: %q", warning)
	}
	// A solar broadcaster can advance the tick while persistence is in flight.
	c.advanceSolarSummaryAt(now.Add(30 * time.Minute))
	expectedTick := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	s.publishConfiguration(c, before.Clone(), prepared, ref, now)
	if c.filter != pointer || c.filter.PreparedRules() != programs || c.pathSnapshot() != state {
		t.Fatal("unchanged publication reset GRID/NEARBY runtime state")
	}
	if !c.solarNextSummaryAt.Equal(expectedTick) || c.getSolarSummaryMinutes() != 30 {
		t.Fatalf("unchanged publication reset the advanced solar tick: %v", c.solarNextSummaryAt)
	}
	if c.getDiagMode() != diagModeSource || c.readPauseEpoch != 7 || !c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != now.Add(10*time.Minute).UnixNano() || c.readPauseDiscardBefore.Load() != 1234 || c.readPauseSuppressed.Load() != 9 {
		t.Fatal("publication changed diagnostic or pause state")
	}
	if c.presetReference != ref || c.configurationRevision != revision {
		t.Fatal("unchanged publication changed reference/revision")
	}
}

func TestPublishChangedRulePreservesUnrelatedRuntime(t *testing.T) {
	s := presetTestServer(t)
	c, before, now := publishTestClient(t, s)
	replacement := passRuleTestFilter(t, "DXCONT=NA AND DXCALL=W1*")
	next := filter.ConfigurationFromFilter(replacement, before.Settings)
	previous, state, tick, revision := c.filter.PreparedRules(), c.pathSnapshot(), c.solarNextSummaryAt, c.configurationRevision
	prepared, _ := s.prepareConfigurationUpdate(c, before, next, now)
	s.publishConfiguration(c, next, prepared, nil, now)
	if c.filter.PreparedRules().Definitions()[0].Expr != "DXCONT=NA AND DXCALL=W1*" || c.configurationRevision != revision+1 {
		t.Fatal("changed rule update lost definition or failed to advance revision")
	}
	if c.pathSnapshot() != state || c.solarNextSummaryAt != tick ||
		previous.Definitions()[0].Expr != "DXCONT=EU AND DXCALL=K1*,W1* AND NEARBY" {
		t.Fatal("changed-rule publication reset unrelated runtime state or mutated the old program")
	}
}

func TestPublishNoisePreservesNearbyRulesAndCells(t *testing.T) {
	s := presetTestServer(t)
	c, before, now := publishTestClient(t, s)
	next := before.Clone()
	next.Settings.NoiseClass = "URBAN"
	programs, state, tick := c.filter.PreparedRules(), c.pathSnapshot(), c.solarNextSummaryAt
	prepared, _ := s.prepareConfigurationUpdate(c, before, next, now)
	s.publishConfiguration(c, next, prepared, nil, now)
	if c.noiseClass != "URBAN" || c.filter.PreparedRules() != programs || c.solarNextSummaryAt != tick ||
		c.gridCell != state.gridCell || c.gridCoarseCell != state.gridCoarseCell {
		t.Fatal("noise change reset unrelated runtime state")
	}
	if c.filter.PreparedRules() != programs || programs.Definitions()[0].Expr != "DXCONT=EU AND DXCALL=K1*,W1* AND NEARBY" {
		t.Fatal("noise change discarded saved geographic rule")
	}
}

func TestPublishGeographyAndNearbyRuleRemainExact(t *testing.T) {
	s := presetTestServer(t)
	c, before, now := publishTestClient(t, s)
	replacement := passRuleTestFilter(t, "NEARBY AND DXCONT=NA")
	next := filter.ConfigurationFromFilter(replacement, before.Settings)
	prepared, _ := s.prepareConfigurationUpdate(c, before, next, now)
	if prepared.filter.PreparedRules() != replacement.PreparedRules() {
		t.Fatal("preparation discarded the replacement immutable rules")
	}
	s.publishConfiguration(c, next, prepared, nil, now)
	if c.filter.PreparedRules() != replacement.PreparedRules() || c.filter.PreparedRules().Definitions()[0].Expr != "NEARBY AND DXCONT=NA" {
		t.Fatal("publication did not retain the exact combined predicate")
	}
}

func TestPublishGridChangesNearbyCellsOnly(t *testing.T) {
	s := presetTestServer(t)
	c, before, now := publishTestClient(t, s)
	next := before.Clone()
	next.Settings.Grid = "IO91"
	programs := c.filter.PreparedRules()
	prepared, _ := s.prepareConfigurationUpdate(c, before, next, now)
	s.publishConfiguration(c, next, prepared, nil, now)
	if c.grid != "IO91" || c.gridDerived || c.filter.PreparedRules() != programs || c.gridCell != pathreliability.EncodeCell("IO91") || c.gridCoarseCell != pathreliability.EncodeCoarseCell("IO91") {
		t.Fatal("GRID change discarded immutable rules or failed to replace derived cells")
	}
}

func TestPublishSolarChangesScheduleFromPublication(t *testing.T) {
	s := presetTestServer(t)
	c, before, now := publishTestClient(t, s)
	next := before.Clone()
	next.Settings.SolarSummaryMinutes = 15
	prepared, _ := s.prepareConfigurationUpdate(c, before, next, now)
	publication := time.Date(2026, 10, 5, 14, 46, 0, 0, time.UTC)
	s.publishConfiguration(c, next, prepared, nil, publication)
	if c.solarNextSummaryAt != time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC) {
		t.Fatalf("changed solar cadence used preparation time: %v", c.solarNextSummaryAt)
	}
}

func TestPresetAcknowledgementWireBoundaries(t *testing.T) {
	for _, tc := range []struct {
		parts []string
		fits  bool
	}{
		{[]string{strings.Repeat("A", 65536)}, true},
		{[]string{strings.Repeat("A", 65537)}, false},
		{[]string{strings.Repeat("A", 65534), "\n"}, true},
		{[]string{strings.Repeat("A", 65535), "\n"}, false},
		{[]string{strings.Repeat("A", 65534) + "\r", "\n"}, true},
	} {
		if presetAcknowledgementFits(tc.parts...) != tc.fits {
			t.Fatalf("wrong wire boundary for %d parts", len(tc.parts))
		}
	}
}
