package telnet

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

// These literal inventories are independent of the HELP catalog and parser.
// The real coordinator and byte commit establish acceptance and immediate
// persistence; no acknowledgement-only helper or teardown save is an oracle.
type ccHelpContract struct {
	client   *Client
	engine   *filterCommandEngine
	pointer  *filter.Filter
	settings filter.SettingsConfiguration
	baseline filter.Configuration
	writes   int
}

func newCCHelpContract(t *testing.T) *ccHelpContract {
	t.Helper()
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-17")
	c.dialect, c.grid, c.noiseClass, c.pathMinObservationCount = DialectCC, "FN31", "QUIET", 7
	c.setDedupePolicy(dedupePolicyFast)
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "QUIET", DedupePolicy: "FAST", PathMinObservationCount: 7}
	c.configurationInitialized = true
	c.filter.Reset()
	c.filter.AllModes, c.filter.Modes, c.filter.BlockModes = false, map[string]bool{"FT8": true}, map[string]bool{"CW": true}
	c.filter.Comments, c.filter.BlockComments = []string{"CONTROL"}, []string{"AVOID"}
	c.filter.MinSNR = map[string]int{"RTTY": 7}
	s.clients = map[string]*Client{c.callsign: c}
	baseline, err := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Preset()
	if err != nil {
		t.Fatal(err)
	}
	c.presetReference = &filter.PresetReference{Name: "BASE", Baseline: baseline}
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.configurationRevisionToken(); err != nil {
		t.Fatal(err)
	}
	x := &ccHelpContract{
		client: c, engine: newFilterCommandEngine(), pointer: c.filter, settings: c.configuredSettings,
		baseline: filter.ConfigurationFromPreset(baseline).Clone(),
	}
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
		x.writes++
		return prepared.Commit()
	}
	return x
}

func (x *ccHelpContract) configuration() filter.Configuration {
	return filter.ConfigurationFromFilter(x.client.filter, x.client.configuredSettings)
}

func (x *ccHelpContract) assertSaved(t *testing.T) {
	t.Helper()
	c := x.client
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	saved := filter.ConfigurationFromFilter(&record.Filter, filter.SettingsConfiguration{
		Dialect: record.Dialect, Grid: record.Grid, NoiseClass: record.NoiseClass, DedupePolicy: record.DedupePolicy,
		PathMinObservationCount: record.PathMinObservationCount, SolarSummaryMinutes: record.SolarSummaryMinutes,
	})
	if c.filter != x.pointer || c.configuredSettings != x.settings || c.dialect != DialectCC || c.grid != "FN31" ||
		!saved.Equal(x.configuration()) || c.presetReference == nil || c.presetReference.Name != "BASE" ||
		!x.baseline.Equal(filter.ConfigurationFromPreset(c.presetReference.Baseline)) || record.Preset == nil || record.Preset.Name != "BASE" ||
		!x.baseline.Equal(filter.ConfigurationFromPreset(record.Preset.Baseline)) {
		t.Fatal("command lost stable filter/settings/preset ownership or immediate exact saved configuration")
	}
}

func (x *ccHelpContract) accept(t *testing.T, command string) {
	t.Helper()
	writes := x.writes
	response, handled := x.engine.Handle(x.client, command)
	for _, failure := range []string{"Unknown", "Invalid", "Usage:", "No changes applied", "Warning:"} {
		if !handled || strings.Contains(response, failure) {
			t.Fatalf("advertised command refused: %q -> %q handled=%t", command, response, handled)
		}
	}
	if response == "" || x.writes != writes+1 {
		t.Fatalf("advertised mutation did not commit once: %q -> %q writes=%d", command, response, x.writes-writes)
	}
	x.assertSaved(t)
}

func (x *ccHelpContract) refuse(t *testing.T, command string) {
	t.Helper()
	c := x.client
	before, revision, writes := x.configuration().Clone(), c.configurationRevision, x.writes
	epoch, digest, historyDigest, generation := c.configurationEpoch, c.configurationDigest, c.historyDigest, c.historyGeneration
	path, snapshot := c.pathSnapshot(), c.filter.NearbySnapshot
	snapshotContents := fmt.Sprintf("%#v", snapshot)
	fine, coarse := c.filter.NearbyUserFine, c.filter.NearbyUserCoarse
	file := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	disk, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	response, handled := x.engine.Handle(c, command)
	classified := strings.Contains(response, "Unknown") || strings.Contains(response, "Invalid") ||
		strings.Contains(response, "Usage:") || strings.Contains(response, "No changes applied.") || response == nearbyLocationFilterWarning
	after, err := os.ReadFile(file)
	if !handled || !classified || x.writes != writes || !before.Equal(x.configuration()) || c.configurationRevision != revision ||
		c.configurationEpoch != epoch || c.configurationDigest != digest || c.historyDigest != historyDigest || c.historyGeneration != generation ||
		c.pathSnapshot() != path || c.filter.NearbySnapshot != snapshot || fmt.Sprintf("%#v", c.filter.NearbySnapshot) != snapshotContents ||
		c.filter.NearbyUserFine != fine || c.filter.NearbyUserCoarse != coarse || err != nil || !bytes.Equal(disk, after) {
		t.Fatalf("unsupported form was accepted or had effects: %q -> %q writes=%d err=%v", command, response, x.writes-writes, err)
	}
	x.assertSaved(t)
}

type ccHelpRuleState struct {
	allowAll, blockAll, allow, block bool
	allowCount, blockCount           int
}

func ccHelpMapState[K string | int](rule filter.RuleSet[K], key K) ccHelpRuleState {
	return ccHelpRuleState{rule.AllowAll, rule.BlockAll, rule.Allow[key], rule.Block[key], len(rule.Allow), len(rule.Block)}
}

func ccHelpCallState(allow, block []string, key string) ccHelpRuleState {
	blockAll := slices.Contains(block, "*")
	return ccHelpRuleState{len(allow) == 0 && !blockAll, blockAll, slices.Contains(allow, key), slices.Contains(block, key), len(allow), len(block)}
}

type ccHelpListCase struct {
	domain, value string
	state         func(filter.FilterConfiguration) ccHelpRuleState
}

func ccHelpLiteralLists() []ccHelpListCase {
	return []ccHelpListCase{
		{"BAND", "20", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.Bands, "20m") }},
		{"MODE", "CW", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.Modes, "CW") }},
		{"SOURCE", "HUMAN", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.Sources, "HUMAN") }},
		{"EVENT", "POTA", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.Events, "POTA") }},
		{"DXCALL", "K1*", func(f filter.FilterConfiguration) ccHelpRuleState {
			return ccHelpCallState(f.DXCallsigns, f.BlockDXCallsigns, "K1*")
		}},
		{"DECALL", "W2ABC", func(f filter.FilterConfiguration) ccHelpRuleState {
			return ccHelpCallState(f.DECallsigns, f.BlockDECallsigns, "W2ABC")
		}},
		{"DXGRID2", "FN", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DXGrid2, "FN") }},
		{"DEGRID2", "JO", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DEGrid2, "JO") }},
		{"DXCONT", "EU", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DXContinents, "EU") }},
		{"DECONT", "NA", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DEContinents, "NA") }},
		{"DXZONE", "14", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DXZones, 14) }},
		{"DEZONE", "5", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DEZones, 5) }},
		{"DXDXCC", "230", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DXDXCC, 230) }},
		{"DEDXCC", "291", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DEDXCC, 291) }},
		{"DXSTATE", "CT", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DXStates, "CT") }},
		{"DESTATE", "ON", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.DEStates, "ON") }},
		{"CONFIDENCE", "?", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.Confidence, "?") }},
		{"PATH", "HIGH", func(f filter.FilterConfiguration) ccHelpRuleState { return ccHelpMapState(f.PathClasses, "HIGH") }},
	}
}

func TestCCFilterHelpLiteralListAcceptance(t *testing.T) {
	for _, tc := range ccHelpLiteralLists() {
		t.Run(tc.domain, func(t *testing.T) {
			x := newCCHelpContract(t)
			for _, verb := range []string{"UNSET/FILTER", "SET/FILTER"} {
				x.accept(t, verb+" "+tc.domain+" "+tc.value)
				state := tc.state(x.configuration().Filters)
				wantAllow := verb == "SET/FILTER"
				if state.allow != wantAllow || state.block == wantAllow || state.blockAll ||
					!slices.Equal(x.client.filter.Comments, []string{"CONTROL"}) || !maps.Equal(x.client.filter.MinSNR, map[string]int{"RTTY": 7}) {
					t.Fatalf("literal named selection or unrelated rules changed: %s %+v", verb, state)
				}
			}
		})
	}
}

func TestCCFilterHelpListInlineResets(t *testing.T) {
	for _, tc := range ccHelpLiteralLists() {
		t.Run(tc.domain, func(t *testing.T) {
			x := newCCHelpContract(t)
			for _, flag := range []string{"OFF", "ON"} {
				x.accept(t, "SET/FILTER "+tc.domain+"/"+flag)
				state := tc.state(x.configuration().Filters)
				wantAllow, wantBlockCount := flag == "ON", 0
				if flag == "OFF" && (tc.domain == "DXCALL" || tc.domain == "DECALL") {
					wantBlockCount = 1 // The existing block-all representation is one literal *.
				}
				if state.allowAll != wantAllow || state.blockAll == wantAllow || state.allowCount != 0 || state.blockCount != wantBlockCount ||
					!slices.Equal(x.client.filter.Comments, []string{"CONTROL"}) || !maps.Equal(x.client.filter.MinSNR, map[string]int{"RTTY": 7}) {
					t.Fatalf("inline reset did not target its category: %s %+v", flag, state)
				}
			}
		})
	}
}

func ccHelpLiteralSwitches() []struct {
	domain string
	value  func(filter.FilterConfiguration) filter.DefaultBool
} {
	return []struct {
		domain string
		value  func(filter.FilterConfiguration) filter.DefaultBool
	}{
		{"BEACON", func(f filter.FilterConfiguration) filter.DefaultBool { return f.IncludeBeacons }},
		{"WWV", func(f filter.FilterConfiguration) filter.DefaultBool { return f.AllowWWV }},
		{"WCY", func(f filter.FilterConfiguration) filter.DefaultBool { return f.AllowWCY }},
		{"ANNOUNCE", func(f filter.FilterConfiguration) filter.DefaultBool { return f.AllowAnnounce }},
		{"PC93", func(f filter.FilterConfiguration) filter.DefaultBool { return f.AllowAnnounce }},
		{"SELF", func(f filter.FilterConfiguration) filter.DefaultBool { return f.AllowSelf }},
		{"TOXIC", func(f filter.FilterConfiguration) filter.DefaultBool { return f.AllowToxic }},
	}
}

func TestCCFilterHelpSwitchAcceptance(t *testing.T) {
	for _, tc := range ccHelpLiteralSwitches() {
		t.Run(tc.domain, func(t *testing.T) {
			x := newCCHelpContract(t)
			for _, suffix := range []string{"", " ALL"} {
				for _, verb := range []string{"UNSET/FILTER", "SET/FILTER"} {
					x.accept(t, verb+" "+tc.domain+suffix)
					want := filter.DefaultBoolFalse
					if verb == "SET/FILTER" {
						want = filter.DefaultBoolTrue
					}
					if tc.value(x.configuration().Filters) != want {
						t.Fatalf("switch action reversed or ignored: %s%s", verb, suffix)
					}
				}
			}
			for _, flag := range []string{"OFF", "ON"} {
				x.accept(t, "SET/FILTER "+tc.domain+"/"+flag)
				want := filter.DefaultBoolFalse
				if flag == "ON" {
					want = filter.DefaultBoolTrue
				}
				if tc.value(x.configuration().Filters) != want {
					t.Fatalf("inline switch flag reversed or ignored: %s", flag)
				}
			}
		})
	}
}

func TestCCFilterHelpMinSNRSpecialGrammar(t *testing.T) {
	x := newCCHelpContract(t)
	for _, verb := range []string{"SET/FILTER", "UNSET/FILTER"} {
		x.accept(t, verb+" MINSNR CW 10")
		if !maps.Equal(x.client.filter.MinSNR, map[string]int{"CW": 10, "RTTY": 7}) {
			t.Fatal("numeric UNSET must set the same inclusive minimum as SET")
		}
	}
	x.accept(t, "SET/FILTER MINSNR CW ALL")
	if !maps.Equal(x.client.filter.MinSNR, map[string]int{"RTTY": 7}) {
		t.Fatal("SET clear changed unselected thresholds")
	}
	x.accept(t, "SET/FILTER MINSNR CW -5")
	if !maps.Equal(x.client.filter.MinSNR, map[string]int{"CW": -5, "RTTY": 7}) {
		t.Fatal("signed numeric minimum did not preserve its exact value")
	}
	x.accept(t, "UNSET/FILTER MINSNR CW NONE")
	if !maps.Equal(x.client.filter.MinSNR, map[string]int{"RTTY": 7}) {
		t.Fatal("UNSET clear changed unselected thresholds")
	}
}

func TestCCFilterHelpNearbyValueOwnsBothVerbs(t *testing.T) {
	requireH3Mappings(t)
	for _, verb := range []string{"SET/FILTER", "UNSET/FILTER"} {
		t.Run(verb, func(t *testing.T) {
			x := newCCHelpContract(t)
			c := x.client
			c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
			if c.gridCell == pathreliability.InvalidCell || c.gridCoarseCell == pathreliability.InvalidCell {
				t.Fatal("real fine/coarse FN31 mappings are required")
			}
			c.filter.AllDXContinents, c.filter.DXContinents = false, map[string]bool{"EU": true}
			if err := c.saveFilter(); err != nil {
				t.Fatal(err)
			}
			x.accept(t, verb+" NEARBY ON")
			if !c.filter.NearbyActive() || c.filter.NearbySnapshot == nil || c.filter.NearbyUserFine != c.gridCell || c.filter.NearbyUserCoarse != c.gridCoarseCell {
				t.Fatal("explicit ON did not enable usable NEARBY independently of verb")
			}
			x.refuse(t, "SET/FILTER DXCONT/ON")
			x.refuse(t, "UNSET/FILTER DEZONE ALL")
			x.accept(t, verb+" NEARBY OFF")
			if c.filter.NearbyEnabled || c.filter.NearbySnapshot != nil || c.filter.NearbyUserFine != pathreliability.InvalidCell || c.filter.NearbyUserCoarse != pathreliability.InvalidCell ||
				c.filter.AllDXContinents || !maps.Equal(c.filter.DXContinents, map[string]bool{"EU": true}) {
				t.Fatal("explicit OFF did not restore the existing location selection")
			}
		})
	}
}

func TestCCFilterHelpNoFilterSpecialGrammar(t *testing.T) {
	for _, command := range []string{"SET/FILTER NOFILTER", "SET/FILTER NOFILTER ALL", "SET/FILTER NOFILTER/ON", "SET/NOFILTER"} {
		t.Run(command, func(t *testing.T) {
			x := newCCHelpContract(t)
			x.accept(t, command)
			cfg := x.configuration().Filters
			for _, tc := range ccHelpLiteralLists() {
				state := tc.state(cfg)
				if !state.allowAll || state.blockAll || state.allowCount != 0 || state.blockCount != 0 {
					t.Fatalf("NOFILTER retained restriction %s: %+v", tc.domain, state)
				}
			}
			for _, tc := range ccHelpLiteralSwitches() {
				if tc.value(cfg) != filter.DefaultBoolTrue {
					t.Fatalf("NOFILTER did not enable switch %s", tc.domain)
				}
			}
			if len(cfg.Comments)+len(cfg.BlockComments)+len(cfg.MinSNR) != 0 || cfg.NearbyEnabled {
				t.Fatal("NOFILTER retained special-category restrictions")
			}
		})
	}
}

func TestCCFilterHelpDXBMForms(t *testing.T) {
	for _, tc := range []struct {
		command string
		allow   bool
	}{
		{"SET/FILTER DXBM 20,40", true}, {"UNSET/FILTER DXBM 20,40", false},
		{"SET/FILTER DXBM/PASS 20,40", true}, {"SET/FILTER DXBM/REJECT 20,40", false},
		{"SET/FILTER DXBM PASS 20,40", true}, {"SET/FILTER DXBM REJECT 20,40", false},
	} {
		t.Run(tc.command, func(t *testing.T) {
			x := newCCHelpContract(t)
			x.accept(t, tc.command)
			for _, band := range []string{"20m", "40m"} {
				if x.client.filter.Bands[band] != tc.allow || x.client.filter.BlockBands[band] == tc.allow {
					t.Fatalf("DXBM action/code ignored: %s %s", tc.command, band)
				}
			}
		})
	}
}

func TestCCFilterHelpSharedCommentGrammar(t *testing.T) {
	x := newCCHelpContract(t)
	phrase := "CQ DX:  up 5!"
	x.accept(t, "PASS COMMENT "+phrase)
	if !slices.Equal(x.client.filter.Comments, []string{"CONTROL", phrase}) {
		t.Fatal("shared COMMENT lost literal spacing/punctuation/case")
	}
	x.accept(t, "REJECT COMMENT "+phrase)
	if !slices.Equal(x.client.filter.Comments, []string{"CONTROL"}) || !slices.Equal(x.client.filter.BlockComments, []string{"AVOID", phrase}) {
		t.Fatal("shared REJECT did not move the literal phrase")
	}
	x.accept(t, "REMOVE REJECT COMMENT "+phrase)
	x.accept(t, "REMOVE PASS COMMENT CONTROL")
	if len(x.client.filter.Comments) != 0 || !slices.Equal(x.client.filter.BlockComments, []string{"AVOID"}) {
		t.Fatal("shared removal changed the opposite list or retained its phrase")
	}
	x.accept(t, "PASS COMMENT CONTROL")
	x.accept(t, "RESET FILTER COMMENT PASS")
	if len(x.client.filter.Comments) != 0 || !slices.Equal(x.client.filter.BlockComments, []string{"AVOID"}) {
		t.Fatal("PASS-only reset changed the other list")
	}
	x.accept(t, "RESET FILTER COMMENT REJECT")
	x.accept(t, "PASS COMMENT ALL")
	x.accept(t, "REJECT COMMENT NONE")
	if !slices.Equal(x.client.filter.Comments, []string{"ALL"}) || !slices.Equal(x.client.filter.BlockComments, []string{"NONE"}) {
		t.Fatal("COMMENT sentinels were not literal phrases")
	}
	x.accept(t, "RESET FILTER COMMENT")
	if len(x.client.filter.Comments)+len(x.client.filter.BlockComments) != 0 {
		t.Fatal("shared whole-comment reset retained rules")
	}
}

func TestCCFilterHelpUnsupportedFormsHaveNoEffects(t *testing.T) {
	x := newCCHelpContract(t)
	domains := []string{"COMMENT", "MINSNR", "NEARBY", "DXBM", "NOFILTER"}
	for _, tc := range ccHelpLiteralLists() {
		domains = append(domains, tc.domain)
	}
	for _, tc := range ccHelpLiteralSwitches() {
		domains = append(domains, tc.domain)
		for _, verb := range []string{"SET/FILTER", "UNSET/FILTER"} {
			for _, value := range []string{"ON", "OFF", "NONE", "ALL ALL"} {
				x.refuse(t, verb+" "+tc.domain+" "+value)
			}
		}
	}
	for _, domain := range domains {
		for _, flag := range []string{"ON", "OFF"} {
			x.refuse(t, "UNSET/FILTER "+domain+"/"+flag)
			x.refuse(t, "SET/FILTER "+domain+"/"+flag+" ALL")
		}
	}
	for _, domain := range []string{"COMMENT", "MINSNR", "NEARBY", "DXBM"} {
		for _, flag := range []string{"ON", "OFF"} {
			x.refuse(t, "SET/FILTER "+domain+"/"+flag)
		}
	}
	for _, command := range []string{
		"SET/FILTER COMMENT CQ", "UNSET/FILTER COMMENT QRT", "SET/FILTER NOFILTER/OFF", "UNSET/FILTER NOFILTER", "UNSET/FILTER NOFILTER ALL",
		"SET/FILTER NOFILTER ON", "SET/FILTER NOFILTER ALL ALL", "SET/FILTER SOURCE HUMAN,SKIMMER", "UNSET/FILTER SOURCE HUMAN SKIMMER",
		"SET/FILTER MINSNR CW", "UNSET/FILTER MINSNR CW", "SET/FILTER MINSNR CW NONE", "UNSET/FILTER MINSNR CW ALL",
		"SET/FILTER NEARBY", "UNSET/FILTER NEARBY", "SET/FILTER NEARBY ALL", "UNSET/FILTER NEARBY OFF ON",
		"SET/FILTER DXBM ALL", "UNSET/FILTER DXBM ALL", "SET/FILTER DXBM/PASS ALL", "SET/FILTER DXBM/REJECT ALL",
		"UNSET/FILTER DXBM/PASS 20", "UNSET/FILTER DXBM/REJECT 20", "UNSET/FILTER DXBM PASS 20", "UNSET/FILTER DXBM REJECT 20",
		"RESET FILTER EXTRA", "RESET FILTER COMMENT BAD", "SET/NOFILTER ALL",
	} {
		x.refuse(t, command)
	}
}
