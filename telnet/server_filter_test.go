package telnet

import (
	"strings"
	"testing"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func newTestClient() *Client {
	return &Client{filter: unrestrictedTestFilter(), dialect: DialectGo}
}

// Transport/settings fixtures must not accidentally depend on new-user rule 0.
func unrestrictedTestFilter() *filter.Filter {
	_, value, err := filter.PrepareV6Configuration(filter.V6Configuration{})
	if err != nil {
		panic(err)
	}
	return &value
}

func testRuleFilter(t testing.TB, definitions ...filter.RuleDefinition) *filter.Filter {
	t.Helper()
	_, value, err := filter.PrepareV6Configuration(filter.V6Configuration{
		Filters: filter.V6FilterConfiguration{Rules: definitions},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &value
}

func passRuleTestFilter(t testing.TB, expression string) *filter.Filter {
	t.Helper()
	return testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: expression})
}

func requireH3Mappings(t *testing.T) {
	t.Helper()
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatalf("InitH3Mappings failed: %v", err)
	}
}

func requireDistinctPathCells(t *testing.T) (pathreliability.CellID, pathreliability.CellID, pathreliability.CellID, pathreliability.CellID) {
	t.Helper()
	requireH3Mappings(t)
	user, dx := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCell("IO91")
	userCoarse, dxCoarse := pathreliability.EncodeCoarseCell("FN31"), pathreliability.EncodeCoarseCell("IO91")
	if user == pathreliability.InvalidCell || dx == pathreliability.InvalidCell ||
		userCoarse == pathreliability.InvalidCell || dxCoarse == pathreliability.InvalidCell ||
		user == dx || userCoarse == dxCoarse {
		t.Fatalf("FN31/IO91 must have distinct usable fine/coarse cells: %d/%d %d/%d", user, dx, userCoarse, dxCoarse)
	}
	return user, dx, userCoarse, dxCoarse
}

const sampleCTYPLIST = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>K1</key><dict><key>Country</key><string>Alpha</string><key>Prefix</key><string>K1</string><key>ADIF</key><integer>1</integer><key>CQZone</key><integer>5</integer><key>ITUZone</key><integer>8</integer><key>Continent</key><string>NA</string><key>ExactCallsign</key><false/></dict>
<key>W6</key><dict><key>Country</key><string>United States</string><key>Prefix</key><string>W6</string><key>ADIF</key><integer>291</integer><key>CQZone</key><integer>3</integer><key>ITUZone</key><integer>6</integer><key>Continent</key><string>NA</string><key>ExactCallsign</key><false/></dict>
</dict></plist>`

func loadTestCTY(t *testing.T) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(sampleCTYPLIST))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDialectSwitchCommand(t *testing.T) {
	server := &Server{filterEngine: newFilterCommandEngine()}
	client := newTestClient()
	if response, handled := server.handleDialectCommand(client, "DIALECT"); !handled || !strings.Contains(strings.ToLower(response), "dialect") {
		t.Fatalf("current dialect: %q", response)
	}
	if response, handled := server.handleDialectCommand(client, "DIALECT LIST"); !handled ||
		!strings.Contains(response, "GO") || !strings.Contains(response, "CC") || strings.Contains(strings.ToUpper(response), "LEGACY") {
		t.Fatalf("dialect list: %q", response)
	}
	if response, handled := server.handleDialectCommand(client, "DIALECT cc"); !handled ||
		client.dialect != DialectCC || !strings.Contains(strings.ToUpper(response), "CC") {
		t.Fatalf("dialect switch: %q", response)
	}
}

func TestDialectPersistence(t *testing.T) {
	s := presetTestServer(t)
	client := configurationTestClient(s, "W1TEST")
	client.dialect = DialectCC
	client.recentIPs = filter.UpdateRecentIPs(nil, "1.2.3.4")
	if err := client.saveFilter(); err != nil {
		t.Fatal(err)
	}
	record, err := filter.LoadUserRecord("W1TEST")
	if err != nil || normalizeDialectName(record.Dialect) != DialectCC {
		t.Fatalf("dialect persistence: %+v, %v", record, err)
	}
}

func TestLegacyRecordFallsBackToGoCluster(t *testing.T) {
	if got := normalizeDialectName("legacy"); got != DialectGo {
		t.Fatalf("legacy dialect token normalized to %s", got)
	}
}

func TestDialectWelcomeLine(t *testing.T) {
	server := &Server{dialectSourceDef: "default", dialectSourcePers: "persisted"}
	template := "Current dialect: <DIALECT> (<DIALECT_SOURCE>). Use DIALECT LIST or DIALECT <DIALECT_DEFAULT> to switch. Type HELP for commands in this dialect.\n"
	for _, tc := range []struct {
		dialect DialectName
		missing bool
		source  string
	}{{DialectGo, true, "default"}, {DialectCC, false, "persisted"}} {
		line := formatDialectWelcome(template, dialectTemplateData{
			dialect: strings.ToUpper(string(tc.dialect)), defaultDialect: "GO",
			source: server.dialectSourceLabel(tc.dialect, tc.missing, nil, DialectGo),
		})
		if !strings.Contains(line, strings.ToUpper(string(tc.dialect))) ||
			!strings.Contains(strings.ToLower(line), tc.source) || !strings.Contains(line, "HELP") {
			t.Fatalf("welcome: %q", line)
		}
	}
}

func TestRetiredNearbyToggleCommandsAreAtomic(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		client := newTestClient()
		client.dialect = dialect
		client.filter = passRuleTestFilter(t, "MODE=CW")
		client.setDedupePolicy(dedupePolicySlow)
		client.configuredSettings, client.configurationInitialized = filter.SettingsConfiguration{Dialect: string(dialect), DedupePolicy: "SLOW"}, true
		before := snapshotBooleanCommand(client)
		for _, line := range []string{"PASS NEARBY ON", "PASS NEARBY OFF", "REJECT NEARBY ON", "REJECT NEARBY OFF", "SET/FILTER NEARBY ON", "UNSET/FILTER NEARBY OFF"} {
			response, handled := newFilterCommandEngine().Handle(client, line)
			if !handled || !strings.Contains(response, "No changes applied.") || client.getDedupePolicy() != dedupePolicySlow {
				t.Fatalf("retired toggle mutated preferences: %s => %q", line, response)
			}
			before.assertUnchanged(t, client)
		}
	}
}

func TestNearbyRuleEditingNeedsNoGrid(t *testing.T) {
	client := newTestClient()
	for _, expression := range []string{"NEARBY", "NOT NEARBY", "NEARBY OR MODE=CW"} {
		response, handled := newFilterCommandEngine().Handle(client, "PASS RULE 0 "+expression)
		if !handled || strings.Contains(response, "No changes applied.") || client.filter.PreparedRules().Definitions()[0].Expr != expression {
			t.Fatalf("location-free rule edit failed: %q", response)
		}
	}
}

func TestBroadcastWWVRespectsFilter(t *testing.T) {
	server := &Server{clients: make(map[string]*Client)}
	allow := &Client{callsign: "ALLOW", controlChan: make(chan controlMessage, 1), filter: unrestrictedTestFilter()}
	block := &Client{callsign: "BLOCK", controlChan: make(chan controlMessage, 1), filter: unrestrictedTestFilter()}
	block.filter.SetWWVEnabled(false)
	server.clients["ALLOW"], server.clients["BLOCK"] = allow, block
	server.BroadcastWWV("WWV", "WWV de TEST <00> : SFI=1 A=1 K=1")
	if len(allow.controlChan) != 1 || len(block.controlChan) != 0 {
		t.Fatal("WWV delivery ignored independent control")
	}
}

func TestBroadcastAnnouncementRespectsFilter(t *testing.T) {
	server := &Server{clients: make(map[string]*Client)}
	allow := &Client{callsign: "ALLOW", controlChan: make(chan controlMessage, 1), filter: unrestrictedTestFilter()}
	block := &Client{callsign: "BLOCK", controlChan: make(chan controlMessage, 1), filter: unrestrictedTestFilter()}
	block.filter.SetAnnounceEnabled(false)
	server.clients["ALLOW"], server.clients["BLOCK"] = allow, block
	server.BroadcastAnnouncement("To ALL de TEST: hello")
	if len(allow.controlChan) != 1 || len(block.controlChan) != 0 {
		t.Fatal("announcement delivery ignored independent control")
	}
}

func TestBroadcastSelfRespectsSelfToggle(t *testing.T) {
	server := &Server{clients: make(map[string]*Client)}
	allow := &Client{callsign: "K1ABC", spotChan: make(chan *spotEnvelope, 1), filter: unrestrictedTestFilter()}
	block := &Client{callsign: "K1ABC-7", spotChan: make(chan *spotEnvelope, 1), filter: unrestrictedTestFilter()}
	block.filter.SetSelfEnabled(false)
	server.clients[allow.callsign], server.clients[block.callsign] = allow, block
	server.deliverJob(broadcastJob{
		spot: spot.NewSpot("K1ABC", "W1XYZ", 14025, "CW"), clients: []*Client{allow, block},
		allowFast: true, allowMed: true, allowSlow: true,
	})
	if len(allow.spotChan) != 1 || len(block.spotChan) != 0 {
		t.Fatal("self delivery ignored independent control")
	}
}

func TestDeliverSelfSpotRespectsSelfToggle(t *testing.T) {
	client := &Client{callsign: "K1ABC", spotChan: make(chan *spotEnvelope, 1), filter: unrestrictedTestFilter()}
	server := &Server{clients: map[string]*Client{client.callsign: client}}
	client.filter.SetSelfEnabled(false)
	server.DeliverSelfSpot(spot.NewSpot("K1ABC", "W1XYZ", 14025, "CW"))
	if len(client.spotChan) != 0 {
		t.Fatal("direct self delivery ignored independent control")
	}
}
