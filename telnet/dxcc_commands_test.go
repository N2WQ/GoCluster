package telnet

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestDXCCCanonicalRuleOperands(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, field := range []string{"DXDXCC", "DEDXCC"} {
			for _, operator := range []string{"=", "<>"} {
				t.Run(string(dialect)+"/"+field+operator, func(t *testing.T) {
					candidate, _, _ := booleanCommandFixture(t, dialect, false)
					calls := 0
					candidate.server = &Server{ctyLookup: func() *cty.CTYDatabase {
						calls++
						return canonicalTestCTY()
					}}
					source := field + operator + `[" k ",291,VE,1,IT9,I,IG9,3D2/R,3Y/P]`
					outcome := newFilterCommandEngine().stageBooleanFilterCommand(candidate, "PASS RULE 0 "+source)
					assertBooleanAccepted(t, outcome)
					want := field + operator + "291,291,1,1,248,248,248,460,199"
					if calls != 1 || candidate.filter.PreparedRules().Definitions()[0].Expr != want ||
						booleanDisplayedExpression(t, outcome.response, "PASS RULE 0") != want {
						t.Fatalf("historical operands lost: calls=%d response=%q", calls, outcome.response)
					}
					for _, adif := range []int{1, 199, 248, 291, 460, 99999, 0} {
						probe := &spot.Spot{Mode: "CW", IsHuman: true,
							DXMetadata: spot.CallMetadata{ADIF: adif}, DEMetadata: spot.CallMetadata{ADIF: adif}}
						wantMatch := adif != 99999
						if operator == "<>" {
							wantMatch = !wantMatch
						}
						if adif == 0 {
							wantMatch = false
						}
						if got := candidate.filter.PreparedRules().Matches(probe, "", false); got != wantMatch {
							t.Fatalf("ADIF %d matched %v, want %v", adif, got, wantMatch)
						}
					}
				})
			}
		}
	}
}

func TestDXCCCanonicalHumanPersistenceAndRefusal(t *testing.T) {
	s, c, _ := v5CoordinatorFixture(t)
	db, calls := canonicalTestCTY(), 0
	s.ctyLookup = func() *cty.CTYDatabase { calls++; return db }
	engine := newFilterCommandEngine()
	want := "MODE=CW AND DXDXCC<>291 AND DEDXCC=460"
	response, handled := engine.Handle(c, "PASS RULE 0 MODE=CW AND DXDXCC<>k AND DEDXCC=3D2/R")
	if !handled || calls != 1 || !strings.Contains(response, want) || !strings.Contains(response, "Saved and active.") {
		t.Fatalf("canonical human write: %q, CTY calls %d", response, calls)
	}
	disk := v5CoordinatorDisk(t, c)
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || record.PreparedRules().Definitions()[0].Expr != want {
		t.Fatalf("numeric reload failed: %v", err)
	}
	shown, _ := engine.Handle(c, "SHOW FILTER")
	if !strings.Contains(shown, want) {
		t.Fatalf("SHOW lost numeric expression: %q", shown)
	}
	for _, nearby := range []bool{false, true} {
		c.filter.NearbyEnabled = nearby
		for _, operand := range []string{"K,ZZ", "W6", "K1ABC", "K,0", "K,ALL", "K*"} {
			before, revision := snapshotBooleanCommand(c), v5CoordinatorRevision(t, c)
			response, _ := engine.Handle(c, "PASS RULE 0 DXDXCC="+operand)
			if !strings.Contains(response, "No changes applied.") {
				t.Fatalf("nearby=%v invalid %s accepted: %q", nearby, operand, response)
			}
			before.assertUnchanged(t, c)
			if !bytes.Equal(disk, v5CoordinatorDisk(t, c)) || v5CoordinatorRevision(t, c) != revision {
				t.Fatal("invalid human edit changed disk or revision")
			}
		}
	}
	db.Data["CONFLICT"] = cty.PrefixInfo{Prefix: "K", ADIF: 999}
	before := snapshotBooleanCommand(c)
	response, _ = engine.Handle(c, "REJECT RULE 0 DXDXCC=VE,K")
	if !strings.Contains(response, "conflicting DXCC") || !strings.Contains(response, "No changes applied.") {
		t.Fatalf("conflicting label accepted: %q", response)
	}
	before.assertUnchanged(t, c)
	if !bytes.Equal(disk, v5CoordinatorDisk(t, c)) {
		t.Fatal("conflicting label changed disk")
	}
	s.ctyLookup = nil
	record, err = filter.LoadUserRecord(c.callsign)
	if err != nil || record.PreparedRules().Definitions()[0].Expr != want {
		t.Fatalf("reload acquired CTY dependency: %v", err)
	}
	preset, err := filter.V5SavedPresetFromRuntime(c.presetReference.Baseline)
	if err != nil {
		t.Fatal(err)
	}
	preset.Configuration = filter.V5ConfigurationFromFilter(&record.Filter, c.configuredSettings)
	encoded, err := filter.EncodeV5PresetCollection(&filter.V5PresetCollection{
		Presets: map[string]*filter.V5SavedPreset{"DXCC": preset},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := filter.DecodeV5PresetCollection(encoded)
	if err != nil || decoded.Presets["DXCC"].Configuration.Filters.Rules[0].Expr != want {
		t.Fatalf("numeric preset reload failed: %v", err)
	}
}

func TestDXCCNumericRuleCommandsNeedNoCTY(t *testing.T) {
	candidate, _, _ := booleanCommandFixture(t, DialectGo, false)
	calls := 0
	candidate.server = &Server{ctyLookup: func() *cty.CTYDatabase { calls++; return nil }}
	engine := newFilterCommandEngine()
	for _, operand := range []string{"291", "99999", `"+291"`, "0291"} {
		outcome := engine.stageBooleanFilterCommand(candidate, "PASS RULE 0 DXDXCC="+operand)
		assertBooleanAccepted(t, outcome)
	}
	for _, operand := range []string{strings.Repeat("9", 50), "0", "-1"} {
		before := snapshotBooleanCommand(candidate)
		outcome := engine.stageBooleanFilterCommand(candidate, "PASS RULE 0 DEDXCC="+operand)
		if outcome.publish || !strings.Contains(outcome.response, "No changes applied.") {
			t.Fatalf("invalid integer accepted: %q", outcome.response)
		}
		before.assertUnchanged(t, candidate)
	}
	if calls != 0 {
		t.Fatalf("numeric-only commands captured CTY %d times", calls)
	}
	before := snapshotBooleanCommand(candidate)
	outcome := engine.stageBooleanFilterCommand(candidate, "PASS RULE 0 DXDXCC=K")
	if outcome.publish || calls != 1 || !strings.Contains(outcome.response, "CTY database") ||
		!strings.Contains(outcome.response, "available") || !strings.Contains(outcome.response, "No changes applied.") {
		t.Fatalf("unavailable CTY was not refused: %q, calls=%d", outcome.response, calls)
	}
	before.assertUnchanged(t, candidate)
}

func TestDXCCMachineSnapshotAndNumericReadback(t *testing.T) {
	s, c, _ := v5CoordinatorFixture(t)
	first, second := canonicalTestCTY(), canonicalTestCTY()
	second.Data["K"] = cty.PrefixInfo{Prefix: "K", ADIF: 999}
	second.Data["W6"] = cty.PrefixInfo{Prefix: "K", ADIF: 999}
	calls := 0
	s.ctyLookup = func() *cty.CTYDatabase {
		calls++
		if calls == 1 {
			return first
		}
		return second
	}
	rules := map[string]any{"rules": []filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: "DXDXCC=K AND DEDXCC<>K"},
		{Slot: 9, Action: filter.RuleReject, Expr: "DEDXCC=K"},
	}}
	cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", rules)
	want := []filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: "DXDXCC=291 AND DEDXCC<>291"},
		{Slot: 9, Action: filter.RuleReject, Expr: "DEDXCC=291"},
	}
	if calls != 1 || !reflect.DeepEqual(request.Configuration.Filters.Rules, want) {
		t.Fatalf("mixed-snapshot proposal: calls=%d rules=%v", calls, request.Configuration.Filters.Rules)
	}
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || !reflect.DeepEqual(record.PreparedRules().Definitions(), want) {
		t.Fatalf("YAML write did not persist numeric rules: %v", err)
	}
	response := s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: "FILTER", RequestID: "dxcc"})
	if !strings.Contains(response, "DXDXCC=291 AND DEDXCC<>291") || calls != 1 {
		t.Fatalf("GET lost numeric truth or captured CTY: %q, calls=%d", response, calls)
	}
	_, next := v5CoordinatorProposal(t, c, "PATCH", "FILTER", rules)
	if calls != 2 || next.Configuration.Filters.Rules[1].Expr != "DEDXCC=999" {
		t.Fatal("next request failed to observe replacement CTY")
	}
	_, numeric := v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{
		"rules": []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "DXDXCC=291"}},
	})
	if calls != 2 || numeric.Configuration.Filters.Rules[0].Expr != "DXDXCC=291" {
		t.Fatal("numeric machine request captured CTY")
	}
}

func TestDXCCMachineAtomicRefusalAndValidation(t *testing.T) {
	s, c, before := v5CoordinatorFixture(t)
	disk := v5CoordinatorSeed(t, c, before)
	db := canonicalTestCTY()
	s.ctyLookup = func() *cty.CTYDatabase { return db }
	for _, operand := range []string{"ZZ", "W6", "K1ABC", "K", strings.Repeat("9", 50)} {
		if operand == "K" {
			db.Data["CONFLICT"] = cty.PrefixInfo{Prefix: "K", ADIF: 999}
		}
		revision, snapshot := v5CoordinatorRevision(t, c), snapshotBooleanCommand(c)
		body := fmt.Sprintf("schema_version: 5\nrequest_id: dxcc\nif_revision: %s\nconfiguration:\n  rules:\n"+
			"    - {slot: 0, action: pass, expr: 'DXDXCC=VE'}\n"+
			"    - {slot: 9, action: reject, expr: 'DEDXCC=%s'}\n", revision, operand)
		_, err := s.prepareV5MachineRequest(c, []byte(body), machineCommand{Verb: "PATCH", Resource: "FILTER"})
		if err == nil || !strings.Contains(err.Error(), "configuration.rules[1].expr") || !strings.Contains(err.Error(), "col 8:") {
			t.Fatalf("missing original schema/operand error: %v", err)
		}
		snapshot.assertUnchanged(t, c)
		if !bytes.Equal(disk, v5CoordinatorDisk(t, c)) || v5CoordinatorRevision(t, c) != revision {
			t.Fatal("invalid YAML changed disk or revision")
		}
	}
	delete(db.Data, "CONFLICT")
	proposal := before.Clone()
	proposal.Filters.Rules = []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "DXDXCC=K AND DEDXCC<>3D2/R"}}
	cmd, request := v5CoordinatorProposal(t, c, "VALIDATE", "CONFIG", proposal)
	snapshot := snapshotBooleanCommand(c)
	revision := v5CoordinatorRevision(t, c)
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), false)
	snapshot.assertUnchanged(t, c)
	if !bytes.Equal(disk, v5CoordinatorDisk(t, c)) || v5CoordinatorRevision(t, c) != revision {
		t.Fatal("VALIDATE published or persisted")
	}
	cmd, request = v5CoordinatorProposal(t, c, "PUT", "FILTER", proposal.Filters)
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	if got := c.filter.PreparedRules().Definitions()[0].Expr; got != "DXDXCC=291 AND DEDXCC<>460" {
		t.Fatalf("PUT failed to resolve both fields: %q", got)
	}
}

func canonicalTestCTY() *cty.CTYDatabase {
	return &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{
		"K": {Prefix: "K", ADIF: 291}, "W6": {Prefix: "K", ADIF: 291},
		"VE": {Prefix: "VE", ADIF: 1},
		"I":  {Prefix: "I", ADIF: 248}, "IG9": {Prefix: "IG9", ADIF: 248}, "IT9": {Prefix: "IT9", ADIF: 248},
		"ROTUMA-CALL": {Prefix: "3D2/R", ADIF: 460}, "PETER-CALL": {Prefix: "3Y/P", ADIF: 199},
	}}
}

func TestDXCCBooleanCommandPersistsNumericEntity(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	s.filterEngine = newFilterCommandEngine()
	if response, handled := s.filterEngine.Handle(c, "REJECT RULE 0 DXDXCC=248"); !handled ||
		len(c.filter.PreparedRules().Definitions()) != 1 {
		t.Fatalf("Boolean DXCC command: %q", response)
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || record.PreparedRules().Definitions()[0].Expr != "DXDXCC=248" {
		t.Fatalf("numeric entity reload: %+v %v", record, err)
	}
	for _, call := range []string{"I1ABC", "IT9ABC", "IG9ABC", "K1ABC"} {
		candidate := spot.NewSpot(call, "W1ABC", 14030, "CW")
		candidate.DXMetadata.ADIF = 248
		if c.filter.Matches(candidate) {
			t.Fatalf("entity match depended on callsign spelling: %s", call)
		}
		candidate.DXMetadata.ADIF = 291
		if !c.filter.Matches(candidate) {
			t.Fatalf("unblocked entity rejected: %s", call)
		}
	}
}
