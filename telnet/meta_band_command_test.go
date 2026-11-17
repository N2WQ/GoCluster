package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func TestMetaBandCommandProcessingAndAtomicity(t *testing.T) {
	c, engine, _ := nativeCCCommandFixture(t)
	before := nativeCCConfiguration(t, c)
	outcome := engine.stageNativeCCFilterCommand(c, "set/filter dxbm/pass uhf")
	assertBooleanAccepted(t, outcome)
	after := nativeCCConfiguration(t, c)
	if len(after.DXBM.FrequencyOverrides) == 0 || before.AllowSelf != after.AllowSelf || before.AllowToxic != after.AllowToxic || strings.Join(before.Modes, ",") != strings.Join(after.Modes, ",") {
		t.Fatal("command lost new selection or changed independent settings")
	}
	for _, probe := range []struct {
		frequency float64
		want      bool
	}{{432000, true}, {902500, true}, {2350000, false}, {3500000, false}, {14025, false}} {
		sp := spot.NewSpot("K1DX", "W1SPOT", probe.frequency, "CW")
		if got := c.filter.Matches(sp); got != probe.want {
			t.Fatalf("at %g got %t want %t", probe.frequency, got, probe.want)
		}
	}
	assertNativeCCRefused(t, c, engine, "SET/FILTER DXBM/PASS WARC,INVALID")
	assertBooleanAccepted(t, engine.stageNativeCCFilterCommand(c, "SET/FILTER DXBM/OFF"))
	if len(nativeCCConfiguration(t, c).DXBM.FrequencyOverrides) != 0 {
		t.Fatal("OFF retained feature state")
	}

	goClient, _, _ := booleanCommandFixture(t, DialectGo)
	assertBooleanAccepted(t, engine.stageBooleanFilterCommand(goClient, "PASS RULE 0 BAND=VHF AND MODE=CW"))
	definitions := goClient.filter.PreparedRules().Definitions()
	if definitions[0].Expr != "BAND=6M,4M,2M,1.25M AND MODE=CW" {
		t.Fatalf("command stored unresolved meta: %+v", definitions)
	}
	old := snapshotBooleanCommand(goClient)
	refusal := engine.stageBooleanFilterCommand(goClient, "PASS RULE 0 BAND=WARC,INVALID")
	if refusal.publish {
		t.Fatal("invalid GO command published")
	}
	old.assertUnchanged(t, goClient)
}

func TestMetaBandMachineRawOperandBudget(t *testing.T) {
	rawAtom := "BAND=" + strings.Repeat("HF,", 31) + "HF"
	rawRule := strings.Repeat(rawAtom+" AND ", 7) + rawAtom
	for _, extra := range []bool{false, true} {
		definitions := []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: rawRule}, {Slot: 1, Action: filter.RulePass, Expr: rawRule}}
		if extra {
			definitions = append(definitions, filter.RuleDefinition{Slot: 2, Action: filter.RulePass, Expr: "BAND=HF"})
		}
		data, err := yaml.Marshal(definitions)
		if err != nil {
			t.Fatal(err)
		}
		var node yaml.Node
		if err := yaml.Unmarshal(data, &node); err != nil {
			t.Fatal(err)
		}
		_, err = decodeV6MachineRules(node.Content[0], "rules", nil)
		if (err != nil) != extra {
			t.Fatalf("raw total 512 plus extra=%t: %v", extra, err)
		}
		if extra && !strings.Contains(err.Error(), "rules[2].expr: col 1:") {
			t.Fatalf("aggregate raw budget error lost field/column diagnostics: %v", err)
		}
	}
}

func TestMetaBandMachineErrorPrecedence(t *testing.T) {
	definitions := []filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: "BAND=INVALID"},
		{Slot: 1, Action: filter.RulePass, Expr: "BAD"},
	}
	data, err := yaml.Marshal(definitions)
	if err != nil {
		t.Fatal(err)
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		t.Fatal(err)
	}
	_, err = decodeV6MachineRules(node.Content[0], "rules", nil)
	if err == nil || !strings.Contains(err.Error(), "rules[0].expr") || !strings.Contains(err.Error(), "unsupported BAND") {
		t.Fatalf("later syntax error preempted earlier domain error: %v", err)
	}
}
