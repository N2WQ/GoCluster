package telnet

import (
	"testing"

	"dxcluster/filter"
	"dxcluster/filter/expr"
)

func FuzzBooleanRuleCommand(f *testing.F) {
	for _, line := range []string{
		"PASS RULE 0 MODE=CW", `REJECT RULE 9 COMMENT="MiXeD ""quote"""`,
		`PASS RULE 9 COMMENT="No changes applied."`, "RESET FILTER RULE ALL",
		"PASS RULE 12 BAND=20M", "PASS RULE 1 MODE=CW AND", "SET/FILTER MODE CW",
		"RESET FILTER", "HELP RULE",
	} {
		f.Add(line)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > expr.MaxBytes+32 {
			return
		}
		candidate, source, _ := booleanCommandFixture(t, DialectGo, true)
		before, live := snapshotBooleanCommand(candidate), snapshotBooleanCommand(source)
		outcome := newFilterCommandEngine().stageBooleanFilterCommand(candidate, line)
		live.assertUnchanged(t, source)
		if !outcome.publish {
			before.assertUnchanged(t, candidate)
			return
		}
		if !outcome.handled {
			t.Fatal("unhandled command requested publication")
		}
		assertBooleanHumanWidth(t, outcome.response)
		cfg := filter.V5ConfigurationFromFilter(candidate.filter, candidate.configuredSettings)
		canonical, runtime, err := filter.PrepareV5Configuration(cfg)
		if err != nil || !canonical.Equal(cfg) || runtime.PreparedRules() != candidate.filter.PreparedRules() {
			t.Fatal("accepted candidate was not fully prepared", err)
		}
	})
}
