package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
)

func TestV6MachineCoordinatorHistoryEditRestoreAndExclusions(t *testing.T) {
	s, c, cfg := v6CoordinatorFixture(t)
	generation := c.historyGeneration
	for _, rules := range [][]filter.RuleDefinition{
		{{Slot: 1, Action: filter.RuleReject, Expr: "SNR<-5"}},
		cfg.Filters.Rules,
	} {
		c.historySearch = &historySearch{token: "must-expire"}
		cmd, request := v6CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"rules": rules})
		v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
		generation++
		if c.historyGeneration != generation || c.historySearch != nil {
			t.Fatal("rule edit/restoration revived an old history cursor")
		}
	}
	for _, patch := range []map[string]any{
		{"allow_self": true}, {"allow_wwv": true}, {"allow_wcy": false}, {"allow_announce": true},
	} {
		cursor := &historySearch{token: "keep"}
		c.historySearch = cursor
		cmd, request := v6CoordinatorProposal(t, c, "PATCH", "FILTER", patch)
		v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
		if c.historySearch != cursor || c.historyGeneration != generation {
			t.Fatal("self or bulletin controls invalidated history")
		}
	}
	for _, patch := range []map[string]any{
		{"dedupe_policy": "MED"}, {"solar_summary_minutes": 30},
	} {
		cursor := &historySearch{token: "keep"}
		c.historySearch = cursor
		cmd, request := v6CoordinatorProposal(t, c, "PATCH", "SETTINGS", patch)
		v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
		if c.historySearch != cursor || c.historyGeneration != generation {
			t.Fatal("presentation setting invalidated history")
		}
	}
	for _, patch := range []map[string]any{
		{"include_beacons": false}, {"allow_toxic": false},
	} {
		c.historySearch = &historySearch{token: "expire"}
		cmd, request := v6CoordinatorProposal(t, c, "PATCH", "FILTER", patch)
		if response := s.applyV6MachineRequest(c, cmd, request); strings.Contains(response, "error:") {
			t.Fatal(response)
		}
		generation++
		if c.historySearch != nil || c.historyGeneration != generation {
			t.Fatal("matching control failed to invalidate history")
		}
	}
	cmd, request := v6CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"noise_class": "URBAN"})
	v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
	if c.historyGeneration != generation+1 {
		t.Fatal("effective noise setting failed to invalidate history")
	}
	generation++
	for _, dialect := range []string{"cc", "go"} {
		c.historySearch = &historySearch{token: "bank-activation-must-expire"}
		cmd, request := v6CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"dialect": dialect})
		v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
		generation++
		if c.historySearch != nil || c.historyGeneration != generation {
			t.Fatal("bank activation/restoration revived an earlier history continuation")
		}
	}
}

func TestV6HistoryDigestEffectivePayload(t *testing.T) {
	for _, field := range []string{"grid", "fine", "coarse", "derived", "floor"} {
		t.Run(field, func(t *testing.T) {
			_, c, _ := v6CoordinatorFixture(t)
			c.filter = passRuleTestFilter(t, "NEARBY")
			c.grid = "IO91"
			c.gridCell = 10
			c.gridCoarseCell = 11
			before := c.v6HistoryFilterDigest()
			switch field {
			case "grid":
				c.grid = "FN31"
			case "fine":
				c.gridCell = 101
			case "coarse":
				c.gridCoarseCell = 202
			case "derived":
				c.gridDerived = true
			case "floor":
				c.pathMinObservationCount = 33
			}
			if c.v6HistoryFilterDigest() == before {
				t.Fatalf("effective %s excluded from history fingerprint", field)
			}
		})
	}
}
