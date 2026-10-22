package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
)

func TestV5MachineCoordinatorHistoryEditRestoreAndExclusions(t *testing.T) {
	s, c, cfg := v5CoordinatorFixture(t)
	generation := c.historyGeneration
	for _, rules := range [][]filter.RuleDefinition{
		{{Slot: 1, Action: filter.RuleReject, Expr: "SNR<-5"}},
		cfg.Filters.Rules,
	} {
		c.historySearch = &historySearch{token: "must-expire"}
		cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"rules": rules})
		v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
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
		cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", patch)
		v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
		if c.historySearch != cursor || c.historyGeneration != generation {
			t.Fatal("self or bulletin controls invalidated history")
		}
	}
	for _, patch := range []map[string]any{
		{"dialect": "cc"}, {"dedupe_policy": "MED"}, {"solar_summary_minutes": 30},
	} {
		cursor := &historySearch{token: "keep"}
		c.historySearch = cursor
		cmd, request := v5CoordinatorProposal(t, c, "PATCH", "SETTINGS", patch)
		v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
		if c.historySearch != cursor || c.historyGeneration != generation {
			t.Fatal("presentation setting invalidated history")
		}
	}
	for _, patch := range []map[string]any{
		{"include_beacons": false}, {"allow_toxic": false},
	} {
		c.historySearch = &historySearch{token: "expire"}
		cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", patch)
		if response := s.applyV5MachineRequest(c, cmd, request); strings.Contains(response, "error:") {
			t.Fatal(response)
		}
		generation++
		if c.historySearch != nil || c.historyGeneration != generation {
			t.Fatal("matching control failed to invalidate history")
		}
	}
	cmd, request := v5CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"noise_class": "URBAN"})
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	if c.historyGeneration != generation+1 {
		t.Fatal("effective noise setting failed to invalidate history")
	}
}

func TestV5HistoryDigestEffectivePayload(t *testing.T) {
	for _, field := range []string{"grid", "fine", "coarse", "nearby-fine", "nearby-coarse", "derived", "floor"} {
		t.Run(field, func(t *testing.T) {
			_, c, _ := v5CoordinatorFixture(t)
			before := c.v5HistoryFilterDigest()
			switch field {
			case "grid":
				c.grid = "FN31"
			case "fine":
				c.gridCell = 101
			case "coarse":
				c.gridCoarseCell = 202
			case "nearby-fine":
				c.filter.NearbyUserFine = 101
			case "nearby-coarse":
				c.filter.NearbyUserCoarse = 202
			case "derived":
				c.gridDerived = true
			case "floor":
				c.pathMinObservationCount = 33
			}
			if c.v5HistoryFilterDigest() == before {
				t.Fatalf("effective %s excluded from history fingerprint", field)
			}
		})
	}
}
