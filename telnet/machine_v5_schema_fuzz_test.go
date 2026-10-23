package telnet

import (
	"errors"
	"reflect"
	"testing"

	"dxcluster/filter"
)

func FuzzV5MachineRequest(f *testing.F) {
	f.Add(v5MachineRequestFixture(v5CompleteFilterFixture, true), uint8(0))
	f.Add(v5MachineRequestFixture("rules: []\n", true), uint8(0))
	f.Add(v5MachineRequestFixture("grid: \"\"\nsolar_summary_minutes: 0\n", true), uint8(1))
	f.Add(v5MachineRequestFixture("filters:\n  rules: [{slot: 0, action: pass, expr: 'DXCONT=EU AND MODE=CW'}]\nsettings:\n  grid: \"\"\n", true), uint8(2))
	f.Add([]byte("schema_version: 5\nconfiguration: null\n"), uint8(2))
	f.Fuzz(func(t *testing.T, body []byte, selector uint8) {
		if len(body) > maxYAMLBytes+1 {
			return
		}
		resource := [3]string{"FILTER", "SETTINGS", "CONFIG"}[selector%3]
		request, err := decodeV5MachineRequest(body, machineCommand{Verb: "PATCH", Resource: resource}, nil)
		if err != nil {
			return
		}
		if request.SchemaVersion != 5 || !validMachineRequestID(request.RequestID) || !validMachineRevision(request.IfRevision) {
			t.Fatal("successful decode escaped envelope contract")
		}
		before, oldSet := v5PreparedFixture(t)
		saved := before.Clone()
		next, err := request.apply(before)
		if err != nil || !reflect.DeepEqual(before, saved) {
			t.Fatalf("apply failed or mutated before: %v", err)
		}
		canonical, prepared, err := filter.PrepareV5Configuration(next)
		if err != nil || !canonical.Equal(next) {
			t.Fatalf("successful decode did not yield a canonical valid candidate: %v", err)
		}
		wantSet := oldSet
		if request.filterFields&1 != 0 {
			wantSet = request.preparedRules
		}
		if prepared.PreparedRules() != wantSet {
			t.Fatal("candidate preparation recompiled a validated rule set")
		}
		counts := prepared.PreparedRules().Counts()
		if counts.Rules > 10 || counts.Normal.Atoms > 128 || counts.Normal.Values > 512 {
			t.Fatal("successful decode escaped total budgets")
		}
		response, err := renderV5YAMLReadback(canonical, prepared.PreparedRules(), configurationReadbackStatus{}, resource, request.RequestID, request.IfRevision)
		if err != nil {
			// Valid PATCH settings may be large enough to exceed readback after
			// status duplication/escaping; only the coordinator admits writes.
			if !errors.Is(err, errReadbackTooLarge) || response != "" {
				t.Fatalf("unexpected render failure or partial response: %v", err)
			}
			return
		}
		v5ReadbackDocument(t, response)
		if !reflect.DeepEqual(before, saved) {
			t.Fatal("preparation/render changed before")
		}
	})
}
