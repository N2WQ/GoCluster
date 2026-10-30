package telnet

import (
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func FuzzV7MachineSchema(f *testing.F) {
	for _, configuration := range []string{
		"dialect: cc\nfilters: {modes: []}\n",
		"dialect: cc\nfilters: {geography: {DOC: {bands: {'6': {action: INHERIT}}}}}\n",
		"dialect: cc\nfilters: {dxbm: {active: true, base_allow: false, slots: {20-CW: true}}}\n",
		"dialect: cc\nfilters: {rules: []}\n",
	} {
		f.Add(v7MachineRequestFixture(configuration, true))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 262_144 {
			t.Skip()
		}
		request, err := decodeV7MachineRequest(body, machineCommand{Verb: "PATCH", Resource: "FILTER", SchemaVersion: 7}, nil)
		if err != nil {
			return
		}
		before, _ := v7PreparedFixture(t)
		saved := before.Clone()
		next, err := request.apply(before, "cc")
		if !before.Equal(saved) {
			t.Fatal("proposal mutated its prior state")
		}
		if err != nil {
			return
		}
		canonical, _, _, err := filter.PrepareV7Configuration(next)
		if err != nil {
			return
		} // Model vocabulary/admission is intentionally later.
		encoded, err := yaml.Marshal(canonical)
		if err != nil {
			t.Fatal(err)
		}
		restore, err := decodeV7MachineRequest(v7MachineRequestFixture(string(encoded), true), machineCommand{Verb: "PUT", Resource: "CONFIG", SchemaVersion: 7}, nil)
		if err != nil {
			t.Fatalf("admitted state could not be restored: %v", err)
		}
		restored, err := restore.apply(before, "cc")
		if err != nil || !restored.Equal(canonical) {
			t.Fatalf("complete restore lost accepted preferences: %v", err)
		}
	})
}
