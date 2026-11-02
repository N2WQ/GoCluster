package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func TestCWSpeedV7MachineAndV6Compatibility(t *testing.T) {
	s, c := v7MachineTestClient(t)
	before, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	cmd, req := v7ProposalFixture(t, c, "PATCH", "SETTINGS", "cw_speed: true\n")
	if response := s.applyV7MachineRequest(c, cmd, req); !strings.Contains(response, "persisted: true") {
		t.Fatal(response)
	}
	current, err := c.captureV7Configuration()
	if err != nil || !current.Settings.CWSpeed {
		t.Fatalf("v7 write lost CW speed: %v", err)
	}
	after, err := c.v7ConfigurationRevisionToken()
	if err != nil || before == after {
		t.Fatalf("v7 write did not advance revision: %s %s %v", before, after, err)
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil || !record.Configuration.Settings.CWSpeed {
		t.Fatalf("persisted profile lost CW speed: %v", err)
	}
	v7result := s.handleV7MachineCommand(c, "GET YAML SETTINGS SCHEMA 7 ID view-7")
	if !v7result.handled || v7result.terminal {
		t.Fatalf("v7 readback dispatch: %+v", v7result)
	}
	v7msg := <-c.controlChan
	v7readback := string(v7msg.raw)
	v7msg.configurationReservation.Release()
	if !strings.Contains(v7readback, "cw_speed: true") {
		t.Fatalf("v7 readback did not expose enabled CW speed: %s", v7readback)
	}
	result := s.handleV6MachineCommand(c, "GET YAML SETTINGS SCHEMA 6 ID view-1")
	if !result.handled || result.terminal {
		t.Fatalf("v6 readback dispatch: %+v", result)
	}
	msg := <-c.controlChan
	readback := string(msg.raw)
	msg.configurationReservation.Release()
	if strings.Contains(readback, "cw_speed") {
		t.Fatalf("frozen v6 readback exposed CW speed: %s", readback)
	}
	v6cmd, v6req := bankedV6Proposal(t, c, "PATCH", "SETTINGS", "noise_class: RURAL\n")
	if response := s.applyBankedV6MachineRequest(c, v6cmd, v6req); !strings.Contains(response, "persisted: true") {
		t.Fatal(response)
	}
	current, err = c.captureV7Configuration()
	if err != nil || !current.Settings.CWSpeed || current.Settings.NoiseClass != "RURAL" {
		t.Fatalf("v6 write lost CW speed or requested setting: %+v %v", current.Settings, err)
	}
	if current.Filters.CC.Fingerprint() != record.Configuration.Filters.CC.Fingerprint() {
		t.Fatal("v6 settings write changed the native bank")
	}
}

func TestCWSpeedV7PartialAndOldCompleteSettings(t *testing.T) {
	cfg, _ := v7PreparedFixture(t)
	cfg.Settings.CWSpeed = true
	patch := decodeV7PatchFixture(t, "SETTINGS", "noise_class: RURAL\n")
	changed, err := patch.apply(cfg, "go")
	if err != nil || !changed.Settings.CWSpeed {
		t.Fatalf("omitted partial field cleared CW speed: %v", err)
	}
	newConfig, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	newRequest, err := decodeV7MachineRequest(v7MachineRequestFixture(string(newConfig), true), machineCommand{Verb: "PUT", Resource: "CONFIG", SchemaVersion: 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	off := cfg.Clone()
	off.Settings.CWSpeed = false
	restored, err := newRequest.apply(off, "go")
	if err != nil || !restored.Settings.CWSpeed {
		t.Fatalf("complete restore lost enabled CW speed: %v", err)
	}
	old := cfg.Clone()
	old.Settings.CWSpeed = false
	encoded, err := yaml.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	request, err := decodeV7MachineRequest(v7MachineRequestFixture(string(encoded), true), machineCommand{Verb: "PUT", Resource: "CONFIG", SchemaVersion: 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := request.apply(cfg, "go")
	if err != nil || got.Settings.CWSpeed {
		t.Fatalf("old complete restore failed to default CW speed off: %v", err)
	}
	for _, value := range []string{"null", "1", `"true"`} {
		_, err := decodeV7MachineRequest(v7MachineRequestFixture("cw_speed: "+value+"\n", true), machineCommand{Verb: "PATCH", Resource: "SETTINGS", SchemaVersion: 7}, nil)
		if err == nil {
			t.Fatalf("invalid cw_speed %s accepted", value)
		}
	}
}

func TestCWSpeedV7GetSettingsAndConfigExplicitBoolean(t *testing.T) {
	s, c := v7MachineTestClient(t)
	bankedV6SelectGO(t, c)
	for _, enabled := range []bool{false, true} {
		if enabled {
			cmd, req := v7ProposalFixture(t, c, "PATCH", "SETTINGS", "cw_speed: true\n")
			if response := s.applyV7MachineRequest(c, cmd, req); !strings.Contains(response, "persisted: true") {
				t.Fatal(response)
			}
		}
		for _, resource := range []string{"SETTINGS", "CONFIG"} {
			result := s.handleV7MachineCommand(c, "GET YAML "+resource+" SCHEMA 7 ID test")
			if !result.handled || result.terminal {
				t.Fatalf("%s enabled=%v dispatch: %+v", resource, enabled, result)
			}
			msg := <-c.controlChan
			var document struct {
				Configuration map[string]any `yaml:"configuration"`
				Status        struct {
					Configured map[string]any `yaml:"configured"`
				} `yaml:"status"`
			}
			err := yaml.Unmarshal(msg.raw, &document)
			msg.configurationReservation.Release()
			if err != nil {
				t.Fatal(err)
			}
			settings := document.Configuration
			if resource == "CONFIG" {
				var ok bool
				settings, ok = settings["settings"].(map[string]any)
				if !ok {
					t.Fatalf("CONFIG settings missing or invalid: %+v", document.Configuration)
				}
			}
			for name, values := range map[string]map[string]any{"configuration": settings, "status": document.Status.Configured} {
				value, present := values["cw_speed"]
				if !present || value != enabled {
					t.Fatalf("%s %s enabled=%v: cw_speed=%v present=%v", resource, name, enabled, value, present)
				}
			}
			v6result := s.handleV6MachineCommand(c, "GET YAML "+resource+" SCHEMA 6 ID test")
			if !v6result.handled || v6result.terminal {
				t.Fatalf("v6 %s enabled=%v dispatch: %+v", resource, enabled, v6result)
			}
			v6msg := <-c.controlChan
			v6readback := string(v6msg.raw)
			v6msg.configurationReservation.Release()
			if strings.Contains(v6readback, "cw_speed") || strings.Contains(v6readback, "error:") {
				t.Fatalf("v6 %s enabled=%v exposed CW speed or failed: %s", resource, enabled, v6readback)
			}
		}
	}
}
