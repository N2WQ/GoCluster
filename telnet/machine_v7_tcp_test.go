package telnet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func TestV7MachineRestoreOwnershipPresetsAndReconnectTCP(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "READY\n"
				opts.CommandRatePerSecond, opts.CommandBurst = 1000, 1000
				opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
			})
			const call = "N0CALL-88"
			conn, reader, done, _ := connectUpgradeSession(t, s, call)
			w := nativeTCPWitness{t: t, server: s, conn: conn, reader: reader}
			type envelope struct {
				Version       int                               `yaml:"schema_version"`
				RequestID     string                            `yaml:"request_id"`
				Resource      string                            `yaml:"resource"`
				Revision      string                            `yaml:"revision"`
				Configuration yaml.Node                         `yaml:"configuration"`
				Error         struct{ Code string }             `yaml:"error"`
				Result        struct{ Applied, Persisted bool } `yaml:"result"`
				Status        struct {
					Active  string                          `yaml:"active_dialect"`
					Presets map[string]readbackPresetStatus `yaml:"presets"`
				} `yaml:"status"`
			}
			serial, revision := 0, ""
			exchange := func(verb, resource string, version int, configuration any, errorCode string) envelope {
				t.Helper()
				serial++
				id := fmt.Sprintf("Machine-TCP-%d", serial)
				request := fmt.Sprintf("%s YAML %s SCHEMA %d", verb, resource, version)
				if verb == "GET" {
					request += " ID " + id + "\r\n"
				} else {
					body, err := yaml.Marshal(map[string]any{"schema_version": version, "request_id": id, "if_revision": revision, "configuration": configuration})
					if err != nil {
						t.Fatal(err)
					}
					request += "\r\n---\r\n" + strings.ReplaceAll(string(body), "\n", "\r\n") + "...\r\n"
				}
				if _, err := io.WriteString(conn, request); err != nil {
					t.Fatal(err)
				}
				frame := readMachineTestFrame(t, reader)
				var document envelope
				if err := yaml.Unmarshal([]byte(frame), &document); err != nil {
					t.Fatal(err)
				}
				if document.Version != version || document.RequestID != id || document.Resource != resource || document.Error.Code != errorCode || !strings.Contains(frame, fmt.Sprintf("schema_version: %d\r\n", version)) || strings.ContainsAny(strings.ReplaceAll(frame, "\r\n", ""), "\r\n") {
					t.Fatalf("wrong correlated version/resource/result or CRLF frame: %s", frame)
				}
				if verb != "GET" && errorCode == "" && (!document.Result.Applied || !document.Result.Persisted) {
					t.Fatalf("upload did not durably apply: %s", frame)
				}
				if document.Revision != "" {
					revision = document.Revision
				}
				return document
			}
			initial, _ := w.configuration("DIALECT GO\r\nPASS RULE 3 MODE=FT8 AND SOURCE=SKIMMER\r\nDIALECT CC\r\n")
			wanted := initial.Clone()
			wanted.Filters.GO.Rules = []filter.RuleDefinition{{Slot: 4, Action: filter.RulePass, Expr: "MODE=CW AND SOURCE=HUMAN"}}
			wanted.Filters.CC.Modes, wanted.Filters.CC.Sources = []string{"CW"}, []string{"HUMAN"}
			wanted.Filters.CC.Geography["DOC"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCPass, Countries: []int{291}}, Bands: map[string]filter.CCCategory{"20": {Action: filter.CCOff}}}
			wanted.Filters.CC.DXBM = filter.CCDXBM{Active: true, BaseAllow: false, Slots: map[string]bool{"20-CW": true}}
			exchange("GET", "CONFIG", 7, nil, "")
			exchange("PUT", "CONFIG", 7, wanted, "")
			var restored filter.V7Configuration
			full := exchange("GET", "CONFIG", 7, nil, "")
			if err := full.Configuration.Decode(&restored); err != nil || !restored.Equal(wanted) || full.Status.Active != "cc" {
				t.Fatalf("full restore lost either bank while CC active: %v", err)
			}
			selected := exchange("GET", "FILTER", 7, nil, "")
			var active struct {
				Dialect string
				Filters map[string]any
			}
			if err := selected.Configuration.Decode(&active); err != nil || active.Dialect != "cc" || active.Filters["modes"] == nil || active.Filters["rules"] != nil {
				t.Fatalf("FILTER7 exported the wrong owner: %v", err)
			}
			beforeRevision := revision
			exchange("PATCH", "FILTER", 7, map[string]any{"dialect": "go", "filters": map[string]any{"rules": []any{}}}, "invalid_document")
			if revision != beforeRevision {
				t.Fatal("wrong FILTER tag changed revision")
			}
			exchange("PATCH", "FILTER", 7, map[string]any{"dialect": "cc", "filters": map[string]any{"modes": []string{"FT8"}}}, "")
			wanted.Filters.CC.Modes = []string{"FT8"}
			full = exchange("GET", "CONFIG", 7, nil, "")
			if err := full.Configuration.Decode(&restored); err != nil || !restored.Equal(wanted) {
				t.Fatalf("FILTER7 changed inactive GO/shared state: %v", err)
			}
			for _, resource := range []string{"FILTER", "CONFIG"} {
				exchange("GET", resource, 6, nil, "schema7_required")
			}
			legacy := exchange("GET", "SETTINGS", 6, nil, "")
			var settings map[string]any
			if err := legacy.Configuration.Decode(&settings); err != nil || settings["dialect"] != "cc" || settings["rules"] != nil || settings["filters"] != nil {
				t.Fatalf("legacy CC settings fabricated filter data: %v", err)
			}
			beforeLoad, _ := w.configuration("SAVE PRESET NATIVE7\r\nDIALECT GO\r\nPASS RULE 7 MODE=RTTY\r\nSAVE PRESET BOOLEAN7\r\n")
			loaded, feedback := w.configuration("LOAD PRESET NATIVE7\r\n")
			if loaded.Settings.Dialect != "cc" || !loaded.Filters.CC.Equal(wanted.Filters.CC) || !(filter.V6Configuration{Filters: loaded.Filters.GO}).Equal(filter.V6Configuration{Filters: beforeLoad.Filters.GO}) || !strings.Contains(feedback, "Dialect switched to CC") {
				t.Fatal("tagged native LOAD lost inactive GO or failed to switch")
			}
			verify := func() {
				t.Helper()
				document := exchange("GET", "CONFIG", 7, nil, "")
				var actual filter.V7Configuration
				if err := document.Configuration.Decode(&actual); err != nil || !actual.Equal(loaded) || document.Status.Active != "cc" {
					t.Fatalf("complete machine snapshot lost final state: %v", err)
				}
				for dialect, name := range map[string]string{"go": "BOOLEAN7", "cc": "NATIVE7"} {
					status := document.Status.Presets[dialect]
					if !status.Associated || status.Modified || status.Name != name {
						t.Fatalf("%s reference lost/changed after tagged LOAD or reconnect: %+v", dialect, status)
					}
				}
				stored, err := os.ReadFile(filepath.Join(filter.UserDataDir, call+".yaml"))
				if err != nil || !strings.HasPrefix(string(stored), "configuration_version: 7\n") {
					t.Fatalf("immediate strict7 storage marker missing: %v", err)
				}
				record, err := filter.DecodeV7UserRecord(stored)
				if err != nil || !record.Configuration.Equal(loaded) || record.Presets.GO == nil || record.Presets.CC == nil || record.Presets.GO.Name != "BOOLEAN7" || record.Presets.CC.Name != "NATIVE7" || !record.Presets.GO.Baseline.MatchesConfiguration(loaded) || !record.Presets.CC.Baseline.MatchesConfiguration(loaded) {
					t.Fatalf("durable both-bank preferences/references differ: %v", err)
				}
			}
			verify() // The command save, not disconnect autosave, must be durable.
			closeHandshakeTranscriptSession(t, conn, done)
			conn, reader, _, _ = connectUpgradeSession(t, s, call)
			verify()
		})
	}
}
