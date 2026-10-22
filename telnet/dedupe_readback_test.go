package telnet

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

// The literal scenario matrix shares complete owner checks; keeping them in
// one table-driven test makes each expected interpretation directly reviewable.
func TestDedupeReadbacksPreserveConfiguredOwners(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, tc := range []struct {
			name, raw, effective, note string
			fast, med, slow            bool
			nearby, usable             bool
		}{
			{"fast", "FAST", "FAST", "", true, true, true, false, false},
			{"med", "MED", "MED", "", true, true, true, false, false},
			{"slow", "SLOW", "SLOW", "", true, true, true, false, false},
			{"unavailable", "SLOW", "FAST", "Configured choice is unavailable on this server", true, false, false, false, false},
			{"default", "", "SLOW", "", true, true, true, false, false},
			{"mixed", "sLoW", "SLOW", "", true, true, true, false, false},
			{"padded", " SLOW ", "SLOW", "", true, true, true, false, false},
			{"whitespace", " \t ", "MED", "Configured choice is unrecognized", true, true, true, false, false},
			{"unknown", "OTHER", "MED", "Configured choice is unrecognized", true, true, true, false, false},
			{"nearby", "SLOW", "FAST", "", true, true, true, true, true},
			{"inactive-nearby", "SLOW", "SLOW", "", true, true, true, true, false},
			{"nearby-fallback", "SLOW", "MED", "FAST is unavailable on this server", false, true, true, true, true},
			{"disabled", "SLOW", "FAST", "secondary duplicate suppression disabled", false, false, false, false, false},
		} {
			t.Run(string(dialect)+"/"+tc.name, func(t *testing.T) {
				s := presetTestServer(t)
				s.dedupeFastEnabled, s.dedupeMedEnabled, s.dedupeSlowEnabled = tc.fast, tc.med, tc.slow
				s.defaultDedupePolicy, s.defaultDedupeSet = dedupePolicySlow, true
				c := configurationTestClient(s, "W1ABC-1")
				record := &filter.UserRecord{Filter: *testRuleFilter(t), Dialect: string(dialect), DedupePolicy: tc.raw}
				prepared, err := filter.PrepareUserRecord(c.callsign, record)
				if err != nil {
					t.Fatal(err)
				}
				if err := prepared.Commit(); err != nil {
					t.Fatal(err)
				}
				s.restoreClientRecordOwned(c, time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
				if c.recordProtected || !c.configurationInitialized || c.configuredSettings.DedupePolicy != tc.raw {
					t.Fatal("marked record did not restore exact configuration")
				}
				c.filter.NearbyEnabled = tc.nearby
				if tc.usable {
					c.filter.NearbyUserFine, c.filter.NearbyUserCoarse = 1, 2
				}
				baseline, err := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Preset()
				if err != nil {
					t.Fatal(err)
				}
				c.presetReference = &filter.PresetReference{Name: "CONTROL13", Baseline: baseline}
				if err := c.saveFilter(); err != nil {
					t.Fatal(err)
				}
				before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
				ref, err := c.presetReference.Clone()
				if err != nil {
					t.Fatal(err)
				}
				revision, runtime := c.configurationRevision, c.getDedupePolicy()
				path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
				saved, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				attempts := 0
				s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
				for range 2 {
					show, handled := s.handleDedupeCommand(c, "show dedupe")
					settings, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
					if !handled || err != nil {
						t.Fatalf("read failed: %q, %v", show, err)
					}
					for _, output := range []string{show, settings} {
						assertHumanWire(t, output)
						choice := tc.raw
						if choice == "" {
							choice = "DEFAULT"
						}
						if !simpleHumanValue(choice) {
							if got := unquoteHumanPieces(t, output); got != choice {
								t.Fatalf("exact configured value lost: %q != %q", got, choice)
							}
						} else if !strings.Contains(output, choice) {
							t.Fatalf("configured value absent: %q", output)
						}
						if tc.note != "" && !strings.Contains(output, tc.note) {
							t.Fatalf("readback lacks reason %q: %q", tc.note, output)
						}
						if tc.note == "" && strings.Contains(output, "Configured choice is") {
							t.Fatalf("recognized/default choice misclassified: %q", output)
						}
					}
					if tc.name == "disabled" {
						if strings.Contains(show, "effective FAST") || strings.Contains(show, "(grid2)") {
							t.Fatal("disabled suppression displayed an active FAST sentinel")
						}
					} else if tc.usable {
						if !strings.Contains(show, "NEARBY effective "+tc.effective) || !strings.Contains(settings, "effective "+tc.effective+" while NEARBY is active") {
							t.Fatal("NEARBY runtime override absent")
						}
					} else if tc.raw == "" || tc.raw == "OTHER" || tc.raw == " \t " || tc.name == "unavailable" {
						if !strings.Contains(show, "effective "+tc.effective) || !strings.Contains(settings, "effective "+tc.effective) {
							t.Fatal("configured/effective distinction absent")
						}
					}
					if s.effectiveDedupePolicyForClient(c).label() != tc.effective {
						t.Fatal("readback changed existing runtime interpretation")
					}
					after, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(saved, after) || attempts != 0 || c.configurationRevision != revision ||
						c.getDedupePolicy() != runtime || !reflect.DeepEqual(ref, c.presetReference) ||
						!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
						t.Fatal("read changed configured/runtime/preset/revision/disk owner")
					}
				}
			})
		}
	}
}

func TestDedupeReadbackEscapesAndBudget(t *testing.T) {
	for _, value := range []string{strings.Repeat(" \"\\é\t", 500), string([]byte{0xff, 0, '\r', '\n'}),
		strings.Repeat("\x01", 16384), strings.Repeat("X", maxYAMLBytes+1)} {
		s, c := readbackTestClient()
		s.dedupeFastEnabled, s.dedupeMedEnabled, s.dedupeSlowEnabled = true, true, true
		c.configurationInitialized = true
		c.configuredSettings.DedupePolicy = value
		c.setDedupePolicy(dedupePolicyMed)
		c.refreshConfigurationRevision()
		before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
		revision := c.configurationRevision
		show := s.formatDedupeStatus(c)
		settings, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
		if len(value) >= 16384 {
			if show != "Readback failed: response exceeds 65,536 bytes.\r\n" || settings != "" || !errors.Is(err, errReadbackTooLarge) {
				t.Fatalf("over-budget output partially succeeded: show=%d, settings=%d, err=%v", len(show), len(settings), err)
			}
		} else {
			if err != nil || unquoteHumanPieces(t, show) != value || unquoteHumanPieces(t, settings) != value {
				t.Fatal("quoted response lost bytes or broke complete escapes")
			}
			assertHumanWire(t, settings)
		}
		assertHumanWire(t, show)
		if c.configurationRevision != revision || c.getDedupePolicy() != dedupePolicyMed || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
			t.Fatal("budget refusal or quoting changed an owner")
		}
	}
}

func TestDedupeReadbackFinalSendBudget(t *testing.T) {
	s, c := readbackTestClient()
	s.dedupeFastEnabled, s.dedupeMedEnabled, s.dedupeSlowEnabled = true, true, true
	c.setDedupePolicy(dedupePolicyMed)
	c.configuredSettings.DedupePolicy = strings.Repeat("X", 49366)
	// Literal review vector: the body fits exactly, but an automatic footer
	// would put its final wire response over the limit. No large file is needed.
	if body := s.formatDedupeStatus(c); len(body) != maxYAMLBytes {
		t.Fatalf("complete body boundary changed: %d", len(body))
	}
	s.autoReadPauseMinRows, s.autoReadPauseDuration = 3, 30*time.Second
	for _, count := range []int{49200, 49366} {
		c.configuredSettings.DedupePolicy = strings.Repeat("X", count)
		response := s.formatDedupeStatus(c)
		if count == 49366 && response != "Readback failed: response exceeds 65,536 bytes.\r\n" {
			t.Fatal("body-only admission ignored the final automatic footer")
		}
		if count == 49200 && !strings.HasPrefix(response, "Dedupe: ") {
			t.Fatal("fitting large readback was refused")
		}
		if !s.sendCommandResponse(c, response, "dedupe command response") {
			t.Fatal("actual response queue refused the test")
		}
		message := <-c.controlChan
		wire := strings.ReplaceAll(strings.ReplaceAll(message.line, "\r\n", "\n"), "\n", "\r\n")
		if got := string(appendWriterNormalized(nil, message.line)); got != wire {
			t.Fatal("writer transform differs from complete CRLF wire expectation")
		}
		assertHumanWire(t, wire)
		if count == 49200 && !strings.Contains(wire, "Type RESUME to resume now.") {
			t.Fatal("positive send control bypassed automatic pause")
		}
	}
}
