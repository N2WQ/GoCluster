package telnet

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestV6MachineCoordinatorFailuresPreserveLiveAndDisk(t *testing.T) {
	for _, failure := range []string{
		"protected", "stale", "commit", "unavailable-setting", "padded-setting", "revision-exhausted",
		"legacy-destination", "v4-destination", "v5-destination", "future-destination", "malformed-destination", "oversized-destination",
		"prospective-record", "escaped-full-readback", "oversized-readback-metadata",
	} {
		t.Run(failure, func(t *testing.T) {
			s, c, cfg := v6CoordinatorFixture(t)
			disk := v6CoordinatorSeed(t, c, cfg)
			libraryPath, library := v6CoordinatorLibrary(t, c, cfg)
			cmd, request := v6CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"noise_class": "URBAN"})
			code := "invalid_configuration"
			c.startReadPause(time.Now(), time.Minute)
			c.setDiagMode(diagModePath)
			c.historySearch = &historySearch{token: "keep"}
			commits := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return os.ErrPermission }
			path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
			raw := ""
			switch failure {
			case "protected":
				c.recordProtected, code = true, "record_protected"
			case "stale":
				request.IfRevision, code = "old-session-0", "revision_conflict"
			case "commit":
				code = "persistence_failed"
			case "unavailable-setting":
				s.dedupeSlowEnabled = false
				cmd, request = v6CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"dedupe_policy": "SLOW"})
			case "padded-setting":
				cmd, request = v6CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"noise_class": " URBAN "})
			case "revision-exhausted":
				c.configurationRevision = math.MaxUint64
			case "legacy-destination":
				raw = "all_bands: true\n"
			case "v4-destination":
				raw = "configuration_version: 4\nall_bands: true\n"
			case "v5-destination":
				raw = "configuration_version: 5\nconfiguration: {}\n"
			case "future-destination":
				raw = "configuration_version: 7\nconfiguration: {}\n"
			case "malformed-destination":
				raw = "configuration_version: 6\nconfiguration: [\n"
			case "oversized-destination":
				raw = strings.Repeat("x", filter.MaxUserRecordBytes+1)
			case "prospective-record":
				c.recentIPs = []string{strings.Repeat("x", filter.MaxUserRecordBytes)}
			case "escaped-full-readback":
				// Byte length fits metadata's lower bound; exact escaping does not.
				c.grid = strings.Repeat("\x01", maxYAMLBytes/3)
				code = "response_too_large"
			case "oversized-readback-metadata":
				c.grid = strings.Repeat("X", maxYAMLBytes+1)
				code = "response_too_large"
			}
			if raw != "" {
				disk = []byte(raw)
				if err := os.WriteFile(path, disk, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			request.IfRevision = v6CoordinatorRevision(t, c)
			if failure == "stale" {
				request.IfRevision = "old-session-0"
			}
			before := snapshotBooleanCommand(c)
			ref, clock, pause, revision, generation, cursor := c.presetReference, c.solarNextSummaryAt,
				c.readPauseUntilUnixNano.Load(), c.configurationRevision, c.historyGeneration, c.historySearch
			response := s.applyV6MachineRequest(c, cmd, request)
			if !strings.Contains(response, "code: "+code+"\r\n") || strings.Contains(response, "applied: true") ||
				strings.Contains(response, "persisted: true") {
				t.Fatalf("failure returned wrong status: %s", response)
			}
			if failure == "commit" && commits != 1 || failure != "commit" && commits != 0 {
				t.Fatalf("commit reached before all gates: %d", commits)
			}
			before.assertUnchanged(t, c)
			if after, err := os.ReadFile(libraryPath); err != nil || !bytes.Equal(library, after) {
				t.Fatal("failed write changed the independent preset library")
			}
			if c.presetReference != ref || c.solarNextSummaryAt != clock || c.configurationRevision != revision ||
				c.historyGeneration != generation || c.historySearch != cursor ||
				c.readPauseUntilUnixNano.Load() != pause || c.getDiagMode() != diagModePath ||
				!bytes.Equal(disk, v6CoordinatorDisk(t, c)) {
				t.Fatal("failed coordinator changed live state, history or durable bytes")
			}
		})
	}
}

// The effective GRID is empty so a missing cache cannot be silently repaired by
// the complete test mapping tables. Each captured usable resolution must survive.
func TestV6MachineCoordinatorNearbyPartialGeometry(t *testing.T) {
	requireH3Mappings(t)
	for _, coarseOnly := range []bool{false, true} {
		for _, verb := range []string{"PATCH", "PUT"} {
			for _, resource := range []string{"FILTER", "CONFIG"} {
				t.Run(fmt.Sprintf("coarse=%v/%s/%s", coarseOnly, verb, resource), func(t *testing.T) {
					s, c, cfg := v6CoordinatorFixture(t)
					set, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW AND NEARBY"}})
					if err != nil {
						t.Fatal(err)
					}
					cfg.Filters = cfg.Filters.WithRuleSet(set)
					cfg, f, err := filter.PrepareV6Configuration(cfg)
					if err != nil {
						t.Fatal(err)
					}
					c.filter, c.configuredSettings = &f, cfg.Settings
					c.grid = ""
					c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
					if coarseOnly {
						c.gridCell = pathreliability.InvalidCell
					} else {
						c.gridCoarseCell = pathreliability.InvalidCell
					}
					state, pointer := c.pathSnapshot(), c.filter
					assertMatches := func(mode string, allowed bool) {
						t.Helper()
						for _, frequency := range []float64{14030, 3530} {
							sp := spot.NewSpot("K1ABC", "N2AAA", frequency, mode)
							sp.IsHuman, sp.DXMetadata.Grid = true, "FN31"
							want := allowed && (frequency == 3530) == coarseOnly
							got, _, _ := s.matchSnapshot(c.captureMatchingSnapshot(), sp)
							if got != want {
								t.Fatalf("%s %.0f matched=%v want %v", mode, frequency, got, want)
							}
						}
					}
					assertMatches("CW", true)
					assertMatches("RTTY", false)
					v6CoordinatorSeed(t, c, cfg)
					next := cfg
					set, err = filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW,RTTY AND NEARBY"}})
					if err != nil {
						t.Fatal(err)
					}
					next.Filters = next.Filters.WithRuleSet(set)
					var body any = next.Filters
					if resource == "CONFIG" {
						body = next
					}
					if verb == "PATCH" {
						body = map[string]any{"rules": next.Filters.Rules}
						if resource == "CONFIG" {
							body = map[string]any{"filters": body}
						}
					}
					cmd, request := v6CoordinatorProposal(t, c, verb, resource, body)
					v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
					if c.filter != pointer || c.pathSnapshot() != state {
						t.Fatal("YAML rule update changed captured location or Filter identity")
					}
					assertMatches("CW", true)
					assertMatches("RTTY", true)
					loaded, err := filter.LoadV6UserRecord(c.callsign)
					if err != nil || !loaded.Configuration.Equal(next) || !c.captureV6Configuration().Equal(next) {
						t.Fatalf("YAML update lost exact saved rules/settings: %v", err)
					}
				})
			}
		}
	}
}

func TestV6MachineCoordinatorCanonicalNearbyReadbackAndDedupe(t *testing.T) {
	requireH3Mappings(t)
	s, c, cfg := v6CoordinatorFixture(t)
	c.grid, c.gridDerived = "FN31", true
	c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell(c.grid), pathreliability.EncodeCoarseCell(c.grid)
	c.configuredSettings.DedupePolicy = "SLOW"
	c.setDedupePolicy(dedupePolicySlow)
	cfg.Settings = c.configuredSettings
	v6CoordinatorSeed(t, c, cfg)
	cmd, request := v6CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"rules": []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "NEARBY AND DXCONT=EU AND MODE=CW"}}})
	v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
	state := c.pathSnapshot()
	doc := v6ReadbackDocument(t, s.prepareV6MachineReadback(c, machineCommand{Verb: "GET", Resource: "CONFIG", RequestID: "nearby"}))
	status := doc["status"].(map[string]any)
	entry := status["rules"].(map[string]any)["entries"].([]any)[0].(map[string]any)
	effective := status["effective"].(map[string]any)
	if entry["expr"] != "NEARBY AND DXCONT=EU AND MODE=CW" || len(entry) != 3 || effective["dedupe_policy"] != "SLOW" {
		t.Fatal("readback lost canonical geography or changed dedupe")
	}
	if _, present := effective["nearby_active"]; present {
		t.Fatal("retired global status survived")
	}
	cmd, request = v6CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"rules": []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "NOT NEARBY OR DXCONT=AS"}}})
	v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), true)
	if c.pathSnapshot() != state || c.getDedupePolicy() != dedupePolicySlow {
		t.Fatal("rule edit changed location or dedupe preference")
	}
}

func TestV6MachineCoordinatorUnavailableRuleRecovery(t *testing.T) {
	assertV6UnavailableRuleRecovery(t, "")
}

// The no-table child in TestGridMappingAvailability calls this with explicit
// FN31. That witness is distinct from the empty-GRID lookup sentinel controls.
func assertV6UnavailableRuleRecovery(t *testing.T, configuredGrid string) {
	t.Helper()
	if configuredGrid != "" && (pathreliability.EncodeCell(configuredGrid) != pathreliability.InvalidCell || pathreliability.EncodeCoarseCell(configuredGrid) != pathreliability.InvalidCell) {
		t.Fatal("recovery witness requires unavailable mapping")
	}
	for _, expression := range []string{"MODE=FT8 AND DXCONT=AS", "NEARBY OR MODE=FT8"} {
		for _, verb := range []string{"PATCH", "PUT", "VALIDATE"} {
			for _, resource := range []string{"FILTER", "CONFIG"} {
				if verb == "VALIDATE" && resource != "CONFIG" {
					continue
				}
				t.Run(expression+"/"+verb+"/"+resource, func(t *testing.T) {
					s, c, cfg := v6CoordinatorFixture(t)
					cfg.Settings.Grid = configuredGrid
					cfg, f, err := filter.PrepareV6Configuration(cfg)
					if err != nil {
						t.Fatal(err)
					}
					c.filter, c.configuredSettings = &f, cfg.Settings
					c.grid, c.gridCell, c.gridCoarseCell = configuredGrid, pathreliability.InvalidCell, pathreliability.InvalidCell
					disk := v6CoordinatorSeed(t, c, cfg)
					next := cfg
					set, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: expression}})
					if err != nil {
						t.Fatal(err)
					}
					next.Filters = next.Filters.WithRuleSet(set)
					var body any = next.Filters
					if resource == "CONFIG" {
						body = next
					}
					if verb == "PATCH" {
						body = map[string]any{"rules": next.Filters.Rules}
						if resource == "CONFIG" {
							body = map[string]any{"filters": body}
						}
					}
					cmd, request := v6CoordinatorProposal(t, c, verb, resource, body)
					v6CoordinatorSuccess(t, s.applyV6MachineRequest(c, cmd, request), verb != "VALIDATE")
					if c.grid != configuredGrid || c.configuredSettings.Grid != configuredGrid || c.gridCell != pathreliability.InvalidCell || c.gridCoarseCell != pathreliability.InvalidCell {
						t.Fatal("rule recovery changed captured location")
					}
					if verb == "VALIDATE" {
						if !bytes.Equal(disk, v6CoordinatorDisk(t, c)) || !c.captureV6Configuration().Equal(cfg) {
							t.Fatal("VALIDATE published or persisted rule recovery")
						}
					} else {
						loaded, err := filter.LoadV6UserRecord(c.callsign)
						if err != nil || !loaded.Configuration.Equal(next) || !c.captureV6Configuration().Equal(next) {
							t.Fatalf("recovery failed durable reload: %v", err)
						}
					}
				})
			}
		}
	}
	if configuredGrid != "" {
		assertV6RecoveryRejectsOtherChanges(t, configuredGrid)
	}
}

func assertV6RecoveryRejectsOtherChanges(t *testing.T, grid string) {
	t.Helper()
	for _, failure := range []string{"control", "settings", "grid-syntax", "disabled-dedupe", "invalid-noise", "invalid-path"} {
		t.Run(failure, func(t *testing.T) {
			s, c, cfg := v6CoordinatorFixture(t)
			cfg.Settings.Grid = grid
			switch failure {
			case "disabled-dedupe":
				cfg.Settings.DedupePolicy = "SLOW"
				s.dedupeSlowEnabled = false
			case "invalid-noise":
				cfg.Settings.NoiseClass = "INVALID"
			case "invalid-path":
				cfg.Settings.PathMinObservationCount = -1
			}
			cfg, f, err := filter.PrepareV6Configuration(cfg)
			if err != nil {
				t.Fatal(err)
			}
			c.filter, c.configuredSettings = &f, cfg.Settings
			c.grid = grid
			v6CoordinatorSeed(t, c, cfg)
			set, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "NEARBY"}})
			if err != nil {
				t.Fatal(err)
			}
			next := cfg
			next.Filters = next.Filters.WithRuleSet(set)
			switch failure {
			case "control":
				next.Filters.AllowSelf = filter.DefaultBoolFalse
			case "settings":
				next.Settings.NoiseClass = "URBAN"
			case "grid-syntax":
				next.Settings.Grid = "FN31JUNK"
			}
			before := snapshotBooleanCommand(c)
			disk := v6CoordinatorDisk(t, c)
			commits := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
			cmd, request := v6CoordinatorProposal(t, c, "PUT", "CONFIG", next)
			response := s.applyV6MachineRequest(c, cmd, request)
			if !strings.Contains(response, "code: invalid_configuration") || commits != 0 {
				t.Fatalf("recovery waived other admission: %s", response)
			}
			before.assertUnchanged(t, c)
			if !bytes.Equal(disk, v6CoordinatorDisk(t, c)) {
				t.Fatal("refusal changed disk")
			}
		})
	}
}
