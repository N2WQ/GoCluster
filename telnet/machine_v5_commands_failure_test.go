package telnet

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestV5MachineCoordinatorFailuresPreserveLiveAndDisk(t *testing.T) {
	for _, failure := range []string{
		"protected", "stale", "commit", "unavailable-setting", "padded-setting", "revision-exhausted",
		"legacy-destination", "v4-destination", "future-destination", "malformed-destination", "oversized-destination",
		"prospective-record", "escaped-full-readback", "oversized-readback-metadata", "nearby-unavailable",
		"nearby-enable-with-rules", "nearby-rule-and-control",
	} {
		t.Run(failure, func(t *testing.T) {
			s, c, cfg := v5CoordinatorFixture(t)
			disk := v5CoordinatorSeed(t, c, cfg)
			libraryPath, library := v5CoordinatorLibrary(t, c, cfg)
			cmd, request := v5CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"noise_class": "URBAN"})
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
				cmd, request = v5CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"dedupe_policy": "SLOW"})
			case "padded-setting":
				cmd, request = v5CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"noise_class": " URBAN "})
			case "revision-exhausted":
				c.configurationRevision = math.MaxUint64
			case "legacy-destination":
				raw = "all_bands: true\n"
			case "v4-destination":
				raw = "configuration_version: 4\nall_bands: true\n"
			case "future-destination":
				raw = "configuration_version: 6\nconfiguration: {}\n"
			case "malformed-destination":
				raw = "configuration_version: 5\nconfiguration: [\n"
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
			case "nearby-unavailable":
				c.gridCell, c.gridCoarseCell = pathreliability.InvalidCell, pathreliability.InvalidCell
				cmd, request = v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"nearby_enabled": true})
			case "nearby-enable-with-rules":
				c.grid, c.gridCell, c.gridCoarseCell = "", pathreliability.InvalidCell, pathreliability.InvalidCell
				cmd, request = v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{
					"nearby_enabled": true, "rules": []any{},
				})
			case "nearby-rule-and-control":
				c.filter.NearbyEnabled = true
				c.grid, c.gridCell, c.gridCoarseCell = "", pathreliability.InvalidCell, pathreliability.InvalidCell
				cmd, request = v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{
					"allow_self": false, "rules": []any{},
				})
			}
			if raw != "" {
				disk = []byte(raw)
				if err := os.WriteFile(path, disk, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			request.IfRevision = v5CoordinatorRevision(t, c)
			if failure == "stale" {
				request.IfRevision = "old-session-0"
			}
			before := snapshotBooleanCommand(c)
			ref, clock, pause, revision, generation, cursor := c.presetReference, c.solarNextSummaryAt,
				c.readPauseUntilUnixNano.Load(), c.configurationRevision, c.historyGeneration, c.historySearch
			response := s.applyV5MachineRequest(c, cmd, request)
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
				!bytes.Equal(disk, v5CoordinatorDisk(t, c)) {
				t.Fatal("failed coordinator changed live state, history or durable bytes")
			}
		})
	}
}

func TestV5MachineCoordinatorNearbyPartialGeometry(t *testing.T) {
	requireH3Mappings(t)
	for _, geometry := range []struct {
		name       string
		coarseOnly bool
	}{{"fine-only", false}, {"coarse-only", true}} {
		for _, verb := range []string{"PATCH", "PUT"} {
			for _, resource := range []string{"FILTER", "CONFIG"} {
				t.Run(geometry.name+"/"+verb+"/"+resource, func(t *testing.T) {
					s, c, cfg := v5CoordinatorFixture(t)
					cfg.Filters.NearbyEnabled = true
					cfg, f, err := filter.PrepareV5Configuration(cfg)
					if err != nil {
						t.Fatal(err)
					}
					c.filter, c.configuredSettings = &f, cfg.Settings
					c.grid, c.gridDerived = "FN31", true
					// Path caches intentionally differ from NEARBY's retained location.
					c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell("IO91"), pathreliability.EncodeCoarseCell("IO91")
					fine, coarse := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
					if fine == pathreliability.InvalidCell || coarse == pathreliability.InvalidCell {
						t.Fatal("fixture requires usable FN31 mappings")
					}
					if geometry.coarseOnly {
						fine = pathreliability.InvalidCell
					} else {
						coarse = pathreliability.InvalidCell
					}
					c.filter.UpdateNearbyUserCells(fine, coarse)
					state, pointer := c.pathSnapshot(), c.filter
					assertMatches := func(mode string, allowed bool) {
						t.Helper()
						for _, frequency := range []float64{14030, 3530} {
							sp := spot.NewSpot("K1ABC", "N2AAA", frequency, mode)
							sp.IsHuman, sp.DXMetadata.Grid = true, "FN31"
							want := allowed && (frequency == 3530) == geometry.coarseOnly
							got, _, _ := s.matchSnapshot(c.captureMatchingSnapshot(), sp)
							if got != want {
								t.Fatalf("%s %.0f matched=%v, want %v", mode, frequency, got, want)
							}
						}
					}
					assertMatches("CW", true)
					assertMatches("RTTY", false)
					v5CoordinatorSeed(t, c, cfg)
					set, err := filter.PrepareRuleSet([]filter.RuleDefinition{{
						Slot: 0, Action: filter.RulePass, Expr: "(MODE=CW OR MODE=RTTY) AND DXCONT=EU",
					}})
					if err != nil {
						t.Fatal(err)
					}
					next := cfg
					next.Filters = cfg.Filters.WithRuleSet(set)
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
					cmd, request := v5CoordinatorProposal(t, c, verb, resource, body)
					v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
					if c.filter != pointer || !c.filter.NearbyEnabled || c.pathSnapshot() != state ||
						c.filter.NearbyUserFine != fine || c.filter.NearbyUserCoarse != coarse {
						t.Fatal("YAML rule update changed existing NEARBY or path state")
					}
					assertMatches("CW", true)
					assertMatches("RTTY", true)
					record, err := filter.LoadV5UserRecord(c.callsign)
					if err != nil || !record.Configuration.Equal(next) || !c.captureV5Configuration().Equal(next) {
						t.Fatalf("YAML rule update lost saved rules/settings: %v", err)
					}
				})
			}
		}
	}
}

func TestV5MachineCoordinatorNearbyPreparedProjection(t *testing.T) {
	requireH3Mappings(t)
	s, c, cfg := v5CoordinatorFixture(t)
	c.grid, c.gridDerived = "FN31", true
	c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell(c.grid), pathreliability.EncodeCoarseCell(c.grid)
	v5CoordinatorRevision(t, c)
	cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"nearby_enabled": true})
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	if c.filter.NearbyUserFine != c.gridCell || c.filter.NearbyUserCoarse != c.gridCoarseCell ||
		!c.gridDerived ||
		c.configuredSettings.Grid != "" {
		t.Fatal("effective engine lost derived geometry or restored old category maps")
	}
	doc := v5ReadbackDocument(t, s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: "CONFIG", RequestID: "nearby"}))
	rules := doc["status"].(map[string]any)["rules"].(map[string]any)
	if rules["entries"].([]any)[0].(map[string]any)["effective_expr"] != "(MODE=CW) AND SOURCE=HUMAN" ||
		doc["status"].(map[string]any)["effective"].(map[string]any)["nearby_active"] != true ||
		!slices.Equal(c.captureV5Configuration().Filters.Rules, cfg.Filters.Rules) {
		t.Fatal("actual readback lost full rules or effective geographic projection")
	}
	cmd, request = v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{
		"rules": []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND DXCONT=AS"}},
	})
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	if c.filter.NearbyUserFine != c.gridCell || c.filter.NearbyUserCoarse != c.gridCoarseCell {
		t.Fatal("rule edit lost usable NEARBY cells")
	}
	// Rule-only recovery preserves the unavailable selection and saved grid.
	c.filter.NearbyUserFine, c.filter.NearbyUserCoarse = pathreliability.InvalidCell, pathreliability.InvalidCell
	cmd, request = v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"rules": []any{}})
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	if !c.filter.NearbyEnabled || c.filter.NearbyUserFine != pathreliability.InvalidCell ||
		c.filter.NearbyUserCoarse != pathreliability.InvalidCell || len(c.captureV5Configuration().Filters.Rules) != 0 {
		t.Fatal("rule recovery changed the unavailable NEARBY selection")
	}
}

func TestV5MachineCoordinatorNearbyUnavailableRuleRecovery(t *testing.T) {
	assertV5NearbyUnavailableRuleRecovery(t, "")
}

func assertV5NearbyUnavailableRuleRecovery(t *testing.T, configuredGrid string) {
	t.Helper()
	for _, verb := range []string{"PATCH", "PUT", "VALIDATE"} {
		t.Run(verb, func(t *testing.T) {
			s, c, cfg := v5CoordinatorFixture(t)
			cfg.Filters.NearbyEnabled = true
			cfg.Settings.Grid = configuredGrid
			cfg, f, err := filter.PrepareV5Configuration(cfg)
			if err != nil {
				t.Fatal(err)
			}
			c.filter = &f
			c.configuredSettings = cfg.Settings
			c.grid, c.gridCell, c.gridCoarseCell = configuredGrid, pathreliability.InvalidCell, pathreliability.InvalidCell
			disk := v5CoordinatorSeed(t, c, cfg)
			next := cfg
			set, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND DXCONT=AS"}})
			if err != nil {
				t.Fatal(err)
			}
			next.Filters = next.Filters.WithRuleSet(set)
			resource, body := "FILTER", any(next.Filters)
			switch verb {
			case "PATCH":
				body = map[string]any{"rules": next.Filters.Rules}
			case "VALIDATE":
				resource, body = "CONFIG", next
			}
			cmd, request := v5CoordinatorProposal(t, c, verb, resource, body)
			v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), verb != "VALIDATE")
			if !c.filter.NearbyEnabled || c.grid != configuredGrid || c.configuredSettings.Grid != configuredGrid ||
				c.filter.NearbyUserFine != pathreliability.InvalidCell ||
				c.filter.NearbyUserCoarse != pathreliability.InvalidCell {
				t.Fatal("rule recovery changed saved or effective NEARBY geometry")
			}
			if verb == "VALIDATE" {
				if !bytes.Equal(disk, v5CoordinatorDisk(t, c)) || !c.captureV5Configuration().Equal(cfg) {
					t.Fatal("VALIDATE published or persisted rule recovery")
				}
			} else {
				loaded, err := filter.LoadV5UserRecord(c.callsign)
				if err != nil || !loaded.Configuration.Equal(next) || !c.captureV5Configuration().Equal(next) {
					t.Fatalf("rule recovery did not survive durable reload: %v", err)
				}
			}
		})
	}
}
