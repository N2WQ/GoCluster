package telnet

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
)

func TestGridHumanExactSyntaxAndMachineRoundTrip(t *testing.T) {
	requireH3Mappings(t)
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			for _, value := range []string{"IO91", "fn31", "Fn31pR"} {
				response, handled := s.handlePathSettingsCommand(c, "set\tgrid  "+value)
				grid := strings.ToUpper(value)
				if !handled || response != "Grid set to "+grid+"\n" || c.grid != grid || c.configuredSettings.Grid != grid {
					t.Fatalf("valid GRID did not publish its canonical value: %q", response)
				}
				if err := s.validateMachineSettings(c.configuredSettings); err != nil {
					t.Fatalf("human configuration is machine-invalid: %v", err)
				}
				stored, err := filter.LoadUserRecord(c.callsign)
				if err != nil || stored.Grid != grid {
					t.Fatalf("canonical GRID not saved: %v", err)
				}
			}
			for _, command := range []string{
				"SET GRID", "SET GRID FN31 EXTRA", "SET GRID FN31 PR", "SET GRID FN",
				"SET GRID FN31P", "SET GRID FN31PRJUNK", "SET GRID FN31PR%v", "SET GRID FN31PRA",
				"SET GRID SN31", "SET GRID FS31", "SET GRID FN3A", "SET GRID FN31YZ", "SET GRID FN31PY",
			} {
				assertGridRefusalUnchanged(t, s, c, command, "")
			}
			c.filter = passRuleTestFilter(t, "NEARBY AND DXCONT=EU")
			programs := c.filter.PreparedRules()
			response, _ := s.handlePathSettingsCommand(c, "SET GRID IO91")
			if response != "Grid set to IO91\n" || c.gridCell != pathreliability.EncodeCell("IO91") ||
				c.gridCoarseCell != pathreliability.EncodeCoarseCell("IO91") ||
				c.filter.PreparedRules() != programs {
				t.Fatal("valid active GRID did not update both cells while retaining immutable rules")
			}
		})
	}
}

func assertGridRefusalUnchanged(t *testing.T, s *Server, c *Client, command, message string) {
	t.Helper()
	before := c.captureV6Configuration()
	pathBefore, revision := c.pathSnapshot(), c.configurationRevision
	nearFine, nearCoarse, programs := c.gridCell, c.gridCoarseCell, c.filter.PreparedRules()
	file := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	saved, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	priorCommit := s.commitConfigurationFn
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
	defer func() { s.commitConfigurationFn = priorCommit }()
	response, handled := s.handlePathSettingsCommand(c, command)
	if !handled || response == "" || (message != "" && !strings.Contains(response, message)) || attempts != 0 {
		t.Fatalf("GRID refusal was not effect-free: %q, saves=%d", response, attempts)
	}
	if !before.Equal(c.captureV6Configuration()) || c.pathSnapshot() != pathBefore ||
		c.configurationRevision != revision || c.gridCell != nearFine ||
		c.gridCoarseCell != nearCoarse || c.filter.PreparedRules() != programs {
		t.Fatalf("GRID refusal changed live configuration/runtime: %s", command)
	}
	after, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(saved, after) {
		t.Fatalf("GRID refusal changed saved bytes: %v", err)
	}
}

// Global H3 maps are initialized once. Both fault profiles execute in joined
// fresh children; the parent and the checked-in required tables stay intact.
func TestGridMappingAvailability(t *testing.T) {
	const profileEnv, directoryEnv = "GOCLUSTER_GRID_MAPPING_PROFILE", "GOCLUSTER_GRID_MAPPING_DIRECTORY"
	const marker = "GRID_MAPPING_CHILD_EXECUTED"
	if profile := os.Getenv(profileEnv); profile != "" {
		if profile == "COARSE_MISS" {
			if err := pathreliability.InitH3MappingsFromDir(os.Getenv(directoryEnv)); err != nil {
				t.Fatal(err)
			}
			if pathreliability.EncodeCell("FN31") == pathreliability.InvalidCell ||
				pathreliability.EncodeCoarseCell("FN31") != pathreliability.InvalidCell ||
				pathreliability.EncodeCell("IO91") == pathreliability.InvalidCell ||
				pathreliability.EncodeCoarseCell("IO91") == pathreliability.InvalidCell {
				t.Fatal("coarse-miss fixture lacks the separate old/target cell controls")
			}
		} else if profile != "NONE" || pathreliability.EncodeCell("FN31") != pathreliability.InvalidCell ||
			pathreliability.EncodeCoarseCell("FN31") != pathreliability.InvalidCell {
			t.Fatal("no-mapping child did not start without global maps")
		}
		for _, nearby := range []bool{false, true} {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			// Model an already-connected user's prior, cached location. The NONE
			// profile deliberately has no mapper for a new selection.
			c.grid, c.gridCell, c.gridCoarseCell = "IO91", 1, 2
			if profile == "COARSE_MISS" {
				c.gridCell, c.gridCoarseCell = pathreliability.EncodeCell("IO91"), pathreliability.EncodeCoarseCell("IO91")
			}
			if nearby {
				c.filter = passRuleTestFilter(t, "NEARBY")
			}
			if response, _ := s.handlePathSettingsCommand(c, "SET NOISE QUIET"); response != "Noise class set to QUIET\n" {
				t.Fatalf("prior record setup failed: %q", response)
			}
			assertGridRefusalUnchanged(t, s, c, "SET GRID FN31PRJUNK", "Invalid grid.")
			if profile == "NONE" {
				assertGridRefusalUnchanged(t, s, c, "SET GRID FN31", "Grid mapping is unavailable")
				if err := s.validateMachineSettings(filter.SettingsConfiguration{Grid: "FN31"}); err == nil || !strings.Contains(err.Error(), "mapping is unavailable") {
					t.Fatalf("machine valid syntax received no availability refusal: %v", err)
				}
			} else {
				response, _ := s.handlePathSettingsCommand(c, "SET GRID FN31")
				if response != "Grid set to FN31\n" || c.gridCell == pathreliability.InvalidCell || c.gridCoarseCell != pathreliability.InvalidCell {
					t.Fatal("NEARBY changed ordinary fine-only GRID admission")
				}
			}
			if err := s.validateMachineSettings(filter.SettingsConfiguration{Grid: "FN31PRJUNK"}); err == nil || strings.Contains(err.Error(), "unavailable") {
				t.Fatalf("machine malformed syntax concealed by mapping refusal: %v", err)
			}
			if err := s.validateMachineSettings(filter.SettingsConfiguration{}); err != nil {
				t.Fatalf("empty GRID lookup sentinel changed: %v", err)
			}
		}
		if profile == "NONE" {
			t.Run("rule-recovery-explicit-unmapped-grid", func(t *testing.T) {
				assertV6UnavailableRuleRecovery(t, "FN31")
			})
			t.Run("recovery-preserves-other-setting-validation", func(t *testing.T) {
				s := presetTestServer(t)
				for _, settings := range []filter.SettingsConfiguration{
					{Grid: "FN31PRJUNK"},
					{Grid: "FN31", NoiseClass: "INVALID"},
					{Grid: "FN31", Dialect: "INVALID"},
					{Grid: "FN31", DedupePolicy: "INVALID"},
					{Grid: "FN31", PathMinObservationCount: -1},
					{Grid: "FN31", SolarSummaryMinutes: -1},
				} {
					if err := s.validateMachineSettingsWithGridRecovery(settings, true); err == nil {
						t.Fatalf("GRID recovery admitted invalid unrelated setting: %+v", settings)
					}
				}
			})
		}
		t.Log(marker + " " + profile)
		return
	}
	requireH3Mappings(t)
	directory := t.TempDir()
	coarse := pathreliability.EncodeCoarseCell("FN31")
	if coarse == pathreliability.InvalidCell || coarse == pathreliability.EncodeCoarseCell("IO91") {
		t.Fatal("original tables lack distinct user and proposed coarse cells")
	}
	for _, name := range []string{"res1.bin", "res2.bin"} {
		data, err := os.ReadFile(filepath.Join(testsupport.H3TableDir(t), name))
		if err != nil {
			t.Fatal(err)
		}
		if name == "res1.bin" {
			offset := (int(coarse) - 1) * 8
			clear(data[offset : offset+8])
		}
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"NONE", "COARSE_MISS"} {
		t.Run(profile, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, executable, "-test.run=^TestGridMappingAvailability$", "-test.v")
			child.Env = append(os.Environ(), profileEnv+"="+profile, directoryEnv+"="+directory)
			output, err := child.CombinedOutput()
			if err != nil || !bytes.Contains(output, []byte(marker+" "+profile)) {
				t.Fatalf("GRID mapping child was not qualified: %v\n%s", err, output)
			}
		})
	}
}

func FuzzHumanGridAdmission(f *testing.F) {
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(f)); err != nil {
		f.Fatal(err)
	}
	for _, seed := range []string{"FN31", "fn31pr", "FN31PRJUNK", "FN31P", "FN31 EXTRA", "SN31", "FN31YZ", "", "\x00FN31"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 128 {
			return
		}
		c := &Client{filter: testRuleFilter(t), grid: "IO91", configuredSettings: filter.SettingsConfiguration{Grid: "IO91"}}
		before := filter.V6ConfigurationFromFilter(c.filter, c.configuredSettings)
		fields := strings.Fields("SET GRID " + value)
		grid := ""
		if len(fields) == 3 {
			grid = strings.ToUpper(fields[2])
		}
		// Independent positional grammar: no call to the production locator
		// parser or shared syntax predicate derives the expected domain.
		valid := (len(grid) == 4 || len(grid) == 6) && grid[0] >= 'A' && grid[0] <= 'R' &&
			grid[1] >= 'A' && grid[1] <= 'R' && grid[2] >= '0' && grid[2] <= '9' && grid[3] >= '0' && grid[3] <= '9'
		if valid && len(grid) == 6 {
			valid = grid[4] >= 'A' && grid[4] <= 'X' && grid[5] >= 'A' && grid[5] <= 'X'
		}
		outcome := (&Server{}).stagePathSettingsCommand(c, "SET GRID "+value)
		if outcome.publish {
			if !valid || c.grid != grid || c.configuredSettings.Grid != grid || c.gridCell == pathreliability.InvalidCell {
				t.Fatalf("invalid or noncanonical GRID publication for %q", value)
			}
			if err := (&Server{}).validateMachineSettings(c.configuredSettings); err != nil {
				t.Fatalf("human publication is machine-invalid: %v", err)
			}
		} else {
			if !before.Equal(filter.V6ConfigurationFromFilter(c.filter, c.configuredSettings)) || c.grid != "IO91" || c.gridCell != 0 || c.gridCoarseCell != 0 {
				t.Fatal("refusal mutated candidate GRID state")
			}
			if valid && pathreliability.EncodeCell(grid) != pathreliability.InvalidCell {
				t.Fatalf("valid available GRID was refused: %q", value)
			}
		}
	})
}
