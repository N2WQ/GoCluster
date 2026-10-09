package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

func assertPersistenceWarning(t *testing.T, response string) {
	t.Helper()
	if strings.Count(response, "warning: failed to persist") != 1 ||
		!strings.Contains(response, "\nwarning: failed to persist;") ||
		!strings.Contains(response, "changes are active but were not saved") {
		t.Fatalf("missing or misleading persistence warning: %q", response)
	}
}

func TestHumanPersistenceWarningsKeepLiveChanges(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, fault := range []string{"save", "malformed", "future", "protected"} {
			for _, operation := range []string{"band", "comment-add", "comment-remove", "comment-reset", "reset", "nofilter", "dialect"} {
				t.Run(string(dialect)+"/"+fault+"/"+operation, func(t *testing.T) {
					s := presetTestServer(t)
					s.filterEngine = newFilterCommandEngine()
					c := configurationTestClient(s, "W1ABC-1")
					c.dialect = dialect
					if err := c.saveFilter(); err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
					if fault != "save" {
						body := "[broken"
						if fault != "malformed" {
							body = "configuration_version: 99\nnoise_class: URBAN\n"
						}
						if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
							t.Fatal(err)
						}
						if fault == "protected" {
							restored, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute))
							if err != nil || !c.recordProtected || !strings.Contains(restored.warning, "temporary defaults") {
								t.Fatalf("protected login: %+v, %v", restored, err)
							}
							c.dialect, c.configuredSettings.Dialect = dialect, string(dialect)
						}
						// Otherwise this is a valid session whose authoritative record
						// becomes unreadable after login, exercising real preparation.
					}
					c.filter.SetBand("40m", true)
					c.filter.Comments = []string{"OLD"}
					before, err := c.captureConfiguration(0)
					if err != nil {
						t.Fatal(err)
					}
					oldBytes := presetDiskBytes(t, path)
					attempts := 0
					if fault == "save" {
						s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
							attempts++
							return os.ErrPermission
						}
					}
					line := map[string]string{
						"band": "PASS BAND 20", "comment-add": "PASS COMMENT NEW",
						"comment-remove": "REMOVE PASS COMMENT OLD", "comment-reset": "RESET FILTER COMMENT",
						"reset": "RESET FILTER", "nofilter": "PASS NOFILTER", "dialect": "DIALECT cc",
					}[operation]
					if dialect == DialectCC {
						switch operation {
						case "band":
							line = "SET/FILTER BAND 20"
						case "nofilter":
							line = "SET/NOFILTER"
						case "dialect":
							line = "DIALECT go"
						}
					}
					response, handled := s.filterEngine.Handle(c, line)
					if operation == "dialect" {
						response, handled = s.handleDialectCommand(c, line)
					}
					if !handled {
						t.Fatalf("command not handled: %s", line)
					}
					assertPersistenceWarning(t, response)
					after, err := c.captureConfiguration(0)
					if err != nil || before.Equal(after) {
						t.Fatalf("live command not applied: %v", err)
					}
					valid := false
					switch operation {
					case "band":
						valid = c.filter.Bands["20m"] && !c.filter.AllBands
					case "comment-add":
						valid = strings.Join(c.filter.Comments, ",") == "OLD,NEW"
					case "comment-remove", "comment-reset":
						valid = len(c.filter.Comments) == 0
					case "reset", "nofilter":
						valid = c.filter.AllBands && len(c.filter.Comments) == 0
					case "dialect":
						valid = c.dialect != dialect && after.Settings.Dialect == string(c.dialect)
					}
					if !valid || !bytes.Equal(oldBytes, presetDiskBytes(t, path)) || (fault == "save" && attempts != 1) {
						t.Fatalf("live/durable result: valid=%v attempts=%d", valid, attempts)
					}
					if fault == "protected" {
						c.interrupt()
						s.unregisterClient(c)
						if !bytes.Equal(oldBytes, presetDiskBytes(t, path)) {
							t.Fatal("teardown overwrote protected record")
						}
					}
				})
			}
		}
	}
}

func TestHumanPersistenceWarningPreservesSaveAttemptRules(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			s.filterEngine = newFilterCommandEngine()
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			c.filter.SetBand("20m", true)
			c.filter.Comments = []string{"POTA"}
			if err := c.saveFilter(); err != nil {
				t.Fatal(err)
			}
			attempts := 0
			s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
				attempts++
				return os.ErrPermission
			}
			for _, line := range []string{"DIALECT " + string(dialect), "PASS COMMENT pota", "REMOVE PASS COMMENT absent", "RESET FILTER COMMENT REJECT", "PASS BAND NOTABAND"} {
				response, handled := s.handleDialectCommand(c, line)
				if !handled {
					response, handled = s.filterEngine.Handle(c, line)
				}
				if !handled || attempts != 0 || strings.Contains(response, "warning: failed to persist") {
					t.Fatalf("no-op/invalid command attempted save: %s => %q (%d)", line, response, attempts)
				}
			}
			response, _ := s.filterEngine.Handle(c, "PASS BAND 20")
			assertPersistenceWarning(t, response)
			if attempts != 1 {
				t.Fatal("ordinary repeated filter command stopped attempting save")
			}
		})
	}
}
