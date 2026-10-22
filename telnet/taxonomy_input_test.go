package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

type taxonomyInputConn struct {
	yamlTestConn
	echo bytes.Buffer
}

func (c *taxonomyInputConn) Write(p []byte) (int, error) { return c.echo.Write(p) }

func taxonomyInputReader(t *testing.T, input string, ziutek bool) (*Client, *taxonomyInputConn) {
	t.Helper()
	conn := &taxonomyInputConn{yamlTestConn: yamlTestConn{input: strings.NewReader(input)}}
	wire := &inputWireConn{Conn: conn}
	s := &Server{useZiutek: ziutek}
	reader, writer, err := s.defaultWrapConn(wire)
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{conn: conn, inputWire: wire, server: s, writer: bufio.NewWriter(writer), done: make(chan struct{})}
	var exposed io.Reader = reader
	if ziutek {
		c.inputExposure = &singleByteInput{reader: reader}
		exposed = c.inputExposure
	}
	c.reader = bufio.NewReader(exposed)
	t.Cleanup(c.interrupt)
	return c, conn
}

func TestHumanTaxonomyReaderEditingAndEcho(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, tc := range []struct{ input, want, echo string }{
			{"pass mode test_mode\n", "PASS MODE TEST_MODE", "PASS MODE TEST_MODE\r\n"},
			{"pass mode cw_alias\n", "PASS MODE CW_ALIAS", "PASS MODE CW_ALIAS\r\n"},
			{"pass event test_event\n", "PASS EVENT TEST_EVENT", "PASS EVENT TEST_EVENT\r\n"},
			{"pass mode test_modx\be\n", "PASS MODE TEST_MODE", "PASS MODE TEST_MODX\b \bE\r\n"},
			{"pass mode test_modx\x7fe\n", "PASS MODE TEST_MODE", "PASS MODE TEST_MODX\b \bE\r\n"},
			{"pass mode bad_name\x17test_mode\n", "PASS MODE TEST_MODE", "PASS MODE BAD_NAME" + strings.Repeat("\b \b", 8) + "TEST_MODE\r\n"},
			{"put\x15pass event test_event\n", "PASS EVENT TEST_EVENT", "PUT\b \b\b \b\b \bPASS EVENT TEST_EVENT\r\n"},
		} {
			c, conn := taxonomyInputReader(t, tc.input, ziutek)
			c.echoInput = true
			line, err := c.readCommandLine(128)
			if err != nil || line != tc.want || conn.echo.String() != tc.echo || conn.closed.Load() {
				t.Fatalf("ziutek=%v line=%q echo=%q err=%v", ziutek, line, conn.echo.String(), err)
			}
		}
	}
}

func TestHumanTaxonomyReaderLiteralByteBoundary(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, size := range []int{128, 129} {
			input := "pass mode " + strings.Repeat("_", size-10)
			tail := "\nSHOW OWN\n"
			if size == 129 {
				tail = " REJECT RULE 9 BAND=20M\nSHOW OWN\n"
			}
			c, conn := taxonomyInputReader(t, input+tail, ziutek)
			c.echoInput = true
			line, err := c.readCommandLine(128)
			var validation *InputValidationError
			var terminal *humanInputError
			if size == 128 {
				if err != nil || line != strings.ToUpper(input) || conn.echo.String() != strings.ToUpper(input)+"\r\n" {
					t.Fatalf("exact 128-byte input refused/changed: %q %q %v", line, conn.echo.String(), err)
				}
			} else if line != "" || !errors.As(err, &validation) || validation.kind != inputErrorTooLong ||
				errors.As(err, &terminal) || !strings.Contains(validation.allowed, "'_'") {
				t.Fatalf("129-byte input did not recover as a whole human line: %q %v", line, err)
			}
			if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" || conn.closed.Load() {
				t.Fatalf("physical tail escaped or next line lost: %q %v", next, err)
			}
		}
	}
}

func TestTaxonomyUnderscoreRemainsForbiddenAtLogin(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		c, conn := taxonomyInputReader(t, "w1abc_1 REJECT RULE 9 BAND=20M\nw1abc-1\n", ziutek)
		line, err := c.ReadLine(16, "login", false, false, false, false)
		var validation *InputValidationError
		var terminal *humanInputError
		if line != "" || !errors.As(err, &validation) || validation.kind != inputErrorInvalidChar ||
			strings.Contains(validation.allowed, "'_'") || errors.As(err, &terminal) {
			t.Fatalf("login accepted underscore or lost recoverable refusal: %q %v", line, err)
		}
		if next, err := c.ReadLine(16, "login", false, false, false, false); err != nil || next != "W1ABC-1" || conn.closed.Load() {
			t.Fatalf("next physical login lost: %q %v", next, err)
		}
	}
}

func TestTaxonomyUnderscoreRetainsTerminalMachineIntent(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, tc := range []struct{ input, verb string }{
			{"get yaml config schema 5 id test_mode\n", "GET"},
			{"PUT_ YAML CONFIG\n", "PUT"}, {"patch_\n", "PATCH"}, {"validate_\n", "VALIDATE"},
			{"putx\b_ YAML CONFIG\n", "PUT"}, {"put \x7f_ YAML CONFIG\n", "PUT"},
			{"junk\x15get yaml config schema 5 id test_mode\n", "GET"},
		} {
			c, _ := taxonomyInputReader(t, tc.input+"REJECT RULE 9 BAND=20M\n", ziutek)
			line, err := c.readCommandLine(128)
			var machine *machineInputError
			var validation *InputValidationError
			if line != "" || !errors.As(err, &machine) || !machine.Terminal || machine.Verb != tc.verb ||
				!errors.As(err, &validation) || validation.kind != inputErrorInvalidChar {
				t.Fatalf("input=%q lost terminal %s intent: %q %v", tc.input, tc.verb, line, err)
			}
		}
	}
	for _, id := range []string{"TEST_MODE", "_TEST", "TEST_"} {
		command, handled, err := parseV5MachineHeader("GET YAML CONFIG SCHEMA 5 ID " + id)
		if !handled || err == nil || command.Verb != "GET" {
			t.Fatalf("direct header accepted restricted ID %q: %+v %v", id, command, err)
		}
	}
	command, handled, err := parseV5MachineHeader("GET YAML CONFIG SCHEMA 5 ID test-Mode9")
	if err != nil || !handled || command.RequestID != "test-Mode9" {
		t.Fatalf("valid machine ID changed: %+v %v", command, err)
	}
}

func loadHumanInputTaxonomy(t *testing.T) {
	t.Helper()
	const definition = "modes:\n" +
		"  - name: CW\n    filter_visible: true\n    default_filter_allowed: true\n    variants: [CW_ALIAS]\n" +
		"  - name: TEST_MODE\n    filter_visible: true\n    default_filter_allowed: true\n" +
		"  - name: UNKNOWN\n    filter_visible: true\n    default_filter_allowed: true\n" +
		"events:\n  - name: TEST_EVENT\n    filter_visible: true\n  - name: OTHER_EVENT\n    filter_visible: true\n"
	path := filepath.Join(t.TempDir(), "spot_taxonomy.yaml")
	if err := os.WriteFile(path, []byte(definition), 0600); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
}

func TestHumanTaxonomyReaderPublicationStorageAndMatcher(t *testing.T) {
	loadHumanInputTaxonomy(t)
	for _, ziutek := range []bool{false, true} {
		for _, dialect := range []DialectName{DialectGo, DialectCC} {
			for _, field := range []string{"MODE", "EVENT"} {
				t.Run(fmt.Sprintf("%s/%s/ziutek=%v", dialect, field, ziutek), func(t *testing.T) {
					s := presetTestServer(t)
					expression, excluded := "MODE=cw_alias,test_mode", "MODE=unknown_mode"
					if field == "EVENT" {
						expression, excluded = "EVENT=test_event", "EVENT=unknown_event"
					}
					c, _ := taxonomyInputReader(t, "pass rule 0 "+expression+"\npass rule 1 "+excluded+"\n", ziutek)
					c.server, c.callsign, c.dialect, c.filter = s, "W1ABC-15", dialect, testRuleFilter(t)
					c.noiseClass = "QUIET"
					c.setDedupePolicy(dedupePolicyFast)
					if response, _ := s.handlePresetCommand(c, "SAVE PRESET BASE"); strings.Contains(response, "failed") {
						t.Fatal(response)
					}
					pointer, baseline := c.filter, c.presetReference.Baseline
					writes := 0
					s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error { writes++; return prepared.Commit() }
					line, err := c.readCommandLine(128)
					if err != nil || line != "PASS RULE 0 "+expression {
						t.Fatalf("reader lost literal expression: %q %v", line, err)
					}
					response, handled := newFilterCommandEngine().Handle(c, line)
					if !handled || writes != 1 || c.filter != pointer || c.presetReference.Baseline != baseline ||
						strings.Contains(response, "No changes applied.") || strings.Contains(response, "warning") {
						t.Fatalf("taxonomy command did not publish/save once: %q", response)
					}
					record, err := filter.LoadV5UserRecord(c.callsign)
					if err != nil || !record.Configuration.Equal(c.captureV5Configuration()) ||
						record.Preset == nil || len(record.Preset.Baseline.Configuration.Filters.Rules) != 0 {
						t.Fatalf("saved rules or independent baseline changed: %+v %v", record, err)
					}
					reply := s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: "CONFIG", SchemaVersion: 5, RequestID: "taxonomy-15"})
					var document struct {
						Configuration filter.V5Configuration `yaml:"configuration"`
					}
					if err := yaml.Unmarshal([]byte(reply), &document); err != nil || !document.Configuration.Equal(record.Configuration) {
						t.Fatalf("actual readback lost loaded taxonomy: %v\n%s", err, reply)
					}
					for i, mode := range []string{"CW", "TEST_MODE", "UNKNOWN"} {
						candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, mode)
						want := i < 2
						if field == "EVENT" {
							candidate.Mode = "CW"
							candidate.Events = []spot.EventMask{spot.EventMaskForName("TEST_EVENT"), spot.EventMaskForName("OTHER_EVENT"), 0}[i]
							want = i == 0
						}
						if got := c.filter.Matches(candidate); got != want {
							t.Fatalf("loaded %s matching[%d]=%v want=%v", field, i, got, want)
						}
					}
					before, revision := c.captureV5Configuration(), c.configurationRevision
					path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
					disk := presetDiskBytes(t, path)
					line, err = c.readCommandLine(128)
					if err != nil {
						t.Fatal(err)
					}
					response, handled = newFilterCommandEngine().Handle(c, line)
					if !handled || !strings.Contains(response, "No changes applied.") || writes != 1 ||
						!before.Equal(c.captureV5Configuration()) || c.configurationRevision != revision ||
						!bytes.Equal(disk, presetDiskBytes(t, path)) || c.presetReference.Baseline != baseline {
						t.Fatalf("unknown taxonomy selection had effects: %q", response)
					}
				})
			}
		}
	}
}

func FuzzHumanTaxonomyInput(f *testing.F) {
	for _, input := range []string{"", "TEST_MODE", "cw_alias", "TEST_EVENT", "UNKNOWN_MODE,CW_ALIAS",
		"a/b#@-.,*?_09", "!", "é", "\x80", "\x00", strings.Repeat("_", 118), strings.Repeat("_", 119)} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, token string) {
		if len(token) > 192 {
			return
		}
		for _, value := range []byte(token) {
			switch value {
			case 8, 127, 21, 23, 255, 13, 10:
				return
			}
		}
		input := "PASS MODE " + token
		valid := len(input) <= 128
		const asciiDomain = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 /#@-.,*?_"
		for _, value := range []byte(input) {
			valid = valid && strings.IndexByte(asciiDomain, value) >= 0
		}
		for _, ziutek := range []bool{false, true} {
			c, conn := taxonomyInputReader(t, input+"\nSHOW OWN\n", ziutek)
			line, err := c.readCommandLine(128)
			var validation *InputValidationError
			var terminal *humanInputError
			if valid {
				if err != nil || line != strings.ToUpper(input) {
					t.Fatalf("valid ASCII token changed/refused: %q -> %q %v", token, line, err)
				}
			} else if line != "" || !errors.As(err, &validation) || errors.As(err, &terminal) {
				t.Fatalf("invalid/over-limit token was not recoverably refused: %q -> %q %v", token, line, err)
			}
			if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" || conn.closed.Load() {
				t.Fatalf("lost next physical line: %q %v", next, err)
			}
		}
	})
}
