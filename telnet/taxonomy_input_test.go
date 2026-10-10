package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

// These sequential tests compose real reader, publication, storage and matcher
// evidence. The private loaded taxonomy is not a production model-quality oracle.
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
			t.Run(fmt.Sprintf("ziutek=%t/%q", ziutek, tc.input), func(t *testing.T) {
				c, conn := taxonomyInputReader(t, tc.input, ziutek)
				c.echoInput = true
				line, err := c.readCommandLine(128)
				if err != nil || line != tc.want || conn.echo.String() != tc.echo || conn.closed.Load() {
					t.Fatalf("line=%q echo=%q err=%v; want line=%q echo=%q", line, conn.echo.String(), err, tc.want, tc.echo)
				}
			})
		}
	}
}

func TestHumanTaxonomyReaderLiteralByteBoundary(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, size := range []int{128, 129} {
			t.Run(fmt.Sprintf("ziutek=%t/bytes=%d", ziutek, size), func(t *testing.T) {
				input := "pass mode " + strings.Repeat("_", size-10)
				tail := "\nSHOW OWN\n"
				if size == 129 {
					tail = " REJECT BAND ALL\nSHOW OWN\n"
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
				} else if line != "" || !errors.As(err, &validation) || validation.kind != inputErrorTooLong || errors.As(err, &terminal) || !strings.Contains(validation.allowed, "'_'") {
					t.Fatalf("129-byte input did not recover as a whole human line: %q %v", line, err)
				}
				if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" || conn.closed.Load() {
					t.Fatalf("physical tail escaped or next line lost: %q %v", next, err)
				}
			})
		}
	}
}

func TestTaxonomyUnderscoreRemainsForbiddenAtLogin(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		c, conn := taxonomyInputReader(t, "w1abc_1 REJECT BAND ALL\nw1abc-1\n", ziutek)
		line, err := c.ReadLine(16, "login", false, false, false, false)
		var validation *InputValidationError
		var terminal *humanInputError
		if line != "" || !errors.As(err, &validation) || validation.kind != inputErrorInvalidChar || strings.Contains(validation.allowed, "'_'") || errors.As(err, &terminal) {
			t.Fatalf("ziutek=%t login accepted underscore or lost recoverable refusal: %q %v", ziutek, line, err)
		}
		if next, err := c.ReadLine(16, "login", false, false, false, false); err != nil || next != "W1ABC-1" || conn.closed.Load() {
			t.Fatalf("ziutek=%t next physical login lost: %q %v", ziutek, next, err)
		}
	}
}

func TestTaxonomyUnderscoreRetainsTerminalMachineIntent(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, tc := range []struct{ input, verb string }{
			{"get yaml config id test_mode\n", "GET"},
			{"PUT_ YAML CONFIG\n", "PUT"},
			{"patch_\n", "PATCH"},
			{"validate_\n", "VALIDATE"},
			{"putx\b_ YAML CONFIG\n", "PUT"},
			{"put \x7f_ YAML CONFIG\n", "PUT"},
			{"junk\x15get yaml config id test_mode\n", "GET"},
		} {
			c, _ := taxonomyInputReader(t, tc.input+"REJECT BAND ALL\n", ziutek)
			line, err := c.readCommandLine(128)
			var machine *machineInputError
			var validation *InputValidationError
			if line != "" || !errors.As(err, &machine) || !machine.Terminal || machine.Verb != tc.verb || !errors.As(err, &validation) || validation.kind != inputErrorInvalidChar {
				t.Fatalf("ziutek=%t input=%q lost terminal %s intent: %q %v", ziutek, tc.input, tc.verb, line, err)
			}
		}
	}
	for _, id := range []string{"TEST_MODE", "_TEST", "TEST_"} {
		cmd, handled, err := parseMachineHeader("GET YAML CONFIG ID " + id)
		if !handled || err == nil || cmd.Verb != "GET" {
			t.Fatalf("direct header accepted restricted ID %q: %+v %v", id, cmd, err)
		}
	}
	cmd, handled, err := parseMachineHeader("GET YAML CONFIG ID test-Mode9")
	if err != nil || !handled || cmd.RequestID != "test-Mode9" {
		t.Fatalf("valid machine ID changed: %+v %v", cmd, err)
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
	if !spot.IsSupportedFilterMode("TEST_MODE") || spot.CanonicalModeForFilter("CW_ALIAS") != "CW" || spot.EventMaskForName("TEST_EVENT") == 0 {
		t.Fatal("private loaded taxonomy lacks the required capabilities")
	}
}

type taxonomyCommandStep struct {
	verb, value, response string
	allow, block          map[string]bool
	all, blockAll         bool
	pass                  [3]bool
}

func TestHumanTaxonomyReaderPublicationStorageAndMatcher(t *testing.T) {
	loadHumanInputTaxonomy(t) // Global immutable snapshot: no parallel subtests.
	for _, ziutek := range []bool{false, true} {
		for _, dialect := range []DialectName{DialectGo, DialectCC} {
			for _, domain := range []string{"MODE", "EVENT"} {
				t.Run(fmt.Sprintf("ziutek=%t/%s/%s", ziutek, dialect, domain), func(t *testing.T) {
					qualifyHumanTaxonomyCommands(t, ziutek, dialect, domain)
				})
			}
		}
	}
}

func qualifyHumanTaxonomyCommands(t *testing.T, ziutek bool, dialect DialectName, domain string) {
	t.Helper()
	steps := []taxonomyCommandStep{
		{"REJECT", "ALL", "All modes blocked\n", map[string]bool{}, map[string]bool{}, false, true, [3]bool{}},
		{"PASS", "TEST_MODE", "Modes enabled: TEST_MODE\n", map[string]bool{"TEST_MODE": true}, map[string]bool{}, false, false, [3]bool{true, false, false}},
		{"PASS", "CW_ALIAS", "Modes enabled: CW_ALIAS\n", map[string]bool{"TEST_MODE": true, "CW": true}, map[string]bool{}, false, false, [3]bool{true, true, false}},
		{"REJECT", "CW_ALIAS", "Modes rejected: CW_ALIAS\n", map[string]bool{"TEST_MODE": true}, map[string]bool{"CW": true}, false, false, [3]bool{true, false, false}},
		{"PASS", "CW_ALIAS", "Modes enabled: CW_ALIAS\n", map[string]bool{"TEST_MODE": true, "CW": true}, map[string]bool{}, false, false, [3]bool{true, true, false}},
	}
	unknown, known := "UNKNOWN_MODE", "TEST_MODE"
	if domain == "EVENT" {
		steps = []taxonomyCommandStep{
			{"REJECT", "ALL", "All event-tagged spots rejected\n", map[string]bool{}, map[string]bool{}, false, true, [3]bool{false, false, true}},
			{"PASS", "TEST_EVENT", "Events enabled: TEST_EVENT\n", map[string]bool{"TEST_EVENT": true}, map[string]bool{}, false, false, [3]bool{true, false, true}},
			{"REJECT", "TEST_EVENT", "Events rejected: TEST_EVENT\n", map[string]bool{}, map[string]bool{"TEST_EVENT": true}, true, false, [3]bool{false, true, true}},
			{"PASS", "TEST_EVENT", "Events enabled: TEST_EVENT\n", map[string]bool{"TEST_EVENT": true}, map[string]bool{}, false, false, [3]bool{true, false, true}},
		}
		unknown, known = "UNKNOWN_EVENT", "TEST_EVENT"
	}
	command := func(verb, value string) string {
		if dialect == DialectCC {
			if verb == "PASS" {
				verb = "SET/FILTER"
			} else {
				verb = "UNSET/FILTER"
			}
		}
		return verb + " " + domain + " " + value
	}
	var input strings.Builder
	for _, step := range steps {
		input.WriteString(strings.ToLower(command(step.verb, step.value)) + "\n")
	}
	for _, verb := range []string{"PASS", "REJECT"} {
		for _, value := range []string{unknown, known + "," + unknown, unknown + " " + known} {
			input.WriteString(strings.ToLower(command(verb, value)) + "\n")
		}
	}
	s := presetTestServer(t)
	s.useZiutek = ziutek
	c, _ := taxonomyInputReader(t, input.String(), ziutek)
	c.server, c.callsign, c.address, c.dialect = s, "W1ABC-15", "192.0.2.8:8000", dialect
	c.noiseClass = "QUIET"
	c.setDedupePolicy(dedupePolicyFast)
	c.filter = filter.NewFilter()
	c.filter.Reset()
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	baseline, err := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Preset()
	if err != nil {
		t.Fatal(err)
	}
	c.presetReference = &filter.PresetReference{Name: "BASE", Baseline: baseline}
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	ref, err := c.presetReference.Clone()
	if err != nil {
		t.Fatal(err)
	}
	stableFilter := c.filter
	commits := 0
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
		commits++
		return prepared.Commit()
	}
	e := newFilterCommandEngine()
	for _, step := range steps {
		line, err := c.readCommandLine(128)
		if err != nil || line != command(step.verb, step.value) {
			t.Fatalf("reader lost command: %q %v", line, err)
		}
		priorCommits := commits
		response, handled := e.Handle(c, line)
		if !handled || !strings.HasPrefix(response, step.response) || strings.Contains(response, "warning") || commits != priorCommits+1 || c.filter != stableFilter || !reflect.DeepEqual(c.presetReference, ref) {
			t.Fatalf("accepted command did not publish/save exactly once: %q -> %q commits=%d", line, response, commits-priorCommits)
		}
		assertHumanTaxonomyOwners(t, s, c, domain, step)
	}
	for range 6 {
		assertHumanTaxonomyRefusal(t, c, e, domain, unknown, &commits)
	}
}

func assertHumanTaxonomyOwners(t *testing.T, s *Server, c *Client, domain string, step taxonomyCommandStep) {
	t.Helper()
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	text, err := s.renderYAMLReadbackVersion(c, "CONFIG", "taxonomy-15", "test", 4)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Configuration filter.Configuration `yaml:"configuration"`
	}
	if err := yaml.Unmarshal([]byte(text), &envelope); err != nil {
		t.Fatal(err)
	}
	live := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
	saved := filter.ConfigurationFromFilter(&record.Filter, filter.SettingsConfiguration{
		Dialect: record.Dialect, Grid: record.Grid, NoiseClass: record.NoiseClass,
		DedupePolicy: record.DedupePolicy, PathMinObservationCount: record.PathMinObservationCount,
		SolarSummaryMinutes: record.SolarSummaryMinutes,
	})
	if !live.Equal(saved) {
		t.Fatalf("immediate saved rules differ: live=%+v saved=%+v", live.Filters, saved.Filters)
	}
	if !live.Equal(envelope.Configuration) {
		t.Fatalf("readback differs: live=%+v readback=%+v", live, envelope.Configuration)
	}
	// Stored decoding normalizes empty collections and excludes runtime caches.
	// Compare every configured baseline value, plus the exact association name.
	if record.Dialect != string(c.dialect) || record.Preset == nil || record.Preset.Baseline == nil || record.Preset.Name != c.presetReference.Name ||
		!filter.ConfigurationFromPreset(record.Preset.Baseline).Equal(filter.ConfigurationFromPreset(c.presetReference.Baseline)) {
		t.Fatalf("saved ownership differs: dialect=%q/%q preset=%+v/%+v", record.Dialect, c.dialect, record.Preset, c.presetReference)
	}
	want := filter.StringRules{Allow: step.allow, Block: step.block, AllowAll: step.all, BlockAll: step.blockAll}
	rules := live.Filters.Modes
	if domain == "EVENT" {
		rules = live.Filters.Events
	}
	if !reflect.DeepEqual(rules, want) {
		t.Fatalf("canonical %s rules=%+v want=%+v", domain, rules, want)
	}
	for _, f := range []*filter.Filter{c.filter, &record.Filter} {
		for i, name := range []string{"TEST_MODE", "CW", "UNKNOWN"} {
			candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, name)
			if domain == "EVENT" {
				candidate = spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
				candidate.Events = []spot.EventMask{spot.EventMaskForName("TEST_EVENT"), spot.EventMaskForName("OTHER_EVENT"), 0}[i]
			}
			if got := f.Matches(candidate); got != step.pass[i] {
				t.Fatalf("%s actual matcher[%d]=%t want=%t", domain, i, got, step.pass[i])
			}
		}
	}
}

func assertHumanTaxonomyRefusal(t *testing.T, c *Client, e *filterCommandEngine, domain, unknown string, commits *int) {
	t.Helper()
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	ref, err := c.presetReference.Clone()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	disk, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	line, err := c.readCommandLine(128)
	if err != nil {
		t.Fatal(err)
	}
	priorCommits, pointer := *commits, c.filter
	response, handled := e.Handle(c, line)
	want := "Unknown mode: " + unknown + "\nSupported modes: CW, UNKNOWN, TEST_MODE\n"
	if domain == "EVENT" {
		want = "Unknown event: " + unknown + "\nSupported events: TEST_EVENT, OTHER_EVENT\n"
	}
	after, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	afterRevision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	if !handled || response != want || *commits != priorCommits || pointer != c.filter || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || !reflect.DeepEqual(c.presetReference, ref) || revision != afterRevision || !bytes.Equal(disk, after) {
		t.Fatalf("unsupported/mixed command had effects or wrong refusal: %q -> %q commits=%d", line, response, *commits-priorCommits)
	}
}

func FuzzHumanTaxonomyInput(f *testing.F) {
	for _, input := range []string{"", "TEST_MODE", "cw_alias", "TEST_EVENT", "UNKNOWN_MODE,CW_ALIAS", "a/b#@-.,*?_09", "!", "é", "\x80", "\x00", strings.Repeat("_", 118), strings.Repeat("_", 119)} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, token string) {
		// Exact-token oracle excludes Telnet framing and inline editing; those
		// mechanisms have literal curated controls above. Bound each fuzz case.
		if len(token) > 192 {
			return
		}
		for i := 0; i < len(token); i++ {
			switch token[i] {
			case 8, 127, 21, 23, 255, 13, 10:
				return
			}
		}
		input := "PASS MODE " + token
		valid := len(input) <= 128
		const asciiDomain = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 /#@-.,*?_"
		for i := 0; i < len(input); i++ {
			valid = valid && strings.IndexByte(asciiDomain, input[i]) >= 0
		}
		for _, ziutek := range []bool{false, true} {
			c, conn := taxonomyInputReader(t, input+"\nSHOW OWN\n", ziutek)
			line, err := c.readCommandLine(128)
			var validation *InputValidationError
			var terminal *humanInputError
			if valid {
				if err != nil || line != strings.ToUpper(input) {
					t.Fatalf("ziutek=%t valid ASCII token changed/refused: %q -> %q %v", ziutek, token, line, err)
				}
			} else if line != "" || !errors.As(err, &validation) || errors.As(err, &terminal) {
				t.Fatalf("ziutek=%t invalid/over-limit token was not recoverably refused: %q -> %q %v", ziutek, token, line, err)
			}
			if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" || conn.closed.Load() {
				t.Fatalf("ziutek=%t lost next physical line after %q: %q %v", ziutek, token, next, err)
			}
		}
	})
}
