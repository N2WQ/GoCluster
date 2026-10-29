package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/filter"
	"dxcluster/spot"
)

// Taxonomy is an immutable process-wide startup snapshot. These composed tests
// stay sequential and restore it; they establish command behavior, not inference.
func loadHistoryCommentAliasTaxonomy(t *testing.T, profile string) {
	t.Helper()
	aliases := "BAND, MODE, NEXT"
	if profile == "supported" {
		aliases += ", COMMENT"
	}
	definition := "modes:\n  - name: CW\n    filter_visible: true\n    default_filter_allowed: true\n    variants: [" + aliases + "]\n" +
		"  - name: FT8\n    filter_visible: true\n    default_filter_allowed: true\n" +
		"  - name: RTTY\n    filter_visible: true\n" +
		"  - name: UNKNOWN\n    filter_visible: true\nevents: []\n"
	if profile == "invisible" {
		definition = strings.Replace(definition, "events: []\n", "  - name: HIDDEN\n    filter_visible: false\n    variants: [COMMENT]\nevents: []\n", 1)
	}
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
	wantCanonical := map[string]string{"supported": "CW", "absent": "COMMENT", "invisible": "HIDDEN"}[profile]
	if spot.CanonicalModeForFilter("COMMENT") != wantCanonical || filter.IsSupportedMode("COMMENT") != (profile == "supported") {
		t.Fatalf("loaded %s taxonomy has the wrong alias/visibility contract", profile)
	}
}

func historyCommentAliasSession(t *testing.T, input string, ziutek bool, dialect DialectName, read historyReaderFunc) (*Server, *Client, *taxonomyInputConn, *int) {
	t.Helper()
	s := presetTestServer(t)
	s.useZiutek = ziutek
	s.nowFn = func() time.Time { return time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC) }
	s.processor = commands.NewProcessor(nil, read, nil, nil, nil, nil)
	c, conn := taxonomyInputReader(t, input, ziutek)
	c.server, c.callsign, c.address, c.dialect = s, "W1ABC-16", "192.0.2.8:8000", dialect
	c.noiseClass = "QUIET"
	c.setDedupePolicy(dedupePolicyFast)
	c.filter = unrestrictedTestFilter()
	c.controlChan = make(chan controlMessage, 8)
	s.clients = map[string]*Client{c.callsign: c}
	baseline, err := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{Dialect: string(dialect), NoiseClass: "QUIET", DedupePolicy: "FAST"}).Preset()
	if err != nil {
		t.Fatal(err)
	}
	c.presetReference = &filter.PresetReference{Name: "BASE", Baseline: baseline}
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.configurationRevisionToken(); err != nil {
		t.Fatal(err)
	}
	commits := new(int)
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error {
		(*commits)++
		return errors.New("unexpected history persistence")
	}
	return s, c, conn, commits
}

type historyCommentAliasState struct {
	filter                             *filter.Filter
	configuration, baseline            filter.V6Configuration
	presetName, epoch                  string
	revision                           uint64
	configurationDigest, historyDigest [32]byte
	configurationSet, historySet       bool
	disk                               []byte
	search                             *historySearch
	query, token                       string
	position                           []byte
	unreadable, hadMatches, closed     bool
	generation                         uint64
}

func captureHistoryCommentAliasState(t *testing.T, c *Client) historyCommentAliasState {
	t.Helper()
	disk, err := os.ReadFile(filepath.Join(filter.UserDataDir, c.callsign+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	state := historyCommentAliasState{
		filter: c.filter, configuration: c.captureV6Configuration(),
		baseline: testPresetConfiguration(t, c.presetReference.Baseline), presetName: c.presetReference.Name,
		epoch: c.configurationEpoch, revision: c.configurationRevision, configurationDigest: c.configurationDigest,
		configurationSet: c.configurationDigestSet, historyDigest: c.historyDigest, historySet: c.historyDigestSet,
		disk: disk, search: c.historySearch, generation: c.historyGeneration, closed: c.historyClosed,
	}
	if c.historySearch != nil {
		// Format the opaque query contents into owned bytes. A shallow copy of
		// its private slices would conceal an accidental in-place mutation.
		state.query = fmt.Sprintf("%#v", c.historySearch.query)
		state.token = c.historySearch.token
		state.position = bytes.Clone(c.historySearch.before)
		state.unreadable, state.hadMatches = c.historySearch.unreadable, c.historySearch.hadMatches
	}
	return state
}

func (before historyCommentAliasState) assertOwners(t *testing.T, c *Client, commits int) {
	t.Helper()
	after := captureHistoryCommentAliasState(t, c)
	if commits != 0 || before.filter != after.filter || !before.configuration.Equal(after.configuration) ||
		before.presetName != after.presetName || !before.baseline.Equal(after.baseline) || before.epoch != after.epoch ||
		before.revision != after.revision || before.configurationDigest != after.configurationDigest || before.configurationSet != after.configurationSet ||
		before.historyDigest != after.historyDigest || before.historySet != after.historySet || !bytes.Equal(before.disk, after.disk) {
		t.Fatal("history input changed filter/configuration/revision/preset/disk or attempted persistence")
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	saved := filter.V6ConfigurationFromFilter(&record.Filter, filter.SettingsConfiguration{
		Dialect: record.Dialect, Grid: record.Grid, NoiseClass: record.NoiseClass, DedupePolicy: record.DedupePolicy,
		PathMinObservationCount: record.PathMinObservationCount, SolarSummaryMinutes: record.SolarSummaryMinutes,
	})
	if !before.configuration.Equal(saved) || record.Preset == nil || record.Preset.Name != before.presetName ||
		!before.baseline.Equal(testPresetConfiguration(t, record.Preset.Baseline)) || len(c.controlChan) != 0 {
		t.Fatal("immediate saved record differs or history left an unexpected queued response")
	}
	select {
	case <-c.done:
		t.Fatal("recoverable history input closed the connection")
	default:
	}
}

func (before historyCommentAliasState) assertCursor(t *testing.T, c *Client) {
	t.Helper()
	after := captureHistoryCommentAliasState(t, c)
	if before.search != after.search || before.query != after.query || before.token != after.token || !bytes.Equal(before.position, after.position) ||
		before.unreadable != after.unreadable || before.hadMatches != after.hadMatches || before.generation != after.generation || before.closed != after.closed {
		t.Fatal("refused history input changed cursor/query/token/position/flags/generation")
	}
}

func TestHistoryCommentAliasReaderArchiveSelection(t *testing.T) {
	for _, profile := range []string{"supported", "absent", "invisible"} {
		t.Run(profile, func(t *testing.T) {
			loadHistoryCommentAliasTaxonomy(t, profile)
			cases := []struct {
				input, line, phrase string
				ft8                 bool
			}{
				{"show dx 1 mode comment\n", "SHOW DX 1 MODE COMMENT", "", false},
				{"sh mydx 1 mode ft8, comment\n", "SH MYDX 1 MODE FT8, COMMENT", "", true},
				{"show dx 1 mode ,cw,,comment,\n", "SHOW DX 1 MODE ,CW,,COMMENT,", "", false},
				{"show dx 1 mode comment comment POTA:  up 5!\n", "SHOW DX 1 MODE COMMENT COMMENT POTA:  UP 5!", "POTA:  UP 5!", false},
				{"show dx 1 mode cw,comment comment COMMENT  band mode:!\n", "SHOW DX 1 MODE CW,COMMENT COMMENT COMMENT  BAND MODE:!", "COMMENT  BAND MODE:!", false},
				{"show dx 1 mode band, mode, comment comment :!  *?,x\n", "SHOW DX 1 MODE BAND, MODE, COMMENT COMMENT :!  *?,X", ":!  *?,X", false},
				{"show dx 1 mode comment commentx\b a:b!\n", "SHOW DX 1 MODE COMMENT COMMENT A:B!", "A:B!", false},
				{"show dx 1 mode comment commentx\x7f a:b!\n", "SHOW DX 1 MODE COMMENT COMMENT A:B!", "A:B!", false},
				{"show dx 1 mode comment wrong\x17comment a:b!\n", "SHOW DX 1 MODE COMMENT COMMENT A:B!", "A:B!", false},
				{"pass mode comment\x15show dx 1 mode comment comment a:b!\n", "SHOW DX 1 MODE COMMENT COMMENT A:B!", "A:B!", false},
			}
			if profile != "supported" {
				cases = cases[:0]
				cases = append(cases, struct {
					input, line, phrase string
					ft8                 bool
				}{"show dx 1 mode cw, , comment POTA:  up 5!\n", "SHOW DX 1 MODE CW, , COMMENT POTA:  UP 5!", "POTA:  UP 5!", false})
			}
			for _, ziutek := range []bool{false, true} {
				for _, dialect := range []DialectName{DialectGo, DialectCC} {
					for i, tc := range cases {
						t.Run(fmt.Sprintf("ziutek=%t/%s/vector=%d", ziutek, dialect, i), func(t *testing.T) {
							qualifyHistoryCommentAliasSelection(t, tc.input, tc.line, tc.phrase, ziutek, tc.ft8, dialect)
						})
					}
				}
			}
		})
	}
}

func qualifyHistoryCommentAliasSelection(t *testing.T, input, expectedLine, phrase string, ziutek, ft8 bool, dialect DialectName) {
	t.Helper()
	makeSpot := func(call, mode, comment string) *spot.Spot {
		sp := spot.NewSpot(call, "W2ABC", 14030, mode)
		sp.Comment = comment
		return sp
	}
	wrongSpacing := strings.ReplaceAll(phrase, "  ", " ")
	if phrase != "" && wrongSpacing == phrase {
		wrongSpacing = "UNRELATED"
	}
	rows := []*spot.Spot{makeSpot("K0WRONG", "RTTY", phrase), makeSpot("K1SPACE", "CW", wrongSpacing), makeSpot("K2FT8", "FT8", phrase), makeSpot("K3RIGHT", "CW", phrase)}
	want := []bool{false, phrase == "", ft8, true}
	reads := 0
	var selected *spot.Spot
	read := historyReaderFunc(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		reads++
		if req.Limit != 1 || len(req.Before) != 0 || req.Match == nil {
			t.Fatal("fresh history request lost count-one or predicate ownership")
		}
		for i, row := range rows {
			if got := req.Match(row); got != want[i] {
				t.Fatalf("literal mode/phrase oracle[%d]=%t want=%t", i, got, want[i])
			}
			if want[i] && selected == nil {
				selected = row
			}
		}
		return archive.HistoryPage{Spots: []*spot.Spot{selected}, End: archive.HistoryExhausted}, nil
	})
	s, c, _, commits := historyCommentAliasSession(t, input, ziutek, dialect, read)
	before := captureHistoryCommentAliasState(t, c)
	line, err := c.readCommandLine(128)
	if err != nil || line != expectedLine {
		t.Fatalf("actual reader boundary/editing changed: %q %v want=%q", line, err, expectedLine)
	}
	response := runHistory(t, s, c, line)
	if reads != 1 || selected == nil || !strings.Contains(response, selected.FormatDXCluster()) || strings.Count(response, "DX de") != 1 || c.historySearch != nil {
		t.Fatalf("selected count-one archive response lost: %q reads=%d", response, reads)
	}
	before.assertOwners(t, c, *commits)
}

func TestHistoryCommentAliasRefusalsPreserveContinuation(t *testing.T) {
	for _, profile := range []string{"supported", "absent", "invisible"} {
		t.Run(profile, func(t *testing.T) {
			loadHistoryCommentAliasTaxonomy(t, profile)
			for _, ziutek := range []bool{false, true} {
				for _, dialect := range []DialectName{DialectGo, DialectCC} {
					t.Run(fmt.Sprintf("ziutek=%t/%s", ziutek, dialect), func(t *testing.T) {
						qualifyHistoryCommentAliasContinuation(t, profile, ziutek, dialect)
					})
				}
			}
		})
	}
}

func qualifyHistoryCommentAliasContinuation(t *testing.T, profile string, ziutek bool, dialect DialectName) {
	t.Helper()
	prefix := "SHOW DX"
	if dialect == DialectCC {
		prefix = "SH/DX"
	}
	initial := prefix + " 1 MODE CW, COMMENT POTA:  UP 5!"
	if profile == "supported" {
		initial = prefix + " 1 MODE COMMENT COMMENT POTA:  UP 5!"
	}
	readerRefusals := []string{"NEXT COMMENT :!", "MODE NEXT COMMENT :!"}
	if profile == "supported" {
		readerRefusals = append(readerRefusals, "MODE COMMENT up:5! REJECT RULE 9 BAND=20M", "MODE CW, COMMENT up:5!",
			"MODE COMMENT COMMENT \x17:!", "MODE COMMENT COMMENT \b\b :!",
			"MODE COMMENT COMMENT :\x15"+prefix+" MODE COMMENT :!")
	}
	parserRefusals := []struct{ args, response string }{
		{"MODE CW MODE FT8", "Invalid MODE selection: category may appear only once.\n"},
		{"NEXT BAD COMMENT POTA", "Invalid history continuation. Start a fresh SHOW DX search.\n"},
	}
	if profile == "supported" {
		parserRefusals = append(parserRefusals, struct{ args, response string }{"MODE COMMENT FT8", "Invalid MODE selection: separate values with commas.\n"},
			struct{ args, response string }{"MODE CW,COMMENT FT8", "Invalid MODE selection: separate values with commas.\n"},
			struct{ args, response string }{"MODE COMMENT COMMENT", "Invalid COMMENT phrase. Use 1-64 printable ASCII bytes.\n"},
			struct{ args, response string }{"MODE COMMENT COMMENT " + strings.Repeat("A", 65), "Invalid COMMENT phrase. Use 1-64 printable ASCII bytes.\n"})
	} else {
		parserRefusals = append(parserRefusals, struct{ args, response string }{"MODE CW,COMMENT", "Invalid MODE selection: unsupported value \"COMMENT\".\n"})
	}
	var wire strings.Builder
	wire.WriteString(strings.ToLower(initial) + "\n")
	for _, args := range readerRefusals {
		wire.WriteString(strings.ToLower(prefix+" "+args) + "\nSHOW OWN\n")
	}
	for _, refusal := range parserRefusals {
		wire.WriteString(strings.ToLower(prefix+" "+refusal.args) + "\n")
	}
	first := spot.NewSpot("K1ABC", "W2ABC", 14030, "CW")
	second := spot.NewSpot("K1ABC", "W2ABC", 14031, "CW")
	wrongMode := spot.NewSpot("K1ABC", "W2ABC", 14074, "FT8")
	wrongSpaces := spot.NewSpot("K1ABC", "W2ABC", 14032, "CW")
	first.Comment, second.Comment, wrongMode.Comment, wrongSpaces.Comment = "POTA:  UP 5!", "POTA:  UP 5!", "POTA:  UP 5!", "POTA: UP 5!"
	position := []byte("literal-comment-alias-position")
	reads := 0
	read := historyReaderFunc(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		reads++
		if req.Limit != 1 || req.Match == nil || !req.Match(first) || !req.Match(second) || req.Match(wrongMode) || req.Match(wrongSpaces) {
			t.Fatal("query lost canonical CW, literal spaces/punctuation or count-one selection")
		}
		if reads == 1 {
			if len(req.Before) != 0 {
				t.Fatal("fresh query borrowed an earlier archive position")
			}
			return archive.HistoryPage{Spots: []*spot.Spot{first}, Before: bytes.Clone(position), Unreadable: 2, End: archive.HistoryCountReached}, nil
		}
		if reads != 2 || !bytes.Equal(req.Before, position) {
			t.Fatalf("refusal scanned archive or NEXT changed position: reads=%d before=%q", reads, req.Before)
		}
		return archive.HistoryPage{Spots: []*spot.Spot{second}, End: archive.HistoryExhausted}, nil
	})
	s, c, conn, commits := historyCommentAliasSession(t, wire.String(), ziutek, dialect, read)
	owners := captureHistoryCommentAliasState(t, c)
	line, err := c.readCommandLine(128)
	if err != nil || line != initial {
		t.Fatalf("initial alias/clause ingress failed: %q %v", line, err)
	}
	response := runHistory(t, s, c, line)
	if reads != 1 || c.historySearch == nil || !c.historySearch.unreadable || !c.historySearch.hadMatches || !strings.Contains(response, first.FormatDXCluster()) {
		t.Fatalf("accepted count-one search did not create complete continuation: %q", response)
	}
	owners.assertOwners(t, c, *commits)
	cursor := captureHistoryCommentAliasState(t, c)
	for _, args := range readerRefusals {
		line, err := c.readCommandLine(128)
		var validation *InputValidationError
		var terminal *humanInputError
		if line != "" || !errors.As(err, &validation) || validation.kind != inputErrorInvalidChar || errors.As(err, &terminal) || conn.closed.Load() {
			t.Fatalf("punctuation after alias/edited marker/NEXT was not safely refused: %q -> %q %v", args, line, err)
		}
		if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" {
			t.Fatalf("rejected physical tail escaped or consumed the next line: %q %v", next, err)
		}
		cursor.assertCursor(t, c)
		owners.assertOwners(t, c, *commits)
		if reads != 1 {
			t.Fatal("reader refusal scanned the archive")
		}
	}
	for _, refusal := range parserRefusals {
		line, err := c.readCommandLine(128)
		if err != nil {
			t.Fatal(err)
		}
		if response := runHistory(t, s, c, line); response != refusal.response {
			t.Fatalf("parser refusal changed: %q -> %q want=%q", line, response, refusal.response)
		}
		cursor.assertCursor(t, c)
		owners.assertOwners(t, c, *commits)
		if reads != 1 {
			t.Fatal("parser refusal scanned the archive")
		}
	}
	// The known stream has been fully consumed. Append NEXT through the same
	// codec/reader ownership, retaining its state and the actual generated token.
	if c.reader.Buffered() != 0 {
		t.Fatal("fixture still has undispatched input before NEXT")
	}
	conn.input = strings.NewReader(prefix + " NEXT " + cursor.token + "\n")
	line, err = c.readCommandLine(128)
	if err != nil || line != prefix+" NEXT "+cursor.token {
		t.Fatalf("continued reader failed: %q %v", line, err)
	}
	response = runHistory(t, s, c, line)
	if reads != 2 || c.historySearch != nil || c.historyGeneration != cursor.generation || !strings.Contains(response, second.FormatDXCluster()) || !strings.Contains(response, "Warning: unreadable archive records") {
		t.Fatalf("NEXT lost original selections/flags or failed to retire cursor: %q reads=%d", response, reads)
	}
	owners.assertOwners(t, c, *commits)
}
