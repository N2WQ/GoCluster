package commands

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/cty"
	"dxcluster/spot"
)

type fakeArchive struct {
	spots []*spot.Spot
	err   error
}

func (f *fakeArchive) Recent(limit int) ([]*spot.Spot, error) {
	if f == nil || f.err != nil {
		return nil, f.err
	}
	return takeRecent(f.spots, limit), nil
}

func (f *fakeArchive) RecentFiltered(limit int, match func(*spot.Spot) bool) ([]*spot.Spot, error) {
	if f == nil || f.err != nil {
		return nil, f.err
	}
	if match == nil {
		return takeRecent(f.spots, limit), nil
	}
	if limit <= 0 {
		return nil, nil
	}
	out := make([]*spot.Spot, 0, limit)
	for _, s := range f.spots {
		if s != nil && match(s) {
			out = append(out, s)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func takeRecent(spots []*spot.Spot, limit int) []*spot.Spot {
	if limit <= 0 {
		return nil
	}
	if len(spots) <= limit {
		return append([]*spot.Spot(nil), spots...)
	}
	return append([]*spot.Spot(nil), spots[:limit]...)
}

func TestDXCommandQueuesSpot(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)

	resp := p.ProcessCommandForClient("DX 7001 K8ZB Testing...1...2...3", "N2WQ", "203.0.113.5", nil, "classic")
	if !strings.Contains(resp, "Spot queued") {
		t.Fatalf("expected queue response, got %q", resp)
	}

	select {
	case s := <-input:
		if s.DXCall != "K8ZB" {
			t.Fatalf("DXCall mismatch: %s", s.DXCall)
		}
		if s.DECall != "N2WQ" {
			t.Fatalf("DECall mismatch: %s", s.DECall)
		}
		if math.Abs(s.Frequency-7001.0) > 0.0001 {
			t.Fatalf("Frequency mismatch: %.4f", s.Frequency)
		}
		if s.Comment != "Testing...1...2...3" {
			t.Fatalf("Comment mismatch: %q", s.Comment)
		}
		if s.SourceType != spot.SourceManual || !s.IsHuman {
			t.Fatalf("unexpected source flags: %s human=%t", s.SourceType, s.IsHuman)
		}
		if s.SpotterIP != "203.0.113.5" {
			t.Fatalf("SpotterIP mismatch: %q", s.SpotterIP)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for spot")
	}
}

func TestDXCommandValidation(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)

	resp := p.ProcessCommandForClient("DX 7001 K8ZB", "N2WQ", "", nil, "classic")
	if strings.Contains(resp, "Usage: DX") {
		t.Fatalf("expected optional comment to queue, got %q", resp)
	}
	select {
	case s := <-input:
		if s.Comment != "" {
			t.Fatalf("expected empty comment, got %q", s.Comment)
		}
	default:
		t.Fatal("expected spot to be queued without comment")
	}

	resp = p.ProcessCommandForClient("DX 7001 K8ZB Test", "", "", nil, "classic")
	if resp != noLoggedUserMsg {
		t.Fatalf("expected login error, got %q", resp)
	}
}

func TestDXCommandCTYValidation(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, ctyLookup, nil, nil)

	resp := p.ProcessCommandForClient("DX 7001 K8ZB Test", "N2WQ", "", nil, "classic")
	if strings.Contains(resp, "Unknown DX callsign") {
		t.Fatalf("expected known DX to queue, got %q", resp)
	}
	select {
	case <-input:
	default:
		t.Fatalf("expected spot queued for known CTY call")
	}

	resp = p.ProcessCommandForClient("DX 7001 K4ZZZ Test", "N2WQ", "", nil, "classic")
	if !strings.Contains(resp, "Unknown DX callsign") {
		t.Fatalf("expected CTY validation error, got %q", resp)
	}
}

func TestTestSpotterBaseCall(t *testing.T) {
	cases := []struct {
		call string
		base string
		ok   bool
	}{
		{"K1TEST", "K1TEST", true},
		{"K1TEST-1", "K1TEST", true},
		{"K1TEST-01", "K1TEST", true},
		{"K1TEST-#", "", false},
		{"K1TEST-1-2", "", false},
		{"W6/K1TEST", "", false},
	}
	for _, tc := range cases {
		base, ok := testSpotterBaseCall(tc.call)
		if ok != tc.ok || base != tc.base {
			t.Fatalf("testSpotterBaseCall(%q) = (%q, %t), want (%q, %t)", tc.call, base, ok, tc.base, tc.ok)
		}
	}
}

func TestDXCommandRejectsTestSpotWhenCTYUnavailable(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)

	resp := p.ProcessCommandForClient("DX 7001 K8ZB Test", "K1TEST", "", nil, "classic")
	if resp != testCallCTYUnavailableMsg {
		t.Fatalf("expected CTY unavailable message, got %q", resp)
	}
	select {
	case <-input:
		t.Fatal("expected test spot to be rejected when CTY is unavailable")
	default:
	}
}

func TestDXCommandRejectsTestSpotWhenCTYInvalid(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, ctyLookup, nil, nil)

	resp := p.ProcessCommandForClient("DX 7001 K8ZB Test", "ZZTEST-1", "", nil, "classic")
	if resp != testCallCTYInvalidMsg {
		t.Fatalf("expected CTY invalid message, got %q", resp)
	}
	select {
	case <-input:
		t.Fatal("expected test spot to be rejected when CTY is invalid")
	default:
	}
}

func TestDXCommandQueuesTestSpotWhenCTYValid(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, ctyLookup, nil, nil)

	resp := p.ProcessCommandForClient("DX 7001 K8ZB Test", "K1TEST-1", "", nil, "classic")
	if !strings.Contains(resp, "Spot queued") {
		t.Fatalf("expected spot queued, got %q", resp)
	}
	select {
	case s := <-input:
		if !s.IsTestSpotter {
			t.Fatalf("expected test spotter flag, got %v", s.IsTestSpotter)
		}
	default:
		t.Fatal("expected test spot to be queued")
	}
}

func TestShowDXCCPrefixAndSiblings(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	p := NewProcessor(nil, nil, nil, ctyLookup, nil, nil)

	resp := p.ProcessCommandForClient("SHOW DXCC IT9", "N2WQ", "", nil, "go")
	expected := "IT9 -> ADIF 248 | Sicily (EU) | Prefix: IT9 | CQ 15 | ITU 28 | Other: I"
	if !strings.Contains(resp, expected) {
		t.Fatalf("expected %q in response, got %q", expected, resp)
	}
}

func TestShowDXCCPortableCall(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	p := NewProcessor(nil, nil, nil, ctyLookup, nil, nil)

	resp := p.ProcessCommandForClient("SHOW DXCC W6/LZ5VV", "N2WQ", "", nil, "go")
	expected := "W6/LZ5VV -> ADIF 291 | United States (NA) | Prefix: K | CQ 3 | ITU 6"
	if !strings.Contains(resp, expected) {
		t.Fatalf("expected %q in response, got %q", expected, resp)
	}
	if strings.Contains(resp, "Other:") {
		t.Fatalf("did not expect Other list for single-prefix ADIF: %q", resp)
	}
}

func TestShowDXCCMobileSuffix(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	p := NewProcessor(nil, nil, nil, ctyLookup, nil, nil)

	resp := p.ProcessCommandForClient("SHOW DXCC W6/LZ5VV/M", "N2WQ", "", nil, "go")
	expected := "W6/LZ5VV -> ADIF 291 | United States (NA) | Prefix: K | CQ 3 | ITU 6"
	if !strings.Contains(resp, expected) {
		t.Fatalf("expected %q in response, got %q", expected, resp)
	}
}

func TestShowMYDXRequiresFilter(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	resp := p.ProcessCommandForClient("SHOW MYDX 5", "N2WQ", "", nil, "classic")
	if resp != noLoggedUserMsg {
		t.Fatalf("expected filter requirement message, got %q", resp)
	}
}

func TestShowDXFiltersResults(t *testing.T) {
	spotOld := spot.NewSpot("DXAAA", "DE1AA", 14074.0, "FT8")
	spotOld.Time = time.Now().UTC().Add(-2 * time.Minute)
	spotNew := spot.NewSpot("DXBBB", "DE2BB", 14030.0, "CW")
	spotNew.Time = time.Now().UTC().Add(-1 * time.Minute)
	archive := &fakeArchive{spots: []*spot.Spot{spotNew, spotOld}}

	p := NewProcessor(nil, archive, nil, nil, nil, nil)
	filterFn := func(s *spot.Spot) bool {
		return s != nil && s.DXCall == "DXBBB"
	}
	resp := p.ProcessCommandForClient("SHOW DX 5", "N2WQ", "", filterFn, "classic")
	if !strings.Contains(resp, "DXBBB") {
		t.Fatalf("expected filtered spot DXBBB, got %q", resp)
	}
	if strings.Contains(resp, "DXAAA") {
		t.Fatalf("unexpected filtered spot DXAAA in response: %q", resp)
	}
}

func TestShowMYDXDXCCSelectorOnly(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }

	usNew := spot.NewSpot("K1AAA", "DE1AA", 14074.0, "FT8")
	usNew.DXMetadata.ADIF = 291
	usNew.Time = time.Now().UTC().Add(-30 * time.Second)

	it := spot.NewSpot("IT9BBB", "DE2BB", 14030.0, "CW")
	it.DXMetadata.ADIF = 248
	it.Time = time.Now().UTC().Add(-45 * time.Second)

	usOld := spot.NewSpot("W6CCC", "DE3CC", 10136.0, "CW")
	usOld.DXMetadata.ADIF = 291
	usOld.Time = time.Now().UTC().Add(-60 * time.Second)

	archive := &fakeArchive{spots: []*spot.Spot{usNew, it, usOld}}
	p := NewProcessor(nil, archive, nil, ctyLookup, nil, nil)
	filterFn := func(s *spot.Spot) bool { return s != nil }

	resp := p.ProcessCommandForClient("SHOW MYDX W6/LZ5VV", "N2WQ", "", filterFn, "go")
	if !strings.Contains(resp, "K1AAA") || !strings.Contains(resp, "W6CCC") {
		t.Fatalf("expected US ADIF matches in response, got %q", resp)
	}
	if strings.Contains(resp, "IT9BBB") {
		t.Fatalf("unexpected non-matching ADIF in response: %q", resp)
	}
}

func TestShowDXDXCCSelectorCountBothOrders(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }

	usNew := spot.NewSpot("K1AAA", "DE1AA", 14074.0, "FT8")
	usNew.DXMetadata.ADIF = 291
	usNew.Time = time.Now().UTC().Add(-20 * time.Second)

	usOld := spot.NewSpot("W6CCC", "DE3CC", 10136.0, "CW")
	usOld.DXMetadata.ADIF = 291
	usOld.Time = time.Now().UTC().Add(-40 * time.Second)

	other := spot.NewSpot("IT9BBB", "DE2BB", 14030.0, "CW")
	other.DXMetadata.ADIF = 248
	other.Time = time.Now().UTC().Add(-30 * time.Second)

	archive := &fakeArchive{spots: []*spot.Spot{usNew, other, usOld}}
	p := NewProcessor(nil, archive, nil, ctyLookup, nil, nil)
	filterFn := func(s *spot.Spot) bool { return s != nil }

	respSelectorCount := p.ProcessCommandForClient("SHOW DX W6/LZ5VV/M 1", "N2WQ", "", filterFn, "go")
	respCountSelector := p.ProcessCommandForClient("SHOW DX 1 W6/LZ5VV/M", "N2WQ", "", filterFn, "go")
	if respSelectorCount != respCountSelector {
		t.Fatalf("expected both argument orders to match, got %q vs %q", respSelectorCount, respCountSelector)
	}
	lines := strings.Split(strings.TrimSpace(strings.ReplaceAll(respSelectorCount, "\r", "")), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one line for count=1, got %d (%q)", len(lines), respSelectorCount)
	}
	if !strings.Contains(respSelectorCount, "K1AAA") {
		t.Fatalf("expected newest matching ADIF spot, got %q", respSelectorCount)
	}
}

func TestShowMYDXDXCCSelectorErrors(t *testing.T) {
	filterFn := func(s *spot.Spot) bool { return s != nil }
	pNoCTY := NewProcessor(nil, &fakeArchive{}, nil, nil, nil, nil)
	resp := pNoCTY.ProcessCommandForClient("SHOW MYDX JA1ABC 5", "N2WQ", "", filterFn, "go")
	if resp != "CTY database is not available.\n" {
		t.Fatalf("expected CTY unavailable error, got %q", resp)
	}

	nilLookup := func() *cty.CTYDatabase { return nil }
	pUnloaded := NewProcessor(nil, &fakeArchive{}, nil, nilLookup, nil, nil)
	resp = pUnloaded.ProcessCommandForClient("SHOW MYDX JA1ABC 5", "N2WQ", "", filterFn, "go")
	if resp != "CTY database is not loaded.\n" {
		t.Fatalf("expected CTY unloaded error, got %q", resp)
	}

	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	p := NewProcessor(nil, &fakeArchive{}, nil, ctyLookup, nil, nil)
	resp = p.ProcessCommandForClient("SHOW MYDX ZZ0ZZ", "N2WQ", "", filterFn, "go")
	if resp != "Unknown DXCC/prefix.\n" {
		t.Fatalf("expected unknown selector error, got %q", resp)
	}
}

func TestShowMYDXSelectorArgumentValidation(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	p := NewProcessor(nil, &fakeArchive{}, nil, ctyLookup, nil, nil)
	filterFn := func(s *spot.Spot) bool { return s != nil }

	resp := p.ProcessCommandForClient("SHOW MYDX JA1ABC IT9", "N2WQ", "", filterFn, "go")
	if !strings.Contains(resp, "Usage: SHOW MYDX") {
		t.Fatalf("expected usage for two selectors, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW MYDX 10 20", "N2WQ", "", filterFn, "go")
	if !strings.Contains(resp, "Usage: SHOW MYDX") {
		t.Fatalf("expected usage for two counts, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW MYDX 251 W6/LZ5VV", "N2WQ", "", filterFn, "go")
	if resp != "Invalid count. Use 1-250.\n" {
		t.Fatalf("expected count bounds error, got %q", resp)
	}
}

func TestHelpPerDialect(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)

	classic := p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "classic")
	if !strings.Contains(classic, "HELP <command>") || !strings.Contains(classic, "SHOW DX -") {
		t.Fatalf("classic help missing expected content: %q", classic)
	}
	if !strings.Contains(classic, "List types:") || !strings.Contains(classic, "Supported bands:") {
		t.Fatalf("classic help missing list sections: %q", classic)
	}

	cc := p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "cc")
	if !strings.Contains(strings.ToUpper(cc), "CC SHORTCUTS:") || !strings.Contains(cc, "SHOW/DX -") || !strings.Contains(cc, "SET/ANN -") {
		t.Fatalf("cc help missing cc aliases: %q", cc)
	}
	if !strings.Contains(cc, "SET/FILTER <type>/ON") || !strings.Contains(cc, "SET/FILTER <type>/OFF") {
		t.Fatalf("cc help missing ON/OFF mapping: %q", cc)
	}
}

func TestShowDXDialectVariants(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	filterFn := func(s *spot.Spot) bool { return s != nil }

	resp := p.ProcessCommandForClient("SHOW DX", "N2WQ", "", filterFn, "go")
	if strings.Contains(resp, "Unknown command") {
		t.Fatalf("expected SHOW DX accepted for go dialect, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW/DX", "N2WQ", "", filterFn, "cc")
	if strings.Contains(resp, "Unknown command") {
		t.Fatalf("expected SHOW/DX accepted for cc dialect, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SH/DX 5", "N2WQ", "", filterFn, "cc")
	if strings.Contains(resp, "Unknown command") {
		t.Fatalf("expected SH/DX accepted for cc dialect, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW/DX", "N2WQ", "", filterFn, "go")
	if !strings.Contains(resp, "SHOW DX") {
		t.Fatalf("expected go dialect to reject SHOW/DX with guidance, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW DX", "N2WQ", "", filterFn, "cc")
	if strings.Contains(resp, "SHOW/DX") && strings.Contains(resp, "Use SHOW/DX") {
		t.Fatalf("expected cc dialect to accept SHOW DX alias, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SH DX 5", "N2WQ", "", filterFn, "cc")
	if strings.Contains(resp, "Unknown command") {
		t.Fatalf("expected SH DX accepted for cc dialect, got %q", resp)
	}
}

func TestHelpLineWidth(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	helps := []string{
		p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "classic"),
		p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "cc"),
		p.ProcessCommandForClient("HELP DX", "N2WQ", "", nil, "go"),
		p.ProcessCommandForClient("HELP PASS", "N2WQ", "", nil, "go"),
		p.ProcessCommandForClient("HELP SHOW/DX", "N2WQ", "", nil, "cc"),
		p.ProcessCommandForClient("HELP SET/FILTER", "N2WQ", "", nil, "cc"),
	}
	for _, help := range helps {
		lines := strings.Split(help, "\n")
		for _, line := range lines {
			line = strings.TrimRight(line, "\r")
			if line == "" {
				continue
			}
			if len(line) > 78 {
				t.Fatalf("help line exceeds 78 chars: %q", line)
			}
		}
	}
}

func TestHelpTopicGoDialect(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)

	resp := p.ProcessCommandForClient("HELP DX", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Usage: DX <freq_khz> <callsign> [comment]") {
		t.Fatalf("expected DX usage in help, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP PASS", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Usage: PASS <type> <list>") || !strings.Contains(resp, "Types:") {
		t.Fatalf("expected PASS usage and types, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW DX", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Aliases:") || !strings.Contains(resp, "SHOW DX -") {
		t.Fatalf("expected SHOW DX aliases, got %q", resp)
	}
}

func TestHelpTopicCCDialect(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)

	resp := p.ProcessCommandForClient("HELP SHOW/DX", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "Usage: SHOW/DX [count]") {
		t.Fatalf("expected SHOW/DX usage, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SET/ANN", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "Alias of PASS ANNOUNCE") {
		t.Fatalf("expected SET/ANN alias, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SET/FT8", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "Modes:") || !strings.Contains(resp, "FT8") {
		t.Fatalf("expected mode shortcuts in help, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW DX", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "SHOW/DX - Alias of SHOW MYDX") {
		t.Fatalf("expected HELP SHOW DX to map to SHOW/DX in cc, got %q", resp)
	}
}

func TestHelpEntriesGoDialect(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	cases := []struct {
		topic    string
		contains []string
	}{
		{"HELP", []string{"HELP - Show command list", "Usage: HELP [command]"}},
		{"DX", []string{"DX - Post a spot", "Usage: DX <freq_khz> <callsign> [comment]"}},
		{"SHOW", []string{"SHOW - See SHOW subcommands.", "SHOW DXCC"}},
		{"SHOW DX", []string{"SHOW DX - Alias of SHOW MYDX (stored history)", "Usage: SHOW DX [count]"}},
		{"SHOW MYDX", []string{"SHOW MYDX - Show filtered spot history", "stored spots"}},
		{"SHOW DXCC", []string{"SHOW DXCC - Look up DXCC/ADIF and zones", "other prefixes"}},
		{"SHOW DEDUPE", []string{"SHOW DEDUPE - Show your broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DEDUPE", []string{"SET DEDUPE - Select broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DIAG", []string{"SET DIAG - Toggle diagnostic comments", "Usage: SET DIAG <ON|OFF>"}},
		{"SET GRID", []string{"SET GRID - Set your grid", "Usage: SET GRID <4-6 char maidenhead>"}},
		{"SET NOISE", []string{"SET NOISE - Set your noise class for glyphs", "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN>"}},
		{"SHOW FILTER", []string{"SHOW FILTER - Display current filter state.", "Usage: SHOW FILTER"}},
		{"PASS", []string{"PASS - Allow filter matches", "Usage: PASS <type> <list>", "Types:"}},
		{"REJECT", []string{"REJECT - Block filter matches", "Usage: REJECT <type> <list>", "Types:"}},
		{"RESET FILTER", []string{"RESET FILTER - Reset filters", "Usage: RESET FILTER"}},
		{"DIALECT", []string{"DIALECT - Show or switch filter command dialect", "DIALECT LIST"}},
		{"BYE", []string{"BYE - Disconnect", "Usage: BYE"}},
	}
	for _, tc := range cases {
		resp := p.ProcessCommandForClient("HELP "+tc.topic, "N2WQ", "", nil, "go")
		assertHelpContains(t, tc.topic, resp, tc.contains)
	}
}

func TestHelpEntriesCCDialect(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	cases := []struct {
		topic    string
		contains []string
	}{
		{"HELP", []string{"HELP - Show command list", "Usage: HELP [command]"}},
		{"DX", []string{"DX - Post a spot", "Usage: DX <freq_khz> <callsign> [comment]"}},
		{"SHOW", []string{"SHOW - See SHOW subcommands.", "SHOW DXCC"}},
		{"SHOW/DX", []string{"SHOW/DX - Alias of SHOW MYDX (stored history)", "Usage: SHOW/DX [count]"}},
		{"SHOW MYDX", []string{"SHOW MYDX - Show filtered spot history", "stored spots"}},
		{"SHOW DXCC", []string{"SHOW DXCC - Look up DXCC/ADIF and zones", "other prefixes"}},
		{"SHOW DEDUPE", []string{"SHOW DEDUPE - Show your broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DEDUPE", []string{"SET DEDUPE - Select broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DIAG", []string{"SET DIAG - Toggle diagnostic comments", "Usage: SET DIAG <ON|OFF>"}},
		{"SET GRID", []string{"SET GRID - Set your grid", "Usage: SET GRID <4-6 char maidenhead>"}},
		{"SET NOISE", []string{"SET NOISE - Set your noise class for glyphs", "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN>"}},
		{"SHOW/FILTER", []string{"SHOW/FILTER - Display current filter state.", "Usage: SHOW/FILTER"}},
		{"SET/FILTER", []string{"SET/FILTER - Allow list-based filters", "Example: SET/FILTER BAND/ON", "DXBM"}},
		{"UNSET/FILTER", []string{"UNSET/FILTER - Block list-based filters", "Usage: UNSET/FILTER <type> <list>", "DXBM"}},
		{"SET/NOFILTER", []string{"SET/NOFILTER - Allow everything", "Usage: SET/NOFILTER"}},
		{"SET/ANN", []string{"SET/ANN - Enable announcements", "Alias of PASS ANNOUNCE"}},
		{"SET/NOANN", []string{"SET/NOANN - Disable announcements", "Alias of REJECT ANNOUNCE"}},
		{"SET/BEACON", []string{"SET/BEACON - Enable beacon spots", "Alias of PASS BEACON"}},
		{"SET/NOBEACON", []string{"SET/NOBEACON - Disable beacon spots", "Alias of REJECT BEACON"}},
		{"SET/WWV", []string{"SET/WWV - Enable WWV bulletins", "Alias of PASS WWV"}},
		{"SET/NOWWV", []string{"SET/NOWWV - Disable WWV bulletins", "Alias of REJECT WWV"}},
		{"SET/WCY", []string{"SET/WCY - Enable WCY bulletins", "Alias of PASS WCY"}},
		{"SET/NOWCY", []string{"SET/NOWCY - Disable WCY bulletins", "Alias of REJECT WCY"}},
		{"SET/SELF", []string{"SET/SELF - Enable self spots", "Alias of PASS SELF"}},
		{"SET/NOSELF", []string{"SET/NOSELF - Disable self spots", "Alias of REJECT SELF"}},
		{"SET/SKIMMER", []string{"SET/SKIMMER - Allow skimmer spots", "Alias of PASS SOURCE SKIMMER"}},
		{"SET/NOSKIMMER", []string{"SET/NOSKIMMER - Block skimmer spots", "Alias of REJECT SOURCE SKIMMER"}},
		{"SET/FT8", []string{"SET/<MODE> - Allow a mode", "Alias of PASS MODE <MODE>"}},
		{"SET/NOFT8", []string{"SET/NO<MODE> - Block a mode", "Alias of REJECT MODE <MODE>"}},
		{"DIALECT", []string{"DIALECT - Show or switch filter command dialect", "DIALECT LIST"}},
		{"BYE", []string{"BYE - Disconnect", "Usage: BYE"}},
	}
	for _, tc := range cases {
		resp := p.ProcessCommandForClient("HELP "+tc.topic, "N2WQ", "", nil, "cc")
		assertHelpContains(t, tc.topic, resp, tc.contains)
	}
}

func assertHelpContains(t *testing.T, topic string, resp string, contains []string) {
	t.Helper()
	for _, want := range contains {
		if !strings.Contains(resp, want) {
			t.Fatalf("help %q missing %q in %q", topic, want, resp)
		}
	}
}

func TestShowMYDXCountBounds(t *testing.T) {
	spots := make([]*spot.Spot, 0, 260)
	for i := 0; i < 60; i++ {
		dx := "DX" + strconv.Itoa(i)
		s := spot.NewSpot(dx, "DE1AA", 14074.0, "FT8")
		s.Time = time.Now().UTC().Add(time.Duration(-i) * time.Second)
		spots = append(spots, s)
	}
	archive := &fakeArchive{spots: spots}
	p := NewProcessor(nil, archive, nil, nil, nil, nil)
	filterFn := func(s *spot.Spot) bool { return s != nil }

	resp := p.ProcessCommandForClient("SHOW MYDX", "N2WQ", "", filterFn, "classic")
	lines := strings.Split(strings.TrimRight(resp, "\r\n"), "\n")
	if len(lines) != showDXDefaultCount {
		t.Fatalf("expected %d lines, got %d", showDXDefaultCount, len(lines))
	}

	resp = p.ProcessCommandForClient("SHOW MYDX 251", "N2WQ", "", filterFn, "classic")
	if !strings.Contains(resp, "Invalid count. Use 1-250.") {
		t.Fatalf("expected count error, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW MYDX 250", "N2WQ", "", filterFn, "classic")
	if strings.Contains(resp, "Invalid count") {
		t.Fatalf("expected count 250 accepted, got %q", resp)
	}
}

func TestShowDXArchiveOnly(t *testing.T) {
	rb := buffer.NewRingBuffer(5)
	rb.Add(spot.NewSpot("DXAAA", "DE1AA", 14074.0, "FT8"))
	p := NewProcessor(rb, nil, nil, nil, nil, nil)
	filterFn := func(s *spot.Spot) bool { return s != nil }

	resp := p.ProcessCommandForClient("SHOW DX 1", "N2WQ", "", filterFn, "classic")
	if resp != "No spots available.\n" {
		t.Fatalf("expected archive-only response, got %q", resp)
	}
}

func TestShowMYDXArchiveError(t *testing.T) {
	archive := &fakeArchive{err: errors.New("archive down")}
	p := NewProcessor(nil, archive, nil, nil, nil, nil)
	filterFn := func(s *spot.Spot) bool { return s != nil }

	resp := p.ProcessCommandForClient("SHOW MYDX 1", "N2WQ", "", filterFn, "classic")
	if resp != "No spots available.\n" {
		t.Fatalf("expected archive error to return no spots, got %q", resp)
	}
}

const sampleCTYPLIST = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
<key>K8ZB</key>
	<dict>
		<key>Country</key>
		<string>Alpha</string>
		<key>Prefix</key>
		<string>K8ZB</string>
		<key>ADIF</key>
		<integer>1</integer>
		<key>CQZone</key>
		<integer>5</integer>
		<key>ITUZone</key>
		<integer>8</integer>
		<key>Continent</key>
		<string>NA</string>
		<key>ExactCallsign</key>
		<true/>
	</dict>
<key>K1</key>
	<dict>
		<key>Country</key>
		<string>Alpha</string>
		<key>Prefix</key>
		<string>K1</string>
		<key>ADIF</key>
		<integer>1</integer>
		<key>CQZone</key>
		<integer>5</integer>
		<key>ITUZone</key>
		<integer>8</integer>
		<key>Continent</key>
		<string>NA</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
<key>W6</key>
	<dict>
		<key>Country</key>
		<string>United States</string>
		<key>Prefix</key>
		<string>K</string>
		<key>ADIF</key>
		<integer>291</integer>
		<key>CQZone</key>
		<integer>3</integer>
		<key>ITUZone</key>
		<integer>6</integer>
		<key>Continent</key>
		<string>NA</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
<key>I</key>
	<dict>
		<key>Country</key>
		<string>Italy</string>
		<key>Prefix</key>
		<string>I</string>
		<key>ADIF</key>
		<integer>248</integer>
		<key>CQZone</key>
		<integer>15</integer>
		<key>ITUZone</key>
		<integer>28</integer>
		<key>Continent</key>
		<string>EU</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
<key>IT9</key>
	<dict>
		<key>Country</key>
		<string>Sicily</string>
		<key>Prefix</key>
		<string>IT9</string>
		<key>ADIF</key>
		<integer>248</integer>
		<key>CQZone</key>
		<integer>15</integer>
		<key>ITUZone</key>
		<integer>28</integer>
		<key>Continent</key>
		<string>EU</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
</dict>
</plist>`

func loadTestCTY(t *testing.T) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(sampleCTYPLIST))
	if err != nil {
		t.Fatalf("load test CTY: %v", err)
	}
	return db
}
