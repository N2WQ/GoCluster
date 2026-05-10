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

type fakeWhoSpotsMeQuerier struct {
	window time.Duration
	counts map[string]map[string]map[string][]spot.WhoSpotsMeCountryCount
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

func (f *fakeWhoSpotsMeQuerier) Window() time.Duration {
	if f == nil {
		return 0
	}
	return f.window
}

func (f *fakeWhoSpotsMeQuerier) CountryCountsByContinent(call, band string, _ time.Time) map[string][]spot.WhoSpotsMeCountryCount {
	if f == nil {
		return nil
	}
	call = spot.NormalizeOwnCallsign(call)
	band = spot.NormalizeBand(band)
	if byBand, ok := f.counts[call]; ok {
		return byBand[band]
	}
	return nil
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

func TestDXCommandQueuesLoggerStyleSpot(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)

	resp := p.ProcessCommandForClient("DX K8ZB 7001.25 Testing...1...2...3", "N2WQ", "203.0.113.5", nil, "classic")
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
		if math.Abs(s.Frequency-7001.25) > 0.0001 {
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

func TestDXCommandUsesBaselineSenderAndCanonicalDX(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)

	resp := p.ProcessCommandForClient("DX 7001 K8ZB-2 Testing", "N2WQ-7", "203.0.113.5", nil, "classic")
	if !strings.Contains(resp, "Spot queued") {
		t.Fatalf("expected queue response, got %q", resp)
	}

	select {
	case s := <-input:
		if s.DXCall != "K8ZB" || s.DXCallNorm != "K8ZB" {
			t.Fatalf("expected canonical DX K8ZB, got DXCall=%q DXCallNorm=%q", s.DXCall, s.DXCallNorm)
		}
		if s.DECall != "N2WQ" || s.DECallNorm != "N2WQ" || s.SourceNode != "N2WQ" {
			t.Fatalf("expected baseline sender N2WQ, got DECall=%q DECallNorm=%q SourceNode=%q", s.DECall, s.DECallNorm, s.SourceNode)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for spot")
	}
}

func TestDXCommandLoggerStyleFloatLikeCallsign(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)

	resp := p.ProcessCommandForClient("DX 5E7 14033 CW", "N2WQ", "", nil, "classic")
	if !strings.Contains(resp, "Spot queued") {
		t.Fatalf("expected queue response, got %q", resp)
	}

	select {
	case s := <-input:
		if s.DXCall != "5E7" {
			t.Fatalf("expected DXCall 5E7, got %q", s.DXCall)
		}
		if math.Abs(s.Frequency-14033.0) > 0.0001 {
			t.Fatalf("Frequency mismatch: %.4f", s.Frequency)
		}
		if s.Mode != "CW" {
			t.Fatalf("expected mode CW, got %q", s.Mode)
		}
		if s.ModeProvenance != spot.ModeProvenanceCommentExplicit {
			t.Fatalf("expected comment explicit mode provenance, got %q", s.ModeProvenance)
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

func TestDXCommandParsesFT2Mode(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)

	resp := p.ProcessCommandForClient("DX 14080 K8ZB FT2 CQ TEST", "N2WQ", "", nil, "classic")
	if !strings.Contains(resp, "Spot queued") {
		t.Fatalf("expected queue response, got %q", resp)
	}

	select {
	case s := <-input:
		if s.Mode != "FT2" {
			t.Fatalf("expected FT2 mode, got %q", s.Mode)
		}
		if s.ModeNorm != "FT2" {
			t.Fatalf("expected FT2 ModeNorm, got %q", s.ModeNorm)
		}
		if s.ModeProvenance != spot.ModeProvenanceCommentExplicit {
			t.Fatalf("expected comment explicit mode provenance, got %q", s.ModeProvenance)
		}
		if strings.TrimSpace(s.Comment) != "CQ TEST" {
			t.Fatalf("expected cleaned comment, got %q", s.Comment)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for FT2 spot")
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

func TestDXCommandReportsBadDXCall(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	var got struct {
		source string
		role   string
		reason string
		call   string
		de     string
		dx     string
		mode   string
		detail string
	}
	p := NewProcessor(nil, nil, input, nil, nil, nil, WithBadCallReporter(func(source, role, reason, call, deCall, dxCall, mode, detail string) {
		got.source = source
		got.role = role
		got.reason = reason
		got.call = call
		got.de = deCall
		got.dx = dxCall
		got.mode = mode
		got.detail = detail
	}))

	resp := p.ProcessCommandForClient("DX 7001 BAD! FT8", "N2WQ", "", nil, "classic")
	if !strings.Contains(resp, "Invalid DX callsign") {
		t.Fatalf("expected invalid DX response, got %q", resp)
	}
	if got.source != "manual:N2WQ" || got.role != "DX" || got.reason != "invalid_callsign" || got.call != "BAD!" || got.de != "N2WQ" || got.dx != "BAD!" || got.mode != "FT8" || got.detail != "manual_dx" {
		t.Fatalf("unexpected bad-call report: %+v", got)
	}
}

func TestDXCommandRejectsModeShortcutAsDXCall(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	var got struct {
		role   string
		reason string
		call   string
	}
	p := NewProcessor(nil, nil, input, func() *cty.CTYDatabase { return loadTestCTY(t) }, nil, nil, WithBadCallReporter(func(source, role, reason, call, deCall, dxCall, mode, detail string) {
		got.role = role
		got.reason = reason
		got.call = call
	}))

	resp := p.ProcessCommandForClient("DX 7001 SET/NOFT8 FT8", "N2WQ", "", nil, "classic")
	if !strings.Contains(resp, "Invalid DX callsign") {
		t.Fatalf("expected invalid DX response, got %q", resp)
	}
	if got.role != "DX" || got.reason != "invalid_callsign" || got.call != "SET/NOFT8" {
		t.Fatalf("unexpected bad-call report: %+v", got)
	}
}

func TestDXCommandReportsCTYUnknownDXCall(t *testing.T) {
	ctyDB := loadTestCTY(t)
	ctyLookup := func() *cty.CTYDatabase { return ctyDB }
	input := make(chan *spot.Spot, 1)
	var got struct {
		role   string
		reason string
		call   string
		mode   string
	}
	p := NewProcessor(nil, nil, input, ctyLookup, nil, nil, WithBadCallReporter(func(source, role, reason, call, deCall, dxCall, mode, detail string) {
		got.role = role
		got.reason = reason
		got.call = call
		got.mode = mode
	}))

	resp := p.ProcessCommandForClient("DX 7001 K4ZZZ FT8", "N2WQ", "", nil, "classic")
	if !strings.Contains(resp, "Unknown DX callsign") {
		t.Fatalf("expected CTY validation response, got %q", resp)
	}
	if got.role != "DX" || got.reason != "cty_unknown" || got.call != "K4ZZZ" || got.mode != "FT8" {
		t.Fatalf("unexpected bad-call report: %+v", got)
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

	resp := p.ProcessCommandForClient("DX 7001 K8ZB Test", "Z9TEST-1", "", nil, "classic")
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
	if !strings.Contains(classic, "Confidence glyphs:") || !strings.Contains(classic, "One reporter only;") {
		t.Fatalf("classic help missing confidence legend: %q", classic)
	}

	cc := p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "cc")
	if !strings.Contains(strings.ToUpper(cc), "CC SHORTCUTS:") || !strings.Contains(cc, "SHOW/DX -") || !strings.Contains(cc, "SET/ANN -") {
		t.Fatalf("cc help missing cc aliases: %q", cc)
	}
	if !strings.Contains(cc, "SET/FILTER <type>/ON") || !strings.Contains(cc, "SET/FILTER <type>/OFF") {
		t.Fatalf("cc help missing ON/OFF mapping: %q", cc)
	}
	if !strings.Contains(cc, "Confidence glyphs:") || !strings.Contains(cc, "The call was corrected.") {
		t.Fatalf("cc help missing confidence legend: %q", cc)
	}
}

func TestHelpPathGlyphLegendUsesConfiguredSymbols(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil, WithPathGlyphHelp(PathGlyphHelpConfig{
		Enabled:      true,
		High:         ">",
		Medium:       "=",
		Low:          "<",
		Unlikely:     "-",
		Insufficient: " ",
	}))

	resp := p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "go")
	for _, want := range []string{
		"Path reliability glyphs:",
		`">" - HIGH: favorable path.`,
		`"=" - MEDIUM: workable path.`,
		`"<" - LOW: weak or marginal path.`,
		`"-" - UNLIKELY: poor path.`,
		`" " - INSUFFICIENT: not enough recent evidence.`,
		"PATH filters use HIGH, MEDIUM, LOW, UNLIKELY, INSUFFICIENT.",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("expected configured path glyph help %q in %q", want, resp)
		}
	}
}

func TestHelpPathGlyphLegendOmittedWhenDisabled(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil, WithPathGlyphHelp(PathGlyphHelpConfig{
		Enabled:      false,
		High:         ">",
		Medium:       "=",
		Low:          "<",
		Unlikely:     "-",
		Insufficient: " ",
	}))

	resp := p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "go")
	if strings.Contains(resp, "Path reliability glyphs:") {
		t.Fatalf("expected path glyph legend omitted when disabled, got %q", resp)
	}
}

func TestHelpDedupeNotesUseConfiguredWindows(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil, WithDedupeHelp(DedupeHelpConfig{
		Configured:        true,
		FastWindowSeconds: 121,
		MedWindowSeconds:  305,
		SlowWindowSeconds: 489,
	}))

	for _, topic := range []string{"SHOW DEDUPE", "SET DEDUPE"} {
		resp := p.ProcessCommandForClient("HELP "+topic, "N2WQ", "", nil, "go")
		for _, want := range []string{
			"FAST - 121s window. Key: band + DE DXCC (ADIF) + DE grid2 + DX call.",
			"MED - 305s window. Key: band + DE DXCC (ADIF) + DE grid2 + DX call.",
			"SLOW - 489s window. Key: band + DE DXCC (ADIF) + DE CQ zone + DX call.",
		} {
			if !strings.Contains(resp, want) {
				t.Fatalf("help %q missing %q in %q", topic, want, resp)
			}
		}
		if strings.Contains(strings.ToLower(resp), "source class") {
			t.Fatalf("help %q unexpectedly mentions source class: %q", topic, resp)
		}
		if strings.Contains(strings.ToLower(resp), "normalized dx call") {
			t.Fatalf("help %q unexpectedly mentions normalized DX call: %q", topic, resp)
		}
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

func TestShowBuild(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil, WithBuildInfo(BuildInfo{
		Version:     "v26.24.04-76fa6eac04fb",
		Commit:      "76fa6eac04fb",
		BuildTime:   "2026-04-24T12:34:56Z",
		VCSModified: "true",
		GoVersion:   "go1.26.2",
	}))

	resp := p.ProcessCommandForClient("SHOW BUILD", "N2WQ", "", nil, "go")
	want := []string{
		"Build version: v26.24.04-76fa6eac04fb",
		"Go: go1.26.2",
	}
	for _, line := range want {
		if !strings.Contains(resp, line) {
			t.Fatalf("SHOW BUILD missing %q in %q", line, resp)
		}
	}
	for _, line := range []string{"Commit:", "Built:", "Dirty:"} {
		if strings.Contains(resp, line) {
			t.Fatalf("SHOW BUILD should omit %q, got %q", line, resp)
		}
	}

	resp = p.ProcessCommandForClient("SH BUILD", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Build version: v26.24.04-76fa6eac04fb") {
		t.Fatalf("expected SH BUILD alias to show build info, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW BUILD EXTRA", "N2WQ", "", nil, "go")
	if resp != "Usage: SHOW BUILD\n" {
		t.Fatalf("expected SHOW BUILD usage for extra args, got %q", resp)
	}

	resp = p.ProcessCommandForClient("SHOW BUILD", "", "", nil, "go")
	if resp != noLoggedUserMsg {
		t.Fatalf("expected login gate for SHOW BUILD, got %q", resp)
	}
}

func TestShowOwnReportsLoginAndBaselineCall(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)

	resp := p.ProcessCommandForClient("SHOW OWN", "N2WQ-7", "", nil, "go")
	for _, want := range []string{
		"Own call: N2WQ\n",
		"Login call: N2WQ-7\n",
		"SSID handling: numeric SSIDs are ignored",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("SHOW OWN missing %q in %q", want, resp)
		}
	}

	resp = p.ProcessCommandForClient("SHOW OWN EXTRA", "N2WQ-7", "", nil, "go")
	if resp != "Usage: SHOW OWN\n" {
		t.Fatalf("expected SHOW OWN usage for extra args, got %q", resp)
	}
}

func TestShowBuildDefaults(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)

	resp := p.ProcessCommandForClient("SHOW BUILD", "N2WQ", "", nil, "go")
	want := []string{
		"Build version: dev",
	}
	for _, line := range want {
		if !strings.Contains(resp, line) {
			t.Fatalf("SHOW BUILD defaults missing %q in %q", line, resp)
		}
	}
	if strings.Contains(resp, "Commit:") || strings.Contains(resp, "Built:") ||
		strings.Contains(resp, "Dirty:") || strings.Contains(resp, "Go:") {
		t.Fatalf("expected empty optional build fields to be omitted, got %q", resp)
	}
}

func TestHelpLineWidth(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	pWithPathGlyphs := NewProcessor(nil, nil, nil, nil, nil, nil, WithPathGlyphHelp(PathGlyphHelpConfig{
		Enabled:      true,
		High:         ">",
		Medium:       "=",
		Low:          "<",
		Unlikely:     "-",
		Insufficient: " ",
	}))
	pWithDedupeWindows := NewProcessor(nil, nil, nil, nil, nil, nil, WithDedupeHelp(DedupeHelpConfig{
		Configured:        true,
		FastWindowSeconds: 121,
		MedWindowSeconds:  305,
		SlowWindowSeconds: 489,
	}))
	pWithWhoSpotsMe := NewProcessor(nil, nil, nil, nil, nil, nil, WithWhoSpotsMeHelp(WhoSpotsMeHelpConfig{
		Configured:    true,
		WindowMinutes: 10,
	}))
	helps := []string{
		p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "classic"),
		pWithPathGlyphs.ProcessCommandForClient("HELP", "N2WQ", "", nil, "go"),
		pWithDedupeWindows.ProcessCommandForClient("HELP SHOW DEDUPE", "N2WQ", "", nil, "go"),
		pWithDedupeWindows.ProcessCommandForClient("HELP SET DEDUPE", "N2WQ", "", nil, "go"),
		pWithWhoSpotsMe.ProcessCommandForClient("HELP WHOSPOTSME", "N2WQ", "", nil, "go"),
		p.ProcessCommandForClient("HELP", "N2WQ", "", nil, "cc"),
		p.ProcessCommandForClient("HELP DX", "N2WQ", "", nil, "go"),
		p.ProcessCommandForClient("HELP PASS", "N2WQ", "", nil, "go"),
		p.ProcessCommandForClient("HELP SHOW BUILD", "N2WQ", "", nil, "go"),
		p.ProcessCommandForClient("HELP SHOW OWN", "N2WQ", "", nil, "go"),
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
	if !strings.Contains(resp, "DX <callsign> <freq_khz> [comment]") {
		t.Fatalf("expected alternate DX usage in help, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP PASS", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Usage: PASS <type> <list>") || !strings.Contains(resp, "Types:") {
		t.Fatalf("expected PASS usage and types, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW DX", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Aliases:") || !strings.Contains(resp, "SHOW DX -") {
		t.Fatalf("expected SHOW DX aliases, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW BUILD", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Usage: SHOW BUILD") {
		t.Fatalf("expected SHOW BUILD usage, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW OWN", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Usage: SHOW OWN") {
		t.Fatalf("expected SHOW OWN usage, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP WHOSPOTSME", "N2WQ", "", nil, "go")
	if !strings.Contains(resp, "Usage: WHOSPOTSME [band]") {
		t.Fatalf("expected WHOSPOTSME usage, got %q", resp)
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

	resp = p.ProcessCommandForClient("HELP SET/FT2", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "Modes:") || !strings.Contains(resp, "FT2") {
		t.Fatalf("expected FT2 mode shortcut in help, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW DX", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "SHOW/DX - Alias of SHOW MYDX") {
		t.Fatalf("expected HELP SHOW DX to map to SHOW/DX in cc, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW BUILD", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "Usage: SHOW BUILD") {
		t.Fatalf("expected SHOW BUILD usage in cc, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP SHOW OWN", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "Usage: SHOW OWN") {
		t.Fatalf("expected SHOW OWN usage in cc, got %q", resp)
	}

	resp = p.ProcessCommandForClient("HELP WHOSPOTSME", "N2WQ", "", nil, "cc")
	if !strings.Contains(resp, "Usage: WHOSPOTSME [band]") {
		t.Fatalf("expected WHOSPOTSME usage in cc, got %q", resp)
	}
}

func TestHelpEntriesGoDialect(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	cases := []struct {
		topic    string
		contains []string
	}{
		{"HELP", []string{"HELP - Show command list", "Usage: HELP [command]"}},
		{"DX", []string{"DX - Post a spot", "Usage: DX <freq_khz> <callsign> [comment]", "DX <callsign> <freq_khz> [comment]"}},
		{"SHOW", []string{"SHOW - See SHOW subcommands.", "SHOW DXCC"}},
		{"SHOW DX", []string{"SHOW DX - Alias of SHOW MYDX (stored history)", "Usage: SHOW DX [count]"}},
		{"SHOW MYDX", []string{"SHOW MYDX - Show filtered spot history", "stored spots"}},
		{"SHOW DXCC", []string{"SHOW DXCC - Look up DXCC/ADIF and zones", "other prefixes"}},
		{"SHOW BUILD", []string{"SHOW BUILD - Show binary build metadata", "Usage: SHOW BUILD"}},
		{"SHOW OWN", []string{"SHOW OWN - Show your login and baseline own call", "Usage: SHOW OWN"}},
		{"WHOSPOTSME", []string{"WHOSPOTSME - Show recent spotter countries", "Usage: WHOSPOTSME [band]"}},
		{"SHOW DEDUPE", []string{"SHOW DEDUPE - Show your broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DEDUPE", []string{"SET DEDUPE - Select broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DIAG", []string{"SET DIAG - Select diagnostic comments", "Usage: SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>"}},
		{"SET GRID", []string{"SET GRID - Set your grid", "Usage: SET GRID <4-6 char maidenhead>"}},
		{"SET NOISE", []string{"SET NOISE - Set your noise class for glyphs", "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>"}},
		{"SET PATHSAMPLES", []string{"SET PATHSAMPLES - Set your path sample floor", "Usage: SET PATHSAMPLES <count|DEFAULT>"}},
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
		{"DX", []string{"DX - Post a spot", "Usage: DX <freq_khz> <callsign> [comment]", "DX <callsign> <freq_khz> [comment]"}},
		{"SHOW", []string{"SHOW - See SHOW subcommands.", "SHOW DXCC"}},
		{"SHOW/DX", []string{"SHOW/DX - Alias of SHOW MYDX (stored history)", "Usage: SHOW/DX [count]"}},
		{"SHOW MYDX", []string{"SHOW MYDX - Show filtered spot history", "stored spots"}},
		{"SHOW DXCC", []string{"SHOW DXCC - Look up DXCC/ADIF and zones", "other prefixes"}},
		{"SHOW BUILD", []string{"SHOW BUILD - Show binary build metadata", "Usage: SHOW BUILD"}},
		{"SHOW OWN", []string{"SHOW OWN - Show your login and baseline own call", "Usage: SHOW OWN"}},
		{"WHOSPOTSME", []string{"WHOSPOTSME - Show recent spotter countries", "Usage: WHOSPOTSME [band]"}},
		{"SHOW DEDUPE", []string{"SHOW DEDUPE - Show your broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DEDUPE", []string{"SET DEDUPE - Select broadcast dedupe policy", "FAST = short window", "CQ zones"}},
		{"SET DIAG", []string{"SET DIAG - Select diagnostic comments", "Usage: SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>"}},
		{"SET GRID", []string{"SET GRID - Set your grid", "Usage: SET GRID <4-6 char maidenhead>"}},
		{"SET NOISE", []string{"SET NOISE - Set your noise class for glyphs", "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>"}},
		{"SET PATHSAMPLES", []string{"SET PATHSAMPLES - Set your path sample floor", "Usage: SET PATHSAMPLES <count|DEFAULT>"}},
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
		{"SET/FT2", []string{"SET/<MODE> - Allow a mode", "Alias of PASS MODE <MODE>"}},
		{"SET/NOFT2", []string{"SET/NO<MODE> - Block a mode", "Alias of REJECT MODE <MODE>"}},
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

func TestWhoSpotsMeCommandFormatsPerContinentCounts(t *testing.T) {
	ctyDB := loadTestCTY(t)
	querier := &fakeWhoSpotsMeQuerier{
		window: 10 * time.Minute,
		counts: map[string]map[string]map[string][]spot.WhoSpotsMeCountryCount{
			"W1AW": {
				"20m": {
					"EU": []spot.WhoSpotsMeCountryCount{
						{ADIF: 230, Count: 42},
						{ADIF: 223, Count: 35},
						{ADIF: 227, Count: 22},
						{ADIF: 248, Count: 22},
						{ADIF: 281, Count: 18},
						{ADIF: 275, Count: 10},
					},
					"NA": []spot.WhoSpotsMeCountryCount{
						{ADIF: 291, Count: 57},
						{ADIF: 1, Count: 21},
						{ADIF: 50, Count: 8},
					},
				},
				"40m": {
					"AS": []spot.WhoSpotsMeCountryCount{
						{ADIF: 339, Count: 11},
					},
				},
			},
		},
	}
	p := NewProcessor(nil, nil, nil, func() *cty.CTYDatabase { return ctyDB }, nil, nil,
		WithWhoSpotsMe(querier),
		WithWhoSpotsMeHelp(WhoSpotsMeHelpConfig{
			Configured:    true,
			WindowMinutes: 10,
		}),
	)

	resp := p.ProcessCommandForClient("WHOSPOTSME 20M", "W1AW-7", "", nil, "go")
	if !strings.Contains(resp, "WHOSPOTSME 20M (last 10m):") {
		t.Fatalf("expected heading with window, got %q", resp)
	}
	if strings.Contains(resp, "(no data)") || strings.Contains(resp, "  AF:") {
		t.Fatalf("expected empty continents to be omitted, got %q", resp)
	}
	if !strings.Contains(resp, "  EU:  ADIF230(42) ADIF223(35) ADIF227(22) I(22) ADIF281(18)\n") {
		t.Fatalf("expected capped EU row, got %q", resp)
	}
	if !strings.Contains(resp, "  NA:  K(57) K1(21) ADIF50(8)\n") {
		t.Fatalf("expected NA row, got %q", resp)
	}

	resp = p.ProcessCommandForClient("WHOSPOTSME", "W1AW-7", "", nil, "go")
	if !strings.Contains(resp, "WHOSPOTSME (last 10m):\n") {
		t.Fatalf("expected all-band heading with window, got %q", resp)
	}
	forty := strings.Index(resp, "40M:\n")
	twenty := strings.Index(resp, "20M:\n")
	if forty < 0 || twenty < 0 || forty > twenty {
		t.Fatalf("expected populated bands in canonical order, got %q", resp)
	}
	if strings.Contains(resp, "30M:\n") || strings.Contains(resp, "  AF:") || strings.Contains(resp, "(no data)") {
		t.Fatalf("expected empty bands and continents to be omitted, got %q", resp)
	}
}

func TestWhoSpotsMeCommandUsageAndAvailability(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)

	resp := p.ProcessCommandForClient("WHOSPOTSME", "W1AW", "", nil, "go")
	if resp != "WHOSPOTSME is not available.\n" {
		t.Fatalf("expected unavailable response without store, got %q", resp)
	}

	resp = p.ProcessCommandForClient("WHOSPOTSME BADBAND", "W1AW", "", nil, "go")
	if resp != "Usage: WHOSPOTSME [band]\n" {
		t.Fatalf("expected usage for invalid band, got %q", resp)
	}

	resp = p.ProcessCommandForClient("WHOSPOTSME 20M EXTRA", "W1AW", "", nil, "go")
	if resp != "Usage: WHOSPOTSME [band]\n" {
		t.Fatalf("expected usage for too many arguments, got %q", resp)
	}

	resp = p.ProcessCommandForClient("WHOSPOTSME 20M", "W1AW", "", nil, "go")
	if resp != "WHOSPOTSME is not available.\n" {
		t.Fatalf("expected unavailable response without store, got %q", resp)
	}

	empty := NewProcessor(nil, nil, nil, nil, nil, nil,
		WithWhoSpotsMe(&fakeWhoSpotsMeQuerier{window: 10 * time.Minute}),
		WithWhoSpotsMeHelp(WhoSpotsMeHelpConfig{Configured: true, WindowMinutes: 10}),
	)
	resp = empty.ProcessCommandForClient("WHOSPOTSME 20M", "W1AW", "", nil, "go")
	if resp != "WHOSPOTSME 20M (last 10m): no data\n" {
		t.Fatalf("expected selected-band no-data response, got %q", resp)
	}
	resp = empty.ProcessCommandForClient("WHOSPOTSME", "W1AW", "", nil, "go")
	if resp != "WHOSPOTSME (last 10m): no data\n" {
		t.Fatalf("expected all-band no-data response, got %q", resp)
	}

	resp = empty.ProcessCommandForClient("WHOSPOTSME", "", "", nil, "go")
	if resp != noLoggedUserMsg {
		t.Fatalf("expected no logged user response, got %q", resp)
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
