package telnet

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"

	"dxcluster/archive"
	"dxcluster/filter"
	"dxcluster/spot"
)

const (
	cursorFresh byte = iota
	cursorNext
	cursorFreshFailure
	cursorNextFailure
	cursorSettings
	cursorSettingsRestore
	cursorPresentation
	cursorInvalid
	cursorClose
	cursorReconnect
	cursorFreshSettingsDuringRead
	cursorNextSettingsDuringRead
	cursorFreshReplacementDuringRead
	cursorNextReplacementDuringRead
	cursorCloseDuringRead
	cursorStaleToken
	cursorQueueFull
	cursorOperationCount
)

// The model follows v4's observable contract, not production generations or
// cursor transitions. Random handles are learned from accepted output only;
// expected positions, selection, counts and warning flags come from the script.
// Each connection owns a bounded control queue; programs have at most 128
// operations. No network, disk, goroutine, clock-sensitive retention, or
// background worker is involved.
type cursorModel struct {
	active, closed, blocked bool
	token, retired          string
	before                  []byte
	selector                string
	count                   int
	unreadable, hadMatches  bool
}

type cursorReadPlan struct {
	selector string
	count    int
	before   []byte
	blocked  bool
	page     archive.HistoryPage
	fault    error
	during   byte
}

type cursorFuzzSession struct {
	t      *testing.T
	server *Server
	client *Client
	model  cursorModel
	plan   *cursorReadPlan
	reads  int
	step   int
}

func FuzzHistoryCursorSequences(f *testing.F) {
	for _, seed := range [][]byte{
		{cursorFresh, 0, cursorNext, 16, cursorNext, 2},
		{cursorFresh, 144, cursorNextFailure, 0, cursorNextFailure, 1, cursorNext, 16, cursorStaleToken, 0},
		{cursorFresh, 0, cursorNextFailure, 144, cursorNextFailure, 145, cursorNext, 1, cursorStaleToken, 0},
		{cursorFresh, 0, cursorFreshFailure, 0, cursorStaleToken, 0, cursorFresh, 0},
		{cursorFresh, 0, cursorSettingsRestore, 0, cursorStaleToken, 0, cursorFresh, 0, cursorPresentation, 0, cursorNext, 0},
		{cursorFresh, 0, cursorNextSettingsDuringRead, 0, cursorFreshSettingsDuringRead, 0},
		{cursorFresh, 0, cursorNextReplacementDuringRead, 0, cursorFreshReplacementDuringRead, 0},
		{cursorFresh, 0, cursorCloseDuringRead, 0, cursorNext, 0, cursorReconnect, 0, cursorStaleToken, 0, cursorFresh, 0},
		{cursorFresh, 0, cursorQueueFull, 0, cursorReconnect, 0, cursorFresh, 0, cursorClose, 0, cursorFreshFailure, 0},
		{cursorFresh, 0, cursorInvalid, 0, cursorInvalid, 1, cursorInvalid, 2, cursorInvalid, 3, cursorNext, 1},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 256 {
			t.Skip()
		}
		session := &cursorFuzzSession{t: t}
		session.server, session.client = historyTestSession(session.read)
		// The fixture has no optional connection reporters. Queue-full policy
		// still runs the real close path against this connection's cursor.
		session.client.server = nil
		t.Cleanup(func() { session.client.interrupt() })
		for i := 0; i+1 < len(program); i += 2 {
			session.step = i / 2
			session.apply(program[i]%cursorOperationCount, program[i+1])
			session.assertState()
		}
	})
}

func (s *cursorFuzzSession) retire() {
	if s.model.active {
		s.model.retired = s.model.token
	}
	s.model.active = false
}

func (s *cursorFuzzSession) settings(restore bool) {
	if s.model.closed {
		return
	}
	s.retire()
	toggle := func() {
		s.model.blocked = !s.model.blocked
		s.server.runPreferenceCommand(s.client, "fixture", func(c *Client, _ string) (string, bool) {
			c.filterMu.Lock()
			if s.model.blocked {
				c.filter.BlockBands["20m"] = true
			} else {
				delete(c.filter.BlockBands, "20m")
			}
			c.filterMu.Unlock()
			return "", true
		})
	}
	toggle()
	if restore {
		toggle()
	}
}

func (s *cursorFuzzSession) apply(operation, argument byte) {
	s.plan = nil
	switch operation {
	case cursorSettings, cursorSettingsRestore:
		s.settings(operation == cursorSettingsRestore)
	case cursorPresentation:
		if !s.model.closed {
			s.server.runPreferenceCommand(s.client, "fixture", func(c *Client, _ string) (string, bool) {
				c.pathMu.Lock()
				if c.dialect == DialectGo {
					c.dialect = DialectCC
				} else {
					c.dialect = DialectGo
				}
				c.configuredSettings.Dialect = string(c.dialect)
				c.pathMu.Unlock()
				return "", true
			})
		}
	case cursorClose:
		s.client.interrupt()
		s.retire()
		s.model.closed = true
	case cursorReconnect:
		s.client.interrupt()
		s.retire()
		retired := s.model.retired
		s.client = &Client{callsign: "N2WQ", dialect: DialectGo, filter: filter.NewFilter(), done: make(chan struct{}), controlChan: make(chan controlMessage, 8)}
		s.server.clients[s.client.callsign] = s.client
		s.model = cursorModel{retired: retired}
	case cursorInvalid, cursorStaleToken:
		line := "SHOW DX NEXT " + s.model.retired
		if s.model.retired == "" {
			line = "SHOW DX NEXT H1" + strings.Repeat("F", 32)
		}
		if operation == cursorInvalid {
			line = []string{"SHOW DX 0", "SHOW MYDX NEXT H1BAD", "SHOW DX K$", "SHOW MYDX NEXT"}[int(argument)%4]
		}
		s.reject(line)
	default:
		s.query(operation, argument)
	}
}

func cursorQuerySpec(argument byte) (string, int) {
	return []string{"K1ABC", "W6/LZ5VV", "K", ""}[int(argument>>5)%4], []int{1, 2, 20, 50, 250}[int(argument>>2)%5]
}

func (s *cursorFuzzSession) query(operation, argument byte) {
	older := operation == cursorNext || operation == cursorNextFailure || operation == cursorNextSettingsDuringRead || operation == cursorNextReplacementDuringRead
	if operation == cursorCloseDuringRead || operation == cursorQueueFull {
		older = s.model.active
	}
	selector, count := cursorQuerySpec(argument)
	prefix := []string{"SHOW DX", "SHOW MYDX", "SH DX", "SH MYDX"}[int(argument>>4)%4]
	line := prefix + " " + selector + " " + strconv.Itoa(count)
	if argument&8 != 0 {
		line = prefix + " " + strconv.Itoa(count) + " " + selector
	}
	if older {
		line = prefix + " NEXT " + s.model.token
		if !s.model.active {
			s.reject("SHOW MYDX NEXT H1" + strings.Repeat("F", 32))
			return
		}
		selector, count = s.model.selector, s.model.count
	}
	if s.model.closed {
		s.reject(line)
		return
	}
	plan := &cursorReadPlan{selector: selector, count: count, blocked: s.model.blocked}
	if older {
		plan.before = append([]byte(nil), s.model.before...)
	} else {
		s.retire()
		s.model.unreadable, s.model.hadMatches = false, false
	}
	plan.page = s.page(plan, argument)
	if operation == cursorFreshFailure || operation == cursorNextFailure {
		plan.fault = errors.New("injected history read failure")
		if argument&1 != 0 {
			plan.fault = context.Canceled
		}
	}
	plan.during = operation
	s.plan = plan
	priorToken, priorReads := s.model.token, s.reads
	handled, proceed := s.server.handleHistoryCommand(s.client, line)
	if !handled || s.reads != priorReads+1 {
		s.t.Fatalf("step %d: valid query did not read once", s.step)
	}
	s.checkPublication(plan, older, priorToken, proceed)
	s.plan = nil
}

func (s *cursorFuzzSession) page(plan *cursorReadPlan, argument byte) archive.HistoryPage {
	page := archive.HistoryPage{End: archive.HistoryBudgetReached}
	if argument&3 == 1 && !plan.blocked {
		page.End = archive.HistoryCountReached
	}
	if argument&3 == 2 {
		page.End = archive.HistoryExhausted
	}
	if argument&0x80 != 0 {
		page.Unreadable = 1
	}
	if page.End != archive.HistoryExhausted {
		page.Before = make([]byte, 14)
		copy(page.Before, "s|")
		binary.BigEndian.PutUint64(page.Before[2:], uint64(1000000-s.step))
		binary.BigEndian.PutUint32(page.Before[10:], uint32(s.step))
	}
	if !plan.blocked && (argument&0x10 != 0 || page.End == archive.HistoryCountReached) {
		call := plan.selector
		if call == "" || call == "K" {
			call = "K1ABC"
		}
		row := spot.NewSpot(call, "W1AAA", 14030, "CW")
		row.DXMetadata.ADIF = 291
		rows := 1
		if page.End == archive.HistoryCountReached {
			rows = plan.count
		}
		for range rows {
			page.Spots = append(page.Spots, row)
		}
	}
	return page
}

// The reader validates subsequent requests against model-owned position/count
// and distinct same-ADIF identities. Checking before and after the interleaving
// also detects mutable rules escaping into the request predicate.
func (s *cursorFuzzSession) read(req archive.HistoryRequest) (archive.HistoryPage, error) {
	s.reads++
	plan := s.plan
	if plan == nil || req.Limit != plan.count || !bytes.Equal(req.Before, plan.before) {
		s.t.Fatalf("step %d: unexpected position/count/read", s.step)
	}
	check := func() {
		for _, candidate := range []struct {
			call string
			adif int
		}{{"K1ABC", 291}, {"K1ABCD", 291}, {"W6/LZ5VV", 291}, {"VE3ABC", 1}} {
			row := spot.NewSpot(candidate.call, "W1AAA", 14030, "CW")
			row.DXMetadata.ADIF = candidate.adif
			want := !plan.blocked && (plan.selector == "" || (plan.selector == "K" && candidate.adif == 291) || plan.selector == candidate.call)
			if req.Match(row) != want {
				s.t.Fatalf("step %d: wrong query or mutable snapshot for %s", s.step, candidate.call)
			}
		}
	}
	check()
	switch plan.during {
	case cursorFreshSettingsDuringRead, cursorNextSettingsDuringRead:
		s.settings(true)
	case cursorFreshReplacementDuringRead, cursorNextReplacementDuringRead:
		_, text, err := s.server.beginHistory(s.client, "SHOW DX W6/LZ5VV 2")
		if text != "" || err != nil {
			s.t.Fatalf("replacement failed: %q %v", text, err)
		}
		s.retire()
	case cursorCloseDuringRead:
		s.client.interrupt()
		s.retire()
		s.model.closed = true
	case cursorQueueFull:
		for range cap(s.client.controlChan) {
			s.client.controlChan <- controlMessage{line: "occupied"}
		}
	}
	check()
	return plan.page, plan.fault
}

func (s *cursorFuzzSession) checkPublication(plan *cursorReadPlan, older bool, priorToken string, proceed bool) {
	if plan.during == cursorQueueFull {
		if proceed || len(s.client.controlChan) != cap(s.client.controlChan) {
			s.t.Fatal("queue rejection accepted page")
		}
		for range cap(s.client.controlChan) {
			if (<-s.client.controlChan).line != "occupied" {
				s.t.Fatal("failed enqueue published results")
			}
		}
		s.retire()
		s.model.closed = true
		return
	}
	if s.model.closed {
		if proceed || len(s.client.controlChan) != 0 {
			s.t.Fatal("closed connection delivered page")
		}
		return
	}
	if !proceed || len(s.client.controlChan) != 1 {
		s.t.Fatal("open request did not publish exactly one response")
	}
	response := (<-s.client.controlChan).line
	switch plan.during {
	case cursorFreshSettingsDuringRead, cursorNextSettingsDuringRead, cursorFreshReplacementDuringRead, cursorNextReplacementDuringRead:
		if response != historyRestart {
			s.t.Fatalf("invalidated page delivered: %q", response)
		}
		return
	}
	if plan.fault != nil {
		if response != historyFailure {
			s.t.Fatalf("read failure delivered results: %q", response)
		}
		return
	}
	s.model.unreadable = s.model.unreadable || plan.page.Unreadable > 0
	s.model.hadMatches = s.model.hadMatches || len(plan.page.Spots) > 0
	if strings.Contains(response, "Warning:") != s.model.unreadable || strings.Count(response, "DX de ") != len(plan.page.Spots) {
		s.t.Fatalf("results/warnings disagree with script: %q", response)
	}
	if older != strings.HasPrefix(response, "Older retained history page:\n") {
		s.t.Fatal("older page label incorrect")
	}
	if plan.page.End == archive.HistoryExhausted {
		terminal := "End of retained history search.\n"
		if !s.model.unreadable && !older && !s.model.hadMatches {
			terminal = "No matching retained spots.\n"
		}
		if strings.Contains(response, "NEXT") || !strings.HasSuffix(response, terminal) {
			s.t.Fatalf("exhausted page omitted completion or retained continuation: %q", response)
		}
		if (s.model.unreadable || older || s.model.hadMatches) && strings.Contains(response, "No matching") {
			s.t.Fatal("false definitive no-match")
		}
		s.retire()
		return
	}
	if plan.page.End == archive.HistoryBudgetReached && (!strings.Contains(response, "incomplete") || strings.Contains(response, "No matching")) {
		s.t.Fatal("budget stop presented as complete")
	}
	_, suffix, found := strings.Cut(response, "Continue older history: SHOW DX NEXT ")
	if !found {
		s.t.Fatal("incomplete search omitted continuation")
	}
	token := strings.TrimSpace(suffix)
	if len(token) != 34 || token[:2] != "H1" || token != strings.ToUpper(token) {
		s.t.Fatalf("bad published token: %q", token)
	}
	if decoded, err := hex.DecodeString(token[2:]); err != nil || len(decoded) != 16 {
		s.t.Fatal("bad token entropy encoding")
	}
	if older && token == priorToken {
		s.t.Fatal("successful continuation did not rotate")
	}
	s.retire()
	s.model.active, s.model.token = true, token
	s.model.before = append([]byte(nil), plan.page.Before...)
	s.model.selector, s.model.count = plan.selector, plan.count
}

func (s *cursorFuzzSession) reject(line string) {
	priorReads := s.reads
	handled, proceed := s.server.handleHistoryCommand(s.client, line)
	if !handled || s.reads != priorReads {
		s.t.Fatal("invalid/closed command reached archive")
	}
	if s.model.closed {
		if proceed || len(s.client.controlChan) != 0 {
			s.t.Fatal("closed command published response")
		}
		return
	}
	if !proceed || len(s.client.controlChan) != 1 {
		s.t.Fatal("invalid command response missing")
	}
	response := (<-s.client.controlChan).line
	if strings.Contains(response, "DX de ") || strings.Contains(response, "Continue older") {
		s.t.Fatal("invalid command delivered page")
	}
}

func (s *cursorFuzzSession) assertState() {
	s.client.historyMu.Lock()
	defer s.client.historyMu.Unlock()
	state := s.client.historySearch
	if (state != nil) != s.model.active || s.client.historyClosed != s.model.closed {
		s.t.Fatalf("step %d: cursor ownership differs from model", s.step)
	}
	if state != nil && (state.token != s.model.token || !bytes.Equal(state.before, s.model.before) || state.unreadable != s.model.unreadable || state.hadMatches != s.model.hadMatches) {
		s.t.Fatalf("step %d: failed request advanced state or successful page lost state", s.step)
	}
	if len(s.client.controlChan) != 0 {
		s.t.Fatal("unaccounted output remains")
	}
}
