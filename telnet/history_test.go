package telnet

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

type historyReaderFunc func(archive.HistoryRequest) (archive.HistoryPage, error)

func (f historyReaderFunc) ReadHistoryPage(req archive.HistoryRequest) (archive.HistoryPage, error) {
	return f(req)
}

func historyTestSession(reader historyReaderFunc) (*Server, *Client) {
	s := &Server{clients: make(map[string]*Client), shutdown: make(chan struct{})}
	s.processor = commands.NewProcessor(nil, reader, nil, func() *cty.CTYDatabase { return canonicalTestCTY() }, nil, nil)
	c := &Client{server: s, callsign: "N2WQ", dialect: DialectGo, filter: filter.NewFilter(), done: make(chan struct{}), controlChan: make(chan controlMessage, 8)}
	s.clients[c.callsign] = c
	return s, c
}

func runHistory(t *testing.T, s *Server, c *Client, line string) string {
	t.Helper()
	handled, proceed := s.handleHistoryCommand(c, line)
	if !handled || !proceed {
		t.Fatalf("history command failed: %q, handled=%v proceed=%v", line, handled, proceed)
	}
	select {
	case response := <-c.controlChan:
		return response.line
	default:
		t.Fatal("no published response")
		return ""
	}
}

func TestHistoryContinuationFailuresAndFreshReplacement(t *testing.T) {
	position := []byte("saved-position")
	var seen [][]byte
	var fault error
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		seen = append(seen, append([]byte(nil), req.Before...))
		return archive.HistoryPage{Before: position, End: archive.HistoryBudgetReached}, fault
	})
	first := runHistory(t, s, c, "SHOW DX K1ABC 20")
	if !strings.Contains(first, "incomplete") || strings.Contains(first, "No matching") || c.historySearch == nil {
		t.Fatal(first)
	}
	token := c.historySearch.token
	for _, bad := range []string{"SHOW DX NEXT", "SHOW DX NEXT H1BAD", "SHOW DX 0", "SHOW DX BAD!", "SHOW DX NEXT H1" + strings.Repeat("A", 32)} {
		runHistory(t, s, c, bad)
		if c.historySearch == nil || c.historySearch.token != token {
			t.Fatalf("invalid input replaced search: %s", bad)
		}
	}
	for _, err := range []error{errors.New("injected read failure"), context.Canceled} {
		fault = err
		text := runHistory(t, s, c, "SHOW MYDX NEXT "+token)
		if text != historyFailure || c.historySearch.token != token || !bytes.Equal(c.historySearch.before, position) {
			t.Fatalf("failure advanced search: %q %+v", text, c.historySearch)
		}
	}
	fault = nil
	runHistory(t, s, c, "SH DX NEXT "+token)
	if c.historySearch.token == token || !bytes.Equal(seen[len(seen)-1], position) {
		t.Fatal("successful retry did not rotate same position")
	}
	oldToken := c.historySearch.token
	fault = errors.New("fresh failure")
	if text := runHistory(t, s, c, "SHOW DX W6/LZ5VV"); text != historyFailure || c.historySearch != nil {
		t.Fatalf("fresh failure restored old search: %q", text)
	}
	if text := runHistory(t, s, c, "SHOW DX NEXT "+oldToken); !strings.Contains(text, "Invalid history continuation") {
		t.Fatal(text)
	}
}

func TestHistoryResponseDiscardedAfterSettingsChange(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{false: "changed", true: "changed and restored"}[restore], func(t *testing.T) {
			entered, releaseScan := make(chan struct{}), make(chan struct{})
			sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
			s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
				if !req.Match(sp) {
					t.Error("initial snapshot rejects fixture")
				}
				close(entered)
				<-releaseScan
				if !req.Match(sp) {
					t.Error("scan borrowed mutable settings")
				}
				return archive.HistoryPage{Spots: []*spot.Spot{sp}, Before: []byte("position"), End: archive.HistoryCountReached}, nil
			})
			done := make(chan bool, 1)
			go func() { _, ok := s.handleHistoryCommand(c, "SHOW DX K1ABC 1"); done <- ok }()
			<-entered
			_, handled := s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
				c.filterMu.Lock()
				c.filter.BlockBands["20m"] = true
				c.filterMu.Unlock()
				return "", true
			})
			if !handled {
				t.Fatal("mutation not executed")
			}
			if restore {
				s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
					c.filterMu.Lock()
					delete(c.filter.BlockBands, "20m")
					c.filterMu.Unlock()
					return "", true
				})
			}
			close(releaseScan)
			if !<-done {
				t.Fatal("restart response failed")
			}
			response := <-c.controlChan
			if response.line != historyRestart || c.historySearch != nil || len(c.controlChan) != 0 {
				t.Fatalf("invalidated page published: %q %+v", response.line, c.historySearch)
			}
		})
	}
}

func TestHistoryPublicationGenerationAndClose(t *testing.T) {
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) { return archive.HistoryPage{}, nil })
	request, errText, err := s.beginHistory(c, "SHOW DX 1")
	if err != nil || errText != "" {
		t.Fatalf("begin: %q %v", errText, err)
	}
	// This is the barrier immediately before publication; a new valid search
	// must invalidate results as well as the old cursor.
	if _, text, err := s.beginHistory(c, "SHOW DX 2"); err != nil || text != "" {
		t.Fatalf("replace: %q %v", text, err)
	}
	if !s.publishHistory(c, request, &historySearch{token: "stale"}, "K1ABC results\n", true) {
		t.Fatal("restart publication failed")
	}
	if msg := <-c.controlChan; msg.line != historyRestart || c.historySearch != nil {
		t.Fatalf("stale generation delivered: %+v", msg)
	}
	current, _, _ := s.beginHistory(c, "SHOW DX 1")
	c.interrupt()
	if s.publishHistory(c, current, &historySearch{token: "resurrected"}, "K1ABC\n", true) || c.historySearch != nil || len(c.controlChan) != 0 {
		t.Fatal("closed session resurrected")
	}
}

func TestHistoryOnlyOneConcurrentAdvance(t *testing.T) {
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Before: []byte("p"), End: archive.HistoryBudgetReached}, nil
	})
	runHistory(t, s, c, "SHOW DX 1")
	token := c.historySearch.token
	a, _, err := s.beginHistory(c, "SHOW DX NEXT "+token)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := s.beginHistory(c, "SHOW DX NEXT "+token)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i, req := range []historyRequest{a, b} {
		wg.Go(func() {
			s.publishHistory(c, req, &historySearch{token: strings.Repeat(string(rune('A'+i)), 34)}, "accepted\n", true)
		})
	}
	wg.Wait()
	one, two := <-c.controlChan, <-c.controlChan
	counts := map[string]int{one.line: 1}
	counts[two.line]++
	if counts["accepted\n"] != 1 || counts[historyRestart] != 1 {
		t.Fatalf("two advances: %q %q", one.line, two.line)
	}
}

func TestHistoryQueueFullClosesOutsideGuard(t *testing.T) {
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) { return archive.HistoryPage{}, nil })
	// A nil server suppresses optional close reporting; the publication server
	// still coordinates the request and preserves the existing queue policy.
	c.server = nil
	c.controlChan = make(chan controlMessage, 1)
	c.controlChan <- controlMessage{line: "occupied"}
	req, _, err := s.beginHistory(c, "SHOW DX 1")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() { done <- s.publishHistory(c, req, &historySearch{token: "unpublished"}, "results", true) }()
	select {
	case ok := <-done:
		if ok || c.historySearch != nil || !c.historyClosed {
			t.Fatal("failed enqueue advanced or failed to close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queue overflow deadlocked history guard")
	}
}

func TestHistoryWarningAndCursorBound(t *testing.T) {
	page := 0
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		page++
		if page%2 == 0 {
			return archive.HistoryPage{End: archive.HistoryExhausted}, nil
		}
		return archive.HistoryPage{Before: []byte("p"), Unreadable: 1, End: archive.HistoryBudgetReached}, nil
	})
	for range 100 {
		text := runHistory(t, s, c, "SHOW DX 1")
		if !strings.Contains(text, "Warning") || c.historySearch == nil {
			t.Fatal(text)
		}
		token := c.historySearch.token
		if !commands.ValidHistoryToken(token) || len("SHOW MYDX NEXT "+token) > 128 {
			t.Fatal("invalid or oversized token")
		}
		text = runHistory(t, s, c, "SHOW DX NEXT "+token)
		if !strings.Contains(text, "Warning") || strings.Contains(text, "No matching") || c.historySearch != nil {
			t.Fatalf("warning lost or exhausted state retained: %q", text)
		}
	}
}

func TestHistoryTokensBelongToConnection(t *testing.T) {
	s, first := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Before: []byte("position"), End: archive.HistoryBudgetReached}, nil
	})
	runHistory(t, s, first, "SHOW DX 1")
	token := first.historySearch.token
	other := &Client{server: s, callsign: "N3WQ", filter: filter.NewFilter(), done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
	s.clients[other.callsign] = other
	if text := runHistory(t, s, other, "SHOW DX NEXT "+token); !strings.Contains(text, "Invalid history continuation") || other.historySearch != nil || first.historySearch.token != token {
		t.Fatalf("token crossed connection: %q", text)
	}
	first.interrupt()
	reconnected := &Client{server: s, callsign: first.callsign, filter: filter.NewFilter(), done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
	s.clients[first.callsign] = reconnected
	if text := runHistory(t, s, reconnected, "SHOW DX NEXT "+token); !strings.Contains(text, "Invalid history continuation") || reconnected.historySearch != nil {
		t.Fatalf("token survived reconnect: %q", text)
	}
}

func TestHistorySettingsChangeImmediatelyBeforePublication(t *testing.T) {
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) { return archive.HistoryPage{}, nil })
	req, text, err := s.beginHistory(c, "SHOW DX 1")
	if err != nil || text != "" {
		t.Fatalf("begin: %q %v", text, err)
	}
	s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
		c.filterMu.Lock()
		c.filter.BlockBands["20m"] = true
		c.filterMu.Unlock()
		return "", true
	})
	if !s.publishHistory(c, req, &historySearch{token: "unpublished"}, "K1ABC results\n", true) {
		t.Fatal("restart not delivered")
	}
	if response := <-c.controlChan; response.line != historyRestart || c.historySearch != nil || len(c.controlChan) != 0 {
		t.Fatalf("stale page enqueued: %+v", response)
	}
}
