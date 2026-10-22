package telnet

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"dxcluster/archive"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHistoryCommentSnapshotAndDigest(t *testing.T) {
	c := &Client{callsign: "N0USER", filter: testRuleFilter(t,
		filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "COMMENT=POTA"},
		filter.RuleDefinition{Slot: 1, Action: filter.RuleReject, Expr: "COMMENT=QRT"})}
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.Comment = "POTA up 5"
	snapshot := c.captureHistoryFilter()
	if !snapshot.matches(nil, sp) {
		t.Fatal("initial comment fixture rejected")
	}
	next := testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "COMMENT=SOTA"},
		filter.RuleDefinition{Slot: 1, Action: filter.RuleReject, Expr: `COMMENT="up 5"`})
	c.updateFilter(func(f *filter.Filter) { *f = *next })
	if !snapshot.matches(nil, sp) || c.captureHistoryFilter().matches(nil, sp) || snapshot.digest == c.historyFilterDigest() {
		t.Fatal("snapshot borrows comment rules or digest omits them")
	}
	self := spot.NewSpot(c.callsign, "W1AAA", 14030, "CW")
	self.Comment = "POTA up 5"
	if !c.captureHistoryFilter().matches(nil, self) {
		t.Fatal("saved comment rules changed history self exception")
	}
}

func TestHistoryCommentRequiredForSelfAndRetainedByNext(t *testing.T) {
	self := spot.NewSpot("N2WQ", "W1AAA", 14030, "CW")
	self.Comment = "POTA:  UP 5!"
	wrong := spot.NewSpot("N2WQ", "W1AAA", 14031, "CW")
	wrong.Comment = "POTA: UP 5!"
	position := []byte("comment-position")
	page := 0
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		page++
		if req.Match(wrong) || !req.Match(self) {
			t.Error("literal query was lost, changed spacing or bypassed by self exception")
		}
		if page == 1 {
			return archive.HistoryPage{Spots: []*spot.Spot{self}, Before: position, End: archive.HistoryCountReached}, nil
		}
		if !bytes.Equal(req.Before, position) {
			t.Errorf("NEXT lost archive position: %q", req.Before)
		}
		return archive.HistoryPage{Spots: []*spot.Spot{self}, End: archive.HistoryExhausted}, nil
	})
	// Saved rules exclude the matching self-spot but retain their existing
	// exception. The explicit query remains mandatory on each continued page.
	c.filter = testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RuleReject, Expr: "COMMENT=POTA"})
	if text := runHistory(t, s, c, "SHOW DX 1 COMMENT pota:  up 5!"); !strings.Contains(text, self.FormatDXCluster()) || c.historySearch == nil {
		t.Fatalf("first page failed: %q", text)
	}
	token := c.historySearch.token
	if text := runHistory(t, s, c, "SHOW MYDX NEXT "+token); !strings.Contains(text, self.FormatDXCluster()) || c.historySearch != nil || page != 2 {
		t.Fatalf("continued selection changed: %q", text)
	}
}

func TestHistoryCommentChangesInvalidateCursor(t *testing.T) {
	for _, definition := range []filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: "COMMENT=POTA"},
		{Slot: 0, Action: filter.RuleReject, Expr: "COMMENT=QRT"},
	} {
		s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
			return archive.HistoryPage{Before: []byte("p"), End: archive.HistoryBudgetReached}, nil
		})
		runHistory(t, s, c, "SHOW DX COMMENT POTA")
		token := c.historySearch.token
		next := testRuleFilter(t, definition)
		s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
			c.updateFilter(func(f *filter.Filter) { *f = *next })
			return "", true
		})
		if text := runHistory(t, s, c, "SHOW DX NEXT "+token); !strings.Contains(text, "Invalid history continuation") || c.historySearch != nil {
			t.Fatalf("comment mutation retained cursor: %q", text)
		}
	}
}

func TestHistoryCommentPendingPageInvalidatedAfterRestore(t *testing.T) {
	entered, releaseScan := make(chan struct{}), make(chan struct{})
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.Comment = "POTA"
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		if !req.Match(sp) {
			t.Error("initial fixture rejected")
		}
		close(entered)
		<-releaseScan
		if !req.Match(sp) {
			t.Error("scan borrowed comment backing slice")
		}
		return archive.HistoryPage{Spots: []*spot.Spot{sp}, Before: []byte("p"), End: archive.HistoryCountReached}, nil
	})
	c.filter = passRuleTestFilter(t, "COMMENT=POTA")
	done := make(chan bool, 1)
	go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX 1 COMMENT POTA"); done <- proceed }()
	<-entered
	for _, phrase := range []string{"SOTA", "POTA"} {
		next := passRuleTestFilter(t, "COMMENT="+phrase)
		s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
			c.updateFilter(func(f *filter.Filter) { *f = *next })
			return "", true
		})
	}
	close(releaseScan)
	if !<-done {
		t.Fatal("restart response could not be published")
	}
	if response := <-c.controlChan; response.line != historyRestart || c.historySearch != nil {
		t.Fatalf("stale page published after restoration: %q", response.line)
	}
}

func TestHistoryCommentConcurrentSnapshotIsolation(t *testing.T) {
	first := testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "COMMENT=POTA"},
		filter.RuleDefinition{Slot: 1, Action: filter.RuleReject, Expr: "COMMENT=QRT"})
	second := testRuleFilter(t, filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "COMMENT=SOTA"},
		filter.RuleDefinition{Slot: 1, Action: filter.RuleReject, Expr: "COMMENT=POTA"})
	c := &Client{callsign: "N0USER", filter: first}
	states := [2]filter.Filter{*first, *second}
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.Comment = "POTA"
	initial := c.captureHistoryFilter()
	var group sync.WaitGroup
	group.Go(func() {
		for i := range 500 {
			c.updateFilter(func(f *filter.Filter) {
				*f = states[i%2]
			})
		}
	})
	group.Go(func() {
		for range 500 {
			c.captureHistoryFilter().matches(nil, sp)
			if !initial.matches(nil, sp) {
				t.Error("held snapshot changed during comment mutation")
			}
		}
	})
	group.Wait()
}
