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
	c := &Client{callsign: "N0USER", filter: filter.NewFilter()}
	c.filter.Comments = []string{"POTA"}
	c.filter.BlockComments = []string{"QRT"}
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.Comment = "POTA up 5"
	snapshot := c.captureHistoryFilter()
	if !snapshot.matches(nil, sp) {
		t.Fatal("initial comment fixture rejected")
	}
	c.updateFilter(func(f *filter.Filter) { f.Comments[0], f.BlockComments[0] = "SOTA", "up 5" })
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
	c.filter.BlockComments = []string{"POTA"}
	if text := runHistory(t, s, c, "SHOW DX 1 COMMENT pota:  up 5!"); !strings.Contains(text, self.FormatDXCluster()) || c.historySearch == nil {
		t.Fatalf("first page failed: %q", text)
	}
	token := c.historySearch.token
	if text := runHistory(t, s, c, "SHOW MYDX NEXT "+token); !strings.Contains(text, self.FormatDXCluster()) || c.historySearch != nil || page != 2 {
		t.Fatalf("continued selection changed: %q", text)
	}
}

func TestHistoryCommentChangesInvalidateCursor(t *testing.T) {
	for _, change := range []func(*filter.Filter){
		func(f *filter.Filter) { f.Comments = []string{"POTA"} },
		func(f *filter.Filter) { f.BlockComments = []string{"QRT"} },
	} {
		s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
			return archive.HistoryPage{Before: []byte("p"), End: archive.HistoryBudgetReached}, nil
		})
		runHistory(t, s, c, "SHOW DX COMMENT POTA")
		token := c.historySearch.token
		s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
			c.updateFilter(change)
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
	c.filter.Comments = []string{"POTA"}
	done := make(chan bool, 1)
	go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX 1 COMMENT POTA"); done <- proceed }()
	<-entered
	for _, phrase := range []string{"SOTA", "POTA"} {
		s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
			c.updateFilter(func(f *filter.Filter) { f.Comments[0] = phrase })
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
	c := &Client{callsign: "N0USER", filter: filter.NewFilter()}
	c.filter.Comments, c.filter.BlockComments = []string{"POTA"}, []string{"QRT"}
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.Comment = "POTA"
	initial := c.captureHistoryFilter()
	var group sync.WaitGroup
	group.Go(func() {
		for i := range 500 {
			c.updateFilter(func(f *filter.Filter) {
				f.Comments[0] = []string{"POTA", "SOTA"}[i%2]
				f.BlockComments[0] = []string{"QRT", "POTA"}[i%2]
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
