package telnet

import (
	"strings"
	"sync"
	"testing"

	"dxcluster/archive"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestMinSNRHistorySnapshotAndDigest(t *testing.T) {
	c := &Client{callsign: "N0USER", filter: passRuleTestFilter(t, "MODE=CW AND SNR>=0 AND SOURCE=SKIMMER")}
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.HasReport, sp.Report = true, 0
	sp.IsHuman = false
	snapshot := c.captureHistoryFilter()
	if !snapshot.matches(nil, sp) {
		t.Fatal("exact threshold must pass history")
	}
	next := passRuleTestFilter(t, "MODE=CW AND SNR>=1 AND SOURCE=SKIMMER")
	c.updateFilter(func(f *filter.Filter) { *f = *next })
	if !snapshot.matches(nil, sp) || c.captureHistoryFilter().matches(nil, sp) || snapshot.digest == c.historyFilterDigest() {
		t.Fatal("snapshot aliases live threshold or digest omits it")
	}
	self := spot.NewSpot(c.callsign, "W1AAA", 14030, "CW")
	self.IsHuman, self.HasReport, self.Report = false, true, 0
	if !c.captureHistoryFilter().matches(nil, self) {
		t.Fatal("MINSNR changed history's self-match exception")
	}
}

func TestMinSNRHistoryCursorInvalidation(t *testing.T) {
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Before: []byte("p"), End: archive.HistoryBudgetReached}, nil
	})
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return nil }
	runHistory(t, s, c, "SHOW DX 1")
	token := c.historySearch.token
	newFilterCommandEngine().Handle(c, "PASS RULE 0 MODE=CW AND SNR>=10")
	if text := runHistory(t, s, c, "SHOW DX NEXT "+token); !strings.Contains(text, "Invalid history continuation") || c.historySearch != nil {
		t.Fatalf("old cursor accepted after threshold edit: %q", text)
	}
}

func TestMinSNRHistoryPendingPageInvalidation(t *testing.T) {
	entered, releaseScan := make(chan struct{}), make(chan struct{})
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.HasReport, sp.Report = true, 0
	sp.IsHuman = false
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		if !req.Match(sp) {
			t.Error("initial snapshot rejects fixture")
		}
		close(entered)
		<-releaseScan
		if !req.Match(sp) {
			t.Error("archive scan borrowed live threshold")
		}
		return archive.HistoryPage{Spots: []*spot.Spot{sp}, Before: []byte("p"), End: archive.HistoryCountReached}, nil
	})
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return nil }
	done := make(chan bool, 1)
	go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX K1ABC 1"); done <- proceed }()
	<-entered
	newFilterCommandEngine().Handle(c, "PASS RULE 0 MODE=CW AND SNR>=1")
	close(releaseScan)
	if !<-done {
		t.Fatal("history restart could not be queued")
	}
	if response := <-c.controlChan; response.line != historyRestart || c.historySearch != nil {
		t.Fatalf("invalidated page published: %q", response.line)
	}
}

func TestMinSNRConcurrentMutation(t *testing.T) {
	first, second := passRuleTestFilter(t, "MODE=CW AND SNR>=0 AND SOURCE=SKIMMER"), passRuleTestFilter(t, "MODE=CW AND SNR>=1 AND SOURCE=SKIMMER")
	c := &Client{callsign: "N0USER", filter: first}
	states := [2]filter.Filter{*first, *second}
	sp := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	sp.HasReport, sp.Report = true, 0
	sp.IsHuman = false
	initial := c.captureHistoryFilter()
	var group sync.WaitGroup
	group.Go(func() {
		for i := range 500 {
			c.updateFilter(func(f *filter.Filter) { *f = states[i%2] })
		}
	})
	group.Go(func() {
		for range 500 {
			c.captureHistoryFilter().matches(nil, sp)
			c.filterMu.RLock()
			c.filter.Matches(sp)
			c.filterMu.RUnlock()
			if !initial.matches(nil, sp) {
				t.Error("held snapshot changed during mutation")
			}
		}
	})
	group.Wait()
}
