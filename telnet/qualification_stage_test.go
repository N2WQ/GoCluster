//go:build qualification

package telnet

import (
	"testing"

	"dxcluster/internal/qualificationstage"
	"dxcluster/spot"
)

func TestQualificationStageMarkers(t *testing.T) {
	s := &Server{broadcastWorkers: 2, clients: map[string]*Client{"A": {peerSessionID: 11}, "B": {peerSessionID: 22}}, broadcast: make(chan *broadcastPayload, 1)}
	s.shardsDirty.Store(true)
	s.workerQueues = []chan broadcastJob{make(chan broadcastJob, 1), make(chan broadcastJob, 1)}
	fanout, err := s.QualificationStageFanout()
	if err != nil || fanout.Workers != 2 || fanout.Count != 2 || fanout.Mask != 3 {
		t.Fatalf("fanout=%+v err=%v", fanout, err)
	}
	var stages []qualificationstage.Event
	r, ok := qualificationstage.Install(func(event qualificationstage.Event) { stages = append(stages, event) })
	if !ok {
		t.Fatal("observer occupied")
	}
	defer qualificationstage.Remove(r)
	sp := spot.NewSpot("K1ABC", "DL1AAA", 14020, "CW")
	sp.Comment = "QID0000000"
	s.BroadcastSpotOwned(sp, true, true, true)
	s.broadcastSpot(<-s.broadcast)
	if len(stages) != 4 || stages[0].Stage != qualificationstage.BroadcastReady || stages[1].Stage != qualificationstage.BroadcastReceived {
		t.Fatal(stages)
	}
	for worker := range 2 {
		if stages[worker+2].Stage != qualificationstage.WorkerDispatched || stages[worker+2].Worker != worker {
			t.Fatal(stages)
		}
		if len(s.workerQueues[worker]) != 1 {
			t.Fatal("marker changed dispatch")
		}
	}
}
