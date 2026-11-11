package telnet

import (
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"dxcluster/spot"
)

func TestClientShardAffinitySurvivesChurn(t *testing.T) {
	s := NewServer(ServerOptions{BroadcastWorkers: 2}, nil)
	a, b := newBroadcastWorkerTestClient(s, 4), newBroadcastWorkerTestClient(s, 4)
	s.clients = map[string]*Client{"A": a, "B": b}
	s.shardsDirty.Store(true)
	old := s.cachedClientShards()
	if len(old[0]) != 1 || len(old[1]) != 1 {
		t.Fatal("initial assignment must balance new sessions")
	}
	survivor, removed := old[1][0], old[0][0]
	s.clientsMutex.Lock()
	for key, client := range s.clients {
		if client == removed {
			delete(s.clients, key)
		}
	}
	s.shardsDirty.Store(true)
	s.clientsMutex.Unlock()
	next := s.cachedClientShards()
	if len(next[0]) != 0 || len(next[1]) != 1 || next[1][0] != survivor {
		t.Fatal("surviving session migrated away from worker 1")
	}
	if len(old[0]) != 1 || old[0][0] != removed || len(old[1]) != 1 || old[1][0] != survivor {
		t.Fatal("rebuild mutated an already dispatched snapshot")
	}
	replacement := newBroadcastWorkerTestClient(s, 4)
	s.clientsMutex.Lock()
	s.clients["replacement"] = replacement
	s.shardsDirty.Store(true)
	s.clientsMutex.Unlock()
	latest := s.cachedClientShards()
	if len(latest[0]) != 1 || latest[0][0] != replacement || len(latest[1]) != 1 || latest[1][0] != survivor {
		t.Fatal("new session did not fill the shortest shard while preserving its survivor")
	}
	// Repeated reads reuse the immutable snapshot without allocating or rebalancing.
	if got := testing.AllocsPerRun(100, func() { s.cachedClientShards() }); got != 0 {
		t.Fatalf("cached shards allocate: %g", got)
	}
}

func TestBroadcastOrderDuringMembershipChurn(t *testing.T) {
	for _, batching := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch_%v", batching), func(t *testing.T) {
			interval := time.Duration(0)
			if batching {
				interval = time.Hour
			}
			s := NewServer(ServerOptions{BroadcastWorkers: 2, BroadcastBatchInterval: interval, BroadcastBatchIntervalSet: true}, nil)
			s.batchMax = 2
			s.workerQueues = []chan broadcastJob{make(chan broadcastJob, 8), make(chan broadcastJob, 8)}
			a, b := newBroadcastWorkerTestClient(s, 8), newBroadcastWorkerTestClient(s, 8)
			s.clients = map[string]*Client{"A": a, "B": b}
			s.shardsDirty.Store(true)
			old := s.cachedClientShards()
			survivor, removed := old[1][0], old[0][0]
			first := spot.NewSpot("N0CALL", "K1ABC", 14020, "CW")
			first.Comment = "older"
			second := spot.NewSpot("N0CALL", "K1ABC", 14020, "CW")
			second.Comment = "newer"
			s.broadcastSpot(&broadcastPayload{spot: first, allowFast: true, allowMed: true, allowSlow: true})
			s.clientsMutex.Lock()
			for key, client := range s.clients {
				if client == removed {
					delete(s.clients, key)
				}
			}
			s.shardsDirty.Store(true)
			s.clientsMutex.Unlock()
			s.broadcastSpot(&broadcastPayload{spot: second, allowFast: true, allowMed: true, allowSlow: true})
			marker := newBroadcastWorkerTestClient(s, 8)
			markerSpot := spot.NewSpot("N0CALL", "W1XYZ", 14020, "CW")
			markers := 1
			if batching {
				markers = s.batchMax - len(s.workerQueues[0])%s.batchMax
			}
			for i := 0; i < markers; i++ {
				s.workerQueues[0] <- broadcastJob{spot: markerSpot, clients: []*Client{marker}, allowFast: true}
			}
			// A trailing sentinel also proves worker 1 completed every prior job.
			// Exact fillers exercise batch flush without waiting for a real tick.
			workerOneMarker := newBroadcastWorkerTestClient(s, 8)
			workerOneMarkers := 1
			if batching {
				workerOneMarkers = s.batchMax - len(s.workerQueues[1])%s.batchMax
			}
			for i := 0; i < workerOneMarkers; i++ {
				s.workerQueues[1] <- broadcastJob{spot: markerSpot, clients: []*Client{workerOneMarker}, allowFast: true}
			}
			var workers sync.WaitGroup
			start := func(id int) {
				workers.Add(1)
				go func() { defer workers.Done(); s.broadcastWorker(id, s.workerQueues[id]) }()
			}
			defer func() {
				close(s.shutdown)
				done := make(chan struct{})
				go func() { workers.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("broadcast worker did not join")
				}
			}()
			start(0)
			waitForSpotCount(t, marker.spotChan, markers) // proves worker 0 completed every prior job.
			start(1)
			for i, want := range []*spot.Spot{first, second} {
				select {
				case got := <-survivor.spotChan:
					if got.spot != want {
						t.Fatalf("delivery %d = %q, want %q", i, got.spot.Comment, want.Comment)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("survivor lost an admitted broadcast")
				}
			}
			waitForSpotCount(t, workerOneMarker.spotChan, workerOneMarkers)
			assertNoExtraBroadcastWorkerSpots(t, survivor.spotChan)
			queueDrops, clientDrops, _ := s.BroadcastMetricSnapshot()
			if queueDrops != 0 || clientDrops != 0 {
				t.Fatalf("ordered delivery used a drop: worker=%d client=%d", queueDrops, clientDrops)
			}
		})
	}
}

func TestClientShardAffinityChurnAndReplacement(t *testing.T) {
	s := NewServer(ServerOptions{BroadcastWorkers: 2}, nil)
	s.clients = map[string]*Client{"A": {}, "B": {}}
	s.shardsDirty.Store(true)
	initial := s.cachedClientShards()
	survivor := initial[1][0]
	var replaceKey string
	for key, client := range s.clients {
		if client == initial[0][0] {
			replaceKey = key
		}
	}
	for generation := 0; generation < 128; generation++ {
		replacement := &Client{callsign: "K1ABC"}
		s.clientsMutex.Lock()
		s.clients[replaceKey] = replacement
		s.shardsDirty.Store(true)
		s.clientsMutex.Unlock()
		snapshot := s.cachedClientShards()
		if len(snapshot[0]) != 1 || snapshot[0][0] != replacement || len(snapshot[1]) != 1 || snapshot[1][0] != survivor {
			t.Fatalf("replacement generation %d retained or migrated an old session", generation)
		}
	}
	if initial[0][0] == s.cachedClientShards()[0][0] || initial[1][0] != survivor {
		t.Fatal("replacement mutated old snapshot ownership")
	}
}

func BenchmarkClientShardAffinity(b *testing.B) {
	b.Logf("client_size=%d", unsafe.Sizeof(Client{}))
	for _, clients := range []int{100, 1000} {
		for _, rebuild := range []bool{false, true} {
			b.Run(fmt.Sprintf("clients_%d/rebuild_%v", clients, rebuild), func(b *testing.B) {
				s := NewServer(ServerOptions{BroadcastWorkers: 4}, nil)
				for i := 0; i < clients; i++ {
					s.clients[fmt.Sprint(i)] = &Client{}
				}
				s.shardsDirty.Store(true)
				s.cachedClientShards()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if rebuild {
						s.shardsDirty.Store(true)
					}
					s.cachedClientShards()
				}
				b.StopTimer()
				count := 0
				for _, shard := range s.cachedClientShards() {
					count += len(shard)
				}
				if count != clients {
					b.Fatalf("snapshot lost clients: %d", count)
				}
			})
		}
	}
}
