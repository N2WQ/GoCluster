package telnet

import (
	"testing"
	"time"
	"unsafe"

	"dxcluster/spot"
)

func TestDispatchSpotToWorkersDropsFullWorkerQueue(t *testing.T) {
	server := NewServer(ServerOptions{BroadcastWorkers: 1, WorkerQueue: 1}, nil)
	server.workerQueues = []chan broadcastJob{make(chan broadcastJob, 1)}

	dxSpot := spot.NewSpot("N0CALL", "K1ABC", 14074.0, "FT8")
	client := newBroadcastWorkerTestClient(server, 1)
	server.workerQueues[0] <- broadcastJob{spot: dxSpot, clients: []*Client{client}, allowFast: true}
	payload := &broadcastPayload{
		spot:      dxSpot,
		allowFast: true,
		allowMed:  true,
		allowSlow: true,
		enqueueAt: time.Unix(1700000000, 0).UTC(),
	}

	done := make(chan struct{})
	go func() {
		server.dispatchSpotToWorkers(payload, [][]*Client{{client}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchSpotToWorkers blocked on full worker queue")
	}

	queueDrops, _, _ := server.BroadcastMetricSnapshot()
	if queueDrops != 1 {
		t.Fatalf("queueDrops = %d, want 1", queueDrops)
	}
	if pending := len(server.workerQueues[0]); pending != 1 {
		t.Fatalf("worker queue pending jobs = %d, want 1", pending)
	}
}

func TestBroadcastWorkerBatchMaxFlush(t *testing.T) {
	server := NewServer(ServerOptions{
		BroadcastBatchInterval:    time.Hour,
		BroadcastBatchIntervalSet: true,
		ClientBuffer:              4,
	}, nil)
	server.batchMax = 2
	client := newBroadcastWorkerTestClient(server, 4)
	jobs := make(chan broadcastJob, 2)
	go server.broadcastWorker(0, jobs)
	t.Cleanup(func() { closeServerShutdown(server) })

	dxSpot := spot.NewSpot("N0CALL", "K1ABC", 14074.0, "FT8")
	jobs <- broadcastJob{spot: dxSpot, clients: []*Client{client}, allowFast: true}
	select {
	case <-client.spotChan:
		t.Fatal("unexpected spot before batch reached batchMax")
	default:
	}

	jobs <- broadcastJob{spot: dxSpot, clients: []*Client{client}, allowFast: true}
	waitForSpotCount(t, client.spotChan, 2)
}

func TestBroadcastWorkerTickerFlush(t *testing.T) {
	server := NewServer(ServerOptions{
		BroadcastBatchInterval:    10 * time.Millisecond,
		BroadcastBatchIntervalSet: true,
		ClientBuffer:              2,
	}, nil)
	server.batchMax = 8
	client := newBroadcastWorkerTestClient(server, 2)
	jobs := make(chan broadcastJob, 1)
	go server.broadcastWorker(0, jobs)
	t.Cleanup(func() { closeServerShutdown(server) })

	dxSpot := spot.NewSpot("N0CALL", "K1ABC", 14074.0, "FT8")
	jobs <- broadcastJob{spot: dxSpot, clients: []*Client{client}, allowFast: true}
	waitForSpotCount(t, client.spotChan, 1)
}

func TestBroadcastWorkerShutdownFlushesReceivedBatchOnly(t *testing.T) {
	server := NewServer(ServerOptions{
		BroadcastBatchInterval:    time.Hour,
		BroadcastBatchIntervalSet: true,
		ClientBuffer:              1,
	}, nil)
	server.batchMax = 8
	client := newBroadcastWorkerTestClient(server, 1)
	jobs := make(chan broadcastJob)
	go server.broadcastWorker(0, jobs)

	dxSpot := spot.NewSpot("N0CALL", "K1ABC", 14074.0, "FT8")
	sendDone := make(chan struct{})
	go func() {
		jobs <- broadcastJob{spot: dxSpot, clients: []*Client{client}, allowFast: true}
		close(sendDone)
	}()

	select {
	case <-sendDone:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not receive shutdown-flush test job")
	}
	close(server.shutdown)
	waitForSpotCount(t, client.spotChan, 1)
}

func TestBroadcastWorkerIgnoresZeroValueJob(t *testing.T) {
	server := NewServer(ServerOptions{
		BroadcastBatchInterval:    0,
		BroadcastBatchIntervalSet: true,
		ClientBuffer:              1,
	}, nil)
	client := newBroadcastWorkerTestClient(server, 1)
	jobs := make(chan broadcastJob, 2)
	go server.broadcastWorker(0, jobs)
	t.Cleanup(func() { closeServerShutdown(server) })

	dxSpot := spot.NewSpot("N0CALL", "K1ABC", 14074.0, "FT8")
	jobs <- broadcastJob{}
	jobs <- broadcastJob{spot: dxSpot, clients: []*Client{client}, allowFast: true}
	waitForSpotCount(t, client.spotChan, 1)
	assertNoExtraBroadcastWorkerSpots(t, client.spotChan)
}

func TestDeliverJobPreservesEnqueueAt(t *testing.T) {
	server := NewServer(ServerOptions{ClientBuffer: 1}, nil)
	client := newBroadcastWorkerTestClient(server, 1)
	dxSpot := spot.NewSpot("N0CALL", "K1ABC", 14074.0, "FT8")
	enqueueAt := time.Unix(1700000000, 123).UTC()

	server.deliverJob(broadcastJob{
		spot:      dxSpot,
		clients:   []*Client{client},
		allowFast: true,
		enqueueAt: enqueueAt,
	})

	select {
	case env := <-client.spotChan:
		if !env.enqueueAt.Equal(enqueueAt) {
			t.Fatalf("enqueueAt = %v, want %v", env.enqueueAt, enqueueAt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected delivered spot")
	}
}

func TestBroadcastJobRetainedMemoryDeltaIsBounded(t *testing.T) {
	jobSize := unsafe.Sizeof(broadcastJob{})
	pointerSize := unsafe.Sizeof((*broadcastJob)(nil))
	if jobSize <= pointerSize {
		t.Fatalf("broadcastJob size = %d, pointer size = %d; expected value job to be larger", jobSize, pointerSize)
	}

	defaultSlots := uintptr(defaultBroadcastWorkers() * (defaultWorkerQueueSize + defaultBroadcastBatch))
	runtimeSlots := uintptr(10 * (1024 + defaultBroadcastBatch))
	t.Logf("broadcastJob=%dB pointer=%dB default_delta=%dB runtime_yaml_delta=%dB",
		jobSize,
		pointerSize,
		defaultSlots*(jobSize-pointerSize),
		runtimeSlots*(jobSize-pointerSize),
	)
}

func BenchmarkDispatchSpotToWorkersValueJobs(b *testing.B) {
	const workers = 4
	server := NewServer(ServerOptions{BroadcastWorkers: workers, WorkerQueue: 1}, nil)
	server.workerQueues = make([]chan broadcastJob, workers)
	for i := range server.workerQueues {
		server.workerQueues[i] = make(chan broadcastJob, 1)
	}

	dxSpot := spot.NewSpot("N0CALL", "K1ABC", 14074.0, "FT8")
	payload := &broadcastPayload{
		spot:      dxSpot,
		allowFast: true,
		allowMed:  true,
		allowSlow: true,
		enqueueAt: time.Unix(1700000000, 0).UTC(),
	}
	shards := make([][]*Client, workers)
	for i := range shards {
		shards[i] = []*Client{newBroadcastWorkerTestClient(server, 1)}
	}

	b.ReportAllocs()
	beforeDrops, _, _ := server.BroadcastMetricSnapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.dispatchSpotToWorkers(payload, shards)
		drainBroadcastJobQueues(server.workerQueues)
	}
	b.StopTimer()
	afterDrops, _, _ := server.BroadcastMetricSnapshot()
	if afterDrops != beforeDrops {
		b.Fatalf("queueDrops changed during accepted-send benchmark: before=%d after=%d", beforeDrops, afterDrops)
	}
}

func newBroadcastWorkerTestClient(server *Server, buffer int) *Client {
	return &Client{
		server:   server,
		callsign: "N0CALL",
		filter:   unrestrictedTestFilter(),
		spotChan: make(chan *spotEnvelope, buffer),
		done:     make(chan struct{}),
	}
}

func waitForSpotCount(t *testing.T, ch <-chan *spotEnvelope, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < want; i++ {
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("received %d spots, want %d", i, want)
		}
	}
}

func assertNoExtraBroadcastWorkerSpots(t *testing.T, ch <-chan *spotEnvelope) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("unexpected extra spot")
	default:
	}
}

func closeServerShutdown(server *Server) {
	defer func() { _ = recover() }()
	close(server.shutdown)
}

func drainBroadcastJobQueues(queues []chan broadcastJob) {
nextQueue:
	for _, queue := range queues {
		for {
			select {
			case <-queue:
			default:
				continue nextQueue
			}
		}
	}
}
