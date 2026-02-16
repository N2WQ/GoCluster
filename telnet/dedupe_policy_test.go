package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/dedup"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHandleDedupeCommandSetShow(t *testing.T) {
	tmp := t.TempDir()
	orig := filter.UserDataDir
	filter.UserDataDir = tmp
	t.Cleanup(func() { filter.UserDataDir = orig })

	server := NewServer(ServerOptions{
		DedupeFastEnabled: true,
		DedupeMedEnabled:  true,
		DedupeSlowEnabled: true,
	}, nil)
	client := &Client{
		callsign: "N0CALL",
		filter:   filter.NewFilter(),
	}
	client.setDedupePolicy(dedupePolicyFast)

	resp, handled := server.handleDedupeCommand(client, "SHOW DEDUPE")
	if !handled {
		t.Fatalf("expected SHOW DEDUPE to be handled")
	}
	if !strings.Contains(resp, "FAST") {
		t.Fatalf("expected SHOW DEDUPE to mention FAST, got %q", resp)
	}

	resp, handled = server.handleDedupeCommand(client, "SET DEDUPE MED")
	if !handled {
		t.Fatalf("expected SET DEDUPE MED to be handled")
	}
	if client.getDedupePolicy() != dedupePolicyMed {
		t.Fatalf("expected dedupe policy MED, got %v", client.getDedupePolicy())
	}
	if !strings.Contains(resp, "MED") {
		t.Fatalf("expected SET DEDUPE response to mention MED, got %q", resp)
	}

	resp, handled = server.handleDedupeCommand(client, "SET DEDUPE SLOW")
	if !handled {
		t.Fatalf("expected SET DEDUPE SLOW to be handled")
	}
	if client.getDedupePolicy() != dedupePolicySlow {
		t.Fatalf("expected dedupe policy SLOW, got %v", client.getDedupePolicy())
	}
	if !strings.Contains(resp, "SLOW") {
		t.Fatalf("expected SET DEDUPE response to mention SLOW, got %q", resp)
	}
}

func TestHandleDedupeCommandFallbackToFast(t *testing.T) {
	tmp := t.TempDir()
	orig := filter.UserDataDir
	filter.UserDataDir = tmp
	t.Cleanup(func() { filter.UserDataDir = orig })

	server := NewServer(ServerOptions{
		DedupeFastEnabled: true,
		DedupeMedEnabled:  true,
		DedupeSlowEnabled: false,
	}, nil)
	client := &Client{
		callsign: "N0CALL",
		filter:   filter.NewFilter(),
	}
	client.setDedupePolicy(dedupePolicyFast)

	resp, handled := server.handleDedupeCommand(client, "SET DEDUPE SLOW")
	if !handled {
		t.Fatalf("expected SET DEDUPE to be handled")
	}
	if client.getDedupePolicy() != dedupePolicyFast {
		t.Fatalf("expected fallback to FAST, got %v", client.getDedupePolicy())
	}
	if !strings.Contains(resp, "FAST") {
		t.Fatalf("expected fallback response to mention FAST, got %q", resp)
	}
}

func TestBroadcastRespectsDedupePolicyWindows(t *testing.T) {
	server := NewServer(ServerOptions{
		BroadcastWorkers:       1,
		BroadcastQueue:         16384,
		WorkerQueue:            16384,
		ClientBuffer:           16384,
		BroadcastBatchInterval: time.Millisecond,
	}, nil)
	t.Cleanup(func() {
		defer func() { _ = recover() }()
		close(server.shutdown)
	})

	server.startWorkerPool()
	go server.handleBroadcasts()

	fastClient := &Client{
		callsign: "FAST1",
		filter:   filter.NewFilter(),
		spotChan: make(chan *spotEnvelope, 16384),
	}
	fastClient.setDedupePolicy(dedupePolicyFast)
	medClient := &Client{
		callsign: "MED1",
		filter:   filter.NewFilter(),
		spotChan: make(chan *spotEnvelope, 16384),
	}
	medClient.setDedupePolicy(dedupePolicyMed)
	slowClient := &Client{
		callsign: "SLOW1",
		filter:   filter.NewFilter(),
		spotChan: make(chan *spotEnvelope, 16384),
	}
	slowClient.setDedupePolicy(dedupePolicySlow)

	server.clientsMutex.Lock()
	server.clients[fastClient.callsign] = fastClient
	server.clients[medClient.callsign] = medClient
	server.clients[slowClient.callsign] = slowClient
	server.shardsDirty.Store(true)
	server.clientsMutex.Unlock()

	secondaryFast := dedup.NewSecondaryDeduper(120*time.Second, false)
	secondaryMed := dedup.NewSecondaryDeduper(300*time.Second, false)
	secondarySlow := dedup.NewSecondaryDeduperWithKey(480*time.Second, false, dedup.SecondaryKeyCQZone)
	secondaryFast.Start()
	secondaryMed.Start()
	secondarySlow.Start()
	t.Cleanup(secondaryFast.Stop)
	t.Cleanup(secondaryMed.Stop)
	t.Cleanup(secondarySlow.Stop)

	base := spot.NewSpot("DXAAA", "DEBBB", 14030.0, "CW")
	base.DEMetadata.ADIF = 1
	base.DEMetadata.CQZone = 5
	base.DEGrid2 = "FN"

	start := time.Now().UTC()
	for i := 0; i < 10000; i++ {
		s := base.CloneWithComment(base.Comment)
		s.Time = start.Add(time.Duration(i*60) * time.Second)
		allowFast := secondaryFast.ShouldForward(s)
		allowMed := secondaryMed.ShouldForward(s)
		allowSlow := secondarySlow.ShouldForward(s)
		server.BroadcastSpot(s, allowFast, allowMed, allowSlow)
	}

	fastCount := drainSpotCount(t, fastClient.spotChan, 5000)
	medCount := drainSpotCount(t, medClient.spotChan, 2000)
	slowCount := drainSpotCount(t, slowClient.spotChan, 1250)
	if fastCount != 5000 {
		t.Fatalf("expected fast policy to receive 5000 spots, got %d", fastCount)
	}
	if medCount != 2000 {
		t.Fatalf("expected med policy to receive 2000 spots, got %d", medCount)
	}
	if slowCount != 1250 {
		t.Fatalf("expected slow policy to receive 1250 spots, got %d", slowCount)
	}
	assertNoExtraSpots(t, fastClient.spotChan)
	assertNoExtraSpots(t, medClient.spotChan)
	assertNoExtraSpots(t, slowClient.spotChan)
}

func drainSpotCount(t *testing.T, ch <-chan *spotEnvelope, want int) int {
	t.Helper()
	count := 0
	deadline := time.After(2 * time.Second)
	for count < want {
		select {
		case <-deadline:
			return count
		case <-ch:
			count++
		}
	}
	return count
}

func assertNoExtraSpots(t *testing.T, ch <-chan *spotEnvelope) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("unexpected extra spot delivered")
	case <-time.After(150 * time.Millisecond):
	}
}
