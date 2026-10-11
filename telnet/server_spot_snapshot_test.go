package telnet

import (
	"sync"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestBroadcastSpotSnapshotsPayload(t *testing.T) {
	srv := NewServer(ServerOptions{BroadcastQueue: 1}, nil)
	src := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8")
	src.Comment = "first"
	src.DEMetadata.Grid = "FN31"

	srv.BroadcastSpot(src, true, true, true)

	select {
	case payload := <-srv.broadcast:
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				_ = payload.spot.FormatDXCluster()
			}
		}()
		for i := 0; i < 128; i++ {
			src.Comment = "mutated"
			src.DEMetadata.Grid = "EM10"
			src.InvalidateMetadataCache()
			src.EnsureNormalized()
		}
		wg.Wait()

		if payload.spot == src {
			t.Fatalf("expected broadcast payload to own a snapshot, got original pointer")
		}
		if payload.spot.Comment != "first" {
			t.Fatalf("expected broadcast comment to remain stable, got %q", payload.spot.Comment)
		}
		if payload.spot.DEMetadata.Grid != "FN31" {
			t.Fatalf("expected broadcast grid to remain stable, got %q", payload.spot.DEMetadata.Grid)
		}
	default:
		t.Fatalf("expected broadcast payload")
	}
}

func TestBroadcastSpotOwnedReusesSnapshot(t *testing.T) {
	srv := NewServer(ServerOptions{BroadcastQueue: 1}, nil)
	snapshot := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8").SnapshotForAsync()
	if snapshot == nil {
		t.Fatal("expected snapshot")
	}

	srv.BroadcastSpotOwned(snapshot, true, true, true)

	select {
	case payload := <-srv.broadcast:
		if payload.spot != snapshot {
			t.Fatalf("expected owned broadcast to reuse snapshot pointer")
		}
	default:
		t.Fatalf("expected broadcast payload")
	}
}

func TestTryBroadcastSpotOwnedAdmissionAndRefusal(t *testing.T) {
	var absent *Server
	snapshot := spot.NewSpot("K1ABC", "W1XYZ", 14020, "CW").SealForAsync()
	if absent.TryBroadcastSpotOwned(snapshot, true, true, true) {
		t.Fatal("nil server cannot admit a broadcast")
	}
	srv := NewServer(ServerOptions{BroadcastQueue: 1}, nil)
	if srv.TryBroadcastSpotOwned(nil, true, true, true) {
		t.Fatal("nil snapshot cannot admit a broadcast")
	}
	if !srv.TryBroadcastSpotOwned(snapshot, true, false, true) {
		t.Fatal("free queue refused its immutable snapshot")
	}
	if srv.TryBroadcastSpotOwned(snapshot, false, true, false) {
		t.Fatal("full queue claimed admission")
	}
	if queue, clients, senders := srv.BroadcastMetricSnapshot(); queue != 1 || clients != 0 || senders != 0 {
		t.Fatalf("refusal changed drop ownership: %d/%d/%d", queue, clients, senders)
	}
	payload := <-srv.broadcast
	if payload.spot != snapshot || !payload.allowFast || payload.allowMed || !payload.allowSlow || payload.enqueueAt.IsZero() {
		t.Fatalf("admitted payload changed pointer, lane mask or enqueue clock: %+v", payload)
	}
	// Existing void callers retain admission and exact refusal accounting.
	srv.BroadcastSpotOwned(snapshot, false, true, false)
	srv.BroadcastSpotOwned(snapshot, true, true, true)
	if queue, _, _ := srv.BroadcastMetricSnapshot(); queue != 2 {
		t.Fatalf("void compatibility path counted refusal %d times, want 2 total", queue)
	}
	payload = <-srv.broadcast
	if payload.spot != snapshot || payload.allowFast || !payload.allowMed || payload.allowSlow {
		t.Fatal("void wrapper changed its admitted payload")
	}
}

func TestDeliverSelfSpotSnapshotsPayload(t *testing.T) {
	srv := NewServer(ServerOptions{ClientBuffer: 1}, nil)
	f := filter.NewFilter()
	f.SetSelfEnabled(true)
	client := &Client{
		server:   srv,
		callsign: "K1ABC-2",
		filter:   f,
		spotChan: make(chan *spotEnvelope, 1),
		done:     make(chan struct{}),
	}
	srv.clients["K1ABC-2"] = client

	src := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8")
	src.Comment = "first"
	src.DEMetadata.Grid = "FN31"

	srv.DeliverSelfSpot(src)

	select {
	case env := <-client.spotChan:
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				_ = env.spot.FormatDXCluster()
			}
		}()
		for i := 0; i < 128; i++ {
			src.Comment = "mutated"
			src.DEMetadata.Grid = "EM10"
			src.InvalidateMetadataCache()
			src.EnsureNormalized()
		}
		wg.Wait()

		if env.spot == src {
			t.Fatalf("expected self-delivery payload to own a snapshot, got original pointer")
		}
		if env.spot.Comment != "first" {
			t.Fatalf("expected self-delivery comment to remain stable, got %q", env.spot.Comment)
		}
		if env.spot.DEMetadata.Grid != "FN31" {
			t.Fatalf("expected self-delivery grid to remain stable, got %q", env.spot.DEMetadata.Grid)
		}
	default:
		t.Fatalf("expected self-delivery payload")
	}
}

func TestDeliverSelfSpotOwnedReusesSnapshot(t *testing.T) {
	srv := NewServer(ServerOptions{ClientBuffer: 1}, nil)
	f := filter.NewFilter()
	f.SetSelfEnabled(true)
	client := &Client{
		server:   srv,
		callsign: "K1ABC-2",
		filter:   f,
		spotChan: make(chan *spotEnvelope, 1),
		done:     make(chan struct{}),
	}
	srv.clients["K1ABC-2"] = client

	snapshot := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8").SnapshotForAsync()
	if snapshot == nil {
		t.Fatal("expected snapshot")
	}

	srv.DeliverSelfSpotOwned("K1ABC", snapshot)

	select {
	case env := <-client.spotChan:
		if env.spot != snapshot {
			t.Fatalf("expected owned self-delivery to reuse snapshot pointer")
		}
	default:
		t.Fatalf("expected self-delivery payload")
	}
}
