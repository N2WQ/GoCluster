package main

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"dxcluster/peer"
)

func TestProbeCommandTimestampSequenceSurvivesReconnect(t *testing.T) {
	clock := newProbeTimestampClock(probeNoonUTC)
	r := newTimestampCommand(t, 2, clock.now, nil)
	first := r.next(t)
	initializeTimestampPeer(t, first, "43200", "43200.01")
	first.syncReader(t)
	first.readerAfterSync(t)
	if err := first.peer.Close(); err != nil {
		t.Fatal(err)
	}
	r.ended(t, io.EOF)
	awaitProbe(r.guard, t, first.conn.closed, "first timestamp connection retirement")
	second := r.next(t)
	initializeTimestampPeer(t, second, "43200.02", "43200.03")
	second.syncReader(t)
	if clock.calls.Load() != 4 {
		t.Fatalf("reconnect clock calls = %d", clock.calls.Load())
	}
	r.stop(t, second)
}

func TestProbeFailedPublicationLeavesReconnectGap(t *testing.T) {
	clock := newProbeTimestampClock(probeNoonUTC)
	fault := probeWireError("uncertain PC92 prefix")
	const prefix = "PC92^N2WQ-33^43200.01^K^"
	r := newTimestampCommand(t, 2, clock.now, func(index int, conn *fixtureProbeConn) {
		if index != 0 {
			return
		}
		conn.writeFault = func(data []byte) (int, error, bool) {
			if !strings.Contains(string(data), "^43200.01^") {
				return 0, nil, false
			}
			n, err := conn.Conn.Write([]byte(prefix)) // Send real bytes, not a fabricated count.
			if err != nil {
				return n, err, true
			}
			return n, fault, true
		}
	})
	first := r.next(t)
	first.send(t, "PC18^remote^pc9x^\r\n")
	requireTimestampRecord(t, first, "43200", true)
	got := make([]byte, len(prefix))
	// The fixture wire methods own real-time read bounds, independently of UTC.
	if err := first.peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(first.wire, got); err != nil || string(got) != prefix {
		t.Fatalf("actual uncertain prefix = %q / %v", got, err)
	}
	r.ended(t, fault)
	awaitProbe(r.guard, t, first.conn.closed, "uncertain publisher retirement")
	found := false
	for len(first.conn.writeExited) > 0 {
		exit := <-first.conn.writeExited
		if strings.Contains(exit.data, "^43200.01^") {
			found = true
			if !errors.Is(exit.err, fault) || exit.closed || exit.interrupted {
				t.Fatalf("uncertain send cause/ownership receipt = %+v", exit)
			}
		}
	}
	if !found {
		t.Fatal("missing actual uncertain send exit")
	}
	if writes := first.conn.writes(); len(writes) != 2 {
		t.Fatalf("failed connection replayed/completed output: %q", writes)
	}
	second := r.next(t)
	initializeTimestampPeer(t, second, "43200.02", "43200.03")
	second.syncReader(t)
	r.stop(t, second)
}

func TestProbeClockRefusalPersistsAcrossReconnect(t *testing.T) {
	clock := newProbeTimestampClock(probeNoonUTC)
	r := newTimestampCommand(t, 4, clock.now, nil)
	first := r.next(t)
	initializeTimestampPeer(t, first, "43200", "43200.01")
	first.syncReader(t)
	first.readerAfterSync(t)
	clock.unix.Store(probeNoonUTC.Unix() - 1)
	if err := first.peer.Close(); err != nil {
		t.Fatal(err)
	}
	r.ended(t, io.EOF)
	regressed := r.next(t)
	regressed.send(t, "PC18^remote^pc9x^\r\n")
	requireTimestampRefusal(t, r, regressed, peer.ErrTimestampClock)
	if len(regressed.conn.writes()) != 0 {
		t.Fatal("regression published PC92")
	}
	clock.unix.Store(probeNoonUTC.Unix())
	catchup := r.next(t)
	catchup.send(t, "PC18^remote^pc9x^\r\n")
	requireTimestampRefusal(t, r, catchup, peer.ErrTimestampClock)
	if len(catchup.conn.writes()) != 0 {
		t.Fatal("retained second catch-up published PC92")
	}
	clock.unix.Store(probeNoonUTC.Unix() + 1)
	recovered := r.next(t)
	initializeTimestampPeer(t, recovered, "43201", "43201.01")
	recovered.syncReader(t)
	if clock.calls.Load() != 6 {
		t.Fatalf("unsafe retry clock calls = %d", clock.calls.Load())
	}
	r.stop(t, recovered)
}
