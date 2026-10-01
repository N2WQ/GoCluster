package peer

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"dxcluster/config"
)

func initialRetryOwner(t *testing.T) (*protocolController, *session, *time.Time) {
	t.Helper()
	m, err := NewManager(config.PeeringConfig{NodeVersion: "5457", NodeBuild: "633", PC92Bitmap: 5, HopCount: 99}, "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	ep := PeerEndpoint{host: "pipe", remoteCall: "N1REM", family: config.PeeringPeerFamilyDXSpider}
	s := newSession(local, dirOutbound, m, ep, m.sessionSettings(ep))
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.pc9x = true
	if err := m.trackCandidate(s); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close(); _ = remote.Close(); m.releaseCandidate(s) })
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	m.protocol.wallNow = func() time.Time { return now }
	return m.protocol, s, &now
}

func TestPC92InitialRateRetryResumesAfterAcceptedA(t *testing.T) {
	p, s, now := initialRetryOwner(t)
	// Consume through .98. Initial A can use the final .99 slot, but K must
	// wait for the next second. Check wire output, not the implementation marker.
	for range 99 {
		if _, err := p.timestamps.NextAt(*now); err != nil {
			t.Fatal(err)
		}
	}
	for range 1000 {
		if err := p.request(protocolRequest{kind: "initial", source: s}); !errors.Is(err, ErrTimestampRate) {
			t.Fatalf("same-second initial retry=%v", err)
		}
	}
	if len(s.priorityLineCh) != 1 {
		t.Fatalf("initial retries queued %d records; want one A", len(s.priorityLineCh))
	}
	*now = now.Add(time.Second)
	if err := p.request(protocolRequest{kind: "initial", source: s}); err != nil {
		t.Fatal(err)
	}
	if len(s.priorityLineCh) != 2 {
		t.Fatalf("completed initial queue=%d; want A then K", len(s.priorityLineCh))
	}
	for _, want := range []struct{ action, timestamp string }{{"A", "43200.99"}, {"K", "43201"}} {
		wire := <-s.priorityLineCh
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		record, err := DecodePC92(frame)
		if err != nil || record.Action != want.action || record.Timestamp != want.timestamp {
			t.Fatalf("initial record=%q decode=%v; want %s at%s", wire, err, want.action, want.timestamp)
		}
	}
}

func TestPC92InitialOutputFailureCannotPublishK(t *testing.T) {
	p, s, _ := initialRetryOwner(t)
	for range 128 {
		if err := s.sendControlLine("occupied"); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.request(protocolRequest{kind: "initial", source: s}); err == nil || s.ctx.Err() == nil {
		t.Fatal("failed initial A did not terminate initialization")
	}
	if len(s.priorityLineCh) != 128 {
		t.Fatal("failed initialization changed the saturated output queue")
	}
	for range 128 {
		if got := <-s.priorityLineCh; got != "occupied" {
			t.Fatalf("failed initialization published %q", got)
		}
	}
}

func TestPC92InitialRequiresCandidateOwnership(t *testing.T) {
	p, s, _ := initialRetryOwner(t)
	p.manager.releaseCandidate(s)
	if err := p.request(protocolRequest{kind: "initial", source: s}); err == nil || len(s.priorityLineCh) != 0 {
		t.Fatal("unowned identity published initial topology")
	}
}
