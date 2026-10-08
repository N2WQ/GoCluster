package peer

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"dxcluster/config"
)

func ccResponseOwner(t *testing.T) (*protocolController, *session, *time.Time) {
	t.Helper()
	p, s, now := initialRetryOwner(t)
	s.peer.family = config.PeeringPeerFamilyCCluster
	return p, s, now
}

func TestCCResponseRetryResumesKWithFreshA(t *testing.T) {
	p, s, now := ccResponseOwner(t)
	// Initial publication has already succeeded. The PC20 response is a
	// separate configuration exchange and must still emit its own A.
	if err := p.request(protocolRequest{kind: "initial", source: s}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		<-s.priorityLineCh
	}
	*now = now.Add(950 * time.Millisecond)
	for range 93 {
		if _, err := p.timestamps.NextAt(*now); err != nil {
			t.Fatal(err)
		}
	}
	for range 100 {
		if err := p.request(protocolRequest{kind: "cc_response", source: s}); !errors.Is(err, ErrTimestampRate) {
			t.Fatalf("partial response=%v", err)
		}
	}
	if len(s.priorityLineCh) != 1 {
		t.Fatalf("partial response queued %d records", len(s.priorityLineCh))
	}
	*now = now.Add(time.Second)
	if err := p.request(protocolRequest{kind: "cc_response", source: s}); err != nil {
		t.Fatal(err)
	}
	if len(s.priorityLineCh) != 2 {
		t.Fatalf("completed response queued %d records", len(s.priorityLineCh))
	}
	for _, want := range []struct{ action, timestamp string }{{"A", "43200.95"}, {"K", "43201"}} {
		wire := <-s.priorityLineCh
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		record, err := DecodePC92(frame)
		if err != nil || record.Action != want.action || record.Timestamp != want.timestamp {
			t.Fatalf("response %q decode=%v; want %s at %s", wire, err, want.action, want.timestamp)
		}
	}
}

func TestCCResponseRequiresCandidateAndAvailableOutput(t *testing.T) {
	for _, failure := range []string{"unowned", "queue full", "canceled", "expired"} {
		t.Run(failure, func(t *testing.T) {
			p, s, _ := ccResponseOwner(t)
			req := protocolRequest{kind: "cc_response", source: s}
			switch failure {
			case "unowned":
				p.manager.releaseCandidate(s)
			case "queue full":
				for range 128 {
					if err := s.sendControlLine("occupied"); err != nil {
						t.Fatal(err)
					}
				}
			case "canceled":
				s.cancel()
			case "expired":
				req.deadline = time.Now().Add(-time.Second)
			}
			err := p.request(req)
			if err == nil {
				t.Fatal("invalid response owner/output succeeded")
			}
			if failure == "expired" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expired response=%v", err)
			}
			want := 0
			if failure == "queue full" {
				want = 128
				if s.ctx.Err() == nil {
					t.Fatal("output failure did not cancel session")
				}
			}
			if len(s.priorityLineCh) != want {
				t.Fatalf("failed response queued %d records, want %d", len(s.priorityLineCh), want)
			}
		})
	}
}

func TestCCResponseDeadlineIncludesControllerQueue(t *testing.T) {
	p, s, _ := ccResponseOwner(t)
	m := p.manager
	m.ctx, m.cancel = context.WithCancel(context.Background())
	t.Cleanup(m.cancel)
	s.phaseDeadline = time.Now().Add(40 * time.Millisecond)
	result := make(chan error, 1)
	go func() { result <- m.protocolCall("cc_response", s) }()
	req := takeProtocolRequest(t, p)
	if err := requireProtocolResult(t, result); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued response deadline=%v", err)
	}
	if err := p.request(req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale response=%v", err)
	}
	if len(s.priorityLineCh) != 0 {
		t.Fatal("expired queued response emitted configuration")
	}
}

func TestCCStartupControlQueueRefusal(t *testing.T) {
	for _, frameWire := range []string{"PC51^N0CALL^N1REM^1^", "PC20^"} {
		t.Run(frameWire, func(t *testing.T) {
			_, s, _ := ccResponseOwner(t)
			// Legacy completion avoids a controller worker; PC22 itself must
			// still fail closed when bounded control admission is exhausted.
			s.pc9x = false
			occupied := 128
			if frameWire == "PC20^" {
				occupied = 127
			}
			for range occupied {
				if err := s.sendControlLine("occupied"); err != nil {
					t.Fatal(err)
				}
			}
			frame, err := ParseFrame(frameWire)
			if err != nil {
				t.Fatal(err)
			}
			initialized := true
			done, err := s.handleOutboundFrame(frame, &initialized)
			if err == nil || s.established || s.ctx.Err() == nil {
				t.Fatalf("refused startup control done=%v err=%v canceled=%v", done, err, s.ctx.Err())
			}
			if len(s.priorityLineCh) != 128 {
				t.Fatalf("refusal queue=%d", len(s.priorityLineCh))
			}
			for range occupied {
				if got := <-s.priorityLineCh; got != "occupied" {
					t.Fatalf("queued=%q", got)
				}
			}
			if frameWire == "PC20^" {
				if got := <-s.priorityLineCh; len(got) < 5 || got[:5] != "PC19^" {
					t.Fatalf("accepted legacy configuration=%q", got)
				}
			}
		})
	}
}

func TestCCResponseKQueueRefusalAndFreshCandidate(t *testing.T) {
	p, s, _ := ccResponseOwner(t)
	for range 127 {
		if err := s.sendControlLine("occupied"); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.request(protocolRequest{kind: "cc_response", source: s}); s.ctx.Err() == nil || s.established {
		t.Fatalf("CC K output refusal err=%v canceled=%v established=%v queue=%d", err, s.ctx.Err(), s.established, len(s.priorityLineCh))
	}
	for range 127 {
		<-s.priorityLineCh
	}
	if got := <-s.priorityLineCh; !pc92TypeLine("A").match(got) {
		t.Fatalf("accepted partial response=%q", got)
	}
	if len(s.priorityLineCh) != 0 {
		t.Fatal("refused K was published")
	}
	if err := s.sendHandshakeLine("PC22^"); !errors.Is(err, context.Canceled) || len(s.priorityLineCh) != 0 {
		t.Fatalf("failed response published completion: err=%v queue=%d", err, len(s.priorityLineCh))
	}
	p.manager.releaseCandidate(s)
	// A fresh reconnect has independent candidate-owned response progress.
	local, remote := net.Pipe()
	fresh := newSession(local, dirOutbound, p.manager, s.peer, p.manager.sessionSettings(s.peer))
	fresh.ctx, fresh.cancel = context.WithCancel(context.Background())
	fresh.pc9x = true
	if err := p.manager.trackCandidate(fresh); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fresh.close(); _ = remote.Close(); p.manager.releaseCandidate(fresh) })
	if err := p.request(protocolRequest{kind: "cc_response", source: fresh}); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"A", "K"} {
		if got := <-fresh.priorityLineCh; !pc92TypeLine(action).match(got) {
			t.Fatalf("fresh candidate want %s got %q", action, got)
		}
	}
}

func TestCCStartupPingExpiredDeadline(t *testing.T) {
	_, s, _ := ccResponseOwner(t)
	s.phaseDeadline = time.Now().Add(-time.Second)
	frame, err := ParseFrame("PC51^N0CALL^N1REM^1^")
	if err != nil {
		t.Fatal(err)
	}
	initialized := false
	done, err := s.handleOutboundFrame(frame, &initialized)
	if err == nil || done || initialized || len(s.priorityLineCh) != 0 {
		t.Fatalf("expired ping done=%v init=%v err=%v queue=%d", done, initialized, err, len(s.priorityLineCh))
	}
}
