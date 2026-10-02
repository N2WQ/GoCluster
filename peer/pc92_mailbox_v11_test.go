package peer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
)

func mailboxTestOwner(t *testing.T) (*Manager, *session, time.Time) {
	t.Helper()
	m := newProtocolTestManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &session{id: "N1PEER", remoteCall: "N1PEER", localCall: m.localCall, manager: m, pc9x: true, ctx: ctx, cancel: cancel, priorityLineCh: make(chan string, 128), writeCh: make(chan string, 128), peer: PeerEndpoint{family: config.PeeringPeerFamilyDXSpider}}
	m.sessions.Set(s.id, s)
	return m, s, time.Now()
}

func mailboxFrame(t *testing.T, wire string) *Frame {
	t.Helper()
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestPC92MailboxEligibilityMatchesNormalPath(t *testing.T) {
	for _, full := range []bool{false, true} {
		for _, tc := range []struct {
			name, origin, action, tail string
			hop                        int
		}{
			{"canonical own origin", "EA8/N0LOCAL/P", "K", "0^0^^branch", 99},
			{"zero hop", "N2AAA", "K", "0^0^^branch", 0},
			{"unsupported", "N2AAA", "F", "0^0^^branch", 99},
			{"malformed C member", "N2AAA", "C", "1K1GOOD^H9x", 99},
		} {
			t.Run(fmt.Sprintf("%s/full=%v", tc.name, full), func(t *testing.T) {
				m, source, now := mailboxTestOwner(t)
				p := m.protocol
				if full {
					filler := mailboxFrame(t, fmt.Sprintf("PC92^N2AAA^%d^K^5N2AAA^0^0^H99^", utcSecond(now)))
					for i := 0; i < 192; i++ {
						if !p.enqueue(filler, source, now) {
							t.Fatal("filler refused")
						}
					}
				}
				frame := mailboxFrame(t, fmt.Sprintf("PC92^%s^%d^%s^5%s^%s^H%d^", tc.origin, utcSecond(now), tc.action, tc.origin, tc.tail, tc.hop))
				m.HandleFrame(frame, source)
				if !full {
					select {
					case work := <-p.input:
						p.consumeInput(work)
					default:
					}
				}
				keys, _, _ := p.pc92.occupancy()
				if source.ctx.Err() != nil || m.blockedPeers.Len() != 0 || m.admissionFailures.Len() != 0 || p.graph.nodes.Len() != 0 || p.graph.freshness.Len() != 0 || keys != 0 {
					t.Fatal("excluded record changed authority or gated source")
				}
			})
		}
	}
}

func TestPC92MailboxStaleOwnerCannotGateReplacement(t *testing.T) {
	m, stale, now := mailboxTestOwner(t)
	p := m.protocol
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	replacement := &session{id: stale.id, remoteCall: stale.remoteCall, pc9x: true, ctx: ctx, cancel: cancel}
	m.sessions.Set(stale.id, replacement)
	frame := mailboxFrame(t, fmt.Sprintf("PC92^N2AAA^%d^C^5N2AAA^H99^", utcSecond(now)))
	for i := 0; i < 192; i++ {
		if !p.enqueue(frame, replacement, now) {
			t.Fatal("filler refused")
		}
	}
	m.HandleFrame(frame, stale)
	if replacement.ctx.Err() != nil || m.blockedPeers.Len() != 0 || m.admissionFailures.Len() != 0 || m.sessions.Value(stale.id) != replacement {
		t.Fatal("stale refusal affected replacement ownership")
	}
}

func TestPC93MailboxRefusalsPersistAndRemainIsolated(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprintf("byte-bound=%v", large), func(t *testing.T) {
			m, source, now := mailboxTestOwner(t)
			p := m.protocol
			message := "hello"
			if large {
				message = strings.Repeat("x", 60000)
			}
			frame := mailboxFrame(t, fmt.Sprintf("PC93^N2AAA^%d^LOGGER^K1FROM^*^%s^H99^", utcSecond(now), message))
			count := 0
			for p.enqueue(frame, source, now) {
				count++
				if count > 64 {
					t.Fatal("unbounded input count")
				}
			}
			if !large && count != 64 || large && count >= 64 {
				t.Fatalf("wrong capacity exercised: %d", count)
			}
			p.sampleStats()
			stats := m.ProtocolStats()
			if stats.PC93InputRefused != 1 || stats.PC93Refused != 0 || stats.InputPC93 != count || source.ctx.Err() != nil {
				t.Fatalf("refusal stats=%+v", stats)
			}
			pc92 := mailboxFrame(t, fmt.Sprintf("PC92^N3AAA^%d^C^5N3AAA^H99^", utcSecond(now)))
			if !p.enqueue(pc92, source, now) {
				t.Fatal("message pressure consumed topology mailbox")
			}
			for len(p.input) > 0 {
				p.consumeInput(<-p.input)
			}
			p.sampleStats()
			stats = m.ProtocolStats()
			if stats.PC93InputRefused != 1 || stats.InputPC93 != 0 || stats.InputPC93Bytes != 0 || stats.PC93Refused != 0 || p.graph.nodes.Value("N3AAA") == nil || source.ctx.Err() != nil {
				t.Fatalf("drain lost counter or isolation: %+v", stats)
			}
			// Prove an old cumulative refusal does not produce another periodic
			// diagnostic after the throttle interval. No new refusal occurred.
			old := now.Add(-2 * time.Minute)
			p.diagnosticAt.Set("PC93 input admission refused", old)
			p.sampleStats()
			if p.diagnosticAt.Value("PC93 input admission refused") != old {
				t.Fatal("old refusal was re-reported without new pressure")
			}
			for i := 0; i < count; i++ {
				if !p.enqueue(frame, source, now) {
					t.Fatal("drained capacity did not recover")
				}
			}
			m.HandleFrame(frame, source)
			p.sampleStats()
			if m.ProtocolStats().PC93InputRefused != 2 || p.diagnosticAt.Value("PC93 input admission refused").Equal(old) {
				t.Fatal("new refusal not counted/reported")
			}
		})
	}
}

func TestPC93MailboxConcurrentRefusalStats(t *testing.T) {
	m, source, now := mailboxTestOwner(t)
	p := m.protocol
	frame := mailboxFrame(t, fmt.Sprintf("PC93^N2AAA^%d^LOGGER^K1FROM^*^hello^H99^", utcSecond(now)))
	for i := 0; i < 64; i++ {
		if !p.enqueue(frame, source, now) {
			t.Fatal("filler refused")
		}
	}
	var producers sync.WaitGroup
	for i := 0; i < 4; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < 100; j++ {
				p.enqueue(frame, source, now)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		p.sampleStats()
	}
	producers.Wait()
	p.sampleStats()
	if got := m.ProtocolStats().PC93InputRefused; got != 400 {
		t.Fatalf("concurrent refusal count=%d", got)
	}
}
