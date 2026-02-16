package peer

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestKeepaliveLoopSendsPC51ForPC9x(t *testing.T) {
	s := &session{
		localCall:   "N0CALL",
		remoteCall:  "N0PEER",
		pc92Bitmap:  5,
		nodeVersion: "5457",
		hopCount:    99,
		pc9x:        true,
		keepalive:   5 * time.Millisecond,
		writeCh:     make(chan string, 8),
		tsGen:       &timestampGenerator{},
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	defer s.cancel()

	go s.keepaliveLoop()

	waitForKeepalives(t, s.writeCh, true)
}

func TestKeepaliveLoopSendsPC51ForLegacy(t *testing.T) {
	s := &session{
		localCall:  "N0CALL",
		remoteCall: "N0PEER",
		pc9x:       false,
		keepalive:  5 * time.Millisecond,
		writeCh:    make(chan string, 4),
		tsGen:      &timestampGenerator{},
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	defer s.cancel()

	go s.keepaliveLoop()

	waitForKeepalives(t, s.writeCh, false)
}

func waitForKeepalives(t *testing.T, ch <-chan string, wantPC92 bool) {
	t.Helper()
	timeout := time.NewTimer(500 * time.Millisecond)
	defer timeout.Stop()

	var gotPC51, gotPC92 bool
	for !gotPC51 || (wantPC92 && !gotPC92) {
		select {
		case line := <-ch:
			if strings.HasPrefix(line, "PC51^") {
				gotPC51 = true
			}
			if strings.HasPrefix(line, "PC92^") && strings.Contains(line, "^K^") {
				gotPC92 = true
			}
		case <-timeout.C:
			t.Fatalf("timeout waiting for keepalives (pc51=%v pc92=%v)", gotPC51, gotPC92)
		}
	}
}
