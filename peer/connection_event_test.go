package peer

import (
	"testing"
	"time"
)

func TestManagerReportsConnectionEvent(t *testing.T) {
	service, events, _ := newDiagnosticTestService(t, true, false)
	m := &Manager{diagnostics: service}
	m.reportConnection(ConnectionEvent{Direction: "outbound", Action: "dial_failed", Peer: "N0PEER-1", Endpoint: "peer.example:7300", Reason: "connection_refused"})
	select {
	case event := <-events:
		if event.stream != diagnosticTestDaily || event.line != "event=peer_connection direction=outbound action=dial_failed peer=N0PEER-1 endpoint=peer.example:7300 reason=connection_refused" {
			t.Fatalf("event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("connection diagnostic not delivered")
	}
}

func TestDirectionLabel(t *testing.T) {
	if got := directionLabel(dirInbound); got != "inbound" {
		t.Fatalf("dirInbound label = %q", got)
	}
	if got := directionLabel(dirOutbound); got != "outbound" {
		t.Fatalf("dirOutbound label = %q", got)
	}
}
