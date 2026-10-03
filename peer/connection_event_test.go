package peer

import (
	"dxcluster/internal/peerdiag"
	"testing"
)

func TestManagerReportsConnectionEvent(t *testing.T) {
	m := &Manager{diagnostics: peerdiag.New(peerdiag.Options{Enabled: true})}
	t.Cleanup(m.diagnostics.Stop)
	m.reportConnection(ConnectionEvent{Direction: "outbound", Action: "dial_failed", Peer: "N0PEER-1", Endpoint: "peer.example:7300", Reason: "connection_refused"})
	event, ok := m.diagnostics.Next()
	if !ok || event.Kind != peerdiag.Connection || string(event.Data[:event.Length]) != "event=peer_connection direction=outbound action=dial_failed peer=N0PEER-1 endpoint=peer.example:7300 reason=connection_refused" {
		t.Fatalf("event=%+v", event)
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
