package peer

import (
	"dxcluster/internal/peerdiag"
	"strings"
	"testing"
)

func TestHandleFrameReportsBadCallParseDrop(t *testing.T) {
	m := &Manager{}
	m.diagnostics = peerdiag.New(peerdiag.Options{Enabled: true})
	t.Cleanup(m.diagnostics.Stop)
	frame, err := ParseFrame("PC61^14074.0^BAD!^23-Dec-2025^2001Z^FT8 CQ^W1XYZ^ORIGIN^203.0.113.7^H3^")
	if err != nil {
		t.Fatalf("ParseFrame() error: %v", err)
	}

	m.HandleFrame(frame, &session{remoteCall: "SRC"})

	event, ok := m.diagnostics.Next()
	if !ok || !strings.Contains(string(event.Data[:event.Length]), "action=parse_rejected") || !strings.Contains(string(event.Data[:event.Length]), "dx=BAD! de=W1XYZ") {
		t.Fatalf("bad-call diagnostic=%+v", event)
	}
}
