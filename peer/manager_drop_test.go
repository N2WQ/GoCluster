package peer

import (
	"strings"
	"testing"
	"time"
)

func TestHandleFrameReportsBadCallParseDrop(t *testing.T) {
	m := &Manager{}
	service, events, _ := newDiagnosticTestService(t, true, false)
	m.diagnostics = service
	frame, err := ParseFrame("PC61^14074.0^BAD!^23-Dec-2025^2001Z^FT8 CQ^W1XYZ^ORIGIN^203.0.113.7^H3^")
	if err != nil {
		t.Fatalf("ParseFrame() error: %v", err)
	}

	m.HandleFrame(frame, &session{remoteCall: "SRC"})

	select {
	case event := <-events:
		if !strings.Contains(event.line, "action=parse_rejected") || !strings.Contains(event.line, "dx=BAD! de=W1XYZ") {
			t.Fatalf("bad-call diagnostic=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("parse diagnostic not delivered")
	}
}
