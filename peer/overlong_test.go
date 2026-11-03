package peer

import (
	"strings"
	"testing"
	"time"
)

func TestOverlongDiagnosticOwnsBoundedPreview(t *testing.T) {
	service, events, _ := newDiagnosticTestService(t, false, false)
	m := &Manager{diagnostics: service}
	s := &session{manager: m, peer: PeerEndpoint{host: "example"}}
	s.reportOverlong(ErrLineTooLong{Preview: strings.Repeat("x", 10000), Length: 10000, Limit: 4096, Reason: "line_limit"})
	select {
	case event := <-events:
		if event.stream != diagnosticTestOverlong || strings.Count(event.line, "x") != 513 {
			t.Fatalf("overlong event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("overlong diagnostic not delivered")
	}
}
