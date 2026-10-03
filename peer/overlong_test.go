package peer

import (
	"dxcluster/internal/peerdiag"
	"strings"
	"testing"
)

func TestOverlongDiagnosticOwnsBoundedPreview(t *testing.T) {
	m := &Manager{diagnostics: peerdiag.New(peerdiag.Options{})}
	t.Cleanup(m.diagnostics.Stop)
	s := &session{manager: m, peer: PeerEndpoint{host: "example"}}
	s.reportOverlong(ErrLineTooLong{Preview: strings.Repeat("x", 10000), Length: 10000, Limit: 4096, Reason: "line_limit"})
	event, ok := m.diagnostics.Next()
	if !ok || event.Kind != peerdiag.Overlong || strings.Count(string(event.Data[:event.Length]), "x") != 513 {
		t.Fatalf("overlong event=%+v", event)
	}
}
