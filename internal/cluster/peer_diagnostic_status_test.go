package cluster

import (
	"strings"
	"testing"

	"dxcluster/peer"
)

func TestV15PeerDiagnosticStatusVisibleWithoutSessions(t *testing.T) {
	sources := []dashboardIngestSource{{Label: "Peers", Enabled: true, Connected: false}}
	// A manager without a diagnostic service represents disabled diagnostics;
	// its status must still survive the disconnected-peer dashboard rendering.
	appendPeerDiagnosticStatus(sources, &peer.Manager{})
	lines := strings.Join(formatIngestSourceLines(sources), "\n")
	if !strings.Contains(lines, "Peer log disabled; dropped=0 unconfirmed=0") {
		t.Fatalf("missing independent peer-log status: %s", lines)
	}
}
