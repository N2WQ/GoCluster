package cluster

import (
	"fmt"

	"dxcluster/peer"
)

// Peer logging health is independent of connectivity. Its counters remain
// readable when the companion or its disk is unavailable; no sink is called.
func appendPeerDiagnosticStatus(sources []dashboardIngestSource, manager *peer.Manager) {
	if manager == nil {
		return
	}
	stats := manager.DiagnosticStats()
	state := "ready"
	if stats.Disabled {
		state = "disabled"
	}
	if stats.Degraded {
		state = "degraded"
	}
	if stats.CleanupFailed {
		state = "cleanup-failed"
	}
	if stats.Disabled && state != "disabled" {
		state = "disabled/" + state
	}
	for i := range sources {
		if sources[i].Label == "Peers" {
			sources[i].Details = append(sources[i].Details, fmt.Sprintf("Peer log %s; dropped=%d unconfirmed=%d", state, stats.Dropped, stats.Unconfirmed))
			return
		}
	}
}
