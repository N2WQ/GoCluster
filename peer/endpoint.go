package peer

import "dxcluster/config"

// PeerEndpoint wraps a configured peer.
type PeerEndpoint struct {
	host       string
	port       int
	loginCall  string
	remoteCall string
	password   string
	preferPC9x bool
}

// Purpose: Build a peer endpoint from configuration.
// Key aspects: Copies connection credentials and preferences.
// Upstream: Peer manager initialization.
// Downstream: PeerEndpoint.ID and session creation.
func newPeerEndpoint(p config.PeeringPeer) PeerEndpoint {
	return PeerEndpoint{
		host:       p.Host,
		port:       p.Port,
		loginCall:  p.LoginCallsign,
		remoteCall: p.RemoteCallsign,
		password:   p.Password,
		preferPC9x: p.PreferPC9x,
	}
}

// Purpose: Return a stable identifier for this peer.
// Key aspects: Prefers remote callsign; falls back to host.
// Upstream: Peer manager maps and logs.
// Downstream: None.
func (p PeerEndpoint) ID() string {
	if p.remoteCall != "" {
		return p.remoteCall
	}
	return p.host
}
