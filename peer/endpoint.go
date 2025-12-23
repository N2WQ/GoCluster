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

func (p PeerEndpoint) ID() string {
	if p.remoteCall != "" {
		return p.remoteCall
	}
	return p.host
}
