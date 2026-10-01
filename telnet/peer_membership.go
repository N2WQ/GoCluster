// Current telnet ownership is the source of local peer membership. These
// snapshots are bounded independently of login admission and never retain a
// second user registry. Protocol canonicalization belongs to the peer manager.
package telnet

import "sort"

// MaxPeerMembershipSessions bounds each publication snapshot without changing
// the local admission policy. Overflow pauses peering instead of publishing a
// partial population; local users remain connected.
const MaxPeerMembershipSessions = 1000

// PeerUser identifies a currently admitted owner, including the IP available at
// login. SessionID distinguishes an old connection from its replacement.
type PeerUser struct {
	SessionID uint64
	Login     string
	IP        string
}

// PeerMembership is a caller-owned snapshot. Complete=false means Users is
// empty because the raw population exceeds the supported publication bound.
type PeerMembership struct {
	Revision uint64
	RawCount int
	Complete bool
	Users    []PeerUser
}

// CurrentPeerMembership reads one coherent current population under the same
// lock that admits and replaces clients. The returned slice belongs to the
// caller; neither it nor a historical identity index is retained by the server.
func (s *Server) CurrentPeerMembership() PeerMembership {
	if s == nil {
		return PeerMembership{Complete: true}
	}
	s.clientsMutex.RLock()
	snapshot := PeerMembership{
		Revision: s.peerMembershipRevision,
		RawCount: len(s.clients),
		Complete: len(s.clients) <= MaxPeerMembershipSessions,
	}
	if snapshot.Complete {
		snapshot.Users = make([]PeerUser, 0, len(s.clients))
		for _, client := range s.clients {
			snapshot.Users = append(snapshot.Users, PeerUser{
				SessionID: client.peerSessionID,
				Login:     client.callsign,
				IP:        spotterIP(client.address),
			})
		}
	}
	s.clientsMutex.RUnlock()
	sort.Slice(snapshot.Users, func(i, j int) bool {
		return snapshot.Users[i].Login < snapshot.Users[j].Login
	})
	return snapshot
}

// SetPeerMembershipListener installs a short, nonblocking wakeup callback.
// Notifications may coalesce: consumers must fetch CurrentPeerMembership and
// use its revision rather than treating callback order as a change journal.
// This hook is separate from the dashboard's client-list listener.
func (s *Server) SetPeerMembershipListener(fn func()) {
	if s != nil {
		s.peerMembershipListener.Store(fn)
	}
}

func (s *Server) notifyPeerMembershipChange() {
	if value := s.peerMembershipListener.Load(); value != nil {
		if fn, ok := value.(func()); ok && fn != nil {
			fn()
		}
	}
}

// SendCurrentDirectMessage admits a private peer message only while this exact
// login/session pair owns the current connection. The peer manager resolves a
// canonical destination against a fresh membership snapshot first. Requiring
// its revision also rejects a canonical collision joining between lookup and
// admission, even when the original session remains current. Direct messages
// deliberately bypass bulletin deduplication.
func (s *Server) SendCurrentDirectMessage(login string, sessionID, revision uint64, line string) bool {
	if s == nil || sessionID == 0 {
		return false
	}
	message := prepareBulletinLine(line)
	if message == "" {
		return false
	}
	s.clientsMutex.RLock()
	client := s.clients[login]
	if client == nil || client.peerSessionID != sessionID || s.peerMembershipRevision != revision {
		s.clientsMutex.RUnlock()
		return false
	}
	select {
	case <-client.done:
		s.clientsMutex.RUnlock()
		return false
	default:
	}
	select {
	case client.controlChan <- controlMessage{line: message}:
		s.clientsMutex.RUnlock()
		return true
	default:
		s.clientsMutex.RUnlock()
		_ = client.controlQueueFull() //nolint:errcheck // Reports and closes; this API returns delivery failure below.
		return false
	}
}
