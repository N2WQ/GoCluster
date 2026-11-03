package peer

import "dxcluster/internal/peerdiag"

// reportOverlong borrows bounded reader metadata. The shared in-process logger
// owns the admitted preview and its sample file and rotation.
func (s *session) reportOverlong(line ErrLineTooLong) {
	if s.manager == nil {
		return
	}
	s.manager.diagnosticsMu.RLock()
	defer s.manager.diagnosticsMu.RUnlock()
	if s.manager.diagnostics == nil {
		return
	}
	s.manager.diagnostics.Emit(peerdiag.Overlong, peerdiag.Fields{Endpoint: s.peer.host, Reason: line.Reason, Detail: line.Preview, Count: int64(line.Length), Limit: int64(line.Limit)})
}
