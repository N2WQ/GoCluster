package peer

import "dxcluster/internal/peerdiag"

// reportOverlong borrows only bounded reader metadata. The fixed mailbox owns
// the preview copy; the companion exclusively owns the sample file/rotation.
func (s *session) reportOverlong(line ErrLineTooLong) {
	if s.manager == nil || s.manager.diagnostics == nil {
		return
	}
	s.manager.diagnostics.Emit(peerdiag.Overlong, peerdiag.Fields{Endpoint: s.peer.host, Reason: line.Reason, Detail: line.Preview, Count: int64(line.Length), Limit: int64(line.Limit)})
}
