package peer

// eligiblePC92Record contains only authority-independent exclusions shared by
// normal receive, handshake staging and full-mailbox fallback. The caller must
// separately prove current session ownership before any mutation or gating.
func eligiblePC92Record(frame *Frame, source *session, localCall string) (*PC92Record, bool) {
	if frame == nil || source == nil || !source.pc9x || frame.Hop == 0 {
		return nil, false
	}
	if source.ctx != nil && source.ctx.Err() != nil {
		return nil, false
	}
	record, err := DecodePC92(frame)
	if err != nil || record.Origin == localCall {
		return nil, false
	}
	return record, true
}
