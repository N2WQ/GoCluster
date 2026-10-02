package peer

import "time"

type admissionCause uint8

const (
	admissionAuthority admissionCause = iota
	admissionMailbox
	admissionIngress
)

func (c admissionCause) String() string {
	switch c {
	case admissionMailbox:
		return "mailbox"
	case admissionIngress:
		return "alternate_ingress"
	default:
		return "new_authority"
	}
}

// The controller retains only the current diagnostic failure per identity.
// Retry eligibility belongs to Manager.mu; no refused record is retained.
type admissionEpisode struct {
	generation uint64
	cause      admissionCause
	at         time.Time
}

func laterAdmissionTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func (p *protocolController) emitMailboxLocked(now time.Time) {
	p.qualificationAdmissionEvent(qualificationAdmissionEvent{Kind: "mailbox", At: now, InputPC92: p.queued[0], InputPC92Bytes: p.bytes[0]})
}
