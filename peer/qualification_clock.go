//go:build qualification

package peer

import (
	"context"
	"errors"
	"time"
)

// The qualification driver owns fault application independently of the actor.
// Immutable settings are replaced atomically, so a saturated lifecycle mailbox
// cannot delay the start of the measured fault. No deadline/TTL clock is changed.
type qualificationClock struct {
	offset time.Duration
	frozen time.Time
}

func (p *protocolController) authorityWallNow() time.Time {
	return p.qualificationAuthorityTime(p.wallNow())
}

// QualificationApplyClockFault returns a conservative monotonic application
// instant captured immediately before the atomic replacement. Tests serialize
// applications; the pointer retains only the current setting, not fault history.
func (m *Manager) QualificationApplyClockFault(ctx context.Context, offset time.Duration, freeze bool) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if m == nil || m.protocol == nil || m.ctx == nil || m.ctx.Err() != nil {
		return time.Time{}, errors.New("peer manager not running")
	}
	clock := &qualificationClock{offset: offset}
	applied := time.Now()
	if freeze {
		clock.frozen = applied.Add(offset).UTC()
	}
	m.protocol.qualification.clock.Store(clock)
	return applied, nil
}

// QualificationPublication records successful admission, not network delivery.
// Consumers must copy anything they retain into independently bounded storage.
type QualificationPublication struct {
	Peer, Action, Wire string
	At                 time.Time
}
type qualificationPublicationObserver struct {
	observe func(QualificationPublication)
}

// QualificationSetPublicationObserver installs a nonblocking external observer.
// The immutable callback is qualification-only and may not mutate protocol state.
func (m *Manager) QualificationSetPublicationObserver(fn func(QualificationPublication)) {
	if fn == nil {
		m.protocol.qualification.publication.Store(nil)
		return
	}
	m.protocol.qualification.publication.Store(&qualificationPublicationObserver{observe: fn})
}

func (p *protocolController) qualificationPublicationAdmitted(s *session, action, wire string, at time.Time) {
	if observer := p.qualification.publication.Load(); observer != nil {
		observer.observe(QualificationPublication{Peer: s.remoteCall, Action: action, Wire: wire, At: at})
	}
}
