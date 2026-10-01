//go:build qualification

package telnet

import (
	"sync/atomic"
	"time"
)

// QualificationEnqueue describes successful admission to one current client's
// spot queue. Strings are borrowed immutable values and must not be retained by
// the observer. ObservedAt conservatively includes any scheduling delay after
// the nonblocking channel send; it is never a timestamp reset on the spot.
type QualificationEnqueue struct {
	SessionID              uint64
	Login, Comment, DXCall string
	ObservedAt             time.Time
}

type qualificationObserver struct{ observe func(QualificationEnqueue) }

var qualificationEnqueueObserver atomic.Pointer[qualificationObserver]

// SetQualificationEnqueueObserver installs a process-local diagnostic observer
// in qualification builds only. The caller owns its bounded, nonblocking
// callback and storage lifetime; remove it after stopping network producers.
// A callback already loaded by a producer may finish after removal, so caller
// storage must remain live through teardown. There is no telemetry queue to
// silently drop observations or backpressure client delivery.
func SetQualificationEnqueueObserver(observe func(QualificationEnqueue)) {
	if observe == nil {
		qualificationEnqueueObserver.Store(nil)
		return
	}
	qualificationEnqueueObserver.Store(&qualificationObserver{observe: observe})
}

func (c *Client) observeQualificationEnqueue(env *spotEnvelope) {
	if observer := qualificationEnqueueObserver.Load(); observer != nil {
		observer.observe(QualificationEnqueue{
			SessionID: c.peerSessionID, Login: c.callsign,
			Comment: env.spot.Comment, DXCall: env.spot.DXCall,
			ObservedAt: time.Now(),
		})
	}
}
