//go:build !qualification

package peer

import (
	"errors"
	"time"
)

// Production builds have no qualification clock, observer, or mutable hook.
type protocolQualificationState struct{}
type protocolQualificationRequest struct{}

func (p *protocolController) authorityWallNow() time.Time                                        { return p.wallNow() }
func (*protocolController) qualificationPublicationAdmitted(*session, string, string, time.Time) {}
func (*protocolController) qualificationAdmissionEvent(qualificationAdmissionEvent)              {}

func (p *protocolController) qualificationAuthorityTime(now time.Time) time.Time {
	// Keep the zero-sized tag-selected field/type visible to normal-build static
	// analysis; production has no mutable qualification state or clock offset.
	_ = p.qualification
	return now
}
func (*protocolController) handleQualificationRequest(*protocolQualificationRequest) error {
	return errors.New("qualification hooks are not built")
}
