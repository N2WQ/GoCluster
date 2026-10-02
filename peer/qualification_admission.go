//go:build qualification

package peer

import "sync"

// QualificationAdmissionEvent is a read-only observation of an actual capacity
// or gate transition. Sequence orders callback delivery, not independent owners'
// physical transition times. The driver must retain only bounded evidence.
type QualificationAdmissionEvent = qualificationAdmissionEvent

type qualificationAdmissionObserver struct {
	mu       sync.Mutex
	sequence uint64
	observe  func(QualificationAdmissionEvent)
}

// QualificationSetAdmissionObserver installs a qualification-only observer.
// Callbacks are serialized, must return immediately, and must never call back
// into the protocol: some events are delivered while an ownership lock is held.
// This hook cannot grant authority or alter production timing clocks.
func (m *Manager) QualificationSetAdmissionObserver(fn func(QualificationAdmissionEvent)) {
	if fn == nil {
		m.protocol.qualification.admission.Store(nil)
		return
	}
	m.protocol.qualification.admission.Store(&qualificationAdmissionObserver{observe: fn})
}

func (p *protocolController) qualificationAdmissionEvent(event qualificationAdmissionEvent) {
	if observer := p.qualification.admission.Load(); observer != nil {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		observer.sequence++
		event.Sequence = observer.sequence
		observer.observe(event)
	}
}
