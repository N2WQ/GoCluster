package peer

import "time"

// PC92 retains only monotonic observation and factual qualification emission.
// Cache health intervals no longer control retry eligibility.
type dedupeRecovery struct {
	observedAt time.Time
	emit       func(qualificationAdmissionEvent)
}

func (c *dedupeCache) recoveryEventLocked(kind, key string, at, admissionAt, expiry time.Time) {
	if c.recovery != nil && c.recovery.emit != nil {
		c.recovery.emit(qualificationAdmissionEvent{Kind: kind, Key: key, At: at, AdmissionAt: admissionAt, LogicalExpiry: expiry, PC92Keys: c.items.Len(), PC92KeyBytes: c.bytes})
	}
}
