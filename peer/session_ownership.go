package peer

// Pending reservations bound handshakes; owner reservations continue through
// established service and terminal callbacks until Run releases all ownership.
// Keeping the combined64+128 transport allowance until final cleanup prevents
// rapid replacements from accumulating retired sessions outside either class.
// Admission reserves both before constructing a reader, writer, or goroutine.
func (m *Manager) reserveCandidateSlots() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reserveCandidateSlotsLocked()
}

func (m *Manager) reserveCandidateSlotsLocked() bool {
	if m.stopping || (m.ctx != nil && m.ctx.Err() != nil) {
		return false
	}
	if m.pendingSlots == nil {
		m.pendingSlots = make(chan struct{}, 128)
	}
	if m.ownerSlots == nil {
		m.ownerSlots = make(chan struct{}, 64+128)
	}
	if len(m.pendingSlots) == cap(m.pendingSlots) || len(m.ownerSlots) == cap(m.ownerSlots) {
		return false
	}
	m.pendingSlots <- struct{}{}
	m.ownerSlots <- struct{}{}
	return true
}

func (m *Manager) releaseUnstartedCandidateSlots() {
	m.mu.Lock()
	defer m.mu.Unlock()
	<-m.pendingSlots
	<-m.ownerSlots
}

// releaseCandidateSlotsLocked runs only after terminal work, or before Run
// acquired ownership. Successful establishment releases just pendingReserved;
// ownerReserved always survives until this final release.
func (m *Manager) releaseCandidateSlotsLocked(s *session) {
	if s.pendingReserved {
		<-m.pendingSlots
		s.pendingReserved = false
	}
	if s.ownerReserved {
		<-m.ownerSlots
		s.ownerReserved = false
	}
}
