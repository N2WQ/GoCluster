package peer

import "context"

// contextOwner is a permanent child of the manager. Only its one active
// operation is replaced. Go's cancelCtx child map therefore never accumulates
// deletion history at the manager root under connection or projection churn.
// Manager.mu protects active; cancellation completes before an owner is reused.
type contextOwner struct {
	parent context.Context
	cancel context.CancelFunc
	active bool
}

type contextOperation struct {
	owner  *contextOwner
	ctx    context.Context
	cancel context.CancelFunc
}

type ContextOwnershipStats struct {
	Parents, Active, ProjectionParents int
}

// ContextOwnership reports structural occupancy without making a heap or
// native-memory claim. The corresponding allocation proof includes the pinned
// Go context/map backing and descendants of each active operation separately.
func (m *Manager) ContextOwnership() ContextOwnershipStats {
	if m == nil {
		return ContextOwnershipStats{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats := ContextOwnershipStats{Parents: len(m.contextOwners)}
	for i := range m.contextOwners {
		if m.contextOwners[i].active {
			stats.Active++
		}
	}
	for i := range m.projectionContexts {
		if m.projectionContexts[i].parent != nil {
			stats.ProjectionParents++
		}
	}
	return stats
}

func (m *Manager) initializeContextOwnersLocked() {
	if m.contextOwners != nil || m.ctx == nil {
		return
	}
	m.contextOwners = make([]contextOwner, m.cfg.MaxPeers+128)
	for i := range m.contextOwners {
		m.contextOwners[i].parent, m.contextOwners[i].cancel = context.WithCancel(m.ctx) // #nosec G118 -- Permanent owner cancellation is released by stopContextOwners after workers join.
	}
	if m.topology != nil {
		for i := range m.projectionContexts {
			m.projectionContexts[i].parent, m.projectionContexts[i].cancel = context.WithCancel(m.ctx) // #nosec G118 -- Permanent owner cancellation is released by stopContextOwners after workers join.
		}
	}
}

// beginContextOperation runs after a transport credit is reserved and before
// dialing or constructing session workers. WithCancel is deliberate even when
// an ancestor has an earlier deadline: the operation always owns a root that
// its dialer, parser and session descendants cannot bypass.
func (m *Manager) beginContextOperation() *contextOperation {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initializeContextOwnersLocked()
	if m.stopping || m.ctx == nil || m.ctx.Err() != nil {
		return nil
	}
	for i := range m.contextOwners {
		owner := &m.contextOwners[i]
		if !owner.active {
			owner.active = true
			ctx, cancel := context.WithCancel(owner.parent) // #nosec G118 -- Cancellation transfers to contextOperation and is released by endContextOperation before slot reuse.
			return &contextOperation{owner: owner, ctx: ctx, cancel: cancel}
		}
	}
	return nil
}

func (m *Manager) endContextOperation(operation *contextOperation) {
	if operation == nil {
		return
	}
	// cancelCtx removes itself from its parent synchronously. The owner may not
	// be marked free before that deletion has completed.
	operation.cancel()
	m.mu.Lock()
	operation.owner.active = false
	m.mu.Unlock()
}

func (m *Manager) stopContextOwners() {
	for i := range m.contextOwners {
		m.contextOwners[i].cancel()
	}
	for i := range m.projectionContexts {
		if cancel := m.projectionContexts[i].cancel; cancel != nil {
			cancel()
		}
	}
}
