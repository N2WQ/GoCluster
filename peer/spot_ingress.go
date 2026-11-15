// File role: Owns bounded peer spot work between reader parsing and shared
// validation. Credits cover builders, FIFO items and the active consumer.
package peer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"dxcluster/internal/peerdiag"
	"dxcluster/spot"
)

const (
	peerIngressCount = 64
	peerIngressBytes = 8 << 20
)

const (
	ingressFree = iota
	ingressBuilding
	ingressQueued
	ingressActive
)

// sourceIdentity deliberately has nonzero size: distinct session identities
// must never compare equal through the zero-size allocation exception.
type sourceIdentity struct{ _ byte }

// SpotIngress is a single-consumer FIFO. A manager closes it before retiring
// resources; the validator consumes it without retaining source sessions.
type SpotIngress struct {
	mu                            sync.Mutex
	changed                       *sync.Cond
	ctx                           context.Context
	cancel                        context.CancelFunc
	ready                         chan struct{}
	closed                        bool
	used, count, builders, active int
	refused                       uint64
	head, length                  int
	order                         [peerIngressCount]uint8
	jobs                          [peerIngressCount]SpotAdmission
}

// SpotAdmission is a leased fixed descriptor. Exactly one consumer must call
// Finish after TakeSpot and optional Relay. No method reads the Spot afterward.
type SpotAdmission struct {
	owner           *SpotIngress
	manager         *Manager
	identity        *sourceIdentity
	state, index    int
	charge          int
	local           *spot.Spot
	original        Frame
	fields          [8]string
	stamp, received time.Time
	key             string
}

// NewSpotIngress reserves fixed metadata independently of runtime YAML limits.
func NewSpotIngress() *SpotIngress {
	ctx, cancel := context.WithCancel(context.Background())
	q := &SpotIngress{ctx: ctx, cancel: cancel, ready: make(chan struct{}, 1)}
	q.changed = sync.NewCond(&q.mu)
	// The sole active relay can encode modern and legacy variants after the
	// reader's parse lease has ended. Reserve that workspace inside the same
	// budget even while empty; destination clones retain their existing quotas.
	q.used = pointerAllocationBytes(int(unsafe.Sizeof(*q))) + 4096 +
		2*allocationBytes(MaxPeerFrameBytes) + 1024
	return q
}

// Ready signals queued work. Take rearms it while further FIFO items exist.
func (q *SpotIngress) Ready() <-chan struct{} { return q.ready }

// Done cancels active validation when this manager's ingress retires.
func (q *SpotIngress) Done() <-chan struct{} { return q.ctx.Done() }

// Usage reports conservative owned backing, including fixed metadata and all
// reservations. It is not a whole-process heap/RSS measurement.
func (q *SpotIngress) Usage() (count, backing int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count, q.used
}

// Refusals counts count/byte/closed admission refusals without retaining keys.
func (q *SpotIngress) Refusals() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.refused
}

func (q *SpotIngress) reserve(m *Manager, f *Frame) *SpotAdmission {
	// Original clones plus a separately rewritten human comment, normalized
	// calls and the Spot object. Parser scratch remains in the existing lease.
	charge := pointerAllocationBytes(int(unsafe.Sizeof(spot.Spot{}))) + 4096 + 16
	remaining := MaxPeerFrameBytes
	bounded := true
	for _, field := range f.Fields {
		if len(field) > remaining {
			bounded = false
			break
		}
		remaining -= len(field)
		charge += allocationBytes(len(field))
	}
	if bounded {
		charge += 2 * allocationBytes(len(f.Fields[4]))
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if !bounded || q.closed || q.count == peerIngressCount || charge > peerIngressBytes-q.used {
		q.refused++
		return nil
	}
	for i := range q.jobs {
		j := &q.jobs[i]
		if j.state != ingressFree {
			continue
		}
		*j = SpotAdmission{owner: q, manager: m, state: ingressBuilding, index: i, charge: charge}
		q.count++
		q.builders++
		q.used += charge
		return j
	}
	panic("peer ingress credit/slot mismatch")
}

func (j *SpotAdmission) publish() bool {
	q := j.owner
	q.mu.Lock()
	defer q.mu.Unlock()
	q.builders--
	if q.closed {
		q.releaseLocked(j)
		return false
	}
	j.state = ingressQueued
	q.order[(q.head+q.length)%peerIngressCount] = uint8(j.index)
	q.length++
	q.signalLocked()
	q.changed.Broadcast()
	return true
}

func (q *SpotIngress) signalLocked() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// Take claims the next ready descriptor. Only the shared validator may consume.
func (q *SpotIngress) Take() *SpotAdmission {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.length == 0 || q.active != 0 {
		return nil
	}
	j := &q.jobs[q.order[q.head]]
	q.order[q.head] = 0
	q.head = (q.head + 1) % peerIngressCount
	q.length--
	q.active++
	j.state = ingressActive
	if q.length > 0 {
		q.signalLocked()
	}
	return j
}

// TakeSpot transfers local mutable ownership before any primary channel send.
func (j *SpotAdmission) TakeSpot() *spot.Spot { s := j.local; j.local = nil; return s }

// Relay uses only immutable captured fields. Validation rejection may retain
// transit eligibility; cancellation and pressure refusal must not call Relay.
func (j *SpotAdmission) Relay() {
	m := j.manager
	if j.original.Hop <= 1 || !m.shouldRelayDataFrame(j.original.Type) {
		return
	}
	select {
	case <-j.owner.Done():
		return
	default:
	}
	if m.dedupe.markSeen(j.key, j.received) {
		m.relayOriginalPeerSpotIdentity(&j.original, j.stamp, j.original.Hop-1, j.identity)
	}
}

func (q *SpotIngress) releaseLocked(j *SpotAdmission) {
	q.used -= j.charge
	q.count--
	*j = SpotAdmission{}
	q.changed.Broadcast()
}

// Finish relinquishes one builder or active lease exactly once.
func (j *SpotAdmission) Finish() {
	q := j.owner
	q.mu.Lock()
	defer q.mu.Unlock()
	switch j.state {
	case ingressBuilding:
		q.builders--
	case ingressActive:
		q.active--
	default:
		panic("peer ingress invalid completion")
	}
	q.releaseLocked(j)
}

// Close refuses new reservations, drains queued backing immediately, and joins
// builders/active work. It never waits with manager/flood locks held.
func (q *SpotIngress) Close() {
	if q == nil {
		return
	}
	q.beginClose()
	q.mu.Lock()
	for q.builders != 0 || q.active != 0 {
		q.changed.Wait()
	}
	q.mu.Unlock()
}

// beginClose does not join: Manager must first cancel parser/read owners after
// its bounded control withdrawal. Closed descriptors cannot publish new work.
func (q *SpotIngress) beginClose() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cancel()
	q.mu.Lock()
	for q.length > 0 {
		j := &q.jobs[q.order[q.head]]
		q.order[q.head] = 0
		q.head = (q.head + 1) % peerIngressCount
		q.length--
		q.releaseLocked(j)
	}
	q.mu.Unlock()
}

func (m *Manager) queuePeerSpot(q *SpotIngress, frame *Frame, source *session, stamp, received time.Time) {
	if m.maxAgeSeconds > 0 && time.Since(stamp) > time.Duration(m.maxAgeSeconds)*time.Second {
		return
	}
	j := q.reserve(m, frame)
	if j == nil {
		count, backing := q.Usage()
		m.diagnosticsMu.RLock()
		if m.diagnostics != nil {
			m.diagnostics.Emit(peerdiag.Diagnostic, peerdiag.Fields{Action: "peer_ingress_refused", Peer: sessionLabel(source), Reason: "bounded_spot_admission", Count: int64(count), Limit: peerIngressCount, Detail: fmt.Sprintf("owned_backing=%d byte_limit=%d", backing, peerIngressBytes)})
		}
		m.diagnosticsMu.RUnlock()
		return
	}
	j.original = Frame{Type: strings.Clone(frame.Type), Hop: frame.Hop, Fields: j.fields[:len(frame.Fields)]}
	for i, field := range frame.Fields {
		j.fields[i] = strings.Clone(field)
	}
	local, err := parseSpotFromFrame(&j.original, "")
	if err != nil {
		j.Finish()
		m.reportBadCallParseDrop(frame, source, err)
		return
	}
	local.Time = stamp
	j.local, j.stamp, j.received = local, stamp, received
	if source != nil {
		j.identity = source.identity
	}
	j.key = dxKey(&j.original, local)
	j.publish()
}
