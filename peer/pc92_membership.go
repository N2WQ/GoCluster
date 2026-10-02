package peer

import (
	"fmt"
	"time"
)

const membershipServiceInterval = 100 * time.Millisecond

// Reserve four values for each remaining membership service point in this UTC
// second. Startup, recovery and keepalives may use the rest, but cannot consume
// the final D/A and recovery C/A capacity before a notification becomes visible.
// This is pacing inside the existing100-value wire sequence, not invented timestamps or a new
// queue. Startup still has its fixed deadline if competing candidates cannot fit.
func (p *protocolController) nonMembershipCapacity(needed int) error {
	wall := p.authorityWallNow().UTC()
	remaining, err := p.timestamps.RemainingAt(wall)
	if err != nil {
		return err
	}
	ticks := int((time.Second - time.Duration(wall.Nanosecond()) + membershipServiceInterval - 1) / membershipServiceInterval)
	if remaining-needed < 4*ticks {
		return ErrTimestampRate
	}
	return nil
}

func membershipDelta(previous, current *boundedIndex[string, PC92Entry]) (removed, added []PC92Entry) {
	for call, old := range previous.All() {
		if _, ok := current.Get(call); !ok {
			removed = append(removed, old)
		}
	}
	for call, entry := range current.All() {
		if old, ok := previous.Get(call); !ok || old != entry {
			added = append(added, entry)
		}
	}
	return removed, added
}

// Check the complete delta before its first enqueue. An exhausted second must
// not leave D repeatedly reissued while its matching A waits for a later slot.
func (p *protocolController) publishDelta(recipients []*session, previous, current *boundedIndex[string, PC92Entry]) error {
	if len(recipients) == 0 {
		return nil
	}
	removed, added := membershipDelta(previous, current)
	needed := 0
	if len(removed) != 0 {
		needed++
	}
	if len(added) != 0 {
		needed++
	}
	remaining, err := p.timestamps.RemainingAt(p.authorityWallNow().UTC())
	if err != nil {
		return err
	}
	if remaining < needed {
		return ErrTimestampRate
	}
	if err := p.sendRecord(recipients, "D", removed, false); err != nil {
		return err
	}
	return p.sendRecord(recipients, "A", added, false)
}

func (p *protocolController) publishMembership(entries *boundedIndex[string, PC92Entry], now time.Time) {
	if !sameMembership(entries, p.current) {
		p.current = entries
		p.dirty = true
	}
	// Normal peers receive their delta before unrelated initial recoveries/K.
	// Peers with a captured baseline are completed and caught up separately.
	var normal []*session
	for _, s := range p.sessions() {
		if _, recovering := p.recovering.Get(s); !recovering {
			normal = append(normal, s)
		}
	}
	if p.dirty {
		if err := p.publishDelta(normal, p.published, entries); err != nil {
			p.publicationError(err, now)
			return
		}
	}
	// This generation is now admitted for every non-recovering owner. Recovery
	// owns its independent baseline; pacing it must not resend normal deltas or
	// let unrelated recoveries postpone the next ordinary membership obligation.
	p.published = entries
	p.dirty = false
	if err := p.finishRecoveries(entries); err != nil {
		p.publicationError(err, now)
		return
	}
	// Identical root/count K records share one allocation and ordered enqueue
	// across every due peer. Pending state remains bounded by configured peers.
	var keepalive []*session
	for s := range p.pendingK.All() {
		if s.ctx != nil && s.ctx.Err() != nil {
			p.pendingK.Delete(s)
		} else if _, recovering := p.recovering.Get(s); !recovering {
			keepalive = append(keepalive, s)
		}
	}
	if len(keepalive) == 0 {
		return
	}
	if err := p.nonMembershipCapacity(1); err != nil {
		p.publicationError(err, now)
		return
	}
	if err := p.sendRecord(keepalive, "K", nil, false); err != nil {
		p.publicationError(err, now)
		return
	}
	for _, s := range keepalive {
		p.pendingK.Delete(s)
	}
}

func (p *protocolController) finishRecoveries(entries *boundedIndex[string, PC92Entry]) error {
	peers := make([]*session, 0, p.recovering.Len())
	for s := range p.recovering.All() {
		peers = append(peers, s)
	}
	// Complete captured pairs first. Revisions never restart an already sent C.
	for _, s := range peers {
		state, exists := p.recovering.Get(s)
		if !exists {
			continue
		}
		if s.ctx != nil && s.ctx.Err() != nil {
			p.recovering.Delete(s)
			continue
		}
		if state.phase == 0 {
			continue
		}
		if err := p.finishRecovery(s, state, entries); err != nil {
			return err
		}
	}
	var starting []*session
	for _, s := range peers {
		if state, exists := p.recovering.Get(s); exists && state.phase == 0 {
			starting = append(starting, s)
		}
	}
	if len(starting) == 0 {
		return nil
	}
	// Reserve both records before C. Normal production therefore never begins
	// a pair in the last single slot and needlessly strands A across UTC seconds.
	remaining, err := p.timestamps.RemainingAt(p.authorityWallNow().UTC())
	if err != nil {
		return err
	}
	if remaining < 2 {
		return ErrTimestampRate
	}
	members := entryValues(entries)
	metadata, err := p.encodeRecord("A", "0", members)
	if err != nil {
		return err
	}
	if err := p.sendRecord(starting, "C", members, false); err != nil {
		return err
	}
	for _, s := range starting {
		p.recovering.Set(s, recoveryState{phase: 1, metadata: metadata})
	}
	if err := p.sendRecord(starting, "A", members, false); err != nil {
		return err
	}
	for _, s := range starting {
		p.recovering.Delete(s)
	}
	return nil
}

func (p *protocolController) finishRecovery(s *session, state recoveryState, entries *boundedIndex[string, PC92Entry]) error {
	f, err := ParseFrame(state.metadata)
	if err != nil {
		return fmt.Errorf("invalid retained recovery: %w", err)
	}
	r, err := DecodePC92(f)
	if err != nil {
		return fmt.Errorf("invalid retained recovery members: %w", err)
	}
	if state.phase == 1 {
		if err := p.sendRecord([]*session{s}, "A", r.Members, false); err != nil {
			return err
		}
		state.phase = 2
		p.recovering.Set(s, state)
	}
	baseline := newFixedIndex[string, PC92Entry](1064)
	for _, entry := range r.Members {
		baseline.Set(entry.Call, entry)
	}
	if err := p.publishDelta([]*session{s}, baseline, entries); err != nil {
		return err
	}
	p.recovering.Delete(s)
	return nil
}
