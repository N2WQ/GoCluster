package peer

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"time"
)

func entryValues(entries *boundedIndex[string, PC92Entry]) []PC92Entry {
	calls := make([]string, 0, entries.Len())
	for call := range entries.All() {
		calls = append(calls, call)
	}
	sort.Strings(calls)
	out := make([]PC92Entry, 0, len(calls))
	for _, call := range calls {
		out = append(out, entries.Value(call))
	}
	return out
}
func (p *protocolController) rootEntry() PC92Entry {
	m := p.manager
	return PC92Entry{Call: m.localCall, Flags: uint8(m.cfg.PC92Bitmap), Version: m.cfg.NodeVersion, Build: m.cfg.NodeBuild}
}
func (p *protocolController) membershipEntries() (*boundedIndex[string, PC92Entry], bool) {
	m := p.manager
	snapshot := m.membership()
	if !snapshot.Complete || snapshot.RawCount > 1000 || len(snapshot.Users) > 1000 {
		return nil, false
	}
	entries := newFixedIndex[string, PC92Entry](1064)
	reserved := p.reservedNodes()
	if reserved == nil {
		return nil, false
	}
	counts := newFixedIndex[string, int](1000)
	// The snapshot was bounded above before either index can receive a key.
	// At most1000 user identities plus64 registered peers enter entries.
	for _, user := range snapshot.Users {
		call, ok := CanonicalPC92Call(user.Login)
		if ok && len(call) <= 15 {
			counts.Set(call, counts.Value(call)+1)
		}
	}
	excluded := 0
	for _, user := range snapshot.Users {
		call, ok := CanonicalPC92Call(user.Login)
		if !ok || len(call) > 15 || counts.Value(call) != 1 || reserved.Value(call) {
			excluded++
			continue
		}
		e := PC92Entry{Call: call, Flags: 1}
		if ip, err := netip.ParseAddr(user.IP); err == nil {
			e.IP = ip.Unmap()
		}
		entries.Set(call, e)
	}
	if excluded > 0 {
		p.diagnostic("local identities excluded from PC92 publication")
	}
	m.mu.RLock()
	for _, s := range m.sessions.All() {
		if !s.remotePublicationMetadataOK() {
			m.mu.RUnlock()
			return nil, false
		}
		e := p.remoteEntry(s)
		if e.Call != "" {
			entries.Set(e.Call, e)
		}
	}
	m.mu.RUnlock()
	return entries, true
}
func (p *protocolController) reservedNodes() *boundedIndex[string, bool] {
	m := p.manager
	// Validated configuration admits at most64 enabled peers. Refuse an invalid
	// constructor caller's oversized registry as a whole, never a partial set.
	nodes := newFixedIndex[string, bool](65)
	nodes.Set(m.localCall, true)
	for _, e := range m.outboundPeers {
		if nodes.Set(e.remoteCall, true) == nil {
			return nil
		}
	}
	for _, e := range m.inboundPeers {
		if nodes.Set(e.remoteCall, true) == nil {
			return nil
		}
	}
	return nodes
}
func (p *protocolController) frameLimit() int {
	limit := 65536
	for _, value := range []int{p.manager.cfg.MaxLineLength, p.manager.cfg.PC92MaxBytes} {
		if value > 0 && value < limit {
			limit = value
		}
	}
	return limit
}
func validPublicationEntry(e PC92Entry) bool {
	return len(e.Call) <= 15 && len(e.Version) <= 10 && len(e.Build) <= 10
}
func (p *protocolController) encodeRecord(action, stamp string, members []PC92Entry) (string, error) {
	r := &PC92Record{Origin: p.manager.localCall, Timestamp: stamp, Action: action, Subject: p.rootEntry(), Members: members, Hop: p.manager.cfg.HopCount}
	if r.Hop <= 0 {
		r.Hop = 99
	}
	if !validPublicationEntry(r.Subject) {
		return "", fmt.Errorf("local publication metadata limit")
	}
	for _, e := range members {
		if !validPublicationEntry(e) {
			return "", fmt.Errorf("member publication metadata limit")
		}
	}
	if action == "K" {
		current, ok := p.membershipEntries()
		if !ok {
			return "", fmt.Errorf("local membership unavailable")
		}
		for _, e := range current.All() {
			if e.IsNode() {
				r.NodeCount++
			} else {
				r.UserCount++
			}
		}
	}
	wire, err := EncodePC92(r)
	if err != nil {
		return "", err
	}
	if len(wire) > p.frameLimit() {
		return "", fmt.Errorf("complete PC92 publication exceeds byte limit")
	}
	return wire, nil
}
func (p *protocolController) publicationFits(entries *boundedIndex[string, PC92Entry]) bool {
	// Reserve maximum metadata for every enabled peer, even while disconnected.
	// Closing those links cannot itself make this gate resume and oscillate.
	reserved := newFixedIndex[string, PC92Entry](1128)
	for call, e := range entries.All() {
		// Check present metadata before replacing it with the disconnected-peer
		// reservation. Otherwise an oversized legacy peer could repeatedly clear
		// this gate, or enter retained snapshots when no PC9x recipient exists.
		if !validPublicationEntry(e) {
			return false
		}
		if reserved.Set(call, e) == nil {
			return false
		}
	}
	ip := netip.MustParseAddr("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
	nodes := p.reservedNodes()
	if nodes == nil {
		return false
	}
	for call := range nodes.All() {
		if call == p.manager.localCall {
			continue
		}
		if reserved.Set(call, PC92Entry{Call: call, Flags: 5, Version: "9999999999", Build: "9999999999", IP: ip}) == nil {
			return false
		}
	}
	_, err := p.encodeRecord("C", "86399.99", entryValues(reserved))
	return err == nil
}
func (p *protocolController) sendRecord(recipients []*session, action string, members []PC92Entry, _ bool) error {
	if len(recipients) == 0 || ((action == "A" || action == "D") && len(members) == 0) {
		return nil
	}
	wall := p.wallNow().UTC()
	if wall.Unix() <= p.startupSecond || wall.Unix() <= p.clockFloor || wall.Before(p.observedWall) {
		return ErrTimestampClock
	}
	stamp, err := p.timestamps.NextAt(wall)
	if err != nil {
		return err
	}
	wire, err := p.encodeRecord(action, stamp, members)
	if err != nil {
		return err
	}
	for _, s := range recipients {
		if err := s.sendControlLine(wire); err != nil {
			p.diagnostic("PC92 control output refused")
			s.close()
		}
	}
	p.lastPublish = p.elapsedNow()
	p.unsafeSince = time.Time{}
	return nil
}
func (p *protocolController) gate(reason string) {
	p.manager.pc9xGated.Store(true)
	p.diagnostic(reason)
	for _, s := range p.sessions() {
		s.close()
	}
	p.manager.mu.RLock()
	var pending []*session
	for s, c := range p.manager.candidates.All() {
		if c.pc9x {
			pending = append(pending, s)
		}
	}
	p.manager.mu.RUnlock()
	for _, s := range pending {
		s.close()
	}
}
func (p *protocolController) tick(now time.Time) {
	if p.quiescing {
		return
	}
	// UTC deliberately removes Go's monotonic reading. Wall-clock health must
	// compare UTC; elapsed deadlines below keep their independent monotonic clock.
	wall := p.wallNow().UTC()
	regressed := !p.observedWall.IsZero() && wall.Before(p.observedWall)
	if p.observedWall.IsZero() || wall.After(p.observedWall) {
		p.observedWall, p.wallAdvancedAt = wall, now
	}
	clockErr := p.timestamps.ClockSafe(wall)
	if regressed {
		p.clockFloor = p.observedWall.Unix()
		clockErr = ErrTimestampClock
	}
	if now.Sub(p.wallAdvancedAt) >= 5*time.Second {
		// A frozen clock may never exhaust the 100-value sequence when traffic
		// is quiet. Detect lack of UTC progress independently of publication.
		p.clockFloor = p.observedWall.Unix()
		if p.unsafeSince.IsZero() {
			p.unsafeSince = p.wallAdvancedAt
		}
		clockErr = ErrTimestampClock
	}
	if wall.Unix() <= p.startupSecond || wall.Unix() <= p.clockFloor {
		clockErr = ErrTimestampClock
	}
	if clockErr != nil {
		if p.unsafeSince.IsZero() {
			p.unsafeSince = now
		}
		if now.Sub(p.unsafeSince) >= 5*time.Second && !p.clockGate {
			p.clockGate = true
			p.gate("PC9x gated: unsafe local clock")
		}
		p.safeSince = time.Time{}
		return
	}
	entries, complete := p.membershipEntries()
	fits := complete && p.publicationFits(entries)
	if !fits {
		if !p.capacityGate {
			p.capacityGate = true
			p.gate("PC9x gated: complete local snapshot unavailable")
		}
		p.safeSince = time.Time{}
		return
	}
	if p.clockGate || p.capacityGate {
		if p.safeSince.IsZero() {
			p.safeSince = now
			p.recoveryWall = wall
			return
		}
		if now.Sub(p.safeSince) < time.Second || (p.clockGate && !wall.After(p.recoveryWall)) {
			return
		}
		p.clockGate = false
		p.capacityGate = false
		p.unsafeSince = time.Time{}
		p.manager.pc9xGated.Store(false)
		p.dirty = true
		p.diagnostic("PC9x admission resumed; complete recovery required")
	}
	// Every class retains its own headroom. Do not redial a PC92 authority failure
	// merely because closing its transport freed an unrelated resource.
	p.drainFailures()
	// Snapshot the bounded keys before updating values: index traversals permit
	// deleting the yielded entry, but must never span a Set.
	blocked := make([]string, 0, p.blocked.Len())
	for call := range p.blocked.All() {
		blocked = append(blocked, call)
	}
	for _, call := range blocked {
		since := p.blocked.Value(call)
		if !p.admissionHeadroom(call) {
			p.blocked.Set(call, time.Time{})
			continue
		}
		if since.IsZero() {
			p.blocked.Set(call, now)
			continue
		}
		if now.Sub(since) < time.Second {
			continue
		}
		p.manager.mu.Lock()
		p.manager.blockedPeers.Delete(call)
		p.manager.mu.Unlock()
		p.blocked.Delete(call)
		p.blockedRecords.Delete(call)
		p.blockedInput.Delete(call)
	}
	if !sameMembership(entries, p.current) {
		p.revision++
		p.current = entries
	}
	members := entryValues(entries)
	recovering := make([]*session, 0, p.recovering.Len())
	for s := range p.recovering.All() {
		recovering = append(recovering, s)
	}
	for _, s := range recovering {
		recovery := p.recovering.Value(s)
		if s.ctx != nil && s.ctx.Err() != nil {
			p.recovering.Delete(s)
			continue
		}
		if recovery.phase == 0 || recovery.revision != p.revision {
			if err := p.sendRecord([]*session{s}, "C", members, false); err != nil {
				p.publicationError(err, now)
				return
			}
			p.recovering.Set(s, recoveryState{phase: 1, revision: p.revision})
		}
		if err := p.sendRecord([]*session{s}, "A", members, false); err != nil {
			p.publicationError(err, now)
			return
		}
		p.recovering.Delete(s)
	}
	for s := range p.pendingK.All() {
		if s.ctx != nil && s.ctx.Err() != nil {
			p.pendingK.Delete(s)
			continue
		}
		if err := p.sendRecord([]*session{s}, "K", nil, false); err != nil {
			p.publicationError(err, now)
			return
		}
		p.pendingK.Delete(s)
	}
	if !p.dirty || (!p.lastPublish.IsZero() && now.Sub(p.lastPublish) < 100*time.Millisecond) {
		return
	}
	var removed, added []PC92Entry
	for call, old := range p.published.All() {
		if _, ok := entries.Get(call); !ok {
			removed = append(removed, old)
		}
	}
	for call, e := range entries.All() {
		old, ok := p.published.Get(call)
		if !ok || old != e {
			added = append(added, e)
		}
	}
	recipients := p.sessions()
	if len(removed) > 0 {
		if err := p.sendRecord(recipients, "D", removed, false); err != nil {
			p.publicationError(err, now)
			return
		}
	}
	if len(added) > 0 {
		if err := p.sendRecord(recipients, "A", added, false); err != nil {
			p.publicationError(err, now)
			return
		}
	}
	p.published = entries
	p.dirty = false
}
func (p *protocolController) publicationError(err error, now time.Time) {
	if errors.Is(err, ErrTimestampRate) || errors.Is(err, ErrTimestampClock) {
		if p.unsafeSince.IsZero() {
			p.unsafeSince = now
		}
		if now.Sub(p.unsafeSince) >= 5*time.Second {
			p.clockGate = true
			p.gate("PC9x gated: timestamp progress unavailable")
		}
		return
	}
	p.capacityGate = true
	p.gate("PC9x gated: publication encoding unavailable")
}

func sameMembership(a, b *boundedIndex[string, PC92Entry]) bool {
	if a.Len() != b.Len() {
		return false
	}
	for call, e := range a.All() {
		if other, ok := b.Get(call); !ok || other != e {
			return false
		}
	}
	return true
}

// waitStartupSecond prevents a normal same-second restart from issuing an
// integer timestamp below the old process's .NN. The wait is bounded and only
// affects PC9x: a stalled startup clock gates its links while local service lives.
func (p *protocolController) waitStartupSecond(ctx context.Context) error {
	p.startupSecond = p.wallNow().Unix()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for p.wallNow().Unix() <= p.startupSecond {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			p.clockGate = true
			p.manager.pc9xGated.Store(true)
			return nil
		case <-poll.C:
		}
	}
	return nil
}
