package peer

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func publicationRecords(t *testing.T, session *session) []*PC92Record {
	t.Helper()
	var records []*PC92Record
	for {
		select {
		case wire := <-session.priorityLineCh:
			records = append(records, graphRecord(t, wire))
		default:
			return records
		}
	}
}

// referenceMembership applies the relevant pinned DXSpider receiver behavior:
// C replaces identities, preserving metadata for entries already present; A
// reasserts metadata. This intentionally does not reuse protocolGraph.commit.
func referenceMembership(current map[string]PC92Entry, record *PC92Record) map[string]PC92Entry {
	switch record.Action {
	case "C":
		next := make(map[string]PC92Entry)
		for _, entry := range record.Members {
			if old, exists := current[entry.Call]; exists {
				next[entry.Call] = old
			} else {
				next[entry.Call] = entry
			}
		}
		return next
	case "A":
		for _, entry := range record.Members {
			current[entry.Call] = entry
		}
	case "D":
		for _, entry := range record.Members {
			delete(current, entry.Call)
		}
	}
	return current
}

func TestPC92PublicationRecoveryRestartsCWhenMembershipChangesBetweenCA(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after LocalUser
	}{
		{"replacement", LocalUser{SessionID: 1, Login: "K1OLD", IP: "192.0.2.1"}, LocalUser{SessionID: 2, Login: "K1NEW", IP: "192.0.2.2"}},
		{"metadata update", LocalUser{SessionID: 1, Login: "K1USER", IP: "192.0.2.1"}, LocalUser{SessionID: 2, Login: "K1USER", IP: "192.0.2.2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, receiver, _, wall := controllerTestOwner(t)
			elapsed := wall
			p.wallNow = func() time.Time { return wall }
			p.elapsedNow = func() time.Time { return elapsed }
			snapshot := LocalMembership{Revision: 1, Complete: true, RawCount: 1, Users: []LocalUser{tc.before}}
			p.manager.SetMembershipProvider(func() LocalMembership { return snapshot })
			// Leave precisely one stamp available: C can be emitted, then A
			// must wait for the next real UTC second without inventing time.
			for i := 0; i < 99; i++ {
				if _, err := p.timestamps.NextAt(wall); err != nil {
					t.Fatal(err)
				}
			}
			p.recovering.Set(receiver, recoveryState{})
			p.tick(elapsed)
			first := publicationRecords(t, receiver)
			if len(first) != 1 || first[0].Action != "C" {
				t.Fatalf("expected C before timestamp exhaustion, got %+v", first)
			}
			remote := referenceMembership(make(map[string]PC92Entry), first[0])
			if remote[tc.before.Login].IP != netip.MustParseAddr(tc.before.IP) {
				t.Fatal("initial C was not received with old membership metadata")
			}
			if err := p.request(protocolRequest{kind: "K", source: receiver}); err != nil {
				t.Fatal(err)
			}
			if len(receiver.priorityLineCh) != 0 {
				t.Fatal("ordinary K overtook unfinished recovery")
			}
			snapshot.Revision++
			snapshot.Users[0] = tc.after
			wall = wall.Add(time.Second)
			elapsed = elapsed.Add(time.Second)
			p.tick(elapsed)
			recovered := publicationRecords(t, receiver)
			if len(recovered) != 2 || recovered[0].Action != "C" || recovered[1].Action != "A" {
				t.Fatalf("changed recovery did not restart with ordered C then A: %+v", recovered)
			}
			for _, record := range recovered {
				remote = referenceMembership(remote, record)
			}
			if remote[tc.after.Login].IP != netip.MustParseAddr(tc.after.IP) {
				t.Fatal("receiver did not converge to current user IP")
			}
			if tc.before.Login != tc.after.Login {
				if _, stale := remote[tc.before.Login]; stale {
					t.Fatal("stale user survived C/A recovery revision change")
				}
			}
			if _, pending := p.recovering.Get(receiver); pending {
				t.Fatal("successful complete recovery remained pending")
			}
		})
	}
}

func TestPC92PublicationClockGateUsesElapsedFailureAndStableRecovery(t *testing.T) {
	p, first, second, wall := controllerTestOwner(t)
	elapsed := wall
	p.wallNow = func() time.Time { return wall }
	p.elapsedNow = func() time.Time { return elapsed }
	issuedAt := wall
	if _, err := p.timestamps.NextAt(wall); err != nil {
		t.Fatal(err)
	}
	legacyCtx, legacyCancel := context.WithCancel(context.Background())
	t.Cleanup(legacyCancel)
	legacy := &session{id: "legacy", remoteCall: "N5LEG", ctx: legacyCtx, cancel: legacyCancel, pc9x: false}
	p.manager.sessions.Set(legacy.id, legacy)
	pendingCtx, pendingCancel := context.WithCancel(context.Background())
	t.Cleanup(pendingCancel)
	pending := &session{ctx: pendingCtx, cancel: pendingCancel}
	p.manager.candidates.Set(pending, &candidateState{pc9x: true})
	wall = wall.Add(-2 * time.Second)
	p.tick(elapsed)
	elapsed = elapsed.Add(4999 * time.Millisecond)
	p.tick(elapsed)
	if p.clockGate || first.ctx.Err() != nil {
		t.Fatal("clock failure gate fired before its elapsed deadline")
	}
	elapsed = elapsed.Add(time.Millisecond)
	p.tick(elapsed)
	if !p.clockGate || !p.manager.pc9xGated.Load() || first.ctx.Err() == nil || second.ctx.Err() == nil || pending.ctx.Err() == nil {
		t.Fatal("sustained clock failure did not close and gate established/pending PC9x")
	}
	if legacy.ctx.Err() != nil {
		t.Fatal("PC9x clock failure closed established legacy traffic")
	}
	p.manager.sessions.Delete(first.id)
	p.manager.sessions.Delete(second.id)
	p.manager.candidates.Delete(pending)
	wall = issuedAt
	elapsed = elapsed.Add(time.Second)
	p.tick(elapsed)
	if !p.manager.pc9xGated.Load() {
		t.Fatal("catching up to the last issued second incorrectly resumed publication")
	}
	wall = issuedAt.Add(time.Second)
	elapsed = elapsed.Add(time.Second)
	p.tick(elapsed)
	elapsed = elapsed.Add(999 * time.Millisecond)
	p.tick(elapsed)
	if !p.manager.pc9xGated.Load() {
		t.Fatal("clock gate resumed before one stable second")
	}
	elapsed = elapsed.Add(time.Millisecond)
	p.tick(elapsed)
	if !p.clockGate {
		t.Fatal("a clock that nudged once then froze resumed publication")
	}
	wall = wall.Add(time.Second)
	p.tick(elapsed)
	if p.clockGate || p.manager.pc9xGated.Load() {
		t.Fatal("safe advancing clock did not resume after stable interval")
	}
}

func TestPC92PublicationOverflowCannotResumeByClosingReservedPeers(t *testing.T) {
	p, first, second, wall := controllerTestOwner(t)
	elapsed := wall
	p.wallNow = func() time.Time { return wall }
	p.elapsedNow = func() time.Time { return elapsed }
	p.manager.outboundPeers = []PeerEndpoint{{remoteCall: first.remoteCall}, {remoteCall: second.remoteCall}}
	p.manager.cfg.PC92MaxBytes = 100
	p.manager.SetMembershipProvider(func() LocalMembership { return LocalMembership{Complete: true} })
	p.tick(elapsed)
	if !p.capacityGate || first.ctx.Err() == nil || second.ctx.Err() == nil {
		t.Fatal("unrepresentable complete snapshot did not close PC9x")
	}
	p.manager.sessions.Delete(first.id)
	p.manager.sessions.Delete(second.id)
	for i := 0; i < 4; i++ {
		elapsed = elapsed.Add(time.Second)
		wall = wall.Add(time.Second)
		p.tick(elapsed)
	}
	if !p.capacityGate || !p.manager.pc9xGated.Load() {
		t.Fatal("closing peers incorrectly made reserved complete snapshot fit")
	}
	p.manager.cfg.PC92MaxBytes = 512
	elapsed = elapsed.Add(time.Second)
	wall = wall.Add(time.Second)
	p.tick(elapsed)
	if !p.capacityGate {
		t.Fatal("capacity returned without required stable interval")
	}
	elapsed = elapsed.Add(time.Second)
	wall = wall.Add(time.Second)
	p.tick(elapsed)
	if p.capacityGate || p.manager.pc9xGated.Load() {
		t.Fatal("snapshot gate did not resume after genuine stable headroom")
	}
}

func TestPC92PublicationOversizedLegacyMetadataKeepsGateClosed(t *testing.T) {
	for _, field := range []string{"version", "build"} {
		for _, hasReceiver := range []bool{false, true} {
			name := field + "/without-PC9x"
			if hasReceiver {
				name = field + "/with-PC9x"
			}
			t.Run(name, func(t *testing.T) {
				p, legacy, receiver, wall := controllerTestOwner(t)
				elapsed := wall
				p.wallNow = func() time.Time { return wall }
				p.elapsedNow = func() time.Time { return elapsed }
				legacy.pc9x = false
				legacy.remoteVersion, legacy.remoteBuild = "5457", "633"
				if field == "version" {
					legacy.remoteVersion = strings.Repeat("9", 65536)
				} else {
					legacy.remoteBuild = strings.Repeat("9", 65536)
				}
				p.manager.outboundPeers = []PeerEndpoint{{remoteCall: legacy.remoteCall}, {remoteCall: receiver.remoteCall}}
				p.manager.SetMembershipProvider(func() LocalMembership { return LocalMembership{Complete: true} })
				if !hasReceiver {
					p.manager.sessions.Delete(receiver.id)
				}
				p.dirty = true
				p.tick(elapsed)
				if !p.capacityGate || !p.manager.pc9xGated.Load() || legacy.ctx.Err() != nil {
					t.Fatal("invalid legacy metadata must gate PC9x while retaining the legacy session")
				}
				if hasReceiver && receiver.ctx.Err() == nil {
					t.Fatal("publication overflow did not close the PC9x receiver")
				}
				p.manager.sessions.Delete(receiver.id)
				for i := 0; i < 4; i++ {
					wall = wall.Add(time.Second)
					elapsed = elapsed.Add(time.Second)
					p.tick(elapsed)
					if !p.capacityGate || !p.manager.pc9xGated.Load() || !p.safeSince.IsZero() {
						t.Fatal("reserved metadata concealed the invalid live legacy metadata")
					}
					if p.current.Len() != 0 || p.published.Len() != 0 {
						t.Fatal("invalid metadata entered a retained publication generation")
					}
				}
				legacy.remoteVersion, legacy.remoteBuild = "5457", "633"
				wall = wall.Add(time.Second)
				elapsed = elapsed.Add(time.Second)
				p.tick(elapsed)
				wall = wall.Add(999 * time.Millisecond)
				elapsed = elapsed.Add(999 * time.Millisecond)
				p.tick(elapsed)
				if !p.capacityGate {
					t.Fatal("metadata recovery skipped the continuous safe interval")
				}
				wall = wall.Add(time.Millisecond)
				elapsed = elapsed.Add(time.Millisecond)
				p.tick(elapsed)
				if p.capacityGate || p.manager.pc9xGated.Load() || legacy.ctx.Err() != nil {
					t.Fatal("valid metadata did not restore PC9x admission after one stable second")
				}
				if p.current.Value(legacy.remoteCall).Version != "5457" || p.published.Value(legacy.remoteCall).Build != "633" {
					t.Fatal("recovered publication does not retain the current valid metadata")
				}
			})
		}
	}
}

func TestPC92PublicationRawPopulationOverflowRequiresStableCompleteSnapshot(t *testing.T) {
	p, first, second, wall := controllerTestOwner(t)
	elapsed := wall
	p.wallNow = func() time.Time { return wall }
	p.elapsedNow = func() time.Time { return elapsed }
	snapshot := LocalMembership{RawCount: 1001, Complete: false}
	p.manager.SetMembershipProvider(func() LocalMembership { return snapshot })
	p.tick(elapsed)
	if !p.capacityGate || first.ctx.Err() == nil || second.ctx.Err() == nil {
		t.Fatal("raw population overflow did not pause PC9x")
	}
	if snapshot.RawCount != 1001 {
		t.Fatal("peer publication gate modified local admission population")
	}
	p.manager.sessions.Delete(first.id)
	p.manager.sessions.Delete(second.id)
	snapshot = LocalMembership{Complete: true}
	elapsed = elapsed.Add(time.Second)
	wall = wall.Add(time.Second)
	p.tick(elapsed)
	snapshot = LocalMembership{RawCount: 1001, Complete: false}
	elapsed = elapsed.Add(500 * time.Millisecond)
	wall = wall.Add(time.Second)
	p.tick(elapsed)
	snapshot = LocalMembership{Complete: true}
	elapsed = elapsed.Add(500 * time.Millisecond)
	wall = wall.Add(time.Second)
	p.tick(elapsed)
	if !p.capacityGate {
		t.Fatal("brief capacity flap bypassed continuous stable recovery")
	}
	elapsed = elapsed.Add(time.Second)
	wall = wall.Add(time.Second)
	p.tick(elapsed)
	if p.capacityGate {
		t.Fatal("complete empty snapshot did not clear overflow after stable recovery")
	}
}

func TestPC92PublicationZeroPeriodicCStillRequiresRecoveryAndHonestZeroCounts(t *testing.T) {
	p, receiver, other, wall := controllerTestOwner(t)
	p.wallNow = func() time.Time { return wall }
	p.elapsedNow = func() time.Time { return wall }
	p.manager.cfg.ConfigSeconds = 0
	p.manager.cfg.KeepaliveSeconds = 0
	p.manager.cfg.NodeCount = 99
	p.manager.cfg.UserCount = 99
	p.manager.SetMembershipProvider(func() LocalMembership { return LocalMembership{Complete: true} })
	p.manager.sessions.Delete(receiver.id)
	p.manager.sessions.Delete(other.id)
	if err := p.sendRecord([]*session{receiver}, "K", nil, false); err != nil {
		t.Fatal(err)
	}
	zero := publicationRecords(t, receiver)
	if len(zero) != 1 || zero[0].NodeCount != 0 || zero[0].UserCount != 0 {
		t.Fatalf("empty real population used fallback counts: %+v", zero)
	}
	if err := p.sendRecord([]*session{receiver}, "A", nil, false); err != nil {
		t.Fatal(err)
	}
	if len(receiver.priorityLineCh) != 0 {
		t.Fatal("empty metadata recovery emitted invalid A")
	}
	// Simulate successful establishment; the manager must schedule one-shot
	// complete recovery even though both periodic timers are disabled.
	if err := p.request(protocolRequest{kind: "establish", source: receiver}); err != nil {
		t.Fatal(err)
	}
	records := publicationRecords(t, receiver)
	if len(records) < 2 || records[0].Action != "C" || records[1].Action != "A" {
		t.Fatalf("zero periodic C suppressed one-shot recovery: %+v", records)
	}
	for _, record := range records[2:] {
		if record.Action != "A" || len(record.Members) == 0 {
			t.Fatal("post-recovery publication emitted empty metadata or reordered control")
		}
	}
	if err := p.sendRecord([]*session{receiver}, "K", nil, false); err != nil {
		t.Fatal(err)
	}
	counts := publicationRecords(t, receiver)
	if len(counts) != 1 || counts[0].NodeCount != 1 || counts[0].UserCount != 0 {
		t.Fatal("established K counted local root or invented users")
	}
}
