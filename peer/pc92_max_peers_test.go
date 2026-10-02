package peer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
)

func TestNewManagerMaxPeersValidationBeforeResources(t *testing.T) {
	for _, n := range []int{-1, 0, 65, int(^uint(0) >> 1)} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			cfg := completeProtocolTestConfig(config.PeeringConfig{}, "N0LOCAL")
			cfg.MaxPeers = n // Invalid-construction checks bypass fixture defaults.
			cfg.Topology.DBPath = filepath.Join(t.TempDir(), "must-not-open.sqlite")
			m, err := NewManager(cfg, "N0LOCAL", nil, 0, nil)
			if err == nil {
				m.Stop()
				t.Fatal("invalid max_peers accepted")
			}
			if !strings.Contains(err.Error(), "peering.max_peers") {
				t.Fatalf("wrong construction error: %v", err)
			}
			if _, err := os.Stat(cfg.Topology.DBPath); !os.IsNotExist(err) {
				t.Fatalf("invalid cap reached persistence: %v", err)
			}
		})
	}
}

func TestConfiguredCapacityConsumers(t *testing.T) {
	for _, n := range []int{1, 2, 8, 63, 64} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			cfg := completeProtocolTestConfig(config.PeeringConfig{MaxPeers: n}, "N0LOCAL")
			for i := range n {
				cfg.Peers = append(cfg.Peers, config.PeeringPeer{Enabled: true, Direction: config.PeeringPeerDirectionBoth, Host: "peer.example.invalid", Port: 7300, RemoteCallsign: fmt.Sprintf("W%dNODE", i)})
			}
			m, err := NewManager(cfg, "N0LOCAL", nil, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(m.Stop)
			p := m.protocol
			for name, got := range map[string]int{"sessions": m.sessions.limit, "blocked": m.blockedPeers.limit, "handoff": m.admissionFailures.limit, "replay": p.replays.limit, "recovery": p.recovering.limit, "K": p.pendingK.limit} {
				if got != n {
					t.Fatalf("%s logical bound=%d want %d", name, got, n)
				}
			}
			if cap(m.pendingSlots) != 128 || cap(m.ownerSlots) != n+128 || m.ownedRuns.limit != n+128 {
				t.Fatal("cap changed ordinary pending allowance or missed retained owners")
			}
			users := make([]LocalUser, 1000)
			for i := range users {
				users[i] = LocalUser{SessionID: uint64(i + 1), Login: fmt.Sprintf("K%dUSER", i)}
			}
			m.SetMembershipProvider(func() LocalMembership { return LocalMembership{Complete: true, RawCount: len(users), Users: users} })
			for i := range n + 1 {
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				call := fmt.Sprintf("W%dNODE", i)
				s := &session{id: call, remoteCall: call, ctx: ctx, cancel: cancel}
				err := m.registerSession(s)
				if (err != nil) != (i == n) {
					t.Fatalf("registration %d under cap %d: %v", i, n, err)
				}
			}
			entries, complete := p.membershipEntries()
			if !complete || entries.Len() != 1000+n || entries.limit != 1000+n || p.published.limit != 1000+n {
				t.Fatal("complete membership lost configured direct population")
			}
			reserved := p.reservedNodes()
			if reserved == nil || reserved.Len() != n+1 || reserved.limit != n+1 || !p.publicationFits(entries) {
				t.Fatal("both-direction identities were double counted or publication omitted members")
			}
			// Exercise the transport owner limit independently of registry state.
			for range 128 {
				if !m.reserveCandidateSlots() {
					t.Fatal("ordinary pending allowance below 128")
				}
			}
			if m.reserveCandidateSlots() {
				t.Fatal("129th pending candidate admitted")
			}
			for range n {
				<-m.pendingSlots
			}
			for range n {
				if !m.reserveCandidateSlots() {
					t.Fatal("retained owner allowance below N+128")
				}
			}
			<-m.pendingSlots
			if m.reserveCandidateSlots() {
				t.Fatal("N+129 transport owner admitted")
			}
			for len(m.pendingSlots) > 0 {
				<-m.pendingSlots
			}
			for len(m.ownerSlots) > 0 {
				<-m.ownerSlots
			}
			// Direct-constructor calls are active even if cfg.Enabled is false.
			cfg.Peers = append(cfg.Peers, config.PeeringPeer{Enabled: true, RemoteCallsign: "W999NODE"})
			if extra, err := NewManager(cfg, "N0LOCAL", nil, 0, nil); err == nil {
				extra.Stop()
				t.Fatal("constructor admitted N+1 enabled identities")
			}
		})
	}
}
