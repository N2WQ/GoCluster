//go:build qualification

package cluster

import (
	"testing"

	"dxcluster/internal/peerdiag"
	"dxcluster/peer"
)

func TestQ4RequiresEnabledOwnership(t *testing.T) {
	valid := peer.QualificationState{
		MaxPeers: 64, Established: 64,
		Persistence: peer.TopologyPersistenceStats{Enabled: true, ReservedBytes: 16 << 20, EngineBackingBytes: 8 << 20, HostReservedBytes: 2 << 20, Active: 1, ProjectionCommits: 1},
		Diagnostics: peerdiag.Stats{Generation: 1, Written: 1, ChargedBytes: 3 << 20},
		Contexts:    peer.ContextOwnershipStats{Parents: 192, Active: 192, ProjectionParents: 2},
	}
	valid.Pending = 128
	if err := q4EnabledOwnership(valid); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*peer.QualificationState){
		"database disabled":         func(s *peer.QualificationState) { s.Persistence.Enabled = false },
		"database uncharged":        func(s *peer.QualificationState) { s.Persistence.ReservedBytes = 0 },
		"engine absent":             func(s *peer.QualificationState) { s.Persistence.EngineBackingBytes = 0 },
		"no actual commit":          func(s *peer.QualificationState) { s.Persistence.ProjectionCommits = 0 },
		"retired database":          func(s *peer.QualificationState) { s.Persistence.Retiring = 1 },
		"failed database cleanup":   func(s *peer.QualificationState) { s.Persistence.CleanupFailed = true },
		"helper disabled":           func(s *peer.QualificationState) { s.Diagnostics.Disabled = true },
		"helper degraded":           func(s *peer.QualificationState) { s.Diagnostics.Degraded = true },
		"helper absent":             func(s *peer.QualificationState) { s.Diagnostics.Generation = 0 },
		"no actual helper write":    func(s *peer.QualificationState) { s.Diagnostics.Written = 0 },
		"helper uncharged":          func(s *peer.QualificationState) { s.Diagnostics.ChargedBytes = 0 },
		"missing fixed contexts":    func(s *peer.QualificationState) { s.Contexts.Parents-- },
		"missing active context":    func(s *peer.QualificationState) { s.Contexts.Active-- },
		"missing projection parent": func(s *peer.QualificationState) { s.Contexts.ProjectionParents-- },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			state := valid
			change(&state)
			if q4EnabledOwnership(state) == nil {
				t.Fatal("missing owned population was accepted")
			}
		})
	}
}
