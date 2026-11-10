package telnet

import (
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestDedupeDiagnosticUsesOwnedDecisionAnchor(t *testing.T) {
	srv := NewServer(ServerOptions{DedupeFastEnabled: true, DedupeMedEnabled: true, DedupeSlowEnabled: true}, nil)
	c := &Client{}
	c.setDedupePolicy(dedupePolicySlow)
	c.setDiagMode(diagModeDedupe)
	s := spot.NewSpot("K1ABC", "W1XYZ", 14025, "CW")
	s.SourceType, s.DEMetadata.ADIF, s.DEMetadata.CQZone = spot.SourceRBN, 291, 5
	baseline := srv.diagDedupeTag(c, s)
	if baseline != "291|05|S|S" {
		t.Fatalf("legacy tag=%s", baseline)
	}
	s.SecondaryDedupe[2] = spot.SecondaryDedupeEvidence{Enabled: true, HasAnchor: true, AnchorHz: 14024000, ModeID: 7}
	queued := s.Clone()
	s.SecondaryDedupe[2].AnchorHz = 14030000
	tag := srv.diagDedupeTag(c, queued)
	if tag != "07@14024000|291|05|S|S" {
		t.Fatalf("tag=%s", tag)
	}
	queued.HasReport, queued.Report = true, -20
	if line := srv.formatSpotForClient(c, queued); !strings.Contains(line, "07@14024000") {
		t.Fatalf("anchor clipped from wire: %q", line)
	}
	queued.SecondaryDedupe[2].HasAnchor = false
	if tag := srv.diagDedupeTag(c, queued); tag != "07@-|291|05|S|S" {
		t.Fatalf("new entry tag=%s", tag)
	}
	c.setDedupePolicy(dedupePolicy(255))
	queued.DEGrid2 = "FN"
	queued.SecondaryDedupe[0] = spot.SecondaryDedupeEvidence{Enabled: true, HasAnchor: true, AnchorHz: 14023000, ModeID: 7}
	if tag := srv.diagDedupeTag(c, queued); tag != "07@14023000|291|FN|S|F" {
		t.Fatalf("unknown-policy legacy fallback tag=%s", tag)
	}
}
