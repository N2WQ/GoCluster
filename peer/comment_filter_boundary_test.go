package peer

import (
	"strings"
	"testing"

	"dxcluster/filter"
)

// The qualified envelope includes the terminal tilde; PC61's IP and PC26's
// placeholder reduce available comment bytes. Keep the tail inside one token
// so ingestion preserves it rather than consuming a mode, report or time tag.
func TestCommentFilterPeerFrameBoundary(t *testing.T) {
	const marker = "tAiL:!MaRk"
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		t.Run(kind, func(t *testing.T) {
			prefix := kind + "^14074.019^K1CMT-123^01-Oct-2026^1200Z^"
			suffix := "^W1CMT-#^N1CMT"
			switch kind {
			case "PC61":
				suffix += "^192.0.2.1"
			case "PC26":
				suffix += "^ "
			}
			suffix += "^H99^"
			commentBytes := MaxPeerFrameBytes - len(prefix) - len(suffix) - 1
			comment := strings.Repeat("A", commentBytes-len(marker)) + marker
			wire := prefix + comment + suffix
			if len(wire)+1 != MaxPeerFrameBytes {
				t.Fatal("fixture does not fill the qualified frame envelope")
			}
			fixture := newPeerSpotResourceFixture(t.Context(), t, false)
			if err := handleLeasedPeerSpot(fixture, wire); err != nil {
				t.Fatal(err)
			}
			if len(fixture.ingest) != 1 {
				t.Fatalf("local admission count=%d, want 1", len(fixture.ingest))
			}
			local := <-fixture.ingest
			if len(local.Comment) != commentBytes || local.Comment != comment || !strings.HasSuffix(local.Comment, marker) {
				t.Fatalf("ingestion changed the maximum comment or tail: bytes=%d want=%d", len(local.Comment), commentBytes)
			}
			for i, phrase := range []string{"TAIL:!MARK", "TAIL:!MISSING"} {
				_, f, err := filter.PrepareV5Configuration(filter.V5Configuration{Filters: filter.V5FilterConfiguration{
					Rules: []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: `COMMENT="` + phrase + `"`}},
				}})
				if err != nil {
					t.Fatal(err)
				}
				if got := f.Matches(local); got != (i == 0) {
					t.Fatalf("saved COMMENT %q matched=%v; want %v", phrase, got, i == 0)
				}
			}
			if used, _ := fixture.manager.parseBudget.usage(); used != 0 {
				t.Fatalf("handler retained a parse lease: %d bytes", used)
			}
			if len(fixture.ingest) != 0 || len(fixture.source.writeCh) != 0 {
				t.Fatal("handler left ingest backlog or relayed to its source")
			}
			t.Logf("frame=%d bytes including ~; stored comment=%d bytes", len(wire)+1, commentBytes)
		})
	}
}
