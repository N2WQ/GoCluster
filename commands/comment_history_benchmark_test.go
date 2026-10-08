package commands

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/spot"
)

// This measures the actual ReadHistoryPage predicate, including the existing
// fake archive's closure/result costs, rather than an isolated comment matcher.
// Wrong-selector cases reveal whether selection avoids scanning irrelevant long
// comments; matching negative and tail-positive cases prevent a skipped matcher
// from masquerading as that improvement. Fixtures and query parsing are untimed.
func BenchmarkReadHistoryPageCommentSelection(b *testing.B) {
	db := historyCanonicalCTY(b)
	const phrase = "POTA: up 5!"
	for _, size := range []int{128, 1024, 65500} {
		for _, selection := range []struct {
			name, input, matchingCall, wrongCall string
			matchingADIF, wrongADIF              int
		}{
			{"exactcall", "K1ABC", "K1ABC", "K2ABC", 291, 291},
			{"entity", "K", "K1ABC", "I1ABC", 291, 248},
		} {
			for _, shape := range []string{"wrong-selector", "matching-negative", "matching-tail-positive"} {
				b.Run(fmt.Sprintf("%s/%s/bytes-%d", selection.name, shape, size), func(b *testing.B) {
					candidate := spot.NewSpot(selection.matchingCall, "W1AAA", 14030, "CW")
					candidate.DXMetadata.ADIF = selection.matchingADIF
					candidate.Comment = strings.Repeat("x", size)
					if shape == "wrong-selector" {
						candidate.DXCall, candidate.DXCallNorm = selection.wrongCall, selection.wrongCall
						candidate.DXMetadata.ADIF = selection.wrongADIF
					}
					rows := []*spot.Spot{candidate}
					wantCount := 0
					if shape == "matching-tail-positive" {
						candidate.Comment = strings.Repeat("x", size-len(phrase)) + phrase
						second := spot.NewSpot(selection.matchingCall, "W1AAA", 14031, "CW")
						second.DXMetadata.ADIF, second.Comment = selection.matchingADIF, candidate.Comment
						rows = append(rows, second)
						wantCount = 1
					}
					if len(candidate.Comment) != size {
						b.Fatal("fixture comment byte length changed")
					}
					p := NewProcessor(nil, &fakeArchive{spots: rows}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
					command, handled, text := p.ParseHistoryCommand("SHOW DX "+selection.input+" 1 COMMENT "+phrase, "go")
					if !handled || text != "" || command.Query.count != 1 || command.Query.comment != phrase {
						b.Fatalf("invalid benchmark query: %+v %v %q", command, handled, text)
					}
					now := time.Now()
					// No selection and a rejecting callback proves fakeArchive invokes
					// Match for every candidate, rather than returning an empty fixture.
					visits := 0
					guard, err := p.ReadHistoryPage(HistoryQuery{count: 1}, nil, func(*spot.Spot) bool { visits++; return false }, now, nil)
					if err != nil || len(guard.Spots) != 0 || visits != len(rows) {
						b.Fatalf("archive failed predicate guard: visits=%d rows=%d page=%+v err=%v", visits, len(rows), guard, err)
					}
					visits = 0
					guard, err = p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { visits++; return true }, now, nil)
					if err != nil || len(guard.Spots) != wantCount || visits != wantCount || (wantCount == 1 && guard.Spots[0] != candidate) {
						b.Fatalf("selection/count guard failed: visits=%d page=%+v err=%v", visits, guard, err)
					}
					match := func(*spot.Spot) bool { return true }
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						page, err := p.ReadHistoryPage(command.Query, nil, match, now, nil)
						if err != nil || len(page.Spots) != wantCount || (wantCount == 1 && page.Spots[0] != candidate) {
							b.Fatal("benchmark selection changed")
						}
					}
				})
			}
		}
	}
}
