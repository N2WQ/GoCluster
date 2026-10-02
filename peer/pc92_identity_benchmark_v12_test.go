package peer

import (
	"fmt"
	"net/netip"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

type v12DecoderFixture struct {
	name    string
	frame   *Frame
	members int
	invalid bool
}

func v12DecoderFixtures(t testing.TB) []v12DecoderFixture {
	t.Helper()
	parse := func(wire string) *Frame {
		f, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	const origin = "N99999ABCDEFG-1"
	ip := netip.MustParseAddr("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
	r := &PC92Record{Origin: origin, Timestamp: "86399.99", Action: "C", Subject: PC92Entry{Call: origin, Flags: 5, Version: "9999999999", Build: "9999999999", IP: ip}, Hop: 99}
	for i := 0; i < 1000; i++ {
		r.Members = append(r.Members, PC92Entry{Call: fmt.Sprintf("N%05dABCDEFG-1", i), Flags: 1, IP: ip})
	}
	for i := 0; i < 64; i++ {
		r.Members = append(r.Members, PC92Entry{Call: fmt.Sprintf("K%05dABCDEFG-1", i), Flags: 5, Version: "9999999999", Build: "9999999999", IP: ip})
	}
	large, err := EncodePC92(r)
	if err != nil || len(large) != 62171 {
		t.Fatalf("qualified C fixture len=%d err=%v", len(large), err)
	}
	maximal := "PC92^K1ABC^43200^C^5K1ABC^" + strings.Repeat("1K2ABC^", 8191) + "H99^"
	malformed := strings.Replace(maximal, "1K2ABC^H99^", "9K2ABC^H99^", 1)
	return []v12DecoderFixture{
		{"small-K", parse("PC92^K1ABC^43200^K^5K1ABC:5457:633^0^0^H99^"), 0, false},
		{"complete-62171-byte-C", parse(large), 1064, false},
		{"maximum-entry-C", parse(maximal), 8191, false},
		{"invalid-final-member-C", parse(malformed), 0, true},
		{"long-invalid-call", parse("PC92^K1ABC^43200^C^5K1ABC^1" + strings.Repeat("K", 65000) + "^H99^"), 0, true},
	}
}

func BenchmarkPC92V12Decode(b *testing.B) {
	for _, fixture := range v12DecoderFixtures(b) {
		b.Run(fixture.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.frame.Raw)))
			for b.Loop() {
				r, err := DecodePC92(fixture.frame)
				if fixture.invalid {
					if err == nil || r != nil {
						b.Fatal("invalid fixture accepted")
					}
				} else if err != nil || r == nil || len(r.Members) != fixture.members || r.Hop != 99 {
					b.Fatalf("decoder lost fixture authority: %+v %v", r, err)
				}
			}
		})
	}
}

func TestPC92V12DecodeAllocationBound(t *testing.T) {
	for _, fixture := range v12DecoderFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			_, _ = DecodePC92(fixture.frame) // warm regexp machinery outside measurement
			previous := debug.SetGCPercent(-1)
			defer debug.SetGCPercent(previous)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			r, err := DecodePC92(fixture.frame)
			runtime.ReadMemStats(&after)
			if (err != nil) != fixture.invalid || fixture.invalid && r != nil || !fixture.invalid && len(r.Members) != fixture.members {
				t.Fatalf("allocation fixture changed behavior: %+v %v", r, err)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			if graphScratchRaceInstrumented {
				// Race builds deliberately discard sync.Pool entries, repeatedly
				// allocating regexp scratch. Keep the behavior checks above, but
				// compare the production ceiling only in the normal build.
				t.Log("production allocation ceiling requires the normal build; race sync.Pool discard inflates cumulative regexp allocation")
			} else if allocated > graphMutationScratchBytes {
				t.Fatalf("decoder alone allocated %d beyond the shared %d-byte mutation allowance", allocated, graphMutationScratchBytes)
			}
			t.Logf("decoded %d-byte frame: %d allocated bytes; this is decoder-only evidence, not full graph scratch proof", len(fixture.frame.Raw), allocated)
		})
	}
}
