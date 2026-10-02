//go:build qualification

package peer

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestQualificationClockSeparatesAuthorityAndPayloadExpiry(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	p.qualification.clock.Store(&qualificationClock{offset: 2 * time.Hour})
	frame := receiveControllerWire(t, p, source, "PC92^N2AAA^50400^C^5N2AAA^1K1USER^H1^", now)
	if p.graph.nodes.Value("N2AAA") == nil || p.graph.freshness.Value("N2AAA").Accepted != now.Add(2*time.Hour) {
		t.Fatal("authority did not use controlled clock")
	}
	key := pc92Key(frame)
	if !p.pc92.contains(key, now.Add(600*time.Second)) {
		t.Fatal("authority clock affected payload expiry at strict boundary")
	}
	if p.pc92.contains(key, now.Add(600*time.Second+time.Nanosecond)) {
		t.Fatal("payload did not expire on real elapsed time")
	}
	receiveControllerWire(t, p, source, "PC93^N2AAA^50401^V0VOID^W0TEST^^renew^H1^", now.Add(time.Second))
	if p.graph.freshness.Value("N2AAA").Value != 50401 || p.graph.freshness.Value("N2AAA").Accepted != now.Add(2*time.Hour+time.Second) {
		t.Fatal("PC93 used a different authority clock")
	}
	if p.graph.nodes.Value("N2AAA").Observations != 3 || p.graph.edges != 1 {
		t.Fatal("PC93 renewal changed membership or liveness")
	}
}

func TestQualificationTopologyFixtureAndFalseGreenChecks(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := fmt.Sprintf("full=%v", full)
		t.Run(name, func(t *testing.T) {
			peers := 16
			if full {
				peers = 64
			}
			f, err := NewQualificationTopology(full, make([]string, peers))
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]int)
			edges := 0
			for i := range f.nodes {
				members := f.members(i)
				edges += len(members)
				line := qualificationFrame(qualificationCall("N0", i), "43200", "C", members, 1)
				frame, err := ParseFrame(line)
				if err != nil {
					t.Fatal(err)
				}
				record, err := DecodePC92(frame)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range record.Members {
					seen[e.Call]++
				}
			}
			if len(seen) != f.users || edges != 2*f.users {
				t.Fatalf("wrong initial population: users=%d edges=%d", len(seen), edges)
			}
			for call, count := range seen {
				if count != 2 {
					t.Fatalf("%s has%d parents", call, count)
				}
			}
			if full {
				f.SetUnavailablePeers(0xff00000000000000)
			}
			_, line, err := f.Next(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), 0)
			if err != nil {
				t.Fatal(err)
			}
			if f.Verify() == nil {
				t.Fatal("missing relays became a pass")
			}
			forwarded := strings.Replace(strings.TrimSpace(line), "H2^", "H1^", 1)
			for peer := 1; peer < peers; peer++ {
				if full && peer >= 56 {
					continue
				}
				if err := f.Observe(peer, forwarded); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.Verify(); err != nil {
				t.Fatal(err)
			}
			if err := f.Observe(1, forwarded); err == nil {
				t.Fatal("duplicate relay accepted")
			}
			if f.Verify() == nil {
				t.Fatal("duplicate output not retained in verdict")
			}
		})
	}
}

func TestQualificationTopologyMixedStreamPreservesMembership(t *testing.T) {
	f, err := NewQualificationTopology(false, make([]string, 16))
	if err != nil {
		t.Fatal(err)
	}
	g := newProtocolGraph(time.Now())
	for i := range f.nodes {
		frame, err := ParseFrame(qualificationFrame(qualificationCall("N0", i), "43200", "C", f.members(i), 1))
		if err != nil {
			t.Fatal(err)
		}
		record, err := DecodePC92(frame)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := g.prepare(record, "N0CALL-1", "P0AAA", nil)
		if err != nil {
			t.Fatal(err)
		}
		g.commit(plan, time.Now())
	}
	counts := make(map[string]int)
	for i := range 200 {
		_, line, err := f.Next(time.Date(2026, 10, 1, 12, 1, 0, 0, time.UTC).Add(time.Duration(i)*10*time.Millisecond), i)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := ParseFrame(strings.TrimSpace(line))
		if err != nil {
			t.Fatal(err)
		}
		record, err := DecodePC92(frame)
		if err != nil {
			t.Fatal(err)
		}
		counts[record.Action]++
		plan, err := g.prepare(record, "N0CALL-1", "P0AAA", nil)
		if err != nil {
			t.Fatal(err)
		}
		g.commit(plan, time.Now())
		if (i+1)%100 == 0 && (g.edges != f.users*2 || g.users.Len() != f.users) {
			t.Fatalf("cycle changed baseline: users%d edges%d", g.users.Len(), g.edges)
		}
	}
	if counts["A"] != 90 || counts["D"] != 90 || counts["C"] != 4 || counts["K"] != 16 {
		t.Fatalf("wrong mix:%v", counts)
	}
}
