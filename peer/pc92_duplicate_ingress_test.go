package peer

import (
	"fmt"
	"testing"
	"time"
)

const duplicateExternalWire = "PC92^N2AAA^43200^C^7N3EXT:5401^1K1USER^H1^"

func TestPC92DuplicateIngressUsesPayloadAdmissionAge(t *testing.T) {
	p, source, alternate, now := controllerTestOwner(t)
	frame := receiveControllerWire(t, p, source, duplicateExternalWire, now)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43201^K^5N2AAA^1^1^H1^", now.Add(time.Second))
	receiveControllerWire(t, p, source, "PC93^N2AAA^43202^*^K1FROM^*^hello^H1^", now.Add(2*time.Second))
	originWM := p.graph.freshness.Value("N2AAA")
	subjectWM := p.graph.freshness.Value("N3EXT")
	originSeen := p.graph.nodes.Value("N2AAA").Seen
	p.receive(frame, alternate, now.Add(3*time.Second))
	for _, origin := range []string{"N2AAA", "N3EXT"} {
		observation, ok := p.graph.ingress.Get(ingressKey{origin, alternate.remoteCall})
		if !ok || !observation.Seen.Equal(now) || observation.Hop != 1 {
			t.Fatalf("alternate %s age=%v present=%v; want original admission %v", origin, observation.Seen, ok, now)
		}
	}
	if p.graph.freshness.Value("N2AAA") != originWM || p.graph.freshness.Value("N3EXT") != subjectWM || p.graph.nodes.Value("N2AAA").Seen != originSeen || p.graph.nodes.Value("N3EXT").Seen != now || p.graph.edges != 2 || p.graph.users.Value("K1USER") != 1 || len(source.priorityLineCh) != 0 {
		t.Fatal("duplicate renewed authority, replayed membership, or forwarded")
	}
	// A repeated alternate observation must preserve its original age and hop.
	frame.Hop = 9
	p.receive(frame, alternate, now.Add(4*time.Second))
	if got := p.graph.ingress.Value(ingressKey{"N3EXT", alternate.remoteCall}); !got.Seen.Equal(now) || got.Hop != 1 {
		t.Fatal("known alternate observation was refreshed")
	}
	p.graph.loseIngress(source.remoteCall)
	if p.graph.ingress.Len() != 2 || p.graph.ingress.Value(ingressKey{"N3EXT", alternate.remoteCall}).Seen != now {
		t.Fatal("first-ingress loss discarded the alternate external-subject observation")
	}
}

func TestPC92DuplicateExternalIngressAtomicCapacity(t *testing.T) {
	for _, slots := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(slots), func(t *testing.T) {
			p, source, alternate, now := controllerTestOwner(t)
			frame := receiveControllerWire(t, p, source, duplicateExternalWire, now)
			for i := 0; p.graph.ingress.Len() < maxIngressObservations-slots; i++ {
				p.graph.observe(fmt.Sprintf("W%dAA", i/64), fmt.Sprintf("W%dPEER", i%64), 8, now, false)
			}
			beforeCount, beforeBytes := p.graph.ingress.Len(), p.graph.ingressBytes
			beforeWM := p.graph.freshness.Value("N2AAA")
			p.receive(frame, alternate, now.Add(time.Second))
			wantAdded := 0
			if slots == 2 {
				wantAdded = 2
			}
			if p.graph.ingress.Len() != beforeCount+wantAdded || p.graph.freshness.Value("N2AAA") != beforeWM || p.graph.edges != 2 {
				t.Fatal("duplicate capacity boundary partially changed authority")
			}
			for _, origin := range []string{"N2AAA", "N3EXT"} {
				_, exists := p.graph.ingress.Get(ingressKey{origin, alternate.remoteCall})
				if exists != (slots == 2) {
					t.Fatal("duplicate pair was partially admitted")
				}
			}
			if slots < 2 && (alternate.ctx.Err() == nil || p.graph.ingressBytes != beforeBytes || !p.manager.blockedPeers.Value(alternate.remoteCall)) {
				t.Fatal("refusal failed to gate affected session or changed owned strings")
			}
			if slots == 2 && alternate.ctx.Err() != nil {
				t.Fatal("exact two-slot admission closed healthy session")
			}
		})
	}
}

func TestPC92DuplicateExternalIngressByteBoundary(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			p, source, alternate, now := controllerTestOwner(t)
			frame := receiveControllerWire(t, p, source, duplicateExternalWire, now)
			// Each new observation: 160 fixed + two independent rounded 8-byte
			// strings. Literal arithmetic keeps the oracle out of the helper.
			const pairCharge = 2 * (160 + 8 + 8)
			p.graph.metadataBytes += (96 << 20) - p.graph.retainedCharge() - pairCharge + extra
			beforeCount, beforeBytes, beforeCharge := p.graph.ingress.Len(), p.graph.ingressBytes, p.graph.retainedCharge()
			p.receive(frame, alternate, now.Add(time.Second))
			if extra == 1 {
				if alternate.ctx.Err() == nil || p.graph.ingress.Len() != beforeCount || p.graph.ingressBytes != beforeBytes || p.graph.retainedCharge() != beforeCharge {
					t.Fatal("one-byte-over duplicate partially admitted or escaped refusal")
				}
			} else if alternate.ctx.Err() != nil || p.graph.ingress.Len() != beforeCount+2 || p.graph.retainedCharge() != 96<<20 {
				t.Fatal("exact byte boundary failed complete admission")
			}
		})
	}
}

func TestPC92DuplicateExternalPreservesKnownObservation(t *testing.T) {
	p, source, alternate, now := controllerTestOwner(t)
	frame := receiveControllerWire(t, p, source, duplicateExternalWire, now)
	original := now.Add(-time.Minute)
	p.graph.observe("N2AAA", alternate.remoteCall, 7, original, false)
	// Only the external subject is missing. Exactly one observation's charge
	// remains; reserving the already-known origin again would falsely refuse.
	p.graph.metadataBytes += (96 << 20) - p.graph.retainedCharge() - (160 + 8 + 8)
	p.receive(frame, alternate, now.Add(time.Second))
	if alternate.ctx.Err() != nil || p.graph.ingress.Len() != 4 || p.graph.retainedCharge() != 96<<20 {
		t.Fatal("one missing observation did not use exactly its own reservation")
	}
	if got := p.graph.ingress.Value(ingressKey{"N2AAA", alternate.remoteCall}); got.Seen != original || got.Hop != 7 {
		t.Fatal("duplicate replaced an already-known observation")
	}
	if got := p.graph.ingress.Value(ingressKey{"N3EXT", alternate.remoteCall}); got.Seen != now || got.Hop != 1 {
		t.Fatal("missing external observation did not inherit original payload admission")
	}
}

func TestDedupeFirstAdmissionDoesNotRefresh(t *testing.T) {
	now := time.Now()
	c := newBoundedDedupe(600*time.Second, 2, 64)
	if c.admit("payload", now) != dedupeAccepted {
		t.Fatal("setup")
	}
	if c.admit("payload", now.Add(300*time.Second)) != dedupeDuplicate {
		t.Fatal("duplicate")
	}
	for _, elapsed := range []time.Duration{301 * time.Second, 600 * time.Second} {
		first, ok := c.firstAdmission("payload", now.Add(elapsed))
		if !ok || first.Sub(now) != 0 {
			t.Fatal("lookup refreshed age or changed original monotonic instant")
		}
	}
	if _, ok := c.firstAdmission("payload", now.Add(600*time.Second+time.Nanosecond)); ok {
		t.Fatal("strictly expired entry retained observation authority")
	}
	if count, bytes, _ := c.occupancy(); count != 0 || bytes != 0 {
		t.Fatal("lookup retained expired key backing")
	}
}

func TestPC92DuplicateExpiredPayloadCannotCreateIngress(t *testing.T) {
	p, source, alternate, now := controllerTestOwner(t)
	frame := receiveControllerWire(t, p, source, duplicateExternalWire, now)
	p.receive(frame, alternate, now.Add(600*time.Second+time.Nanosecond))
	if p.graph.ingress.Len() != 2 || p.graph.ingress.Value(ingressKey{"N3EXT", alternate.remoteCall}).Hop != 0 {
		t.Fatal("expired duplicate created alternate authority")
	}
}
