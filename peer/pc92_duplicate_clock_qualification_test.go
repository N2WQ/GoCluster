//go:build qualification

package peer

import (
	"testing"
	"time"
)

func TestPC92DuplicateIngressElapsedClockDomain(t *testing.T) {
	p, source, alternate, now := controllerTestOwner(t)
	p.qualification.clock.Store(&qualificationClock{offset: 2 * time.Hour})
	frame := receiveControllerWire(t, p, source, "PC92^N2AAA^50400^C^7N3EXT^1K1USER^H1^", now)
	for _, origin := range []string{"N2AAA", "N3EXT"} {
		if got := p.graph.ingress.Value(ingressKey{origin, source.remoteCall}).Seen; got != now {
			t.Fatalf("fresh observation used authority UTC instead of elapsed admission: %v", got)
		}
	}
	// A later timestamp advances shared authority, then UTC is moved backward
	// and frozen. The old payload remains cached by its unchanged elapsed age.
	receiveControllerWire(t, p, source, "PC93^N2AAA^50401^*^K1FROM^*^newer^H1^", now.Add(time.Second))
	watermark := p.graph.freshness.Value("N2AAA")
	p.qualification.clock.Store(&qualificationClock{frozen: now.Add(-time.Hour)})
	p.receive(frame, alternate, now.Add(2*time.Second))
	for _, origin := range []string{"N2AAA", "N3EXT"} {
		observation, exists := p.graph.ingress.Get(ingressKey{origin, alternate.remoteCall})
		if !exists || observation.Seen != now {
			t.Fatalf("duplicate %s age changed with UTC control: %+v present=%v", origin, observation, exists)
		}
	}
	if p.graph.freshness.Value("N2AAA") != watermark {
		t.Fatal("duplicate changed shared authority")
	}
	if _, exists := p.pc92.firstAdmission(pc92Key(frame), now.Add(600*time.Second+time.Nanosecond)); exists {
		t.Fatal("frozen UTC prevented elapsed payload expiry")
	}
}
