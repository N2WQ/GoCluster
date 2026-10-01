package peer

import (
	"fmt"
	"testing"
	"time"
)

func TestPC92ResourceExternalIngressReservationIsAtomic(t *testing.T) {
	p, source, alternate, now := controllerTestOwner(t)
	// Unrelated owners occupy all but one observation slot. Neither incoming
	// identity has an observation, so external publication needs two new slots.
	for i := 0; i < maxIngressObservations-1; i++ {
		p.graph.observe(fmt.Sprintf("W%dAA", i/64), fmt.Sprintf("W%dPEER", i%64), 8, now, false)
	}
	beforeBytes, beforeCharge := p.graph.ingressBytes, p.graph.retainedCharge()
	frame := receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^7N3EXT:5401^1K1USER^H1^", now)
	if source.ctx.Err() == nil || !p.manager.blockedPeers.Value(source.remoteCall) {
		t.Fatal("two-slot ingress refusal did not close and gate the affected peer")
	}
	if p.graph.ingress.Len() != maxIngressObservations-1 || p.graph.ingressBytes != beforeBytes || p.graph.retainedCharge() != beforeCharge {
		t.Fatal("failed external admission partially consumed ingress capacity")
	}
	if p.graph.nodes.Len() != 0 || p.graph.users.Len() != 0 || p.graph.edges != 0 || p.graph.freshness.Len() != 0 {
		t.Fatal("failed external admission changed topology or shared freshness")
	}
	if count, _, _ := p.pc92.occupancy(); count != 0 || p.pc92.contains(pc92Key(frame), now) {
		t.Fatal("failed external admission consumed dedupe authority")
	}
	for key, observation := range p.graph.ingress.All() {
		if observation.Seen != now || observation.Hop != 8 || key.Origin == "N2AAA" || key.Origin == "N3EXT" {
			t.Fatal("failed external admission altered an existing ingress observation")
		}
	}
	// Releasing one unrelated owner leaves exactly two slots. The same frame
	// must remain admissible through another live peer after the first refusal.
	key := ingressKey{"W0AA", "W0PEER"}
	p.graph.ingress.Delete(key)
	p.graph.ingressBytes -= ingressEntryBytes(key.Origin, key.Ingress)
	p.receive(frame, alternate, now)
	if alternate.ctx.Err() != nil || p.graph.ingress.Len() != maxIngressObservations || p.graph.users.Value("K1USER") != 1 {
		t.Fatal("external record did not succeed after both ingress slots became available")
	}
	for _, origin := range []string{"N2AAA", "N3EXT"} {
		if _, ok := p.graph.ingress.Get(ingressKey{origin, alternate.remoteCall}); !ok {
			t.Fatalf("accepted external record lacks ingress for %s", origin)
		}
		if watermark, ok := p.graph.freshness.Get(origin); !ok || watermark.Value != 43200 {
			t.Fatalf("accepted external record lacks freshness for %s", origin)
		}
	}
}

func TestPC92ResourceOrdinaryIngressStringsConsumeByteBudget(t *testing.T) {
	p, source, alternate, now := controllerTestOwner(t)
	const origin = "N2AAA"
	const graphLimit = 96 << 20
	// Inject preexisting retained metadata at the admission boundary. The new
	// empty C needs one node, its call metadata, one watermark, and one ingress
	// observation including BOTH owned strings. The two peer calls are equal
	// length, so retrying through the alternate has the same exact charge.
	newBytes := 512 + 8 + 196 + 160 + 16 // 5/6-byte calls each own an 8-byte allocation.
	p.graph.metadataBytes = graphLimit - graphMutationScratchBytes - newBytes + 1
	beforeCharge := p.graph.retainedCharge()
	frame := receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA^H1^", now)
	if source.ctx.Err() == nil {
		t.Fatal("ordinary-origin ingress strings were omitted from byte admission")
	}
	if p.graph.retainedCharge() != beforeCharge || p.graph.nodes.Len() != 0 || p.graph.ingress.Len() != 0 || p.graph.ingressBytes != 0 || p.graph.freshness.Len() != 0 {
		t.Fatal("one-byte-over-budget record changed retained authority")
	}
	if count, _, _ := p.pc92.occupancy(); count != 0 || p.pc92.contains(pc92Key(frame), now) {
		t.Fatal("byte-refused record entered the payload cache")
	}
	p.graph.metadataBytes--
	p.receive(frame, alternate, now)
	if alternate.ctx.Err() != nil || p.graph.nodes.Len() != 1 || p.graph.ingress.Len() != 1 || p.graph.freshness.Value(origin).Value != 43200 {
		t.Fatal("exact-budget retry did not acquire authority")
	}
	wantIngressBytes := 16
	if p.graph.ingressBytes != wantIngressBytes || p.graph.retainedCharge() != graphLimit {
		t.Fatalf("accepted ingress charge = %d strings, %d graph; want %d strings, %d graph", p.graph.ingressBytes, p.graph.retainedCharge(), wantIngressBytes, graphLimit)
	}
	p.graph.loseIngress(alternate.remoteCall)
	if p.graph.ingressBytes != 0 || p.graph.ingress.Len() != 0 || p.graph.retainedCharge() != graphLimit-160-wantIngressBytes {
		t.Fatal("ingress removal retained its owned string or observation charge")
	}
}

func TestPC93ResourceFreshnessByteRefusalLeavesMessageRetryable(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	var delivered int
	p.manager.SetAnnouncementBroadcast(func(string) { delivered++ })
	// The accounting seam isolates the byte bound from count limits without
	// allocating 96 MiB of unrelated metadata. A new watermark costs 196 bytes.
	p.graph.metadataBytes = (96 << 20) - graphMutationScratchBytes - 195
	beforeCharge := p.graph.retainedCharge()
	frame := receiveControllerWire(t, p, source, "PC93^N2AAA^43200^*^K1FROM^*^retryable message^H1^", now)
	if delivered != 0 || source.ctx.Err() != nil || p.graph.freshness.Len() != 0 || p.graph.messageOrigins != 0 || p.graph.retainedCharge() != beforeCharge {
		t.Fatal("freshness byte refusal delivered, advanced authority, or closed the peer")
	}
	if count, _, _ := p.pc93.occupancy(); count != 0 || p.pc93.contains(pc93Key(frame), now) {
		t.Fatal("freshness byte refusal poisoned message dedupe")
	}
	p.graph.metadataBytes--
	p.receive(frame, source, now)
	watermark, present := p.graph.freshness.Get("N2AAA")
	if delivered != 1 || !present || watermark.Value != 43200 || !watermark.MessageOnly || p.graph.messageOrigins != 1 || p.graph.retainedCharge() != 96<<20 {
		t.Fatal("same message did not succeed when exactly 196 bytes became available")
	}
	if count, _, _ := p.pc93.occupancy(); count != 1 {
		t.Fatal("accepted retry did not retain exactly one dedupe key")
	}
	p.receive(frame, source, now)
	if delivered != 1 {
		t.Fatal("accepted message retry was delivered more than once")
	}
}

func TestPC92ResourceFullMailboxRejectsOnlyValidAuthority(t *testing.T) {
	p, source, alternate, _ := controllerTestOwner(t)
	p.manager.admissionFailures = newFixedIndex[string, admissionFailure](64)
	valid, err := ParseFrame("PC92^N2AAA^43200^C^5N2AAA^1K1USER^H1^")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 192; i++ {
		if !p.enqueue(valid, alternate, time.Now()) {
			t.Fatalf("mailbox refused record %d before reaching its count bound", i)
		}
	}
	malformed, err := ParseFrame("PC92^N2AAA^43200^C^5N2AAA^1K1USER^9BAD^H1^")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePC92(malformed); err == nil {
		t.Fatal("malformed C fixture unexpectedly has valid typed authority")
	}
	p.manager.HandleFrame(malformed, source)
	if source.ctx.Err() != nil || p.manager.blockedPeers.Value(source.remoteCall) || p.manager.admissionFailures.Len() != 0 {
		t.Fatal("malformed full-mailbox input closed or gated the peer")
	}
	if p.queued[0] != 192 || len(p.input) != 192 {
		t.Fatal("malformed input displaced already admitted mailbox work")
	}
	p.manager.HandleFrame(valid, source)
	if source.ctx.Err() == nil || !p.manager.blockedPeers.Value(source.remoteCall) || p.manager.admissionFailures.Len() != 1 {
		t.Fatal("valid full-mailbox authority did not close and gate the peer")
	}
	failure := p.manager.admissionFailures.Value(source.remoteCall)
	p.drainFailures()
	if !p.blockedInput.Value(source.remoteCall) || p.admissionHeadroom(source.remoteCall) {
		t.Fatal("mailbox refusal lost its recovery cause or resumed while still full")
	}
	if p.graph.nodes.Len() != 0 || p.graph.freshness.Len() != 0 {
		t.Fatal("full-mailbox refusal changed topology or freshness")
	}
	if count, _, _ := p.pc92.occupancy(); count != 0 {
		t.Fatal("full-mailbox refusal consumed dedupe authority")
	}
	// Model the owner's receive boundary, releasing exactly one real queued
	// item. No record is processed, so only mailbox capacity has changed.
	work := <-p.input
	p.queueMu.Lock()
	p.queued[work.class]--
	p.bytes[work.class] -= work.charge
	p.queueMu.Unlock()
	p.graph.metadataBytes = 96 << 20
	if !p.admissionHeadroom(source.remoteCall) {
		t.Fatal("mailbox recovery incorrectly depends on unrelated graph headroom")
	}
	// First observe available space 900 ms after failure. One full second of
	// observed headroom must pass before readmission, independent of the age
	// of the failed record or time spent waiting while the mailbox was full.
	wall := failure.at.UTC()
	p.wallNow = func() time.Time { return wall }
	p.elapsedNow = func() time.Time { return wall }
	p.dirty = false
	for _, elapsed := range []time.Duration{900 * time.Millisecond, time.Second, 1899 * time.Millisecond} {
		wall = failure.at.Add(elapsed).UTC()
		p.tick(wall)
		if !p.manager.blockedPeers.Value(source.remoteCall) {
			t.Fatalf("mailbox gate resumed at %s since failure, before one second of observed headroom", elapsed)
		}
	}
	wall = failure.at.Add(1900 * time.Millisecond).UTC()
	p.tick(wall)
	if p.manager.blockedPeers.Value(source.remoteCall) || p.blocked.Len() != 0 || p.blockedRecords.Len() != 0 || p.blockedInput.Len() != 0 {
		t.Fatal("mailbox gate or retained failure state survived stable mailbox recovery")
	}
	if len(p.input) != 191 || p.queued[0] != 191 || p.graph.metadataBytes != 96<<20 {
		t.Fatal("mailbox recovery consumed work or required graph capacity to change")
	}
}
