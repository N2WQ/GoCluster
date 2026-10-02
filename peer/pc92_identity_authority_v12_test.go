package peer

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

type v12NodeState struct {
	Entry        PC92Entry
	Members      map[memberKey]PC92Entry
	Complete     bool
	Observations int
	Seen         time.Time
}

type v12AuthorityState struct {
	Nodes     map[string]v12NodeState
	Users     map[string]int
	Ingress   map[ingressKey]ingressObservation
	Freshness map[string]originWatermark
	Keys      int
	KeyBytes  int
	Refused   uint64
	Charge    int
	Edges     int
}

func v12AuthoritySnapshot(p *protocolController) v12AuthorityState {
	s := v12AuthorityState{Nodes: make(map[string]v12NodeState), Users: make(map[string]int), Ingress: make(map[ingressKey]ingressObservation), Freshness: make(map[string]originWatermark), Charge: p.graph.retainedCharge(), Edges: p.graph.edges}
	for call, node := range p.graph.nodes.All() {
		n := v12NodeState{Entry: node.Entry, Members: make(map[memberKey]PC92Entry), Complete: node.Complete, Observations: node.Observations, Seen: node.Seen}
		for key, entry := range node.Members.All() {
			n.Members[key] = entry
		}
		s.Nodes[call] = n
	}
	for call, refs := range p.graph.users.All() {
		s.Users[call] = refs
	}
	for key, observation := range p.graph.ingress.All() {
		s.Ingress[key] = observation
	}
	for call, watermark := range p.graph.freshness.All() {
		s.Freshness[call] = watermark
	}
	s.Keys, s.KeyBytes, s.Refused = p.pc92.occupancy()
	return s
}

func TestPC92V12MalformedNoAuthority(t *testing.T) {
	for _, raw := range []string{"K1ABC/", "/K1ABC", "K1ABC-000"} {
		for _, role := range []string{"origin", "subject", "member"} {
			for _, action := range []string{"A", "C", "D", "K"} {
				if role == "member" && action == "K" {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", raw, role, action), func(t *testing.T) {
					p, source, destination, now := controllerTestOwner(t)
					for _, call := range []string{"N2AAA", "K1ABC", "K2EXT"} {
						receiveControllerWire(t, p, source, "PC92^"+call+"^43200^C^5"+call+":5457:633^1K1OLD:192.0.2.1^H10^", now)
					}
					for len(destination.priorityLineCh) != 0 {
						<-destination.priorityLineCh
					}
					before := v12AuthoritySnapshot(p)
					bad := v12IdentityFrame(raw, role, action)
					p.receive(bad, source, now)
					if !reflect.DeepEqual(before, v12AuthoritySnapshot(p)) || len(destination.priorityLineCh) != 0 || source.ctx.Err() != nil {
						t.Fatal("malformed record changed seeded authority, metadata, cache, relay or session")
					}
					if _, exists := p.pc92.firstAdmission(pc92Key(bad), now); exists {
						t.Fatal("malformed wire acquired a dedupe key")
					}
					corrected := v12IdentityFrame("K1ABC", role, action)
					p.receive(corrected, source, now)
					origin, subject := "N2AAA", "N2AAA"
					switch role {
					case "origin":
						origin, subject = "K1ABC", "K2EXT"
					case "subject":
						subject = "K1ABC"
					}
					keys, _, _ := p.pc92.occupancy()
					if p.graph.freshness.Value(origin).Value != 43201 || p.graph.freshness.Value(subject).Value != 43201 || keys != before.Keys+1 || len(destination.priorityLineCh) != 1 {
						t.Fatal("corrected same-timestamp record was not fully admitted and forwarded")
					}
				})
			}
		}
	}
}

func TestPC92V12StartupMalformedNoEffect(t *testing.T) {
	p, source, _, _ := controllerTestOwner(t)
	m := p.manager
	m.sessions.Delete(source.id)
	c := &candidateState{}
	m.candidates.Set(source, c)
	source.remoteVersion, source.remoteBuild, source.remoteBitmap = "5457", "633", 4
	before := v12AuthoritySnapshot(p)
	for _, raw := range []string{"K1ABC/", "/K1ABC", "K1ABC-000"} {
		for _, role := range []string{"origin", "subject", "member"} {
			origin, subject, member := "N1PEER", "N1PEER", "K1ABC"
			switch role {
			case "origin":
				origin = strings.ReplaceAll(raw, "K1ABC", "N1PEER")
			case "subject":
				subject = strings.ReplaceAll(raw, "K1ABC", "N1PEER")
			case "member":
				member = raw
			}
			wire := "PC92^" + origin + "^43201^C^5" + subject + ":9999:777^1K2GOOD^1" + member + "^H99^"
			accepted, err := source.stageStartupPC92(mailboxFrame(t, wire))
			if err != nil || accepted || source.remoteVersion != "5457" || source.remoteBuild != "633" || source.remoteBitmap != 4 || c.pc9x || c.staged != nil || c.bytes != 0 || m.stagedRecords != 0 || m.stagedBytes != 0 || m.sessions.Len() != 1 || !reflect.DeepEqual(before, v12AuthoritySnapshot(p)) {
				t.Fatalf("malformed startup %s changed metadata/staging/authority: accepted=%v err=%v", role, accepted, err)
			}
		}
	}
	accepted, err := source.stageStartupPC92(mailboxFrame(t, "PC92^N1PEER^43201^C^5N1PEER:9999:777^1K2GOOD^H99^"))
	if err != nil || !accepted || source.remoteVersion != "9999" || source.remoteBuild != "777" || source.remoteBitmap != 5 || !c.pc9x || len(c.staged) != 1 || m.stagedRecords != 1 || c.bytes == 0 || m.stagedBytes != c.bytes || !reflect.DeepEqual(before, v12AuthoritySnapshot(p)) {
		t.Fatalf("valid candidate control failed: accepted=%v err=%v", accepted, err)
	}
	m.releaseStaged(c)
}

func TestPC92V12MailboxRawEligibility(t *testing.T) {
	for _, mode := range []string{"normal", "count", "bytes"} {
		for _, raw := range []string{"K1ABC/", "/K1ABC", "K1ABC-000", "K1ABC"} {
			for _, role := range []string{"origin", "subject", "member"} {
				t.Run(fmt.Sprintf("%s/%s/%s", mode, raw, role), func(t *testing.T) {
					m, source, now := mailboxTestOwner(t)
					p := m.protocol
					f := v12IdentityFrame(raw, role, "C")
					f.Fields[1] = fmt.Sprint(utcSecond(now))
					if mode == "bytes" {
						// Keep the tested record large enough that residual space
						// after large fillers cannot accidentally admit it.
						f.Fields[4] += ":" + strings.Repeat("1", 60000)
					}
					if mode != "normal" {
						filler := mailboxFrame(t, "PC92^N2AAA^43200^K^5N2AAA^0^0^^branch^H99^")
						if mode == "bytes" {
							filler.Fields[7] = strings.Repeat("x", 61000)
						}
						n := 0
						for p.enqueue(filler, source, now) {
							n++
						}
						if mode == "count" && n != 192 || mode == "bytes" && n >= 192 {
							t.Fatalf("wrong saturation boundary: mode=%s n=%d", mode, n)
						}
					}
					m.HandleFrame(f, source)
					if mode == "normal" {
						p.consumeInput(<-p.input)
					}
					valid := raw == "K1ABC"
					if mode != "normal" && valid {
						if source.ctx.Err() == nil || !m.blockedPeers.Value(source.remoteCall) || m.admissionFailures.Len() != 1 {
							t.Fatal("valid current-owner control did not close and gate under pressure")
						}
					} else if source.ctx.Err() != nil || m.blockedPeers.Len() != 0 || m.admissionFailures.Len() != 0 {
						t.Fatal("excluded record or healthy control closed/gated source")
					}
					keys, _, _ := p.pc92.occupancy()
					if valid && mode == "normal" {
						if keys != 1 || p.graph.nodes.Len() == 0 {
							t.Fatal("valid normal-path control never acquired authority")
						}
					} else if keys != 0 || p.graph.nodes.Len() != 0 || p.graph.freshness.Len() != 0 {
						t.Fatal("rejected record acquired authority")
					}
				})
			}
		}
	}
}
