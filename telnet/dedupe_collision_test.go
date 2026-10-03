package telnet

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"dxcluster/dedup"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestBroadcastCollisionDedupePolicies(t *testing.T) {
	data, err := os.ReadFile("../dedup/testdata/q1-collisions.json")
	if err != nil {
		t.Fatal(err)
	}
	type input struct {
		Call, Time, Mode string
		Frequency        float64
	}
	var fixture struct {
		Pairs []struct {
			ID            int
			First, Second input
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Pairs) != 9 {
		t.Fatal("missing historical pair")
	}
	for _, pair := range fixture.Pairs {
		t.Run(fmt.Sprint(pair.ID), func(t *testing.T) {
			srv := NewServer(ServerOptions{BroadcastQueue: 4, DedupeFastEnabled: true, DedupeMedEnabled: true, DedupeSlowEnabled: true}, nil)
			var clients []*Client
			for i, policy := range []dedupePolicy{dedupePolicyFast, dedupePolicyMed, dedupePolicySlow} {
				client := &Client{callsign: fmt.Sprintf("N%dTEST", i), filter: filter.NewFilter(), spotChan: make(chan *spotEnvelope, 4)}
				client.setDedupePolicy(policy)
				clients = append(clients, client)
			}
			fast := dedup.NewSecondaryDeduper(179*time.Second, false)
			med := dedup.NewSecondaryDeduper(359*time.Second, false)
			slow := dedup.NewSecondaryDeduperWithKey(479*time.Second, false, dedup.SecondaryKeyCQZone)
			for _, item := range []input{pair.First, pair.Second, pair.First, pair.Second} {
				at, err := time.Parse(time.RFC3339, item.Time)
				if err != nil {
					t.Fatal(err)
				}
				s := spot.NewSpot(item.Call, "DL1AAA", item.Frequency, item.Mode)
				s.Time, s.SourceType = at, spot.SourcePeer
				s.DEMetadata = spot.CallMetadata{ADIF: 230, CQZone: 14}
				srv.BroadcastSpot(s, fast.ShouldForward(s), med.ShouldForward(s), slow.ShouldForward(s))
				// Exercise the real admission consumer synchronously, so exact
				// absence assertions do not depend on worker scheduling/sleeps.
				payload := <-srv.broadcast
				srv.deliverJob(broadcastJob{spot: payload.spot, allowFast: payload.allowFast, allowMed: payload.allowMed, allowSlow: payload.allowSlow, enqueueAt: payload.enqueueAt, clients: clients})
			}
			for _, client := range clients {
				for _, want := range []string{pair.First.Call, pair.Second.Call} {
					select {
					case got := <-client.spotChan:
						if got.spot.DXCallNorm != want {
							t.Fatalf("%s got=%s want=%s", client.callsign, got.spot.DXCallNorm, want)
						}
					default:
						t.Fatalf("%s missing %s", client.callsign, want)
					}
				}
				select {
				case <-client.spotChan:
					t.Fatal("true repeat delivered")
				default:
				}
			}
		})
	}
}
