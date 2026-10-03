package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/dedup"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestOutputPipelineCollisionDeliveryRails(t *testing.T) {
	data, err := os.ReadFile("../../dedup/testdata/q1-collisions.json")
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
			ring := buffer.NewRingBuffer(4)
			writer := newDeliveryTestArchiveWriter(t)
			srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
			pipeline := newDeliveryTestPipeline(ring, writer, srv)
			pipeline.secondaryFast = dedup.NewSecondaryDeduper(179*time.Second, false)
			pipeline.secondaryMed = dedup.NewSecondaryDeduper(359*time.Second, false)
			pipeline.secondarySlow = dedup.NewSecondaryDeduperWithKey(479*time.Second, false, dedup.SecondaryKeyCQZone)
			pipeline.secondaryActive = true
			for _, item := range []input{pair.First, pair.Second, pair.First, pair.Second} {
				at, err := time.Parse(time.RFC3339, item.Time)
				if err != nil {
					t.Fatal(err)
				}
				s := spot.NewSpot(item.Call, "DL1AAA", item.Frequency, item.Mode)
				s.Time, s.SourceType = at, spot.SourcePeer
				s.DEMetadata = spot.CallMetadata{ADIF: 230, CQZone: 14}
				pipeline.deliverSpot(&outputSpotContext{spot: s})
			}
			if ring.GetCount() != 2 {
				t.Fatalf("ring count=%d want2", ring.GetCount())
			}
			for _, want := range []string{pair.First.Call, pair.Second.Call} {
				if got, ok := tryReadArchiveQueuedSpot(writer); !ok || got.DXCallNorm != want {
					t.Fatalf("archive spot=%v present=%t want=%s", got, ok, want)
				}
				if got, ok := tryReadTelnetBroadcastSpot(srv); !ok || got.DXCallNorm != want {
					t.Fatalf("broadcast spot=%v present=%t want=%s", got, ok, want)
				}
			}
			if _, ok := tryReadArchiveQueuedSpot(writer); ok {
				t.Fatal("true repeat archived")
			}
			if _, ok := tryReadTelnetBroadcastSpot(srv); ok {
				t.Fatal("true repeat broadcast")
			}
		})
	}
}
