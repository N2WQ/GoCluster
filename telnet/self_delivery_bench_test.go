package telnet

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/spot"
)

// Each iteration covers one fan-out of the same immutable CW spot. Exactly
// one login owns its DX call. Count and check every expected queued envelope
// so filter failures and dropped deliveries cannot masquerade as savings.
func BenchmarkSelfDelivery(b *testing.B) {
	for _, name := range []string{"ordinary-300", "suppressed-300"} {
		b.Run(name, func(b *testing.B) {
			s := NewServer(ServerOptions{ClientBuffer: 1}, nil)
			dx := spot.NewSpot("K1AA/P", "W1AW", 14025, "CW").SealForAsync()
			job := broadcastJob{
				spot: dx, clients: make([]*Client, 300),
				allowFast: true, allowMed: true, allowSlow: true,
			}
			for i := range job.clients {
				login := fmt.Sprintf("K1%c%c-7", 'A'+i/26, 'A'+i%26)
				client := newBroadcastWorkerTestClient(s, 1)
				client.setLoginCallsign(login)
				s.clients[login], job.clients[i] = client, client
			}
			want := job.clients
			if name == "suppressed-300" {
				want = want[:1]
			}
			b.ReportAllocs()
			for b.Loop() {
				if name == "ordinary-300" {
					job.enqueueAt = time.Now()
					s.deliverJob(job)
				} else {
					s.DeliverSelfSpotOwned(dx.DXCallNorm, dx)
				}
				for _, recipient := range want {
					select {
					case env := <-recipient.spotChan:
						if env == nil || env.spot != dx {
							b.Fatalf("wrong spot for %s", recipient.callsign)
						}
					default:
						b.Fatalf("missing delivery to %s", recipient.callsign)
					}
				}
			}
			for _, client := range job.clients {
				if len(client.spotChan) != 0 {
					b.Fatalf("unexpected delivery to %s", client.callsign)
				}
			}
			if q, c, f := s.BroadcastMetricSnapshot(); q != 0 || c != 0 || f != 0 {
				b.Fatalf("delivery failures: queues=%d clients=%d senders=%d", q, c, f)
			}
			b.ReportMetric(float64(len(want)), "deliveries/op")
		})
	}
}
