package telnet

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"dxcluster/spot"
)

// Each iteration publishes a fresh spot. Every writer must flush exactly one
// fixed wire record per publication, including under mixed session settings.
func BenchmarkCWBroadcastWriters(b *testing.B) {
	for _, name := range []string{"ordinary-8", "equal-off-8", "projected-off-8", "projected-on-8", "mixed-8", "on-1", "mixed-32"} {
		b.Run(name, func(b *testing.B) {
			count := 8
			switch name {
			case "on-1":
				count = 1
			case "mixed-32":
				count = 32
			}
			workers := min(2, count)
			if width := spot.CurrentDXClusterLayout().LineLength; width != 78 {
				b.Fatalf("wire fixture requires 78 columns, got %d", width)
			}
			s := NewServer(ServerOptions{BroadcastWorkers: workers, BroadcastQueue: 1, WriterBatchMaxBytes: 80}, nil)
			s.workerQueues = make([]chan broadcastJob, workers)
			shards := make([][]*Client, workers)
			clients := make([]*Client, count)
			sinks := make([]*writerV15Conn, count)
			done := make([]<-chan struct{}, count)
			for i := range s.workerQueues {
				s.workerQueues[i] = make(chan broadcastJob, 1)
			}
			for i := range clients {
				on := name == "projected-on-8" || name == "on-1" || ((name == "mixed-8" || name == "mixed-32") && i%4 < 2)
				comment := "CW POTA"
				if on {
					comment += " 22 WPM 30 WPM"
				}
				sinks[i] = &writerV15Conn{expected: cwPublicationWire(comment), target: b.N, reached: make(chan struct{})}
				c := writerV15Client(s, sinks[i])
				c.setLoginCallsign(fmt.Sprintf("W1U%d", i))
				c.filter = unrestrictedTestFilter()
				c.configuredSettings.CWSpeed = on
				clients[i] = c
				shards[i%workers] = append(shards[i%workers], c)
				done[i] = writerV15Start(c)
			}
			b.Cleanup(func() {
				for i, c := range clients {
					c.close("")
					writerV15Wait(b, done[i], "shutdown")
				}
			})
			s.clientShardCache.Store(&clientShardSnapshot{shards: shards})
			s.shardsDirty.Store(false)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sp := cwSpeedFixture()
				sp.CWDisplayAvailable = name != "ordinary-8"
				if !sp.CWDisplayAvailable || name == "equal-off-8" {
					sp.Comment = "POTA"
				}
				if !s.TryBroadcastSpotOwned(sp, true, true, true) {
					b.Fatal("refused publication")
				}
				s.broadcastSpot(<-s.broadcast)
				for _, q := range s.workerQueues {
					s.deliverJob(<-q)
				}
				for i, c := range clients {
					if len(c.spotChan) <= cap(c.spotChan)/2 {
						continue
					}
					// Pace the producer without changing the production admission/drop path.
					deadline := time.Now().Add(5 * time.Second)
					for len(c.spotChan) > cap(c.spotChan)/2 {
						if time.Now().After(deadline) {
							b.Fatalf("writer %d stopped draining", i)
						}
						runtime.Gosched()
					}
				}
			}
			for _, sink := range sinks {
				writerV15Wait(b, sink.reached, "all records")
			}
			b.StopTimer()
			for i, c := range clients {
				c.close("")
				writerV15Wait(b, done[i], "complete")
				writerV15Check(b, sinks[i])
			}
			if q, c, w := s.BroadcastMetricSnapshot(); q != 0 || c != 0 || w != 0 {
				b.Fatalf("drops/failures: %d/%d/%d", q, c, w)
			}
			b.ReportMetric(float64(count), "records/op")
		})
	}
}
