package telnet

import (
	"testing"
	"time"
)

// This benchmark closes only after the sink has observed every required spot.
// The historical close-first burst benchmark exercises control priority but
// does not prove that its queued spot was formatted or written.
func BenchmarkWriterV15SpotDrain(b *testing.B) {
	for _, mode := range []struct {
		name      string
		diag      diagMode
		nilServer bool
	}{
		{name: "normal"}, {name: "diag-source", diag: diagModeSource},
		{name: "nil-server", nilServer: true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			server := &Server{writerBatchMaxBytes: 16384, writerBatchWait: 5 * time.Millisecond}
			if mode.nilServer {
				server = nil
			}
			sp, expectedSpot := writerV15Spot(), writerV15Spot()
			line := expectedSpot.FormatDXCluster()
			if mode.diag == diagModeSource {
				line = expectedSpot.FormatDXClusterWithComment("MAN")
			}
			conn := &writerV15Conn{expected: []byte(writerV15Oracle(line + "\n")), target: b.N, reached: make(chan struct{})}
			client := writerV15Client(server, conn)
			client.setDiagMode(mode.diag)
			// Exclude base cache construction to isolate warmed delivery.
			sp.FormatDXCluster()
			env := &spotEnvelope{spot: sp}
			done := writerV15Start(client)
			defer func() { client.close(""); writerV15Wait(b, done, "shutdown") }()
			deadline := time.NewTimer(30 * time.Second)
			defer deadline.Stop()
			b.ReportAllocs()
			b.SetBytes(int64(len(conn.expected)))
			b.ResetTimer()
			for range b.N {
				select {
				case client.spotChan <- env:
				case <-done:
					b.Fatal("writer exited before all spot admissions")
				case <-deadline.C:
					b.Fatal("spot admission timed out")
				}
			}
			writerV15Wait(b, conn.reached, "all spot bytes")
			b.StopTimer()
			client.close("")
			writerV15Wait(b, done, "shutdown")
			writerV15Check(b, conn)
			b.ReportMetric(float64(conn.count), "verified-spots")
		})
	}
}
