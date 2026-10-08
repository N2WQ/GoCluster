package telnet

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

// Exercise the actual synchronous fanout admission, without socket writers or
// worker scheduling masking missing deliveries. Guards remain timed: recovered
// panics, filter skips and dropped envelopes cannot appear as faster fanout.
func BenchmarkCommentFanout(b *testing.B) {
	for _, clientCount := range []int{1, 8} {
		b.Run(fmt.Sprintf("clients-%d", clientCount), func(b *testing.B) {
			server, job := newCommentFanoutFixture(b, clientCount, 65500)
			before := commentFanoutDropSnapshot(server)
			var delivered uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				server.deliverJob(job)
				delivered += drainCommentFanout(b, server, job, nil, before)
			}
			b.StopTimer()
			if want := uint64(b.N) * uint64(clientCount); delivered != want {
				b.Fatalf("delivered %d envelopes, want %d", delivered, want)
			}
			// Include calibration runs when normalizing the whole CPU profile;
			// its samples cover more calls than the final benchmark line alone.
			b.Logf("completed fanout calls=%d delivered=%d", b.N, delivered)
		})
	}
}

func TestCommentFanout(t *testing.T) {
	for _, tc := range []struct {
		name          string
		blockComment  bool
		self          bool
		selfDisabled  bool
		policyAllowed bool
		want          int
	}{
		{"ordinary-pass", false, false, false, true, 8},
		{"ordinary-comment-reject", true, false, false, true, 0},
		{"self-bypasses-comment", true, true, false, true, 1},
		{"self-disabled", true, true, true, true, 0},
		{"self-bypasses-dedupe-and-comment", true, true, false, false, 1},
		{"ordinary-dedupe-reject", false, false, false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, job := newCommentFanoutFixture(t, 8, 128)
			if tc.blockComment {
				for _, client := range job.clients {
					client.filter.BlockComments = []string{"AAA"}
				}
			}
			if tc.self {
				job.clients[0].callsign = job.spot.DXCall + "/P"
				job.clients[0].filter.SetSelfEnabled(!tc.selfDisabled)
			}
			job.allowFast, job.allowMed, job.allowSlow = tc.policyAllowed, tc.policyAllowed, tc.policyAllowed
			expected := make([]bool, len(job.clients))
			for i := range expected {
				expected[i] = tc.want == 8 || tc.want == 1 && i == 0
			}
			before := commentFanoutDropSnapshot(server)
			server.deliverJob(job)
			if got := drainCommentFanout(t, server, job, expected, before); got != uint64(tc.want) {
				t.Fatalf("delivered %d envelopes, want %d", got, tc.want)
			}
		})
	}
}

func newCommentFanoutFixture(t testing.TB, count, commentBytes int) (*Server, broadcastJob) {
	t.Helper()
	server := NewServer(ServerOptions{ClientBuffer: 1}, nil)
	s := spot.NewSpot("K1CMT", "W1SPT", 14025, "CW")
	s.Comment = strings.Repeat("A", commentBytes)
	job := broadcastJob{spot: s, clients: make([]*Client, count), allowFast: true,
		allowMed: true, allowSlow: true, enqueueAt: time.Now().UTC()}
	for i := range job.clients {
		client := newBroadcastWorkerTestClient(server, 1)
		client.callsign = fmt.Sprintf("N%dCMT", i)
		client.filter.Reset()
		for j := range filter.MaxCommentPhrases {
			client.filter.BlockComments = append(client.filter.BlockComments, strings.Repeat("A", 61)+fmt.Sprintf("%03d", j))
		}
		if isSelfMatch(s, client.callsign) {
			t.Fatal("fanout benchmark fixture unexpectedly matches SELF")
		}
		job.clients[i] = client
	}
	return server, job
}

func commentFanoutDropSnapshot(server *Server) [3]uint64 {
	queues, clients, senders := server.BroadcastMetricSnapshot()
	return [3]uint64{queues, clients, senders}
}

// A nil expectation means every client must receive exactly one envelope.
func drainCommentFanout(t testing.TB, server *Server, job broadcastJob, expected []bool, before [3]uint64) uint64 {
	var delivered uint64
	for i, client := range job.clients {
		want := expected == nil || expected[i]
		pending := len(client.spotChan)
		if pending != 0 && !want || pending != 1 && want {
			t.Fatalf("client %s pending=%d, expected delivery=%v", client.callsign, pending, want)
		}
		if want {
			env := <-client.spotChan
			if env == nil || env.spot != job.spot || !env.enqueueAt.Equal(job.enqueueAt) || env.pathPrediction != nil {
				t.Fatalf("client %s received a changed spot or envelope", client.callsign)
			}
			delivered++
		}
		if len(client.spotChan) != 0 || len(client.controlChan) != 0 || atomic.LoadUint64(&client.dropCount) != 0 {
			t.Fatalf("client %s retained backlog or recorded drops", client.callsign)
		}
		select {
		case <-client.done:
			t.Fatalf("client %s closed during delivery", client.callsign)
		default:
		}
	}
	if len(server.broadcast) != 0 {
		t.Fatal("fanout left a broadcast backlog")
	}
	for _, queue := range server.workerQueues {
		if len(queue) != 0 {
			t.Fatal("fanout left a worker backlog")
		}
	}
	if after := commentFanoutDropSnapshot(server); after != before || after != [3]uint64{} {
		t.Fatalf("fanout drop metrics changed: before=%v after=%v", before, after)
	}
	return delivered
}
