package peer

import (
	"testing"
	"time"
)

// Actual receive admission replaces the superseded refused-wire feasibility
// probe. Synthetic occupancy isolates each boundary; it is not a load proof.
func TestPC92V14RetryRefusalAuthority(t *testing.T) {
	for _, resource := range []string{"nodes", "users", "edges", "freshness", "ingress", "graph_bytes", "cache_count", "cache_bytes"} {
		t.Run(resource, func(t *testing.T) {
			p, source, alternate, _, base := recoveryV12Owner(t)
			frame, err := ParseFrame("PC92^N2AAA^43200^C^7N3EXT^1K1USER^H1^")
			if err != nil {
				t.Fatal(err)
			}
			var release func()
			switch resource {
			case "nodes":
				p.graph.nodes.count = maxGraphNodes - 1
				release = func() { p.graph.nodes.count-- }
			case "users":
				p.graph.users.count = maxGraphUsers
				release = func() { p.graph.users.count-- }
			case "edges":
				p.graph.edges = maxGraphEdges - 1
				release = func() { p.graph.edges-- }
			case "freshness":
				p.graph.freshness.count = maxFreshnessOrigins - 1
				release = func() { p.graph.freshness.count-- }
			case "ingress":
				p.graph.ingress.count = maxIngressObservations - 1
				release = func() { p.graph.ingress.count-- }
			case "graph_bytes":
				// Two nodes, one user, two edges, two watermarks, two
				// observations and the owned calls require exactly 2444 bytes.
				p.graph.metadataBytes = (96 << 20) - graphMutationScratchBytes - 2444 + 1
				release = func() { p.graph.metadataBytes-- }
			case "cache_count":
				p.pc92 = newBoundedDedupe(600*time.Second, 1, 8<<20)
				if p.pc92.admit("unrelated", base) != dedupeAccepted {
					t.Fatal("cache fixture failed")
				}
				release = func() { p.pc92.prune(base.Add(600*time.Second + time.Nanosecond)) }
			case "cache_bytes":
				p.pc92.byteLimit = len(pc92Key(frame)) - 1
				release = func() { p.pc92.byteLimit++ }
			}
			before := [6]int{p.graph.nodes.Len(), p.graph.users.Len(), p.graph.edges, p.graph.freshness.Len(), p.graph.ingress.Len(), p.graph.retainedCharge()}
			p.receive(frame, source, base)
			if source.ctx.Err() == nil || !p.manager.blockedPeers.Value(source.remoteCall) {
				t.Fatal("exhausted authority boundary did not close/fence source")
			}
			after := [6]int{p.graph.nodes.Len(), p.graph.users.Len(), p.graph.edges, p.graph.freshness.Len(), p.graph.ingress.Len(), p.graph.retainedCharge()}
			if after != before || p.pc92.contains(pc92Key(frame), base) {
				t.Fatal("refusal partially committed topology, freshness or dedupe authority")
			}
			release()
			p.receive(frame, alternate, base)
			if alternate.ctx.Err() != nil || p.graph.users.Value("K1USER") != 1 || p.graph.freshness.Value("N3EXT").Value != 43200 {
				t.Fatal("same timestamp/key did not remain retryable through valid owner")
			}
		})
	}
}
