package toxicity

import (
	"testing"
	"time"

	"dxcluster/spot"
)

// BenchmarkClassifierAdmission guards the normal single-item paths while
// measuring terminal fencing overhead. Workers remain unstarted: the full-queue
// case is deterministic, and no HTTP or result consumer belongs to this bench.
func BenchmarkClassifierAdmission(b *testing.B) {
	for _, path := range []string{"nonhuman", "local", "cache", "full_queue"} {
		b.Run(path, func(b *testing.B) {
			gate, err := NewSafeGate(safeGateConfig{MaxTokens: 8, SafeTokens: []string{"CQ"}})
			if err != nil {
				b.Fatal(err)
			}
			c := NewClassifier(Config{Enabled: true, Workers: 1, QueueSize: 1,
				CacheMaxEntries: 8, CacheTTLSeconds: 60, MaxCommentBytes: 512, TimeoutMS: 100}, gate, errorClient{})
			b.Cleanup(c.Stop)
			now := time.Unix(1700000000, 0)
			s := &spot.Spot{IsHuman: path != "nonhuman", Comment: "CQ"}
			want := spot.ToxicitySafeLocal
			if path == "nonhuman" {
				want = spot.ToxicityUnknown
			}
			if path == "cache" {
				s.Comment, want = "a conversation", spot.ToxicityToxic
				c.cache.Put(s.Comment, Decision{Status: want, Categories: []string{"fixture"}, Model: "fixture"}, now)
			}
			if path == "full_queue" {
				if c.ClassifyOrEnqueue(&spot.Spot{IsHuman: true, Comment: "queued conversation", ToxicityStatus: spot.ToxicityUnknown}, now) {
					b.Fatal("full-queue setup did not admit its pre-Start job")
				}
				s.Comment, want = "another conversation", spot.ToxicityUnavailable
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.ToxicityStatus = spot.ToxicityUnknown
				if !c.ClassifyOrEnqueue(s, now) || s.ToxicityStatus != want {
					b.Fatal("admission benchmark changed its guarded decision")
				}
			}
			b.StopTimer()
			stats := c.Snapshot()
			if stats.AICalls != 0 || stats.ResultBacklog != 0 ||
				(path == "local" && stats.SafeBypass != uint64(b.N)) ||
				(path == "cache" && (stats.CacheHits != uint64(b.N) || stats.Toxic != uint64(b.N) || stats.CacheEntries != 1)) ||
				(path == "full_queue" && (stats.QueueFull != uint64(b.N) || stats.Unavailable != uint64(b.N) || stats.Pending != 1)) ||
				(path != "full_queue" && stats.Pending != 0) {
				b.Fatalf("admission benchmark lost its bounded state: %#v", stats)
			}
		})
	}
}
