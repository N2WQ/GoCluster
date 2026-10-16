package pathreliability

import (
	"testing"
	"time"
)

var livePolicyAllocationWitness *[64]byte

func TestVOACAPLiveCachePolicyAllocations(t *testing.T) {
	f, now := newLivePolicyFixture(t, 2)
	c, d := livePolicyKeys(1)
	cacheEntry := voacapCacheEntry{storedAt: now, forecast: VOACAPClosedForecast{
		Records: []VOACAPHourlyForecast{{HourUTC: 20, FT8SNRDB: -17}},
	}}
	delayEntry := voacapDelayEntry{firstSeen: now.Add(-time.Minute), lastSeen: now}
	f.addCacheLocked(c, cacheEntry)
	f.addDelayLocked(d, delayEntry)
	otherC, otherD := livePolicyKeys(2)
	f.addCacheLocked(otherC, cacheEntry)
	f.addDelayLocked(otherD, delayEntry)
	batch := func() {
		for i := 0; i < 1000; i++ {
			f.addCacheLocked(c, cacheEntry)
			f.addDelayLocked(d, delayEntry)
			f.delayElapsedLocked(d, now)
			for _, pair := range livePolicyCacheTies {
				if compareLiveCacheKey(&pair.keep, &pair.discard) >= 0 || compareLiveCacheKey(&pair.discard, &pair.keep) <= 0 || compareLiveCacheKey(&pair.keep, &pair.keep) != 0 {
					t.Fatal("comparison witness failed")
				}
			}
			for _, pair := range livePolicyDelayTies {
				if compareLiveDelayKey(&pair.keep, &pair.discard) >= 0 || compareLiveDelayKey(&pair.discard, &pair.keep) <= 0 || compareLiveDelayKey(&pair.keep, &pair.keep) != 0 {
					t.Fatal("delay comparison witness failed")
				}
			}
		}
	}
	batch()
	// Score the complete callback, without dividing by its operation count.
	// One escaping allocation anywhere in the batch must remain observable.
	if got := testing.AllocsPerRun(1, batch); got != 0 {
		t.Fatalf("warmed refresh/comparison batch allocated %v objects", got)
	}
	if got := testing.AllocsPerRun(1, func() {
		batch()
		livePolicyAllocationWitness = new([64]byte)
	}); got != 1 {
		t.Fatalf("single-allocation negative witness measured %v objects, want 1", got)
	}
	livePolicyAllocationWitness = nil
	if len(f.cache) != 2 || len(f.delays) != 2 || f.cache[c].forecast.Records[0].FT8SNRDB != -17 || f.delays[d] != delayEntry {
		t.Fatal("allocation batch did not preserve complete replacement state")
	}
}
