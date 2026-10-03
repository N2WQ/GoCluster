package dedup

import (
	"testing"
	"time"

	"dxcluster/spot"
	"github.com/zeebo/xxh3"
)

func uint32Hash(key []byte) uint32 { return uint32(xxh3.Hash(key)) }

func TestPrimaryCleanupPreservesConcurrentRefresh(t *testing.T) {
	base := time.Unix(60, 0)
	for _, concurrent := range []bool{false, true} {
		name := "refresh_before_cleanup"
		if concurrent {
			name = "refresh_during_cleanup"
		}
		t.Run(name, func(t *testing.T) {
			d := NewDeduplicator(120*time.Second, false, 4)
			old := policySpot(base, false, -10)
			fresh := policySpot(base.Add(50*time.Second), true, -20)
			control := policySpot(base, false, -10)
			control.DXCall, control.DXCallNorm = "K2OLD", "K2OLD"
			key := old.DedupeKey()
			if fresh.DedupeKey() != key {
				t.Fatal("refresh must keep the same full key")
			}
			for _, s := range []*spot.Spot{old, control} {
				d.processSpot(s)
				requirePrimaryOutput(t, d, s)
			}
			done := make(chan struct{})
			lockHeld, entered := false, false
			if !concurrent {
				d.processSpot(fresh)
				requirePrimaryOutput(t, d, fresh)
				close(done)
			}
			d.cleanupAt(base.Add(121*time.Second), func(shard *cacheShard, deleting [42]byte) {
				if deleting != key {
					return
				}
				entered = true
				if !concurrent {
					return
				}
				lockHeld = !shard.mu.TryLock()
				if !lockHeld {
					shard.mu.Unlock()
				}
				started := make(chan struct{})
				go func() { close(started); d.processSpot(fresh); close(done) }()
				<-started
				// A restored split-lock implementation allows the real refresh to
				// finish before stale deletion; wait for it to make the regression
				// deterministic. The correct transaction keeps it blocked until
				// deletion completes, when the refreshed observation is reinserted.
				if !lockHeld {
					<-done
				}
			})
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("refresh did not complete")
			}
			if concurrent {
				requirePrimaryOutput(t, d, fresh)
			}
			if concurrent && (!entered || !lockHeld) {
				t.Error("expiry/deletion did not retain the actual shard lock")
			}
			if !concurrent && entered {
				t.Error("cleanup selected a current unexpired entry")
			}
			shard := d.shardFor(old.Hash32())
			shard.mu.Lock()
			entry, exists := shard.cache[key]
			shard.mu.Unlock()
			if !exists || !entry.when.Equal(fresh.Time) || !entry.hasReport {
				t.Error("current refreshed entry lost")
			}
			controlShard := d.shardFor(control.Hash32())
			controlShard.mu.Lock()
			_, retained := controlShard.cache[control.DedupeKey()]
			controlShard.mu.Unlock()
			if retained {
				t.Error("unchanged expired control retained")
			}
			d.processSpot(fresh)
			requirePrimaryOutput(t, d, nil)
		})
	}
}

func TestDedupeCleanupExpiryBoundaries(t *testing.T) {
	now := time.Unix(180, 0)
	d := NewDeduplicator(time.Minute, false, 4)
	for i, age := range []time.Duration{time.Minute + 1, time.Minute, time.Minute - 1, -time.Second} {
		d.shards[0].cache[[42]byte{byte(i)}] = cachedEntry{when: now.Add(-age)}
	}
	d.cleanupAt(now, nil)
	for i := range 4 {
		_, exists := d.shards[0].cache[[42]byte{byte(i)}]
		if exists != (i != 0) {
			t.Fatalf("boundary %d retained=%t", i, exists)
		}
	}
	secondary := NewSecondaryDeduper(time.Minute, false)
	for i, age := range []time.Duration{time.Minute + 1, time.Minute, time.Minute - 1, -time.Second} {
		secondary.shards[0].cache[[32]byte{byte(i)}] = secondaryEntry{when: now.Add(-age)}
	}
	secondary.cleanupAt(now)
	for i := range 4 {
		_, exists := secondary.shards[0].cache[[32]byte{byte(i)}]
		if exists != (i != 0) {
			t.Fatalf("secondary boundary %d retained=%t", i, exists)
		}
	}
	if len(d.shards) != 64 || len(NewSecondaryDeduper(time.Minute, false).shards) != 64 {
		t.Fatal("shard count changed")
	}
}
