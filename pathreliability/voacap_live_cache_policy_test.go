package pathreliability

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func newLivePolicyFixture(t *testing.T, capacity int) (*VOACAPClosedFallback, time.Time) {
	t.Helper()
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.MaxCacheEntries = capacity
	cfg.VOACAPFallback.MaxDelayEntries = capacity
	f := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
	return f, time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
}

func livePolicyKeys(id int) (voacapCacheKey, voacapDelayKey) {
	return voacapCacheKey{userCell: CellID(id)}, voacapDelayKey{userCell: CellID(id)}
}

func TestVOACAPLiveCachePolicyReplacement(t *testing.T) {
	for _, capacity := range []int{2, 3} {
		for _, replaced := range []int{1, 2} {
			for _, offset := range []time.Duration{-time.Hour, 0, time.Hour} {
				t.Run(fmt.Sprintf("cap%d/key%d/%s", capacity, replaced, offset), func(t *testing.T) {
					f, now := newLivePolicyFixture(t, capacity)
					for id := 1; id <= 2; id++ {
						c, d := livePolicyKeys(id)
						at := now.Add(time.Duration(id) * time.Second)
						f.addCacheLocked(c, voacapCacheEntry{storedAt: at})
						f.addDelayLocked(d, voacapDelayEntry{firstSeen: at, lastSeen: at})
					}
					c, d := livePolicyKeys(replaced)
					otherC, otherD := livePolicyKeys(3 - replaced)
					otherCache, otherDelay := f.cache[otherC], f.delays[otherD]
					at := f.cache[c].storedAt.Add(offset)
					wantCache := voacapCacheEntry{storedAt: at, forecast: VOACAPClosedForecast{
						OutputPath: "replacement", Records: []VOACAPHourlyForecast{{HourUTC: 20, FT8SNRDB: -17}},
					}}
					wantDelay := voacapDelayEntry{firstSeen: now.Add(-2 * time.Hour), lastSeen: at}
					f.addCacheLocked(c, wantCache)
					f.addDelayLocked(d, wantDelay)
					if len(f.cache) != 2 || len(f.delays) != 2 || !reflect.DeepEqual(f.cache[c], wantCache) || f.delays[d] != wantDelay {
						t.Fatal("replacement lost owners or failed to install the complete incoming entry")
					}
					if !reflect.DeepEqual(f.cache[otherC], otherCache) || f.delays[otherD] != otherDelay {
						t.Fatal("replacement changed an unrelated entry")
					}
				})
			}
		}
	}
}

// Literal pairs establish the policy independently of the production comparator.
// Each later field opposes the decisive field, so accidentally changing precedence
// cannot pass merely because all fields happen to point in the same direction.
var livePolicyCacheTies = []struct {
	name          string
	keep, discard voacapCacheKey
}{
	{"user", voacapCacheKey{2, 10, "z", 10, 10, 10, 10, "z"}, voacapCacheKey{10, 2, "a", 2, 2, 2, 2, "a"}},
	{"dx", voacapCacheKey{1, 2, "z", 10, 10, 10, 10, "z"}, voacapCacheKey{1, 10, "a", 2, 2, 2, 2, "a"}},
	{"band", voacapCacheKey{1, 1, "a", 10, 10, 10, 10, "z"}, voacapCacheKey{1, 1, "z", 2, 2, 2, 2, "a"}},
	{"frequency", voacapCacheKey{1, 1, "a", 2, 10, 10, 10, "z"}, voacapCacheKey{1, 1, "a", 10, 2, 2, 2, "a"}},
	{"year", voacapCacheKey{1, 1, "a", 1, 2, 10, 10, "z"}, voacapCacheKey{1, 1, "a", 1, 10, 2, 2, "a"}},
	{"month", voacapCacheKey{1, 1, "a", 1, 1, 2, 10, "z"}, voacapCacheKey{1, 1, "a", 1, 1, 10, 2, "a"}},
	{"ssn", voacapCacheKey{1, 1, "a", 1, 1, 1, 2, "z"}, voacapCacheKey{1, 1, "a", 1, 1, 1, 10, "a"}},
	{"direction", voacapCacheKey{1, 1, "a", 1, 1, 1, 1, "a"}, voacapCacheKey{1, 1, "a", 1, 1, 1, 1, "z"}},
}

var livePolicyDelayTies = []struct {
	name          string
	keep, discard voacapDelayKey
}{
	{"user", voacapDelayKey{2, 10, "z", 10, "z"}, voacapDelayKey{10, 2, "a", 2, "a"}},
	{"dx", voacapDelayKey{1, 2, "z", 10, "z"}, voacapDelayKey{1, 10, "a", 2, "a"}},
	{"band", voacapDelayKey{1, 1, "a", 10, "z"}, voacapDelayKey{1, 1, "z", 2, "a"}},
	{"frequency", voacapDelayKey{1, 1, "a", 2, "z"}, voacapDelayKey{1, 1, "a", 10, "a"}},
	{"direction", voacapDelayKey{1, 1, "a", 1, "a"}, voacapDelayKey{1, 1, "a", 1, "z"}},
}

func TestVOACAPLiveCachePolicyTies(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, tc := range livePolicyCacheTies {
			t.Run(fmt.Sprintf("cache/%s/reverse%t", tc.name, reverse), func(t *testing.T) {
				f, now := newLivePolicyFixture(t, 2)
				a, b := tc.keep, tc.discard
				if reverse {
					a, b = b, a
				}
				f.addCacheLocked(a, voacapCacheEntry{storedAt: now})
				f.addCacheLocked(b, voacapCacheEntry{storedAt: now})
				f.addCacheLocked(tc.discard, voacapCacheEntry{storedAt: now}) // equal-time refresh has no tie advantage
				incoming := voacapCacheKey{userCell: 100}
				f.addCacheLocked(incoming, voacapCacheEntry{storedAt: now.Add(-time.Hour)})
				_, kept := f.cache[tc.keep]
				_, removed := f.cache[tc.discard]
				_, admitted := f.cache[incoming]
				if len(f.cache) != 2 || !kept || removed || !admitted {
					t.Fatal("wrong literal survivors or rejected older incoming entry")
				}
			})
		}
		for _, tc := range livePolicyDelayTies {
			t.Run(fmt.Sprintf("delay/%s/reverse%t", tc.name, reverse), func(t *testing.T) {
				f, now := newLivePolicyFixture(t, 2)
				a, b := tc.keep, tc.discard
				if reverse {
					a, b = b, a
				}
				f.addDelayLocked(a, voacapDelayEntry{firstSeen: now.Add(-time.Hour), lastSeen: now})
				f.addDelayLocked(b, voacapDelayEntry{firstSeen: now.Add(time.Hour), lastSeen: now})
				f.addDelayLocked(tc.discard, voacapDelayEntry{firstSeen: now, lastSeen: now})
				incoming := voacapDelayKey{userCell: 100}
				f.addDelayLocked(incoming, voacapDelayEntry{lastSeen: now.Add(-time.Hour)})
				_, kept := f.delays[tc.keep]
				_, removed := f.delays[tc.discard]
				_, admitted := f.delays[incoming]
				if len(f.delays) != 2 || !kept || removed || !admitted {
					t.Fatal("wrong literal survivors or rejected older incoming entry")
				}
			})
		}
	}
}

func TestVOACAPLiveCachePolicyBoundedChurn(t *testing.T) {
	for _, capacity := range []int{1, 2} {
		f, now := newLivePolicyFixture(t, capacity)
		for id := 1; id <= 1000; id++ {
			c, d := livePolicyKeys(id)
			at := now.Add(time.Duration(id) * time.Second)
			f.addCacheLocked(c, voacapCacheEntry{storedAt: at})
			f.addDelayLocked(d, voacapDelayEntry{firstSeen: now.Add(-time.Duration(id) * time.Hour), lastSeen: at})
			want := min(id, capacity)
			if len(f.cache) != want || len(f.delays) != want {
				t.Fatal("live map exceeded its capacity")
			}
			for survivor := id - want + 1; survivor <= id; survivor++ {
				c, d := livePolicyKeys(survivor)
				if _, ok := f.cache[c]; !ok {
					t.Fatalf("cache lost newest survivor %d", survivor)
				}
				if _, ok := f.delays[d]; !ok {
					t.Fatalf("delay lost newest lastSeen survivor %d", survivor)
				}
			}
		}
		// Even at capacity one, an older incoming entry must be admitted.
		c, d := livePolicyKeys(1001)
		f.addCacheLocked(c, voacapCacheEntry{storedAt: now.Add(-time.Hour)})
		f.addDelayLocked(d, voacapDelayEntry{lastSeen: now.Add(-time.Hour)})
		if _, ok := f.cache[c]; !ok {
			t.Fatal("older incoming cache entry rejected")
		}
		if _, ok := f.delays[d]; !ok {
			t.Fatal("older incoming delay entry rejected")
		}
		f.addCacheLocked(c, voacapCacheEntry{storedAt: now, forecast: VOACAPClosedForecast{OutputPath: "refreshed"}})
		f.addDelayLocked(d, voacapDelayEntry{firstSeen: now, lastSeen: now})
		if len(f.cache) != capacity || len(f.delays) != capacity || f.cache[c].forecast.OutputPath != "refreshed" || f.delays[d].firstSeen != now {
			t.Fatal("replacement after bounded churn lost an owner or incoming state")
		}
	}
}

func TestVOACAPLiveCachePolicyExpiry(t *testing.T) {
	for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		f, now := newLivePolicyFixture(t, 2)
		f.fallback.CacheTTLSeconds, f.fallback.DelaySeconds = 60, 10
		c, d := livePolicyKeys(1)
		f.addCacheLocked(c, voacapCacheEntry{storedAt: now.Add(-time.Minute - delta)})
		f.addDelayLocked(d, voacapDelayEntry{firstSeen: now.Add(-24 * time.Hour), lastSeen: now.Add(-70*time.Second - delta)})
		f.pruneLocked(now)
		_, cachePresent := f.cache[c]
		_, delayPresent := f.delays[d]
		if cachePresent != (delta <= 0) || delayPresent != (delta <= 0) || !f.lastPrune.Equal(now) {
			t.Fatalf("strict TTL boundary %s violated", delta)
		}
	}
	for _, advance := range []time.Duration{-time.Hour, time.Minute - time.Nanosecond, time.Minute, time.Minute + time.Nanosecond} {
		f, now := newLivePolicyFixture(t, 1)
		f.fallback.CacheTTLSeconds, f.fallback.DelaySeconds = 60, 10
		f.lastPrune = now
		c, d := livePolicyKeys(1)
		f.addCacheLocked(c, voacapCacheEntry{storedAt: now.Add(-24 * time.Hour)})
		f.addDelayLocked(d, voacapDelayEntry{lastSeen: now.Add(-24 * time.Hour)})
		f.pruneLocked(now.Add(advance))
		wantSweep := advance >= time.Minute
		if (len(f.cache) == 0) != wantSweep || (len(f.delays) == 0) != wantSweep {
			t.Fatalf("minute/backward gate %s violated", advance)
		}
		wantLast := now
		if wantSweep {
			wantLast = now.Add(advance)
		}
		if !f.lastPrune.Equal(wantLast) {
			t.Fatal("suppressed sweep moved lastPrune")
		}
	}
}

func TestVOACAPLiveCachePolicyDelayTouch(t *testing.T) {
	f, now := newLivePolicyFixture(t, 1)
	f.fallback.DelaySeconds = 10
	_, key := livePolicyKeys(1)
	for _, tc := range []struct {
		at      time.Time
		elapsed bool
	}{
		{now, false}, {now.Add(10*time.Second - time.Nanosecond), false},
		{now.Add(10 * time.Second), true}, {now.Add(-time.Second), false},
	} {
		if got := f.delayElapsedLocked(key, tc.at); got != tc.elapsed {
			t.Fatalf("delay at %v = %t", tc.at, got)
		}
		if got := f.delays[key]; !got.firstSeen.Equal(now) || !got.lastSeen.Equal(tc.at) {
			t.Fatalf("touch changed firstSeen or ignored backward lastSeen: %+v", got)
		}
	}
}

func TestVOACAPLiveCachePolicyDurationArithmetic(t *testing.T) {
	// Preserve the legacy expression, including duration overflow; replacing it
	// with a deadline comparison would change these outcomes.
	for _, seconds := range []int{60, int(^uint(0)>>1) / 2} {
		for _, age := range []time.Duration{-time.Hour, 0, time.Hour} {
			f, now := newLivePolicyFixture(t, 1)
			f.fallback.CacheTTLSeconds, f.fallback.DelaySeconds = seconds, 10
			c, d := livePolicyKeys(1)
			at := now.Add(-age)
			f.addCacheLocked(c, voacapCacheEntry{storedAt: at})
			f.addDelayLocked(d, voacapDelayEntry{firstSeen: at, lastSeen: at})
			cacheExpired := now.Sub(at) > time.Duration(seconds)*time.Second
			delayExpired := now.Sub(at) > time.Duration(seconds+10)*time.Second
			f.pruneLocked(now)
			if (len(f.cache) == 0) != cacheExpired || (len(f.delays) == 0) != delayExpired {
				t.Fatalf("legacy duration arithmetic changed for seconds=%d age=%s", seconds, age)
			}
		}
	}
}
