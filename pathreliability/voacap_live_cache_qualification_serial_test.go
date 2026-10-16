// File role: Common-source bounded serial qualification of live value maps.
// Setup, restoration and full state oracles stay outside immutable-clock work
// intervals. Small sweeps use prepared independent owners to resolve real work.
package pathreliability

import (
	"context"
	"dxcluster/internal/testsupport"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// N=1 has no competing equal-age victims; its tied label covers the same
// single-owner capacity cost. Larger tied batches never outlive the seed group.
var liveQualificationSerialCases = []string{"cache_hit", "delay_touch", "delay_repeat", "replace_oldest", "insert_cache", "insert_delay", "insert_cache_tied", "insert_delay_tied", "sweep_none", "expiry_one", "expiry_10", "expiry_half", "expiry_all"}

func liveQualificationSerialShape(name string, n int) (passes, batch, operations, expired int) {
	passes, batch, operations = 1, 1, 100000
	switch name {
	case "cache_hit", "delay_touch", "delay_repeat":
	case "replace_oldest", "insert_cache", "insert_delay", "insert_cache_tied", "insert_delay_tied":
		operations = 64
		switch n {
		case 1:
			operations = 16384
		case 64:
			operations = 4096
			if strings.HasSuffix(name, "_tied") {
				operations = n
			}
		}
	case "sweep_none", "expiry_one", "expiry_10", "expiry_half", "expiry_all":
		passes, operations = 8, 1
		switch n {
		case 1:
			batch = 1024
		case 64:
			batch = 64
		}
		switch name {
		case "expiry_one":
			expired = 1
		case "expiry_10":
			expired = max(1, n/10)
		case "expiry_half":
			expired = max(1, n/2)
		case "expiry_all":
			expired = n
		}
	default:
		return 0, 0, 0, 0
	}
	return
}

func liveQualificationScale(t *testing.T, name string, n int) int {
	t.Helper()
	value := os.Getenv("GOCLUSTER_LIVE_SCALE")
	if encoded := os.Getenv("GOCLUSTER_LIVE_SCALES"); encoded != "" {
		var scales map[string]int
		if err := json.Unmarshal([]byte(encoded), &scales); err != nil {
			t.Fatal(err)
		}
		scale, ok := scales[fmt.Sprintf("%s/%d", name, n)]
		if !ok {
			t.Fatal("missing frozen serial scale")
		}
		value = strconv.Itoa(scale)
	}
	if value == "" {
		value = "1"
	}
	scale, err := strconv.Atoi(value)
	if err != nil || (scale != 1 && scale != 4 && scale != 16 && scale != 64 && scale != 256 && scale != 1024) {
		t.Fatal("invalid frozen serial scale")
	}
	return scale
}

type liveQualificationPrepared struct {
	f      *VOACAPClosedFallback
	cache  []voacapCacheEntry
	delays []voacapDelayEntry
}

func liveQualificationPrepare(t *testing.T, n, expired int, tied bool) liveQualificationPrepared {
	p := liveQualificationPrepared{f: liveQualificationSeed(t, n, expired, 8, 0), cache: make([]voacapCacheEntry, n), delays: make([]voacapDelayEntry, n)}
	for i := 0; i < n; i++ {
		ck, dk := liveQualificationKeys(i)
		p.cache[i], p.delays[i] = p.f.cache[ck], p.f.delays[dk]
		if tied {
			p.cache[i].storedAt = liveQualificationNow()
			p.delays[i] = voacapDelayEntry{firstSeen: liveQualificationNow(), lastSeen: liveQualificationNow()}
		}
	}
	return p
}

// Value-map restoration is intentionally outside timing. No historical key or
// forecast survives a pass through either live owner; saved inputs are bounded.
func (p *liveQualificationPrepared) reset() {
	clear(p.f.cache)
	clear(p.f.delays)
	for i := range p.cache {
		ck, dk := liveQualificationKeys(i)
		p.f.cache[ck] = p.cache[i]
		p.f.delays[dk] = p.delays[i]
	}
	p.f.lastPrune = time.Time{}
}

func liveQualificationPerformance(t *testing.T, r *liveQualificationReport) {
	t.Helper()
	passes, batch, operations, expired := liveQualificationSerialShape(r.Case, r.N)
	if passes == 0 {
		t.Fatal("unknown serial case")
	}
	r.Scale = liveQualificationScale(t, r.Case, r.N)
	r.Passes = passes * r.Scale
	r.Batch = batch
	r.WorkOperations = operations
	r.Operations = r.Passes * batch * operations
	if r.Case == "replace_oldest" {
		r.Operations *= 2
	}
	prepared := make([]liveQualificationPrepared, batch)
	tied := strings.HasSuffix(r.Case, "_tied")
	for i := range prepared {
		prepared[i] = liveQualificationPrepare(t, r.N, expired, tied)
	}
	keys := make([]voacapCacheKey, max(r.N, operations))
	delays := make([]voacapDelayKey, len(keys))
	newKeys := make([]voacapCacheKey, operations)
	newDelays := make([]voacapDelayKey, operations)
	for i := range keys {
		keys[i], delays[i] = liveQualificationKeys(i)
	}
	for i := range newKeys {
		newKeys[i], newDelays[i] = liveQualificationKeys(r.N + i)
	}
	now := liveQualificationNow()
	entry := liveQualificationEntry(now, 8)
	req := testClosedRequest()
	req.UserCell, req.DXCell = keys[0].userCell, keys[0].dxCell
	r.Witnesses = make([]liveQualificationWitness, 0, r.Passes)
	r.WorkIntervals = make([]liveQualificationWorkInterval, 0, r.Passes)
	testsupport.RunFixedBenchmark(t, func(_ *testing.B, ctx context.Context, _ context.CancelFunc, clock *testsupport.Clock) bool {
		r.ClockSource, r.ClockFrequencyHz, r.ClockProtocol = clock.Source(), clock.FrequencyHz(), testsupport.FixedBenchmarkProtocol
		for pass := 0; pass < r.Passes; pass++ {
			if ctx.Err() != nil {
				r.Canceled = true
				return false
			}
			w := liveQualificationWitness{Pass: pass, Operations: r.Operations / r.Passes, ExpectedExpired: batch * expired}
			for i := range prepared {
				prepared[i].reset()
				w.CacheBefore += len(prepared[i].f.cache)
				w.DelayBefore += len(prepared[i].f.delays)
				if !prepared[i].f.lastPrune.IsZero() {
					w.PruneBeforeNS = prepared[i].f.lastPrune.UnixNano()
				}
			}
			work := liveQualificationWorkInterval{Pass: pass, StartNS: clock.NowNS()}
			for j := range prepared {
				f := prepared[j].f
				for i := 0; i < operations; i++ {
					switch r.Case {
					case "cache_hit":
						if _, ok := f.CheckCachedForecast(req, now); ok {
							w.SuccessfulOperations++
						}
					case "delay_touch", "delay_repeat":
						index := i % r.N
						if r.Case == "delay_repeat" {
							index = 0
						}
						f.delayElapsedLocked(delays[index], now.Add(time.Duration(i+1)))
						w.SuccessfulOperations++
					case "replace_oldest":
						index := i % r.N
						at := now.Add(time.Duration(r.N + i + 1))
						entry.storedAt = at
						f.addCacheLocked(keys[index], entry)
						f.addDelayLocked(delays[index], voacapDelayEntry{firstSeen: at, lastSeen: at})
						w.SuccessfulOperations += 2
					case "insert_cache", "insert_cache_tied":
						entry.storedAt = now.Add(time.Duration(r.N + i + 1))
						f.addCacheLocked(newKeys[i], entry)
						w.SuccessfulOperations++
					case "insert_delay", "insert_delay_tied":
						at := now.Add(time.Duration(r.N + i + 1))
						f.addDelayLocked(newDelays[i], voacapDelayEntry{firstSeen: at, lastSeen: at})
						w.SuccessfulOperations++
					default:
						f.pruneLocked(now)
						w.SuccessfulOperations++
					}
				}
			}
			work.EndNS = clock.NowNS()
			work.CycleEndNS = work.EndNS
			r.ElapsedNS += work.EndNS - work.StartNS
			for j := range prepared {
				f := prepared[j].f
				w.CacheAfter += len(f.cache)
				w.DelayAfter += len(f.delays)
				if !f.lastPrune.IsZero() {
					w.PruneAfterNS = f.lastPrune.UnixNano()
				}
				liveQualificationCheckState(f, r.Case, r.N, expired, operations, &w)
			}
			r.Witnesses = append(r.Witnesses, w)
			r.WorkIntervals = append(r.WorkIntervals, work)
		}
		r.Completed = true
		return true
	})
	r.NSPerOperation = float64(r.ElapsedNS) / float64(r.Operations)
	if err := liveQualificationValidateSerial(r); err != nil {
		r.Valid = false
		r.Failure = err.Error()
	}
}

// Tied victims are allowed to differ in the original baseline. Verify survivor
// timestamp classes and population, plus exact admission of newer input keys.
func liveQualificationCheckState(f *VOACAPClosedFallback, name string, n, expired, operations int, w *liveQualificationWitness) {
	now := liveQualificationNow()
	tied := strings.HasSuffix(name, "_tied")
	cacheInsert := strings.HasPrefix(name, "insert_cache")
	delayInsert := strings.HasPrefix(name, "insert_delay")
	for i := 0; i < n; i++ {
		ck, dk := liveQualificationKeys(i)
		c, co := f.cache[ck]
		d, do := f.delays[dk]
		wc, wd := i >= expired, i >= expired
		if cacheInsert {
			wc = i >= min(n, operations)
		}
		if delayInsert {
			wd = i >= min(n, operations)
		}
		w.StateKeysChecked += 2
		if tied && cacheInsert {
			if !co || c.storedAt.Equal(now) {
				w.StateKeysMatched++
			}
			if co {
				w.OldRemaining++
			}
		} else if co == wc {
			w.StateKeysMatched++
		}
		if tied && delayInsert {
			if !do || d.lastSeen.Equal(now) {
				w.StateKeysMatched++
			}
			if do {
				w.OldRemaining++
			}
		} else if do == wd {
			w.StateKeysMatched++
		}
		if name == "replace_oldest" && i < operations {
			last := i + ((operations-1-i)/n)*n
			want := now.Add(time.Duration(n + last + 1))
			w.StateKeysChecked += 2
			if co && c.storedAt.Equal(want) {
				w.StateKeysMatched++
			}
			if do && d.lastSeen.Equal(want) {
				w.StateKeysMatched++
			}
		}
		if (name == "delay_touch" && i < operations) || (name == "delay_repeat" && i == 0) {
			last := operations - 1
			if name == "delay_touch" {
				last = i + ((operations-1-i)/n)*n
			}
			w.StateKeysChecked += 2
			if do && d.lastSeen.Equal(now.Add(time.Duration(last+1))) {
				w.StateKeysMatched++
			}
			if do && d.firstSeen.Equal(now.Add(time.Duration(i))) {
				w.StateKeysMatched++
			}
		}
	}
	if cacheInsert || delayInsert {
		for i := 0; i < operations; i++ {
			ck, dk := liveQualificationKeys(n + i)
			want := i >= max(0, operations-n)
			at := now.Add(time.Duration(n + i + 1))
			w.StateKeysChecked++
			if cacheInsert {
				c, ok := f.cache[ck]
				if ok == want && (!ok || c.storedAt.Equal(at)) {
					w.StateKeysMatched++
				}
			} else {
				d, ok := f.delays[dk]
				if ok == want && (!ok || d.lastSeen.Equal(at)) {
					w.StateKeysMatched++
				}
			}
		}
	}
}

func liveQualificationValidateSerial(r *liveQualificationReport) error {
	passes, batch, ops, expired := liveQualificationSerialShape(r.Case, r.N)
	passes *= r.Scale
	count := batch * ops
	if r.Case == "replace_oldest" {
		count *= 2
	}
	if !r.Completed || r.Canceled || passes <= 0 || r.Passes != passes || r.Batch != batch || r.WorkOperations != ops || r.Operations != passes*count || len(r.Witnesses) != passes || len(r.WorkIntervals) != passes {
		return fmt.Errorf("serial shape/completion mismatch")
	}
	var elapsed int64
	for p, w := range r.Witnesses {
		work := r.WorkIntervals[p]
		if work.Pass != p || work.StartNS < 0 || work.EndNS <= work.StartNS || work.CycleEndNS != work.EndNS || (p > 0 && work.StartNS < r.WorkIntervals[p-1].EndNS) {
			return fmt.Errorf("unresolved or nonmonotonic serial interval")
		}
		elapsed += work.EndNS - work.StartNS
		checked := 2 * r.N
		if r.Case == "replace_oldest" || r.Case == "delay_touch" {
			checked += 2 * min(r.N, ops)
		} else if r.Case == "delay_repeat" {
			checked += 2
		} else if strings.HasPrefix(r.Case, "insert_") {
			checked += ops
		}
		old := 0
		if strings.HasSuffix(r.Case, "_tied") {
			old = max(0, r.N-ops)
		}
		prune := int64(0)
		if r.Case == "sweep_none" || expired > 0 {
			prune = liveQualificationNow().UnixNano()
		}
		if w.Pass != p || w.CacheBefore != batch*r.N || w.DelayBefore != batch*r.N || w.CacheAfter != batch*(r.N-expired) || w.DelayAfter != batch*(r.N-expired) || w.Operations != count || w.SuccessfulOperations != count || w.ExpectedExpired != batch*expired || w.PruneBeforeNS != 0 || w.PruneAfterNS != prune || w.StateKeysChecked != batch*checked || w.StateKeysMatched != w.StateKeysChecked || w.OldRemaining != batch*old {
			return fmt.Errorf("serial state witness mismatch at pass %d", p)
		}
	}
	if elapsed != r.ElapsedNS || elapsed <= 0 || r.NSPerOperation != float64(elapsed)/float64(r.Operations) || r.ClockSource != "testing.B.Elapsed(shared live benchmark)" || r.ClockFrequencyHz <= 0 || r.ClockProtocol != testsupport.FixedBenchmarkProtocol {
		return fmt.Errorf("serial aggregate/clock mismatch")
	}
	return nil
}
