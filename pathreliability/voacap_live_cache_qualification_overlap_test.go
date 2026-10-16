// File role: Bounded common-source reader-blocking qualification for value maps.
// Every interval shares one immutable benchmark clock. Readers always cancel,
// release and join before that clock stops; setup/state checks are not timed.
package pathreliability

import (
	"context"
	"dxcluster/internal/testsupport"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func liveQualificationConcurrent(name string) bool {
	switch name {
	case "overlap_half", "overlap_all", "block_insert_cache", "block_insert_delay", "block_insert_cache_tied", "block_insert_delay_tied":
		return true
	}
	return false
}

type liveQualificationReaders struct {
	start    chan struct{}
	release  sync.Once
	wait     sync.WaitGroup
	cancel   context.CancelFunc
	finished bool
}

func newLiveQualificationReaders(ctx context.Context, cancel context.CancelFunc, count int, work func(int)) *liveQualificationReaders {
	g := &liveQualificationReaders{start: make(chan struct{}), cancel: cancel}
	g.wait.Add(count)
	for reader := 0; reader < count; reader++ {
		go func(reader int) {
			defer g.wait.Done()
			select {
			case <-ctx.Done():
				return
			case <-g.start:
			}
			work(reader)
		}(reader)
	}
	return g
}

func (g *liveQualificationReaders) Release() { g.release.Do(func() { close(g.start) }) }

func (g *liveQualificationReaders) Join() {
	g.wait.Wait()
	g.finished = true
}

func (g *liveQualificationReaders) Close() {
	if !g.finished {
		g.cancel()
	}
	g.Release()
	g.wait.Wait()
}

func liveQualificationOverlapSettings(t *testing.T, r *liveQualificationReport) {
	t.Helper()
	r.Passes, r.MinimumOverlapSamples = 256, 1000
	for _, item := range []struct {
		name   string
		target *int
	}{{"GOCLUSTER_LIVE_OVERLAP_PASSES", &r.Passes}, {"GOCLUSTER_LIVE_MIN_OVERLAP", &r.MinimumOverlapSamples}} {
		if s := os.Getenv(item.name); s != "" {
			v, e := strconv.Atoi(s)
			if e != nil {
				t.Fatal(e)
			}
			*item.target = v
		}
	}
	if (r.Passes != 64 && r.Passes != 256 && r.Passes != 512 && r.Passes != 1024) || (r.MinimumOverlapSamples != 0 && r.MinimumOverlapSamples != 1000) || (r.Passes == 64) != (r.MinimumOverlapSamples == 0) {
		t.Fatal("invalid pilot/scored overlap settings")
	}
	r.Readers, r.ReadsPerReader = 8, 256
}

func liveQualificationOverlap(t *testing.T, r *liveQualificationReport) {
	t.Helper()
	if r.N != 50000 {
		t.Fatal("blocking qualification requires50000 entries")
	}
	liveQualificationOverlapSettings(t, r)
	testsupport.RunFixedBenchmark(t, func(_ *testing.B, ctx context.Context, cancel context.CancelFunc, clock *testsupport.Clock) bool {
		r.ClockSource, r.ClockFrequencyHz, r.ClockProtocol = clock.Source(), clock.FrequencyHz(), testsupport.FixedBenchmarkProtocol
		for p := 0; p < r.Passes; p++ {
			if ctx.Err() != nil {
				r.Canceled = true
				return false
			}
			if !liveQualificationConcurrentPass(ctx, t, cancel, clock, r, p) {
				return false
			}
		}
		r.Completed = true
		return true
	})
	r.Operations = r.Passes * r.Readers * r.ReadsPerReader
	r.MaintenanceOperations = r.Passes
	liveQualificationScoreIntervals(r)
	if err := liveQualificationValidateConcurrent(r); err != nil {
		r.Valid = false
		r.Failure = err.Error()
	}
}

func liveQualificationConcurrentPass(ctx context.Context, t *testing.T, cancel context.CancelFunc, clock *testsupport.Clock, r *liveQualificationReport, pass int) bool {
	insert := strings.HasPrefix(r.Case, "block_insert_")
	tied := strings.HasSuffix(r.Case, "_tied")
	expired, population, lookupID := r.N/2, r.N+1, r.N
	if r.Case == "overlap_all" {
		expired = r.N
	}
	if insert {
		expired, population, lookupID = 0, r.N, r.N-1
	}
	f := liveQualificationSeed(t, population, expired, 8, 0)
	if tied {
		for i := 0; i < population-1; i++ {
			ck, dk := liveQualificationKeys(i)
			c, d := f.cache[ck], f.delays[dk]
			c.storedAt = liveQualificationNow()
			d.firstSeen, d.lastSeen = liveQualificationNow(), liveQualificationNow()
			f.cache[ck], f.delays[dk] = c, d
		}
	}
	key, _ := liveQualificationKeys(lookupID)
	req := testClosedRequest()
	req.UserCell, req.DXCell = key.userCell, key.dxCell
	w := liveQualificationWitness{Pass: pass, CacheBefore: len(f.cache), DelayBefore: len(f.delays), Operations: 2048, ExpectedExpired: expired}
	if !f.lastPrune.IsZero() {
		w.PruneBeforeNS = f.lastPrune.UnixNano()
	}
	samples := make([][]liveQualificationInterval, 8)
	for i := range samples {
		samples[i] = make([]liveQualificationInterval, 256)
	}
	readers := newLiveQualificationReaders(ctx, cancel, 8, func(reader int) {
		for seq := 0; seq < 256; seq++ {
			if ctx.Err() != nil {
				return
			}
			s := liveQualificationInterval{Pass: pass, Reader: reader, Sequence: seq, StartNS: clock.NowNS()}
			func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				s.AcquiredNS = clock.NowNS()
				_, s.Ready, _ = f.cachedForecastWindowLocked(key, req, liveQualificationNow())
			}()
			s.EndNS = clock.NowNS()
			samples[reader][seq] = s
		}
	})
	defer readers.Close()
	ck, dk := liveQualificationKeys(population)
	at := liveQualificationNow().Add(time.Duration(population + 1))
	entry := liveQualificationEntry(at, 8)
	work := liveQualificationWorkInterval{Pass: pass, StartNS: clock.NowNS()}
	readers.Release()
	runtime.Gosched()
	hold := liveQualificationMaintenance{Pass: pass, RequestStartNS: clock.NowNS()}
	func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		hold.StartNS = clock.NowNS()
		switch r.Case {
		case "block_insert_cache", "block_insert_cache_tied":
			f.addCacheLocked(ck, entry)
		case "block_insert_delay", "block_insert_delay_tied":
			f.addDelayLocked(dk, voacapDelayEntry{firstSeen: at, lastSeen: at})
		default:
			f.pruneLocked(liveQualificationNow())
		}
		hold.EndNS = clock.NowNS()
	}()
	hold.RequestEndNS = clock.NowNS()
	work.CycleEndNS = hold.RequestEndNS
	w.TriggerCompleted = true
	readers.Join()
	work.EndNS = clock.NowNS()
	if ctx.Err() != nil {
		r.Canceled = true
		return false
	}
	w.CacheAfter, w.DelayAfter = len(f.cache), len(f.delays)
	if !f.lastPrune.IsZero() {
		w.PruneAfterNS = f.lastPrune.UnixNano()
	}
	for _, row := range samples {
		for _, s := range row {
			if s.Ready {
				w.SuccessfulOperations++
			}
		}
		r.Intervals = append(r.Intervals, row...)
	}
	if insert {
		liveQualificationCheckBlockingInsert(f, r.Case, population, &w)
	} else {
		liveQualificationCheckState(f, r.Case, population, expired, 1, &w)
	}
	r.Witnesses = append(r.Witnesses, w)
	r.Maintenance = append(r.Maintenance, hold)
	r.WorkIntervals = append(r.WorkIntervals, work)
	runtime.KeepAlive(f)
	return true
}

func liveQualificationCheckBlockingInsert(f *VOACAPClosedFallback, name string, n int, w *liveQualificationWitness) {
	tied := strings.HasSuffix(name, "_tied")
	cacheInsert := strings.Contains(name, "cache")
	for i := 0; i <= n; i++ {
		ck, dk := liveQualificationKeys(i)
		c, co := f.cache[ck]
		d, do := f.delays[dk]
		wc, wd := i < n, i < n
		if cacheInsert {
			wc = i > 0
		} else {
			wd = i > 0
		}
		at := liveQualificationNow().Add(time.Duration(i))
		if tied && i < n-1 {
			at = liveQualificationNow()
		}
		if i == n {
			at = liveQualificationNow().Add(time.Duration(n + 1))
		}
		w.StateKeysChecked += 2
		if tied && cacheInsert && i < n-1 {
			if !co || c.storedAt.Equal(at) {
				w.StateKeysMatched++
			}
			if co {
				w.OldRemaining++
			}
		} else if co == wc && (!co || c.storedAt.Equal(at)) {
			w.StateKeysMatched++
		}
		if tied && !cacheInsert && i < n-1 {
			if !do || d.lastSeen.Equal(at) {
				w.StateKeysMatched++
			}
			if do {
				w.OldRemaining++
			}
		} else if do == wd && (!do || d.lastSeen.Equal(at)) {
			w.StateKeysMatched++
		}
	}
}

func liveQualificationQuantile(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[int(math.Ceil(.99*float64(len(values))))-1]
}

func liveQualificationScoreIntervals(r *liveQualificationReport) {
	all, overlap, triggers := make([]int64, 0, len(r.Intervals)), make([]int64, 0, r.Passes*8), make([]int64, 0, r.Passes)
	r.ElapsedNS, r.TriggerNS = 0, 0
	for _, work := range r.WorkIntervals {
		r.ElapsedNS += work.EndNS - work.StartNS
	}
	for _, hold := range r.Maintenance {
		v := hold.RequestEndNS - hold.RequestStartNS
		r.TriggerNS += v
		triggers = append(triggers, v)
	}
	for _, s := range r.Intervals {
		all = append(all, s.EndNS-s.StartNS)
		if s.Pass >= 0 && s.Pass < len(r.Maintenance) {
			hold := r.Maintenance[s.Pass]
			if s.StartNS < hold.EndNS && s.AcquiredNS > hold.EndNS {
				overlap = append(overlap, s.EndNS-s.StartNS)
			}
		}
	}
	r.LookupP99NS, r.OverlapP99NS, r.TriggerP99NS = liveQualificationQuantile(all), liveQualificationQuantile(overlap), liveQualificationQuantile(triggers)
	r.OverlapSamples = len(overlap)
	if r.Operations > 0 {
		r.NSPerOperation = float64(r.ElapsedNS) / float64(r.Operations)
	}
}

func liveQualificationValidateConcurrent(r *liveQualificationReport) error {
	if !r.Completed || r.Canceled || !liveQualificationConcurrent(r.Case) || r.Passes <= 0 || r.Readers != 8 || r.ReadsPerReader != 256 || r.ClockSource != "testing.B.Elapsed(shared live benchmark)" || r.ClockFrequencyHz <= 0 || r.ClockProtocol != testsupport.FixedBenchmarkProtocol {
		return fmt.Errorf("concurrent identity or completion invalid")
	}
	if len(r.WorkIntervals) != r.Passes || len(r.Witnesses) != r.Passes || len(r.Maintenance) != r.Passes || len(r.Intervals) != r.Passes*2048 || r.Operations != len(r.Intervals) || r.MaintenanceOperations != r.Passes {
		return fmt.Errorf("concurrent population incomplete")
	}
	var elapsed int64
	for p, work := range r.WorkIntervals {
		if work.Pass != p || work.StartNS < 0 || work.CycleEndNS <= work.StartNS || work.EndNS < work.CycleEndNS || (p > 0 && work.StartNS < r.WorkIntervals[p-1].EndNS) {
			return fmt.Errorf("work interval malformed")
		}
		elapsed += work.EndNS - work.StartNS
		h := r.Maintenance[p]
		if h.Pass != p || h.Sequence != 0 || h.RequestStartNS < work.StartNS || h.StartNS < h.RequestStartNS || h.EndNS <= h.StartNS || h.RequestEndNS < h.EndNS || h.RequestEndNS > work.CycleEndNS {
			return fmt.Errorf("maintenance interval malformed")
		}
		overlaps := 0
		for reader := 0; reader < 8; reader++ {
			var previous int64
			for seq := 0; seq < 256; seq++ {
				s := r.Intervals[(p*8+reader)*256+seq]
				if s.Pass != p || s.Reader != reader || s.Sequence != seq || !s.Ready || s.StartNS < work.StartNS || s.AcquiredNS < s.StartNS || s.EndNS < s.AcquiredNS || s.EndNS <= s.StartNS || s.EndNS > work.EndNS || s.StartNS < previous {
					return fmt.Errorf("reader interval malformed or unresolved")
				}
				previous = s.EndNS
				if s.StartNS < h.EndNS && s.AcquiredNS > h.EndNS {
					overlaps++
				}
			}
		}
		if overlaps > 8 {
			return fmt.Errorf("impossible overlap population")
		}
		if err := liveQualificationValidateWitness(r, p); err != nil {
			return err
		}
	}
	if elapsed <= 0 || elapsed != r.ElapsedNS || r.LookupP99NS <= 0 || r.OverlapSamples < r.MinimumOverlapSamples || (r.MinimumOverlapSamples > 0 && r.OverlapP99NS <= 0) {
		return fmt.Errorf("unresolved elapsed or insufficient overlap")
	}
	computed := *r
	liveQualificationScoreIntervals(&computed)
	if computed.LookupP99NS != r.LookupP99NS || computed.OverlapP99NS != r.OverlapP99NS || computed.OverlapSamples != r.OverlapSamples || computed.TriggerNS != r.TriggerNS || computed.TriggerP99NS != r.TriggerP99NS || computed.NSPerOperation != r.NSPerOperation {
		return fmt.Errorf("raw intervals do not reproduce aggregates")
	}
	return nil
}

func liveQualificationValidateWitness(r *liveQualificationReport, p int) error {
	insert := strings.HasPrefix(r.Case, "block_insert_")
	before, expired := r.N+1, r.N/2
	if r.Case == "overlap_all" {
		expired = r.N
	}
	if insert {
		before, expired = r.N, 0
	}
	old := 0
	if strings.HasSuffix(r.Case, "_tied") {
		old = r.N - 2
	}
	checked := 2 * before
	if insert {
		checked = 2 * (r.N + 1)
	}
	prune := liveQualificationNow().UnixNano()
	if insert {
		prune = 0
	}
	w := r.Witnesses[p]
	if w.Pass != p || w.CacheBefore != before || w.DelayBefore != before || w.CacheAfter != before-expired || w.DelayAfter != before-expired || w.ExpectedExpired != expired || w.Operations != 2048 || w.SuccessfulOperations != 2048 || w.StateKeysChecked != checked || w.StateKeysMatched != checked || !w.TriggerCompleted || w.PruneBeforeNS != 0 || w.PruneAfterNS != prune || w.OldRemaining != old {
		return fmt.Errorf("concurrent state witness mismatch")
	}
	return nil
}
