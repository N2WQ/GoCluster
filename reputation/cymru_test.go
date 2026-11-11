package reputation

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"
)

func cymruTestAddr(i int) netip.Addr {
	return netip.AddrFrom4([4]byte{198, 18, byte(i >> 8), byte(i)})
}

func cymruPendingCount(c *cymruResolver) int {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return len(c.pending)
}

func TestCymruPendingQueueAdmission(t *testing.T) {
	c := newCymruResolver(true, time.Second, time.Minute, time.Second, 32, 2)
	now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
	for i := 0; i < cap(c.queue)+3000; i++ {
		addr := cymruTestAddr(i)
		c.enqueue(addr, addr.String(), now)
	}
	if got := cymruPendingCount(c); got != cap(c.queue) {
		t.Fatalf("pending=%d for queue capacity %d; rejected IPs must own no reservation", got, cap(c.queue))
	}
	for i := 0; i < cap(c.queue); i++ {
		if _, ok := c.pending[cymruTestAddr(i).String()]; !ok {
			t.Fatalf("admitted IP %d lost its pending ownership", i)
		}
	}
}

func TestCymruPendingConcurrentDuplicate(t *testing.T) {
	c := newCymruResolver(true, time.Second, time.Minute, time.Second, 32, 2)
	addr := cymruTestAddr(1)
	now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
	start := make(chan struct{})
	var producers sync.WaitGroup
	for i := 0; i < 32; i++ {
		producers.Go(func() {
			<-start
			c.enqueue(addr, addr.String(), now)
		})
	}
	close(start)
	producers.Wait()
	if got := cymruPendingCount(c); got != 1 || len(c.queue) != 1 {
		t.Fatalf("same-key admission pending=%d queued=%d; want one owner/request", got, len(c.queue))
	}
}

type cymruTestWorker struct {
	entered chan netip.Addr
	release chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
	once    sync.Once
}

func startCymruTestWorker(t *testing.T, c *cymruResolver, result LookupResult, ok bool) *cymruTestWorker {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := &cymruTestWorker{entered: make(chan netip.Addr, cap(c.queue)+1), release: make(chan struct{}), done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(w.done)
		c.runWorker(ctx, func(_ context.Context, addr netip.Addr) (LookupResult, bool) {
			w.entered <- addr
			<-w.release
			return result, ok
		})
	}()
	t.Cleanup(func() { w.finish(t) })
	return w
}

func (w *cymruTestWorker) await(t *testing.T) netip.Addr {
	t.Helper()
	select {
	case addr := <-w.entered:
		return addr
	case <-time.After(3 * time.Second):
		t.Fatal("Cymru worker did not receive request")
		return netip.Addr{}
	}
}

func (w *cymruTestWorker) finish(t *testing.T) {
	t.Helper()
	w.cancel()
	w.once.Do(func() { close(w.release) })
	select {
	case <-w.done:
	case <-time.After(3 * time.Second):
		t.Error("Cymru worker did not retire after its lookup was released")
	}
}

func TestCymruPendingRejectedKeyRetriesImmediately(t *testing.T) {
	c := newCymruResolver(true, time.Second, time.Minute, time.Second, 32, 2)
	now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
	for i := 0; i < cap(c.queue); i++ {
		addr := cymruTestAddr(i)
		c.enqueue(addr, addr.String(), now)
	}
	rejected := cymruTestAddr(500)
	c.enqueue(rejected, rejected.String(), now)
	w := startCymruTestWorker(t, c, LookupResult{}, false)
	w.await(t) // One queue slot is now free, and that request remains active.
	c.enqueue(rejected, rejected.String(), now)
	if len(c.queue) != cap(c.queue) {
		t.Fatalf("rejected key could not retry at the same time: queued=%d want=%d", len(c.queue), cap(c.queue))
	}
	if got := cymruPendingCount(c); got != cap(c.queue)+1 {
		t.Fatalf("pending=%d; want queued plus one active owner=%d", got, cap(c.queue)+1)
	}
}

func TestCymruPendingTTLBoundary(t *testing.T) {
	for _, tc := range []struct {
		offset     time.Duration
		wantQueued int
	}{{5*time.Minute - time.Nanosecond, 1}, {5 * time.Minute, 2}, {5*time.Minute + time.Nanosecond, 2}} {
		t.Run(tc.offset.String(), func(t *testing.T) {
			c := newCymruResolver(true, time.Second, time.Minute, time.Second, 32, 2)
			addr := cymruTestAddr(1)
			now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
			c.enqueue(addr, addr.String(), now)
			first := c.pending[addr.String()]
			c.enqueue(addr, addr.String(), now.Add(tc.offset))
			if len(c.queue) != tc.wantQueued || cymruPendingCount(c) != 1 {
				t.Fatalf("offset=%s queued=%d pending=%d; want queued=%d pending=1", tc.offset, len(c.queue), cymruPendingCount(c), tc.wantQueued)
			}
			if (c.pending[addr.String()] == first) != (tc.wantQueued == 1) {
				t.Fatal("TTL boundary changed the wrong ownership generation")
			}
		})
	}
}

func TestCymruPendingRejectedReplacement(t *testing.T) {
	c := newCymruResolver(true, time.Second, time.Minute, time.Second, 32, 2)
	now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
	for i := 0; i < cap(c.queue); i++ {
		addr := cymruTestAddr(i)
		c.enqueue(addr, addr.String(), now)
	}
	addr := cymruTestAddr(0)
	owner := c.pending[addr.String()]
	c.enqueue(addr, addr.String(), now.Add(c.pendingTTL))
	if c.pending[addr.String()] != owner || len(c.queue) != cap(c.queue) {
		t.Fatal("failed stale retry replaced an admitted owner")
	}
	key := addr.String()
	if allocations := testing.AllocsPerRun(100, func() { c.enqueue(addr, key, now.Add(c.pendingTTL)) }); allocations != 0 {
		t.Fatalf("full-queue rejection allocates %g ownership objects per attempt", allocations)
	}
}

func TestCymruPendingCompletionOwnership(t *testing.T) {
	for _, tc := range []struct {
		name        string
		oldFirst    bool
		newestShift time.Duration
	}{{"old_first", true, 0}, {"new_first_with_timestamp_reuse", false, 0}, {"new_first_with_backwards_clock", false, -time.Hour}} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCymruResolver(true, time.Second, time.Minute, time.Second, 32, 3)
			addr := cymruTestAddr(1)
			key := addr.String()
			now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
			c.enqueue(addr, key, now)
			old := startCymruTestWorker(t, c, LookupResult{}, false)
			old.await(t)
			c.enqueue(addr, key, now.Add(c.pendingTTL))
			newer := startCymruTestWorker(t, c, LookupResult{}, false)
			newer.await(t)
			c.pendingMu.Lock()
			newOwner := c.pending[key]
			c.pendingMu.Unlock()
			if tc.oldFirst {
				old.finish(t)
				c.pendingMu.Lock()
				got := c.pending[key]
				c.pendingMu.Unlock()
				if got != newOwner {
					t.Fatal("old completion removed a newer admitted owner")
				}
				// Bypass cache: a cache hit could hide erroneous pending deletion.
				c.enqueue(addr, key, now.Add(c.pendingTTL))
				if len(c.queue) != 0 {
					t.Fatal("newer active request lost duplicate suppression")
				}
				newer.finish(t)
				if got := cymruPendingCount(c); got != 0 {
					t.Fatalf("completed newest request retained %d pending identities", got)
				}
				return
			}
			newer.finish(t)
			if got := cymruPendingCount(c); got != 0 {
				t.Fatalf("newest completion retained %d pending identities", got)
			}
			// A supplied wall clock can return to exactly the old timestamp.
			// The oldest still-live request must not acquire this new authority.
			c.enqueue(addr, key, now.Add(tc.newestShift))
			current := startCymruTestWorker(t, c, LookupResult{}, false)
			current.await(t) // Queue is empty before the old worker retires.
			c.pendingMu.Lock()
			currentOwner := c.pending[key]
			c.pendingMu.Unlock()
			old.finish(t)
			c.pendingMu.Lock()
			got := c.pending[key]
			c.pendingMu.Unlock()
			if got != currentOwner || got == nil {
				t.Fatal("old completion removed a new owner after timestamp reuse")
			}
			current.finish(t)
			if got := cymruPendingCount(c); got != 0 {
				t.Fatalf("retired requests retained %d pending identities", got)
			}
		})
	}
}

func TestCymruPendingOwnershipBoundUnderChurn(t *testing.T) {
	c := newCymruResolver(true, time.Second, time.Minute, time.Minute, 32, 2)
	now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
	for round := 0; round < 3; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		release := make(chan struct{})
		var releaseOnce sync.Once
		entered := make(chan netip.Addr, cap(c.queue)+c.workers)
		var done []chan struct{}
		for i := 0; i < c.workers; i++ {
			finished := make(chan struct{})
			done = append(done, finished)
			go func() {
				defer close(finished)
				c.runWorker(ctx, func(_ context.Context, addr netip.Addr) (LookupResult, bool) {
					entered <- addr
					<-release
					return LookupResult{}, false
				})
			}()
		}
		finish := func() {
			cancel()
			releaseOnce.Do(func() { close(release) })
			for _, finished := range done {
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("Cymru churn worker failed to retire")
				}
			}
		}
		t.Cleanup(finish)
		await := func() {
			t.Helper()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("Cymru churn request was not received")
			}
		}
		for i := 0; i < c.workers; i++ {
			addr := cymruTestAddr(round*4000 + i)
			c.enqueue(addr, addr.String(), now)
			await() // Occupy each worker before filling the queue.
		}
		for i := 0; i < cap(c.queue)+1000; i++ {
			addr := cymruTestAddr(round*4000 + c.workers + i)
			c.enqueue(addr, addr.String(), now)
		}
		if got, want := cymruPendingCount(c), cap(c.queue)+c.workers; got != want {
			t.Fatalf("round %d pending=%d; want queue plus live workers=%d", round, got, want)
		}
		releaseOnce.Do(func() { close(release) })
		for i := 0; i < cap(c.queue); i++ {
			await() // Every admitted request is now received before cancellation.
		}
		finish() // Join the real completion path before checking retirement.
		if len(c.queue) != 0 || cymruPendingCount(c) != 0 {
			t.Fatalf("round %d retained completed history: queued=%d pending=%d", round, len(c.queue), cymruPendingCount(c))
		}
	}
}

func TestCymruWorkerCancellation(t *testing.T) {
	for _, active := range []bool{false, true} {
		name := "idle"
		if active {
			name = "active"
		}
		t.Run(name, func(t *testing.T) {
			c := newCymruResolver(true, time.Second, time.Minute, time.Minute, 32, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			done := make(chan struct{})
			if active {
				addr := cymruTestAddr(1)
				c.enqueue(addr, addr.String(), time.Now().UTC())
			} else {
				cancel()
			}
			go func() {
				defer close(done)
				c.runWorker(ctx, func(ctx context.Context, _ netip.Addr) (LookupResult, bool) {
					close(entered)
					<-ctx.Done()
					return LookupResult{}, false
				})
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("canceled worker remained active during cleanup")
				}
			})
			if active {
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("worker never began lookup")
				}
				cancel()
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not honor cancellation after lookup returned")
			}
			if got := cymruPendingCount(c); got != 0 {
				t.Fatalf("completed active ownership retained %d identities", got)
			}
		})
	}
}

func TestCymruWorkerCache(t *testing.T) {
	for _, success := range []bool{true, false} {
		name := "negative"
		if success {
			name = "positive"
		}
		t.Run(name, func(t *testing.T) {
			c := newCymruResolver(true, time.Second, time.Minute, time.Minute, 32, 1)
			addr := cymruTestAddr(1)
			want := LookupResult{}
			if success {
				want = LookupResult{ASN: "64500", CountryCode: "US", Source: cymruSource}
			}
			c.enqueue(addr, addr.String(), time.Now().UTC())
			w := startCymruTestWorker(t, c, want, success)
			w.await(t)
			w.finish(t)
			got, hit, negative := c.lookup(addr, time.Now().UTC())
			if !hit || negative != !success || got.ASN != want.ASN || got.CountryCode != want.CountryCode || got.Source != want.Source || cymruPendingCount(c) != 0 {
				t.Fatalf("worker cache got=%+v hit=%v negative=%v pending=%d; want %+v success=%v", got, hit, negative, cymruPendingCount(c), want, success)
			}
		})
	}
}
