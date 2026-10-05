package telnet

import (
	"sync"
	"testing"
	"time"
)

type readbackPauseTestState struct {
	until, cutoff int64
	count         uint64
	epoch         uint64
	pending       bool
	closed        bool
}

func assertReadbackPauseState(t *testing.T, c *Client, want readbackPauseTestState) {
	t.Helper()
	c.readPauseMu.Lock()
	got := readbackPauseTestState{
		until: c.readPauseUntilUnixNano.Load(), cutoff: c.readPauseDiscardBefore.Load(),
		count: c.readPauseSuppressed.Load(), epoch: c.readPauseEpoch,
		pending: c.readPausePending.Load(), closed: c.readPauseClosed,
	}
	c.readPauseMu.Unlock()
	if got != want {
		t.Fatalf("pause state = %+v, want %+v", got, want)
	}
}

func TestHumanReadbackPendingAndFullReadingInterval(t *testing.T) {
	c := &Client{}
	completion := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second)
	if completion.epoch != 1 || completion.duration != 30*time.Second {
		t.Fatalf("completion = %+v", completion)
	}
	assertReadbackPauseState(t, c, readbackPauseTestState{epoch: 1, pending: true})
	if active, remaining, count := c.readPauseStatus(time.Unix(1700000120, 0)); !active || remaining != 0 || count != 0 {
		t.Fatalf("pending status = %t, %s, %d", active, remaining, count)
	}
	if !c.suppressSpotForReadPause(&spotEnvelope{enqueueAt: time.Unix(1700000120, 0)}, time.Unix(1700000120, 0)) {
		t.Fatal("pending delivery did not suppress a spot after the nominal reading interval")
	}
	c.completeHumanReadback(completion, time.Unix(1700000120, 0))
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000150000000000, cutoff: 1700000150000000000, count: 1, epoch: 1,
	})
	if active, remaining, count := c.readPauseStatus(time.Unix(1700000120, 0)); !active || remaining != 30*time.Second || count != 1 {
		t.Fatalf("delivered status = %t, %s, %d", active, remaining, count)
	}
	if active, remaining, _ := c.readPauseStatus(time.Unix(1700000149, 999999999)); !active || remaining != time.Nanosecond {
		t.Fatalf("before deadline status = %t, %s", active, remaining)
	}
	if active, remaining, count := c.readPauseStatus(time.Unix(1700000150, 0)); active || remaining != 0 || count != 1 {
		t.Fatalf("at deadline status = %t, %s, %d", active, remaining, count)
	}
	now := time.Unix(1700000151, 0)
	if c.suppressSpotForReadPause(&spotEnvelope{enqueueAt: now}, now) {
		t.Fatal("finite cutoff suppressed fresh traffic after expiry")
	}
	if !c.suppressSpotForReadPause(&spotEnvelope{enqueueAt: time.Unix(1700000149, 0)}, now) {
		t.Fatal("finite cutoff did not discard stale queued traffic")
	}
}

func TestHumanReadbackDefaultDuration(t *testing.T) {
	for _, duration := range []time.Duration{0, -time.Second} {
		c := &Client{}
		completion := c.beginHumanReadback(time.Unix(1700000000, 0), duration)
		if completion.duration != 30*time.Second {
			t.Fatalf("duration %s produced %+v", duration, completion)
		}
		c.completeHumanReadback(completion, time.Unix(1700000005, 0))
		assertReadbackPauseState(t, c, readbackPauseTestState{
			until: 1700000035000000000, cutoff: 1700000035000000000, epoch: 1,
		})
	}
}

func TestHumanReadbackPreservesLongerFinitePause(t *testing.T) {
	c := &Client{}
	c.readPauseUntilUnixNano.Store(1700000300000000000)
	c.readPauseDiscardBefore.Store(1700000300000000000)
	c.readPauseSuppressed.Store(3)
	completion := c.beginHumanReadback(time.Unix(1700000010, 0), 30*time.Second)
	if !c.suppressSpotForReadPause(nil, time.Unix(1700000020, 0)) {
		t.Fatal("pending hold did not suppress a spot")
	}
	c.completeHumanReadback(completion, time.Unix(1700000020, 0))
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000300000000000, cutoff: 1700000300000000000, count: 4, epoch: 1,
	})
}

func TestPendingPauseCarriesSuppressionCount(t *testing.T) {
	c := &Client{}
	c.readPauseUntilUnixNano.Store(1700000005000000000)
	c.readPauseDiscardBefore.Store(1700000005000000000)
	c.readPauseSuppressed.Store(7)
	completion := c.beginHumanReadback(time.Unix(1700000003, 0), 30*time.Second)
	if !c.suppressSpotForReadPause(nil, time.Unix(1700000008, 0)) {
		t.Fatal("pending hold expired with its older finite deadline")
	}
	c.completeHumanReadback(completion, time.Unix(1700000010, 0))
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000040000000000, cutoff: 1700000040000000000, count: 8, epoch: 1,
	})
}

func TestPendingReadbackManualControlPrecedence(t *testing.T) {
	for _, tc := range []struct {
		line     string
		response string
		want     readbackPauseTestState
	}{
		{
			line:     "PAUSE 5",
			response: "Live spots paused for 5s. Type RESUME to resume now.\nMissed spots are not replayed.\n",
			want:     readbackPauseTestState{until: 1700000015000000000, cutoff: 1700000015000000000, count: 3, epoch: 2},
		},
		{
			line: "RESUME", response: "Live spots resumed. Suppressed spots: 3.\n",
			want: readbackPauseTestState{cutoff: 1700000010000000000, epoch: 2},
		},
		{
			line: "PAUSE 0", response: "Usage: PAUSE [seconds 1-300] (default 30)\n",
			want: readbackPauseTestState{until: 1700000050000000000, cutoff: 1700000050000000000, count: 3, epoch: 1},
		},
	} {
		t.Run(tc.line, func(t *testing.T) {
			c := &Client{}
			completion := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second)
			c.readPauseSuppressed.Store(3)
			s := &Server{nowFn: func() time.Time { return time.Unix(1700000010, 0) }}
			response, handled := s.handleReadPauseCommand(c, tc.line)
			if !handled || response != tc.response {
				t.Fatalf("response = %q, handled=%t", response, handled)
			}
			c.completeHumanReadback(completion, time.Unix(1700000020, 0))
			assertReadbackPauseState(t, c, tc.want)
		})
	}
}

func TestLatestHumanReadbackCompletionOnly(t *testing.T) {
	c := &Client{}
	first := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second)
	c.readPauseSuppressed.Store(2)
	latest := c.beginHumanReadback(time.Unix(1700000010, 0), 45*time.Second)
	c.completeHumanReadback(first, time.Unix(1700000020, 0))
	assertReadbackPauseState(t, c, readbackPauseTestState{count: 2, epoch: 2, pending: true})
	c.completeHumanReadback(latest, time.Unix(1700000040, 0))
	want := readbackPauseTestState{until: 1700000085000000000, cutoff: 1700000085000000000, count: 2, epoch: 2}
	assertReadbackPauseState(t, c, want)
	c.completeHumanReadback(latest, time.Unix(1700000060, 0))
	assertReadbackPauseState(t, c, want)
}

func TestGenericPausePreservesPendingAuthority(t *testing.T) {
	c := &Client{}
	completion := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second)
	c.readPauseSuppressed.Store(2)
	if duration := c.extendReadPause(time.Unix(1700000010, 0), 90*time.Second); duration != 90*time.Second {
		t.Fatalf("extended duration = %s", duration)
	}
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000100000000000, cutoff: 1700000100000000000, count: 2, epoch: 1, pending: true,
	})
	c.completeHumanReadback(completion, time.Unix(1700000020, 0))
	want := readbackPauseTestState{until: 1700000100000000000, cutoff: 1700000100000000000, count: 2, epoch: 1}
	assertReadbackPauseState(t, c, want)
	if duration := c.extendReadPause(time.Unix(1700000030, 0), 5*time.Second); duration != 70*time.Second {
		t.Fatalf("preserved duration = %s", duration)
	}
	assertReadbackPauseState(t, c, want)
	if duration := c.extendReadPause(time.Unix(1700000030, 0), 0); duration != 0 {
		t.Fatalf("zero duration = %s", duration)
	}
	assertReadbackPauseState(t, c, want)
}

func TestReadbackInvalidationPreservesState(t *testing.T) {
	c := &Client{}
	c.readPauseUntilUnixNano.Store(1700000180000000000)
	c.readPauseDiscardBefore.Store(1700000180000000000)
	c.readPauseSuppressed.Store(7)
	completion := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second)
	c.invalidateHumanReadback()
	want := readbackPauseTestState{until: 1700000180000000000, cutoff: 1700000180000000000, count: 7, epoch: 2, closed: true}
	assertReadbackPauseState(t, c, want)
	c.completeHumanReadback(completion, time.Unix(1700000200, 0))
	c.invalidateHumanReadback()
	c.startReadPause(time.Unix(1700000200, 0), 30*time.Second)
	if got := c.beginHumanReadback(time.Unix(1700000200, 0), 30*time.Second); got != (readbackCompletion{}) {
		t.Fatalf("closed client accepted completion %+v", got)
	}
	if got := c.extendReadPause(time.Unix(1700000200, 0), 30*time.Second); got != 0 {
		t.Fatalf("closed client extended pause by %s", got)
	}
	if active, count := c.resumeReadPause(time.Unix(1700000200, 0)); active || count != 7 {
		t.Fatalf("closed resume = %t, %d", active, count)
	}
	assertReadbackPauseState(t, c, want)
}

func TestConcurrentAutomaticPauseAndHumanCompletion(t *testing.T) {
	c := &Client{}
	completion := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second)
	c.readPauseSuppressed.Store(4)
	// Both goroutines are ready to contend for the shared authority before it
	// is released. Either acquisition order must retain the later deadline and
	// the count.
	c.readPauseMu.Lock()
	ready := make(chan struct{}, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		ready <- struct{}{}
		c.extendReadPause(time.Unix(1700000010, 0), 90*time.Second)
	}()
	go func() {
		defer workers.Done()
		ready <- struct{}{}
		c.completeHumanReadback(completion, time.Unix(1700000020, 0))
	}()
	<-ready
	<-ready
	c.readPauseMu.Unlock()
	workers.Wait()
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000100000000000, cutoff: 1700000100000000000, count: 4, epoch: 1,
	})
}

func TestConcurrentPendingSuppressionCarriesIntoReading(t *testing.T) {
	c := &Client{}
	c.readPauseUntilUnixNano.Store(1700000002000000000)
	c.readPauseDiscardBefore.Store(1700000002000000000)
	c.readPauseSuppressed.Store(7)
	completion := c.beginHumanReadback(time.Unix(1700000001, 0), 30*time.Second)
	const workerCount, perPhase = 4, 125
	ready := make(chan struct{}, workerCount)
	reading := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for range perPhase {
				if !c.suppressSpotForReadPause(nil, time.Unix(1700000005, 0)) {
					t.Error("pending hold did not suppress a spot")
				}
			}
			ready <- struct{}{}
			<-reading
			for range perPhase {
				if !c.suppressSpotForReadPause(nil, time.Unix(1700000011, 0)) {
					t.Error("reading interval did not suppress a spot")
				}
			}
		}()
	}
	for range workerCount {
		<-ready
	}
	c.completeHumanReadback(completion, time.Unix(1700000010, 0))
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000040000000000, cutoff: 1700000040000000000, count: 507, epoch: 1,
	})
	close(reading)
	workers.Wait()
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000040000000000, cutoff: 1700000040000000000, count: 1007, epoch: 1,
	})
}

func TestConcurrentLaterManualPauseWins(t *testing.T) {
	c := &Client{}
	completion := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second)
	processed := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		c.startReadPause(time.Unix(1700000010, 0), 5*time.Second)
		close(processed)
	}()
	go func() {
		defer workers.Done()
		<-processed
		c.completeHumanReadback(completion, time.Unix(1700000020, 0))
	}()
	workers.Wait()
	assertReadbackPauseState(t, c, readbackPauseTestState{
		until: 1700000015000000000, cutoff: 1700000015000000000, epoch: 2,
	})
}

func TestReadbackPauseNilClient(t *testing.T) {
	var c *Client
	if got := c.beginHumanReadback(time.Unix(1700000000, 0), 30*time.Second); got != (readbackCompletion{}) {
		t.Fatalf("nil begin = %+v", got)
	}
	c.completeHumanReadback(readbackCompletion{epoch: 1, duration: 30 * time.Second}, time.Unix(1700000000, 0))
	c.invalidateHumanReadback()
	c.startReadPause(time.Unix(1700000000, 0), 30*time.Second)
	if got := c.extendReadPause(time.Unix(1700000000, 0), 30*time.Second); got != 0 {
		t.Fatalf("nil extend = %s", got)
	}
	if active, remaining, count := c.readPauseStatus(time.Unix(1700000000, 0)); active || remaining != 0 || count != 0 {
		t.Fatalf("nil status = %t, %s, %d", active, remaining, count)
	}
	if active, count := c.resumeReadPause(time.Unix(1700000000, 0)); active || count != 0 {
		t.Fatalf("nil resume = %t, %d", active, count)
	}
	if c.suppressSpotForReadPause(nil, time.Unix(1700000000, 0)) {
		t.Fatal("nil client suppressed a spot")
	}
}

func BenchmarkReadbackPauseSpotSuppression(b *testing.B) {
	for _, tc := range []struct {
		name           string
		pending        bool
		until, cutoff  int64
		enqueueSeconds int64
		want           bool
	}{
		{name: "pending", pending: true, enqueueSeconds: 1700000010, want: true},
		{name: "finite", until: 1700000030000000000, enqueueSeconds: 1700000010, want: true},
		{name: "stale", cutoff: 1700000005000000000, enqueueSeconds: 1700000004, want: true},
		{name: "fresh", cutoff: 1700000005000000000, enqueueSeconds: 1700000010},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := &Client{}
			c.readPausePending.Store(tc.pending)
			c.readPauseUntilUnixNano.Store(tc.until)
			c.readPauseDiscardBefore.Store(tc.cutoff)
			env := &spotEnvelope{enqueueAt: time.Unix(tc.enqueueSeconds, 0)}
			now := time.Unix(1700000010, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := c.suppressSpotForReadPause(env, now); got != tc.want {
					b.Fatalf("suppressed = %t, want %t", got, tc.want)
				}
			}
			b.StopTimer()
			var wantCount uint64
			if tc.want {
				wantCount = uint64(b.N)
			}
			if got := c.readPauseSuppressed.Load(); got != wantCount {
				b.Fatalf("suppression count = %d, want %d", got, wantCount)
			}
		})
	}
}
