package testsupport

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFixedBenchmarkClockSharedDomain(t *testing.T) {
	requireFixedBenchmarkClockChild(t, "clock_domain")
}

func TestFixedBenchmarkClockConcurrentReads(t *testing.T) {
	requireFixedBenchmarkClockChild(t, "clock_concurrent")
}

func TestFixedBenchmarkClockQueuedOrdering(t *testing.T) {
	requireFixedBenchmarkClockChild(t, "clock_ordering")
}

func TestFixedBenchmarkClockNowNSAllocations(t *testing.T) {
	requireFixedBenchmarkClockChild(t, "clock_allocations")
}

func TestFixedBenchmarkClockResolution(t *testing.T) {
	requireFixedBenchmarkClockChild(t, "clock_resolution")
}

func requireFixedBenchmarkClockChild(t *testing.T, scenario string) {
	t.Helper()
	output := runFixedBenchmarkChild(t, scenario, false)
	if strings.Count(output, "fixed-benchmark-valid-receipt:"+scenario) != 1 {
		t.Fatalf("clock child did not complete exactly once:\n%s", output)
	}
}

func runFixedBenchmarkClockChild(t *testing.T, scenario string) {
	callbacks := 0
	RunFixedBenchmark(t, func(b *testing.B, ctx context.Context, cancel context.CancelFunc, clock *Clock) bool {
		callbacks++
		defer cancel()
		if clock.Source() != "testing.B.Elapsed(shared live benchmark)" || clock.FrequencyHz() <= 0 {
			b.Fatalf("wrong live benchmark clock identity: %q/%d", clock.Source(), clock.FrequencyHz())
		}
		if reference := NewClock(b); reference.FrequencyHz() != clock.FrequencyHz() {
			b.Fatalf("benchmark frequency=%d differs from platform frequency=%d", clock.FrequencyHz(), reference.FrequencyHz())
		}
		switch scenario {
		case "clock_domain":
			fixedBenchmarkClockDomain(b, clock)
		case "clock_concurrent":
			fixedBenchmarkClockConcurrent(ctx, b, cancel, clock)
		case "clock_ordering":
			fixedBenchmarkClockOrdering(ctx, b, cancel, clock)
		case "clock_allocations":
			fixedBenchmarkClockAllocations(b, clock)
		case "clock_resolution":
			fixedBenchmarkClockResolution(b, clock)
		default:
			b.Fatalf("unknown clock scenario %q", scenario)
		}
		return true
	})
	if callbacks != 1 {
		t.Fatalf("clock callbacks=%d, want one", callbacks)
	}
	fmt.Println("fixed-benchmark-valid-receipt:" + scenario)
}

func fixedBenchmarkClockDomain(b *testing.B, clock *Clock) {
	first := clock.NowNS()
	time.Sleep(2 * time.Millisecond)
	// Copies are distinct instances with the same immutable live benchmark
	// origin, rather than independent elapsed clocks with plausible offsets.
	second := *clock
	left, middle, right := clock.NowNS(), second.NowNS(), clock.NowNS()
	if first < 0 || first > left || left > middle || middle > right || right-first < int64(2*time.Millisecond) || right-first > int64(5*time.Second) {
		b.Fatalf("shared domain/wait invalid: %d <= %d <= %d <= %d", first, left, middle, right)
	}
}

func fixedBenchmarkClockConcurrent(ctx context.Context, b *testing.B, cancel context.CancelFunc, clock *Clock) {
	const owners, samples = 4, 16_384
	var clocks [owners]Clock
	var first, last [owners]int64
	var counts [owners]int
	var backward [owners]bool
	var workers sync.WaitGroup
	start := make(chan struct{})
	for owner := range clocks {
		clocks[owner] = *clock
	}
	workers.Add(owners)
	defer func() { cancel(); workers.Wait() }()
	before := clock.NowNS()
	for owner := range owners {
		go func(owner int) {
			defer workers.Done()
			select {
			case <-start:
			case <-ctx.Done():
				return
			}
			previous := clocks[owner].NowNS()
			first[owner] = previous
			for range samples {
				select {
				case <-ctx.Done():
					return
				default:
				}
				now := clocks[owner].NowNS()
				backward[owner] = backward[owner] || now < previous
				previous = now
				counts[owner]++
			}
			last[owner] = previous
		}(owner)
	}
	close(start)
	workers.Wait()
	after := clock.NowNS()
	for owner := range owners {
		if counts[owner] != samples || backward[owner] || before > first[owner] || first[owner] > last[owner] || last[owner] > after {
			b.Fatalf("owner %d count=%d backward=%t bounds=%d <= %d <= %d <= %d", owner, counts[owner], backward[owner], before, first[owner], last[owner], after)
		}
	}
}

func fixedBenchmarkClockOrdering(ctx context.Context, b *testing.B, cancel context.CancelFunc, clock *Clock) {
	const roles, samples = 4, 1024
	type reading struct {
		sequence int
		ns       int64
	}
	var queues [roles]chan reading
	var populations [roles][]int64
	var completed [roles]int
	var invalid [roles]bool
	var workers sync.WaitGroup
	for role := range roles {
		queues[role] = make(chan reading, 64)
		populations[role] = make([]int64, samples)
		for sample := range populations[role] {
			populations[role][sample] = -1
		}
	}
	workers.Add(roles * 2)
	defer func() { cancel(); workers.Wait() }()
	before := clock.NowNS()
	for role := range roles {
		go func(role int) {
			defer workers.Done()
			defer close(queues[role])
			for sample := range samples {
				item := reading{sequence: sample, ns: clock.NowNS()}
				select {
				case queues[role] <- item:
				case <-ctx.Done():
					return
				}
			}
		}(role)
		go func(role int) {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-queues[role]:
					if !ok {
						return
					}
					finish := clock.NowNS()
					if item.sequence != completed[role] || item.ns < before || finish < item.ns {
						invalid[role] = true
					}
					populations[role][completed[role]] = finish - item.ns
					completed[role]++
				}
			}
		}(role)
	}
	workers.Wait()
	for role := range roles {
		if invalid[role] || completed[role] != samples {
			b.Fatalf("role %d ordered completion=%d/%d invalid=%t", role, completed[role], samples, invalid[role])
		}
		for sample, elapsed := range populations[role] {
			if elapsed < 0 {
				b.Fatalf("role %d sample %d absent/negative: %d", role, sample, elapsed)
			}
		}
	}
}

func fixedBenchmarkClockAllocations(b *testing.B, clock *Clock) {
	previous, backward := clock.NowNS(), false
	beforeCalls := runtime.NumCgoCall()
	// One measured batch reports exact events, without integer-average loss.
	allocations := testing.AllocsPerRun(1, func() {
		for range 10_000 {
			now := clock.NowNS()
			backward = backward || now < previous
			previous = now
		}
	})
	afterCalls := runtime.NumCgoCall()
	if backward || allocations != 0 {
		b.Fatalf("live NowNS backwards=%t allocations=%g per warmed 10000-read batch, want false/zero", backward, allocations)
	}
	if runtime.GOOS == "windows" && afterCalls != beforeCalls {
		b.Fatalf("live NowNS introduced %d cgocalls, want zero", afterCalls-beforeCalls)
	}
}

func fixedBenchmarkClockResolution(b *testing.B, clock *Clock) {
	previous, minimum := clock.NowNS(), int64(math.MaxInt64)
	positive := 0
	for range 100_000 {
		now := clock.NowNS()
		delta := now - previous
		if delta < 0 {
			b.Fatal("live benchmark clock moved backward")
		}
		if delta > 0 && delta < minimum {
			minimum = delta
		}
		if delta > 0 && delta < int64(time.Millisecond) {
			positive++
		}
		previous = now
	}
	if positive < 100 || runtime.GOOS == "windows" && minimum > int64(10*time.Microsecond) {
		b.Fatalf("live benchmark resolution insufficient: positive sub-ms=%d minimum=%dns", positive, minimum)
	}
}
