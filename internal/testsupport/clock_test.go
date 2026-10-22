package testsupport

import (
	"math"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestClockSharedDomain(t *testing.T) {
	first := NewClock(t)
	before := first.NowNS()
	// Creating a new instance later must not reset its origin. An instance-
	// relative timer would put the second reading before the first's later one.
	time.Sleep(2 * time.Millisecond)
	left := first.NowNS()
	second := NewClock(t)
	middle := second.NowNS()
	right := first.NowNS()
	if before < 0 || before > left || left > middle || middle > right {
		t.Fatalf("clock domains do not share ordered readings: %d <= %d <= %d <= %d", before, left, middle, right)
	}
	if first.Source() == "" || first.Source() != second.Source() {
		t.Fatalf("inconsistent source identity: %q versus %q", first.Source(), second.Source())
	}
	if first.FrequencyHz() <= 0 || first.FrequencyHz() != second.FrequencyHz() {
		t.Fatalf("inconsistent frequency: %d versus %d", first.FrequencyHz(), second.FrequencyHz())
	}
	if runtime.GOOS == "windows" && first.Source() != "windows.QueryPerformanceCounter" {
		t.Fatalf("Windows requires QPC; source=%q", first.Source())
	}
}

func TestClockShortWait(t *testing.T) {
	requireFixedBenchmarkClockChild(t, "clock_short_wait")
}

func TestClockShortWaitRejectsWrongUnits(t *testing.T) {
	for _, scenario := range []string{"clock_short_wait_small_units", "clock_short_wait_large_units"} {
		t.Run(scenario, func(t *testing.T) {
			output := runFixedBenchmarkChild(t, scenario, true)
			if !strings.Contains(output, "fixed-clock-interval-refused:") ||
				strings.Contains(output, "fixed-benchmark-valid-receipt:") {
				t.Fatalf("incorrect clock units escaped the interval check:\n%s", output)
			}
		})
	}
}

func TestClockSubmillisecondResolution(t *testing.T) {
	clock := NewClock(t)
	previous := clock.NowNS()
	minimumPositive := int64(math.MaxInt64)
	submillisecond := 0
	const samples = 100_000
	for range samples {
		now := clock.NowNS()
		delta := now - previous
		if delta < 0 {
			t.Fatalf("clock moved backward: %d then %d", previous, now)
		}
		if delta > 0 && delta < minimumPositive {
			minimumPositive = delta
		}
		if delta > 0 && delta < int64(time.Millisecond) {
			submillisecond++
		}
		previous = now
	}
	if submillisecond < 100 {
		t.Fatalf("only %d/%d nonzero submillisecond increments; minimum positive=%dns", submillisecond, samples, minimumPositive)
	}
	if runtime.GOOS == "windows" && minimumPositive > int64(10*time.Microsecond) {
		t.Fatalf("QPC minimum positive increment=%dns, want <=10000ns", minimumPositive)
	}
	t.Logf("source=%s frequency_hz=%d minimum_positive_ns=%d nonzero_submillisecond=%d/%d", clock.Source(), clock.FrequencyHz(), minimumPositive, submillisecond, samples)
}

func TestClockNowNSAllocations(t *testing.T) {
	clock := NewClock(t)
	previous := clock.NowNS()
	backward := false
	// Measure a whole batch once so integer averaging cannot hide events.
	allocations := testing.AllocsPerRun(1, func() {
		for range 10_000 {
			now := clock.NowNS()
			if now < previous {
				backward = true
			}
			previous = now
		}
	})
	if backward {
		t.Fatal("clock moved backward during allocation measurement")
	}
	if allocations != 0 {
		t.Fatalf("NowNS allocations=%g per warmed 10000-read batch, want zero", allocations)
	}
}

func TestClockAllocationBatchWitness(t *testing.T) {
	var sink [2]*[64]byte
	batches, operations := 0, 0
	allocations := testing.AllocsPerRun(1, func() {
		for operation := range 1000 {
			if operation == 500 {
				// One escaping allocation in each full batch must remain visible
				// even though it is only one allocation per thousand operations.
				sink[batches] = new([64]byte)
			}
			operations++
		}
		batches++
	})
	if allocations < 1 || batches != 2 || operations != 2000 {
		t.Fatalf("allocation witness lost event: allocations=%g batches=%d operations=%d", allocations, batches, operations)
	}
	// Passing the pointers through logging keeps both objects escaping and live.
	t.Logf("bounded allocation witness objects: %p %p", sink[0], sink[1])
}
