package peer

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestTimestampHundredValuesThenRateRefusal(t *testing.T) {
	g := NewTimestampGenerator()
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	previous := -1.0
	for i := 0; i < 100; i++ {
		stamp, err := g.NextAt(now)
		if err != nil {
			t.Fatal(err)
		}
		value, err := strconv.ParseFloat(stamp, 64)
		if err != nil || value <= previous || value >= 1 {
			t.Fatalf("non-increasing or future second at %d: %s", i, stamp)
		}
		if i == 0 && stamp != "0" {
			t.Fatalf("cold midnight starts with %s", stamp)
		}
		if i == 99 && stamp != "0.99" {
			t.Fatalf("last stamp = %s", stamp)
		}
		previous = value
	}
	if stamp, err := g.NextAt(now); stamp != "" || !errors.Is(err, ErrTimestampRate) {
		t.Fatalf("overflow: %q %v", stamp, err)
	}
	if err := g.ClockSafe(now); err != nil {
		t.Fatalf("rate is not clock failure: %v", err)
	}
	if stamp, err := g.NextAt(now.Add(time.Second)); err != nil || stamp != "1" {
		t.Fatalf("resume %s %v", stamp, err)
	}
}

func TestTimestampClockRegressionAndMidnight(t *testing.T) {
	g := NewTimestampGenerator()
	now := time.Date(2026, 10, 1, 23, 59, 59, 0, time.UTC)
	if stamp, err := g.NextAt(now); err != nil || stamp != "86399" {
		t.Fatalf("%s %v", stamp, err)
	}
	if stamp, err := g.NextAt(now.Add(time.Second)); err != nil || stamp != "0" {
		t.Fatalf("midnight %s %v", stamp, err)
	}
	if stamp, err := g.NextAt(now); stamp != "" || !errors.Is(err, ErrTimestampClock) {
		t.Fatalf("regression %s %v", stamp, err)
	}
	if err := g.ClockSafe(now.Add(time.Second)); !errors.Is(err, ErrTimestampClock) {
		t.Fatalf("old second prematurely clears clock gate: %v", err)
	}
	if err := g.ClockSafe(now.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if stamp, err := g.NextAt(now.Add(2 * time.Second)); err != nil || stamp != "1" {
		t.Fatalf("probe consumed sequence: %s %v", stamp, err)
	}
}

func TestTimestampConcurrentOriginSequence(t *testing.T) {
	g := NewTimestampGenerator()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	results := make(chan string, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stamp, err := g.NextAt(now)
			if err != nil {
				t.Error(err)
				return
			}
			results <- stamp
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[string]bool)
	for stamp := range results {
		if seen[stamp] {
			t.Fatalf("duplicate %s", stamp)
		}
		seen[stamp] = true
	}
	if len(seen) != 100 {
		t.Fatalf("issued %d", len(seen))
	}
}
