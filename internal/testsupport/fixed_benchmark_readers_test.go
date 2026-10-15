package testsupport

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func fixedBenchmarkReaderFault(ctx context.Context, b *testing.B, cancel context.CancelFunc, clock *Clock, scenario string) bool {
	const owners = 4
	ready, retired := make(chan struct{}, owners), make(chan struct{}, owners)
	workerError := make(chan error, 1)
	var workers sync.WaitGroup
	var reads [owners]int
	var backwards [owners]bool
	workers.Add(owners)
	for owner := range owners {
		go func(owner int) {
			defer workers.Done()
			defer func() { retired <- struct{}{} }()
			previous := clock.NowNS()
			reads[owner]++
			ready <- struct{}{}
			if owner == 0 && scenario == "worker_error" {
				workerError <- fmt.Errorf("injected worker error after a live clock read")
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				now := clock.NowNS()
				backwards[owner] = backwards[owner] || now < previous
				previous = now
				reads[owner]++
			}
		}(owner)
	}
	defer func() {
		cancel() // Cancellation must precede the join, including Goexit/panic.
		workers.Wait()
		for owner := range owners {
			if reads[owner] == 0 || backwards[owner] {
				b.Errorf("reader %d reads=%d backwards=%t", owner, reads[owner], backwards[owner])
			}
		}
		if ctx.Err() == nil || len(retired) != owners {
			b.Error("reader retirement requires canceled context and all completed owners")
			return
		}
		fmt.Printf("fixed-benchmark-readers-retired:%s canceled=true readers=%d\n", scenario, len(retired))
	}()
	b.Cleanup(func() { fmt.Println("fixed-benchmark-domain-cleanup:" + scenario) })
	for range owners {
		select {
		case <-ready:
		case <-ctx.Done():
			b.Fatal("context expired before all readers were active")
		}
	}
	switch scenario {
	case "fatal":
		b.Fatal("injected active-reader Fatal")
	case "skip":
		b.Skip("injected active-reader Skip")
	case "worker_error":
		select {
		case err := <-workerError:
			b.Error(err)
		case <-ctx.Done():
			b.Fatal("worker error did not arrive before deadline")
		}
	case "cleanup_failure":
		b.Cleanup(func() { b.Error("injected late cleanup failure") })
	case "panic":
		panic("injected active-reader panic")
	case "incomplete":
		return false
	case "success":
	default:
		b.Fatalf("unexpected active-reader fault scenario %q", scenario)
	}
	return true
}
