package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"dxcluster/dedup"
	"dxcluster/spot"
)

// loadharness generates synthetic spots and pushes them through the deduplicator
// to provide a repeatable load for profiling/benchmarking. It is intentionally
// minimal and does not require external feeds or telnet clients.
func main() {
	var (
		ratePerSec     = flag.Int("rate", 200, "spots per second to generate")
		runFor         = flag.Duration("duration", 1*time.Minute, "how long to run the load")
		window         = flag.Duration("window", 120*time.Second, "deduplication window")
		outputBuffer   = flag.Int("output-buffer", 40960, "dedup output buffer size")
		preferStronger = flag.Bool("prefer-stronger", true, "dedup prefers stronger SNR on collisions")
		baseFreq       = flag.Float64("base-freq-khz", 14000.0, "base frequency in kHz for synthetic spots")
	)
	flag.Parse()

	if *ratePerSec <= 0 {
		log.Fatalf("rate must be >0 (got %d)", *ratePerSec)
	}
	if *runFor <= 0 {
		log.Fatalf("duration must be >0 (got %s)", runFor.String())
	}

	log.Printf("loadharness: starting with rate=%d/s duration=%s window=%s output-buffer=%d prefer-stronger=%t",
		*ratePerSec, runFor.String(), window.String(), *outputBuffer, *preferStronger)

	deduper := dedup.NewDeduplicator(*window, *preferStronger, *outputBuffer)
	deduper.Start()
	defer deduper.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), *runFor)
	defer cancel()

	var sent uint64
	var inDropped uint64
	var forwarded uint64

	input := deduper.GetInputChannel()
	output := deduper.GetOutputChannel()

	go generateSpots(ctx, input, *ratePerSec, *baseFreq, &sent, &inDropped)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-output:
				atomic.AddUint64(&forwarded, 1)
			}
		}
	}()

	<-ctx.Done()
	// Give goroutines a moment to drain after cancellation.
	time.Sleep(100 * time.Millisecond)

	processed, duplicates, cacheSize := deduper.GetStats()
	elapsed := runFor.Seconds()
	rate := float64(forwarded) / elapsed

	log.Println("loadharness: complete")
	log.Printf("sent=%d forwarded=%d input_dropped=%d dedup_processed=%d dedup_duplicates=%d cache_size=%d",
		sent, forwarded, inDropped, processed, duplicates, cacheSize)
	log.Printf("throughput=%.1f spots/sec over %s", rate, runFor.String())
}

func generateSpots(ctx context.Context, input chan<- *spot.Spot, ratePerSec int, baseFreq float64, sent *uint64, dropped *uint64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	modes := []string{"FT8", "FT4", "CW", "RTTY", "SSB"}
	var seq uint64
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := 0; i < ratePerSec; i++ {
				s := makeSpot(seq, baseFreq, modes, rng)
				seq++
				select {
				case input <- s:
					atomic.AddUint64(sent, 1)
				default:
					atomic.AddUint64(dropped, 1)
				}
			}
		}
	}
}

func makeSpot(seq uint64, baseFreq float64, modes []string, rng *rand.Rand) *spot.Spot {
	mode := modes[seq%uint64(len(modes))]
	dx := fmt.Sprintf("DX%05d", seq%100000)
	de := fmt.Sprintf("DE%05d", (seq*7)%50000)
	freq := baseFreq + float64(seq%1000)*0.1 + rng.Float64()*0.01

	s := spot.NewSpot(dx, de, freq, mode)
	s.Report = int(seq%30) - 10
	s.SourceType = spot.SourceManual
	return s
}
