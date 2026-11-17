//go:build windows && !qualification

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"
)

// The main-loop barrier is an authoritative local self-spot. The existing grid
// callback acknowledges its final fanout entry after all earlier raw inputs have
// left the main loop. It is not resolver evidence and is excluded from the raw
// CPU denominator. The ring/archive retain this one explicitly counted control.
func ccQualRecover(t *testing.T, opts ccQualOptions, f *ccQualRuntime, result *ccQualResult) {
	t.Helper()
	cpuStart, started := ccQualProcessCPU(t), time.Now()
	deadline := started.Add(opts.DrainTimeout)
	main := spot.NewSpot(ccQualMainCall, ccQualMainCall, 14060, "CW")
	main.SourceType, main.IsHuman, main.Confidence = spot.SourceManual, true, "V"
	main.DXMetadata.Grid, main.DEMetadata.Grid = "KP00AA", "KP00AA"
	main.Comment, main.SpotterIP = " ", "192.0.2.1"
	main.EnsureNormalized()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	mainAccepted := false
	select {
	case f.input <- main:
		mainAccepted = true
	case <-timer.C:
		ccQualError(t, result, "recovery could not enqueue the bounded main-loop barrier")
	}
	mainJoined := false
	if mainAccepted {
		select {
		case <-f.mainBarrier:
			mainJoined = true
		case <-timer.C:
			ccQualError(t, result, "main output consumer did not reach its final-fanout barrier before drain timeout")
		}
	}
	var beforeRetirement ccQualSnapshot
	quiet := false
	if mainJoined {
		beforeRetirement, quiet = ccQualWaitQuiet(f, deadline)
		if !quiet {
			ccQualError(t, result, "asynchronous owners did not naturally recover before drain timeout")
		}
	}
	// Retire only after the natural-drain observation. Explicit joins also detect
	// any in-flight release which Pending no longer represents. New final output
	// or disposition during retirement makes the natural-recovery claim fail.
	if f.pipeline != nil {
		f.pipeline.beginRetirement()
		if !ccQualWaitUntil(f.pipeline.done, deadline) {
			ccQualError(t, result, "output owner did not join before drain timeout")
		} else {
			result.OutputJoined = true
		}
	}
	if quiet && result.OutputJoined {
		after := f.snapshot()
		result.NaturalRecovery = !ccQualHasBacklog(after) && after.RingOutputs == beforeRetirement.RingOutputs &&
			after.Finalized == beforeRetirement.Finalized && after.Suppressed == beforeRetirement.Suppressed &&
			after.AgeDropped == beforeRetirement.AgeDropped
		if !result.NaturalRecovery {
			ccQualError(t, result, "retirement completed new output/dispositions: natural drain was not established")
		}
	}
	if result.OutputJoined {
		wire := spot.NewSpot(ccQualWireCall, "DL9ZZZ", 14060, "CW")
		wire.SourceType, wire.IsHuman, wire.Confidence = spot.SourceRBN, false, "V"
		wire.Comment = " "
		wire.EnsureNormalized()
		// Every registered session has one stable ordered worker shard. A DX
		// marker in the existing broadcast queue joins earlier accepted work at
		// the actual socket reader, including configured worker/client batching.
		if !f.server.TryBroadcastSpotOwned(wire.SealForAsync(), true, true, true) {
			ccQualError(t, result, "telnet FIFO drain barrier was refused")
		} else {
			result.TCPBarriersJoined = true
			for i, reader := range f.readers {
				if !ccQualWaitUntil(reader.barrier, deadline) {
					result.TCPBarriersJoined = false
					ccQualError(t, result, "TCP reader %d did not receive its FIFO drain barrier", i)
				}
			}
		}
	}
	result.ShutdownJoined = f.shutdown(t, opts.DrainTimeout)
	result.ResolverJoined = f.resolverJoined.Load()
	result.RecoveryNS, result.RecoveryCPUNS = time.Since(started).Nanoseconds(), ccQualProcessCPU(t)-cpuStart
	result.AfterRecovery = f.snapshot()
	if len(f.readers) == 2 {
		result.NormalTCP, result.DiagTCP = f.readers[0].count.Load(), f.readers[1].count.Load()
	}
	for i, reader := range f.readers {
		select {
		case err := <-reader.errors:
			ccQualError(t, result, "TCP reader %d failed: %v", i, err)
		default:
		}
	}
}

func ccQualWaitQuiet(f *ccQualRuntime, deadline time.Time) (ccQualSnapshot, bool) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	var previous ccQualSnapshot
	var quietSince time.Time
	for {
		current := f.snapshot()
		if !ccQualHasBacklog(current) && current == previous {
			if quietSince.IsZero() {
				quietSince = time.Now()
			}
			if time.Since(quietSince) >= 250*time.Millisecond {
				return current, true
			}
		} else {
			quietSince = time.Time{}
		}
		previous = current
		select {
		case <-timer.C:
			return current, false
		case <-ticker.C:
		}
	}
}

func ccQualWaitUntil(done <-chan struct{}, deadline time.Time) bool {
	if done == nil {
		return false
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// One cleanup owner exists even after partial startup. No downstream closes
// before output completion. The timeout reports failed ownership retirement;
// it never turns an unfinished stop into a successful qualification result.
func (f *ccQualRuntime) shutdown(t *testing.T, timeout time.Duration) bool {
	t.Helper()
	f.stopOnce.Do(func() {
		go func() {
			defer close(f.stopDone)
			if f.pipeline != nil {
				f.pipeline.stop()
			}
			if f.input != nil {
				f.inputOnce.Do(func() { close(f.input) })
			}
			if f.resolver != nil {
				f.resolver.Stop()
				f.resolverJoined.Store(true)
			}
			if f.refresher != nil {
				f.refresher.Stop()
			}
			if f.sampleDone != nil {
				f.sampleOnce.Do(func() { close(f.sampleStop) })
				<-f.sampleDone
			}
			for _, reader := range f.readers {
				reader.closing.Store(true)
				_ = reader.conn.Close()
				<-reader.done
			}
			if f.server != nil {
				f.server.Stop()
				deadline := time.Now().Add(min(timeout, 5*time.Second))
				for len(f.server.ListClientCallsigns()) != 0 {
					if time.Now().After(deadline) {
						f.stopErrors = append(f.stopErrors, fmt.Errorf("telnet clients did not retire before restoring fixture directory"))
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
			if f.writer != nil {
				f.writer.Stop() // Includes all queued and batched archive writes.
			}
			if f.custom != nil {
				if err := f.custom.Close(); err != nil {
					f.stopErrors = append(f.stopErrors, fmt.Errorf("Custom SCP close: %w", err))
				}
			}
			if f.legacy != nil {
				f.legacy.StopCleanup()
			}
		}()
	})
	if !ccQualWaitUntil(f.stopDone, time.Now().Add(timeout)) {
		t.Error("qualification cleanup timeout: owner retirement did not join")
		return false
	}
	for _, err := range f.stopErrors {
		t.Error(err)
	}
	return true
}

func ccQualCountArchive(t *testing.T, cfg config.ArchiveConfig, timeout time.Duration) uint64 {
	t.Helper()
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatalf("reopen qualification archive: %v", err)
	}
	defer w.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var count uint64
	var before []byte
	for pageNumber := 0; pageNumber <= int(ccQualMaxOffered/250)+1; pageNumber++ {
		page, err := w.ReadHistoryPage(archive.HistoryRequest{Limit: 250, Before: before, Done: ctx.Done()})
		if err != nil || page.Unreadable != 0 {
			t.Fatalf("qualification archive readback: unreadable=%d error=%v", page.Unreadable, err)
		}
		count += uint64(len(page.Spots))
		if count > ccQualMaxOffered+1 {
			t.Fatal("qualification archive exceeded the fixed input bound")
		}
		if page.End == archive.HistoryExhausted {
			return count
		}
		if len(page.Before) == 0 {
			t.Fatal("qualification archive continuation made no progress")
		}
		before = page.Before
	}
	t.Fatal("qualification archive readback exceeded the bounded page count")
	return 0
}
