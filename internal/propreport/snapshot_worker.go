package propreport

import (
	"context"
	"errors"
	"io"
	"sync"
)

var errReportWorkerUnavailable = errors.New("propagation report snapshot worker unavailable")
var ErrReportCleanupUncertain = errors.New("propagation report snapshot cleanup uncertain")
var errReportCleanupUncertain = ErrReportCleanupUncertain

// reportSnapshot is owned by exactly one report worker through Close. Opening
// must be synchronous and must not return until any failed acquisition has
// been cleaned up. Close failure means cleanup cannot be positively proven.
type reportSnapshot interface {
	Reader() io.Reader
	Close() error
}

type reportSnapshotResult struct {
	entries []string
	err     error
}

type reportWorkerGate struct {
	mu       sync.Mutex
	occupied bool
	poisoned bool
	// Preserve ownership if Close cannot confirm that the lease was released.
	uncertain     reportSnapshot
	uncertainOpen error
}

var reportSnapshots reportWorkerGate

// scan reserves before starting any potentially blocking operation. A caller
// may stop waiting when its context expires, but its one strongly retained
// worker owns the reservation until the scan and cleanup actually complete.
func (g *reportWorkerGate) scan(ctx context.Context, open func() (reportSnapshot, error)) (reportSnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return reportSnapshotResult{}, err
	}
	g.mu.Lock()
	if g.occupied || g.poisoned {
		g.mu.Unlock()
		return reportSnapshotResult{}, errReportWorkerUnavailable
	}
	g.occupied = true
	g.mu.Unlock()
	result := make(chan reportSnapshotResult, 1)
	go func() {
		var r reportSnapshotResult
		snapshot, err := open()
		if err != nil {
			r.err = err
			if errors.Is(err, errReportCleanupUncertain) {
				g.mu.Lock()
				g.poisoned = true
				g.uncertainOpen = err
				g.mu.Unlock()
			}
		} else if snapshot == nil {
			r.err = errReportCleanupUncertain
			g.mu.Lock()
			g.poisoned = true
			g.mu.Unlock()
		} else {
			r.entries, r.err = parseLogReader(snapshot.Reader())
			if closeErr := snapshot.Close(); closeErr != nil {
				r.err = errors.Join(r.err, closeErr)
				g.mu.Lock()
				g.poisoned = true
				g.uncertain = snapshot
				g.mu.Unlock()
			}
		}
		g.mu.Lock()
		g.occupied = false
		g.mu.Unlock()
		result <- r
	}()
	select {
	case r := <-result:
		return r, r.err
	case <-ctx.Done():
		return reportSnapshotResult{}, ctx.Err()
	}
}
