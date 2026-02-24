package spot

import (
	"errors"
	"time"
)

// SignalResolverDriver provides deterministic, event-time control over a SignalResolver
// without starting its background goroutine.
//
// Concurrency contract:
//   - A SignalResolverDriver must be owned by a single goroutine.
//   - The underlying SignalResolver must not be started via (*SignalResolver).Start while
//     a driver exists (they would compete for the same input channel).
//   - Enqueue may be called concurrently with Drain/Step (channel operations are safe),
//     but Drain/Step must not be called concurrently.
//
// Primary use case: offline/historical replay where wall-clock tickers would
// introduce nondeterminism.
type SignalResolverDriver struct {
	resolver *SignalResolver
	states   map[ResolverSignalKey]*resolverKeyState

	lastSweepAt time.Time
}

// NewSignalResolverDriver constructs a deterministic driver for resolver.
// It returns an error when resolver is nil or already started.
func NewSignalResolverDriver(resolver *SignalResolver) (*SignalResolverDriver, error) {
	if resolver == nil {
		return nil, errors.New("signal resolver driver: resolver is nil")
	}
	if resolver.started.Load() {
		return nil, errors.New("signal resolver driver: resolver already started")
	}
	if resolver.input == nil {
		return nil, errors.New("signal resolver driver: resolver input channel is nil")
	}
	sizeHint := resolver.cfg.MaxActiveKeys / 2
	if sizeHint < 16 {
		sizeHint = 16
	}
	return &SignalResolverDriver{
		resolver: resolver,
		states:   make(map[ResolverSignalKey]*resolverKeyState, sizeHint),
	}, nil
}

// Drain consumes all currently queued evidence and applies it to resolver state.
// It returns the number of evidence items processed.
func (d *SignalResolverDriver) Drain() int {
	if d == nil || d.resolver == nil {
		return 0
	}
	processed := 0
	for {
		select {
		case ev := <-d.resolver.input:
			d.resolver.processed.Add(1)
			d.resolver.applyEvidence(d.states, ev)
			processed++
		default:
			d.resolver.activeKeys.Store(int64(len(d.states)))
			return processed
		}
	}
}

// Sweep prunes and evaluates resolver state as of now.
func (d *SignalResolverDriver) Sweep(now time.Time) {
	if d == nil || d.resolver == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	d.resolver.sweep(d.states, now)
	d.lastSweepAt = now
}

// Step drains the input queue and performs a sweep when now advanced by at least
// the resolver's configured sweep interval (or when no prior sweep was performed).
// It returns the number of evidence items processed during the drain phase.
func (d *SignalResolverDriver) Step(now time.Time) int {
	if d == nil || d.resolver == nil {
		return 0
	}
	processed := d.Drain()

	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	if d.lastSweepAt.IsZero() || now.Sub(d.lastSweepAt) >= d.resolver.cfg.SweepInterval {
		d.resolver.sweep(d.states, now)
		d.lastSweepAt = now
	}

	return processed
}
