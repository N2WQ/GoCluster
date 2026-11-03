package cluster

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/internal/asynclog"
	"dxcluster/internal/peerdiag"
)

// v6FileEmitter holds only generation handles. Each destination's permanent
// worker owns its sink and keeps its slot until a blocked operation returns.
type v6FileEmitter struct {
	handles [asynclog.MaxStreams]asynclog.Handle
}

func (e *v6FileEmitter) Emit(stream uint8, at time.Time, payload []byte) bool {
	if e == nil || int(stream) >= len(e.handles) || len(payload) > asynclog.MaxRecordBytes {
		return false
	}
	return e.handles[stream].Emit(at, 0, payload)
}

func (e *v6FileEmitter) Status(stream uint8) peerdiag.StreamStatus {
	if e == nil || int(stream) >= len(e.handles) {
		return peerdiag.StreamStatus{Degraded: true}
	}
	s := e.handles[stream].Stats()
	result := peerdiag.StreamStatus{Written: s.Written, Dropped: s.Dropped, Suppressed: s.Suppressed, Queued: s.Queued,
		Closed: s.Closed, CleanupFailed: s.CleanupFailed, Degraded: s.Generation == 0 || s.Degraded,
		Generation: s.Generation}
	// A fixed queue size is not a complete accounting of the file sink and
	// path workspaces. Leave ChargedBytes unavailable until that bound is
	// established; the historical PC92 qualification then fails closed.
	return result
}

func (e *v6FileEmitter) stop() {
	if e != nil {
		for _, h := range e.handles {
			h.Stop()
		}
	}
}

// v6Admission is the same fixed-queue admission used by files and the console.
type v6Admission struct {
	handle asynclog.Handle
}

func (a v6Admission) emit(at time.Time, p []byte) { a.handle.Emit(at, 0, p) }
func (a v6Admission) drop()                       {}

type v6ConsoleSink struct {
	writer io.Writer
	buffer [21 * (asynclog.MaxRecordBytes + 1)]byte
}

func (s *v6ConsoleSink) Write(at time.Time, _ uint8, payload []byte) asynclog.WriteResult {
	if len(payload) > asynclog.MaxRecordBytes || s.writer == nil {
		return asynclog.NotWritten
	}
	out := s.buffer[:0]
	for {
		i := bytes.IndexByte(payload, '\n')
		line := payload
		if i >= 0 {
			line = payload[:i]
		}
		out = at.UTC().AppendFormat(out, logTimestampLayout)
		out = append(out, ' ')
		out = append(out, line...)
		out = append(out, '\n')
		if i < 0 {
			break
		}
		payload = payload[i+1:]
	}
	n, err := s.writer.Write(out)
	if err != nil || n != len(out) {
		return asynclog.NotWritten
	}
	return asynclog.Written
}
func (*v6ConsoleSink) Close() error { return nil }

type v6LogRuntime struct {
	fanout                  systemLogFanout
	files                   v6FileEmitter
	early                   asynclog.Handle
	stdout                  asynclog.Handle
	conflicts               chan [2]uint8
	stop                    chan struct{}
	starts                  [asynclog.MaxStreams]*logStartGuard
	openLogRoot             func(string) (*os.Root, error)
	rootedRotation          func(time.Time, string, string, *os.Root)
	registerPropagationRoot func(*os.Root)
	closeOnce               sync.Once
}

// Conflict reports can arrive from a retired worker after a new runtime has
// started. Route them to the current runtime without retaining that runtime.
var activeLogConflicts struct {
	sync.Mutex
	target     chan [2]uint8
	batch      *atomic.Bool
	conflicted bool
}

func newV6LogRuntime(early io.Writer) (*v6LogRuntime, error) {
	l := &v6LogRuntime{conflicts: make(chan [2]uint8, 1), stop: make(chan struct{})}
	activeLogConflicts.Lock()
	if !sharedProcessLogRouter.install(&l.fanout) {
		activeLogConflicts.Unlock()
		return nil, errors.New("another runtime still owns the process log router")
	}
	activeLogConflicts.target = l.conflicts
	activeLogConflicts.batch = nil
	activeLogConflicts.conflicted = false
	activeLogConflicts.Unlock()
	if owner, err := asynclog.StartSingle(streamSystemEarly, func() asynclog.Sink {
		return &v6ConsoleSink{writer: early}
	}); err == nil {
		l.early = owner
	}
	l.fanout.setConsole(v6Admission{handle: l.early})
	return l, nil
}

// reportConflict never waits for shutdown or another filesystem worker. One
// report is sufficient because a proven conflict rejects the configuration.
func (l *v6LogRuntime) reportConflict(first, second uint8) {
	activeLogConflicts.Lock()
	target := activeLogConflicts.target
	if target == nil {
		target = l.conflicts
	} else {
		activeLogConflicts.conflicted = true
		if activeLogConflicts.batch != nil {
			activeLogConflicts.batch.Store(true)
		}
	}
	select {
	case target <- [2]uint8{first, second}:
	default:
	}
	activeLogConflicts.Unlock()
}

func (l *v6LogRuntime) useStdout(w io.Writer) {
	if owner, err := asynclog.StartSingle(streamSystemConsole, func() asynclog.Sink {
		return &v6ConsoleSink{writer: w}
	}); err == nil {
		l.stdout = owner
	}
	l.fanout.setConsole(v6Admission{handle: l.stdout})
}

func (l *v6LogRuntime) startFiles(cfg config.LoggingConfig, hook logRotateHook, peering bool) {
	destinations := fileDestinationConfigs(cfg, hook)
	if !peering {
		for i, destination := range destinations {
			if destination.Stream == streamPeerOverlong {
				destinations = append(destinations[:i], destinations[i+1:]...)
				break
			}
		}
	}
	if len(destinations) == 0 {
		return
	}
	streams := make([]uint8, 0, len(destinations))
	for _, destination := range destinations {
		streams = append(streams, destination.Stream)
	}
	leases, _, err := sharedLogIdentities.BeginBatch(streams)
	if err != nil {
		return
	}
	batch := &logIdentityBatch{remaining: len(destinations), ready: make(chan struct{}), stop: l.stop, logging: l}
	activeLogConflicts.Lock()
	if activeLogConflicts.target == l.conflicts {
		activeLogConflicts.batch = &batch.conflict
		if activeLogConflicts.conflicted {
			batch.conflict.Store(true)
		}
	}
	activeLogConflicts.Unlock()
	for _, destination := range destinations {
		cfg := destination
		lease := leases[cfg.Stream]
		if !lease.Valid() {
			batch.completed()
			continue
		}
		start := &logStartGuard{lease: lease, batch: batch}
		l.starts[cfg.Stream] = start
		h, err := asynclog.StartSingle(cfg.Stream, func() asynclog.Sink { return start.start(cfg) })
		if err == nil {
			l.files.handles[cfg.Stream] = h
		} else {
			start.skip()
		}
	}
	if cfg.Enabled {
		l.fanout.setFile(v6Admission{handle: l.files.handles[streamSystemFile]})
	}
}

func (l *v6LogRuntime) startupFailure(message string) {
	if len(message) <= asynclog.MaxRecordBytes {
		l.early.EmitString(time.Now().UTC(), 0, message)
	}
}

func (l *v6LogRuntime) close(deadline time.Time) {
	if l == nil {
		return
	}
	l.closeOnce.Do(func() {
		defer func() {
			if l.stop != nil {
				close(l.stop)
			}
			for _, start := range l.starts {
				if start != nil {
					start.skip()
				}
			}
		}()
		activeLogConflicts.Lock()
		routerDone := sharedProcessLogRouter.clearForRetirement(&l.fanout)
		if activeLogConflicts.target == l.conflicts {
			activeLogConflicts.target = nil
			activeLogConflicts.batch = nil
			activeLogConflicts.conflicted = false
		}
		activeLogConflicts.Unlock()
		l.early.Stop()
		l.stdout.Stop()
		l.files.stop()
		for stream, start := range l.starts {
			if start != nil && l.files.handles[stream].Stats().Queued == 0 {
				start.skip()
			}
		}
		finishRouter := func() {
			l.fanout.close()
			sharedProcessLogRouter.finishRetirement(routerDone)
		}
		if routerDone != nil {
			select {
			case <-routerDone:
				finishRouter()
			default:
				go func() { <-routerDone; finishRouter() }()
			}
		}
		for _, h := range append(l.files.handles[:], l.early, l.stdout) {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return
			}
			timer := time.NewTimer(remaining)
			select {
			case <-h.Done():
				timer.Stop()
			case <-timer.C:
				return
			}
		}
	})
}
