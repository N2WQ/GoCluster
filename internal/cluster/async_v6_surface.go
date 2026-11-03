package cluster

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/internal/asynclog"
	"dxcluster/ui"
)

var v6SurfaceGate struct {
	sync.Mutex
	occupied bool
}

var v6SurfaceClosed = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type v6Surface struct {
	mu       sync.Mutex
	target   *ui.DashboardV2
	handles  [6]asynclog.Handle
	limits   [6]int
	created  chan struct{}
	ready    chan struct{}
	done     chan struct{}
	readyOK  bool
	stopping bool
	active   int
	drained  chan struct{}
	stopOnce sync.Once
	rawGap   atomic.Bool
}

type v6PaneSink struct {
	surface *v6Surface
	pane    int
	raw     *ui.DashboardV2BoundedSystemWriter
}

func (s *v6PaneSink) Write(_ time.Time, frame uint8, payload []byte) asynclog.WriteResult {
	s.surface.mu.Lock()
	target := s.surface.target
	s.surface.mu.Unlock()
	if target == nil {
		return asynclog.NotWritten
	}
	if s.pane == 5 && frame == 2 {
		if s.raw == nil {
			var err error
			s.raw, err = target.NewBoundedSystemWriter(asynclog.MaxMessageBytes, func(ui.DashboardSystemDiscard) {})
			if err != nil {
				return asynclog.NotWritten
			}
		}
		if s.surface.rawGap.Load() {
			s.raw.MarkGap()
			return asynclog.NotWritten
		}
		n, err := s.raw.Write(payload)
		if err == nil && n == len(payload) {
			return asynclog.Written
		}
		return asynclog.NotWritten
	}
	if s.pane == 5 && frame != 0 {
		// Standard log calls may contain multiple physical lines. Each line
		// retains the dashboard's configured message-shortening behavior.
		payload = bytes.TrimSuffix(payload, []byte("\n"))
		for {
			line, rest, more := bytes.Cut(payload, []byte("\n"))
			target.AppendSystem(strings.TrimRight(string(line), "\r"))
			if !more {
				return asynclog.Written
			}
			payload = rest
		}
	}
	line := string(payload)
	switch s.pane {
	case 0:
		target.AppendDropped(line)
	case 1:
		target.AppendCall(line)
	case 2:
		target.AppendUnlicensed(line)
	case 3:
		target.AppendHarmonic(line)
	case 4:
		target.AppendReputation(line)
	case 5:
		target.AppendSystem(line)
	default:
		return asynclog.NotWritten
	}
	return asynclog.Written
}
func (s *v6PaneSink) Close() error {
	if s.raw != nil {
		return s.raw.Close()
	}
	return nil
}

// newV6Surface owns at most one dashboard generation process-wide, including
// a constructor, UI callback, or Stop that has not returned after shutdown.
func newV6Surface(cfg config.UIConfig) (*v6Surface, error) {
	return newV6SurfaceWithFactory(func() *ui.DashboardV2 { return ui.NewDashboardV2(cfg, true) }, cfg)
}

func newV6SurfaceWithFactory(factory func() *ui.DashboardV2, configs ...config.UIConfig) (*v6Surface, error) {
	if factory == nil {
		return nil, errors.New("dashboard factory is nil")
	}
	v6SurfaceGate.Lock()
	if v6SurfaceGate.occupied {
		v6SurfaceGate.Unlock()
		return nil, errors.New("previous dashboard remains active")
	}
	v6SurfaceGate.occupied = true
	v6SurfaceGate.Unlock()
	s := &v6Surface{created: make(chan struct{}), ready: make(chan struct{}), done: make(chan struct{})}
	if len(configs) != 0 {
		for pane := 0; pane < 5; pane++ {
			s.limits[pane] = configs[0].V2.EventBuffer.MaxMessageBytes
		}
		s.limits[5] = configs[0].V2.DebugBuffer.MaxMessageBytes
	}
	streams := [...]uint8{streamSurfaceDropped, streamSurfaceCall, streamSurfaceUnlicensed, streamSurfaceHarmonic, streamSurfaceReputation, streamSurfaceSystem}
	for pane, stream := range streams {
		p := pane
		h, err := asynclog.Start(stream, func() asynclog.Sink { return &v6PaneSink{surface: s, pane: p} })
		if err != nil {
			close(s.created)
			close(s.ready)
			s.beginStop()
			return nil, err
		}
		s.handles[pane] = h
	}
	go func() {
		target := callV6DashboardFactory(factory)
		s.mu.Lock()
		s.target = target
		s.mu.Unlock()
		close(s.created)
		ok := target != nil && target.WaitReadyStatus()
		s.mu.Lock()
		s.readyOK = ok && !s.stopping
		s.mu.Unlock()
		close(s.ready)
	}()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-s.ready:
		s.mu.Lock()
		ok := s.readyOK
		s.mu.Unlock()
		if ok {
			return s, nil
		}
	case <-timer.C:
	}
	s.beginStop()
	return nil, errors.New("dashboard did not become ready")
}

func callV6DashboardFactory(factory func() *ui.DashboardV2) (target *ui.DashboardV2) {
	defer func() {
		if recover() != nil {
			target = nil
		}
	}()
	return factory()
}

func (s *v6Surface) emit(pane int, payload []byte, frame uint8) {
	if s != nil {
		s.handles[pane].Emit(time.Now().UTC(), frame, payload)
	}
}
func (s *v6Surface) emitString(pane int, line string) {
	if s != nil {
		limit := s.limits[pane]
		if limit > 0 && limit < asynclog.MaxMessageBytes && len(line) > limit {
			// Keep one extra byte so the pane applies its normal truncation and
			// records it, without copying an arbitrarily long caller string.
			line = line[:limit+1]
		}
		s.handles[pane].EmitString(time.Now().UTC(), 0, line)
	}
}
func (s *v6Surface) AppendDropped(line string) {
	if strings.HasPrefix(line, "CTY drop:") {
		s.emitString(0, line)
	}
}
func (s *v6Surface) AppendCall(line string)       { s.emitString(1, line) }
func (s *v6Surface) AppendUnlicensed(line string) { s.emitString(2, line) }
func (s *v6Surface) AppendHarmonic(line string)   { s.emitString(3, line) }
func (s *v6Surface) AppendReputation(string)      {} // Dashboard routes reputation to its existing event pane.
func (s *v6Surface) AppendSystem(line string)     { s.emitString(5, line) }
func (s *v6Surface) WaitReady() {
	s.mu.Lock()
	ready := s.ready
	s.mu.Unlock()
	<-ready
}

func (s *v6Surface) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

func (s *v6Surface) beginUse() *ui.DashboardV2 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || s.target == nil {
		return nil
	}
	s.active++
	return s.target
}
func (s *v6Surface) endUse() {
	s.mu.Lock()
	s.active--
	if s.stopping && s.active == 0 && s.drained != nil {
		close(s.drained)
	}
	s.mu.Unlock()
}
func (s *v6Surface) SetStats(lines []string) {
	if t := s.beginUse(); t != nil {
		defer s.endUse()
		t.SetStats(lines)
	}
}
func (s *v6Surface) UpdateNetworkStatus(summary string, lines []string) {
	if t := s.beginUse(); t != nil {
		defer s.endUse()
		t.UpdateNetworkStatus(summary, lines)
	}
}
func (s *v6Surface) SetSnapshot(snapshot ui.Snapshot) {
	if t := s.beginUse(); t != nil {
		defer s.endUse()
		t.SetSnapshot(snapshot)
	}
}

type v6SurfaceWriter struct{ surface *v6Surface }

func (w v6SurfaceWriter) Write(p []byte) (int, error) {
	w.surface.emit(5, p, 1)
	return len(p), nil
}
func (s *v6Surface) SystemWriter() io.Writer { return v6SurfaceWriter{surface: s} }

func (s *v6Surface) beginStop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		s.drained = make(chan struct{})
		if s.active == 0 {
			close(s.drained)
		}
		drained := s.drained
		s.mu.Unlock()
		for _, h := range s.handles {
			h.Stop()
		}
		go func(state *v6Surface) {
			<-state.created
			for _, h := range state.handles {
				<-h.Done()
			}
			<-drained
			state.mu.Lock()
			target := state.target
			state.mu.Unlock()
			if target != nil {
				target.Stop()
				target.WaitStopped()
			}
			target = nil //nolint:wastedassign // Release the old dashboard before opening the generation gate.
			<-state.ready
			v6SurfaceGate.Lock()
			state.mu.Lock()
			state.target = nil
			done := state.done
			state.created, state.ready, state.done, state.drained = v6SurfaceClosed, v6SurfaceClosed, v6SurfaceClosed, v6SurfaceClosed
			close(done)
			state.mu.Unlock()
			state = nil //nolint:wastedassign // Release the retired adapter before opening the generation gate.
			v6SurfaceGate.occupied = false
			v6SurfaceGate.Unlock()
		}(s)
	})
}
func (s *v6Surface) Stop() { s.beginStop() }

type v6DashboardAdmission struct{ surface *v6Surface }

func (a v6DashboardAdmission) emit(at time.Time, p []byte) {
	if len(p) <= asynclog.MaxMessageBytes {
		a.surface.emit(5, p, 1)
		return
	}
	// An oversized call may contain many short rows. Admit bounded rows
	// individually instead of clipping the whole call at the first pane limit.
	// Work and queued records stay bounded even for an arbitrarily large call.
	limit := a.surface.limits[5]
	budget := asynclog.MaxMessageBytes
	for rows := 0; rows < asynclog.QueueSize && len(p) != 0 && budget > 0; rows++ {
		span := p[:min(len(p), budget)]
		i := bytes.IndexByte(span, '\n')
		row := span
		if i >= 0 {
			row = span[:i]
		}
		if limit > 0 && limit < asynclog.MaxMessageBytes && len(row) > limit {
			row = row[:limit+1]
		}
		// If the scan budget ends within a row, emit it only when the bytes
		// already seen prove that the pane will shorten it to its own limit.
		if i < 0 && len(p) > len(span) && (limit <= 0 || len(span) <= limit) {
			return
		}
		if !a.surface.handles[5].Emit(at, 1, row) {
			return
		}
		if i < 0 {
			return
		}
		p = p[i+1:]
		budget -= i + 1
	}
}
func (a v6DashboardAdmission) emitStream(at time.Time, p []byte) {
	if !a.surface.handles[5].Emit(at, 2, p) {
		a.surface.rawGap.Store(true)
	}
}
func (v6DashboardAdmission) drop() {}

var _ ui.Surface = (*v6Surface)(nil)
