package peerdiag

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	ipcTimeout        = 250 * time.Millisecond
	joinTimeout       = time.Second
	helperRetry       = 5 * time.Second
	ackBytes          = 16
	ackWritten        = 1
	ackSuppressed     = 2
	ackFailed         = 3
	parentReservation = 1 << 20
	helperReservation = 2 << 20
)

// Options consume the already-normalized logging configuration. In particular,
// zero dedupe means disabled and is never replaced by a helper-side default.
type Options struct {
	Enabled       bool
	Directory     string
	RetentionDays int
	DedupeWindow  time.Duration
	OverlongPath  string
}

// Service owns at most one companion generation, its IPC socket, and Wait
// goroutine. Failed termination keeps that generation charged and prevents all
// replacements. Protocol actors only access Mailbox; they never wait on I/O.
type Service struct {
	*Mailbox
	options                                             Options
	startOnce, stopOnce                                 sync.Once
	stop, done                                          chan struct{}
	started                                             bool
	refused                                             bool
	mu                                                  sync.Mutex
	retired                                             *helperProcess
	reportedDropped, reportedUnconfirmed                uint64 // supervisor-owned summary progress
	summarySequence, summaryDropped, summaryUnconfirmed uint64
}

type helperProcess struct {
	process                                   *companionProcess
	waitSucceeded                             bool
	conn                                      net.Conn
	listener                                  *net.TCPListener
	devnull                                   *os.File
	connClosed, listenerClosed, devnullClosed bool
	cleanupFailed                             bool
	exited                                    chan struct{}
	failedExit                                bool // read only after exited closes
}

// A strong process-lifetime owner survives a discarded Manager after failed
// shutdown. Reserve before constructing the queue: refusing only at launch
// would permit another 512 KiB mailbox beside an unretired 3 MiB generation.
var companionOwner struct {
	sync.Mutex
	service *Service
}
var refusedEnabled = &Service{refused: true, Mailbox: &Mailbox{closed: true, stats: Stats{Degraded: true}}}
var refusedDisabled = &Service{refused: true, Mailbox: &Mailbox{closed: true, disabled: true, stats: Stats{Disabled: true, Degraded: true}}}

func New(options Options) *Service {
	companionOwner.Lock()
	defer companionOwner.Unlock()
	if companionOwner.service != nil {
		if options.Enabled {
			return refusedEnabled
		}
		return refusedDisabled
	}
	mailbox := NewMailbox(options.Enabled)
	mailbox.stats.ChargedBytes = parentReservation
	s := &Service{Mailbox: mailbox, options: options, stop: make(chan struct{}), done: make(chan struct{})}
	companionOwner.service = s
	return s
}

// Refused handles own no queue or worker, but expose a prior owner's uncertain
// cleanup and backing so reconstruction cannot hide retained ownership. Producer
// counters remain those of the refused handle; they are not the old producer's.
func (s *Service) Snapshot() Stats {
	stats := s.Mailbox.Snapshot()
	if s.refused {
		companionOwner.Lock()
		if owner := companionOwner.service; owner != nil {
			owned := owner.Mailbox.Snapshot()
			stats.CleanupFailed = owned.CleanupFailed
			stats.ChargedBytes = owned.ChargedBytes
			stats.Generation = owned.Generation
		}
		companionOwner.Unlock()
	}
	return stats
}

func (s *Service) Start() {
	if s == nil || s.refused {
		return
	}
	s.startOnce.Do(func() {
		s.mu.Lock()
		select {
		case <-s.stop:
			s.mu.Unlock()
			return
		default:
		}
		s.started = true
		s.mu.Unlock()
		go s.run()
	})
}

func (s *Service) Stop() {
	if s == nil || s.refused {
		return
	}
	s.stopOnce.Do(func() { s.closeAdmission(); close(s.stop) })
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-s.done:
		case <-timer.C:
			// Even process creation/OS teardown can fail to return. Keep the
			// service generation owned and charged, and report failure rather
			// than allowing diagnostics to hold protocol shutdown forever.
			s.Mailbox.mu.Lock()
			s.stats.CleanupFailed, s.stats.Degraded = true, true
			s.Mailbox.mu.Unlock()
		}
	} else {
		s.releaseOwner()
	}
}

func (s *Service) releaseOwner() {
	s.mu.Lock()
	uncertain := s.retired != nil
	s.mu.Unlock()
	if uncertain {
		return
	}
	companionOwner.Lock()
	defer companionOwner.Unlock()
	if companionOwner.service != s {
		return
	}
	s.Mailbox.mu.Lock()
	s.events = nil
	s.stats.ChargedBytes = 0
	s.Mailbox.mu.Unlock()
	s.options = Options{}
	companionOwner.service = nil
}

func (s *Service) setDegraded(degraded bool) {
	s.Mailbox.mu.Lock()
	s.stats.Degraded = degraded
	s.Mailbox.mu.Unlock()
}

func (s *Service) run() {
	defer func() { s.releaseOwner(); close(s.done) }()
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		// Reserve the whole generation before launch builds argv, paths, socket
		// buffers or process state, including a partially failed launch.
		s.Mailbox.mu.Lock()
		s.stats.ChargedBytes = parentReservation + helperReservation
		s.Mailbox.mu.Unlock()
		child, err := s.launch()
		if err != nil {
			s.setDegraded(true)
			if child != nil {
				if !s.retire(child) {
					if !s.awaitRetirement(child) {
						return
					}
				}
			} else {
				s.released()
			}
			if !s.waitRetry() {
				return
			}
			continue
		}
		s.Mailbox.mu.Lock()
		s.stats.Degraded = false
		s.stats.Generation++
		// This reservation includes both live processes' protocol-owned data,
		// including the active IPC record and all deduper slots. Its breakdown is
		// verified separately; it is not a measurement of Go runtime overhead.
		s.Mailbox.mu.Unlock()
		s.emitLossSummary()
		s.exchange(child)
		if !s.retire(child) {
			if !s.awaitRetirement(child) {
				return
			}
		}
		select {
		case <-s.stop:
			return
		default:
		}
		s.setDegraded(true)
		if !s.waitRetry() {
			return
		}
	}
}

func (s *Service) waitRetry() bool {
	timer := time.NewTimer(helperRetry)
	defer timer.Stop()
	select {
	case <-s.stop:
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) exchange(child *helperProcess) {
	var wire [RecordBytes]byte
	var ack [ackBytes]byte
	for {
		select {
		case <-s.stop:
			return
		case <-child.exited:
			return
		default:
		}
		event, ok := s.Next()
		if !ok {
			select {
			case <-s.stop:
				return
			case <-child.exited:
				return
			case <-s.notify:
				continue
			}
		}
		event.encode(&wire)
		if err := child.conn.SetDeadline(time.Now().Add(ipcTimeout)); err != nil {
			if event.Sequence == s.summarySequence {
				s.summarySequence = 0
			}
			s.Mailbox.mu.Lock()
			s.stats.Dropped++
			s.Mailbox.mu.Unlock()
			return
		}
		if writeFull(child.conn, wire[:]) != nil {
			if event.Sequence == s.summarySequence {
				s.summarySequence = 0
			}
			s.unconfirmed()
			return
		}
		if _, err := io.ReadFull(child.conn, ack[:]); err != nil || binary.LittleEndian.Uint64(ack[:8]) != event.Sequence {
			if event.Sequence == s.summarySequence {
				s.summarySequence = 0
			}
			s.unconfirmed()
			return
		}
		outcome := binary.LittleEndian.Uint32(ack[8:12])
		keys := binary.LittleEndian.Uint32(ack[12:16])
		if outcome < ackWritten || outcome > ackFailed || keys > 512 {
			if event.Sequence == s.summarySequence {
				s.summarySequence = 0
			}
			s.unconfirmed()
			return
		}
		s.Mailbox.mu.Lock()
		s.stats.DeduperKeys = int(keys)
		switch outcome {
		case ackWritten:
			s.stats.Written++
			s.stats.Degraded = false
		case ackSuppressed:
			s.stats.Suppressed++
		case ackFailed:
			// A failed file write can have written a prefix. Without a successful
			// acknowledgement its durable extent is unknown, never a known drop.
			s.stats.Unconfirmed++
			s.stats.Degraded = true
		}
		s.Mailbox.mu.Unlock()
		if event.Sequence == s.summarySequence {
			if outcome == ackWritten {
				s.reportedDropped, s.reportedUnconfirmed = s.summaryDropped, s.summaryUnconfirmed
			}
			s.summarySequence = 0
		}
		if outcome == ackWritten {
			s.emitLossSummary()
		}
	}
}

func (s *Service) emitLossSummary() {
	if s.summarySequence != 0 {
		return
	}
	stats := s.Snapshot()
	if stats.Dropped == s.reportedDropped && stats.Unconfirmed == s.reportedUnconfirmed {
		return
	}
	s.Mailbox.mu.Lock()
	defer s.Mailbox.mu.Unlock()
	if s.count == QueueSize || s.closed {
		return
	}
	if s.emitLocked(LossSummary, Fields{Action: "recovered", Reason: "known_drops_and_unconfirmed_writes", Count: int64(stats.Dropped), Limit: int64(stats.Unconfirmed)}) {
		s.summarySequence = s.sequence
		s.summaryDropped, s.summaryUnconfirmed = stats.Dropped, stats.Unconfirmed
	}
}

func (s *Service) unconfirmed() {
	s.Mailbox.mu.Lock()
	s.stats.Unconfirmed++
	s.Mailbox.mu.Unlock()
}

func (s *Service) retire(child *helperProcess) bool {
	child.closeHandles(true)
	if child.process == nil || !child.process.started() {
		return s.confirmRetirement(child)
	}
	select {
	case <-child.exited:
		return s.confirmRetirement(child)
	default:
	}
	if err := child.process.Kill(); err != nil {
		// A process can finish between the prior observation and Kill. Only
		// Wait proves retirement, regardless of the platform's Kill error.
		select {
		case <-child.exited:
			return s.confirmRetirement(child)
		default:
		}
	}
	timer := time.NewTimer(joinTimeout)
	defer timer.Stop()
	select {
	case <-child.exited:
		return s.confirmRetirement(child)
	case <-timer.C:
		return s.retainUncertain(child)
	}
}

func (child *helperProcess) closeHandles(includeConnection bool) {
	if includeConnection && child.conn != nil && !child.connClosed {
		child.connClosed = true
		if err := child.conn.Close(); err != nil {
			child.cleanupFailed = true
		} else {
			child.conn = nil
		}
	}
	if child.listener != nil && !child.listenerClosed {
		child.listenerClosed = true
		if err := child.listener.Close(); err != nil {
			child.cleanupFailed = true
		} else {
			child.listener = nil
		}
	}
	if child.devnull != nil && !child.devnullClosed {
		child.devnullClosed = true
		if err := child.devnull.Close(); err != nil {
			child.cleanupFailed = true
		} else {
			child.devnull = nil
		}
	}
}

func (s *Service) retainUncertain(child *helperProcess) bool {
	s.mu.Lock()
	s.retired = child
	s.mu.Unlock()
	s.Mailbox.mu.Lock()
	s.stats.CleanupFailed, s.stats.Degraded = true, true
	s.stats.ChargedBytes = BackingLimit
	s.Mailbox.mu.Unlock()
	return false
}

func (s *Service) confirmRetirement(child *helperProcess) bool {
	if child.cleanupFailed || (child.process != nil && child.process.started() && (child.failedExit || !child.waitSucceeded)) {
		return s.retainUncertain(child)
	}
	if child.process != nil && child.process.release() != nil {
		return s.retainUncertain(child)
	}
	s.mu.Lock()
	s.retired = nil
	s.mu.Unlock()
	s.Mailbox.mu.Lock()
	s.stats.CleanupFailed = false
	s.Mailbox.mu.Unlock()
	return s.released()
}

func (s *Service) released() bool {
	s.Mailbox.mu.Lock()
	s.stats.DeduperKeys = 0
	s.stats.ChargedBytes = parentReservation
	s.Mailbox.mu.Unlock()
	return true
}

// A failed bounded join does not release the generation. The sole supervisor
// can remain here after bounded Stop returns; only actual Wait completion can
// discharge its ownership and permit a replacement. No polling or extra worker
// is created for delayed cleanup.
func (s *Service) awaitRetirement(child *helperProcess) bool {
	if child.process != nil && child.process.started() {
		<-child.exited
	}
	return s.confirmRetirement(child)
}

func (s *Service) launch() (*helperProcess, error) {
	deadline := time.Now().Add(ipcTimeout)
	if !validOptions(s.options) {
		return nil, errors.New("diagnostic options exceed bounded launch storage")
	}
	environmentRoot := helperEnvironmentRoot()
	executable, err := helperExecutablePath(helperEnvironmentBytes(environmentRoot))
	if err != nil {
		return nil, err
	}
	path := filepath.Join(filepath.Dir(executable), helperExecutable)
	if !parentLaunchFits(len(executable), len(path), helperEnvironmentBytes(environmentRoot)) {
		return nil, errors.New("diagnostic launch reservation exhausted")
	}
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	child := &helperProcess{listener: listener}
	var token [32]byte
	if _, err = rand.Read(token[:]); err != nil {
		return child, err
	}
	// Use the exact absolute sibling and short argv. exec.Command/Start performs
	// Windows extension lookup even for an absolute .exe and can copy arbitrary
	// inherited PATHEXT. The owned Windows adapter/Linux StartProcess has neither
	// that lookup nor an extra cancellation owner. Larger options use IPC.
	args := []string{helperExecutable, "-address", listener.Addr().String(), "-token", hex.EncodeToString(token[:])}
	devnull, err := diagnosticOpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return child, err
	}
	child.devnull = devnull
	attributes := &os.ProcAttr{Env: helperEnvironment(environmentRoot), Files: []*os.File{devnull, devnull, devnull}, Sys: helperProcessAttributes()}
	process, err := startCompanion(path, args, attributes)
	child.process = process
	if process != nil && process.started() {
		child.exited = make(chan struct{})
		go func() {
			var waitErr error
			child.waitSucceeded, waitErr = process.Wait()
			child.failedExit = waitErr != nil
			close(child.exited)
		}()
	}
	if err != nil {
		return child, err
	}
	if err = listener.SetDeadline(deadline); err != nil {
		return child, err
	}
	conn, err := listener.AcceptTCP()
	if err != nil {
		return child, err
	}
	child.conn = conn
	if err = conn.SetDeadline(deadline); err != nil {
		return child, err
	}
	var received [32]byte
	if _, err = io.ReadFull(conn, received[:]); err != nil || received != token {
		return child, errors.New("invalid diagnostic helper handshake")
	}
	if err = sendOptions(conn, s.options); err != nil {
		return child, err
	}
	child.closeHandles(false)
	if child.cleanupFailed {
		return child, errors.New("diagnostic setup handle release unconfirmed")
	}
	return child, nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
