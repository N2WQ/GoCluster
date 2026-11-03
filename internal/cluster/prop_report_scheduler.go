// File role: Owns bounded asynchronous daily propagation-report generation.
// Crawler notes: Start here to trace how propagation log rotation and fixed UTC
// schedules enqueue report jobs without blocking runtime shutdown or logging.
// Related docs: docs/OPERATOR_GUIDE.md, data/config/README.md.
// Related tests: internal/cluster/prop_report_scheduler_test.go.
package cluster

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dxcluster/internal/logutil"
	"dxcluster/internal/propreport"
)

const (
	propReportQueueDepth = 1
	propReportTimeout    = 2 * time.Minute
	propReportDateLayout = "2006-01-02"
)

// propReportJob identifies one completed UTC log archive that should produce a
// propagation report. Date is the stable dedupe key; LogPath is allowed to be
// filled in late so manual and rotation-triggered jobs share the same path logic.
type propReportJob struct {
	Date    time.Time
	LogPath string
	root    *os.Root
	archive reportArchiveLease
}

// Two slots cover the single queued and single running report. A failed close
// permanently retains its slot, so uncertain handles cannot accumulate.
var reportArchiveOwners struct {
	sync.Mutex
	slots [2]struct {
		generation uint64
		state      uint8 // 0 free, 1 reserved, 2 ready, 3 borrowed, 4 closing, 5 uncertain
		file       *os.File
		uncertain  *logCloseOwner
	}
}

// retainUncertain keeps failed acquisition cleanup owned by the lease.
func (l reportArchiveLease) retainUncertain(owner *logCloseOwner) {
	if l.generation == 0 || owner == nil || owner.err == nil {
		return
	}
	reportArchiveOwners.Lock()
	s := &reportArchiveOwners.slots[l.index]
	if s.generation == l.generation && s.state == 1 {
		s.uncertain, s.state = owner, 5
	}
	reportArchiveOwners.Unlock()
}

type reportAcquisitionError struct{ owner *logCloseOwner }

func (e *reportAcquisitionError) Error() string { return e.owner.err.Error() }
func (e *reportAcquisitionError) Unwrap() error { return propreport.ErrReportCleanupUncertain }

type reportArchiveLease struct {
	index      uint8
	generation uint64
}

func reserveReportArchive() reportArchiveLease {
	reportArchiveOwners.Lock()
	defer reportArchiveOwners.Unlock()
	for i := range reportArchiveOwners.slots {
		s := &reportArchiveOwners.slots[i]
		if s.state == 0 && s.generation != math.MaxUint64 {
			s.generation++
			s.state = 1
			return reportArchiveLease{uint8(i), s.generation}
		}
	}
	return reportArchiveLease{}
}

func (l reportArchiveLease) set(file *os.File) bool {
	if l.generation == 0 || file == nil {
		return false
	}
	reportArchiveOwners.Lock()
	defer reportArchiveOwners.Unlock()
	s := &reportArchiveOwners.slots[l.index]
	if s.generation != l.generation || s.state != 1 {
		return false
	}
	s.file, s.state = file, 2
	return true
}

func (l reportArchiveLease) take() (io.ReadCloser, error) {
	if l.generation == 0 {
		return nil, fmt.Errorf("report archive unavailable")
	}
	reportArchiveOwners.Lock()
	defer reportArchiveOwners.Unlock()
	s := &reportArchiveOwners.slots[l.index]
	if s.generation != l.generation || s.state != 2 {
		return nil, fmt.Errorf("report archive unavailable")
	}
	s.state = 3
	return &reportArchiveReader{lease: l, file: s.file}, nil
}

// release closes a reserved or ready file, or the scanner's borrowed file.
// It never holds the owner mutex across a potentially stuck filesystem call.
func (l reportArchiveLease) release(borrowed bool) error {
	if l.generation == 0 {
		return nil
	}
	reportArchiveOwners.Lock()
	s := &reportArchiveOwners.slots[l.index]
	if s.generation != l.generation || (borrowed && s.state != 3) || (!borrowed && s.state != 1 && s.state != 2) {
		reportArchiveOwners.Unlock()
		return nil
	}
	file := s.file
	s.state = 4
	reportArchiveOwners.Unlock()
	var err error
	if file != nil {
		err = file.Close()
	}
	reportArchiveOwners.Lock()
	if err != nil {
		s.state = 5
	} else {
		s.file, s.state = nil, 0
	}
	reportArchiveOwners.Unlock()
	return err
}

type reportArchiveReader struct {
	lease reportArchiveLease
	file  *os.File
}

func (r *reportArchiveReader) Read(p []byte) (int, error) { return r.file.Read(p) }
func (r *reportArchiveReader) Close() error               { return r.lease.release(true) }

type propReportRunner interface {
	Run(ctx context.Context, job propReportJob) error
}

// propReportScheduler isolates slow report generation from log rotation and
// shutdown paths. It keeps at most one queued job plus one running job so a bad
// LLM/tool boundary cannot build an unbounded backlog.
type propReportScheduler struct {
	queue   chan propReportJob
	runner  propReportRunner
	logger  *log.Logger
	timeout time.Duration

	mu      sync.Mutex
	pending map[string]struct{}
	running string
	started bool
	stopped bool
	done    chan struct{}
}

// The live scheduler remains strongly owned until its worker really returns,
// including a Run call that ignores cancellation or a blocked callback.
var propReportWorkerOwner struct {
	sync.Mutex
	owner *propReportScheduler
}

// newPropReportScheduler wires a bounded queue and timeout around the runner.
func newPropReportScheduler(runner propReportRunner, logger *log.Logger, timeout time.Duration) *propReportScheduler {
	if timeout <= 0 {
		timeout = propReportTimeout
	}
	return &propReportScheduler{
		queue:   make(chan propReportJob, propReportQueueDepth),
		runner:  runner,
		logger:  logger,
		timeout: timeout,
		pending: make(map[string]struct{}),
		done:    make(chan struct{}),
	}
}

// Start launches the single worker. The worker exits through ctx cancellation
// so shutdown owns the lifetime instead of report generation.
func (s *propReportScheduler) Start(ctx context.Context) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopped || ctx == nil || ctx.Err() != nil {
		return false
	}
	propReportWorkerOwner.Lock()
	if propReportWorkerOwner.owner != nil {
		propReportWorkerOwner.Unlock()
		s.stopped = true
		s.clearPendingLocked()
		close(s.done)
		return false
	}
	propReportWorkerOwner.owner = s
	propReportWorkerOwner.Unlock()
	s.started = true
	go s.run(ctx)
	return true
}

// Wait lets shutdown drain an in-flight report worker without accepting new
// jobs through any separate close protocol.
func (s *propReportScheduler) Wait() {
	s.WaitContext(context.Background())
}

// WaitContext waits for positive worker retirement, or returns false when the
// caller's existing shutdown deadline expires. It never starts a helper worker.
func (s *propReportScheduler) WaitContext(ctx context.Context) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	started := s.started
	done := s.done
	if !started && !s.stopped {
		s.stopped = true
		s.clearPendingLocked()
		close(done)
	}
	s.mu.Unlock()
	if !started {
		return true
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// clearPendingLocked releases any queued rotation paths after admission ends.
// The file logger retains their backing reservation until the worker retires.
func (s *propReportScheduler) clearPendingLocked() {
	for {
		select {
		case <-s.queue:
		default:
			clear(s.pending)
			return
		}
	}
}

// Enqueue coalesces jobs by date to avoid duplicate reports for the same log
// rotation. A full queue drops the request and logs the skip rather than
// blocking the runtime path that noticed the report opportunity.
func (s *propReportScheduler) Enqueue(job propReportJob) bool {
	if s == nil {
		return false
	}
	job.Date = dateOnly(job.Date.UTC())
	key := job.Date.Format(propReportDateLayout)

	s.mu.Lock()
	if s.stopped || (job.archive.generation != 0 && !s.started) {
		s.mu.Unlock()
		return false
	}
	if s.running == key {
		s.mu.Unlock()
		s.logf("Prop report skip: already running for %s", key)
		return false
	}
	if _, ok := s.pending[key]; ok {
		s.mu.Unlock()
		s.logf("Prop report skip: already queued for %s", key)
		return false
	}
	select {
	case s.queue <- job:
		s.pending[key] = struct{}{}
		s.mu.Unlock()
		s.logf("Prop report queued for %s (%s)", key, job.LogPath)
		return true
	default:
		s.mu.Unlock()
		s.logf("Prop report drop: queue full; skipping %s (%s)", key, job.LogPath)
		return false
	}
}

// run serializes report generation so the report tool reads one log/config
// snapshot at a time and pending/running bookkeeping converges after failures.
func (s *propReportScheduler) run(ctx context.Context) {
	defer func() {
		s.mu.Lock()
		s.stopped = true
		var pending []reportArchiveLease
		for {
			select {
			case job := <-s.queue:
				pending = append(pending, job.archive)
			default:
				clear(s.pending)
				s.mu.Unlock()
				for _, archive := range pending {
					_ = archive.release(false) //nolint:errcheck // Failed close keeps the bounded owner reserved.
				}
				goto drained
			}
		}
	drained:
		propReportWorkerOwner.Lock()
		if propReportWorkerOwner.owner == s {
			propReportWorkerOwner.owner = nil
		}
		propReportWorkerOwner.Unlock()
		close(s.done)
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case job := <-s.queue:
			if ctx.Err() != nil {
				_ = job.archive.release(false) //nolint:errcheck // Failed close keeps the bounded owner reserved.
				return
			}
			job.Date = dateOnly(job.Date.UTC())
			key := job.Date.Format(propReportDateLayout)
			s.mu.Lock()
			delete(s.pending, key)
			s.running = key
			s.mu.Unlock()

			s.logf("Prop report start for %s (%s)", key, job.LogPath)
			runCtx, cancel := context.WithTimeout(ctx, s.timeout)
			err := s.runner.Run(runCtx, job)
			cancel()
			_ = job.archive.release(false) //nolint:errcheck // Scanner owns a transferred archive; failures keep its slot reserved.
			if err != nil {
				s.logf("Prop report failed for %s: %v", key, err)
			} else {
				s.logf("Prop report complete for %s", key)
			}

			s.mu.Lock()
			if s.running == key {
				s.running = ""
			}
			s.mu.Unlock()
		}
	}
}

func (s *propReportScheduler) logf(format string, args ...any) {
	if s == nil || s.logger == nil {
		return
	}
	logutil.SafePrintf(s.logger, format, args...)
}

type propReportGenerator struct {
	configDir    string
	openAIConfig string
	logger       *log.Logger
}

// newPropReportGenerator binds report generation to the active config
// directory, including the optional private openai.yaml used at the tool
// boundary.
func newPropReportGenerator(configDir string, logger *log.Logger) *propReportGenerator {
	return &propReportGenerator{
		configDir:    configDir,
		openAIConfig: filepath.Join(configDir, "openai.yaml"),
		logger:       logger,
	}
}

// Run invokes the report tool with deterministic output paths for the report
// date. The timeout comes from the scheduler so tool failures remain bounded.
func (g *propReportGenerator) Run(ctx context.Context, job propReportJob) error {
	if job.Date.IsZero() {
		return fmt.Errorf("missing job date")
	}
	if strings.TrimSpace(job.LogPath) == "" {
		job.LogPath = logutil.DailyArchivePath(filepath.Join("data", "logs", "propagation"), job.Date)
	}

	var openLog func() (io.ReadCloser, error)
	if job.archive.generation != 0 {
		openLog = job.archive.take
	} else if job.root != nil {
		root, name := job.root, filepath.Base(job.LogPath)
		openLog = func() (io.ReadCloser, error) {
			owner := &logCloseOwner{}
			file, err := openSafeLogInRootOwned(root, name, os.O_RDONLY, 0, owner)
			if owner.err != nil {
				return nil, &reportAcquisitionError{owner: owner}
			}
			return file, err
		}
	}
	_, err := propreport.Generate(ctx, propreport.Options{
		Date:             job.Date,
		LogPath:          job.LogPath,
		OpenLog:          openLog,
		JSONOut:          filepath.Join("data", "reports", fmt.Sprintf("prop-%s.json", job.Date.Format(propReportDateLayout))),
		ReportOut:        filepath.Join("data", "reports", fmt.Sprintf("prop-%s.md", job.Date.Format(propReportDateLayout))),
		ConfigDir:        g.configDir,
		OpenAIConfigPath: g.openAIConfig,
		NoLLM:            false,
		Logger:           g.logger,
	})
	return err
}
