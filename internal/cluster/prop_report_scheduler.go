// File role: Owns bounded asynchronous daily propagation-report generation.
// Crawler notes: Start here to trace how propagation log rotation and fixed UTC
// schedules enqueue report jobs without blocking runtime shutdown or logging.
// Related docs: docs/OPERATOR_GUIDE.md, data/config/README.md.
// Related tests: internal/cluster/prop_report_scheduler_test.go.
package cluster

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dxcluster/internal/logutil"
	"dxcluster/internal/propreport"
)

const (
	propReportQueueDepth    = 1
	propReportTimeout       = 2 * time.Minute
	propReportDateLayout    = "2006-01-02"
	propReportLogDateLayout = "02-Jan-2006"
)

// propReportJob identifies one daily log file that should produce a propagation
// report. Date is the stable dedupe key; LogPath is allowed to be filled in late
// so manual and rotation-triggered jobs share the same path logic.
type propReportJob struct {
	Date    time.Time
	LogPath string
}

type propReportRunner interface {
	Run(ctx context.Context, job propReportJob) error
}

// propReportScheduler isolates slow report generation from log rotation and
// shutdown paths. It keeps at most one queued job plus one running job so a bad
// LLM/tool boundary cannot build an unbounded backlog.
type propReportScheduler struct {
	enabled bool
	queue   chan propReportJob
	runner  propReportRunner
	logger  *log.Logger
	timeout time.Duration

	mu      sync.Mutex
	pending map[string]struct{}
	running string
	wg      sync.WaitGroup
}

// newPropReportScheduler wires a bounded queue and timeout around the runner.
// A disabled scheduler still returns an object so startup code does not need a
// second nil-oriented control path.
func newPropReportScheduler(enabled bool, runner propReportRunner, logger *log.Logger, timeout time.Duration) *propReportScheduler {
	if timeout <= 0 {
		timeout = propReportTimeout
	}
	return &propReportScheduler{
		enabled: enabled,
		queue:   make(chan propReportJob, propReportQueueDepth),
		runner:  runner,
		logger:  logger,
		timeout: timeout,
		pending: make(map[string]struct{}),
	}
}

// Start launches the single worker. The worker exits through ctx cancellation
// so shutdown owns the lifetime instead of report generation.
func (s *propReportScheduler) Start(ctx context.Context) {
	if s == nil || !s.enabled {
		return
	}
	s.wg.Add(1)
	go s.run(ctx)
}

// Wait lets shutdown drain an in-flight report worker without accepting new
// jobs through any separate close protocol.
func (s *propReportScheduler) Wait() {
	if s == nil {
		return
	}
	s.wg.Wait()
}

// Enqueue coalesces jobs by date to avoid duplicate reports for the same log
// rotation. A full queue drops the request and logs the skip rather than
// blocking the runtime path that noticed the report opportunity.
func (s *propReportScheduler) Enqueue(job propReportJob) bool {
	if s == nil || !s.enabled {
		return false
	}
	job.Date = dateOnly(job.Date.UTC())
	key := job.Date.Format(propReportDateLayout)

	s.mu.Lock()
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
	s.mu.Unlock()

	select {
	case s.queue <- job:
		s.mu.Lock()
		s.pending[key] = struct{}{}
		s.mu.Unlock()
		s.logf("Prop report queued for %s (%s)", key, job.LogPath)
		return true
	default:
		s.logf("Prop report drop: queue full; skipping %s (%s)", key, job.LogPath)
		return false
	}
}

// run serializes report generation so the report tool reads one log/config
// snapshot at a time and pending/running bookkeeping converges after failures.
func (s *propReportScheduler) run(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-s.queue:
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
	if s == nil {
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
		job.LogPath = filepath.Join("data", "logs", "propagation", fmt.Sprintf("%s.log", job.Date.Format(propReportLogDateLayout)))
	}

	_, err := propreport.Generate(ctx, propreport.Options{
		Date:             job.Date,
		LogPath:          job.LogPath,
		JSONOut:          filepath.Join("data", "reports", fmt.Sprintf("prop-%s.json", job.Date.Format(propReportDateLayout))),
		ReportOut:        filepath.Join("data", "reports", fmt.Sprintf("prop-%s.md", job.Date.Format(propReportDateLayout))),
		ConfigDir:        g.configDir,
		OpenAIConfigPath: g.openAIConfig,
		NoLLM:            false,
		Logger:           g.logger,
	})
	return err
}
