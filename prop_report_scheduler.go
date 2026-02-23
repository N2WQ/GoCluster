package main

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

type propReportJob struct {
	Date    time.Time
	LogPath string
}

type propReportRunner interface {
	Run(ctx context.Context, job propReportJob) error
}

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

func (s *propReportScheduler) Start(ctx context.Context) {
	if s == nil || !s.enabled {
		return
	}
	s.wg.Add(1)
	go s.run(ctx)
}

func (s *propReportScheduler) Wait() {
	if s == nil {
		return
	}
	s.wg.Wait()
}

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
	pathConfig   string
	openAIConfig string
	logger       *log.Logger
}

func newPropReportGenerator(configDir string, logger *log.Logger) *propReportGenerator {
	return &propReportGenerator{
		pathConfig:   filepath.Join(configDir, "path_reliability.yaml"),
		openAIConfig: filepath.Join(configDir, "openai.yaml"),
		logger:       logger,
	}
}

func (g *propReportGenerator) Run(ctx context.Context, job propReportJob) error {
	if job.Date.IsZero() {
		return fmt.Errorf("missing job date")
	}
	if strings.TrimSpace(job.LogPath) == "" {
		job.LogPath = filepath.Join("data", "logs", fmt.Sprintf("%s.log", job.Date.Format(propReportLogDateLayout)))
	}

	_, err := propreport.Generate(ctx, propreport.Options{
		Date:             job.Date,
		LogPath:          job.LogPath,
		JSONOut:          filepath.Join("data", "reports", fmt.Sprintf("prop-%s.json", job.Date.Format(propReportDateLayout))),
		ReportOut:        filepath.Join("data", "reports", fmt.Sprintf("prop-%s.md", job.Date.Format(propReportDateLayout))),
		PathConfigPath:   g.pathConfig,
		OpenAIConfigPath: g.openAIConfig,
		NoLLM:            false,
		Logger:           g.logger,
	})
	return err
}
