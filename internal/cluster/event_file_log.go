package cluster

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
)

const defaultEventLogDedupeMaxKeys = 512

// eventFileLogger keeps support-critical connection and gate events in
// separate files so operators can troubleshoot login, reputation, telnet,
// ingest, and peer behavior without scraping the console stream. Each sink is
// optional and independently configured so a bad path disables only that event
// file instead of preventing the cluster from starting.
type eventFileLogger struct {
	loginAttempts     *eventLogSink
	reputationDrops   *eventLogSink
	telnetConnections *eventLogSink
	ingestConnections *eventLogSink
}

// eventLogSink couples a daily file sink with a bounded per-line deduper. The
// deduper is intentionally local to one event class so high-volume churn in one
// class cannot hide unrelated support evidence in another file.
type eventLogSink struct {
	sink    lineSink
	deduper *eventLogDeduper
}

type eventLogField struct {
	key   string
	value string
}

// newEventFileLogger builds every enabled event sink up front to surface bad
// paths during startup while still returning the partially usable logger. The
// caller decides whether setup errors are fatal for the deployment.
func newEventFileLogger(cfg config.LoggingConfig) (*eventFileLogger, error) {
	logger := &eventFileLogger{}
	var setupErrs []string
	setup := func(eventCfg config.EventFileLoggingConfig, label string) *eventLogSink {
		if !eventCfg.Enabled {
			return nil
		}
		sink, err := newDailyLogSink(eventCfg.Dir, eventCfg.RetentionDays)
		if err != nil {
			setupErrs = append(setupErrs, fmt.Sprintf("%s: %v", label, err))
			return nil
		}
		window := time.Duration(eventCfg.DedupeWindowSeconds) * time.Second
		return &eventLogSink{
			sink:    sink,
			deduper: newEventLogDeduper(window, defaultEventLogDedupeMaxKeys),
		}
	}
	logger.loginAttempts = setup(cfg.LoginAttempts, "login_attempts")
	logger.reputationDrops = setup(cfg.ReputationDrops, "reputation_drops")
	logger.telnetConnections = setup(cfg.TelnetConnections, "telnet_connections")
	logger.ingestConnections = setup(cfg.IngestConnections, "ingest_connections")
	if len(setupErrs) > 0 {
		return logger, fmt.Errorf("event logging setup: %s", strings.Join(setupErrs, "; "))
	}
	return logger, nil
}

// Close flushes all enabled event sinks during shutdown. It preserves the first
// close error because later sinks should still get a chance to release files.
func (l *eventFileLogger) Close() error {
	if l == nil {
		return nil
	}
	var firstErr error
	for _, sink := range []*eventLogSink{l.loginAttempts, l.reputationDrops, l.telnetConnections, l.ingestConnections} {
		if sink == nil || sink.sink == nil {
			continue
		}
		if err := sink.sink.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *eventFileLogger) LogLoginAttempt(fields ...eventLogField) {
	l.write(l.loginAttempts, fields...)
}

func (l *eventFileLogger) LogReputationDrop(fields ...eventLogField) {
	l.write(l.reputationDrops, fields...)
}

func (l *eventFileLogger) LogTelnetConnection(fields ...eventLogField) {
	l.write(l.telnetConnections, fields...)
}

func (l *eventFileLogger) LogIngestConnection(fields ...eventLogField) {
	l.write(l.ingestConnections, fields...)
}

// write turns structured support fields into one sanitized line. A disabled
// sink is a no-op so call sites can report events unconditionally.
func (l *eventFileLogger) write(sink *eventLogSink, fields ...eventLogField) {
	if l == nil || sink == nil || sink.sink == nil {
		return
	}
	line := formatEventLogLine(fields...)
	if line == "" {
		return
	}
	if sink.deduper != nil {
		var ok bool
		line, ok = sink.deduper.Process(line)
		if !ok {
			return
		}
	}
	sink.sink.WriteLine(line, time.Now().UTC())
}

// formatEventLogLine uses key=value tokens so support tooling can grep without
// needing a schema registry. Empty or unsafe keys are dropped rather than
// emitting ambiguous fields.
func formatEventLogLine(fields ...eventLogField) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		key := sanitizeEventLogKey(field.key)
		if key == "" {
			continue
		}
		parts = append(parts, key+"="+sanitizeEventLogValue(field.value, "unknown"))
	}
	return strings.Join(parts, " ")
}

// sanitizeEventLogKey keeps field names stable and shell-friendly for ad hoc
// support investigation.
func sanitizeEventLogKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		ch := value[i]
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteByte(ch)
		case ch >= '0' && ch <= '9':
			b.WriteByte(ch)
		case ch == '_':
			b.WriteByte(ch)
		}
	}
	return b.String()
}

// sanitizeEventLogValue preserves support evidence while preventing multiline
// or extremely large values from corrupting the daily log format.
func sanitizeEventLogValue(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	value = strings.ReplaceAll(value, "\r", "_")
	value = strings.ReplaceAll(value, "\n", "_")
	value = strings.Join(strings.Fields(value), "_")
	if value == "" {
		return fallback
	}
	if len(value) > 256 {
		value = value[:256]
	}
	return value
}

func eventLogEndpoint(host string, port int) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return "unknown"
	}
	if port <= 0 {
		return host
	}
	return host + ":" + fmt.Sprintf("%d", port)
}

func eventLogIP(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = strings.TrimSpace(address)
	}
	return droppedLogValue(host, "unknown")
}

// eventLogDeduper bounds repeated event chatter by exact emitted line. The
// maxKeys cap protects memory under reconnect storms or hostile address churn;
// oldest-seen eviction is acceptable because a later duplicate can reappear as
// fresh evidence.
type eventLogDeduper struct {
	mu      sync.Mutex
	window  time.Duration
	maxKeys int
	now     func() time.Time
	entries map[string]eventLogDedupeEntry
}

type eventLogDedupeEntry struct {
	nextEmit   time.Time
	lastSeen   time.Time
	suppressed uint64
}

// newEventLogDeduper returns nil when dedupe is disabled so callers keep the
// same reporting path regardless of the operator's dedupe setting.
func newEventLogDeduper(window time.Duration, maxKeys int) *eventLogDeduper {
	if window <= 0 || maxKeys <= 0 {
		return nil
	}
	return &eventLogDeduper{
		window:  window,
		maxKeys: maxKeys,
		now:     func() time.Time { return time.Now().UTC() },
		entries: make(map[string]eventLogDedupeEntry, maxKeys),
	}
}

// Process emits the first line in a window and later summarizes suppressed
// repeats. This keeps event files useful during storms without losing the fact
// that repetition happened.
func (d *eventLogDeduper) Process(line string) (string, bool) {
	if d == nil {
		return line, true
	}
	now := d.now()
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, found := d.entries[line]
	if !found {
		d.evictOneIfNeededLocked()
		d.entries[line] = eventLogDedupeEntry{nextEmit: now.Add(d.window), lastSeen: now}
		return line, true
	}
	entry.lastSeen = now
	if now.Before(entry.nextEmit) {
		entry.suppressed++
		d.entries[line] = entry
		return "", false
	}
	suppressed := entry.suppressed
	entry.suppressed = 0
	entry.nextEmit = now.Add(d.window)
	d.entries[line] = entry
	if suppressed > 0 {
		line = fmt.Sprintf("%s suppressed=%d window=%s", line, suppressed, d.window)
	}
	return line, true
}

// evictOneIfNeededLocked enforces the hard dedupe-key cap. Callers hold d.mu.
func (d *eventLogDeduper) evictOneIfNeededLocked() {
	if d == nil || d.maxKeys <= 0 || len(d.entries) < d.maxKeys {
		return
	}
	var oldestKey string
	var oldestSeen time.Time
	haveOldest := false
	for key, entry := range d.entries {
		if !haveOldest || entry.lastSeen.Before(oldestSeen) {
			oldestKey = key
			oldestSeen = entry.lastSeen
			haveOldest = true
		}
	}
	if haveOldest {
		delete(d.entries, oldestKey)
	}
}
