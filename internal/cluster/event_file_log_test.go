package cluster

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func TestEventFileLoggerWritesSeparateDailyFiles(t *testing.T) {
	dir := t.TempDir()
	logger, err := newEventFileLogger(config.LoggingConfig{
		LoginAttempts:     eventCfg(filepath.Join(dir, "login")),
		ReputationDrops:   eventCfg(filepath.Join(dir, "reputation")),
		TelnetConnections: eventCfg(filepath.Join(dir, "telnet")),
		IngestConnections: eventCfg(filepath.Join(dir, "ingest")),
		PeerConnections:   eventCfg(filepath.Join(dir, "peer")),
	})
	if err != nil {
		t.Fatalf("newEventFileLogger() error: %v", err)
	}
	logger.LogLoginAttempt(eventLogField{key: "event", value: "login_attempt"}, eventLogField{key: "reason", value: "cty_unknown"}, eventLogField{key: "call", value: "ZZ9ABC"})
	logger.LogReputationDrop(eventLogField{key: "event", value: "reputation_drop"}, eventLogField{key: "call", value: "K1ABC"}, eventLogField{key: "reason", value: "probation"})
	logger.LogTelnetConnection(eventLogField{key: "event", value: "telnet_connection"}, eventLogField{key: "action", value: "connect"}, eventLogField{key: "ip", value: "203.0.113.1"})
	logger.LogIngestConnection(eventLogField{key: "event", value: "ingest_connection"}, eventLogField{key: "source", value: "RBN"}, eventLogField{key: "action", value: "connected"})
	logger.LogPeerConnection(eventLogField{key: "event", value: "peer_connection"}, eventLogField{key: "peer", value: "N0PEER-1"}, eventLogField{key: "action", value: "established"})
	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	assertEventLogLine(t, filepath.Join(dir, "login"), "event=login_attempt reason=cty_unknown call=ZZ9ABC")
	assertEventLogLine(t, filepath.Join(dir, "reputation"), "event=reputation_drop call=K1ABC reason=probation")
	assertEventLogLine(t, filepath.Join(dir, "telnet"), "event=telnet_connection action=connect ip=203.0.113.1")
	assertEventLogLine(t, filepath.Join(dir, "ingest"), "event=ingest_connection source=RBN action=connected")
	assertEventLogLine(t, filepath.Join(dir, "peer"), "event=peer_connection peer=N0PEER-1 action=established")
}

func TestEventLogDeduperBoundsKeys(t *testing.T) {
	d := newEventLogDeduper(time.Minute, 2)
	if d == nil {
		t.Fatalf("expected deduper")
	}
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return now }
	for _, line := range []string{"a=1", "a=2", "a=3"} {
		if _, ok := d.Process(line); !ok {
			t.Fatalf("first line %q should emit", line)
		}
	}
	if len(d.entries) != 2 {
		t.Fatalf("expected capped entries=2, got %d", len(d.entries))
	}
}

func TestEventLogSanitizesAndTruncatesValues(t *testing.T) {
	long := strings.Repeat("A", 300)
	line := formatEventLogLine(
		eventLogField{key: "Bad-Key!", value: "two words\r\n"},
		eventLogField{key: "long", value: long},
	)
	if strings.Contains(line, "\r") || strings.Contains(line, "\n") || strings.Contains(line, "two words") {
		t.Fatalf("line was not sanitized: %q", line)
	}
	if !strings.Contains(line, "badkey=two_words") {
		t.Fatalf("expected sanitized key/value, got %q", line)
	}
	if len(strings.TrimPrefix(strings.Split(line, " ")[1], "long=")) != 256 {
		t.Fatalf("expected long value truncated to 256 bytes, got %q", line)
	}
}

func eventCfg(dir string) config.EventFileLoggingConfig {
	return config.EventFileLoggingConfig{
		Enabled:             true,
		Dir:                 dir,
		RetentionDays:       1,
		DedupeWindowSeconds: 0,
	}
}

func assertEventLogLine(t *testing.T, dir, wantBody string) {
	t.Helper()
	path := filepath.Join(dir, activeLogFileNameForDir(dir))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	line := strings.TrimSpace(string(data))
	pattern := regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ` + regexp.QuoteMeta(wantBody) + `$`)
	if !pattern.MatchString(line) {
		t.Fatalf("unexpected line in %s:\ngot  %q\nwant timestamp + %q", path, line, wantBody)
	}
}
