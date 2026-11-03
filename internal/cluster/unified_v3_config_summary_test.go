package cluster

import (
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/asynclog"
)

// Saturate the actual OS stdout as well as the shared console sink. A former
// direct fmt.Printf path would block on the former even if a fake sink test
// happened to pass. The separate common file owner receives the summary.
func TestV6HeadlessConfigurationSummaryDoesNotBlockOnStdout(t *testing.T) {
	t.Chdir(t.TempDir())
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = writer
	filled := make(chan struct{})
	go func() { _, _ = writer.Write(make([]byte, 1<<20)); close(filled) }()
	select {
	case <-filled:
		os.Stdout = stdout
		reader.Close()
		writer.Close()
		t.Fatal("test stdout pipe unexpectedly accepted all bytes")
	case <-time.After(50 * time.Millisecond):
	}
	oldLog, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	defer func() {
		os.Stdout = stdout
		_ = reader.Close()
		_ = writer.Close()
		<-filled
		log.SetOutput(oldLog)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	}()
	blocked := &stalledConsoleWriter{entered: make(chan struct{}), release: make(chan struct{})}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	l.useStdout(blocked)
	capture := &summaryCaptureWriter{lines: make(chan string, 256)}
	file, err := asynclog.StartSingle(30, func() asynclog.Sink {
		return &v6ConsoleSink{writer: capture}
	})
	if err != nil {
		close(blocked.release)
		l.close(time.Now().Add(time.Second))
		t.Fatal(err)
	}
	l.fanout.setFile(v6Admission{handle: file})
	log.SetOutput(&sharedProcessLogRouter)
	log.SetFlags(0)
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
		file.Stop()
		l.close(time.Now().Add(2 * time.Second))
		<-file.Done()
	}()
	cfg := &config.Config{}
	cfg.Server.Name = "summary-proof"
	cfg.Server.NodeID = "N0CALL-1"
	r := newClusterRuntime(BuildInfo{}, cfg, "", config.LoadDiagnostics{})
	r.v6Logging = l
	done := make(chan struct{})
	go func() { r.initializeCallCacheAndMeta(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		// Unblock any old direct stdout path before reporting failure.
		_ = reader.Close()
		<-done
		t.Fatal("headless configuration summary blocked on OS stdout")
	}
	if r.metaCache == nil {
		t.Fatal("call metadata initialization did not complete")
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("summary did not use the common console owner")
	}
	select {
	case line := <-capture.lines:
		if !strings.Contains(line, "Server: summary-proof (N0CALL-1)") {
			t.Fatalf("wrong common-file summary: %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("summary did not reach the separate common file owner")
	}
	close(blocked.release)
	released = true
}
