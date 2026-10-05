package telnet

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestYAMLWatchdogCleanupIgnoresBlockedConnectionReporter(t *testing.T) {
	const frame = "---\nnoise_class: URBAN\n...\nRESUME\nPAUSE 1\n"
	c, conn := newYAMLTestClient(frame)
	reportEntered, releaseReport := make(chan struct{}), make(chan struct{})
	var releaseReportOnce sync.Once
	t.Cleanup(func() { releaseReportOnce.Do(func() { close(releaseReport) }) })
	c.server = &Server{connectionReporter: func(event ConnectionEvent) {
		if event.Action == "disconnect" {
			close(reportEntered)
			<-releaseReport
		}
	}}
	c.callsign = "W1ABC-1"
	c.startReadPause(time.Unix(1700000000, 0), 60*time.Second)
	hooks, timer := fixedYAMLTestHooks()
	startTime := hooks.now()
	var receiveNow atomic.Int64
	receiveNow.Store(startTime.UnixNano())
	hooks.now = func() time.Time { return time.Unix(0, receiveNow.Load()) }
	deadline := startTime.Add(30 * time.Second)
	timer.stopCalled = make(chan struct{}, 1)
	registered, releaseReception := make(chan struct{}), make(chan struct{})
	var releaseReceptionOnce sync.Once
	t.Cleanup(func() { releaseReceptionOnce.Do(func() { close(releaseReception) }) })
	originalAfter := hooks.afterFunc
	hooks.afterFunc = func(duration time.Duration, callback func()) yamlWatchdog {
		watchdog := originalAfter(duration, callback)
		close(registered)
		<-releaseReception
		return watchdog
	}
	result := make(chan struct {
		body []byte
		err  error
	}, 1)
	go func() {
		body, err := c.receiveYAMLBodyWithHooks(deadline, hooks)
		result <- struct {
			body []byte
			err  error
		}{body, err}
	}()
	awaitYAMLTestSignal(t, registered, "watchdog registration before body reads")
	closeDone := make(chan struct{})
	go func() { c.close("deadline lifecycle test"); close(closeDone) }()
	awaitYAMLTestSignal(t, reportEntered, "blocked optional reporter")
	select {
	case <-c.done:
	default:
		t.Fatal("reporter started before mandatory done closure")
	}
	if !conn.closed.Load() {
		t.Fatal("reporter started before mandatory socket closure")
	}
	callbackDone := make(chan struct{})
	receiveNow.Store(deadline.UnixNano())
	go func() { timer.fire(); close(callbackDone) }()
	// The callback must finish before optional reporting is released. Otherwise
	// disarm's join would trap reception behind unrelated reporting work.
	awaitYAMLTestSignal(t, callbackDone, "watchdog callback independent of reporter")
	releaseReceptionOnce.Do(func() { close(releaseReception) })
	awaitYAMLTestSignal(t, timer.stopCalled, "watchdog disarm/join")
	select {
	case received := <-result:
		if received.body != nil || !errors.Is(received.err, errYAMLUploadDeadline) {
			t.Fatal("expired upload returned an applicable body or lost its deadline error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mandatory upload cleanup waited for optional reporting")
	}
	select {
	case <-closeDone:
		t.Fatal("optional reporter unexpectedly completed before release")
	default:
	}
	if !timer.hasFired() || timer.fire() {
		t.Fatal("reception retained a runnable watchdog after cleanup")
	}
	c.readPauseMu.Lock()
	pauseClosed, pauseDeadline := c.readPauseClosed, c.readPauseUntilUnixNano.Load()
	c.readPauseMu.Unlock()
	if conn.input.(*strings.Reader).Len() != len(frame) || pauseDeadline != 1700000060000000000 || !pauseClosed {
		t.Fatal("command-like payload was consumed or altered the preserved pause after terminal interruption")
	}
	releaseReportOnce.Do(func() { close(releaseReport) })
	awaitYAMLTestSignal(t, closeDone, "optional reporter cleanup")
}
