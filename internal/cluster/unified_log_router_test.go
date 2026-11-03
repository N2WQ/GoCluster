package cluster

import (
	"log"
	"testing"
	"time"
)

type parkedRouterAdmission struct {
	entered chan struct{}
	release chan struct{}
}

func (a *parkedRouterAdmission) emit(time.Time, []byte) {
	close(a.entered)
	<-a.release
}

func (*parkedRouterAdmission) drop() {}

func TestProcessLogRouterRetainsBorrowedFanoutUntilWriterReturns(t *testing.T) {
	var router processLogRouter
	target := &systemLogFanout{}
	parked := &parkedRouterAdmission{entered: make(chan struct{}), release: make(chan struct{})}
	target.setConsole(parked)
	if !router.install(target) {
		t.Fatal("could not install target")
	}
	finished := make(chan struct{})
	go func() { _, _ = router.Write([]byte("held\n")); close(finished) }()
	select {
	case <-parked.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter admission")
	}
	done := router.clear(target)
	if done == nil {
		t.Fatal("missing retirement witness")
	}
	select {
	case <-done:
		t.Fatal("router retired a borrowed target")
	default:
	}
	if router.install(&systemLogFanout{}) {
		t.Fatal("replacement overlapped a borrowed target")
	}
	close(parked.release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("writer did not return")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retirement did not follow the writer")
	}
	if !router.install(&systemLogFanout{}) {
		t.Fatal("retired router refused replacement")
	}
}

func TestProcessLogRouterFollowsCurrentRuntime(t *testing.T) {
	var router processLogRouter
	firstTarget, secondTarget := &systemLogFanout{}, &systemLogFanout{}
	first, second := &fanoutTestAdmission{}, &fanoutTestAdmission{}
	firstTarget.setConsole(first)
	secondTarget.setConsole(second)
	if !router.install(firstTarget) || router.install(secondTarget) {
		t.Fatal("router accepted overlapping runtime targets")
	}
	// A package may capture this writer once, before the runtime is replaced.
	logger := log.New(&router, "", 0)
	logger.Print("first")
	router.clear(firstTarget)
	if !router.install(secondTarget) {
		t.Fatal("router did not accept a retired runtime's replacement")
	}
	router.clear(firstTarget) // A stale close cannot detach the new target.
	logger.Print("second")
	if len(first.records) != 1 || first.records[0] != "first" || len(second.records) != 1 || second.records[0] != "second" {
		t.Fatalf("captured writer retained old target: first=%v second=%v", first.records, second.records)
	}
	router.clear(secondTarget)
	if router.target != nil {
		t.Fatal("router retained the retired runtime")
	}
}
