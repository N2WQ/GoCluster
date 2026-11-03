package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/internal/asynclog"
)

type lineSinkTestEmitter struct{ records []string }

func (e *lineSinkTestEmitter) Emit(_ uint8, _ time.Time, p []byte) bool {
	e.records = append(e.records, string(p))
	return true
}

type refusingLineSinkEmitter struct{ calls int }

func (e *refusingLineSinkEmitter) Emit(_ uint8, _ time.Time, _ []byte) bool {
	e.calls++
	return false
}

func TestUnifiedLogLineSinkAdmissionAndClose(t *testing.T) {
	const stream = 31
	e := &lineSinkTestEmitter{}
	s := &unifiedLogLineSink{emitter: e, stream: stream}
	s.WriteLine("normal", time.Now())
	s.WriteLine(strings.Repeat("x", asynclog.MaxRecordBytes+1), time.Now())
	_ = s.Close()
	s.WriteLine("after close", time.Now())
	if len(e.records) != 1 || e.records[0] != "normal" {
		t.Fatalf("records=%v", e.records)
	}
	if s.emitter != nil {
		t.Fatal("closed line sink retained its coordinator")
	}
}

func TestUnifiedLogLineSinkDoesNotDoubleCountRefusal(t *testing.T) {
	const stream = 30
	emitter := &refusingLineSinkEmitter{}
	s := &unifiedLogLineSink{emitter: emitter, stream: stream}
	s.WriteLine("refused", time.Now())
	if emitter.calls != 1 {
		t.Fatalf("refused record sent %d times", emitter.calls)
	}
}
