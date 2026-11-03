package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/internal/asynclog"
)

type fanoutTestAdmission struct {
	records []string
	drops   int
}

func (s *fanoutTestAdmission) emit(_ time.Time, p []byte) { s.records = append(s.records, string(p)) }
func (s *fanoutTestAdmission) drop()                      { s.drops++ }

func TestSystemLogFanoutWholeMessageAndRoutes(t *testing.T) {
	console, file := &fanoutTestAdmission{}, &fanoutTestAdmission{}
	var f systemLogFanout
	f.setConsole(console)
	f.setFile(file)
	_, _ = f.Write([]byte("first\nsecond\n"))
	_, _ = f.WriteStream([]byte("partial"))
	_, _ = f.WriteStream([]byte(" line\n"))
	f.writeFileOnly("file-only", time.Now())
	for _, got := range []struct {
		name string
		sink *fanoutTestAdmission
		want string
	}{
		{"console", console, "first\nsecond|partial line"},
		{"file", file, "first\nsecond|partial line|file-only"},
	} {
		if joined := strings.Join(got.sink.records, "|"); joined != got.want {
			t.Errorf("%s got %q, want %q", got.name, joined, got.want)
		}
	}
}

func TestSystemLogFanoutCountsOversizeAndUnfinishedOnce(t *testing.T) {
	console, file := &fanoutTestAdmission{}, &fanoutTestAdmission{}
	var f systemLogFanout
	f.setConsole(console)
	f.setFile(file)
	_, _ = f.Write([]byte(strings.Repeat("x", asynclog.MaxRecordBytes) + "\ny\n"))
	_, _ = f.WriteStream([]byte("unfinished"))
	f.close()
	f.close()
	if f.console != nil || f.file != nil {
		t.Fatal("retired fanout retained destination references")
	}
	if console.drops != 2 || file.drops != 2 || len(console.records) != 0 || len(file.records) != 0 {
		t.Fatalf("unexpected delivery or loss counts: console=%+v file=%+v", console, file)
	}
}

func TestSystemLogFanoutBoundsOversizedRawWrite(t *testing.T) {
	file := &fanoutTestAdmission{}
	var f systemLogFanout
	f.setFile(file)
	input := strings.Repeat("x", asynclog.QueueSize*asynclog.MaxRecordBytes+1)
	_, _ = f.WriteStream([]byte(input))
	if file.drops != 1 {
		t.Fatalf("oversized raw write: drops=%d", file.drops)
	}
	// The next line is used for resynchronization and must not be spliced into
	// the discarded bytes. A later complete line is delivered independently.
	_, _ = f.WriteStream([]byte("unsafe\nsafe\n"))
	if len(file.records) != 1 || file.records[0] != "safe" {
		t.Fatalf("unexpected post-gap records: %v", file.records)
	}
}

type discardLogAdmission struct{}

func (discardLogAdmission) emit(time.Time, []byte) {}
func (discardLogAdmission) drop()                  {}

func BenchmarkSystemLogFanout(b *testing.B) {
	var f systemLogFanout
	f.setConsole(discardLogAdmission{})
	f.setFile(discardLogAdmission{})
	p := []byte("system log record\n")
	b.ReportAllocs()
	for b.Loop() {
		_, _ = f.Write(p)
	}
}
