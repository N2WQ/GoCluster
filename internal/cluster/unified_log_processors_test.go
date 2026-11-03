package cluster

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"dxcluster/internal/asynclog"
	"dxcluster/internal/logfiles"
)

func TestEventLogProcessorPreservesDedupe(t *testing.T) {
	p := newEventLogProcessor(time.Minute)
	var output [asynclog.MaxRecordBytes]byte
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	input := []byte("event=login peer=N0CALL")
	for _, tt := range []struct {
		at       time.Time
		decision logfiles.ProcessDecision
		want     string
	}{
		{at, logfiles.ProcessWrite, "event=login peer=N0CALL"},
		{at.Add(time.Second), logfiles.ProcessSuppress, ""},
		{at.Add(time.Minute), logfiles.ProcessWrite, "event=login peer=N0CALL suppressed=1 window=1m0s"},
	} {
		n, decision := p.Prepare(tt.at, input, &output)
		if decision != tt.decision || string(output[:n]) != tt.want {
			t.Fatalf("decision %v, output %q; want %v, %q", decision, output[:n], tt.decision, tt.want)
		}
		if decision == logfiles.ProcessWrite {
			p.Complete(input, true)
		}
	}
}

func TestLogProcessorBackingAndHighChurnAreBounded(t *testing.T) {
	if uint64(unsafe.Sizeof(eventLogProcessor{}))+64*1024 > logProcessorBacking() || uint64(unsafe.Sizeof(droppedLogProcessor{}))+64*1024 > logProcessorBacking() {
		t.Fatal("fixed processor and workspace exceed reservation")
	}
	p := newEventLogProcessor(time.Minute)
	var output [asynclog.MaxRecordBytes]byte
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < fixedLogDedupeKeys*8; i++ {
		input := []byte(strconv.Itoa(i))
		if _, d := p.Prepare(at.Add(time.Duration(i)*time.Second), input, &output); d != logfiles.ProcessWrite {
			t.Fatalf("record %d: %v", i, d)
		}
		p.Complete(input, true)
	}
	if p.table.used != fixedLogDedupeKeys {
		t.Fatalf("retained keys = %d", p.table.used)
	}
	if _, found := p.table.locate([]byte("0")); found {
		t.Fatal("oldest key survived churn")
	}
	if _, found := p.table.locate([]byte(strconv.Itoa(fixedLogDedupeKeys*8 - 1))); !found {
		t.Fatal("latest key not retained")
	}
}

func TestLogProcessorsPreserveSuppressionAfterFailedWrite(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var output [asynclog.MaxRecordBytes]byte
	for _, tt := range []struct {
		name string
		p    logfiles.Processor
		line string
	}{
		{"event", newEventLogProcessor(time.Minute), "event=login peer=N0CALL"},
		{"dropped", newDroppedLogProcessor(time.Minute), "source=rbn role=DX reason=bad call=N0CALL de=AA1AA dx=N0CALL mode=CW detail=none"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			input := []byte(tt.line)
			if _, d := tt.p.Prepare(at, input, &output); d != logfiles.ProcessWrite {
				t.Fatal(d)
			}
			tt.p.Complete(input, true)
			if _, d := tt.p.Prepare(at.Add(time.Second), input, &output); d != logfiles.ProcessSuppress {
				t.Fatal(d)
			}
			n, d := tt.p.Prepare(at.Add(time.Minute), input, &output)
			if d != logfiles.ProcessWrite || !strings.Contains(string(output[:n]), "suppressed=1") {
				t.Fatalf("first summary: %v %q", d, output[:n])
			}
			tt.p.Complete(input, false)
			n, d = tt.p.Prepare(at.Add(time.Minute), input, &output)
			if d != logfiles.ProcessWrite || !strings.Contains(string(output[:n]), "suppressed=1") {
				t.Fatalf("lost summary after failed write: %v %q", d, output[:n])
			}
			tt.p.Complete(input, true)
		})
	}
}

func TestEventLogProcessorSaturatesAndEvictsEmptyKey(t *testing.T) {
	p := newEventLogProcessor(time.Minute)
	var output [asynclog.MaxRecordBytes]byte
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	input := []byte("event=login")
	if _, d := p.Prepare(at, input, &output); d != logfiles.ProcessWrite {
		t.Fatal(d)
	}
	p.Complete(input, true)
	index, found := p.table.locate(input)
	if !found {
		t.Fatal("entry missing")
	}
	p.table.entries[index].suppressed = math.MaxUint64
	if _, d := p.Prepare(at.Add(time.Second), input, &output); d != logfiles.ProcessSuppress {
		t.Fatal(d)
	}
	if got := p.table.entries[index].suppressed; got != math.MaxUint64 {
		t.Fatalf("suppression counter wrapped to %d", got)
	}
	if n, d := p.Prepare(at.Add(time.Minute), input, &output); d != logfiles.ProcessWrite || !strings.Contains(string(output[:n]), "suppressed_count_is_lower_bound=true") {
		t.Fatalf("saturated summary: %v %q", d, output[:n])
	}
	p.Complete(input, true)
	q := newEventLogProcessor(time.Minute)
	for i := 0; i < defaultEventLogDedupeMaxKeys+1; i++ {
		key := ""
		if i > 0 {
			key = strconv.Itoa(i)
		}
		data := []byte(key)
		if _, d := q.Prepare(at.Add(time.Duration(i)*time.Second), data, &output); d != logfiles.ProcessWrite {
			t.Fatal(d)
		}
		q.Complete(data, true)
	}
	if q.table.used != defaultEventLogDedupeMaxKeys {
		t.Fatalf("dedupe cardinality grew to %d", q.table.used)
	}
	if _, found := q.table.locate(nil); found {
		t.Fatal("oldest empty key was not evicted")
	}
}

func TestLogProcessorEvictionTieIsDeterministic(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var table fixedLogTable
	for i := range table.entries {
		key := []byte("z")
		if i == 3 {
			key = []byte("a")
		}
		table.entries[i].length = uint16(copy(table.entries[i].key[:], key))
		table.entries[i].lastSeen = at
	}
	table.used = len(table.entries)
	index, found := table.locate([]byte("unseen"))
	if found || string(table.entries[index].key[:table.entries[index].length]) != "a" {
		t.Fatalf("wrong tied eviction index: %d", index)
	}
}

func TestDroppedLogProcessorRetainsSaturatedSummary(t *testing.T) {
	p := newDroppedLogProcessor(time.Minute)
	var output [asynclog.MaxRecordBytes]byte
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prefix := "source=rbn role=DX reason=bad call=N0CALL de=AA1AA dx=N0CALL mode=CW detail="
	long := []byte(prefix + strings.Repeat("x", asynclog.MaxRecordBytes-len(prefix)))
	if _, d := p.Prepare(at, long, &output); d != logfiles.ProcessWrite {
		t.Fatal(d)
	}
	p.Complete(long, true)
	key := "rbn|DX|bad|N0CALL"
	index, found := p.table.locate([]byte(key))
	if !found {
		t.Fatal("entry missing")
	}
	p.table.entries[index].suppressed = math.MaxUint64
	if _, d := p.Prepare(at.Add(time.Second), long, &output); d != logfiles.ProcessSuppress {
		t.Fatal(d)
	}
	if n, d := p.Prepare(at.Add(time.Minute), long, &output); d != logfiles.ProcessWrite || string(output[:n]) != string(long) {
		t.Fatalf("oversized saturated summary decision %v output %q", d, output[:n])
	}
	p.Complete(long, false)
	if p.table.entries[index].suppressed != math.MaxUint64 {
		t.Fatal("oversized summary erased saturated suppression")
	}
	short := []byte(prefix + "none")
	for _, success := range []bool{false, true} {
		n, d := p.Prepare(at.Add(time.Minute), short, &output)
		if d != logfiles.ProcessWrite || !strings.Contains(string(output[:n]), "suppressed_count_is_lower_bound=true") {
			t.Fatalf("saturated retry: %v %q", d, output[:n])
		}
		p.Complete(short, success)
	}
}

func TestDroppedLogProcessorPreservesDedupeKeyAndDetail(t *testing.T) {
	p := newDroppedLogProcessor(time.Minute)
	var output [asynclog.MaxRecordBytes]byte
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first := []byte("source=rbn role=DX reason=bad call=N0CALL de=AA1AA dx=N0CALL mode=CW detail=none")
	second := []byte("source=rbn role=DX reason=bad call=N0CALL de=BB1BB dx=N0CALL mode=SSB detail=evidence")
	if n, decision := p.Prepare(at, first, &output); decision != logfiles.ProcessWrite || string(output[:n]) != string(first) {
		t.Fatalf("first decision %v output %q", decision, output[:n])
	}
	p.Complete(first, true)
	if _, decision := p.Prepare(at.Add(time.Second), second, &output); decision != logfiles.ProcessSuppress {
		t.Fatalf("same key with changed DE/mode: %v", decision)
	}
	n, decision := p.Prepare(at.Add(time.Minute), second, &output)
	want := string(second) + ",suppressed=1_window=1m0s"
	if decision != logfiles.ProcessWrite || string(output[:n]) != want {
		t.Fatalf("summary decision %v output %q; want %q", decision, output[:n], want)
	}
	p.Complete(second, true)
}

func TestEventLogProcessorWritesOriginalWhenSummaryDoesNotFit(t *testing.T) {
	p := newEventLogProcessor(time.Second)
	var output [asynclog.MaxRecordBytes]byte
	at := time.Now().UTC()
	input := []byte(strings.Repeat("x", asynclog.MaxRecordBytes))
	if _, d := p.Prepare(at, input, &output); d != logfiles.ProcessWrite {
		t.Fatal(d)
	}
	p.Complete(input, true)
	if _, d := p.Prepare(at.Add(time.Millisecond), input, &output); d != logfiles.ProcessSuppress {
		t.Fatal(d)
	}
	if n, d := p.Prepare(at.Add(time.Second), input, &output); d != logfiles.ProcessWrite || string(output[:n]) != string(input) {
		t.Fatalf("oversized summary decision %v output %q", d, output[:n])
	}
	p.Complete(input, true)
	index, found := p.table.locate(input)
	if !found {
		t.Fatal("entry missing")
	}
	if got := p.table.entries[index].suppressed; got != 0 {
		t.Fatalf("suppression not reset after successful write: %d", got)
	}
	if n, d := p.Prepare(at.Add(2*time.Second), input, &output); d != logfiles.ProcessWrite || string(output[:n]) != string(input) {
		t.Fatalf("later record decision %v output %q", d, output[:n])
	}
}
