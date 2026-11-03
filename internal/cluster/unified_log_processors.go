package cluster

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"time"
	"unsafe"

	"dxcluster/internal/asynclog"
	"dxcluster/internal/logfiles"
)

const fixedLogDedupeKeys = 512

type fixedLogEntry struct {
	key                [asynclog.MaxRecordBytes]byte
	length             uint16
	nextEmit, lastSeen time.Time
	suppressed         uint64
}

// fixedLogTable is owned by one destination worker. A candidate does not
// change an entry until the physical write succeeds; suppression is committed
// immediately. Keys and candidate state have fixed backing across churn.
type fixedLogTable struct {
	entries       [fixedLogDedupeKeys]fixedLogEntry
	used          int
	window        time.Duration
	pending       bool
	pendingIndex  int
	pendingKey    [asynclog.MaxRecordBytes]byte
	pendingLength uint16
	pendingAt     time.Time
}

// The reservation includes the fixed table, processor, allocation rounding,
// and bounded formatting/parsing workspace for one 2 KiB record.
func logProcessorBacking() uint64 {
	const page = uint64(8192)
	size := uint64(unsafe.Sizeof(fixedLogTable{})) + 64*1024
	return (size + page - 1) &^ (page - 1)
}

// locate uses oldest last-seen followed by lexicographic key order, so ties
// are deterministic, including the empty key.
func (d *fixedLogTable) locate(key []byte) (int, bool) {
	oldest := 0
	for i := 0; i < d.used; i++ {
		e := &d.entries[i]
		if int(e.length) == len(key) && bytes.Equal(e.key[:e.length], key) {
			return i, true
		}
		first := &d.entries[oldest]
		if e.lastSeen.Before(first.lastSeen) || (e.lastSeen.Equal(first.lastSeen) && bytes.Compare(e.key[:e.length], first.key[:first.length]) < 0) {
			oldest = i
		}
	}
	if d.used < len(d.entries) {
		return d.used, false
	}
	return oldest, false
}

func (d *fixedLogTable) suppress(index int, at time.Time) bool {
	e := &d.entries[index]
	if !at.Before(e.nextEmit) {
		return false
	}
	e.lastSeen = at
	if e.suppressed < math.MaxUint64 {
		e.suppressed++
	}
	return true
}

func (d *fixedLogTable) stage(index int, key []byte, at time.Time) {
	d.pending = true
	d.pendingIndex = index
	d.pendingLength = uint16(copy(d.pendingKey[:], key))
	d.pendingAt = at
}

func (d *fixedLogTable) complete(written bool) {
	if !d.pending {
		return
	}
	if written {
		e := &d.entries[d.pendingIndex]
		*e = fixedLogEntry{length: d.pendingLength, nextEmit: d.pendingAt.Add(d.window), lastSeen: d.pendingAt}
		copy(e.key[:], d.pendingKey[:d.pendingLength])
		if d.pendingIndex == d.used {
			d.used++
		}
	}
	d.pending = false
	clear(d.pendingKey[:d.pendingLength])
	d.pendingLength = 0
}

type eventLogProcessor struct{ table fixedLogTable }

func newEventLogProcessor(window time.Duration) *eventLogProcessor {
	return &eventLogProcessor{table: fixedLogTable{window: window}}
}

func (p *eventLogProcessor) Prepare(at time.Time, input []byte, output *[asynclog.MaxRecordBytes]byte) (int, logfiles.ProcessDecision) {
	d := &p.table
	if len(input) > len(output) || d.pending {
		return 0, logfiles.ProcessDrop
	}
	if d.window <= 0 {
		return copy(output[:], input), logfiles.ProcessWrite
	}
	at = at.UTC()
	index, found := d.locate(input)
	if found && d.suppress(index, at) {
		return 0, logfiles.ProcessSuppress
	}
	line := string(input)
	if found && d.entries[index].suppressed > 0 {
		n := d.entries[index].suppressed
		line = fmt.Sprintf("%s suppressed=%d window=%s", line, n, d.window)
		if n == math.MaxUint64 {
			line += " suppressed_count_is_lower_bound=true"
		}
	}
	if len(line) > len(output) {
		// Keep the original admissible record when the optional suppression
		// summary would exceed its limit. A successful write resets the window.
		line = string(input)
	}
	d.stage(index, input, at)
	return copy(output[:], line), logfiles.ProcessWrite
}

func (p *eventLogProcessor) Complete(_ []byte, written bool) { p.table.complete(written) }

type droppedLogProcessor struct{ table fixedLogTable }

func newDroppedLogProcessor(window time.Duration) *droppedLogProcessor {
	return &droppedLogProcessor{table: fixedLogTable{window: window}}
}

func (p *droppedLogProcessor) Prepare(at time.Time, input []byte, output *[asynclog.MaxRecordBytes]byte) (int, logfiles.ProcessDecision) {
	d := &p.table
	if len(input) > len(output) || d.pending {
		return 0, logfiles.ProcessDrop
	}
	if d.window <= 0 {
		return copy(output[:], input), logfiles.ProcessWrite
	}
	event, ok := parseDroppedLogRecord(string(input))
	if !ok {
		return 0, logfiles.ProcessDrop
	}
	key := []byte(droppedCallLogDedupeKey(event))
	if len(key) > asynclog.MaxRecordBytes {
		return 0, logfiles.ProcessDrop
	}
	at = at.UTC()
	index, found := d.locate(key)
	if found && d.suppress(index, at) {
		return 0, logfiles.ProcessSuppress
	}
	if found && d.entries[index].suppressed > 0 {
		n := d.entries[index].suppressed
		summary := fmt.Sprintf("suppressed=%d_window=%s", n, d.window)
		if n == math.MaxUint64 {
			summary += ",suppressed_count_is_lower_bound=true"
		}
		if event.Detail == "" || event.Detail == "none" {
			event.Detail = summary
		} else {
			event.Detail += "," + summary
		}
	}
	line := event.Line()
	if len(line) > len(output) {
		// The summary is disposable; do not keep dropping this valid record on
		// every later retry merely because the summary does not fit.
		line = string(input)
	}
	d.stage(index, key, at)
	return copy(output[:], line), logfiles.ProcessWrite
}

func (p *droppedLogProcessor) Complete(_ []byte, written bool) { p.table.complete(written) }

func parseDroppedLogRecord(line string) (droppedCallLogEvent, bool) {
	var e droppedCallLogEvent
	var ok bool
	if line, ok = strings.CutPrefix(line, "source="); !ok {
		return e, false
	}
	fields := []*string{&e.Source, &e.Role, &e.Reason, &e.Call, &e.DE, &e.DX, &e.Mode}
	separators := [...]string{" role=", " reason=", " call=", " de=", " dx=", " mode=", " detail="}
	for i, sep := range separators {
		*fields[i], line, ok = strings.Cut(line, sep)
		if !ok {
			return droppedCallLogEvent{}, false
		}
	}
	e.Detail = line
	return e, true
}
