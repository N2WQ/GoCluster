package logfiles

import (
	"bytes"
	"dxcluster/internal/asynclog"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestDeduperExactKeysWindowSummaryAndOldestEviction(t *testing.T) {
	d := &deduper{window: time.Minute}
	var scratch [asynclog.MaxRecordBytes]byte
	at := time.Unix(1000, 0)
	line := []byte("event=peer_connection action=connected")
	n, decision := d.Prepare(at, line, &scratch)
	if decision != ProcessWrite || !bytes.Equal(scratch[:n], line) {
		t.Fatal("first event")
	}
	d.Complete(line, true)
	if _, decision = d.Prepare(at.Add(59*time.Second), line, &scratch); decision != ProcessSuppress {
		t.Fatal("duplicate emitted")
	}
	n, decision = d.Prepare(at.Add(time.Minute), line, &scratch)
	if decision != ProcessWrite || string(scratch[:n]) != "event=peer_connection action=connected suppressed=1 window=1m0s" {
		t.Fatalf("summary %q", scratch[:n])
	}
	d.Complete(line, true)
	for i := 0; i < dedupeSlots; i++ {
		item := []byte(fmt.Sprintf("unique=%03d", i))
		d.Prepare(at.Add(time.Duration(i+100)*time.Second), item, &scratch)
		d.Complete(item, true)
	}
	if d.used != dedupeSlots {
		t.Fatalf("keys=%d", d.used)
	}
	if _, decision = d.Prepare(at.Add(700*time.Second), line, &scratch); decision != ProcessWrite {
		t.Fatal("evicted key was suppressed")
	}
	d.Complete(line, true)
}
func TestDeduperPreservesSuppressionAfterFailedWrite(t *testing.T) {
	d := &deduper{window: time.Second}
	var scratch [asynclog.MaxRecordBytes]byte
	line := bytes.Repeat([]byte{'x'}, asynclog.MaxRecordBytes)
	at := time.Unix(100, 0)
	if _, decision := d.Prepare(at, line, &scratch); decision != ProcessWrite {
		t.Fatal("first event refused")
	}
	d.Complete(line, true)
	if _, decision := d.Prepare(at, line, &scratch); decision != ProcessSuppress {
		t.Fatal("repeat not suppressed")
	}
	if n, decision := d.Prepare(at.Add(time.Second), line, &scratch); decision != ProcessWrite || !bytes.Equal(scratch[:n], line) {
		t.Fatal("original record was not preserved when summary did not fit")
	}
	d.Complete(line, false)
	if d.entries[0].suppressed != 1 {
		t.Fatal("failed write lost suppressed count")
	}
	if n, decision := d.Prepare(at.Add(time.Second), line, &scratch); decision != ProcessWrite || !bytes.Equal(scratch[:n], line) {
		t.Fatal("valid record could not be retried")
	}
	d.Complete(line, true)
	other := []byte("other")
	if _, decision := d.Prepare(at.Add(time.Second), other, &scratch); decision != ProcessWrite {
		t.Fatal("other refused")
	}
	d.Complete(other, true)
	if _, decision := d.Prepare(at.Add(time.Second), other, &scratch); decision != ProcessSuppress {
		t.Fatal("other repeat not suppressed")
	}
	n, decision := d.Prepare(at.Add(2*time.Second), other, &scratch)
	if decision != ProcessWrite {
		t.Fatal("other summary refused")
	}
	want := string(scratch[:n])
	d.Complete(other, false)
	n, decision = d.Prepare(at.Add(3*time.Second), other, &scratch)
	if decision != ProcessWrite || string(scratch[:n]) != want {
		t.Fatal("failed write erased suppression")
	}
	d.Complete(other, true)
	d.window = 0
	if n, decision = d.Prepare(at, line, &scratch); decision != ProcessWrite || !bytes.Equal(scratch[:n], line) {
		t.Fatal("zero window changed message")
	}
	d.Complete(line, true)
}

func TestDeduperSaturationIsLowerBoundAndHonorsPayloadCap(t *testing.T) {
	d := &deduper{window: time.Second}
	var scratch [asynclog.MaxRecordBytes]byte
	at := time.Unix(100, 0)
	input := []byte("x")
	if _, decision := d.Prepare(at, input, &scratch); decision != ProcessWrite {
		t.Fatal("first refused")
	}
	d.Complete(input, true)
	d.entries[0].suppressed = math.MaxUint64
	if _, decision := d.Prepare(at, input, &scratch); decision != ProcessSuppress {
		t.Fatal("repeat not suppressed")
	}
	n, decision := d.Prepare(at.Add(time.Second), input, &scratch)
	if decision != ProcessWrite || string(scratch[:n]) != "x suppressed>=18446744073709551615 window=1s" {
		t.Fatalf("saturated summary %q", scratch[:n])
	}
	d.Complete(input, false)
	if !d.entries[0].saturated || d.entries[0].suppressed != math.MaxUint64 {
		t.Fatal("failed write lost lower bound")
	}
	d.entries[0].length = asynclog.MaxRecordBytes
	for i := range d.entries[0].key {
		d.entries[0].key[i] = 'x'
	}
	long := bytes.Repeat([]byte{'x'}, asynclog.MaxRecordBytes)
	if n, decision = d.Prepare(at.Add(time.Second), long, &scratch); decision != ProcessWrite || !bytes.Equal(scratch[:n], long) {
		t.Fatal("valid record was lost when saturated summary did not fit")
	}
	d.Complete(long, false)
	if !d.entries[0].saturated {
		t.Fatal("overlong summary lost lower bound")
	}
}

func TestDeduperEqualTimestampEvictsFirstOccupiedKey(t *testing.T) {
	d := &deduper{window: time.Hour}
	var scratch [asynclog.MaxRecordBytes]byte
	at := time.Unix(100, 0)
	for i := 0; i < dedupeSlots; i++ {
		line := []byte(fmt.Sprintf("key=%03d", i))
		if _, decision := d.Prepare(at, line, &scratch); decision != ProcessWrite {
			t.Fatalf("insert %d", i)
		}
		d.Complete(line, true)
	}
	replacement := []byte("key=512")
	if _, decision := d.Prepare(at, replacement, &scratch); decision != ProcessWrite {
		t.Fatal("replacement refused")
	}
	d.Complete(replacement, true)
	if _, decision := d.Prepare(at, []byte("key=001"), &scratch); decision != ProcessSuppress {
		t.Fatal("second key was evicted")
	}
	first := []byte("key=000")
	if _, decision := d.Prepare(at, first, &scratch); decision != ProcessWrite {
		t.Fatal("first key was not evicted")
	}
	d.Complete(first, true)
}
