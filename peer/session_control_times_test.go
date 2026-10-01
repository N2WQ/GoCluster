package peer

import (
	"testing"
	"time"
	"unsafe"
)

func TestControlTimesExactAgeFIFOAndEpochReset(t *testing.T) {
	now := time.Now()
	var q controlTimes
	q.push(now)
	q.push(now.Add(time.Second))
	if q.offsets[q.head] != 0 || q.expired(now.Add(5*time.Second)) || !q.expired(now.Add(5*time.Second+time.Nanosecond)) {
		t.Fatal("zero-offset first item or exact five-second boundary changed")
	}
	q.pop()
	if q.epoch != now || q.expired(now.Add(6*time.Second)) || !q.expired(now.Add(6*time.Second+time.Nanosecond)) {
		t.Fatal("pop changed the remaining FIFO deadline or reset the active epoch")
	}
	q.pop()
	if !q.epoch.IsZero() || q.expired(now.Add(time.Hour)) {
		t.Fatal("empty ring retained an epoch or an expired item")
	}
	// The next population may use an earlier time; no old epoch survives empty.
	next := now.Add(-time.Hour)
	q.push(next)
	if q.epoch != next || q.offsets[q.head] != 0 || q.expired(next.Add(5*time.Second)) {
		t.Fatal("new population inherited the previous epoch")
	}
	if unsafe.Sizeof(q) > 1100 {
		t.Fatalf("queue-age metadata no longer compact: %d bytes", unsafe.Sizeof(q))
	}
}

func TestControlTimesWrapRetainsMonotonicOffsets(t *testing.T) {
	now := time.Now()
	var q controlTimes
	for i := range defaultPriorityQueue {
		q.push(now.Add(time.Duration(i) * time.Millisecond))
	}
	for i := range defaultPriorityQueue * 3 {
		oldest := now.Add(time.Duration(i) * time.Millisecond)
		if q.expired(oldest.Add(peerControlMaxAge)) || !q.expired(oldest.Add(peerControlMaxAge+time.Nanosecond)) {
			t.Fatalf("wrapped FIFO deadline changed at iteration%d", i)
		}
		q.pop()
		q.push(now.Add(time.Duration(i+defaultPriorityQueue) * time.Millisecond))
	}
}
