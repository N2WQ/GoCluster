package peer

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"
)

const (
	peerQueueBytes       = 1 << 20
	peerControlMaxAge    = 5 * time.Second
	peerTelnetReplyBytes = 3
)

// Fixed channel storage remains reserved even while a lane is empty. The
// configured normal count is an upper bound; physical backing and payload share
// its existing 1 MiB quota. queueMu owns initialization after construction.
func (s *session) initializeOutputBudgetLocked() {
	if s.dataFixedBytes == 0 && s.writeCh != nil {
		s.dataFixedBytes = channelAllocationBytes(cap(s.writeCh), 16)
		s.dataBytes += s.dataFixedBytes
	}
	if s.controlFixedBytes == 0 {
		s.controlFixedBytes = channelAllocationBytes(cap(s.priorityLineCh), 16) + channelAllocationBytes(cap(s.priorityRawCh), 24)
		s.controlBytes += s.controlFixedBytes
	}
}

// Only a registry winner owns normal-lane backing. Pending candidates retain
// their small control lane; even an extreme configured normal count cannot
// multiply a 1 MiB channel array across the 128 pending handshakes.
func (s *session) activateNormalQueue() error {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.ctx != nil && s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	if s.normalReady == nil || s.writeCh != nil {
		return nil
	}
	s.writeCh = make(chan string, s.normalCapacity)
	s.initializeOutputBudgetLocked()
	// Closing publishes the channel exactly once to the writer. The reader
	// cannot retire this session while registration holds manager ownership.
	close(s.normalReady)
	return nil
}

// controlTimes is a fixed FIFO parallel to a control channel. queueMu owns it;
// removing an entry transfers its charge to the one shared active writer. The
// queued lane limits exclude that bounded write. One monotonic epoch and fixed
// elapsed offsets preserve each deadline without retaining128 time.Time values.
// Count, rather than a zero offset, marks an occupied slot. The epoch resets only
// after the last queued item leaves. No index grows with history.
type controlTimes struct {
	epoch   time.Time
	offsets [defaultPriorityQueue]int64
	head    int
	count   int
}

func (q *controlTimes) push(now time.Time) {
	if q.count == 0 {
		q.epoch = now
	}
	q.offsets[(q.head+q.count)%len(q.offsets)] = int64(now.Sub(q.epoch))
	q.count++
}

func (q *controlTimes) pop() {
	if q.count == 0 {
		return
	}
	q.offsets[q.head] = 0
	q.head = (q.head + 1) % len(q.offsets)
	q.count--
	if q.count == 0 {
		q.epoch = time.Time{}
	}
}

func (q *controlTimes) expired(now time.Time) bool {
	return q.count > 0 && now.Sub(q.epoch)-time.Duration(q.offsets[q.head]) > peerControlMaxAge
}

func (s *session) startWorker(fn func()) {
	s.workers.Add(1)
	go func() { defer s.workers.Done(); fn() }()
}

func (s *session) controlAgeLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			s.queueMu.Lock()
			expired := s.lineTimes.expired(now) || s.rawTimes.expired(now)
			s.queueMu.Unlock()
			if expired {
				log.Printf("Peering: control queue age exceeded for %s", s.diagnosticLabel)
				s.close()
				return
			}
		}
	}
}

// writerLoop is the sole owner of buffered socket writes. All PC92 records use
// priorityLineCh, preserving manager allocation/enqueue order on the wire.
func (s *session) writerLoop() {
	ready := s.normalReady
	var data chan string
	if ready == nil {
		data = s.writeCh // Existing internally constructed sessions are ready.
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		select {
		case raw := <-s.priorityRawCh:
			if !s.writeQueuedRaw(raw) {
				return
			}
			continue
		case line := <-s.priorityLineCh:
			if !s.writeQueuedLine(line, true) {
				return
			}
			continue
		default:
		}
		select {
		case <-s.ctx.Done():
			return
		case raw := <-s.priorityRawCh:
			if !s.writeQueuedRaw(raw) {
				return
			}
		case line := <-s.priorityLineCh:
			if !s.writeQueuedLine(line, true) {
				return
			}
		case <-ready:
			data, ready = s.writeCh, nil
		case line := <-data:
			if !s.writeQueuedLine(line, false) {
				return
			}
		}
	}
}

func (s *session) writeQueuedLine(line string, control bool) bool {
	s.queueMu.Lock()
	expired := false
	if control {
		expired = s.lineTimes.expired(time.Now())
		s.lineTimes.pop()
		s.controlBytes -= queuedLineBytes(line)
		s.controlCount--
	} else {
		s.dataBytes -= queuedLineBytes(line)
	}
	if !expired {
		s.activeBytes, s.activeControl = queuedLineBytes(line), control
	}
	s.queueMu.Unlock()
	if expired {
		s.close()
		return false
	}
	err := s.writeLine(line)
	s.finishActiveWrite()
	if err != nil {
		s.handleWriterError("line", err)
		return false
	}
	return true
}

func (s *session) writeQueuedRaw(raw []byte) bool {
	s.queueMu.Lock()
	expired := s.rawTimes.expired(time.Now())
	s.rawTimes.pop()
	s.controlBytes -= allocationBytes(len(raw))
	s.controlCount--
	if !expired {
		s.activeBytes, s.activeControl = allocationBytes(len(raw)), true
	}
	s.queueMu.Unlock()
	if expired {
		s.close()
		return false
	}
	err := s.writeRaw(raw)
	s.finishActiveWrite()
	if err != nil {
		s.handleWriterError("raw", err)
		return false
	}
	return true
}

func (s *session) finishActiveWrite() {
	s.queueMu.Lock()
	s.activeBytes, s.activeControl = 0, false
	s.queueMu.Unlock()
}

// discardQueuedOutput runs only after cancellation and joining every worker.
// Controller work may still retain the closed session pointer; remove queued
// payload references here so those older generations cannot retain full lanes.
func (s *session) discardQueuedOutput() {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	for {
		select {
		case <-s.writeCh:
		case <-s.priorityLineCh:
		case <-s.priorityRawCh:
		default:
			s.dataBytes, s.controlBytes, s.controlCount = 0, 0, 0
			s.dataFixedBytes, s.controlFixedBytes = 0, 0
			s.writeCh, s.priorityLineCh, s.priorityRawCh = nil, nil, nil
			s.activeBytes, s.activeControl = 0, false
			s.lineTimes, s.rawTimes = controlTimes{}, controlTimes{}
			if s.reader != nil {
				s.reader.release()
			}
			s.writer = nil
			return
		}
	}
}

// sendLine refuses data overload without disconnecting. PC92 callers cannot
// accidentally bypass the single ordered control lane.
func (s *session) sendLine(line string) error {
	if strings.HasPrefix(line, "PC92^") {
		return s.sendControlLine(line)
	}
	if s.ctx == nil {
		return errSessionContextUnset
	}
	if len(line) > MaxPeerFrameBytes {
		return errSessionWriteQueueFull
	}
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.initializeOutputBudgetLocked()
	charge := queuedLineBytes(line)
	if charge > peerQueueBytes-s.dataBytes {
		return errSessionWriteQueueFull
	}
	if len(s.writeCh) == cap(s.writeCh) {
		return errSessionWriteQueueFull
	}
	// queueMu serializes producers; consumers only make more room. Clone only
	// admitted records so a small slice cannot retain an uncharged large input.
	s.writeCh <- strings.Clone(line)
	s.dataBytes += charge
	return nil
}

// sendControlLine bounds queued records and bytes. The sole active write is
// separately capped at MaxPeerFrameBytes plus CRLF. Losing control output
// invalidates the session; close it instead of silently dropping state.
func (s *session) sendControlLine(line string) error {
	return s.enqueueControlLine(line, true)
}

// The initial password is already immutable configuration-owned storage.
// Borrow that exact field, avoiding another maximum-frame allocation for each
// competing outbound candidate. Logical queue bytes/counts remain unchanged.
// Untrusted protocol frames always enter through the cloning sendControlLine.
func (s *session) sendInitialPassword() error {
	return s.enqueueControlLineBefore(s.password, false, s.phaseDeadline)
}

func (s *session) sendHandshakeLine(line string) error {
	return s.enqueueControlLineBefore(line, true, s.phaseDeadline)
}

func (s *session) enqueueControlLine(line string, clone bool) error {
	return s.enqueueControlLineBefore(line, clone, time.Time{})
}

func (s *session) enqueueControlLineBefore(line string, clone bool, deadline time.Time) error {
	if s == nil || s.ctx == nil {
		return errSessionContextUnset
	}
	if len(line) > MaxPeerFrameBytes {
		s.close()
		return errSessionPriorityQueueFull
	}
	s.queueMu.Lock()
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		s.queueMu.Unlock()
		return context.DeadlineExceeded
	}
	if err := s.ctx.Err(); err != nil {
		s.queueMu.Unlock()
		return err
	}
	s.initializeOutputBudgetLocked()
	charge := queuedLineBytes(line)
	if charge <= peerQueueBytes-s.controlBytes && s.controlCount < defaultPriorityQueue {
		if clone {
			line = strings.Clone(line)
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			s.queueMu.Unlock()
			return context.DeadlineExceeded
		}
		select {
		case s.priorityLineCh <- line:
			s.controlBytes += charge
			s.controlCount++
			s.lineTimes.push(time.Now())
			s.queueMu.Unlock()
			return nil
		default:
		}
	}
	s.queueMu.Unlock()
	s.close()
	return errSessionPriorityQueueFull
}

func (s *session) sendPriorityRaw(data []byte) bool {
	if s == nil || s.ctx == nil || len(data) == 0 {
		return false
	}
	// Native Telnet only emits one three-byte refusal command per call.
	if len(data) > peerTelnetReplyBytes {
		s.close()
		return false
	}
	s.queueMu.Lock()
	if s.ctx.Err() != nil {
		s.queueMu.Unlock()
		return false
	}
	s.initializeOutputBudgetLocked()
	charge := allocationBytes(len(data))
	if charge <= peerQueueBytes-s.controlBytes && s.controlCount < defaultPriorityQueue {
		select {
		case s.priorityRawCh <- append([]byte(nil), data...):
			s.controlBytes += charge
			s.controlCount++
			s.rawTimes.push(time.Now())
			s.queueMu.Unlock()
			return true
		default:
		}
	}
	s.queueMu.Unlock()
	s.close()
	return false
}

func (s *session) writeLine(line string) error {
	if s.conn == nil {
		return errors.New("peer: nil conn")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	deadline := time.Now().Add(defaultPeerWriteDeadline)
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := s.qualificationWriter.wait(s.ctx, deadline); err != nil {
		return err
	}
	if _, err := s.writer.WriteString(line); err != nil {
		return err
	}
	if !strings.HasSuffix(line, "\n") {
		if _, err := s.writer.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return s.writer.Flush()
}

func (s *session) writeRaw(data []byte) error {
	if s.conn == nil {
		return errors.New("peer: nil conn")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	deadline := time.Now().Add(defaultPeerWriteDeadline)
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := s.qualificationWriter.wait(s.ctx, deadline); err != nil {
		return err
	}
	if _, err := s.writer.Write(data); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.conn != nil {
			_ = s.conn.Close()
		}
	})
}

func (s *session) handleWriterError(kind string, err error) {
	if err == nil || s.ctx != nil && s.ctx.Err() != nil {
		return
	}
	log.Printf("Peering: writer %s failed for %s: %v", kind, s.diagnosticLabel, err)
	s.close()
}
