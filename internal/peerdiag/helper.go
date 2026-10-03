package peerdiag

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"time"
)

// RunHelper serves one parent connection. It has no cluster imports, global
// workers, extensions, callback registry, or stdout/stderr capture. File work
// is synchronous here so the parent can retire one whole blocked generation.
func RunHelper(address, token string) (err error) {
	if len(token) != 64 || len(address) > 21 {
		return errors.New("invalid helper options")
	}
	endpoint, parseErr := netip.ParseAddrPort(address)
	if parseErr != nil || endpoint.Addr() != netip.AddrFrom4([4]byte{127, 0, 0, 1}) || endpoint.Port() == 0 {
		return errors.New("diagnostic helper requires literal parent loopback endpoint")
	}
	var auth [32]byte
	if _, err := hex.Decode(auth[:], []byte(token)); err != nil {
		return err
	}
	dialer := net.Dialer{Timeout: time.Second}
	connection, err := dialer.DialContext(context.Background(), "tcp4", address)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err = connection.SetDeadline(time.Now().Add(ipcTimeout)); err != nil {
		return err
	}
	if err = writeFull(connection, auth[:]); err != nil {
		return err
	}
	options, err := receiveOptions(connection)
	if err != nil {
		return err
	}
	if err = connection.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	sink := helperSink{options: options}
	defer func() {
		if closeErr := sink.close(); err == nil {
			err = closeErr
		}
	}()
	var wire [RecordBytes]byte
	var ack [ackBytes]byte
	for {
		if _, err = io.ReadFull(connection, wire[:]); err != nil {
			return err
		}
		event, ok := decodeEvent(&wire)
		if !ok {
			return errors.New("invalid diagnostic event")
		}
		outcome := sink.write(event)
		binary.LittleEndian.PutUint64(ack[:8], event.Sequence)
		binary.LittleEndian.PutUint32(ack[8:12], outcome)
		binary.LittleEndian.PutUint32(ack[12:16], uint32(sink.used))
		if err = connection.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		if err = writeFull(connection, ack[:]); err != nil {
			return err
		}
		if outcome == ackFailed {
			// A failed file write/close may leave uncertain native ownership.
			// End this entire generation so process retirement bounds that state.
			return errors.New("diagnostic sink failed")
		}
	}
}

type dedupeEntry struct {
	key                [RecordBytes]byte
	length             uint16
	nextEmit, lastSeen int64
	suppressed         uint64
}

// deduplicate preserves exact emitted-line identity and oldest-last-seen
// eviction. Its slots never grow; deliberate diagnostic eviction is separate
// from the protocol caches' no-eviction policy.
func (s *helperSink) deduplicate(line []byte, now time.Time) ([]byte, bool) {
	if s.options.DedupeWindow <= 0 {
		return line, true
	}
	oldest := 0
	for i := 0; i < s.used; i++ {
		entry := &s.entries[i]
		if entry.lastSeen < s.entries[oldest].lastSeen {
			oldest = i
		}
		if int(entry.length) == len(line) && equalBytes(entry.key[:entry.length], line) {
			entry.lastSeen = now.UnixNano()
			if now.UnixNano() < entry.nextEmit {
				entry.suppressed++
				return nil, false
			}
			suppressed := entry.suppressed
			entry.suppressed = 0
			entry.nextEmit = now.Add(s.options.DedupeWindow).UnixNano()
			if suppressed > 0 {
				copy(s.line[:], line)
				line = s.line[:len(line)]
				line = append(line, " suppressed="...)
				line = strconv.AppendUint(line, suppressed, 10)
				line = append(line, " window="...)
				line = append(line, s.options.DedupeWindow.String()...)
			}
			return line, true
		}
	}
	index := oldest
	if s.used < len(s.entries) {
		index = s.used
		s.used++
	}
	entry := &s.entries[index]
	*entry = dedupeEntry{length: uint16(len(line)), nextEmit: now.Add(s.options.DedupeWindow).UnixNano(), lastSeen: now.UnixNano()}
	copy(entry.key[:], line)
	return line, true
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
