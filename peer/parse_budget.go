package peer

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

const peerParseScratchBytes int64 = 8 << 20

// frameParseBudget belongs to the manager, shared by all candidate and
// established readers. Waiting readers retain only their bounded raw line; no
// field array or typed member slice is allocated until its lease is acquired.
// The semaphore has at most one waiter per owned session (128 pending +64
// established), and cancellation/deadlines remove waiters immediately.
type frameParseBudget struct {
	space *semaphore.Weighted
	used  atomic.Int64
	peak  atomic.Int64
}

func newFrameParseBudget() *frameParseBudget {
	return &frameParseBudget{space: semaphore.NewWeighted(peerParseScratchBytes)}
}

type frameParseLease struct {
	budget *frameParseBudget
	bytes  int64
}

func (b *frameParseBudget) acquire(ctx context.Context, deadline time.Time, line string) (frameParseLease, error) {
	charge, err := frameParseCharge(line)
	if err != nil {
		return frameParseLease{}, err
	}
	return b.acquireCharge(ctx, deadline, charge)
}

func (b *frameParseBudget) acquireCharge(ctx context.Context, deadline time.Time, charge int64) (frameParseLease, error) {
	if b == nil || ctx == nil {
		return frameParseLease{}, errors.New("peer: parse budget or session context unavailable")
	}
	if err := ctx.Err(); err != nil {
		return frameParseLease{}, err
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return frameParseLease{}, context.DeadlineExceeded
	}
	if charge <= 0 || charge > peerParseScratchBytes {
		return frameParseLease{}, errors.New("peer: invalid scratch reservation")
	}
	if !b.space.TryAcquire(charge) {
		if !deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		if err := b.space.Acquire(ctx, charge); err != nil {
			return frameParseLease{}, err
		}
	}
	used := b.used.Add(charge)
	for previous := b.peak.Load(); used > previous; previous = b.peak.Load() {
		if b.peak.CompareAndSwap(previous, used) {
			break
		}
	}
	return frameParseLease{budget: b, bytes: charge}, nil
}

// release is called once by the scoped handler, after every synchronous parser
// consumer is finished. Queued work must own a separately charged raw copy; it
// must never retain Frame.Fields or PC92Record.Members past this boundary.
func (l frameParseLease) release() {
	l.budget.used.Add(-l.bytes)
	l.budget.space.Release(l.bytes)
}

func (b *frameParseBudget) usage() (used, peak int64) {
	return b.used.Load(), b.peak.Load()
}

// Charge before Split using conservative 64-bit allocation envelopes: four
// string-header arrays, eight wire-sized metadata/encoding copies, and 192
// bytes per typed PC92 entry (entry, colon split, alignment). Authority-frame
// pre-split checks guarantee their smaller field bounds even for caret floods.
// Generic frame grammar is unchanged. Charges include allocator headroom.
func frameParseCharge(line string) (int64, error) {
	if len(line) > MaxPeerFrameBytes {
		return 0, errors.New("peer: frame exceeds parse envelope")
	}
	fields := strings.Count(line, "^") + 1
	var members int
	header := strings.TrimSpace(line)
	if len(header) >= 5 && (strings.EqualFold(header[:5], "PC11^") || strings.EqualFold(header[:5], "PC61^") || strings.EqualFold(header[:5], "PC26^")) {
		commentBytes, tokens := spotCommentShape(header[5:])
		// The comment parser owns exact-size token/consumed/output arrays and
		// streams scanner matches. Include invalid-call diagnostic parsing and
		// callback normalization: those consumers are mutually exclusive with
		// successful spot parsing. Persistent application logging is not leased.
		// F<=L-C+1 and2T<=C+1 bound this at7,929,984B for a64KiB frame,
		// leaving room for one288KiB reader batch in the same8MiB pool.
		return int64(65536 + 64*fields + 32*len(line) + 24*commentBytes + 128*tokens), nil
	}
	if len(header) >= 5 && strings.EqualFold(header[:5], "PC92^") {
		if fields > 8198 {
			fields = 8198
		}
		members = fields
		if members > 8192 {
			members = 8192
		}
	} else if len(header) >= 5 && strings.EqualFold(header[:5], "PC93^") {
		if fields > 11 {
			fields = 11
		}
	}
	charge := int64(4096 + fields*64 + len(line)*8 + members*192)
	if charge > peerParseScratchBytes {
		return 0, errors.New("peer: frame exceeds parse scratch budget")
	}
	return charge, nil
}

// Locate payload field4 without allocating any field headers. Match the
// comment tokenizer's ASCII space/tab boundaries; Unicode whitespace remains
// inside a token until the ordinary parser handles it. Malformed short frames
// still receive a conservative charge without introducing a grammar rule.
func spotCommentShape(payload string) (bytes, tokens int) {
	for range 4 {
		i := strings.IndexByte(payload, '^')
		if i < 0 {
			return 0, 0
		}
		payload = payload[i+1:]
	}
	if i := strings.IndexByte(payload, '^'); i >= 0 {
		payload = payload[:i]
	}
	inside := false
	for i := range payload {
		separator := payload[i] == ' ' || payload[i] == '\t'
		if !separator && !inside {
			tokens++
		}
		inside = !separator
	}
	return len(payload), tokens
}

// Each call is a scope: its defer releases before the reader's next iteration,
// including parser rejection, handshake completion, or handler failure. Waiting
// for scratch shares the original read/phase deadline; traffic cannot extend it.
func (s *session) withParsedFrame(line string, deadline time.Time, handle func(*Frame) (bool, error)) (bool, error) {
	lease, err := s.manager.parseBudget.acquire(s.ctx, deadline, line)
	if err != nil {
		return false, err
	}
	defer lease.release()
	frame, err := ParseFrame(line)
	if err != nil {
		return false, nil
	}
	return handle(frame)
}
