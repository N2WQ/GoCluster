//go:build qualification

package peer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

// Qualification evidence is bounded independently of run duration. Each spot
// slot is reconciled before reuse; losing an observation therefore fails rather
// than disappearing from a growing or truncated history. Runtime Q1-Q4 retain
// responsibility for the separate local-client latency and allocation verdict.
type retryMixedSpot struct {
	id             uint64
	at             time.Time
	ingest         uint8
	required, seen uint64
	generation     [64]uint64
}

type retryMixedMessage struct {
	id      uint64
	at      time.Time
	private bool
	seen    bool
}

type retryMixedRecipient struct {
	call       string
	generation uint64
	started    time.Time
	active     bool
	stable     bool
	faultAt    time.Time
}

type retryMixedQualification struct {
	mu                                     sync.Mutex
	m                                      *Manager
	input                                  chan *spot.Spot
	spots                                  [4096]retryMixedSpot
	messages                               [256]retryMixedMessage
	bulletins                              [128]retryMixedMessage
	recipients                             [64]retryMixedRecipient
	err                                    error
	newSpots, duplicates, pc93, wwv        uint64
	ingested, announcements, private, wwvs uint64
	mandatory, delivered, boundary, absent uint64
	maxLateness                            time.Duration
	completed                              bool
	pc92Actions                            [4]uint64 // Actual parsed A, D, C, K offers.
	minCBytes, maxCBytes                   int
}

// Install before Manager.Start. The buffered ingest sink is the ordinary
// Manager ingestion boundary; all inputs below use its normal HandleFrame path.
func newRetryMixedQualification(m *Manager, calls []string) *retryMixedQualification {
	q := &retryMixedQualification{m: m, input: make(chan *spot.Spot, 4096)}
	for i, call := range calls {
		q.recipients[i].call = call
	}
	m.ingest = q.input
	m.SetAnnouncementBroadcast(func(line string) { q.observeMessage(line, false, "") })
	m.SetDirectMessage(func(call, line string) { q.observeMessage(line, true, call) })
	m.SetWWVBroadcast(func(_, line string) { q.observeBulletin(line) })
	return q
}

func (q *retryMixedQualification) failLocked(format string, args ...any) {
	if q.err == nil {
		q.err = fmt.Errorf(format, args...)
	}
}

func retryMixedToken(line, prefix string) (uint64, bool) {
	at := strings.Index(line, prefix)
	if at < 0 || len(line) < at+len(prefix)+9 {
		return 0, false
	}
	id, err := strconv.ParseUint(line[at+len(prefix):at+len(prefix)+9], 10, 64)
	return id, err == nil && id > 0
}

func (q *retryMixedQualification) recipientLocked(call string) int {
	for i := range q.recipients {
		if q.recipients[i].call == call {
			return i
		}
	}
	return -1
}

func (q *retryMixedQualification) BeginRecipient(call string, generation uint64, at time.Time) {
	q.beginRecipient(call, generation, at, false)
}

// Released peers remain mandatory throughout the sustained recovery tail. They
// acquire no implicit planned-fault boundary merely by surviving one second.
func (q *retryMixedQualification) BeginStableRecipient(call string, generation uint64, at time.Time) {
	q.beginRecipient(call, generation, at, true)
}

func (q *retryMixedQualification) beginRecipient(call string, generation uint64, at time.Time, stable bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	i := q.recipientLocked(call)
	if i < 0 || generation == 0 {
		q.failLocked("unknown mixed recipient %s generation%d", call, generation)
		return
	}
	r := &q.recipients[i]
	if r.active || generation <= r.generation {
		q.failLocked("overlapping/stale recipient %s generation%d", call, generation)
		return
	}
	r.generation, r.started, r.active = generation, at, true
	r.stable, r.faultAt = stable, time.Time{}
}

// Declare a future fault before waiting its one-second drain margin. Existing
// mandatory observations stay immutable, including those not yet delivered.
func (q *retryMixedQualification) PlanRecipientFault(call string, generation uint64) (time.Time, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	i := q.recipientLocked(call)
	if i < 0 {
		return time.Time{}, fmt.Errorf("unknown planned-fault recipient %s", call)
	}
	r := &q.recipients[i]
	if !r.active || !r.stable || r.generation != generation || !r.faultAt.IsZero() {
		return time.Time{}, fmt.Errorf("invalid/stale stable-recipient fault for %s generation%d", call, generation)
	}
	r.faultAt = time.Now()
	return r.faultAt, q.err
}

// The first second is an irrevocable delivery obligation. The second second
// provides drain margin before the explicit fault, without removing earlier
// obligations. A later disconnect never turns missing data into a success.
func (q *retryMixedQualification) EndRecipient(call string, generation uint64, at time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	i := q.recipientLocked(call)
	if i < 0 {
		return fmt.Errorf("unknown mixed recipient %s", call)
	}
	r := &q.recipients[i]
	early := at.Sub(r.started) < 2*time.Second
	if r.stable {
		early = r.faultAt.IsZero() || at.Before(r.faultAt.Add(time.Second))
	}
	if !r.active || r.generation != generation || early {
		return fmt.Errorf("premature/stale planned fault for %s generation%d", call, generation)
	}
	bit := uint64(1) << uint(i)
	for slot := range q.spots {
		record := &q.spots[slot]
		if record.required&bit != 0 && record.generation[i] == generation && record.seen&bit == 0 {
			q.failLocked("missing spot%d at planned fault for %s generation%d", record.id, call, generation)
		}
	}
	r.active = false
	return q.err
}

func (q *retryMixedQualification) ObserveWire(call string, generation uint64, line string, at time.Time) {
	id, ok := retryMixedToken(line, "V14S")
	if !ok {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	i := q.recipientLocked(call)
	if i < 0 {
		q.failLocked("spot%d to unknown recipient %s", id, call)
		return
	}
	r := &q.spots[id%uint64(len(q.spots))]
	if r.id != id {
		q.failLocked("unknown/retired output spot%d to %s", id, call)
		return
	}
	bit := uint64(1) << uint(i)
	if r.seen&bit != 0 {
		q.failLocked("duplicate output spot%d to %s", id, call)
		return
	}
	if r.required&bit != 0 {
		if r.generation[i] != generation || at.Before(r.at) || at.Sub(r.at) > time.Second {
			q.failLocked("late/wrong-generation output spot%d to %s generation%d", id, call, generation)
			return
		}
		q.delivered++
	}
	r.seen |= bit
}

func (q *retryMixedQualification) observeIngest(s *spot.Spot) {
	id, ok := retryMixedToken(s.Comment, "V14S")
	q.mu.Lock()
	defer q.mu.Unlock()
	if !ok {
		q.failLocked("ingested spot lost stable identifier")
		return
	}
	r := &q.spots[id%uint64(len(q.spots))]
	if r.id != id || r.ingest >= 11 || time.Since(r.at) > time.Second {
		q.failLocked("unknown/duplicate/late ingest spot%d", id)
		return
	}
	r.ingest++
	q.ingested++
}

func (q *retryMixedQualification) observeMessage(line string, private bool, recipient string) {
	id, ok := retryMixedToken(line, "V14M")
	q.mu.Lock()
	defer q.mu.Unlock()
	if !ok {
		q.failLocked("PC93 output lost stable identifier")
		return
	}
	r := &q.messages[id%uint64(len(q.messages))]
	if r.id != id || r.seen || r.private != private || time.Since(r.at) > time.Second {
		q.failLocked("unknown/duplicate/misrouted/late PC93%d", id)
		return
	}
	if private && recipient != qualificationCall("L0", int((id-1)/2)%100) {
		q.failLocked("misrouted private PC93%d to %s", id, recipient)
		return
	}
	r.seen = true
	if private {
		q.private++
	} else {
		q.announcements++
	}
}

func (q *retryMixedQualification) observeBulletin(line string) {
	id, ok := retryMixedToken(line, "V14W")
	q.mu.Lock()
	defer q.mu.Unlock()
	if !ok {
		q.failLocked("bulletin output lost stable identifier")
		return
	}
	r := &q.bulletins[id%uint64(len(q.bulletins))]
	if r.id != id || r.seen || time.Since(r.at) > time.Second {
		q.failLocked("unknown/duplicate/late bulletin%d", id)
		return
	}
	r.seen = true
	q.wwvs++
}

func retryMixedSpotWire(index uint64, now time.Time) string {
	kind := "PC61"
	if index%10 >= 4 {
		kind = "PC11"
	}
	if index%10 >= 8 {
		kind = "PC26"
	}
	line := fmt.Sprintf("%s^14074.0^%s^%s^%s^V14S%09d FT8^DL1AAA^DL1PAA^", kind,
		qualificationCall("D1", int(index)), now.UTC().Format("02-Jan-2006"), now.UTC().Format("1504Z"), index+1)
	switch kind {
	case "PC61":
		line += "192.0.2.1^"
	case "PC26":
		line += "^"
	}
	return line + "H10^"
}

func (q *retryMixedQualification) admitSpot(live *session, index uint64, at time.Time) error {
	q.m.mu.RLock()
	q.mu.Lock()
	id := index + 1
	r := &q.spots[id%uint64(len(q.spots))]
	if r.id != 0 && (r.ingest != 11 || r.seen&r.required != r.required) {
		q.failLocked("unreconciled spot%d at bounded evidence reuse", r.id)
	}
	*r = retryMixedSpot{id: id, at: at}
	for i, recipient := range q.recipients {
		s := q.m.sessions.Value(recipient.call)
		if s == nil || s == live || s.ctx.Err() != nil {
			q.absent++
			continue
		}
		deadline := recipient.started.Add(time.Second)
		if recipient.stable {
			deadline = recipient.faultAt
		}
		if recipient.active && !at.Before(recipient.started) && (deadline.IsZero() || at.Before(deadline)) {
			r.required |= uint64(1) << uint(i)
			r.generation[i] = recipient.generation
			q.mandatory++
		} else {
			q.boundary++
		}
	}
	q.newSpots++
	q.duplicates += 10
	err := q.err
	q.mu.Unlock()
	q.m.mu.RUnlock()
	return err
}

func (q *retryMixedQualification) admitMessage(index uint64, at time.Time, bulletin bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	id := index + 1
	r := &q.messages[id%uint64(len(q.messages))]
	if bulletin {
		r = &q.bulletins[id%uint64(len(q.bulletins))]
	}
	if r.id != 0 && !r.seen {
		q.failLocked("unreconciled message%d at bounded evidence reuse", r.id)
	}
	*r = retryMixedMessage{id: id, at: at, private: !bulletin && index%2 != 0}
	if bulletin {
		q.wwv++
	} else {
		q.pc93++
	}
	return q.err
}

// Read the fields already produced by ParseFrame. This evidence must not add a
// second 8,000-entry decode to each large C on the qualification input path.
func (q *retryMixedQualification) observePC92Offer(frame *Frame, wire string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	fields := frame.payloadFields()
	if frame.Type != "PC92" || len(fields) < 4 {
		return fmt.Errorf("mixed PC92 offer is malformed")
	}
	var action int
	switch fields[2] {
	case "A":
		action = 0
	case "D":
		action = 1
	case "C":
		action = 2
		// Literal fixture envelope: PC92 + six-byte origin + timestamp +
		// C + explicit 5origin:5457:633 subject + 8,000 eight-byte members
		// + H1. The timestamp is one-to-eight bytes across daily rollover
		// and fractional sequence values; no CRLF is part of ParseFrame.
		wantBytes := 64035 + len(fields[1])
		if len(fields)-4 != 8000 || len(fields[1]) < 1 || len(fields[1]) > 8 || len(wire) != wantBytes || len(wire) > MaxPeerFrameBytes {
			return fmt.Errorf("mixed C workload changed: members=%d bytes=%d want=8000/%d", len(fields)-4, len(wire), wantBytes)
		}
		if q.minCBytes == 0 || len(wire) < q.minCBytes {
			q.minCBytes = len(wire)
		}
		q.maxCBytes = max(q.maxCBytes, len(wire))
	case "K":
		action = 3
	default:
		return fmt.Errorf("unexpected mixed PC92 action %q", fields[2])
	}
	q.pc92Actions[action]++
	return nil
}

func (q *retryMixedQualification) verifyPC92MixLocked() error {
	total := q.pc92Actions[0] + q.pc92Actions[1] + q.pc92Actions[2] + q.pc92Actions[3]
	if total == 0 || total%100 != 0 {
		return fmt.Errorf("mixed PC92 workload lacks complete 100-record cycles: %d", total)
	}
	for i, perCycle := range [4]uint64{45, 45, 2, 8} {
		if q.pc92Actions[i] != total/100*perCycle {
			return fmt.Errorf("mixed PC92 action totals differ from 45/45/2/8: %v", q.pc92Actions)
		}
	}
	return nil
}

// Run uses absolute schedules, never ticker delivery count. It replaces the
// PC92-only producer so PC93 is not accidentally offered twice. Every spot has
// ten exact duplicates, preserving 10k new keys and 100k duplicate arrivals/minute.
func (q *retryMixedQualification) Run(ctx context.Context, live *session, topology *QualificationTopology, start time.Time, duration time.Duration, offered *atomic.Int64) error {
	consumeCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case s := <-q.input:
				q.observeIngest(s)
			case <-consumeCtx.Done():
				return
			}
		}
	}()
	defer func() { cancel(); <-done }()
	intervals := [4]time.Duration{6 * time.Millisecond, 10 * time.Millisecond, 600 * time.Millisecond, 3 * time.Second}
	var indexes [4]uint64
	var small, large, message TimestampGenerator
	pairs := 0
	for {
		class := 0
		for i := 1; i < len(intervals); i++ {
			if time.Duration(indexes[i])*intervals[i] < time.Duration(indexes[class])*intervals[class] {
				class = i
			}
		}
		offset := time.Duration(indexes[class]) * intervals[class]
		if offset >= duration {
			break
		}
		target := start.Add(offset)
		if err := qualificationWait(ctx, max(0, time.Until(target))); err != nil {
			return err
		}
		at := time.Now()
		if !at.Before(start.Add(duration)) || at.Sub(target) > time.Second {
			return fmt.Errorf("mixed producer missed class%d schedule by%s", class, at.Sub(target))
		}
		q.mu.Lock()
		q.maxLateness = max(q.maxLateness, at.Sub(target))
		err := q.err
		q.mu.Unlock()
		if err != nil {
			return err
		}
		index := indexes[class]
		var wire string
		switch class {
		case 0:
			if err := q.admitSpot(live, index, at); err != nil {
				return err
			}
			wire = retryMixedSpotWire(index, at)
		case 1:
			node, action, entries, generator := 1, "D", topology.members(1)[:1], &small
			switch index % 100 {
			case 0, 50:
				node, action, entries, generator = 0, "C", topology.members(0), &large
			case 1, 2, 3, 4, 5, 6, 7, 8:
				action, entries = "K", nil
			default:
				if pairs%2 != 0 {
					action = "A"
				}
				pairs++
			}
			stamp, err := generator.NextAt(at)
			if err != nil {
				return err
			}
			wire = qualificationFrame(qualificationCall("N0", node), stamp, action, entries, 1)
		case 2:
			if err := q.admitMessage(index, at, false); err != nil {
				return err
			}
			stamp, err := message.NextAt(at)
			if err != nil {
				return err
			}
			target := "ALL"
			if index%2 != 0 {
				target = qualificationCall("L0", int(index/2)%100)
			}
			wire = fmt.Sprintf("PC93^M0AAAA^%s^%s^DL1AAA^*^V14M%09d^H1^", stamp, target, index+1)
		case 3:
			if err := q.admitMessage(index, at, true); err != nil {
				return err
			}
			wire = fmt.Sprintf("PC23^%s^%02d^100^5^2^V14W%09d^DL1AAA^DL1PAA^H2^", at.UTC().Format("02-Jan-2006"), at.UTC().Hour(), index+1)
		}
		frame, err := ParseFrame(wire)
		if err != nil {
			return err
		}
		if class == 1 {
			if err := q.observePC92Offer(frame, wire); err != nil {
				return err
			}
			offered.Add(1)
		}
		var key string
		if class == 0 {
			parsed, err := parseSpotFromFrame(frame, live.remoteCall)
			if err != nil {
				return err
			}
			key = dxKey(frame, parsed)
		}
		q.m.HandleFrame(frame, live)
		if class == 0 {
			admitted, ok := q.spotAdmission(key)
			if !ok || admitted.Before(at) {
				return fmt.Errorf("new distinct spot%d was not admitted for peer forwarding", index+1)
			}
			for range 10 {
				q.m.HandleFrame(frame, live)
			}
			if retained, ok := q.spotAdmission(key); !ok || !retained.Equal(admitted) {
				return fmt.Errorf("duplicates renewed or removed original spot%d admission", index+1)
			}
		}
		if live.ctx.Err() != nil {
			return fmt.Errorf("healthy mixed source closed during class%d: %w", class, live.ctx.Err())
		}
		indexes[class]++
	}
	for i, interval := range intervals {
		want := uint64((duration + interval - 1) / interval)
		if indexes[i] != want {
			return fmt.Errorf("mixed class%d underproduced: got%d want%d", i, indexes[i], want)
		}
	}
	if err := qualificationWait(ctx, max(0, time.Until(start.Add(duration)))); err != nil {
		return err
	}
	_, err := qualificationAwaitUntil(ctx, time.Now().Add(time.Second), time.Millisecond, func(context.Context) (bool, error) {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.ingested == 11*q.newSpots && q.announcements+q.private == q.pc93 && q.wwvs == q.wwv && q.delivered == q.mandatory, q.err
	}, func(ready bool) bool { return ready })
	q.mu.Lock()
	q.completed = err == nil
	q.mu.Unlock()
	return err
}

// Inspect without firstAdmission/contains: those production helpers can prune
// expired entries. An evidence reader must never perform the cleanup being
// qualified, nor renew the actual admission age of a duplicate.
func (q *retryMixedQualification) spotAdmission(key string) (time.Time, bool) {
	c := q.m.dedupe
	c.mu.Lock()
	defer c.mu.Unlock()
	offset, ok := c.items.Get(key)
	return c.epoch.Add(time.Duration(offset)), ok
}

func (q *retryMixedQualification) Verify() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	if err := q.verifyPC92MixLocked(); err != nil {
		return err
	}
	if !q.completed || q.newSpots == 0 || q.ingested != 11*q.newSpots || q.duplicates != 10*q.newSpots || q.announcements != (q.pc93+1)/2 || q.private != q.pc93/2 || q.wwvs != q.wwv || q.mandatory == 0 || q.delivered != q.mandatory {
		return fmt.Errorf("mixed mandatory outputs did not reconcile: complete=%v new=%d duplicate=%d ingest=%d PC93=%d announce=%d private=%d WWV=%d/%d peers=%d/%d", q.completed, q.newSpots, q.duplicates, q.ingested, q.pc93, q.announcements, q.private, q.wwvs, q.wwv, q.delivered, q.mandatory)
	}
	return nil
}

func (q *retryMixedQualification) Summary() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return fmt.Sprintf("mixed new=%d duplicates=%d ingest=%d announce=%d private=%d bulletin=%d mandatory_peer=%d delivered_peer=%d planned_boundary=%d absent_or_source=%d max_lateness=%s PC92_A_D_C_K=%v C_bytes=%d..%d C_members=8000", q.newSpots, q.duplicates, q.ingested, q.announcements, q.private, q.wwvs, q.mandatory, q.delivered, q.boundary, q.absent, q.maxLateness, q.pc92Actions, q.minCBytes, q.maxCBytes)
}

func TestPC92V14MixedWireMix(t *testing.T) {
	counts := make(map[string]int)
	keys := make(map[string]bool)
	for i := range 100 {
		frame, err := ParseFrame(retryMixedSpotWire(uint64(i), time.Now()))
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parseSpotFromFrame(frame, "P0AAAA")
		if err != nil {
			t.Fatal(err)
		}
		id, ok := retryMixedToken(parsed.Comment, "V14S")
		if !ok || id != uint64(i+1) {
			t.Fatalf("spot comment changed stable ID: %q", parsed.Comment)
		}
		key := dxKey(frame, parsed)
		if keys[key] {
			t.Fatal("distinct fixture spot repeated a forwarding key")
		}
		keys[key] = true
		counts[frame.Type]++
	}
	if counts["PC61"] != 40 || counts["PC11"] != 40 || counts["PC26"] != 20 {
		t.Fatalf("unexpected input mix: %v", counts)
	}
}

func TestPC92V14MixedOracleRejectsInvalidDelivery(t *testing.T) {
	for _, scenario := range []string{"valid", "missing", "duplicate", "late", "wrong_generation", "premature_fault", "unknown"} {
		t.Run(scenario, func(t *testing.T) {
			q := &retryMixedQualification{}
			q.recipients[0].call = "P0AAAA"
			at := time.Now()
			q.BeginRecipient("P0AAAA", 7, at)
			q.spots[1] = retryMixedSpot{id: 1, at: at, required: 1, generation: [64]uint64{7}}
			generation, observed := uint64(7), at.Add(10*time.Millisecond)
			wire := retryMixedSpotWire(0, at)
			switch scenario {
			case "late":
				observed = at.Add(time.Second + time.Nanosecond)
			case "wrong_generation":
				generation++
			case "unknown":
				wire = retryMixedSpotWire(1, at)
			}
			if scenario != "missing" {
				q.ObserveWire("P0AAAA", generation, wire, observed)
				if scenario == "duplicate" {
					q.ObserveWire("P0AAAA", generation, wire, observed)
				}
			}
			fault := at.Add(2 * time.Second)
			if scenario == "premature_fault" {
				fault = at.Add(time.Second)
			}
			err := q.EndRecipient("P0AAAA", 7, fault)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("%s delivery oracle: %v", scenario, err)
			}
		})
	}
}

func TestPC92V14MixedRecipientWindowBoundaries(t *testing.T) {
	started := time.Now()
	for _, tc := range []struct {
		name string
		at   time.Time
		want uint64
	}{
		{"before registration", started.Add(-time.Nanosecond), 0},
		{"registration", started, 1},
		{"last eligible instant", started.Add(time.Second - time.Nanosecond), 1},
		{"planned fault boundary", started.Add(time.Second), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Manager{sessions: newFixedIndex[string, *session](64)}
			m.sessions.Set("P0AAAA", &session{ctx: context.Background()})
			q := &retryMixedQualification{m: m}
			q.recipients[0].call = "P0AAAA"
			q.BeginRecipient("P0AAAA", 1, started)
			if err := q.admitSpot(nil, 0, tc.at); err != nil {
				t.Fatal(err)
			}
			if q.spots[1].required != tc.want {
				t.Fatalf("input at %s: required mask=%d want=%d", tc.at, q.spots[1].required, tc.want)
			}
		})
	}
}

func TestPC92V14MixedPrivateRecipient(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		q := &retryMixedQualification{}
		if err := q.admitMessage(1, time.Now(), false); err != nil {
			t.Fatal(err)
		}
		recipient := qualificationCall("L0", 0)
		if wrong {
			recipient = qualificationCall("L0", 1)
		}
		q.observeMessage("V14M000000002", true, recipient)
		if (q.err != nil) != wrong || (q.private == 1) == wrong {
			t.Fatalf("wrong=%v: private=%d err=%v", wrong, q.private, q.err)
		}
	}
}

func TestPC92V14MixedStableRecipient(t *testing.T) {
	for _, scenario := range []string{"valid", "late", "missing", "early_end", "unplanned_end", "stale_generation", "wrong_phase", "repeated_plan"} {
		t.Run(scenario, func(t *testing.T) {
			m := &Manager{sessions: newFixedIndex[string, *session](64)}
			m.sessions.Set("P0AAAA", &session{ctx: context.Background()})
			q := &retryMixedQualification{m: m}
			q.recipients[0].call = "P0AAAA"
			started := time.Now().Add(-5 * time.Second)
			if scenario == "wrong_phase" {
				q.BeginRecipient("P0AAAA", 1, started)
				if _, err := q.PlanRecipientFault("P0AAAA", 1); err == nil {
					t.Fatal("ordinary two-second refusal acquired stable fault authority")
				}
				return
			}
			q.BeginStableRecipient("P0AAAA", 1, started)
			at := time.Now()
			if err := q.admitSpot(nil, 0, at); err != nil {
				t.Fatal(err)
			}
			if q.spots[1].required != 1 {
				t.Fatal("stable recipient lost delivery obligation after first second")
			}
			if scenario == "unplanned_end" {
				if err := q.EndRecipient("P0AAAA", 1, at.Add(2*time.Second)); err == nil {
					t.Fatal("stable recipient ended without declared future fault")
				}
				return
			}
			if scenario == "stale_generation" {
				if _, err := q.PlanRecipientFault("P0AAAA", 2); err == nil {
					t.Fatal("wrong generation planned a stable fault")
				}
				return
			}
			cutoff, err := q.PlanRecipientFault("P0AAAA", 1)
			if err != nil {
				t.Fatal(err)
			}
			if q.spots[1].required != 1 {
				t.Fatal("fault plan waived an existing obligation")
			}
			if scenario == "repeated_plan" {
				if _, err := q.PlanRecipientFault("P0AAAA", 1); err == nil {
					t.Fatal("repeated plan moved stable fault boundary")
				}
				return
			}
			if err := q.admitSpot(nil, 1, cutoff); err != nil {
				t.Fatal(err)
			}
			if q.spots[2].required != 0 {
				t.Fatal("declared fault boundary acquired a new obligation")
			}
			if scenario != "missing" {
				observed := at.Add(time.Millisecond)
				if scenario == "late" {
					observed = at.Add(time.Second + time.Nanosecond)
				}
				q.ObserveWire("P0AAAA", 1, retryMixedSpotWire(0, at), observed)
			}
			end := cutoff.Add(time.Second)
			if scenario == "early_end" {
				end = end.Add(-time.Nanosecond)
			}
			err = q.EndRecipient("P0AAAA", 1, end)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("%s: %v", scenario, err)
			}
		})
	}
}

func TestPC92V14MixedPC92Workload(t *testing.T) {
	largeC := func(stamp string, members int) string {
		var wire strings.Builder
		fmt.Fprintf(&wire, "PC92^N0AAAA^%s^C^5N0AAAA:5457:633^", stamp)
		for i := range members {
			fmt.Fprintf(&wire, "1%s^", qualificationCall("U0", i))
		}
		wire.WriteString("H1^")
		return wire.String()
	}
	for _, scenario := range []string{"valid", "wrong_mix", "undersized_C", "short_wire", "narrow_timestamp", "wide_timestamp"} {
		t.Run(scenario, func(t *testing.T) {
			q := &retryMixedQualification{}
			stamp := "43200"
			switch scenario {
			case "narrow_timestamp":
				stamp = "0"
			case "wide_timestamp":
				stamp = "86399.99"
			}
			var observationError error
			for action, count := range map[string]int{"A": 45, "D": 45, "C": 2, "K": 8} {
				for i := range count {
					actualAction := action
					if scenario == "wrong_mix" && action == "A" && i == 0 {
						actualAction = "D"
					}
					wire := "PC92^N0AAAA^" + stamp + "^" + actualAction + "^5N0AAAA:5457:633^1U0AAAA^H1^"
					if action == "K" {
						wire = "PC92^N0AAAA^" + stamp + "^K^5N0AAAA:5457:633^0^0^H1^"
					}
					if action == "C" {
						members := 8000
						if scenario == "undersized_C" {
							members--
						}
						wire = largeC(stamp, members)
						if scenario == "short_wire" {
							wire = strings.Replace(wire, ":5457:", ":54:", 1)
						}
					}
					frame, err := ParseFrame(wire)
					if err != nil {
						t.Fatal(err)
					}
					if err := q.observePC92Offer(frame, wire); err != nil {
						observationError = err
					}
				}
			}
			mixError := q.verifyPC92MixLocked()
			if scenario == "undersized_C" || scenario == "short_wire" {
				if observationError == nil {
					t.Fatal("undersized C passed actual offered workload check")
				}
			} else if observationError != nil || (mixError != nil) != (scenario == "wrong_mix") {
				t.Fatalf("%s: observation=%v mixture=%v", scenario, observationError, mixError)
			}
		})
	}
}
