//go:build qualification

package peer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func q6PublicationFault(t *testing.T, zero bool) {
	r := newQ6Rig(t, zero, 512, 0)
	var extras []*q6Local
	for index := 0; index < 24; index++ {
		extras = append(extras, r.login(fmt.Sprintf("K%dEX", index+2), fmt.Sprintf("127.0.0.%d", index+20)))
	}
	r.wait("publication gate", 5*time.Second, func(s QualificationState) bool { return s.PublicationGated && s.Established == 1 })
	r.closed(r.primary)
	r.closed(r.alternate)
	r.refused("GB7REF")
	r.legacyAlive()
	r.login("K1USER", "127.0.0.3")
	resume := time.Now()
	for _, local := range extras {
		_ = local.conn.Close()
	}
	r.wait("publication headroom and stable resume", 5*time.Second, func(s QualificationState) bool {
		return !s.PublicationGated && r.server.CurrentPeerMembership().RawCount == 1
	})
	if time.Since(resume) < time.Second {
		t.Fatal("publication gate resumed without one second of stable headroom")
	}
	r.recovered("GB7REF")
}

func q6ClockFault(t *testing.T, zero bool) {
	r := newQ6Rig(t, zero, 65536, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	start := time.Now()
	err := r.m.QualificationSetClockOffset(ctx, -time.Hour)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	r.wait("clock gate", 8*time.Second, func(s QualificationState) bool { return s.ClockGated && s.Established == 1 })
	if time.Since(start) < 5*time.Second {
		t.Fatal("clock gate ignored the publication-stall grace interval")
	}
	r.closed(r.primary)
	r.closed(r.alternate)
	r.refused("GB7REF")
	r.legacyAlive()
	r.login("K1USER", "127.0.0.3")
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	resume := time.Now()
	err = r.m.QualificationSetClockOffset(ctx, 0)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	r.wait("clock stable and UTC-advancing resume", 5*time.Second, func(s QualificationState) bool { return !s.ClockGated })
	if time.Since(resume) < time.Second {
		t.Fatal("clock gate resumed without one second of stable progress")
	}
	r.recovered("GB7REF")
}

func q6User(index int) string {
	var suffix [4]byte
	for position := len(suffix) - 1; position >= 0; position-- {
		suffix[position] = byte('A' + index%26)
		index /= 26
	}
	return "K1" + string(suffix[:])
}

func q6Record(t *testing.T, origin, action string, generator *TimestampGenerator, users []PC92Entry) string {
	t.Helper()
	stamp, err := generator.NextAt(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	wire, err := EncodePC92(&PC92Record{Origin: origin, Timestamp: stamp, Action: action,
		Subject: PC92Entry{Call: origin, Flags: 5, Version: "5457", Build: "633"}, Members: users, Hop: 10})
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func q6AdmissionFault(t *testing.T, zero bool) {
	r := newQ6Rig(t, zero, 65536, 0)
	var generators [16]TimestampGenerator
	for cohort := 0; cohort < len(generators); cohort++ {
		users := make([]PC92Entry, 4096)
		for index := range users {
			users[index] = PC92Entry{Call: q6User(cohort*4096 + index), Flags: 1}
		}
		r.primary.send(t, q6Record(t, fmt.Sprintf("N%dCAP", cohort), "C", &generators[cohort], users))
		r.wait("ordinary C user occupancy", 5*time.Second, func(s QualificationState) bool { return s.Users == (cohort+1)*4096 })
	}
	before := r.snapshot()
	refused := q6Record(t, "N0CAP", "A", &generators[0], []PC92Entry{{Call: q6User(65536), Flags: 1}})
	r.primary.send(t, refused)
	after := r.wait("authoritative admission closes and gates source", 5*time.Second, func(s QualificationState) bool {
		return s.BlockedPeers == 1 && s.Established == 2 && s.CompleteNodes == 0
	})
	if before.Users != after.Users || before.Nodes != after.Nodes || before.Freshness != after.Freshness || before.PC92Keys != after.PC92Keys {
		t.Fatalf("refused authoritative A changed graph/freshness/dedupe: before=%+v after=%+v", before, after)
	}
	r.closed(r.primary)
	r.refused("GB7REF")
	r.legacyAlive()
	r.login("K1USER", "127.0.0.3")
	r.alternate.send(t, q6Record(t, "N1CAP", "C", &generators[1], nil))
	r.wait("ordinary replacement C supplies admission headroom", 5*time.Second, func(s QualificationState) bool {
		return s.Users == 61440 && s.BlockedPeers == 0 && s.CompleteNodes == 1
	})
	// Equal cardinalities alone could hide an incorrectly advanced watermark.
	// Retry the exact refused timestamp/key after another origin frees capacity.
	r.alternate.send(t, refused)
	r.wait("refused timestamp and dedupe key remained admissible", 5*time.Second, func(s QualificationState) bool {
		return s.Users == 61441 && s.PC92Keys == before.PC92Keys+2 && s.Freshness == before.Freshness
	})
	r.recovered("GB7REF")
}

func q6StagingFault(t *testing.T, zero, deadline bool) {
	r := newQ6Rig(t, zero, 65536, 0)
	before := r.snapshot()
	candidate := r.connect("GB7PEND", true, false)
	start := time.Now()
	stamp := strconv.Itoa(start.UTC().Hour()*3600 + start.UTC().Minute()*60 + start.UTC().Second())
	wire := "PC92^GB7PEND^" + stamp + "^C^5GB7PEND:5457:633^H10^"
	count := 256
	if deadline {
		count = 1
	}
	for index := 0; index < count; index++ {
		candidate.send(t, wire)
	}
	staged := r.wait("candidate staged storage", 5*time.Second, func(s QualificationState) bool { return s.StagedRecords == count })
	if staged.Nodes != before.Nodes || staged.Freshness != before.Freshness || staged.PC92Keys != before.PC92Keys || staged.Established != before.Established {
		t.Fatalf("pending candidate leaked authority: before=%+v staged=%+v", before, staged)
	}
	if deadline {
		// Valid initialization traffic must not extend the original deadline.
		time.Sleep(time.Until(start.Add(30 * time.Second)))
		candidate.send(t, wire)
		r.wait("late valid startup record staged", 5*time.Second, func(s QualificationState) bool { return s.StagedRecords == 2 })
		select {
		case <-candidate.done:
		case <-time.After(time.Until(start.Add(65 * time.Second))):
			t.Fatal("shipped 60-second startup deadline did not close candidate")
		}
		if time.Since(start) < 59*time.Second {
			t.Fatal("candidate deadline was shortened")
		}
	} else {
		candidate.send(t, wire)
		r.closed(candidate)
	}
	after := r.wait("terminal candidate releases all staging", 5*time.Second, func(s QualificationState) bool { return s.StagedRecords == 0 && s.StagedBytes == 0 && s.Pending == 0 })
	if after.Nodes != before.Nodes || after.Freshness != before.Freshness || after.PC92Keys != before.PC92Keys {
		t.Fatal("failed candidate changed global authority")
	}
	r.legacyAlive()
	r.login("K1USER", "127.0.0.3")
	r.recovered("GB7PEND")
}

func q6StallFault(t *testing.T, zero bool) {
	r := newQ6Rig(t, zero, 65536, 0)
	if err := r.primary.conn.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	r.primary.hold.Store(true)
	_ = r.primary.conn.SetReadDeadline(time.Now())
	select {
	case <-r.primary.paused:
	case <-time.After(3 * time.Second):
		t.Fatal("external reader did not pause")
	}
	users := make([]PC92Entry, 4096)
	for index := range users {
		users[index] = PC92Entry{Call: q6User(index), Flags: 1}
	}
	var generator TimestampGenerator
	start := time.Now()
	peakQueue, peakActive := 0, 0
	// Pace ordinary input below the timestamp limit and observe every acceptance.
	// No server socket buffer, queue, or production admission limit is modified.
	for index := 0; index < 256; index++ {
		r.alternate.send(t, q6Record(t, "N0STALL", "C", &generator, users))
		s := r.wait("stall traffic accepted", 3*time.Second, func(s QualificationState) bool { return s.PC92Keys == index+1 })
		if s.ControlQueued > peakQueue {
			peakQueue = s.ControlQueued
		}
		if s.ActiveBytes > peakActive {
			peakActive = s.ActiveBytes
		}
		if s.ControlQueued >= 2 && s.ActiveWrites > 0 {
			// Stop input while far below either queue cap. The stalled active
			// socket write must now hit its deadline; queue overflow cannot be
			// mistaken for successful write-stall enforcement.
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.wait("only stalled peer closes", 5*time.Second, func(s QualificationState) bool { return s.Established == 2 && s.OwnedSessions == 2 })
	if peakQueue == 0 || peakActive == 0 {
		t.Fatalf("fault never demonstrated queued and active backpressure: queue=%d active_bytes=%d", peakQueue, peakActive)
	}
	r.primary.release.Do(func() { close(r.primary.resume) })
	r.closed(r.primary)
	closedAfter := time.Since(start)
	r.legacyAlive()
	r.login("K1USER", "127.0.0.3")
	r.recovered("GB7REF")
	t.Logf("socket-reader stall closed target after %s; observed_peak_queued=%d active_bytes=%d", closedAfter, peakQueue, peakActive)
}

func q6CandidateRace(t *testing.T, zero bool) {
	r := newQ6Rig(t, zero, 65536, 0)
	winner := r.connect("GB7PEND", true, false)
	loser := r.connect("GB7PEND", true, false)
	var generator TimestampGenerator
	winner.send(t, q6Record(t, "GB7PEND", "C", &generator, []PC92Entry{{Call: "K2FIRST", Flags: 1}}))
	losingWire := q6Record(t, "GB7PEND", "C", &generator, []PC92Entry{{Call: "K2LOSE", Flags: 1}, {Call: "K3LOSE", Flags: 1}})
	loser.send(t, losingWire)
	r.wait("both authenticated candidates staged without authority", 5*time.Second, func(s QualificationState) bool {
		return s.Pending == 2 && s.StagedRecords == 2 && s.Users == 0 && s.Freshness == 0 && s.PC92Keys == 0
	})
	r.login("K1USER", "127.0.0.3")
	winner.send(t, "PC20^")
	c, a := winner.recovery(t)
	loser.send(t, "PC20^")
	r.closed(loser)
	r.wait("first established ownership and losing staged state released", 5*time.Second, func(s QualificationState) bool {
		return s.Established == 4 && s.Pending == 0 && s.StagedRecords == 0 && s.StagedBytes == 0 && s.Nodes == 1 && s.Users == 1 && s.Freshness == 1 && s.PC92Keys == 1
	})
	forwarded := r.primary.await(t, func(line string) bool { return strings.HasPrefix(line, "PC92^GB7PEND^") }, 3*time.Second)
	if !strings.Contains(forwarded.line, "1K2FIRST") || strings.Contains(forwarded.line, "LOSE") {
		t.Fatalf("winning startup state was not the admitted state: %s", forwarded.line)
	}
	losingStamp := strings.Split(losingWire, "^")[2]
	winner.send(t, "PC92^GB7PEND^"+losingStamp+"^A^5GB7PEND:5457:633^1K4AFTER^H10^")
	r.wait("losing candidate did not advance the winning origin watermark", 5*time.Second, func(s QualificationState) bool { return s.Users == 2 && s.PC92Keys == 2 })
	r.verifyRecovery(c, a)
}

func TestPC92QualificationQ6Faults(t *testing.T) {
	full := q6Profile(t)
	repetitions := 1
	if full {
		repetitions = 2
	}
	for _, zero := range []bool{false, true} {
		for repeat := 0; repeat < repetitions; repeat++ {
			for _, fault := range []struct {
				name string
				run  func(*testing.T, bool)
			}{
				{"publication", q6PublicationFault}, {"clock", q6ClockFault}, {"admission", q6AdmissionFault},
				{"staging-capacity", func(t *testing.T, zero bool) { q6StagingFault(t, zero, false) }},
				{"stall", q6StallFault}, {"candidate-race", q6CandidateRace},
			} {
				t.Run(fmt.Sprintf("zero=%v/repeat=%d/%s", zero, repeat+1, fault.name), func(t *testing.T) { fault.run(t, zero) })
			}
			if full {
				t.Run(fmt.Sprintf("zero=%v/repeat=%d/staging-deadline", zero, repeat+1), func(t *testing.T) { q6StagingFault(t, zero, true) })
			}
		}
	}
}

func q6Spot(index int, at time.Time) string {
	kind, extra := "PC11", ""
	if index%5 == 2 || index%5 == 3 {
		kind, extra = "PC61", "^127.0.0.8"
	} else if index%5 == 4 {
		kind, extra = "PC26", "^ "
	}
	return fmt.Sprintf("%s^14020.0^%s^%s^%sZ^Q6%06d^W1AAA^GB7REF%s^H10^", kind, q6User(index), at.UTC().Format("02-Jan-2006"), at.UTC().Format("1504"), index, extra)
}

func q6Token(comment string) (int, bool) {
	if len(comment) != 8 || comment[:2] != "Q6" {
		return 0, false
	}
	index := 0
	for position := 2; position < len(comment); position++ {
		if comment[position] < '0' || comment[position] > '9' {
			return 0, false
		}
		index = index*10 + int(comment[position]-'0')
	}
	return index, true
}

func TestPC92QualificationQ6ReceiveOnly(t *testing.T) {
	full := q6Profile(t)
	duration := 5 * time.Second
	if full {
		duration = 20 * time.Minute
	}
	const period = 6 * time.Millisecond // 10,000 distinct and 100,000 duplicate keys/minute.
	keys := int(duration / period)
	r := newQ6Rig(t, false, 65536, keys)
	start := time.Now()
	nextSample := start
	for index := 0; index < keys; index++ {
		due := start.Add(time.Duration(index) * period)
		if delay := time.Until(due); delay > 0 {
			time.Sleep(delay)
		} else if delay < -time.Second {
			t.Fatalf("receive-only input driver fell behind by %s", -delay)
		}
		wire := q6Spot(index, time.Now())
		for duplicate := 0; duplicate < 11; duplicate++ {
			r.primary.send(t, wire)
		}
		if time.Now().After(nextSample) {
			s := r.snapshot()
			if s.SpotKeys != 0 || s.SpotKeyBytes != 0 || s.SpotRefused != 0 || s.Established != 3 {
				t.Fatalf("receive-only forwarding resource/session invariant failed: %+v", s)
			}
			nextSample = time.Now().Add(time.Second)
		}
	}
	if delay := time.Until(start.Add(duration)); delay > 0 {
		time.Sleep(delay)
	}
	r.wait("all expected ingestion", 5*time.Second, func(QualificationState) bool { return r.ingested.Load() == int64(keys*11) })
	for index := range r.counts {
		if count := r.counts[index].Load(); count != 11 {
			t.Fatalf("immutable input token %d received %d times, want11", index, count)
		}
	}
	if r.badToken.Load() || r.dropped.Load() != 0 || r.primary.spots.Load()+r.alternate.spots.Load()+r.legacy.spots.Load() != 0 {
		t.Fatalf("receive-only dropped, changed token, or forwarded a spot: bad_token=%v drops=%d forwarded=%d", r.badToken.Load(), r.dropped.Load(), r.primary.spots.Load()+r.alternate.spots.Load()+r.legacy.spots.Load())
	}
	s := r.snapshot()
	if s.SpotKeys != 0 || s.SpotKeyBytes != 0 || s.SpotRefused != 0 {
		t.Fatalf("receive-only final cache=%+v", s)
	}
	t.Logf("qualification_eligible=%v real_duration=%s distinct_keys=%d expected_ingested=%d actual_ingested=%d spot_cache_keys=%d spot_cache_bytes=%d", full, time.Since(start), keys, keys*11, r.ingested.Load(), s.SpotKeys, s.SpotKeyBytes)
}
