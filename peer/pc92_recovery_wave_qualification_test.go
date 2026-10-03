//go:build qualification

package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
)

// retryWireHandshake uses the actual authenticated listener and protocol
// completion path. The external read deadline is a terminal observation bound;
// it never changes the server's original 60-second initialization deadline.
func retryWireHandshake(ctx context.Context, address, call string) (result net.Conn, resultReader *bufio.Reader, resultErr error) {
	var ready time.Time
	defer func() {
		if resultErr != nil {
			resultErr = &retryHandshakeError{recovery: !ready.IsZero(), err: resultErr}
		}
	}()
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, err
	}
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancel()
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(65 * time.Second)); err != nil {
		return nil, nil, err
	}
	reader := bufio.NewReaderSize(conn, MaxPeerFrameBytes+2)
	var prompt strings.Builder
	for !strings.HasSuffix(prompt.String(), "login:") {
		if prompt.Len() >= 1024 {
			return nil, nil, fmt.Errorf("oversized retry login prompt")
		}
		value, err := reader.ReadByte()
		if err != nil {
			return nil, nil, err
		}
		prompt.WriteByte(value)
	}
	if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
		return nil, nil, err
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, nil, err
		}
		if len(line) > MaxPeerFrameBytes+2 {
			return nil, nil, fmt.Errorf("oversized retry banner")
		}
		if strings.HasPrefix(strings.TrimSpace(line), "PC18^") {
			break
		}
	}
	if _, err := io.WriteString(conn, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^\r\nPC20^\r\n"); err != nil {
		return nil, nil, err
	}
	var c time.Time
	var cPayload string
	for {
		line, err := reader.ReadString('\n')
		// Inspect completion time after every blocking read. A buffered or
		// late successful read cannot turn an expired recovery into success.
		if !ready.IsZero() && time.Now().After(ready.Add(5*time.Second)) {
			return nil, nil, fmt.Errorf("retry recovery exceeded original five-second deadline: %w", context.DeadlineExceeded)
		}
		if err != nil {
			return nil, nil, err
		}
		if len(line) > MaxPeerFrameBytes+2 {
			return nil, nil, fmt.Errorf("oversized retry recovery")
		}
		line = strings.TrimSpace(line)
		if line == "PC22^" {
			if !ready.IsZero() {
				return nil, nil, fmt.Errorf("repeated PC22 during retry recovery")
			}
			ready = time.Now()
			if err := conn.SetReadDeadline(ready.Add(5 * time.Second)); err != nil {
				return nil, nil, err
			}
		}
		if !ready.IsZero() && strings.HasPrefix(line, "PC92^N0LOCAL^") {
			fields := strings.SplitN(line, "^", 5)
			if len(fields) != 5 {
				return nil, nil, fmt.Errorf("malformed recovery line")
			}
			frame, err := ParseFrame(line)
			if err != nil {
				return nil, nil, fmt.Errorf("malformed recovery frame: %w", err)
			}
			record, err := DecodePC92(frame)
			if err != nil {
				return nil, nil, fmt.Errorf("malformed recovery record: %w", err)
			}
			// Receiver grammar also permits implicit empty snapshots. This
			// fixture's publisher always emits its explicit local node and a
			// positive hop; a truncated local output must not pass as empty.
			if record.SubjectImplicit || record.Subject.Call != "N0LOCAL" || record.Hop <= 0 {
				return nil, nil, fmt.Errorf("malformed local recovery subject or hop")
			}
			switch fields[3] {
			case "C":
				if !c.IsZero() {
					return nil, nil, fmt.Errorf("repeated C during retry recovery")
				}
				c = time.Now()
				cPayload = strings.Clone(fields[4])
			case "A":
				if c.IsZero() || time.Since(ready) > 5*time.Second {
					return nil, nil, fmt.Errorf("retry recovery not ordered within five seconds")
				}
				if fields[4] != cPayload {
					return nil, nil, fmt.Errorf("retry recovery A changed immutable C payload")
				}
				if err := conn.SetDeadline(time.Time{}); err != nil {
					return nil, nil, err
				}
				ok = true
				return conn, reader, nil
			}
		}
	}
}

// Startup transport refusal/expiry is an observed unsuccessful candidate. Once
// PC22 establishes the link, every failure is a qualification failure, including
// absent recovery output. Protocol-output defects are never retryable in either
// phase. The caller must preserve this distinction across complete attempts.
type retryHandshakeError struct {
	recovery bool
	err      error
}

func (e *retryHandshakeError) Error() string { return e.err.Error() }
func (e *retryHandshakeError) Unwrap() error { return e.err }

func retryHandshakeMayRetry(err error) bool {
	var failure *retryHandshakeError
	if !errors.As(err, &failure) || failure.recovery {
		return false
	}
	var transport *net.OpError
	return errors.Is(failure.err, io.EOF) || errors.Is(failure.err, io.ErrUnexpectedEOF) || errors.As(failure.err, &transport)
}

type retryWaveCounters struct {
	attempts, recovered       atomic.Int64
	grants, failures, expired atomic.Int64
	recoveredConn             atomic.Pointer[retryWaveConnection]
	expectClose               atomic.Bool
}

type retryWaveConnection struct {
	conn       net.Conn
	generation uint64
}

// One bounded driver goroutine belongs to each identity. Client reconnects do
// not bypass server authentication, retry ownership, grants or handshake limits.
func retryWaveClient(ctx context.Context, address, call string, members []PC92Entry, released *atomic.Bool, count *retryWaveCounters, failures chan<- error, mixed *retryMixedQualification) {
	var stamps TimestampGenerator
	report := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	for ctx.Err() == nil {
		generation := uint64(count.attempts.Add(1))
		conn, reader, err := retryWireHandshake(ctx, address, call)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !retryHandshakeMayRetry(err) {
				report(fmt.Errorf("retry handshake %s: %w", call, err))
				return
			}
			// A 64-way wave legitimately exceeds a60s server init window.
			// Denied/expired candidates are counted and retried, not hidden by
			// extending that window or assumed to be protocol successes.
			count.expired.Add(1)
			if err := qualificationWait(ctx, 100*time.Millisecond); err != nil {
				return
			}
			continue
		}
		opened := time.Now()
		isReleased := released.Load()
		if isReleased {
			mixed.BeginStableRecipient(call, generation, opened)
		} else {
			mixed.BeginRecipient(call, generation, opened)
		}
		readerDone := make(chan error, 1)
		stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
		go func() {
			for {
				line, err := reader.ReadString('\n')
				if len(line) > MaxPeerFrameBytes+2 {
					readerDone <- fmt.Errorf("oversized retry recipient output")
					return
				}
				if err != nil {
					readerDone <- err
					return
				}
				mixed.ObserveWire(call, generation, strings.TrimSpace(line), time.Now())
			}
		}()
		if isReleased {
			count.recoveredConn.Store(&retryWaveConnection{conn: conn, generation: generation})
			count.recovered.Add(1)
			err = <-readerDone
			stopClose()
			_ = conn.Close()
			if ctx.Err() == nil && !count.expectClose.Load() {
				report(fmt.Errorf("recovered %s closed unexpectedly: %w", call, err))
			}
			return
		}
		// The first second is mandatory healthy delivery. The next second is
		// a declared lead-in to intentional refusal, so no post-hoc loss waiver
		// can erase a first-second obligation. The sole reader stays active.
		if err := qualificationWait(ctx, max(0, time.Until(opened.Add(2*time.Second)))); err != nil {
			_ = conn.Close()
			<-readerDone
			stopClose()
			return
		}
		if err := mixed.EndRecipient(call, generation, time.Now()); err != nil {
			_ = conn.Close()
			<-readerDone
			stopClose()
			report(err)
			return
		}
		stamp, err := stamps.NextAt(time.Now())
		if err == nil {
			_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			// The healthy producer owns origins0 and1. Use origin2 here so
			// its newer watermarks cannot make an intended refusal stale.
			_, err = io.WriteString(conn, qualificationFrame(qualificationCall("N0", 2), stamp, "C", members, 1)+"\r\n")
		}
		if err != nil {
			_ = conn.Close()
			<-readerDone
			stopClose()
			report(fmt.Errorf("retry refusal stimulus %s: %w", call, err))
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		<-readerDone
		stopClose()
		_ = conn.Close()
		if err := qualificationWait(ctx, 100*time.Millisecond); err != nil {
			return
		}
	}
}

// This gate exercises real retry attempts at the maximum identity population.
// It is deliberately separate from aggregate qualification: the 30-minute mode
// supplies the approved mixed workload and retries. Other qualification cases
// and aggregate allocation proofs remain separate mandatory evidence.
func TestPC92V14RetryWaveService(t *testing.T) {
	profile := os.Getenv("GOCLUSTER_PC92_V14_RETRY_PROFILE")
	if profile != "preflight" && profile != "qualification" {
		t.Skip("opt-in actual retry-wave service gate")
	}
	for _, tc := range []struct {
		name     string
		blocked  int
		periodic bool
	}{{"recovering_63", 63, false}, {"recovering_63_periodic", 63, true}, {"recovering_64", 64, false}} {
		t.Run(tc.name, func(t *testing.T) {
			runRetryWaveService(t, tc.blocked, profile == "qualification", tc.periodic)
		})
	}
}

func runRetryWaveService(t *testing.T, blocked int, qualified, periodic bool) {
	full := qualified && !periodic
	port := q6Port(t)
	cfg := completeProtocolTestConfig(config.PeeringConfig{Enabled: true, ForwardSpots: true, ListenPort: port, WriteQueueSize: 128,
		Timeouts: config.PeeringTimeouts{LoginSeconds: 60, InitSeconds: 60, IdleSeconds: 3600},
		Backoff:  config.PeeringBackoff{BaseMS: 2000, MaxMS: 300000}}, "N0LOCAL")
	if periodic {
		// Enabled configuration must still recover promptly without waiting
		// for these periodic intervals; this 75-second case does not fire C.
		cfg.KeepaliveSeconds, cfg.ConfigSeconds = 600, 1800
	}
	var calls []string
	for i := range 64 {
		call := qualificationCall("P0", i)
		calls = append(calls, call)
		cfg.Peers = append(cfg.Peers, config.PeeringPeer{Enabled: true, Family: config.PeeringPeerFamilyDXSpider,
			Direction: config.PeeringPeerDirectionInbound, PreferPC9x: true, RemoteCallsign: call})
	}
	m, err := NewManager(cfg, "N0LOCAL", nil, 600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetBuildIdentity("v14-retry-wave", "", "local", "2026-10-02", "go1.26"); err != nil {
		t.Fatal(err)
	}
	mixed := newRetryMixedQualification(m, calls)
	f := schedulerFullGraph(t, m.protocol, calls)
	// Keep detached/message watermarks at their real admission UTC value.
	// A synthetic zero can reject present traffic around midnight.
	for _, population := range []struct {
		prefix string
		count  int
	}{{"M0", 4096}, {"T0", 8192}} {
		for i := range population.count {
			call := qualificationCall(population.prefix, i)
			watermark := m.protocol.graph.freshness.Value(call)
			at := watermark.Accepted.UTC()
			watermark.Value = float64(at.Hour()*3600 + at.Minute()*60 + at.Second())
			m.protocol.graph.freshness.Set(call, watermark)
		}
	}
	before := &LocalMembership{Revision: 1, Complete: true, RawCount: 1000}
	for i := range 1000 {
		before.Users = append(before.Users, LocalUser{SessionID: uint64(i + 1), Login: qualificationCall("L0", i), IP: "192.0.2.1"})
	}
	var membership atomic.Pointer[LocalMembership]
	membership.Store(before)
	m.SetMembershipProvider(func() LocalMembership { return *membership.Load() })
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	var initial []net.Conn
	t.Cleanup(func() {
		cancel()
		for _, conn := range initial {
			_ = conn.Close()
		}
		m.Stop()
		workers.Wait()
	})
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	address := fmt.Sprintf("127.0.0.1:%d", port)
	readers := make([]*bufio.Reader, 64)
	for i, call := range calls {
		conn, reader, err := retryWireHandshake(ctx, address, call)
		if err != nil {
			t.Fatal(err)
		}
		initial = append(initial, conn)
		readers[i] = reader
	}
	var counts [64]retryWaveCounters
	var lastGrant time.Time
	var observerMu sync.Mutex
	var observerError error
	var offered, committed atomic.Int64
	var membershipAdmitted atomic.Pointer[time.Time]
	var membershipCheck *retryMembershipQualification
	if blocked == 63 {
		membershipCheck = newRetryMembershipQualification(m, &membership, before, calls[0])
		m.QualificationSetPublicationObserver(func(event QualificationPublication) {
			membershipCheck.Observe(event)
			if event.Peer == calls[63] && event.Action == "A" && strings.Contains(event.Wire, "203.0.113.9") {
				at := event.At
				membershipAdmitted.CompareAndSwap(nil, &at)
			}
		})
	}
	evidence := make([]*retryEvidence, blocked)
	for i := range blocked {
		evidence[i] = &retryEvidence{call: calls[i]}
	}
	m.QualificationSetAdmissionObserver(func(event QualificationAdmissionEvent) {
		observerMu.Lock()
		defer observerMu.Unlock()
		if blocked == 63 && event.Kind == "pc92_commit" && event.Call == calls[63] {
			committed.Add(1)
		}
		for i := range blocked {
			if event.Call != calls[i] {
				continue
			}
			evidence[i].observe(event)
			switch event.Kind {
			case "startup_grant":
				if !lastGrant.IsZero() && event.At.Sub(lastGrant) < time.Second {
					observerError = fmt.Errorf("global retry grants separated by %s", event.At.Sub(lastGrant))
				}
				lastGrant = event.At
				counts[i].grants.Add(1)
			case "failure":
				counts[i].failures.Add(1)
				if event.Cause != "new_authority" {
					observerError = fmt.Errorf("retry %s failed for %s instead of the still-infeasible graph record", event.Call, event.Cause)
				}
			}
			break
		}
	})
	members := f.members(0)
	members[len(members)-1] = PC92Entry{Call: "W1NEW", Flags: 1}
	wire := qualificationFrame(qualificationCall("N0", 2), qualificationStamp(time.Now()), "C", members, 1)
	for i := range blocked {
		_ = initial[i].SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(initial[i], wire+"\r\n"); err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, readers[i])
		failure, err := evidence[i].latest("failure")
		if err != nil || failure.Generation == 0 {
			t.Fatalf("initial source %s did not produce authoritative refusal: %v", calls[i], err)
		}
		_ = initial[i].Close()
	}
	if blocked == 63 {
		workers.Add(1)
		go func() { defer workers.Done(); _, _ = io.Copy(io.Discard, readers[63]) }()
	}
	var released atomic.Bool
	failures := make(chan error, 1)
	if membershipCheck != nil {
		membershipCheck.Arm()
	}
	for i := range blocked {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			retryWaveClient(ctx, address, calls[i], members, &released, &counts[i], failures, mixed)
		}(i)
	}
	duration := 75 * time.Second
	if full && blocked == 63 {
		duration = 30 * time.Minute
	}
	loadDuration := duration
	if qualified {
		// Keep the same absolute offered schedule through recovery, all
		// 60-second resets and the subsequent failure. Early success never
		// shortens this declared tail or its mandatory delivery population.
		loadDuration += 600 * time.Second
	}
	start := time.Now()
	maintenanceDone := make(chan struct{})
	var capacity QualificationCapacityChecks
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(maintenanceDone)
		for target := start.Add(100 * time.Millisecond); target.Before(start.Add(loadDuration)); target = target.Add(100 * time.Millisecond) {
			if err := qualificationWait(ctx, max(0, time.Until(target))); err != nil {
				return
			}
			observeCtx, stop := context.WithTimeout(ctx, time.Second)
			state, err := m.QualificationSnapshot(observeCtx)
			stop()
			if err == nil {
				err = capacity.Observe(state)
			}
			if err != nil {
				select {
				case failures <- fmt.Errorf("queued maintenance observation: %w", err):
				default:
				}
				return
			}
		}
	}()
	producerDone := make(chan struct{})
	if blocked == 63 {
		m.mu.RLock()
		live := m.sessions.Value(calls[63])
		m.mu.RUnlock()
		if live == nil {
			t.Fatal("healthy source missing before mixed input")
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer close(producerDone)
			if err := mixed.Run(ctx, live, f, start, loadDuration, &offered); err != nil {
				select {
				case failures <- err:
				default:
				}
			}
		}()
	} else {
		close(producerDone)
	}
	awaitLoadEnd := func() {
		retryWaveAwait(ctx, t, failures, start.Add(loadDuration))
		for _, done := range []<-chan struct{}{producerDone, maintenanceDone} {
			select {
			case err := <-failures:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-done:
			}
		}
		if blocked == 63 {
			_, err := qualificationAwaitUntil(ctx, time.Now().Add(time.Second), time.Millisecond, func(context.Context) (bool, error) {
				return committed.Load() == offered.Load(), nil
			}, func(ready bool) bool { return ready })
			if err != nil {
				t.Fatalf("wave mandatory PC92 outputs did not reconcile: offered=%d committed=%d: %v", offered.Load(), committed.Load(), err)
			}
			if err := mixed.Verify(); err != nil {
				t.Fatal(err)
			}
			t.Logf("wave PC92 offered=%d committed=%d; %s", offered.Load(), committed.Load(), mixed.Summary())
		}
		t.Logf("full offered workload duration=%s; capacity samples=%d max owners=%d max pending=%d loaded_reset=%v", loadDuration, capacity.Samples, capacity.MaxOwned, capacity.MaxPending, qualified)
		select {
		case err := <-failures:
			t.Fatal(err)
		default:
		}
	}
	retryWaveAwait(ctx, t, failures, start.Add(duration))
	if !qualified {
		awaitLoadEnd()
		t.Log("Preflight uses a quiet recovery tail and does not prove reset timing under continuous mixed load")
	}
	if blocked == 63 {
		if err := membershipCheck.Verify(); err != nil {
			t.Fatal(err)
		}
		membershipChanged := membershipCheck.ChangeTime()
		at := membershipAdmitted.Load()
		if at == nil || membershipChanged.IsZero() || at.Before(membershipChanged) || at.Sub(membershipChanged) > time.Second {
			t.Fatalf("membership admission under actual retries failed: changed=%s admitted=%v", membershipChanged, at)
		}
		t.Logf("healthy membership queue admission=%s", at.Sub(membershipChanged))
	}
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	observerMu.Lock()
	err = observerError
	observerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for i := range blocked {
		if counts[i].grants.Load() == 0 || counts[i].failures.Load() < 2 {
			t.Fatalf("identity %s did not actually retry: grants=%d failures=%d attempts=%d", calls[i], counts[i].grants.Load(), counts[i].failures.Load(), counts[i].attempts.Load())
		}
		if err := evidence[i].check(2*time.Second, 300*time.Second, false); err != nil {
			t.Fatalf("identity %s: %v", calls[i], err)
		}
	}
	if full && blocked == 63 {
		capObserved := false
		for _, identity := range evidence {
			fact, found, err := identity.cappedGrant(2*time.Second, 300*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				t.Logf("actual capped retry: call=%s failure_ordinal=%d failure_generation=%d grant_generation=%d failed_at=%s granted_at=%s elapsed=%s required=300s", fact.Call, fact.FailureOrdinal,
					fact.FailureGeneration, fact.GrantGeneration, fact.FailedAt.UTC().Format(time.RFC3339Nano), fact.GrantedAt.UTC().Format(time.RFC3339Nano), fact.GrantedAt.Sub(fact.FailedAt))
				capObserved = true
				break
			}
		}
		if !capObserved {
			t.Fatal("full retry qualification did not observe any failed attempt followed by an independently verified 300-second capped grant")
		}
	}
	t.Logf("actual retry wave: identities=%d elapsed=%s periodic_C=%d periodic_K=%d original_init_deadline=60s; global pacing and per-identity failure/backoff evidence checked", blocked, time.Since(start), cfg.ConfigSeconds, cfg.KeepaliveSeconds)
	// No admission headroom is released: small new startup and complete local
	// C/A recovery are valid even while the large refused population cannot fit.
	released.Store(true)
	// This outer observation window combines independent worst cases; it
	// grants no extension to an individual handshake, C/A or healthy reset.
	// One original init window covers a candidate retiring during the wave.
	outerRecovery := time.Duration(cfg.Backoff.MaxMS)*time.Millisecond +
		time.Duration(cfg.MaxPeers)*(time.Second+membershipServiceInterval) +
		time.Duration(cfg.Timeouts.InitSeconds)*time.Second + 5*time.Second + 61*time.Second
	deadline := time.Now().Add(outerRecovery)
	_, err = qualificationAwaitUntil(ctx, deadline, 100*time.Millisecond, func(context.Context) (bool, error) {
		select {
		case err := <-failures:
			return false, err
		default:
		}
		for i := range blocked {
			reset, err := evidence[i].latest("healthy_reset")
			if err != nil {
				return false, err
			}
			if counts[i].recovered.Load() != 1 || reset.Generation == 0 {
				return false, nil
			}
		}
		return true, nil
	}, func(ready bool) bool { return ready })
	if err != nil {
		t.Fatal(err)
	}
	for i := range blocked {
		if err := evidence[i].check(2*time.Second, 300*time.Second, true); err != nil {
			t.Fatalf("healthy reset %s: %v", calls[i], err)
		}
	}
	observerMu.Lock()
	err = observerError
	observerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	// All other identities remain established. This sole ready identity must
	// therefore return to the2s base delay after an externally driven failure.
	further := &retryEvidence{call: calls[0]}
	m.QualificationSetAdmissionObserver(func(event QualificationAdmissionEvent) {
		if blocked == 63 && event.Kind == "pc92_commit" && event.Call == calls[63] {
			committed.Add(1)
		}
		further.observe(event)
	})
	connection := counts[0].recoveredConn.Load()
	if connection == nil {
		t.Fatal("reset identity has no externally held connection")
	}
	cutoff, err := mixed.PlanRecipientFault(calls[0], connection.generation)
	if err != nil {
		t.Fatal(err)
	}
	retryWaveAwait(ctx, t, failures, cutoff.Add(time.Second))
	if err := mixed.EndRecipient(calls[0], connection.generation, time.Now()); err != nil {
		t.Fatal(err)
	}
	counts[0].expectClose.Store(true)
	_ = connection.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(connection.conn, qualificationFrame(qualificationCall("N0", 2), qualificationStamp(time.Now()), "C", members, 1)+"\r\n"); err != nil {
		t.Fatal(err)
	}
	failure, err := qualificationAwaitUntil(ctx, time.Now().Add(5*time.Second), time.Millisecond, func(context.Context) (QualificationAdmissionEvent, error) {
		select {
		case err := <-failures:
			return QualificationAdmissionEvent{}, err
		default:
			return further.latest("failure")
		}
	}, func(event QualificationAdmissionEvent) bool { return event.Generation != 0 })
	if err != nil {
		t.Fatal(err)
	}
	retryWaveAwait(ctx, t, failures, failure.At.Add(2*time.Second))
	conn, reader, err := retryWaveFinalHandshake(ctx, address, calls[0], failures)
	if err != nil {
		t.Fatalf("post-reset base-delay startup failed: %v", err)
	}
	initial = append(initial, conn)
	finalGeneration := uint64(counts[0].attempts.Add(1))
	mixed.BeginStableRecipient(calls[0], finalGeneration, time.Now())
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			line, err := reader.ReadString('\n')
			if len(line) > MaxPeerFrameBytes+2 {
				err = fmt.Errorf("oversized post-reset recipient output")
			}
			if err != nil {
				if ctx.Err() == nil {
					select {
					case failures <- fmt.Errorf("post-reset recipient closed: %w", err):
					default:
					}
				}
				return
			}
			mixed.ObserveWire(calls[0], finalGeneration, strings.TrimSpace(line), time.Now())
		}
	}()
	grant, err := further.latest("startup_grant")
	if err != nil || grant.Generation == 0 || grant.At.After(failure.At.Add(3*time.Second)) {
		t.Fatalf("post-reset startup did not return to2s base: failure=%+v grant=%+v err=%v", failure, grant, err)
	}
	if err := further.check(2*time.Second, 300*time.Second, false); err != nil {
		t.Fatal(err)
	}
	t.Logf("post-reset new failure returned to base: startup grant after%s", grant.At.Sub(failure.At))
	if qualified {
		awaitLoadEnd()
	}
	t.Log("Controlled retry, healthy reset and further failure checks passed; this is not the overall PC92 or 480 MiB acceptance verdict")
}

func retryWaveAwait(ctx context.Context, t *testing.T, failures <-chan error, until time.Time) {
	t.Helper()
	timer := time.NewTimer(max(0, time.Until(until)))
	defer timer.Stop()
	select {
	case err := <-failures:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
}

// One bounded fixture worker allows another producer/recipient failure to
// cancel the final socket handshake immediately. Every exit joins the worker;
// a late successful connection is closed on a losing/canceled outcome.
func retryWaveFinalHandshake(ctx context.Context, address, call string, failures <-chan error) (net.Conn, *bufio.Reader, error) {
	type outcome struct {
		conn   net.Conn
		reader *bufio.Reader
		err    error
	}
	handshakeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan outcome, 1)
	go func() {
		conn, reader, err := retryWireHandshake(handshakeCtx, address, call)
		result <- outcome{conn, reader, err}
	}()
	var failure error
	select {
	case got := <-result:
		return got.conn, got.reader, got.err
	case failure = <-failures:
	case <-ctx.Done():
		failure = ctx.Err()
	}
	cancel()
	got := <-result
	if got.conn != nil {
		_ = got.conn.Close()
	}
	return nil, nil, failure
}
