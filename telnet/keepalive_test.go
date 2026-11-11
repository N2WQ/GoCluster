package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"dxcluster/filter"
)

func TestPeriodicKeepaliveWireBothTransports(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, telnetEchoOff, "ready\n"
			})
			_, conn, ownerDone := startHandshakeTranscriptSession(t, s)
			var client *Client
			t.Cleanup(func() {
				closeHandshakeTranscriptSession(t, conn, ownerDone)
				if client != nil {
					writerV15Wait(t, client.writerStopped, "transport heartbeat cleanup")
				}
			})
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			readMachineEchoCoverageLiteral(t, reader, "login: ")
			if _, err := io.WriteString(conn, "W1ABC-1\r\n"); err != nil {
				t.Fatal(err)
			}
			readMachineEchoCoverageLiteral(t, reader, "ready\r\n")
			s.clientsMutex.RLock()
			client = s.clients["W1ABC-1"]
			s.clientsMutex.RUnlock()
			if client == nil {
				t.Fatal("heartbeat fixture did not register its session")
			}
			s.sendKeepalives()
			if err := client.enqueueControl(controlMessage{line: "after\n"}); err != nil {
				t.Fatal(err)
			}
			readMachineEchoCoverageLiteral(t, reader, "\r\nafter\r\n")
		})
	}
}

func TestPeriodicKeepalivePreservesPauseAndSettings(t *testing.T) {
	for _, state := range []string{"inactive", "paused", "pending_readback"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
				s := &Server{clients: make(map[string]*Client), writerBatchWait: time.Millisecond, nowFn: func() time.Time { return now }}
				conn := &writerV15Conn{expected: []byte{13, 10}, target: 1, reached: make(chan struct{})}
				c := writerV15Client(s, conn)
				c.configuredSettings = filter.SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "RURAL",
					DedupePolicy: "SLOW", PathMinObservationCount: 7, SolarSummaryMinutes: 5, CWSpeed: true}
				c.configurationInitialized = true
				s.clients[c.callsign] = c
				if state != "inactive" {
					c.startReadPause(now, 30*time.Second)
					c.enqueueSpot(&spotEnvelope{spot: writerV15Spot(), enqueueAt: now.Add(-time.Second)})
					if len(c.spotChan) != 0 || c.readPauseSuppressed.Load() != 1 {
						t.Fatal("fixture failed to suppress the stale spot")
					}
				}
				if state == "pending_readback" {
					c.beginHumanReadback(now, time.Minute)
				}
				pause, settings := showHoldPauseState(c), c.configuredSettings
				queueDrops, clientDrops, senderFailures := s.BroadcastMetricSnapshot()
				pacing := s.CommandPacingWaits()
				s.sendKeepalives()
				done := writerV15Start(c)
				defer func() { c.close(""); writerV15Wait(t, done, "paused heartbeat cleanup") }()
				writerV15Wait(t, conn.reached, "paused heartbeat bytes")
				// Conn.Write observes bytes before post-flush completion runs. Wait
				// for the real writer to quiesce before asserting readback authority.
				synctest.Wait()
				assertReadbackPauseState(t, c, pause)
				if !reflect.DeepEqual(c.configuredSettings, settings) || s.CommandPacingWaits() != pacing {
					t.Fatal("periodic heartbeat changed settings or command pacing")
				}
				q, d, f := s.BroadcastMetricSnapshot()
				if q != queueDrops || d != clientDrops || f != senderFailures {
					t.Fatalf("healthy heartbeat changed drop/failure counters: %d/%d/%d", q, d, f)
				}
				c.close("")
				writerV15Wait(t, done, "paused heartbeat shutdown")
				writerV15Check(t, conn)
			})
		})
	}
}

func TestPeriodicKeepaliveControlPriority(t *testing.T) {
	s := &Server{clients: make(map[string]*Client), writerBatchWait: time.Millisecond}
	want := "\r\n" + writerV15Oracle(writerV15Spot().FormatDXCluster()+"\n")
	conn := &writerV15Conn{expected: []byte(want), target: 1, reached: make(chan struct{})}
	c := writerV15Client(s, conn)
	s.clients[c.callsign] = c
	c.spotChan <- &spotEnvelope{spot: writerV15Spot()}
	s.sendKeepalives()
	done := writerV15Start(c)
	defer func() { c.close(""); writerV15Wait(t, done, "priority heartbeat cleanup") }()
	writerV15Wait(t, conn.reached, "control-before-spot heartbeat")
	c.close("")
	writerV15Wait(t, done, "priority heartbeat shutdown")
	writerV15Check(t, conn)
}

func TestPeriodicKeepaliveWriteFailure(t *testing.T) {
	s := peerMembershipTestServer()
	conn := &errConn{writeErr: errors.New("heartbeat write failure")}
	c := peerMembershipTestClient(s, "N0CALL", "")
	c.conn, c.writer, c.spotChan = conn, bufio.NewWriter(conn), make(chan *spotEnvelope, 1)
	s.clients[c.callsign] = c
	s.sendKeepalives()
	done := writerV15Start(c)
	defer func() { c.close(""); writerV15Wait(t, done, "failed heartbeat cleanup") }()
	writerV15Wait(t, done, "failed heartbeat writer")
	if !conn.closed {
		t.Fatal("failed heartbeat did not close its connection")
	}
	q, d, f := s.BroadcastMetricSnapshot()
	if q != 0 || d != 0 || f != 1 {
		t.Fatalf("failed heartbeat counters=%d/%d/%d; want 0/0/1", q, d, f)
	}
}

func TestPeriodicKeepaliveAdmission(t *testing.T) {
	s := peerMembershipTestServer()
	first := peerMembershipTestClient(s, "N0CALL", "")
	second := peerMembershipTestClient(s, "K1ABC", "")
	closed := peerMembershipTestClient(s, "W1XYZ", "")
	closed.interrupt()
	s.clients[first.callsign], s.clients[second.callsign], s.clients[closed.callsign], s.clients["nil"] = first, second, closed, nil
	s.sendKeepalives()
	for _, client := range []*Client{first, second} {
		if len(client.controlChan) != 1 {
			t.Fatalf("periodic keepalive queued %d records for %s; want one", len(client.controlChan), client.callsign)
		}
	}
	left, right := <-first.controlChan, <-second.controlChan
	if left.line != "" || right.line != "" || !bytes.Equal(left.raw, []byte{13, 10}) || !bytes.Equal(right.raw, []byte{13, 10}) {
		t.Fatalf("heartbeat payloads = %+v/%+v; want exact raw CRLF", left, right)
	}
	left.raw[0] = 'X'
	if right.raw[0] != 13 {
		t.Fatal("recipients share mutable heartbeat backing")
	}
	if len(closed.controlChan) != 0 {
		t.Fatal("closed client admitted a keepalive")
	}
	// Only periodic heartbeat traffic is repaired. Generic blank messages retain
	// their prior suppression, including blank-command replies through Send.
	for _, line := range []string{"\r\n", " \t\n", ""} {
		if err := first.Send(line); err != nil || len(first.controlChan) != 0 {
			t.Fatalf("generic whitespace behavior changed for %q: err=%v queued=%d", line, err, len(first.controlChan))
		}
	}
}

func TestPeriodicKeepaliveOverflowReporterOutsideClientsLock(t *testing.T) {
	s := peerMembershipTestServer()
	c := peerMembershipTestClient(s, "N0CALL", "")
	s.clients[c.callsign] = c
	for i := 0; i < cap(c.controlChan); i++ {
		c.controlChan <- controlMessage{line: "occupied\n"}
	}
	reports := 0
	s.connectionReporter = func(event ConnectionEvent) {
		reports++
		// A recursive reader can succeed under a held RLock. A write TryLock
		// deterministically distinguishes an emitter that retained either lock.
		if !s.clientsMutex.TryLock() {
			t.Error("overflow reporter called while membership was locked")
			return
		}
		s.clientsMutex.Unlock()
		_ = s.GetClientCount()
		if event.Action != "disconnect" || event.Reason != "control queue full" || event.Call != c.callsign {
			t.Errorf("unexpected heartbeat overflow report: %+v", event)
		}
	}
	s.sendKeepalives()
	select {
	case <-c.done:
	default:
		t.Fatal("full control queue did not disconnect client")
	}
	_, drops, failures := s.BroadcastMetricSnapshot()
	if reports != 1 || drops != 1 || failures != 0 || len(c.controlChan) != cap(c.controlChan) {
		t.Fatalf("heartbeat overflow reports=%d drops=%d failures=%d queued=%d", reports, drops, failures, len(c.controlChan))
	}
}

func TestKeepaliveLoopCadenceAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := peerMembershipTestServer()
		s.keepaliveInterval = time.Minute
		s.shutdown = make(chan struct{})
		c := peerMembershipTestClient(s, "N0CALL", "")
		s.clients[c.callsign] = c
		done := make(chan struct{})
		go func() { defer close(done); s.keepaliveLoop() }()
		var stopOnce sync.Once
		stop := func() {
			stopOnce.Do(func() { close(s.shutdown) })
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Error("keepalive loop did not retire after shutdown")
			}
		}
		defer stop()
		synctest.Wait()
		time.Sleep(time.Minute - time.Nanosecond)
		synctest.Wait()
		if len(c.controlChan) != 0 {
			t.Fatal("keepalive fired before its configured interval")
		}
		for _, advance := range []time.Duration{time.Nanosecond, time.Minute} {
			time.Sleep(advance)
			synctest.Wait()
			if len(c.controlChan) != 1 {
				t.Fatalf("configured heartbeat tick queued %d messages; want one", len(c.controlChan))
			}
			if got := <-c.controlChan; !bytes.Equal(got.raw, []byte{13, 10}) {
				t.Fatalf("configured tick payload = %+v", got)
			}
		}
		stop()
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		if len(c.controlChan) != 0 {
			t.Fatal("retired keepalive loop continued sending")
		}
	})
}
