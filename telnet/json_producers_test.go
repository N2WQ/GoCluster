package telnet

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"dxcluster/commands"
	"dxcluster/filter"
)

func TestJSONMachineWholeSchemaReplies(t *testing.T) {
	for _, schema := range []int{6, 7} {
		for _, terminal := range []bool{false, true} {
			line := "GET YAML SETTINGS SCHEMA " + strconv.Itoa(schema) + " ID sample"
			if terminal {
				line = "PUT YAML CONFIG SCHEMA " + strconv.Itoa(schema) + " extra"
			}
			t.Run(line, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					s, c := v7MachineTestClient(t)
					s.handleMachineCommand(c, line)
					text := <-c.controlChan
					text.configurationReservation.Release()
					c.jsonRequested.Store(true)
					done := make(chan machineDispatch, 1)
					go func() { done <- s.handleMachineCommand(c, line) }()
					msg := <-c.controlChan
					if msg.kind != machineReplyKind(terminal) || msg.closeAfter != terminal || !bytes.Equal(msg.raw, text.raw) {
						t.Fatalf("schema %d selected mismatched raw frame, kind or terminal action", schema)
					}
					conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
					c.conn = conn
					w := jsonSessionWriter{out: jsonWriterOutput{client: c, buf: make([]byte, 0, 64<<10)}}
					finished := make(chan error, 1)
					go func() { _, _, err := w.control(msg); finished <- err }()
					<-conn.entered
					if terminal {
						select {
						case <-done:
							t.Fatal("terminal reader released before socket flush")
						default:
						}
					} else if msg.configurationReservation == nil || msg.configurationReservation.released.Load() {
						t.Fatal("large readback lost ownership before flush")
					}
					close(conn.release)
					if err := <-finished; err != nil {
						t.Fatal(err)
					}
					if result := <-done; !result.handled || result.terminal != terminal || result.closeQueued != terminal {
						t.Fatalf("machine disposition: %+v", result)
					}
					var values []string
					if json.Unmarshal(conn.written, &values) != nil || len(values) != 2 || values[0] != "yaml" || values[1] != string(text.raw) || bytes.Count(conn.written, []byte{'\n'}) != 1 {
						t.Fatal("JSON did not deliver one independently decodable, byte-identical YAML frame")
					}
					if msg.configurationReservation != nil && !msg.configurationReservation.released.Load() {
						t.Fatal("successful flush retained its reservation")
					}
				})
			})
		}
	}
}

func TestJSONMutationAdmissionAndCancellation(t *testing.T) {
	for _, full := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			s, c := v7MachineTestClient(t)
			c.jsonRequested.Store(true)
			c.controlChan = make(chan controlMessage, 1)
			input, output := configurationTransferBudgetStats()
			if full {
				c.controlChan <- controlMessage{line: "earlier"}
			}
			cmd, request := v7ProposalFixture(t, c, "PATCH", "SETTINGS", "noise_class: URBAN")
			before := c.configuredSettings
			commits := 0
			called := false
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
			done := make(chan machineDispatch, 1)
			go func() {
				done <- s.runJSONMachineRequest(c, cmd, request.RequestID, func() string {
					called = true
					return s.applyV7MachineRequestOwned(c, cmd, request)
				})
			}()
			synctest.Wait()
			if full {
				if result := <-done; !result.terminal || called || commits != 0 || c.configuredSettings != before {
					t.Fatal("mutation ran without a reserved reply queue position")
				}
			} else {
				if !called || commits != 1 || c.configuredSettings.NoiseClass != "URBAN" {
					t.Fatal("admitted mutation did not run")
				}
				assertConfigurationTransferStats(t, input, output+configurationOutputReservationBytes)
				// Disconnect before the writer reaches the pending reply.
				c.interrupt()
				if result := <-done; !result.terminal {
					t.Fatal("reader ignored canceled writer rendezvous")
				}
			}
			assertConfigurationTransferStats(t, input, output)
		})
	}
}

func TestJSONSmallYAMLCanceledDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c := v7MachineTestClient(t)
		c.jsonRequested.Store(true)
		c.retired = make(chan struct{})
		done := make(chan bool, 1)
		go func() {
			done <- s.queueMachineReply(c, renderV7YAMLCommandError("CONFIG", "abc", "", "configuration_busy", "busy"), true)
		}()
		msg := <-c.controlChan
		close(c.retired) // A replacement leaves the old writer alive to drain its FIFO.
		synctest.Wait()
		if msg.kind != controlYAMLTerminal || msg.yamlFlushed == nil || msg.configurationReservation != nil {
			t.Fatal("busy reply did not hold the reader's delivery receipt")
		}
		select {
		case <-done:
			t.Fatal("reader accepted an undelivered error")
		default:
		}
		c.interrupt()
		if <-done {
			t.Fatal("reader marked a discarded reply delivered")
		}
		if s.queueMachineReply(c, "discarded", false) || len(c.controlChan) != 0 {
			t.Fatal("closed writer admitted a new YAML response")
		}
	})
}

func TestJSONMutationReplacementOrdering(t *testing.T) {
	for _, when := range []string{"before-commit", "after-commit"} {
		t.Run(when, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, old := v7MachineTestClient(t)
				s.duplicateLoginMsg = "replacement session"
				old.jsonRequested.Store(true)
				old.retired = make(chan struct{})
				s.registerClient(old)
				if err := old.saveFilter(); err != nil {
					t.Fatal(err)
				}
				cmd, request := v7ProposalFixture(t, old, "PATCH", "SETTINGS", "noise_class: URBAN")
				input, output := configurationTransferBudgetStats()
				start := make(chan struct{})
				done := make(chan machineDispatch, 1)
				go func() {
					done <- s.runJSONMachineRequest(old, cmd, request.RequestID, func() string {
						<-start
						return s.applyV7MachineRequestOwned(old, cmd, request)
					})
				}()
				queued := <-old.controlChan
				if when == "after-commit" {
					close(start)
					synctest.Wait() // Producer now waits to transfer the committed result.
				}
				next := configurationTestClient(s, old.callsign)
				result, err := s.restoreAndRegisterClient(next, s.now(), time.Now().Add(time.Second))
				if err != nil || result.evicted != old {
					t.Fatalf("replacement could not acquire the configuration lease: %v", err)
				}
				s.finishClientRegistration(next, old, result.total)
				wantNoise, wantYAML := "URBAN", "applied: true"
				if when == "before-commit" {
					close(start)
					wantNoise, wantYAML = "QUIET", "code: session_unavailable"
				}
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("retirement discarded an admitted reply before its writer took ownership")
				default:
				}
				if next.configuredSettings.NoiseClass != wantNoise || old.configuredSettings.NoiseClass != wantNoise {
					t.Fatalf("replacement or predecessor settings changed out of order, want %s", wantNoise)
				}
				conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
				old.conn = conn
				w := jsonSessionWriter{out: jsonWriterOutput{client: old, buf: make([]byte, 0, 2048)}}
				flushed := make(chan error, 1)
				go func() { _, _, err := w.control(queued); flushed <- err }()
				<-conn.entered
				if result := <-done; !result.handled || result.terminal {
					t.Fatalf("unexpected disposition after writer handoff: %+v", result)
				}
				assertConfigurationTransferStats(t, input, output+configurationOutputReservationBytes)
				release, err := s.acquireConfiguration(next, false, false, time.Now().Add(time.Second))
				if err != nil {
					t.Fatal("socket write retained configuration lease", err)
				}
				release()
				close(conn.release)
				if err := <-flushed; err != nil {
					t.Fatal(err)
				}
				assertConfigurationTransferStats(t, input, output)
				var yaml []string
				if json.Unmarshal(conn.written, &yaml) != nil || len(yaml) != 2 || yaml[0] != "yaml" || !strings.Contains(yaml[1], wantYAML) {
					t.Fatalf("unexpected mutation reply after replacement: %s", conn.written)
				}
				conn.written, conn.entered = nil, make(chan struct{})
				if _, terminal, err := w.control(<-old.controlChan); !terminal || err != nil || string(conn.written) != "[\"text\",\"replacement session\"]\n" {
					t.Fatalf("replacement notice lost its FIFO terminal position: %q, %v", conn.written, err)
				}
			})
		})
	}
}

func TestJSONPausePolicyAndReadback(t *testing.T) {
	s, c := v7MachineTestClient(t)
	at := time.Unix(1700000000, 0)
	c.startReadPause(at, 10*time.Second)
	c.extendReadPause(at, time.Minute)
	old := c.beginHumanReadback(at, time.Minute)
	c.jsonRequested.Store(true)
	c.enterJSONPausePolicy(at.Add(time.Second))
	c.completeHumanReadback(old, at.Add(2*time.Second))
	active, remaining, _ := c.readPauseStatus(at.Add(3 * time.Second))
	if !active || remaining != 7*time.Second || c.readPausePending.Load() {
		t.Fatalf("manual deadline or stale completion: active=%v remaining=%v", active, remaining)
	}
	fresh := &spotEnvelope{enqueueAt: at.Add(11 * time.Second)}
	if c.suppressSpotForReadPause(fresh, fresh.enqueueAt) || !c.suppressSpotForReadPause(&spotEnvelope{enqueueAt: at}, fresh.enqueueAt) {
		t.Fatal("switch did not distinguish stale and fresh spots")
	}
	s.nowFn = func() time.Time { return fresh.enqueueAt }
	s.autoReadPauseDuration, s.autoReadPauseMinRows = time.Minute, 1
	for _, line := range []string{"SHOW SETTINGS", "SHOW FILTER", "SHOW SETTINGS EXTRA"} {
		if !s.handleHumanReadback(c, line) {
			t.Fatal(line)
		}
		msg := <-c.controlChan
		msg.configurationReservation.Release()
		if msg.kind != controlReadback || msg.readback.epoch != 0 || bytes.Contains(msg.raw, []byte("Type RESUME")) || c.readPausePending.Load() {
			t.Fatalf("%s applied automatic pause behavior", line)
		}
		if line == "SHOW SETTINGS" && !bytes.Contains(msg.raw, []byte("JSON          On")) {
			t.Fatalf("missing session JSON readback: %s", msg.raw)
		}
	}
	if got := s.maybeApplyAutoReadPause(c, "one\ntwo\n"); got != "one\ntwo\n" {
		t.Fatal("generic response acquired a JSON auto-pause")
	}
	c.resumeReadPause(fresh.enqueueAt)
	if c.manualPauseUntil != 0 || c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != 0 {
		t.Fatal("JSON RESUME retained manual pause")
	}
}

// These two steps exercise the real admission and fanout path. Holding a
// payload between them simulates a queued broadcast that outlives a command.
func admitJSONTestSpot(t *testing.T, s *Server, id uint64) *broadcastPayload {
	t.Helper()
	sp := ve7ccWireFixture()
	sp.ID = id
	if !s.TryBroadcastSpotOwned(sp, true, true, true) {
		t.Fatal("broadcast admission refused")
	}
	return <-s.broadcast
}

func deliverJSONTestSpot(s *Server, c *Client, payload *broadcastPayload) {
	s.dispatchSpotToWorkers(payload, [][]*Client{{c}})
	s.deliverJob(<-s.workerQueues[0])
}

func TestJSONPauseWallClockTie(t *testing.T) {
	for _, command := range []string{"RESUME", "ON during automatic pause", "ON during manual pause", "manual expiry"} {
		t.Run(command, func(t *testing.T) {
			at := time.Unix(1700000000, 0).UTC()
			s := NewServer(ServerOptions{BroadcastWorkers: 1, WorkerQueue: 1}, nil)
			s.nowFn = func() time.Time { return at }
			s.workerQueues = []chan broadcastJob{make(chan broadcastJob, 1)}
			c := newBroadcastWorkerTestClient(s, 1)
			c.jsonRequested.Store(command == "RESUME" || command == "manual expiry")
			stale := admitJSONTestSpot(t, s, 10)
			released := true
			switch command {
			case "RESUME":
				c.startReadPause(at, time.Minute)
				c.resumeReadPause(at)
			case "ON during automatic pause":
				c.extendReadPause(at, time.Minute)
				c.jsonRequested.Store(true)
				c.enterJSONPausePolicy(at)
			case "ON during manual pause":
				c.startReadPause(at.Add(-time.Minute), time.Minute)
				c.extendReadPause(at.Add(-time.Second), time.Hour)
				c.jsonRequested.Store(true)
				c.enterJSONPausePolicy(at.Add(-time.Second))
				released = false // Natural expiry has no command ordering the tie.
			case "manual expiry":
				c.startReadPause(at.Add(-time.Minute), time.Minute)
				released = false
			}
			fresh := admitJSONTestSpot(t, s, 11)
			if !stale.enqueueAt.Equal(fresh.enqueueAt) || c.readPauseDiscardBefore.Load() != at.UnixNano() {
				t.Fatal("fixture did not create an exact timestamp tie")
			}
			deliverJSONTestSpot(s, c, stale)
			if len(c.spotChan) != 0 || c.readPauseSuppressed.Load() != 1 {
				t.Fatal("a broadcast queued before the cutoff was delivered")
			}
			deliverJSONTestSpot(s, c, fresh)
			if got := len(c.spotChan) == 1; got != released {
				t.Fatalf("broadcast after %s delivered=%v, want %v", command, got, released)
			}
			if released {
				if env := <-c.spotChan; env.spot.ID != 11 || c.suppressSpotForReadPause(env, at) {
					t.Fatal("writer could not deliver the post-release envelope")
				}
			}
			c.jsonRequested.Store(false)
			deliverJSONTestSpot(s, c, fresh)
			if len(c.spotChan) != 0 {
				t.Fatal("legacy text mode admitted a tie")
			}
		})
	}
}

func TestJSONBulletinProducerMetadata(t *testing.T) {
	at := time.Unix(1700000000, 12345)
	s := peerMembershipTestServer()
	c := peerMembershipTestClient(s, "K1ABC", "192.0.2.1:1")
	s.registerClient(c)
	view := s.CurrentPeerMembership()
	if !s.SendCurrentDirectMessageAt(c.callsign, c.peerSessionID, view.Revision, "To K1ABC de W1AW: hello", "W1AW", at) {
		t.Fatal("current recipient refused")
	}
	msg := <-c.controlChan
	if msg.kind != controlBulletin || msg.bulletinKind != "TALK" || msg.bulletinAt != at || msg.bulletinFrom != "W1AW" || strings.TrimSpace(msg.line) != "To K1ABC de W1AW: hello" {
		t.Fatalf("incorrect TALK metadata: %+v", msg)
	}
}

// A TCP client receives an eligible spot after each kind of readback. A blank
// command marks the boundary after preceding command output has been flushed.
func TestJSONReadbacksKeepDeliveringTCP(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) {
		o.LoginGreeting, o.EchoMode = "READY\n", "off"
		o.AutoReadPauseDuration, o.AutoReadPauseMinRows = time.Minute, 1
		o.CommandRatePerSecond, o.CommandBurst = 1000, 1000
	})
	s.processor = commands.NewProcessor(nil, nil, nil, nil, nil, nil)
	w, _ := ve7ccConnectWitness(t, s, "N0CALL-65")
	ve7ccTCPSelector(t, w, "SET JSON ON", "JSON ON 1\r\n")
	jsonWireRecord(t, w.reader, "hello")
	jsonWireRecord(t, w.reader, "schema")
	jsonWireRecord(t, w.reader, "schema")
	readCommand := func(command string) (text []string, document string) {
		t.Helper()
		if _, err := io.WriteString(w.conn, command+"\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		for {
			line, err := w.reader.ReadBytes('\n')
			var value []string
			if err != nil || json.Unmarshal(line, &value) != nil || len(value) < 1 {
				t.Fatalf("%s returned invalid JSON %q, %v", command, line, err)
			}
			switch value[0] {
			case "keepalive":
				return text, document
			case "text":
				text = append(text, value[1])
			case "yaml":
				if document != "" {
					t.Fatal("multiple documents for one GET")
				}
				document = value[1]
			default:
				t.Fatalf("%s unexpected record %q", command, value)
			}
		}
	}
	s.workerQueues = []chan broadcastJob{make(chan broadcastJob, 1)}
	inject := func(id uint64) {
		deliverJSONTestSpot(s, w.client, admitJSONTestSpot(t, s, id))
	}
	expectSpot := func(id uint64) {
		t.Helper()
		inject(id)
		f := jsonWireRecord(t, w.reader, "s")
		if string(f[1]) != strconv.FormatUint(id, 10) {
			t.Fatalf("expected spot %d after readback, got %s", id, f[1])
		}
	}
	for i, cmd := range []string{"HELP COMMANDS", "SHOW FILTER", "SHOW SETTINGS", "GET YAML CONFIG SCHEMA 7 ID live"} {
		lines, document := readCommand(cmd)
		if strings.HasPrefix(cmd, "GET") {
			if !strings.Contains(document, "request_id: live\r\n") || !strings.Contains(document, "configuration:") {
				t.Fatalf("missing complete schema 7 readback: %q", document)
			}
		} else if len(lines) < 3 || strings.Contains(strings.Join(lines, "\n"), "Live spots paused for") {
			t.Fatalf("%s did not yield the expected unpaused multi-line readback: %q", cmd, lines)
		}
		expectSpot(uint64(i + 1))
	}
	lines, _ := readCommand("PAUSE 60")
	if !strings.Contains(strings.Join(lines, " "), "paused for 60") {
		t.Fatalf("manual pause reply %q", lines)
	}
	inject(10)
	_, _, suppressed := w.client.readPauseStatus(s.now())
	if suppressed == 0 {
		t.Fatal("eligible spot entered delivery during manual pause")
	}
	readCommand("RESUME")
	expectSpot(11)
}
