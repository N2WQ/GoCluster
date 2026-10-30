package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"dxcluster/filter"
)

func queueMixedConfigurationOwners(t *testing.T, s *Server, c *Client) *configurationOutputReservation {
	t.Helper()
	if !s.reserveBulletinResponse() {
		t.Fatal("bulletin budget was already exhausted")
	}
	c.controlChan <- controlMessage{line: "bulletin-history\n", bulletinReservation: bulletinResponseReservation}
	owner, err := c.reserveConfigurationOutput()
	if err != nil {
		c.drainBulletinReservations()
		t.Fatal(err)
	}
	t.Cleanup(owner.Release)
	raw := []byte("---\r\nschema_version: 7\r\npayload: " + strings.Repeat("X", 4096) + "\r\n...\r\n")
	if err := c.enqueueConfigurationResponse(owner, raw, false); err != nil {
		c.drainBulletinReservations()
		t.Fatal(err)
	}
	return owner
}

func assertMixedConfigurationOwnersRetired(t *testing.T, s *Server, c *Client, owner *configurationOutputReservation, input, output int64) {
	t.Helper()
	assertConfigurationTransferStats(t, input, output)
	if used := s.bulletinResponseBytes.Load(); used != 0 {
		t.Fatalf("writer retained %d bulletin bytes", used)
	}
	if !owner.released.Load() || c.configurationOutputHeld || len(c.controlChan) != 0 {
		t.Fatal("writer completion retained a raw queue/charge owner")
	}
	select {
	case <-c.writerStopped:
	default:
		t.Fatal("writer completion preceded its composed final drain")
	}
}

func TestConfigurationMixedWriterFlushAndFailureReleaseBothOwners(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "failure"}[fail], func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
			if fail {
				conn.err = errors.New("mixed writer flush refused")
			}
			var unblock sync.Once
			s := &Server{writerBatchMaxBytes: 32 << 10}
			c := &Client{server: s, conn: conn, writer: bufio.NewWriterSize(conn, 32<<10), done: make(chan struct{}), writerStopped: make(chan struct{}), controlChan: make(chan controlMessage, 4), spotChan: make(chan *spotEnvelope, 1)}
			owner := queueMixedConfigurationOwners(t, s, c)
			c.controlChan <- controlMessage{closeAfter: true}
			done := writerV15Start(c)
			t.Cleanup(func() { unblock.Do(func() { close(conn.release) }); writerV15Wait(t, done, "mixed cleanup") })
			writerV15Wait(t, conn.entered, "mixed flush entered")
			assertConfigurationTransferStats(t, input, output+configurationOutputReservationBytes)
			if used := s.bulletinResponseBytes.Load(); used != bulletinResponseReservation || owner.released.Load() {
				t.Fatalf("mixed in-flight flush dropped owner early: bulletin=%d released=%t", used, owner.released.Load())
			}
			unblock.Do(func() { close(conn.release) })
			writerV15Wait(t, done, "mixed flush retired")
			assertMixedConfigurationOwnersRetired(t, s, c, owner, input, output)
			want := "bulletin-history\r\n---\r\nschema_version: 7\r\npayload: " + strings.Repeat("X", 4096) + "\r\n...\r\n"
			if !bytes.Equal(conn.written, []byte(want)) {
				t.Fatal("mixed writer changed/interleaved raw and normalized frames")
			}
		})
	}
}

func TestConfigurationMixedWriterShutdownAndClosedQueueDrainBothOwners(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "shutdown"}[shutdown], func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			conn := &recordingConn{}
			s := &Server{shutdown: make(chan struct{}), writerBatchMaxBytes: 32 << 10}
			c := &Client{server: s, conn: conn, writer: bufio.NewWriter(conn), done: make(chan struct{}), writerStopped: make(chan struct{}), controlChan: make(chan controlMessage, 4), spotChan: make(chan *spotEnvelope, 1)}
			owner := queueMixedConfigurationOwners(t, s, c)
			if shutdown {
				close(s.shutdown)
			} else {
				c.interrupt()
			}
			done := writerV15Start(c)
			writerV15Wait(t, done, "mixed closed/shutdown drain")
			assertMixedConfigurationOwnersRetired(t, s, c, owner, input, output)
			if err := c.enqueueConfigurationResponse(owner, nil, false); !errors.Is(err, errClientClosed) {
				t.Fatalf("closed writer accepted late raw owner: %v", err)
			}
			assertMixedConfigurationOwnersRetired(t, s, c, owner, input, output)
		})
	}
}

func TestConfigurationFinalSaveWaitsForWriterDrainWithoutWaitingForReporter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c, _, _ := v7NoticeFixture(t)
		input, output := configurationTransferBudgetStats()
		conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{}), err: errors.New("closed connection flush")}
		c.conn, c.writer = conn, bufio.NewWriterSize(conn, 32<<10)
		c.writerStopped = make(chan struct{})
		s.writerBatchMaxBytes = 32 << 10
		reporterEntered, reporterRelease := make(chan struct{}), make(chan struct{})
		var reporterOnce, writeOnce sync.Once
		s.connectionReporter = func(ConnectionEvent) { close(reporterEntered); <-reporterRelease }
		owner := queueMixedConfigurationOwners(t, s, c)
		c.configurationUnsaved = true
		writerDone := writerV15Start(c)
		t.Cleanup(func() {
			writeOnce.Do(func() { close(conn.release) })
			reporterOnce.Do(func() { close(reporterRelease) })
			writerV15Wait(t, writerDone, "final-save writer cleanup")
		})
		<-conn.entered
		c.interrupt()
		commits := 0
		s.commitConfigurationFn = func(record *filter.PreparedUserRecord) error {
			commits++
			select {
			case <-c.writerStopped:
			default:
				t.Error("final save committed before its writer retired raw owners")
			}
			actualInput, actualOutput := configurationTransferBudgetStats()
			if actualInput != input || actualOutput != output+configurationOutputReservationBytes {
				t.Errorf("final save charge input=%d output=%d; want input=%d output=%d", actualInput, actualOutput, input, output+configurationOutputReservationBytes)
			}
			if s.bulletinResponseBytes.Load() != 0 {
				t.Error("final save overlaps old bulletin owner")
			}
			return record.Commit()
		}
		saved := make(chan error, 1)
		go func() {
			release, err := s.acquireConfiguration(c, true, false, time.Time{})
			if err == nil {
				defer release()
				err = c.saveFilterOwned()
			}
			saved <- err
		}()
		synctest.Wait()
		select {
		case err := <-saved:
			t.Fatalf("closed owner did not await in-flight writer: %v", err)
		default:
		}
		if commits != 0 {
			t.Fatal("save prepared/committed while writer retained its old owner")
		}
		assertConfigurationTransferStats(t, input, output+configurationOutputReservationBytes)
		writeOnce.Do(func() { close(conn.release) })
		synctest.Wait()
		select {
		case <-reporterEntered:
		default:
			t.Fatal("writer did not reach its deliberately blocked optional reporter")
		}
		select {
		case err := <-saved:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("optional reporter prevented bounded current-owner final save")
		}
		if commits != 1 || c.configurationUnsaved {
			t.Fatal("final save lost pending both-bank preferences")
		}
		assertMixedConfigurationOwnersRetired(t, s, c, owner, input, output)
		select {
		case <-writerDone:
			t.Fatal("optional reporter unexpectedly returned")
		default:
		}
		reporterOnce.Do(func() { close(reporterRelease) })
		synctest.Wait()
		<-writerDone
	})
}
