package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestV5MachineCoordinatorActualTransport(t *testing.T) {
	s, c, cfg := v5CoordinatorFixture(t)
	disk := v5CoordinatorSeed(t, c, cfg)
	revision := v5CoordinatorRevision(t, c)
	input := fmt.Sprintf("---\r\nschema_version: 5\r\nrequest_id: wire-1\r\nif_revision: %s\r\nconfiguration:\r\n  rules: []\r\n...\r\nRESUME\r\n", revision)
	transport, _ := newYAMLTestClient(input)
	c.conn, c.reader = transport.conn, transport.reader
	result := s.handleV5MachineCommand(c, "PATCH YAML FILTER")
	if !result.handled || result.terminal || result.closeQueued {
		t.Fatalf("actual valid upload failed: %+v", result)
	}
	select {
	case reply := <-c.controlChan:
		v5CoordinatorSuccess(t, string(reply.raw), true)
	default:
		t.Fatal("actual coordinator did not queue a reply")
	}
	line, err := c.readCommandLine(128)
	if err != nil || line != "RESUME" || c.preserveRecordOnExit.Load() ||
		len(c.captureV5Configuration().Filters.Rules) != 0 || bytes.Equal(disk, v5CoordinatorDisk(t, c)) {
		t.Fatal("valid framing lost subsequent command or did not commit immediate bytes")
	}
	// A completely framed, invalid document is recoverable, unlike an uncertain
	// upload boundary. Its body cannot become a human command.
	transport, _ = newYAMLTestClient("---\nschema_version: 4\nrequest_id: bad\nconfiguration: {}\n...\nRESUME\n")
	c.conn, c.reader = transport.conn, transport.reader
	result = s.handleV5MachineCommand(c, "PATCH YAML CONFIG")
	reply := <-c.controlChan
	if result.terminal || !strings.Contains(string(reply.raw), "code: invalid_document") || c.preserveRecordOnExit.Load() {
		t.Fatal("bounded document rejection became terminal or succeeded")
	}
	if line, err := c.readCommandLine(128); err != nil || line != "RESUME" {
		t.Fatal("rejected body escaped framing or consumed the following command")
	}
	result = s.handleV5MachineCommand(c, "GET YAML CONFIG SCHEMA 5 ID wire-get")
	if result.terminal || !result.handled {
		t.Fatal("GET dispatch failed")
	}
	v5ReadbackDocument(t, string((<-c.controlChan).raw))
	if result := s.handleV5MachineCommand(c, "SHOW FILTER"); result.handled {
		t.Fatal("v5 dispatcher claimed a human command")
	}
}

func TestV5MachineCoordinatorTerminalUploadAndHeaderFailures(t *testing.T) {
	for _, mode := range []string{"header", "framing", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			s, c, cfg := v5CoordinatorFixture(t)
			disk := v5CoordinatorSeed(t, c, cfg)
			header, input := "PUT YAML FILTER", "--- EXTRA\nRESUME\n...\n"
			switch mode {
			case "header":
				header, input = "PUT YAML FILTER SCHEMA 5", "---\nRESUME\n...\n"
			case "oversized":
				input = "---\n" + strings.Repeat("x", maxYAMLBytes+1) + "\nRESUME\n...\n"
			}
			transport, conn := newYAMLTestClient(input)
			c.conn, c.reader = transport.conn, transport.reader
			result := s.handleV5MachineCommand(c, header)
			if !result.handled || !result.terminal || !c.preserveRecordOnExit.Load() {
				t.Fatalf("upload/header refusal was not terminal: %+v", result)
			}
			if mode == "header" {
				reply := <-c.controlChan
				if !result.closeQueued || !reply.closeAfter || !strings.Contains(string(reply.raw), "schema_version: 5") ||
					!strings.Contains(string(reply.raw), "code: invalid_header") {
					t.Fatal("terminal header rejection did not queue a complete v5 close frame")
				}
			} else if !conn.closed.Load() || result.closeQueued {
				t.Fatal("uncertain body framing failed to interrupt the connection")
			}
			s.unregisterClient(c)
			if !bytes.Equal(disk, v5CoordinatorDisk(t, c)) {
				t.Fatal("terminal rejection allowed teardown to rewrite the v5 record")
			}
		})
	}
}

func TestV5MachineCoordinatorGETBadHeaderIsNonterminal(t *testing.T) {
	s, c, _ := v5CoordinatorFixture(t)
	for _, line := range []string{"GET YAML FILTER", "GET YAML FILTER SCHEMA 4"} {
		result := s.handleV5MachineCommand(c, line)
		if !result.handled || result.terminal || c.preserveRecordOnExit.Load() {
			t.Fatal("GET schema refusal interrupted ordinary command recovery")
		}
		response := string((<-c.controlChan).raw)
		if !strings.Contains(response, "schema_version: 5") || !strings.Contains(response, "code: invalid_header") {
			t.Fatal(response)
		}
	}
	// Queue overflow may synchronously report/close. Ownership must already be
	// released so teardown or a replacement session can acquire the same stripe.
	for range cap(c.controlChan) {
		c.controlChan <- controlMessage{line: "occupied\n"}
	}
	reportEntered, resume := make(chan struct{}), make(chan struct{})
	s.connectionReporter = func(event ConnectionEvent) {
		if event.Action == "disconnect" {
			close(reportEntered)
			<-resume
		}
	}
	done := make(chan struct{})
	go func() { s.handleV5MachineCommand(c, "GET YAML CONFIG SCHEMA 5 ID full"); close(done) }()
	select {
	case <-reportEntered:
	case <-time.After(3 * time.Second):
		close(resume)
		t.Fatal("queue overflow did not reach the blocked reporter")
	}
	release, err := s.acquireConfiguration(c, true, false, time.Now().Add(time.Second))
	close(resume)
	if err != nil {
		t.Fatal("GET queued its reply while still owning the configuration stripe")
	}
	release()
	waitConfigurationTest(t, done)
}

func TestV5MachineCoordinatorPreparationBoundAndCancellation(t *testing.T) {
	s, c, cfg := v5CoordinatorFixture(t)
	cmd := machineCommand{Verb: "PATCH", Resource: "FILTER"}
	body := []byte("schema_version: 5\nrequest_id: prep\nif_revision: opaque-1\nconfiguration: {rules: []}\n")
	for range 4 {
		release, err := s.acquireYAMLPreparation(c)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	done := make(chan error, 1)
	go func() { _, err := s.prepareV5MachineRequest(c, body, cmd); done <- err }()
	close(c.done)
	select {
	case err := <-done:
		if !errors.Is(err, errClientClosed) {
			t.Fatalf("canceled v5 preparation returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("saturated v5 preparation ignored cancellation")
	}
	if len(s.yamlPreparation) != 4 || !c.captureV5Configuration().Equal(cfg) {
		t.Fatal("canceled v5 preparation consumed a permit or changed preferences")
	}
}

func TestV5MachineCoordinatorWriteReplyReleasesLease(t *testing.T) {
	s, c, _ := v5CoordinatorFixture(t)
	revision := v5CoordinatorRevision(t, c)
	input := fmt.Sprintf("---\nschema_version: 5\nrequest_id: write\nif_revision: %s\nconfiguration: {rules: []}\n...\n", revision)
	transport, _ := newYAMLTestClient(input)
	c.conn, c.reader = transport.conn, transport.reader
	for range cap(c.controlChan) {
		c.controlChan <- controlMessage{line: "occupied\n"}
	}
	reentered := make(chan error, 1)
	s.connectionReporter = func(event ConnectionEvent) {
		if event.Action == "disconnect" {
			release, err := s.acquireConfiguration(c, true, false, time.Now().Add(time.Second))
			if err == nil {
				release()
			}
			reentered <- err
		}
	}
	result := s.handleV5MachineCommand(c, "PATCH YAML FILTER")
	if !result.terminal {
		t.Fatal("reply queue overflow did not terminate the connection")
	}
	select {
	case err := <-reentered:
		if err != nil {
			t.Fatalf("write reply cleanup could not reacquire the released lease: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("write reply failed to report queue overflow")
	}
	record, err := filter.LoadV5UserRecord(c.callsign)
	if err != nil || len(record.Configuration.Filters.Rules) != 0 || c.configurationRevision != 1 {
		t.Fatal("lost acknowledgement lost the completed durable transaction")
	}
}
