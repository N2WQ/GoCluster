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

func TestV6MachineCoordinatorActualTransport(t *testing.T) {
	s, c, cfg := v6CoordinatorFixture(t)
	disk := v6CoordinatorSeed(t, c, cfg)
	revision := v6CoordinatorRevision(t, c)
	input := fmt.Sprintf("---\r\nschema_version: 6\r\nrequest_id: wire-1\r\nif_revision: %s\r\nconfiguration:\r\n  rules: []\r\n...\r\nRESUME\r\n", revision)
	transport, _ := newYAMLTestClient(input)
	c.conn, c.reader = transport.conn, transport.reader
	result := s.handleV6MachineCommand(c, "PATCH YAML FILTER")
	if !result.handled || result.terminal || result.closeQueued {
		t.Fatalf("actual valid upload failed: %+v", result)
	}
	select {
	case reply := <-c.controlChan:
		v6CoordinatorSuccess(t, string(reply.raw), true)
	default:
		t.Fatal("actual coordinator did not queue a reply")
	}
	line, err := c.readCommandLine(128)
	if err != nil || line != "RESUME" || c.preserveRecordOnExit.Load() ||
		len(c.captureV6Configuration().Filters.Rules) != 0 || bytes.Equal(disk, v6CoordinatorDisk(t, c)) {
		t.Fatal("valid framing lost subsequent command or did not commit immediate bytes")
	}
	// A completely framed, invalid document is recoverable, unlike an uncertain
	// upload boundary. Its body cannot become a human command.
	transport, _ = newYAMLTestClient("---\nschema_version: 5\nrequest_id: bad\nconfiguration: {}\n...\nRESUME\n")
	c.conn, c.reader = transport.conn, transport.reader
	result = s.handleV6MachineCommand(c, "PATCH YAML CONFIG")
	reply := <-c.controlChan
	if result.terminal || !strings.Contains(string(reply.raw), "code: invalid_document") || c.preserveRecordOnExit.Load() {
		t.Fatal("bounded document rejection became terminal or succeeded")
	}
	if line, err := c.readCommandLine(128); err != nil || line != "RESUME" {
		t.Fatal("rejected body escaped framing or consumed the following command")
	}
	// Even a false retired toggle is an invalid schema-6 proposal. Preserve the
	// current live/durable state and consume exactly its complete upload frame.
	before, saved := snapshotBooleanCommand(c), v6CoordinatorDisk(t, c)
	transport, _ = newYAMLTestClient(fmt.Sprintf("---\nschema_version: 6\nrequest_id: retired\nif_revision: %s\nconfiguration: {nearby_enabled: false}\n...\nRESUME\n", v6CoordinatorRevision(t, c)))
	c.conn, c.reader = transport.conn, transport.reader
	result = s.handleV6MachineCommand(c, "PATCH YAML FILTER")
	reply = <-c.controlChan
	if result.terminal || !strings.Contains(string(reply.raw), "code: invalid_document") || !strings.Contains(string(reply.raw), "nearby_enabled") {
		t.Fatal("retired toggle was accepted or broke framed recovery")
	}
	before.assertUnchanged(t, c)
	if line, err := c.readCommandLine(128); err != nil || line != "RESUME" || !bytes.Equal(saved, v6CoordinatorDisk(t, c)) {
		t.Fatal("retired toggle changed durable bytes or consumed the next command")
	}
	result = s.handleV6MachineCommand(c, "GET YAML CONFIG SCHEMA 6 ID wire-get")
	if result.terminal || !result.handled {
		t.Fatal("GET dispatch failed")
	}
	v6ReadbackDocument(t, string((<-c.controlChan).raw))
	if result := s.handleV6MachineCommand(c, "SHOW FILTER"); result.handled {
		t.Fatal("v6 dispatcher claimed a human command")
	}
}

func TestV6MachineCoordinatorTerminalUploadAndHeaderFailures(t *testing.T) {
	for _, mode := range []string{"header", "framing", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			s, c, cfg := v6CoordinatorFixture(t)
			disk := v6CoordinatorSeed(t, c, cfg)
			header, input := "PUT YAML FILTER", "--- EXTRA\nRESUME\n...\n"
			switch mode {
			case "header":
				header, input = "PUT YAML FILTER SCHEMA 6", "---\nRESUME\n...\n"
			case "oversized":
				input = "---\n" + strings.Repeat("x", maxYAMLBytes+1) + "\nRESUME\n...\n"
			}
			transport, conn := newYAMLTestClient(input)
			c.conn, c.reader = transport.conn, transport.reader
			result := s.handleV6MachineCommand(c, header)
			if !result.handled || !result.terminal || !c.preserveRecordOnExit.Load() {
				t.Fatalf("upload/header refusal was not terminal: %+v", result)
			}
			if mode == "header" {
				reply := <-c.controlChan
				if !result.closeQueued || !reply.closeAfter || !strings.Contains(string(reply.raw), "schema_version: 6") ||
					!strings.Contains(string(reply.raw), "code: invalid_header") {
					t.Fatal("terminal header rejection did not queue a complete v6 close frame")
				}
			} else if !conn.closed.Load() || result.closeQueued {
				t.Fatal("uncertain body framing failed to interrupt the connection")
			}
			s.unregisterClient(c)
			if !bytes.Equal(disk, v6CoordinatorDisk(t, c)) {
				t.Fatal("terminal rejection allowed teardown to rewrite the v6 record")
			}
		})
	}
}

func TestV6MachineCoordinatorGETBadHeaderIsNonterminal(t *testing.T) {
	s, c, _ := v6CoordinatorFixture(t)
	for _, line := range []string{"GET YAML FILTER", "GET YAML FILTER SCHEMA 4", "GET YAML FILTER SCHEMA 5"} {
		result := s.handleV6MachineCommand(c, line)
		if !result.handled || result.terminal || c.preserveRecordOnExit.Load() {
			t.Fatal("GET schema refusal interrupted ordinary command recovery")
		}
		response := string((<-c.controlChan).raw)
		if !strings.Contains(response, "schema_version: 6") || !strings.Contains(response, "code: invalid_header") {
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
	go func() { s.handleV6MachineCommand(c, "GET YAML CONFIG SCHEMA 6 ID full"); close(done) }()
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

func TestV6MachineCoordinatorPreparationBoundAndCancellation(t *testing.T) {
	s, c, cfg := v6CoordinatorFixture(t)
	cmd := machineCommand{Verb: "PATCH", Resource: "FILTER"}
	body := []byte("schema_version: 6\nrequest_id: prep\nif_revision: opaque-1\nconfiguration: {rules: []}\n")
	for range 4 {
		release, err := s.acquireYAMLPreparation(c)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	done := make(chan error, 1)
	go func() { _, err := s.prepareV6MachineRequest(c, body, cmd); done <- err }()
	close(c.done)
	select {
	case err := <-done:
		if !errors.Is(err, errClientClosed) {
			t.Fatalf("canceled v6 preparation returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("saturated v6 preparation ignored cancellation")
	}
	if len(s.yamlPreparation) != 4 || !c.captureV6Configuration().Equal(cfg) {
		t.Fatal("canceled v6 preparation consumed a permit or changed preferences")
	}
}

func TestV6MachineCoordinatorWriteReplyReleasesLease(t *testing.T) {
	s, c, _ := v6CoordinatorFixture(t)
	revision := v6CoordinatorRevision(t, c)
	input := fmt.Sprintf("---\nschema_version: 6\nrequest_id: write\nif_revision: %s\nconfiguration: {rules: []}\n...\n", revision)
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
	result := s.handleV6MachineCommand(c, "PATCH YAML FILTER")
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
	record, err := filter.LoadV6UserRecord(c.callsign)
	if err != nil || len(record.Configuration.Filters.Rules) != 0 || c.configurationRevision != 1 {
		t.Fatal("lost acknowledgement lost the completed durable transaction")
	}
}
