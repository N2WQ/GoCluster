// File role: Dispatches framed machine read/write commands and atomic proposals.
// A complete reply is one control message. Upload failures never return payload
// tails to ordinary command dispatch; successful YAML commands do not touch pause.
package telnet

import (
	"errors"
)

// machineDispatch reports whether a terminal reply owns the writer's close.
// The command loop then exits without interrupting that queued final frame.
type machineDispatch struct {
	handled     bool
	terminal    bool
	closeQueued bool
}

func (s *Server) queueMachineReply(c *Client, response string, terminal bool) bool {
	if terminal {
		c.preserveRecordOnExit.Store(true)
	}
	msg := controlMessage{kind: machineReplyKind(terminal), raw: []byte(response), closeAfter: terminal}
	// Unreserved replies (including a large-transfer busy error) use the
	// command reader's one implicit slot. A client cannot pile up a queue of
	// these while the writer is busy with a history or configuration page.
	if !c.jsonRequested.Load() {
		return c.enqueueControl(msg) == nil
	}
	msg.yamlFlushed = make(chan struct{})
	if err := c.admitJSONMachineReply(msg); err != nil {
		if errors.Is(err, errControlQueueFull) {
			_ = c.controlQueueFull() //nolint:errcheck // The existing policy reports and disconnects.
		}
		return false
	}
	msg.raw = nil // Only the writer retains the charged payload while waiting.
	select {
	case <-msg.yamlFlushed:
		return true
	case <-c.done:
	case <-s.shutdown:
	}
	// A delivered terminal frame can close the socket before this reader
	// resumes. Its receipt still owns that close.
	select {
	case <-msg.yamlFlushed:
		return true
	default:
		return false
	}
}

// Both bounded machine output paths share the writer teardown fence. A
// replacement may retire an open socket while it still owes its FIFO notices.
func (c *Client) admitJSONMachineReply(msg controlMessage) error {
	c.bulletinResponseMu.Lock()
	defer c.bulletinResponseMu.Unlock()
	if configurationTransferClosed(c) {
		return errClientClosed
	}
	select {
	case c.controlChan <- msg:
		return nil
	default:
		return errControlQueueFull
	}
}

func machineReplyKind(terminal bool) controlKind {
	if terminal {
		return controlYAMLTerminal
	}
	return controlYAML
}

// Admit both the expensive output preparation and its FIFO position before
// a JSON upload is allowed to commit. The reader owns the reservation until
// the writer takes the result; cancellation cannot leak or release a live
// preparation early. The selected schema's renderer enforces its raw limit.
func (s *Server) runJSONMachineRequest(c *Client, cmd machineCommand, id string, apply func() string) machineDispatch {
	owner, err := c.reserveConfigurationOutput()
	if err != nil {
		render := renderV6YAMLCommandError
		if cmd.SchemaVersion == 7 {
			render = renderV7YAMLCommandError
		}
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, render(cmd.Resource, id, "", v7MachineErrorCode(err), err.Error()), false)}
	}
	ready := make(chan controlMessage)
	if err := c.admitJSONMachineReply(controlMessage{kind: controlYAML, yamlReady: ready}); err != nil {
		owner.Release()
		if errors.Is(err, errControlQueueFull) {
			_ = c.controlQueueFull() //nolint:errcheck // Existing disconnect/report policy; command loop exits below.
		}
		return machineDispatch{handled: true, terminal: true}
	}
	raw := []byte(apply())
	select {
	case ready <- controlMessage{kind: controlYAML, raw: raw, configurationReservation: owner}:
		return machineDispatch{handled: true}
	case <-c.done:
	case <-s.shutdown:
	}
	owner.Release()
	return machineDispatch{handled: true, terminal: true}
}

func (c *Client) receiveJSONMachineReply(pending controlMessage) (controlMessage, bool) {
	var shutdown <-chan struct{}
	if c.server != nil {
		shutdown = c.server.shutdown
	}
	select {
	case msg := <-pending.yamlReady:
		return msg, true
	case <-c.done:
	case <-shutdown:
	}
	return controlMessage{}, false
}

func (s *Server) handleMachineCommand(c *Client, line string) machineDispatch {
	if v7MachineActive() {
		return s.handleV7MachineCommand(c, line)
	}
	return s.handleV6MachineCommand(c, line)
}

func (s *Server) acquireYAMLPreparation(c *Client) (func(), error) {
	s.yamlPreparationOnce.Do(func() { s.yamlPreparation = make(chan struct{}, 4) })
	select {
	case s.yamlPreparation <- struct{}{}:
	case <-c.done:
		return nil, errClientClosed
	case <-s.shutdown:
		return nil, errClientClosed
	}
	release := func() { <-s.yamlPreparation }
	select {
	case <-c.done:
		release()
		return nil, errClientClosed
	case <-s.shutdown:
		release()
		return nil, errClientClosed
	default:
	}
	return release, nil
}

func machineErrorCode(err error) string {
	switch {
	case errors.Is(err, errReadbackTooLarge):
		return "response_too_large"
	case errors.Is(err, errProtectedRecord):
		return "record_protected"
	case errors.Is(err, errClientClosed), errors.Is(err, errRetiredConfiguration):
		return "session_unavailable"
	default:
		return "invalid_configuration"
	}
}
