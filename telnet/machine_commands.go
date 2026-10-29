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
	err := c.enqueueControl(controlMessage{raw: []byte(response), closeAfter: terminal})
	return err == nil
}

func (s *Server) handleMachineCommand(c *Client, line string) machineDispatch {
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
