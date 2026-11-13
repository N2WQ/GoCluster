// File role: Connection-only CC11 selection and bounded output diagnostics.
// The configuration lease owns requested mode; only the writer's queued
// acknowledgement changes effective mode. Neither is a saved preference.
package telnet

import (
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

type spotFormatAction uint8

const (
	spotFormatUnchanged spotFormatAction = iota
	spotFormatText
	spotFormatVE7CC
)

const (
	ve7ccLiveFailure    = "VE7CC spot omitted: invalid or oversized native record.\r\n"
	ve7ccHistoryFailure = "VE7CC history failed: invalid or oversized native response; request fewer spots.\n"
	ve7ccHistoryBusy    = "VE7CC history is busy; retry the request or continuation.\n"
)

// Fixed counters and clocks die with the connection. No free-text keys,
// historical callsigns, timer or diagnostic queue are retained.
type ve7ccDiagnostics struct {
	sanitized  atomic.Uint64
	missing    atomic.Uint64
	conflict   atomic.Uint64
	refused    atomic.Uint64
	lastLog    atomic.Int64
	lastNotice atomic.Int64
}

func (s *Server) ve7ccContext(historical bool) ve7ccContext {
	context := ve7ccContext{historical: historical}
	if s != nil {
		context.node = s.clusterCall
		if s.ctyLookup != nil {
			context.cty = s.ctyLookup()
		}
	}
	return context
}

// Saved-record protection does not prohibit this nonpersistent selection.
// Terminal machine input exits in the command loop before reaching this handler.
func (s *Server) handleVE7CCCommand(c *Client, line string) (handled, proceed bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 || (!strings.EqualFold(fields[0], "SET/VE7CC") && !strings.EqualFold(fields[0], "UNSET/VE7CC")) {
		return false, true
	}
	if c == nil || s == nil {
		return true, false
	}
	if len(fields) != 1 {
		return true, s.sendClientMessage(c, "Usage: "+strings.ToUpper(fields[0])+"\n", "VE7CC usage")
	}
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return true, s.finishVE7CCSend(c, err, "selection")
	}
	enable := strings.EqualFold(fields[0], "SET/VE7CC")
	state, action := "disabled", spotFormatText
	if enable {
		state, action = "enabled", spotFormatVE7CC
	}
	body := "VE7CC " + state + " for this session.\n"
	if c.ve7ccEnabled == enable {
		body = "VE7CC already " + state + " for this session.\n"
	}
	c.bulletinResponseMu.Lock()
	if configurationTransferClosed(c) {
		err = errClientClosed
	} else {
		select {
		case c.controlChan <- controlMessage{line: body, spotFormat: action}:
			c.ve7ccEnabled = enable
		default:
			err = errControlQueueFull
		}
	}
	c.bulletinResponseMu.Unlock()
	release()
	return true, s.finishVE7CCSend(c, err, "selection")
}

func (s *Server) finishVE7CCSend(c *Client, err error, purpose string) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, errControlQueueFull) {
		return c.controlQueueFull() == nil
	}
	if !isExpectedClientSendErr(err) && !errors.Is(err, errRetiredConfiguration) {
		log.Printf("VE7CC response failed for %s (%s): %v", c.identity(), purpose, err)
		c.close("VE7CC response failure")
	}
	return false
}

func ve7ccDiagnosticDue(clock *atomic.Int64, now time.Time) bool {
	stamp := now.UnixNano()
	previous := clock.Load()
	if previous != 0 && stamp-previous < int64(30*time.Second) {
		return false
	}
	stored := stamp
	if stored == 0 {
		stored = 1 // Zero denotes an unused clock, not a repeatedly due epoch.
	}
	return clock.CompareAndSwap(previous, stored)
}

func (c *Client) allowVE7CCRefusalNotice(now time.Time) bool {
	return ve7ccDiagnosticDue(&c.ve7ccDiagnostics.lastNotice, now)
}

func (c *Client) reportVE7CC(notes ve7ccFormatNotes, refused bool) {
	if notes.sanitized {
		c.ve7ccDiagnostics.sanitized.Add(1)
	}
	if notes.unavailable {
		c.ve7ccDiagnostics.missing.Add(1)
	}
	if notes.conflict {
		c.ve7ccDiagnostics.conflict.Add(1)
	}
	if refused {
		c.ve7ccDiagnostics.refused.Add(1)
	}
	if !notes.sanitized && !notes.unavailable && !notes.conflict && !refused {
		return
	}
	now := time.Now().UTC()
	if c.server != nil {
		now = c.server.now()
	}
	if ve7ccDiagnosticDue(&c.ve7ccDiagnostics.lastLog, now) {
		log.Printf("VE7CC output for %s: sanitized=%d unavailable=%d conflict=%d refused=%d",
			c.identity(), c.ve7ccDiagnostics.sanitized.Load(), c.ve7ccDiagnostics.missing.Load(),
			c.ve7ccDiagnostics.conflict.Load(), c.ve7ccDiagnostics.refused.Load())
	}
}
