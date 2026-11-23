// The human command reader owns echo and waits outside the configuration lease.
// The writer acknowledges a switch only after the client can observe its final
// frame. Reconnection constructs a new Client with both formats disabled.
package telnet

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

type jsonFormatSwitch struct {
	flushed chan struct{}
}

func (s *Server) sendJSONRefusal(c *Client, reason string) bool {
	msg := controlMessage{kind: controlRefused, line: "JSON REFUSED " + reason + "\n"}
	return s.finishVE7CCSend(c, c.enqueueControl(msg), "JSON refusal")
}

func (s *Server) handleJSONCommand(c *Client, line string) (bool, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SET") || !strings.EqualFold(fields[1], "JSON") {
		return false, true
	}
	for _, token := range fields[2:] {
		if strings.EqualFold(token, "DEFLATE") {
			return true, s.sendJSONRefusal(c, "DEFLATE not available")
		}
	}
	on := len(fields) >= 3 && strings.EqualFold(fields[2], "ON")
	off := len(fields) == 3 && strings.EqualFold(fields[2], "OFF")
	if on && len(fields) == 5 && strings.EqualFold(fields[3], "VERSION") {
		version, err := strconv.ParseUint(fields[4], 10, 32)
		if err != nil || version != 1 {
			return true, s.sendJSONRefusal(c, "unsupported version")
		}
	} else if !off && (!on || len(fields) != 3) {
		return true, s.sendJSONRefusal(c, "usage: SET JSON ON [VERSION 1] or SET JSON OFF")
	}

	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		if errors.Is(err, errRetiredConfiguration) {
			s.waitForRetiredWriter(c)
		}
		return true, s.finishVE7CCSend(c, err, "JSON selection")
	}
	if on && c.ve7ccEnabled {
		release()
		return true, s.sendJSONRefusal(c, "VE7CC is on; send UNSET/VE7CC first")
	}
	if c.jsonRequested.Load() == on {
		release()
		if on {
			return true, s.sendJSONRefusal(c, "already on")
		}
		return true, s.sendClientMessage(c, "JSON already off.\n", "JSON selection")
	}
	sw := &jsonFormatSwitch{flushed: make(chan struct{})}
	msg := controlMessage{jsonSwitch: sw}
	if off {
		msg.kind = controlJSONOff
	}
	c.bulletinResponseMu.Lock()
	if configurationTransferClosed(c) {
		err = errClientClosed
	} else {
		// Only this reader accesses echoInput. No next read (including
		// deferred codec negotiation) can begin before the writer's receipt.
		c.echoInput = false
		select {
		case c.controlChan <- msg:
		default:
			err = errControlQueueFull
		}
	}
	c.bulletinResponseMu.Unlock()
	release()
	if err != nil {
		return true, s.finishVE7CCSend(c, err, "JSON switch")
	}
	select {
	case <-sw.flushed:
		if s.commandSessionClosed(c) {
			return true, false
		}
		if off {
			c.echoInput = c.web == nil && s.echoMode == telnetEchoServer
		}
		return true, true
	case <-c.retired:
		s.waitForRetiredWriter(c)
	case <-s.shutdown:
	case <-c.done:
	case <-c.writerStopped:
	}
	return true, false
}

// writeJSONHello runs with the raw wire exclusively owned by the writer.
// Streaming node/login strings also bounds scratch for long operator labels.
func (c *Client) writeJSONHello(out *jsonWriterOutput, quote *jsonQuoteScratch) error {
	if err := out.literal(`["hello",1,{"node":`); err != nil {
		return err
	}
	if err := out.quoted(writeJSONString(out, quote, c.server.clusterCall)); err != nil {
		return err
	}
	if err := out.literal(`,"login":`); err != nil {
		return err
	}
	if err := out.quoted(writeJSONString(out, quote, c.callsign)); err != nil {
		return err
	}
	timeToken := jsonTimeToken(c.server.now())
	if err := out.literal(`,"server":"GoCluster","time":` + timeToken + `,"limits":`); err != nil {
		return err
	}
	// Limits consist of startup-bounded decimal digits and fixed field names.
	var limits [256]byte
	if _, err := out.Write(c.server.jsonLimits.appendHelloLimits(limits[:0])); err != nil {
		return err
	}
	return out.literal("}]\n" + jsonSpotSchemaLine + jsonStationSchemaLine)
}

func jsonTimeToken(t time.Time) string {
	if t.IsZero() {
		return "null"
	}
	return `"` + strconv.FormatInt(t.UnixNano(), 10) + `"`
}
