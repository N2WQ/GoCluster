// File role: The authenticated connection writer retains ownership from the ON
// acknowledgement through the final json-off flush. Each socket write has the
// ordinary send deadline. A partial record retires the connection.
package telnet

import (
	"errors"
	"io"
	"time"
)

type jsonWriterOutput struct {
	client       *Client
	buf          []byte
	enqueueTimes []time.Time // Borrow the legacy batch timestamps for the same flush-time metric.
	nextSpot     time.Time   // Moves to enqueueTimes when the first bytes of this spot enter a batch.
}

func (o *jsonWriterOutput) Write(p []byte) (int, error) {
	// The spot encoder passes an ordinary complete row in one Write. Start
	// a fresh batch if that row fits by itself but not in the remaining space.
	if len(o.buf) == cap(o.buf) || (len(p) <= cap(o.buf) && len(p) > cap(o.buf)-len(o.buf)) {
		if err := o.Flush(); err != nil {
			return 0, err
		}
	}
	if !o.nextSpot.IsZero() && len(p) > 0 {
		o.enqueueTimes = append(o.enqueueTimes, o.nextSpot)
		o.nextSpot = time.Time{}
	}
	return bufferJSON(o, p)
}
func (o *jsonWriterOutput) WriteString(p string) (int, error) { return bufferJSON(o, p) }
func (o *jsonWriterOutput) literal(s string) error {
	_, err := o.WriteString(s)
	return err
}

// Control strings share the spot counter for malformed source bytes. Count
// once the corresponding string has been encoded into the output stream.
func (o *jsonWriterOutput) quoted(invalid int, err error) error {
	if invalid != 0 && err == nil {
		o.client.server.jsonSpotStats.invalidUTF8.Add(uint64(invalid))
	}
	return err
}

// Appending to the fixed writer batch avoids a second buffer for large rows.
func bufferJSON[S ~[]byte | ~string](o *jsonWriterOutput, p S) (int, error) {
	n := 0
	for len(p) > 0 {
		if len(o.buf) == cap(o.buf) {
			if err := o.Flush(); err != nil {
				return n, err
			}
		}
		size := min(len(p), cap(o.buf)-len(o.buf))
		o.buf = append(o.buf, p[:size]...)
		p = p[size:]
		n += size
	}
	return n, nil
}

func (o *jsonWriterOutput) Flush() error {
	if len(o.buf) == 0 {
		return nil
	}
	c := o.client
	if configurationTransferClosed(c) {
		return errClientClosed
	}
	if err := c.setWriteDeadline(time.Now().Add(defaultSendDeadline)); err != nil {
		return err
	}
	start := time.Now()
	var n int
	var err error
	if c.web != nil {
		n, err = c.web.writeJSON(o.buf)
	} else {
		n, err = c.conn.Write(o.buf)
	}
	c.clearWriteDeadline()
	if err == nil && n != len(o.buf) {
		err = io.ErrShortWrite
	}
	if err == nil {
		flushed := time.Now()
		c.server.observeWriteStallLatency(flushed.Sub(start))
		for _, enqueued := range o.enqueueTimes {
			c.server.observeFirstByteLatency(flushed.Sub(enqueued))
		}
		o.enqueueTimes = o.enqueueTimes[:0]
	}
	o.buf = o.buf[:0]
	return err
}

type jsonSessionWriter struct {
	out   jsonWriterOutput
	spots jsonSpotEncoder
}

func (w *jsonSessionWriter) live(env *spotEnvelope) error {
	c := w.out.client
	if env == nil || env.spot == nil || c.suppressSpotForReadPause(env, c.server.now()) {
		return nil
	}
	if len(w.out.enqueueTimes) > 0 && len(w.out.enqueueTimes) == cap(w.out.enqueueTimes) {
		if err := w.out.Flush(); err != nil {
			return err
		}
	}
	w.out.nextSpot = env.enqueueAt
	err := w.spots.writeLive(&w.out, c, env)
	w.out.nextSpot = time.Time{} // A refused record produced no first byte.
	if errors.Is(err, errJSONInvalidSpot) {
		return w.out.literal("[\"notice\",\"omitted\",\"Invalid spot frequency.\"]\n")
	}
	return err
}

func writeJSONLines[S ~string | ~[]byte](out *jsonWriterOutput, quote *jsonQuoteScratch, text S) error {
	for len(text) > 0 {
		i := 0
		for i < len(text) && text[i] != '\n' && text[i] != '\r' {
			i++
		}
		if err := out.literal(`["text",`); err != nil {
			return err
		}
		if err := out.quoted(writeJSONString(out, quote, text[:i])); err != nil {
			return err
		}
		if err := out.literal("]\n"); err != nil {
			return err
		}
		if i == len(text) {
			break
		}
		if text[i] == '\r' && i+1 < len(text) && text[i+1] == '\n' {
			i++
		}
		text = text[i+1:]
	}
	return nil
}

func (w *jsonSessionWriter) writeControl(msg controlMessage) error {
	switch msg.kind {
	case controlKeepalive:
		return w.out.literal("[\"keepalive\"]\n")
	case controlYAML, controlYAMLTerminal:
		return w.out.quoted(writeJSONYAML(&w.out, &w.spots.buf, msg.raw))
	case controlRefused, controlSessionReplaced:
		prefix := `["notice","refused",`
		if msg.kind == controlSessionReplaced {
			prefix = `["notice","session-replaced",`
		}
		if err := w.out.literal(prefix); err != nil {
			return err
		}
		if err := w.out.quoted(writeJSONString(&w.out, &w.spots.buf, trimJSONLineEnding(msg.line))); err != nil {
			return err
		}
		return w.out.literal("]\n")
	case controlAdvisory:
		if err := w.out.literal("[\"notice\",\"" + msg.noticeCode + "\","); err != nil {
			return err
		}
		if err := w.out.quoted(writeJSONString(&w.out, &w.spots.buf, trimJSONLineEnding(msg.line))); err != nil {
			return err
		}
		return w.out.literal(",\"" + msg.noticeVersion + "\"]\n")
	case controlBulletin:
		if err := w.out.literal(`["bulletin",`); err != nil {
			return err
		}
		if err := w.out.quoted(writeJSONString(&w.out, &w.spots.buf, msg.bulletinKind)); err != nil {
			return err
		}
		if err := w.out.literal("," + jsonTimeToken(msg.bulletinAt) + ","); err != nil {
			return err
		}
		if msg.bulletinFrom == "" {
			if err := w.out.literal("null"); err != nil {
				return err
			}
		} else if err := w.out.quoted(writeJSONString(&w.out, &w.spots.buf, msg.bulletinFrom)); err != nil {
			return err
		}
		if err := w.out.literal(","); err != nil {
			return err
		}
		if err := w.out.quoted(writeJSONString(&w.out, &w.spots.buf, trimJSONLineEnding(msg.line))); err != nil {
			return err
		}
		return w.out.literal("]\n")
	default:
		if msg.raw != nil {
			return writeJSONLines(&w.out, &w.spots.buf, msg.raw)
		}
		return writeJSONLines(&w.out, &w.spots.buf, msg.line)
	}
}

func trimJSONLineEnding(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\n' {
		s = s[:len(s)-1]
	}
	if len(s) > 0 && s[len(s)-1] == '\r' {
		s = s[:len(s)-1]
	}
	return s
}

func (w *jsonSessionWriter) control(msg controlMessage) (off, terminal bool, err error) {
	c := w.out.client
	if msg.jsonPage != nil {
		return false, false, msg.jsonPage.write(c, &w.out, &w.spots, w.live)
	}
	if msg.jsonSwitch != nil {
		select {
		case <-c.retired:
			// The duplicate-login notice still belongs to the current format.
			return false, false, nil
		default:
		}
		if err := w.out.literal("[\"notice\",\"json-off\",\"\"]\n"); err != nil {
			return false, false, err
		}
		if err := w.out.Flush(); err != nil {
			return false, false, err
		}
		if c.inputWire != nil {
			c.inputWire.muteCodec.Store(false)
		}
		c.jsonRequested.Store(false)
		close(msg.jsonSwitch.flushed)
		return true, false, nil
	}
	if msg.yamlReady != nil {
		var ok bool
		msg, ok = c.receiveJSONMachineReply(msg)
		if !ok {
			return false, true, errClientClosed
		}
	}
	err = w.writeControl(msg)
	if err == nil {
		err = w.out.Flush()
	}
	msg.raw, msg.line = nil, ""
	msg.configurationReservation.Release()
	c.releaseBulletinReservations(msg.bulletinReservation)
	if err == nil {
		if msg.yamlFlushed != nil {
			close(msg.yamlFlushed)
		}
		if msg.resetNotice != nil {
			msg.resetNotice.signal()
		}
		c.completeHumanReadback(msg.readback, c.server.now())
	}
	return false, msg.closeAfter, err
}

// runJSONWriter borrows the ordinary batch until OFF or terminal teardown.
// The reader is waiting while the gate changes in both directions. Thus codec
// writes from before ON finish before the ack, and early input after ON cannot
// produce echo bytes inside the stream.
func (c *Client) runJSONWriter(on *jsonFormatSwitch, batch []byte, enqueueTimes []time.Time, shutdown <-chan struct{}) (bool, error) {
	w := jsonSessionWriter{out: jsonWriterOutput{client: c, buf: batch, enqueueTimes: enqueueTimes}}
	if c.inputWire != nil {
		c.inputWire.muteCodec.Store(true)
	}
	c.jsonRequested.Store(true)
	c.enterJSONPausePolicy(c.server.now())
	if err := w.out.literal("JSON ON 1\r\n"); err != nil {
		return true, err
	}
	if err := w.out.Flush(); err != nil {
		return true, err
	}
	if err := c.writeJSONHello(&w.out, &w.spots.buf); err != nil {
		return true, err
	}
	if err := w.out.Flush(); err != nil {
		return true, err
	}
	close(on.flushed)
	for {
		// Drain immediately eligible spots into a bounded batch. Always give
		// a queued control its FIFO turn before selecting the next live spot.
		var msg controlMessage
		select {
		case <-c.done:
			return true, nil
		case <-shutdown:
			return true, nil
		case msg = <-c.controlChan:
		default:
			select {
			case msg = <-c.controlChan:
			case <-c.notices.wake:
				msg = c.nextSessionNotice()
				if msg.line == "" {
					continue
				}
			case env := <-c.spotChan:
				if err := w.live(env); err != nil {
					return true, err
				}
				continue
			default:
				if err := w.out.Flush(); err != nil {
					return true, err
				}
				select {
				case <-c.done:
					return true, nil
				case <-shutdown:
					return true, nil
				case msg = <-c.controlChan:
				case <-c.notices.wake:
					msg = c.nextSessionNotice()
					if msg.line == "" {
						continue
					}
				case env := <-c.spotChan:
					if err := w.live(env); err != nil {
						return true, err
					}
					continue
				}
			}
		}
		off, terminal, err := w.control(msg)
		if err != nil || off || terminal {
			return terminal, err
		}
	}
}
