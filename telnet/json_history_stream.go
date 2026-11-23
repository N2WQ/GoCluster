// File role: Deliver one bounded JSON history page, then publish its cursor
// only after the client can observe the he record. The queue transfers a
// single owner; the socket writer or its final queue drain releases the rows.
package telnet

import (
	"errors"
	"io"
	"strconv"
)

type jsonHistoryDelivery struct {
	rows                jsonHistoryRows
	next                *historySearch
	generation          uint64
	oldToken, end       string
	older, unreadable   bool
	hadMatches, flushed bool
	done                chan struct{}
}

// empty is called before announcing a page. A declined scan contributes
// neither rows nor a candidate continuation.
func (p *jsonHistoryDelivery) empty(end string) {
	p.rows.release()
	p.end, p.next = end, nil
	if end == "restart" {
		p.unreadable, p.hadMatches = false, false
	}
}

func (p *jsonHistoryDelivery) discard() {
	p.rows.release()
	p.next = nil
	close(p.done)
}

func (p *jsonHistoryDelivery) validLocked(c *Client) bool {
	select {
	case <-c.retired:
		// The writer must finish this response and then deliver the queued
		// duplicate-login notice before closing the retired connection.
		return false
	default:
	}
	return !c.historyClosed && c.historyGeneration == p.generation &&
		(!p.older || c.historySearch != nil && c.historySearch.token == p.oldToken)
}

func (p *jsonHistoryDelivery) valid(c *Client) bool {
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	return p.validLocked(c)
}

// Both a real transport and the flush-boundary tests implement this contract.
// Flush makes all previously written record bytes visible or returns an error.
type jsonHistoryOutput interface {
	io.Writer
	Flush() error
}

func (p *jsonHistoryDelivery) write(c *Client, out jsonHistoryOutput, encoder *jsonSpotEncoder, live func(*spotEnvelope) error) error {
	defer p.discard()
	if !p.valid(c) {
		p.empty("restart")
	}
	var control [160]byte // includes the generated 34-byte ASCII cursor
	buf := append(control[:0], `["hb",{"count":`...)
	buf = strconv.AppendInt(buf, int64(p.rows.count), 10)
	buf = append(buf, `,"older":`...)
	buf = strconv.AppendBool(buf, p.older)
	buf = append(buf, '}', ']', '\n')
	if err := writeHistoryControl(out, buf); err != nil {
		return err
	}

	// The first row is visible promptly. Thereafter ordinary transport batches
	// (also bounded by the writer's byte limit) are flushed at least every
	// eight rows. Offer a live spot between rows. At the final boundary,
	// drain exactly the already queued prefix: a sustained producer cannot
	// postpone the last history row, and an admitted live spot behind older
	// entries can still cross the wire before the page ends.
	const rowsPerBatch = 8
	for row, sent := p.rows.first, 0; row != nil; row = row.next {
		select {
		case <-c.done:
			return errClientClosed
		case <-c.server.shutdown:
			return errClientClosed
		default:
		}
		if !p.valid(c) {
			p.end, p.next = "failed", nil
			break
		}
		if sent > 0 {
			opportunities := 1
			if row.next == nil {
				opportunities = max(1, len(c.spotChan))
			}
			for range opportunities {
				select {
				case env := <-c.spotChan:
					if err := live(env); err != nil {
						return err
					}
				default:
				}
			}
		}
		if err := encoder.writeHistory(out, c, row); err != nil {
			if errors.Is(err, errJSONInvalidSpot) {
				// Semantic validation precedes the first byte of each row.
				p.end, p.next = "failed", nil
				break
			}
			return err
		}
		sent++
		if sent == 1 || sent%rowsPerBatch == 0 {
			if err := out.Flush(); err != nil {
				return err
			}
		}
	}
	// The final history row itself may span several socket writes. Honor
	// already-admitted arrivals during that row before committing he.
	if p.rows.count > 0 && p.end != "failed" {
		for range len(c.spotChan) {
			if err := live(<-c.spotChan); err != nil {
				return err
			}
		}
	}
	if p.end != "restart" && !p.valid(c) {
		p.end, p.next = "failed", nil
	}
	buf = append(control[:0], `["he","`...)
	buf = append(buf, p.end...)
	buf = append(buf, '"', ',')
	if p.next == nil {
		buf = append(buf, "null"...)
	} else {
		buf = strconv.AppendQuote(buf, p.next.token)
	}
	buf = append(buf, `,{"unreadable":`...)
	buf = strconv.AppendBool(buf, p.unreadable)
	buf = append(buf, `,"hadMatches":`...)
	buf = strconv.AppendBool(buf, p.hadMatches)
	buf = append(buf, '}', ']', '\n')
	if err := writeHistoryControl(out, buf); err != nil {
		return err
	}
	c.historyMu.Lock()
	if p.validLocked(c) && (p.end == "count" || p.end == "budget" || p.end == "exhausted") {
		c.historySearch = p.next
	}
	c.historyMu.Unlock()
	p.flushed = true
	return nil
}

func writeHistoryControl(out jsonHistoryOutput, buf []byte) error {
	n, err := out.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return out.Flush()
}
