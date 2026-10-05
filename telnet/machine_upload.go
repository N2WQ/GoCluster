package telnet

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	maxYAMLBytes      = 65_536
	yamlUploadTimeout = 30 * time.Second
)

var (
	errYAMLUploadDeadline = errors.New("YAML upload deadline exceeded")
	errYAMLUploadFraming  = errors.New("incomplete or unreliable YAML framing")
	errYAMLUploadTooLarge = errors.New("YAML body exceeds 65536-byte limit")
)

type yamlWatchdog interface {
	Stop() bool
}

// yamlReceptionHooks is passed to one reception only. Tests can delay a callback
// or advance its clock without process-wide hooks or additional worker goroutines.
type yamlReceptionHooks struct {
	now       func() time.Time
	afterFunc func(time.Duration, func()) yamlWatchdog
}

// yamlReception owns exactly one timer and its callback-completion channel. The
// mutex linearizes acceptance against expiry; cleanup joins a callback that has
// started before returning, so no retired callback can close a later reception.
type yamlReception struct {
	client   *Client
	deadline time.Time
	now      func() time.Time
	mu       sync.Mutex
	finished bool
	expired  bool
	done     chan struct{}
	timer    yamlWatchdog
}

func (c *Client) receiveYAMLBody(deadline time.Time) ([]byte, error) {
	return c.receiveYAMLBodyWithHooks(deadline, yamlReceptionHooks{
		now: time.Now,
		afterFunc: func(duration time.Duration, callback func()) yamlWatchdog {
			return time.AfterFunc(duration, callback)
		},
	})
}

// receiveYAMLBodyWithHooks preserves body bytes after Telnet negotiation, including
// their LF/CRLF endings. Body storage never exceeds the advertised limit; the end
// marker needs only a three-byte probe. Every failure interrupts the connection
// before returning, so partially consumed payloads cannot reenter command dispatch.
func (c *Client) receiveYAMLBodyWithHooks(deadline time.Time, hooks yamlReceptionHooks) (body []byte, err error) {
	defer func() {
		if err != nil {
			c.preserveRecordOnExit.Store(true)
			c.interrupt()
			body = nil
		}
	}()
	if !hooks.now().Before(deadline) {
		return nil, errYAMLUploadDeadline
	}
	if err := c.setReadDeadline(deadline); err != nil {
		return nil, fmt.Errorf("%w: setting deadline: %w", errYAMLUploadFraming, err)
	}
	reception := &yamlReception{client: c, deadline: deadline, now: hooks.now, done: make(chan struct{})}
	reception.timer = hooks.afterFunc(deadline.Sub(hooks.now()), reception.expire)
	defer reception.disarm()
	if err := reception.startMarker(); err != nil {
		return nil, err
	}
	body = make([]byte, 0, maxYAMLBytes)
	for {
		complete, err := reception.bodyLine(&body)
		if err != nil {
			return nil, err
		}
		if complete {
			if err := reception.accept(); err != nil {
				return nil, err
			}
			if err := c.setReadDeadline(time.Time{}); err != nil {
				return nil, fmt.Errorf("%w: clearing deadline: %w", errYAMLUploadFraming, err)
			}
			return body, nil
		}
	}
}

func (r *yamlReception) expire() {
	defer close(r.done)
	r.mu.Lock()
	expire := !r.finished
	if expire {
		r.expired = true
	}
	r.mu.Unlock()
	if expire {
		// This phase closes done/socket independently of optional reporting.
		r.client.preserveRecordOnExit.Store(true)
		r.client.interrupt()
	}
}

func (r *yamlReception) disarm() {
	r.mu.Lock()
	r.finished = true
	r.mu.Unlock()
	if !r.timer.Stop() {
		<-r.done
	}
}

func (r *yamlReception) accept() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.expired || !r.now().Before(r.deadline) {
		return errYAMLUploadDeadline
	}
	if r.client.done != nil {
		select {
		case <-r.client.done:
			return net.ErrClosed
		default:
		}
	}
	r.finished = true
	return nil
}

func (r *yamlReception) startMarker() error {
	for range 3 {
		b, err := r.readByte()
		if err != nil {
			return err
		}
		if b != '-' {
			return fmt.Errorf("%w: expected standalone --- line", errYAMLUploadFraming)
		}
	}
	b, err := r.readByte()
	if err != nil {
		return err
	}
	return r.lineEnding(b)
}

func (r *yamlReception) bodyLine(body *[]byte) (bool, error) {
	var probe [3]byte
	for i := range probe {
		b, err := r.readByte()
		if err != nil {
			return false, err
		}
		probe[i] = b
		if b != '.' {
			if err := appendYAMLBody(body, probe[:i+1]); err != nil {
				return false, err
			}
			return false, r.bodyLineRest(body, b)
		}
	}
	b, err := r.readByte()
	if err != nil {
		return false, err
	}
	if b == '\n' || b == '\r' {
		return true, r.lineEnding(b)
	}
	if err := appendYAMLBody(body, probe[:]); err != nil {
		return false, err
	}
	if err := appendYAMLBody(body, []byte{b}); err != nil {
		return false, err
	}
	return false, r.bodyLineRest(body, b)
}

func (r *yamlReception) bodyLineRest(body *[]byte, b byte) error {
	for b != '\n' {
		if b == '\r' {
			next, err := r.readByte()
			if err != nil {
				return err
			}
			if next != '\n' {
				return fmt.Errorf("%w: CR must be followed by LF", errYAMLUploadFraming)
			}
			return appendYAMLBody(body, []byte{next})
		}
		next, err := r.readByte()
		if err != nil {
			return err
		}
		if err := appendYAMLBody(body, []byte{next}); err != nil {
			return err
		}
		b = next
	}
	return nil
}

func appendYAMLBody(body *[]byte, data []byte) error {
	if len(data) > maxYAMLBytes-len(*body) {
		return errYAMLUploadTooLarge
	}
	*body = append(*body, data...)
	return nil
}

func (r *yamlReception) lineEnding(b byte) error {
	if b == '\n' {
		return nil
	}
	if b == '\r' {
		next, err := r.readByte()
		if err != nil {
			return err
		}
		if next == '\n' {
			return nil
		}
	}
	return fmt.Errorf("%w: expected LF or CRLF marker ending", errYAMLUploadFraming)
}

func (r *yamlReception) readByte() (byte, error) {
	for {
		if !r.now().Before(r.deadline) {
			return 0, errYAMLUploadDeadline
		}
		if r.client.done != nil {
			select {
			case <-r.client.done:
				return 0, net.ErrClosed
			default:
			}
		}
		b, err := r.client.reader.ReadByte()
		if err != nil {
			if isTimeoutErr(err) || !r.now().Before(r.deadline) {
				return 0, errYAMLUploadDeadline
			}
			return 0, fmt.Errorf("%w: %w", errYAMLUploadFraming, err)
		}
		if r.client.skipNextEOL {
			r.client.skipNextEOL = false
			if b == '\n' || b == 0 {
				continue
			}
		}
		// ziutek has already removed negotiations and decoded IAC escapes. A
		// second pass would mistake its literal 0xFF data byte for negotiation.
		if b != IAC || (r.client.server != nil && r.client.server.useZiutek) {
			return b, nil
		}
		b, err = r.telnetDataByte()
		if err != nil {
			return 0, err
		}
		if b == IAC {
			return b, nil
		}
	}
}

func (r *yamlReception) telnetDataByte() (byte, error) {
	cmd, err := r.client.reader.ReadByte()
	if err == nil {
		switch cmd {
		case IAC:
			return IAC, nil
		case DO, DONT, WILL, WONT:
			_, err = r.client.reader.ReadByte()
		case SB:
			err = r.client.consumeSubnegotiation()
		}
	}
	if err != nil {
		if isTimeoutErr(err) || !r.now().Before(r.deadline) {
			return 0, errYAMLUploadDeadline
		}
		return 0, fmt.Errorf("%w: %w", errYAMLUploadFraming, err)
	}
	return 0, nil
}
