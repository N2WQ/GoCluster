// A probe session has one synchronous protocol/read owner, one cancellation
// interruptor, and at most one keeper. Retirement joins both workers before
// BYE or socket closure so a reconnect cannot inherit an old writer.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"dxcluster/peer"
)

type probeSession struct {
	cfg           probeConfig
	ctx           context.Context
	cancel        context.CancelCauseFunc
	transport     *probeTransport
	reader        *peer.LineReader
	writer        *bufio.Writer
	wireMu        sync.Mutex
	timestamps    *peer.TimestampGenerator
	timestampNow  func() time.Time
	newTicker     func(time.Duration) probeTicker
	interruptDone chan struct{}
	keeperDone    chan struct{}
	pc22Seen      bool
}

func newProbeSession(parent context.Context, cfg probeConfig, conn net.Conn, timestamps *peer.TimestampGenerator, timestampNow func() time.Time, ticker func(time.Duration) probeTicker) *probeSession {
	ctx, cancel := context.WithCancelCause(parent)
	s := &probeSession{
		cfg: cfg, ctx: ctx, cancel: cancel,
		transport:  &probeTransport{conn: conn, ctx: ctx},
		timestamps: timestamps, timestampNow: timestampNow, newTicker: ticker,
		interruptDone: make(chan struct{}),
	}
	s.writer = bufio.NewWriter(s.transport)
	s.reader = peer.NewLineReader(s.transport, 4096, 0, func(data []byte) {
		if err := s.sendRaw(data); err != nil {
			s.fail(fmt.Errorf("telnet reply write: %w", err))
		}
	})
	return s
}

func (s *probeSession) run() error {
	go func() {
		defer close(s.interruptDone)
		<-s.ctx.Done()
		s.transport.interrupt()
	}()
	err := s.runProtocol()
	s.fail(err)
	// No join or deadline interruption takes wireMu: a blocked producer must
	// first be released by cancellation before retirement can wait for it.
	if s.keeperDone != nil {
		<-s.keeperDone
	}
	<-s.interruptDone
	if s.pc22Seen {
		s.sendBye()
	}
	if err := s.transport.Close(); err != nil {
		log.Printf("close peer: %v", err)
	}
	return context.Cause(s.ctx)
}

func (s *probeSession) fail(err error) {
	if err != nil {
		// WithCancelCause installs the first cause before waking I/O owners.
		s.cancel(err)
	}
}

func (s *probeSession) sendLine(line string) error {
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	if err := context.Cause(s.ctx); err != nil {
		return err
	}
	if err := s.writeLine(line); err != nil {
		s.fail(err)
		return context.Cause(s.ctx)
	}
	return nil
}

// writeLine is called under wireMu. A failed buffered writer stays failed: a
// fresh writer could replay an uncertain partial frame during best-effort BYE.
func (s *probeSession) writeLine(line string) error {
	if !strings.HasSuffix(line, "\n") {
		line += "\r\n"
	}
	if _, err := s.writer.WriteString(line); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *probeSession) sendRaw(data []byte) error {
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	if err := context.Cause(s.ctx); err != nil {
		return err
	}
	n, err := s.transport.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.fail(fmt.Errorf("telnet reply write: %w", err))
		return context.Cause(s.ctx)
	}
	return nil
}

func (s *probeSession) sendBye() {
	// Both workers are joined. Only BYE may install a future write deadline
	// after interruption, and its failure cannot replace the primary cause.
	if !s.transport.beginBye(time.Now().UTC().Add(2 * time.Second)) {
		return
	}
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	if err := s.writeLine("BYE"); err != nil {
		log.Printf("bye send error: %v", err)
	} else {
		log.Printf("TX BYE")
	}
}

// The deadline gate guards only short setter operations, never blocking I/O.
// Checking context under that gate prevents LineReader's next ordinary setter
// from undoing cancellation even before the interruptor acquires the gate.
type probeTransport struct {
	conn    net.Conn
	ctx     context.Context
	gate    sync.Mutex
	retired bool
	closed  bool
}

func (c *probeTransport) SetReadDeadline(deadline time.Time) error {
	c.gate.Lock()
	defer c.gate.Unlock()
	if err := context.Cause(c.ctx); err != nil {
		return err
	}
	return c.conn.SetReadDeadline(deadline)
}

func (c *probeTransport) Read(data []byte) (int, error) { return c.conn.Read(data) }

func (c *probeTransport) Write(data []byte) (int, error) { return c.conn.Write(data) }

func (c *probeTransport) LocalAddr() net.Addr { return c.conn.LocalAddr() }

func (c *probeTransport) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// SetDeadline is unused by ordinary producers. Interrupt and post-join BYE use
// conn directly so ordinary phase admission cannot revive canceled I/O.
func (c *probeTransport) SetDeadline(deadline time.Time) error { return c.conn.SetDeadline(deadline) }

func (c *probeTransport) SetWriteDeadline(deadline time.Time) error {
	c.gate.Lock()
	defer c.gate.Unlock()
	if err := context.Cause(c.ctx); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(deadline)
}

func (c *probeTransport) interrupt() {
	c.gate.Lock()
	defer c.gate.Unlock()
	c.retired = true
	if err := c.conn.SetDeadline(time.Now().UTC()); err != nil {
		log.Printf("interrupt peer deadline: %v", err)
		if err := c.closeLocked(); err != nil {
			log.Printf("close peer after interrupt deadline failure: %v", err)
		}
	}
}

func (c *probeTransport) beginBye(deadline time.Time) bool {
	c.gate.Lock()
	defer c.gate.Unlock()
	if !c.retired || c.closed {
		return false
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		log.Printf("failed to set BYE deadline: %v", err)
		if err := c.closeLocked(); err != nil {
			log.Printf("close peer after BYE deadline failure: %v", err)
		}
		return false
	}
	return true
}

func (c *probeTransport) closeLocked() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

func (c *probeTransport) Close() error {
	c.gate.Lock()
	defer c.gate.Unlock()
	return c.closeLocked()
}
