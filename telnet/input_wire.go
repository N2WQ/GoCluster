// File role: Bounds human recovery below the optional Telnet codec. All mutable
// fields belong to the connection's read owner; writers and watchdogs only use
// the embedded Conn's concurrent-safe I/O/Close methods.
package telnet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync/atomic"
)

var errHumanRecoveryWireLimit = errors.New("human line recovery exceeds wire budget")

type inputWireConn struct {
	net.Conn
	muteCodec  atomic.Bool // Writer owns transitions while the sole reader waits on its switch receipt.
	total      uint64
	state      byte
	remaining  int // Inactive unless recovering; includes all returned wire bytes.
	recovering bool
}

// The telnet decoder writes negotiations from inside Read. In JSON the
// application writer goes directly to Conn. Reporting success here lets the
// decoder consume the next command even when the network writer is stalled.
func (c *inputWireConn) Write(p []byte) (int, error) {
	if c.muteCodec.Load() {
		return len(p), nil
	}
	return c.Conn.Write(p)
}

func (c *inputWireConn) Read(p []byte) (int, error) {
	if c.recovering {
		if c.remaining <= 0 {
			return 0, errHumanRecoveryWireLimit
		}
		if len(p) > c.remaining {
			p = p[:c.remaining]
		}
	}
	n, err := c.Conn.Read(p)
	c.total += uint64(n)
	if c.recovering {
		c.remaining -= n
	}
	c.observeTelnet(p[:n])
	return n, err
}

// observeTelnet records only incomplete wire negotiation, not application
// parsing. When an idle read fails, hidden codec progress cannot be mistaken for
// a safely empty read. Normal ASCII chunks skip the state loop without allocation.
func (c *inputWireConn) observeTelnet(data []byte) {
	for len(data) > 0 {
		if c.state == 0 {
			i := bytes.IndexByte(data, IAC)
			if i < 0 {
				return
			}
			data = data[i+1:]
			c.state = 1
			continue
		}
		b := data[0]
		data = data[1:]
		switch c.state {
		case 1:
			switch b {
			case DO, DONT, WILL, WONT:
				c.state = 2
			case SB:
				c.state = 3
			default:
				c.state = 0
			}
		case 2:
			c.state = 0
		case 3:
			if b == IAC {
				c.state = 4
			}
		case 4:
			c.state = 3
			if b == SE {
				c.state = 0
			}
		}
	}
}

// singleByteInput exposes an offending byte before ziutek can perform a later
// negotiation Write in the same bulk Read. Only human/header reception enables
// it; YAML bodies retain their existing bulk reads and owned upload watchdog.
type singleByteInput struct {
	reader  io.Reader
	enabled bool
}

func (r *singleByteInput) Read(p []byte) (int, error) {
	if r.enabled && len(p) > 1 {
		p = p[:1]
	}
	return r.reader.Read(p)
}
