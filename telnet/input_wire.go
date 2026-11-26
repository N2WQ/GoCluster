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
	ziutekNOP  bool        // Set at wrapping, before the reader starts.
	total      uint64
	progress   uint64 // Monotonic activity marker that excludes complete top-level NOPs.
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

// observeTelnet classifies wire progress before the codec can hide it. Defer
// counting a leading IAC until its command byte arrives, even across reads.
// An open negotiation is independently visible through state. Recovery still
// charges the actual wire bytes, including NOP, before this observation.
func (c *inputWireConn) observeTelnet(data []byte) {
	for len(data) > 0 {
		if c.state == 0 {
			i := bytes.IndexByte(data, IAC)
			if i < 0 {
				c.progress += uint64(len(data))
				return
			}
			c.progress += uint64(i)
			data = data[i+1:]
			c.state = 1
			continue
		}
		b := data[0]
		if c.state == 1 && b == telnetNOP {
			if c.ziutekNOP {
				// Ziutek v0.1.0 silently consumes IAC GA but rejects IAC NOP.
				// Change just the codec-facing byte, without shrinking the read.
				data[0] = telnetGA
			}
			c.state = 0
			data = data[1:]
			continue
		}
		data = data[1:]
		c.progress++
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
