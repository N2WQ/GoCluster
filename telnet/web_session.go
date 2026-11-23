// File role: Adapt one HTTP-upgraded browser to the shared cluster session.
// The reader owns command/message boundaries and detached upload readers. The
// cluster writer alone owns application messages; heartbeat uses control frames.
package telnet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

type webSessionConn struct {
	net.Conn
	ws          *websocket.Conn
	peer        *net.TCPAddr
	headerLimit int
	allowUpload bool
	header      []byte
	upload      io.Reader
	partialJSON io.WriteCloser
	start       time.Time
	readStarted atomic.Int64 // Monotonic offset; zero while the session processes a request.
}

func (s *Server) serveWebSocketSession(ws *websocket.Conn, addr *net.TCPAddr, ticket *preloginTicket, acceptedAt time.Time, heartbeat time.Duration) {
	web := &webSessionConn{Conn: ws.UnderlyingConn(), ws: ws, peer: addr, headerLimit: s.loginLineLimit, start: time.Now()}
	ws.SetReadLimit(int64(max(s.loginLineLimit, s.commandLineLimit, defaultCommandLineLimit)) + maxV7YAMLBytes + 16)
	if heartbeat <= 0 {
		heartbeat = 30 * time.Second
	}
	var lastPong atomic.Int64
	ws.SetPongHandler(func(payload string) error {
		if payload == "GoCluster" {
			lastPong.Store(int64(time.Since(web.start)))
		}
		return nil
	})
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				// Gorilla dispatches pongs only in its reader. History, uploads
				// and other synchronous work own independent bounds and can
				// keep that reader away from the next header for many ticks.
				reading := web.readStarted.Load()
				if reading == 0 {
					continue
				}
				lastOpportunity := time.Duration(max(reading, lastPong.Load()))
				missed := (time.Since(web.start)-lastOpportunity)/heartbeat >= 3
				if (missed && web.readStarted.Load() == reading) ||
					ws.WriteControl(websocket.PingMessage, []byte("GoCluster"), time.Now().Add(min(heartbeat, defaultSendDeadline))) != nil {
					_ = web.Close()
					return
				}
			}
		}
	}()
	defer func() { close(stop); _ = web.Close(); <-done }()
	s.handleAcceptedClient(web, ticket, acceptedAt)
}

func (w *webSessionConn) RemoteAddr() net.Addr               { return w.peer }
func (w *webSessionConn) SetWriteDeadline(t time.Time) error { return w.ws.SetWriteDeadline(t) }

// Read exposes one validated command with a synthetic newline. For uploads
// only the header is read here. The configuration receiver consumes the rest
// of the same message under its existing byte reservation and deadline.
func (w *webSessionConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(w.header) == 0 {
		if w.upload != nil {
			return 0, errYAMLUploadFraming
		}
		w.readStarted.Store(max(1, int64(time.Since(w.start))))
		defer w.readStarted.Store(0)
		kind, r, err := w.ws.NextReader()
		if err != nil {
			return 0, err
		}
		if kind != websocket.TextMessage {
			return 0, errors.New("cluster command requires a WebSocket text message")
		}
		var octet [1]byte
		limit := w.headerLimit
		if limit <= 0 {
			limit = defaultCommandLineLimit
		}
		for {
			_, err := io.ReadFull(r, octet[:])
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, err
			}
			b := octet[0]
			if b == '\n' {
				cmd, handled, parseErr := parseV7MachineHeader(string(w.header))
				if !w.allowUpload || !handled || parseErr != nil || cmd.Verb == "GET" {
					return 0, errYAMLUploadFraming
				}
				w.upload = r
				break
			}
			if b < ' ' || b > '~' || len(w.header) >= limit {
				return 0, errors.New("invalid browser command header")
			}
			w.header = append(w.header, b)
		}
		w.header = append(w.header, '\n')
	}
	n := copy(p, w.header)
	w.header = w.header[n:]
	return n, nil
}

func (w *webSessionConn) Write(p []byte) (int, error) {
	body := p
	if !utf8.Valid(body) {
		body = bytes.ToValidUTF8(body, []byte("\uFFFD"))
	}
	if err := w.ws.WriteMessage(websocket.TextMessage, body); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Keep a split JSON record in a single WebSocket message even when several
// bounded encoder batches carry it. Gorilla streams fragments to the peer.
func (w *webSessionConn) writeJSON(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		if w.partialJSON == nil {
			writer, err := w.ws.NextWriter(websocket.TextMessage)
			if err != nil {
				return 0, err
			}
			w.partialJSON = writer
		}
		end := bytes.LastIndexByte(p, '\n') + 1
		if end == 0 {
			if _, err := w.partialJSON.Write(p); err != nil {
				return 0, err
			}
			break
		}
		if _, err := w.partialJSON.Write(p[:end]); err != nil {
			return 0, err
		}
		if err := w.partialJSON.Close(); err != nil {
			return 0, err
		}
		w.partialJSON = nil
		p = p[end:]
	}
	return total, nil
}
