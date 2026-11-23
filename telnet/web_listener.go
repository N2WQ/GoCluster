package telnet

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"dxcluster/config"

	"github.com/gorilla/websocket"
	"golang.org/x/net/netutil"
)

const webHeaderTimeout = 5 * time.Second

type webAcceptTimeKey struct{}

// WebListener owns both the bounded HTTP listener and its upgraded sessions.
// net/http drops ownership of hijacked sockets as soon as it sends the upgrade.
type WebListener struct {
	cluster    *Server
	cfg        config.WebConfig
	secret     [sha256.Size]byte
	httpServer *http.Server
	done       chan struct{}
	stopOnce   sync.Once
	mu         sync.Mutex
	closing    bool
	upgraded   map[*websocket.Conn]struct{}
	handlers   sync.WaitGroup
}

// StartWebListener opens the configured private origin. A missing secret is a
// startup error whenever the origin is enabled.
func (s *Server) StartWebListener(ctx context.Context, cfg config.WebConfig, secret string) (*WebListener, error) {
	if !cfg.Enabled {
		return nil, errors.New("web listener is disabled")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(secret) < 32 || len(secret) > 256 || strings.IndexFunc(secret, func(r rune) bool { return r < '!' || r > '~' }) >= 0 {
		return nil, errors.New("GOCLUSTER_WEB_EDGE_SECRET requires 32 to 256 printable, non-space ASCII characters")
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddress)
	if err != nil {
		return nil, err
	}
	return s.serveWebListener(listener, cfg, secret), nil
}

func (s *Server) serveWebListener(listener net.Listener, cfg config.WebConfig, secret string) *WebListener {
	w := &WebListener{
		cluster:  s,
		cfg:      cfg,
		secret:   sha256.Sum256([]byte(secret)),
		done:     make(chan struct{}),
		upgraded: make(map[*websocket.Conn]struct{}),
	}
	w.httpServer = &http.Server{
		Handler:           http.HandlerFunc(w.handleUpgrade),
		ReadHeaderTimeout: webHeaderTimeout,
		WriteTimeout:      webHeaderTimeout,
		MaxHeaderBytes:    8 << 10,
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
			return context.WithValue(ctx, webAcceptTimeKey{}, time.Now())
		},
	}
	w.httpServer.SetKeepAlivesEnabled(false)
	go func() {
		defer close(w.done)
		// The accepted connection retains its slot through HTTP hijacking.
		err := w.httpServer.Serve(netutil.LimitListener(listener, cfg.MaxConnections))
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("Web console listener stopped: %v", err)
		}
	}()
	return w
}

// Stop fences new upgrades and closes both slow HTTP handshakes and WebSockets.
func (w *WebListener) Stop() {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.closing = true
		for ws := range w.upgraded {
			_ = ws.Close()
		}
		w.mu.Unlock()
		_ = w.httpServer.Close()
		<-w.done
		w.handlers.Wait()
	})
}

func singleWebHeader(h http.Header, key string) string {
	if values := h.Values(key); len(values) == 1 {
		return values[0]
	}
	return ""
}

// visitor verifies the private hop and public origin before trusting the
// Worker-stamped visitor address. Header disagreement fails closed.
func (w *WebListener) visitor(r *http.Request) (*net.TCPAddr, bool) {
	peer, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil || !peer.Addr().IsLoopback() || r.Host != w.cfg.Hostname ||
		singleWebHeader(r.Header, "Origin") != "https://"+w.cfg.Hostname {
		return nil, false
	}
	secret := sha256.Sum256([]byte(singleWebHeader(r.Header, "X-GoCluster-Edge-Secret")))
	if subtle.ConstantTimeCompare(secret[:], w.secret[:]) != 1 {
		return nil, false
	}
	cf, err := netip.ParseAddr(singleWebHeader(r.Header, "CF-Connecting-IP"))
	if err != nil || cf.Zone() != "" {
		return nil, false
	}
	visitor, err := netip.ParseAddr(singleWebHeader(r.Header, "X-GoCluster-Visitor-IP"))
	if err != nil || visitor.Zone() != "" || visitor.Unmap() != cf.Unmap() {
		return nil, false
	}
	return &net.TCPAddr{IP: cf.Unmap().AsSlice()}, true
}

func (w *WebListener) handleUpgrade(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet || r.RequestURI != "/ws" {
		http.NotFound(rw, r)
		return
	}
	addr, ok := w.visitor(r)
	if !ok {
		http.Error(rw, "Forbidden", http.StatusForbidden)
		return
	}
	if r.ProtoMajor != 1 || r.ProtoMinor != 1 || r.ContentLength != 0 || len(r.TransferEncoding) != 0 ||
		!websocket.IsWebSocketUpgrade(r) || singleWebHeader(r.Header, "Sec-WebSocket-Version") != "13" {
		http.Error(rw, "Invalid upgrade", http.StatusBadRequest)
		return
	}
	acceptedAt, ok := r.Context().Value(webAcceptTimeKey{}).(time.Time)
	if !ok {
		http.Error(rw, "Unavailable", http.StatusServiceUnavailable)
		return
	}
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		http.Error(rw, "Unavailable", http.StatusServiceUnavailable)
		return
	}
	w.handlers.Add(1)
	w.mu.Unlock()
	defer w.handlers.Done()

	ticket, reason := w.cluster.tryAcquirePrelogin(addr)
	if ticket == nil {
		address := peerAddressString(addr)
		w.cluster.recordPreloginReject(reason)
		w.cluster.logAdmissionReject(reason, address)
		w.cluster.reportLoginAttempt("blocked", string(reason), "", address, "")
		http.Error(rw, "Server busy", http.StatusTooManyRequests)
		return
	}
	upgrader := websocket.Upgrader{HandshakeTimeout: webHeaderTimeout}
	ws, err := upgrader.Upgrade(rw, r, nil)
	if err != nil {
		ticket.Release()
		return
	}
	defer ws.Close()
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		ticket.Release()
		return
	}
	w.upgraded[ws] = struct{}{}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.upgraded, ws)
		w.mu.Unlock()
	}()
	w.cluster.serveWebSocketSession(ws, addr, ticket, acceptedAt, time.Duration(w.cfg.HeartbeatSeconds)*time.Second)
}
