// Package netutil provides shared TCP connection helpers.
package netutil

import (
	"net"
	"time"
)

// EnableTCPKeepAlive enables keepalive on TCP connections and sets its period.
// It returns (false, nil, nil) for non-TCP connections.
func EnableTCPKeepAlive(conn net.Conn, period time.Duration) (bool, error, error) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok || tcp == nil {
		return false, nil, nil
	}
	return true, tcp.SetKeepAlive(true), tcp.SetKeepAlivePeriod(period)
}
