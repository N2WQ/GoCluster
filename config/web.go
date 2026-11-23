package config

import (
	"errors"
	"net/netip"
	"strings"
)

// WebConfig controls the private browser entry point. The process environment
// supplies its edge secret only when the listener is enabled.
type WebConfig struct {
	Enabled          bool   `yaml:"enabled"`
	ListenAddress    string `yaml:"listen_address"`
	Hostname         string `yaml:"hostname"`
	HeartbeatSeconds int    `yaml:"heartbeat_seconds"`
	MaxConnections   int    `yaml:"max_connections"`
}

// Validate checks the actual bind target before net.Listen and constrains the
// exact HTTPS origin and connection/time budgets trusted by the web listener.
func (w WebConfig) Validate() error {
	address, err := netip.ParseAddrPort(w.ListenAddress)
	if err != nil || !address.Addr().IsLoopback() || address.Addr().Zone() != "" || address.Port() == 0 {
		return errors.New("web.listen_address must be a literal loopback IP and nonzero port")
	}
	if !validWebHostname(w.Hostname) {
		return errors.New("web.hostname must be a lower-case DNS hostname without a port")
	}
	if w.HeartbeatSeconds < 1 || w.HeartbeatSeconds > 300 {
		return errors.New("web.heartbeat_seconds must be between 1 and 300")
	}
	if w.MaxConnections < 1 || w.MaxConnections > 1024 {
		return errors.New("web.max_connections must be between 1 and 1024")
	}
	return nil
}

func validWebHostname(host string) bool {
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	if len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	for label := range strings.SplitSeq(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' ||
			strings.Trim(label, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
			return false
		}
	}
	return true
}
