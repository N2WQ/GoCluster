package peer

import (
	"fmt"
	"net"
	"strings"
)

// Purpose: Parse CIDR strings into IPNet blocks for ACL checks.
// Key aspects: Accepts single-host entries by appending /32 or /128.
// Upstream: Peer ACL configuration loading.
// Downstream: net.ParseCIDR.
func parseIPACL(cidrs []string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, cidr := range cidrs {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}
		if !strings.Contains(cidr, "/") {
			// treat as single host
			cidr += "/32"
			if strings.Contains(cidr, ":") {
				cidr = cidr[:len(cidr)-3] + "/128"
			}
		}
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid cidr %q: %w", cidr, err)
		}
		out = append(out, block)
	}
	return out, nil
}
