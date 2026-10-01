package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// The grammar is pinned to DXSpider DXUtil::is_callsign. It deliberately does
// not reuse spot normalization, which strips portable suffixes and can conflate
// distinct login identities. Its bounded components cap accepted input at 36
// bytes; local publication has the separately qualified 15-byte envelope.
var peerCallPattern = regexp.MustCompile(`^(?:[0-9]?[A-Z]{1,2}[0-9]{0,2}/)?(?:[0-9]?[A-Z]{1,2}[0-9]{1,5})[A-Z]{1,8}(?:-[0-9]{1,2})?(?:/[0-9A-Z]{1,7})?(?:/(?:AM?|MM?|P))?$`)

// CanonicalPeeringCall returns the shared PC92/login identity. SSID leading
// zeros are removed so publication collision handling can refuse ambiguity.
func CanonicalPeeringCall(call string) (string, bool) {
	call = strings.ToUpper(strings.TrimSpace(call))
	if len(call) > 36 || !peerCallPattern.MatchString(call) {
		return "", false
	}
	if dash := strings.IndexByte(call, '-'); dash >= 0 {
		end := strings.IndexByte(call[dash+1:], '/')
		if end < 0 {
			end = len(call)
		} else {
			end += dash + 1
		}
		ssid, err := strconv.Atoi(call[dash+1 : end])
		if err != nil {
			return "", false
		}
		if ssid == 0 {
			call = call[:dash] + call[end:]
		} else {
			call = call[:dash+1] + strconv.Itoa(ssid) + call[end:]
		}
	}
	return call, true
}

// validatePeeringWireContract runs after legacy defaults and registry shape
// checks. Disabled peering retains its historical dormant-config behavior.
func validatePeeringWireContract(cfg *PeeringConfig) error {
	if !cfg.Enabled {
		return nil
	}
	call, ok := CanonicalPeeringCall(cfg.LocalCallsign)
	if !ok || len(call) > 15 {
		return fmt.Errorf("invalid peering.local_callsign: requires a DXSpider callsign of at most 15 bytes")
	}
	cfg.LocalCallsign = call
	for _, entry := range []struct{ name, value string }{
		{"node_version", cfg.NodeVersion}, {"node_build", cfg.NodeBuild}, {"legacy_version", cfg.LegacyVersion},
	} {
		if entry.value == "" && entry.name == "node_build" {
			continue
		}
		if !peerNumericMetadata(entry.value) {
			return fmt.Errorf("invalid peering.%s: requires 1 to 10 decimal digits", entry.name)
		}
	}
	if cfg.HopCount > 99 {
		return fmt.Errorf("invalid peering.hop_count: must be <= 99")
	}
	if cfg.PC92Bitmap < 4 || cfg.PC92Bitmap > 7 {
		return fmt.Errorf("invalid peering.pc92_bitmap: local node flag must be 4 through 7")
	}
	if cfg.MaxLineLength > 64<<10 || cfg.PC92MaxBytes > 64<<10 {
		return fmt.Errorf("invalid peering frame limits: max_line_length and pc92_max_bytes must be <= 65536")
	}
	enabled := 0
	seen := make(map[string]int)
	for i := range cfg.Peers {
		p := &cfg.Peers[i]
		if !p.Enabled {
			continue
		}
		enabled++
		if enabled > 64 {
			return fmt.Errorf("invalid peering.peers: at most 64 enabled peers are supported")
		}
		if p.LoginCallsign == "" {
			p.LoginCallsign = call
		}
		login, valid := CanonicalPeeringCall(p.LoginCallsign)
		if !valid || login != call {
			return fmt.Errorf("invalid peering.peers[%d].login_callsign: must identify peering.local_callsign", i)
		}
		p.LoginCallsign = login
		remote, valid := CanonicalPeeringCall(p.RemoteCallsign)
		if !valid || len(remote) > 15 || remote == call {
			return fmt.Errorf("invalid peering.peers[%d].remote_callsign: requires a distinct DXSpider callsign of at most 15 bytes", i)
		}
		if previous, exists := seen[remote]; exists {
			return fmt.Errorf("invalid peering.peers[%d].remote_callsign: canonical identity duplicates peer %d", i, previous)
		}
		seen[remote] = i
		p.RemoteCallsign = remote
	}
	return nil
}

func peerNumericMetadata(value string) bool {
	if len(value) == 0 || len(value) > 10 {
		return false
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
