package config

import (
	"fmt"
	"regexp"
	"strings"
)

// MaxPeeringPeers is the supported resource envelope, not a runtime default.
// Operators must explicitly choose peering.max_peers in the range 1..64.
const MaxPeeringPeers = 64

// validateRawPeeringMaxPeers runs before re-marshaling and typed decoding:
// yaml.v3 accepts floating-point scalars for int fields by truncating them.
// Missing and null values are reported by the generic required-key walker.
func validateRawPeeringMaxPeers(raw map[string]any) error {
	value, present := yamlValueAt(raw, "peering", "max_peers")
	if !present || value == nil {
		return nil
	}
	n, ok := value.(int)
	if !ok || n < 1 || n > MaxPeeringPeers {
		return fmt.Errorf("invalid peering.max_peers: requires an integer between 1 and %d", MaxPeeringPeers)
	}
	return nil
}

func validatePeeringMaxPeers(n int) error {
	if n < 1 || n > MaxPeeringPeers {
		return fmt.Errorf("invalid peering.max_peers: must be between 1 and %d", MaxPeeringPeers)
	}
	return nil
}

// The two expressions are pinned to DXSpider DXUtil::normalise_call and
// is_callsign, in that order. Login normalization differs from spot identity:
// in particular its optional prefix is greedy and its SSID follows the suffix.
var peerCallPattern = regexp.MustCompile(`^(?:[0-9]?[A-Z]{1,2}[0-9]{0,2}/)?(?:[0-9]?[A-Z]{1,2}[0-9]{1,5})[A-Z]{1,8}(?:-[0-9]{1,2})?(?:/[0-9A-Z]{1,7})?(?:/(?:AM?|MM?|P))?$`)
var peerNormalizePattern = regexp.MustCompile(`^(?:\w{0,4}/)?(\w+)(?:/\w{0,4})?(?:-(\d+))?$`)

// IsRawPeeringCall checks the receiver's uppercase ASCII wire grammar without
// login repairs. Callers own role-specific padding removal; validity alone does
// not guarantee that CanonicalPeeringCall can represent a stable local identity.
func IsRawPeeringCall(call string) bool {
	return len(call) <= 36 && peerCallPattern.MatchString(call)
}

// CanonicalPeeringCall returns a valid, stable receiver identity. The bounded
// input protects publication/private lookup from arbitrary local login strings;
// the separately qualified local publication envelope is 15 canonical bytes.
func CanonicalPeeringCall(call string) (string, bool) {
	call = strings.TrimSpace(call)
	if len(call) > 36 {
		return "", false
	}
	call = strings.ToUpper(call)
	// Canonical wire traffic is already slash-free. Keep the common identity
	// lookup allocation-free; only portable forms require capture storage.
	if !strings.ContainsRune(call, '/') && peerCallPattern.MatchString(call) {
		if base, suffix, found := strings.Cut(call, "-"); found {
			ssid := strings.TrimLeft(suffix, "0")
			if ssid == "" {
				return base, true
			}
			if ssid != suffix {
				return base + "-" + ssid, true
			}
		}
		return call, true
	}
	parts := peerNormalizePattern.FindStringSubmatch(call)
	if len(parts) != 3 {
		return "", false
	}
	call = parts[1]
	if ssid := strings.TrimLeft(parts[2], "0"); ssid != "" {
		call += "-" + ssid
	}
	if !peerCallPattern.MatchString(call) {
		return "", false
	}
	return call, true
}

// IsPeeringCallCandidate distinguishes callsign-shaped private destinations
// from named chat groups. A failed normalization must not broadcast private
// text. This predicate grants no identity or admission authority.
func IsPeeringCallCandidate(call string) bool {
	call = strings.TrimSpace(call)
	return len(call) <= 36 && peerCallPattern.MatchString(strings.ToUpper(call))
}

// NormalizeActivePeeringWireContract protects direct manager construction as
// well as the loader. It owns at most max_peers active peers; dormant configuration
// remains caller-owned. It does not fill defaults: a supplied local argument
// is authoritative only when the config omits it.
func NormalizeActivePeeringWireContract(cfg PeeringConfig, localCall string) (PeeringConfig, string, error) {
	if err := validatePeeringMaxPeers(cfg.MaxPeers); err != nil {
		return cfg, "", err
	}
	local, ok := CanonicalPeeringCall(localCall)
	if !ok || len(local) > 15 {
		return cfg, "", fmt.Errorf("invalid peering manager local callsign")
	}
	if cfg.LocalCallsign == "" {
		cfg.LocalCallsign = local
	}
	enabled := cfg.Enabled
	cfg.Enabled = true
	count := 0
	for i := range cfg.Peers {
		if cfg.Peers[i].Enabled {
			count++
		}
		if count > cfg.MaxPeers {
			return cfg, "", fmt.Errorf("invalid peering.peers: enabled peer count exceeds peering.max_peers (%d)", cfg.MaxPeers)
		}
	}
	active := make([]PeeringPeer, 0, count)
	for i := range cfg.Peers {
		if cfg.Peers[i].Enabled {
			active = append(active, cfg.Peers[i])
		}
	}
	cfg.Peers = active
	if err := validatePeeringWireContract(&cfg); err != nil {
		return cfg, "", err
	}
	cfg.Enabled = enabled
	if cfg.LocalCallsign != local {
		return cfg, "", fmt.Errorf("peering manager local callsign must identify peering.local_callsign")
	}
	return cfg, local, nil
}

// validatePeeringWireContract runs after legacy defaults and registry shape
// checks. The cap is always valid; disabled peering retains its historical
// dormant row-count and wire-identity behavior.
func validatePeeringWireContract(cfg *PeeringConfig) error {
	if err := validatePeeringMaxPeers(cfg.MaxPeers); err != nil {
		return err
	}
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
	if cfg.HopCount < 0 || cfg.HopCount > 99 {
		return fmt.Errorf("invalid peering.hop_count: must be between 0 and 99")
	}
	if cfg.PC92Bitmap != 4 && cfg.PC92Bitmap != 5 {
		return fmt.Errorf("invalid peering.pc92_bitmap: local node flag must be 4 or 5")
	}
	if cfg.Backoff.BaseMS > 300000 || cfg.Backoff.MaxMS > 300000 {
		return fmt.Errorf("invalid peering.backoff: positive base_ms and max_ms must not exceed 300000")
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
		if enabled > cfg.MaxPeers {
			return fmt.Errorf("invalid peering.peers: enabled peer count exceeds peering.max_peers (%d)", cfg.MaxPeers)
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
