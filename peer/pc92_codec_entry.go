package peer

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"
)

// DecodePC92Entry follows the flag/call[:version[:build[:IP]]] representation
// and DXSpider's short call:IP form. IPv6 colons are commas on the wire. Numeric
// metadata normalization mirrors _decode_pc92_call; invalid address material is
// rejected atomically rather than silently turning a C into a partial snapshot.
func DecodePC92Entry(raw string) (PC92Entry, error) {
	// Reject the already-invalid shape before allocating its token array.
	// A colon flood must not multiply a bounded frame into unleased headers.
	if strings.Count(raw, ":") > 3 {
		return PC92Entry{}, errors.New("invalid PC92 flag or entry shape")
	}
	parts := strings.Split(raw, ":")
	if len(parts[0]) < 2 || parts[0][0] < '0' || parts[0][0] > '7' {
		return PC92Entry{}, errors.New("invalid PC92 flag or entry shape")
	}
	call, ok := CanonicalPC92Call(parts[0][1:])
	if !ok {
		return PC92Entry{}, errors.New("invalid PC92 callsign")
	}
	entry := PC92Entry{Call: call, Flags: parts[0][0] - '0'}
	if len(parts) > 1 {
		if strings.ContainsAny(parts[1], ".,") && len(parts[1]) > 4 {
			if ip, err := decodePC92IP(parts[1]); err == nil {
				entry.IP = ip
			} else {
				return PC92Entry{}, err
			}
		} else {
			entry.Version = pc92Numeric(parts[1], false)
		}
	}
	if len(parts) > 2 {
		entry.Build = pc92Numeric(parts[2], true)
	}
	if len(parts) > 3 && parts[3] != "" {
		ip, err := decodePC92IP(parts[3])
		if err != nil {
			return PC92Entry{}, err
		}
		entry.IP = ip
	}
	return entry, nil
}

func decodePC92IP(raw string) (netip.Addr, error) {
	ip, err := netip.ParseAddr(strings.ReplaceAll(raw, ",", ":"))
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}, errors.New("invalid PC92 IP address")
	}
	return ip.Unmap(), nil
}

func pc92Numeric(raw string, build bool) string {
	if build {
		raw = strings.TrimPrefix(raw, "0.")
	}
	allDigits := true
	for i := range raw {
		if raw[i] < '0' || raw[i] > '9' {
			allDigits = false
			break
		}
	}
	if allDigits {
		return strings.Clone(strings.TrimLeft(raw, "0"))
	}
	var result strings.Builder
	for i := range raw {
		if raw[i] >= '0' && raw[i] <= '9' {
			result.WriteByte(raw[i])
		}
	}
	// Normalization can leave one digit from a64KiB field. Compact that result
	// before it becomes session metadata; a tiny substring must not retain the
	// builder's entire growth backing through a pending or stale session owner.
	return strings.Clone(strings.TrimLeft(result.String(), "0"))
}

// EncodePC92Entry emits explicit metadata slots when IP is present. This keeps
// numeric metadata separate from the abbreviated IP form and makes missing
// version/build unambiguous to DXSpider's positional decoder.
func EncodePC92Entry(entry PC92Entry) (string, error) {
	call, ok := CanonicalPC92Call(entry.Call)
	if !ok || entry.Flags > 7 {
		return "", errors.New("invalid PC92 entry identity")
	}
	for _, field := range []string{entry.Version, entry.Build} {
		for i := range field {
			if field[i] < '0' || field[i] > '9' {
				return "", errors.New("PC92 numeric metadata requires decimal digits")
			}
		}
	}
	encoded := fmt.Sprintf("%d%s", entry.Flags, call)
	if entry.IP.IsValid() {
		if entry.IP.Zone() != "" {
			return "", errors.New("PC92 IP address cannot contain a zone")
		}
		ip := strings.ReplaceAll(entry.IP.Unmap().String(), ":", ",")
		if entry.Version == "" && entry.Build == "" && len(ip) > 4 {
			return encoded + ":" + ip, nil
		}
		return encoded + ":" + entry.Version + ":" + entry.Build + ":" + ip, nil
	}
	if entry.Version != "" || entry.Build != "" {
		encoded += ":" + entry.Version
	}
	if entry.Build != "" {
		encoded += ":" + entry.Build
	}
	return encoded, nil
}
