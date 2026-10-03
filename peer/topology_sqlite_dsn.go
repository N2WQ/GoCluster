package peer

import (
	"errors"
	"net/url"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const topologyDSNBytes = 64 << 10

type topologyPragma struct{ sql, key string }
type topologyDSN struct {
	filename, begin string
	pragmas         [128]topologyPragma
	count           int
}

// Parse without a query map: arbitrarily many unrelated keys must not multiply
// host backing. The fixed statement table and byte cap are part of the 2 MiB
// host reservation. Modernc applies busy_timeout first, then case-insensitive
// lexical pragma order; preserve that order for the existing configured DSN.
func parseTopologyDSN(path string) (out topologyDSN, err error) {
	if len(path) > topologyDSNBytes {
		return out, errTopologyBudget
	}
	out.filename = path
	out.begin = "begin"
	name, query, found := strings.Cut(path, "?")
	if !found || name == "" {
		return out, nil
	}
	uri := strings.HasPrefix(path, "file:")
	var filename strings.Builder
	if uri {
		filename.Grow(len(path))
		filename.WriteString(name)
	} else {
		out.filename = name
	}
	var txlock, timefmt string
	var txlockSeen, timefmtSeen bool
	for query != "" {
		pair, rest, _ := strings.Cut(query, "&")
		query = rest
		if strings.ContainsRune(pair, ';') {
			return out, errors.New("invalid semicolon separator in topology DSN")
		}
		if pair == "" {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		key, err = url.QueryUnescape(key)
		if err != nil {
			return out, err
		}
		value, err = url.QueryUnescape(value)
		if err != nil {
			return out, err
		}
		switch key {
		case "_pragma":
			if out.count == len(out.pragmas) {
				return out, errTopologyBudget
			}
			// Keep a borrowed trimmed key. Materializing ToLower can expand
			// invalid UTF-8 threefold and overlap geometrically grown buffers.
			out.pragmas[out.count] = topologyPragma{value, strings.TrimSpace(value)}
			out.count++
		case "_txlock":
			if !txlockSeen {
				txlock = value
				txlockSeen = true
			}
		case "_time_format":
			if !timefmtSeen {
				timefmt = value
				timefmtSeen = true
			}
		default:
			if uri {
				if filename.Len() == len(name) {
					filename.WriteByte('?')
				} else {
					filename.WriteByte('&')
				}
				filename.WriteString(pair)
			}
		}
	}
	if uri {
		out.filename = filename.String()
	}
	if timefmt != "" && timefmt != "sqlite" {
		return out, errors.New("unknown topology _time_format")
	}
	if txlock != "" {
		switch {
		case topologyLowerCompare(txlock, "deferred") == 0,
			topologyLowerCompare(txlock, "immediate") == 0,
			topologyLowerCompare(txlock, "exclusive") == 0:
			out.begin = "begin " + txlock
		default:
			return out, errors.New("unknown topology _txlock")
		}
	}
	sort.Slice(out.pragmas[:out.count], func(i, j int) bool {
		x, y := out.pragmas[i].key, out.pragmas[j].key
		if topologyHasBusyTimeoutPrefix(x) {
			return true
		}
		if topologyHasBusyTimeoutPrefix(y) {
			return false
		}
		return topologyLowerCompare(x, y) < 0
	})
	return out, nil
}

// Compare the exact output of strings.ToLower without allocating it. UTF-8
// order agrees with rune order; decoding invalid bytes emits RuneError exactly
// as strings.ToLower does. Simple case folding would change accepted txlocks.
func topologyLowerCompare(x, y string) int {
	for x != "" && y != "" {
		rx, nx := utf8.DecodeRuneInString(x)
		ry, ny := utf8.DecodeRuneInString(y)
		rx, ry = unicode.ToLower(rx), unicode.ToLower(ry)
		if rx < ry {
			return -1
		}
		if rx > ry {
			return 1
		}
		x, y = x[nx:], y[ny:]
	}
	if x != "" {
		return 1
	}
	if y != "" {
		return -1
	}
	return 0
}

// The modernc ordering treats either busy_timeout prefix as first, including
// returning true when both operands have it. Preserve that existing behavior.
func topologyHasBusyTimeoutPrefix(value string) bool {
	prefix := "busy_timeout"
	for prefix != "" {
		if value == "" {
			return false
		}
		r, n := utf8.DecodeRuneInString(value)
		if unicode.ToLower(r) != rune(prefix[0]) {
			return false
		}
		value, prefix = value[n:], prefix[1:]
	}
	return true
}
