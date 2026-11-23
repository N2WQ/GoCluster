// File role: Derive the public JSON v1 record ceiling from the node's input
// limits and the readable archive. It is a wire-size bound, never an allocation.
package telnet

import (
	"math"
	"math/big"
	"strconv"

	"dxcluster/spot"
)

// The three optional fields have independent raw-byte limits and omission
// counters. Quoting the bytes on the wire can require up to six times as much.
const (
	jsonNodeLimit      = 256
	jsonDXCountryLimit = 256
	jsonDECountryLimit = 256
)

// Store decimal digits once. A legal 64-bit configuration need not have an
// escaped bound representable as an int64, or even as a uint64.
type jsonSpotLimits struct {
	spotBytes string // Includes the terminating LF.
}

func newJSONSpotLimits(commandBytes int, dxSummitResponseBytes int64, taxonomy *spot.Taxonomy) jsonSpotLimits {
	const escapeFactor = 6 // A control byte can become a six-byte Unicode escape.

	// PC11/61 peer lines are capped at 64 KiB. Both RBN feeds and the human
	// upstream reader cap lines at 1,024 bytes. Their comment rewrites and CW
	// projection fit under the peer envelope. Manual DX uses the edited command
	// limit. PSKReporter emits an empty comment and only routed taxonomy modes.
	// DXSummit limits the entire JSON response before decoding. A malformed raw
	// byte in a JSON string can expand into three UTF-8 bytes for U+FFFD, which
	// AppendQuote copies literally. An escaped control is already six input
	// bytes. Six wire bytes per response byte covers both paths.
	liveComment := max(int64(64<<10), int64(commandBytes), dxSummitResponseBytes)
	live := big.NewInt(liveComment)
	live.Add(live, big.NewInt(int64(max(len("RTTY"), taxonomy.MaxModeLabelBytes()))))
	live.Mul(live, big.NewInt(escapeFactor))

	// Every archive version has uint16 string lengths. Reserve the full mode,
	// band, source and comment, plus both calls even for noncanonical but
	// readable fields. Unicode uppercasing, including invalid-byte repair, can
	// produce at most four output bytes per input byte for each call. This
	// deliberately covers the full normalized calls before text display rules.
	const archiveVariableBytes = escapeFactor * math.MaxUint16 * (4 + 2*4)
	if live.Cmp(big.NewInt(archiveVariableBytes)) < 0 {
		live.SetInt64(archiveVariableBytes)
	}

	// All fixed grammar, separators, LF, booleans, nulls, enums, station
	// regions/locations, and numbers fit in 2 KiB: two finite float64 tokens
	// at <=384 bytes, <=16 integer/timestamp tokens at <=22, <=64 separators
	// and delimiters, with >850 bytes left for short strings and literals.
	// Each endpoint contributes at most six JSON locator characters and a
	// two-character field, even if its immutable input has greater precision.
	// The live calls and band also fit here. CW labels come from disjoint
	// 1,024-byte RBN input or a validated 1,024-byte archive speed field:
	// at most 170 labels (shortest "0 WPM" plus a separator). Include three
	// extra JSON delimiters per label and allow for array brackets.
	const fixedBytes = 2048 + 1024 + 3*170 + 2
	const optionalBytes = escapeFactor * (jsonNodeLimit + jsonDXCountryLimit + jsonDECountryLimit)
	live.Add(live, big.NewInt(fixedBytes+optionalBytes))
	for _, name := range taxonomy.EventNames(^spot.EventMask(0)) {
		// Names are ASCII letters, digits, '-' or '_'. A quote pair and a
		// separator cover each name, even when all 64 bits are active.
		live.Add(live, big.NewInt(int64(len(name))))
		live.Add(live, big.NewInt(3))
	}

	// JSON clients parsing numbers as binary64 must never round the advertised
	// size down. Beyond its consecutive-integer range publish an exact power
	// of two. This is still only an envelope, including at extreme settings.
	if live.BitLen() > 53 {
		rounded := new(big.Int).Sub(live, big.NewInt(1))
		live.Lsh(big.NewInt(1), uint(rounded.BitLen()))
	}
	return jsonSpotLimits{spotBytes: live.String()}
}

func (l jsonSpotLimits) appendHelloLimits(dst []byte) []byte {
	dst = append(dst, `{"spotBytes":`...)
	dst = append(dst, l.spotBytes...)
	dst = append(dst, `,"nodeBytes":`...)
	dst = strconv.AppendInt(dst, jsonNodeLimit, 10)
	dst = append(dst, `,"dxCountryBytes":`...)
	dst = strconv.AppendInt(dst, jsonDXCountryLimit, 10)
	dst = append(dst, `,"deCountryBytes":`...)
	dst = strconv.AppendInt(dst, jsonDECountryLimit, 10)
	return append(dst, '}')
}
