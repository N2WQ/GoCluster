// File role: Explicit v6 authoritative reads, without migration or write authority.
package filter

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"dxcluster/spot"
)

func v6UserRecordPath(callsign string) (string, error) {
	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	if !spot.IsValidNormalizedCallsign(callsign) {
		return "", errors.New("invalid login callsign")
	}
	return filepath.Abs(userRecordPath(callsign))
}

// LoadV6UserRecord uses the same validated full-SSID destination as preparation.
// Missing files return os.ErrNotExist; all other refusals preserve the bytes.
// The coordinator, not this reader, owns the lease through replacement.
func LoadV6UserRecord(callsign string) (*V6UserRecord, error) {
	path, err := v6UserRecordPath(callsign)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := readUserRecordBytes(file)
	if err != nil {
		return nil, err
	}
	return DecodeV6UserRecord(data)
}
