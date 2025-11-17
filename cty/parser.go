package cty

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"howett.net/plist"
)

// PrefixInfo describes the metadata stored for each CTY entry.
type PrefixInfo struct {
	Country       string  `plist:"Country"`
	Prefix        string  `plist:"Prefix"`
	ADIF          int     `plist:"ADIF"`
	CQZone        int     `plist:"CQZone"`
	ITUZone       int     `plist:"ITUZone"`
	Continent     string  `plist:"Continent"`
	Latitude      float64 `plist:"Latitude"`
	Longitude     float64 `plist:"Longitude"`
	GMTOffset     float64 `plist:"GMTOffset"`
	ExactCallsign bool    `plist:"ExactCallsign"`
}

// CTYDatabase holds the plist data and sorted keys for longest-prefix lookup.
type CTYDatabase struct {
	Data map[string]PrefixInfo
	Keys []string
}

// LoadCTYDatabase loads cty.plist into a lookup database.
func LoadCTYDatabase(path string) (*CTYDatabase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open cty plist: %w", err)
	}
	defer f.Close()
	return LoadCTYDatabaseFromReader(f)
}

// LoadCTYDatabaseFromReader decodes CTY data from an io.Reader (exposed for testing).
func LoadCTYDatabaseFromReader(r io.ReadSeeker) (*CTYDatabase, error) {
	data, err := decodeCTYData(r)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) == len(keys[j]) {
			return keys[i] < keys[j]
		}
		return len(keys[i]) > len(keys[j])
	})
	return &CTYDatabase{Data: data, Keys: keys}, nil
}

func decodeCTYData(r io.ReadSeeker) (map[string]PrefixInfo, error) {
	var raw map[string]PrefixInfo
	decoder := plist.NewDecoder(r)
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode plist: %w", err)
	}
	data := make(map[string]PrefixInfo, len(raw))
	for k, v := range raw {
		norm := strings.ToUpper(strings.TrimSpace(k))
		data[norm] = v
	}
	return data, nil
}

var suffixes = []string{"/QRP", "/P", "/M", "/MM", "/AM"}

func normalizeCallsign(cs string) string {
	cs = strings.ToUpper(strings.TrimSpace(cs))
	for _, suf := range suffixes {
		if strings.HasSuffix(cs, suf) {
			return strings.TrimSuffix(cs, suf)
		}
	}
	return cs
}

// LookupCallsign returns metadata for the callsign or false if unknown.
func (db *CTYDatabase) LookupCallsign(cs string) (*PrefixInfo, bool) {
	cs = normalizeCallsign(cs)
	if info, ok := db.Data[cs]; ok {
		return &info, true
	}

	for _, key := range db.Keys {
		if len(key) > len(cs) {
			continue
		}
		if strings.HasPrefix(cs, key) {
			info := db.Data[key]
			return &info, true
		}
	}
	return nil, false
}

// KeysWithPrefix returns all known CTY keys starting with prefix (used for testing).
func (db *CTYDatabase) KeysWithPrefix(pref string) []string {
	norm := strings.ToUpper(strings.TrimSpace(pref))
	matches := make([]string, 0)
	for _, key := range db.Keys {
		if strings.HasPrefix(key, norm) {
			matches = append(matches, key)
		}
	}
	return matches
}
