package reputation

import (
	"bufio"
	"dxcluster/strutil"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	ipinfoSource = "ipinfo"
)

type ipRange4 struct {
	start       uint32
	end         uint32
	asn         string
	countryCode string
}

type ipRange6 struct {
	start       [16]byte
	end         [16]byte
	asn         string
	countryCode string
}

type ipinfoIndex struct {
	v4       []ipRange4
	v6       []ipRange6
	loadedAt time.Time
	source   string
}

// LoadIPInfoSnapshot parses the IPinfo Lite CSV into an in-memory index.
func LoadIPInfoSnapshot(path string) (*ipinfoIndex, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("ipinfo snapshot path is empty")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(bufio.NewReader(file))
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("ipinfo: read header: %w", err)
	}
	for i := range header {
		header[i] = strings.ToLower(strings.TrimSpace(header[i]))
	}
	startIdx := findHeaderIndex(header, "start_ip", "start", "range_start", "network_start")
	endIdx := findHeaderIndex(header, "end_ip", "end", "range_end", "network_end")
	cidrIdx := findHeaderIndex(header, "cidr", "network", "ip_range", "prefix")
	ipIdx := findHeaderIndex(header, "ip", "address")
	countryCodeIdx := findHeaderIndex(header, "country_code", "countrycode", "country")
	asnIdx := findHeaderIndex(header, "asn", "as_number", "as", "as_number")

	if startIdx < 0 && cidrIdx < 0 && ipIdx < 0 {
		return nil, errors.New("ipinfo: missing IP range columns (start/end, cidr, or ip)")
	}

	stringPool := make(map[string]string)
	index := &ipinfoIndex{
		source:   path,
		loadedAt: stat.ModTime().UTC(),
	}

	for {
		row, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("ipinfo: read row: %w", err)
		}
		start, end, ok := parseRowRange(row, startIdx, endIdx, cidrIdx, ipIdx)
		if !ok {
			continue
		}
		asn := normalizeASN(fieldAt(row, asnIdx))
		countryCode := strutil.NormalizeUpper(fieldAt(row, countryCodeIdx))
		asn = internString(stringPool, asn)
		countryCode = internString(stringPool, countryCode)

		if start.Is4() {
			start4 := start.As4()
			end4 := end.As4()
			index.v4 = append(index.v4, ipRange4{
				start:       ip4ToUint32(start4),
				end:         ip4ToUint32(end4),
				asn:         asn,
				countryCode: countryCode,
			})
			continue
		}
		start16 := start.As16()
		end16 := end.As16()
		index.v6 = append(index.v6, ipRange6{
			start:       start16,
			end:         end16,
			asn:         asn,
			countryCode: countryCode,
		})
	}

	sort.Slice(index.v4, func(i, j int) bool { return index.v4[i].start < index.v4[j].start })
	sort.Slice(index.v6, func(i, j int) bool { return compareIPv6(index.v6[i].start, index.v6[j].start) < 0 })

	return index, nil
}

func (idx *ipinfoIndex) lookup(addr netip.Addr) (LookupResult, bool) {
	if idx == nil {
		return LookupResult{}, false
	}
	if addr.Is4() {
		ip := ip4ToUint32(addr.As4())
		i := sort.Search(len(idx.v4), func(i int) bool { return idx.v4[i].start > ip })
		if i == 0 {
			return LookupResult{}, false
		}
		r := idx.v4[i-1]
		if ip > r.end {
			return LookupResult{}, false
		}
		return LookupResult{
			ASN:         r.asn,
			CountryCode: r.countryCode,
			Source:      ipinfoSource,
			FetchedAt:   idx.loadedAt,
		}, true
	}
	ip6 := addr.As16()
	i := sort.Search(len(idx.v6), func(i int) bool { return compareIPv6(idx.v6[i].start, ip6) > 0 })
	if i == 0 {
		return LookupResult{}, false
	}
	r := idx.v6[i-1]
	if compareIPv6(ip6, r.end) > 0 {
		return LookupResult{}, false
	}
	return LookupResult{
		ASN:         r.asn,
		CountryCode: r.countryCode,
		Source:      ipinfoSource,
		FetchedAt:   idx.loadedAt,
	}, true
}

func parseRowRange(row []string, startIdx, endIdx, cidrIdx, ipIdx int) (netip.Addr, netip.Addr, bool) {
	if startIdx >= 0 && endIdx >= 0 {
		start, ok := parseAddr(fieldAt(row, startIdx))
		if !ok {
			return netip.Addr{}, netip.Addr{}, false
		}
		end, ok := parseAddr(fieldAt(row, endIdx))
		if !ok {
			return netip.Addr{}, netip.Addr{}, false
		}
		return start, end, true
	}
	if cidrIdx >= 0 {
		prefix, ok := parsePrefix(fieldAt(row, cidrIdx))
		if !ok {
			return netip.Addr{}, netip.Addr{}, false
		}
		start, end := prefixRange(prefix)
		return start, end, true
	}
	if ipIdx >= 0 {
		addr, ok := parseAddr(fieldAt(row, ipIdx))
		if !ok {
			return netip.Addr{}, netip.Addr{}, false
		}
		return addr, addr, true
	}
	return netip.Addr{}, netip.Addr{}, false
}

func fieldAt(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return row[idx]
}

func findHeaderIndex(header []string, names ...string) int {
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		for i, col := range header {
			if col == name {
				return i
			}
		}
	}
	return -1
}

func parseAddr(raw string) (netip.Addr, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return netip.Addr{}, false
	}
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		return netip.Addr{}, false
	}
	if !addr.IsValid() {
		return netip.Addr{}, false
	}
	return addr, true
}

func parsePrefix(raw string) (netip.Prefix, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return netip.Prefix{}, false
	}
	prefix, err := netip.ParsePrefix(raw)
	if err != nil {
		return netip.Prefix{}, false
	}
	if !prefix.IsValid() {
		return netip.Prefix{}, false
	}
	return prefix.Masked(), true
}

func prefixRange(prefix netip.Prefix) (netip.Addr, netip.Addr) {
	addr := prefix.Addr()
	bits := prefix.Bits()
	if addr.Is4() {
		start := ip4ToUint32(addr.As4())
		mask := uint32(0xffffffff)
		if bits < 32 {
			mask = mask << (32 - bits)
		}
		end := start | ^mask
		return addr, netip.AddrFrom4(uint32ToIP4(end))
	}
	start := addr.As16()
	end := start
	bitLen := 128
	for i := bits; i < bitLen; i++ {
		byteIdx := i / 8
		bitIdx := 7 - (i % 8)
		end[byteIdx] |= 1 << bitIdx
	}
	return addr, netip.AddrFrom16(end)
}

func ip4ToUint32(ip [4]byte) uint32 {
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}

func uint32ToIP4(v uint32) [4]byte {
	return [4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func compareIPv6(a, b [16]byte) int {
	for i := 0; i < len(a); i++ {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	return 0
}

func normalizeASN(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(strings.ToUpper(raw), "AS")
	if raw == "" {
		return ""
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] >= '0' && raw[i] <= '9' {
			out = append(out, raw[i])
		}
	}
	return string(out)
}

func internString(pool map[string]string, value string) string {
	if value == "" {
		return ""
	}
	if existing, ok := pool[value]; ok {
		return existing
	}
	pool[value] = value
	return value
}
