package reputation

import (
	"bufio"
	"context"
	"dxcluster/strutil"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

const (
	ipinfoPebbleVersion = 1

	ipinfoPebblePrefixV4 = byte('4')
	ipinfoPebblePrefixV6 = byte('6')

	ipinfoPebbleCurrentFile = "CURRENT_DB"

	ipinfoPebbleMetaVersion = "meta|version"
	ipinfoPebbleMetaBuiltAt = "meta|built_at"
	ipinfoPebbleMetaRowsV4  = "meta|rows_v4"
	ipinfoPebbleMetaRowsV6  = "meta|rows_v6"
)

var (
	ipinfoPebbleV4Lower = []byte{ipinfoPebblePrefixV4}
	ipinfoPebbleV4Upper = []byte{ipinfoPebblePrefixV4 + 1}
	ipinfoPebbleV6Lower = []byte{ipinfoPebblePrefixV6}
	ipinfoPebbleV6Upper = []byte{ipinfoPebblePrefixV6 + 1}
)

// ipinfoStore provides range lookups backed by a Pebble database.
// Invariants: keys are sorted by range start; ranges are expected not to overlap.
type ipinfoStore struct {
	db      *pebble.DB
	cache   *pebble.Cache
	path    string
	builtAt time.Time
}

// openIPInfoStore opens an existing Pebble DB for lookups.
func openIPInfoStore(path string, cacheBytes int64) (*ipinfoStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("ipinfo pebble path is empty")
	}
	opts := &pebble.Options{
		ReadOnly: true,
	}
	if cacheBytes > 0 {
		opts.Cache = pebble.NewCache(cacheBytes)
	}
	level := pebble.LevelOptions{
		FilterPolicy: bloom.FilterPolicy(10),
		FilterType:   pebble.TableFilter,
	}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i] = level
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		if opts.Cache != nil {
			opts.Cache.Unref()
		}
		return nil, fmt.Errorf("ipinfo pebble open: %w", err)
	}
	store := &ipinfoStore{
		db:    db,
		cache: opts.Cache,
		path:  path,
	}
	if builtAt, err := readIPInfoMetaTime(db, ipinfoPebbleMetaBuiltAt); err == nil {
		store.builtAt = builtAt
	}
	version, err := readIPInfoMetaInt(db, ipinfoPebbleMetaVersion)
	if err == nil && version != ipinfoPebbleVersion {
		store.Close()
		return nil, fmt.Errorf("ipinfo pebble version %d unsupported (expected %d)", version, ipinfoPebbleVersion)
	}
	if store.builtAt.IsZero() {
		store.builtAt = time.Now().UTC()
	}
	return store, nil
}

// Close releases Pebble resources.
func (s *ipinfoStore) Close() error {
	if s == nil {
		return nil
	}
	if s.db != nil {
		_ = s.db.Close()
	}
	if s.cache != nil {
		s.cache.Unref()
	}
	return nil
}

// lookupV4 returns the range record covering the provided IPv4 address.
func (s *ipinfoStore) lookupV4(addr netip.Addr) (LookupResult, bool) {
	if s == nil || s.db == nil || !addr.Is4() {
		return LookupResult{}, false
	}
	ip := ip4ToUint32(addr.As4())
	key := makeV4Key(ip)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: ipinfoPebbleV4Lower,
		UpperBound: ipinfoPebbleV4Upper,
	})
	if err != nil {
		return LookupResult{}, false
	}
	defer iter.Close()

	if !iter.SeekGE(key) {
		iter.Last()
	} else if bytesCompare(iter.Key(), key) > 0 {
		iter.Prev()
	}
	if !iter.Valid() {
		return LookupResult{}, false
	}
	value := iter.Value()
	endBytes, asn, country, ok := decodeIPInfoValue(value, 4)
	if !ok {
		return LookupResult{}, false
	}
	end := binary.BigEndian.Uint32(endBytes)
	if ip > end {
		return LookupResult{}, false
	}
	return LookupResult{
		ASN:         asn,
		CountryCode: country,
		Source:      ipinfoSource,
		FetchedAt:   s.builtAt,
	}, true
}

// lookupV6 returns the range record covering the provided IPv6 address.
func (s *ipinfoStore) lookupV6(addr netip.Addr) (LookupResult, bool) {
	if s == nil || s.db == nil || !addr.Is6() {
		return LookupResult{}, false
	}
	ip := addr.As16()
	key := makeV6Key(ip)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: ipinfoPebbleV6Lower,
		UpperBound: ipinfoPebbleV6Upper,
	})
	if err != nil {
		return LookupResult{}, false
	}
	defer iter.Close()

	if !iter.SeekGE(key) {
		iter.Last()
	} else if bytesCompare(iter.Key(), key) > 0 {
		iter.Prev()
	}
	if !iter.Valid() {
		return LookupResult{}, false
	}
	value := iter.Value()
	endBytes, asn, country, ok := decodeIPInfoValue(value, 16)
	if !ok {
		return LookupResult{}, false
	}
	var end [16]byte
	copy(end[:], endBytes)
	if compareIPv6(ip, end) > 0 {
		return LookupResult{}, false
	}
	return LookupResult{
		ASN:         asn,
		CountryCode: country,
		Source:      ipinfoSource,
		FetchedAt:   s.builtAt,
	}, true
}

// buildIPInfoPebble imports a CSV snapshot into a new Pebble DB under rootDir.
// It returns the absolute path to the new DB directory.
func buildIPInfoPebble(ctx context.Context, csvPath, rootDir string, compact bool) (string, error) {
	csvPath = strings.TrimSpace(csvPath)
	if csvPath == "" {
		return "", errors.New("ipinfo csv path is empty")
	}
	if strings.TrimSpace(rootDir) == "" {
		return "", errors.New("ipinfo pebble root path is empty")
	}
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return "", fmt.Errorf("ipinfo pebble root mkdir: %w", err)
	}

	dirName := fmt.Sprintf("db-%s", time.Now().UTC().Format("20060102-150405"))
	dbPath := filepath.Join(rootDir, dirName)
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return "", fmt.Errorf("ipinfo pebble mkdir: %w", err)
	}
	cleanup := func(err error) (string, error) {
		_ = os.RemoveAll(dbPath)
		return "", err
	}
	opts := &pebble.Options{
		DisableWAL:            true,
		MemTableSize:          64 << 20,
		L0CompactionThreshold: 4,
		L0StopWritesThreshold: 16,
	}
	var db *pebble.DB
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return cleanup(fmt.Errorf("ipinfo pebble open: %w", err))
	}
	cleanup = func(err error) (string, error) {
		if db != nil {
			_ = db.Close()
		}
		_ = os.RemoveAll(dbPath)
		return "", err
	}

	file, err := os.Open(csvPath)
	if err != nil {
		return cleanup(err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		return cleanup(fmt.Errorf("ipinfo csv header: %w", err))
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
		return cleanup(errors.New("ipinfo csv missing range columns"))
	}

	batch := db.NewBatch()
	defer batch.Close()
	const batchLimit = 20000
	rowCount := 0
	rowCountV4 := 0
	rowCountV6 := 0
	var lastStartV4 uint32
	var lastEndV4 uint32
	var haveV4 bool
	var lastStartV6 [16]byte
	var lastEndV6 [16]byte
	var haveV6 bool

	for {
		if err := ctx.Err(); err != nil {
			return cleanup(err)
		}
		row, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return cleanup(fmt.Errorf("ipinfo csv read: %w", err))
		}
		start, end, ok := parseRowRange(row, startIdx, endIdx, cidrIdx, ipIdx)
		if !ok {
			continue
		}
		asn := normalizeASN(fieldAt(row, asnIdx))
		countryCode := strutil.NormalizeUpper(fieldAt(row, countryCodeIdx))
		if start.Is4() {
			start4 := ip4ToUint32(start.As4())
			end4 := ip4ToUint32(end.As4())
			if haveV4 {
				if start4 < lastStartV4 {
					return cleanup(fmt.Errorf("ipinfo csv v4 ranges out of order at %d < %d", start4, lastStartV4))
				}
				if start4 <= lastEndV4 {
					return cleanup(fmt.Errorf("ipinfo csv v4 ranges overlap at %d <= %d", start4, lastEndV4))
				}
			}
			haveV4 = true
			lastStartV4 = start4
			lastEndV4 = end4
			key := makeV4Key(start4)
			endBytes4 := end.As4()
			value := encodeIPInfoValue(endBytes4[:], asn, countryCode)
			if err := batch.Set(key, value, pebble.NoSync); err != nil {
				return cleanup(err)
			}
			rowCountV4++
		} else {
			start16 := start.As16()
			end16 := end.As16()
			if haveV6 {
				if compareIPv6(start16, lastStartV6) < 0 {
					return cleanup(fmt.Errorf("ipinfo csv v6 ranges out of order"))
				}
				if compareIPv6(start16, lastEndV6) <= 0 {
					return cleanup(fmt.Errorf("ipinfo csv v6 ranges overlap"))
				}
			}
			haveV6 = true
			lastStartV6 = start16
			lastEndV6 = end16
			key := makeV6Key(start16)
			value := encodeIPInfoValue(end16[:], asn, countryCode)
			if err := batch.Set(key, value, pebble.NoSync); err != nil {
				return cleanup(err)
			}
			rowCountV6++
		}
		rowCount++
		if rowCount%batchLimit == 0 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return cleanup(err)
			}
			batch.Reset()
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return cleanup(err)
	}
	if err := db.Set([]byte(ipinfoPebbleMetaVersion), encodeIPInfoMetaInt(ipinfoPebbleVersion), pebble.NoSync); err != nil {
		return cleanup(err)
	}
	now := time.Now().UTC()
	if err := db.Set([]byte(ipinfoPebbleMetaBuiltAt), encodeIPInfoMetaTime(now), pebble.NoSync); err != nil {
		return cleanup(err)
	}
	if err := db.Set([]byte(ipinfoPebbleMetaRowsV4), encodeIPInfoMetaInt(rowCountV4), pebble.NoSync); err != nil {
		return cleanup(err)
	}
	if err := db.Set([]byte(ipinfoPebbleMetaRowsV6), encodeIPInfoMetaInt(rowCountV6), pebble.NoSync); err != nil {
		return cleanup(err)
	}
	if err := db.Flush(); err != nil {
		return cleanup(err)
	}
	if compact {
		if err := ctx.Err(); err != nil {
			return cleanup(err)
		}
		// Compact full keyspace to minimize read amplification for daily lookups.
		compactStart := time.Now().UTC()
		startKey := []byte{0x00}
		endKey := []byte{0xFF}
		if err := db.Compact(startKey, endKey, false); err != nil {
			return cleanup(fmt.Errorf("ipinfo pebble compact: %w", err))
		}
		log.Printf("IPinfo pebble compaction completed in %s", time.Since(compactStart))
	}
	if err := db.Close(); err != nil {
		return cleanup(err)
	}
	db = nil
	return dbPath, nil
}

func resolveIPInfoPebblePath(rootDir string) (string, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return "", errors.New("ipinfo pebble root path is empty")
	}
	currentPath := filepath.Join(rootDir, ipinfoPebbleCurrentFile)
	if data, err := os.ReadFile(currentPath); err == nil {
		trimmed := strings.TrimSpace(string(data))
		if trimmed != "" {
			return filepath.Join(rootDir, trimmed), nil
		}
	}
	return filepath.Join(rootDir, "db"), nil
}

func updateIPInfoPebbleCurrent(rootDir, dbPath string) error {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return errors.New("ipinfo pebble root path is empty")
	}
	if dbPath == "" {
		return errors.New("ipinfo pebble db path is empty")
	}
	rel, err := filepath.Rel(rootDir, dbPath)
	if err != nil {
		return err
	}
	tmp := filepath.Join(rootDir, ipinfoPebbleCurrentFile+".tmp")
	if err := os.WriteFile(tmp, []byte(rel), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(rootDir, ipinfoPebbleCurrentFile))
}

func cleanupIPInfoPebbleDirs(rootDir, keepPath string) error {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return nil
	}
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "db" || strings.HasPrefix(name, "db-") {
			full := filepath.Join(rootDir, name)
			if keepPath != "" {
				if samePath(full, keepPath) {
					continue
				}
			}
			_ = os.RemoveAll(full)
		}
	}
	return nil
}

func samePath(a, b string) bool {
	absA, errA := filepath.Abs(a)
	absB, errB := filepath.Abs(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return absA == absB
}

func loadIPv4IndexFromStore(store *ipinfoStore) (*ipinfoIndex, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("ipinfo store not initialized")
	}
	iter, err := store.db.NewIter(&pebble.IterOptions{
		LowerBound: ipinfoPebbleV4Lower,
		UpperBound: ipinfoPebbleV4Upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	stringPool := make(map[string]string)
	index := &ipinfoIndex{
		loadedAt: store.builtAt,
		source:   store.path,
	}
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 5 {
			continue
		}
		start := binary.BigEndian.Uint32(key[1:])
		value := iter.Value()
		endBytes, asn, country, ok := decodeIPInfoValue(value, 4)
		if !ok {
			continue
		}
		asn = internString(stringPool, asn)
		country = internString(stringPool, country)
		index.v4 = append(index.v4, ipRange4{
			start:       start,
			end:         binary.BigEndian.Uint32(endBytes),
			asn:         asn,
			countryCode: country,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return index, nil
}

func makeV4Key(start uint32) []byte {
	var buf [5]byte
	buf[0] = ipinfoPebblePrefixV4
	binary.BigEndian.PutUint32(buf[1:], start)
	return buf[:]
}

func makeV6Key(start [16]byte) []byte {
	var buf [17]byte
	buf[0] = ipinfoPebblePrefixV6
	copy(buf[1:], start[:])
	return buf[:]
}

func encodeIPInfoValue(end []byte, asn, country string) []byte {
	out := make([]byte, 0, len(end)+len(asn)+len(country)+16)
	out = append(out, end...)
	out = appendUvarint(out, uint64(len(asn)))
	out = append(out, asn...)
	out = appendUvarint(out, uint64(len(country)))
	out = append(out, country...)
	return out
}

func decodeIPInfoValue(data []byte, endLen int) ([]byte, string, string, bool) {
	if len(data) < endLen {
		return nil, "", "", false
	}
	end := data[:endLen]
	rest := data[endLen:]
	asnLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, "", "", false
	}
	rest = rest[n:]
	if int(asnLen) > len(rest) {
		return nil, "", "", false
	}
	asn := string(rest[:asnLen])
	rest = rest[asnLen:]
	countryLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, "", "", false
	}
	rest = rest[n:]
	if int(countryLen) > len(rest) {
		return nil, "", "", false
	}
	country := string(rest[:countryLen])
	return end, asn, country, true
}

func appendUvarint(dst []byte, value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:n]...)
}

func encodeIPInfoMetaInt(value int) []byte {
	return []byte(fmt.Sprintf("%d", value))
}

func encodeIPInfoMetaTime(value time.Time) []byte {
	return []byte(fmt.Sprintf("%d", value.Unix()))
}

func readIPInfoMetaInt(db *pebble.DB, key string) (int, error) {
	data, closer, err := db.Get([]byte(key))
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	var value int
	if _, err := fmt.Sscanf(string(data), "%d", &value); err != nil {
		return 0, err
	}
	return value, nil
}

func readIPInfoMetaTime(db *pebble.DB, key string) (time.Time, error) {
	data, closer, err := db.Get([]byte(key))
	if err != nil {
		return time.Time{}, err
	}
	defer closer.Close()
	var unix int64
	if _, err := fmt.Sscanf(string(data), "%d", &unix); err != nil {
		return time.Time{}, err
	}
	if unix <= 0 {
		return time.Time{}, errors.New("invalid meta time")
	}
	return time.Unix(unix, 0).UTC(), nil
}

func bytesCompare(a, b []byte) int {
	min := len(a)
	if len(b) < min {
		min = len(b)
	}
	for i := 0; i < min; i++ {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}
