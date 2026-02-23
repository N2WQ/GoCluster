package reputation

import (
	"dxcluster/strutil"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Store persists per-callsign ASN/country history outside filter.UserRecord.
type Store struct {
	dir           string
	maxASNHistory int
	maxGeoHistory int
	mu            sync.Mutex
}

// Record captures bounded reputation history for a callsign.
type Record struct {
	RecentASNs       []string  `yaml:"recent_asns,omitempty"`
	RecentCountries  []string  `yaml:"recent_countries,omitempty"`
	RecentContinents []string  `yaml:"recent_continents,omitempty"`
	UpdatedAt        time.Time `yaml:"updated_at,omitempty"`
}

// NewStore builds a reputation store rooted at dir with bounded history lengths.
func NewStore(dir string, maxASNHistory, maxGeoHistory int) *Store {
	if maxASNHistory <= 0 {
		maxASNHistory = 5
	}
	if maxGeoHistory <= 0 {
		maxGeoHistory = 5
	}
	if strings.TrimSpace(dir) == "" {
		dir = "data/reputation"
	}
	return &Store{
		dir:           dir,
		maxASNHistory: maxASNHistory,
		maxGeoHistory: maxGeoHistory,
	}
}

// Load reads a stored record from disk.
func (s *Store) Load(callsign string) (*Record, error) {
	callsign = normalizeCallKey(callsign)
	if callsign == "" {
		return nil, errors.New("empty callsign")
	}
	path := s.recordPath(callsign)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rec Record
	if err := yaml.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	rec.RecentASNs = trimHistory(rec.RecentASNs, s.maxASNHistory)
	rec.RecentCountries = trimHistory(rec.RecentCountries, s.maxGeoHistory)
	rec.RecentContinents = trimHistory(rec.RecentContinents, s.maxGeoHistory)
	return &rec, nil
}

// Touch loads and updates ASN/country history, then persists the record.
func (s *Store) Touch(callsign, asn, country, continent string) (*Record, bool, error) {
	callsign = normalizeCallKey(callsign)
	if callsign == "" {
		return nil, false, errors.New("empty callsign")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, err := s.Load(callsign)
	created := false
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			rec = &Record{}
			created = true
		} else {
			return nil, false, err
		}
	}
	if asn = strings.TrimSpace(asn); asn != "" {
		rec.RecentASNs = prependUnique(asn, rec.RecentASNs, s.maxASNHistory)
	}
	if country = strings.TrimSpace(country); country != "" {
		rec.RecentCountries = prependUnique(country, rec.RecentCountries, s.maxGeoHistory)
	}
	if continent = strings.TrimSpace(continent); continent != "" {
		rec.RecentContinents = prependUnique(continent, rec.RecentContinents, s.maxGeoHistory)
	}
	rec.UpdatedAt = time.Now().UTC()
	if err := s.save(callsign, rec); err != nil {
		return nil, created, err
	}
	return rec, created, nil
}

func (s *Store) save(callsign string, rec *Record) error {
	if rec == nil {
		return errors.New("nil record")
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	path := s.recordPath(callsign)
	data, err := yaml.Marshal(rec)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (s *Store) recordPath(callsign string) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s.yaml", strings.ToUpper(callsign)))
}

func normalizeCallKey(call string) string {
	return strutil.NormalizeUpper(call)
}

func trimHistory(values []string, limit int) []string {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	return values[:limit]
}

func prependUnique(value string, existing []string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	out := make([]string, 0, len(existing)+1)
	out = append(out, value)
	for _, v := range existing {
		if v == value {
			continue
		}
		out = append(out, v)
		if len(out) >= limit {
			break
		}
	}
	return out
}
