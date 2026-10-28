package bulletinhistory

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// snapshot has explicit structural and encoded-size limits; identities contain
// hashes of full originals, never their potentially longer plaintext.
type snapshot struct {
	Version    int        `json:"version"`
	Sequence   uint64     `json:"sequence"`
	Watermark  time.Time  `json:"watermark"`
	WWV        []Record   `json:"wwv"`
	WCY        []Record   `json:"wcy"`
	Identities []identity `json:"identities,omitempty"`
	WWVLossAt  time.Time  `json:"wwv_loss_at,omitempty"`
	WCYLossAt  time.Time  `json:"wcy_loss_at,omitempty"`
	Clean      bool       `json:"clean"`
}

func (s *Store) load() error {
	if strings.TrimSpace(s.path) == "" {
		return errors.New("bulletin history path is empty")
	}
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxSnapshotBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxSnapshotBytes {
		return errors.New("bulletin history snapshot exceeds size limit")
	}
	saved, err := decodeSnapshot(data)
	if err != nil {
		return err
	}
	s.state = saved
	if len(s.state.Identities) > s.maxKeys {
		s.stats.DedupeEvictions += uint64(len(s.state.Identities) - s.maxKeys)
		s.state.Identities = slices.Clone(s.state.Identities[len(s.state.Identities)-s.maxKeys:])
	}
	s.pruneLocked(time.Now().UTC())
	return nil
}

// decodeSnapshot admits only bounded, strictly decoded and validated state.
// Kept separate from I/O so malformed input can be fuzzed without filesystem
// side effects or starting the persistence worker.
func decodeSnapshot(data []byte) (snapshot, error) {
	if len(data) > maxSnapshotBytes {
		return snapshot{}, errors.New("bulletin history snapshot exceeds size limit")
	}
	var saved snapshot
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&saved); err != nil {
		return snapshot{}, fmt.Errorf("decode bulletin history: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return snapshot{}, errors.New("bulletin history contains trailing data")
	}
	if err := validateSnapshot(saved); err != nil {
		return snapshot{}, err
	}
	return saved, nil
}

func validateSnapshot(saved snapshot) error {
	if saved.Version != 1 {
		return fmt.Errorf("unsupported bulletin history version %d", saved.Version)
	}
	if len(saved.WWV) > maxRecords || len(saved.WCY) > maxRecords || len(saved.Identities) > maxDedupeEntries {
		return errors.New("bulletin history exceeds record or identity bounds")
	}
	maxTime := time.Now().UTC().Add(time.Hour)
	if saved.Watermark.After(maxTime) || saved.WWVLossAt.After(maxTime) || saved.WCYLossAt.After(maxTime) {
		return errors.New("bulletin history contains a future watermark or loss timestamp")
	}
	if (!saved.WWVLossAt.IsZero() && saved.WWVLossAt.After(saved.Watermark)) ||
		(!saved.WCYLossAt.IsZero() && saved.WCYLossAt.After(saved.Watermark)) {
		return errors.New("bulletin history contains a loss later than its watermark")
	}
	for _, records := range [][]Record{saved.WWV, saved.WCY} {
		for _, r := range records {
			if r.Time.IsZero() || r.Time.After(maxTime) || r.Time.After(saved.Watermark) || r.Sequence == 0 || r.Sequence > saved.Sequence || !utf8.ValidString(r.Text) || utf8.RuneCountInString(r.Text) > maxCharacters {
				return errors.New("bulletin history contains an invalid record")
			}
		}
		if !slices.IsSortedFunc(records, compareRecord) {
			return errors.New("bulletin history records are out of order")
		}
	}
	for _, k := range saved.Identities {
		if (k.Kind != "WWV" && k.Kind != "WCY") || k.Time.IsZero() || k.Time.After(maxTime) || k.Time.After(saved.Watermark) {
			return errors.New("bulletin history contains an invalid identity")
		}
	}
	return nil
}

func (s *Store) snapshotLocked(clean bool) snapshot {
	result := s.state
	result.WWV = slices.Clone(s.state.WWV)
	result.WCY = slices.Clone(s.state.WCY)
	result.Identities = slices.Clone(s.state.Identities)
	result.Clean = clean
	return result
}

// worker owns writes and retries; a size-one dirty signal coalesces bursts.
// The final snapshot is attempted after producers have been stopped by runtime.
func (s *Store) worker() {
	defer close(s.done)
	retry := time.NewTicker(5 * time.Second)
	defer retry.Stop()
	needRetry := false
	for {
		select {
		case <-s.stop:
			s.closeErr = s.save(true)
			return
		case <-s.dirty:
			needRetry = s.save(false) != nil
		case <-retry.C:
			if needRetry {
				needRetry = s.save(false) != nil
			}
		}
	}
}

func (s *Store) save(clean bool) error {
	s.mu.Lock()
	state := s.snapshotLocked(clean)
	s.mu.Unlock()
	data, err := json.Marshal(state)
	if err == nil && len(data) > maxSnapshotBytes {
		err = errors.New("bulletin snapshot exceeds size bound")
	}
	if err == nil {
		err = writeSnapshot(s.path, data)
	}
	s.mu.Lock()
	if err != nil {
		changed := s.storageErrorLocked(err)
		s.mu.Unlock()
		if changed {
			log.Printf("Bulletin history storage unavailable: %v", err)
		}
		return err
	}
	if !s.stats.StorageHealthy && s.lastStorageError != "" {
		// Report a possible gap for the 24 hours following recovery.
		s.unknownUntil = time.Now().UTC().Add(retention)
	}
	s.stats.StorageHealthy = true
	s.stats.StorageError = ""
	s.lastStorageError = ""
	s.mu.Unlock()
	return nil
}

func writeSnapshot(path string, data []byte) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("bulletin history path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		_ = os.Remove(tmp)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if err := replaceFile(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
