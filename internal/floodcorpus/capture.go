package floodcorpus

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const captureRecordBytes = 32768

type captureRecord struct {
	Kind       string            `json:"kind"`
	Seq        *int64            `json:"seq"`
	NS         *int64            `json:"ns"`
	Version    *int              `json:"version,omitempty"`
	Direction  string            `json:"direction,omitempty"`
	Offset     *int64            `json:"offset,omitempty"`
	Data       string            `json:"data,omitempty"`
	Reason     *string           `json:"reason,omitempty"`
	Received   map[string]*int64 `json:"received,omitempty"`
	Recorded   map[string]*int64 `json:"recorded,omitempty"`
	Sent       map[string]*int64 `json:"sent,omitempty"`
	UTC        string            `json:"utc,omitempty"`
	Python     string            `json:"python,omitempty"`
	ToolSHA    string            `json:"tool_sha256,omitempty"`
	Listen     []json.RawMessage `json:"listen,omitempty"`
	Upstream   []json.RawMessage `json:"upstream,omitempty"`
	MaxSeconds *int64            `json:"max_seconds,omitempty"`
	MaxBytes   *int64            `json:"max_bytes,omitempty"`
}

// CaptureInfo contains bounded counters only; no endpoints, errors or payload.
type CaptureInfo struct {
	SHA256                   string
	StartUTC                 string
	LastNS                   int64
	Complete                 bool
	Received, Recorded, Sent [2]int64
	Records                  int64
}

func directionIndex(direction string) int {
	switch direction {
	case "c2s":
		return 0
	case "s2c":
		return 1
	default:
		return -1
	}
}

// ScanCapture validates the complete recorder-v1 journal before any publication.
// visit is private orchestration used only in the second, rehashed pass.
func ScanCapture(path string) (CaptureInfo, error) { return scanCapture(path, nil) }

func scanCapture(path string, visit func(captureRecord, []byte) error) (CaptureInfo, error) {
	var result CaptureInfo
	if !localPath(path) {
		return result, errors.New("local capture required")
	}
	f, err := os.Open(path) // #nosec G703 -- Local file is explicitly selected by the operator, never derived from captured fields.
	if err != nil {
		return result, errors.New("cannot open capture")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > MaxInputBytes {
		return result, errors.New("capture must be a bounded regular file")
	}
	h := sha256.New()
	r := bufio.NewReaderSize(io.TeeReader(io.LimitReader(f, MaxInputBytes+1), h), captureRecordBytes+1)
	var total, previous int64
	previous = -1
	var offsets [2]int64
	var eof [2]bool
	ended := false
	var limit int64
	for {
		raw, readErr := r.ReadSlice('\n')
		if len(raw) == 0 && readErr == io.EOF {
			break
		}
		total += int64(len(raw))
		if readErr != nil || len(raw) > captureRecordBytes || total > MaxInputBytes || ended {
			return result, errors.New("damaged capture record or input limit")
		}
		var rec captureRecord
		if strictJSON(raw, &rec) != nil || rec.Seq == nil || *rec.Seq != result.Records || rec.NS == nil || *rec.NS < 0 || *rec.NS < previous {
			return result, fmt.Errorf("invalid capture record %d", result.Records)
		}
		previous = *rec.NS
		result.Records++
		var data []byte
		if result.Records == 1 {
			if rec.Kind != "start" || rec.Version == nil || *rec.Version != 1 || !validUTC(rec.UTC) || strings.ToLower(rec.ToolSHA) != RecorderSHA256 || rec.MaxBytes == nil || *rec.MaxBytes < 65536 || *rec.MaxBytes > MaxInputBytes || rec.MaxSeconds == nil || *rec.MaxSeconds < 1 || *rec.MaxSeconds > 3600 {
				return result, errors.New("unsupported capture start or recorder hash")
			}
			limit = *rec.MaxBytes
			result.StartUTC = rec.UTC
		} else {
			switch rec.Kind {
			case "data":
				i := directionIndex(rec.Direction)
				if i < 0 || eof[i] || rec.Offset == nil || *rec.Offset != offsets[i] {
					return result, errors.New("capture directional offset or EOF violation")
				}
				data, err = base64.StdEncoding.Strict().DecodeString(rec.Data)
				if err != nil || len(data) == 0 || len(data) > 16384 {
					return result, errors.New("invalid capture payload")
				}
				offsets[i] += int64(len(data))
			case "eof":
				i := directionIndex(rec.Direction)
				if i < 0 || eof[i] {
					return result, errors.New("invalid capture EOF")
				}
				eof[i] = true
			case "end":
				if rec.Reason == nil || len(*rec.Reason) > 512 {
					return result, errors.New("invalid capture termination")
				}
				if err := captureCounts(&result, rec, offsets); err != nil {
					return result, err
				}
				result.Complete = *rec.Reason == "eof"
				if result.Complete && (!eof[0] || !eof[1] || result.Received != offsets || result.Sent != offsets) {
					return result, errors.New("incomplete normal EOF")
				}
				ended = true
				result.LastNS = *rec.NS
			default:
				return result, errors.New("unknown capture record")
			}
		}
		if visit != nil {
			if err := visit(rec, data); err != nil {
				return result, err
			}
		}
	}
	if !ended || total > limit {
		return result, errors.New("missing capture footer or declared file limit")
	}
	result.SHA256 = hex.EncodeToString(h.Sum(nil))
	return result, nil
}

func captureCounts(info *CaptureInfo, rec captureRecord, offsets [2]int64) error {
	for k, m := range []map[string]*int64{rec.Received, rec.Recorded, rec.Sent} {
		if len(m) != 2 {
			return errors.New("invalid capture counters")
		}
		for i, name := range []string{"c2s", "s2c"} {
			n := m[name]
			if n == nil || *n < 0 {
				return errors.New("invalid capture counter")
			}
			switch k {
			case 0:
				info.Received[i] = *n
			case 1:
				info.Recorded[i] = *n
			case 2:
				info.Sent[i] = *n
			}
		}
	}
	for i := range 2 {
		if info.Recorded[i] != offsets[i] || info.Received[i] < info.Recorded[i] || info.Recorded[i] < info.Sent[i] || info.Received[i]-info.Recorded[i] > 16384 || info.Recorded[i]-info.Sent[i] > 16384 {
			return errors.New("impossible capture counters")
		}
	}
	return nil
}
