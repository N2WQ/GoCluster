// Package floodcorpus owns bounded, private offline flood evidence. It never
// connects to a network, starts a peer manager or selects server policy.
package floodcorpus

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

const (
	Schema               = 1
	MaxRecordBytes       = 256 << 10
	MaxInputBytes  int64 = 256 << 20
	MaxEvents            = 100000
	RecorderSHA256       = "274a81a5d1ccbb1292d8ebdeaf7acb5fcf23acc07920508aa0e427eda6a9b490"
	corpusStage          = "native_original_fields_valid_assumed_eligible"
)

// Event retains receipt timing separately from the decoded sentence's wire
// time. SentenceBase64 is AFTER native Telnet/framing, not raw socket bytes.
type Event struct {
	Version        int         `json:"version"`
	Arrival        int         `json:"arrival"`
	NS             int64       `json:"receipt_ns"`
	Source         string      `json:"source"`
	Transport      string      `json:"transport"`
	Scenario       string      `json:"synthetic_scenario,omitempty"`
	SentenceBase64 string      `json:"decoded_sentence_base64,omitempty"`
	Spot           *SpotRecord `json:"synthetic_spot,omitempty"`
	Truth          *TruthLabel `json:"independent_truth,omitempty"`
}

// SpotRecord is a synthetic-input projection; real peer inputs use native
// sentence decoding. Comment bytes, when relevant, stay in the base64 sentence.
type SpotRecord struct {
	DX        string  `json:"dx"`
	DE        string  `json:"de"`
	Node      string  `json:"node,omitempty"`
	IP        string  `json:"ip,omitempty"`
	Mode      string  `json:"mode"`
	Frequency float64 `json:"frequency_khz"`
	Time      string  `json:"wire_time_utc"`
}

// TruthLabel is optional independently supplied knowledge, never inferred by
// field equality. IDs and provenance are corpus-local bounded identifiers.
type TruthLabel struct {
	Kind       string `json:"kind"`
	LogicalID  string `json:"logical_id"`
	Provenance string `json:"provenance"`
}

// Manifest is written last. A present manifest binds the exact event bytes;
// complete=false still explicitly disqualifies the capture as a complete run.
type Manifest struct {
	Version         int    `json:"version"`
	Stage           string `json:"stage"`
	Synthetic       bool   `json:"synthetic"`
	CaptureSHA256   string `json:"capture_sha256,omitempty"`
	RecorderSHA256  string `json:"recorder_sha256,omitempty"`
	EventsSHA256    string `json:"events_sha256"`
	Direction       string `json:"cluster_directed_direction,omitempty"`
	Transport       string `json:"transport"`
	Complete        bool   `json:"complete"`
	CaptureComplete bool   `json:"capture_complete"`
	PendingTail     bool   `json:"pending_native_tail"`
	Received        int64  `json:"received_bytes"`
	Recorded        int64  `json:"recorded_bytes"`
	Sent            int64  `json:"locally_send_accepted_bytes"`
	Unsent          int64  `json:"recorded_unsent_bytes"`
	Events          int    `json:"events"`
	Rejected        int    `json:"native_rejected_frames"`
	Other           int    `json:"other_sentences"`
	Timing          string `json:"timing_limit"`
	Eligibility     string `json:"eligibility_limit"`
	Implementation  Build  `json:"implementation"`
	CaptureStartUTC string `json:"capture_start_utc,omitempty"`
	LastNS          int64  `json:"last_observation_ns"`
}

func validUTC(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return false
	}
	_, offset := at.Zone()
	return offset == 0
}

// Build reports embedded provenance without shelling out or guessing a revision.
type Build struct {
	Revision  string `json:"revision"`
	Modified  *bool  `json:"modified"`
	GoVersion string `json:"go_version"`
}

func buildIdentity() Build {
	b := Build{Revision: "unavailable", GoVersion: runtime.Version()}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				b.Revision = s.Value
			case "vcs.modified":
				modified := s.Value == "true"
				b.Modified = &modified
			}
		}
	}
	return b
}

func identifier(s string, maximum int) bool {
	if s == "" || len(s) > maximum {
		return false
	}
	for _, c := range s {
		valid := c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.ContainsRune("_.-", c)
		if !valid {
			return false
		}
	}
	return true
}

// strictJSON rejects duplicate keys/deep nesting before typed decoding. Neither
// parser errors nor target-supplied values are returned as terminal diagnostics.
func strictJSON(raw []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := jsonValue(d, 0); err != nil {
		return errors.New("invalid JSON structure")
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON value")
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return errors.New("invalid JSON fields")
	}
	return nil
}

func integerFields(raw []byte, names ...string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	for _, name := range names {
		value, present := fields[name]
		if !present {
			return false
		}
		d := json.NewDecoder(bytes.NewReader(value))
		d.UseNumber()
		var scalar any
		if d.Decode(&scalar) != nil {
			return false
		}
		n, ok := scalar.(json.Number)
		if !ok {
			return false
		}
		if _, err := n.Int64(); err != nil {
			return false
		}
	}
	return true
}

func jsonValue(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("nesting limit")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			s, ok := key.(string)
			if !ok || seen[s] || len(seen) >= 64 {
				return errors.New("duplicate or excessive keys")
			}
			seen[s] = true
			if err := jsonValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := jsonValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid delimiter")
	}
	_, err = d.Token()
	return err
}
