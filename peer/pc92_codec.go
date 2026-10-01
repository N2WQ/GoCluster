package peer

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"

	"dxcluster/config"
)

var ErrUnsupportedPC92 = errors.New("unsupported PC92 action")

// PC92Entry retains absence of IP separately from a present address. Consumers
// must not erase previously learned metadata merely because an entry omits it.
type PC92Entry struct {
	Call           string
	Flags          uint8
	Version, Build string
	IP             netip.Addr
}

func (e PC92Entry) IsNode() bool     { return e.Flags&4 != 0 }
func (e PC92Entry) IsExternal() bool { return e.Flags&2 != 0 }
func (e PC92Entry) Here() bool       { return e.Flags&1 != 0 }

// PC92Record separates the sender of an update from its subject. Ingress is
// deliberately absent: the authenticated session/controller supplies it, never
// untrusted wire data. A successfully decoded C is a complete atomic snapshot,
// including the valid empty-membership case.
type PC92Record struct {
	Origin, Timestamp, Action string
	TimestampValue            float64
	Subject                   PC92Entry
	SubjectImplicit           bool
	Members                   []PC92Entry
	NodeCount, UserCount      int
	Extensions                []string
	Hop                       int
}

// CanonicalPC92Call shares the protocol identity rule with config validation.
// Local publishers additionally enforce the qualified 15-byte envelope.
func CanonicalPC92Call(call string) (string, bool) {
	return config.CanonicalPeeringCall(call)
}

// DecodePC92 validates the entire record before returning any authority-bearing
// data. It never updates freshness, topology, caches, or the session. Unknown
// actions are rejected before entry decoding so F/R cannot become memberships.
func DecodePC92(frame *Frame) (*PC92Record, error) {
	if frame == nil || frame.Type != "PC92" {
		return nil, errors.New("not a PC92 frame")
	}
	fields := frame.payloadFields()
	if len(fields) < 4 {
		return nil, errors.New("PC92 requires origin, timestamp, action and subject")
	}
	action := fields[2]
	if action != "A" && action != "C" && action != "D" && action != "K" {
		return nil, ErrUnsupportedPC92
	}
	if err := validatePC92Fields(fields, frame.Hop); err != nil {
		return nil, err
	}
	origin, ok := CanonicalPC92Call(fields[0])
	if !ok {
		return nil, errors.New("invalid PC92 origin")
	}
	stamp, err := ParsePC9xTimestamp(fields[1])
	if err != nil {
		return nil, err
	}
	record := &PC92Record{Origin: origin, Timestamp: fields[1], TimestampValue: stamp, Action: action, Hop: frame.Hop}
	if fields[3] == "" && (action == "A" || action == "D") {
		record.Subject = PC92Entry{Call: origin, Flags: 5}
		record.SubjectImplicit = true
	} else {
		record.Subject, err = DecodePC92Entry(fields[3])
		if err != nil || !record.Subject.IsNode() {
			return nil, errors.New("PC92 subject must be a valid node entry")
		}
		if record.Subject.Call != origin && !record.Subject.IsExternal() {
			return nil, errors.New("PC92 non-origin subject must be external")
		}
	}
	if action == "K" {
		if err := decodePC92Keepalive(record, fields); err != nil {
			return nil, err
		}
		return record, nil
	}
	record.Members = make([]PC92Entry, 0, len(fields)-4)
	for _, field := range fields[4:] {
		entry, err := DecodePC92Entry(field)
		if err != nil {
			return nil, fmt.Errorf("invalid PC92 member: %w", err)
		}
		if entry.Call == record.Subject.Call {
			return nil, errors.New("PC92 member cannot be its own subject")
		}
		record.Members = append(record.Members, entry)
	}
	if (action == "A" || action == "D") && len(record.Members) == 0 {
		return nil, errors.New("PC92 A/D requires at least one member")
	}
	return record, nil
}

func validatePC92Fields(fields []string, hop int) error {
	if hop < 0 || hop > 99 {
		return errors.New("PC92 hop must be between 0 and 99")
	}
	if len(fields) > 8195 {
		return errors.New("PC92 exceeds 8192 records")
	}
	bytes := len("PC92^") + len(fmt.Sprintf("^H%d^", hop)) - 1
	for _, field := range fields {
		bytes += len(field) + 1
		if bytes > MaxPeerFrameBytes {
			return errors.New("PC92 exceeds the frame envelope")
		}
		for i := range field {
			if field[i] < 32 || field[i] > 126 || field[i] == '^' || field[i] == '~' {
				return errors.New("invalid PC92 field character")
			}
		}
	}
	return nil
}

func decodePC92Keepalive(record *PC92Record, fields []string) error {
	if len(fields) < 6 {
		return errors.New("PC92 K requires node and user counts")
	}
	var err error
	if record.NodeCount, err = pc92Count(fields[4]); err != nil {
		return err
	}
	if record.UserCount, err = pc92Count(fields[5]); err != nil {
		return err
	}
	if len(fields) > 6 {
		// Current DXSpider emits IP, then branch/revision. Preserve further
		// bounded extensions for forwarding rather than treating them as users.
		if fields[6] != "" {
			ip, err := decodePC92IP(fields[6])
			if err != nil {
				return err
			}
			if !record.Subject.IP.IsValid() {
				record.Subject.IP = ip
			}
		}
		record.Extensions = append([]string(nil), fields[6:]...)
	}
	return nil
}

func pc92Count(raw string) (int, error) {
	if raw == "" || len(raw) > 10 {
		return 0, errors.New("invalid PC92 count")
	}
	for i := range raw {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, errors.New("invalid PC92 count")
		}
	}
	n, err := strconv.ParseUint(raw, 10, 31)
	if err != nil {
		return 0, errors.New("invalid PC92 count")
	}
	return int(n), nil
}

// ParsePC9xTimestamp parses the daily numeric timestamp shared by PC92/PC93.
// Clock-window and origin-order checks belong to the controller and must occur
// only after the complete action payload has validated.
func ParsePC9xTimestamp(raw string) (float64, error) {
	if len(raw) == 0 || len(raw) > 32 || raw[0] == '.' || raw[len(raw)-1] == '.' {
		return 0, errors.New("invalid PC9x timestamp")
	}
	dots := 0
	for i := range raw {
		if raw[i] == '.' {
			dots++
		} else if raw[i] < '0' || raw[i] > '9' {
			return 0, errors.New("invalid PC9x timestamp")
		}
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || dots > 1 || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value >= 86400 {
		return 0, errors.New("invalid PC9x timestamp")
	}
	return value, nil
}

// EncodePC92 produces one complete bounded frame; it does not allocate a
// timestamp or change any publication state. Callers own sequence+queue order.
func EncodePC92(record *PC92Record) (string, error) {
	if record == nil {
		return "", errors.New("nil PC92 record")
	}
	subjectEntry := record.Subject
	if record.Action == "K" && len(record.Extensions) > 0 && record.Extensions[0] != "" {
		if ip, err := decodePC92IP(record.Extensions[0]); err == nil && ip == subjectEntry.IP {
			// The decoded K IP is exposed on Subject for graph consumers, but
			// remains in its original extension slot when encoding. Duplicating
			// it into the subject could push a valid maximum-sized frame over.
			subjectEntry.IP = netip.Addr{}
		}
	}
	subject, err := EncodePC92Entry(subjectEntry)
	if err != nil {
		return "", err
	}
	if record.SubjectImplicit && (record.Action == "A" || record.Action == "D") {
		subject = ""
	}
	fields := []string{record.Origin, record.Timestamp, record.Action, subject}
	if record.Action == "K" {
		fields = append(fields, strconv.Itoa(record.NodeCount), strconv.Itoa(record.UserCount))
		fields = append(fields, record.Extensions...)
	} else {
		for _, entry := range record.Members {
			encoded, err := EncodePC92Entry(entry)
			if err != nil {
				return "", err
			}
			fields = append(fields, encoded)
		}
	}
	frame := &Frame{Type: "PC92", Fields: fields, Hop: record.Hop}
	if _, err := DecodePC92(frame); err != nil {
		return "", err
	}
	return "PC92^" + strings.Join(fields, "^") + fmt.Sprintf("^H%d^", record.Hop), nil
}
