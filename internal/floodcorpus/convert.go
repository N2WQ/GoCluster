package floodcorpus

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/peer"
	"dxcluster/spot"
)

type nativeDecoder struct {
	manager *peer.Manager
	queue   *peer.SpotIngress
}

func newNativeDecoder() (*nativeDecoder, error) {
	q := peer.NewSpotIngress()
	cfg := config.PeeringConfig{MaxPeers: 1, LocalCallsign: "N0CALL-1", NodeVersion: "5457", LegacyVersion: "5401", PC92Bitmap: 5, MaxLineLength: peer.MaxPeerFrameBytes, PC92MaxBytes: peer.MaxPeerFrameBytes}
	m, err := peer.NewManager(cfg, "N0CALL-1", q, 0, nil)
	if err != nil {
		q.Close()
		return nil, errors.New("cannot construct offline native decoder")
	}
	return &nativeDecoder{m, q}, nil
}

func (d *nativeDecoder) decode(line string) (*spot.Spot, bool) {
	f, err := peer.ParseFrame(line)
	if err != nil {
		return nil, false
	}
	if f.Type != "PC11" && f.Type != "PC61" && f.Type != "PC26" {
		return nil, true
	}
	d.manager.HandleFrame(f, nil)
	j := d.queue.Take()
	if j == nil {
		return nil, false
	}
	s := j.TakeSpot()
	j.Finish()
	return s, false
}

func localPath(path string) bool {
	return !strings.HasPrefix(path, `\\`) && !strings.HasPrefix(path, "//")
}

// Convert imports one trusted PEER connection. The dependency's observed clock
// is not an original posting clock, a controller clock or an exact merged clock.
func Convert(input, output, direction, transport string) (Manifest, error) {
	return convert(input, output, direction, transport, nil)
}

func convert(input, output, direction, transport string, betweenPasses func()) (Manifest, error) {
	var manifest Manifest
	i := directionIndex(direction)
	if i < 0 || !identifier(transport, 64) || !localPath(input) || !localPath(output) {
		return manifest, errors.New("invalid local import options")
	}
	info, err := ScanCapture(input)
	if err != nil {
		return manifest, err
	}
	if betweenPasses != nil {
		betweenPasses()
	}
	stream, err := openCaptureStream(input, i, info.Sent[i])
	if err != nil {
		return manifest, err
	}
	defer stream.Close()
	d, err := newNativeDecoder()
	if err != nil {
		return manifest, err
	}
	defer d.manager.Stop()
	if err := os.Mkdir(output, 0700); err != nil { // #nosec G703 -- Operator-selected local output must be a new directory.
		return manifest, errors.New("output directory must be new")
	}
	f, err := os.OpenFile(filepath.Join(output, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600) // #nosec G703 -- Fixed filename in the exclusively created operator directory.
	if err != nil {
		return manifest, errors.New("cannot create private event file")
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	h := sha256.New()
	writer := &boundedWriter{writer: io.MultiWriter(f, h), remaining: MaxInputBytes}
	reader := peer.NewLineReader(stream, peer.MaxPeerFrameBytes, peer.MaxPeerFrameBytes, nil)
	manifest = Manifest{Version: Schema, Stage: corpusStage, CaptureSHA256: info.SHA256, RecorderSHA256: RecorderSHA256, Direction: direction, Transport: transport, CaptureComplete: info.Complete, Received: info.Received[i], Recorded: info.Recorded[i], Sent: info.Sent[i], Unsent: info.Recorded[i] - info.Sent[i], Timing: "recorder read completion; frames in one read share time; selector/disk backpressure and independent capture alignment are uncertain", Eligibility: "original fields valid; maxAge=0; no CTY/license or live handshake eligibility reproduced", Implementation: buildIdentity()}
	manifest.CaptureStartUTC, manifest.LastNS = info.StartUTC, info.LastNS
	for {
		line, readErr := reader.ReadLine(time.Time{})
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			var overlong peer.ErrLineTooLong
			if errors.As(readErr, &overlong) {
				manifest.Rejected++
				continue
			}
			return manifest, errors.New("capture native read failed")
		}
		s, other := d.decode(line)
		if other {
			manifest.Other++
			continue
		}
		if s == nil {
			manifest.Rejected++
			continue
		}
		manifest.Events++
		if manifest.Events > MaxEvents {
			return manifest, errors.New("event count limit")
		}
		e := Event{Version: Schema, Arrival: manifest.Events, NS: stream.lastNS, Source: "PEER", Transport: transport, SentenceBase64: base64.StdEncoding.EncodeToString([]byte(line))}
		if err := writeEvent(writer, e); err != nil {
			return manifest, err
		}
	}
	if stream.bytesReturned != info.Sent[i] || stream.hashString() != info.SHA256 {
		return manifest, errors.New("capture changed between validation and import")
	}
	manifest.PendingTail = reader.PendingInput()
	manifest.Complete = info.Complete && !manifest.PendingTail
	manifest.EventsSHA256 = hex.EncodeToString(h.Sum(nil))
	if err := f.Close(); err != nil {
		return manifest, errors.New("event file close failed")
	}
	closed = true
	if err := writeExclusiveJSON(filepath.Join(output, "manifest.json"), manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, errors.New("event output limit")
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func writeEvent(w io.Writer, e Event) error {
	raw, err := json.Marshal(e)
	if err != nil || len(raw)+1 > MaxRecordBytes {
		return errors.New("event record limit")
	}
	if _, err := w.Write(append(raw, '\n')); err != nil {
		return errors.New("event write failed")
	}
	return nil
}

func writeExclusiveJSON(path string, value any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600) // #nosec G703 -- Internal callers supply a fixed filename below their new operator-selected directory; exclusive create refuses overwrite.
	if err != nil {
		return errors.New("output must not exist")
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		_, err = f.Write(append(raw, '\n'))
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.New("output write or close failed")
	}
	return nil
}

// captureStream supplies at most one journal data record per Read. It drains
// the rest of the journal even after the send-accepted prefix ends, so the
// second pass hashes all source bytes before manifest publication.
type captureStream struct {
	file                                    *os.File
	reader                                  *bufio.Reader
	hash                                    hash.Hash
	direction                               int
	remaining, bytesReturned, lastNS, total int64
	pending                                 []byte
}

func openCaptureStream(path string, direction int, sent int64) (*captureStream, error) {
	f, err := os.Open(path) // #nosec G703 -- Same operator-selected local capture already validated in the first pass.
	if err != nil {
		return nil, errors.New("cannot reopen capture")
	}
	h := sha256.New()
	return &captureStream{file: f, reader: bufio.NewReaderSize(io.TeeReader(io.LimitReader(f, MaxInputBytes+1), h), captureRecordBytes+1), hash: h, direction: direction, remaining: sent}, nil
}

func (s *captureStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(s.pending) == 0 {
		raw, err := s.reader.ReadSlice('\n')
		s.total += int64(len(raw))
		if len(raw) == 0 && errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil || len(raw) > captureRecordBytes || s.total > MaxInputBytes {
			return 0, errors.New("second pass record limit")
		}
		var rec captureRecord
		if strictJSON(raw, &rec) != nil || rec.NS == nil || *rec.NS < 0 {
			return 0, errors.New("second pass invalid record")
		}
		if rec.Kind != "data" || directionIndex(rec.Direction) != s.direction || s.remaining == 0 {
			continue
		}
		data, err := base64.StdEncoding.Strict().DecodeString(rec.Data)
		if err != nil || len(data) == 0 || len(data) > 16384 {
			return 0, errors.New("second pass invalid payload")
		}
		data = data[:min(int64(len(data)), s.remaining)]
		s.pending = data
		s.lastNS = *rec.NS
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	s.remaining -= int64(n)
	s.bytesReturned += int64(n)
	return n, nil
}

func (s *captureStream) hashString() string             { return hex.EncodeToString(s.hash.Sum(nil)) }
func (s *captureStream) Close() error                   { return s.file.Close() }
func (*captureStream) Write([]byte) (int, error)        { return 0, errors.New("offline input cannot write") }
func (*captureStream) LocalAddr() net.Addr              { return offlineAddr{} }
func (*captureStream) RemoteAddr() net.Addr             { return offlineAddr{} }
func (*captureStream) SetDeadline(time.Time) error      { return nil }
func (*captureStream) SetReadDeadline(time.Time) error  { return nil }
func (*captureStream) SetWriteDeadline(time.Time) error { return nil }

type offlineAddr struct{}

func (offlineAddr) Network() string { return "offline" }
func (offlineAddr) String() string  { return "offline" }
