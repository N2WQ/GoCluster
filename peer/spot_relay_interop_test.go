package peer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

type spotReferenceResult struct {
	State          string     `json:"state"`
	PC9x           bool       `json:"pc9x"`
	SpotsBase64    [][]string `json:"spots_base64"`
	DiskBase64     string     `json:"disk_base64"`
	ReceivedBase64 []string   `json:"received_base64"`
	TotalSpots     int        `json:"total_spots"`
	ReceivedBytes  int        `json:"received_bytes"`
	ReceivedLines  int        `json:"received_lines"`
}

type spotReference struct {
	t      *testing.T
	input  *json.Encoder
	output *json.Decoder
}

func startSpotReference(t *testing.T, pc9x bool, at time.Time) *spotReference {
	t.Helper()
	root, perl := os.Getenv("DXSPIDER_ROOT"), os.Getenv("DXSPIDER_PERL")
	if root == "" || perl == "" {
		t.Skip("spot receiver observations require explicit DXSPIDER_ROOT and DXSPIDER_PERL; this skip is not interoperability evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	commit, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(commit)) != dxSpiderReferenceCommit {
		cancel()
		t.Fatalf("reference checkout must be %s: %s %v", dxSpiderReferenceCommit, commit, err)
	}
	if err := exec.CommandContext(ctx, "git", "-C", root, "diff", "--quiet", dxSpiderReferenceCommit, "--", "perl", "data/prefix_data.pl").Run(); err != nil {
		cancel()
		t.Fatalf("reference receiver source or prefix data differs from its pin: %v", err)
	}
	repo, err := filepath.Abs("..")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	args := []string{}
	if lib := os.Getenv("DXSPIDER_PERL_LIB"); lib != "" {
		args = append(args, "-I", lib)
	}
	args = append(args, filepath.Join(repo, "scripts", "spot-dxspider-interop.pl"))
	command := exec.CommandContext(ctx, perl, args...)
	// Spot::init creates a relative directory before constructing its log. Keep
	// every receiver write, including that directory, outside the checkout.
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(), "DXSPIDER_TEST_STATE="+command.Dir, "GOCLUSTER_ROOT="+repo, "LC_ALL=C")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	input, err := command.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = input.Close()
		err := command.Wait()
		cancel()
		if err != nil {
			t.Errorf("actual DXSpider spot receiver failed: %v\n%s", err, stderr.String())
		}
	})
	reference := &spotReference{t: t, input: json.NewEncoder(input), output: json.NewDecoder(output)}
	reference.step(map[string]any{"command": "init", "pc9x": pc9x, "at": at.Unix()})
	marker := ""
	if pc9x {
		marker = " pc9x"
	}
	reference.frame([]byte("PC18^GoCluster Version: 261004"+marker+"^5457^\r\n"), at)
	if pc9x {
		reference.frame([]byte(fmt.Sprintf("PC92^N0CALL^%d^C^5N0CALL:5457:633^H99^\r\n", utcSecond(at))), at)
	}
	result := reference.frame([]byte("PC22^\r\n"), at)
	if result.State != "normal" || result.PC9x != pc9x {
		t.Fatalf("actual receiver did not finish the requested handshake: %+v", result)
	}
	return reference
}

func (r *spotReference) step(request map[string]any) spotReferenceResult {
	r.t.Helper()
	if err := r.input.Encode(request); err != nil {
		r.t.Fatal(err)
	}
	var result spotReferenceResult
	if err := r.output.Decode(&result); err != nil {
		r.t.Fatalf("actual spot receiver response: %v", err)
	}
	return result
}

func (r *spotReference) frame(wire []byte, at time.Time) spotReferenceResult {
	return r.step(map[string]any{"command": "frame", "wire_base64": base64.StdEncoding.EncodeToString(wire), "at": at.Unix(), "chunk": 17})
}

func decodeSpotReferenceBytes(t *testing.T, encoded string) string {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return string(decoded)
}

// Capture the actual production HandleFrame -> data queue -> writerLoop ->
// writeLine path. The returned bytes include native CRLF and arbitrary permitted
// bytes, avoiding both a formatter oracle and a JSON Unicode conversion.
func captureSpotRelayWriter(t *testing.T, incoming string, pc9x bool) ([]byte, *spot.Spot) {
	t.Helper()
	source := &session{id: "src", remoteCall: "N1SRC", ctx: context.Background(), writeCh: make(chan string, 1)}
	destination, remote := newTransportTestSession(t)
	destination.id, destination.pc9x = "dst", pc9x
	localQueue := make(chan *spot.Spot, 1)
	manager := &Manager{
		cfg: config.PeeringConfig{ForwardSpots: true}, ingest: localQueue, dedupe: newBoundedDedupe(time.Minute, 16, 4096),
		sessions: sessionTestIndex(map[string]*session{"src": source, "dst": destination}),
	}
	frame := readNativePeerSpotFrame(t, incoming)
	manager.HandleFrame(frame, source)
	var local *spot.Spot
	select {
	case local = <-localQueue:
	default:
		t.Fatal("valid original did not enter local processing")
	}
	if len(source.writeCh) != 0 {
		t.Fatal("relay returned to its source")
	}
	wire, _ := readNativePeerSpotWriter(t, destination, remote)
	return []byte(wire), local
}

type spotInteropCase struct {
	name          string
	incoming      string
	wantWire      string
	dx            string
	de            string
	storedComment string
	storedIP      string
}

func spotInteropCases(kind string, modern bool, at time.Time) []spotInteropCase {
	comments := []struct{ name, original, stored string }{
		{"parsed comment", "  FT8 -10 dB\tCQ TEST  ", "FT8 -10 dB\tCQ TEST"},
		{"tab-only comment", "\t", ""},
		{"space-only comment", "   ", ""},
		{"non-UTF8 permitted byte", "A\xfeB", "A\xfeB"},
		{"internal tilde", "CQ~TEST", "CQ~TEST"},
		{"leading tilde", "~CQ", "~CQ"},
		{"trailing tilde", "CQ~", "CQ~"},
		{"only tilde", "~", "~"},
		{"repeated and header-like tildes", "CQ~~PC61~TEST~~", "CQ~~PC61~TEST~~"},
	}
	var cases []spotInteropCase
	for i, comment := range comments {
		dx, de := fmt.Sprintf("K1AA%c-01", 'A'+i), fmt.Sprintf("W1XA%c-01", 'A'+i)
		payload := fmt.Sprintf("014074.1234^%s^%s^%s^%s^%s^W0NODE", dx, at.Format("02-Jan-2006"), at.Format("1504Z"), comment.original, de)
		outKind, outPayload, storedIP := kind, payload, ""
		if kind == "PC61" {
			ip := fmt.Sprintf("203.0.113.%d", i+7)
			payload += "^" + ip
			if modern {
				outPayload += "^" + ip
				storedIP = ip
			} else {
				outKind = "PC11"
			}
		}
		cases = append(cases, spotInteropCase{
			name: comment.name, incoming: kind + "^" + payload + "^H3^~",
			wantWire: outKind + "^" + outPayload + "^H2^~\r\n", dx: dx,
			de: strings.TrimSuffix(de, "-01"), storedComment: comment.stored, storedIP: storedIP,
		})
	}
	return cases
}

func TestPeerSpotProductionWriterPreservesOriginalBytes(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	for _, kind := range []string{"PC11", "PC61"} {
		for _, modern := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/modern=%t", kind, modern), func(t *testing.T) {
				for _, tc := range spotInteropCases(kind, modern, at) {
					t.Run(tc.name, func(t *testing.T) {
						wire, local := captureSpotRelayWriter(t, tc.incoming, modern)
						if string(wire) != tc.wantWire {
							t.Fatalf("production writer bytes=%q, want original bytes=%q", wire, tc.wantWire)
						}
						if local.Frequency != 14074.12 || local.DXCall != strings.TrimSuffix(tc.dx, "-01") || local.DECall != tc.de+"-01" {
							t.Fatalf("fixture did not exercise current local normalization: freq=%v DX=%s DE=%s", local.Frequency, local.DXCall, local.DECall)
						}
					})
				}
			})
		}
	}
}

func TestPeerSpotNativeEscapedIACRejected(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, forward := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/forward=%t", kind, forward), func(t *testing.T) {
				payload := fmt.Sprintf("14074.1234^K1ABC^%s^%s^A\xffB^W1XYZ^W0NODE", at.Format("02-Jan-2006"), at.Format("1504Z"))
				if kind == "PC61" {
					payload += "^203.0.113.7"
				}
				decoded := kind + "^" + payload + "^H3^~"
				local, remote := net.Pipe()
				reader := newLineReader(local, MaxPeerFrameBytes, MaxPeerFrameBytes, nil)
				t.Cleanup(reader.release)
				done := make(chan error, 1)
				joined := make(chan struct{})
				t.Cleanup(func() { _ = local.Close(); _ = remote.Close(); <-joined })
				go func() {
					defer close(joined)
					_, err := io.WriteString(remote, strings.ReplaceAll(decoded, "\xff", "\xff\xff")+"\r\n")
					done <- err
				}()
				line, err := reader.ReadLine(time.Now().Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(line, "A\xffB") {
					t.Fatalf("native transport did not decode the escaped literal IAC: %q", line)
				}
				frame, err := ParseFrame(line)
				if err != nil {
					t.Fatal(err)
				}
				source := &session{id: "src", remoteCall: "N1SRC", ctx: context.Background(), writeCh: make(chan string, 1)}
				destination := &session{id: "dst", remoteCall: "N1DST", pc9x: true, ctx: context.Background(), writeCh: make(chan string, 1)}
				ingest := make(chan *spot.Spot, 1)
				manager := &Manager{
					cfg: config.PeeringConfig{ForwardSpots: forward}, ingest: ingest, dedupe: newBoundedDedupe(time.Minute, 16, 4096),
					sessions: sessionTestIndex(map[string]*session{"src": source, "dst": destination}),
				}
				manager.HandleFrame(frame, source)
				entries, keyBytes, refused := manager.dedupe.occupancy()
				if len(ingest) != 0 || len(source.writeCh) != 0 || len(destination.writeCh) != 0 || entries != 0 || keyBytes != 0 || refused != 0 {
					t.Fatal("decoded literal IAC entered ingestion, peer dedupe or relay")
				}
			})
		}
	}
}

func TestDXSpiderReferencePeerSpotStorage(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	for _, kind := range []string{"PC11", "PC61"} {
		for _, modern := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/modern=%t", kind, modern), func(t *testing.T) {
				reference := startSpotReference(t, modern, at)
				for i, tc := range spotInteropCases(kind, modern, at) {
					t.Run(tc.name, func(t *testing.T) {
						wire, _ := captureSpotRelayWriter(t, tc.incoming, modern)
						if string(wire) != tc.wantWire {
							t.Fatalf("writer fidelity failed before receiver observation: %q != %q", wire, tc.wantWire)
						}
						before := reference.step(map[string]any{"command": "snapshot", "at": at.Unix()})
						result := reference.frame(wire, at)
						if result.ReceivedBytes-before.ReceivedBytes != len(wire) || result.ReceivedLines-before.ReceivedLines != 1 {
							t.Fatalf("receiver framing counters did not consume exactly one writer record: before=%+v after=%+v", before, result)
						}
						if got := decodeSpotReferenceBytes(t, result.ReceivedBase64[len(result.ReceivedBase64)-1]); got != strings.TrimSuffix(tc.wantWire, "\r\n") {
							t.Fatalf("actual receiver transport changed sentence bytes: %q", got)
						}
						if strings.HasPrefix(tc.wantWire, "PC11^") && result.TotalSpots != i {
							t.Fatal("PC11 was stored before its normal delayed-processing tick")
						}
						result = reference.step(map[string]any{"command": "tick", "at": at.Add(3 * time.Second).Unix()})
						assertReferenceStoredSpot(t, result, tc, at, i+1)
					})
				}
			})
		}
	}
}

func assertReferenceStoredSpot(t *testing.T, result spotReferenceResult, tc spotInteropCase, at time.Time, count int) {
	t.Helper()
	if result.TotalSpots != count || len(result.SpotsBase64) != count {
		t.Fatalf("actual Spot::add_local did not store the expected spot count: %+v", result)
	}
	var stored []string
	for _, encoded := range result.SpotsBase64 {
		row := make([]string, len(encoded))
		for i, field := range encoded {
			row[i] = decodeSpotReferenceBytes(t, field)
		}
		if len(row) >= 15 && row[1] == tc.dx {
			stored = row
		}
	}
	if stored == nil {
		t.Fatalf("receiver spot cache lacks %s", tc.dx)
	}
	// Expected receiver transformations are explicit and independent of the Go
	// local Spot. Receiver normalization is not a relay-byte fidelity claim.
	for _, field := range []struct {
		index int
		want  string
	}{
		{0, "14074.1"}, {1, tc.dx}, {2, strconv.FormatInt(at.Unix(), 10)},
		{3, tc.storedComment}, {4, tc.de}, {7, "W0NODE"}, {14, tc.storedIP},
	} {
		if stored[field.index] != field.want {
			t.Fatalf("receiver stored field %d=%q want %q (row=%q)", field.index, stored[field.index], field.want, stored)
		}
	}
	disk := decodeSpotReferenceBytes(t, result.DiskBase64)
	if !strings.Contains(disk, strings.Join(stored, "^")+"\n") && !strings.Contains(disk, strings.Join(stored, "^")+"\r\n") {
		t.Fatalf("actual receiver spot log lacks the cache record: disk=%q row=%q", disk, stored)
	}
}

func TestDXSpiderReferenceBroaderOriginalSpotterRejected(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	for _, kind := range []string{"PC11", "PC61"} {
		for _, de := range []string{"W1XYZ-#", "W1XYZ-123"} {
			t.Run(kind+"/"+de, func(t *testing.T) {
				reference := startSpotReference(t, true, at)
				payload := fmt.Sprintf("14074.1234^K1ABC^%s^%s^CQ TEST^%s^W0NODE", at.Format("02-Jan-2006"), at.Format("1504Z"), de)
				if kind == "PC61" {
					payload += "^203.0.113.7"
				}
				wire, local := captureSpotRelayWriter(t, kind+"^"+payload+"^H3^~", true)
				want := kind + "^" + payload + "^H2^~\r\n"
				if string(wire) != want || local.DECall != strings.TrimSuffix(de, "-#") {
					t.Fatalf("GoCluster did not admit and preserve its broader original syntax: local=%s wire=%q", local.DECall, wire)
				}
				reference.frame(wire, at)
				result := reference.step(map[string]any{"command": "tick", "at": at.Add(3 * time.Second).Unix()})
				if result.TotalSpots != 0 || len(result.SpotsBase64) != 0 || result.DiskBase64 != "" {
					t.Fatalf("pinned receiver unexpectedly accepted its unsupported spotter syntax: %+v", result)
				}
				t.Logf("GoCluster admitted and preserved %s; unmodified pinned DXSpider stored no spot", de)
			})
		}
	}
}

func TestDXSpiderReferenceMappedOriginalIP(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute)
	payload := fmt.Sprintf("014074.1234^K1ABC-01^%s^%s^CQ TEST^W1XYZ-01^W0NODE", at.Format("02-Jan-2006"), at.Format("1504Z"))
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprintf("modern=%t", modern), func(t *testing.T) {
			reference := startSpotReference(t, modern, at)
			wire, local := captureSpotRelayWriter(t, "PC61^"+payload+"^::ffff:203.0.113.7^H3^~", modern)
			want := "PC11^" + payload + "^H2^~\r\n"
			if modern {
				want = "PC61^" + payload + "^::ffff:203.0.113.7^H2^~\r\n"
			}
			if string(wire) != want || local.SpotterIP != "::ffff:203.0.113.7" {
				t.Fatalf("GoCluster did not preserve its accepted original mapped IP: local=%s wire=%q", local.SpotterIP, wire)
			}
			reference.frame(wire, at)
			result := reference.step(map[string]any{"command": "tick", "at": at.Add(3 * time.Second).Unix()})
			if modern {
				if result.TotalSpots != 0 || len(result.SpotsBase64) != 0 || result.DiskBase64 != "" {
					t.Fatalf("pinned receiver unexpectedly stored unsupported mapped IP text: %+v", result)
				}
			} else {
				assertReferenceStoredSpot(t, result, spotInteropCase{dx: "K1ABC-01", de: "W1XYZ", storedComment: "CQ TEST"}, at, 1)
			}
			t.Logf("GoCluster admitted the mapped original; receiver modern=%t stored %d spots after authorized conversion", modern, result.TotalSpots)
		})
	}
}
