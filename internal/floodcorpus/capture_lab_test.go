package floodcorpus

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This opt-in must be required by the approving driver: no dependency means a
// visible SKIP, not capture evidence. Only owned loopback endpoints are used.
func TestRecorderLoopbackCaptureToComparison(t *testing.T) {
	recorder := os.Getenv("GOCLUSTER_FLOOD_RECORDER_PATH")
	if recorder == "" {
		t.Skip("requires hash-pinned GOCLUSTER_FLOOD_RECORDER_PATH; skip is not capture evidence")
	}
	raw, err := os.ReadFile(recorder)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != RecorderSHA256 {
		t.Fatal("recorder dependency changed")
	}
	python, err := exec.LookPath("python")
	if err != nil {
		t.Fatal("Python prerequisite missing")
	}
	dir := t.TempDir()
	if root := os.Getenv("GOCLUSTER_FLOOD_LAB_DIR"); root != "" {
		if !localPath(root) {
			t.Fatal("local lab directory required")
		}
		dir, err = os.MkdirTemp(root, "capture-lab-")
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var listenConfig net.ListenConfig
	backend, err := listenConfig.Listen(ctx, "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	port := backend.Addr().(*net.TCPAddr).Port
	input := filepath.Join(dir, "capture.jsonl")
	cmd := exec.CommandContext(ctx, python, recorder, "record", "--listen-port", "0", "--upstream-ip", "127.0.0.1", "--upstream-port", strconv.Itoa(port), "--output", input, "--max-seconds", "10", "--max-bytes", "1048576")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal("cannot launch recorder")
	}
	waited := false
	defer func() {
		if !waited {
			cancel()
			_ = cmd.Wait()
		}
	}()
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	var line string
	select {
	case line = <-ready:
	case <-ctx.Done():
		t.Fatal("recorder readiness timeout")
	}
	match := regexp.MustCompile(`^Listening \('127\.0\.0\.1', ([0-9]+)\)$`).FindStringSubmatch(line)
	if len(match) != 2 {
		t.Fatal("recorder did not publish loopback readiness")
	}
	first := testWire("PC11", "K1AAA", "  CQ\t~literal\xfe  ")
	payload := []byte("\xff\xfb\x01" + first + first + testWire("PC61", "K1BBB", "CQ") + testWire("PC26", "K1CCC", "CQ"))
	backendDone := make(chan error, 1)
	go func() {
		conn, err := backend.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			backendDone <- err
			return
		}
		received, err := io.ReadAll(io.LimitReader(conn, 1024))
		if err != nil || !bytes.Equal(received, []byte("LAB_ONLY\r\n")) {
			backendDone <- errors.New("unexpected client bytes or proxy-generated negotiation")
			return
		}
		// Split Telnet control, original fields and terminal markers. Native
		// framing must recover them without a second implementation grammar.
		for start := 0; start < len(payload); {
			end := min(start+7, len(payload))
			n, err := conn.Write(payload[start:end])
			if err != nil {
				backendDone <- err
				return
			}
			if n == 0 {
				backendDone <- io.ErrShortWrite
				return
			}
			start += n
		}
		backendDone <- conn.(*net.TCPConn).CloseWrite()
	}()
	dialer := net.Dialer{Timeout: 3 * time.Second}
	client, err := dialer.DialContext(ctx, "tcp4", "127.0.0.1:"+match[1])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(12 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(client, "LAB_ONLY\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(io.LimitReader(client, 1<<20))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("actual receiver bytes differ")
	}
	if err := <-backendDone; err != nil {
		t.Fatal("loopback backend failed")
	}
	err = cmd.Wait()
	waited = true
	if err != nil {
		t.Fatal("recorder did not finish cleanly")
	}
	info, err := ScanCapture(input)
	if err != nil || !info.Complete || info.Sent[1] != int64(len(payload)) {
		t.Fatalf("capture counters incomplete: %+v %v", info, err)
	}
	corpus := filepath.Join(dir, "corpus")
	manifest, err := Convert(input, corpus, "s2c", "lab-peer")
	if err != nil || !manifest.Complete || manifest.Events != 4 {
		t.Fatalf("lab import: %+v %v", manifest, err)
	}
	r, err := Compare(corpus, Options{Threshold: 2, WindowSeconds: 10, ActorCap: 4})
	if err != nil || r.Arrivals != 4 || r.RollingAllowed != 2 || r.ReplenishingAllowed != 2 || r.RecoveredLogicalPosts != nil {
		t.Fatalf("lab comparison: %+v %v", r, err)
	}
	var sentences []string
	if err := readEvents(corpus, manifest, func(e Event) error { sentences = append(sentences, e.SentenceBase64); return nil }); err != nil {
		t.Fatal(err)
	}
	if sentences[0] != sentences[1] {
		t.Fatal("known lab byte copies changed or were coalesced")
	}
	decoded, decodeErr := base64.StdEncoding.DecodeString(sentences[0])
	if decodeErr != nil || !bytes.Equal(decoded, []byte(first[:len(first)-3])) {
		t.Fatal("actual captured original comment/byte payload changed")
	}
	// Persisted raw/event files remain private; this transcript contains only
	// fixed status and counts, never payload, endpoint, comment or credential.
	t.Logf("hash-pinned loopback capture: events=%d received=%d sent=%d; original byte copies retained, harm unknown", manifest.Events, info.Received[1], info.Sent[1])
	if strings.Contains(stderr.String(), "LAB_ONLY") {
		t.Fatal("payload appeared in recorder diagnostic")
	}
}
