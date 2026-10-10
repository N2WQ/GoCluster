//go:build !qualification

package cluster

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/spot"
	"dxcluster/telnet"

	"github.com/cockroachdb/pebble"
)

const customSCPDeliveryCall = "K1HST"

type customSCPDeliveryReady struct {
	RunID              string `json:"run_id"`
	TimeUnix           int64  `json:"time_unix"`
	ObservationPresent bool   `json:"observation_present"`
	StaticPresent      bool   `json:"static_present"`
	RecoveredSupport   int    `json:"recovered_support"`
}

// The optional producer handoff connects the forced cleanup interleaving to
// actual TCP delivery and persisted archive readback. Reported producer state
// is diagnostic; the positive oracle always requires both retained observers.
func TestCustomSCPDeliveredOutput(t *testing.T) {
	t.Chdir(t.TempDir()) // Telnet login records must remain inside this fixture.
	previousLayout := spot.CurrentDXClusterLayout()
	if err := spot.SetDXClusterLineLength(78); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := spot.SetDXClusterLineLength(previousLayout.LineLength); err != nil {
			t.Error(err)
		}
	})
	path, at, ready := customSCPDeliveryFixture(t)
	t.Run("preserved_observation_delivers", func(t *testing.T) {
		store := customSCPDeliveryOpen(t, path)
		t.Logf("producer run=%q observation_present=%v static_present=%v recovered_support=%d; reopened observations=%d qualified_support=%d",
			ready.RunID, ready.ObservationPresent, ready.StaticPresent, ready.RecoveredSupport,
			store.StatsSnapshot().ObservationSpotters, store.RecentSupportCount(customSCPDeliveryCall, "20m", "CW", at))
		customSCPDeliveryCheck(t, store, at, true)
		if got := store.RecentSupportCount(customSCPDeliveryCall, "20m", "CW", at); got != 2 {
			t.Fatalf("positive fixture must recover qualified support 2, got %d", got)
		}
	})
	t.Run("missing_observation_holds_with_same_static_membership", func(t *testing.T) {
		missingPath := filepath.Join(t.TempDir(), "scp")
		customSCPDeliverySeed(t, missingPath, at, 1)
		store := customSCPDeliveryOpen(t, missingPath)
		if got := store.StatsSnapshot().ObservationSpotters; got != 1 {
			t.Fatalf("negative fixture must retain one observer, got %d", got)
		}
		if store.HasRecentSupport(customSCPDeliveryCall, "20m", "CW", 2, at) {
			t.Fatal("one observer must not satisfy the two-observer support gate")
		}
		customSCPDeliveryCheck(t, store, at, false)
	})
}

func customSCPDeliveryFixture(t *testing.T) (string, time.Time, customSCPDeliveryReady) {
	t.Helper()
	root, runID := os.Getenv("GOCLUSTER_CUSTOMSCP_FIXTURE"), os.Getenv("GOCLUSTER_CUSTOMSCP_RUN_ID")
	if root == "" && runID == "" {
		at := time.Now().UTC().Truncate(time.Second)
		path := filepath.Join(t.TempDir(), "scp")
		customSCPDeliverySeed(t, path, at, 2)
		return path, at, customSCPDeliveryReady{TimeUnix: at.Unix(), ObservationPresent: true, StaticPresent: true, RecoveredSupport: 2}
	}
	if root == "" || strings.TrimSpace(runID) == "" || !filepath.IsAbs(root) {
		t.Fatal("producer handoff requires an absolute GOCLUSTER_CUSTOMSCP_FIXTURE and nonempty GOCLUSTER_CUSTOMSCP_RUN_ID")
	}
	root = filepath.Clean(root)
	for _, path := range []string{root, filepath.Join(root, "scp")} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("producer handoff requires an existing nonsymlink directory %q: %v", path, err)
		}
	}
	readyPath := filepath.Join(root, "ready.json")
	info, err := os.Lstat(readyPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
		t.Fatalf("producer readiness must be an existing regular file of at most 4096 bytes: %v", err)
	}
	f, err := os.Open(readyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 4097))
	decoder.DisallowUnknownFields()
	var ready customSCPDeliveryReady
	if err := decoder.Decode(&ready); err != nil {
		t.Fatalf("decode producer readiness: %v", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("producer readiness has trailing data: %v", err)
	}
	if ready.RunID != runID {
		t.Fatalf("producer readiness run_id=%q does not match requested run_id=%q", ready.RunID, runID)
	}
	at := time.Unix(ready.TimeUnix, 0).UTC()
	if ready.TimeUnix <= 0 || time.Since(at) > 10*time.Minute || at.After(time.Now().UTC().Add(10*time.Second)) {
		t.Fatalf("producer readiness timestamp is not fresh: %s", at)
	}
	return filepath.Join(root, "scp"), at, ready
}

// Seed the established persistence encoding directly so these controls do not
// depend on global H3 mapping initialization or private production test hooks.
func customSCPDeliverySeed(t *testing.T, path string, at time.Time, observers int) {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	var meta [8]byte
	binary.BigEndian.PutUint64(meta[:], uint64(at.Unix()))
	if err := db.Set([]byte("m|"+customSCPDeliveryCall), meta[:], pebble.Sync); err != nil {
		t.Fatal(err)
	}
	for i, de := range []string{"N0AAA", "N0BBB"}[:observers] {
		var value [10]byte
		copy(value[:8], meta[:])
		binary.BigEndian.PutUint16(value[8:], uint16(101*(i+1)))
		if err := db.Set([]byte("o|"+customSCPDeliveryCall+"|20m|cw|"+de), value[:], pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
}

func customSCPDeliveryOpen(t *testing.T, path string) *spot.CustomSCPStore {
	t.Helper()
	store, err := spot.OpenCustomSCPStore(spot.CustomSCPOptions{
		Path: path, HorizonDays: 1, StaticHorizonDays: 1, CoreMinScore: 1, CoreMinH3Cells: 2,
	})
	if err != nil {
		t.Fatalf("reopen producer state: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if !store.StaticContains(customSCPDeliveryCall) {
		t.Fatal("both delivery cases must retain the same static membership")
	}
	return store
}

func customSCPDeliveryCheck(t *testing.T, store *spot.CustomSCPStore, at time.Time, deliver bool) {
	t.Helper()
	ring := buffer.NewRingBuffer(4)
	writer, err := archive.NewWriter(config.ArchiveConfig{
		DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	writer.Start()
	t.Cleanup(writer.Stop)
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{
		Port: port, LoginPrompt: "login: ", LoginGreeting: "CUSTOMSCP_READY\n",
		MaxConnections: 2, BroadcastWorkers: 1, BroadcastQueue: 4, WorkerQueue: 4, ClientBuffer: 8,
		BroadcastBatchIntervalSet: true,
	}, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	conn, reader := runtimeQualificationLogin(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), "", "DL1ABC")
	customSCPDeliveryAwaitLogin(t, srv, conn, reader)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.recentBandStore, pipeline.customSCPStore = store, store
	pipeline.correctionCfg.CustomSCP.Enabled = true
	pipeline.correctionCfg.RecentBandRecordMinUniqueSpotters = 2
	pipeline.stabilizerEnabled = true
	pipeline.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 4)
	t.Cleanup(pipeline.telnetStabilizer.Stop)
	s := newDeliveryTestSpot(customSCPDeliveryCall, "W2TT", at)
	ctx := outputSpotContext{spot: s, modeUpper: "CW"}
	// Both cases earn the same static floor through the production stage; only
	// preserved recent observations may allow final fanout afterward.
	if !pipeline.applyPostResolverAdjustments(&ctx) || s.Confidence != "S" {
		t.Fatalf("same static membership must promote both cases from ? to S, got %q", s.Confidence)
	}
	pipeline.deliverSpot(&ctx)
	if !deliver {
		customSCPDeliveryNoDX(t, conn, reader)
		rows, err := writer.Recent(2)
		if err != nil {
			t.Fatal(err)
		}
		if pipeline.telnetStabilizer.Pending() != 1 || ring.GetCount() != 0 || len(rows) != 0 {
			t.Fatalf("missing observation must hold before all fanout: pending=%d ring=%d archive=%d",
				pipeline.telnetStabilizer.Pending(), ring.GetCount(), len(rows))
		}
		return
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	lineBytes, tcpErr := reader.ReadSlice('\n')
	if tcpErr != nil {
		rows, archiveErr := writer.Recent(2)
		t.Fatalf("expected actual TCP delivery: read_err=%v partial=%q pending=%d ring=%d persisted_archive_rows=%d archive_read_err=%v",
			tcpErr, lineBytes, pipeline.telnetStabilizer.Pending(), ring.GetCount(), len(rows), archiveErr)
	}
	line := string(lineBytes)
	if want := customSCPDeliveryWire(at); line != want {
		t.Fatalf("delivered TCP line mismatch:\n got %q\nwant %q", line, want)
	}
	t.Logf("received exact TCP line %q", line)
	if pipeline.telnetStabilizer.Pending() != 0 || ring.GetCount() != 1 {
		t.Fatalf("preserved observation must deliver once: pending=%d ring=%d", pipeline.telnetStabilizer.Pending(), ring.GetCount())
	}
	customSCPDeliveryAssertSpot(t, "ring", ring.GetRecent(1)[0], at)
	deadline := time.Now().Add(3 * time.Second)
	for {
		rows, err := writer.Recent(2)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 0 {
			if len(rows) != 1 {
				t.Fatalf("expected exactly one persisted archive row, got %d", len(rows))
			}
			customSCPDeliveryAssertSpot(t, "persisted archive", rows[0], at)
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("delivered TCP spot did not reach persisted archive")
		}
		time.Sleep(time.Millisecond)
	}
}

func customSCPDeliveryAwaitLogin(t *testing.T, srv *telnet.Server, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for len(srv.ListClientCallsigns()) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("CustomSCP recipient did not register")
		}
		time.Sleep(time.Millisecond)
	}
	if calls := srv.ListClientCallsigns(); calls[0] != "DL1ABC" {
		t.Fatalf("unexpected registered recipient: %v", calls)
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if strings.TrimSpace(customSCPDeliveryReadLine(t, reader)) == "CUSTOMSCP_READY" {
			return
		}
	}
	t.Fatal("CustomSCP login greeting exceeded 16 lines")
}

func customSCPDeliveryReadLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	line, err := reader.ReadSlice('\n')
	if err != nil {
		t.Fatalf("read actual CustomSCP TCP output: %v (partial %q)", err, line)
	}
	return string(line)
}

func customSCPDeliveryNoDX(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadSlice('\n')
	var timeout net.Error
	if len(line) != 0 || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("held spot must produce no TCP output, got %q err=%v", line, err)
	}
}

func customSCPDeliveryAssertSpot(t *testing.T, source string, s *spot.Spot, at time.Time) {
	t.Helper()
	if s.DXCallNorm != customSCPDeliveryCall || s.DECallNorm != "W2TT" || s.Frequency != 14020 ||
		s.ModeNorm != "CW" || s.Confidence != "S" || !s.Time.Equal(at) {
		t.Fatalf("%s lost delivered fields: DX=%q DE=%q frequency=%v mode=%q confidence=%q time=%s",
			source, s.DXCallNorm, s.DECallNorm, s.Frequency, s.ModeNorm, s.Confidence, s.Time)
	}
}

// This byte-position oracle comes from the 78-column wire contract, independently
// of Spot.FormatDXCluster and the delivered object's cached formatting.
func customSCPDeliveryWire(at time.Time) string {
	line := []byte(strings.Repeat(" ", 78))
	copy(line, "DX de W2TT:      14020.00")
	copy(line[27:], customSCPDeliveryCall)
	copy(line[39:], "CW")
	line[71] = 'S'
	copy(line[73:], at.UTC().Format("1504Z"))
	return fmt.Sprintf("%s\r\n", line)
}
