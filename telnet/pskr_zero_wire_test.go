package telnet

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

const pskrZeroBroadcastSchema = "gocluster.pskr-zero.broadcast.v1"

// This test-only handoff preserves the producer's actual public Spot fields.
// Pointer fields distinguish a mandatory false/zero from absent or null JSON.
// The controller establishes producer RUN/PASS and freezes the expected hashes;
// this consumer proves sequential delivery, not a continuous MQTT-to-wire run.
type pskrZeroWireEnvelope struct {
	Schema               string               `json:"schema"`
	RunID                string               `json:"run_id"`
	ProducerCase         string               `json:"producer_case"`
	BuildInputsSHA256    string               `json:"build_inputs_sha256"`
	ConfigSHA256         string               `json:"config_sha256"`
	CorpusSHA256         string               `json:"corpus_sha256"`
	ProducerBinarySHA256 string               `json:"producer_binary_sha256"`
	ObservationUnix      int64                `json:"observation_unix"`
	Records              []pskrZeroWireRecord `json:"records"`
}

type pskrZeroWireRecord struct {
	CaseID    string     `json:"case_id"`
	Spot      *spot.Spot `json:"spot"`
	AllowFast *bool      `json:"allow_fast"`
	AllowMed  *bool      `json:"allow_med"`
	AllowSlow *bool      `json:"allow_slow"`
	HasReport *bool      `json:"has_report"`
	Report    *int       `json:"report"`
}

type pskrZeroWireCase struct {
	id, dx, de, mode, dxGrid, deGrid string
	frequency                        float64
	hasReport                        bool
	report                           int
	source                           spot.SourceType
}

func pskrZeroWireCases() [5]pskrZeroWireCase {
	return [5]pskrZeroWireCase{
		{"ft8_zero_fast", "K1ZFA", "N0CALL", "FT8", "FN20", "EM12", 14074, true, 0, spot.SourcePSKReporter},
		{"ft8_zero_compat", "K1ZFB", "N0CALL", "FT8", "JN58", "IO91", 14074, true, 0, spot.SourcePSKReporter},
		{"ft8_nonzero", "K1NZA", "N0CALL", "FT8", "KP20", "KO85", 14074, true, -7, spot.SourcePSKReporter},
		{"ft8_fence", "K1FNA", "N0CALL", "FT8", "RF64", "CN89", 14074, true, -8, spot.SourcePSKReporter},
		{"manual_barrier", "K1BAR", "N1MAN", "CW", "FN31", "DM79", 14020, false, 0, spot.SourceManual},
	}
}

type pskrZeroWireIdentity struct {
	workspace, artifact, artifactSHA, receipt, consumerSHA string
}

type pskrZeroWireRow struct {
	CaseID string `json:"case_id"`
	Line   string `json:"line"`
}

func TestPSKRZeroSplitWireReplay(t *testing.T) {
	if os.Getenv("GOCLUSTER_PSKR_ZERO_ARTIFACT") == "" {
		t.Skip("split wire replay requires the approved stable-path S5 controller")
	}
	identity := requirePSKRZeroWireIdentity(t)
	envelope := loadPSKRZeroWireEnvelope(t, identity)
	stateDir := filepath.Join(filepath.Dir(identity.receipt), "wire-state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatalf("create fresh isolated wire state: %v", err)
	}
	t.Chdir(stateDir)
	previousUsers := filter.UserDataDir
	filter.UserDataDir = filepath.Join(stateDir, "users")
	t.Cleanup(func() { filter.UserDataDir = previousUsers })

	server, conn, reader, retire := startPSKRZeroWireServer(t, envelope.ObservationUnix)
	for _, record := range envelope.Records {
		if !replayPSKRZeroWireRecord(server, record) {
			t.Fatalf("actual broadcast snapshot %s was not admitted", record.CaseID)
		}
	}
	rows := collectPSKRZeroWireRows(t, conn, reader, envelope.ObservationUnix)
	queue, clients, senders := server.BroadcastMetricSnapshot()
	if queue != 0 || clients != 0 || senders != 0 {
		t.Fatalf("wire replay lost work: queue=%d client=%d sender=%d", queue, clients, senders)
	}
	// Stop does not itself join all server owners. Retirement must succeed before
	// publishing a receipt or restoring user/cwd globals, including on failure.
	retire()
	receipt := struct {
		Schema               string            `json:"schema"`
		RunID                string            `json:"run_id"`
		ProducerCase         string            `json:"producer_case"`
		ConsumerCase         string            `json:"consumer_case"`
		Boundary             string            `json:"evidence_boundary"`
		ArtifactSHA256       string            `json:"producer_artifact_sha256"`
		BuildInputsSHA256    string            `json:"build_inputs_sha256"`
		ConfigSHA256         string            `json:"config_sha256"`
		CorpusSHA256         string            `json:"corpus_sha256"`
		ProducerBinarySHA256 string            `json:"producer_binary_sha256"`
		ConsumerBinarySHA256 string            `json:"consumer_binary_sha256"`
		Listener             string            `json:"listener"`
		OwnersRetired        bool              `json:"owners_retired"`
		Rows                 []pskrZeroWireRow `json:"rows"`
	}{
		Schema:               "gocluster.pskr-zero.wire.v1",
		RunID:                envelope.RunID,
		ProducerCase:         envelope.ProducerCase,
		ConsumerCase:         t.Name(),
		Boundary:             "sequential actual producer broadcast snapshots and masks to telnet wire",
		ArtifactSHA256:       identity.artifactSHA,
		BuildInputsSHA256:    envelope.BuildInputsSHA256,
		ConfigSHA256:         envelope.ConfigSHA256,
		CorpusSHA256:         envelope.CorpusSHA256,
		ProducerBinarySHA256: envelope.ProducerBinarySHA256,
		ConsumerBinarySHA256: identity.consumerSHA,
		Listener:             server.listener.Addr().String(),
		OwnersRetired:        true,
		Rows:                 rows,
	}
	writePSKRZeroWireReceipt(t, identity.receipt, receipt)
}

func requirePSKRZeroWireIdentity(t *testing.T) pskrZeroWireIdentity {
	t.Helper()
	workspace := os.Getenv("GOCLUSTER_PSKR_ZERO_WORKSPACE")
	executable, err := os.Executable()
	if err != nil || runtime.GOOS != "windows" || !filepath.IsAbs(workspace) ||
		!strings.EqualFold(filepath.Clean(executable), filepath.Join(workspace, ".tmp", "windows-tests", "dxcluster", "internal", "cluster", "package.test.exe")) {
		t.Fatalf("wire replay requires the approved Windows cluster executable: %q (%v)", executable, err)
	}
	base := filepath.Join(workspace, ".tmp", "path-reliability-first-wave", "s5")
	artifact := requirePSKRZeroWirePath(t, base, "GOCLUSTER_PSKR_ZERO_ARTIFACT")
	receipt := requirePSKRZeroWirePath(t, base, "GOCLUSTER_PSKR_ZERO_WIRE_RECEIPT")
	if _, err := os.Stat(receipt); !os.IsNotExist(err) {
		t.Fatalf("wire receipt must not exist before replay: %q (%v)", receipt, err)
	}
	consumerSHA, err := pskrZeroWireFileSHA(executable)
	if err != nil || consumerSHA != requirePSKRZeroWireSHA(t, "GOCLUSTER_PSKR_ZERO_CONSUMER_BINARY_SHA256") {
		t.Fatalf("current consumer executable differs from controller identity: %s (%v)", consumerSHA, err)
	}
	return pskrZeroWireIdentity{workspace, artifact,
		requirePSKRZeroWireSHA(t, "GOCLUSTER_PSKR_ZERO_ARTIFACT_SHA256"), receipt, consumerSHA}
}

func requirePSKRZeroWirePath(t *testing.T, base, variable string) string {
	t.Helper()
	path := os.Getenv(variable)
	relative, err := filepath.Rel(base, path)
	if !filepath.IsAbs(path) || err != nil || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		t.Fatalf("%s must stay under %q: %q (%v)", variable, base, path, err)
	}
	return filepath.Clean(path)
}

func requirePSKRZeroWireSHA(t *testing.T, variable string) string {
	t.Helper()
	value := os.Getenv(variable)
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		t.Fatalf("%s must contain an exact SHA256 identity", variable)
	}
	return strings.ToLower(value)
}

func pskrZeroWireFileSHA(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func loadPSKRZeroWireEnvelope(t *testing.T, identity pskrZeroWireIdentity) pskrZeroWireEnvelope {
	t.Helper()
	file, err := os.Open(identity.artifact)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) > 64<<10 {
		t.Fatalf("read bounded producer handoff: %v/%v bytes=%d", readErr, closeErr, len(data))
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != identity.artifactSHA {
		t.Fatal("producer artifact does not match the successful controller receipt")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope pskrZeroWireEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode producer handoff: %v", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("producer handoff has trailing JSON: %v", err)
	}
	runID := os.Getenv("GOCLUSTER_PSKR_ZERO_RUN_ID")
	if len(runID) < 8 || envelope.RunID != runID || envelope.Schema != pskrZeroBroadcastSchema ||
		envelope.ProducerCase != "TestPSKRZeroConnectedRoutes" || envelope.ObservationUnix <= 0 {
		t.Fatal("producer schema, fresh run identity, case or observation reference differs")
	}
	for _, binding := range []struct{ value, variable string }{
		{envelope.BuildInputsSHA256, "GOCLUSTER_PSKR_ZERO_BUILD_INPUTS_SHA256"},
		{envelope.ConfigSHA256, "GOCLUSTER_PSKR_ZERO_CONFIG_SHA256"},
		{envelope.CorpusSHA256, "GOCLUSTER_PSKR_ZERO_CORPUS_SHA256"},
		{envelope.ProducerBinarySHA256, "GOCLUSTER_PSKR_ZERO_PRODUCER_BINARY_SHA256"},
	} {
		if binding.value != requirePSKRZeroWireSHA(t, binding.variable) {
			t.Fatalf("producer identity differs for %s", binding.variable)
		}
	}
	configSHA, err := pskrZeroWireFileSHA(filepath.Join(identity.workspace, "data", "config", "path_reliability.yaml"))
	if err != nil || configSHA != envelope.ConfigSHA256 || envelope.ProducerBinarySHA256 == identity.consumerSHA {
		t.Fatalf("live config or separate producer/consumer binary identity differs: %v", err)
	}
	validatePSKRZeroWireRecords(t, data, envelope)
	return envelope
}

func validatePSKRZeroWireRecords(t *testing.T, data []byte, envelope pskrZeroWireEnvelope) {
	t.Helper()
	var raw struct {
		Records []struct {
			Spot map[string]json.RawMessage `json:"spot"`
		} `json:"records"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	want := pskrZeroWireCases()
	if len(envelope.Records) != len(want) || len(raw.Records) != len(want) {
		t.Fatalf("producer broadcast record count=%d/%d, want exactly %d", len(envelope.Records), len(raw.Records), len(want))
	}
	seen := make(map[uint64]bool, len(want))
	for i, record := range envelope.Records {
		for _, field := range []string{"Report", "HasReport"} {
			value := bytes.TrimSpace(raw.Records[i].Spot[field])
			if len(value) == 0 || bytes.Equal(value, []byte("null")) {
				t.Fatalf("record %d raw Spot lacks non-null %s", i, field)
			}
		}
		if record.Spot == nil || record.AllowFast == nil || record.AllowMed == nil || record.AllowSlow == nil ||
			record.HasReport == nil || record.Report == nil {
			t.Fatalf("record %d lacks mandatory snapshot, policy or report presence", i)
		}
		sp, expected := record.Spot, want[i]
		if record.CaseID != expected.id || sp.ID == 0 || seen[sp.ID] || sp.DXCall != expected.dx || sp.DXCallNorm != expected.dx ||
			sp.DECall != expected.de || sp.DECallNorm != expected.de || sp.Mode != expected.mode || sp.Frequency != expected.frequency ||
			sp.Band != "20m" || sp.SourceType != expected.source || sp.DXMetadata.Grid != expected.dxGrid || sp.DEMetadata.Grid != expected.deGrid ||
			sp.HasReport != expected.hasReport || sp.Report != expected.report || *record.HasReport != expected.hasReport || *record.Report != expected.report ||
			!sp.Time.Equal(time.Unix(envelope.ObservationUnix, 0).UTC()) || sp.IsBeacon || !*record.AllowFast {
			t.Fatalf("producer record %d differs from literal case %s: %+v", i, expected.id, record)
		}
		seen[sp.ID] = true
		if expected.source == spot.SourcePSKReporter {
			if sp.SourceNode != "PSKREPORTER" || sp.Comment != "" || sp.SpotterIP != "" || sp.IsHuman {
				t.Fatalf("actual PSKR fields were changed to help delivery: %+v", sp)
			}
		} else if !sp.IsHuman || sp.Comment == "" || strings.TrimSpace(sp.Comment) != "" || net.ParseIP(sp.SpotterIP) == nil {
			t.Fatalf("manual final barrier is not the representable frozen tuple: %+v", sp)
		}
	}
}

func startPSKRZeroWireServer(t *testing.T, observation int64) (*Server, net.Conn, *bufio.Reader, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := NewServer(ServerOptions{
		LoginPrompt: "login:\n", LoginGreeting: "ready\n", HandshakeMode: "none", EchoMode: "local",
		BroadcastWorkers: 1, BroadcastQueue: 8, WorkerQueue: 8, ClientBuffer: 8,
		BroadcastBatchIntervalSet: true, BroadcastBatchInterval: time.Millisecond,
		DedupeFastEnabled: true, DedupeMedEnabled: true, DedupeSlowEnabled: true, DefaultDedupePolicy: "fast",
		USLicenseCheck: func(string) bool { return true }, CanadianLicenseCheck: func(string) bool { return true },
	}, nil)
	server.nowFn = func() time.Time { return time.Unix(observation, 0).UTC() }
	server.startWithListener(listener)
	var conn net.Conn
	retired := false
	retire := func() {
		if retired {
			return
		}
		retired = true
		if conn != nil {
			_ = conn.Close()
		}
		server.Stop()
		waitShardingWireRetirement(t)
	}
	// Register before Dial: login/read failures must still stop and join owners.
	t.Cleanup(retire)
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err = dialer.DialContext(ctx, "tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(conn, "DL1ZRO\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReaderSize(conn, 4096)
	for lines := 0; ; lines++ {
		if lines == 32 {
			t.Fatal("bounded wire greeting did not complete")
		}
		if readPSKRZeroWireLine(t, conn, reader) == "ready" {
			break
		}
	}
	server.clientsMutex.RLock()
	client := server.clients["DL1ZRO"]
	server.clientsMutex.RUnlock()
	if client == nil {
		t.Fatal("ready greeting lacked a registered replay client")
	}
	client.setDedupePolicy(dedupePolicyFast)
	client.filterMu.Lock()
	client.filter.SetMode("FT8", true)
	client.filter.SetMode("CW", true)
	client.filterMu.Unlock()
	return server, conn, reader, retire
}

// Decoded spots have no private owned/cache state. SnapshotForAsync restores
// ownership without reconstructing or normalizing any producer field.
func replayPSKRZeroWireRecord(server *Server, record pskrZeroWireRecord) bool {
	if record.Spot == nil || record.AllowFast == nil || record.AllowMed == nil || record.AllowSlow == nil {
		return false
	}
	return server.TryBroadcastSpotOwned(record.Spot.SnapshotForAsync(), *record.AllowFast, *record.AllowMed, *record.AllowSlow)
}

func readPSKRZeroWireLine(t *testing.T, conn net.Conn, reader *bufio.Reader) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadSlice('\n')
	if err != nil {
		t.Fatalf("read bounded loopback wire line: %v", err)
	}
	return strings.TrimRight(string(line), "\r\n")
}

func collectPSKRZeroWireRows(t *testing.T, conn net.Conn, reader *bufio.Reader, observation int64) []pskrZeroWireRow {
	t.Helper()
	want := pskrZeroWireCases()
	rows := make([]pskrZeroWireRow, 0, len(want))
	layout := spot.CurrentDXClusterLayout()
	for lines := 0; lines < 32 && len(rows) < len(want); lines++ {
		line := readPSKRZeroWireLine(t, conn, reader)
		if !strings.HasPrefix(line, "DX ") {
			continue
		}
		// Every DX row is examined. A marker-skipping reader could conceal a
		// WSPR leak, duplicate or unknown row before the actual final barrier.
		expected := want[len(rows)]
		fields := strings.Fields(line)
		if len(fields) < 7 || fields[0] != "DX" || fields[1] != "de" || fields[2] != expected.de+":" ||
			fields[4] != expected.dx || fields[5] != expected.mode || len(line) != layout.LineLength {
			t.Fatalf("unexpected/out-of-order DX row for %s: %q", expected.id, line)
		}
		frequency, err := strconv.ParseFloat(fields[3], 64)
		if err != nil || frequency != expected.frequency ||
			line[layout.GridColumn-1:layout.GridColumn+3] != expected.dxGrid ||
			line[layout.TimeColumn-1:] != time.Unix(observation, 0).UTC().Format("1504Z") {
			t.Fatalf("wire tuple frequency/grid/time differs for %s: %q (%v)", expected.id, line, err)
		}
		if expected.hasReport {
			if len(fields) < 8 || fields[6] != fmt.Sprintf("%+d", expected.report) || fields[7] != "dB" {
				t.Fatalf("literal FT8 report missing for %s: %q", expected.id, line)
			}
		} else if strings.Contains(line, " dB") {
			t.Fatalf("manual absent report became a zero report: %q", line)
		}
		rows = append(rows, pskrZeroWireRow{expected.id, line})
	}
	if len(rows) != len(want) {
		t.Fatalf("wire completed %d rows, want exactly five through K1BAR", len(rows))
	}
	return rows
}

func writePSKRZeroWireReceipt(t *testing.T, path string, receipt any) {
	t.Helper()
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	written, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil || written != len(data)+1 {
		t.Fatalf("write successful wire receipt: %v/%v bytes=%d", writeErr, closeErr, written)
	}
}

func TestPSKRWireReplayPreservesMasks(t *testing.T) {
	server := NewServer(ServerOptions{BroadcastQueue: 1}, nil)
	t.Cleanup(server.Stop)
	for bits := 0; bits < 8; bits++ {
		fast, med, slow := bits&1 != 0, bits&2 != 0, bits&4 != 0
		sp := spot.NewSpot("K1ZFA", "N0CALL", 14074, "FT8")
		sp.HasReport = true
		record := pskrZeroWireRecord{Spot: sp, AllowFast: &fast, AllowMed: &med, AllowSlow: &slow}
		if !replayPSKRZeroWireRecord(server, record) {
			t.Fatalf("bounded queue refused mask %03b", bits)
		}
		select {
		case payload := <-server.broadcast:
			if payload.allowFast != fast || payload.allowMed != med || payload.allowSlow != slow ||
				payload.spot == sp || payload.spot.ID != sp.ID || !payload.spot.HasReport || payload.spot.Report != 0 {
				t.Fatalf("replay changed actual mask or zero-report snapshot for %03b: %+v", bits, payload)
			}
		default:
			t.Fatalf("replay claimed admission without a payload for mask %03b", bits)
		}
	}
}
