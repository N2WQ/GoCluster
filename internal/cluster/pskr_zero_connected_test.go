// File role: Proves present zero reports cross public MQTT ingress and the
// actual normal/path-only consumers before exporting actual broadcast tuples.
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/pathreliability"
	"dxcluster/pskreporter"
	"dxcluster/spot"
	"dxcluster/telnet"
)

type pskrZeroCase struct {
	id, call, mode, dxGrid, deGrid string
	report                         int
	compat                         bool
	p50                            float64
}

// Literal populations and medians are independent of the ingestion functions.
// Each directional fine/coarse key must be disjoint from all other cases.
func pskrZeroCases() []pskrZeroCase {
	return []pskrZeroCase{
		{"ft8_zero_fast", "K1ZFA", "FT8", "FN20", "EM12", 0, false, .5},
		{"ft8_zero_compat", "K1ZFB", "FT8", "JN58", "IO91", 0, true, .5},
		{"wspr_zero_fast", "K1ZWA", "WSPR", "QF56", "PM95", 0, false, .5},
		{"wspr_zero_compat", "K1ZWB", "WSPR", "GF05", "JJ00", 0, true, .5},
		{"ft8_nonzero", "K1NZA", "FT8", "KP20", "KO85", -7, false, -6.5},
		{"wspr_nonzero", "K1NZW", "WSPR", "JF95", "KM72", -7, false, -6.5},
		{"ft8_fence", "K1FNA", "FT8", "RF64", "CN89", -8, false, -7.5},
		{"wspr_fence", "K1FNW", "WSPR", "IL18", "OL72", -8, false, -7.5},
	}
}

type pskrZeroBroadcastRecord struct {
	CaseID    string     `json:"case_id"`
	Spot      *spot.Spot `json:"spot"`
	AllowFast bool       `json:"allow_fast"`
	AllowMed  bool       `json:"allow_med"`
	AllowSlow bool       `json:"allow_slow"`
	HasReport bool       `json:"has_report"`
	Report    int        `json:"report"`
}

type pskrZeroBroadcastEnvelope struct {
	Schema               string                    `json:"schema"`
	RunID                string                    `json:"run_id"`
	ProducerCase         string                    `json:"producer_case"`
	BuildInputsSHA256    string                    `json:"build_inputs_sha256"`
	ConfigSHA256         string                    `json:"config_sha256"`
	CorpusSHA256         string                    `json:"corpus_sha256"`
	ProducerBinarySHA256 string                    `json:"producer_binary_sha256"`
	ObservationUnix      int64                     `json:"observation_unix"`
	Records              []pskrZeroBroadcastRecord `json:"records"`
}

func TestPSKRZeroConnectedRoutes(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("listening S5 qualification uses the approved native Windows executable")
	}
	pathShardingRequireExecutable(t)
	repo := pathShardingRepoRoot()
	requirePathReportH3Mappings(t)
	cfgPath := filepath.Join(repo, "data", "config", "path_reliability.yaml")
	pathCfg, err := pathreliability.LoadFile(cfgPath)
	if err != nil || !pathCfg.Enabled {
		t.Fatalf("load enabled predictor: %v", err)
	}
	if pathCfg.ModeOffsets.WSPR != 0 {
		t.Fatal("literal S5 connected medians require the shipped zero WSPR offset")
	}
	predictor := pathreliability.NewPredictor(pathCfg, pathCfg.AllowedBands)
	cases := pskrZeroCases()
	pskrZeroCheckDisjoint(t, cases)
	observed := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Second)
	corpus := pskrZeroCorpus(cases, observed.Unix())
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	broker, err := startPSKRZeroBroker(ctx, "s5/reports", corpus)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = broker.Close() })
	peerWire, err := startPSKRZeroPeer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peerWire.Close() })
	if err := peerWire.WaitActive(ctx); err != nil {
		t.Fatal(err)
	}
	artifactRoot := filepath.Join(repo, ".tmp", "path-reliability-first-wave", "s5")
	if err := os.MkdirAll(artifactRoot, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(artifactRoot, "connected-")
	if err != nil {
		t.Fatal(err)
	}
	archiveCfg := config.ArchiveConfig{DBPath: filepath.Join(dir, "archive"), QueueSize: 64}
	writer, err := archive.NewWriter(archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	writer.Start()
	t.Cleanup(writer.Stop)
	ring := buffer.NewRingBuffer(64)
	display := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 64}, nil)
	t.Cleanup(display.Stop)
	metrics, pathStats := newPathReportMetrics(), &pathOnlyStats{}
	allowed := make(map[string]struct{}, len(pathCfg.AllowedBands))
	for _, band := range pathCfg.AllowedBands {
		allowed[band] = struct{}{}
	}
	policy := config.SpotPolicy{MaxAgeSeconds: 300}
	pipeline := &outputPipeline{buf: ring, archiveWriter: writer, telnet: display,
		peerManager: peerWire.Manager(), ctyLookup: func() *cty.CTYDatabase { return nil },
		pathPredictor: predictor, pathReport: metrics, allowedBands: allowed, spotPolicy: policy}
	host, port := broker.Endpoint()
	client := pskreporter.NewClient(host, port, []string{"s5/reports"}, "S5-loopback", 1, 1, 64, 0, nil, nil, false, 64, 0)
	var clientStop sync.Once
	stopClient := func() { clientStop.Do(client.Stop) }
	// Public output channels are client-owned and never closed by this fixture.
	// Relays own separate channels that can close after Stop and quiescent drain.
	relayCtx, stopRelays := context.WithCancel(ctx)
	drain := make(chan struct{})
	var drainOnce, ingestClose sync.Once
	normal, pathOnly := make(chan *spot.Spot, 64), make(chan *spot.Spot, 64)
	var relays sync.WaitGroup
	for _, route := range []struct {
		in  <-chan *spot.Spot
		out chan *spot.Spot
	}{{client.GetSpotChannel(), normal}, {client.GetPathOnlyChannel(), pathOnly}} {
		relays.Go(func() { pskrZeroRelay(relayCtx, drain, route.in, route.out) })
	}
	relayDone := make(chan struct{})
	go func() { relays.Wait(); close(relayDone) }()
	ingest := make(chan *spot.Spot, 64)
	forwardDone, pathDone, outputDone, normalFence := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var normalRecords []*spot.Spot // output owner writes; lead reads after join.
	var fenceOnce sync.Once
	go func() { defer close(forwardDone); forwardSpots(normal, ingest, "S5-PSKR", policy, nil) }()
	go func() {
		defer close(pathDone)
		processPSKRPathOnlyChannel(pathOnly, predictor, metrics, pathStats, policy, allowed)
	}()
	go func() {
		defer close(outputDone)
		for s := range ingest {
			pipeline.processSpot(s, nil)
			normalRecords = append(normalRecords, s.Clone())
			if s.DXCall == "K1FNA" {
				fenceOnce.Do(func() { close(normalFence) })
			}
		}
	}()
	t.Cleanup(func() {
		stopClient()
		stopRelays()
		drainOnce.Do(func() { close(drain) })
		pskrZeroCleanupWait(t, relayDone, "relay retirement")
		if pskrZeroCleanupWait(t, forwardDone, "forward retirement") {
			ingestClose.Do(func() { close(ingest) })
		}
		pskrZeroCleanupWait(t, pathDone, "path-only retirement")
		pskrZeroCleanupWait(t, outputDone, "output retirement")
	})
	mqttBaseline := pskrZeroMQTTOwners(t)
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-normalFence:
	case <-ctx.Done():
		t.Fatalf("positive normal fence missing: %v", ctx.Err())
	}
	// These positive fences work with the old zero-drop expression too. A
	// negative control must reach retirement and fail a zero-specific assertion.
	pskrZeroAwaitPopulation(ctx, t, predictor, cases[6], observed)
	pskrZeroAwaitPopulation(ctx, t, predictor, cases[7], observed)
	t.Log("positive FT8 and WSPR learning fences completed")
	if !client.HealthSnapshot().Connected {
		t.Fatal("MQTT connection ended before the requested Stop")
	}
	pskrZeroRequireLiveMQTT(t, mqttBaseline)
	stopClient()
	if client.HealthSnapshot().Connected {
		t.Fatal("MQTT client remained connected after Stop")
	}
	drainOnce.Do(func() { close(drain) })
	pskrZeroWait(t, relayDone, "quiescent relay drain")
	pskrZeroWait(t, forwardDone, "normal forward completion")
	pskrZeroWait(t, pathDone, "path-only completion")
	manual := spot.NewSpot("K1BAR", "N1MAN", 14020, "CW")
	manual.Time, manual.Comment, manual.SpotterIP = observed, " ", "192.0.2.1"
	manual.DXMetadata.Grid, manual.DEMetadata.Grid = "FN31", "DM79"
	manual.InvalidateMetadataCache()
	manual.EnsureNormalized()
	if !peerWire.Manager().ShouldPublishLocalSpot(manual) {
		t.Fatal("manual peer observer barrier is ineligible")
	}
	ingest <- manual
	ingestClose.Do(func() { close(ingest) })
	pskrZeroWait(t, outputDone, "normal output completion")
	frames, err := peerWire.WaitFinal(ctx, "K1BAR")
	if err != nil {
		t.Fatal(err)
	}
	pskrZeroCheckPeerFrame(t, frames, observed)
	if err := peerWire.Close(); err != nil {
		t.Fatal(err)
	}
	brokerResult, err := broker.Close()
	if err != nil || brokerResult.Connections != 1 || brokerResult.Subscriptions != 1 || brokerResult.Published != len(corpus) || (!brokerResult.Disconnected && !brokerResult.ClientClosed) {
		t.Fatalf("MQTT protocol/retirement incomplete: %+v err=%v", brokerResult, err)
	}
	pskrZeroAwaitMQTTRetirement(t, mqttBaseline)
	// Archive insertion has its own fence; Stop alone is not an insertion fence.
	pskrZeroAwaitArchiveControls(ctx, t, writer)
	writer.Stop()
	reopened, err := archive.NewWriter(archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := reopened.Recent(64)
	reopened.Stop()
	if err != nil {
		t.Fatal(err)
	}
	broadcasts := pskrZeroDrainBroadcasts(t, display, cases)
	t.Log("positive processing, archive and actual manual peer fences completed; protocol and item owners retired")
	// Keep this before generic total-count oracles: the restored old gate must
	// fail the actual absent zero reports, never a timeout waiting on them.
	for _, tc := range cases[:4] {
		pskrZeroRequirePopulation(t, predictor, tc, observed)
		if tc.mode == "FT8" && !pskrZeroContains(normalRecords, tc.call) {
			t.Fatalf("present zero admission missing for %s", tc.id)
		}
	}
	if len(normalRecords) != 5 || ring.GetCount() != 5 || len(broadcasts) != 5 || len(stored) != 5 {
		t.Fatalf("normal final sets: processed=%d ring=%d broadcasts=%d archive=%d; want5", len(normalRecords), ring.GetCount(), len(broadcasts), len(stored))
	}
	for _, records := range [][]*spot.Spot{normalRecords, ring.GetRecent(64), stored} {
		pskrZeroCheckNormalSet(t, records, cases, observed)
	}
	var broadcastSpots []*spot.Spot
	for i, wantID := range []string{"ft8_zero_fast", "ft8_zero_compat", "ft8_nonzero", "ft8_fence", "manual_barrier"} {
		if broadcasts[i].CaseID != wantID {
			t.Fatalf("broadcast order/identity %d=%s want %s", i, broadcasts[i].CaseID, wantID)
		}
		broadcastSpots = append(broadcastSpots, broadcasts[i].Spot)
	}
	pskrZeroCheckNormalSet(t, broadcastSpots, cases, observed)
	for _, tc := range cases {
		pskrZeroRequirePopulation(t, predictor, tc, observed)
	}
	if got := snapshotPathOnly(pathStats); got.updates != 4 || got.drops != 0 {
		t.Fatalf("path-only exact work: %+v", got)
	}
	if got := metrics.SnapshotSources(); got["PSK"] != 8 || len(got) != 1 {
		t.Fatalf("exact learned source accounting: %+v", got)
	}
	health := client.HealthSnapshot()
	if health.PayloadDrops != 0 || health.PayloadTooLarge != 0 || health.SpotDrops != 0 || health.PathOnlyDrops != 0 || health.ParseErrors != 0 || health.MQTTInboundDropsQoS0 != 0 || health.MQTTInboundQoS12Timeouts != 0 || health.MQTTInboundQoS12Disconnects != 0 {
		t.Fatalf("small corpus lost input: %+v", health)
	}
	for _, s := range normalRecords {
		if s.SourceType == spot.SourcePSKReporter && peerWire.Manager().ShouldPublishLocalSpot(s) {
			t.Fatalf("PSKR source became peer eligible: %+v", s)
		}
	}
	if a := predictor.AdmissionStats(); a != (pathreliability.AdmissionStats{}) {
		t.Fatalf("valid learning reports rejected: %+v", a)
	}
	stopRelays()
	pskrZeroExport(t, cfgPath, corpus, observed, broadcasts)
	t.Logf("S5 connected exact ingress/learning/archive/peer evidence passed; fixture=%s", dir)
}

func pskrZeroRelay(ctx context.Context, drain <-chan struct{}, in <-chan *spot.Spot, out chan<- *spot.Spot) {
	defer close(out)
	emit := func(s *spot.Spot) bool {
		select {
		case out <- s:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		select {
		case s := <-in:
			if !emit(s) {
				return
			}
		case <-drain:
			for {
				select {
				case s := <-in:
					if !emit(s) {
						return
					}
				default:
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func pskrZeroWait(t *testing.T, done <-chan struct{}, owner string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s failed to join", owner)
	}
}

// Cleanup must attempt every owner even if one retirement assertion fails.
func pskrZeroCleanupWait(t *testing.T, done <-chan struct{}, owner string) bool {
	t.Helper()
	select {
	case <-done:
		return true
	case <-time.After(5 * time.Second):
		t.Errorf("%s failed to join during cleanup", owner)
		return false
	}
}

func pskrZeroCorpus(cases []pskrZeroCase, observed int64) [][]byte {
	var corpus [][]byte
	for i, tc := range cases {
		if i == 6 {
			for _, control := range []string{`"sc":"K1ABS","md":"FT8"`, `"sc":"K1NUL","md":"WSPR","rp":null`, `"sc":"K1IGN","md":"PACKET","rp":0`} {
				corpus = append(corpus, []byte(fmt.Sprintf(`{"f":14074000,"t":%d,"rc":"N0CALL","sl":"FN20","rl":"EM12",%s}`, observed, control)))
			}
		}
		call := tc.call
		if tc.compat {
			call = tc.call[:3] + fmt.Sprintf(`\u%04x`, tc.call[3]) + tc.call[4:]
		}
		frequency := 14074000
		if tc.mode == "WSPR" {
			frequency = 14097000
		}
		corpus = append(corpus, []byte(fmt.Sprintf(`{"f":%d,"md":"%s","rp":%d,"t":%d,"sc":"%s","sl":"%s","rc":"N0CALL","rl":"%s"}`, frequency, tc.mode, tc.report, observed, call, tc.dxGrid, tc.deGrid)))
	}
	return corpus
}

func pskrZeroCheckDisjoint(t *testing.T, cases []pskrZeroCase) {
	t.Helper()
	for _, coarse := range []bool{false, true} {
		seen := make(map[[2]pathreliability.CellID]string)
		for _, tc := range cases {
			dx, de := pathreliability.EncodeCell(tc.dxGrid), pathreliability.EncodeCell(tc.deGrid)
			if coarse {
				dx, de = pathreliability.EncodeCoarseCell(tc.dxGrid), pathreliability.EncodeCoarseCell(tc.deGrid)
			}
			if dx == 0 || de == 0 || dx == de {
				t.Fatalf("non-disjoint mapped endpoints for %s coarse=%v", tc.id, coarse)
			}
			for _, key := range [][2]pathreliability.CellID{{de, dx}, {dx, de}} {
				if prior := seen[key]; prior != "" {
					t.Fatalf("learning controls overlap %s/%s coarse=%v", prior, tc.id, coarse)
				}
				seen[key] = tc.id
			}
		}
	}
}

func pskrZeroPopulation(p *pathreliability.Predictor, tc pskrZeroCase, at time.Time) pathreliability.Result {
	return p.Predict(pathreliability.EncodeCell(tc.deGrid), pathreliability.EncodeCell(tc.dxGrid), pathreliability.EncodeCoarseCell(tc.deGrid), pathreliability.EncodeCoarseCell(tc.dxGrid), "20m", "FT8", 0, at)
}

func pskrZeroAwaitPopulation(ctx context.Context, t *testing.T, p *pathreliability.Predictor, tc pskrZeroCase, at time.Time) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		got := pskrZeroPopulation(p, tc, at)
		if got.RawCount == 1 && got.HasP50 && got.P50DB == tc.p50 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("positive learning fence %s missing: %+v (%v)", tc.id, got, ctx.Err())
		case <-tick.C:
		}
	}
}

func pskrZeroRequirePopulation(t *testing.T, p *pathreliability.Predictor, tc pskrZeroCase, at time.Time) {
	t.Helper()
	got := pskrZeroPopulation(p, tc, at)
	if got.RawCount != 1 || !got.HasP50 || got.P50DB != tc.p50 || !got.HasObservedSamplingTime || !got.ObservedDistributionAvailable || got.ObservedSamplingUnix < at.Unix() || got.AgeSec != got.ObservedSamplingUnix-at.Unix() {
		t.Fatalf("present report learning missing for %s: %+v want raw1 p50=%v", tc.id, got, tc.p50)
	}
}

func pskrZeroContains(records []*spot.Spot, call string) bool {
	for _, s := range records {
		if s.DXCall == call {
			return true
		}
	}
	return false
}

func pskrZeroAwaitArchiveControls(ctx context.Context, t *testing.T, writer *archive.Writer) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		rows, err := writer.Recent(64)
		if err != nil {
			t.Fatal(err)
		}
		if pskrZeroContains(rows, "K1FNA") && pskrZeroContains(rows, "K1BAR") {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("positive archive fences missing: %v", ctx.Err())
		case <-tick.C:
		}
	}
}

func pskrZeroDrainBroadcasts(t *testing.T, server *telnet.Server, cases []pskrZeroCase) []pskrZeroBroadcastRecord {
	t.Helper()
	var records []pskrZeroBroadcastRecord
	queue := unsafeValue(reflect.ValueOf(server).Elem().FieldByName("broadcast"))
	for {
		value, ok := queue.TryRecv()
		if !ok {
			return records
		}
		if len(records) >= 64 {
			t.Fatal("unbounded broadcast corpus")
		}
		if value.Kind() == reflect.Pointer {
			value = value.Elem()
		}
		s := unsafeValue(value.FieldByName("spot")).Interface().(*spot.Spot)
		id := ""
		if s.DXCall == "K1BAR" {
			id = "manual_barrier"
		}
		for _, tc := range cases {
			if tc.call == s.DXCall && tc.mode == "FT8" {
				id = tc.id
			}
		}
		if id == "" {
			t.Fatalf("unexpected/path-only broadcast: %+v", s)
		}
		records = append(records, pskrZeroBroadcastRecord{id, s, value.FieldByName("allowFast").Bool(), value.FieldByName("allowMed").Bool(), value.FieldByName("allowSlow").Bool(), s.HasReport, s.Report})
	}
}

func pskrZeroCheckNormalSet(t *testing.T, records []*spot.Spot, cases []pskrZeroCase, observed time.Time) {
	t.Helper()
	seen := make(map[string]bool)
	for _, s := range records {
		if s == nil || seen[s.DXCall] {
			t.Fatalf("nil/duplicate normal output: %+v", s)
		}
		seen[s.DXCall] = true
		if s.DXCall == "K1BAR" {
			if s.DECall != "N1MAN" || s.SourceType != spot.SourceManual || s.HasReport || s.Report != 0 || s.Frequency != 14020 || !s.Time.Equal(observed) {
				t.Fatalf("manual barrier changed: %+v", s)
			}
			continue
		}
		found := false
		for _, tc := range cases {
			if tc.mode != "FT8" || tc.call != s.DXCall {
				continue
			}
			found = true
			if s.DECall != "N0CALL" || s.SourceType != spot.SourcePSKReporter || !s.HasReport || s.Report != tc.report || s.Frequency != 14074 || s.Mode != "FT8" || s.DXMetadata.Grid != tc.dxGrid || s.DEMetadata.Grid != tc.deGrid || !s.Time.Equal(observed) {
				t.Fatalf("normal tuple changed for %s: %+v", tc.id, s)
			}
		}
		if !found {
			t.Fatalf("path-only/rejected/unknown normal tuple: %+v", s)
		}
	}
}

func pskrZeroCheckPeerFrame(t *testing.T, frames []string, at time.Time) {
	t.Helper()
	if len(frames) != 1 {
		t.Fatalf("peer exclusion/manual fence: frames=%q", frames)
	}
	fields := strings.Split(frames[0], "^")
	if len(fields) < 9 {
		t.Fatalf("short manual peer frame: %q", frames[0])
	}
	frequency, err := strconv.ParseFloat(fields[1], 64)
	if err != nil || (fields[0] != "PC11" && fields[0] != "PC61") || frequency != 14020 || fields[2] != "K1BAR" || fields[3] != at.Format("02-Jan-2006") || fields[4] != at.Format("1504Z") || fields[5] != " " || fields[6] != "N1MAN" || fields[7] != "N0CALL" {
		t.Fatalf("actual manual peer tuple changed: %q (%v)", frames[0], err)
	}
}

func pskrZeroExport(t *testing.T, configPath string, corpus [][]byte, at time.Time, records []pskrZeroBroadcastRecord) {
	t.Helper()
	output := os.Getenv("GOCLUSTER_PSKR_ZERO_EXPORT")
	if output == "" {
		return
	}
	root := filepath.Join(pathShardingRepoRoot(), ".tmp", "path-reliability-first-wave", "s5")
	rel, err := filepath.Rel(root, output)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		t.Fatalf("export outside approved S5 artifacts: %q", output)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("export must be a fresh path: %q (%v)", output, err)
	}
	runID, sourceID := os.Getenv("GOCLUSTER_PSKR_ZERO_RUN_ID"), os.Getenv("GOCLUSTER_PSKR_ZERO_BUILD_INPUTS_SHA256")
	if runID == "" || len(sourceID) != 64 {
		t.Fatal("export requires controller run/source identity")
	}
	if _, err := hex.DecodeString(sourceID); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	var size [8]byte
	for _, payload := range corpus {
		binary.BigEndian.PutUint64(size[:], uint64(len(payload)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(payload)
	}
	envelope := pskrZeroBroadcastEnvelope{"gocluster.pskr-zero.broadcast.v1", runID, "TestPSKRZeroConnectedRoutes", sourceID, pathShardingFileHash(t, configPath), hex.EncodeToString(hash.Sum(nil)), pathShardingFileHash(t, executable), at.Unix(), records}
	data, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 5 || len(data) > 64*1024 {
		t.Fatal("invalid bounded export population")
	}
	if err := os.WriteFile(output, data, 0600); err != nil {
		t.Fatal(err)
	}
}
