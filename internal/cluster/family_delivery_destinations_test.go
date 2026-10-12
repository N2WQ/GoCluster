//go:build !qualification

// File role: Exercises telnet-only family hiding through loaded configuration,
// production construction, actual recipients and independently persisted bytes.
// Scheduler ownership is real; the test consumes releases synchronously instead
// of starting a second output consumer. Missing baseline output is an assertion.
package cluster

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/peer"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"

	"github.com/cockroachdb/pebble"
	"gopkg.in/yaml.v3"
)

type familyDeliveryInput struct {
	call, receiver, comment, glyph string
	at                             time.Time
}

func (in familyDeliveryInput) spot() *spot.Spot {
	s := spot.NewSpot(in.call, in.receiver, 14030, "CW")
	s.SourceType = spot.SourceManual
	s.Time, s.Comment, s.SpotterIP = in.at, in.comment, "192.0.2.1"
	s.Report, s.HasReport = 10, true
	s.DEMetadata.ADIF, s.DEMetadata.CQZone, s.DEMetadata.Continent = 291, 5, "NA"
	return s
}

type familyDeliveryFixture struct {
	ambiguityDeliveryFixture
	selfConn     net.Conn
	selfReader   *bufio.Reader
	recent       spot.RecentSupportStore
	custom       *spot.CustomSCPStore
	customOpts   spot.CustomSCPOptions
	who          *spot.WhoSpotsMeStore
	deduplicator *dedup.Deduplicator
}

func TestFamilyDeliveryImmediateDestinationsAndHistory(t *testing.T) {
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, custom := range []bool{false, true} {
		for _, self := range []bool{false, true} {
			t.Run(fmt.Sprintf("custom_%t/self_%t", custom, self), func(t *testing.T) {
				temporalDeliveryDirectory(t)
				f := newFamilyDeliveryFixture(t, source, custom, self, 15, 8, stabilizerTimeoutRelease, true)
				at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
				seed := familyDeliveryInput{"KP4/N2WQ", "KP4/N2WQ", "family seed", "V", at}
				incoming := familyDeliveryInput{"N2WQ", "N2WQ", "family incoming", "V", at.Add(time.Second)}
				barrier := familyDeliveryInput{"K1BAR", "K1BAR", "family barrier", "V", at.Add(2 * time.Second)}
				f.pipeline.processSpot(seed.spot(), nil)
				f.assertReceipts(t, []familyDeliveryInput{seed}, []familyDeliveryInput{seed}, []familyDeliveryInput{seed}, seed.call)
				f.assertHistory(t, false, true)
				f.pipeline.processSpot(incoming.spot(), nil)
				f.pipeline.processSpot(barrier.spot(), nil)
				selfWant := []familyDeliveryInput{barrier}
				if self {
					selfWant = []familyDeliveryInput{incoming, barrier}
				}
				// Missing fresh evidence provides no hiding authority. The saved
				// destinations/history remain the independent F1 contract.
				f.assertReceipts(t, []familyDeliveryInput{incoming, barrier}, selfWant, []familyDeliveryInput{incoming, barrier}, barrier.call)
				if f.pipeline.telnetStabilizer.Pending() != 0 || f.pipeline.tracker.StabilizerHeld() != 0 {
					t.Error("authoritative local self V must not acquire stabilizer ownership")
				}
				f.assertHistory(t, true, true)
				f.assertStored(t, []familyDeliveryInput{seed, incoming, barrier})
			})
		}
	}
}

func TestFamilyDeliveryWithoutTelnetProductionConstructor(t *testing.T) {
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom_%t", custom), func(t *testing.T) {
			temporalDeliveryDirectory(t)
			f := newFamilyDeliveryFixture(t, source, custom, false, 15, 8, stabilizerTimeoutRelease, false)
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			inputs := []familyDeliveryInput{
				{"KP4/N2WQ", "KP4/N2WQ", "family seed", "V", at},
				{"N2WQ", "N2WQ", "family incoming", "V", at.Add(time.Second)},
				{"K1BAR", "K1BAR", "family barrier", "V", at.Add(2 * time.Second)},
			}
			for _, in := range inputs {
				f.pipeline.processSpot(in.spot(), nil)
			}
			// With enabled config and nil telnet, neither telnet component may
			// exist or gate the otherwise eligible original peer/local fanout.
			if f.pipeline.telnet != nil || f.pipeline.stabilizerEnabled || f.pipeline.telnetStabilizer != nil || f.pipeline.familySuppressor != nil ||
				f.pipeline.tracker.StabilizerHeld() != 0 || f.pipeline.tracker.CallCorrections() != 0 {
				t.Error("no-telnet production construction created a local display gate or correction")
			}
			f.assertPeerReceipts(t, inputs, "K1BAR")
			f.assertHistory(t, true, true)
			f.assertStored(t, inputs)
		})
	}
}

func TestFamilyDeliverySchedulerOwnershipAndHistory(t *testing.T) {
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, custom := range []bool{false, true} {
		for _, timing := range []string{"overflow", "delayed", "timeout_suppress"} {
			t.Run(fmt.Sprintf("custom_%t/%s", custom, timing), func(t *testing.T) {
				temporalDeliveryDirectory(t)
				delay, capacity, timeout := 1, 8, stabilizerTimeoutRelease
				switch timing {
				case "overflow":
					delay, capacity = 3600, 1
				case "timeout_suppress":
					timeout = stabilizerTimeoutSuppress
				}
				f := newFamilyDeliveryFixture(t, source, custom, false, delay, capacity, timeout, true)
				at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
				seed := familyDeliveryInput{"KP4/N2WQ", "KP4/N2WQ", "family seed", "V", at}
				glyph := "?"
				if custom {
					glyph = "S" // Seeded learned membership; no current resolver population.
				}
				incoming := familyDeliveryInput{"N2WQ", "N0AAA", "family incoming", glyph, at.Add(time.Second)}
				barrier := familyDeliveryInput{"K1BAR", "K1BAR", "family barrier", "V", at.Add(2 * time.Second)}
				f.pipeline.processSpot(seed.spot(), nil)
				f.assertReceipts(t, []familyDeliveryInput{seed}, []familyDeliveryInput{seed}, []familyDeliveryInput{seed}, seed.call)
				if timing == "overflow" {
					filler := familyDeliveryInput{"K1HOLD", "N0BBB", "owned filler", "?", at}
					f.pipeline.processSpot(filler.spot(), nil)
					if f.pipeline.telnetStabilizer.Pending() != 1 || f.pipeline.tracker.StabilizerHeld() != 1 {
						t.Fatal("actual filler did not reserve the only stabilizer slot")
					}
				}
				f.pipeline.processSpot(incoming.spot(), nil)
				if timing == "overflow" {
					if f.pipeline.tracker.StabilizerOverflowRelease() != 1 || f.pipeline.telnetStabilizer.Pending() != 1 {
						t.Error("overflow must refuse the base hold while preserving the filler's reservation")
					}
				} else {
					if f.pipeline.telnetStabilizer.Pending() != 1 || f.pipeline.tracker.StabilizerHeld() != 1 || f.ring.GetCount() != 1 {
						t.Fatal("base must remain owned without local fanout before scheduler release")
					}
					f.assertHistory(t, false, false)
					envelope := familyDeliveryReceiveRelease(t, f.pipeline.telnetStabilizer)
					if envelope.spot.DXCallNorm != incoming.call || envelope.checksCompleted != 0 || !envelope.hasResolverKey ||
						envelope.evidenceEnqueued || envelope.peerOriginal == nil || f.pipeline.telnetStabilizer.Pending() != 0 {
						t.Fatal("actual scheduler release lost its sole ownership or original publication context")
					}
					f.pipeline.handleStabilizerRelease(envelope)
					select {
					case <-f.pipeline.telnetStabilizer.ReleaseChan():
						t.Error("one held base produced more than one scheduler envelope")
					default:
					}
				}
				admitted := timing != "timeout_suppress"
				if timing == "delayed" && f.pipeline.tracker.StabilizerReleasedDelayed() != 1 ||
					timing == "timeout_suppress" && f.pipeline.tracker.StabilizerSuppressedTimeout() != 1 {
					t.Error("actual final delay disposition was not counted exactly once")
				}
				f.pipeline.processSpot(barrier.spot(), nil)
				peerWant, rows := []familyDeliveryInput{barrier}, []familyDeliveryInput{seed, barrier}
				if admitted {
					peerWant, rows = []familyDeliveryInput{incoming, barrier}, []familyDeliveryInput{seed, incoming, barrier}
				}
				normalWant := []familyDeliveryInput{barrier}
				if admitted {
					normalWant = []familyDeliveryInput{incoming, barrier}
				}
				f.assertReceipts(t, normalWant, []familyDeliveryInput{barrier}, peerWant, barrier.call)
				f.assertHistory(t, admitted, false)
				if counts := f.who.CountryCountsByContinent("K1HOLD", "20m", time.Now().UTC()); len(counts) != 0 {
					t.Error("an unreleased filler taught acceptance history")
				}
				f.pipeline.telnetStabilizer.Stop()
				if f.pipeline.telnetStabilizer.Pending() != 0 {
					t.Error("joining the scheduler retained a source reservation")
				}
				f.assertStored(t, rows)
			})
		}
	}
}

// Keep the effective settings from the complete loader, with private storage
// and explicit test timing. No fixture edits the repository configuration.
func familyDeliveryLoadedConfig(t *testing.T, source string, custom bool, delay, capacity int, timeout string) *config.Config {
	t.Helper()
	directory := t.TempDir()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.EqualFold(filepath.Ext(entry.Name()), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if entry.Name() == "pipeline.yaml" {
			var pipeline map[string]any
			if err := yaml.Unmarshal(data, &pipeline); err != nil {
				t.Fatal(err)
			}
			correction, ok := pipeline["call_correction"].(map[string]any)
			if !ok {
				t.Fatal("complete configuration lacks call_correction")
			}
			correction["stabilizer_delay_seconds"], correction["stabilizer_max_pending"] = delay, capacity
			correction["stabilizer_max_checks"], correction["stabilizer_timeout_action"] = 1, timeout
			scp, ok := correction["custom_scp"].(map[string]any)
			if !ok {
				t.Fatal("complete configuration lacks custom_scp")
			}
			scp["enabled"], scp["path"] = custom, filepath.Join(t.TempDir(), "scp")
			scp["block_cache_mb"], scp["memtable_size_mb"] = 1, 4
			data, err = yaml.Marshal(pipeline)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(directory, entry.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := config.Load(directory)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func newFamilyDeliveryFixture(t *testing.T, source string, custom, self bool, delay, capacity int, timeout string, withTelnet bool) familyDeliveryFixture {
	t.Helper()
	return newFamilyDeliveryFixtureWithResolver(t, source, custom, self, delay, capacity, timeout, withTelnet, nil)
}

func newFamilyDeliveryFixtureWithResolver(t *testing.T, source string, custom, self bool, delay, capacity int, timeout string, withTelnet bool, resolver *spot.SignalResolver) familyDeliveryFixture {
	t.Helper()
	cfg := familyDeliveryLoadedConfig(t, source, custom, delay, capacity, timeout)
	if resolver != nil {
		// This running-loop witness isolates stabilizer scheduling; contested
		// temporal policy keeps its separate exhaustive and composed owners.
		cfg.CallCorrection.TemporalDecoder.Enabled = false
	}
	if !cfg.CallCorrection.Enabled || !cfg.CallCorrection.StabilizerEnabled || !cfg.CallCorrection.FamilyPolicy.TelnetSuppression.Enabled {
		t.Fatal("loaded fixture must retain enabled correction/stabilizer/family switches")
	}
	f := familyDeliveryFixture{ambiguityDeliveryFixture: ambiguityDeliveryFixture{
		ring: buffer.NewRingBuffer(8), archiveCfg: config.ArchiveConfig{
			DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1,
		},
	}, who: spot.NewWhoSpotsMeStore(time.Minute)}
	var err error
	f.writer, err = archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.writer.Start()
	t.Cleanup(f.writer.Stop)
	f.recent = spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
	if custom {
		c := cfg.CallCorrection.CustomSCP
		f.customOpts = spot.CustomSCPOptions{Path: c.Path, HorizonDays: c.HistoryHorizonDays, StaticHorizonDays: c.StaticHorizonDays,
			MaxKeys: c.MaxKeys, MaxSpottersPerKey: c.MaxSpottersPerKey, CacheSizeBytes: int64(c.BlockCacheMB) << 20,
			MemTableSizeBytes: uint64(c.MemTableSizeMB) << 20, CoreMinScore: c.ResolverMinScore, CoreMinH3Cells: c.ResolverMinUniqueH3Cells,
			SFloorMinScore: c.SFloorMinScore, SFloorExactMinH3Cells: c.SFloorMinUniqueH3CellsExact,
			SFloorFamilyMinH3Cells: c.SFloorMinUniqueH3CellsFamily, MinSNRDBCW: c.MinSNRDBCW, MinSNRDBRTTY: c.MinSNRDBRTTY}
		f.custom = customSCPFreshnessOpen(t, f.customOpts)
		f.recent = f.custom
	}
	var srv *telnet.Server
	if withTelnet {
		srv = f.startTelnet(t, self)
	}
	manager, recipient := correctionEvidencePeer(t)
	f.peer = recipient
	f.deduplicator = dedup.NewDeduplicator(0, false, 8)
	f.pipeline = newOutputPipeline(f.deduplicator, nil, nil, nil, nil, nil,
		f.ring, srv, manager, stats.NewTracker(), resolver, cfg.CallCorrection, func() *cty.CTYDatabase { return nil },
		nil, nil, cfg.Harmonics, nil, cfg.SpotPolicy, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		f.recent, f.custom, f.who, false, f.writer, nil, nil, nil, nil, nil)
	if withTelnet && (!f.pipeline.stabilizerEnabled || f.pipeline.telnetStabilizer == nil || f.pipeline.familySuppressor == nil) {
		t.Fatal("loaded production constructor omitted the enabled stabilizer or family suppressor")
	}
	if f.pipeline.telnetStabilizer != nil {
		t.Cleanup(f.pipeline.telnetStabilizer.Stop)
	}
	return f
}

func (f *familyDeliveryFixture) startTelnet(t *testing.T, self bool) *telnet.Server {
	t.Helper()
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n",
		MaxConnections: 4, BroadcastWorkers: 1, BroadcastQueue: 8, WorkerQueue: 8, ClientBuffer: 8,
		BroadcastBatchIntervalSet: true}, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	familyAdmissionJoinClients(t, srv)
	f.normalConn, f.normalReader = correctionEvidenceLogin(t, port, "DL1ABC", "OFF")
	f.diagConn, f.diagReader = correctionEvidenceLogin(t, port, "DL2ABC", "CONF")
	f.selfConn, f.selfReader = correctionEvidenceLogin(t, port, "N2WQ", "OFF")
	command, acknowledgement := "REJECT SELF\r\n", "Self spots disabled"
	if self {
		command, acknowledgement = "PASS SELF\r\n", "Self spots enabled"
	}
	if _, err := io.WriteString(f.selfConn, command); err != nil {
		t.Fatal(err)
	}
	acknowledged := false
	for i := 0; i < 16; i++ {
		if strings.Contains(customSCPDeliveryReadLine(t, f.selfReader), acknowledgement) {
			acknowledged = true
			break
		}
	}
	if !acknowledged {
		t.Fatal("matching client did not acknowledge the explicit SELF policy")
	}
	return srv
}

func familyDeliveryReceiveRelease(t *testing.T, scheduler *telnetSpotStabilizer) *telnetStabilizerEnvelope {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case envelope := <-scheduler.ReleaseChan():
		if envelope == nil || envelope.spot == nil {
			t.Fatal("actual scheduler published an empty envelope")
		}
		return envelope
	case <-timer.C:
		t.Fatal("actual scheduler did not release the held family spot")
		return nil
	}
}

// Positive FIFO barriers distinguish an absent base from a delayed socket.
func (f familyDeliveryFixture) assertReceipts(t *testing.T, normal, self, peers []familyDeliveryInput, barrier string) {
	t.Helper()
	for _, dst := range []struct {
		name   string
		conn   net.Conn
		reader *bufio.Reader
		want   []familyDeliveryInput
	}{{"normal", f.normalConn, f.normalReader, normal}, {"DIAG", f.diagConn, f.diagReader, normal}, {"SELF", f.selfConn, f.selfReader, self}} {
		var got []customSCPConsumerReceipt
		for i := 0; i < 4; i++ {
			line := correctionEvidenceReadDX(t, dst.conn, dst.reader)
			if len(line) < 78 {
				t.Fatalf("short actual family TCP line: %q", line)
			}
			receipt := customSCPConsumerReceipt{strings.TrimSpace(line[27:39]), line[71:72]}
			got = append(got, receipt)
			if receipt.call == barrier {
				break
			}
		}
		want := make([]customSCPConsumerReceipt, 0, len(dst.want))
		for _, in := range dst.want {
			want = append(want, customSCPConsumerReceipt{in.call, in.glyph})
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s actual family TCP sequence=%+v want %+v", dst.name, got, want)
		}
	}
	f.assertPeerReceipts(t, peers, barrier)
}

func (f familyDeliveryFixture) assertPeerReceipts(t *testing.T, peers []familyDeliveryInput, barrier string) {
	t.Helper()
	var got [][]string
	for i := 0; i < 4; i++ {
		frame, err := peer.ParseFrame(peerAdmissionReadSpot(t, f.peer))
		if err != nil || frame.Type != "PC61" || len(frame.Fields) != 8 {
			t.Fatalf("actual family peer frame=%+v err=%v", frame, err)
		}
		got = append(got, frame.Fields)
		if frame.Fields[1] == barrier {
			break
		}
	}
	want := make([][]string, 0, len(peers))
	for _, in := range peers {
		want = append(want, []string{"14030", in.call, in.at.Format("02-Jan-2006"), in.at.Format("1504Z"),
			in.comment, in.receiver, "N0CALL-1", "192.0.2.1"})
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("original-input family PC61 sequence=%q want %q", got, want)
	}
}

func (f familyDeliveryFixture) assertHistory(t *testing.T, admitted, selfV bool) {
	t.Helper()
	want := 1
	if admitted && (f.custom == nil || selfV) {
		want = 2
	}
	var count int
	if f.custom != nil {
		count = f.custom.RecentSupportCountWithRequirements("N2WQ", "20m", "CW", spot.CustomSCPRequirements{MinScore: 5}, time.Now().UTC())
	} else {
		count = f.recent.RecentSupportCount("N2WQ", "20m", "CW", time.Now().UTC())
	}
	if count != want {
		t.Errorf("history distinct base receivers=%d want %d (admitted=%t selfV=%t)", count, want, admitted, selfV)
	}
	wantWho := map[string][]spot.WhoSpotsMeCountryCount(nil)
	if admitted {
		wantWho = map[string][]spot.WhoSpotsMeCountryCount{"NA": {{ADIF: 291, Count: 1}}}
	}
	if got := f.who.CountryCountsByContinent("N2WQ", "20m", time.Now().UTC()); !reflect.DeepEqual(got, wantWho) {
		t.Errorf("accepted base WHOSPOTSME=%+v want %+v", got, wantWho)
	}
}

func (f familyDeliveryFixture) assertStored(t *testing.T, inputs []familyDeliveryInput) {
	t.Helper()
	f.assertRows(t, "sealed ring", f.ring.GetRecent(8), inputs, true)
	f.writer.Stop()
	familyDeliveryAssertArchiveBytes(t, f.archiveCfg.DBPath, inputs)
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(8)
	if err != nil {
		t.Fatal(err)
	}
	f.assertRows(t, "reopened archive", rows, inputs, false)
	if f.custom != nil {
		if err := f.custom.Close(); err != nil {
			t.Fatal(err)
		}
		customSCPFreshnessAssertBytes(t, f.customOpts.Path, familyDeliveryExpectedSCPBytes(inputs))
		store := customSCPFreshnessOpen(t, f.customOpts)
		want := 1
		for _, in := range inputs {
			if in.call == "N2WQ" && in.glyph == "V" {
				want = 2
			}
		}
		if !store.StaticContains("N2WQ/KP4") || !store.StaticContains("N2WQ") ||
			store.RecentSupportCountWithRequirements("N2WQ", "20m", "CW", spot.CustomSCPRequirements{MinScore: 5}, time.Now().UTC()) != want {
			t.Error("independently reopened CustomSCP lost admitted membership or receiver history")
		}
	}
}

func (f familyDeliveryFixture) assertRows(t *testing.T, destination string, rows []*spot.Spot, inputs []familyDeliveryInput, sealed bool) {
	t.Helper()
	want := make(map[string]familyDeliveryInput, len(inputs))
	for _, in := range inputs {
		want[in.call] = in
	}
	if len(rows) != len(want) {
		t.Errorf("%s count=%d want %d", destination, len(rows), len(want))
	}
	for _, row := range rows {
		if row == nil {
			t.Errorf("%s published a nil record", destination)
			continue
		}
		in, exists := want[row.DXCallNorm]
		if !exists || row.DXCallNorm != in.call || row.DECallNorm != in.receiver || row.Frequency != 14030 ||
			row.ModeNorm != "CW" || row.Confidence != in.glyph || row.Comment != in.comment || !row.Time.Equal(in.at) {
			t.Errorf("%s unexpected literal record: %+v want %+v", destination, row, in)
		}
		delete(want, row.DXCallNorm)
		if sealed && row.SnapshotForAsync() != row {
			t.Errorf("%s published an unsealed record", destination)
		}
	}
	if len(want) != 0 {
		t.Errorf("%s missing independently expected identities: %+v", destination, want)
	}
}

// Literal version-six values are assembled before decoding without borrowing
// emitted Spots or the archive encoder. Timestamp keys give a separate oracle.
func familyDeliveryAssertArchiveBytes(t *testing.T, path string, inputs []familyDeliveryInput) {
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
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("s|"), UpperBound: []byte("s}")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			t.Error(err)
		}
	}()
	want := make(map[int64][]byte, len(inputs))
	for _, in := range inputs {
		want[in.at.UnixNano()] = familyDeliveryExpectedArchiveBytes(in)
	}
	for iter.First(); iter.Valid(); iter.Next() {
		key, value := iter.Key(), iter.Value()
		if len(key) != 14 {
			t.Errorf("raw family archive key=%x has changed shape", key)
			continue
		}
		stamp := int64(binary.BigEndian.Uint64(key[2:10]))
		expected, exists := want[stamp]
		if !exists || !bytes.Equal(value, expected) {
			t.Errorf("raw family archive bytes before decoding: timestamp=%d got=%x want=%x", stamp, value, expected)
		}
		delete(want, stamp)
	}
	if err := iter.Error(); err != nil {
		t.Error(err)
	}
	if len(want) != 0 {
		t.Errorf("raw family archive missing independent timestamp records: %v", want)
	}
}

func familyDeliveryExpectedArchiveBytes(in familyDeliveryInput) []byte {
	fields := []string{in.call, in.receiver, in.receiver, "CW", in.comment, "MANUAL", "", in.glyph, "20m",
		"", "", "", "NA", "", "UNKNOWN", "", "", "", ""}
	raw := make([]byte, 74)
	raw[0], raw[1], raw[2] = 6, 3, 5 // report + human; unknown grids.
	binary.BigEndian.PutUint64(raw[4:12], math.Float64bits(14030))
	binary.BigEndian.PutUint32(raw[12:16], 10)
	binary.BigEndian.PutUint16(raw[18:20], 5)
	binary.BigEndian.PutUint32(raw[24:28], 291)
	binary.BigEndian.PutUint64(raw[28:36], math.Float64bits(14030))
	for i, field := range fields {
		binary.BigEndian.PutUint16(raw[36+i*2:38+i*2], uint16(len(field)))
	}
	for _, field := range fields {
		raw = append(raw, field...)
	}
	return raw
}

func familyDeliveryExpectedSCPBytes(inputs []familyDeliveryInput) map[string][]byte {
	want := make(map[string][]byte)
	for _, in := range inputs {
		if in.glyph != "V" {
			continue // Preserve production V-only admission for S/? releases.
		}
		calls := []string{in.call}
		if in.call == "KP4/N2WQ" || in.call == "N2WQ/KP4" {
			calls = []string{"N2WQ/KP4", "N2WQ"}
		}
		for _, call := range calls {
			membership := make([]byte, 8)
			binary.BigEndian.PutUint64(membership, uint64(in.at.Unix()))
			want["m|"+call] = membership
			observation := make([]byte, 10)
			binary.BigEndian.PutUint64(observation[:8], uint64(in.at.Unix()))
			// The input supplies no grid; the persisted cell must remain zero.
			want["o|"+call+"|20m|cw|"+in.receiver] = observation
		}
	}
	return want
}
