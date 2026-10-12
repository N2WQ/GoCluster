//go:build !qualification

// File role: Proves that family hiding and actual bounded output-queue refusal
// cannot change accepted learning. Successful isolated controls qualify both
// receiver history and the per-occurrence WhoSpotsMe oracle before exclusions.
package cluster

import (
	"bufio"
	"fmt"
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
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"

	"gopkg.in/yaml.v3"
)

func acceptedLearningOutputSource(t *testing.T) string {
	t.Helper()
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func acceptedLearningOutputWho(t *testing.T, f familyDeliveryFixture, call string, want int) {
	t.Helper()
	var expected map[string][]spot.WhoSpotsMeCountryCount
	if want > 0 {
		expected = map[string][]spot.WhoSpotsMeCountryCount{"NA": {{ADIF: 291, Count: want}}}
	}
	if got := f.who.CountryCountsByContinent(call, "20m", time.Now().UTC()); !reflect.DeepEqual(got, expected) {
		t.Fatalf("accepted occurrence count for %s=%+v want %+v", call, got, expected)
	}
}

func acceptedLearningOutputSupport(f familyDeliveryFixture, call string) int {
	if f.custom != nil {
		return f.custom.RecentSupportCountWithRequirements(call, "20m", "CW", spot.CustomSCPRequirements{MinScore: 5}, time.Now().UTC())
	}
	return f.recent.RecentSupportCount(call, "20m", "CW", time.Now().UTC())
}

// Positive FIFO receipts establish delivery or hiding without a socket-silence
// oracle. SELF V inputs have no current percentage; DIAG must show that absence.
func acceptedLearningOutputReceipts(t *testing.T, f familyDeliveryFixture, normal, self, peers []familyDeliveryInput, barrier string) {
	t.Helper()
	for _, dst := range []struct {
		name   string
		conn   net.Conn
		reader *bufio.Reader
		want   []familyDeliveryInput
	}{{"normal", f.normalConn, f.normalReader, normal}, {"DIAG", f.diagConn, f.diagReader, normal}, {"SELF", f.selfConn, f.selfReader, self}} {
		var got []customSCPConsumerReceipt
		for i := 0; i < 6; i++ {
			line := correctionEvidenceReadDX(t, dst.conn, dst.reader)
			if len(line) < 78 {
				t.Fatalf("short %s learning receipt: %q", dst.name, line)
			}
			receipt := customSCPConsumerReceipt{strings.TrimSpace(line[27:39]), line[71:72]}
			got = append(got, receipt)
			comment := line[39:71]
			if dst.name == "DIAG" && !strings.Contains(comment, "--%") {
				t.Errorf("DIAG must retain unavailable current percentage for authoritative input: %q", line)
			}
			if dst.name != "DIAG" && strings.Contains(comment, "%") {
				t.Errorf("ordinary learning receipt acquired diagnostic percentage: %q", line)
			}
			if receipt.call == barrier {
				break
			}
		}
		want := make([]customSCPConsumerReceipt, 0, len(dst.want))
		for _, in := range dst.want {
			want = append(want, customSCPConsumerReceipt{in.call, in.glyph})
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s accepted learning FIFO=%+v want %+v", dst.name, got, want)
		}
	}
	f.assertPeerReceipts(t, peers, barrier)
}

func acceptedLearningOutputWitness(t *testing.T, s *telnetFamilySuppressor, call string) ([3]telnetFamilyWitness, bool) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var lanes [3]telnetFamilyWitness
	found := false
	for _, calls := range s.buckets {
		if entry := calls[spot.CorrectionVoteKey(call)]; entry != nil {
			if found {
				t.Fatal("family learning fixture unexpectedly spans multiple frequency buckets")
			}
			lanes, found = entry.lanes, true
		}
	}
	return lanes, found
}

// Expected disk bytes come from accepted inputs, including independently named
// slash/base keys. Output rows cannot hide accepted learning after thinning.
func acceptedLearningOutputCustomDisk(t *testing.T, f familyDeliveryFixture, accepted []familyDeliveryInput) {
	t.Helper()
	if f.custom == nil {
		return
	}
	var diskInputs []familyDeliveryInput
	for _, in := range accepted {
		diskInputs = append(diskInputs, in)
		if in.call == "N2WQ/KP4" {
			base := in
			base.call = "N2WQ"
			diskInputs = append(diskInputs, base)
		}
	}
	if err := f.custom.Close(); err != nil {
		t.Fatal(err)
	}
	customSCPFreshnessAssertBytes(t, f.customOpts.Path, familyDeliveryExpectedSCPBytes(diskInputs))
	reopened := customSCPFreshnessOpen(t, f.customOpts)
	wantReceivers := make(map[string]map[string]struct{})
	for _, in := range diskInputs {
		if in.glyph != "V" {
			continue
		}
		if wantReceivers[in.call] == nil {
			wantReceivers[in.call] = make(map[string]struct{})
		}
		wantReceivers[in.call][in.receiver] = struct{}{}
	}
	for _, in := range accepted {
		if want := in.glyph == "V"; reopened.StaticContains(in.call) != want {
			t.Fatalf("reopened static membership for %s does not preserve V-only admission", in.call)
		}
		got := reopened.RecentSupportCountWithRequirements(in.call, "20m", "CW", spot.CustomSCPRequirements{MinScore: 5}, time.Now().UTC())
		if want := len(wantReceivers[in.call]); got != want {
			t.Fatalf("reopened accepted receiver history for %s=%d want %d", in.call, got, want)
		}
	}
}

func TestAcceptedLearningOutputFamilyHidden(t *testing.T) {
	source := acceptedLearningOutputSource(t)
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom_%t", custom), func(t *testing.T) {
			temporalDeliveryDirectory(t)
			f := newFamilyDeliveryFixture(t, source, custom, false, 15, 8, stabilizerTimeoutRelease, true)
			at := time.Now().UTC().Truncate(time.Second).Add(-4 * time.Second)
			control := familyDeliveryInput{"K1CTL", "K1CTL", "learning control", "V", at}
			f.pipeline.processSpot(control.spot(), nil)
			acceptedLearningOutputWho(t, f, control.call, 1)
			if n := acceptedLearningOutputSupport(f, control.call); n != 1 {
				t.Fatalf("eligible isolated receiver control=%d want 1", n)
			}
			acceptedLearningOutputReceipts(t, f, []familyDeliveryInput{control}, []familyDeliveryInput{control}, []familyDeliveryInput{control}, control.call)

			strong := familyDeliveryInput{"N2WQ/KP4", "N2WQ/KP4", "strong learning seed", "V", at.Add(time.Second)}
			weak := familyDeliveryInput{"N2WQ", "N2WQ", "hidden learning input", "V", at.Add(2 * time.Second)}
			barrier := familyDeliveryInput{"K1BAR", "K1BAR", "learning FIFO barrier", "V", at.Add(3 * time.Second)}
			cfg := f.pipeline.correctionCfg
			f.pipeline.signalResolver = familyAdmissionResolver(t, cfg.FrequencyToleranceHz, at, strong.call, weak.call)
			key := spot.NewResolverSignalKey(14030, "20m", "CW", cfg.FrequencyToleranceHz)
			selected := correctionflow.SelectResolverPrimarySnapshotForCallAt(f.pipeline.signalResolver, key, cfg, weak.call, time.Now().UTC())
			if !selected.SnapshotOK || selected.NeighborhoodSplit || selected.Snapshot.EvidenceLoss ||
				selected.Snapshot.State != spot.ResolverStateConfident || selected.Snapshot.TotalReporters != 4 || selected.Snapshot.TotalWeightedSupportMilli != 4000 {
				t.Fatalf("family learning fixture requires healthy actual weighted 3/1 evidence: %+v", selected)
			}
			for _, check := range []struct {
				call string
				want int
			}{{strong.call, 75}, {weak.call, 25}} {
				if percent, ok := correctionflow.ResolverCallConfidencePercent(selected.Snapshot, check.call); !ok || percent != check.want {
					t.Fatalf("qualified fresh family share for %s=%d available=%t want %d", check.call, percent, ok, check.want)
				}
			}
			acceptedLearningOutputWho(t, f, weak.call, 0)
			f.pipeline.processSpot(strong.spot(), nil)
			acceptedLearningOutputReceipts(t, f, []familyDeliveryInput{strong}, []familyDeliveryInput{strong}, []familyDeliveryInput{strong}, strong.call)
			before, found := acceptedLearningOutputWitness(t, f.pipeline.familySuppressor, strong.call)
			if !found {
				t.Fatal("successful strong broadcast did not commit its family witness")
			}
			for _, lane := range before {
				if lane.call != strong.call || lane.at.IsZero() || lane.freqKHz != 14030 {
					t.Fatal("strong offer must positively occupy each actual admitted lane")
				}
			}
			f.pipeline.processSpot(weak.spot(), nil)
			after, found := acceptedLearningOutputWitness(t, f.pipeline.familySuppressor, strong.call)
			if !found || before != after {
				t.Fatal("hidden accepted input changed the successful strong lane witness")
			}
			if _, found := acceptedLearningOutputWitness(t, f.pipeline.familySuppressor, weak.call); found {
				t.Fatal("hidden accepted input committed an ordinary family offer")
			}
			acceptedLearningOutputWho(t, f, strong.call, 1)
			acceptedLearningOutputWho(t, f, weak.call, 1)
			if n := acceptedLearningOutputSupport(f, weak.call); n != 2 {
				t.Fatalf("accepted hidden base must learn its receiver alongside the strong base receiver: %d want 2", n)
			}
			f.pipeline.processSpot(barrier.spot(), nil)
			acceptedLearningOutputReceipts(t, f, []familyDeliveryInput{barrier}, []familyDeliveryInput{barrier}, []familyDeliveryInput{weak, barrier}, barrier.call)
			if queue, clients, senders := f.pipeline.telnet.BroadcastMetricSnapshot(); queue != 0 || clients != 0 || senders != 0 {
				t.Fatalf("family hiding proof acquired unrelated queue/client/sender drops: %d/%d/%d", queue, clients, senders)
			}
			if f.pipeline.tracker.CallCorrections() != 0 || f.pipeline.tracker.StabilizerHeld() != 0 {
				t.Fatal("authoritative accepted family identities changed or acquired a hold")
			}
			accepted := []familyDeliveryInput{control, strong, weak, barrier}
			f.assertRows(t, "accepted family ring", f.ring.GetRecent(8), accepted, true)
			f.writer.Stop()
			familyDeliveryAssertArchiveBytes(t, f.archiveCfg.DBPath, accepted)
			reopened, err := archive.NewWriter(f.archiveCfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reopened.Stop)
			rows, err := reopened.Recent(8)
			if err != nil {
				t.Fatal(err)
			}
			f.assertRows(t, "reopened accepted family archive", rows, accepted, false)
			acceptedLearningOutputCustomDisk(t, f, accepted)
		})
	}
}

// The complete private YAML chooses the tested acceptance route before loading.
// Repository values remain untouched; the real constructor sees bonus-enabled,
// non-stabilized owners rather than a manually assembled pipeline.
func acceptedLearningOutputQueueConfig(t *testing.T, source string, custom bool) *config.Config {
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
				t.Fatal("complete fixture lacks call_correction")
			}
			correction["stabilizer_enabled"], correction["recent_band_bonus_enabled"] = false, true
			scp, ok := correction["custom_scp"].(map[string]any)
			if !ok {
				t.Fatal("complete fixture lacks custom_scp")
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
	cfg, err := config.Load(directory)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func acceptedLearningOutputQueueFixture(t *testing.T, source string, custom bool) familyDeliveryFixture {
	t.Helper()
	cfg := acceptedLearningOutputQueueConfig(t, source, custom)
	f := familyDeliveryFixture{ambiguityDeliveryFixture: ambiguityDeliveryFixture{
		ring: buffer.NewRingBuffer(8), archiveCfg: config.ArchiveConfig{
			DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 1, BatchSize: 1, BatchIntervalMS: 1,
		},
	}, who: spot.NewWhoSpotsMeStore(time.Duration(cfg.WhoSpotsMe.WindowMinutes) * time.Minute)}
	var err error
	f.writer, err = archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
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
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 1}, nil)
	t.Cleanup(srv.Stop)
	f.pipeline = newOutputPipeline(dedup.NewDeduplicator(0, false, 8), nil, nil, nil, nil, nil,
		f.ring, srv, nil, stats.NewTracker(), nil, cfg.CallCorrection, func() *cty.CTYDatabase { return nil },
		nil, nil, cfg.Harmonics, nil, cfg.SpotPolicy, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		f.recent, f.custom, f.who, false, f.writer, nil, nil, nil, nil, nil)
	if !cfg.CallCorrection.RecentBandBonusEnabled || cfg.CallCorrection.StabilizerEnabled || f.pipeline.stabilizerEnabled ||
		f.pipeline.telnetStabilizer != nil || f.pipeline.familySuppressor == nil {
		t.Fatal("loaded queue fixture did not construct its non-stabilized accepted learning owners")
	}
	// Neither output owner starts: after the control drain, one real filler
	// reserves the whole queue and no consumer can disguise a refusal.
	return f
}

func acceptedLearningOutputQueued(t *testing.T, f familyDeliveryFixture, broadcast, archived familyDeliveryInput) {
	t.Helper()
	got, ok := tryReadTelnetBroadcastSpot(f.pipeline.telnet)
	if !ok || got.DXCallNorm != broadcast.call || got.DECallNorm != broadcast.receiver || got.Confidence != broadcast.glyph || !got.Time.Equal(broadcast.at) {
		t.Fatalf("actual bounded broadcast queue changed its admitted tuple: got=%+v available=%t want=%+v", got, ok, broadcast)
	}
	if _, ok := tryReadTelnetBroadcastSpot(f.pipeline.telnet); ok {
		t.Fatal("refused input entered an extra broadcast slot")
	}
	got, ok = tryReadArchiveQueuedSpot(f.writer)
	if !ok || got.DXCallNorm != archived.call || got.DECallNorm != archived.receiver || got.Confidence != archived.glyph || !got.Time.Equal(archived.at) {
		t.Fatalf("actual bounded archive queue changed its admitted tuple: got=%+v available=%t want=%+v", got, ok, archived)
	}
	if _, ok := tryReadArchiveQueuedSpot(f.writer); ok {
		t.Fatal("refused input entered an extra archive slot")
	}
}

func TestAcceptedLearningOutputQueueRefusal(t *testing.T) {
	source := acceptedLearningOutputSource(t)
	for _, custom := range []bool{false, true} {
		for _, destination := range []string{"broadcast", "archive"} {
			for _, glyph := range []string{"V", "?"} {
				t.Run(fmt.Sprintf("custom_%t/%s/glyph_%s", custom, destination, glyph), func(t *testing.T) {
					temporalDeliveryDirectory(t)
					f := acceptedLearningOutputQueueFixture(t, source, custom)
					at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
					control := familyDeliveryInput{"K1CTL", "K1CTL", "queue learning control", "V", at}
					f.pipeline.processSpot(control.spot(), nil)
					acceptedLearningOutputWho(t, f, control.call, 1)
					if n := acceptedLearningOutputSupport(f, control.call); n != 1 {
						t.Fatalf("successful isolated queue control learned %d receivers want 1", n)
					}
					acceptedLearningOutputQueued(t, f, control, control)
					if queue, clients, senders := f.pipeline.telnet.BroadcastMetricSnapshot(); queue != 0 || clients != 0 || senders != 0 {
						t.Fatal("positive queue control did not qualify successful output admission")
					}
					incoming := familyDeliveryInput{"K1REF", "K1REF", "accepted refused output", glyph, at.Add(time.Second)}
					if glyph != "V" {
						incoming.receiver = "N0AAA" // Natural unresolved identity, preserving CustomSCP's refusal.
					}
					acceptedLearningOutputWho(t, f, incoming.call, 0)
					filler := familyDeliveryInput{"K1FILL", "K1FILL", "bounded queue filler", "V", at}
					fillSpot := filler.spot()
					fillSpot.Confidence = "V"
					fillSpot = fillSpot.SealForAsync()
					if destination == "broadcast" {
						if !f.pipeline.telnet.TryBroadcastSpotOwned(fillSpot, true, true, true) {
							t.Fatal("real one-slot broadcast prefill was refused")
						}
					} else {
						f.writer.EnqueueOwned(fillSpot)
					}
					f.pipeline.processSpot(incoming.spot(), nil)
					acceptedLearningOutputWho(t, f, incoming.call, 1)
					acceptedLearningOutputWho(t, f, control.call, 1)
					wantSupport := 1
					if custom && glyph != "V" {
						wantSupport = 0
					}
					if n := acceptedLearningOutputSupport(f, incoming.call); n != wantSupport {
						t.Fatalf("accepted refused input learned %d receivers want %d", n, wantSupport)
					}
					wantBroadcast, wantArchive := incoming, incoming
					wantDrops := uint64(0)
					if destination == "broadcast" {
						wantBroadcast, wantDrops = filler, 1
						if _, found := acceptedLearningOutputWitness(t, f.pipeline.familySuppressor, incoming.call); found {
							t.Fatal("refused broadcast taught ordinary family offer history")
						}
					} else {
						wantArchive = filler
					}
					if queue, clients, senders := f.pipeline.telnet.BroadcastMetricSnapshot(); queue != wantDrops || clients != 0 || senders != 0 {
						t.Fatalf("actual refusal drop accounting=%d/%d/%d want %d/0/0", queue, clients, senders, wantDrops)
					}
					acceptedLearningOutputQueued(t, f, wantBroadcast, wantArchive)
					accepted := []familyDeliveryInput{control, incoming}
					f.assertRows(t, "accepted ring despite queue refusal", f.ring.GetRecent(8), accepted, true)
					if f.pipeline.tracker.CallCorrections() != 0 || f.pipeline.tracker.StabilizerHeld() != 0 {
						t.Fatal("queue refusal changed core identity or created delayed ownership")
					}
					acceptedLearningOutputCustomDisk(t, f, accepted)
				})
			}
		}
	}
}
