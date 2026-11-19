//go:build windows && !qualification

package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

type ccQualReader struct {
	conn    net.Conn
	done    chan struct{}
	barrier chan struct{}
	errors  chan error
	closing atomic.Bool
	count   atomic.Uint64
}

type ccQualRuntime struct {
	input          chan *spot.Spot
	pipeline       *outputPipeline
	resolver       *spot.SignalResolver
	server         *telnet.Server
	writer         *archive.Writer
	archiveCfg     config.ArchiveConfig
	custom         *spot.CustomSCPStore
	legacy         *spot.RecentBandStore
	refresher      *adaptiveRefresher
	ring           *buffer.RingBuffer
	tracker        *stats.Tracker
	fanout         atomic.Uint64
	mainBarrier    chan struct{}
	mainOnce       sync.Once
	readers        []*ccQualReader
	inputOnce      sync.Once
	stopOnce       sync.Once
	stopDone       chan struct{}
	stopErrors     []error // One shutdown owner; read only after stopDone.
	resolverJoined atomic.Bool
	sampleStop     chan struct{}
	sampleDone     chan struct{}
	sampleOnce     sync.Once
	sampled        ccQualSampled // Single sampler owner; read only after sampleDone.
}

func ccQualRun(t *testing.T, opts ccQualOptions, assets *ccQualAssets, result *ccQualResult) {
	t.Helper()
	inputs, description := ccQualWorkload(t, assets, result.Name)
	result.WorkloadTemplates, result.WorkloadDescription = len(inputs), description
	result.WorkloadSHA256 = ccQualHashJSON(t, struct {
		Version string
		Seed    uint64
		Inputs  []ccQualInput
	}{"paced_v1", ccQualSeed, inputs})
	rate := opts.TypicalRate
	switch result.Name {
	case "dense_canonical":
		rate = opts.DenseRate
	case "drain_recovery":
		rate = opts.DrainRate
	}
	warmCount := uint64(opts.Warmup) * uint64(rate) / uint64(time.Second)
	steadyCount := uint64(opts.Duration) * uint64(rate) / uint64(time.Second)
	if warmCount+steadyCount > ccQualMaxOffered || warmCount+steadyCount+1 > uint64(assets.cfg.Buffer.Capacity) {
		t.Fatal("invalid qualification environment: per-case offered count must be <=100000 and fit configured ring plus one barrier without wrapping")
	}
	if warmCount == 0 || steadyCount == 0 {
		t.Fatal("invalid qualification environment: warmup and steady windows must each offer at least one input")
	}
	result.WarmupCoversWorkload = warmCount >= uint64(len(inputs))
	if !result.WarmupCoversWorkload {
		t.Fatal("invalid qualification warm state: warmup must cover one complete frozen workload cycle")
	}
	ccQualValidateInputs(t, assets, inputs)
	if result.Name == "dense_canonical" {
		ccQualValidateDense(t, assets, inputs)
	}
	// These are the actual configured distance costs. This opt-in test must run
	// alone; cleanup restores the package's documented initialized defaults.
	cw, rtty := assets.cfg.CallCorrection.MorseWeights, assets.cfg.CallCorrection.BaudotWeights
	spot.ConfigureMorseWeights(cw.Insert, cw.Delete, cw.Sub, cw.Scale)
	spot.ConfigureBaudotWeights(rtty.Insert, rtty.Delete, rtty.Sub, rtty.Scale)
	t.Cleanup(func() {
		spot.ConfigureMorseWeights(1, 1, 2, 2)
		spot.ConfigureBaudotWeights(1, 1, 2, 2)
	})
	temporalDeliveryDirectory(t)
	f := ccQualStart(t, opts, assets)
	result.Warmup = ccQualProduce(t, f, assets, inputs, 0, warmCount, rate)
	result.Steady = ccQualProduce(t, f, assets, inputs, warmCount, steadyCount, rate)
	result.BeforeRecovery = f.snapshot()
	ccQualRecover(t, opts, f, result)
	if !result.ShutdownJoined {
		return
	}
	result.Sampled = f.sampled
	result.ArchiveExpected = uint64(f.ring.GetCount())
	result.ArchiveRows = ccQualCountArchive(t, f.archiveCfg, opts.DrainTimeout)
	if result.ArchiveRows < result.ArchiveExpected {
		result.ArchiveMissing = result.ArchiveExpected - result.ArchiveRows
	}
	if result.ArchiveRows != result.ArchiveExpected {
		ccQualError(t, result, "archive persistence loss: observed handoffs=%d reopened rows=%d", result.ArchiveExpected, result.ArchiveRows)
	}
	end := result.AfterRecovery
	accepted := result.Warmup.Accepted + result.Steady.Accepted
	if end.Resolver.Accepted+end.Resolver.DropQueueFull != accepted || end.Resolver.Processed != end.Resolver.Accepted {
		ccQualError(t, result, "raw/resolver accounting mismatch: raw accepted=%d resolver accepted=%d refused=%d processed=%d", accepted, end.Resolver.Accepted, end.Resolver.DropQueueFull, end.Resolver.Processed)
	}
	if result.Warmup.Refused+result.Steady.Refused != 0 || end.Resolver.DropQueueFull != 0 || end.QueueDrops+end.ClientDrops+end.SenderFailures != 0 {
		ccQualError(t, result, "capacity loss observed: input refused=%d resolver refused=%d telnet queue/client/sender=%d/%d/%d", result.Warmup.Refused+result.Steady.Refused, end.Resolver.DropQueueFull, end.QueueDrops, end.ClientDrops, end.SenderFailures)
	}
	if end.Resolver.DropMaxKeys+end.Resolver.DropMaxCandidates+end.Resolver.DropMaxReporters+end.Resolver.DropReliability+end.Resolver.CapLossDiscards != 0 {
		ccQualError(t, result, "resolver retained-evidence refusal/loss observed: keys=%d candidates=%d reporters=%d reliability=%d discarded=%d", end.Resolver.DropMaxKeys, end.Resolver.DropMaxCandidates, end.Resolver.DropMaxReporters, end.Resolver.DropReliability, end.Resolver.CapLossDiscards)
	}
}

func ccQualValidateInputs(t *testing.T, a *ccQualAssets, inputs []ccQualInput) {
	t.Helper()
	for _, input := range inputs {
		info, ok := a.cty.LookupCallsignPortable(input.Call)
		if !spot.IsValidCallsign(input.Call) || !ok || info == nil || info.ADIF == 291 || info.ADIF == 1 {
			t.Fatalf("qualification requires valid non-US/non-Canadian DX calls, rejected %q", input.Call)
		}
		s := ccQualSpot(a, input, 0)
		if _, ok := buildResolverEvidenceSnapshot(s, a.cfg.CallCorrection, nil, time.Now().UTC()); !ok || s.IsTestSpotter || s.IsBeacon {
			t.Fatalf("qualification input is not ordinary resolver evidence: %+v", input)
		}
	}
}

// Start uses the production constructor, output loop, resolver Start, and real
// downstream owners. Only the post-primary input channel and storage paths are
// fixture-owned. A zero unstarted Deduplicator supplies the constructor's input
// accessor; no dedup worker or serial resolver driver participates in this test.
func ccQualStart(t *testing.T, opts ccQualOptions, a *ccQualAssets) *ccQualRuntime {
	t.Helper()
	f := &ccQualRuntime{mainBarrier: make(chan struct{}), stopDone: make(chan struct{}),
		sampleStop: make(chan struct{})}
	t.Cleanup(func() { f.shutdown(t, opts.DrainTimeout) })
	cfg := *a.cfg
	cfg.CallCorrection.CustomSCP.Path = filepath.Join(t.TempDir(), "scp")
	f.archiveCfg = cfg.Archive
	f.archiveCfg.DBPath = filepath.Join(t.TempDir(), "archive")
	var err error
	f.writer, err = archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.writer.Start()
	recent := ccQualOpenRecent(t, f, cfg.CallCorrection)
	f.resolver = spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: shadowResolverQueueSize, MaxActiveKeys: shadowResolverMaxActiveKeys,
		MaxCandidatesPerKey: shadowResolverMaxCandidatesPerKey, MaxReportersPerCand: shadowResolverMaxReportersPerCand,
		InactiveTTL: shadowResolverInactiveTTL, EvalMinInterval: shadowResolverEvalMinInterval,
		SweepInterval: shadowResolverSweepInterval, HysteresisWindows: shadowResolverHysteresisWindows,
		FreqGuardMinSeparationKHz: cfg.CallCorrection.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:    cfg.CallCorrection.FreqGuardRunnerUpRatio,
		MaxEditDistance:           cfg.CallCorrection.MaxEditDistance, DistanceModelCW: cfg.CallCorrection.DistanceModelCW,
		DistanceModelRTTY: cfg.CallCorrection.DistanceModelRTTY, SpotterReliability: a.global,
		SpotterReliabilityCW: a.cw, SpotterReliabilityRTTY: a.rtty, MinSpotterReliability: cfg.CallCorrection.MinSpotterReliability,
		ConfusionModel: a.model, ConfusionWeight: cfg.CallCorrection.ConfusionModelWeight,
		FamilyPolicy: spot.CorrectionFamilyPolicy{Configured: true,
			TruncationEnabled:          cfg.CallCorrection.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch},
	})
	f.resolver.Start()
	adaptive := spot.NewAdaptiveMinReports(cfg.CallCorrection)
	f.refresher = newAdaptiveRefresher(adaptive, cfg.CallCorrection.AdaptiveRefreshByBand, noopRefresh)
	if f.refresher != nil {
		f.refresher.Start()
	}
	port := runtimeQualificationPort(t)
	f.server = telnet.NewServer(telnet.ServerOptions{
		Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n", MaxConnections: cfg.Telnet.MaxConnections,
		MaxSessionsPerBaseCall: cfg.Telnet.MaxSessionsPerBaseCall, MaxSessionsPerIP: cfg.Telnet.MaxSessionsPerIP,
		BroadcastWorkers: cfg.Telnet.BroadcastWorkers, BroadcastQueue: cfg.Telnet.BroadcastQueue,
		WorkerQueue: cfg.Telnet.WorkerQueue, ClientBuffer: cfg.Telnet.ClientBuffer, ControlQueue: cfg.Telnet.ControlQueueSize,
		BroadcastBatchInterval: time.Duration(cfg.Telnet.BroadcastBatchIntervalMS) * time.Millisecond, BroadcastBatchIntervalSet: true,
		WriterBatchMaxBytes: cfg.Telnet.WriterBatchMaxBytes, WriterBatchWait: time.Duration(cfg.Telnet.WriterBatchWaitMS) * time.Millisecond,
		Transport: cfg.Telnet.Transport, EchoMode: cfg.Telnet.EchoMode, DefaultDedupePolicy: cfg.Dedup.DefaultPolicy,
		CTYLookup: func() *cty.CTYDatabase { return a.cty },
	}, nil)
	if err := f.server.Start(); err != nil {
		t.Fatal(err)
	}
	for _, client := range []struct{ call, diag string }{{"DL1QUAL", "OFF"}, {"DL2QUAL", "CONF"}} {
		conn, reader := correctionEvidenceLogin(t, port, client.call, client.diag)
		if err := conn.SetDeadline(time.Time{}); err != nil {
			t.Fatal(err)
		}
		f.readers = append(f.readers, ccQualStartReader(t, conn, reader, opts.Warmup+opts.Duration+3*opts.DrainTimeout))
	}
	f.ring, f.tracker = buffer.NewRingBuffer(cfg.Buffer.Capacity), stats.NewTracker()
	f.input = make(chan *spot.Spot, cfg.Dedup.OutputBufferSize)
	f.pipeline = newOutputPipeline(&dedup.Deduplicator{}, nil, nil, nil, &f.fanout, nil, f.ring, f.server, nil,
		f.tracker, f.resolver, cfg.CallCorrection, func() *cty.CTYDatabase { return a.cty },
		newCallMetaCache(cfg.GridCacheSize, time.Duration(cfg.GridCacheTTLSec)*time.Second), nil, cfg.Harmonics, nil,
		cfg.SpotPolicy, nil, nil, nil, nil, nil, nil, adaptive, f.refresher, a.global, a.cw, a.rtty, a.model,
		recent, f.custom, nil, cfg.RBN.KeepSSIDSuffix, f.writer, nil, nil, nil, nil, nil)
	f.pipeline.outputChan = f.input
	f.pipeline.gridUpdate = func(call, _ string) {
		if call == ccQualMainCall {
			f.mainOnce.Do(func() { close(f.mainBarrier) })
		}
	}
	go f.pipeline.run()
	f.startSampler(opts.SampleEvery)
	return f
}

func ccQualOpenRecent(t *testing.T, f *ccQualRuntime, cfg config.CallCorrectionConfig) spot.RecentSupportStore {
	t.Helper()
	if cfg.CustomSCP.Enabled {
		if err := cfg.ValidateCustomSCPCapacity(); err != nil {
			t.Fatal(err)
		}
		c := cfg.CustomSCP
		var err error
		f.custom, err = spot.OpenCustomSCPStore(spot.CustomSCPOptions{
			Path: c.Path, HorizonDays: c.HistoryHorizonDays, StaticHorizonDays: c.StaticHorizonDays,
			MaxKeys: c.MaxKeys, MaxSpottersPerKey: c.MaxSpottersPerKey, CleanupInterval: time.Duration(c.CleanupIntervalSeconds) * time.Second,
			CacheSizeBytes: int64(c.BlockCacheMB) << 20, BloomFilterBitsPerKey: c.BloomFilterBits,
			MemTableSizeBytes: uint64(c.MemTableSizeMB) << 20, L0CompactionThreshold: c.L0CompactionThreshold,
			L0StopWritesThreshold: c.L0StopWritesThreshold, CoreMinScore: c.ResolverMinScore, CoreMinH3Cells: c.ResolverMinUniqueH3Cells,
			SFloorMinScore: c.SFloorMinScore, SFloorExactMinH3Cells: c.SFloorMinUniqueH3CellsExact,
			SFloorFamilyMinH3Cells: c.SFloorMinUniqueH3CellsFamily, MinSNRDBCW: c.MinSNRDBCW, MinSNRDBRTTY: c.MinSNRDBRTTY,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.custom.StartCleanup()
		return f.custom
	}
	if cfg.RecentBandBonusEnabled || cfg.StabilizerEnabled {
		f.legacy = spot.NewRecentBandStore(time.Duration(cfg.RecentBandWindowSeconds) * time.Second)
		f.legacy.StartCleanup()
		return f.legacy
	}
	return nil
}

func ccQualSpot(a *ccQualAssets, input ccQualInput, sequence uint64) *spot.Spot {
	s := spot.NewSpot(input.Call, input.Receiver, input.Frequency, input.Mode)
	s.ID, s.SourceType, s.IsHuman = sequence+1, spot.SourceRBN, false
	s.SourceNode, s.SpotterIP, s.Comment = "CCV2QUAL", "192.0.2.1", " "
	s.Report, s.HasReport = input.Report, true
	if info, ok := a.cty.LookupCallsignPortable(input.Call); ok {
		s.DXMetadata = metadataFromPrefix(info)
	}
	if info, ok := a.cty.LookupCallsignPortable(input.Receiver); ok {
		s.DEMetadata = metadataFromPrefix(info)
	}
	s.EnsureNormalized()
	return s
}

// Absolute slots keep offers paced. An overrun starts the next slot one interval
// later instead of catching up with an unbounded burst; every frozen raw ID is
// still offered exactly once, and actual elapsed time/lateness are recorded.
func ccQualProduce(t *testing.T, f *ccQualRuntime, a *ccQualAssets, inputs []ccQualInput, offset, count uint64, rate int) ccQualPhase {
	t.Helper()
	phase := ccQualPhase{Start: f.snapshot()}
	cpuStart, started := ccQualProcessCPU(t), time.Now()
	interval := time.Second / time.Duration(rate)
	next := started.Add(interval)
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for i := uint64(0); i < count; i++ {
		select {
		case <-t.Context().Done():
			t.Fatal("paced producer canceled")
		case <-timer.C:
		}
		now := time.Now()
		if lateness := now.Sub(next); lateness >= interval {
			phase.LateSlots++
			phase.MaxLatenessNS = max(phase.MaxLatenessNS, lateness.Nanoseconds())
		}
		sequence := offset + i
		s := ccQualSpot(a, inputs[(sequence+ccQualSeed)%uint64(len(inputs))], sequence)
		phase.Offered++
		select {
		case f.input <- s:
			phase.Accepted++
		default:
			phase.Refused++
		}
		next = next.Add(interval)
		if !time.Now().Before(next) {
			next = time.Now().Add(interval)
		}
		timer.Reset(time.Until(next))
	}
	phase.ElapsedNS = time.Since(started).Nanoseconds()
	phase.CPUNS = ccQualProcessCPU(t) - cpuStart
	if phase.Accepted > 0 {
		phase.CPUPerAccepted = float64(phase.CPUNS) / float64(phase.Accepted)
	}
	phase.End = f.snapshot()
	return phase
}

func (f *ccQualRuntime) snapshot() ccQualSnapshot {
	queueDrops, clientDrops, senderFailures := f.server.BroadcastMetricSnapshot()
	return ccQualSnapshot{OutputDepth: len(f.input), Resolver: f.resolver.MetricsSnapshot(),
		Finalized: f.tracker.GetTotal(), FanoutEntries: f.fanout.Load(), RingOutputs: f.ring.GetCount(),
		TemporalPending: f.tracker.TemporalPending(), StabilizerPending: f.pipeline.telnetStabilizer.Pending(),
		StabilizerHeld: f.tracker.StabilizerHeld(), StabilizerDelayed: f.tracker.StabilizerReleasedDelayed(),
		Suppressed: f.tracker.StabilizerSuppressedTimeout(), AgeDropped: f.tracker.StabilizerAgeDropped(),
		QueueDrops: queueDrops, ClientDrops: clientDrops, SenderFailures: senderFailures}
}

func (f *ccQualRuntime) startSampler(every time.Duration) {
	f.sampled = ccQualSampled{Kind: "SAMPLED", EveryNS: every.Nanoseconds()}
	f.sampleDone = make(chan struct{})
	go func() {
		defer close(f.sampleDone)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-f.sampleStop:
				return
			case <-ticker.C:
				snapshot := f.snapshot()
				f.sampled.Samples++
				f.sampled.OutputQueue = max(f.sampled.OutputQueue, snapshot.OutputDepth)
				f.sampled.ResolverQueue = max(f.sampled.ResolverQueue, snapshot.Resolver.QueueDepth)
				f.sampled.TemporalPending = max(f.sampled.TemporalPending, snapshot.TemporalPending)
				f.sampled.StabilizerPending = max(f.sampled.StabilizerPending, snapshot.StabilizerPending)
				f.sampled.ActiveKeys = max(f.sampled.ActiveKeys, snapshot.Resolver.ActiveKeys)
			}
		}
	}()
}

func ccQualStartReader(t *testing.T, conn net.Conn, reader *bufio.Reader, timeout time.Duration) *ccQualReader {
	t.Helper()
	r := &ccQualReader{conn: conn, done: make(chan struct{}), barrier: make(chan struct{}), errors: make(chan error, 1)}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(r.done)
		for {
			line, err := reader.ReadSlice('\n') // Existing reader has a finite 65538-byte buffer.
			if err != nil {
				if !r.closing.Load() {
					r.errors <- err
				}
				return
			}
			if !bytes.Contains(line, []byte("DX de ")) {
				continue
			}
			if bytes.Contains(line, []byte(ccQualWireCall)) {
				close(r.barrier)
				return
			}
			if !bytes.Contains(line, []byte(ccQualMainCall)) {
				r.count.Add(1)
			}
		}
	}()
	return r
}

func ccQualHasBacklog(s ccQualSnapshot) bool {
	return s.OutputDepth != 0 || s.Resolver.QueueDepth != 0 || s.Resolver.Processed != s.Resolver.Accepted || s.TemporalPending != 0 || s.StabilizerPending != 0
}

func ccQualError(t *testing.T, result *ccQualResult, format string, args ...any) {
	t.Helper()
	message := fmt.Sprintf(format, args...)
	result.Errors = append(result.Errors, message)
	t.Error(message)
}
