//go:build !qualification

package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/stats"
	"dxcluster/telnet"
)

func TestSelectiveRescueRefreshesClassificationAndGridCaches(t *testing.T) {
	requirePathReportH3Mappings(t)
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	cfg := selectiveLiveConfig()
	r, _, key := selectiveLivePopulation(t, cfg, "K1ABD", "DL9ZZZ", 2, at)
	archiveCfg := config.ArchiveConfig{DBPath: t.TempDir(), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1}
	writer, err := archive.NewWriter(archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	writer.Start()
	t.Cleanup(writer.Stop)
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), writer, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil))
	p.correctionCfg, p.signalResolver, p.recentBandStore, p.tracker = cfg, r, nil, stats.NewTracker()
	db := correctedGridCTY(t)
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	p.gridLookupSync = func(call string) (string, bool, bool) {
		if call == "K1ABD" {
			return "QF56", false, true
		}
		return "", false, false
	}
	var writes []string
	p.gridUpdate = func(call, _ string) { writes = append(writes, call) }
	selectiveLivePrecondition(t, p, key, "K1ABC", "K1ABD")
	s := correctedGridSource(at, false)
	s.DXMetadata.State = "stale original state"
	s.DEMetadata.Country, s.DEMetadata.ADIF, s.DEMetadata.Continent = "stale receiver", 999, "AS"
	s.InvalidateMetadataCache()
	hydrateSpotPathCells(s)
	if s.DXCellID == 0 || s.DECellID == 0 || s.DEMetadata.ADIF != 999 {
		t.Fatal("positive source caches must contain the old endpoint facts")
	}
	p.processSpot(s, nil)
	assertCorrectedGridOutput(t, p, writer, archiveCfg, correctedGridCase{want: "QF56", percent: 66}, at)
	rows := p.buf.GetRecent(2)
	out := rows[0]
	if out.DEMetadata.Country != "Alpha" || out.DEMetadata.ADIF != 230 || out.DEMetadata.Continent != "EU" ||
		out.DEMetadata.CQZone != 1 || out.DEMetadata.ITUZone != 2 || out.DEMetadata.State != "AA" ||
		out.DXMetadata.Continent != "NA" || out.DXContinentNorm != "NA" || out.DEContinentNorm != "EU" {
		t.Fatalf("C must refresh both endpoint classifications and normalized caches: %+v", out)
	}
	if p.tracker.CallCorrections() != 1 || p.tracker.CorrectionDecisionAppliedReasons()["resolver_selective_applied"] != 1 ||
		len(writes) != 1 || writes[0] != "N0AAA" {
		t.Fatalf("C must count once and never write its target grid as a new observation: writes=%v reasons=%v", writes, p.tracker.CorrectionDecisionReasons())
	}
}

func TestSelectiveRescueKeepsStoreSpecificAcceptedLearning(t *testing.T) {
	source := acceptedLearningPathsConfigSource(t)
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom_%t", custom), func(t *testing.T) {
			temporalDeliveryDirectory(t)
			f := newFamilyDeliveryFixture(t, source, custom, false, 60, 8, stabilizerTimeoutRelease, true)
			p := f.pipeline
			p.stabilizerEnabled = false
			p.telnetStabilizer.Stop()
			p.correctionCfg.TemporalDecoder.Enabled = false
			p.temporal = nil
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			control := acceptedLearningPathsControl(t, f, at)
			cfg := p.correctionCfg // Retain the loaded store's admission contract.
			cfg.Enabled, cfg.SelectiveRescueEnabled = true, true
			cfg.FrequencyToleranceHz, cfg.RecencySeconds, cfg.MaxEditDistance = 1000, 120, 3
			cfg.MinConsensusReports, cfg.MinAdvantage, cfg.MinConfidencePercent = 2, 1, 45
			cfg.DistanceModelCW, cfg.DistanceModelRTTY = "plain", "plain"
			cfg.ResolverRecentPlus1Enabled, cfg.BayesBonus.Enabled = false, false
			p.correctionCfg = cfg
			r, _, key := selectiveLivePopulation(t, cfg, "K1ABC", "DL9ZZZ", 2, at)
			p.signalResolver = r
			selectiveLivePrecondition(t, p, key, "K1ABD", "K1ABC")
			s := selectiveLiveSpot("K1ABD", at.Add(time.Second))
			s.DEMetadata.Continent, s.Report, s.HasReport = "NA", 10, true
			p.processSpot(s, nil)
			if s.DXCallNorm != "K1ABC" || s.Confidence != "C" || !s.ConfidencePercentOK || s.ConfidencePercent != 66 || p.tracker.CallCorrections() != 1 {
				t.Fatalf("learning witness must actually accept one C with ordinary public share: %+v", s)
			}
			acceptedLearningPathsWho(t, f, "K1ABD", 0)
			acceptedLearningPathsWho(t, f, "K1ABC", 1)
			wantSupport := 1
			if custom {
				wantSupport = 0 // C is not a V observation or static membership.
				if f.custom.StaticContains("K1ABC") || f.custom.StaticContains("K1ABD") {
					t.Fatal("C rescue taught V-only CustomSCP membership")
				}
			}
			if acceptedLearningPathsSupport(f, "K1ABC") != wantSupport || acceptedLearningPathsSupport(f, "K1ABD") != 0 ||
				acceptedLearningPathsSupport(f, control.call) != 1 {
				t.Fatal("accepted C must preserve legacy emitted-identity learning and V-only CustomSCP learning")
			}
			acceptedLearningPathsAssertCustomDisk(t, f, []familyDeliveryInput{control})
		})
	}
}
