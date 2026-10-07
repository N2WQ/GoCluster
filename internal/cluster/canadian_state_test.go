// File role: Proves Canadian licensing and province facts through central role
// gates, resolver delivery, stored history and the runtime's refresh ownership.
package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
	"dxcluster/telnet"
	"dxcluster/uls"
)

func canadianClusterDatabase(t testing.TB, changed bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ised.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// Literal schema isolates consumers from the parser and its expected values.
	_, err = db.ExecContext(t.Context(), `CREATE TABLE CA(call_sign TEXT PRIMARY KEY,state TEXT NOT NULL);
		CREATE TABLE Events(id INTEGER PRIMARY KEY,special TEXT NOT NULL,start_day TEXT NOT NULL,end_day TEXT NOT NULL,
			trustee TEXT NOT NULL,use_by TEXT NOT NULL,uncertain INTEGER NOT NULL,prefix_kind INTEGER NOT NULL);
		CREATE INDEX idx_Events_special ON Events(special);
		CREATE TABLE SourceMeta(id INTEGER PRIMARY KEY,main_sha TEXT NOT NULL,special_sha TEXT NOT NULL);
		INSERT INTO SourceMeta VALUES(1,'0000000000000000000000000000000000000000000000000000000000000000',
			'1111111111111111111111111111111111111111111111111111111111111111');
		INSERT INTO CA VALUES('VE3ABC','ON'),('VE3ABD','BC'),('VE2AAA','QC'),
			('CY0ABC','NS'),('CY0AAA','NL'),('CY9ABC','NS'),('CY9AAA','ON');
		PRAGMA user_version=1;`)
	if err == nil && changed {
		_, err = db.ExecContext(t.Context(), "UPDATE CA SET state='SK' WHERE call_sign='VE3ABD'; UPDATE CA SET state='NB' WHERE call_sign='VE2AAA';")
	}
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("Canadian cluster fixture: %v %v", err, closeErr)
	}
	return path
}

func configureCanadianClusterFixture(t testing.TB, enabled bool) string {
	t.Helper()
	path := canadianClusterDatabase(t, false)
	previous := uls.CanadianLicenseChecksEnabled()
	uls.SetCanadianRefreshInProgress(false)
	uls.SetCanadianLicenseDBPath(path)
	uls.SetCanadianLicenseChecksEnabled(enabled)
	t.Cleanup(func() {
		uls.SetCanadianRefreshInProgress(false)
		uls.SetCanadianLicenseDBPath("")
		uls.SetCanadianLicenseChecksEnabled(previous)
	})
	return path
}

func canadianClusterCTY(t testing.TB) *cty.CTYDatabase {
	t.Helper()
	var data strings.Builder
	data.WriteString("<plist><dict>")
	for _, entity := range []struct {
		prefix string
		adif   int
	}{{"VE3", 1}, {"VE2", 1}, {"CY0", 211}, {"CY9", 252}, {"K1", 291}, {"N2", 291}, {"DL1", 230}} {
		fmt.Fprintf(&data, `<key>%s</key><dict><key>Country</key><string>Fixture</string><key>Prefix</key><string>%s</string><key>ADIF</key><integer>%d</integer><key>Continent</key><string>NA</string><key>CQZone</key><integer>5</integer></dict>`, entity.prefix, entity.prefix, entity.adif)
	}
	data.WriteString("</dict></plist>")
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(data.String()))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestCanadianRoleJurisdictionAndPortableRouting(t *testing.T) {
	configureCanadianClusterFixture(t, true)
	configureFCCStateFixture(t, true)
	db := canadianClusterCTY(t)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	for _, test := range []struct {
		dx, de           string
		dxADIF, deADIF   int
		dxState, deState string
	}{
		{"VE3ABC", "VE2AAA", 1, 1, "ON", "QC"},
		{"CY0ABC", "CY0AAA", 211, 211, "NS", "NL"},
		{"CY9ABC", "CY9AAA", 252, 252, "NS", "ON"},
		{"K1/VE3ABC", "VE3/K1ABC", 291, 1, "ON", "CA"},
		{"VE3/K1ABC", "K1/VE3ABC", 1, 291, "CA", "ON"},
		{"VE3ABC/K1", "K1ABC/VE3", 291, 1, "ON", "CA"},
	} {
		t.Run(test.dx+"/"+test.de, func(t *testing.T) {
			s := spot.NewSpot(test.dx, test.de, 14020, "CW")
			s.DEMetadata.Grid, s.DEMetadata.GridDerived = "FN31", true
			s.DXMetadata.State, s.DEMetadata.State = "TX", "TX"
			if !v.validateSpot(s) || s.DXMetadata.State != "" || s.DEMetadata.State != test.deState {
				t.Fatalf("central DE gate: DX=%+v DE=%+v", s.DXMetadata, s.DEMetadata)
			}
			s.Confidence = "C"
			if applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != test.dxState || s.DEMetadata.State != test.deState ||
				s.DXMetadata.ADIF != test.dxADIF || s.DEMetadata.ADIF != test.deADIF || s.DEMetadata.Grid != "FN31" || !s.DEMetadata.GridDerived {
				t.Fatalf("final gate mixed base/location identity: DX=%+v DE=%+v", s.DXMetadata, s.DEMetadata)
			}
		})
	}
}

func TestCanadianRoleRejectionAndAdmissionExceptions(t *testing.T) {
	configureCanadianClusterFixture(t, true)
	db := canadianClusterCTY(t)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	allowlist := filepath.Join(t.TempDir(), "allowlist.txt")
	if err := os.WriteFile(allowlist, []byte("VE3NON\nUS:VE3NON\nADIF211:VE3NON\nADIF1:VE3ALW\nADIF211:CY0ALW\nADIF252:CY9ALW\n"), 0600); err != nil {
		t.Fatal(err)
	}
	uls.SetAllowlistPath(allowlist)
	t.Cleanup(func() { uls.SetAllowlistPath("") })
	for _, prefix := range []string{"VE3", "CY0", "CY9"} {
		t.Run(prefix, func(t *testing.T) {
			s := spot.NewSpot(prefix+"ABC", prefix+"NON", 14020, "CW")
			if v.validateSpot(s) {
				t.Fatal("central DE gate admitted a missing license")
			}
			s.IsTestSpotter = true
			if !v.validateSpot(s) || s.DEMetadata.State != "" {
				t.Fatal("TEST bypass was lost or fabricated a province")
			}
			s = spot.NewSpot(prefix+"NON", "VE2AAA", 14020, "CW")
			if !v.validateSpot(s) || !applyLicenseGate(s, db, nil, nil) {
				t.Fatal("DX licensing was applied before the final DX gate or was skipped")
			}
			s.IsBeacon = true
			if applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "" {
				t.Fatal("BEACON bypass was lost or fabricated a province")
			}
			s = spot.NewSpot(prefix+"ALW", prefix+"ALW", 14020, "CW")
			if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "" || s.DEMetadata.State != "" {
				t.Fatal("qualified ADIF exception was lost or fabricated province metadata")
			}
		})
	}
}

func TestCanadianSourceOutageDoesNotInterruptFCC(t *testing.T) {
	canadianPath := configureCanadianClusterFixture(t, true)
	configureFCCStateFixture(t, true)
	db := canadianClusterCTY(t)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	uls.SetCanadianLicenseDBPath(filepath.Join(t.TempDir(), "unavailable.db"))
	s := spot.NewSpot("VE3NON", "VE3NON", 14020, "CW")
	s.DXMetadata.State, s.DEMetadata.State = "ON", "ON"
	if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "" || s.DEMetadata.State != "" {
		t.Fatal("Canadian outage rejected activity or retained stale province metadata")
	}
	s = spot.NewSpot("K1ABC", "N2AAA", 14020, "CW")
	if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "CA" || s.DEMetadata.State != "AP" {
		t.Fatal("Canadian outage interrupted FCC enrichment")
	}
	uls.SetCanadianLicenseDBPath(canadianPath)
	uls.SetRefreshInProgress(true)
	t.Cleanup(func() { uls.SetRefreshInProgress(false) })
	s = spot.NewSpot("VE3ABC", "VE2AAA", 14020, "CW")
	if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "ON" || s.DEMetadata.State != "QC" {
		t.Fatal("FCC refresh interrupted Canadian enrichment")
	}
	s = spot.NewSpot("K1NON", "N2NON", 14020, "CW")
	if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "" || s.DEMetadata.State != "" {
		t.Fatal("FCC refresh no longer failed open")
	}
}

func TestCanadianResolverDeliveryRetainsRecordedHistory(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, delayed := range []bool{false, true} {
			t.Run(fmt.Sprintf("enforcement=%t/delayed=%t", enabled, delayed), func(t *testing.T) {
				configureCanadianClusterFixture(t, enabled)
				db := canadianClusterCTY(t)
				cfg := config.CallCorrectionConfig{
					Enabled: true, MaxEditDistance: 6, MinConsensusReports: 2, MinAdvantage: 1,
					FrequencyToleranceHz: 500, DistanceModelCW: "morse", DistanceModelRTTY: "baudot", InvalidAction: "broadcast",
					StabilizerEnabled: true, StabilizerMaxChecks: 1, StabilizerTimeoutAction: stabilizerTimeoutRelease,
				}
				writer, err := archive.NewWriter(config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1})
				if err != nil {
					t.Fatal(err)
				}
				writer.Start()
				t.Cleanup(writer.Stop)
				ring := buffer.NewRingBuffer(4)
				srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
				p := newDeliveryTestPipeline(ring, writer, srv)
				p.ctyLookup = func() *cty.CTYDatabase { return db }
				p.signalResolver, p.correctionCfg = canadianClusterResolver(t, cfg), cfg
				v := newIngestValidator(p.ctyLookup, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
				s := spot.NewSpot("VE3ABC", "VE2AAA", 14020, "CW")
				if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "ON" || s.DEMetadata.State != "QC" {
					t.Fatalf("initial licensed provinces: DX=%+v DE=%+v", s.DXMetadata, s.DEMetadata)
				}
				if delayed {
					p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s})
				} else {
					ctx := &outputSpotContext{spot: s, ctyDB: db, modeUpper: "CW"}
					if !p.applyResolverStage(ctx, nil) || !p.finalizeSpotForMetrics(ctx) || !p.prepareFanoutSpot(ctx) {
						t.Fatal("licensed corrected spot was suppressed")
					}
					p.deliverSpot(ctx)
				}
				out, ok := tryReadTelnetBroadcastSpot(srv)
				if !ok || out == nil || ring.GetCount() != 1 || out.DXCallNorm != "VE3ABD" || out.Confidence != "C" || out.DXMetadata.State != "BC" || out.DEMetadata.State != "QC" {
					t.Fatalf("corrected Canadian delivery: ok=%t count=%d spot=%+v", ok, ring.GetCount(), out)
				}
				uls.SetCanadianLicenseDBPath(canadianClusterDatabase(t, true))
				if current := uls.LookupCanadian("VE3ABD"); !current.Available || !current.Found || current.State != "SK" {
					t.Fatalf("replacement snapshot was not active: %+v", current)
				}
				if current := uls.LookupCanadian("VE2AAA"); !current.Available || !current.Found || current.State != "NB" {
					t.Fatalf("replacement DE province was not active: %+v", current)
				}
				canadianClusterAwaitArchive(t, writer)
				f := filter.NewFilter()
				f.SetDXState("BC", true)
				f.SetDEState("QC", true)
				page, err := writer.ReadHistoryPage(archive.HistoryRequest{Limit: 1, Now: time.Now().UTC(), Match: f.Matches})
				if err != nil || len(page.Spots) != 1 || page.Spots[0].DXMetadata.State != "BC" || page.Spots[0].DEMetadata.State != "QC" {
					t.Fatalf("history replaced recorded provinces: %v %+v", err, page.Spots)
				}
				f.ResetDXStates()
				f.SetDXState("SK", true)
				page, err = writer.ReadHistoryPage(archive.HistoryRequest{Limit: 1, Now: time.Now().UTC(), Match: f.Matches})
				if err != nil || len(page.Spots) != 0 {
					t.Fatal("history matched today's province instead of the recorded province")
				}
			})
		}
	}
}

func canadianClusterResolver(t *testing.T, cfg config.CallCorrectionConfig) *spot.SignalResolver {
	t.Helper()
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 64, MaxActiveKeys: 16, MaxCandidatesPerKey: 8, MaxReportersPerCand: 16,
		InactiveTTL: time.Minute, EvalMinInterval: 5 * time.Millisecond, SweepInterval: 5 * time.Millisecond,
		HysteresisWindows: 1, FreqGuardRunnerUpRatio: 0.6, MaxEditDistance: 3,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)
	var key spot.ResolverSignalKey
	for _, reporter := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		evidence, ok := buildResolverEvidenceSnapshot(spot.NewSpot("VE3ABD", reporter, 14020, "CW"), cfg, nil, time.Now().UTC())
		if !ok || !resolver.Enqueue(evidence) {
			t.Fatal("could not enqueue Canadian resolver evidence")
		}
		key = evidence.Key
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, ok := resolver.Lookup(key)
		if ok && snapshot.Winner == "VE3ABD" && snapshot.WinnerSupport == 3 &&
			(snapshot.State == spot.ResolverStateConfident || snapshot.State == spot.ResolverStateProbable) {
			return resolver
		}
		select {
		case <-deadline.C:
			t.Fatalf("resolver did not select Canadian winner: available=%t snapshot=%+v", ok, snapshot)
		case <-ticker.C:
		}
	}
}

func canadianClusterAwaitArchive(t *testing.T, writer *archive.Writer) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, err := writer.Recent(1)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 {
			if rows[0].DXCallNorm != "VE3ABD" || rows[0].DXMetadata.State != "BC" || rows[0].DEMetadata.State != "QC" {
				t.Fatalf("decoded archive changed provinces: %+v", rows[0])
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("Canadian spot did not reach the archive")
		case <-ticker.C:
		}
	}
}

func TestCanadianDisabledRuntimeCancelsAndJoinsSources(t *testing.T) {
	requested := make(chan string, 2)
	source := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		select {
		case requested <- req.URL.Path:
		default:
		}
		<-req.Context().Done()
	}))
	t.Cleanup(source.Close)
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	previousFCC, previousCanada := uls.LicenseChecksEnabled(), uls.CanadianLicenseChecksEnabled()
	previousTTL := uls.LookupStats().TTL
	r := &clusterRuntime{ctx: ctx, cancel: cancel, cfg: &config.Config{
		FCCULS: config.FCCULSConfig{Enabled: false, URL: source.URL + "/fcc", DBPath: filepath.Join(dir, "fcc.db"), Archive: filepath.Join(dir, "fcc.zip"), RefreshUTC: "22:00"},
		ISED:   config.ISEDConfig{Enabled: false, URL: source.URL + "/main", SpecialURL: source.URL + "/special", DBPath: filepath.Join(dir, "ised.db"), Archive: filepath.Join(dir, "ised.zip"), SpecialArchive: filepath.Join(dir, "events.zip"), RefreshUTC: "22:00"},
	}}
	t.Cleanup(func() {
		cancel()
		for name, done := range map[string]<-chan struct{}{"FCC": r.ulsDone, "ISED": r.canadianDone} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Errorf("%s source did not finish before test cleanup", name)
			}
		}
		uls.SetLicenseDBPath("")
		uls.SetCanadianLicenseDBPath("")
		uls.SetLicenseChecksEnabled(previousFCC)
		uls.SetCanadianLicenseChecksEnabled(previousCanada)
		uls.SetLicenseCacheTTL(previousTTL)
	})
	r.initializeULSAndCTY()
	seen := make(map[string]bool, 2)
	for len(seen) < 2 {
		select {
		case path := <-requested:
			seen[path] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("disabled enforcement did not start both sources: %v", seen)
		}
	}
	if !seen["/fcc"] || !seen["/main"] || uls.LicenseChecksEnabled() || uls.CanadianLicenseChecksEnabled() {
		t.Fatalf("disabled startup reference contract: paths=%v", seen)
	}
	closed := make(chan struct{})
	go func() { r.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime close did not cancel and join both in-flight HTTP source workers")
	}
	for name, done := range map[string]<-chan struct{}{"FCC": r.ulsDone, "ISED": r.canadianDone} {
		select {
		case <-done:
		default:
			t.Errorf("runtime close returned before %s source completion", name)
		}
	}
}

// Controlled completion barriers make joining each owner falsifiable even when
// cancellation would usually let the real HTTP workers finish immediately.
func TestCanadianRuntimeCloseWaitsForBothSourceOwners(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fccDone, canadianDone := make(chan struct{}), make(chan struct{})
	r := &clusterRuntime{ctx: ctx, cancel: cancel, ulsDone: fccDone, canadianDone: canadianDone}
	closed := make(chan struct{})
	fccReleased, canadianReleased := false, false
	t.Cleanup(func() {
		cancel()
		if !fccReleased {
			close(fccDone)
		}
		if !canadianReleased {
			close(canadianDone)
		}
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Error("test-owned close goroutine did not finish")
		}
	})
	go func() { r.close(); close(closed) }()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime close did not cancel the source context")
	}
	select {
	case <-closed:
		t.Fatal("runtime close returned with both source owners still active")
	case <-time.After(25 * time.Millisecond):
	}
	close(fccDone)
	fccReleased = true
	select {
	case <-closed:
		t.Fatal("runtime close joined FCC but skipped the Canadian owner")
	case <-time.After(25 * time.Millisecond):
	}
	close(canadianDone)
	canadianReleased = true
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime close remained blocked after both owners completed")
	}
}
