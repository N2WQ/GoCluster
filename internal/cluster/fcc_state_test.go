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
	"dxcluster/spot"
	"dxcluster/telnet"
	"dxcluster/uls"
)

func configureFCCStateFixture(t testing.TB, enabled bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fcc.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), `CREATE TABLE AM (unique_system_identifier INTEGER, call_sign TEXT, state TEXT NOT NULL);
		CREATE INDEX idx_AM_call_sign ON AM(call_sign);
		INSERT INTO AM VALUES(1,'K1ABC','CA'),(2,'K1XYZ','TX'),(3,'N2AAA','AP'),(4,'K1ABD','TX');
		PRAGMA user_version=1;`)
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("fixture: %v %v", err, closeErr)
	}
	previous := uls.LicenseChecksEnabled()
	uls.SetRefreshInProgress(false)
	uls.SetLicenseDBPath(path)
	uls.SetLicenseChecksEnabled(enabled)
	t.Cleanup(func() {
		uls.SetLicenseDBPath("")
		uls.SetLicenseChecksEnabled(previous)
	})
}

func TestFCCDEStateMetadataRefresh(t *testing.T) {
	configureFCCStateFixture(t, false)
	db := loadIngestCTY(t)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	s := spot.NewSpot("K1ABC", "N2AAA", 14020, "CW")
	s.DEMetadata.Grid, s.DEMetadata.GridDerived = "EM10", true
	if !v.validateSpot(s) || s.DEMetadata.State != "AP" {
		t.Fatalf("ingest state: %+v", s.DEMetadata)
	}
	s.Confidence = "C" // Force final CTY reconstruction of both roles.
	if applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "CA" || s.DEMetadata.State != "AP" || s.DEMetadata.Grid != "EM10" {
		t.Fatalf("final CTY refresh: %+v/%+v", s.DXMetadata, s.DEMetadata)
	}
	s.DEMetadata.ADIF, s.DEMetadata.CQZone = 0, 0 // Force secondary metadata refresh.
	p := &outputPipeline{secondaryActive: true}
	if !p.prepareFanoutSpot(&outputSpotContext{spot: s, ctyDB: db}) || s.DEMetadata.State != "AP" || s.DEMetadata.Grid != "EM10" {
		t.Fatalf("secondary CTY refresh: %+v", s.DEMetadata)
	}
}

func TestFCCDisabledEnrichmentDeliveryArchive(t *testing.T) {
	configureFCCStateFixture(t, false)
	db := loadIngestCTY(t)
	for _, delayed := range []bool{false, true} {
		t.Run(fmt.Sprintf("delayed=%t", delayed), func(t *testing.T) {
			for _, final := range []struct{ call, state string }{{"K1XYZ", "TX"}, {"K1NON", ""}, {"DL1ABC", ""}} {
				t.Run(final.call, func(t *testing.T) {
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
					v := newIngestValidator(p.ctyLookup, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
					s := spot.NewSpot("K1ABC", "N2AAA", 14020, "CW")
					if !v.validateSpot(s) {
						t.Fatal("disabled enforcement rejected ingress")
					}
					if applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "CA" {
						t.Fatalf("initial DX state: %+v", s.DXMetadata)
					}
					// The final materialized identity changes after initial enrichment.
					s.DXCall, s.DXCallNorm, s.Confidence = final.call, final.call, "C"
					s.InvalidateMetadataCache()
					if delayed {
						p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s})
					} else {
						ctx := &outputSpotContext{spot: s, ctyDB: db, dirty: true, modeUpper: "CW"}
						if !p.finalizeSpotForMetrics(ctx) || !p.prepareFanoutSpot(ctx) {
							t.Fatal("disabled enforcement rejected final output")
						}
						p.deliverSpot(ctx)
					}
					out, ok := tryReadTelnetBroadcastSpot(srv)
					if !ok || ring.GetCount() != 1 || out.DXCallNorm != final.call || out.DXMetadata.State != final.state || out.DEMetadata.State != "AP" {
						t.Fatalf("delivery: ok=%t count=%d out=%+v", ok, ring.GetCount(), out)
					}
					deadline := time.Now().Add(2 * time.Second)
					for {
						rows, err := writer.Recent(1)
						if err != nil {
							t.Fatal(err)
						}
						if len(rows) == 1 {
							if rows[0].DXCallNorm != final.call || rows[0].DXMetadata.State != final.state || rows[0].DEMetadata.State != "AP" {
								t.Fatalf("decoded archive: %+v", rows[0])
							}
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("archive did not persist delivered spot")
						}
						time.Sleep(time.Millisecond)
					}
				})
			}
		})
	}
}

func TestFCCCoverageAndAdmissionExceptions(t *testing.T) {
	// Independent fixture entities exercise the predicate at its real consumer.
	entities := []int{6, 9, 20, 43, 103, 110, 123, 138, 166, 174, 182, 197, 202, 285, 291, 297, 515, 105, 134, 1}
	for _, entity := range entities {
		plist := fmt.Sprintf(`<plist><dict><key>K1</key><dict><key>Country</key><string>Fixture</string><key>Prefix</key><string>K1</string><key>ADIF</key><integer>%d</integer><key>Continent</key><string>NA</string><key>CQZone</key><integer>5</integer></dict></dict></plist>`, entity)
		db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(plist))
		if err != nil {
			t.Fatal(err)
		}
		v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
		queried := false
		v.lookupUS = func(string) uls.LookupResult { queried = true; return uls.LookupResult{Available: true} }
		v.licenseChecksEnabled = func() bool { return true }
		s := spot.NewSpot("K1ABC", "K1XYZ", 14020, "CW")
		wantLookup := entity != 105 && entity != 134 && entity != 1
		if accepted := v.validateSpot(s); accepted == wantLookup || queried != wantLookup {
			t.Fatalf("ADIF %d accepted=%t queried=%t", entity, accepted, queried)
		}
		s.IsTestSpotter = true
		if !v.validateSpot(s) {
			t.Fatalf("ADIF %d rejected TEST exception", entity)
		}
	}
	configureFCCStateFixture(t, true)
	db := loadIngestCTY(t)
	s := spot.NewSpot("K1NON", "N2AAA", 14020, "CW")
	if !applyLicenseGate(s, db, nil, nil) {
		t.Fatal("enabled enforcement admitted missing DX license")
	}
	s.IsBeacon = true
	if applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "" {
		t.Fatal("beacon exception lost or fabricated state")
	}
}

func TestFCCDisabledRuntimeSourceLifecycle(t *testing.T) {
	requested := make(chan struct{}, 1)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requested <- struct{}{}:
		default:
		}
		http.Error(w, "fixture unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(source.Close)
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	r := &clusterRuntime{ctx: ctx, cancel: cancel, cfg: &config.Config{FCCULS: config.FCCULSConfig{
		Enabled: false, URL: source.URL, DBPath: filepath.Join(dir, "fcc.db"), Archive: filepath.Join(dir, "fcc.zip"), RefreshUTC: "22:00",
	}}}
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.ulsDone:
		case <-time.After(2 * time.Second):
			t.Error("FCC worker did not stop before cleanup")
		}
		uls.SetLicenseDBPath("")
		uls.SetLicenseChecksEnabled(true)
	})
	r.initializeULSAndCTY()
	select {
	case <-requested:
	case <-time.After(2 * time.Second):
		t.Fatal("disabled enforcement suppressed reference-data startup")
	}
	if uls.LicenseChecksEnabled() {
		t.Fatal("disabled config enabled rejection")
	}
	cancel()
	select {
	case <-r.ulsDone:
	case <-time.After(2 * time.Second):
		t.Fatal("FCC refresh worker did not exit after cancellation")
	}
}

// Qualification owns its reference source separately from the enforcement
// setting. An empty ready database and a local HTTP endpoint keep these runs
// isolated without teaching production code a second disable policy.
func configureFCCQualificationSource(t *testing.T, cfg *config.Config) {
	t.Helper()
	dir := t.TempDir()
	t.Cleanup(func() { uls.SetLicenseDBPath(""); uls.SetLicenseChecksEnabled(true) })
	path := filepath.Join(dir, "fcc.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), `CREATE TABLE AM(unique_system_identifier INTEGER,call_sign TEXT,state TEXT NOT NULL);
		CREATE INDEX idx_AM_call_sign ON AM(call_sign);
		CREATE TABLE HD(unique_system_identifier INTEGER,call_sign TEXT,license_status TEXT,
			radio_service_code TEXT,grant_date TEXT,expired_date TEXT,cancellation_date TEXT,last_action_date TEXT);
		PRAGMA user_version=1;`)
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("qualification FCC fixture: %v %v", err, closeErr)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "isolated qualification reference source", http.StatusServiceUnavailable)
	}))
	t.Cleanup(source.Close)
	cfg.FCCULS.DBPath, cfg.FCCULS.Archive, cfg.FCCULS.TempDir = path, filepath.Join(dir, "fcc.zip"), dir
	cfg.FCCULS.URL = source.URL
}

// The pipeline benchmark exercises both role consumers with an already warm
// factual cache. It makes no network-delivery or deployed latency claim.
func BenchmarkFCCStatePipeline(b *testing.B) {
	configureFCCStateFixture(b, false)
	db := loadIngestCTY(b)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	s := spot.NewSpot("K1ABC", "N2AAA", 14020, "CW")
	if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) {
		b.Fatal("warmup rejected")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) {
			b.Fatal("enrichment rejected")
		}
	}
	b.StopTimer()
	if s.DXMetadata.State != "CA" || s.DEMetadata.State != "AP" || uls.LookupStats().Entries != 2 {
		b.Fatal("pipeline did not retain bounded role facts")
	}
}

func TestFCCAllowlistConsumerCoverage(t *testing.T) {
	configureFCCStateFixture(t, true)
	path := filepath.Join(t.TempDir(), "allowlist.txt")
	if err := os.WriteFile(path, []byte("K1XYZ\nUS:K1NON\n"), 0600); err != nil {
		t.Fatal(err)
	}
	uls.SetAllowlistPath(path)
	t.Cleanup(func() { uls.SetAllowlistPath("") })
	for _, entity := range []int{6, 9, 20, 43, 103, 110, 123, 138, 166, 174, 182, 197, 202, 285, 291, 297, 515} {
		plist := fmt.Sprintf(`<plist><dict><key>K1</key><dict><key>Country</key><string>Fixture</string><key>Prefix</key><string>K1</string><key>ADIF</key><integer>%d</integer><key>Continent</key><string>NA</string><key>CQZone</key><integer>5</integer></dict></dict></plist>`, entity)
		db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(plist))
		if err != nil {
			t.Fatal(err)
		}
		v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
		v.lookupUS = func(string) uls.LookupResult { return uls.LookupResult{Available: true} }
		s := spot.NewSpot("K1NON", "K1XYZ", 14020, "CW")
		if !v.validateSpot(s) || s.DEMetadata.State != "" || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "" {
			t.Fatalf("ADIF %d lost allowlist exception or fabricated state", entity)
		}
	}
}
