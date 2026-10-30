package telnet

import (
	"bufio"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

// Current-session lifecycle fixtures use the same explicit two-bank owner as
// login. Frozen v6 carrier tests keep configurationTestClient instead.
func v7CurrentTestClient(t *testing.T, s *Server, call string) *Client {
	t.Helper()
	c := configurationTestClient(s, call)
	settings := filter.SettingsConfiguration{Dialect: "go", DedupePolicy: "FAST"}
	cfg, err := filter.DefaultV7Configuration(settings)
	if err != nil {
		t.Fatal(err)
	}
	canonical, value, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = value
	c.filterBanks, c.dialect = &canonical.Filters, DialectGo
	c.configuredSettings, c.configurationInitialized = settings, true
	c.setDedupePolicy(dedupePolicyFast)
	return c
}

func v7IndependentClient(t *testing.T) (*Server, *Client) {
	t.Helper()
	s, c := v7MachineTestClient(t)
	s.filterEngine = newFilterCommandEngine()
	cfg, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Filters.CC.Geography = nil
	cfg.Filters.CC.Modes = []string{"CW", "FT8"}
	cfg.Filters.CC.AllowSelf = filter.DefaultBoolFalse
	cfg.Filters.GO.AllowSelf = filter.DefaultBoolTrue
	canonical, _, ccRuntime, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.filterBanks, c.filter = &canonical.Filters, &ccRuntime
	return s, c
}

func TestV7ActiveCaptureOwnBank(t *testing.T) {
	s, c := v7IndependentClient(t)
	before, _ := c.captureV7Configuration()
	pointer := c.filter
	response, handled := s.filterEngine.Handle(c, "SET/NOFT8")
	if !handled || !strings.Contains(response, "Saved and active") {
		t.Fatalf("edit: %q", response)
	}
	after, _ := c.captureV7Configuration()
	if !(filter.V6Configuration{Filters: before.Filters.GO}).Equal(filter.V6Configuration{Filters: after.Filters.GO}) || after.Filters.CC.Equal(before.Filters.CC) {
		t.Fatal("CC edit changed wrong bank")
	}
	response, handled = s.handleDialectCommand(c, "DIALECT GO")
	if !handled || c.dialect != DialectGo || c.filter.IsCC() {
		t.Fatalf("GO switch: %q", response)
	}
	if !c.filter.SelfEnabled() {
		t.Fatal("GO lost its independent SELF choice")
	}
	returned, _ := c.captureV7Configuration()
	if !returned.Filters.CC.Equal(after.Filters.CC) || c.filter != pointer {
		t.Fatal("switch changed CC bank or stable Filter identity")
	}
	_, _ = s.handleDialectCommand(c, "DIALECT CC")
	if c.filter.SelfEnabled() || !c.filter.IsCC() {
		t.Fatal("CC did not restore its own controls/program")
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Configuration.Filters.CC.Equal(after.Filters.CC) {
		t.Fatal("complete save lost inactive CC state")
	}
}

func TestV7UnsavedBanksAcrossSwitch(t *testing.T) {
	s, c := v7IndependentClient(t)
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	response, _ := s.filterEngine.Handle(c, "SET/NOFT8")
	if !strings.Contains(response, "not saved") || !c.configurationUnsaved {
		t.Fatalf("missing late-failure warning: %q", response)
	}
	cc, _ := c.captureV7Configuration()
	_, _ = s.handleDialectCommand(c, "DIALECT GO")
	response, _ = s.filterEngine.Handle(c, "PASS RULE 3 MODE=RTTY")
	if !strings.Contains(response, "not saved") {
		t.Fatalf("GO latefailure: %q", response)
	}
	both, _ := c.captureV7Configuration()
	if !both.Filters.CC.Equal(cc.Filters.CC) || len(both.Filters.GO.Rules) == 0 {
		t.Fatal("unsaved switch discarded another bank")
	}
	s.commitConfigurationFn = nil
	if err := c.saveFilterOwned(); err != nil {
		t.Fatal(err)
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Configuration.Equal(both) || c.configurationUnsaved {
		t.Fatal("later whole save lost pending state")
	}
}

func TestV7PresetBankOwnershipAndPartialSave(t *testing.T) {
	s, c := v7IndependentClient(t)
	before, _ := c.captureV7Configuration()
	response, handled := s.handlePresetCommand(c, "SAVE PRESET NATIVE")
	if !handled || !strings.Contains(response, "Saved preset") {
		t.Fatalf("SAVE: %q", response)
	}
	_, _ = s.handleDialectCommand(c, "DIALECT GO")
	_, _ = s.filterEngine.Handle(c, "PASS RULE 4 MODE=RTTY")
	goState, _ := c.captureV7Configuration()
	response, _ = s.handlePresetCommand(c, "LOAD PRESET NATIVE")
	after, _ := c.captureV7Configuration()
	if !strings.Contains(response, "Dialect switched to CC") || c.dialect != DialectCC || !after.Filters.CC.Equal(before.Filters.CC) || !(filter.V6Configuration{Filters: after.Filters.GO}).Equal(filter.V6Configuration{Filters: goState.Filters.GO}) {
		t.Fatalf("tagged LOAD overwrote wrong bank: %q", response)
	}
	prior := c.v7PresetReferences
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	response, _ = s.handlePresetCommand(c, "SAVE PRESET PARTIAL")
	if !strings.Contains(response, "Previous preset associations") || c.v7PresetReferences != prior {
		t.Fatalf("partial association was published: %q", response)
	}
	if _, err := filter.LoadV7Preset(c.callsign, "PARTIAL"); err != nil {
		t.Fatal("library success was rolled back", err)
	}
}

func TestV7ConcurrentBankPublication(t *testing.T) {
	s, c := v7IndependentClient(t)
	cc, _ := c.captureV7Configuration()
	before := cc.Clone()
	cc.Settings.Grid = "FN31"
	goConfig := cc.Clone()
	goConfig.Settings.Dialect = "go"
	goConfig.Settings.Grid = "IO91"
	ccPrepared, err := s.prepareV7ConfigurationUpdate(c, before, cc, s.now())
	if err != nil {
		t.Fatal(err)
	}
	goPrepared, err := s.prepareV7ConfigurationUpdate(c, before, goConfig, s.now())
	if err != nil {
		t.Fatal(err)
	}
	s.publishConfigurationState(c, cc.Settings, ccPrepared, nil, time.Time{}, false)
	old := c.captureMatchingSnapshot()
	sp := spot.NewSpot("K1DX", "W1SPOT", 14020, "CW")
	var readers sync.WaitGroup
	var mixed atomic.Bool
	readers.Add(1)
	go func() {
		defer readers.Done()
		for range 1000 {
			snap := c.captureMatchingSnapshot()
			_, _, _ = s.matchSnapshot(snap, sp)
			want := "FN31"
			if snap.filter.SelfEnabled() {
				want = "IO91"
			}
			if snap.path.grid != want {
				mixed.Store(true)
			}
		}
	}()
	for range 1000 {
		s.publishConfigurationState(c, cc.Settings, ccPrepared, nil, time.Time{}, false)
		s.publishConfigurationState(c, goConfig.Settings, goPrepared, nil, time.Time{}, false)
	}
	readers.Wait()
	if c.filter.IsCC() || mixed.Load() || old.path.grid != "FN31" || old.filter.SelfEnabled() {
		t.Fatal("publication mixed program/control/location or changed old snapshot")
	}
}

func TestV7ConfigurationWriterRetainsAndReleasesCharge(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "failure"}[fail], func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
			if fail {
				conn.err = errors.New("blocked flush failed")
			}
			c := &Client{server: &Server{writerBatchMaxBytes: 1024}, conn: conn, writer: bufio.NewWriterSize(conn, 256), done: make(chan struct{}), controlChan: make(chan controlMessage, 3), spotChan: make(chan *spotEnvelope, 1)}
			reservation, err := c.reserveConfigurationOutput()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reservation.Release)
			if err := c.enqueueConfigurationResponse(reservation, []byte(strings.Repeat("x", 128<<10)), false); err != nil {
				t.Fatal(err)
			}
			c.controlChan <- controlMessage{closeAfter: true}
			done := writerV15Start(c)
			writerV15Wait(t, conn.entered, "reserved write")
			assertConfigurationTransferStats(t, input, output+1<<20)
			close(conn.release)
			writerV15Wait(t, done, "writer release")
			assertConfigurationTransferStats(t, input, output)
		})
	}
}

func TestV7NativeDeliveryBothSourcesAndOwnCallPaths(t *testing.T) {
	s, c := v7IndependentClient(t)
	c.spotChan = make(chan *spotEnvelope, 8)
	s.clients = map[string]*Client{c.callsign: c}
	_, _ = s.filterEngine.Handle(c, "SET/NOFT8")
	for _, source := range []spot.SourceType{spot.SourceManual, spot.SourceRBN} {
		positive := spot.NewSpot("K1POS", "W1SPOT", 14020, "CW")
		positive.SourceType = source
		spot.ApplySourceHumanFlag(positive)
		negative := spot.NewSpot("K1NEG", "W1SPOT", 14074, "FT8")
		negative.SourceType = source
		spot.ApplySourceHumanFlag(negative)
		s.deliverJob(broadcastJob{spot: positive, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
		if len(c.spotChan) != 1 || (<-c.spotChan).spot.DXCall != "K1POS" {
			t.Fatalf("no positive %s delivery", source)
		}
		s.deliverJob(broadcastJob{spot: negative, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
		if len(c.spotChan) != 0 {
			t.Fatalf("excluded FT8 reached %s consumer", source)
		}
	}
	_, _ = s.filterEngine.Handle(c, "SET/SELF")
	_, _ = s.filterEngine.Handle(c, "SET/NOTOXIC")
	self := spot.NewSpot("W1ABC", "K1REPORT", 14074, "FT8")
	for _, direct := range []bool{false, true} {
		for _, toxic := range []bool{false, true} {
			self.ToxicityStatus = spot.ToxicitySafe
			if toxic {
				self.ToxicityStatus = spot.ToxicityToxic
			}
			if direct {
				s.DeliverSelfSpotOwned("W1ABC", self)
			} else {
				s.deliverJob(broadcastJob{spot: self, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
			}
			want := 0
			if !toxic {
				want = 1
			}
			if len(c.spotChan) != want {
				t.Fatalf("own-call direct=%v toxic=%v delivered=%d", direct, toxic, len(c.spotChan))
			}
			if want == 1 {
				<-c.spotChan
			}
		}
	}
	_, _ = s.filterEngine.Handle(c, "SET/NOSELF")
	self.ToxicityStatus = spot.ToxicitySafe
	s.DeliverSelfSpotOwned("W1ABC", self)
	s.deliverJob(broadcastJob{spot: self, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
	if len(c.spotChan) != 0 {
		t.Fatal("SELF OFF did not gate both own-call paths")
	}
}
