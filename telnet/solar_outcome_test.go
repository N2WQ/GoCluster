package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/solarweather"
	"gopkg.in/yaml.v3"
)

// This is composed evidence: solarweather owns actual active/fresh-calm
// generation; these tests qualify supplied-summary dispatch and consumer status.
// They start no polling and establish no polling-to-wire or retirement claim.
const solarOutcomeMessage = "SOLAR R3 G2 impacting 80m/60m/40m/30m/20m/17m/15m/12m/10m"

func solarOutcomeUTC(hour, minute int) time.Time {
	return time.Date(2026, 10, 9, hour, minute, 0, 0, time.UTC)
}

func solarOutcomeManager(enabled bool) *solarweather.Manager {
	cfg := solarweather.DefaultConfig()
	cfg.Enabled = enabled
	return solarweather.NewManager(cfg, nil)
}

func solarOutcomeDispatchClient(s *Server, minutes int) *Client {
	c := &Client{callsign: fmt.Sprintf("SOLAR%d", minutes), done: make(chan struct{}), controlChan: make(chan controlMessage, 8)}
	c.setSolarSummaryMinutes(minutes, time.Date(2026, 10, 9, 12, 7, 30, 0, time.UTC))
	s.clients[c.callsign] = c
	return c
}

func assertSolarOutcomeQueue(t *testing.T, c *Client, count int, next time.Time) {
	t.Helper()
	if cap(c.controlChan) == 0 || len(c.controlChan) != count {
		t.Fatalf("%s: queued=%d, want %d with usable control queue", c.callsign, len(c.controlChan), count)
	}
	for range count {
		message := <-c.controlChan
		if message.line != solarOutcomeMessage || message.raw != nil || message.closeAfter {
			t.Fatalf("%s: wrong dispatched control: %+v", c.callsign, message)
		}
	}
	c.solarMu.Lock()
	got := c.solarNextSummaryAt
	c.solarMu.Unlock()
	if !got.Equal(next) {
		t.Fatalf("%s: next deadline %v, want literal %v", c.callsign, got, next)
	}
	select {
	case <-c.done:
		t.Fatal("dispatch closed a client with an available queue")
	default:
	}
}

func TestSolarDispatchLiteralWallClockSlots(t *testing.T) {
	s := &Server{clients: make(map[string]*Client)}
	clients := make(map[int]*Client)
	for _, minutes := range []int{15, 30, 60, 0} {
		clients[minutes] = solarOutcomeDispatchClient(s, minutes)
	}
	// Expected deadlines are literal UTC clock slots, not derived by calling the
	// scheduling helper under test. Each enabled cadence has positive delivery;
	// OFF always stays disabled and has no deadline.
	for _, tc := range []struct {
		name                      string
		at                        time.Time
		count15, count30, count60 int
		next15, next30, next60    time.Time
	}{
		{"before", solarOutcomeUTC(12, 15).Add(-time.Nanosecond), 0, 0, 0, solarOutcomeUTC(12, 15), solarOutcomeUTC(12, 30), solarOutcomeUTC(13, 0)},
		{"exact 15", solarOutcomeUTC(12, 15), 1, 0, 0, solarOutcomeUTC(12, 30), solarOutcomeUTC(12, 30), solarOutcomeUTC(13, 0)},
		{"repeated 15", solarOutcomeUTC(12, 15), 0, 0, 0, solarOutcomeUTC(12, 30), solarOutcomeUTC(12, 30), solarOutcomeUTC(13, 0)},
		{"before 30", solarOutcomeUTC(12, 30).Add(-time.Nanosecond), 0, 0, 0, solarOutcomeUTC(12, 30), solarOutcomeUTC(12, 30), solarOutcomeUTC(13, 0)},
		{"exact 30", solarOutcomeUTC(12, 30), 1, 1, 0, solarOutcomeUTC(12, 45), solarOutcomeUTC(13, 0), solarOutcomeUTC(13, 0)},
		{"repeated 30", solarOutcomeUTC(12, 30), 0, 0, 0, solarOutcomeUTC(12, 45), solarOutcomeUTC(13, 0), solarOutcomeUTC(13, 0)},
		{"late 15", solarOutcomeUTC(12, 47), 1, 0, 0, solarOutcomeUTC(13, 0), solarOutcomeUTC(13, 0), solarOutcomeUTC(13, 0)},
		{"before hour", solarOutcomeUTC(13, 0).Add(-time.Nanosecond), 0, 0, 0, solarOutcomeUTC(13, 0), solarOutcomeUTC(13, 0), solarOutcomeUTC(13, 0)},
		{"exact hour", solarOutcomeUTC(13, 0), 1, 1, 1, solarOutcomeUTC(13, 15), solarOutcomeUTC(13, 30), solarOutcomeUTC(14, 0)},
		{"repeated hour", solarOutcomeUTC(13, 0), 0, 0, 0, solarOutcomeUTC(13, 15), solarOutcomeUTC(13, 30), solarOutcomeUTC(14, 0)},
		{"late all cadences", solarOutcomeUTC(15, 47), 1, 1, 1, solarOutcomeUTC(16, 0), solarOutcomeUTC(16, 0), solarOutcomeUTC(16, 0)},
		{"repeated late call", solarOutcomeUTC(15, 47), 0, 0, 0, solarOutcomeUTC(16, 0), solarOutcomeUTC(16, 0), solarOutcomeUTC(16, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.dispatchSolarSummary(tc.at, solarOutcomeMessage)
			assertSolarOutcomeQueue(t, clients[15], tc.count15, tc.next15)
			assertSolarOutcomeQueue(t, clients[30], tc.count30, tc.next30)
			assertSolarOutcomeQueue(t, clients[60], tc.count60, tc.next60)
			assertSolarOutcomeQueue(t, clients[0], 0, time.Time{})
			for minutes, client := range clients {
				if client.getSolarSummaryMinutes() != minutes {
					t.Fatal("dispatch changed the selected cadence")
				}
			}
		})
	}
}

func TestSolarDispatchEmptyDueSlotWaitsForNextSelectedTick(t *testing.T) {
	s := &Server{clients: make(map[string]*Client)}
	c := solarOutcomeDispatchClient(s, 15)
	s.dispatchSolarSummary(solarOutcomeUTC(12, 15), "")
	assertSolarOutcomeQueue(t, c, 0, solarOutcomeUTC(12, 30))
	s.dispatchSolarSummary(solarOutcomeUTC(12, 16), solarOutcomeMessage)
	assertSolarOutcomeQueue(t, c, 0, solarOutcomeUTC(12, 30))
	s.dispatchSolarSummary(solarOutcomeUTC(12, 30), solarOutcomeMessage)
	assertSolarOutcomeQueue(t, c, 1, solarOutcomeUTC(12, 45))
	s.dispatchSolarSummary(solarOutcomeUTC(12, 30), solarOutcomeMessage)
	assertSolarOutcomeQueue(t, c, 0, solarOutcomeUTC(12, 45))
}

func TestSolarEmitEmptyActualManagerResult(t *testing.T) {
	for _, availability := range []string{"absent", "disabled", "enabled unobserved"} {
		t.Run(availability, func(t *testing.T) {
			s := &Server{clients: make(map[string]*Client)}
			if availability != "absent" {
				s.solarWeather = solarOutcomeManager(availability == "enabled unobserved")
			}
			c := solarOutcomeDispatchClient(s, 15)
			s.emitSolarSummaries(solarOutcomeUTC(12, 15))
			next := solarOutcomeUTC(12, 30)
			if availability == "absent" {
				next = solarOutcomeUTC(12, 15)
			}
			assertSolarOutcomeQueue(t, c, 0, next)
		})
	}
}

func TestSolarReadbacksPreserveConfiguredAndRuntimeOwners(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, availability := range []string{"absent", "disabled", "enabled unobserved"} {
			t.Run(string(dialect)+"/"+availability, func(t *testing.T) {
				s := presetTestServer(t)
				available := availability == "enabled unobserved"
				if availability != "absent" {
					s.solarWeather = solarOutcomeManager(available)
				}
				clock := solarOutcomeUTC(12, 7)
				s.nowFn = func() time.Time { return clock }
				c := configurationTestClient(s, "W1ABC-14")
				record := &filter.UserRecord{Filter: *testRuleFilter(t), Dialect: string(dialect), NoiseClass: "QUIET", DedupePolicy: "FAST", SolarSummaryMinutes: 15}
				prepared, err := filter.PrepareUserRecord(c.callsign, record)
				if err != nil {
					t.Fatal(err)
				}
				if err := prepared.Commit(); err != nil {
					t.Fatal(err)
				}
				s.restoreClientRecordOwned(c, clock)
				baseline, err := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Preset()
				if err != nil {
					t.Fatal(err)
				}
				c.presetReference = &filter.PresetReference{Name: "SOLAR14", Baseline: baseline}
				if err := c.saveFilter(); err != nil {
					t.Fatal(err)
				}
				assertSolarOutcomeReadOwners(t, s, c, available)
			})
		}
	}
}

func assertSolarOutcomeReadOwners(t *testing.T, s *Server, c *Client, available bool) {
	t.Helper()
	c.advanceSolarSummaryAt(solarOutcomeUTC(12, 31))
	next := solarOutcomeUTC(12, 45)
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	ref, err := c.presetReference.Clone()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pointer, runtime, calls := c.filter, c.pathSnapshot(), 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { calls++; return errors.New("unexpected read commit") }
	effective, row := 0, "Solar         15 minutes; unavailable on this server\r\n"
	if available {
		effective, row = 15, "Solar         Every 15 minutes when conditions are active\r\n"
	}
	proposal := before.Clone()
	proposal.Settings.SolarSummaryMinutes = 30
	staged, _ := s.prepareConfigurationUpdate(c, before, proposal, s.now())
	proposedEffective := 0
	if available {
		proposedEffective = 30
	}
	if staged.getSolarSummaryMinutes() != 30 || s.captureReadbackStatus(c, staged).Effective.SolarSummaryMinutes != proposedEffective {
		t.Fatal("candidate status used the live cadence or changed an unavailable proposed choice")
	}
	for range 2 {
		settings, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
		if err != nil || !strings.Contains(settings, row) {
			t.Fatalf("human availability/choice lost: %q, %v", settings, err)
		}
		assertHumanWire(t, settings)
		{
			response := s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: "CONFIG", SchemaVersion: 5, RequestID: "solar-14"})
			var document struct {
				SchemaVersion int `yaml:"schema_version"`
				Configuration struct {
					Settings filter.SettingsConfiguration `yaml:"settings"`
				} `yaml:"configuration"`
				Status struct {
					Configured filter.SettingsConfiguration `yaml:"configured"`
					Effective  struct {
						SolarSummaryMinutes int `yaml:"solar_summary_minutes"`
					} `yaml:"effective"`
				} `yaml:"status"`
			}
			if err := yaml.Unmarshal([]byte(response), &document); err != nil {
				t.Fatal(err)
			}
			if document.SchemaVersion != 5 || document.Configuration.Settings.SolarSummaryMinutes != 15 ||
				document.Status.Configured.SolarSummaryMinutes != 15 || document.Status.Effective.SolarSummaryMinutes != effective {
				t.Fatalf("schema 5 lost configured/effective solar choice: %q", response)
			}
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(saved, after) || calls != 0 || c.filter != pointer || c.pathSnapshot() != runtime ||
		!reflect.DeepEqual(ref, c.presetReference) || !reflect.DeepEqual(before, filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) ||
		c.getSolarSummaryMinutes() != 15 || !c.solarNextSummaryAt.Equal(next) {
		t.Fatal("read changed configuration, runtime, preset, stable pointer, deadline or disk owner")
	}
	if afterRevision, err := c.configurationRevisionToken(); err != nil || afterRevision != revision {
		t.Fatal("read changed revision owner")
	}
}

func TestSolarPersistenceFailureRetainsChoiceAndConditionalOutcome(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, minutes := range []int{15, 30, 60, 0} {
			t.Run(fmt.Sprintf("enabled=%t/cadence=%d", enabled, minutes), func(t *testing.T) {
				s := presetTestServer(t)
				s.solarWeather = solarOutcomeManager(enabled)
				clock := time.Date(2026, 10, 9, 12, 14, 59, 0, time.UTC)
				s.nowFn = func() time.Time { return clock }
				c := configurationTestClient(s, "W1ABC-14")
				c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST", SolarSummaryMinutes: 30}
				c.configurationInitialized = true
				c.setSolarSummaryMinutes(30, solarOutcomeUTC(12, 7))
				if err := c.saveFilter(); err != nil {
					t.Fatal(err)
				}
				c.advanceSolarSummaryAt(solarOutcomeUTC(12, 30))
				path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
				saved, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				commits := 0
				s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
					commits++
					if prepared == nil || prepared.ByteLen() == 0 {
						t.Error("warning control bypassed preparation")
					}
					return errors.New("blocked")
				}
				token, prefix, next := fmt.Sprint(minutes), fmt.Sprintf("Solar cadence: %d minutes; unavailable on this server", minutes), time.Time{}
				if enabled {
					prefix = fmt.Sprintf("Solar summaries: every %d minutes when conditions are active", minutes)
				}
				switch minutes {
				case 0:
					token, prefix = "OFF", "Solar summaries: OFF"
				case 15:
					next = solarOutcomeUTC(12, 15)
				case 30:
					next = solarOutcomeUTC(12, 30)
				case 60:
					next = solarOutcomeUTC(13, 0)
				}
				response, handled := s.handleSolarCommand(c, "SET SOLAR "+token)
				if !handled || response != prefix+" (warning: failed to persist: blocked)\n" || commits != 1 ||
					c.configuredSettings.SolarSummaryMinutes != minutes || c.getSolarSummaryMinutes() != minutes || !c.solarNextSummaryAt.Equal(next) {
					t.Fatalf("failed save lost active cadence/scheduling or truthful warning: %q, %v", response, c.solarNextSummaryAt)
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(saved, after) {
					t.Fatal("injected commit failure changed saved bytes")
				}
			})
		}
	}
}
