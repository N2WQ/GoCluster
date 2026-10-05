package telnet

import (
	"errors"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

const readbackFooterLiteral = "Live spots paused during delivery and for at least 30s after delivery. Type RESUME to resume now.\r\nMissed spots are not replayed.\r\n"

func readbackTestClient() (*Server, *Client) {
	s := &Server{nowFn: func() time.Time { return time.Unix(1700000000, 0) }}
	c := &Client{
		server: s, callsign: "W1ABC-1", filter: &filter.Filter{}, dialect: DialectGo,
		grid: "FN31", gridDerived: true, noiseClass: "QUIET", configurationInitialized: true,
		done: make(chan struct{}), controlChan: make(chan controlMessage, 8),
	}
	return s, c
}

func TestHumanReadbackLiteralCategoryAndAliases(t *testing.T) {
	for _, command := range []string{"SHOW FILTER BAND", "SHOW/FILTER BAND", "SH/FILTER BAND"} {
		t.Run(command, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter.AllBands = true
			c.filter.Bands = map[string]bool{"40m": false, "20m": true}
			c.filter.BlockBands = map[string]bool{"160m": false}
			if !s.handleHumanReadback(c, command) {
				t.Fatal("readback alias not recognized")
			}
			message := <-c.controlChan
			want := "FILTER for W1ABC-1\r\nPreset: (none)\r\nBAND:\r\n  allow_all: true\r\n  block_all: false\r\n  allow: {\"20m\": true, \"40m\": false}\r\n  block: {\"160m\": false}\r\n" + readbackFooterLiteral
			if string(message.raw) != want || message.line != "" || len(c.controlChan) != 0 {
				t.Fatalf("complete one-message readback mismatch: %q", message.raw)
			}
			if message.readback.epoch == 0 || message.readback.duration != 30*time.Second || !c.readPausePending.Load() {
				t.Fatal("disabled automatic settings did not establish the human delivery hold")
			}
		})
	}
}

func TestHumanReadbackLiteralSettingsDefaults(t *testing.T) {
	s, c := readbackTestClient()
	got, err := s.renderHumanReadback(c, "SETTINGS", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := "SETTINGS for W1ABC-1\r\nPreset: (none)\r\n" +
		"DIALECT: DEFAULT (empty) (effective \"go\")\r\n" +
		"GRID: DEFAULT (empty) (effective \"FN31\")\r\n" +
		"NOISE: DEFAULT (empty) (effective \"QUIET\")\r\n" +
		"DEDUPE: DEFAULT (empty) (effective \"FAST\")\r\n" +
		"GRID source: derived=true\r\nPATHSAMPLES: 0 (0=cluster default; effective 0)\r\n" +
		"SOLAR: 0 minutes (0=OFF)\r\nDIAG: OFF (session only)\r\n" +
		"Live spots: paused=false; delivery pending=false; remaining=0s; suppressed=0\r\n" +
		"Persistence: temporary defaults=false\r\n" +
		"Server defaults: dialect=go dedupe=FAST noise=QUIET PATHSAMPLES=0\r\n" + readbackFooterLiteral
	if got != want {
		t.Fatalf("human settings mismatch: %q", got)
	}
}

func TestHumanEventReadbackExplainsFalseKeyPresence(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.AllEvents = false
	c.filter.Events = map[string]bool{"POTA": false}
	c.filter.BlockEvents = map[string]bool{"SOTA": false}
	detail := "EVENT:\r\n  allow_all: false\r\n  block_all: false\r\n  allow: {\"POTA\": false}\r\n  block: {\"SOTA\": false}\r\n  note: EVENT rules use key presence; false does not remove a rule.\r\n"
	got, err := s.renderHumanReadback(c, "FILTER", "EVENT", 30*time.Second)
	want := "FILTER for W1ABC-1\r\nPreset: (none)\r\n" + detail + readbackFooterLiteral
	if err != nil || got != want {
		t.Fatalf("EVENT category mismatch: %q, %v", got, err)
	}
	got, err = s.renderHumanReadback(c, "FILTER", "FULL", 30*time.Second)
	if err != nil || !strings.Contains(got, detail) {
		t.Fatalf("FULL omitted EVENT explanation: %q, %v", got, err)
	}
	got, err = s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
	want = "EVENT: allow_all=false block_all=false; allow=0/1 true; block=0/1 true\r\n  note: EVENT rules use key presence; false does not remove a rule.\r\n"
	if err != nil || !strings.Contains(got, want) {
		t.Fatalf("overview described stored false keys as disabled: %q, %v", got, err)
	}
	for _, tc := range []struct {
		name   string
		events spot.EventMask
		want   bool
	}{
		{name: "false allow key", events: spot.EventPOTA, want: true},
		{name: "false block key", events: spot.EventPOTA | spot.EventSOTA, want: false},
		{name: "absent allow key", events: spot.EventIOTA, want: false},
		{name: "eventless", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
			candidate.Events = tc.events
			if c.filter.Matches(candidate) != tc.want {
				t.Fatalf("existing EVENT matching disagrees with readback for %s", tc.name)
			}
		})
	}
}

func TestHumanReadbackOverviewCountsOversizedRulesWithoutDetail(t *testing.T) {
	s, c := readbackTestClient()
	c.filter.AllBands = true
	c.filter.Bands = map[string]bool{"40m": false, "20m": true}
	c.filter.BlockBands = map[string]bool{"160m": false}
	c.filter.DXCallsigns = []string{strings.Repeat("X", 1024*1024)}
	got, err := s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > 4096 || strings.Contains(got, "XXXX") || !strings.Contains(got, "BAND: allow_all=true block_all=false; allow=1/2 enabled; block=0/1 enabled\r\n") || !strings.Contains(got, "DXCALL: allow=1 patterns; block=0 patterns\r\n") {
		t.Fatalf("compact readback did not use bounded counts: length=%d", len(got))
	}
	for _, resource := range []string{"FILTER", "CONFIG"} {
		if response, err := s.renderYAMLReadback(c, resource, "read-1", "session-0"); response != "" || !errors.Is(err, errReadbackTooLarge) {
			t.Fatalf("oversized %s returned partial success: %q, %v", resource, response, err)
		}
	}
	if response, err := s.renderHumanReadback(c, "FILTER", "FULL", 30*time.Second); response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("oversized FULL must fail without truncated output")
	}
	if response, err := s.renderHumanReadback(c, "FILTER", "BAND", 30*time.Second); err != nil || !strings.Contains(response, "\"40m\": false") {
		t.Fatal("bounded category was rejected because an unrelated category is oversized")
	}
	if _, err := s.renderYAMLReadback(c, "SETTINGS", "read-1", "session-0"); err != nil {
		t.Fatal("bounded SETTINGS was rejected because FILTER is oversized")
	}
}

func TestYAMLReadbackLiteralSettingsAndNoPauseEffects(t *testing.T) {
	s, c := readbackTestClient()
	c.readPauseUntilUnixNano.Store(1700000030000000000)
	c.readPauseDiscardBefore.Store(1700000000000000000)
	c.readPauseSuppressed.Store(7)
	got, err := s.renderYAMLReadback(c, "SETTINGS", "noise-1", "session-0")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.ReplaceAll(`---
schema_version: 1
request_id: noise-1
resource: SETTINGS
revision: session-0
configuration:
  dialect: ""
  grid: ""
  noise_class: ""
  dedupe_policy: ""
  path_min_observation_count: 0
  solar_summary_minutes: 0
status:
  configured:
    dialect: ""
    grid: ""
    noise_class: ""
    dedupe_policy: ""
    path_min_observation_count: 0
    solar_summary_minutes: 0
  effective:
    dialect: go
    grid: FN31
    grid_derived: true
    noise_class: QUIET
    dedupe_policy: FAST
    path_min_observation_count: 0
    solar_summary_minutes: 0
    nearby_active: false
  session:
    callsign: W1ABC-1
    diagnostic_comments: "OFF"
    pause_active: true
    pause_pending_delivery: false
    pause_remaining_seconds: 30
    suppressed_spots: 7
    temporary_defaults: false
  server:
    default_dialect: go
    default_dedupe_policy: FAST
    default_noise_class: QUIET
    path_min_observation_count: 0
    auto_read_pause_min_rows: 0
    auto_read_pause_seconds: 0
  preset:
    associated: false
    name: ""
    modified: false
...
`, "\n", "\r\n")
	if got != want {
		t.Fatalf("YAML settings mismatch:\n%s", got)
	}
	_ = renderYAMLCommandError("SETTINGS", "noise-1", "session-0", "invalid_configuration", "Invalid value.")
	if c.readPauseUntilUnixNano.Load() != 1700000030000000000 || c.readPauseDiscardBefore.Load() != 1700000000000000000 || c.readPauseSuppressed.Load() != 7 || c.readPausePending.Load() || c.readPauseEpoch != 0 {
		t.Fatal("YAML success or error changed pause state")
	}
}

func TestYAMLReadbackExactValuesAndPresetComparison(t *testing.T) {
	s, c := readbackTestClient()
	c.filter.Bands = map[string]bool{"40m": false, "20m": true}
	c.filter.DXZones = map[int]bool{3: false, 14: true}
	c.filter.DXCallsigns = []string{"Z*", "W1*", "W1*"}
	c.filter.IncludeBeacons = nil
	c.filter.AllowWWV = filter.DefaultBoolFalse.Pointer()
	c.filter.AllowSelf = filter.DefaultBoolTrue.Pointer()
	c.presetReference = &filter.PresetReference{Name: "CONTEST", Baseline: &filter.SavedPreset{Filter: *c.filter}}
	c.presetReference.Baseline.DXCallsigns = []string{"W1*", "Z*", "W1*"}
	for _, resource := range []string{"FILTER", "CONFIG"} {
		got, err := s.renderYAMLReadback(c, resource, "read-1", "session-0")
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(got), &doc); err != nil {
			t.Fatal(err)
		}
		cfg := doc["configuration"].(map[string]any)
		if resource == "CONFIG" {
			if len(cfg["settings"].(map[string]any)) != 6 {
				t.Fatal("CONFIG omitted a writable setting")
			}
			cfg = cfg["filters"].(map[string]any)
		}
		if len(cfg) != 25 || cfg["include_beacons"] != "DEFAULT" || cfg["allow_wwv"] != false || cfg["allow_self"] != true || cfg["nearby_enabled"] != false {
			t.Fatalf("exact filter schema/default selections lost: %v", cfg)
		}
		bands := cfg["bands"].(map[string]any)
		if len(bands) != 4 || !reflect.DeepEqual(bands["allow"], map[string]any{"20m": true, "40m": false}) || bands["allow_all"] != false || bands["block_all"] != false {
			t.Fatal("false map entries or all-selection flags were lost")
		}
		zones := cfg["dx_zones"].(map[string]any)["allow"].(map[any]any)
		if zones[3] != false || zones[14] != true || len(zones) != 2 {
			t.Fatal("integer-key false selections were lost")
		}
		if !reflect.DeepEqual(cfg["dx_callsigns"], []any{"Z*", "W1*", "W1*"}) {
			t.Fatal("ordered callsign list or multiplicity changed")
		}
		preset := doc["status"].(map[string]any)["preset"].(map[string]any)
		if len(preset) != 3 || preset["name"] != "CONTEST" || preset["modified"] != false || strings.Contains(got, "baseline") {
			t.Fatal("preset status comparison exposed baseline or treated order as modified")
		}
	}
	c.filter.DXCallsigns = []string{"W1*", "Z*"}
	got, err := s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
	if err != nil || !strings.Contains(got, "Preset: CONTEST (modified)\r\n") || !strings.Contains(got, "allow: [\"W1*\", \"Z*\"]") {
		t.Fatal("pattern multiplicity change was not described exactly")
	}
}

func TestYAMLCommandLiteralFramedReplies(t *testing.T) {
	got := renderYAMLCommandError("CONFIG", "edit-1", "session-0", "revision_conflict", "GET again before retrying.")
	want := "---\r\nschema_version: 1\r\nrequest_id: edit-1\r\nresource: CONFIG\r\nrevision: session-0\r\nerror:\r\n  code: revision_conflict\r\n  message: GET again before retrying.\r\n...\r\n"
	if got != want {
		t.Fatalf("literal error mismatch: %q", got)
	}
	got, err := renderYAMLCommandSuccess("CONFIG", "edit-1", "session-0", "PUT", true, true)
	want = "---\r\nschema_version: 1\r\nrequest_id: edit-1\r\nresource: CONFIG\r\nrevision: session-0\r\nresult:\r\n  operation: PUT\r\n  valid: true\r\n  applied: true\r\n  persisted: true\r\n...\r\n"
	if err != nil || got != want {
		t.Fatalf("literal success mismatch: %q %v", got, err)
	}
	if response, err := renderYAMLCommandSuccess("CONFIG", "edit-1", "session-0", strings.Repeat("X", 65537), true, true); response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("oversized success scalar must fail before encoding")
	}
}

func TestReadbackBoundedWriterExactFinalBytes(t *testing.T) {
	var b boundedResponse
	if n, err := b.Write([]byte(strings.Repeat("X", 65534))); n != 65534 || err != nil {
		t.Fatal("exact-limit preparation rejected")
	}
	if n, err := b.Write([]byte("\n")); n != 1 || err != nil || len(b.data) != 65536 || cap(b.data) > 65536 || string(b.data[65534:]) != "\r\n" {
		t.Fatal("CRLF conversion failed exact final-byte limit")
	}
	if n, err := b.Write([]byte("X")); n != 0 || !errors.Is(err, errReadbackTooLarge) || len(b.data) != 65536 {
		t.Fatal("limit-plus-one modified or accepted the bounded result")
	}
	var split boundedResponse
	_, _ = split.Write([]byte("A\r"))
	_, _ = split.Write([]byte("\nB\n"))
	if string(split.data) != "A\r\nB\r\n" {
		t.Fatal("split existing CRLF was expanded twice")
	}
}

func TestYAMLReadbackFinalEnvelopeBoundary(t *testing.T) {
	// The literal document has 32 non-payload bytes after CRLF conversion:
	// marker(5), key(7), break(2), tail key/value(11), break(2), end(5).
	// Include a safe unquoted scalar; no encoder-derived length is an oracle.
	for _, size := range []int{65504, 65505} {
		value := strings.Repeat("X", size)
		response, err := encodeBoundedYAML(struct {
			Value string `yaml:"value"`
			Tail  bool   `yaml:"tail"`
		}{value, false})
		if size == 65504 {
			if err != nil || len(response) != 65536 || !strings.HasPrefix(response, "---\r\nvalue: ") || !strings.HasSuffix(response, "\r\ntail: false\r\n...\r\n") {
				t.Fatalf("exact YAML boundary mismatch: length=%d error=%v", len(response), err)
			}
		} else if response != "" || !errors.Is(err, errReadbackTooLarge) || machineErrorCode(err) != "response_too_large" {
			t.Fatal("encoder-wrapped overflow lost its typed error or returned partial output")
		}
	}
}

func TestHumanReadbackHoldStartsBeforeStripeWait(t *testing.T) {
	s, c := readbackTestClient()
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan bool, 1)
	go func() { finished <- s.handleHumanReadback(c, "SHOW FILTER BAND") }()
	deadline := time.Now().Add(3 * time.Second)
	for !c.readPausePending.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !c.readPausePending.Load() || len(c.controlChan) != 0 {
		release()
		t.Fatal("human pause did not precede preparation/ownership wait")
	}
	if !c.suppressSpotForReadPause(&spotEnvelope{enqueueAt: time.Unix(1700000000, 0)}, time.Unix(1700000000, 0)) {
		release()
		t.Fatal("live traffic passed the pending preparation hold")
	}
	release()
	select {
	case recognized := <-finished:
		if !recognized || len(c.controlChan) != 1 || c.readPauseSuppressed.Load() != 1 {
			t.Fatal("human readback did not enqueue exactly one complete response")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("human readback deadlocked after acquiring ownership")
	}
}

func TestHumanReadbackSizeErrorsPauseAndAliasValidation(t *testing.T) {
	s, c := readbackTestClient()
	c.filter.DXCallsigns = []string{strings.Repeat("X", 65537)}
	if !s.handleHumanReadback(c, "SHOW FILTER FULL") {
		t.Fatal("FULL not recognized")
	}
	response := <-c.controlChan
	want := "Readback failed: response exceeds 65,536 bytes; request a smaller filter category or reduce the configuration\r\n" + readbackFooterLiteral
	if string(response.raw) != want || response.readback.epoch == 0 || !c.readPausePending.Load() {
		t.Fatal("human size error omitted the reading pause or returned partial success")
	}
	for _, command := range []string{"SHOW FILTER CONF", "SHOW FILTER PC93", "SHOW FILTER UNKNOWN", "SHOW SETTINGS EXTRA", "SHOW/FILTER BAND EXTRA"} {
		if !s.handleHumanReadback(c, command) || len(c.controlChan) != 1 {
			t.Fatal("human alias/error did not yield a single complete response")
		}
		message := <-c.controlChan
		if !strings.HasSuffix(string(message.raw), readbackFooterLiteral) || message.readback.epoch == 0 {
			t.Fatal("human alias/error omitted readback pause metadata")
		}
	}
	if s.handleHumanReadback(c, "SHOW DX") {
		t.Fatal("readback handler consumed unrelated commands")
	}
}

func TestReadbackAdmissionReservesMetadataGrowth(t *testing.T) {
	s, c := readbackTestClient()
	base := filter.Configuration{Filters: filter.FilterConfiguration{DXCallsigns: []string{strings.Repeat("X", 60000)}}}
	prepared, _ := s.prepareConfiguration(c, base, time.Unix(1700000000, 0))
	if err := s.configurationReadbackFits(c, base, prepared); err != nil {
		t.Fatal(err)
	}
	// Actual candidate runtime may include a legacy lookup locator longer than
	// six characters. Admission must not substitute a shorter locator to pass.
	prepared.grid = strings.Repeat("A", 6000)
	if err := s.configurationReadbackFits(c, base, prepared); !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("candidate admission ignored actual effective GRID width")
	}
	c.filter.DXCallsigns = []string{strings.Repeat("X", 60000)}
	c.readPauseSuppressed.Store(math.MaxUint64)
	c.presetReference = &filter.PresetReference{Name: strings.Repeat("1", 32)}
	response, err := s.renderYAMLReadback(c, "CONFIG", strings.Repeat("1", 32), strings.Repeat("\"", 128))
	if err != nil || len(response) > 65536 {
		t.Fatal("admitted candidate failed with longest request/revision/status metadata")
	}
}

func TestYAMLReadbackUnchangedConfigurationEnvelopeGrowth(t *testing.T) {
	s, c := readbackTestClient()
	// This independent literal schema fixes the byte count. It does not ask the
	// encoder or production normalizer to calculate its own expected boundary.
	literal := "---\r\nschema_version: 1\r\nrequest_id: x\r\nresource: CONFIG\r\nrevision: session-0\r\nconfiguration:\r\n  filters:\r\n"
	for _, field := range []string{"bands", "modes", "sources", "events", "confidence", "path_classes", "dx_continents", "de_continents", "dx_zones", "de_zones", "dx_grid2", "de_grid2", "dx_dxcc", "de_dxcc"} {
		literal += "    " + field + ":\r\n      allow_all: false\r\n      block_all: false\r\n      allow: {}\r\n      block: {}\r\n"
	}
	literal += strings.ReplaceAll(`    dx_callsigns:
      - PAYLOAD
    block_dx_callsigns: []
    de_callsigns: []
    block_de_callsigns: []
    include_beacons: DEFAULT
    allow_wwv: DEFAULT
    allow_wcy: DEFAULT
    allow_announce: DEFAULT
    allow_self: DEFAULT
    allow_toxic: DEFAULT
    nearby_enabled: false
  settings:
    dialect: ""
    grid: ""
    noise_class: ""
    dedupe_policy: ""
    path_min_observation_count: 0
    solar_summary_minutes: 0
status:
  configured:
    dialect: ""
    grid: ""
    noise_class: ""
    dedupe_policy: ""
    path_min_observation_count: 0
    solar_summary_minutes: 0
  effective:
    dialect: go
    grid: FN31
    grid_derived: true
    noise_class: QUIET
    dedupe_policy: FAST
    path_min_observation_count: 0
    solar_summary_minutes: 0
    nearby_active: false
  session:
    callsign: W1ABC-1
    diagnostic_comments: "OFF"
    pause_active: false
    pause_pending_delivery: false
    pause_remaining_seconds: 0
    suppressed_spots: 0
    temporary_defaults: false
  server:
    default_dialect: go
    default_dedupe_policy: FAST
    default_noise_class: QUIET
    path_min_observation_count: 0
    auto_read_pause_min_rows: 0
    auto_read_pause_seconds: 0
  preset:
    associated: false
    name: ""
    modified: false
...
`, "\n", "\r\n")
	payload := strings.Repeat("X", 65536-(len(literal)-len("PAYLOAD")))
	want := strings.Replace(literal, "PAYLOAD", payload, 1)
	c.filter.DXCallsigns = []string{payload}
	response, err := s.renderYAMLReadback(c, "CONFIG", "x", "session-0")
	if err != nil || response != want || len(response) != 65536 {
		t.Fatalf("literal complete CONFIG boundary mismatch: length=%d expected=%d error=%v", len(response), len(want), err)
	}
	if response, err := s.renderYAMLReadback(c, "CONFIG", strings.Repeat("1", 32), "session-0"); response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("unchanged configuration did not account for grown, quoted request ID")
	}
	if err := s.configurationReadbackFits(c, filter.ConfigurationFromFilter(c.filter, c.configuredSettings), c); !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("write admission did not reserve worst-case envelope metadata")
	}
}

func TestReadbackCapabilitiesActiveChoicesAndExactFields(t *testing.T) {
	s, c := readbackTestClient()
	s.dedupeFastEnabled, s.dedupeMedEnabled, s.dedupeSlowEnabled = true, true, false
	s.noiseModel = pathreliability.DefaultConfig().NoiseModel()
	response, err := s.renderYAMLReadback(c, "CAPABILITIES", "caps-1", "session-0")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(response), &doc); err != nil {
		t.Fatal(err)
	}
	capabilities := doc["configuration"].(map[string]any)
	for _, name := range []string{"response_bytes", "upload_bytes"} {
		if capabilities[name] != 65536 {
			t.Fatalf("wrong capability %s", name)
		}
	}
	if len(capabilities["filter_fields"].([]any)) != 25 || len(capabilities["settings"].([]any)) != 6 || len(capabilities["commands"].([]any)) != 11 || capabilities["upload_seconds"] != 30 {
		t.Fatal("capabilities omitted writable schema or machine commands")
	}
	if !reflect.DeepEqual(capabilities["dedupe_available"], map[string]any{"FAST": true, "MED": true, "SLOW": false}) {
		t.Fatal("capabilities did not distinguish a recognized disabled dedupe choice")
	}
	choices := capabilities["choices"].(map[string]any)
	if !reflect.DeepEqual(choices["noise_classes"], []any{"", "QUIET", "RURAL", "SUBURBAN", "URBAN", "INDUSTRIAL"}) || !reflect.DeepEqual(choices["solar_summary_minutes"], []any{0, 15, 30, 60}) {
		t.Fatal("server setting choices are incomplete")
	}
	for _, name := range []string{"bands", "modes", "sources", "events", "confidence", "path_classes", "continents"} {
		if len(choices[name].([]any)) == 0 {
			t.Fatalf("missing %s choices", name)
		}
	}
	if strings.Contains(response, "baseline") || strings.Contains(response, "Type RESUME") {
		t.Fatal("machine capabilities exposed private baseline or human pause footer")
	}
}

func BenchmarkReadbackOverviewOversizedPattern(b *testing.B) {
	s, c := readbackTestClient()
	c.filter.DXCallsigns = []string{strings.Repeat("X", 1024*1024)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		response, err := s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
		if err != nil || len(response) > 4096 {
			b.Fatalf("overview length=%d error=%v", len(response), err)
		}
	}
}
