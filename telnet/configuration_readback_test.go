package telnet

import (
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

const readbackFooterLiteral = "\r\nLive spots paused during delivery and for at least 30s afterward.\r\nType RESUME when ready. Missed spots are not replayed.\r\n"

func readbackTestClient() (*Server, *Client) {
	s := &Server{nowFn: func() time.Time { return time.Unix(1700000000, 0) }}
	c := &Client{
		server: s, callsign: "W1ABC-1", filter: unrestrictedTestFilter(), dialect: DialectGo,
		grid: "FN31", gridDerived: true, noiseClass: "QUIET", configurationInitialized: true,
		done: make(chan struct{}), controlChan: make(chan controlMessage, 8),
	}
	return s, c
}

func TestHumanReadbackLiteralSettingsDefaults(t *testing.T) {
	s, c := readbackTestClient()
	got, err := s.renderHumanReadback(c, "SETTINGS", "", 0)
	want := "User          W1ABC-1\r\nPreset        (none)\r\n\r\n" +
		"Dialect       DEFAULT; effective GO\r\n" +
		"Grid          DEFAULT; using FN31 from callsign lookup\r\n" +
		"Noise         DEFAULT; effective QUIET\r\n" +
		"Dedupe        DEFAULT; secondary duplicate suppression disabled\r\n" +
		"Path samples  DEFAULT; prediction unavailable\r\n" +
		"Solar         Off\r\n\r\nSession only\r\nDiagnostics   Off\r\nLive spots    Flowing\r\n" + readbackFooterLiteral
	if err != nil || got != want {
		t.Fatalf("human settings mismatch: %q, %v", got, err)
	}
}

func TestHumanReadbackBooleanAliases(t *testing.T) {
	for _, command := range []string{"SHOW FILTER RULE", "SHOW/FILTER RULE", "SH/FILTER RULE", "SHOW FILTER FULL"} {
		s, c := readbackTestClient()
		c.filter = passRuleTestFilter(t, "BAND=20M")
		if !s.handleHumanReadback(c, command) {
			t.Fatalf("alias not recognized: %s", command)
		}
		message := <-c.controlChan
		body := string(message.raw)
		assertHumanWire(t, body)
		if booleanDisplayedExpression(t, body, "PASS RULE 0") != "BAND=20M" ||
			message.line != "" || len(c.controlChan) != 0 || message.readback.epoch == 0 ||
			message.readback.duration != 30*time.Second || !c.readPausePending.Load() {
			t.Fatal("readback lost canonical rule, single-response ownership or hold")
		}
	}
}

func TestYAMLReadbackLiteralSettingsAndNoPauseEffects(t *testing.T) {
	s, c := readbackTestClient()
	c.readPauseUntilUnixNano.Store(1700000030000000000)
	c.readPauseDiscardBefore.Store(1700000000000000000)
	c.readPauseSuppressed.Store(7)
	before := machineEchoCoveragePause(c)
	reply := s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: "SETTINGS", RequestID: "noise-1", SchemaVersion: 5})
	var document struct {
		Version       int                          `yaml:"schema_version"`
		RequestID     string                       `yaml:"request_id"`
		Configuration filter.SettingsConfiguration `yaml:"configuration"`
	}
	if err := yaml.Unmarshal([]byte(reply), &document); err != nil || document.Version != 5 ||
		document.RequestID != "noise-1" || document.Configuration != (filter.SettingsConfiguration{}) ||
		machineEchoCoveragePause(c) != before || strings.Contains(reply, "Type RESUME") {
		t.Fatalf("GET changed exact defaults/pause or returned wrong schema: %v\n%s", err, reply)
	}
}

func TestReadbackBoundedWriterExactFinalBytes(t *testing.T) {
	var response boundedResponse
	if n, err := response.Write([]byte(strings.Repeat("X", 65534))); n != 65534 || err != nil {
		t.Fatal("exact-limit preparation rejected")
	}
	if n, err := response.Write([]byte("\n")); n != 1 || err != nil || len(response.data) != 65536 ||
		cap(response.data) > 65536 || string(response.data[65534:]) != "\r\n" {
		t.Fatal("CRLF conversion failed exact final-byte limit")
	}
	if n, err := response.Write([]byte("X")); n != 0 || !errors.Is(err, errReadbackTooLarge) || len(response.data) != 65536 {
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
	for _, size := range []int{65504, 65505} {
		response, err := encodeBoundedYAML(struct {
			Value string `yaml:"value"`
			Tail  bool   `yaml:"tail"`
		}{strings.Repeat("X", size), false})
		if size == 65504 {
			if err != nil || len(response) != 65536 || !strings.HasPrefix(response, "---\r\nvalue: ") ||
				!strings.HasSuffix(response, "\r\ntail: false\r\n...\r\n") {
				t.Fatalf("exact YAML boundary mismatch: length=%d error=%v", len(response), err)
			}
		} else if response != "" || !errors.Is(err, errReadbackTooLarge) || machineErrorCode(err) != "response_too_large" {
			t.Fatal("encoder-wrapped overflow lost typed error or returned partial output")
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
	go func() { finished <- s.handleHumanReadback(c, "SHOW FILTER RULE") }()
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

func TestHumanReadbackAliasValidationKeepsPause(t *testing.T) {
	s, c := readbackTestClient()
	for _, command := range []string{"SHOW FILTER BAND", "SHOW FILTER PC93", "SHOW FILTER UNKNOWN",
		"SHOW SETTINGS EXTRA", "SHOW/FILTER RULE EXTRA"} {
		if !s.handleHumanReadback(c, command) || len(c.controlChan) != 1 {
			t.Fatal("human alias/error did not yield a single complete response")
		}
		message := <-c.controlChan
		if !strings.HasSuffix(string(message.raw), readbackFooterLiteral) || message.readback.epoch == 0 {
			t.Fatal("human error omitted readback pause metadata")
		}
	}
	if s.handleHumanReadback(c, "SHOW DX") {
		t.Fatal("readback handler consumed unrelated commands")
	}
}

func TestReadbackAdmissionReservesActualV5MetadataGrowth(t *testing.T) {
	s, c := readbackTestClient()
	cfg := c.captureV5Configuration()
	cfg.Settings.Grid = strings.Repeat("X", 29000)
	prepared := newHumanPreferenceCandidate(c)
	prepared.configuredSettings = cfg.Settings
	if err := s.v5ConfigurationReadbackFits(c, cfg, prepared, nil); err != nil {
		t.Fatalf("fitting v5 envelope refused: %v", err)
	}
	prepared.grid = strings.Repeat("A", 10000)
	if err := s.v5ConfigurationReadbackFits(c, cfg, prepared, nil); !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("v5 admission ignored actual effective GRID width")
	}
	c.configuredSettings = cfg.Settings
	c.readPauseSuppressed.Store(math.MaxUint64)
	reply := s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: "CONFIG", RequestID: strings.Repeat("1", 32), SchemaVersion: 5})
	if len(reply) > 65536 || strings.Contains(reply, "error:") || !strings.Contains(reply, "schema_version: 5") {
		t.Fatal("admitted v5 configuration failed with widest ordinary request/session metadata")
	}
}
