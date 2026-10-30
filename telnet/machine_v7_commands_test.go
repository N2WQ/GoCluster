package telnet

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/synctest"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func v7MachineTestClient(t *testing.T) (*Server, *Client) {
	t.Helper()
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	cfg, _ := v7PreparedFixture(t)
	cfg.Settings = filter.SettingsConfiguration{Dialect: "cc", NoiseClass: "QUIET", DedupePolicy: "FAST"}
	canonical, _, ccRuntime, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.filterBanks = &canonical.Filters
	c.filter = &ccRuntime
	c.dialect = DialectCC
	c.configuredSettings = canonical.Settings
	c.configurationInitialized = true
	c.noiseClass = "QUIET"
	c.setDedupePolicy(dedupePolicyFast)
	return s, c
}

func v7ProposalFixture(t *testing.T, c *Client, verb, resource, configuration string) (machineCommand, v7MachineRequest) {
	t.Helper()
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	body := string(v7MachineRequestFixture(configuration, true))
	body = strings.Replace(body, "opaque-revision-1", revision, 1)
	command := machineCommand{Verb: verb, Resource: resource, SchemaVersion: 7}
	request, err := decodeV7MachineRequest([]byte(body), command, nil)
	if err != nil {
		t.Fatal(err)
	}
	return command, request
}

func TestV7MachineCommandInvalidHeaderPreservesSelectedVersion(t *testing.T) {
	for _, tc := range []struct {
		line     string
		version  int
		terminal bool
	}{
		{"GET YAML CONFIG SCHEMA 6 ID bad_id", 6, false},
		{"GET YAML CONFIG SCHEMA 7 ID bad_id", 7, false},
		{"GET YAML CONFIG", 6, false},
		{"PUT YAML CONFIG extra", 6, true},
		{"PUT YAML CONFIG SCHEMA 7 extra", 7, true},
	} {
		t.Run(tc.line, func(t *testing.T) {
			s, c := v7MachineTestClient(t)
			result := s.handleV7MachineCommand(c, tc.line)
			if !result.handled || result.terminal != tc.terminal || result.closeQueued != tc.terminal {
				t.Fatalf("invalid-header disposition=%+v", result)
			}
			message := <-c.controlChan
			var document struct {
				Version int `yaml:"schema_version"`
				Error   struct {
					Code string `yaml:"code"`
				} `yaml:"error"`
			}
			if err := yaml.Unmarshal(message.raw, &document); err != nil {
				t.Fatal(err)
			}
			if document.Version != tc.version || document.Error.Code != "invalid_header" || message.closeAfter != tc.terminal || message.configurationReservation != nil {
				t.Fatalf("wrong protocol or owner in malformed-header error: %q", message.raw)
			}
		})
	}
}

func TestV7MachineCommandCommitFailureIsAtomic(t *testing.T) {
	s, c := v7MachineTestClient(t)
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	commits := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return os.ErrPermission }
	cmd, request := v7ProposalFixture(t, c, "PATCH", "FILTER", "dialect: cc\nfilters: {modes: []}\n")
	response := s.applyV7MachineRequest(c, cmd, request)
	after, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	afterRevision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	if commits != 1 || !strings.Contains(response, "code: persistence_failed") || !after.Equal(before) || afterRevision != revision {
		t.Fatalf("failed commit changed live state/revision or skipped commit seam: %q", response)
	}
}

func TestV7MachineCommandValidateProtectedIsReadOnly(t *testing.T) {
	s, c := v7MachineTestClient(t)
	c.recordProtected = true
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := filter.EncodeV7Configuration(before)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	commits := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
	cmd, request := v7ProposalFixture(t, c, "VALIDATE", "CONFIG", string(configuration))
	response := s.applyV7MachineRequest(c, cmd, request)
	after, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	afterRevision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	if commits != 0 || !strings.Contains(response, "valid: true") || !strings.Contains(response, "persisted: false") || !after.Equal(before) || afterRevision != revision {
		t.Fatalf("VALIDATE changed state or refused temporary profile: %q", response)
	}
	entries, err := os.ReadDir(filter.UserDataDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("VALIDATE created/reset files: entries=%d err=%v", len(entries), err)
	}
}

func TestV7MachineCommandReadbackBusyDoesNotPrepareSecondFrame(t *testing.T) {
	s, c := v7MachineTestClient(t)
	initialInput, initialOutput := configurationTransferBudgetStats()
	first := s.handleV7MachineCommand(c, "GET YAML CONFIG SCHEMA 7 ID first-1")
	if !first.handled || first.terminal {
		t.Fatalf("first GET failed: %+v", first)
	}
	msg := <-c.controlChan
	if msg.configurationReservation == nil {
		t.Fatal("GET queued without its output owner")
	}
	t.Cleanup(func() {
		msg.raw = nil
		msg.configurationReservation.Release()
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	})
	// Deliberately unusable capture state distinguishes an early busy refusal
	// from a second readback preparation, without a production mutable hook.
	c.filterBanks = nil
	c.filter = nil
	second := s.handleV7MachineCommand(c, "GET YAML CONFIG SCHEMA 7 ID second-1")
	if !second.handled || second.terminal {
		t.Fatalf("busy GET became terminal: %+v", second)
	}
	busy := <-c.controlChan
	if busy.configurationReservation != nil || !bytes.Contains(busy.raw, []byte("code: configuration_busy")) {
		t.Fatalf("second GET built/charged a frame: %q", busy.raw)
	}
	assertConfigurationTransferStats(t, initialInput, initialOutput+(1<<20))
}

func TestV7MachineCommandRevisionIncludesInactiveBank(t *testing.T) {
	s, c := v7MachineTestClient(t)
	cmd, request := v7ProposalFixture(t, c, "PATCH", "FILTER", "dialect: cc\nfilters: {modes: []}\n")
	old := request.IfRevision
	copy := *c.filterBanks
	copy.GO.AllowSelf = filter.DefaultBoolTrue
	c.filterBanks = &copy
	response := s.applyV7MachineRequest(c, cmd, request)
	if !strings.Contains(response, "code: revision_conflict") {
		t.Fatalf("inactive-bank edit did not invalidate stale token %q: %q", old, response)
	}
	entries, err := os.ReadDir(filter.UserDataDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("revision rejection wrote profile data: entries=%d err=%v", len(entries), err)
	}
}

func TestV7MachineCommandNinthOutputIsBusyBeforeCapture(t *testing.T) {
	initialInput, initialOutput := configurationTransferBudgetStats()
	var owners []*configurationOutputReservation
	t.Cleanup(func() {
		for _, owner := range owners {
			owner.Release()
		}
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	})
	for range 8 {
		owner, err := (&Client{}).reserveConfigurationOutput()
		if err != nil {
			t.Fatal(err)
		}
		owners = append(owners, owner)
	}
	s := &Server{}
	c := configurationTestClient(s, "W1ABC-2")
	c.filter = nil // Preparation is deliberately impossible in this fixture.
	result := s.handleV7MachineCommand(c, "GET YAML CONFIG SCHEMA 7 ID ninth-1")
	if !result.handled || result.terminal {
		t.Fatalf("budget refusal became terminal: %+v", result)
	}
	msg := <-c.controlChan
	if msg.configurationReservation != nil || !bytes.Contains(msg.raw, []byte("code: configuration_busy")) {
		t.Fatalf("ninth GET reached capture/rendering: %q", msg.raw)
	}
	assertConfigurationTransferStats(t, initialInput, initialOutput+(8<<20))
}

func TestV7MachineCommandInputChargeSurvivesDecoderWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		initialInput, initialOutput := configurationTransferBudgetStats()
		s := &Server{shutdown: make(chan struct{})}
		var releases []func()
		defer func() {
			for _, release := range releases {
				release()
			}
		}()
		for range 4 {
			release, err := s.acquireYAMLPreparation(&Client{done: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			releases = append(releases, release)
		}
		body := string(v7MachineRequestFixture("dialect: cc\nfilters: {modes: []}\n", true))
		c, _ := newYAMLTestClient("---\n" + body + "...\nRESUME\n")
		c.server = s
		done := make(chan error, 1)
		go func() {
			_, terminal, err := s.receiveV7MachineRequest(c, machineCommand{Verb: "PATCH", Resource: "FILTER", SchemaVersion: 7})
			if terminal {
				done <- errors.New("complete body was treated as unread")
			} else {
				done <- err
			}
		}()
		// The fake connection is fully readable. Quiescence therefore proves the
		// completed body is waiting on the four occupied decoder permits.
		synctest.Wait()
		assertConfigurationTransferStats(t, initialInput+(256<<10), initialOutput)
		if len(s.yamlPreparation) != 4 {
			t.Fatal("decoder concurrency changed")
		}
		c.interrupt()
		synctest.Wait()
		if err := <-done; !errors.Is(err, errClientClosed) {
			t.Fatalf("canceled decoder waiter returned %v", err)
		}
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	})
}
