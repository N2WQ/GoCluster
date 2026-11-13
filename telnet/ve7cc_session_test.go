package telnet

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"unicode"

	"dxcluster/filter"
)

// ve7ccTakeControl transfers the message, NOT its output reservation. A caller
// taking a non-selector reply must abandon its raw references and Release once.
// Selector acknowledgements themselves have no reservation.
func ve7ccTakeControl(t testing.TB, c *Client) controlMessage {
	t.Helper()
	select {
	case message := <-c.controlChan:
		return message
	default:
		t.Fatal("VE7CC command queued no response")
		return controlMessage{}
	}
}

// ve7ccSelect consumes a selector reply; it does not release output ownership.
func ve7ccSelect(t testing.TB, s *Server, c *Client, line string) controlMessage {
	t.Helper()
	if handled, proceed := s.handleVE7CCCommand(c, line); !handled || !proceed {
		t.Fatalf("%q: handled=%v proceed=%v", line, handled, proceed)
	}
	return ve7ccTakeControl(t, c)
}

func TestVE7CCSessionSelectionDoesNotChangeV7Configuration(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s, c := v7MachineTestClient(t)
			s.filterEngine = newFilterCommandEngine()
			for _, line := range []string{"SAVE PRESET NATIVE", "DIALECT GO", "SAVE PRESET BOOLEAN", "DIALECT " + string(dialect)} {
				var response string
				var handled bool
				if strings.HasPrefix(line, "DIALECT") {
					response, handled = s.handleDialectCommand(c, line)
				} else {
					response, handled = s.handlePresetCommand(c, line)
				}
				if !handled || (!strings.Contains(response, "Saved preset") && !strings.Contains(response, "Dialect set to") && !strings.Contains(response, "Dialect already set to")) {
					t.Fatalf("v7 setup %q: %q", line, response)
				}
			}
			before, err := c.captureV7Configuration()
			if err != nil {
				t.Fatal(err)
			}
			before = before.Clone()
			revision, err := c.v7ConfigurationRevisionToken()
			if err != nil {
				t.Fatal(err)
			}
			refs, err := c.v7PresetReferences.Clone()
			if err != nil || refs.GO == nil || refs.CC == nil {
				t.Fatalf("both-bank preset fixture: %+v, %v", refs, err)
			}
			banks, active, references := c.filterBanks, c.filter, c.v7PresetReferences
			path, library := filepath.Join(filter.UserDataDir, c.callsign+".yaml"), presetLibraryPathFor(c.callsign)
			disk, presets := presetDiskBytes(t, path), presetDiskBytes(t, library)
			commits := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error {
				commits++
				return os.ErrPermission
			}
			for _, tc := range []struct {
				line, reply string
				action      spotFormatAction
				enabled     bool
			}{
				{" \tsEt/Ve7Cc  ", "VE7CC enabled for this session.\n", spotFormatVE7CC, true},
				{"SET/VE7CC", "VE7CC already enabled for this session.\n", spotFormatVE7CC, true},
				{"unset/ve7cc", "VE7CC disabled for this session.\n", spotFormatText, false},
				{"UNSET/VE7CC", "VE7CC already disabled for this session.\n", spotFormatText, false},
			} {
				message := ve7ccSelect(t, s, c, tc.line)
				if c.ve7ccEnabled != tc.enabled || message.spotFormat != tc.action || message.line != tc.reply ||
					message.raw != nil || message.configurationReservation != nil || message.bulletinReservation != 0 || message.closeAfter {
					t.Fatalf("%q: state=%v reply=%+v", tc.line, c.ve7ccEnabled, message)
				}
				after, err := c.captureV7Configuration()
				if err != nil || !after.Equal(before) || c.filterBanks != banks || c.filter != active ||
					c.v7PresetReferences != references || !reflect.DeepEqual(c.v7PresetReferences, refs) {
					t.Fatalf("%q changed a bank, setting, filter identity or preset reference: %v", tc.line, err)
				}
				if next, err := c.v7ConfigurationRevisionToken(); err != nil || next != revision {
					t.Fatalf("selector changed revision: %q -> %q, %v", revision, next, err)
				}
			}
			if commits != 0 || !bytes.Equal(disk, presetDiskBytes(t, path)) || !bytes.Equal(presets, presetDiskBytes(t, library)) {
				t.Fatal("connection-only selection wrote the profile or shared preset library")
			}
			// Inspect the actual reserved schema-seven reply, not just capture.
			input, output := configurationTransferBudgetStats()
			result := s.handleV7MachineCommand(c, "GET YAML CONFIG SCHEMA 7 ID CC11-read-1")
			if !result.handled || result.terminal {
				t.Fatalf("v7 readback: %+v", result)
			}
			func() {
				message := ve7ccTakeControl(t, c)
				defer func() {
					message.raw = nil
					message.configurationReservation.Release()
				}()
				if message.configurationReservation == nil || bytes.Contains(bytes.ToLower(message.raw), []byte("ve7cc")) {
					t.Fatal("selector leaked into configuration export or GET lost its owner")
				}
			}()
			assertConfigurationTransferStats(t, input, output)
		})
	}
}

func TestVE7CCSessionGrammar(t *testing.T) {
	s, c := v7MachineTestClient(t)
	for _, enabled := range []bool{false, true} {
		c.ve7ccEnabled = enabled
		for _, line := range []string{"SET/VE7CC ON", "unset/ve7cc OFF", "SET/VE7CC\tEXTRA MORE"} {
			message := ve7ccSelect(t, s, c, line)
			want := "Usage: " + strings.ToUpper(strings.Fields(line)[0]) + "\n"
			if message.line != want || message.spotFormat != spotFormatUnchanged || c.ve7ccEnabled != enabled {
				t.Fatalf("invalid arguments changed selection: %q, %+v", line, message)
			}
		}
		for _, line := range []string{"", "SET/VE7C", "SET/VE7CCX", "UNSET/VE7CCX", "SET/VE7CC/ON", "SET /VE7CC", "HELP SET/VE7CC", "ECHO UNSET/VE7CC"} {
			if handled, proceed := s.handleVE7CCCommand(c, line); handled || !proceed || len(c.controlChan) != 0 || c.ve7ccEnabled != enabled {
				t.Fatalf("non-head or prefix command intercepted: %q", line)
			}
		}
	}
}

func TestVE7CCSessionSurvivesV7Publication(t *testing.T) {
	s, c := v7MachineTestClient(t)
	s.filterEngine = newFilterCommandEngine()
	if reply, handled := s.handlePresetCommand(c, "SAVE PRESET NATIVE"); !handled || !strings.Contains(reply, "Saved preset NATIVE (CC)") {
		t.Fatalf("save current tagged preset: %q", reply)
	}
	ve7ccSelect(t, s, c, "SET/VE7CC")
	for _, dialect := range []DialectName{DialectGo, DialectCC, DialectGo} {
		reply, handled := s.handleDialectCommand(c, "DIALECT "+string(dialect))
		if !handled || !strings.Contains(reply, "Dialect set to") || c.dialect != dialect || !c.ve7ccEnabled {
			t.Fatalf("DIALECT %s cleared selection or failed publication: %q", dialect, reply)
		}
	}
	reply, handled := s.handlePresetCommand(c, "LOAD PRESET NATIVE")
	if !handled || !strings.Contains(reply, "Dialect switched to CC") || c.dialect != DialectCC || !c.ve7ccEnabled {
		t.Fatalf("LOAD did not publish CC while retaining selection: %q", reply)
	}
	command, request := v7ProposalFixture(t, c, "PATCH", "FILTER", "dialect: cc\nfilters: {modes: [CW]}\n")
	response := s.applyV7MachineRequest(c, command, request)
	current, err := c.captureV7Configuration()
	if err != nil || !strings.Contains(response, "persisted: true") || !c.ve7ccEnabled ||
		!reflect.DeepEqual(current.Filters.CC.Modes, []string{"CW"}) {
		t.Fatalf("YAML publication cleared selection or did not publish: %q, %v", response, err)
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil || !record.Configuration.Equal(current) {
		t.Fatalf("publication was not durable: %v", err)
	}
}

func TestVE7CCSessionProtectedSavedProfile(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	const broken = "[protected invalid saved profile\n"
	if err := os.WriteFile(path, []byte(broken), 0o600); err != nil {
		t.Fatal(err)
	}
	result := s.restoreV7ClientRecordOwned(c, time.Unix(1800000000, 0).UTC())
	if result.loadError == nil || !c.recordProtected || c.filterBanks == nil || configurationTransferClosed(c) {
		t.Fatalf("fixture did not establish a live protected v7 session: %+v", result)
	}
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	message := ve7ccSelect(t, s, c, "SET/VE7CC")
	after, err := c.captureV7Configuration()
	if err != nil || !c.ve7ccEnabled || message.spotFormat != spotFormatVE7CC || !after.Equal(before) ||
		string(presetDiskBytes(t, path)) != broken {
		t.Fatal("protected saved profile prevented selection or was overwritten", err)
	}
}

func TestVE7CCSessionConfigurationStripeCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c := v7MachineTestClient(t)
		release, err := s.acquireConfiguration(c, false, false, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		defer c.interrupt()
		finished := make(chan bool, 1)
		go func() { _, proceed := s.handleVE7CCCommand(c, "SET/VE7CC"); finished <- proceed }()
		synctest.Wait() // The selector must be durably waiting on the occupied stripe.
		if len(finished) != 0 || len(c.controlChan) != 0 || c.ve7ccEnabled {
			t.Fatal("selector bypassed the full-callsign configuration lease")
		}
		assertHandoffClientLocksAvailable(t, c)
		c.interrupt()
		synctest.Wait()
		if <-finished || len(c.controlChan) != 0 || c.ve7ccEnabled || len(s.configurationTxnStripes[configurationStripe(c.callsign)]) != 1 {
			t.Fatal("canceled selector published state or released the other transaction's lease")
		}
	})
}

func TestVE7CCSessionCloseFence(t *testing.T) {
	s, c := v7MachineTestClient(t)
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	release()
	c.bulletinResponseMu.Lock()
	var unlocked sync.Once
	unlock := func() { unlocked.Do(c.bulletinResponseMu.Unlock) }
	finished := make(chan struct{})
	var handled, proceed bool
	go func() { handled, proceed = s.handleVE7CCCommand(c, "SET/VE7CC"); close(finished) }()
	t.Cleanup(func() { unlock(); c.interrupt(); waitConfigurationTest(t, finished) })
	stripe := s.configurationTxnStripes[configurationStripe(c.callsign)]
	until := time.Now().Add(3 * time.Second)
	for len(stripe) == 0 && time.Now().Before(until) {
		runtime.Gosched()
	}
	if len(stripe) != 1 {
		t.Fatal("selector never reached the held publication fence")
	}
	// A real bounded competing acquisition must expire while the selector owns
	// its lease behind the fence. A started goroutine/flag assertion is not enough.
	probe := configurationTestClient(s, c.callsign)
	probeRelease, err := s.acquireConfiguration(probe, false, true, time.Now().Add(25*time.Millisecond))
	if err == nil {
		probeRelease()
		t.Fatal("selector did not wait for the publication/close fence")
	}
	if !errors.Is(err, errLoginDeadline) {
		t.Fatal(err)
	}
	// Inject interrupt's done transition under its actual fence. This fixture
	// has no socket or writer; closeOnce also makes failure cleanup safe.
	c.closeOnce.Do(func() { close(c.done) })
	unlock()
	waitConfigurationTest(t, finished)
	if !handled || proceed || c.ve7ccEnabled || len(c.controlChan) != 0 {
		t.Fatal("selector published after close won the publication fence")
	}
}

func TestVE7CCSessionOverflowClosesOutsideGuards(t *testing.T) {
	s, c := v7MachineTestClient(t)
	s.registerClient(c)
	c.controlChan = make(chan controlMessage, 1)
	c.controlChan <- controlMessage{line: "occupied\n"}
	entered, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumed sync.Once
	unblock := func() { resumed.Do(func() { close(resume) }) }
	s.connectionReporter = func(event ConnectionEvent) {
		if event.Action == "disconnect" {
			close(entered)
			<-resume
		}
	}
	var proceed bool
	go func() { _, proceed = s.handleVE7CCCommand(c, "SET/VE7CC"); close(finished) }()
	t.Cleanup(func() { unblock(); waitConfigurationTest(t, finished) })
	waitConfigurationTest(t, entered)
	if !configurationTransferClosed(c) || c.ve7ccEnabled {
		t.Fatal("overflow reported before closing, or published unqueued requested state")
	}
	if !c.bulletinResponseMu.TryLock() {
		t.Fatal("overflow reporter retained the publication/close fence")
	}
	c.bulletinResponseMu.Unlock()
	assertHandoffClientLocksAvailable(t, c)
	// Reconnect performs actual ownership acquisition, restore and replacement
	// while the old selector is still inside the optional reporter.
	next := configurationTestClient(s, c.callsign)
	t.Cleanup(next.interrupt)
	result, err := s.restoreAndRegisterClient(next, s.now(), time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("overflow retained configuration or registry ownership: %v", err)
	}
	s.finishClientRegistration(next, result.evicted, result.total)
	if handled, ok := s.handleVE7CCCommand(c, "SET/VE7CC"); !handled || ok || next.ve7ccEnabled || len(next.controlChan) != 0 {
		t.Fatal("closed predecessor affected its replacement")
	}
	unblock()
	waitConfigurationTest(t, finished)
	if proceed || c.ve7ccEnabled {
		t.Fatal("failed enqueue reported success")
	}
}

func TestVE7CCSessionRetiredOpenOwnerCannotSelect(t *testing.T) {
	s, old := v7MachineTestClient(t)
	s.duplicateLoginMsg = "replacement session\n" // Leave closure to the absent writer.
	old.retired = make(chan struct{})
	t.Cleanup(old.interrupt)
	if err := old.saveFilter(); err != nil {
		t.Fatal(err)
	}
	s.registerClient(old)
	ve7ccSelect(t, s, old, "SET/VE7CC")
	next := configurationTestClient(s, old.callsign)
	t.Cleanup(next.interrupt)
	result, err := s.restoreAndRegisterClient(next, s.now(), time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	s.finishClientRegistration(next, result.evicted, result.total)
	if configurationTransferClosed(old) {
		t.Fatal("fixture closed the predecessor instead of exercising retirement")
	}
	select {
	case <-old.retired:
	default:
		t.Fatal("replacement did not retire the predecessor")
	}
	notice := ve7ccTakeControl(t, old)
	if !notice.closeAfter || notice.spotFormat != spotFormatUnchanged {
		t.Fatal("missing existing replacement notice")
	}
	path := filepath.Join(filter.UserDataDir, old.callsign+".yaml")
	before := presetDiskBytes(t, path)
	if handled, proceed := s.handleVE7CCCommand(old, "UNSET/VE7CC"); !handled || proceed || !old.ve7ccEnabled ||
		next.ve7ccEnabled || len(old.controlChan) != 0 || len(next.controlChan) != 0 {
		t.Fatal("retired but open owner admitted another selector")
	}
	if !bytes.Equal(before, presetDiskBytes(t, path)) || s.clients[next.callsign] != next {
		t.Fatal("retired selector changed successor ownership or persistence")
	}
	old.interrupt()
	next.interrupt()
}

func FuzzVE7CCSessionAdmission(f *testing.F) {
	for _, line := range []string{"SET/VE7CC", "unset/ve7cc", " \tsEt/Ve7Cc ", "SET/VE7CC ON", "UNSET/VE7CC OFF",
		"SET/VE7CCX", "HELP SET/VE7CC", "SET /VE7CC", "SET/VE7CC\x00", "SET/VE7CC\u2003arg", ""} {
		f.Add(line, false)
		f.Add(line, true)
	}
	// One sandbox per fuzz worker, only as protection against accidental future
	// persistence. Iterations do no filesystem setup, restore or readback.
	previous := filter.UserDataDir
	filter.UserDataDir = f.TempDir()
	f.Cleanup(func() { filter.UserDataDir = previous })
	base, err := filter.DefaultV7Configuration(filter.SettingsConfiguration{Dialect: "go", DedupePolicy: "FAST"})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, line string, initially bool) {
		if len(line) > 256 {
			return
		}
		cfg, runtimeFilter, _, err := filter.PrepareV7Configuration(base.Clone())
		if err != nil {
			t.Fatal(err)
		}
		before := cfg.Clone()
		commits := 0
		s := &Server{commitConfigurationFn: func(*filter.PreparedUserRecord) error { commits++; return os.ErrPermission }}
		c := &Client{server: s, callsign: "W1FUZZ-1", done: make(chan struct{}), controlChan: make(chan controlMessage, 1),
			filter: &runtimeFilter, filterBanks: &cfg.Filters, dialect: DialectGo, configuredSettings: cfg.Settings,
			configurationInitialized: true, configurationEpoch: "session-fuzz", configurationRevision: 17, ve7ccEnabled: initially}
		banks, active := c.filterBanks, c.filter
		trimmed := strings.TrimSpace(line)
		head := trimmed
		if end := strings.IndexFunc(head, unicode.IsSpace); end >= 0 {
			head = head[:end]
		}
		isSet, isUnset := strings.EqualFold(trimmed, "SET/VE7CC"), strings.EqualFold(trimmed, "UNSET/VE7CC")
		wantHandled := strings.EqualFold(head, "SET/VE7CC") || strings.EqualFold(head, "UNSET/VE7CC")
		handled, proceed := s.handleVE7CCCommand(c, line)
		wantEnabled := initially
		if isSet || isUnset {
			wantEnabled = isSet
		}
		if handled != wantHandled || !proceed || c.ve7ccEnabled != wantEnabled {
			t.Fatalf("%q: handled=%v proceed=%v enabled=%v", line, handled, proceed, c.ve7ccEnabled)
		}
		if handled {
			message := ve7ccTakeControl(t, c)
			action := spotFormatUnchanged
			if isSet {
				action = spotFormatVE7CC
			} else if isUnset {
				action = spotFormatText
			} else if message.line != "Usage: "+strings.ToUpper(head)+"\n" {
				t.Fatalf("argument admission did not produce usage: %+v", message)
			}
			if message.spotFormat != action || message.configurationReservation != nil || message.raw != nil || message.closeAfter {
				t.Fatalf("wrong typed selector action or output ownership: %+v", message)
			}
		}
		after, err := c.captureV7Configuration()
		if err != nil || !after.Equal(before) || c.filterBanks != banks || c.filter != active || c.configurationRevision != 17 ||
			c.configurationEpoch != "session-fuzz" || c.configurationUnsaved || c.preserveRecordOnExit.Load() || commits != 0 || len(c.controlChan) != 0 {
			t.Fatalf("session admission changed saved configuration/revision or persisted: %v", err)
		}
	})
}
