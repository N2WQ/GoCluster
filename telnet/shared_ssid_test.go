package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/filter"
	"dxcluster/spot"
	"github.com/gorilla/websocket"
)

// Read unstructured startup separately from application notices. These tests
// verify the Telnet and WebSocket wires while controlling both cleanup orders.
func connectSharedBrowser(t *testing.T, s *Server, call, ip string) (*websocket.Conn, <-chan struct{}) {
	t.Helper()
	ws, done := browserWirePair(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP(ip)}, nil, time.Now(), 10*time.Second)
	})
	if prompt := browserReceive(t, ws); prompt != "login: " {
		t.Fatalf("browser prompt %q", prompt)
	}
	browserSend(t, ws, call)
	for !strings.Contains(browserReceive(t, ws), "READY\r\n") {
	}
	return ws, done
}

func closeSharedBrowser(t *testing.T, ws *websocket.Conn, done <-chan struct{}) {
	t.Helper()
	_ = ws.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("browser writer or reader did not retire")
	}
}

func TestSharedSSIDSlotsAndPrivateRouting(t *testing.T) {
	for _, webFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("web-first-%t", webFirst), func(t *testing.T) {
			const call = "VE3EID"
			s := newSessionCeilingServer(t, 2)
			var peerChanges atomic.Int32
			s.SetPeerMembershipListener(func() { peerChanges.Add(1) })
			address := startSessionCeilingListener(t, s)
			var first *websocket.Conn
			var firstDone <-chan struct{}
			if webFirst {
				first, firstDone = connectSharedBrowser(t, s, call, "192.0.2.57")
			}
			logger := ceilingConnect(t, address, call)
			ceilingReadThrough(t, logger, "READY\r\n")
			if !webFirst {
				first, firstDone = connectSharedBrowser(t, s, call, "192.0.2.57")
			}
			ceilingReadThrough(t, logger, "192.0.2.57 (unknown).")
			// Direct SELF bypasses the ordinary fan-out when all lanes suppress
			// a spot. Both registered transports must still see the same bytes.
			self := spot.NewSpot(call+"-9/P", "W1AW", 14025, "CW")
			s.DeliverSelfSpot(self)
			wire := self.FormatDXCluster() + "\r\n"
			ceilingReadThrough(t, logger, wire)
			for !strings.Contains(browserReceive(t, first), wire) {
			}
			snapshot := s.CurrentPeerMembership()
			initialPeerChanges := peerChanges.Load()
			if snapshot.RawCount != 1 || snapshot.Users[0].IP != "127.0.0.1" || s.GetClientCount() != 1 {
				t.Fatalf("expected one representative using logger IP: %+v", snapshot)
			}

			refused := ceilingConnect(t, address, "VE3EID-1")
			ceilingReadThrough(t, refused, "Server full")
			second, secondDone := connectSharedBrowser(t, s, call, "192.0.2.58")
			ceilingReadThrough(t, logger, "192.0.2.58 (unknown).")
			closeSharedBrowser(t, first, firstDone)
			current := s.CurrentPeerMembership()
			if current.Revision != snapshot.Revision || current.Users[0] != snapshot.Users[0] || peerChanges.Load() != initialPeerChanges {
				t.Fatalf("browser flap invalidated the logger route: before=%+v after=%+v", snapshot, current)
			}
			if !s.SendCurrentDirectMessage(call, snapshot.Users[0].SessionID, snapshot.Revision, "private to logger") {
				t.Fatal("healthy logger refused a private message after the browser flap")
			}
			ceilingReadThrough(t, logger, "private to logger")
			_ = logger.Close()
			waitSessionCeiling(t, func() bool {
				now := s.CurrentPeerMembership()
				return now.RawCount == 1 && now.Users[0].IP == "192.0.2.58" && peerChanges.Load() == initialPeerChanges+1
			})
			current = s.CurrentPeerMembership()
			if s.SendCurrentDirectMessage(call, snapshot.Users[0].SessionID, snapshot.Revision, "stale") {
				t.Fatal("retired logger recipient was accepted")
			}
			if !s.SendCurrentDirectMessage(call, current.Users[0].SessionID, current.Revision, "private to browser") {
				t.Fatal("browser-only recipient refused a private message")
			}
			for !strings.Contains(browserReceive(t, second), "private to browser") {
			}
			closeSharedBrowser(t, second, secondDone)
			if current = s.CurrentPeerMembership(); current.RawCount != 0 || peerChanges.Load() != initialPeerChanges+2 {
				t.Fatalf("final departure did not publish removal: %+v", current)
			}
		})
	}
}

func TestSharedSSIDCapacityDuringDualReplacementAndShutdown(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 3)
	s.maxSessionsPerIP, s.maxSessionsPerBaseCall = 1, 2
	input, output := configurationTransferBudgetStats()
	address := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, address, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	browser, done := connectSharedBrowser(t, s, call, "192.0.2.57")
	for _, rejection := range []struct{ login, ip, message string }{
		{call, "127.0.0.1", "IP session limit reached."},
		{call + "-1", "192.0.2.59", "Station session limit reached."},
	} {
		refused, refusedDone := browserWirePair(t, func(ws *websocket.Conn) {
			s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP(rejection.ip)}, nil, time.Now(), time.Second)
		})
		browserReceive(t, refused)
		browserSend(t, refused, rejection.login)
		if reply := browserReceive(t, refused); !strings.Contains(reply, rejection.message) {
			t.Fatalf("quota refusal for %v: %q", rejection, reply)
		}
		closeSharedBrowser(t, refused, refusedDone)
	}
	if _, err := io.WriteString(logger, "DIALECT\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "Current dialect: GO")
	browserSend(t, browser, "DIALECT")
	for !strings.Contains(browserReceive(t, browser), "Current dialect: GO") {
	}
	for _, ip := range []string{"192.0.2.58", "192.0.2.57", "192.0.2.58"} {
		nextLogger := ceilingConnect(t, address, call)
		ceilingReadThrough(t, nextLogger, "READY\r\n")
		nextBrowser, nextDone := connectSharedBrowser(t, s, call, ip)
		_ = logger.Close()
		closeSharedBrowser(t, browser, done)
		logger, browser, done = nextLogger, nextBrowser, nextDone
		s.clientsMutex.RLock()
		active, base, browserIP, loggerIP, addresses := s.activeConnectionCountLocked(), s.baseSessionCounts[call], s.ipSessionCounts[ip], s.ipSessionCounts["127.0.0.1"], len(s.ipSessionCounts)
		s.clientsMutex.RUnlock()
		if active != 2 || base != (baseSessionCount{registered: 2}) || browserIP != (ipSessionCount{registered: 1}) || loggerIP != browserIP || addresses != 2 {
			t.Fatalf("replacement leaked or released a slot: active=%d base=%+v browser=%+v logger=%+v addresses=%d", active, base, browserIP, loggerIP, addresses)
		}
	}
	s.Stop()
	closeSharedBrowser(t, browser, done)
	waitSessionCeiling(t, func() bool {
		s.clientsMutex.RLock()
		defer s.clientsMutex.RUnlock()
		return s.activeConnectionCountLocked() == 0 && s.sessionReservations == 0 && len(s.baseSessionCounts) == 0 && len(s.ipSessionCounts) == 0
	})
	assertConfigurationTransferStats(t, input, output)
}

func TestSharedSSIDMetadataSaveKeepsSavedAndUnsavedSettingsSeparate(t *testing.T) {
	const call = "VE3EID-2"
	s := newSessionCeilingServer(t, 2)
	var fail atomic.Bool
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
		if fail.Load() {
			return fmt.Errorf("injected save failure")
		}
		return prepared.Commit()
	}
	address := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, address, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "CW speed: OFF")
	fail.Store(true)
	if _, err := io.WriteString(logger, "SET CWSPEED ON\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "injected save failure")
	ws, done := connectSharedBrowser(t, s, call, "192.0.2.57")
	browserSend(t, ws, "GET YAML SETTINGS SCHEMA 7 ID shared")
	for !strings.Contains(browserReceive(t, ws), "cw_speed: true") {
	}
	saved, err := filter.LoadV7UserRecord(call)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Configuration.Settings.CWSpeed || len(saved.RecentIPs) < 2 ||
		saved.RecentIPs[0] != "192.0.2.57" || saved.RecentIPs[1] != "127.0.0.1" {
		t.Fatalf("metadata save changed durable preferences or lost login order: %+v", saved)
	}
	closeSharedBrowser(t, ws, done)
	_ = logger.Close()
}

func TestSharedSSIDDepartureRetriesAndFinalSave(t *testing.T) {
	for _, browserLast := range []bool{false, true} {
		for _, storageRecovers := range []bool{false, true} {
			t.Run(fmt.Sprintf("browser-last-%t-storage-recovers-%t", browserLast, storageRecovers), func(t *testing.T) {
				const call = "VE3EID"
				s := newSessionCeilingServer(t, 2)
				var fail atomic.Bool
				var attempts atomic.Int32
				s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
					attempts.Add(1)
					if fail.Load() {
						return os.ErrPermission
					}
					return prepared.Commit()
				}
				address := startSessionCeilingListener(t, s)
				logger := ceilingConnect(t, address, call)
				ceilingReadThrough(t, logger, "READY\r\n")
				browser, done := connectSharedBrowser(t, s, call, "192.0.2.57")
				fail.Store(true)
				if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
					t.Fatal(err)
				}
				ceilingReadThrough(t, logger, "permission denied")
				if browserLast {
					_ = logger.Close()
					waitSessionCeiling(t, func() bool { return attempts.Load() == 2 })
					// Readback acquires the same lease and can complete once the
					// departure's failed retry has left the survivor in charge.
					browserSend(t, browser, "GET YAML SETTINGS SCHEMA 7 ID after-departure")
					for !strings.Contains(browserReceive(t, browser), "cw_speed: false") {
					}
				} else {
					closeSharedBrowser(t, browser, done)
					if _, err := io.WriteString(logger, "GET YAML SETTINGS SCHEMA 7 ID after-departure\r\n"); err != nil {
						t.Fatal(err)
					}
					ceilingReadThrough(t, logger, "cw_speed: false")
				}
				if attempts.Load() != 2 {
					t.Fatalf("first departure attempted %d saves including the edit, want 2", attempts.Load())
				}
				fail.Store(!storageRecovers)
				if browserLast {
					closeSharedBrowser(t, browser, done)
				} else {
					_ = logger.Close()
				}
				waitSessionCeiling(t, func() bool { return s.GetClientCount() == 0 })
				if attempts.Load() != 3 {
					t.Fatalf("last departure attempted %d saves including the edit, want 3", attempts.Load())
				}
				saved, err := filter.LoadV7UserRecord(call)
				if err != nil || saved.Configuration.Settings.CWSpeed == storageRecovers {
					t.Fatalf("final disk record did not reflect the final save outcome: %+v, %v", saved, err)
				}
				joined, joinedDone := connectSharedBrowser(t, s, call, "192.0.2.58")
				browserSend(t, joined, "GET YAML SETTINGS SCHEMA 7 ID fresh")
				want := fmt.Sprintf("cw_speed: %t", !storageRecovers)
				for !strings.Contains(browserReceive(t, joined), want) {
				}
				closeSharedBrowser(t, joined, joinedDone)
			})
		}
	}
}

func TestSharedSSIDPresetNoticeAcrossSSIDs(t *testing.T) {
	s := newSessionCeilingServer(t, 3)
	s.dedupeMedEnabled = true
	address := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, address, "VE3EID")
	ceilingReadThrough(t, logger, "READY\r\n")
	other := ceilingConnect(t, address, "VE3EID-3")
	ceilingReadThrough(t, other, "READY\r\n")
	browser, done := connectSharedBrowser(t, s, "VE3EID", "192.0.2.57")
	browserSend(t, browser, "SET JSON ON")
	for !strings.Contains(browserReceive(t, browser), `["hello",`) {
	}
	settings := browserYAML(t, browser, "GET YAML SETTINGS SCHEMA 7 ID baseline")
	revision, ok := settings["revision"].(string)
	if !ok || revision == "" {
		t.Fatalf("missing profile revision in %v", settings)
	}
	for _, verb := range []string{"SAVE", "DELETE"} {
		if _, err := fmt.Fprintf(other, "%s PRESET SHARED\r\n", verb); err != nil {
			t.Fatal(err)
		}
		ceilingReadThrough(t, logger, "Preset library changed.")
		for !strings.Contains(browserReceive(t, browser), `["notice","presets-changed",`) {
		}
	}
	patch := func(revision string) map[string]any {
		return browserYAML(t, browser, "PATCH YAML SETTINGS SCHEMA 7\n---\nschema_version: 7\nrequest_id: update\nif_revision: "+revision+"\nconfiguration:\n  solar_summary_minutes: 15\n...")
	}
	updated := patch(revision)
	if updated["error"] != nil || updated["result"] == nil {
		t.Fatalf("an unrelated preset edit invalidated a profile update: %v", updated)
	}
	if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "CW speed: OFF")
	conflict := patch(fmt.Sprint(updated["revision"]))
	problem, ok := conflict["error"].(map[string]any)
	if !ok || problem["code"] != "revision_conflict" {
		t.Fatalf("browser accepted a revision invalidated by the logger: %v", conflict)
	}
	closeSharedBrowser(t, browser, done)
}

func TestSharedSSIDStalledNoticeSummary(t *testing.T) {
	c := &Client{
		sessionProfile: &sessionProfile{},
		notices:        sessionNotices{wake: make(chan struct{}, 1)},
		controlChan:    make(chan controlMessage, 1),
		done:           make(chan struct{}),
	}
	c.enableSessionNotices()
	c.controlChan <- controlMessage{line: "existing reply"}
	for version := uint64(1); version <= 100; version++ {
		c.offerNotice(presenceNoticeKind, version, fmt.Sprintf("web console state %d", version), true)
	}
	select {
	case <-c.done:
		t.Fatal("advisory notices disconnected a slow client")
	default:
	}
	<-c.notices.wake
	message := c.nextSessionNotice()
	if !strings.Contains(message.line, "100 times; web console state 100") || message.noticeVersion != "100" {
		t.Fatalf("latest summary: %+v", message)
	}
	if extra := c.nextSessionNotice(); extra.line != "" || len(c.controlChan) != 1 {
		t.Fatalf("unexpected pending notice or displaced ordinary reply: %+v", extra)
	}
}

// Freeze writes at the test seam, independently of the production socket's
// write deadline. The actual socket timeout is qualified separately.
type sharedNoticeGatedConn struct{ *retirementBlockedConn }

func (sharedNoticeGatedConn) SetWriteDeadline(time.Time) error { return nil }

func TestSharedSSIDStalledLoggerSurvivesBrowserFlaps(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	serverConn, logger := net.Pipe()
	hold := &retirementBlockedConn{
		Conn: serverConn, blocked: make(chan struct{}), release: make(chan struct{}), stop: make(chan struct{}),
	}
	loggerDone := make(chan struct{})
	go func() {
		defer close(loggerDone)
		s.handleClient(sharedNoticeGatedConn{hold}, nil)
	}()
	t.Cleanup(func() {
		close(hold.stop)
		_ = logger.Close()
		waitConfigurationTest(t, loggerDone)
	})
	ceilingReadThrough(t, logger, "login: ")
	if _, err := io.WriteString(logger, call+"\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "READY\r\n")
	s.clientsMutex.RLock()
	currentLogger := s.clients[call]
	s.clientsMutex.RUnlock()
	if err := currentLogger.Send("BLOCKMARKER\n"); err != nil {
		t.Fatal(err)
	}
	retirementSignal(t, hold.blocked, "logger's blocked writer")
	before := s.CurrentPeerMembership()
	for i := range 100 {
		address := fmt.Sprintf("192.0.2.%d", i+1)
		ws, done := connectSharedBrowser(t, s, call, address)
		closeSharedBrowser(t, ws, done)
	}
	after := s.CurrentPeerMembership()
	if before.Revision != after.Revision || before.Users[0] != after.Users[0] {
		t.Fatalf("browser churn affected logger route: before=%+v after=%+v", before, after)
	}
	s.clientsMutex.RLock()
	occupied := s.activeConnectionCountLocked()
	reserved := s.sessionReservations
	s.clientsMutex.RUnlock()
	if occupied != 1 || reserved != 0 {
		t.Fatalf("leaked registered slots or reservations: %d registered, %d pending", occupied, reserved)
	}
	close(hold.release)
	ceilingReadThrough(t, logger, "200 times; Web console disconnected.")
	if _, err := io.WriteString(logger, "DIALECT\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "Current dialect: GO")
}

func TestSharedSSIDBrowserGoRetryAndVe7cc(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	s.processor = commands.NewProcessor(nil, historyReaderFunc(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Spots: []*spot.Spot{ve7ccTestSpot()}, End: archive.HistoryExhausted}, nil
	}), nil, nil, nil, nil)
	var fail atomic.Bool
	var attempts atomic.Int32
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
		attempts.Add(1)
		if fail.Load() {
			return errors.New("storage unavailable")
		}
		return prepared.Commit()
	}
	address := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, address, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	if _, err := io.WriteString(logger, "DIALECT CC\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "CC")
	fail.Store(true)
	browser, firstDone := connectSharedBrowser(t, s, call, "192.0.2.57")
	ceilingReadThrough(t, logger, "GO is active but unsaved.")
	for !strings.Contains(browserReceive(t, browser), "GO is active but unsaved.") {
	}
	if _, err := io.WriteString(logger, "DIALECT CC\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "Use DIALECT CC after it disconnects.")
	beforeRetry := attempts.Load()
	fail.Store(false)
	rejoined, rejoinedDone := connectSharedBrowser(t, s, call, "192.0.2.58")
	closeSharedBrowser(t, browser, firstDone)
	ceilingReadThrough(t, logger, "Live profile is saved.")
	if attempts.Load() != beforeRetry+1 {
		t.Fatalf("expected exactly one pending GO retry: %d -> %d", beforeRetry, attempts.Load())
	}
	final, finalDone := connectSharedBrowser(t, s, call, "192.0.2.59")
	closeSharedBrowser(t, rejoined, rejoinedDone)
	if attempts.Load() != beforeRetry+1 {
		t.Fatal("rejoin attempted to write an already-saved GO setting")
	}
	_ = logger.Close()
	logger = ceilingConnect(t, address, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	if _, err := io.WriteString(logger, "SET/VE7CC\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "VE7CC enabled for this session.")
	browserSend(t, final, "SET JSON ON")
	for !strings.Contains(browserReceive(t, final), `["hello",`) {
	}
	s.BroadcastSpot(ve7ccWireFixture(), true, true, true)
	ceilingReadThrough(t, logger, ve7ccWireSpot)
	for !strings.Contains(browserReceive(t, final), `["s",`) {
	}
	withHistory, withHistoryDone := connectSharedBrowser(t, s, call, "192.0.2.60")
	ceilingReadThrough(t, logger, "Web console connected from 192.0.2.60 (unknown).\r\n")
	if _, err := io.WriteString(logger, "SH/DX 1\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, ve7ccHistoryWireSpot)
	closeSharedBrowser(t, final, finalDone)
	closeSharedBrowser(t, withHistory, withHistoryDone)
	if _, err := io.WriteString(logger, "DIALECT\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "Current dialect: GO")
}

func TestSharedSSIDGoRetryCreatesMissingStartingRecord(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	var fail atomic.Bool
	fail.Store(true)
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
		if fail.Load() {
			return os.ErrPermission
		}
		return prepared.Commit()
	}
	address := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, address, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "permission denied")
	first, firstDone := connectSharedBrowser(t, s, call, "192.0.2.57")
	if _, err := filter.LoadV7UserRecord(call); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("storage unexpectedly has a starting record: %v", err)
	}
	fail.Store(false)
	rejoined, rejoinedDone := connectSharedBrowser(t, s, call, "192.0.2.58")
	closeSharedBrowser(t, first, firstDone)
	saved, err := filter.LoadV7UserRecord(call)
	if err != nil || saved.Configuration.Settings.Dialect != "go" || !saved.Configuration.Settings.CWSpeed {
		t.Fatalf("retry did not save GO starting defaults separately from the live edit: %+v, %v", saved, err)
	}
	browserSend(t, rejoined, "GET YAML SETTINGS SCHEMA 7 ID live")
	for !strings.Contains(browserReceive(t, rejoined), "cw_speed: false") {
	}
	closeSharedBrowser(t, rejoined, rejoinedDone)
}

func TestSharedSSIDUploadDoesNotBlockLoggerSave(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	address := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, address, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	browser, done := connectSharedBrowser(t, s, call, "192.0.2.57")
	frame, err := browser.NextWriter(websocket.TextMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(frame, "PATCH YAML SETTINGS SCHEMA 7\n---\n"+strings.Repeat("# incomplete\n", 256)); err != nil {
		t.Fatal(err)
	}
	// More than the transport write buffer was sent. The detached request is
	// still waiting for its message tail, which is deliberately never sent.
	if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "CW speed: OFF")
	closeSharedBrowser(t, browser, done)
	record, err := filter.LoadV7UserRecord(call)
	if err != nil || record.Configuration.Settings.CWSpeed {
		t.Fatalf("logger edit did not persist during stalled upload: %+v, %v", record, err)
	}
}

func TestSharedSSIDMetadataAndLoggerSaveSerialize(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	var holdMetadata atomic.Bool
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	s.commitLoginRecordFn = func(record *filter.PreparedUserRecord) error {
		if holdMetadata.CompareAndSwap(true, false) {
			close(entered)
			<-resume
		}
		return record.Commit()
	}
	addr := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, addr, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	holdMetadata.Store(true)
	ws, done := browserWirePair(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP("192.0.2.57")}, nil, time.Now(), time.Second)
	})
	browserReceive(t, ws)
	browserSend(t, ws, call)
	retirementSignal(t, entered, "authoritative metadata byte preparation")
	if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
		t.Fatal(err)
	}
	// The later edit must not be overtaken by the already-prepared metadata.
	release()
	for !strings.Contains(browserReceive(t, ws), "READY\r\n") {
	}
	ceilingReadThrough(t, logger, "CW speed: OFF")
	record, err := filter.LoadV7UserRecord(call)
	if err != nil || record.Configuration.Settings.CWSpeed || len(record.RecentIPs) < 2 || record.RecentIPs[0] != "192.0.2.57" {
		t.Fatalf("disk lost the admitted edit or login metadata: %+v, %v", record, err)
	}

	// Reverse the order. Admission must read the latest saved settings even
	// when a prior browser connection is still present.
	if _, err := io.WriteString(logger, "SET CWSPEED ON\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "CW speed: ON")
	replacement, replacementDone := connectSharedBrowser(t, s, call, "192.0.2.58")
	record, err = filter.LoadV7UserRecord(call)
	if err != nil || !record.Configuration.Settings.CWSpeed || record.RecentIPs[0] != "192.0.2.58" {
		t.Fatalf("rejoin carried an obsolete saved record: %+v, %v", record, err)
	}
	closeSharedBrowser(t, ws, done)
	closeSharedBrowser(t, replacement, replacementDone)
}

type sharedReadbackListener struct {
	net.Listener
	entered chan struct{}
	resume  chan struct{}
	marker  string
}

func (l sharedReadbackListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &sharedReadbackConn{Conn: conn, entered: l.entered, resume: l.resume, marker: l.marker}, nil
}

type sharedReadbackConn struct {
	net.Conn
	entered chan struct{}
	resume  chan struct{}
	marker  string
	once    sync.Once
}

func (c *sharedReadbackConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(c.marker)) {
		c.once.Do(func() { close(c.entered) })
		<-c.resume
	}
	return c.Conn.Write(p)
}

func TestSharedSSIDStalledReadbackAndLateCleanup(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	addr := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, addr, call)
	ceilingReadThrough(t, logger, "READY\r\n")

	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	browser, browserDone := browserWirePairWithListener(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP("192.0.2.57")}, nil, time.Now(), time.Second)
	}, func(l net.Listener) net.Listener {
		return sharedReadbackListener{Listener: l, entered: entered, resume: resume, marker: "schema_version: 7"}
	})
	browserReceive(t, browser)
	browserSend(t, browser, call)
	for !strings.Contains(browserReceive(t, browser), "READY\r\n") {
	}
	s.clientsMutex.RLock()
	old := s.browserCompanions[call]
	s.clientsMutex.RUnlock()
	browserSend(t, browser, "GET YAML SETTINGS SCHEMA 7 ID blocked")
	retirementSignal(t, entered, "browser readback socket stall")
	if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "CW speed: OFF")

	_ = browser.Close()
	retirementSignal(t, old.done, "old browser interrupt")
	replacement, replacementDone := connectSharedBrowser(t, s, call, "192.0.2.58")
	if _, err := io.WriteString(logger, "SET CWSPEED ON\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "CW speed: ON")
	release()
	closeSharedBrowser(t, browser, browserDone)

	browserSend(t, replacement, "GET YAML SETTINGS SCHEMA 7 ID current")
	for !strings.Contains(browserReceive(t, replacement), "cw_speed: true") {
	}
	s.clientsMutex.RLock()
	active := s.activeConnectionCountLocked()
	s.clientsMutex.RUnlock()
	if active != 2 {
		t.Fatalf("late cleanup changed current slot accounting: %d", active)
	}
	closeSharedBrowser(t, replacement, replacementDone)
}

func TestSharedSSIDDropAfterEditBeforeReply(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	addr := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, addr, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	browser, browserDone := browserWirePairWithListener(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP("192.0.2.57")}, nil, time.Now(), time.Second)
	}, func(l net.Listener) net.Listener {
		return sharedReadbackListener{Listener: l, entered: entered, resume: resume, marker: "CW speed: OFF"}
	})
	browserReceive(t, browser)
	browserSend(t, browser, call)
	for !strings.Contains(browserReceive(t, browser), "READY\r\n") {
	}
	s.clientsMutex.RLock()
	old := s.browserCompanions[call]
	s.clientsMutex.RUnlock()
	browserSend(t, browser, "SET CWSPEED OFF")
	retirementSignal(t, entered, "committed edit reply")
	ceilingReadThrough(t, logger, "Live profile changed from the web console.")
	_ = browser.Close()
	retirementSignal(t, old.done, "browser dropped before reply")
	rejoined, rejoinedDone := connectSharedBrowser(t, s, call, "192.0.2.58")
	release()
	closeSharedBrowser(t, browser, browserDone)
	record, err := filter.LoadV7UserRecord(call)
	if err != nil || record.Configuration.Settings.CWSpeed {
		t.Fatalf("committed edit was lost on disk: %+v, %v", record, err)
	}
	browserSend(t, rejoined, "GET YAML SETTINGS SCHEMA 7 ID committed")
	for !strings.Contains(browserReceive(t, rejoined), "cw_speed: false") {
	}
	closeSharedBrowser(t, rejoined, rejoinedDone)
}

func TestSharedSSIDRejoinBeforeLastDepartureReleasesProfile(t *testing.T) {
	const call = "VE3EID"
	s := newSessionCeilingServer(t, 2)
	var saves atomic.Int32
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error {
		saves.Add(1)
		return os.ErrPermission
	}
	addr := startSessionCeilingListener(t, s)
	logger := ceilingConnect(t, addr, call)
	ceilingReadThrough(t, logger, "READY\r\n")
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	browser, browserDone := browserWirePairWithListener(t, func(ws *websocket.Conn) {
		s.serveWebSocketSession(ws, &net.TCPAddr{IP: net.ParseIP("192.0.2.57")}, nil, time.Now(), time.Second)
	}, func(l net.Listener) net.Listener {
		return sharedReadbackListener{Listener: l, entered: entered, resume: resume, marker: "schema_version: 7"}
	})
	browserReceive(t, browser)
	browserSend(t, browser, call)
	for !strings.Contains(browserReceive(t, browser), "READY\r\n") {
	}
	s.clientsMutex.RLock()
	old := s.browserCompanions[call]
	s.clientsMutex.RUnlock()
	if _, err := io.WriteString(logger, "SET CWSPEED OFF\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, logger, "permission denied")
	browserSend(t, browser, "GET YAML SETTINGS SCHEMA 7 ID blocked")
	retirementSignal(t, entered, "last connection's writer")
	_ = logger.Close()
	waitSessionCeiling(t, func() bool {
		s.clientsMutex.RLock()
		defer s.clientsMutex.RUnlock()
		return s.clients[call] == old && s.activeConnectionCountLocked() == 1
	})
	_ = browser.Close()
	retirementSignal(t, old.done, "last connection closed while its writer is blocked")
	rejoined, rejoinedDone := connectSharedBrowser(t, s, call, "192.0.2.58")
	release()
	closeSharedBrowser(t, browser, browserDone)
	if saves.Load() != 2 {
		t.Fatalf("a retired last connection attempted a save: %d saves", saves.Load())
	}
	browserSend(t, rejoined, "GET YAML SETTINGS SCHEMA 7 ID live-handoff")
	for !strings.Contains(browserReceive(t, rejoined), "cw_speed: false") {
	}
	stored, err := filter.LoadV7UserRecord(call)
	if err != nil || !stored.Configuration.Settings.CWSpeed {
		t.Fatalf("handoff changed the last successful profile save: %+v, %v", stored, err)
	}
	closeSharedBrowser(t, rejoined, rejoinedDone)
}
