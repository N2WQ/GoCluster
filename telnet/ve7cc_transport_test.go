package telnet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/filter"
	"dxcluster/spot"
)

func ve7ccConnectWitness(t *testing.T, s *Server, call string) (*nativeTCPWitness, <-chan struct{}) {
	t.Helper()
	conn, reader, done, greeting := connectUpgradeSession(t, s, call)
	s.clientsMutex.RLock()
	c := s.clients[call]
	s.clientsMutex.RUnlock()
	if c == nil || !strings.HasSuffix(greeting, "READY\r\n") || strings.Contains(greeting, "CC11^") {
		t.Fatal("existing real-TCP login did not establish the ordinary greeting and delivery owner")
	}
	t.Cleanup(func() {
		closeHandshakeTranscriptSession(t, conn, done)
		writerV15Wait(t, c.writerStopped, "connected writer shutdown")
	})
	return &nativeTCPWitness{t: t, server: s, client: c, conn: conn, reader: reader}, done
}

func ve7ccTCPSelector(t *testing.T, w *nativeTCPWitness, command, expected string) {
	t.Helper()
	if _, err := io.WriteString(w.conn, command+"\r\n"); err != nil {
		t.Fatal(err)
	}
	if got := w.throughLine(strings.TrimSuffix(expected, "\r\n")); got != expected {
		t.Fatalf("real reader/dispatcher acknowledgement:\n got %q\nwant %q", got, expected)
	}
}

func ve7ccTCPSpot(t *testing.T, w *nativeTCPWitness, expected string) {
	t.Helper()
	// This is production fan-out, not a direct write or a queue-only assertion.
	// nativeTCPWitness.positive cannot serve this oracle: it requires DX-de text.
	w.inject(ve7ccWireFixture())
	if got := w.throughLine("K1ABC"); got != expected {
		t.Fatalf("real fan-out/writer record:\n got %q\nwant %q", got, expected)
	}
}

func TestVE7CCConnectedProtectedProfileTerminalTail(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "READY\n"
			})
			call := "N0CALL-79"
			path := filepath.Join(filter.UserDataDir, call+".yaml")
			const original = "[invalid protected profile\n"
			if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
				t.Fatal(err)
			}
			w, done := ve7ccConnectWitness(t, s, call)
			ve7ccTCPSelector(t, w, "SET/VE7CC", "VE7CC enabled for this session.\r\n")
			ve7ccTCPSelector(t, w, "UNSET/VE7CC", "VE7CC disabled for this session.\r\n")
			if _, err := io.WriteString(w.conn, "PUT YAML CONFIG SCHEMA 7 EXTRA\r\nSET/VE7CC\r\n"); err != nil {
				t.Fatal(err)
			}
			if err := w.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			tail, err := io.ReadAll(io.LimitReader(w.reader, maxV7YAMLBytes+1))
			if err != nil || !strings.Contains(string(tail), "invalid_header") || strings.Contains(string(tail), "VE7CC enabled") {
				t.Fatalf("terminal machine tail reached the native selector: %q, %v", tail, err)
			}
			writerV15Wait(t, done, "protected terminal command loop")
			stored, err := os.ReadFile(path)
			if err != nil || string(stored) != original || !w.client.recordProtected ||
				!w.client.preserveRecordOnExit.Load() || w.client.ve7ccEnabled {
				t.Fatalf("native selection or terminal close weakened saved-record protection: %v", err)
			}
		})
	}
}

func TestVE7CCConnectedSessionTransportAndReconnect(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, dialect := range []DialectName{DialectGo, DialectCC} {
			t.Run(transport+"/"+string(dialect), func(t *testing.T) {
				input, output := configurationTransferBudgetStats()
				t.Cleanup(func() { assertConfigurationTransferStats(t, input, output) })
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "READY\n"
					opts.CommandRatePerSecond, opts.CommandBurst = 1000, 1000
					opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
				})
				call := "N0CALL-78"
				saved, err := filter.DefaultV7Configuration(filter.SettingsConfiguration{
					Dialect: string(dialect), NoiseClass: "QUIET", DedupePolicy: "FAST",
				})
				if err != nil {
					t.Fatal(err)
				}
				// Both non-default banks admit the CW/manual byte witness. A
				// reconnect must restore them, not recreate new-user defaults.
				saved.Filters.GO.Rules = []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW AND SOURCE=HUMAN"}}
				saved.Filters.CC.Modes, saved.Filters.CC.Sources = []string{"CW"}, []string{"HUMAN"}
				saved.Filters.CC.Geography = nil
				saved, _, _, err = filter.PrepareV7Configuration(saved)
				if err != nil {
					t.Fatal(err)
				}
				payload, err := filter.PrepareV7UserRecord(call, &filter.V7UserRecord{
					ConfigurationVersion: filter.V7ConfigurationVersion, Configuration: saved,
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := payload.Commit(); err != nil {
					t.Fatal(err)
				}
				retained := ve7ccWireFixture()
				var historyReads atomic.Int32
				reader := historyReaderFunc(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
					historyReads.Add(1)
					if req.Limit != 1 || req.Done == nil || req.Match == nil || !req.Match(retained) {
						return archive.HistoryPage{}, fmt.Errorf("real SHOW DX did not supply the current saved-bank predicate/count/lifetime")
					}
					return archive.HistoryPage{Spots: []*spot.Spot{retained}, End: archive.HistoryExhausted}, nil
				})
				s.processor = commands.NewProcessor(nil, reader, nil, s.ctyLookup, nil, nil)
				w, done := ve7ccConnectWitness(t, s, call)
				current, exported := w.configuration("RESUME\r\n")
				waitUpgradeOutputReleased(t, s, call)
				if !current.Equal(saved) || current.Settings.Dialect != string(dialect) ||
					strings.Contains(strings.ToLower(exported), "ve7cc") {
					t.Fatal("login failed to restore both saved banks/dialect or exported a session selector")
				}
				text := writerV15Oracle(ve7ccWireFixture().FormatDXCluster() + "\n")
				ve7ccTCPSpot(t, w, text)
				ve7ccTCPSelector(t, w, "sEt/Ve7Cc", "VE7CC enabled for this session.\r\n")
				ve7ccTCPSpot(t, w, ve7ccWireSpot)
				if _, err := io.WriteString(w.conn, "SHOW DX 1\r\n"); err != nil {
					t.Fatal(err)
				}
				wantHistory := ve7ccHistoryWireSpot + "End of retained history search.\r\n"
				if got := w.throughLine("End of retained history search."); got != wantHistory || historyReads.Load() != 1 {
					t.Fatalf("connected native history:\n got %q\nwant %q; archive reads=%d", got, wantHistory, historyReads.Load())
				}
				waitUpgradeOutputReleased(t, s, call)
				ve7ccTCPSelector(t, w, "UNSET/VE7CC", "VE7CC disabled for this session.\r\n")
				current, _ = w.configuration("RESUME\r\n")
				waitUpgradeOutputReleased(t, s, call)
				if !current.Equal(saved) {
					t.Fatal("selectors changed saved settings or either filter bank")
				}
				ve7ccTCPSpot(t, w, text)

				// Disconnect while *enabled*, not after UNSET: otherwise an
				// accidentally persisted mode could pass the reconnect oracle.
				ve7ccTCPSelector(t, w, "SET/VE7CC", "VE7CC enabled for this session.\r\n")
				ve7ccTCPSpot(t, w, ve7ccWireSpot)
				closeHandshakeTranscriptSession(t, w.conn, done)
				writerV15Wait(t, w.client.writerStopped, "first connected writer shutdown")
				next, _ := ve7ccConnectWitness(t, s, call)
				restored, _ := next.configuration("RESUME\r\n")
				waitUpgradeOutputReleased(t, s, call)
				if !restored.Equal(saved) || restored.Settings.Dialect != string(dialect) || next.client == w.client {
					t.Fatal("reconnect changed saved banks/dialect or reused the old connection owner")
				}
				ve7ccTCPSpot(t, next, text)
			})
		}
	}
}
