package telnet

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

// These witnesses use the existing real-TCP login/handleClient/writer fixture.
// Injection enters production fan-out after upstream classification. It does not
// claim to exercise the upstream correction/classification pipeline.
type nativeTCPWitness struct {
	t      *testing.T
	server *Server
	client *Client
	conn   net.Conn
	reader *bufio.Reader
	serial int
}

func (w *nativeTCPWitness) configuration(prefix string) (filter.V7Configuration, string) {
	w.t.Helper()
	if err := w.conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		w.t.Fatal(err)
	}
	if _, err := io.WriteString(w.conn, prefix+"GET YAML CONFIG SCHEMA 7 ID Native-I14\r\n"); err != nil {
		w.t.Fatal(err)
	}
	var response, frame strings.Builder
	for response.Len() <= maxV7YAMLBytes {
		line, err := w.reader.ReadString('\n')
		if err != nil {
			w.t.Fatalf("native TCP configuration barrier: %v, partial=%q", err, response.String())
		}
		response.WriteString(line)
		if line == "---\r\n" || frame.Len() != 0 {
			frame.WriteString(line)
		}
		if line != "...\r\n" || frame.Len() == 0 {
			continue
		}
		var document struct {
			SchemaVersion int                    `yaml:"schema_version"`
			RequestID     string                 `yaml:"request_id"`
			Configuration filter.V7Configuration `yaml:"configuration"`
			Error         *struct{ Code string } `yaml:"error"`
		}
		if err := yaml.Unmarshal([]byte(frame.String()), &document); err != nil {
			w.t.Fatal(err)
		}
		if document.SchemaVersion != 7 || document.RequestID != "Native-I14" || document.Error != nil {
			w.t.Fatalf("native TCP barrier failed: %s", response.String())
		}
		return document.Configuration, response.String()
	}
	w.t.Fatal("native TCP configuration barrier exceeded its response bound")
	return filter.V7Configuration{}, ""
}

func (w *nativeTCPWitness) throughLine(contains string) string {
	w.t.Helper()
	if err := w.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		w.t.Fatal(err)
	}
	var response strings.Builder
	for response.Len() <= maxV7YAMLBytes {
		line, err := w.reader.ReadString('\n')
		if err != nil {
			w.t.Fatalf("native TCP positive line %q: %v, partial=%q", contains, err, response.String())
		}
		response.WriteString(line)
		if strings.Contains(line, contains) {
			return response.String()
		}
	}
	w.t.Fatal("native TCP witness exceeded its response bound")
	return ""
}

func (w *nativeTCPWitness) makeSpot(mode string, source spot.SourceType) *spot.Spot {
	w.serial++
	sp := spot.NewSpot(fmt.Sprintf("K1A%c%c", 'A'+w.serial/26, 'A'+w.serial%26), "W2ABC", 14025, mode)
	sp.SourceType = source
	spot.ApplySourceHumanFlag(sp)
	return sp
}

func (w *nativeTCPWitness) inject(sp *spot.Spot) {
	w.server.deliverJob(broadcastJob{spot: sp, clients: []*Client{w.client}, allowFast: true, allowMed: true, allowSlow: true})
}

func (w *nativeTCPWitness) positive(mode string, source spot.SourceType) string {
	w.t.Helper()
	sp := w.makeSpot(mode, source)
	w.inject(sp)
	response := w.throughLine(sp.DXCall)
	if !strings.Contains(response, "DX de") {
		w.t.Fatalf("positive witness was not actual spot output: %q", response)
	}
	return response
}

// The accepted sentinel follows the negative through the same FIFO spot queue.
// Reading a control barrier alone could overtake a mistakenly queued spot.
func (w *nativeTCPWitness) negativeThenPositive(mode string, source spot.SourceType, acceptedMode string, acceptedSource spot.SourceType) {
	w.t.Helper()
	negative, sentinel := w.makeSpot(mode, source), w.makeSpot(acceptedMode, acceptedSource)
	w.inject(negative)
	w.inject(sentinel)
	response := w.throughLine(sentinel.DXCall)
	if !strings.Contains(response, "DX de") || strings.Contains(response, negative.DXCall) {
		w.t.Fatalf("native filter leaked negative %s before accepted sentinel: %q", negative.DXCall, response)
	}
}

func TestNativeCCDeliveryResumeDialectAndReconnectTCP(t *testing.T) {
	if err := filter.ConfigureDefaultRules([]string{"CW", "FT8"}, []string{"HUMAN", "SKIMMER"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := filter.ConfigureDefaultRules(nil, nil); err != nil {
			t.Error(err)
		}
	})
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "READY\n"
				opts.CommandRatePerSecond, opts.CommandBurst = 1000, 1000
				opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
			})
			call := "N0CALL-77"
			conn, reader, done, _ := connectUpgradeSession(t, s, call)
			s.clientsMutex.RLock()
			client := s.clients[call]
			s.clientsMutex.RUnlock()
			if client == nil {
				t.Fatal("real TCP login did not register a delivery owner")
			}
			w := nativeTCPWitness{t: t, server: s, client: client, conn: conn, reader: reader}
			cfg, response := w.configuration("DIALECT CC\r\nSET/NOFT8\r\n")
			if cfg.Settings.Dialect != "cc" || slices.Contains(cfg.Filters.CC.Modes, "FT8") || !slices.Equal(cfg.Filters.CC.Sources, []string{"HUMAN", "SKIMMER"}) || strings.Contains(response, "No changes applied.") {
				t.Fatal("real CC shortcut did not establish the requested saved mode/source state")
			}
			if _, err := io.WriteString(conn, "SHOW/FILTER FULL\r\n"); err != nil {
				t.Fatal(err)
			}
			readback := w.throughLine("Type RESUME when ready.")
			if !strings.Contains(readback, "Active filter bank: CC") {
				t.Fatal("real native readback reached the Boolean renderer")
			}
			if !client.readPausePending.Load() && client.readPauseUntilUnixNano.Load() <= s.now().UnixNano() {
				t.Fatal("native human readback failed to suppress live spots")
			}
			held := w.makeSpot("CW", spot.SourceManual)
			w.inject(held)
			_, resumed := w.configuration("RESUME\r\n")
			positive := w.positive("CW", spot.SourceManual)
			if strings.Contains(resumed+positive, held.DXCall) {
				t.Fatal("a suppressed native spot was replayed after RESUME")
			}
			w.positive("CW", spot.SourceRBN)
			w.negativeThenPositive("FT8", spot.SourceManual, "CW", spot.SourceManual)
			w.negativeThenPositive("FT8", spot.SourceRBN, "CW", spot.SourceRBN)
			cfg, _ = w.configuration("SET/NOSKIMMER\r\n")
			if !slices.Equal(cfg.Filters.CC.Sources, []string{"HUMAN"}) {
				t.Fatal("real source edit did not retain its native selection")
			}
			w.positive("CW", spot.SourceManual)
			w.negativeThenPositive("CW", spot.SourceRBN, "CW", spot.SourceManual)
			retainedCC := cfg.Filters.CC.Clone()
			cfg, _ = w.configuration("DIALECT GO\r\nPASS RULE 0 MODE=FT8 AND SOURCE=SKIMMER\r\n")
			if cfg.Settings.Dialect != "go" || !cfg.Filters.CC.Equal(retainedCC) {
				t.Fatal("real GO activation changed the inactive native bank")
			}
			w.positive("FT8", spot.SourceRBN)
			w.negativeThenPositive("CW", spot.SourceManual, "FT8", spot.SourceRBN)
			final, _ := w.configuration("DIALECT CC\r\n")
			if !final.Filters.CC.Equal(retainedCC) {
				t.Fatal("native bank changed while GO was active")
			}
			w.positive("CW", spot.SourceManual)
			w.negativeThenPositive("CW", spot.SourceRBN, "CW", spot.SourceManual)
			closeHandshakeTranscriptSession(t, conn, done)
			conn, reader, _, _ = connectUpgradeSession(t, s, call)
			s.clientsMutex.RLock()
			w.client = s.clients[call]
			s.clientsMutex.RUnlock()
			w.conn, w.reader = conn, reader
			restored, _ := w.configuration("RESUME\r\n")
			if !restored.Equal(final) || restored.Settings.Dialect != "cc" {
				t.Fatal("reconnect reset a saved bank or selected the wrong dialect")
			}
			w.positive("CW", spot.SourceManual)
			w.negativeThenPositive("FT8", spot.SourceManual, "CW", spot.SourceManual)
			w.negativeThenPositive("CW", spot.SourceRBN, "CW", spot.SourceManual)
		})
	}
}
