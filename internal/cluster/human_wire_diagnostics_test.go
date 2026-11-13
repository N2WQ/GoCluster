package cluster

import (
	"context"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/asynclog"
	"dxcluster/rbn"
)

// Follow a real source reader through the standard logger's bounded admission
// path. A local formatting test alone cannot detect an oversized dropped record.
func TestHumanWireTimeDiagnosticsThroughLogAdmission(t *testing.T) {
	for _, retired := range []bool{false, true} {
		name := "admitted"
		if retired {
			name = "retired"
		}
		t.Run(name, func(t *testing.T) {
			listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			host, port := splitRuntimeTestAddress(t, listener.Addr().String())
			admission := &fanoutTestAdmission{}
			var fanout systemLogFanout
			fanout.setFile(admission)
			fanout.framer.closed = retired
			previousWriter, previousFlags, previousPrefix := log.Writer(), log.Flags(), log.Prefix()
			log.SetOutput(&fanout)
			log.SetFlags(0)
			log.SetPrefix("")
			defer func() {
				log.SetOutput(previousWriter)
				log.SetFlags(previousFlags)
				log.SetPrefix(previousPrefix)
			}()
			// Configuration bounds real source names. Also defend direct constructor
			// callers with a longer name and worst-case escaping in this witness.
			client := rbn.NewClient(host, port, "N0CALL", strings.Repeat("\x01", 200), nil, false, 1)
			client.UseMinimalParser()
			client.SetTelnetTransport("native")
			if err := client.Connect(); err != nil {
				t.Fatal(err)
			}
			defer client.Stop()
			conn := acceptRuntimeTestConnection(t, listener, 0)
			defer conn.Close()
			line := "DX de W1AW: 14074 K1ABC FT8 " + strings.Repeat("\x01", 800) + " 1200Z FN20\r\n"
			if _, err := conn.Write([]byte(line + line)); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(4 * time.Second)
			for client.HealthSnapshot().HumanWireTimeRejects != 2 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			client.Stop() // Join the reader before inspecting the logger's test sink.
			if client.HealthSnapshot().HumanWireTimeRejects != 2 {
				t.Fatal("suppressed or unadmitted logs prevented rejection counting")
			}
			var samples []string
			for _, record := range admission.records {
				if strings.Contains(record, "reason=unusable_wire_time") {
					samples = append(samples, record)
				}
			}
			if retired {
				if len(samples) != 0 || admission.drops == 0 {
					t.Fatalf("retired logger unexpectedly admitted samples: %+v", admission)
				}
			} else {
				if len(samples) != 1 {
					t.Fatalf("expected one admitted rate-limited sample: %+v", admission)
				}
				sample := samples[0]
				if len(sample) > asynclog.MaxRecordBytes || !strings.Contains(sample, `truncated=true`) || !strings.Contains(sample, `FN20"`) || !strings.Contains(sample, `\x01`) || strings.ContainsRune(sample, '\x01') {
					t.Fatalf("sample lost its bound, escaping or terminal evidence: %q", sample)
				}
			}
			source := rbnHealthSourceWithID("human:feed", "HUMAN/FEED", client)
			snap := source.snapshot()
			if snap.HumanWireTimeRejects != 2 || snap.SpotDrops != 0 || !snap.LastSpotAt.IsZero() || ingestIsIdle(snap, time.Now()) {
				t.Fatalf("health adapter changed rejection count or liveness rules: %+v", snap)
			}
			if got := formatIngestHealthLine(source.name, snap, false, time.Now()); !strings.Contains(got, "wire_time=2") {
				t.Fatalf("health output omitted wire rejection total: %q", got)
			}
		})
	}
}
