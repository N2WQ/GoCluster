package rbn

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func captureHumanWireLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	output := new(bytes.Buffer)
	previousWriter, previousFlags, previousPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})
	return output
}

func TestHumanWireTimeRejectionLogged(t *testing.T) {
	output := captureHumanWireLogs(t)
	c := NewClient("example.com", 0, "N0FT", "FEED-A", nil, false, 1)
	c.UseMinimalParser()
	c.parseSpot("DX de W1AW: 14074 K1ABC FT8 -14 1200Z FN20")
	if got := output.String(); !strings.Contains(got, "unusable_wire_time") || !strings.Contains(got, "FEED-A") || !strings.Contains(got, "FN20") {
		t.Fatalf("missing actionable rejection sample: %q", got)
	}
}

func TestHumanWireTimeCountersAndDropSeparation(t *testing.T) {
	output := captureHumanWireLogs(t)
	first := NewClient("example.com", 0, "N0FT", "FEED-A", nil, false, 1)
	second := NewClient("example.com", 0, "N0FT", "FEED-B", nil, false, 1)
	first.UseMinimalParser()
	second.UseMinimalParser()
	invalid := "DX de W1AW: 14074 K1ABC FT8 -14 1200Z FN20"
	for range 3 {
		first.parseSpot(invalid)
	}
	second.parseSpot(invalid)
	if strings.Count(output.String(), "reason=unusable_wire_time") != 2 || !strings.Contains(output.String(), `source="FEED-A"`) || !strings.Contains(output.String(), `source="FEED-B"`) {
		t.Fatalf("expected one immediate sample per source despite burst: %q", output.String())
	}
	if first.HealthSnapshot().HumanWireTimeRejects != 3 || second.HealthSnapshot().HumanWireTimeRejects != 1 {
		t.Fatal("source totals were suppressed or shared")
	}
	// Other malformed structures fail before the terminal-time gate.
	first.parseSpot("DX de W1AW: no_frequency ???")
	first.parseSpot("DX de : 14074 K1ABC 1200Z")
	first.parseSpot("DX de W1AW: 14074 ???")
	first.parseSpot("DX de W1AW: 14074 K1ABC FT8 -14 0000Z")
	first.parseSpot("DX de W1AW: 14074 K1ABC FT8 -14 0000Z")
	got := first.HealthSnapshot()
	if got.HumanWireTimeRejects != 3 || got.SpotDrops != 1 || got.LastSpotAt.IsZero() || got.SpotQueueLen != 1 {
		t.Fatalf("gate count, recovery and queue loss conflated: %+v", got)
	}
	legacy := NewClient("example.com", 0, "N0FT", "RBN", nil, false, 1)
	legacy.parseSpot(invalid)
	if legacy.HealthSnapshot().HumanWireTimeRejects != 0 {
		t.Fatal("legacy parsing contributed human wire rejections")
	}
}

func TestHumanWireTimeConcurrentCounterSnapshots(t *testing.T) {
	captureHumanWireLogs(t)
	c := NewClient("example.com", 0, "N0FT", "FEED", nil, false, 1)
	c.UseMinimalParser()
	var writers sync.WaitGroup
	var done atomic.Bool
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for !done.Load() {
			_ = c.HealthSnapshot()
		}
	}()
	for range 8 {
		writers.Go(func() {
			for range 100 {
				c.parseSpot("DX de W1AW: 14074 K1ABC FT8 -14 invalid")
			}
		})
	}
	writers.Wait()
	done.Store(true)
	<-readerDone
	if got := c.HealthSnapshot(); got.HumanWireTimeRejects != 800 || got.SpotDrops != 0 || !got.LastSpotAt.IsZero() {
		t.Fatalf("lost concurrent rejections or changed accepted liveness: %+v", got)
	}
}

type humanWireTestConn struct {
	*controlledConn
	prefix *strings.Reader
}

func (c *humanWireTestConn) Read(p []byte) (int, error) {
	if c.prefix.Len() > 0 {
		return c.prefix.Read(p)
	}
	return c.controlledConn.Read(p)
}

func TestHumanWireTimeCounterSurvivesReconnect(t *testing.T) {
	captureHumanWireLogs(t)
	c := newLifecycleTestClient()
	c.UseMinimalParser()
	c.loginDelay = time.Nanosecond
	c.reconnectInitial, c.reconnectMax = time.Millisecond, time.Millisecond
	line := "DX de W1AW: 14074 K1ABC FT8 -14 invalid\r\n"
	first := &humanWireTestConn{newControlledConn(), strings.NewReader(line)}
	second := &humanWireTestConn{newControlledConn(), strings.NewReader(line)}
	var attempts atomic.Int32
	c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		switch attempts.Add(1) {
		case 1:
			return first, nil
		case 2:
			return second, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	awaitSignal(t, first.readEntered, "first rejected source line")
	if c.HealthSnapshot().HumanWireTimeRejects != 1 {
		t.Fatal("first read loop did not count the rejection")
	}
	first.failRead(io.EOF)
	awaitSignal(t, second.readEntered, "reconnected rejected source line")
	if got := c.HealthSnapshot(); got.HumanWireTimeRejects != 2 || got.LastLineAt.IsZero() || !got.LastSpotAt.IsZero() {
		t.Fatalf("reconnect reset total or changed liveness: %+v", got)
	}
	c.Stop()
	if c.HealthSnapshot().HumanWireTimeRejects != 2 || newLifecycleTestClient().HealthSnapshot().HumanWireTimeRejects != 0 {
		t.Fatal("shutdown/reset lifetime mismatch")
	}
}
