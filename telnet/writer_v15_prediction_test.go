package telnet

import (
	"bufio"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestWriterV15PredictionStateAndAge(t *testing.T) {
	for _, tc := range []struct {
		name        string
		age         time.Duration
		noiseChange bool
		lookups     int64
	}{
		{name: "current", lookups: 1},
		{name: "age-boundary", age: time.Second, lookups: 1},
		{name: "expired", age: time.Second + time.Nanosecond, lookups: 2},
		{name: "clock-backward", age: -time.Nanosecond, lookups: 2},
		{name: "noise-changed", noiseChange: true, lookups: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, client, env, fallback := writerV15PredictionFixture(t, tc.age, tc.noiseChange)
			oracleServer, oracleClient, oracleEnv, _ := writerV15PredictionFixture(t, tc.age, tc.noiseChange)
			want := writerV15Oracle(oracleServer.formatSpotEnvelopeForClient(oracleClient, oracleEnv))
			conn := &writerV15Conn{expected: []byte(want), target: 1, reached: make(chan struct{})}
			client.conn, client.writer, client.server = conn, bufio.NewWriter(conn), server
			client.done, client.controlChan = make(chan struct{}), make(chan controlMessage, 1)
			client.callsign = "N0CALL"
			client.spotChan <- env
			done := writerV15Start(client)
			defer func() { client.close(""); writerV15Wait(t, done, "cleanup") }()
			writerV15Wait(t, conn.reached, "prediction spot")
			client.close("")
			writerV15Wait(t, done, "shutdown")
			writerV15Check(t, conn)
			if got := fallback.cachedCalls.Load(); got != tc.lookups {
				t.Fatalf("prediction lookup count=%d, want %d", got, tc.lookups)
			}
			if got := server.PathPredictionStatsSnapshot().Total; got != 1 {
				t.Fatalf("displayed predictions=%d, want 1", got)
			}
		})
	}
}

func writerV15PredictionFixture(t *testing.T, age time.Duration, noiseChange bool) (*Server, *Client, *spotEnvelope, *countingPathClosedFallback) {
	t.Helper()
	server, client, sp, fallback := newPathPredictionReuseFixture(t, 1, true, true)
	sp.Time = time.Date(2026, time.October, 2, 12, 34, 0, 0, time.UTC)
	client.filter.SetPathClass(filter.PathClassHigh, true)
	env := deliverPathPredictionReuseSpot(t, server, client, sp)
	now := server.now().Add(age)
	server.nowFn = func() time.Time { return now }
	if noiseChange {
		client.pathMu.Lock()
		client.noiseClass = "URBAN"
		client.pathMu.Unlock()
	}
	return server, client, env, fallback
}
