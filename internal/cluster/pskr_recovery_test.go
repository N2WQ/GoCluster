package cluster

import (
	"context"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestPSKReporterRuntimeRecoversBothRoutes(t *testing.T) {
	if runtime.GOOS == "windows" {
		ordinaryHarnessRequireExecutable(t)
	}
	requirePathReportH3Mappings(t)
	pathCfg, err := pathreliability.LoadFile(filepath.Join(pathShardingRepoRoot(), "data", "config", "path_reliability.yaml"))
	if err != nil || !pathCfg.Enabled {
		t.Fatalf("load path predictor: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().(*net.TCPAddr)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	unavailable := make(chan struct{})
	var reportOnce sync.Once

	cfg := &config.Config{SpotPolicy: config.SpotPolicy{MaxAgeSeconds: 300}}
	r := newClusterRuntime(BuildInfo{}, cfg, "", config.LoadDiagnostics{})
	r.ctx = ctx
	r.eventFileLogger = &eventFileLogger{ingestConnections: &eventLogSink{
		sink: &ioLineSink{w: pskrInitialFailureObserver{
			fragment: "source=PSKReporter action=failed endpoint=" + address.String() + " state=disconnected reason=connect:",
			notify:   func() { reportOnce.Do(func() { close(unavailable) }) },
		}},
	}}
	ingest := make(chan *spot.Spot, 8)
	r.ingestInput = ingest
	r.pathPredictor = pathreliability.NewPredictor(pathCfg, pathCfg.AllowedBands)
	r.allowedBandSet = map[string]struct{}{"20m": {}}
	r.connectPSKReporterFeed()
	if r.pskrClient != nil {
		t.Fatal("disabled PSKReporter created a client")
	}
	cfg.PSKReporter = config.PSKReporterConfig{Enabled: true, Broker: "127.0.0.1", Port: address.Port, Topic: "s5/recovery", Workers: 1}
	t.Cleanup(r.stopPSKReporterFeed)
	r.connectPSKReporterFeed()
	client := r.pskrClient
	if client == nil {
		t.Fatal("enabled PSKReporter did not start")
	}
	r.connectPSKReporterFeed()
	if r.pskrClient != client {
		t.Fatal("second call replaced the feed")
	}
	select {
	case <-unavailable:
	case <-ctx.Done():
		t.Fatal("no initial dial failure before broker startup")
	}
	if client.IsConnected() {
		t.Fatal("unavailable broker reported ready")
	}
	observed := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Second)
	cases := pskrZeroCases()
	normal, pathOnly := cases[4], cases[5]
	broker, err := startPSKRZeroBrokerOnPort(ctx, cfg.PSKReporter.Topic, pskrZeroCorpus([]pskrZeroCase{normal, pathOnly}, observed.Unix()), address.Port)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.stopPSKReporterFeed(); _, _ = broker.Close() })
	select {
	case got := <-ingest:
		if got.DXCall != normal.call || got.Mode != "FT8" {
			t.Fatalf("wrong ordinary route output: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("recovered feed did not reach the ordinary ingest boundary")
	}
	pskrZeroAwaitPopulation(ctx, t, r.pathPredictor, pathOnly, observed)
	if r.pskrPathOnlyStats.updates.Load() == 0 {
		t.Fatal("recovered WSPR observation did not pass the production path-only consumer")
	}

	r.stopPSKReporterFeed()
	if client.IsConnected() || len(ingest) != 0 {
		t.Fatal("retired feed reported ready or sent an unexpected duplicate spot")
	}
	result, err := broker.Close()
	if err != nil || result.Connections != 1 || result.Subscriptions != 1 || result.Published != 2 || !result.ClientClosed {
		t.Fatalf("unexpected broker recovery/retirement result: %+v, %v", result, err)
	}
}

type pskrInitialFailureObserver struct {
	fragment string
	notify   func()
}

func (observer pskrInitialFailureObserver) Write(line []byte) (int, error) {
	if strings.Contains(string(line), observer.fragment) {
		observer.notify()
	}
	return len(line), nil
}
