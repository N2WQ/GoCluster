package cluster

import (
	"context"
	"net"
	"strings"
	"testing"

	"dxcluster/config"
	"dxcluster/telnet"
)

func peerRuntimeTestConfig() *config.Config {
	return &config.Config{Peering: config.PeeringConfig{
		Enabled: true, MaxPeers: 64, LocalCallsign: "N0CALL-1", HopCount: 99,
		NodeVersion: "5457", NodeBuild: "633", LegacyVersion: "5401", PC92Bitmap: 5,
		MaxLineLength: 65536, PC92MaxBytes: 65536, WriteQueueSize: 128,
	}}
}

func peerRuntimeTestBuild() BuildInfo {
	return BuildInfo{
		Version: "v26.01.10", Commit: "91abcdef0123456789", BuildTime: "2026-10-01T12:00:00Z",
		GoVersion:  "go1.26.0",
		ReleaseTag: "261001r2",
	}
}

func TestPeerInvalidBuildIdentityFailsBeforeNetworkStartup(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*BuildInfo)
	}{
		{"oversized", func(build *BuildInfo) { build.Version = strings.Repeat("x", 4096) }},
		{"oversized release tag", func(build *BuildInfo) { build.ReleaseTag = strings.Repeat("x", 4096) }},
		{"escaped expansion", func(build *BuildInfo) { build.Commit = strings.Repeat("^", 300) }},
		{"combined fields", func(build *BuildInfo) {
			build.Version = strings.Repeat("v", 400)
			build.Commit = strings.Repeat("a", 400)
			build.GoVersion = strings.Repeat("g", 400)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			build := peerRuntimeTestBuild()
			test.mutate(&build)
			runtime := newClusterRuntime(build, peerRuntimeTestConfig(), "", config.LoadDiagnostics{})
			t.Cleanup(runtime.close)
			// The invalid identity is discovered at the first service boundary,
			// before a telnet listener, ingest producer or peer listener starts.
			if runtime.initializeServices() {
				t.Fatal("unsafe peer identity allowed service startup")
			}
			if runtime.startupErr == nil || !strings.Contains(runtime.startupErr.Error(), "Invalid peering build identity") {
				t.Fatalf("wrong startup failure: %v", runtime.startupErr)
			}
			if runtime.telnetServer != nil {
				t.Fatal("telnet started before peer identity was validated")
			}
		})
	}
}

func TestPeerDisabledDoesNotRequireWireBuildIdentity(t *testing.T) {
	cfg := peerRuntimeTestConfig()
	cfg.Peering.Enabled = false
	runtime := newClusterRuntime(BuildInfo{Version: "non-wire^identity"}, cfg, "", config.LoadDiagnostics{})
	if !runtime.initializePeerManager() || runtime.peerManager != nil || runtime.startupErr != nil {
		t.Fatalf("disabled peering validated an unused wire identity: %v", runtime.startupErr)
	}
}

func TestPeerConstructionDoesNotStartListener(t *testing.T) {
	listenerConfig := net.ListenConfig{}
	reserved, err := listenerConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reserved.Close() })
	cfg := peerRuntimeTestConfig()
	cfg.Peering.ListenPort = reserved.Addr().(*net.TCPAddr).Port
	runtime := newClusterRuntime(peerRuntimeTestBuild(), cfg, "", config.LoadDiagnostics{})
	runtime.ctx, runtime.cancel = context.WithCancel(context.Background())
	t.Cleanup(runtime.close)
	// Keeping the selected port occupied makes premature Start observable.
	if !runtime.initializePeerManager() {
		t.Fatalf("construction tried to listen before providers were installed: %v", runtime.startupErr)
	}
	if runtime.peerManager == nil {
		t.Fatal("peer manager was not constructed")
	}
}

func TestCurrentPeerMembershipAdapterPreservesEmptyPopulation(t *testing.T) {
	runtime := &clusterRuntime{telnetServer: telnet.NewServer(telnet.ServerOptions{}, nil)}
	t.Cleanup(runtime.telnetServer.Stop)
	got := runtime.currentPeerMembership()
	if !got.Complete || got.Revision != 0 || got.RawCount != 0 || len(got.Users) != 0 {
		t.Fatalf("empty population was turned into missing provider state: %+v", got)
	}
}
