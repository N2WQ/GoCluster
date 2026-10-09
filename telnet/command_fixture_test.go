package telnet

// This child-process fixture exercises the checkout's telnet, command, storage
// and archive implementations. Its manual-spot bridge is deliberately not the
// production correction/dedupe/classifier pipeline; checkpoint 20b owns that
// shutdown proof. Child exit is the final barrier before parent store cleanup.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/reputation"
	"dxcluster/spot"
	"dxcluster/uls"
)

type commandFixtureSpec struct {
	Nonce             string `json:"nonce"`
	Root              string `json:"root"`
	SourceDigest      string `json:"source_digest"`
	ReputationEnabled bool   `json:"reputation_enabled"`
}

type commandFixtureReceipt struct {
	Nonce             string            `json:"nonce"`
	PID               int               `json:"pid"`
	Address           string            `json:"address"`
	BinaryHash        string            `json:"binary_hash"`
	SourceDigest      string            `json:"source_digest"`
	ConfigHashes      map[string]string `json:"config_hashes"`
	Started           bool              `json:"started"`
	ClientsRetired    bool              `json:"clients_retired"`
	ConsumerJoined    bool              `json:"consumer_joined"`
	ArchiveStopped    bool              `json:"archive_stopped"`
	ReputationEnabled bool              `json:"reputation_enabled"`
}

func loadCommandFixtureInputs(t *testing.T, root string) (*config.Config, *cty.CTYDatabase) {
	t.Helper()
	configDir := filepath.Join(root, "config")
	cfg, err := config.Load(configDir)
	if err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(filepath.Join(configDir, "spot_taxonomy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	spot.ConfigureTaxonomy(taxonomy)
	for _, load := range []struct {
		name string
		fn   func(string) error
	}{{"iaru_regions.yaml", spot.LoadIARURegionsFile}, {"iaru_mode_inference.yaml", spot.LoadIARUModeInferenceFile}} {
		if err := load.fn(filepath.Join(configDir, load.name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := pathreliability.InitH3MappingsFromDir(filepath.Join(root, "h3")); err != nil {
		t.Fatal(err)
	}
	if pathreliability.EncodeCell("FN31") == pathreliability.InvalidCell || pathreliability.EncodeCoarseCell("FN31") == pathreliability.InvalidCell {
		t.Fatal("loaded H3 mappings cannot resolve the control grid")
	}
	ctyDB, err := cty.LoadCTYDatabase(filepath.Join(root, "cty.plist"))
	if err != nil {
		t.Fatal(err)
	}
	filter.UserDataDir = filepath.Join(root, "users")
	filter.SetDefaultModeSelection(cfg.Filter.DefaultModes)
	filter.SetDefaultSourceSelection(cfg.Filter.DefaultSources)
	uls.SetLicenseChecksEnabled(false)
	uls.SetCanadianLicenseChecksEnabled(false)
	uls.SetLicenseDBPath(filepath.Join(root, "fcc.sqlite"))
	uls.SetCanadianLicenseDBPath(filepath.Join(root, "ised.sqlite"))
	return cfg, ctyDB
}

func fixtureHash(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func writeFixtureReceipt(path string, receipt commandFixtureReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".new", data, 0600); err != nil {
		return err
	}
	return os.Rename(path+".new", path)
}

// TestCheckoutCommandFixture runs only when the local driver supplies a private
// specification. Ordinary package tests do not qualify fixture execution.
func TestCheckoutCommandFixture(t *testing.T) {
	specPath := os.Getenv("GOCLUSTER_COMMAND_FIXTURE")
	if specPath == "" {
		t.Log("checkout fixture requires scripts/test-command-fixes.py")
		return
	}
	data, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatal(err)
	}
	var spec commandFixtureSpec
	if err := json.Unmarshal(data, &spec); err != nil || len(spec.Nonce) < 8 || !filepath.IsAbs(spec.Root) {
		t.Fatalf("invalid fixture specification: %v", err)
	}
	configDir := filepath.Join(spec.Root, "config")
	cfg, ctyDB := loadCommandFixtureInputs(t, spec.Root)
	defer uls.CloseLicenseDatabases()
	if cfg.Reputation.Enabled != spec.ReputationEnabled {
		t.Fatal("loaded reputation profile differs from requested fixture")
	}
	var gate *reputation.Gate
	if spec.ReputationEnabled {
		// Construct the production gate from the private loaded config, sharing
		// it between login and manual posting. Start would launch enrichment and
		// maintenance workers; those are deliberately outside this local proof.
		if cfg.Reputation.ReputationDir != filepath.Join(spec.Root, "reputation") ||
			cfg.Reputation.IPInfoDownloadEnabled || cfg.Reputation.IPInfoAPIEnabled || cfg.Reputation.FallbackTeamCymru ||
			cfg.Reputation.InitialWaitSeconds != 60 || cfg.Reputation.RampWindowSeconds != 3600 ||
			cfg.Reputation.PerBandStart != 1 || cfg.Reputation.PerBandCap != 1 ||
			cfg.Reputation.TotalCapStart != 2 || cfg.Reputation.TotalCapPostRamp != 2 ||
			cfg.Reputation.IPv4BucketSize != 10000 || cfg.Reputation.IPv4BucketRefillPerSec != 10000 ||
			cfg.Reputation.IPv6BucketSize != 10000 || cfg.Reputation.IPv6BucketRefillPerSec != 10000 {
			t.Fatal("reputation profile lacks the isolated loaded quota controls")
		}
		gate, err = reputation.NewGate(cfg.Reputation, func() *cty.CTYDatabase { return ctyDB })
		if err != nil {
			t.Fatal(err)
		}
		defer gate.Close()
	}
	writer, err := archive.NewWriter(cfg.Archive)
	if err != nil {
		t.Fatal(err)
	}
	writer.Start()
	spotInput := make(chan *spot.Spot, 32)
	processor := commands.NewProcessor(nil, writer, spotInput, func() *cty.CTYDatabase { return ctyDB }, gate, nil,
		commands.WithBuildInfo(commands.BuildInfo{Version: "fixture-" + spec.Nonce, GoVersion: runtime.Version()}))
	options := commandFixtureOptions(cfg, ctyDB)
	options.ReputationGate = gate
	server := NewServer(options, processor)
	if server.reputationGate != gate {
		writer.Stop()
		t.Fatal("fixture login and posting gates differ")
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		writer.Stop()
		t.Fatal(err)
	}
	server.startWithListener(listener)
	ctx, cancel := context.WithCancel(context.Background())
	var consumer sync.WaitGroup
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		for {
			select {
			case <-ctx.Done():
				// Server owners have retired before cancellation. Drain their
				// bounded admitted queue before closing the archive.
				for {
					select {
					case posted := <-spotInput:
						writer.Enqueue(posted)
					default:
						return
					}
				}
			case posted := <-spotInput:
				// Both APIs snapshot independently before asynchronous publication.
				writer.Enqueue(posted)
				server.BroadcastSpot(posted, true, true, true)
			}
		}
	}()
	receipt := commandFixtureReceipt{Nonce: spec.Nonce, PID: os.Getpid(), Address: listener.Addr().String(),
		SourceDigest: spec.SourceDigest, ConfigHashes: make(map[string]string), Started: true,
		ReputationEnabled: gate != nil}
	defer func() {
		server.Stop()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			active, _, _, _, _, _, _ := server.PreloginMetricSnapshot()
			if len(server.ListClientCallsigns()) == 0 && active == 0 {
				receipt.ClientsRetired = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		consumer.Wait()
		receipt.ConsumerJoined = true
		writer.Stop()
		receipt.ArchiveStopped = true
		if err := writeFixtureReceipt(filepath.Join(spec.Root, "stopped-"+spec.Nonce+".json"), receipt); err != nil {
			t.Error(err)
		}
		if !receipt.ClientsRetired {
			t.Error("client storage owners did not retire before fixture shutdown")
		}
	}()
	// Fixed synthetic metadata controls use the existing bounded bridge. They
	// qualify archive/history NEARBY decisions, not manual admission or metadata
	// enrichment. Each child has distinct short labels and each row a distinct
	// frequency, so neither an old row nor an archive key collision can qualify it.
	for _, seed := range []struct {
		name      string
		frequency float64
		dx, de    string
	}{
		{"DX20", 14021, "FN31", ""}, {"DE20", 14022, "", "FN31"},
		{"DXBAD20", 14023, "FN31", "BAD"}, {"DEBAD20", 14024, "BAD", "FN31"},
		{"NONE20", 14025, "", ""}, {"FAR20", 14026, "EM10", ""},
		{"DX80", 3521, "FN31", ""}, {"DE80", 3522, "", "FN31"},
		{"DX60", 5355, "FN31", ""}, {"DE160", 1821, "", "FN31"},
	} {
		sp := spot.NewSpotNormalized("W6ABC", "N0XYZ", seed.frequency, "CW")
		sp.Comment = "N08" + strings.ToUpper(spec.Nonce[:8]) + seed.name
		sp.IsHuman = true
		sp.DXMetadata.Grid, sp.DEMetadata.Grid = seed.dx, seed.de
		spotInput <- sp
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	receipt.BinaryHash, err = fixtureHash(binary)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(configDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		digest, err := fixtureHash(filepath.Join(configDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		receipt.ConfigHashes[entry.Name()] = digest
	}
	if err := writeFixtureReceipt(filepath.Join(spec.Root, "ready-"+spec.Nonce+".json"), receipt); err != nil {
		t.Fatal(err)
	}
	control := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			control <- scanner.Text()
		} else {
			control <- "EOF"
		}
	}()
	select {
	case line := <-control:
		if line != "STOP" {
			t.Fatalf("fixture control failed: %s", line)
		}
	case <-time.After(150 * time.Second):
		t.Fatal("fixture control deadline expired")
	}
	fmt.Println("CHECKOUT_FIXTURE_EXECUTED " + spec.Nonce)
}
