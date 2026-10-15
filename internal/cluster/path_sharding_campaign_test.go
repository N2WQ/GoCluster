// File role: Frozen opt-in S4 connected-consumer campaign and bounded artifact
// summaries. Setup/guards/sorting are outside scored elapsed time. Profiles and
// the real-cadence soak are separate tests and do not supply scored timings.
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
)

type pathShardingLatencySummary struct {
	Role      string `json:"role"`
	Count     int    `json:"count"`
	ZeroCount int    `json:"zero_count"`
	MinimumNS int64  `json:"minimum_ns"`
	P50NS     int64  `json:"p50_ns"`
	P99NS     int64  `json:"p99_ns"`
	P999NS    int64  `json:"p99_9_ns"`
	MaximumNS int64  `json:"maximum_ns"`
}

type pathShardingManifest struct {
	MeasurementProtocol     string                        `json:"measurement_protocol"`
	Scored                  bool                          `json:"scored"`
	Test                    string                        `json:"test"`
	Boundary                string                        `json:"evidence_boundary"`
	LatencyClock            string                        `json:"latency_clock"`
	ClockSource             string                        `json:"clock_source"`
	ClockFrequencyHz        int64                         `json:"clock_frequency_hz"`
	ClockResolution         string                        `json:"clock_resolution_limit"`
	Executable              string                        `json:"executable"`
	ExecutableSHA256        string                        `json:"executable_sha256"`
	GoVersion               string                        `json:"go_version"`
	GOOS                    string                        `json:"goos"`
	GOARCH                  string                        `json:"goarch"`
	GOGC                    string                        `json:"gogc"`
	MemProfileRate          int                           `json:"mem_profile_rate"`
	GOMAXPROCS              int                           `json:"gomaxprocs"`
	Mode                    string                        `json:"receiver_mode"`
	QueueCapacity           int                           `json:"input_queue_capacity"`
	ConfigSHA256            string                        `json:"config_sha256"`
	InputSHA256             string                        `json:"input_sha256"`
	Inputs                  []pathShardingPath            `json:"inputs"`
	Run                     pathShardingRun               `json:"run"`
	Latency                 [4]pathShardingLatencySummary `json:"latency"`
	ResidentBuckets         int                           `json:"resident_buckets"`
	WriterIdentitiesPerRole int                           `json:"writer_identities_per_role"`
	PublicSupportBefore     pathShardingPublicSupport     `json:"public_beacon_support_before"`
	PublicSupportAfter      pathShardingPublicSupport     `json:"public_beacon_support_after"`
}

func TestPathShardingConnectedCampaign(t *testing.T) {
	if os.Getenv("GOCLUSTER_PATH_SHARDING_CAMPAIGN") != "1" {
		t.Skip("opt-in fixed-work S4 connected consumer campaign")
	}
	pathShardingRequireExecutable(t)
	for _, mode := range pathShardingModes {
		t.Run(mode, func(t *testing.T) {
			name := t.Name()
			fixture, run := pathShardingFixedConnected(t, mode, pathShardingCampaignOps)
			report := pathShardingBuildManifest(t, name, fixture, run, true)
			for _, latency := range report.Latency {
				if latency.P999NS <= 0 {
					t.Fatalf("%s p99.9 is not resolvable as a positive interval; a paired latency ratio cannot be scored: %+v", latency.Role, latency)
				}
			}
			dir := pathShardingArtifactDir(t, "connected-"+mode+"-")
			pathShardingWriteJSON(t, filepath.Join(dir, "manifest.json"), report)
			t.Logf("S4 Connected mode=%s elapsed_ns=%d overlap_ns=%d p99.9_ns=%d/%d/%d/%d manifest=%s", mode, run.ElapsedNS, run.OverlapNS,
				report.Latency[0].P999NS, report.Latency[1].P999NS, report.Latency[2].P999NS, report.Latency[3].P999NS, filepath.Join(dir, "manifest.json"))
		})
	}
}

// The bridge must succeed before the outer owner serializes a valid receipt.
// No outer T failure method or B.Context is used by the live callback.
func pathShardingFixedConnected(t *testing.T, mode string, operations int) (fixture *pathShardingConnectedFixture, run pathShardingRun) {
	t.Helper()
	testsupport.RunFixedBenchmark(t, func(b *testing.B, ctx context.Context, cancel context.CancelFunc, clock *testsupport.Clock) bool {
		defer cancel()
		fixture = newPathShardingConnectedFixture(b, mode)
		fixture.channelWitness(b)
		var err error
		run, err = fixture.runOwned(ctx, cancel, b, [4]int{operations, operations, operations, operations}, clock, pathShardingRunControl{})
		if err != nil {
			b.Fatal(err)
		}
		if run.OverlapNS <= 0 {
			b.Fatal("fixed roles never overlapped")
		}
		fixture.guard(b)
		return true
	})
	return fixture, run
}

func TestPathShardingRepairedPreflight(t *testing.T) {
	if os.Getenv("GOCLUSTER_PATH_SHARDING_REPAIRED_PREFLIGHT") != "1" {
		t.Skip("opt-in unscored repaired connected integration")
	}
	pathShardingRequireExecutable(t)
	for _, mode := range pathShardingModes {
		t.Run(mode, func(t *testing.T) {
			name := t.Name()
			fixture, run := pathShardingFixedConnected(t, mode, 262144)
			report := pathShardingBuildManifest(t, name, fixture, run, false)
			dir := pathShardingArtifactDir(t, "repaired-connected-preflight-"+mode+"-")
			pathShardingWriteJSON(t, filepath.Join(dir, "manifest.json"), report)
			t.Logf("unscored repaired connected integration: %s", dir)
		})
	}
}

func pathShardingBuildManifest(t testing.TB, name string, fixture *pathShardingConnectedFixture, run pathShardingRun, scored bool) pathShardingManifest {
	t.Helper()
	if run.ClockSource != "testing.B.Elapsed(shared live benchmark)" {
		t.Fatalf("repaired receipt used wrong clock domain: %s", run.ClockSource)
	}
	if scored {
		for _, role := range run.Roles {
			if role.Completed != pathShardingCampaignOps || role.Submitted != pathShardingCampaignOps {
				t.Fatal("nonqualifying fixed population cannot be scored")
			}
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(struct {
		Paths       []pathShardingPath `json:"paths"`
		Permutation string             `json:"permutation"`
		Queue       int                `json:"queue"`
		Counts      [4]uint64          `json:"ops_per_role"`
		Starts      [4]uint64          `json:"start_sequence_per_role"`
	}{fixture.paths, "index=(sequence*17+role*7)%paths; direction=(sequence/paths)&1; receiver=K1S(endpointFine*64+12+role*24+(sequence/(2*paths))%24); warm receivers=K1S(endpointFine*64+[0..11])", pathShardingQueueSize,
		[4]uint64{run.Roles[0].Submitted, run.Roles[1].Submitted, run.Roles[2].Submitted, run.Roles[3].Submitted},
		[4]uint64{run.Roles[0].StartSequence, run.Roles[1].StartSequence, run.Roles[2].StartSequence, run.Roles[3].StartSequence}})
	if err != nil {
		t.Fatal(err)
	}
	report := pathShardingManifest{MeasurementProtocol: testsupport.FixedBenchmarkProtocol, Scored: scored, Test: name, Boundary: "Actual normal prepareFanoutSpot and path-only item consumer, bounded submission-to-completion queues, and public ordinary/beacon Predictor readers. The unchanged path-only channel loop has a separate preflight. Telnet wire correctness is a separate package witness; these samples are not end-to-end wire latency.",
		LatencyClock: "Every qualifying submission, consumer and maintenance timestamp uses one primed live testing.B.Elapsed domain; no owner reads it after callback return.", ClockSource: run.ClockSource, ClockFrequencyHz: run.ClockFrequencyHz,
		ClockResolution: "Exact zero intervals are retained and counted; missing/negative intervals fail. A frequency is not a measured effective resolution. Scoring requires a positive p99.9 interval.",
		Executable:      executable, ExecutableSHA256: pathShardingFileHash(t, executable), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOGC: os.Getenv("GOGC"), MemProfileRate: runtime.MemProfileRate, GOMAXPROCS: runtime.GOMAXPROCS(0), Mode: fixture.cfg.ReceiverContributionMode,
		QueueCapacity: pathShardingQueueSize, ConfigSHA256: pathShardingFileHash(t, filepath.Join(pathShardingRepoRoot(), "data", "config", "path_reliability.yaml")), InputSHA256: pathShardingHash(input), Inputs: fixture.paths,
		Run: run, ResidentBuckets: fixture.predictor.TotalBuckets(), WriterIdentitiesPerRole: 24, PublicSupportBefore: fixture.initialSupport, PublicSupportAfter: fixture.lastSupport}
	for role, completed := range run.Roles {
		samples := completed.Samples
		if len(samples) == 0 {
			t.Fatalf("missing %s complete latency population", completed.Name)
		}
		slices.Sort(samples)
		zeros := 0
		for _, sample := range samples {
			if sample < 0 {
				t.Fatalf("%s contains a missing/negative latency: %d", completed.Name, sample)
			}
			if sample == 0 {
				zeros++
			}
		}
		report.Latency[role] = pathShardingLatencySummary{Role: completed.Name, Count: len(samples), ZeroCount: zeros, MinimumNS: samples[0], P50NS: pathShardingQuantile(samples, 500),
			P99NS: pathShardingQuantile(samples, 990), P999NS: pathShardingQuantile(samples, 999), MaximumNS: samples[len(samples)-1]}
	}
	return report
}

// Nearest-rank percentiles of the entire completion population. No clipping,
// sampling, missing-value substitution or cross-role pooling is permitted.
func pathShardingQuantile(sorted []int64, permille int) int64 {
	position := (len(sorted)*permille+999)/1000 - 1
	return sorted[max(0, min(position, len(sorted)-1))]
}

func pathShardingRequireExecutable(t testing.TB) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil || !strings.EqualFold(filepath.Clean(executable), filepath.Clean(pathShardingApprovedExecutable)) {
		t.Fatalf("controlled S4 fixtures require approved executable %s; got %q error=%v", pathShardingApprovedExecutable, executable, err)
	}
}

func pathShardingArtifactDir(t testing.TB, prefix string) string {
	t.Helper()
	root := filepath.Join(pathShardingRepoRoot(), ".tmp", "path-reliability-first-wave", "s4")
	if requested := os.Getenv("GOCLUSTER_PATH_SHARDING_ARTIFACT_DIR"); requested != "" {
		root = requested
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	approved := `C:\src\gocluster\.tmp\path-reliability-first-wave`
	if runtime.GOOS != "windows" {
		// Default lifecycle checks also run in Linux CI. Their portless profile
		// artifacts stay in this checkout; native Windows keeps its approved root.
		approved = filepath.Join(pathShardingRepoRoot(), ".tmp", "path-reliability-first-wave")
	}
	relative, err := filepath.Rel(approved, root)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		t.Fatalf("artifact path must stay inside %s: %s (%v)", approved, root, err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func pathShardingFileHash(t testing.TB, file string) string {
	t.Helper()
	value, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return pathShardingHash(value)
}

func pathShardingHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func pathShardingWriteJSON(t testing.TB, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func pathShardingCheckDeadline(t *testing.T, duration time.Duration) {
	t.Helper()
	if deadline, ok := t.Deadline(); ok && time.Until(deadline) < duration {
		t.Fatalf("fixture needs at least %s remaining test timeout", duration)
	}
}
