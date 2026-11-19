//go:build windows && !qualification

// File role: Opt-in paced qualification of the actual output and resolver owners.
// This is a baseline-compatible measurement fixture, not a baseline/candidate
// comparator or an accuracy oracle. Run through scripts/test-windows.ps1.
package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"

	"golang.org/x/sys/windows"
)

const (
	ccQualSeed       = uint64(0x43435632)
	ccQualMaxOffered = uint64(100000)
	ccQualMainCall   = "OH0ZZA"
	ccQualWireCall   = "OH0ZZZ"
)

type ccQualOptions struct {
	Root         string            `json:"root"`
	Out          string            `json:"out"`
	Duration     time.Duration     `json:"duration_ns"`
	Warmup       time.Duration     `json:"warmup_ns"`
	DrainTimeout time.Duration     `json:"drain_timeout_ns"`
	SampleEvery  time.Duration     `json:"sample_every_ns"`
	TypicalRate  int               `json:"typical_rate_per_second"`
	DenseRate    int               `json:"dense_rate_per_second"`
	DrainRate    int               `json:"drain_rate_per_second"`
	Environment  map[string]string `json:"environment"`
}

type ccQualSnapshot struct {
	OutputDepth       int                        `json:"output_queue_depth"`
	Resolver          spot.SignalResolverMetrics `json:"resolver"`
	Finalized         uint64                     `json:"finalized_before_fanout"`
	FanoutEntries     uint64                     `json:"fanout_entries_before_stabilizer"`
	RingOutputs       int                        `json:"ring_outputs_including_main_barrier"`
	TemporalPending   int64                      `json:"temporal_pending"`
	StabilizerPending int64                      `json:"stabilizer_pending"`
	StabilizerHeld    uint64                     `json:"stabilizer_admissions_including_reholds"`
	StabilizerDelayed uint64                     `json:"stabilizer_delayed_outputs"`
	Suppressed        uint64                     `json:"stabilizer_timeout_suppressed"`
	AgeDropped        uint64                     `json:"stabilizer_age_dropped"`
	QueueDrops        uint64                     `json:"telnet_queue_drops"`
	ClientDrops       uint64                     `json:"telnet_client_drops"`
	SenderFailures    uint64                     `json:"telnet_sender_failures"`
}

type ccQualPhase struct {
	Offered        uint64         `json:"offered_raw_inputs"`
	Accepted       uint64         `json:"accepted_raw_inputs"`
	Refused        uint64         `json:"output_queue_refused"`
	ElapsedNS      int64          `json:"elapsed_ns"`
	CPUNS          int64          `json:"process_cpu_ns"`
	CPUPerAccepted float64        `json:"process_cpu_ns_per_accepted_raw_input"`
	LateSlots      uint64         `json:"late_pacing_slots"`
	MaxLatenessNS  int64          `json:"max_pacing_lateness_ns"`
	Start          ccQualSnapshot `json:"start"`
	End            ccQualSnapshot `json:"end"`
}

type ccQualSampled struct {
	Kind              string `json:"kind"`
	EveryNS           int64  `json:"interval_ns"`
	Samples           uint64 `json:"samples"`
	OutputQueue       int    `json:"output_queue_high_water"`
	ResolverQueue     int    `json:"resolver_queue_high_water"`
	TemporalPending   int64  `json:"temporal_pending_high_water"`
	StabilizerPending int64  `json:"stabilizer_pending_high_water"`
	ActiveKeys        int    `json:"active_keys_high_water"`
}

type ccQualResult struct {
	Name                 string         `json:"name"`
	Status               string         `json:"status"`
	WorkloadSHA256       string         `json:"workload_sha256"`
	WorkloadTemplates    int            `json:"workload_templates"`
	WorkloadDescription  string         `json:"workload_description"`
	WarmupCoversWorkload bool           `json:"warmup_covers_one_workload_cycle"`
	Warmup               ccQualPhase    `json:"warmup"`
	Steady               ccQualPhase    `json:"steady"`
	Sampled              ccQualSampled  `json:"queue_observations"`
	BeforeRecovery       ccQualSnapshot `json:"input_stopped"`
	AfterRecovery        ccQualSnapshot `json:"owners_joined"`
	RecoveryNS           int64          `json:"recovery_elapsed_ns"`
	RecoveryCPUNS        int64          `json:"recovery_process_cpu_ns"`
	NaturalRecovery      bool           `json:"natural_recovery_before_retirement"`
	OutputJoined         bool           `json:"output_joined"`
	ResolverJoined       bool           `json:"resolver_joined"`
	ShutdownJoined       bool           `json:"all_fixture_owners_joined"`
	NormalTCP            uint64         `json:"normal_tcp_dx_receipts_excluding_barriers"`
	DiagTCP              uint64         `json:"diag_tcp_dx_receipts_excluding_barriers"`
	TCPBarriersJoined    bool           `json:"tcp_fifo_barriers_joined"`
	ArchiveRows          uint64         `json:"archive_rows_including_main_barrier"`
	ArchiveExpected      uint64         `json:"archive_handoffs_from_unwrapped_ring"`
	ArchiveMissing       uint64         `json:"archive_missing_after_stop_and_reopen"`
	Errors               []string       `json:"errors,omitempty"`
}

type ccQualArtifact struct {
	Schema        string            `json:"schema"`
	Status        string            `json:"invocation_status"`
	StartedUTC    string            `json:"started_utc"`
	Options       ccQualOptions     `json:"inputs"`
	Seed          uint64            `json:"seed"`
	GoVersion     string            `json:"go_version"`
	GOOS          string            `json:"goos"`
	GOARCH        string            `json:"goarch"`
	GOMAXPROCS    int               `json:"gomaxprocs"`
	CPUCount      int               `json:"cpu_count"`
	Fingerprints  map[string]string `json:"sha256_fingerprints"`
	Caps          map[string]int    `json:"configured_caps"`
	Limits        []string          `json:"limits"`
	Results       []*ccQualResult   `json:"results"`
	RecoveryCases []string          `json:"observed_backlog_recovery_cases"`
}

func TestCallCorrectionPacedQualification(t *testing.T) {
	gate := os.Getenv("CCV2_PACED_QUAL")
	if gate == "" {
		t.Skip("not qualification: set CCV2_PACED_QUAL=1 and an explicit CCV2_QUAL_OUT")
	}
	if gate != "1" {
		t.Fatal("environment_validation: invalid CCV2_PACED_QUAL; only the exact value 1 enables qualification")
	}
	opts := ccQualReadOptions(t)
	assets := ccQualLoadAssets(t, opts)
	artifact := ccQualArtifact{
		Schema: "gocluster.call_correction.paced.v1", StartedUTC: time.Now().UTC().Format(time.RFC3339Nano),
		Options: opts, Seed: ccQualSeed, GoVersion: runtime.Version(), GOOS: runtime.GOOS,
		GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), CPUCount: runtime.NumCPU(),
		Fingerprints: assets.fingerprints, Caps: assets.caps,
		Limits: []string{
			"Comparison required: this artifact alone does not pass the <=10 percent typical CPU/input regression gate.",
			"Post-primary-dedupe correction pipeline only; no peer fanout, upstream ingest, harmonic/path/grid services, production deployment, or user-count ceiling claim.",
			"Configured correction policy/models/queue caps; isolated initially empty Custom SCP and archive stores, warmed only by the frozen workload.",
			"Process CPU includes producer, samplers, TCP readers and active storage workers; steady excludes artifact encoding and archive reopen/readback.",
			"All queue high-water observations are SAMPLED; no public archive/telnet queue-depth API is claimed.",
			"Archive missing count is end-to-end loss after joined stop/reopen, not a queue-specific attribution. Ring capacity must exceed all offered inputs plus the main barrier.",
			"Normal/DIAG counts may differ from archive due to configured family suppression; FIFO barriers exclude both fixture control spots.",
		},
	}
	t.Cleanup(func() {
		if got := ccQualProductionHash(t, opts.Root); got != artifact.Fingerprints["production_go"] {
			t.Error("qualification invalid: production source changed during the invocation")
			artifact.Limits = append(artifact.Limits, "INVALID: production source changed during measurement")
		}
		artifact.Status = "recorded_comparison_required"
		if t.Failed() {
			artifact.Status = "failed_unqualified"
		}
		ccQualWriteArtifact(t, opts.Out, artifact)
	})
	for _, name := range []string{"typical", "dense_canonical", "drain_recovery"} {
		t.Run(name, func(t *testing.T) {
			result := &ccQualResult{Name: name, Status: "failed"}
			artifact.Results = append(artifact.Results, result)
			defer func() {
				if !t.Failed() {
					result.Status = "recorded_comparison_required"
				}
			}()
			ccQualRun(t, opts, assets, result)
		})
	}
	artifact.RecoveryCases = ccQualObservedRecoveryCases(artifact.Results)
	if len(artifact.RecoveryCases) == 0 {
		t.Error("fixed workload did not demonstrate natural completion from actual input-stop backlog in any case")
	}
}

// Recovery coverage belongs to the unchanged invocation, not to one case name.
// A faster case may already be empty at input stop and supplies no coverage.
// Sampled high-water observations or forced retirement cannot establish it.
func ccQualObservedRecoveryCases(results []*ccQualResult) []string {
	var cases []string
	for _, result := range results {
		if result == nil || result.Status != "recorded_comparison_required" || len(result.Errors) != 0 ||
			!ccQualHasBacklog(result.BeforeRecovery) || !result.NaturalRecovery ||
			!result.OutputJoined || !result.ResolverJoined || !result.ShutdownJoined || !result.TCPBarriersJoined ||
			ccQualHasBacklog(result.AfterRecovery) || result.ArchiveMissing != 0 || result.ArchiveRows != result.ArchiveExpected {
			continue
		}
		cases = append(cases, result.Name)
	}
	return cases
}

func ccQualReadOptions(t *testing.T) ccQualOptions {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate qualification source root")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	if override := os.Getenv("CCV2_QUAL_ROOT"); override != "" {
		if !filepath.IsAbs(override) || !strings.EqualFold(filepath.Clean(override), root) {
			t.Fatal("invalid CCV2_QUAL_ROOT: must name the absolute checkout containing the compiled fixture")
		}
	}
	out := os.Getenv("CCV2_QUAL_OUT")
	if !filepath.IsAbs(out) {
		t.Fatal("invalid CCV2_QUAL_OUT: an explicit absolute directory under checkout/.tmp/call-correctionv2 is required")
	}
	out = filepath.Clean(out)
	rel, err := filepath.Rel(filepath.Join(root, ".tmp", "call-correctionv2"), out)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		t.Fatal("invalid CCV2_QUAL_OUT: directory must remain under checkout/.tmp/call-correctionv2")
	}
	o := ccQualOptions{Root: root, Out: out, Environment: make(map[string]string)}
	o.Duration = ccQualEnvDuration(t, "CCV2_QUAL_DURATION", time.Minute, time.Second, 10*time.Minute)
	o.Warmup = ccQualEnvDuration(t, "CCV2_QUAL_WARMUP", 15*time.Second, time.Second, 5*time.Minute)
	o.DrainTimeout = ccQualEnvDuration(t, "CCV2_QUAL_DRAIN_TIMEOUT", 90*time.Second, time.Second, 5*time.Minute)
	o.SampleEvery = ccQualEnvDuration(t, "CCV2_QUAL_SAMPLE_EVERY", 25*time.Millisecond, time.Millisecond, time.Second)
	o.TypicalRate = ccQualEnvRate(t, "CCV2_QUAL_RATE", 200)
	o.DenseRate = ccQualEnvRate(t, "CCV2_QUAL_DENSE_RATE", 800)
	o.DrainRate = ccQualEnvRate(t, "CCV2_QUAL_DRAIN_RATE", 200)
	for _, name := range []string{"CCV2_PACED_QUAL", "CCV2_QUAL_ROOT", "CCV2_QUAL_OUT", "CCV2_QUAL_DURATION", "CCV2_QUAL_WARMUP", "CCV2_QUAL_DRAIN_TIMEOUT", "CCV2_QUAL_SAMPLE_EVERY", "CCV2_QUAL_RATE", "CCV2_QUAL_DENSE_RATE", "CCV2_QUAL_DRAIN_RATE"} {
		o.Environment[name] = os.Getenv(name)
	}
	return o
}

func ccQualEnvDuration(t *testing.T, name string, fallback, minimum, maximum time.Duration) time.Duration {
	t.Helper()
	value, present := os.LookupEnv(name)
	if !present {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || strings.TrimSpace(value) != value || parsed < minimum || parsed > maximum {
		t.Fatalf("invalid %s: require a duration from %s through %s", name, minimum, maximum)
	}
	return parsed
}

func ccQualEnvRate(t *testing.T, name string, fallback int) int {
	t.Helper()
	value, present := os.LookupEnv(name)
	if !present {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || strconv.Itoa(parsed) != value || parsed < 1 || parsed > 10000 {
		t.Fatalf("invalid %s: require an integer from 1 through 10000", name)
	}
	return parsed
}

func ccQualProcessCPU(t *testing.T) int64 {
	t.Helper()
	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(windows.CurrentProcess(), &created, &exited, &kernel, &user); err != nil {
		t.Fatalf("GetProcessTimes: %v", err)
	}
	ticks := (uint64(kernel.HighDateTime)<<32 | uint64(kernel.LowDateTime)) +
		(uint64(user.HighDateTime)<<32 | uint64(user.LowDateTime))
	return int64(ticks * 100) // FILETIME counts 100 ns units, not wall-clock time.
}

func ccQualWriteArtifact(t *testing.T, directory string, artifact ccQualArtifact) {
	t.Helper()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("create CCV2_QUAL_OUT: %v", err)
	}
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 1<<20 {
		t.Fatal("qualification artifact exceeded its 1 MiB bound")
	}
	name := fmt.Sprintf("paced-%s-%d.json", time.Now().UTC().Format("20060102T150405.000000000Z"), os.Getpid())
	path := filepath.Join(directory, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write(append(data, '\n'))
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("write qualification artifact: write=%v close=%v", writeErr, closeErr)
	}
	t.Logf("paced qualification artifact: %s (comparison still required)", path)
}

func ccQualHashJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
