// File role: Shared-benchmark-domain profiles and actual connected-owner
// lifecycle witnesses. External maintenance retires before capture closes;
// role elapsed stays separate. The unchanged soak owns hourly-cadence proof.
package cluster

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
)

func TestPathShardingConnectedProfile(t *testing.T) {
	if os.Getenv("GOCLUSTER_PATH_SHARDING_PROFILE") != "1" {
		t.Skip("opt-in S4 connected consumer profiles")
	}
	pathShardingRequireExecutable(t)
	for _, mode := range pathShardingModes {
		t.Run(mode, func(t *testing.T) { pathShardingFixedProfile(t, mode, pathShardingCampaignOps) })
	}
}

func TestPathShardingRepairedProfilePreflight(t *testing.T) {
	if os.Getenv("GOCLUSTER_PATH_SHARDING_REPAIRED_PREFLIGHT") != "1" {
		t.Skip("opt-in unscored repaired controlled-profile integration")
	}
	pathShardingRequireExecutable(t)
	for _, mode := range pathShardingModes {
		t.Run(mode, func(t *testing.T) { pathShardingFixedProfile(t, mode, 262144) })
	}
}

func pathShardingFixedProfile(t *testing.T, mode string, operations int) {
	t.Helper()
	name := t.Name()
	dir := pathShardingArtifactDir(t, "connected-profile-"+mode+"-")
	var fixture *pathShardingConnectedFixture
	var result pathShardingProfileResult
	testsupport.RunFixedBenchmark(t, func(b *testing.B, ctx context.Context, cancel context.CancelFunc, clock *testsupport.Clock) bool {
		defer cancel()
		fixture = newPathShardingConnectedFixture(b, mode)
		var err error
		result, err = fixture.controlledProfile(ctx, cancel, b, clock, pathShardingProfileOptions{operations: operations, directory: dir})
		if err != nil {
			b.Fatal(err)
		}
		pathShardingCheckMaintenance(b, fixture, &result, true)
		fixture.guard(b)
		return true
	})
	// Only the successful after-return bridge authorizes a valid JSON receipt.
	pathShardingWriteProfile(t, filepath.Join(dir, "mutex.pprof"), "mutex")
	pathShardingWriteProfile(t, filepath.Join(dir, "block.pprof"), "block")
	runtime.GC()
	runtime.GC()
	pathShardingWriteProfile(t, filepath.Join(dir, "heap.pprof"), "heap")
	pathShardingWriteJSON(t, filepath.Join(dir, "manifest.json"), struct {
		MeasurementProtocol string                        `json:"measurement_protocol"`
		Manifest            pathShardingManifest          `json:"fixture"`
		Maintenance         pathShardingMaintenanceResult `json:"controlled_maintenance"`
		Scored              bool                          `json:"scored"`
		ProfileClosedNS     int64                         `json:"profile_closed_ns"`
		ProfileBoundary     string                        `json:"profile_boundary"`
	}{testsupport.FixedBenchmarkProtocol, pathShardingBuildManifest(t, name, fixture, result.Run, false), result.Maintenance, false, result.ProfileClosedNS,
		"Profiles begin after queues, complete samples, clock priming and all role owners are ready. They remain open through role retirement and the final external maintenance API, then stop before checker reconstruction/oracles. Run elapsed is role elapsed; domain elapsed includes external retirement. Allocation snapshots bracket this released-work window and include profiler bookkeeping. Retained heap is separate after completion and forced GC."})
	runtime.KeepAlive(fixture)
	t.Logf("unscored shared-domain connected profiles: %s", dir)
}

type pathShardingMaintenanceResult struct {
	StartedUTC         time.Time `json:"started_utc"`
	FinishedUTC        time.Time `json:"finished_utc"`
	CompactStartedUTC  time.Time `json:"compact_started_utc"`
	CompactFinishedUTC time.Time `json:"compact_finished_utc"`
	CompactStartedNS   int64     `json:"compact_started_ns"`
	CompactFinishedNS  int64     `json:"compact_finished_ns"`
	FinalAPINS         int64     `json:"final_api_ns"`
	RoleOverlapNS      [4]int64  `json:"actual_compact_role_overlap_ns"`
	Removed            int       `json:"removed"`
	Compacted          int       `json:"compacted"`
	Error              string    `json:"error,omitempty"`
}

type pathShardingProfileOptions struct {
	operations      int
	directory       string
	delayAfterRoles bool
	setupFailure    bool
}

type pathShardingProfileResult struct {
	Run             pathShardingRun
	Maintenance     pathShardingMaintenanceResult
	Growth          int
	ProfileOpened   bool
	ProfileClosed   bool
	ProfileClosedNS int64
	ExternalRetired bool
}

var errPathShardingProfileSetup = errors.New("injected connected profile setup failure")

// The callback owner cancels before joining parked/active queue owners and
// external maintenance. Once maintenance starts its bounded APIs, cancellation
// does not cut off the final measurement. No clock read survives this owner.
func (f *pathShardingConnectedFixture) controlledProfile(ctx context.Context, cancel context.CancelFunc, t testing.TB, clock *testsupport.Clock, options pathShardingProfileOptions) (result pathShardingProfileResult, runErr error) {
	result.Growth = f.seedCold(t, f.coldCohort(t))
	oldMutex, enabled := 0, false
	stopCPU := func() {}
	var stopOnce sync.Once
	endProfiles := func() {
		stopOnce.Do(func() {
			if enabled {
				runtime.SetMutexProfileFraction(oldMutex)
				runtime.SetBlockProfileRate(0)
			}
			stopCPU()
			if enabled {
				pathShardingWriteProfile(t, filepath.Join(options.directory, "allocs-after.pprof"), "allocs")
			}
			result.ProfileClosed, result.ProfileClosedNS = true, clock.NowNS()
		})
	}
	beginProfiles := func() error {
		runtime.GC()
		runtime.GC()
		pathShardingWriteProfile(t, filepath.Join(options.directory, "allocs-before.pprof"), "allocs")
		oldMutex = runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)
		enabled = true
		stopCPU = pathShardingStartCPU(t, filepath.Join(options.directory, "cpu.pprof"))
		result.ProfileOpened = true
		if options.setupFailure {
			return errPathShardingProfileSetup
		}
		return nil
	}
	var firstSuccess [4]chan struct{}
	for role := range firstSuccess {
		firstSuccess[role] = make(chan struct{})
	}
	rolesRetired, maintenanceDone := make(chan struct{}), make(chan struct{})
	var maintenance pathShardingMaintenanceResult
	go func() {
		defer close(maintenanceDone)
		done := ctx.Done()
		for _, started := range firstSuccess {
			select {
			case <-done:
				// A normal retirement cancels after all start signals exist.
				// Preserve that proof even if this owner was scheduled late.
				select {
				case <-started:
					continue
				default:
				}
				maintenance.Error = "maintenance canceled before all roles started"
				return
			case <-started:
			}
		}
		maintenance.StartedUTC = time.Now().UTC()
		maintenance.Removed = f.predictor.PurgeStale(maintenance.StartedUTC)
		maintenance.CompactStartedUTC, maintenance.CompactStartedNS = time.Now().UTC(), clock.NowNS()
		maintenance.Compacted = f.predictor.Compact(1000, .5)
		maintenance.CompactFinishedNS, maintenance.CompactFinishedUTC = clock.NowNS(), time.Now().UTC()
		if options.delayAfterRoles {
			<-rolesRetired
		}
		f.predictor.RefreshStatsSnapshot(time.Now().UTC())
		f.predictor.WeightHistogramByBand(time.Now().UTC())
		f.metrics.HourlyCounts(time.Now().UTC())
		maintenance.FinalAPINS, maintenance.FinishedUTC = clock.NowNS(), time.Now().UTC()
	}()
	var retireOnce sync.Once
	retireExternal := func() {
		retireOnce.Do(func() {
			cancel()
			close(rolesRetired)
			<-maintenanceDone
			result.Maintenance, result.ExternalRetired = maintenance, true
		})
	}
	// Fatal profile setup also follows cancellation, external join, profile
	// stop; runOwned owns release/join of all eight queue owners first.
	defer func() { cancel(); retireExternal(); endProfiles() }()
	result.Run, runErr = f.runOwned(ctx, cancel, t, [4]int{options.operations, options.operations, options.operations, options.operations}, clock,
		pathShardingRunControl{firstSuccess: &firstSuccess, beforeStart: beginProfiles, retireExternal: retireExternal, afterJoin: endProfiles})
	return result, runErr
}

func pathShardingCheckMaintenance(t testing.TB, f *pathShardingConnectedFixture, result *pathShardingProfileResult, requireOverlap bool) {
	t.Helper()
	m := result.Maintenance
	if !result.ExternalRetired || result.Run.OwnersRetired != 8 || !result.ProfileOpened || !result.ProfileClosed || m.Error != "" || m.Removed != result.Growth-f.hotBuckets || m.Compacted == 0 {
		t.Fatalf("controlled profile ownership/population failure: %+v", result)
	}
	if m.FinalAPINS < m.CompactFinishedNS || result.Run.DomainFinishedNS < m.FinalAPINS || result.ProfileClosedNS < m.FinalAPINS {
		t.Fatalf("maintenance outlived clock/profile domain: %+v", result)
	}
	for index, role := range result.Run.Roles {
		overlap := min(m.CompactFinishedNS, role.FinishedNS) - max(m.CompactStartedNS, role.StartedNS)
		if requireOverlap && overlap <= 0 {
			t.Fatalf("actual Compact missed %s interval: %+v", role.Name, result)
		}
		result.Maintenance.RoleOverlapNS[index] = overlap
	}
}

// Parent runs exact child leaves at the same saved executable with explicit
// 1x. A missing marker, watchdog timeout or crash cannot stand in for proof.
func TestPathShardingRepairedProfileLifecycle(t *testing.T) {
	for _, fault := range []string{"delayed_maintenance", "setup_failure"} {
		t.Run(fault, func(t *testing.T) {
			if os.Getenv("GOCLUSTER_PATH_SHARDING_LIFECYCLE_CHILD") != fault {
				pathShardingProfileLifecycleChild(t, fault)
				return
			}
			dir := pathShardingArtifactDir(t, "repaired-profile-lifecycle-"+fault+"-")
			testsupport.RunFixedBenchmark(t, func(b *testing.B, ctx context.Context, cancel context.CancelFunc, clock *testsupport.Clock) bool {
				defer cancel()
				fixture := newPathShardingConnectedFixture(b, "enforce")
				result, err := fixture.controlledProfile(ctx, cancel, b, clock, pathShardingProfileOptions{operations: 4096, directory: dir, delayAfterRoles: fault == "delayed_maintenance", setupFailure: fault == "setup_failure"})
				if result.Run.OwnersRetired != 8 || !result.ExternalRetired || !result.ProfileOpened || !result.ProfileClosed {
					b.Fatalf("actual profile owners did not retire: %+v", result)
				}
				if fault == "setup_failure" {
					if !errors.Is(err, errPathShardingProfileSetup) || ctx.Err() != context.Canceled || result.Maintenance.FinalAPINS != 0 || fixture.predictor.TotalBuckets() != result.Growth {
						b.Fatalf("wrong setup-failure retirement: err=%v result=%+v", err, result)
					}
					for _, completed := range fixture.completed {
						if completed != 0 {
							b.Fatal("parked role performed work after setup failure")
						}
					}
					if _, receiptErr := os.Stat(filepath.Join(dir, "manifest.json")); !os.IsNotExist(receiptErr) {
						b.Fatalf("setup failure emitted or cannot exclude a qualification receipt: %v", receiptErr)
					}
				} else {
					if err != nil {
						b.Fatal(err)
					}
					pathShardingCheckMaintenance(b, fixture, &result, false)
					if result.Maintenance.FinalAPINS <= result.Run.FinishedNS {
						b.Fatal("delayed final maintenance API did not follow role retirement")
					}
					fixture.guard(b)
				}
				return true
			})
			t.Logf("CONNECTED_PROFILE_LIFECYCLE_RETIRED %s", fault)
		})
	}
}

func pathShardingProfileLifecycleChild(t *testing.T, fault string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestPathShardingRepairedProfileLifecycle$/^"+fault+"$", "-test.benchtime=1x", "-test.timeout=45s", "-test.v")
	cmd.Env = append(os.Environ(), "GOCLUSTER_PATH_SHARDING_LIFECYCLE_CHILD="+fault)
	cmd.WaitDelay = 2 * time.Second
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil || err != nil || !strings.Contains(string(output), "CONNECTED_PROFILE_LIFECYCLE_RETIRED "+fault) || !strings.Contains(string(output), "--- PASS: TestPathShardingRepairedProfileLifecycle/"+fault) {
		t.Fatalf("actual connected lifecycle child did not prove retirement: context=%v err=%v output=%s", ctx.Err(), err, output)
	}
}

func pathShardingStartCPU(t testing.TB, file string) func() {
	t.Helper()
	output, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(output); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			if err := output.Close(); err != nil {
				t.Error(err)
			}
		})
	}
}

func pathShardingWriteProfile(t testing.TB, file, kind string) {
	t.Helper()
	profile := pprof.Lookup(kind)
	if profile == nil {
		t.Fatalf("missing runtime profile %s", kind)
	}
	output, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	writeErr, closeErr := profile.WriteTo(output, 0), output.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("%s profile: write=%v close=%v", kind, writeErr, closeErr)
	}
}
