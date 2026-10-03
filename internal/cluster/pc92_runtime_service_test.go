//go:build qualification

package cluster

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/filter"
	"dxcluster/telnet"
)

// The service process contains the real cluster plus bounded enqueue counters.
// All load generation, recipient sockets, PC92 oracle and reporting run in the
// parent. No service queue/admission/correction behavior is changed.
func TestPC92RuntimeService(t *testing.T) {
	if os.Getenv("GOCLUSTER_PC92_RUNTIME_CHILD") != "1" {
		t.Skip("private runtime qualification service")
	}
	profile, err := runtimeQualificationProfile(os.Getenv("GOCLUSTER_PC92_RUNTIME_PROFILE"))
	if err != nil {
		t.Fatal(err)
	}
	repo, output := os.Getenv("GOCLUSTER_PC92_RUNTIME_REPO"), os.Getenv("GOCLUSTER_PC92_RUNTIME_OUTPUT")
	if !filepath.IsAbs(repo) || !filepath.IsAbs(output) {
		t.Fatal("child requires absolute repository and evidence paths")
	}
	count, err := strconv.Atoi(os.Getenv("GOCLUSTER_PC92_RUNTIME_INPUT_COUNT"))
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := openQualificationSharedInputs(os.Getenv("GOCLUSTER_PC92_RUNTIME_INPUTS"), count, false)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.close()
	now, frequency, err := qualificationCounterClock()
	if err != nil {
		t.Fatal(err)
	}
	minutes := int((profile.load + time.Minute - 1) / time.Minute)
	oracle := newQualificationOracleInputs(mapping.inputs, 100, 0, minutes)
	oracle.clockNow, oracle.clockFrequency = now, frequency
	cfg, err := config.Load(filepath.Join(repo, "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	filter.UserDataDir = filepath.Join(filepath.Dir(output), "isolated-users")
	configureRuntimeQualification(t, cfg, repo, profile)
	r := newClusterRuntime(BuildInfo{Version: "runtime-qualification", Commit: "local", BuildTime: time.Now().UTC().Format(time.RFC3339), GoVersion: runtime.Version()}, cfg, filepath.Join(repo, "data", "config"), config.LoadDiagnostics{})
	applyGoRuntimeTuning(cfg.GoRuntime)
	defer r.close()
	if !r.initialize() {
		t.Fatalf("runtime startup: %v", r.startupErr)
	}
	defer r.shutdown()
	guard := &qualificationObservationGuard{oracle: oracle}
	telnet.SetQualificationEnqueueObserver(guard.observe)
	defer guard.stop()
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", os.Getenv("GOCLUSTER_PC92_RUNTIME_CONTROL"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := qualificationWritePacket(conn, qualificationReply{Config: cfg, Clock: now().UnixNano(), Frequency: frequency, GOMAXPROCS: runtime.GOMAXPROCS(0), OracleBytes: oracle.allocatedBytes}); err != nil {
		t.Fatal(err)
	}
	service := &qualificationChild{t: t, runtime: r, oracle: oracle, profile: profile}
	defer service.retireStages()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(70 * time.Minute))
		var request qualificationRequest
		if err := qualificationReadPacket(conn, &request); err != nil {
			return // parent EOF/cancellation still runs bounded shutdown
		}
		reply, err := service.handle(t.Context(), request)
		if err != nil {
			reply.Error = err.Error()
		}
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := qualificationWritePacket(conn, reply); err != nil {
			return
		}
		if request.Kind == "stop" {
			return
		}
	}
}

type qualificationObservationGuard struct {
	oracle *qualificationOracle
	closed atomic.Bool
	active atomic.Int64
}

func (g *qualificationObservationGuard) observe(event telnet.QualificationEnqueue) {
	if g.closed.Load() {
		return
	}
	g.active.Add(1)
	defer g.active.Add(-1)
	// A callback loaded immediately before removal may arrive after stop sees
	// zero active calls. Its second check prevents any access to unmapped data.
	if !g.closed.Load() {
		g.oracle.enqueued(event)
	}
}

func (g *qualificationObservationGuard) stop() {
	g.closed.Store(true)
	telnet.SetQualificationEnqueueObserver(nil)
	for g.active.Load() != 0 {
		runtime.Gosched()
	}
}

type qualificationChild struct {
	t           *testing.T
	runtime     *clusterRuntime
	oracle      *qualificationOracle
	profile     qualificationProfile
	stopProfile func() qualificationLoadProfile
}

func (s *qualificationChild) handle(ctx context.Context, request qualificationRequest) (qualificationReply, error) {
	var reply qualificationReply
	switch request.Kind {
	case "snapshot":
		state, err := s.runtime.peerManager.QualificationSnapshot(ctx)
		reply.State = state
		return reply, err
	case "clock":
		return reply, s.runtime.peerManager.QualificationSetClockOffset(ctx, time.Duration(request.Offset))
	case "membership":
		reply.Membership = s.runtime.telnetServer.CurrentPeerMembership()
	case "profile-start":
		if s.stopProfile != nil {
			return reply, fmt.Errorf("profile already started")
		}
		s.stopProfile = startQualificationLoadProfile(ctx, s.t, s.profile, s.runtime.peerManager, s.oracle.clockNow)
	case "arm":
		if request.Frequency != s.oracle.clockFrequency || request.Epoch <= 0 || !s.oracle.measurementEpoch.IsZero() {
			return reply, fmt.Errorf("invalid measurement clock epoch/frequency")
		}
		s.oracle.measurementEpoch = time.Unix(0, request.Epoch)
		membership := s.runtime.telnetServer.CurrentPeerMembership()
		if !membership.Complete || membership.RawCount != len(s.oracle.clients) {
			return reply, fmt.Errorf("child current membership incomplete")
		}
		for _, user := range membership.Users {
			var index int
			if _, err := fmt.Sscanf(user.Login, "DL%dCAA", &index); err != nil || index < 1 || index > len(s.oracle.clients) {
				return reply, fmt.Errorf("unexpected child current login %s", user.Login)
			}
			s.oracle.sessionIDs[index-1] = user.SessionID
		}
		if err := s.armStages(); err != nil {
			return reply, err
		}
		reply.Clock = s.oracle.clockNow().UnixNano()
	case "profile-stop":
		if s.stopProfile != nil {
			reply.Profile = s.stopProfile()
		}
	case "results":
		reply.Stages = s.finishStages(request.Used)
		rows, err := s.oracle.enqueueResults(request.Used)
		if err != nil {
			return reply, err
		}
		reply.Enqueue, reply.Failures = rows, s.oracle.failures.Load()
		s.oracle.failureMu.Lock()
		reply.Examples = append([]string(nil), s.oracle.examples...)
		s.oracle.failureMu.Unlock()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		reply.HeapAlloc, reply.HeapInuse, reply.ProcessSys = memory.HeapAlloc, memory.HeapInuse, memory.Sys
		reply.GOMAXPROCS, reply.OracleBytes = runtime.GOMAXPROCS(0), s.oracle.allocatedBytes
	case "stop":
	default:
		return reply, fmt.Errorf("unknown qualification request %q", request.Kind)
	}
	return reply, nil
}
