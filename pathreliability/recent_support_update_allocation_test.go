package pathreliability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

const (
	recentUpdateAllocationChild    = "TestRecentSupportUpdateAllocationChild"
	recentUpdateAllocationPrefix   = "S7_UPDATE_ALLOCATION_CONTROL "
	recentUpdateAllocationSchema   = "gocluster.s7.update-allocation.v6"
	recentUpdateAllocationCaseEnv  = "GOCLUSTER_S7_UPDATE_ALLOCATION_CASE"
	recentUpdateAllocationFaultEnv = "GOCLUSTER_S7_UPDATE_ALLOCATION_FAULT"
	recentUpdateAllocationWork     = 100000
)

var recentUpdateAllocationRetained []byte

var recentUpdateAllocationSHA = regexp.MustCompile(`^[0-9a-f]{64}$`)

type recentUpdateAllocationProof struct {
	Schema               string
	Shape                string
	Mode                 string
	Fault                bool
	InputSHA256          string
	Runs                 int
	Callbacks            int
	Completed            [2]int
	LastAccepted         [2]bool
	LastObservationUnix  [2]int64
	InitialStateVerified [2]bool
	FinalStateVerified   [2]bool
	DistinctOwners       bool
	MatchingInputs       bool
	Recent               [2][2]ReportSupport
	OuterProcs           int
	CallbackProcs        [2]int
	RestoredProcs        int
	GCPercent            int
	InjectedFaults       [2]int
	RetainedFaultBytes   int
	RetainedFaultValue   byte
	Allocations          float64
}

func TestRecentSupportUpdateWarmedAllocations(t *testing.T) {
	runRecentUpdateAllocationParents(t, false)
}

func TestRecentSupportUpdateAllocationWitness(t *testing.T) {
	runRecentUpdateAllocationParents(t, true)
}

func runRecentUpdateAllocationParents(t *testing.T, fault bool) {
	t.Helper()
	for _, shape := range []string{"same_reference_rejected", "half_life_admitted"} {
		for _, mode := range []string{"off", "shadow", "enforce"} {
			t.Run(shape+"/"+mode, func(t *testing.T) {
				output, exit := runRecentUpdateAllocationChild(t, shape, mode, fault)
				proof, err := validateRecentUpdateAllocationOutput(output, exit, shape, mode, fault)
				if err != nil {
					t.Fatalf("incomplete update-allocation child: %v\n%s", err, output)
				}
				outcome := "PASS"
				if fault {
					outcome = "FAIL"
				}
				// Preserve child evidence without printing an intentional child FAIL
				// as an unindented apparent failure of the outer package suite.
				receipt := struct {
					Schema       string
					ChildExit    int
					ChildOutcome string
					Proof        recentUpdateAllocationProof
				}{"gocluster.s7.update-allocation-parent.v6", exit, outcome, proof}
				encoded, err := json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("S7_UPDATE_ALLOCATION_PARENT_CONTROL %s", encoded)
			})
		}
	}
}

func runRecentUpdateAllocationChild(t *testing.T, shape, mode string, fault bool) (string, int) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 105*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.paniconexit0", "-test.run=^"+recentUpdateAllocationChild+"$", "-test.v", "-test.timeout=90s")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, recentUpdateAllocationCaseEnv+"=") && !strings.HasPrefix(entry, recentUpdateAllocationFaultEnv+"=") && !strings.HasPrefix(entry, "GOCLUSTER_S6_POPULATION_PROFILE_DIR=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, recentUpdateAllocationCaseEnv+"="+shape+"/"+mode,
		fmt.Sprintf("%s=%t", recentUpdateAllocationFaultEnv, fault), "GOCLUSTER_S6_POPULATION_PROFILE_DIR=")
	command.WaitDelay = 2 * time.Second
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("update-allocation child exceeded lifetime: %v\n%s", ctx.Err(), output)
	}
	if command.ProcessState == nil || !command.ProcessState.Exited() {
		t.Fatalf("update-allocation child was not normally retired: %v\n%s", err, output)
	}
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatalf("update-allocation child transport failed: %v\n%s", err, output)
		}
	}
	return string(output), command.ProcessState.ExitCode()
}

func recentUpdateAllocationSupport(shape, mode string, seed bool) ReportSupport {
	raw, admitted := 100096.0, 96.0
	if shape == "half_life_admitted" {
		raw, admitted = 2, 2
	}
	if seed {
		raw, admitted = 96, 96
	}
	if mode == "off" {
		admitted = 0
	}
	return ReportSupport{RawUsable: raw, Admitted: admitted, AdmittedAvailable: mode != "off", Enforced: mode == "enforce"}
}

func assertRecentUpdateAllocationOwners(t *testing.T, a, b *recentSupportUpdateFixture) {
	t.Helper()
	if a == b || a.predictor == b.predictor || a.predictor.combined == b.predictor.combined {
		t.Fatal("warm and measured update fixtures share a store")
	}
	for _, first := range a.owners {
		for _, second := range b.owners {
			if first == nil || second == nil || first == second {
				t.Fatal("warm and measured update fixtures share or lack a bucket owner")
			}
		}
	}
	if a.mode != "off" && (a.extraOwner == nil || b.extraOwner == nil || a.extraOwner == b.extraOwner || a.slotsOwner == nil || b.slotsOwner == nil || a.slotsOwner == b.slotsOwner) {
		t.Fatal("warm and measured fixtures share or lack coarse sidecar owners")
	}
}

func recentUpdateAllocationBatch(f *recentSupportUpdateFixture, phase int, fault bool, completed *int, last *ReportAdmission, injected *int) {
	at := time.Unix(recentSupportUpdateBase, 0).UTC()
	cycle := [6]uint64{12, 2, 3, 4, 5, 6}
	for i := range recentUpdateAllocationWork {
		if fault && phase == 1 && i == 0 {
			recentUpdateAllocationRetained = make([]byte, 64)
			recentUpdateAllocationRetained[0] = 2
			(*injected)++
		}
		hash := cycle[i%6]
		if f.shape == "half_life_admitted" {
			at = time.Unix(recentSupportUpdateBase+360*int64(i+1), 0).UTC()
			hash = uint64(i%12 + 1)
		}
		*last = f.predictor.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -10.5, 1,
			ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, hash)
		(*completed)++
	}
}

func recentUpdateAllocationSamples(t *testing.T, f *recentSupportUpdateFixture, at int64, seed bool) [2]ReportSupport {
	t.Helper()
	fine, coarse := f.predictor.combined.Lookup(1, 2, 3, 4, "20m", time.Unix(at, 0).UTC())
	actual := [2]ReportSupport{fine.RecentSupport, coarse.RecentSupport}
	want := recentUpdateAllocationSupport(f.shape, f.mode, seed)
	for _, support := range actual {
		if support != want {
			t.Fatalf("update-allocation recent support=%+v want %+v", support, want)
		}
	}
	return actual
}

func TestRecentSupportUpdateAllocationChild(t *testing.T) {
	selector, faultValue := os.Getenv(recentUpdateAllocationCaseEnv), os.Getenv(recentUpdateAllocationFaultEnv)
	if selector == "" && faultValue == "" {
		return // The parent, never this unselected child alone, owns required proof.
	}
	parts := strings.Split(selector, "/")
	if len(parts) != 2 || (parts[0] != "same_reference_rejected" && parts[0] != "half_life_admitted") || (parts[1] != "off" && parts[1] != "shadow" && parts[1] != "enforce") || (faultValue != "false" && faultValue != "true") {
		t.Fatal("invalid update-allocation child selectors")
	}
	shape, mode, fault := parts[0], parts[1], faultValue == "true"
	previousProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(previousProcs)
	previousGC := debug.SetGCPercent(50)
	defer debug.SetGCPercent(previousGC)
	fixtures := [2]*recentSupportUpdateFixture{
		newRecentSupportUpdateFixture(t, shape, mode, "candidate", recentUpdateAllocationWork),
		newRecentSupportUpdateFixture(t, shape, mode, "candidate", recentUpdateAllocationWork),
	}
	assertRecentUpdateAllocationOwners(t, fixtures[0], fixtures[1])
	if fixtures[0].inputHash != fixtures[1].inputHash || !recentUpdateAllocationSHA.MatchString(fixtures[0].inputHash) {
		t.Fatal("warm and measured update inputs differ")
	}
	proof := recentUpdateAllocationProof{Schema: recentUpdateAllocationSchema, Shape: shape, Mode: mode, Fault: fault,
		InputSHA256: fixtures[0].inputHash, Runs: 1, DistinctOwners: true, MatchingInputs: true,
		OuterProcs: runtime.GOMAXPROCS(0), GCPercent: 50}
	for i, fixture := range fixtures {
		fixture.assertState(t, 0)
		recentUpdateAllocationSamples(t, fixture, 1700000000, true)
		proof.InitialStateVerified[i] = true
	}
	recentUpdateAllocationRetained = nil
	var last [2]ReportAdmission
	proof.Allocations = testing.AllocsPerRun(1, func() {
		phase := proof.Callbacks
		proof.Callbacks++
		if phase >= len(fixtures) {
			return
		}
		proof.CallbackProcs[phase] = runtime.GOMAXPROCS(0)
		recentUpdateAllocationBatch(fixtures[phase], phase, fault, &proof.Completed[phase], &last[phase], &proof.InjectedFaults[phase])
	})
	proof.RestoredProcs = runtime.GOMAXPROCS(0)
	finalUnix := int64(1700000000)
	if shape == "half_life_admitted" {
		finalUnix = 1736000000
	}
	for i, fixture := range fixtures {
		if proof.Completed[i] != 100000 || last[i].Status != AdmissionAccepted || !last[i].EffectiveObservation.Equal(time.Unix(finalUnix, 0).UTC()) {
			t.Fatalf("incomplete update-allocation phase%d: completed%d last%+v", i, proof.Completed[i], last[i])
		}
		fixture.assertState(t, 100000)
		proof.Recent[i] = recentUpdateAllocationSamples(t, fixture, finalUnix, false)
		proof.LastAccepted[i], proof.LastObservationUnix[i], proof.FinalStateVerified[i] = true, finalUnix, true
	}
	proof.RetainedFaultBytes = len(recentUpdateAllocationRetained)
	if proof.RetainedFaultBytes > 0 {
		proof.RetainedFaultValue = recentUpdateAllocationRetained[0]
	}
	// All work/state checks finish before this record and the allocation verdict.
	// A nonzero result is never attributed or tolerated as runtime activity.
	if err := validateRecentUpdateAllocationProof(proof, shape, mode, fault, false); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%s%s\n", recentUpdateAllocationPrefix, encoded)
	if proof.Allocations != 0 {
		t.Fatalf("recent-update-allocation-verdict allocations=%g (want zero)", proof.Allocations)
	}
}

func validateRecentUpdateAllocationProof(proof recentUpdateAllocationProof, shape, mode string, fault, allocationVerdict bool) error {
	if proof.Schema != recentUpdateAllocationSchema || proof.Shape != shape || proof.Mode != mode || proof.Fault != fault || !recentUpdateAllocationSHA.MatchString(proof.InputSHA256) {
		return fmt.Errorf("wrong update-allocation proof identity")
	}
	if proof.Runs != 1 || proof.Callbacks != 2 || proof.Completed != [2]int{100000, 100000} || proof.LastAccepted != [2]bool{true, true} || proof.InitialStateVerified != [2]bool{true, true} || proof.FinalStateVerified != [2]bool{true, true} || !proof.DistinctOwners || !proof.MatchingInputs {
		return fmt.Errorf("incomplete update-allocation work/state")
	}
	if proof.OuterProcs != 4 || proof.CallbackProcs != [2]int{1, 1} || proof.RestoredProcs != 4 || proof.GCPercent != 50 {
		return fmt.Errorf("wrong update-allocation processor/GC settings")
	}
	wantUnix := int64(1700000000)
	if shape == "half_life_admitted" {
		wantUnix = 1736000000
	}
	if proof.LastObservationUnix != [2]int64{wantUnix, wantUnix} {
		return fmt.Errorf("wrong update-allocation final clock")
	}
	wantSupport := recentUpdateAllocationSupport(shape, mode, false)
	for _, phase := range proof.Recent {
		if phase != [2]ReportSupport{wantSupport, wantSupport} {
			return fmt.Errorf("wrong update-allocation recent report units")
		}
	}
	wantFaults, wantBytes, wantValue := [2]int{}, 0, byte(0)
	if fault {
		wantFaults, wantBytes, wantValue = [2]int{0, 1}, 64, 2
	}
	if proof.InjectedFaults != wantFaults || proof.RetainedFaultBytes != wantBytes || proof.RetainedFaultValue != wantValue {
		return fmt.Errorf("missing or misplaced update-allocation fault")
	}
	wantAllocations := 0.0
	if fault {
		wantAllocations = 1
	}
	if allocationVerdict && proof.Allocations != wantAllocations {
		return fmt.Errorf("wrong exact update allocation total: %g want %g", proof.Allocations, wantAllocations)
	}
	return nil
}

func validateRecentUpdateAllocationOutput(output string, exit int, shape, mode string, fault bool) (recentUpdateAllocationProof, error) {
	var proof recentUpdateAllocationProof
	output = strings.ReplaceAll(output, "\r\n", "\n")
	for _, token := range []string{"SKIP", "panic:", "fatal error:", "timed out", "WARNING: DATA RACE"} {
		if strings.Contains(output, token) {
			return proof, fmt.Errorf("invalid child termination: %s", token)
		}
	}
	wantExit, wantOutcome := 0, "PASS"
	if fault {
		wantExit, wantOutcome = 1, "FAIL"
	}
	if exit != wantExit {
		return proof, fmt.Errorf("child exit%d want%d", exit, wantExit)
	}
	runs, outcomes, completions, records := 0, 0, 0, 0
	runAt, proofAt, outcomeAt, completionAt := -1, -1, -1, -1
	for index, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "=== RUN") {
			if line != "=== RUN   "+recentUpdateAllocationChild {
				return proof, fmt.Errorf("foreign child RUN")
			}
			runs++
			runAt = index
		}
		if strings.HasPrefix(line, "--- ") {
			fields := strings.Fields(line)
			if len(fields) != 4 || fields[1] != wantOutcome+":" || fields[2] != recentUpdateAllocationChild {
				return proof, fmt.Errorf("wrong child outcome")
			}
			outcomes++
			outcomeAt = index
		}
		if line == "PASS" || line == "FAIL" {
			if line != wantOutcome {
				return proof, fmt.Errorf("wrong package outcome")
			}
			completions++
			completionAt = index
		}
		if !strings.HasPrefix(line, recentUpdateAllocationPrefix) {
			continue
		}
		records++
		proofAt = index
		payload := strings.TrimPrefix(line, recentUpdateAllocationPrefix)
		decoder := json.NewDecoder(strings.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&proof); err != nil {
			return proof, err
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return proof, fmt.Errorf("extra child proof JSON")
		}
		canonical, err := json.Marshal(proof)
		if err != nil || string(canonical) != payload {
			return proof, fmt.Errorf("noncanonical or duplicate child proof fields")
		}
	}
	if runs != 1 || outcomes != 1 || completions != 1 || records != 1 || runAt >= proofAt || proofAt >= outcomeAt || outcomeAt >= completionAt || !strings.HasSuffix(strings.TrimSpace(output), "\n"+wantOutcome) {
		return proof, fmt.Errorf("missing/duplicate child lifecycle or proof")
	}
	if fault && strings.Count(output, "recent-update-allocation-verdict allocations=1 (want zero)") != 1 {
		return proof, fmt.Errorf("missing specific deliberate allocation failure")
	}
	return proof, validateRecentUpdateAllocationProof(proof, shape, mode, fault, true)
}

func literalRecentUpdateAllocationProof(shape, mode string, fault bool) recentUpdateAllocationProof {
	wantUnix := int64(1700000000)
	if shape == "half_life_admitted" {
		wantUnix = 1736000000
	}
	support := recentUpdateAllocationSupport(shape, mode, false)
	proof := recentUpdateAllocationProof{Schema: recentUpdateAllocationSchema, Shape: shape, Mode: mode, Fault: fault,
		InputSHA256: strings.Repeat("a", 64), Runs: 1, Callbacks: 2, Completed: [2]int{100000, 100000},
		LastAccepted: [2]bool{true, true}, LastObservationUnix: [2]int64{wantUnix, wantUnix},
		InitialStateVerified: [2]bool{true, true}, FinalStateVerified: [2]bool{true, true}, DistinctOwners: true, MatchingInputs: true,
		Recent: [2][2]ReportSupport{{support, support}, {support, support}}, OuterProcs: 4,
		CallbackProcs: [2]int{1, 1}, RestoredProcs: 4, GCPercent: 50}
	if fault {
		proof.InjectedFaults, proof.RetainedFaultBytes, proof.RetainedFaultValue, proof.Allocations = [2]int{0, 1}, 64, 2, 1
	}
	return proof
}

func literalRecentUpdateAllocationOutput(t *testing.T, proof recentUpdateAllocationProof) string {
	t.Helper()
	encoded, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	outcome, verdict := "PASS", ""
	if proof.Fault {
		outcome, verdict = "FAIL", "    fixture.go:1: recent-update-allocation-verdict allocations=1 (want zero)\n"
	}
	return fmt.Sprintf("=== RUN   %s\n%s%s\n%s--- %s: %s (0.01s)\n%s\n",
		recentUpdateAllocationChild, recentUpdateAllocationPrefix, encoded, verdict, outcome, recentUpdateAllocationChild, outcome)
}

func TestRecentSupportUpdateAllocationProof(t *testing.T) {
	for _, shape := range []string{"same_reference_rejected", "half_life_admitted"} {
		for _, mode := range []string{"off", "shadow", "enforce"} {
			for _, fault := range []bool{false, true} {
				t.Run(fmt.Sprintf("valid/%s/%s/%t", shape, mode, fault), func(t *testing.T) {
					proof := literalRecentUpdateAllocationProof(shape, mode, fault)
					exit := 0
					if fault {
						exit = 1
					}
					if got, err := validateRecentUpdateAllocationOutput(literalRecentUpdateAllocationOutput(t, proof), exit, shape, mode, fault); err != nil || got != proof {
						t.Fatalf("valid literal proof rejected: %v", err)
					}
				})
			}
		}
	}
	base := literalRecentUpdateAllocationProof("same_reference_rejected", "enforce", true)
	for _, tc := range []struct {
		name   string
		change func(*recentUpdateAllocationProof)
	}{
		{"wrong_shape", func(p *recentUpdateAllocationProof) { p.Shape = "half_life_admitted" }},
		{"wrong_mode", func(p *recentUpdateAllocationProof) { p.Mode = "off" }},
		{"bad_input_hash", func(p *recentUpdateAllocationProof) { p.InputSHA256 = "bad" }},
		{"rounded_runs", func(p *recentUpdateAllocationProof) { p.Runs = 100000 }},
		{"missing_callback", func(p *recentUpdateAllocationProof) { p.Callbacks = 1 }},
		{"missing_work", func(p *recentUpdateAllocationProof) { p.Completed[1]-- }},
		{"missing_state", func(p *recentUpdateAllocationProof) { p.FinalStateVerified[1] = false }},
		{"shared_owners", func(p *recentUpdateAllocationProof) { p.DistinctOwners = false }},
		{"different_inputs", func(p *recentUpdateAllocationProof) { p.MatchingInputs = false }},
		{"wrong_support", func(p *recentUpdateAllocationProof) { p.Recent[1][1].RawUsable-- }},
		{"wrong_clock", func(p *recentUpdateAllocationProof) { p.LastObservationUnix[1]++ }},
		{"wrong_processor", func(p *recentUpdateAllocationProof) { p.CallbackProcs[1] = 4 }},
		{"missing_restoration", func(p *recentUpdateAllocationProof) { p.RestoredProcs = 1 }},
		{"wrong_gc", func(p *recentUpdateAllocationProof) { p.GCPercent = 100 }},
		{"warm_fault", func(p *recentUpdateAllocationProof) { p.InjectedFaults = [2]int{1, 0} }},
		{"missing_fault", func(p *recentUpdateAllocationProof) { p.InjectedFaults = [2]int{} }},
		{"unretained_fault", func(p *recentUpdateAllocationProof) { p.RetainedFaultBytes = 0 }},
		{"fault_not_observed", func(p *recentUpdateAllocationProof) { p.Allocations = 0 }},
		{"unattributed_extra_malloc", func(p *recentUpdateAllocationProof) { p.Allocations = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			tc.change(&changed)
			if _, err := validateRecentUpdateAllocationOutput(literalRecentUpdateAllocationOutput(t, changed), 1, base.Shape, base.Mode, true); err == nil {
				t.Fatal("invalid semantic proof accepted")
			}
		})
	}
	output := literalRecentUpdateAllocationOutput(t, base)
	lines := strings.Split(output, "\n")
	for _, tc := range []struct {
		name, output string
		exit         int
	}{
		{"missing_record", strings.Replace(output, lines[1]+"\n", "", 1), 1},
		{"duplicate_record", strings.Replace(output, lines[1]+"\n", lines[1]+"\n"+lines[1]+"\n", 1), 1},
		{"truncated", strings.TrimSuffix(output, "FAIL\n"), 1},
		{"wrong_exit", output, 0},
		{"foreign_run", strings.Replace(output, "=== RUN   "+recentUpdateAllocationChild, "=== RUN   TestOther", 1), 1},
		{"missing_run", strings.Replace(output, lines[0]+"\n", "", 1), 1},
		{"out_of_order", strings.Replace(output, lines[0]+"\n"+lines[1], lines[1]+"\n"+lines[0], 1), 1},
		{"skip", strings.Replace(output, "--- FAIL:", "--- SKIP:", 1), 1},
		{"panic", "panic: injected\n" + output, 1},
		{"timeout", "test timed out\n" + output, 1},
		{"generic_fail", strings.Replace(output, "recent-update-allocation-verdict allocations=1 (want zero)", "other failure", 1), 1},
		{"duplicate_json_field", strings.Replace(output, `"Runs":1`, `"Runs":1,"Runs":1`, 1), 1},
		{"unknown_json_field", strings.Replace(output, `"Runs":1`, `"Runs":1,"Unknown":0`, 1), 1},
		{"no_work_child_pass", "=== RUN   " + recentUpdateAllocationChild + "\n--- PASS: " + recentUpdateAllocationChild + " (0.01s)\nPASS\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantFault := tc.name != "no_work_child_pass"
			if _, err := validateRecentUpdateAllocationOutput(tc.output, tc.exit, base.Shape, base.Mode, wantFault); err == nil {
				t.Fatal("invalid child output accepted")
			}
		})
	}
}
