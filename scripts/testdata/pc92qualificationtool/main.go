// Command pc92qualificationtool is a deterministic fake tool/process used only
// by the wrapper's behavioral fixtures. It never supplies qualification data.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	name := strings.ToLower(filepath.Base(os.Args[0]))
	if name == "git.exe" || name == "git" {
		runGit()
		return
	}
	if strings.Contains(name, ".test") {
		runTest()
		return
	}
	if os.Getenv("PC92_FIXTURE_FAMILY") == "q6" && strings.Contains(os.Getenv("PATH"), os.Getenv("PC92_FIXTURE_DLL")) {
		fmt.Fprintln(os.Stderr, "compiler selection contaminated by runtime-only DLL path")
		os.Exit(2)
	}
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version":
		fmt.Println("go version fixture")
	case "env":
		fmt.Println("windows\namd64\nv1\n0\n\nlocal")
	case "test":
		for i, arg := range os.Args {
			if arg == "-o" && i+1 < len(os.Args) {
				// Each isolated wrapper keeps a distinct executable path. Hardlinks
				// avoid copying the immutable mock image for every negative case.
				must(os.Link(os.Args[0], os.Args[i+1]))
				log("build:" + os.Args[i+1])
				if scenario() == "build_source_changed" {
					mutate("subject.go")
				}
				return
			}
		}
		os.Exit(2)
	default:
		os.Exit(2)
	}
}

func runGit() {
	if len(os.Args) > 3 && os.Args[1] == "-C" && filepath.Clean(os.Args[2]) == filepath.Clean(os.Getenv("PC92_FIXTURE_REFERENCE")) {
		if os.Args[3] == "rev-parse" {
			if scenario() == "wrong_reference" {
				fmt.Println("wrong")
				return
			}
			fmt.Println("3e9b3621d94dd45c68702e4a0f896aac33f2a91d")
			return
		}
		if os.Args[3] == "diff" {
			return
		}
	}
	cmd := exec.Command(os.Getenv("PC92_FIXTURE_REAL_GIT"), os.Args[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}

func scenario() string { return os.Getenv("PC92_FIXTURE_SCENARIO") }
func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
func log(value string) {
	f, err := os.OpenFile(os.Getenv("PC92_FIXTURE_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = fmt.Fprintln(f, value)
	must(err)
	must(f.Close())
}
func mutate(relative string) {
	must(os.WriteFile(filepath.Join(os.Getenv("PC92_FIXTURE_REPO"), relative), []byte("changed\n"), 0600))
}

func runTest() {
	log("execute:" + os.Args[0])
	family, profile := os.Getenv("PC92_FIXTURE_FAMILY"), ""
	if family == "q6" && !strings.Contains(os.Getenv("PATH"), os.Getenv("PC92_FIXTURE_DLL")) {
		fmt.Fprintln(os.Stderr, "runtime DLL path was not applied at execution")
		os.Exit(2)
	}
	var cases []string
	switch family {
	case "runtime":
		profile = os.Getenv("GOCLUSTER_PC92_RUNTIME_PROFILE")
		cases = []string{"TestPC92RuntimeQualification"}
	case "q4":
		profile = os.Getenv("GOCLUSTER_PC92_Q4_PROFILE")
		cases = []string{"TestPC92Q4RuntimeQualification"}
	case "q5":
		profile = os.Getenv("GOCLUSTER_PC92_Q5_PROFILE")
		cases = []string{"TestPC92QualificationQ5Isolation"}
		for _, class := range []string{"spot", "pc92", "pc93", "bulletin"} {
			cases = append(cases, "TestPC92QualificationQ5Isolation/"+class)
		}
	case "q6":
		profile = os.Getenv("GOCLUSTER_PC92_Q6_PROFILE")
		cases = []string{"TestPC92QualificationQ6Faults", "TestPC92QualificationQ6ReceiveOnly"}
		repeats := 1
		if profile == "qualification" {
			repeats = 2
		}
		faults := []string{"publication", "clock-regression", "clock-frozen", "clock-frozen-loaded", "admission", "staging-capacity", "stall", "candidate-race"}
		if repeats == 2 {
			faults = append(faults, "staging-deadline")
		}
		for _, zero := range []bool{false, true} {
			for repeat := 1; repeat <= repeats; repeat++ {
				for _, fault := range faults {
					cases = append(cases, fmt.Sprintf("TestPC92QualificationQ6Faults/zero=%v/repeat=%d/%s", zero, repeat, fault))
				}
			}
		}
	case "cache":
		profile = os.Getenv("GOCLUSTER_PC92_QUALIFICATION")
		cases = []string{"TestPC92QualificationCacheMemory"}
		if profile == "cache-sustained" {
			cases[0] = "TestPC92QualificationCacheSustained"
		}
	case "retry":
		profile = os.Getenv("GOCLUSTER_PC92_V14_RETRY_PROFILE")
		if profile != "preflight" && profile != "qualification" {
			fmt.Fprintln(os.Stderr, "missing or invalid retry profile environment")
			os.Exit(2)
		}
		cases = []string{"TestPC92V14RetryWaveService", "TestPC92V14RetryWaveService/recovering_63", "TestPC92V14RetryWaveService/recovering_63_periodic", "TestPC92V14RetryWaveService/recovering_64"}
	default:
		os.Exit(2)
	}
	if scenario() == "missing_case" {
		cases = cases[:len(cases)-1]
	}
	if scenario() == "missing_retry_63" {
		cases = []string{cases[0], cases[2], cases[3]}
	}
	if scenario() == "missing_retry_periodic" {
		cases = []string{cases[0], cases[1], cases[3]}
	}
	if scenario() == "missing_retry_root" {
		cases = cases[1:]
	}
	for _, name := range cases {
		fmt.Printf("--- PASS: %s (1.00s)\n", name)
	}
	if family == "runtime" || family == "q4" {
		observations(family, profile)
	}
	switch scenario() {
	case "source_changed":
		mutate("subject.go")
	case "source_added":
		mutate("new-input.pl")
	case "source_deleted":
		must(os.Remove(filepath.Join(os.Getenv("PC92_FIXTURE_REPO"), "subject.go")))
	case "asset_changed":
		mutate("data/cty/cty.plist")
	case "reference_changed":
		must(os.WriteFile(filepath.Join(os.Getenv("PC92_FIXTURE_REFERENCE"), "perl/Fixture.pm"), []byte("changed"), 0600))
	case "test_failed":
		os.Exit(3)
	case "binary_changed":
		// Rename only this hardlink, then replace its old path. Other cases'
		// retained mock images and the original fake Go executable are untouched.
		must(os.Rename(os.Args[0], os.Args[0]+".retired"))
		must(os.WriteFile(os.Args[0], []byte("wrong executed-artifact association"), 0600))
	}
}

func observations(family, profile string) {
	path := os.Getenv("GOCLUSTER_PC92_RUNTIME_OUTPUT")
	if scenario() == "missing_observations" {
		return
	}
	if scenario() == "malformed_observations" {
		must(os.WriteFile(path, []byte("{broken"), 0600))
		return
	}
	if scenario() == "array_observations" {
		must(os.WriteFile(path, []byte("[]"), 0600))
		return
	}
	r := map[string]any{"RunID": os.Getenv("GOCLUSTER_PC92_RUN_ID"), "Profile": profile, "Diagnostic": strings.HasPrefix(profile, "preflight") || profile == "diagnostic-full", "MeasurementPassed": true, "Failures": 0, "LoadSeconds": 2700, "DrainSeconds": 660}
	if family == "q4" {
		r["Phase"] = strings.TrimPrefix(profile, "preflight-")
		r["DurationSeconds"] = 1800
		r["Failures"] = []string{}
		r["OpenEvidence"] = []string{"allocation proof open"}
	}
	switch scenario() {
	case "wrong_profile":
		r["Profile"], r["Phase"] = "wrong", "wrong"
	case "stale_run":
		r["RunID"] = "stale-run"
	case "measurement_failed":
		r["MeasurementPassed"] = false
	case "qualified_provisional":
		r["Qualified"] = true
	}
	data, err := json.Marshal(r)
	must(err)
	must(os.WriteFile(path, data, 0600))
}
