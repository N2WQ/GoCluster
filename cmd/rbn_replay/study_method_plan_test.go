package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func methodTestWrite(t testing.TB, path string, data []byte) studyFile {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := studySHA256Bytes(data)
	return studyFile{Path: path, SHA256: hex.EncodeToString(sum[:])}
}

func methodTestPlan(t testing.TB) studyMethodPlan {
	t.Helper()
	model := methodTestWrite(t, filepath.Join(t.TempDir(), "model.json"), ordinalWireFixture(t))
	plan := studyMethodPlan{SchemaVersion: 1, Experimental: true, SourceRevision: strings.Repeat("a", 40), ReferenceVersion: studyReferenceVersion, Model: model}
	for _, gap := range []int{150, 250} {
		for _, seconds := range []int{60, 120} {
			for _, ranking := range []string{"geometry", "ordinal"} {
				for _, distances := range [][]int{{1}, {1, 2}} {
					plan.Candidates = append(plan.Candidates, studyMethodCandidate{ID: fmt.Sprintf("f%d_t%d_%s_d%d", gap, seconds, ranking, len(distances)), GapHz: gap, MaxWitnessSeconds: seconds, Ranking: ranking, CWDistances: distances})
				}
			}
		}
	}
	return plan
}

func methodTestRequest(t testing.TB, plan studyMethodPlan) (studyCLIRequest, studyArm) {
	t.Helper()
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	file := methodTestWrite(t, filepath.Join(t.TempDir(), "plan.json"), data)
	return studyCLIRequest{MethodPlan: file.Path, MethodFreeze: file.SHA256, MethodCandidate: plan.Candidates[0].ID, Phase: "tuning", Arm: "repaired_off"}, studyArm{Name: "repaired_off", SourceRevision: plan.SourceRevision}
}

func methodTestPrepared(t testing.TB) *studyMethodPrepared {
	t.Helper()
	req, arm := methodTestRequest(t, methodTestPlan(t))
	prepared, err := prepareStudyMethod(req, arm)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func TestCCV2F2MethodPlan(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		plan := methodTestPlan(t)
		plan.Enabled = enabled
		req, arm := methodTestRequest(t, plan)
		prepared, err := prepareStudyMethod(req, arm)
		if err != nil || prepared == nil || prepared.Model == nil || prepared.Plan.Enabled != enabled || len(prepared.Plan.Candidates) != 16 || prepared.Candidate.ID != req.MethodCandidate {
			t.Fatalf("explicit enabled=%t lost the requested experiment/model: %v", enabled, err)
		}
	}
	if prepared, err := prepareStudyMethod(studyCLIRequest{}, studyArm{}); err != nil || prepared != nil {
		t.Fatal("absent method changed ordinary execution:", err)
	}
	plan := methodTestPlan(t)
	for _, scenario := range []string{"missing_freeze", "missing_plan", "missing_candidate", "wrong_arm", "wrong_source", "compare", "diagnostics", "holdout_unselected", "unknown_candidate", "bad_model"} {
		t.Run(scenario, func(t *testing.T) {
			req, arm := methodTestRequest(t, plan)
			switch scenario {
			case "missing_freeze":
				req.MethodFreeze = ""
			case "missing_plan":
				req.MethodPlan = ""
			case "missing_candidate":
				req.MethodCandidate = ""
			case "wrong_arm":
				arm.Name = "repaired_on"
			case "wrong_source":
				arm.SourceRevision = strings.Repeat("b", 40)
			case "compare":
				req.Compare = "reports"
			case "diagnostics":
				req.DiagnosticSelection = "selection"
			case "holdout_unselected":
				req.Phase = "holdout"
			case "unknown_candidate":
				req.MethodCandidate = "other"
			case "bad_model":
				// Disabled parity must still load and validate its requested asset.
				if err := os.Remove(plan.Model.Path); err != nil {
					t.Fatal(err)
				}
			}
			if prepared, err := prepareStudyMethod(req, arm); err == nil || prepared != nil {
				t.Fatal("invalid requested method silently became usable")
			}
		})
	}
	plan = methodTestPlan(t)
	plan.SelectedCandidate = plan.Candidates[0].ID
	req, arm := methodTestRequest(t, plan)
	req.Phase = "holdout"
	if _, err := prepareStudyMethod(req, arm); err != nil {
		t.Fatal("frozen holdout selection refused:", err)
	}
	req.MethodCandidate = plan.Candidates[1].ID
	if _, err := prepareStudyMethod(req, arm); err == nil {
		t.Fatal("holdout selected another complete candidate")
	}
}

func TestCCV2F2MethodPlanWireBounds(t *testing.T) {
	plan := methodTestPlan(t)
	valid, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	load := func(data []byte) error {
		file := methodTestWrite(t, filepath.Join(t.TempDir(), "plan.json"), data)
		_, _, err := loadStudyMethodPlan(file.Path, file.SHA256)
		return err
	}
	for _, key := range []string{"schema_version", "experimental", "enabled", "source_revision", "reference_version", "model", "selected_candidate", "candidates"} {
		for _, missing := range []bool{false, true} {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(valid, &root); err != nil {
				t.Fatal(err)
			}
			if missing {
				delete(root, key)
			} else {
				root[key] = json.RawMessage("null")
			}
			data, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := load(data); err == nil {
				t.Fatalf("operative field %s missing=%t accepted", key, missing)
			}
		}
	}
	for _, data := range [][]byte{
		bytes.Replace(valid, []byte(`"enabled":false`), []byte(`"enabled":false,"enabled":false`), 1),
		bytes.Replace(valid, []byte(`"enabled":false`), []byte(`"Enabled":false`), 1),
		bytes.Replace(valid, []byte(`"experimental":true`), []byte(`"experimental":false`), 1),
		bytes.Replace(valid, []byte(`"cw_distances":[1]`), []byte(`"cw_distances":[1,2,3]`), 1),
		bytes.Replace(valid, []byte(`"gap_hz":150`), []byte(`"gap_hz":150,"unknown":0`), 1),
		bytes.Replace(valid, []byte(`"path":`), []byte(`"unknown":0,"path":`), 1),
		bytes.Replace(valid, []byte(`"path":`), []byte(`"path":null,"ignored_path":`), 1),
		append(append([]byte{}, valid...), []byte(` {}`)...),
		bytes.Repeat([]byte(" "), (4<<20)+1),
	} {
		if err := load(data); err == nil {
			t.Fatal("invalid wire shape accepted")
		}
	}
	for _, key := range []string{"id", "gap_hz", "max_witness_seconds", "ranking", "cw_distances"} {
		for _, missing := range []bool{false, true} {
			var root map[string]json.RawMessage
			var candidates []map[string]json.RawMessage
			if err := json.Unmarshal(valid, &root); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(root["candidates"], &candidates); err != nil {
				t.Fatal(err)
			}
			if missing {
				delete(candidates[0], key)
			} else {
				candidates[0][key] = json.RawMessage("null")
			}
			root["candidates"], err = json.Marshal(candidates)
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := load(data); err == nil {
				t.Fatalf("candidate field %s missing=%t accepted", key, missing)
			}
		}
	}
	file := methodTestWrite(t, filepath.Join(t.TempDir(), "plan.json"), valid)
	if _, _, err := loadStudyMethodPlan(file.Path, strings.Repeat("0", 64)); err == nil {
		t.Fatal("plan SHA mismatch accepted")
	}
}

func TestCCV2F2MethodPlanCompleteGrid(t *testing.T) {
	for _, scenario := range []string{"short", "surplus", "duplicate", "renamed", "distance_map", "unknown_selected", "uppercase_revision"} {
		t.Run(scenario, func(t *testing.T) {
			plan := methodTestPlan(t)
			switch scenario {
			case "short":
				plan.Candidates = plan.Candidates[:15]
			case "surplus":
				plan.Candidates = append(plan.Candidates, plan.Candidates[0])
			case "duplicate":
				plan.Candidates[1] = plan.Candidates[0]
			case "renamed":
				plan.Candidates[0].ID = "winner"
			case "distance_map":
				plan.Candidates[0].CWDistances = []int{2}
			case "unknown_selected":
				plan.SelectedCandidate = "winner"
			case "uppercase_revision":
				plan.SourceRevision = strings.Repeat("A", 40)
			}
			req, arm := methodTestRequest(t, plan)
			if _, err := prepareStudyMethod(req, arm); err == nil {
				t.Fatal("incomplete or reinterpreted grid accepted")
			}
		})
	}
}
