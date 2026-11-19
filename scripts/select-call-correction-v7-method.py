#!/usr/bin/env python3
"""Select one complete v7 tuning method from eight closed experimental slices.

Only bounded JSON metadata and the frozen model asset are read. Archives,
prediction streams and confirmation inputs are never opened. Selection is not
activation: a selected complete method still requires frozen confirmation.
"""

import argparse
import copy
from fractions import Fraction
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / ".tmp/call-correctionv2"
BOUND = 4 << 20
STABILITY = {"window_minutes": 15, "min_follow_on": 3, "freq_tolerance_hz": 500}
ORIGINALS = {
    "2026-11-07": (458495, "2a78ec300acc2cfe29048201af4e971cad933db01c6f36b567ac7443b57ccd34"),
    "2026-11-14": (882021, "ad0c8f5a942bab03da052cbe91c2087e59ff566cbd48aa2be8be43b1140bff49"),
    "2026-11-15": (793630, "537ef50ac1893afb9f2b440316f4cab9c0863da0071a81451c94cebdf54519ce"),
    "2026-11-16": (230784, "923e9556f375c126e364c9b84ee3eae8bb2985cc95df944262362779d3d5ef22"),
}
FRACTION = "stable applied defined".split()
REFERENCE = "all_focal_rows r1_strong r2_strong r1_unknown_reasons r2_unknown_reasons strong_label_disagreements".split()
SCALARS = "all_terminal_ids added_applied_receipts lost_applied_receipts common_applied_receipts retargeted_baseline_receipts retimed_baseline_receipts retargeted_and_retimed_baseline_receipts affected_baseline_id_union semantic_target_changed literal_target_changed scorer_key_changed new_identity_renames_outside_active_cells all_cells_new_r1_strong_disagreeing_renames all_cells_new_r2_strong_disagreeing_renames newly_assessable_r1_disagreement_same_target newly_assessable_r2_disagreement_same_target".split()
COUNTS = SCALARS + "control variant terminal_status_pairs applied_pair_classes reference_and_delivery_outcomes control_reference_coverage variant_reference_coverage exact_variant_vs_control_rate".split()
EPISODES = "episodes_with_added_receipt episodes_with_lost_receipt episodes_with_retargeted_receipt episodes_with_retimed_receipt episodes_with_both_retarget_and_retime affected_baseline_episode_union".split()
CELLDATE = "date_utc all_incremental_episode_ids jointly_strong_incremental_episodes r1_covered_incremental_episodes r2_covered_incremental_episodes r1_new_strong_contradictions r2_new_strong_contradictions r1_net_supported_correction_episodes r2_net_supported_correction_episodes".split()
VARIANT = "candidate pooled_counts pooled_modes days baseline_episode_changes active_cell_reference_floors complete_population_objective_pass eligible eligibility_assessed all_cells_strong_disagreeing_rename_gate_pass".split()
EVALPLAN = "schema_version experimental phase capture_plan method_plan control_plan parity_capture_plan parity_control_plan parity groups".split()
AGGREGATE = "schema_version experimental complete phase evaluation_plan frozen_input_bindings role_order current_stability variants ordinal_vs_geometry full_paired_union applied_receipts closed_prior_outputs accounting claim_limits".split()
COMPLETE = "schema_version experimental complete evaluation_plan aggregates full_paired_union applied_receipts".split()
METHOD = "schema_version experimental enabled source_revision reference_version model selected_candidate candidates".split()
CAPTURE = "schema_version input_scenario frozen reference_version reference_contract_sha256 frozen_at_utc selected_at_utc asset_training_through_utc tuning_through_utc receiver_metadata_through_utc receiver_metadata holdout_content_uninspected selected_candidate decision_cutoff_seconds embargo_seconds warmup_seconds max_examples candidates arms inputs".split()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def keys(value, names, nullable=()):
    require(type(value) is dict and set(value) == set(names), "missing/unknown operative fields")
    require(all(item is not None or name in nullable for name, item in value.items()), "null operative fact")
    return value


def integer(value, minimum=0, maximum=32 * 8_000_000):
    require(type(value) is int and minimum <= value <= maximum, "integer outside finite bound")
    return value


def boolean(value):
    require(type(value) is bool, "explicit boolean required")
    return value


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_ref(value):
    keys(value, ("path", "sha256"))
    require(type(value["path"]) is str and 0 < len(value["path"]) <= 1024 and "://" not in value["path"], "local bounded path required")
    require(type(value["sha256"]) is str and re.fullmatch("[0-9a-f]{64}", value["sha256"]), "lowercase SHA256 required")
    return value


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def finite_tree(value, depth=0):
    require(depth <= 32, "metadata nesting exceeds 32")
    if type(value) is dict:
        require(len(value) <= 256, "metadata object exceeds 256 keys")
        for key, child in value.items():
            require(len(key.encode("utf-8")) <= 1024, "oversized metadata key")
            finite_tree(child, depth + 1)
    elif type(value) is list:
        require(len(value) <= 256, "metadata array exceeds 256 entries")
        for child in value:
            finite_tree(child, depth + 1)
    elif type(value) is str:
        require(len(value.encode("utf-8")) <= 4096, "oversized metadata string")
    elif type(value) is float:
        require(math.isfinite(value), "nonfinite number")


def decode(data):
    value = json.loads(data.decode("utf-8"), object_pairs_hook=unique_object,
                       parse_constant=lambda _: require(False, "nonfinite JSON"))
    finite_tree(value)
    return value


class Inputs:
    """One synchronous selector owns at most 64 bounded metadata/asset buffers."""
    def __init__(self, root=ROOT):
        self.root, self.cache = root, {}

    def path(self, value):
        path = Path(value)
        return (path if path.is_absolute() else self.root / path).resolve()

    def read(self, path, expected=None, as_json=True):
        supplied = Path(path)
        require(not (supplied if supplied.is_absolute() else self.root / supplied).is_symlink(), "symlink metadata input")
        path = self.path(path)
        if path not in self.cache:
            require(len(self.cache) < 64 and path.is_file() and not path.is_symlink(), "input inventory/path refusal")
            with path.open("rb") as stream:
                data = stream.read(BOUND + 1)
            require(len(data) <= BOUND, "metadata/asset exceeds 4 MiB")
            self.cache[path] = (data, {"path": str(path), "sha256": sha(data), "bytes": len(data)})
        data, receipt = self.cache[path]
        require(expected is None or receipt["sha256"] == expected, "frozen metadata SHA256 mismatch")
        return (decode(data) if as_json else data), receipt

    def ref(self, value, as_json=True):
        file_ref(value)
        return self.read(value["path"], value["sha256"], as_json)[0]

    def unchanged(self):
        for path, (before, _) in self.cache.items():
            with path.open("rb") as stream:
                require(stream.read(BOUND + 1) == before, "input metadata changed during selection")


def candidate(value):
    keys(value, "id gap_hz max_witness_seconds ranking cw_distances".split())
    require(value["gap_hz"] in (150, 250) and type(value["gap_hz"]) is int, "unsupported gap")
    require(value["max_witness_seconds"] in (60, 120) and type(value["max_witness_seconds"]) is int, "unsupported witness separation")
    require(value["ranking"] in ("geometry", "ordinal") and value["cw_distances"] in ([1], [1, 2]), "unsupported complete ranking/map")
    require(all(type(distance) is int for distance in value["cw_distances"]), "boolean distance is invalid")
    expected = f"f{value['gap_hz']}_t{value['max_witness_seconds']}_{value['ranking']}_d{len(value['cw_distances'])}"
    require(value["id"] == expected, "candidate ID does not bind the complete combination")
    return expected


def grid():
    return {f"f{gap}_t{seconds}_{ranking}_d{distance}": {"id": f"f{gap}_t{seconds}_{ranking}_d{distance}", "gap_hz": gap,
            "max_witness_seconds": seconds, "ranking": ranking, "cw_distances": list(range(1, distance + 1))}
            for gap in (150, 250) for seconds in (60, 120) for ranking in ("geometry", "ordinal") for distance in (1, 2)}


def method_plan(value, enabled):
    keys(value, METHOD)
    require(value["schema_version"] == 1 and type(value["schema_version"]) is int and value["experimental"] is True and value["enabled"] is enabled, "wrong experimental method plan")
    require(type(value["source_revision"]) is str and re.fullmatch("[0-9a-f]{40}", value["source_revision"]), "source revision required")
    require(value["reference_version"] == "ccv2-reference-v1" and value["selected_candidate"] in ("", *grid()), "unsupported reference/selection")
    file_ref(value["model"])
    require(type(value["candidates"]) is list and len(value["candidates"]) == 16, "incomplete candidate grid")
    require({candidate(item) for item in value["candidates"]} == set(grid()), "duplicate/missing grid combination")


def capture_plan(value, method):
    keys(value, CAPTURE)
    integer(value["schema_version"], 2, 2)
    require(value["schema_version"] == 2 and value["frozen"] is True and value["holdout_content_uninspected"] is True, "unsupported capture contract")
    require(value["reference_version"] == method["reference_version"] and value["decision_cutoff_seconds"] == 75 and value["embargo_seconds"] >= 195, "capture clocks/reference changed")
    file_ref(value["receiver_metadata"])
    keys(value["input_scenario"], "receiver_geography cty_sha256 h3_res1_sha256 h3_res2_sha256".split())
    require(value["input_scenario"]["receiver_geography"] == "cty_representative_v1", "unsupported receiver scenario")
    require(len(value["inputs"]) == 4 and {item["date_utc"] for item in value["inputs"]} == set(ORIGINALS), "capture must contain exactly four exposed tuning dates")
    for item in value["inputs"]:
        keys(item, "date_utc phase archive max_rows max_csv_bytes".split())
        require(item["phase"] == "tuning", "confirmation input cannot enter tuning selector")
        file_ref(item["archive"]); integer(item["max_rows"], 1, 8_000_000); integer(item["max_csv_bytes"], 1, 512 << 20)
    require(len(value["arms"]) == 3 and {arm["name"] for arm in value["arms"]} == {"shipped", "repaired_off", "repaired_on"}, "capture arm coverage changed")
    for arm in value["arms"]:
        keys(arm, "name executable_sha256 source_revision config_dir config_files assets".split())
        for asset in arm["assets"]: file_ref(asset)
        require(type(arm["config_files"]) is dict and arm["config_files"], "frozen config file set required")
        if arm["name"] == "repaired_off": require(arm["source_revision"] == method["source_revision"], "capture/method source mismatch")
    require(1 <= len(value["candidates"]) <= 16, "ordinary candidate coverage changed")
    for item in value["candidates"]:
        keys(item, "id parameters mode_distances".split())
        # The Go evaluator already validates the ordinary vector. Preserve its
        # entire hash-bound value; do not reinterpret its inactive Bayes fields.
        require(type(item["parameters"]) is dict and type(item["mode_distances"]) is dict, "ordinary vector/map required")
    require(value["selected_candidate"] in {item["id"] for item in value["candidates"]}, "ordinary complete candidate is missing")


def binding(value, variant=False):
    keys(value, ["complete", "manifest"] + (["candidate_id"] if variant else []))
    file_ref(value["complete"]); file_ref(value["manifest"])


def eval_plan(value):
    keys(value, EVALPLAN)
    integer(value["schema_version"], 1, 1)
    require(value["schema_version"] == 1 and value["experimental"] is True and value["phase"] == "tuning", "only experimental tuning evaluations are selectable")
    for name in ("capture_plan", "method_plan", "control_plan", "parity_capture_plan", "parity_control_plan"): file_ref(value[name])
    require(len(value["parity"]) == 4 and {item["date_utc"] for item in value["parity"]} == set(ORIGINALS), "missing/duplicate parity date")
    for item in value["parity"]:
        keys(item, "date_utc original_decisions control_decisions rows control_complete control_manifest".split())
        rows, digest = ORIGINALS[item["date_utc"]]
        require(type(item["rows"]) is int and item["rows"] == rows, "authoritative original row count changed")
        for name in ("original_decisions", "control_decisions", "control_complete", "control_manifest"): file_ref(item[name])
        require(item["original_decisions"]["sha256"] == digest == item["control_decisions"]["sha256"], "unproven disabled parity")
    require(len(value["groups"]) == 4 and {item["date_utc"] for item in value["groups"]} == set(ORIGINALS), "missing/duplicate evaluation date")
    for group in value["groups"]:
        keys(group, "date_utc control variants".split()); binding(group["control"])
        require(len(group["variants"]) == 2, "each slice needs exactly a geometry/ordinal pair")
        for item in group["variants"]: binding(item, True)


def fraction(value):
    keys(value, FRACTION); integer(value["applied"]); integer(value["stable"], 0, value["applied"])
    require(boolean(value["defined"]) == (value["applied"] > 0), "undefined denominator flag changed")


def counts(value):
    keys(value, COUNTS)
    for name in SCALARS: integer(value[name])
    rows = value["all_terminal_ids"]
    for name in ("control", "variant"): fraction(value[name]); require(value[name]["applied"] <= rows, "applied count exceeds population")
    lost, added, common = (value[name] for name in ("lost_applied_receipts", "added_applied_receipts", "common_applied_receipts"))
    retarget, retime, overlap = (value[name] for name in ("retargeted_baseline_receipts", "retimed_baseline_receipts", "retargeted_and_retimed_baseline_receipts"))
    require(lost + common == value["control"]["applied"] and added + common == value["variant"]["applied"], "applied pair denominator changed")
    require(overlap <= min(retarget, retime) and retarget + retime - overlap <= common, "invalid retarget/retime overlap")
    require(value["affected_baseline_id_union"] == lost + retarget + retime - overlap, "tertiary union double-counted an input")
    for name in ('terminal_status_pairs', 'applied_pair_classes', 'reference_and_delivery_outcomes'):
        require(type(value[name]) is dict and len(value[name]) <= 32, 'bounded descriptive counter map required')
    for name in ('control_reference_coverage', 'variant_reference_coverage'):
        keys(value[name], REFERENCE)
        require(value[name]['all_focal_rows'] == rows, 'reference population changed')
    comparison = rate_compare(value["control"], value["variant"])
    require(value["exact_variant_vs_control_rate"] == comparison, "rounded or incorrect fraction comparison")


def rate_compare(control, variant):
    if not control["applied"] or not variant["applied"]: return "undefined"
    left, right = variant["stable"] * control["applied"], control["stable"] * variant["applied"]
    return "variant_higher" if left > right else "control_higher" if left < right else "equal"


def reasons(value):
    result = []
    if not value["control"]["applied"] or not value["variant"]["applied"]: result.append("undefined stability denominator")
    if value["variant"]["applied"] <= value["control"]["applied"]: result.append("no strict increase in applied outputs")
    if rate_compare(value["control"], value["variant"]) == "control_higher": result.append("exact stability fraction decreased")
    for ref in ("r1", "r2"):
        if value[f"all_cells_new_{ref}_strong_disagreeing_renames"]: result.append(f"new strong-disagreeing rename under {ref}")
    return result


def variant_result(value, assessed=True):
    keys(value, VARIANT, nullable=('active_cell_reference_floors',) if not assessed else ())
    candidate(value['candidate']); counts(value['pooled_counts'])
    require(value['eligibility_assessed'] is assessed, 'eligibility assessment marker changed')
    rejected = reasons(value['pooled_counts'])
    objective = not any(reason in rejected for reason in ('undefined stability denominator', 'no strict increase in applied outputs', 'exact stability fraction decreased'))
    rename = all(value['pooled_counts'][f'all_cells_new_{ref}_strong_disagreeing_renames'] == 0 for ref in ('r1', 'r2'))
    require(boolean(value['complete_population_objective_pass']) == objective and boolean(value['all_cells_strong_disagreeing_rename_gate_pass']) == rename, 'objective/reference flags do not match exact counters')
    require(boolean(value['eligible']) == (not rejected if assessed else False), 'eligibility disagrees with tuning contract')
    require(type(value['pooled_modes']) is dict and set(value['pooled_modes']) <= {'CW', 'RTTY'}, 'bounded descriptive modes required')
    require(len(value['days']) == 4 and {day['date_utc'] for day in value['days']} == set(ORIGINALS), 'candidate does not cover all four dates')
    for day in value['days']:
        keys(day, 'date_utc archive_rows focal_rows counts modes baseline_episode_changes all_observed_cells'.split())
        counts(day['counts'])
        require(type(day['focal_rows']) is int and day['focal_rows'] == ORIGINALS[day['date_utc']][0] == day['counts']['all_terminal_ids'], 'date population changed')
        integer(day['archive_rows'], day['focal_rows'], 8_000_000)
        require(type(day['modes']) is dict and type(day['all_observed_cells']) is dict, 'descriptive mode/cell payload required')
    for name in ('all_terminal_ids', 'affected_baseline_id_union',
                 'all_cells_new_r1_strong_disagreeing_renames',
                 'all_cells_new_r2_strong_disagreeing_renames'):
        require(sum(day['counts'][name] for day in value['days']) == value['pooled_counts'][name], 'selection-operative date counts do not pool')
    for role in ('control', 'variant'):
        for field in ('applied', 'stable'):
            require(sum(day['counts'][role][field] for day in value['days']) == value['pooled_counts'][role][field], 'date fractions do not pool')
    # Reference-floor, episode and mode diagnostics were checked by the native
    # evaluator. Their known nested payload is retained, never rescored here.
    require(type(value['baseline_episode_changes']) is dict, 'episode diagnostics required')
    keys(value['baseline_episode_changes'], EPISODES)
    for day in value['days']:
        keys(day['baseline_episode_changes'], EPISODES)
        for cell in day['all_observed_cells'].values(): keys(cell, CELLDATE)
    require(type(value['active_cell_reference_floors']) is list if assessed else value['active_cell_reference_floors'] is None or type(value['active_cell_reference_floors']) is list, 'reference-floor diagnostics required')


def invariant_inputs(value):
    result = copy.deepcopy(value)
    result["groups"] = [{"date_utc": item["date_utc"], "control": item["control"]} for item in sorted(result["groups"], key=lambda item: item["date_utc"])]
    return result


def load_grid(directories, reader):
    require(len(directories) == 8 and len({reader.path(path) for path in directories}) == 8, "exactly eight distinct completed evaluations required")
    candidates, sources, baseline = {}, [], None
    for directory in sorted(directories, key=lambda path: str(reader.path(path))):
        directory = reader.path(directory); complete, receipt = reader.read(directory / "COMPLETE.json"); keys(complete, COMPLETE)
        integer(complete["schema_version"], 1, 1)
        require(complete["schema_version"] == 1 and complete["experimental"] is True and complete["complete"] is True, "incomplete/nonexperimental evaluation")
        file_ref(complete["aggregates"]); require(reader.path(complete["aggregates"]["path"]) == directory / "aggregates.json", "aggregate escapes evaluation directory")
        aggregate = reader.ref(complete["aggregates"]); keys(aggregate, AGGREGATE)
        integer(aggregate["schema_version"], 1, 1)
        keys(aggregate["current_stability"], STABILITY)
        integer(aggregate["current_stability"]["window_minutes"], 15, 15); integer(aggregate["current_stability"]["min_follow_on"], 3, 3)
        require(type(aggregate["current_stability"]["freq_tolerance_hz"]) in (int, float), "numeric stability tolerance required")
        require(aggregate["schema_version"] == 1 and aggregate["experimental"] is True and aggregate["complete"] is True and aggregate["phase"] == "tuning" and aggregate["current_stability"] == STABILITY, "unsupported completed evaluation")
        for name in ("evaluation_plan", "full_paired_union", "applied_receipts"): file_ref(complete[name]); require(complete[name] == aggregate[name], "completion/aggregate binding mismatch")
        for name, basename in (("full_paired_union", "paired_union.jsonl"), ("applied_receipts", "applied_receipts.jsonl")):
            require(reader.path(complete[name]["path"]) == directory / basename, "closed stream reference escaped its evaluation")
        plan = reader.ref(complete["evaluation_plan"]); eval_plan(plan); require(plan == aggregate["frozen_input_bindings"], "evaluation plan differs from frozen inputs")
        require(len(aggregate["variants"]) == 2 and aggregate["ordinal_vs_geometry"] is not None, "complete pair/attribution missing")
        method = reader.ref(plan["method_plan"]); method_plan(method, True)
        control = reader.ref(plan["control_plan"]); method_plan(control, False)
        normalized = lambda item: {key: item[key] for key in METHOD if key not in ("enabled", "selected_candidate")}
        require(normalized(method) == normalized(control), "method/control source/model/grid changed")
        capture = reader.ref(plan["capture_plan"]); capture_plan(capture, method)
        parity_capture = reader.ref(plan["parity_capture_plan"]); capture_plan(parity_capture, method)
        parity_control = reader.ref(plan["parity_control_plan"]); method_plan(parity_control, False)
        require(normalized(method) == normalized(parity_control), "parity control contract changed")
        reader.ref(method["model"], as_json=False)
        shared = {"inputs": invariant_inputs(plan), "method": method, "control": control, "capture": capture,
                  "parity_capture": parity_capture, "parity_control": parity_control, "stability": aggregate["current_stability"]}
        # The signature retains whole ordinary vectors/assets/configs, not just a
        # candidate name. Paired variants alone differ between the eight slices.
        require(baseline is None or baseline == shared, "source/model/capture/control/ordinary invariant changed between slices")
        baseline = shared
        pair = sorted(aggregate["variants"], key=lambda item: item["candidate"]["ranking"])
        require(aggregate["role_order"] == ["control", *[item["candidate"]["id"] for item in pair]], "role order was relabeled")
        first, second = (item["candidate"] for item in pair)
        require(first["ranking"] == "geometry" and second["ranking"] == "ordinal" and {key: first[key] for key in first if key not in ("id", "ranking")} == {key: second[key] for key in second if key not in ("id", "ranking")}, "mismatched geometry/ordinal pair")
        for group in plan["groups"]: require({item["candidate_id"] for item in group["variants"]} == {first["id"], second["id"]}, "pair candidate changed across dates")
        for value in pair:
            variant_result(value); ident = candidate(value["candidate"])
            require(ident not in candidates and value["candidate"] == grid()[ident], "duplicate/outside candidate")
            signature = {"pooled": value["pooled_counts"]["control"], "pooled_references": value["pooled_counts"]["control_reference_coverage"],
                         "modes": {mode: (item["control"], item["control_reference_coverage"]) for mode, item in value["pooled_modes"].items()},
                         "days": [(day["date_utc"], day["counts"]["control"], day["counts"]["control_reference_coverage"],
                                   {mode: (item["control"], item["control_reference_coverage"]) for mode, item in day["modes"].items()})
                                  for day in sorted(value["days"], key=lambda item: item["date_utc"])]}
            if candidates: require(signature == next(iter(candidates.values()))["control_signature"], "control pooled/date population changed")
            candidates[ident] = {"candidate": value["candidate"], "eligible": value["eligible"], "reasons": reasons(value["pooled_counts"]),
                                 "evaluation_complete": receipt, "evaluator_variant": value, "control_signature": signature}
        variant_result(aggregate["ordinal_vs_geometry"], False)
        require(aggregate["ordinal_vs_geometry"]["candidate"] == second, "attribution candidate differs")
        keys(aggregate["accounting"], "stage charged_bytes limit_bytes reserved_capacity_bytes policy".split())
        for name in ("charged_bytes", "limit_bytes", "reserved_capacity_bytes"): integer(aggregate["accounting"][name], 0, 4 << 30)
        require(aggregate["accounting"]["limit_bytes"] == 4 << 30 and aggregate["accounting"]["reserved_capacity_bytes"] == 16 << 20 and 0 <= aggregate["accounting"]["charged_bytes"] <= (4 << 30) - (16 << 20), "evaluation budget claim changed")
        require(aggregate["accounting"]["stage"] == "closed_evaluation_streams_before_aggregates_and_COMPLETE" and aggregate["accounting"]["policy"] == "cumulative requested writes; no cleanup refunds; rename charged once", "evaluation accounting stage/policy changed")
        require(type(aggregate["closed_prior_outputs"]) is list and len(aggregate["closed_prior_outputs"]) == 2, "evaluation stream inventory changed")
        for item in aggregate["closed_prior_outputs"]: keys(item, ("file", "bytes")); file_ref(item["file"]); integer(item["bytes"], 0, 4 << 30)
        require({item["file"]["sha256"] for item in aggregate["closed_prior_outputs"]} == {aggregate["full_paired_union"]["sha256"], aggregate["applied_receipts"]["sha256"]}, "closed stream hash inventory changed")
        require(type(aggregate["claim_limits"]) is list and all(type(item) is str for item in aggregate["claim_limits"]), "claim limits required")
        sources.append(receipt)
    require(set(candidates) == set(grid()), "all sixteen complete candidates are required")
    return candidates, baseline, sources


def choose(candidates):
    eligible = [item for item in candidates.values() if item["eligible"]]
    def order(item):
        metrics = item["evaluator_variant"]["pooled_counts"]
        return (-metrics["variant"]["applied"], -Fraction(metrics["variant"]["stable"], metrics["variant"]["applied"]),
                metrics["affected_baseline_id_union"], item["candidate"]["id"])
    return min(eligible, key=order) if eligible else None


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def publish(out, value, evidence_root=EVIDENCE, fault=None):
    require(not out.exists() and not out.is_symlink(), "output already exists")
    out = out.resolve(); require(out.parent == evidence_root.resolve() and not out.exists(), "output must be a new direct child of the evidence root")
    data = encode(value); selection = {"path": str(out / "selection.json"), "sha256": sha(data)}
    complete = encode({"schema_version": 1, "experimental": True, "complete": True, "selection": selection})
    require(len(data) + len(complete) <= BOUND, "combined selection metadata exceeds 4 MiB")
    out.mkdir(mode=0o700)
    def point(stage):
        if stage == fault: raise OSError("synthetic publication failure: " + stage)
    def write(path, payload, stage):
        with path.open("xb") as stream:
            point(stage + "_write"); require(stream.write(payload) == len(payload), "short metadata write")
            stream.flush(); point(stage + "_sync"); os.fsync(stream.fileno())
        point(stage + "_close")
        with path.open("rb") as stream: require(stream.read(BOUND + 1) == payload, "written metadata verification failed")
    write(out / "selection.json", data, "selection")
    pending = out / "COMPLETE.json.pending"; write(pending, complete, "complete")
    point("complete_rename"); os.rename(pending, out / "COMPLETE.json")
    return selection


def select(directories, out):
    reader = Inputs(); candidates, common, evaluations = load_grid(directories, reader)
    selected = choose(candidates)
    _, script = reader.read(Path(__file__).resolve(), as_json=False)
    value = {"schema_version": 1, "experimental": True, "phase": "tuning", "status": "selected" if selected else "no_eligible_candidate",
             "source_revision": common["method"]["source_revision"], "model": common["method"]["model"], "selector_source": script,
             "selector_contract": {"eligibility": "strictly more applied outputs; defined exact nondecreasing 15/3/500 fraction; inherited zero-new-strong-disagreeing-rename gate",
                                   "order": ["greatest applied outputs", "greatest exact stable/applied fraction", "smallest affected baseline input ID union", "lexical complete candidate ID"],
                                   "tertiary_union": "lost + retargeted + retimed - retargeted_and_retimed; each affected baseline input counts once", "stability": STABILITY},
             "evaluations": evaluations, "input_hashes": [item[1] for _, item in sorted(reader.cache.items(), key=lambda pair: str(pair[0]))],
             "candidates": [{key: value for key, value in candidates[ident].items() if key != "control_signature"} for ident in sorted(candidates)],
             "selected_complete_method": None if selected is None else {"candidate": selected["candidate"], "model": common["method"]["model"], "complete_enabled_map": {"CW": selected["candidate"]["cw_distances"]}, "method_plan": common["inputs"]["method_plan"]},
             "claim_limits": ["Exposed tuning selection only; freeze the selected complete method and confirmation contract before untouched inputs are opened", "No archive/prediction stream is read or rescored here; referenced closed evaluator evidence remains hash-bound", "No live activation, RF accuracy or calibrated probability claim"]}
    reader.unchanged()
    return publish(out, value)


def selfcheck_variant_pooling():
    """A native-shaped four-date positive case, then hidden day harm per ref."""
    def metrics(rows, multiplier=1):
        value = {name: 0 for name in SCALARS}
        coverage = dict(zip(REFERENCE, (rows, rows, rows, {}, {}, 0)))
        value.update({"all_terminal_ids": rows, "added_applied_receipts": 2 * multiplier,
                      "common_applied_receipts": 10 * multiplier,
                      "control": {"stable": 8 * multiplier, "applied": 10 * multiplier, "defined": True},
                      "variant": {"stable": 10 * multiplier, "applied": 12 * multiplier, "defined": True},
                      "terminal_status_pairs": {"output/output": rows},
                      "applied_pair_classes": {"neither": rows - 12 * multiplier, "on_only": 2 * multiplier, "common_same": 10 * multiplier},
                      "reference_and_delivery_outcomes": {"unchanged": rows - 2 * multiplier, "helpful": 2 * multiplier},
                      "control_reference_coverage": copy.deepcopy(coverage), "variant_reference_coverage": copy.deepcopy(coverage),
                      "exact_variant_vs_control_rate": "variant_higher"})
        return value
    days = []
    for date, (rows, _) in ORIGINALS.items():
        cell = {name: 0 for name in CELLDATE}
        cell.update({"date_utc": date, "all_incremental_episode_ids": 2,
                     "jointly_strong_incremental_episodes": 2, "r1_covered_incremental_episodes": 2,
                     "r2_covered_incremental_episodes": 2, "r1_net_supported_correction_episodes": 2,
                     "r2_net_supported_correction_episodes": 2})
        day_counts = metrics(rows)
        days.append({"date_utc": date, "archive_rows": rows, "focal_rows": rows, "counts": day_counts,
                     "modes": {"CW": copy.deepcopy(day_counts)}, "baseline_episode_changes": {name: 2 if name == "episodes_with_added_receipt" else 0 for name in EPISODES},
                     "all_observed_cells": {"CW/1": cell}})
    pooled = metrics(sum(rows for rows, _ in ORIGINALS.values()), 4)
    value = {"candidate": grid()["f150_t60_geometry_d1"], "pooled_counts": pooled,
             "pooled_modes": {"CW": copy.deepcopy(pooled)}, "days": days,
             "baseline_episode_changes": {name: 8 if name == "episodes_with_added_receipt" else 0 for name in EPISODES},
             "active_cell_reference_floors": [{"cell": "CW/1", "pass": False,
                 "dates": [day["all_observed_cells"]["CW/1"] for day in days],
                 "jointly_strong_incremental_episodes": 8, "r1_coverage_percent": 100,
                 "r2_coverage_percent": 100, "reasons": ["requires >=200 jointly strong additional episodes and >=50 on each of two dates"]}],
             "complete_population_objective_pass": True, "eligible": True, "eligibility_assessed": True,
             "all_cells_strong_disagreeing_rename_gate_pass": True}
    variant_result(value)
    for ref in ("r1", "r2"):
        hidden = copy.deepcopy(value)
        name = f"all_cells_new_{ref}_strong_disagreeing_renames"
        hidden["days"][0]["counts"][name] = 1
        hidden["days"][0]["modes"]["CW"][name] = 1
        hidden["days"][0]["all_observed_cells"]["CW/1"][f"{ref}_new_strong_contradictions"] = 1
        hidden["active_cell_reference_floors"][0]["reasons"].append("new strong contradiction under at least one reference")
        try: variant_result(hidden)
        except ValueError as error: require(str(error) == "selection-operative date counts do not pool", "hidden-harm fixture failed at an unrelated boundary")
        else: raise AssertionError(f"hidden per-date {ref} rename harm passed pooled eligibility")


def selfcheck():
    # Literal order cases use hand-counted fractions and union overlaps; no real
    # evidence files, archives, prediction streams or confirmation dates are read.
    selfcheck_variant_pooling()
    def item(ident, applied, stable, affected, eligible=True):
        return {"candidate": {"id": ident}, "eligible": eligible, "evaluator_variant": {"pooled_counts": {"variant": {"applied": applied, "stable": stable}, "affected_baseline_id_union": affected}}}
    a, b = item("z", 1200, 960, 10), item("a", 1200, 960, 15)
    require(choose({"a": a, "b": b}) is a, "union tie-break must prefer ten retarget+retime IDs to fifteen solely retimed IDs")
    require(choose({"a": item("z", 1201, 960, 100), "b": b})["candidate"]["id"] == "z", "applied count precedence")
    require(choose({"a": item("z", 1200, 961, 100), "b": b})["candidate"]["id"] == "z", "exact fraction precedence")
    require(choose({"a": item("z", 1200, 960, 15), "b": b}) is b, "lexical final tie-break")
    require(choose({"a": item("a", 1200, 960, 0, False)}) is None, "no eligible candidate must remain a no-selection verdict")
    for old_s, old_n, new_s, new_n in ((800, 1000, 959, 1200), (800000, 1000000, 800159, 1000200)):
        require(rate_compare({"stable": old_s, "applied": old_n}, {"stable": new_s, "applied": new_n}) == "control_higher", "rounded fraction hid exact decline")
    for bad in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e9999}'):
        try: decode(bad)
        except ValueError: pass
        else: raise AssertionError("invalid metadata was accepted")
    for invalid in ({"experimental": None}, {"experimental": True, "unknown": 1}, {}):
        try: keys(invalid, ("experimental",))
        except ValueError: pass
        else: raise AssertionError("missing/null/unknown operative field was accepted")
    for directories in (("one",), ("same",) * 8):
        try: load_grid(directories, Inputs())
        except ValueError: pass
        else: raise AssertionError("missing/duplicate evaluation was accepted")
    with tempfile.TemporaryDirectory(prefix="ccv7-selector-selfcheck-") as temp:
        root = Path(temp); path = root / "aggregate.json"; path.write_bytes(b'{}\n')
        reader = Inputs(root)
        try: reader.read(path, "0" * 64)
        except ValueError: pass
        else: raise AssertionError("tampered metadata hash was accepted")
        for fault in ("selection_write", "selection_sync", "selection_close", "complete_write", "complete_sync", "complete_close", "complete_rename"):
            out = root / fault
            try: publish(out, {"experimental": True}, root, fault)
            except OSError: pass
            else: raise AssertionError("publication fault was ignored")
            require(not (out / "COMPLETE.json").exists(), "publication fault exposed COMPLETE")
        out = root / "success"; record = publish(out, {"experimental": True}, root)
        require(sha((out / "selection.json").read_bytes()) == record["sha256"] and decode((out / "COMPLETE.json").read_bytes())["selection"] == record, "COMPLETE-last source binding changed")
    return {"selfcheck": "passed", "literal_ordering": 5, "exact_declines": 2, "variant_pooling_cases": 3,
            "publication_faults": 7, "real_evidence_read": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selfcheck", action="store_true")
    parser.add_argument("--evaluations", nargs=8, type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    require(args.selfcheck != (args.evaluations is not None), "choose --selfcheck or eight --evaluations")
    require((args.out is not None) == (args.evaluations is not None), "selection requires --out; selfcheck forbids it")
    print(json.dumps(selfcheck() if args.selfcheck else select(args.evaluations, args.out), sort_keys=True))


if __name__ == "__main__":
    try: main()
    except (OSError, ValueError, KeyError, TypeError, RecursionError, AssertionError) as error:
        print(f"v7 method selection failed; no new COMPLETE authority: {error}", file=sys.stderr)
        sys.exit(1)
