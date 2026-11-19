#!/usr/bin/env python3
"""Freeze the complete exposed v7 replay grid without executing predictions.

Actual preparation requires a clean call-correctionv2 checkout and an unmodified
native executable from its HEAD. Only four already-exposed dates are admitted.
FREEZE.json is the last publication; partial metadata grants no run authority.
The script selects no method winner and establishes no production qualification.
"""

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / ".tmp/call-correctionv2"
TEMPLATE = EVIDENCE / "v6-e1-exposed-plan-v1.json"
TEMPLATE_SHA = "3515b1444a25351ce6a2cac9f2b620a91877f06671a8778d514f295eb547ef36"
MODEL = ROOT / "cmd/rbn_replay/testdata/ordinal_fitted_v7.json"
MODEL_SHA = "968caf263f3bf44c13509ded8b5e48faf0bd39b1d7cc42dcd9d1e6c0a45e5e1a"
REFERENCE = "ccv2-reference-v1"
ORDINARY = "cw2_r450_a100_d0"
ORDINARY_SHA = "7c32c51dc16353f642e57422fddea192d79b6f40608e9686ff31cc01788e796c"
METADATA_LIMIT = 4 << 20
BINARY_LIMIT = 256 << 20
ARCHIVE_LIMIT = 512 << 20
LINE_LIMIT = 64 << 10
ORIGINALS = (
    ("20261107", 458495, 688555989,
     "2a78ec300acc2cfe29048201af4e971cad933db01c6f36b567ac7443b57ccd34",
     "b525616a21e14892bb1251d066e1b3bf9cd13a769ff55bea677d1cdc807436ac"),
    ("20261114", 882021, 1325680003,
     "ad0c8f5a942bab03da052cbe91c2087e59ff566cbd48aa2be8be43b1140bff49",
     "cac1b5ad664d77e2abefe6e0d6d4ad41c51adaeed017bce97a8de12a408c0e55"),
    ("20261115", 793630, 1193460297,
     "537ef50ac1893afb9f2b440316f4cab9c0863da0071a81451c94cebdf54519ce",
     "08756dc36c582e2a5a3edaff10d1037c49c0cb46d7e9f755a5f529eae868b5e0"),
    ("20261116", 230784, 347469164,
     "923e9556f375c126e364c9b84ee3eae8bb2985cc95df944262362779d3d5ef22",
     "bdcd12efbf3d47f9db521dff7843b2f8e62f38c20936a1cdff54caaf6c5ad711"),
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=unique_object,
                      parse_constant=lambda value: require(False, f"nonfinite JSON: {value}"))


def encode(value, *, sort_keys=True):
    data = (json.dumps(value, sort_keys=sort_keys, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    require(len(data) <= METADATA_LIMIT, "metadata exceeds 4 MiB")
    return data


def safe_path(value, base=ROOT):
    path = Path(value)
    require(path.is_absolute() and not str(path).startswith(("\\\\", "//")), "absolute local path required")
    resolved = path.resolve()
    require(resolved.is_relative_to(base.resolve()), f"path outside declared root: {path}")
    for parent in (path, *path.parents):
        require(not parent.is_symlink() and not parent.is_junction(), f"linked input/output path: {parent}")
        if parent == base:
            break
    return resolved


def read_bounded(path):
    require(path.is_file() and not path.is_symlink(), f"regular metadata file required: {path}")
    with path.open("rb") as stream:
        data = stream.read(METADATA_LIMIT + 1)
    require(len(data) <= METADATA_LIMIT, f"oversized metadata: {path}")
    return data


def hash_file(path, expected=None, limit=BINARY_LIMIT, rows=None, exact_bytes=None):
    require(path.is_file() and not path.is_symlink(), f"regular file required: {path}")
    size = path.stat().st_size
    require(0 <= size <= limit and (exact_bytes is None or size == exact_bytes), f"file size outside frozen bound: {path}")
    hasher, total, count = hashlib.sha256(), 0, 0
    with path.open("rb") as stream:
        while data := stream.readline(LINE_LIMIT + 1) if rows is not None else stream.read(1 << 20):
            total += len(data)
            require(total <= limit, f"file grew beyond bound: {path}")
            if rows is not None:
                require(len(data) <= LINE_LIMIT and data.endswith(b"\n"), f"oversized/incomplete decision row: {path}")
                count += 1
                require(count <= rows, f"excess original decision rows: {path}")
            hasher.update(data)
    value = hasher.hexdigest()
    require(total == size and (rows is None or count == rows), f"file changed or row count differs: {path}")
    require(expected is None or value == expected, f"SHA256 mismatch: {path}")
    return {"path": str(path), "sha256": value, "bytes": total}


def command(arguments):
    # Fixed provenance commands never execute the replay binary. Disk-backed
    # capture avoids retaining arbitrary command output in process memory.
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(arguments, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
                                check=False, timeout=30, shell=False)
        require(output.tell() <= 1 << 20, "provenance command output exceeds 1 MiB")
        output.seek(0)
        text = output.read().decode("utf-8", errors="strict")
    require(result.returncode == 0, f"provenance command failed: {arguments[0]}")
    return text


def validate_checkout(branch, revision, status):
    require(branch == "call-correctionv2", "freeze requires branch call-correctionv2")
    require(re.fullmatch("[0-9a-f]{40}", revision) is not None, "HEAD must be lowercase 40-hex")
    require(status == "", "freeze requires a clean tracked and untracked worktree; no dirty bypass")
    return revision


def checkout():
    require(Path(command(["git", "rev-parse", "--show-toplevel"]).strip()).resolve() == ROOT,
            "Git checkout differs from script root")
    return validate_checkout(command(["git", "branch", "--show-current"]).strip(),
                             command(["git", "rev-parse", "HEAD"]).strip(),
                             command(["git", "status", "--porcelain=v1", "--untracked-files=all"]))


def validate_build_info(text, revision):
    settings, module = {}, ""
    for line in text.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] == "path":
            require(not module, "duplicate executable module path")
            module = fields[1]
        if len(fields) == 2 and fields[0] == "build" and "=" in fields[1]:
            key, value = fields[1].split("=", 1)
            require(key not in settings, f"duplicate build setting: {key}")
            settings[key] = value
    require(module == "dxcluster/cmd/rbn_replay", "executable is not rbn_replay")
    require(settings.get("vcs") == "git" and settings.get("vcs.revision") == revision
            and settings.get("vcs.modified") == "false", "executable must be unmodified exact HEAD")
    require(settings.get("GOOS") == "windows" and settings.get("-buildmode") == "exe", "native Windows executable required")
    return settings


def method_grid():
    return [{"id": f"f{gap}_t{seconds}_{ranking}_d{len(distances)}", "gap_hz": gap,
             "max_witness_seconds": seconds, "ranking": ranking, "cw_distances": distances}
            for gap in (150, 250) for seconds in (60, 120)
            for ranking in ("geometry", "ordinal") for distances in ([1], [1, 2])]


def verify_template(plan):
    require(plan["schema_version"] == 2 and plan["frozen"] is True
            and plan["holdout_content_uninspected"] is True and plan["reference_version"] == REFERENCE,
            "wrong frozen exposed template")
    require(len(plan["candidates"]) == 1 and plan["selected_candidate"] == ORDINARY
            and plan["candidates"][0]["id"] == ORDINARY, "ordinary candidate changed")
    # Native hashAnyStable embeds parameters as RawMessage: their key order
    # must survive cloning even though the numeric parameter values agree.
    require(digest(encode(plan["candidates"][0], sort_keys=False).rstrip(b"\n")) == ORDINARY_SHA,
            "ordinary candidate byte fingerprint changed")
    require([entry["date_utc"].replace("-", "") for entry in plan["inputs"]] == [entry[0] for entry in ORIGINALS], "exposed date inventory changed")
    require([arm["name"] for arm in plan["arms"]] == ["shipped", "repaired_off", "repaired_on"], "arm inventory changed")
    verified = {}
    for arm in plan["arms"]:
        require(len(arm["config_files"]) == 20 and len(arm["assets"]) == 10, "template manifest dimensions changed")
        directory = safe_path(arm["config_dir"])
        with os.scandir(directory) as entries:
            names = set()
            for entry in entries:
                require(len(names) < 20 and entry.is_file(follow_symlinks=False), "configuration inventory changed")
                names.add(entry.name)
        require(names == set(arm["config_files"]), "configuration file set differs from template")
        references = [{"path": str(directory / name), "sha256": value}
                      for name, value in arm["config_files"].items()] + arm["assets"]
        for reference in references:
            path = safe_path(reference["path"])
            previous = verified.get(str(path))
            require(previous is None or previous["sha256"] == reference["sha256"], "conflicting template hashes")
            if previous is None:
                verified[str(path)] = hash_file(path, reference["sha256"])
    for entry in plan["inputs"]:
        require(entry["max_rows"] == 8_000_000 and entry["max_csv_bytes"] == 512 << 20, "archive limits changed")
        reference = entry["archive"]
        path = safe_path(reference["path"])
        verified[str(path)] = hash_file(path, reference["sha256"], ARCHIVE_LIMIT)
    reference = plan["receiver_metadata"]
    path = safe_path(reference["path"])
    verified[str(path)] = hash_file(path, reference["sha256"], METADATA_LIMIT)
    return list(verified.values())


def verify_originals(plan):
    arm = next(arm for arm in plan["arms"] if arm["name"] == "repaired_off")
    originals = []
    for date, rows, size, decisions_sha, complete_sha in ORIGINALS:
        directory = EVIDENCE / "v6-e1-exposed-captures-v1" / date
        metadata_path = safe_path(directory / "complete.json")
        data = read_bounded(metadata_path)
        require(digest(data) == complete_sha, f"authoritative original metadata changed: {date}")
        report = decode(data)
        expected = {"schema_version": 2, "complete": True, "plan_sha256": TEMPLATE_SHA,
                    "reference_version": REFERENCE, "arm": "repaired_off", "candidate": ORDINARY,
                    "date_utc": f"{date[:4]}-{date[4:6]}-{date[6:]}", "phase": "tuning",
                    "source_revision": arm["source_revision"], "executable_sha256": arm["executable_sha256"],
                    "focal_rows": rows, "terminal_rows": rows, "max_rows": 8_000_000,
                    "max_csv_bytes": 512 << 20, "warmup_seconds": plan["warmup_seconds"]}
        require(all(type(report.get(key)) is type(value) and report[key] == value for key, value in expected.items()), f"original metadata mismatch: {date}")
        path = safe_path(directory / "decisions.jsonl")
        require(safe_path(report["decisions"]["path"]) == path and report["decisions"]["sha256"] == decisions_sha,
                "original metadata points at another decision stream")
        stream = hash_file(path, decisions_sha, 2 << 30, rows=rows, exact_bytes=size)
        originals.append({"date_utc": report["date_utc"], "rows": rows, "decisions": stream,
                          "complete": {"path": str(metadata_path), "sha256": complete_sha, "bytes": len(data)}})
        print(json.dumps({"verified_original_date": date, "rows": rows}), file=sys.stderr, flush=True)
    return originals


def plans(template, revision, executable_sha, timestamp):
    capture = copy.deepcopy(template)
    capture["frozen_at_utc"] = capture["selected_at_utc"] = timestamp
    capture["tuning_through_utc"] = "2026-11-16T23:59:59Z"
    for arm in capture["arms"]:
        if arm["name"] in ("repaired_off", "repaired_on"):
            arm["source_revision"], arm["executable_sha256"] = revision, executable_sha
    for entry in capture["inputs"]:
        entry["phase"] = "tuning"
    method = {"schema_version": 1, "experimental": True, "enabled": True,
              "source_revision": revision, "reference_version": REFERENCE,
              "model": {"path": str(MODEL), "sha256": MODEL_SHA},
              "selected_candidate": "", "candidates": method_grid()}
    disabled = copy.deepcopy(method)
    disabled["enabled"] = False
    return {"capture-plan.json": capture, "enabled-method-plan.json": method,
            "disabled-method-plan.json": disabled}


def file_ref(path, data):
    return {"path": str(path), "sha256": digest(data), "bytes": len(data)}


def run_list(out, executable, plan_refs, originals):
    runs = []
    families = [("control", "disabled-method-plan.json", method_grid()[0]["id"])]
    families += [(entry["id"], "enabled-method-plan.json", entry["id"]) for entry in method_grid()]
    for family, method_name, method_id in families:
        for original in originals:
            date = original["date_utc"]
            destination = out / "captures" / family / date.replace("-", "")
            capture, method = plan_refs["capture-plan.json"], plan_refs[method_name]
            arguments = [str(executable), "-study-plan", capture["path"], "-study-freeze", capture["sha256"],
                         "-study-arm", "repaired_off", "-study-candidate", ORDINARY, "-study-phase", "tuning",
                         "-date", date, "-study-out", str(destination), "-study-method-plan", method["path"],
                         "-study-method-freeze", method["sha256"], "-study-method-candidate", method_id]
            runs.append({"role": "control" if family == "control" else "candidate", "date_utc": date,
                         "id": family, "method_candidate_id": method_id, "argv": arguments,
                         "output": str(destination), "control_original_binding": original})
    require(len(runs) == 68 and len({entry["output"] for entry in runs}) == 68, "run inventory is not complete/distinct")
    return {"schema_version": 1, "experimental": True, "phase": "tuning", "runs": runs}


def write_verified(path, data):
    with path.open("xb") as stream:
        require(stream.write(data) == len(data), "short metadata write")
        stream.flush()
        os.fsync(stream.fileno())
    require(read_bounded(path) == data, "metadata readback differs")


def publish(path, data):
    pending, published = Path(str(path) + ".pending"), False
    try:
        write_verified(pending, data)
        require(not path.exists(), "frozen metadata already exists")
        os.rename(pending, path)
        published = True
        require(read_bounded(path) == data, "published metadata readback differs")
    except BaseException:
        if published:
            path.unlink()
        raise


def prepare(executable, out):
    revision = checkout()  # Refuse dirty/new-code runs before touching any inputs/output.
    out = safe_path(out, EVIDENCE)
    require(out.parent == EVIDENCE.resolve() and not out.exists(), "output must be a new direct child of evidence root")
    executable = Path(executable).absolute()
    require(executable.suffix.lower() == ".exe" and not executable.is_symlink() and not executable.is_junction(), "native executable .exe required")
    executable = executable.resolve(strict=True)
    executable_ref = hash_file(executable)
    build_text = command(["go", "version", "-m", str(executable)])
    settings = validate_build_info(build_text, revision)
    template_bytes = read_bounded(TEMPLATE)
    require(digest(template_bytes) == TEMPLATE_SHA, "exposed template hash changed")
    template = decode(template_bytes)
    verified = verify_template(template)
    model_ref = hash_file(safe_path(MODEL), MODEL_SHA, METADATA_LIMIT)
    originals = verify_originals(template)
    require(checkout() == revision and hash_file(executable) == executable_ref, "checkout/executable changed during verification")
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    values = plans(template, revision, executable_ref["sha256"], timestamp)
    encoded = {name: encode(value, sort_keys=name != "capture-plan.json") for name, value in values.items()}
    require(digest(encode(decode(encoded["capture-plan.json"])["candidates"][0], sort_keys=False).rstrip(b"\n")) == ORDINARY_SHA,
            "capture serialization changed the ordinary candidate")
    references = {name: file_ref(out / name, data) for name, data in encoded.items()}
    runs = run_list(out, executable, references, originals)
    encoded["runs.json"] = encode(runs)
    references["runs.json"] = file_ref(out / "runs.json", encoded["runs.json"])
    freeze = {"schema_version": 1, "experimental": True, "complete": True, "phase": "tuning",
              "exposed": True, "never_production_qualified": True, "frozen_at_utc": timestamp,
              "source_revision": revision, "branch": "call-correctionv2", "executable": executable_ref,
              "go_build_settings": settings, "go_build_info_sha256": digest(build_text.encode()),
              "script": hash_file(Path(__file__).resolve(), limit=METADATA_LIMIT), "model": model_ref,
              "template": file_ref(TEMPLATE, template_bytes), "files": references,
              "verified_template_files": verified, "originals": originals, "run_count": 68,
              "selected_method_candidate": "", "ordinary_candidate": ORDINARY,
              "ordinary_previously_selected_at_utc": template["selected_at_utc"],
              "claim_limits": ["Four exposed UTC dates only; no untouched confirmation or production qualification.",
                               "No predictions executed and no method winner selected.",
                               "Original controls are bound to fixed ledger hashes; new disabled parity remains to be verified.",
                               "No prior equivalence proof is used as a substitute for full original stream verification."]}
    freeze_bytes = encode(freeze)
    require(sum(map(len, encoded.values())) + len(freeze_bytes) <= METADATA_LIMIT, "total frozen metadata exceeds 4 MiB")
    out.mkdir()
    (out / "captures").mkdir()
    for family in ("control", *(entry["id"] for entry in method_grid())):
        (out / "captures" / family).mkdir()
    for name, data in encoded.items():
        publish(out / name, data)
    require(checkout() == revision, "checkout changed before final FREEZE publication")
    for name, data in encoded.items():
        require(read_bounded(out / name) == data, "frozen metadata changed before FREEZE")
    publish(out / "FREEZE.json", freeze_bytes)
    return file_ref(out / "FREEZE.json", freeze_bytes)


def selfcheck():
    candidates = method_grid()
    require(len(candidates) == 16 and len({entry["id"] for entry in candidates}) == 16, "grid literal cardinality")
    require(candidates[0]["id"] == "f150_t60_geometry_d1" and candidates[-1]["id"] == "f250_t120_ordinal_d2", "grid endpoint/order")
    expected_ids = ("f150_t60_geometry_d1", "f150_t60_geometry_d2", "f150_t60_ordinal_d1", "f150_t60_ordinal_d2",
                    "f150_t120_geometry_d1", "f150_t120_geometry_d2", "f150_t120_ordinal_d1", "f150_t120_ordinal_d2",
                    "f250_t60_geometry_d1", "f250_t60_geometry_d2", "f250_t60_ordinal_d1", "f250_t60_ordinal_d2",
                    "f250_t120_geometry_d1", "f250_t120_geometry_d2", "f250_t120_ordinal_d1", "f250_t120_ordinal_d2")
    require(tuple(entry["id"] for entry in candidates) == expected_ids, "literal full grid differs")
    require([entry[0] for entry in ORIGINALS] == ["20261107", "20261114", "20261115", "20261116"], "date inventory")
    require(sum(entry[1] for entry in ORIGINALS) == 2364930, "original literal row total")
    template = {"selected_at_utc": "old", "frozen_at_utc": "old", "candidates": [{"parameters": {"Z": 1, "A": 2}}],
                "arms": [{"name": "shipped", "source_revision": "old", "executable_sha256": "old"},
                         {"name": "repaired_off"}, {"name": "repaired_on"}], "inputs": [{"phase": "holdout"}],
                "asset_training_through_utc": "2026-11-14T23:59:59Z", "receiver_metadata_through_utc": "2026-11-14T23:59:59Z"}
    snapshot = copy.deepcopy(template)
    cloned = plans(template, "a" * 40, "b" * 64, "2026-11-18T12:00:00Z")
    capture = cloned["capture-plan.json"]
    require(template == snapshot and capture["arms"][0] == snapshot["arms"][0]
            and capture["candidates"] == snapshot["candidates"] and b'"Z":1,"A":2' in encode(capture, sort_keys=False), "template/parameter order mutated")
    require(cloned["enabled-method-plan.json"]["enabled"] is True and cloned["disabled-method-plan.json"]["enabled"] is False
            and cloned["enabled-method-plan.json"]["selected_candidate"] == "", "method enable/selection policy changed")
    rejected = []
    def refuses(name, action):
        try:
            action()
        except (OSError, ValueError):
            rejected.append(name)
            return
        raise ValueError(f"selfcheck unexpectedly accepted: {name}")
    for branch, revision, status in (("main", "a" * 40, ""), ("call-correctionv2", "A" * 40, ""),
                                     ("call-correctionv2", "a" * 40, " M tracked\n"),
                                     ("call-correctionv2", "a" * 40, "?? untracked\n")):
        refuses("checkout_precondition", lambda b=branch, r=revision, s=status: validate_checkout(b, r, s))
    info = "path dxcluster/cmd/rbn_replay\nbuild vcs=git\nbuild vcs.revision=" + "a" * 40 + "\nbuild vcs.modified=false\nbuild GOOS=windows\nbuild -buildmode=exe\n"
    validate_build_info(info, "a" * 40)
    refuses("modified_binary", lambda: validate_build_info(info.replace("modified=false", "modified=true"), "a" * 40))
    refuses("wrong_revision", lambda: validate_build_info(info, "b" * 40))
    refuses("missing_provenance", lambda: validate_build_info("path dxcluster/cmd/rbn_replay", "a" * 40))
    with tempfile.TemporaryDirectory(prefix="ccv2-v7-prepare-selfcheck-") as private:
        base, payload = Path(private), b'{"selfcheck_only":true}\n'
        original = base / "original.jsonl"
        original.write_bytes(b"a\nb\n")
        require(hash_file(original, digest(b"a\nb\n"), 4, rows=2, exact_bytes=4)["bytes"] == 4,
                "full original stream positive control")
        refuses("original_wrong_rows", lambda: hash_file(original, limit=4, rows=1))
        refuses("original_wrong_hash", lambda: hash_file(original, "f" * 64, 4, rows=2))
        original.write_bytes(b"a\nb")
        refuses("original_truncated_line", lambda: hash_file(original, limit=4, rows=2))
        original.write_bytes(b"a" * LINE_LIMIT + b"\n")
        refuses("original_oversized_line", lambda: hash_file(original, limit=LINE_LIMIT + 1, rows=1))
        refuses("metadata_total_bound", lambda: encode({"x": "a" * METADATA_LIMIT}))
        target = base / "short"
        class ShortWriter:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def write(self, data): return len(data) - 1
        with mock.patch.object(Path, "open", return_value=ShortWriter()):
            refuses("short_write", lambda: write_verified(target, payload))
        original_read, original_open = read_bounded, Path.open
        class FailingWriter:
            def __init__(self, stream, stage): self.stream, self.stage = stream, stage
            def __enter__(self): return self
            def __exit__(self, *args):
                self.stream.close()
                if self.stage == "close": raise OSError("injected close")
            def write(self, data): return self.stream.write(data)
            def flush(self):
                if self.stage == "flush": raise OSError("injected flush")
                self.stream.flush()
            def fileno(self): return self.stream.fileno()
        for stage in ("flush", "fsync", "close", "pending_readback", "rename", "final_readback"):
            target = base / stage
            if stage in ("flush", "close"):
                def faulty_open(path, mode, failure=stage):
                    stream = original_open(path, mode)
                    return FailingWriter(stream, failure) if mode == "xb" else stream
                patch = mock.patch.object(Path, "open", new=faulty_open)
            elif stage == "fsync":
                patch = mock.patch.object(os, "fsync", side_effect=OSError("injected fsync"))
            elif stage == "rename":
                patch = mock.patch.object(os, "rename", side_effect=OSError("injected rename"))
            else:
                def corrupt(path, final=stage == "final_readback", destination=target):
                    return b"wrong" if (path == destination if final else str(path).endswith(".pending")) else original_read(path)
                patch = mock.patch.dict(globals(), {"read_bounded": corrupt})
            with patch:
                refuses(stage, lambda p=target: publish(p, payload))
            require(not target.exists(), "publication fault retained final authority")
        publish(base / "success", payload)
        require((base / "success").read_bytes() == payload, "publication positive control")
        fake_refs = {name: file_ref(base / name, payload) for name in ("capture-plan.json", "enabled-method-plan.json", "disabled-method-plan.json")}
        originals = [{"date_utc": f"{d[:4]}-{d[4:6]}-{d[6:]}", "rows": rows} for d, rows, _, _, _ in ORIGINALS]
        runs = run_list(base, base / "replay.exe", fake_refs, originals)["runs"]
        require([run["role"] for run in runs[:4]] == ["control"] * 4 and all(run["role"] == "candidate" for run in runs[4:]), "controls must precede active grid")
        require(all(not Path(run["output"]).exists() for run in runs), "date output directories were prematurely created")
    return {"grid_candidates": 16, "dates": 4, "runs": 68, "original_rows": 2364930,
            "refusal_checks": len(rejected), "publication_positive_control": True,
            "repository_or_archive_mutations": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selfcheck", action="store_true")
    parser.add_argument("--executable", type=Path)
    parser.add_argument("--out", type=Path, help="new direct child of .tmp/call-correctionv2")
    args = parser.parse_args()
    require((args.selfcheck and args.executable is None and args.out is None)
            or (not args.selfcheck and args.executable is not None and args.out is not None),
            "choose --selfcheck or both --executable and --out")
    result = selfcheck() if args.selfcheck else prepare(args.executable, args.out.resolve())
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"v7 preparation failed; no FREEZE authority: {error}", file=sys.stderr)
        sys.exit(1)
