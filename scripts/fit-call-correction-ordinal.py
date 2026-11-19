#!/usr/bin/env python3
"""Fit the approved v7 experimental CW ordinal event asset, never RF confidence.

Inputs are the complete hash-bound November 7/14 repaired-OFF receipts. The
fixed reference predicate and reference-to-subject suffix alignment are part of
the experiment contract, not inferred physical decoder events. Tables have a
fixed alphabet/bin universe; no vocabulary or alignment cache survives a row.
Only a final COMPLETE receipt authorizes consumption of the accompanying asset.
"""

import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / ".tmp/call-correctionv2"
ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/"
INDEX = {char: i for i, char in enumerate(ALPHABET)}
EDGES = [-999, 0, 6, 12, 18, 24, 999]
COUNT_BOUND = 528_000_000
FILE_BOUND = 4 << 20
LINE_BOUND = 64 << 10
REFERENCE = "ccv2-reference-v1"
SOURCES = (
    ("20261107", 458495, 53862,
     "2a78ec300acc2cfe29048201af4e971cad933db01c6f36b567ac7443b57ccd34"),
    ("20261114", 882021, 84468,
     "ad0c8f5a942bab03da052cbe91c2087e59ff566cbd48aa2be8be43b1140bff49"),
)
CENSUSES = (
    ("v5-e2-snr-opportunities-v1.json",
     "ec3161fd2d6b8292edeb5326b7dd046f6a91d814ed06cbea8a819cb0f1f7028e"),
    ("v5-e2-character-opportunities-by-snr-v1.json",
     "fd027ddb383c63007141bb3dd14c5ce2dea18a4948557e78452aa0daf3053dda"),
)
SCALARS = ("pairs", "exact_pairs", "matches", "substitutions", "deletions",
           "insertions", "source_characters", "subject_characters", "gap_stops")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=unique_object,
                      parse_constant=lambda value: require(False, f"nonfinite JSON: {value}"))


def bound_read(path):
    require(not path.is_symlink(), f"symlink input: {path}")
    with path.open("rb") as stream:
        data = stream.read(FILE_BOUND + 1)
    require(len(data) <= FILE_BOUND, f"oversized metadata: {path}")
    return data


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def snr_bin(present, value):
    require(type(present) is bool, "SNR presence must be boolean")
    if not present:
        return "unmeasured_focal"
    require(type(value) is int, "measured SNR must be an integer")
    return next((str(i) for i in range(6) if EDGES[i] < value <= EDGES[i + 1]),
                "out_of_asset_domain")


def alignment(reference, subject):
    """One unit-cost suffix path, forward precedence M, S, D, I on ties."""
    m, n = len(reference), len(subject)
    require(m <= 128 and n <= 128, "census alignment exceeds bounded domain")
    if reference == subject:
        return [("M", char, char, i) for i, char in enumerate(reference)]
    costs = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        costs[i][n] = m - i
    for j in range(n + 1):
        costs[m][j] = n - j
    for i in range(m - 1, -1, -1):
        for j in range(n - 1, -1, -1):
            costs[i][j] = min(costs[i + 1][j + 1] + (reference[i] != subject[j]),
                              1 + costs[i + 1][j], 1 + costs[i][j + 1])
    result = []
    i = j = 0
    while i < m or j < n:
        if i < m and j < n and reference[i] == subject[j] and costs[i][j] == costs[i + 1][j + 1]:
            op, source, observed = "M", reference[i], subject[j]
        elif i < m and j < n and costs[i][j] == 1 + costs[i + 1][j + 1]:
            op, source, observed = "S", reference[i], subject[j]
        elif i < m and costs[i][j] == 1 + costs[i + 1][j]:
            op, source, observed = "D", reference[i], ""
        else:
            require(j < n and costs[i][j] == 1 + costs[i][j + 1], "invalid suffix walk")
            op, source, observed = "I", "", subject[j]
        result.append((op, source, observed, i))
        i += op != "I"
        j += op != "D"
    require(sum(op != "M" for op, _, _, _ in result) == costs[0][0], "path cost mismatch")
    return result


def empty_cell():
    return {**dict.fromkeys(SCALARS, 0), "opportunities": [0] * 37,
            "events": [[0] * 38 for _ in range(37)], "insertion_symbols": [0] * 37}


def add_pair(cell, reference, subject, path):
    cell["pairs"] += 1
    cell["exact_pairs"] += reference == subject
    cell["source_characters"] += len(reference)
    cell["subject_characters"] += len(subject)
    cell["gap_stops"] += len(reference) + 1
    # Opportunities are counted directly, independently of the alignment events.
    for char in reference:
        cell["opportunities"][INDEX[char]] += 1
    for op, source, observed, _ in path:
        cell[{"M": "matches", "S": "substitutions", "D": "deletions", "I": "insertions"}[op]] += 1
        if op == "I":
            cell["insertion_symbols"][INDEX[observed]] += 1
        else:
            cell["events"][INDEX[source]][37 if op == "D" else INDEX[observed]] += 1


def merge_cells(cells):
    merged = empty_cell()
    for cell in cells:
        for key in SCALARS:
            merged[key] += cell[key]
        for i in range(37):
            merged["opportunities"][i] += cell["opportunities"][i]
            merged["insertion_symbols"][i] += cell["insertion_symbols"][i]
            for j in range(38):
                merged["events"][i][j] += cell["events"][i][j]
    return merged


def check_counts(value):
    if isinstance(value, dict):
        for item in value.values():
            check_counts(item)
    elif isinstance(value, list):
        for item in value:
            check_counts(item)
    else:
        require(type(value) is int and 0 <= value <= COUNT_BOUND, "count exceeds fixed domain")


def validate_cell(cell, *, training=True):
    check_counts(cell)
    require(len(cell["opportunities"]) == len(cell["events"]) == len(cell["insertion_symbols"]) == 37,
            "invalid alphabet dimension")
    require(all(len(row) == 38 for row in cell["events"]), "invalid emission dimension")
    require(all(sum(row) == count for row, count in zip(cell["events"], cell["opportunities"])),
            "event row does not match source opportunities")
    m, s, d, ins = (cell[key] for key in ("matches", "substitutions", "deletions", "insertions"))
    require(m == sum(cell["events"][i][i] for i in range(37)), "match diagonal mismatch")
    require(d == sum(row[37] for row in cell["events"]), "deletion column mismatch")
    require(s == sum(sum(row[:37]) - row[i] for i, row in enumerate(cell["events"])), "substitution mismatch")
    require(cell["source_characters"] == m + s + d == sum(cell["opportunities"]), "source total mismatch")
    require(cell["subject_characters"] == m + s + ins, "subject total mismatch")
    require(cell["gap_stops"] == cell["source_characters"] + cell["pairs"], "gap stop mismatch")
    require(ins == sum(cell["insertion_symbols"]), "insertion symbol mismatch")
    require(cell["exact_pairs"] <= cell["pairs"], "exact pairs exceed pairs")
    if training:
        nonexact, edits = cell["pairs"] - cell["exact_pairs"], s + d + ins
        require(nonexact <= edits <= 32 * nonexact and cell["exact_pairs"] <= m,
                "nonempty bounded training pair/edit totals inconsistent")


def census_row(counts, row, reference=None, subject=None, path=None):
    present, snr = row["focal_snr_present"], row["raw"]["ReportDB"]
    a, b, frozen = row["r1"], row["r2"], row["r2_frozen"]
    counts.update({"focal_rows": 1, "focal_present": int(present),
                   "focal_zero": int(present and snr == 0), "focal_negative": int(present and snr < 0),
                   "output": int(row["status"] == "output"),
                   "identity_correction_output": int(row["status"] == "output" and row["identity_correction"]),
                   "r1_strong": int(a["strong"]), "r2_strong": int(b["strong"]),
                   "r2_frozen_strong": int(frozen["strong"]),
                   "decision_after_cutoff": int(row["decision_after_cutoff"]),
                   "r2_strong_lost_to_cutoff": int(frozen["strong"] and not b["strong"]),
                   "frozen_strong_agree": int(a["strong"] and frozen["strong"] and a["identity"] == frozen["identity"])})
    if not a["strong"] or not b["strong"]:
        counts["not_both_strong"] += 1
        return
    if a["identity"] != b["identity"]:
        counts["strong_discordant"] += 1
        return
    require(reference and subject and path is not None, "accepted pair missing canonical strings")
    ops = collections.Counter(op for op, _, _, _ in path)
    distance = ops["S"] + ops["D"] + ops["I"]
    counts.update(ops)
    counts.update({"strong_agree": 1, "canonical_raw_diff": int(row["raw"]["DXCall"] != subject),
                   "canonical_alias_only": int(row["raw"]["DXCall"] != subject and subject == reference),
                   "reference_characters": len(reference), "subject_characters": len(subject),
                   "reference_chars_outside_asset_alphabet": sum(c not in INDEX for c in reference),
                   "subject_chars_outside_asset_alphabet": sum(c not in INDEX for c in subject),
                   "available_reference_gap_positions": len(reference) + 1,
                   "gaps_with_insertions_under_fixed_alignment": len({gap for op, _, _, gap in path if op == "I"}),
                   "canonical_full_match": int(distance == 0), "plain_distance_1": int(distance == 1),
                   "plain_distance_2": int(distance == 2), "plain_distance_3_plus": int(distance >= 3)})
    if row["status"] == "output":
        counts[f"output_transition_{int(subject == reference)}{int(row['final_identity'] == reference)}"] += 1
    else:
        counts["strong_agree_non_output"] += 1
    if distance in (1, 2):
        attained = row["status"] == "output" and row["identity_correction"] and row["final_identity"] == reference
        counts["near_attained"] += int(attained)
        if not attained:
            counts["near_missed"] += 1
            proposed = row.get("proposed_identity", "")
            category = "selected_reference" if proposed == reference else "selected_other" if proposed else "no_selected_identity"
            counts[f"near_missed_{category}"] += 1


def character_counts(reference, subject):
    return {"pairs": 1, "full_matches": int(reference == subject),
            "reference_characters": len(reference), "subject_characters": len(subject),
            "reference_gap_opportunities": len(reference) + 1,
            "unchanged_call_characters": len(reference) if reference == subject else 0,
            "reference_over_32": int(len(reference) > 32), "subject_over_32": int(len(subject) > 32),
            "reference_unsupported_characters": sum(c not in INDEX for c in reference),
            "subject_unsupported_characters": sum(c not in INDEX for c in subject)}


def process_source(spec, census, characters, character_symbols):
    date, expected_rows, expected_pairs, expected_sha = spec
    directory = EVIDENCE / "study-v2" / f"cw2_r450_a100_d0-{date}-repaired_off"
    metadata_bytes = bound_read(directory / "complete.json")
    metadata = decode(metadata_bytes)
    require(metadata["complete"] is True and metadata["arm"] == "repaired_off"
            and metadata["phase"] == "tuning" and metadata["reference_version"] == REFERENCE
            and metadata["date_utc"] == f"{date[:4]}-{date[4:6]}-{date[6:]}", "wrong source metadata")
    require(metadata["decisions"]["sha256"] == expected_sha and metadata["focal_rows"] == expected_rows,
            "source completion binding mismatch")
    path = directory / "decisions.jsonl"
    require(not path.is_symlink() and Path(metadata["decisions"]["path"]).resolve() == path.resolve(), "source path mismatch")
    tables = [empty_cell() for _ in range(6)]
    accounting = collections.Counter(dict.fromkeys((
        "accepted_pairs", "fitted_cw_pairs", "unsupported_alphabet", "overlength",
        "unsupported_mode", "unmeasured_focal", "out_of_asset_domain",
        "unsupported_alphabet_pairs", "overlength_pairs", "missing_focal_rows",
        "outside_focal_rows", "measured_zero_focal_rows", "measured_zero_accepted_pairs"), 0))
    digest = hashlib.sha256()
    rows = 0
    with path.open("rb") as stream:
        while line := stream.readline(LINE_BOUND + 1):
            require(len(line) <= LINE_BOUND and line.endswith(b"\n"), "truncated/oversized source line")
            digest.update(line)
            rows += 1
            require(rows <= expected_rows, "excess source rows")
            row = decode(line)
            mode = row["raw"]["Mode"]
            require(mode in ("CW", "RTTY"), "unexposed source mode")
            bucket = snr_bin(row["focal_snr_present"], row["raw"]["ReportDB"])
            accounting["missing_focal_rows"] += bucket == "unmeasured_focal"
            accounting["outside_focal_rows"] += bucket == "out_of_asset_domain"
            measured_zero = row["focal_snr_present"] and row["raw"]["ReportDB"] == 0
            accounting["measured_zero_focal_rows"] += measured_zero
            key = (date, mode, bucket)
            a, b = row["r1"], row["r2"]
            require(type(a["strong"]) is bool and type(b["strong"]) is bool, "reference flags must be boolean")
            accepted = a["strong"] and b["strong"] and a["identity"] == b["identity"]
            reference = subject = aligned = None
            if accepted:
                reference, subject = a["identity"], row["subject_identity"]
                require(isinstance(reference, str) and reference and isinstance(subject, str) and subject,
                        "empty/malformed canonical pair")
                aligned = alignment(reference, subject)
                characters[key].update(character_counts(reference, subject))
                for side, value in (("reference", reference), ("subject", subject)):
                    character_symbols[(key, side)].update(char for char in value if char in INDEX)
                accounting["accepted_pairs"] += 1
                accounting["measured_zero_accepted_pairs"] += measured_zero
                unsupported = any(c not in INDEX for c in reference + subject)
                overlength = len(reference) > 32 or len(subject) > 32
                accounting["unsupported_alphabet_pairs"] += int(unsupported)
                accounting["overlength_pairs"] += int(overlength)
                reason = "unsupported_alphabet" if unsupported else "overlength" if overlength else "unsupported_mode" if mode != "CW" else bucket if not bucket.isdigit() else "fitted_cw_pairs"
                accounting[reason] += 1
                if reason == "fitted_cw_pairs":
                    add_pair(tables[int(bucket)], reference, subject, aligned)
            census_row(census[key], row, reference, subject, aligned)
            if rows % 100000 == 0:
                print(json.dumps({"date_utc": date, "rows_read": rows}), flush=True)
    require(rows == expected_rows and digest.hexdigest() == expected_sha, "source rows/SHA mismatch")
    require(accounting["accepted_pairs"] == expected_pairs, "accepted source pair mismatch")
    require(accounting["accepted_pairs"] == sum(accounting[key] for key in (
        "fitted_cw_pairs", "unsupported_alphabet", "overlength", "unsupported_mode",
        "unmeasured_focal", "out_of_asset_domain")), "accepted pair disposition mismatch")
    for table in tables:
        validate_cell(table)
    check_counts(dict(accounting))
    source = {"date_utc": date, "sha256": expected_sha, "focal_rows": rows,
              "accepted_pairs": accounting["accepted_pairs"], "fitted_cw_pairs": accounting["fitted_cw_pairs"]}
    return source, {"source": source, "complete_sha256": sha256(metadata_bytes),
                    "accounting": dict(accounting), "cells": tables}


def reconcile(census, characters, symbols, prior_counts, prior_characters):
    comparisons = 0
    require(set(census) == {(c["date_utc"], c["mode"], c["focal_snr_bin"]) for c in prior_counts["cells"]}, "census cell inventory mismatch")
    for previous in prior_counts["cells"]:
        key = (previous["date_utc"], previous["mode"], previous["focal_snr_bin"])
        current = census[key]
        for field in current.keys() | previous["counts"].keys():
            require(current[field] == previous["counts"].get(field, 0), f"event census mismatch {key}/{field}")
            comparisons += 1
    pooled = sum(census.values(), collections.Counter())
    require(pooled == collections.Counter(prior_counts["pooled"]), "pooled census mismatch")
    require(tuple(pooled[k] for k in ("focal_rows", "strong_agree", "canonical_full_match", "M", "S", "D", "I"))
            == (1340516, 138330, 137115, 672791, 2274, 582, 340), "frozen total mismatch")
    char_comparisons = 0
    for previous in prior_characters["cells"]:
        bucket = {"missing": "unmeasured_focal", "outside": "out_of_asset_domain"}.get(previous["focal_snr_bin"], previous["focal_snr_bin"])
        keys = [(date, previous["mode"], bucket) for date, _, _, _ in SOURCES]
        for field in character_counts("", ""):
            require(sum(characters[key][field] for key in keys) == previous[field], f"character scalar mismatch {keys}/{field}")
            char_comparisons += 1
        for side in ("reference", "subject"):
            for char in ALPHABET:
                require(sum(symbols[(key, side)][char] for key in keys) == previous[f"{side}_character_counts"][char], f"character opportunity mismatch {keys}/{side}/{char}")
                char_comparisons += 1
    return dict(pooled), comparisons, char_comparisons


def selfcheck():
    # Literal paths follow the approved tie rule, not a second DP implementation.
    cases = (("ABBA", "ABA", "MMDM"), ("AA", "A", "MD"), ("A", "AA", "MI"),
             ("AB", "BA", "SS"), ("AB", "AXB", "MIM"), ("AXB", "AB", "MDM"),
             ("A", "AXXX", "MIII"), ("AXXX", "A", "MDDD"), ("", "AB", "II"),
             ("AB", "", "DD"), ("", "", ""))
    for reference, subject, expected in cases:
        path = alignment(reference, subject)
        require("".join(op for op, _, _, _ in path) == expected, f"literal path failed: {reference}->{subject}")
        cell = empty_cell()
        add_pair(cell, reference, subject, path)
        validate_cell(cell, training=False)
    cell = empty_cell()
    add_pair(cell, "A", "AXXX", alignment("A", "AXXX"))
    require((cell["matches"], cell["insertions"], cell["deletions"], cell["gap_stops"]) == (1, 3, 0, 2), "asymmetric insertion counts")
    require(cell["opportunities"][INDEX["A"]] == 1 and cell["insertion_symbols"][INDEX["X"]] == 3, "asymmetric symbol direction")
    inputs = (-999, -998, 0, 1, 6, 7, 12, 13, 18, 19, 24, 25, 999, 1000)
    require([snr_bin(True, value) for value in inputs] == ["out_of_asset_domain", "0", "0", "1", "1", "2", "2", "3", "3", "4", "4", "5", "5", "out_of_asset_domain"], "literal SNR boundary classification")
    require(snr_bin(False, 0) == "unmeasured_focal" and snr_bin(True, 0) == "0", "missing versus zero")
    for value in (-1, COUNT_BOUND + 1, True, 1.0):
        try:
            check_counts(value)
        except ValueError:
            continue
        raise ValueError("invalid count accepted")
    invalid_cells = []
    for reference, subject in (("A", "A"), ("A" * 33, "B" * 33), ("", "")):
        cell = empty_cell()
        add_pair(cell, reference, subject, alignment(reference, subject))
        if reference == subject == "A":
            cell["exact_pairs"] = 0
        invalid_cells.append(cell)
    for cell in invalid_cells:
        try:
            validate_cell(cell)
        except ValueError:
            continue
        raise ValueError("inconsistent training cell accepted")
    return {"literal_alignment_cases": len(cases), "snr_boundary_cases": len(inputs) + 2,
            "invalid_count_cases": 4, "invalid_training_cell_cases": len(invalid_cells),
            "provenance": "hand-derived literal convention cases; not RF oracle"}


def encode(value):
    data = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    require(len(data) <= FILE_BOUND, "output exceeds 4 MiB")
    return data


def write_verified(path, data):
    with path.open("xb") as stream:
        require(stream.write(data) == len(data), f"short write: {path}")
        stream.flush()
        os.fsync(stream.fileno())
    require(bound_read(path) == data, f"output verification failed: {path}")
    return {"path": str(path), "sha256": sha256(data), "bytes": len(data)}


def publish_complete(path, data):
    """Keep final authority absent until pending content is durable and verified."""
    pending = Path(str(path) + ".pending")
    published = False
    try:
        write_verified(pending, data)
        require(not path.exists(), "completion destination already exists")
        os.rename(pending, path)
        published = True
        require(bound_read(path) == data, "published completion verification failed")
        return {"path": str(path), "sha256": sha256(data), "bytes": len(data)}
    except BaseException:
        if published:
            path.unlink()
        raise


def fit(prefix):
    require(prefix.parent.resolve() == EVIDENCE.resolve() and prefix.name.startswith("v7-ordinal-"), "output prefix outside approved evidence namespace")
    outputs = [Path(str(prefix) + suffix) for suffix in ("-model.json", "-receipt.json", "-COMPLETE.json")]
    require(all(not path.exists() for path in (*outputs, Path(str(outputs[2]) + ".pending"))), "frozen output already exists")
    prior = []
    for name, digest in CENSUSES:
        data = bound_read(EVIDENCE / name)
        require(sha256(data) == digest, f"census SHA mismatch: {name}")
        prior.append(decode(data))
    census, characters = collections.defaultdict(collections.Counter), collections.defaultdict(collections.Counter)
    symbols = collections.defaultdict(collections.Counter)
    sources, details = [], []
    prior_sources = {source["date_utc"]: source for source in prior[0]["sources"]}
    require(set(prior_sources) == {spec[0] for spec in SOURCES}, "census source inventory mismatch")
    for spec in SOURCES:
        source, detail = process_source(spec, census, characters, symbols)
        previous = prior_sources[source["date_utc"]]
        require(detail["complete_sha256"] == previous["complete_sha256"]
                and source["sha256"] == previous["decisions"]["sha256"]
                and source["focal_rows"] == previous["rows"], "frozen source completion metadata changed")
        sources.append(source)
        details.append(detail)
    pooled, comparisons, char_comparisons = reconcile(census, characters, symbols, *prior)
    cells = [merge_cells(detail["cells"][i] for detail in details) for i in range(6)]
    for cell in cells:
        validate_cell(cell)
    require(sum(cell["pairs"] for cell in cells) == sum(source["fitted_cw_pairs"] for source in sources), "fitted pair reconciliation")
    model = {"schema_version": 1, "experimental": True, "mode": "CW", "alphabet": ALPHABET,
             "alignment": "unit-suffix-MSDI-v1", "reference_version": REFERENCE, "snr_edges": EDGES,
             "emission_prior_total": 1, "insertion_prior_total": 1, "gap_prior_alpha": 1,
             "gap_prior_beta": 1, "sources": sources, "cells": cells}
    model_bytes = encode(model)
    receipt = {"schema_version": 1, "experimental": True, "authority": "Approved v7 F1",
               "script_sha256": sha256(Path(__file__).read_bytes()), "model_sha256": sha256(model_bytes),
               "reference_version": REFERENCE, "alignment": model["alignment"], "sources": details,
               "censuses": [{"name": name, "sha256": digest} for name, digest in CENSUSES],
               "pooled_all_mode_counts": pooled, "event_census_scalar_comparisons": comparisons,
               "character_census_scalar_comparisons": char_comparisons, "selfchecks": selfcheck(),
               "all_mode_cells": [{"date_utc": key[0], "mode": key[1], "focal_snr_bin": key[2],
                                   "counts": dict(census.get(key, {})), "character_counts": dict(characters[key]),
                                   "reference_character_counts": {char: symbols[(key, "reference")][char] for char in ALPHABET},
                                   "subject_character_counts": {char: symbols[(key, "subject")][char] for char in ALPHABET}}
                                  for key in sorted((date, mode, bucket) for date, _, _, _ in SOURCES
                                                    for mode in ("CW", "RTTY")
                                                    for bucket in (*map(str, range(6)), "unmeasured_focal", "out_of_asset_domain"))],
               "cw_cell_coverage": [{"bin": i, "source_symbols_below_50": [char for char, count in zip(ALPHABET, cell["opportunities"]) if count < 50],
                                     "gap_stops_below_50": cell["gap_stops"] < 50} for i, cell in enumerate(cells)],
               "claim_limits": ["Selected and repeated canonical proxy reports are dependent, not RF truth or calibrated uncertainty.",
                                "This fits event counts, not marginal string probabilities, Bayes factors, public confidence or correction gains.",
                                "CW-only; missing/outside SNR, unsupported alphabet and overlength pairs are separately accounted.",
                                "No November 17+ contents or outcome-selected 1107-case sample was read; no predictor ran."]}
    # All source, census, shape and bound checks finish before any output exists.
    receipt_bytes = encode(receipt)
    asset_record = write_verified(outputs[0], model_bytes)
    receipt_record = write_verified(outputs[1], receipt_bytes)
    complete = {"schema_version": 1, "experimental": True, "complete": True,
                "asset": asset_record, "receipt": receipt_record,
                "focal_rows": pooled["focal_rows"], "accepted_pairs": pooled["strong_agree"],
                "fitted_cw_pairs": sum(cell["pairs"] for cell in cells)}
    complete_record = publish_complete(outputs[2], encode(complete))
    print(json.dumps({**complete, "completion": complete_record}, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selfcheck", action="store_true", help="run literal cases without reading training data")
    parser.add_argument("--out-prefix", type=Path, help="new .tmp/call-correctionv2/v7-ordinal-* prefix")
    args = parser.parse_args()
    require(args.selfcheck != (args.out_prefix is not None), "choose exactly one of --selfcheck or --out-prefix")
    if args.selfcheck:
        print(json.dumps(selfcheck(), sort_keys=True))
    else:
        selfcheck()
        fit(args.out_prefix.resolve())


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"ordinal fit failed; no COMPLETE authority: {error}", file=sys.stderr)
        sys.exit(1)
