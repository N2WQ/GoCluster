#!/usr/bin/env python3
"""Run cumulative command-fix regressions against a freshly built local checkout."""

import sys

# Imported harness methods contain assertions. Refuse optimization before imports,
# tool invocation or connection; importing its guarded main() is insufficient.
if sys.flags.optimize:
    raise SystemExit("Command-fix tests require assertions; remove -O/PYTHONOPTIMIZE.")
sys.dont_write_bytecode = True

import argparse
import contextlib
import copy
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import time
from types import SimpleNamespace
import uuid

from command_fixture_support import (Child, ROOT, build, command, deny_record_replacement,
                                     plain_output, require, runner_lock, stage)

spec = importlib.util.spec_from_file_location("checkout_telnet_harness", ROOT / "scripts" / "test-telnet-commands.py")
harness = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = harness
spec.loader.exec_module(harness)


def qualified(results, required, wire_failures, cleanup_errors):
    require(bool(required), "no required cases executed")
    names = [row["case"] for row in results]
    require(names == required, "required cases missing, duplicated or reordered")
    require(all(row["result"] == "PASS" for row in results), "required semantic case failed")
    require(not wire_failures, "strict wire checks failed")
    require(not cleanup_errors, "cleanup failed")


def require_history_row(row):
    # The isolated fixture uses Spot's 78-column archive layout; it does not run
    # main's runtime-layout initializer. Frequency ends at column 25, DX starts
    # at 28, mode at 40 and the UTC tail is anchored.
    require(len(row) == 78 and all(32 <= ord(ch) <= 126 for ch in row) and
            re.fullmatch(r"DX de [A-Z0-9/#@-]+:\s+\d+\.\d{2}", row[:25]) and
            row[25:27] == "  " and re.fullmatch(r"[A-Z0-9/-]+ +", row[27:39]) and
            all(row[index] == " " for index in (63, 65, 70, 72)) and
            re.fullmatch(r"(?:[01]\d|2[0-3])[0-5]\dZ", row[-5:]),
            "history row violates the fixture column or UTC-tail contract")


def archive_rows(session, label):
    # Pipelining GET can interleave its server echo with a history row. Consume
    # the bounded history body first; never strip arbitrary echo pieces. These
    # zero/one-row queries do not exercise the ten-row automatic pause trailer.
    query = f"SHOW DX W6ABC 10 COMMENT {label}"
    rows, received, raw = [], 0, bytearray()
    try:
        session.send(query + "\r\n")
        while True:
            line = session.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                    "history reply exceeds its bound or has invalid CRLF")
            text = line[:-2].decode("ascii", "strict")
            if text in ("End of retained history search.", "No matching retained spots."):
                require(bool(rows) == (text == "End of retained history search."),
                        "history footer disagrees with returned rows")
                break
            require("Search work limit reached" not in text and "unreadable archive" not in text,
                    "archive selection incomplete")
            if text.startswith("DX de"):
                require_history_row(text)
                rows.append(text)
            else:
                require(text in ("", query.upper()), "unexpected or interleaved history output")
    except Exception:
        # line() leaves an unreturned decoded suffix on EOF/timeout/buffer
        # refusal. Preserve it within the same receipt bound before rethrowing.
        pending = session.reader.buffer
        received += len(pending)
        raw.extend(pending[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{session.call}] {query}\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[history receipt truncated at 65536 bytes]\n"
        session.suite.log.write(receipt)
        session.suite.log.flush()
    session.get()  # Mandatory strict resource/schema/request-ID validation.
    return rows


def require_archive(session, label):
    deadline = time.monotonic() + 5
    while True:
        rows = archive_rows(session, label)
        if rows:
            require(len(rows) == 1 and label in rows[0] and
                    re.search(r"\b14074\.0+\s+W6ABC\s+CW\b", rows[0]), f"wrong archive row: {rows}")
            return
        require(time.monotonic() < deadline, "posted spot absent from archive")
        time.sleep(.025)


def require_reputation_archive_row(rows, label, frequency):
    require(len(rows) == 1 and label in rows[0] and
            re.search(r"^DX de K1ABC:\s+" + re.escape(str(frequency)) +
                      r"\.0+\s+W6ABC\s+CW\b", rows[0]),
            f"wrong baseline-station archive row: {rows}")


def require_reputation_archive(session, label, frequency):
    deadline = time.monotonic() + 5
    while True:
        rows = archive_rows(session, label)
        if rows:
            require_reputation_archive_row(rows, label, frequency)
            return
        require(time.monotonic() < deadline, "eligible reputation spot absent from archive")
        time.sleep(.025)


def require_archive_absent(rows):
    require(not rows, f"reputation-refused spot reached the archive: {rows}")


def require_reputation_refusal(text):
    require(text.strip() == "Spot rejected.", "manual DX rejection feedback is not generic")


def require_frequency_refusal(text):
    require(text.strip() == "Invalid frequency. Use a kHz value like 7001.0.",
            "manual DX frequency refusal is missing or incorrect")


def require_mode_rules(document, expected, record=None):
    require(document["configuration"]["filters"]["modes"] == expected,
            "MODE delta lost its exact open/restrictive state")
    if record is not None:
        saved = stored_document(record.read_bytes())
        require(dict(allow_all=saved["allmodes"], block_all=saved["blockallmodes"],
                     allow=saved["modes"], block=saved["blockmodes"]) == expected,
                "MODE delta was not saved before disconnect")


def require_mode_archive(session, label, mode, visible):
    rows = archive_rows(session, label)
    if not visible:
        require(not rows, "blocked MODE control remained visible in history")
        return
    require(len(rows) == 1 and label in rows[0] and
            re.search(r"\bW6ABC\s+" + re.escape(mode) + r"\b", rows[0]),
            "visible MODE control absent or misclassified in history")


def literal_archive_label(suite, suffix):
    # Hex tokens may contain a number followed by DB, which the production spot
    # parser recognizes as an SNR report. Keep control identity out of that
    # grammar and short enough for the fixed-width history comment.
    token = "".join(chr(ord("A") + int(ch, 16)) for ch in suite.token)
    return "T" + token + suffix


def require_grid_configuration(document, grid, record):
    require(document["configuration"]["settings"]["grid"] == grid and
            document["status"]["effective"]["grid"] == grid and
            stored_document(record.read_bytes())["grid"] == grid,
            "GRID canonical selection, effective location or saved bytes disagree")


def require_callsign_rules(document, expected, record):
    saved = stored_document(record.read_bytes())
    names = dict(dx_callsigns="callsigns", block_dx_callsigns="block_callsigns",
                 de_callsigns="decallsigns", block_de_callsigns="block_decallsigns")
    for key, stored_key in names.items():
        require(document["configuration"]["filters"][key] == expected[key] and
                (saved.get(stored_key) or []) == expected[key],
                "callsign canonical selection or saved patterns disagree")


def require_callsign_archive(session, label, visible):
    rows = archive_rows(session, label)
    if not visible:
        require(not rows, "blocked callsign control remained visible in history")
        return
    require(len(rows) == 1 and label in rows[0] and
            re.search(r"^DX de K1ABC:\s+14074\.0+\s+W6ABC\s+CW\b", rows[0]) and
            rows[0][39:63].rstrip() == "CW " + label,
            "visible callsign control absent or has the wrong DX/DE identity")
    require_history_row(rows[0])


def exact_human_reply(session, command, reply):
    # Receive the known complete refusal before GET can echo into its text.
    expected, index, received, raw = reply.splitlines(), 0, 0, bytearray()
    require(bool(expected), "empty human reply oracle")
    try:
        session.send(command + "\r\n")
        while index < len(expected):
            line = session.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                    "human reply exceeds its bound or has invalid CRLF")
            text = line[:-2].decode("ascii", "strict")
            if index == 0 and text in ("", command.upper()):
                continue
            require(text == expected[index], "human reply differs from the exact refusal")
            index += 1
    except Exception:
        pending = session.reader.buffer
        received += len(pending)
        raw.extend(pending[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{session.call}] {command}\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[human receipt truncated at 65536 bytes]\n"
        session.suite.log.write(receipt)
        session.suite.log.flush()
    return reply, session.get()


def callsign_pattern_cases(suite, root, cw_label, saved_callsigns):
    for dialect, call in (("go", "VE3ABC-70044"), ("cc", "VE3ABC-70045")):
        current = harness.Session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        allow = "PASS" if dialect == "go" else "SET/FILTER"
        block = "REJECT" if dialect == "go" else "UNSET/FILTER"
        record = root / "users" / (call + ".yaml")
        for domain, matching, prefix in (("DXCALL", "W6ABC", "w6*"), ("DECALL", "K1ABC", "k1*")):
            current.human(allow + " " + domain + " ALL", "All ")
            require_callsign_archive(current, cw_label, True)
            current.human(allow + " " + domain + " w6*,*abc,k1abc,ea8/w1xyz,w2abc-1")
            require_callsign_archive(current, cw_label, True)
            for verb in (allow, block):
                # A dot reaches this domain through the human input safe list;
                # underscores/non-ASCII are separately checked at the direct stage.
                for bad_list in ("N0XYZ,W*ABC", "*ABC*,N0XYZ", "N0XYZ,W1?", "W1.ABC,N0XYZ"):
                    before, saved = current.get(), record.read_bytes()
                    _, after = exact_human_reply(current, verb + " " + domain + " " + bad_list,
                                                 "Invalid callsign pattern. Use ASCII letters, digits, / or -.\n"
                                                 "Use at most one * at the beginning or end. No changes applied.")
                    require(after["configuration"] == before["configuration"] and
                            after["revision"] == before["revision"] and
                            after["status"]["effective"] == before["status"]["effective"] and
                            after["status"]["preset"] == before["status"]["preset"] and
                            record.read_bytes() == saved,
                            "invalid callsign list changed live/revision/saved state")
                    require_callsign_archive(current, cw_label, True)
            current.human(block + " " + domain + " " + matching)
            require_callsign_archive(current, cw_label, False)
            current.human(allow + " " + domain + " *")
            require_callsign_archive(current, cw_label, False)
            current.human(allow + " " + domain + " ALL", "All ")
            require_callsign_archive(current, cw_label, True)
            current.human(allow + " " + domain + " " + prefix)
            current.human(block + " " + domain + " N0XYZ")
        document = current.get()
        expected = dict(dx_callsigns=["W6*"], block_dx_callsigns=["N0XYZ"],
                        de_callsigns=["K1*"], block_de_callsigns=["N0XYZ"])
        require_callsign_rules(document, expected, record)
        require_callsign_archive(current, cw_label, True)
        result = current.upload("VALIDATE", "CONFIG", document["configuration"])
        require("error" not in result and result["result"]["operation"] == "VALIDATE" and
                result["result"]["valid"] is True and result["result"]["applied"] is False and
                result["result"]["persisted"] is False, "human callsign configuration is machine-invalid")
        saved_callsigns[call] = (document["configuration"], expected)


def require_filter_argument_refusal(session, command, record, exact_feedback=None):
    before, saved = session.get(), record.read_bytes()
    if exact_feedback is None:
        fields = command.upper().split()
        if fields[0].startswith("SET/") and fields[0] != "SET/FILTER":
            exact_feedback = "Usage: " + fields[0] + "\nNo changes applied."
        elif fields[0] == "SET/FILTER" and "/" in fields[1]:
            exact_feedback = "Usage: SET/FILTER " + fields[1] + "\nNo changes applied."
        else:
            domain = "ANNOUNCE" if fields[1] == "PC93" else fields[1]
            exact_feedback = domain + " accepts no value or ALL. No changes applied."
    text, after = exact_human_reply(session, command, exact_feedback)
    require(text.strip() == exact_feedback, "malformed filter arguments lack a refusal")
    require(after["configuration"] == before["configuration"] and
            after["revision"] == before["revision"] and
            after["status"]["effective"] == before["status"]["effective"] and
            after["status"]["preset"] == before["status"]["preset"] and
            record.read_bytes() == saved, "malformed filter arguments changed live or saved state")


def filter_argument_cases(suite, root, cw_label, saved_arguments):
    switches = dict(BEACON="include_beacons", WWV="allow_wwv", WCY="allow_wcy",
                    ANNOUNCE="allow_announce", SELF="allow_self", TOXIC="allow_toxic")
    for dialect, call in (("go", "VE3ABC-70046"), ("cc", "VE3ABC-70047")):
        current = harness.Session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        allow, block = ("PASS", "REJECT") if dialect == "go" else ("SET/FILTER", "UNSET/FILTER")
        record = root / "users" / (call + ".yaml")
        for domain, key in switches.items():
            for verb, opposite, enabled in ((allow, block, True), (block, allow, False)):
                current.human(opposite + " " + domain)
                for tail in ("OFF", "ALL OFF", "ALL ALL"):
                    require_filter_argument_refusal(current, verb + " " + domain + " " + tail, record)
                _, document = current.human(verb + " " + domain + " ALL")
                require(document["configuration"]["filters"][key] is enabled and
                        stored_document(record.read_bytes())[key] is enabled,
                        "valid switch operation lost its live or saved direction")
        current.human("REJECT BAND ALL")
        require_filter_argument_refusal(current, "PASS NOFILTER OFF", record)
        _, document = current.human("PASS NOFILTER ALL", "All filters disabled")
        require(document["configuration"]["filters"]["bands"]["allow_all"] is True,
                "compatible NOFILTER ALL did not clear restrictions")
        require_callsign_archive(current, cw_label, True)
        if dialect == "cc":
            for yes, no, key in (("SET/ANN", "SET/NOANN", "allow_announce"),
                                 ("SET/BEACON", "SET/NOBEACON", "include_beacons"),
                                 ("SET/WWV", "SET/NOWWV", "allow_wwv"),
                                 ("SET/WCY", "SET/NOWCY", "allow_wcy"),
                                 ("SET/SELF", "SET/NOSELF", "allow_self")):
                for command, opposite, enabled in ((yes, no, True), (no, yes, False)):
                    current.human(opposite)
                    for tail in ("ON", "ALL"):
                        require_filter_argument_refusal(current, command + " " + tail, record)
                    _, document = current.human(command)
                    require(document["configuration"]["filters"][key] is enabled,
                            "valid CC feature shortcut changed the wrong direction")
            for command, opposite, enabled in (("SET/SKIMMER", "SET/NOSKIMMER", True),
                                               ("SET/NOSKIMMER", "SET/SKIMMER", False)):
                current.human(opposite)
                require_filter_argument_refusal(current, command + " ALL", record)
                _, document = current.human(command)
                rule = document["configuration"]["filters"]["sources"]
                require(rule["block"].get("SKIMMER", False) is (not enabled) and
                        rule["allow"].get("SKIMMER", False) is enabled,
                        "valid skimmer shortcut changed the wrong source")
            current.human("PASS NOFILTER")
            for mode in ("CW", "FT8"):
                for command, opposite in (("SET/" + mode, "SET/NO" + mode),
                                          ("SET/NO" + mode, "SET/" + mode)):
                    current.human(opposite)
                    require_filter_argument_refusal(current, command + " CW", record)
                    current.human(command)
            current.human("SET/CW")
            require_callsign_archive(current, cw_label, True)
            current.human("SET/NOCW")
            require_callsign_archive(current, cw_label, False)
            current.human("SET/CW")
            current.human("REJECT BAND ALL")
            require_filter_argument_refusal(current, "SET/NOFILTER ALL", record)
            current.human("SET/NOFILTER")
            for domain in ("BAND", "MODE", "DXCALL", "DECALL", "BEACON", "TOXIC"):
                for flag, opposite in (("OFF", "ON"), ("ON", "OFF")):
                    current.human("SET/FILTER " + domain + "/" + opposite)
                    for tail in ("ALL", "20 CW"):
                        require_filter_argument_refusal(current, "SET/FILTER " + domain + "/" + flag + " " + tail, record)
                    _, document = current.human("SET/FILTER " + domain + "/" + flag)
                    filters = document["configuration"]["filters"]
                    if domain in ("BAND", "MODE"):
                        rule = filters["bands" if domain == "BAND" else "modes"]
                        require(rule == dict(allow_all=flag == "ON", block_all=flag == "OFF",
                                             allow={}, block={}), "valid inline flag lost its ALL direction")
                    elif domain in ("DXCALL", "DECALL"):
                        stem = "dx" if domain == "DXCALL" else "de"
                        require(filters[stem + "_callsigns"] == [] and
                                filters["block_" + stem + "_callsigns"] == ([] if flag == "ON" else ["*"]),
                                "valid inline flag lost its callsign reset/block operation")
                    else:
                        require(filters[switches[domain]] is (flag == "ON"),
                                "valid inline feature flag lost its direction")
                    if domain in ("BAND", "MODE", "DXCALL", "DECALL"):
                        require_callsign_archive(current, cw_label, flag == "ON")
            require_filter_argument_refusal(current, "SET/FILTER NOFILTER/ON OFF", record)
            current.human("REJECT BAND ALL")
            require_filter_argument_refusal(current, "SET/FILTER NOFILTER/OFF", record,
                                            "Invalid filter command. Type HELP for usage.")
            current.human("SET/FILTER NOFILTER/ON")
            # GO fallback must reach the same strict switch owner.
            require_filter_argument_refusal(current, "PASS BEACON OFF", record)
            require_filter_argument_refusal(current, "REJECT PC93 ON", record)
        current.human("PASS NOFILTER")
        current.human("REJECT BEACON")
        _, document = current.human("REJECT MODE FT8")
        require(document["configuration"]["filters"]["include_beacons"] is False and
                document["configuration"]["filters"]["modes"]["block"] == {"FT8": True},
                "final supported switch/MODE selection is incorrect")
        require_callsign_archive(current, cw_label, True)
        saved_arguments[call] = document["configuration"]


def grid_exact_cases(suite, root, saved_grids):
    for dialect, call in (("go", "VE3ABC-70042"), ("cc", "VE3ABC-70043")):
        current = harness.Session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        record = root / "users" / (call + ".yaml")
        current.human("SET GRID IO91", "Grid set to IO91")
        for command in ("SET GRID FN31PRJUNK", "SET GRID FN31P", "SET GRID FN31YZ",
                        "SET GRID SN31", "SET GRID FN31 EXTRA", "SET GRID FN31 PR"):
            before, saved = current.get(), record.read_bytes()
            text, after = current.human(command)
            require("Invalid grid." in text or "Usage: SET GRID" in text,
                    "malformed GRID lacks a specific refusal")
            require(after["configuration"] == before["configuration"] and
                    after["revision"] == before["revision"] and
                    after["status"]["effective"] == before["status"]["effective"] and
                    after["status"]["preset"] == before["status"]["preset"] and
                    record.read_bytes() == saved,
                    "malformed GRID changed configuration, runtime, revision or saved bytes")
        for token in ("fn31", "Fn31pR"):
            grid = token.upper()
            _, document = current.human("SET GRID " + token, "Grid set to " + grid)
            require_grid_configuration(document, grid, record)
            result = current.upload("VALIDATE", "CONFIG", document["configuration"])
            require("error" not in result and result["result"]["operation"] == "VALIDATE" and
                    result["result"]["valid"] is True and
                    result["result"]["applied"] is False and result["result"]["persisted"] is False,
                    "accepted human GRID failed machine validation")
        current.human("PASS NEARBY ON")
        _, document = current.human("SET GRID IO91", "Grid set to IO91")
        require_grid_configuration(document, "IO91", record)
        require(document["status"]["effective"]["nearby_active"] is True,
                "valid GRID update lost active NEARBY cells")
        saved_grids[call] = document["configuration"]


def require_nearby_archive(rows, label, frequency, visible):
    if not visible:
        require(not rows, "unmatched NEARBY control remained visible in history")
        return
    require(len(rows) == 1 and label in rows[0] and
            re.search(r"^DX de N0XYZ:\s+" + re.escape(str(frequency)) +
                      r"\.0+\s+W6ABC\s+CW\b", rows[0]),
            "NEARBY control absent or different from its seeded archive row")


def nearby_endpoint_cases(suite, child):
    # The pre-filter barrier proves every positive and negative row exists in
    # the real archive. Absence after enabling NEARBY then proves filtering.
    seeds = (("DX20", 14021, True), ("DE20", 14022, True),
             ("DXBAD20", 14023, True), ("DEBAD20", 14024, True),
             ("NONE20", 14025, False), ("FAR20", 14026, False),
             ("DX80", 3521, True), ("DE80", 3522, True),
             ("DX60", 5355, True), ("DE160", 1821, True))
    for dialect, call in (("go", "VE3ABC-70040"), ("cc", "VE3ABC-70041")):
        current = harness.Session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        nearby = "PASS NEARBY" if dialect == "go" else "SET/FILTER NEARBY"
        current.human(nearby + " OFF")
        current.human("PASS MODE ALL")
        current.human("SET GRID FN31")
        for name, frequency, _ in seeds:
            label = "N08" + child.nonce[:8].upper() + name
            deadline = time.monotonic() + 5
            while True:
                rows = archive_rows(current, label)
                if rows:
                    require_nearby_archive(rows, label, frequency, True)
                    break
                require(time.monotonic() < deadline, "NEARBY pre-filter archive barrier absent")
                time.sleep(.025)
        _, document = current.human(nearby + " ON")
        require(document["status"]["effective"]["nearby_active"] is True and
                document["status"]["effective"]["grid"] == "FN31",
                "NEARBY endpoint controls lack an active user location")
        for name, frequency, visible in seeds:
            label = "N08" + child.nonce[:8].upper() + name
            require_nearby_archive(archive_rows(current, label), label, frequency, visible)


def mode_delta_cases(suite, root, cw_label, saved_modes):
    poster = harness.Session(suite, "K1ABC-70034")
    poster.human("PAUSE 300")
    poster.human("PASS MODE ALL")
    ft8_label = literal_archive_label(suite, "F8")
    poster.human("DX 7074 W6ABC FT8 " + ft8_label, "Spot queued.")
    deadline = time.monotonic() + 5
    while not archive_rows(poster, ft8_label):
        require(time.monotonic() < deadline, "MODE positive archive barrier absent")
        time.sleep(.025)
    require_mode_archive(poster, ft8_label, "FT8", True)
    for dialect, call in (("go", "VE3ABC-70033"), ("cc", "VE3ABC-70034")):
        current = harness.Session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        allow = "PASS MODE" if dialect == "go" else "SET/FILTER MODE"
        block = "REJECT MODE" if dialect == "go" else "UNSET/FILTER MODE"
        for command, ft8_visible in ((allow + " ALL", True), (block + " FT8", False),
                                     (allow + " FT8", True)):
            current.human(command, "Effective MODE:")
            require_mode_archive(current, cw_label, "CW", True)
            require_mode_archive(current, ft8_label, "FT8", ft8_visible)
        text, document = current.human("SHOW FILTER MODE", "PASS: ALL")
        require("Unknown modes are included" in text, "MODE warning disagrees with open state")
        expected = dict(allow_all=True, block_all=False, allow={}, block={})
        record = root / "users" / (call + ".yaml")
        require_mode_rules(document, expected, record)
        saved_modes[call] = (document["configuration"], expected, cw_label, ft8_label, True)

    current = harness.Session(suite, "VE3ABC-70035")
    current.human("PAUSE 300")
    for command in ("REJECT MODE ALL", "PASS MODE CW", "REJECT MODE CW"):
        current.human(command, "Effective MODE:")
    text, document = current.human("SHOW FILTER MODE", "PASS: NONE")
    require("Unknown modes are hidden" in text, "MODE warning disagrees with restrictive empty state")
    expected = dict(allow_all=False, block_all=False, allow={}, block={"CW": True})
    record = root / "users" / "VE3ABC-70035.yaml"
    require_mode_rules(document, expected, record)
    require_mode_archive(current, cw_label, "CW", False)
    require_mode_archive(current, ft8_label, "FT8", False)
    saved_modes[current.call] = (document["configuration"], expected, cw_label, ft8_label, False)


def invalid_frequency_posts(suite, sessions, labels):
    # Keep wire vectors below the fixture's loaded human-line limit. Direct Go
    # API/fuzz cases cover long decimal parse/scaling overflow separately.
    for index, token in enumerate(("0.004", "135.694", "2450000.005", "NaN", "INF")):
        label = suite.token + f"INVALIDF{index}"
        text, _ = sessions[index % 2].human(f"DX {token} W6ABC CW {label}")
        require_frequency_refusal(text)
        labels["invalid"].append(label)


def require_reputation_history(root, old_files):
    for path, raw in old_files.items():
        require(path.read_bytes() == raw, "old SSID reputation history was changed or merged")
    for call, country in (("K1ABC", "US"), ("VE3ABC", "CA")):
        document = stored_document((root / "reputation" / (call + ".yaml")).read_bytes())
        require(set(document) == {"recent_asns", "recent_countries", "recent_continents", "updated_at"} and
                document.get("recent_asns") == ["AS64500"] and
                document.get("recent_countries") == [country] and
                document.get("recent_continents") == ["NA"],
                "baseline reputation history was lost, replaced or merged")
        updated = document.get("updated_at")
        require(hasattr(updated, "year") and updated.year > 2000,
                "login did not touch the baseline history")
    require({path.name for path in (root / "reputation").iterdir()} ==
            {"K1ABC.yaml", "VE3ABC.yaml"} | {path.name for path in old_files},
            "reputation profile created an unexpected per-SSID history owner")


def require_reputation_preferences(root, sessions, expected):
    require(all(current.call in expected for current in sessions),
            "required full-SSID preference controls missing")
    for current in sessions:
        configuration = current.get()["configuration"]
        require(configuration == expected[current.call], "full-SSID preferences were shared or lost")
        saved = stored_document((root / "users" / (current.call + ".yaml")).read_bytes())
        settings = configuration["settings"]
        require(all(saved[key] == settings[key] for key in ("noise_class", "grid", "dialect")),
                "SSID setting was not saved while connected")
    require(not (root / "users" / "K1ABC.yaml").exists(),
            "station reputation normalization changed preference ownership")


def reputation_posts(suite, sessions, observer, generation, labels):
    for index, frequency in enumerate((14074, 7074)):
        current = sessions[index]
        label = literal_archive_label(suite, f"G{generation}P{frequency}")
        current.human(f"DX {frequency} W6ABC CW {label}", "Spot queued.")
        labels["accepted"].append((label, frequency))
        require_reputation_archive(observer, label, frequency)
        if frequency == 14074:
            if generation == 0:
                sessions[1].close()
                sessions[1] = harness.Session(suite, sessions[1].call)
                sessions[1].human("PAUSE 300")
            refused = literal_archive_label(suite, f"G{generation}BAND")
            text, _ = sessions[1].human(f"DX 14074 W6ABC CW {refused}")
            require_reputation_refusal(text)
            labels["refused"].append(refused)
    # Both per-band and total counts are spent before benign reconnects. The
    # command's correlated GET fences processing before the following archive
    # barrier, so absence is not inferred from a prompt or arbitrary sleep.
    if generation == 0:
        for current in sessions:
            current.close()
        sessions[:] = [harness.Session(suite, current.call) for current in sessions]
        for current in sessions:
            current.human("PAUSE 300")
    for index, current in enumerate(sessions):
        for frequency in (14074, 3504):
            label = literal_archive_label(suite, f"G{generation}R{index}F{frequency}")
            text, _ = current.human(f"DX {frequency} W6ABC CW {label}")
            require_reputation_refusal(text)
            labels["refused"].append(label)
    barrier = literal_archive_label(suite, f"G{generation}BARRIER")
    observer.human(f"DX 3504 W6ABC CW {barrier}", "Spot queued.")
    # The independent, known-history observer's later accepted row proves the
    # shared FIFO bridge and archive have passed every earlier attempted post.
    deadline = time.monotonic() + 5
    while True:
        rows = archive_rows(observer, barrier)
        if rows:
            require(len(rows) == 1 and barrier in rows[0] and
                    re.search(r"^DX de VE3ABC:\s+3504\.0+\s+W6ABC\s+CW\b", rows[0]),
                    "archive refusal barrier lacks its accepted observer control")
            break
        require(time.monotonic() < deadline, "accepted archive refusal barrier absent")
        time.sleep(.025)
    for label in labels["refused"] + labels["invalid"]:
        require_archive_absent(archive_rows(observer, label))


def require_unrestricted_geography(document, saved):
    filters = document["configuration"]["filters"]
    for category in ("dx_continents", "de_continents", "dx_zones", "de_zones",
                     "dx_grid2", "de_grid2", "dx_dxcc", "de_dxcc", "dx_states", "de_states"):
        rule = filters[category]
        require(rule == dict(allow_all=True, block_all=False, allow={}, block={}),
                f"live geography restriction remained: {category}")
    for stem, allow_key in (("dxcontinents", "dxcontinents"), ("decontinents", "decontinents"),
                            ("dxzones", "dxzones"), ("dezones", "dezones"),
                            ("dxgrid2", "dxgrid2prefixes"), ("degrid2", "degrid2prefixes"),
                            ("dxdxcc", "dxdxcc"), ("dedxcc", "dedxcc"),
                            ("dxstates", "dxstates"), ("destates", "destates")):
        require(saved["all" + stem] is True and saved["blockall" + stem] is False and
                saved[allow_key] == {} and saved["block" + stem] == {},
                f"saved geography restriction remained: {stem}")
    require(filters["nearby_enabled"] is False and saved.get("nearby_enabled", False) is False,
            "NEARBY remained enabled")


def require_save_warning(text):
    require(text.count("warning: failed to persist") == 1 and
            "\nwarning: failed to persist;" in text and
            "changes are active but were not saved" in text,
            f"missing or misleading persistence warning: {text[:800]}")


RECORD_BYTES = 1 << 20


def refused_resource_record(kind):
    """Separate raw, normalized-byte and logical-work refusals with valid YAML."""
    if kind == "RAW":
        # A fitting mapping with comment padding would otherwise load normally.
        return b"{}\n#" + b"X" * (RECORD_BYTES - 3)
    if kind == "CANONICAL":
        # Five retained scalar occurrences alone exceed the normalized-record
        # byte quota. P+E stays below 2 Mi units and raw bytes remain small.
        return ("legacy: &text " + "K" * 210000 +
                "\nrecent_ips: [" + ",".join(["*text"] * 5) + "]\n").encode("ascii")
    require(kind == "WORK", "unknown resource fixture")
    # Frozen 04a control: 144 old decode visits, but 70*32768 scalar bytes.
    # Trimming retains only five entries, separating work from final bytes.
    return ("legacy: &text " + "K" * 32768 +
            "\nrecent_ips: [" + ",".join(["*text"] * 70) + "]\n").encode("ascii")


def stored_document(raw):
    document = harness.yaml.load(raw.decode("ascii"), Loader=harness.StrictLoader)
    require(isinstance(document, dict), "stored fixture is not a mapping")
    return document


def record_with_metadata(raw, timestamp, ip):
    document = stored_document(raw)
    document["last_login_utc"] = timestamp
    document["recent_ips"] = [ip]
    return harness.yaml.safe_dump(document, sort_keys=False).encode("ascii")


def require_metadata(raw, timestamp, ip):
    saved = stored_document(raw)
    value = saved["last_login_utc"]
    if not isinstance(value, str):
        value = value.isoformat().replace("+00:00", "Z")
    require(value == timestamp and saved["recent_ips"][0] == "127.0.0.1" and
            ip in saved["recent_ips"] and len(saved["recent_ips"]) <= 5,
            "save used stale session metadata instead of the authoritative repair")


def padded_canonical_record(raw, remaining=8):
    """Pad only the Go-written plain-ASCII metadata block, preserving encoding."""
    require(stored_document(raw)["recent_ips"] == ["127.0.0.1"],
            "canonical padding requires the held loopback login")
    match = re.search(rb"(?m)^recent_ips:\n(?: +[^\n]*\n)+", raw)
    require(match is not None, "Go-written recent IP block absent")
    block = b"recent_ips:\n    - 127.0.0.1\n    - METADATA"
    fixed = raw[:match.start()] + block + b"\n" + raw[match.end():]
    padding = RECORD_BYTES - remaining - len(fixed)
    require(padding > 0, "canonical record has no padding room")
    result = raw[:match.start()] + block + b"A" * padding + b"\n" + raw[match.end():]
    require(len(result) == RECORD_BYTES - remaining and
            stored_document(result)["recent_ips"] == ["127.0.0.1", "METADATA" + "A" * padding],
            "prospective fixture size or retained metadata changed")
    return result


def preset_library(root):
    return root / "users" / "presets" / ("K1ABC".encode().hex() + ".yaml")


def require_transaction_unchanged(before, after, record, saved, library, library_saved):
    require(after["configuration"] == before["configuration"] and
            after["revision"] == before["revision"] and
            after["status"]["preset"] == before["status"]["preset"] and
            after["status"]["effective"] == before["status"]["effective"] and
            after["status"]["session"]["temporary_defaults"] ==
            before["status"]["session"]["temporary_defaults"] and
            record.read_bytes() == saved and library.read_bytes() == library_saved,
            "resource refusal published, protected or durably changed the transaction")


def resource_login_cases(suite, root, refused):
    library = preset_library(root)
    for index, kind in enumerate(("RAW", "CANONICAL", "WORK"), start=21):
        call = f"K1ABC-700{index:02d}"
        record, raw = root / "users" / (call + ".yaml"), refused_resource_record(kind)
        record.write_bytes(raw)
        refused[call] = raw
        current = harness.Session(suite, call)
        startup, document = current.human("PAUSE 300")
        require("temporary defaults" in startup and
                document["status"]["session"]["temporary_defaults"] is True and
                record.read_bytes() == raw, f"{kind} resource-refused login was not protected")
        for dialect in ("cc", "go"):
            text, document = current.human("DIALECT " + dialect)
            require_save_warning(text)
            line = "SET/FILTER BAND 20" if dialect == "cc" else "PASS BAND 40"
            text, document = current.human(line)
            require_save_warning(text)
            require(document["configuration"]["filters"]["bands"]["allow"].get(
                "20m" if dialect == "cc" else "40m") is True and record.read_bytes() == raw,
                f"{kind} protected edit was lost or overwrote storage")
        library_saved = library.read_bytes()
        before = current.get()
        text, after = current.human("SAVE PRESET BLOCK" + kind, "SAVE PRESET failed:")
        require("saved user record is protected" in text,
                "protected SAVE did not stop at the existing record-protection boundary")
        require_transaction_unchanged(before, after, record, raw, library, library_saved)


def resource_warm_cases(suite, root, refused, repaired):
    library = preset_library(root)
    for index, kind in enumerate(("RAW", "CANONICAL", "WORK"), start=24):
        call = f"K1ABC-700{index:02d}"
        current = harness.Session(suite, call)
        current.human("PAUSE 300")
        name = "WARM" + kind
        current.human("SAVE PRESET " + name, "Saved preset " + name)
        record = root / "users" / (call + ".yaml")
        repair_source, raw = record.read_bytes(), refused_resource_record(kind)
        record.write_bytes(raw)
        text, temporary = current.human("DIALECT cc")
        require_save_warning(text)
        text, temporary = current.human("PASS COMMENT TEMP " + kind)
        require_save_warning(text)
        require(temporary["configuration"]["filters"]["comments"] == ["TEMP " + kind] and
                temporary["status"]["session"]["temporary_defaults"] is False and
                record.read_bytes() == raw, "warm refusal became sticky or lost its temporary edit")
        library_saved = library.read_bytes()
        for target in (name, "DENY" + kind):
            text, after = current.human("SAVE PRESET " + target, "SAVE PRESET failed:")
            require("load current user record:" in text and
                    "but could not persist its association" not in text,
                    "warm refusal was misreported as protected or as a successful library write")
            require_transaction_unchanged(temporary, after, record, raw, library, library_saved)
        timestamp, ip = f"2026-10-08T12:34:{index:02d}Z", f"198.51.100.{index}"
        record.write_bytes(record_with_metadata(repair_source, timestamp, ip))
        text, after = current.human("PASS COMMENT RETRY " + kind)
        require("warning: failed to persist" not in text and
                after["configuration"]["filters"]["comments"] == ["TEMP " + kind, "RETRY " + kind] and
                after["status"]["session"]["temporary_defaults"] is False,
                "warm preparation did not retry after authoritative repair")
        require_metadata(record.read_bytes(), timestamp, ip)
        repaired[call] = (after["configuration"], timestamp, ip)

    # A still-refused ordinary logout must preserve bytes; a repaired logout
    # must independently retry without a second mutating command masking it.
    for call, repair in (("K1ABC-70028", False), ("K1ABC-70029", True)):
        current = harness.Session(suite, call)
        current.human("PAUSE 300")
        record = root / "users" / (call + ".yaml")
        repair_source, raw = record.read_bytes(), refused_resource_record("RAW")
        record.write_bytes(raw)
        text, temporary = current.human("PASS COMMENT LOGOUT RETRY")
        require_save_warning(text)
        require(temporary["status"]["session"]["temporary_defaults"] is False and
                record.read_bytes() == raw, "warm logout control did not remain temporary")
        timestamp, ip = "2026-10-08T12:35:29Z", "198.51.100.29"
        if repair:
            record.write_bytes(record_with_metadata(repair_source, timestamp, ip))
        current.close()
        if not repair:
            require(record.read_bytes() == raw, "logout overwrote a still-refused resource file")
            refused[call] = raw
            continue
        deadline = time.monotonic() + 5
        while stored_document(record.read_bytes()).get("comments") != ["LOGOUT RETRY"]:
            require(time.monotonic() < deadline, "repaired logout did not save the temporary live edit")
            time.sleep(.025)
        require_metadata(record.read_bytes(), timestamp, ip)
        repaired[call] = (temporary["configuration"], timestamp, ip)


def resource_prospective_case(suite, root, prospective):
    call = "K1ABC-70027"
    current = harness.Session(suite, call)
    current.human("PAUSE 300")
    current.human("SET NOISE QUIET")
    current.human("SAVE PRESET QUOTAOLD", "Saved preset QUOTAOLD")
    _, before = current.human("PASS COMMENT CURRENT QUOTA")
    require(before["status"]["preset"] == dict(associated=True, name="QUOTAOLD", modified=True),
            "prospective control lacks an older, smaller applied baseline")
    library, record = preset_library(root), root / "users" / (call + ".yaml")
    collection = stored_document(library.read_bytes())
    target = copy.deepcopy(collection["presets"]["QUOTAOLD"])
    target["comments"] = ["LOAD TARGET GROWS BOTH CONFIGURATION AND BASELINE"]
    collection["presets"]["QUOTALOAD"] = target
    library.write_text(harness.yaml.safe_dump(collection, sort_keys=False), encoding="ascii")
    library_saved = library.read_bytes()
    saved = padded_canonical_record(record.read_bytes())
    record.write_bytes(saved)

    text, after = current.human("PASS COMMENT HUMAN PROSPECTIVE QUOTA", "Configuration command failed:")
    require("user record exceeds 1048576 bytes" in text and
            "load current user record:" not in text and "warning: failed to persist" not in text,
            "prospective human quota was misclassified as a temporary live change")
    require_transaction_unchanged(before, after, record, saved, library, library_saved)
    for name in ("QUOTAOLD", "QUOTANEW"):
        text, after = current.human("SAVE PRESET " + name, "SAVE PRESET failed:")
        require("user record exceeds 1048576 bytes" in text and
                "load current user record:" not in text and
                "but could not persist its association" not in text,
                "SAVE did not refuse prospective bytes before library publication")
        require_transaction_unchanged(before, after, record, saved, library, library_saved)
    candidate = copy.deepcopy(before["configuration"])
    candidate["filters"]["comments"].append("EXTRA PROSPECTIVE QUOTA")
    validation = current.upload("VALIDATE", "CONFIG", candidate)
    suite.acknowledge(validation, applied=False)
    require_transaction_unchanged(before, current.get(), record, saved, library, library_saved)
    response = current.upload("PUT", "CONFIG", candidate, revision=before["revision"])
    require(response.get("error", {}).get("code") == "persistence_failed",
            "small valid machine candidate did not reach destination admission")
    require_transaction_unchanged(before, current.get(), record, saved, library, library_saved)
    text, after = current.human("LOAD PRESET QUOTALOAD", "LOAD PRESET failed:")
    require("user record exceeds 1048576 bytes" in text and "load current user record:" not in text,
            "LOAD fixture failed before prospective destination admission")
    require_transaction_unchanged(before, after, record, saved, library, library_saved)
    prospective[call] = (saved, before["configuration"], before["status"]["preset"])


def profile(binary, root, source, transport):
    results, wire, cleanup, children = [], [], [], []
    required = [f"{transport}/0: held endpoint and loaded consumers",
                f"{transport}: rejected lines have no command effects",
                f"{transport}: stalled rejected tail disconnects",
                f"{transport}: live change saved before disconnect",
                f"{transport}: invalid control and actual archive write",
                f"{transport}: NOFILTER retires NEARBY geography and saves",
                f"{transport}: actual replacement failure warns and preserves saved bytes",
                f"{transport}: preparation failure and protected login preserve refused records",
                f"{transport}: compatible storage and old alias refusal reach login and LOAD",
                f"{transport}: real machine LOAD and SAVE commit faults preserve publication",
                f"{transport}: detached human edits preserve refusal and NEARBY restoration",
                f"{transport}: resource-refused login keeps fitting edits temporary",
                f"{transport}: warm resource refusals retry authoritative saves and logout",
                f"{transport}: prospective human SAVE machine and LOAD refuse before effects",
                f"{transport}: MODE deltas preserve visible history and saved fallback",
                f"{transport}/0: NEARBY endpoints filter seeded archive independently",
                f"{transport}: GRID exact syntax preserves refusals and canonical publication",
                f"{transport}: callsign patterns admit complete lists and preserve matching",
                f"{transport}: original filter arguments refuse before normalization",
                f"{transport}/1: held endpoint and loaded consumers",
                f"{transport}: preferences and archive survive fresh child",
                f"{transport}: NOFILTER geography survives fresh child",
                f"{transport}: failed saves and refused records survive fresh child",
                f"{transport}: compatible storage survives fresh child",
                f"{transport}: prepared commit outcomes survive fresh child",
                f"{transport}: detached human settings survive fresh child",
                f"{transport}: resource-refused records survive fresh child",
                f"{transport}: repaired resource metadata survives fresh child",
                f"{transport}: prospective admission outcomes survive fresh child",
                f"{transport}: MODE outcomes survive fresh child",
                f"{transport}/1: NEARBY endpoints filter seeded archive independently",
                f"{transport}: canonical GRID and active cells survive fresh child",
                f"{transport}: callsign patterns and matching survive fresh child",
                f"{transport}: supported filter arguments survive fresh child"]
    call = "K1ABC-70001"
    observer_call, observer_configuration = "VE3ABC-70003", None
    failed_records, refused_records, compatible_records = {}, {}, {}
    prepared_records = {}
    staged_records = {}
    resource_refused, resource_repaired, resource_prospective = {}, {}, {}
    saved_modes = {}
    saved_grids = {}
    saved_callsigns = {}
    saved_arguments = {}
    label = None
    suites = []
    for generation in range(2):
        child, suite = None, None
        replacement_faults = contextlib.ExitStack()
        try:
            child = Child(binary, root, source)
            children.append(child.identity)
            suite = harness.Suite(SimpleNamespace(host=child.host, port=child.port, call="K1ABC",
                                                  timeout=8, run_seconds=120,
                                                  output=root / f"wire-{generation}"))
            if generation == 0:
                label = literal_archive_label(suite, "CW")
            suites.append(suite)
            session = harness.Session(suite, call)

            def case(name, fn):
                suite.case(name, fn)
                child.check_running()

            def identity():
                session.human("PAUSE 300")
                session.human("SHOW BUILD", "Build version: fixture-" + child.nonce)
                session.human("SHOW DXCC VE", "Canada")
                require(session.get()["configuration"]["settings"]["dialect"] in ("go", "cc"),
                        "invalid default dialect")

            case(f"{transport}/{generation}: held endpoint and loaded consumers", identity)
            if generation == 0:
                def rejected_lines():
                    record = root / "users" / (call + ".yaml")
                    for dialect in ("go", "cc"):
                        session.human("DIALECT " + dialect)
                        for bad in ("DX 14074 K1ABC CW !REJECT BAND ALL",
                                    "A" * 129 + "REJECT BAND ALL"):
                            before = session.get()
                            saved = record.read_bytes()
                            identifier = suite.identifier()
                            sync = f"GET YAML CONFIG SCHEMA 4 ID {identifier}"
                            session.send(bad + "\r\nSHOW OWN\r\n" + sync + "\r\n")
                            after, prefix = session.frame("CONFIG", identifier, 4, "rejected physical line")
                            require(prefix.count(b"Own call: K1ABC") == 1,
                                    "recovery lost or duplicated the next read-only command")
                            require(b"Command input" in prefix, "rejection response absent")
                            require(after["configuration"] == before["configuration"] and
                                    after["revision"] == before["revision"],
                                    "rejected line changed live configuration")
                            require(record.read_bytes() == saved,
                                    "rejected line changed saved configuration")
                            require(b"All bands blocked" not in prefix and b"Spot queued" not in prefix,
                                    "rejected physical line executed a command")

                case(f"{transport}: rejected lines have no command effects", rejected_lines)

                def stalled_rejection():
                    terminal = harness.Session(suite, "K1ABC-70002")
                    record = root / "users" / (terminal.call + ".yaml")
                    saved = record.read_bytes()
                    terminal.send("DX 14074 K1ABC CW !REJECT BAND ALL")  # No physical EOL.
                    start = time.monotonic()
                    try:
                        terminal.reader.until(b"unreachable-test-marker")
                        raise ValueError("stalled rejected line stayed open")
                    except EOFError:
                        require(time.monotonic() - start < 7, "recovery exceeded five-second bound")
                        require(b"Spot queued" not in terminal.reader.buffer,
                                "incomplete rejected line posted a spot")
                    finally:
                        terminal.sock.close()
                        terminal.closed = True
                    require(record.read_bytes() == saved,
                            "stalled rejected line changed saved configuration")

                case(f"{transport}: stalled rejected tail disconnects", stalled_rejection)

                def persistence():
                    for dialect in ("cc", "go", "cc"):
                        _, doc = session.human("DIALECT " + dialect)
                        require(doc["configuration"]["settings"]["dialect"] == dialect, "dialect not applied")
                        saved = harness.yaml.load((root / "users" / (call + ".yaml")).read_text(),
                                                  Loader=harness.StrictLoader)
                        require(saved["dialect"] == dialect, "dialect not saved before next command")
                    _, doc = session.human("SET GRID FN31", "Grid set")
                    require(doc["configuration"]["settings"]["grid"] == "FN31", "loaded grid unavailable")
                    _, doc = session.human("SET NOISE URBAN", "Noise class set")
                    require(doc["configuration"]["settings"]["noise_class"] == "URBAN", "live setting absent")
                    # Inspect while still connected: final unregister autosave
                    # must never repair a false-green command-save assertion.
                    saved = harness.yaml.load((root / "users" / (call + ".yaml")).read_text(),
                                              Loader=harness.StrictLoader)
                    require(saved["noise_class"] == "URBAN" and saved["grid"] == "FN31" and
                            saved["dialect"] == "cc", "command persistence absent before disconnect")

                case(f"{transport}: live change saved before disconnect", persistence)

                def posting():
                    session.human("DX NOTAFREQ W6ABC " + label, "Invalid frequency")
                    session.human("DX 14074 W6ABC CW " + label, "Spot queued.")
                    require_archive(session, label)

                case(f"{transport}: invalid control and actual archive write", posting)

                def nofilter():
                    nonlocal observer_configuration
                    observer = harness.Session(suite, observer_call)
                    observer.human("PAUSE 300")
                    observer.human("SET GRID FN31", "Grid set")
                    require_archive(observer, label) # Control: exact row exists and is visible.
                    for dialect, reset in (("go", "PASS NOFILTER"), ("cc", "SET/NOFILTER")):
                        observer.human("DIALECT " + dialect)
                        # The isolated manual bridge does not run production
                        # metadata enrichment. A restrictive whitelist also
                        # hides unknown geography, providing a valid control.
                        observer.human("PASS DXCONT EU")
                        require(not archive_rows(observer, label), "restrictive geography failed to hide control row")
                        observer.human("PASS NEARBY ON", "Nearby filter enabled")
                        _, document = observer.human(reset, "All filters disabled")
                        saved = harness.yaml.load((root / "users" / (observer_call + ".yaml")).read_text(),
                                                  Loader=harness.StrictLoader)
                        require_unrestricted_geography(document, saved)
                        require(saved["grid"] == "FN31" and document["configuration"]["settings"]["grid"] == "FN31",
                                "NOFILTER cleared the user's grid")
                        require_archive(observer, label)
                        observer_configuration = document["configuration"]

                case(f"{transport}: NOFILTER retires NEARBY geography and saves", nofilter)

                def failed_saves():
                    for dialect, failed_call in (("go", "K1ABC-70004"), ("cc", "K1ABC-70007")):
                        failed = harness.Session(suite, failed_call)
                        failed.human("PAUSE 300")
                        failed.human("DIALECT " + ("cc" if dialect == "go" else "go"))
                        failed.human("PASS BAND 40")
                        _, before = failed.human("PASS COMMENT OLD")
                        record = root / "users" / (failed_call + ".yaml")
                        saved = record.read_bytes()
                        failed_records[failed_call] = (saved, before["configuration"])
                        # Keep this handle through child.stop(), not merely BYE:
                        # unregister's final save must not repair a failed save.
                        replacement_faults.enter_context(deny_record_replacement(record))

                        def changed(line, check):
                            text, document = failed.human(line)
                            require_save_warning(text)
                            require(check(document["configuration"]), f"live change absent: {line}")
                            require(record.read_bytes() == saved, f"failed save changed disk: {line}")
                            return document

                        changed("DIALECT " + dialect, lambda cfg: cfg["settings"]["dialect"] == dialect)
                        for line in ("DIALECT " + dialect, "PASS COMMENT old",
                                     "REMOVE PASS COMMENT absent", "RESET FILTER COMMENT REJECT"):
                            before_noop = failed.get()
                            text, after_noop = failed.human(line)
                            require("warning: failed to persist" not in text and
                                    after_noop["configuration"] == before_noop["configuration"] and
                                    record.read_bytes() == saved, f"no-op save behavior changed: {line}")
                        band_command = "PASS BAND 20" if dialect == "go" else "SET/FILTER BAND 20"
                        changed(band_command, lambda cfg: cfg["filters"]["bands"]["allow"].get("20m") is True)
                        changed(band_command, lambda cfg: cfg["filters"]["bands"]["allow"].get("20m") is True)
                        changed("PASS COMMENT NEW", lambda cfg: cfg["filters"]["comments"] == ["OLD", "NEW"])
                        changed("REMOVE PASS COMMENT OLD", lambda cfg: cfg["filters"]["comments"] == ["NEW"])
                        changed("RESET FILTER COMMENT", lambda cfg: cfg["filters"]["comments"] == [])
                        changed("RESET FILTER", lambda cfg: cfg["filters"]["bands"]["allow_all"] is True)
                        changed("REJECT BAND ALL", lambda cfg: cfg["filters"]["bands"]["block_all"] is True)
                        changed("PASS NOFILTER" if dialect == "go" else "SET/NOFILTER",
                                lambda cfg: cfg["filters"]["bands"]["allow_all"] is True and
                                cfg["filters"]["bands"]["block_all"] is False)

                case(f"{transport}: actual replacement failure warns and preserves saved bytes", failed_saves)

                def refused_storage():
                    for refused_call, raw in (("K1ABC-70005", b"[broken"),
                                              ("K1ABC-70006", b"configuration_version: 99\nnoise_class: URBAN\n")):
                        existing = harness.Session(suite, refused_call)
                        existing.human("PAUSE 300")
                        record = root / "users" / (refused_call + ".yaml")
                        record.write_bytes(raw) # Authoritative storage changes after valid login.
                        refused_records[refused_call] = raw
                        for dialect, line in (("cc", "SET/FILTER BAND 20"), ("go", "PASS BAND 40")):
                            text, document = existing.human("DIALECT " + dialect)
                            require_save_warning(text)
                            require(document["configuration"]["settings"]["dialect"] == dialect,
                                    "preparation failure rolled back live dialect")
                            text, document = existing.human(line)
                            require_save_warning(text)
                            require(document["configuration"]["filters"]["bands"]["allow"].get(
                                "20m" if dialect == "cc" else "40m") is True and record.read_bytes() == raw,
                                "preparation failure changed disk or lost live filter")
                        existing.close()
                        protected = harness.Session(suite, refused_call)
                        startup, document = protected.human("PAUSE 300")
                        # The configured greeting ends in a UTC prompt before
                        # the appended restore warning. Synchronize through GET
                        # to consume and check that remainder, not just prompt.
                        require("temporary defaults" in startup,
                                "protected login omitted temporary-settings warning")
                        require(document["status"]["session"]["temporary_defaults"] is True,
                                "protected login status absent")
                        for line in ("DIALECT cc", "SET/FILTER BAND 20", "PASS COMMENT TEMP"):
                            text, document = protected.human(line)
                            require_save_warning(text)
                            require(record.read_bytes() == raw, "protected edit overwrote refused file")
                        require(document["configuration"]["settings"]["dialect"] == "cc" and
                                document["configuration"]["filters"]["comments"] == ["TEMP"],
                                "protected live edits were not applied")

                case(f"{transport}: preparation failure and protected login preserve refused records", refused_storage)

                def compatible_storage():
                    unused = ("a: &a {x: 0}\nb: &b {<<: [*a,*a,*a,*a,*a,*a,*a]}\n"
                              "c: &c {<<: [*b,*b,*b,*b,*b,*b,*b]}\n"
                              "d: &d {<<: [*c,*c,*c,*c,*c,*c,*c]}\n")
                    legacy_call, exact_call, trap_call = "K1ABC-70008", "K1ABC-70009", "K1ABC-70010"
                    users = root / "users"
                    legacy = (unused + "rules: &rules {20m: true, 40m: false}\n"
                              "bands: *rules\nallbands: false\nblockbands: {80m: true}\n"
                              "dialect: cc\ndedupe_policy: MED\n")
                    (users / (legacy_call + ".yaml")).write_text(legacy, encoding="utf-8")
                    restored = harness.Session(suite, legacy_call)
                    startup, document = restored.human("PAUSE 300")
                    bands = document["configuration"]["filters"]["bands"]
                    require("temporary defaults" not in startup and
                            document["status"]["session"]["temporary_defaults"] is False and
                            bands == dict(allow_all=False, block_all=False,
                                          allow={"20m": True, "40m": False}, block={"80m": True}),
                            "legacy aliases, false entries or migration changed at login")
                    compatible_records[legacy_call] = document["configuration"]

                    exact = harness.yaml.load((users / (call + ".yaml")).read_text(), Loader=harness.StrictLoader)
                    exact.update(bands={"20m": True, "40m": False}, blockbands={"80m": False},
                                 allbands=False, modes={}, allmodes=False, min_snr={"CW": -5},
                                 comments=["CQ TEST"], allow_wwv=False)
                    # PyYAML emits a space-separated datetime that time.Time's
                    # pinned decoder refuses. Keep the stored RFC3339 contract.
                    exact["last_login_utc"] = exact["last_login_utc"].isoformat().replace("+00:00", "Z")
                    baseline = {key: value for key, value in exact.items()
                                if key not in ("preset", "recent_ips", "last_login_utc")}
                    exact["preset"] = dict(name="BASE04A", baseline=baseline)
                    (users / (exact_call + ".yaml")).write_text(harness.yaml.safe_dump(exact), encoding="utf-8")
                    restored = harness.Session(suite, exact_call)
                    startup, document = restored.human("PAUSE 300")
                    filters = document["configuration"]["filters"]
                    require("temporary defaults" not in startup and
                            filters["bands"] == dict(allow_all=False, block_all=False,
                                                     allow={"20m": True, "40m": False}, block={"80m": False}) and
                            filters["modes"]["allow_all"] is False and filters["modes"]["allow"] == {} and
                            filters["comments"] == ["CQ TEST"] and filters["min_snr"] == {"CW": -5} and
                            filters["allow_wwv"] is False and
                            document["status"]["preset"]["name"] == "BASE04A" and
                            document["status"]["preset"]["modified"] is False,
                            "exact false/empty values or detached baseline changed at login")
                    library = users / "presets" / ("K1ABC".encode().hex() + ".yaml")
                    library.parent.mkdir(exist_ok=True)
                    collection = harness.yaml.safe_dump(dict(presets=dict(LEGACY04A=dict(
                        bands={"20m": True, "40m": False}, allbands=False, dialect="go", dedupe_policy="MED"))))
                    library.write_text("<<:\n" + "".join("  " + line for line in collection.splitlines(True)),
                                       encoding="utf-8")
                    restored.human("LIST PRESET", "LEGACY04A")
                    _, document = restored.human("LOAD PRESET LEGACY04A", "Loaded preset")
                    require(document["configuration"]["filters"]["bands"]["allow"] ==
                            {"20m": True, "40m": False} and
                            document["status"]["preset"]["name"] == "LEGACY04A" and
                            document["status"]["preset"]["modified"] is False,
                            "merged collection did not load its migrated preset")
                    compatible_records[exact_call] = document["configuration"]

                    trap = (unused + "<<: *d\n").encode("ascii")
                    require(len(trap) == 125, "alias refusal fixture changed")
                    record = users / (trap_call + ".yaml")
                    record.write_bytes(trap)
                    refused_records[trap_call] = trap
                    protected = harness.Session(suite, trap_call)
                    startup, document = protected.human("PAUSE 300")
                    require("temporary defaults" in startup and
                            document["status"]["session"]["temporary_defaults"] is True,
                            "old alias guard refusal was lost at login")
                    text, _ = protected.human("PASS BAND 20")
                    require_save_warning(text)
                    require(record.read_bytes() == trap, "alias-refused file was overwritten")

                case(f"{transport}: compatible storage and old alias refusal reach login and LOAD", compatible_storage)

                def prepared_commit_faults():
                    prepared_call = "K1ABC-70011"
                    current = harness.Session(suite, prepared_call)
                    current.human("PAUSE 300")
                    current.human("SET NOISE QUIET")
                    current.human("SAVE PRESET OLD04B", "Saved preset OLD04B")
                    _, before = current.human("SET NOISE URBAN")
                    require(before["status"]["preset"] ==
                            dict(associated=True, name="OLD04B", modified=True),
                            "baseline/control change did not establish a distinct LOAD target")
                    record = root / "users" / (prepared_call + ".yaml")
                    saved = record.read_bytes()
                    library = root / "users" / "presets" / ("K1ABC".encode().hex() + ".yaml")

                    def preserved(document):
                        require(document["configuration"] == before["configuration"] and
                                document["revision"] == before["revision"] and
                                document["status"]["preset"] == before["status"]["preset"] and
                                document["status"]["effective"] == before["status"]["effective"] and
                                record.read_bytes() == saved,
                                "failed commit published or persisted a staged change")

                    with deny_record_replacement(record):
                        candidate = copy.deepcopy(before["configuration"])
                        candidate["settings"]["noise_class"] = "QUIET"
                        response = current.upload("PUT", "CONFIG", candidate, revision=before["revision"])
                        require(response.get("error", {}).get("code") == "persistence_failed",
                                "machine replacement fault did not reach the commit boundary")
                        preserved(current.get())
                        _, document = current.human("LOAD PRESET OLD04B", "LOAD PRESET failed:")
                        preserved(document)
                        _, document = current.human("SAVE PRESET NEW04B", "but could not persist its association")
                        preserved(document)
                        collection = harness.yaml.load(library.read_text(), Loader=harness.StrictLoader)
                        require(collection["presets"]["NEW04B"]["noise_class"] == "URBAN",
                                "later association I/O failure lost the successful library write")

                    # The record is writable for this control. A library failure
                    # must stop before any attempted association can alter it.
                    library_before = library.read_bytes()
                    with deny_record_replacement(library):
                        _, document = current.human("SAVE PRESET FAIL04B", "SAVE PRESET failed:")
                        preserved(document)
                        require(library.read_bytes() == library_before,
                                "failed library replacement altered the saved presets")
                    prepared_records[prepared_call] = (saved, before["configuration"], before["status"]["preset"])

                case(f"{transport}: real machine LOAD and SAVE commit faults preserve publication", prepared_commit_faults)

                def detached_human_edits():
                    for dialect, staged_call in (("go", "K1ABC-70012"), ("cc", "K1ABC-70013")):
                        current = harness.Session(suite, staged_call)
                        current.human("PAUSE 300")
                        current.human("DIALECT " + dialect)
                        current.human("SET DIAG SOURCE")
                        current.human("SET GRID FN31", "Grid set to FN31")
                        _, ordinary = current.human("PASS DXCONT EU", "Filter set: DX continents EU")
                        restoration = ordinary["configuration"]["filters"]["dx_continents"]
                        _, nearby = current.human("PASS NEARBY ON", "Nearby filter enabled")
                        require(nearby["status"]["effective"]["nearby_active"],
                                "NEARBY control did not become active")
                        record = root / "users" / (staged_call + ".yaml")
                        saved = record.read_bytes()
                        band = "PASS BAND" if dialect == "go" else "SET/FILTER BAND"
                        mode = "REJECT MODE" if dialect == "go" else "UNSET/FILTER MODE"
                        for line, response in ((band + " 20,NOTABAND", "Unknown band"),
                                               (mode + " CW,NOTAMODE", "Unknown mode"),
                                               ("PASS COMMENT", "Usage:"),
                                               ("SET NOISE NOTACLASS", "Unknown noise class"),
                                               ("PASS NEARBY MAYBE", "Usage:")):
                            _, refused = current.human(line, response)
                            require(refused["configuration"] == nearby["configuration"] and
                                    refused["revision"] == nearby["revision"] and
                                    refused["status"]["effective"] == nearby["status"]["effective"] and
                                    record.read_bytes() == saved,
                                    "refused detached edit affected live or durable state")

                        # Readable authoritative file and writable temp directory;
                        # only atomic replacement is refused after valid login.
                        with deny_record_replacement(record):
                            text, temporary = current.human("SET NOISE URBAN", "warning: failed to persist")
                            require(text.count("warning: failed to persist") == 1 and
                                    temporary["configuration"]["settings"]["noise_class"] == "URBAN" and
                                    temporary["status"]["effective"]["nearby_active"] and
                                    record.read_bytes() == saved,
                                    "human commit failure lost the live edit or NEARBY state")

                        _, changed_grid = current.human("SET GRID IO91", "Grid set to IO91")
                        require(changed_grid["status"]["effective"]["nearby_active"] and
                                changed_grid["status"]["effective"]["grid"] == "IO91",
                                "active GRID change lost NEARBY cells")
                        _, restored = current.human("PASS NEARBY OFF", "Nearby filter disabled")
                        require(restored["configuration"]["filters"]["dx_continents"] == restoration and
                                not restored["status"]["effective"]["nearby_active"],
                                "unrelated edits or GRID change lost restoration rules")
                        _, configured = current.human("SET SOLAR 15", "Solar summaries: every 15 minutes")
                        require(configured["status"]["session"]["diagnostic_comments"] == "SOURCE" and
                                configured["status"]["session"]["pause_active"],
                                "human publication changed diagnostics or read pause")
                        persisted = record.read_bytes()
                        current.human("SHOW DEDUPE", "Dedupe")
                        require(record.read_bytes() == persisted, "read-only command saved a record")
                        staged_records[staged_call] = configured["configuration"]

                case(f"{transport}: detached human edits preserve refusal and NEARBY restoration", detached_human_edits)
                case(f"{transport}: resource-refused login keeps fitting edits temporary",
                     lambda: resource_login_cases(suite, root, resource_refused))
                case(f"{transport}: warm resource refusals retry authoritative saves and logout",
                     lambda: resource_warm_cases(suite, root, resource_refused, resource_repaired))
                case(f"{transport}: prospective human SAVE machine and LOAD refuse before effects",
                     lambda: resource_prospective_case(suite, root, resource_prospective))
                case(f"{transport}: MODE deltas preserve visible history and saved fallback",
                     lambda: mode_delta_cases(suite, root, label, saved_modes))
                case(f"{transport}/0: NEARBY endpoints filter seeded archive independently",
                     lambda: nearby_endpoint_cases(suite, child))
                case(f"{transport}: GRID exact syntax preserves refusals and canonical publication",
                     lambda: grid_exact_cases(suite, root, saved_grids))
                case(f"{transport}: callsign patterns admit complete lists and preserve matching",
                     lambda: callsign_pattern_cases(suite, root, label, saved_callsigns))
                case(f"{transport}: original filter arguments refuse before normalization",
                     lambda: filter_argument_cases(suite, root, label, saved_arguments))
            else:
                def restored():
                    settings = session.get()["configuration"]["settings"]
                    require(settings["noise_class"] == "URBAN" and settings["grid"] == "FN31" and
                            settings["dialect"] == "cc", "preferences lost across process replacement")
                    require_archive(session, label)

                case(f"{transport}: preferences and archive survive fresh child", restored)

                def restored_nofilter():
                    observer = harness.Session(suite, observer_call)
                    observer.human("PAUSE 300")
                    document = observer.get()
                    require(document["configuration"] == observer_configuration,
                            "NOFILTER configuration lost across process replacement")
                    saved = harness.yaml.load((root / "users" / (observer_call + ".yaml")).read_text(),
                                              Loader=harness.StrictLoader)
                    require_unrestricted_geography(document, saved)
                    require_archive(observer, label)

                case(f"{transport}: NOFILTER geography survives fresh child", restored_nofilter)

                def restored_failures():
                    for failed_call, (saved, configuration) in failed_records.items():
                        record = root / "users" / (failed_call + ".yaml")
                        require(record.read_bytes() == saved, "teardown repaired failed persistence")
                        restored = harness.Session(suite, failed_call)
                        restored.human("PAUSE 300")
                        require(restored.get()["configuration"] == configuration,
                                "fresh child restored a previously unsaved change")
                        text, document = restored.human("SET GRID FN31")
                        require("warning: failed to persist" not in text and
                                document["configuration"]["settings"]["grid"] == "FN31",
                                "save did not recover after fault release")
                        saved_now = harness.yaml.load(record.read_text(), Loader=harness.StrictLoader)
                        require(saved_now["grid"] == "FN31", "later successful save not immediately durable")
                    for refused_call, raw in refused_records.items():
                        record = root / "users" / (refused_call + ".yaml")
                        require(record.read_bytes() == raw, "teardown overwrote refused record")
                        protected = harness.Session(suite, refused_call)
                        startup, document = protected.human("PAUSE 300")
                        require("temporary defaults" in startup and
                                document["status"]["session"]["temporary_defaults"] is True,
                                "fresh child did not protect refused record")
                        require(record.read_bytes() == raw, "fresh login overwrote refused record")

                case(f"{transport}: failed saves and refused records survive fresh child", restored_failures)

                def restored_compatible_storage():
                    for stored_call, configuration in compatible_records.items():
                        restored = harness.Session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" not in startup and
                                document["configuration"] == configuration,
                                "accepted legacy/exact storage did not survive a fresh process")
                    listing, _ = session.human("LIST PRESET", "LEGACY04A")
                    require("Failed" not in listing, "merged preset library was lost on restart")

                case(f"{transport}: compatible storage survives fresh child", restored_compatible_storage)

                def restored_prepared_commits():
                    for stored_call, (saved, configuration, preset_status) in prepared_records.items():
                        require((root / "users" / (stored_call + ".yaml")).read_bytes() == saved,
                                "teardown changed a failed transaction's stored record")
                        restored = harness.Session(suite, stored_call)
                        _, document = restored.human("PAUSE 300")
                        require(document["configuration"] == configuration and
                                document["status"]["preset"] == preset_status,
                                "fresh child restored a failed staged change or association")
                        listing, _ = restored.human("LIST PRESET", "NEW04B")
                        require("OLD04B" in listing and "FAIL04B" not in listing,
                                "library I/O outcomes did not survive replacement")

                case(f"{transport}: prepared commit outcomes survive fresh child", restored_prepared_commits)

                def restored_detached_humans():
                    for stored_call, configuration in staged_records.items():
                        restored = harness.Session(suite, stored_call)
                        _, document = restored.human("PAUSE 300")
                        require(document["configuration"] == configuration and
                                document["status"]["effective"]["grid"] == "IO91" and
                                document["status"]["effective"]["noise_class"] == "URBAN" and
                                not document["status"]["effective"]["nearby_active"],
                                "fresh child lost accepted detached human outcomes")

                case(f"{transport}: detached human settings survive fresh child", restored_detached_humans)

                def restored_resource_refusals():
                    require(len(resource_refused) == 4, "required resource refusal controls did not execute")
                    for stored_call, raw in resource_refused.items():
                        record = root / "users" / (stored_call + ".yaml")
                        require(record.read_bytes() == raw, "teardown changed resource-refused bytes")
                        restored = harness.Session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" in startup and
                                document["status"]["session"]["temporary_defaults"] is True and
                                record.read_bytes() == raw, "fresh child did not protect resource refusal")

                case(f"{transport}: resource-refused records survive fresh child", restored_resource_refusals)

                def restored_resource_repairs():
                    require(len(resource_repaired) == 4, "required authoritative retry controls did not execute")
                    for stored_call, (configuration, timestamp, ip) in resource_repaired.items():
                        record = root / "users" / (stored_call + ".yaml")
                        require_metadata(record.read_bytes(), timestamp, ip) # Before login changes timestamp.
                        restored = harness.Session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" not in startup and
                                document["status"]["session"]["temporary_defaults"] is False and
                                document["configuration"] == configuration,
                                "fresh child lost the admitted temporary edit or repair")

                case(f"{transport}: repaired resource metadata survives fresh child", restored_resource_repairs)

                def restored_prospective_admission():
                    require(len(resource_prospective) == 1, "prospective admission control did not execute")
                    library = stored_document(preset_library(root).read_bytes())["presets"]
                    require("QUOTANEW" not in library and library["QUOTAOLD"].get("comments", []) == [] and
                            library["QUOTALOAD"]["comments"] ==
                            ["LOAD TARGET GROWS BOTH CONFIGURATION AND BASELINE"],
                            "failed quota SAVE altered the library before restart")
                    for stored_call, (saved, configuration, preset_status) in resource_prospective.items():
                        record = root / "users" / (stored_call + ".yaml")
                        require(record.read_bytes() == saved, "teardown changed refused prospective state")
                        restored = harness.Session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" not in startup and
                                document["configuration"] == configuration and
                                document["status"]["preset"] == preset_status,
                                "fresh child restored a refused candidate or changed its baseline")

                case(f"{transport}: prospective admission outcomes survive fresh child", restored_prospective_admission)

                def restored_modes():
                    require(len(saved_modes) == 3, "required MODE controls did not execute")
                    for saved_call, (configuration, expected, cw_label, ft8_label, visible) in saved_modes.items():
                        current = harness.Session(suite, saved_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration, "MODE configuration lost across process replacement")
                        require_mode_rules(document, expected, root / "users" / (saved_call + ".yaml"))
                        require_mode_archive(current, cw_label, "CW", visible)
                        require_mode_archive(current, ft8_label, "FT8", visible)

                case(f"{transport}: MODE outcomes survive fresh child", restored_modes)
                case(f"{transport}/1: NEARBY endpoints filter seeded archive independently",
                     lambda: nearby_endpoint_cases(suite, child))

                def restored_grids():
                    require(len(saved_grids) == 2, "required GRID controls did not execute")
                    for stored_call, configuration in saved_grids.items():
                        current = harness.Session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration and
                                document["status"]["effective"]["nearby_active"] is True,
                                "fresh child lost canonical GRID or active NEARBY")
                        require_grid_configuration(document, "IO91", root / "users" / (stored_call + ".yaml"))

                case(f"{transport}: canonical GRID and active cells survive fresh child", restored_grids)

                def restored_callsigns():
                    require(len(saved_callsigns) == 2, "required callsign controls did not execute")
                    for stored_call, (configuration, expected) in saved_callsigns.items():
                        current = harness.Session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration,
                                "fresh child lost callsign configuration")
                        require_callsign_rules(document, expected, root / "users" / (stored_call + ".yaml"))
                        require_callsign_archive(current, label, True)

                case(f"{transport}: callsign patterns and matching survive fresh child", restored_callsigns)

                def restored_arguments():
                    require(len(saved_arguments) == 2, "required original-argument controls did not execute")
                    for stored_call, configuration in saved_arguments.items():
                        current = harness.Session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration,
                                "fresh child lost supported filter argument selections")
                        require_callsign_archive(current, label, True)

                case(f"{transport}: supported filter arguments survive fresh child", restored_arguments)
        finally:
            try:
                if suite:
                    for session in suite.sessions:
                        try:
                            session.close()
                        except (OSError, ValueError, EOFError) as error:
                            suite.cleanup_errors.append(str(error))
                    suite.log.close()
                    results.extend(suite.results)
                    wire.extend(suite.wire_failures)
                    cleanup.extend(suite.cleanup_errors)
                if child:
                    try:
                        child.stop()
                    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                        cleanup.append(str(error))
            finally:
                try:
                    replacement_faults.close()
                except (OSError, ValueError) as error:
                    cleanup.append(str(error))
    report = {"transport": transport, "required": required, "results": results,
              "wire_failures": wire, "cleanup_errors": cleanup, "children": children}
    (root / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    qualified(results, required, wire, cleanup)
    return report


def reputation_profile(binary, root, source, transport):
    """Separate loaded-gate profile; counts are intentionally runtime-only."""
    prefix = transport + "/reputation"
    required = [f"{prefix}/0: held endpoint includes the loaded shared gate",
                f"{prefix}: full-SSID settings save independently",
                f"{prefix}: invalid frequencies leave station allowance available",
                f"{prefix}: spent station allowances survive other-SSID reconnects",
                f"{prefix}: baseline history keeps old SSID files untouched",
                f"{prefix}/1: saved ownership and archive outcomes survive fresh child",
                f"{prefix}: fresh runtime resets counts while retaining shared admission"]
    results, wire, cleanup, children = [], [], [], []
    calls = ("K1ABC-70101", "K1ABC-70102")
    expected, labels, old_files = {}, {"accepted": [], "refused": [], "invalid": []}, {}
    directory = root / "reputation"
    directory.mkdir()
    # Existing baseline histories establish eligible first-post controls through
    # the real loaded initial-wait policy. Old SSID evidence is distinct and
    # must never enter either baseline record or be overwritten by login.
    for call, asn, country, continent in (("K1ABC", "AS64500", "US", "NA"),
                                          ("VE3ABC", "AS64500", "CA", "NA"),
                                          (calls[0], "AS64501", "CA", "NA"),
                                          (calls[1], "AS64502", "FR", "EU")):
        raw = (f"recent_asns: [{asn}]\nrecent_countries: [{country}]\n"
               f"recent_continents: [{continent}]\nupdated_at: 2000-01-01T00:00:00Z\n").encode("ascii")
        path = directory / (call + ".yaml")
        path.write_bytes(raw)
        if call in calls:
            old_files[path] = raw
    for generation in range(2):
        child, suite = None, None
        try:
            child = Child(binary, root, source, reputation_enabled=True)
            children.append(child.identity)
            suite = harness.Suite(SimpleNamespace(host=child.host, port=child.port, call="K1ABC",
                                                  timeout=8, run_seconds=120,
                                                  output=root / f"wire-{generation}"))
            sessions = [harness.Session(suite, call) for call in calls]
            observer = harness.Session(suite, "VE3ABC-70103")

            def case(name, fn):
                suite.case(name, fn)
                child.check_running()

            if generation == 0:
                def identity():
                    private = stored_document((root / "config" / "reputation.yaml").read_bytes())["reputation"]
                    require(private["enabled"] is True and private["initial_wait_seconds"] == 60 and
                            private["ramp_window_seconds"] == 3600 and private["per_band_start"] == 1 and
                            private["per_band_cap"] == 1 and private["total_cap_start"] == 2 and
                            private["total_cap_post_ramp"] == 2 and
                            all(private[key] is False for key in
                                ("ipinfo_download_enabled", "ipinfo_api_enabled", "fallback_team_cymru")),
                            "private reputation profile lost its loaded quota or isolation controls")
                    for current in (*sessions, observer):
                        current.human("PAUSE 300")
                        current.human("SHOW BUILD", "Build version: fixture-" + child.nonce)
                        text, _ = current.human("SHOW OWN", "Login call: " + current.call)
                        require("Own call: " + current.call.split("-")[0] in text,
                                "displayed own-call identity retained the numeric SSID")

                case(required[0], identity)

                def preferences():
                    for current, noise, grid, dialect in zip(sessions, ("QUIET", "URBAN"),
                                                            ("FN31", "IO91"), ("go", "cc")):
                        current.human("SET NOISE " + noise, "Noise class set")
                        current.human("SET GRID " + grid, "Grid set")
                        _, document = current.human("DIALECT " + dialect)
                        expected[current.call] = document["configuration"]
                    require(expected[calls[0]] != expected[calls[1]], "SSID ownership controls are identical")
                    require_reputation_preferences(root, sessions, expected)

                case(required[1], preferences)

                def invalid_frequencies():
                    invalid_frequency_posts(suite, sessions, labels)
                    require(len(labels["invalid"]) == 5, "required frequency refusal controls missing")
                    require_reputation_preferences(root, sessions, expected)

                case(required[2], invalid_frequencies)

                def exhausted():
                    require(len(expected) == 2, "SSID preference controls did not execute")
                    reputation_posts(suite, sessions, observer, generation, labels)
                    require_reputation_preferences(root, sessions, expected)

                case(required[3], exhausted)
                case(required[4], lambda: require_reputation_history(root, old_files))
            else:
                def restored():
                    require(len(expected) == 2 and len(labels["accepted"]) == 2 and
                            len(labels["refused"]) == 5 and len(labels["invalid"]) == 5,
                            "required first-generation admission controls missing")
                    for current in (*sessions, observer):
                        current.human("PAUSE 300")
                        current.human("SHOW BUILD", "Build version: fixture-" + child.nonce)
                    require_reputation_preferences(root, sessions, expected)
                    require_reputation_history(root, old_files)
                    for label, frequency in labels["accepted"]:
                        require_reputation_archive(observer, label, frequency)
                    # Generation zero has exited and drained before this query:
                    # rejected absence cannot be hidden in its pending queue.
                    for label in labels["refused"] + labels["invalid"]:
                        require_archive_absent(archive_rows(observer, label))

                case(required[5], restored)

                def runtime_reset():
                    reputation_posts(suite, sessions, observer, generation, labels)
                    require_reputation_preferences(root, sessions, expected)
                    require_reputation_history(root, old_files)

                case(required[6], runtime_reset)
        finally:
            try:
                if suite:
                    for current in suite.sessions:
                        try:
                            current.close()
                        except (OSError, ValueError, EOFError) as error:
                            suite.cleanup_errors.append(str(error))
                    suite.log.close()
                    results.extend(suite.results)
                    wire.extend(suite.wire_failures)
                    cleanup.extend(suite.cleanup_errors)
            finally:
                if child:
                    try:
                        child.stop()
                    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                        cleanup.append(str(error))
    report = {"transport": transport, "profile": "reputation", "required": required,
              "results": results, "wire_failures": wire, "cleanup_errors": cleanup,
              "children": children, "runtime_counts": "reset on fresh child; not persisted"}
    (root / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    qualified(results, required, wire, cleanup)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--race", action="store_true", help="build and run fixture with Go race detector")
    args = parser.parse_args()
    run = plain_output(ROOT / ".tmp" / "command-fixes" / (time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]))
    run.mkdir(parents=True, exist_ok=False)
    report = {"race": args.race, "profiles": [], "result": "FAIL"}
    try:
        with runner_lock():
            binary, source = build(args.race)
            report["source"] = source
            report["tools"] = {"go": command(["go", "version"]), "python": sys.version,
                               "yaml": harness.yaml.__version__}
            require(harness.yaml.__version__ == "6.0.3", "use pinned PyYAML 6.0.3 test environment")
            for transport in ("native", "ziutek"):
                root = stage(run / transport, transport)
                report["profiles"].append(profile(binary, root, source, transport))
                reputation_root = stage(run / (transport + "-reputation"), transport, reputation_enabled=True)
                report["profiles"].append(reputation_profile(binary, reputation_root, source, transport))
        report["result"] = "PASS"
    except (OSError, ValueError, EOFError, TimeoutError, subprocess.TimeoutExpired, harness.yaml.YAMLError) as error:
        report["error"] = str(error)
    finally:
        (run / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"result": report["result"], "output": str(run), "error": report.get("error")}, indent=2))
    return int(report["result"] != "PASS")


if __name__ == "__main__":
    raise SystemExit(main())
