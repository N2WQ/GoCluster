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
import math
from pathlib import Path
import re
import subprocess
import time
from types import SimpleNamespace
import uuid

from command_fixture_support import (Child, ROOT, build, command, deny_record_replacement,
                                     plain_output, require, resource_fixture, runner_lock, stage, yaml)

spec = importlib.util.spec_from_file_location("checkout_telnet_harness", ROOT / "scripts" / "test-telnet-commands.py")
harness = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = harness
spec.loader.exec_module(harness)


# Allocate positive logins once. Exact spelling owns settings; reconnects reuse
# an allocation, while independent cases get distinct full identities.
FIXTURE_LOGINS = {
    "main": tuple(f"{base}-{number:02d}" for base, numbers in (
        ("K1ABC", (1, 2, *range(4, 14), *range(21, 30), 34, *range(50, 56))),
        ("VE3ABC", (3, *range(33, 36), *range(40, 50), 56, 57, *range(63, 72), *range(73, 81))),
    ) for number in numbers),
    "dedupe": ("K1ABC-58", "K1ABC-59", "VE3ABC-60", "VE3ABC-61", "VE3ABC-62"),
    "reputation": ("K1ABC-81", "K1ABC-82", "VE3ABC-83"),
}


def validate_fixture_calls(inventory):
    require(0 < len(inventory) <= 3, "invalid fixture profile inventory")
    for profile, calls in inventory.items():
        require(profile in FIXTURE_LOGINS and 0 < len(calls) <= 200,
                "invalid fixture login allocation bound")
        seen = set()
        for call in calls:
            require(isinstance(call, str) and re.fullmatch(r"(?:K1ABC|VE3ABC)-[0-9]{1,2}", call),
                    f"invalid fixture login: {call!r}")
            require(call not in seen, f"duplicate fixture login allocation: {call}")
            seen.add(call)


def fixture_session(suite, call):
    require(any(call in calls for calls in FIXTURE_LOGINS.values()),
            f"unallocated fixture login: {call!r}")
    current = FixtureSession(suite, call)
    # The reusable client paces logins for public nodes. This private fixture
    # explicitly loads 100 accepts/second with a 100-token burst; remain below
    # that unchanged server limit without spending the case budget sleeping.
    suite.next_connection_at = time.monotonic() + .02
    return current


class FixtureSession(harness.V7Session):
    """Retain the existing client's consumed login text as failure evidence."""

    def send(self, text, count=True):
        if not count:
            self.sock.setsockopt(harness.socket.IPPROTO_TCP, harness.socket.TCP_NODELAY, 1)
        super().send(text, count)
        if not count:
            # The V7 client owns login and its bounds. Capture its normal next
            # until() result, without performing another read or replaying input.
            original = self.reader.until

            def login_receipt(marker):
                self.reader.until = original
                value = original(marker)
                self.login_greeting = value
                self.suite.log.write(f"\n[{self.call}] login\n".encode() + value)
                self.suite.log.flush()
                return value
            self.reader.until = login_receipt

    def remember_configuration(self, document):
        configuration, status = document.get("configuration", {}), document.get("status", {})
        if (document.get("resource") == "CONFIG" and document.get("schema_version") == 7 and
                "error" not in document and isinstance(configuration, dict) and
                isinstance(configuration.get("filters"), dict) and set(configuration["filters"]) == {"go", "cc"} and
                isinstance(configuration.get("settings"), dict) and isinstance(status, dict) and
                isinstance(status.get("presets"), dict) and set(status["presets"]) == {"go", "cc"}):
            # This fixture is the only command writer for the connection. A
            # complete correlated reply already fences its previous command;
            # retain one detached snapshot for the next adjacent edit check.
            self.last_configuration = copy.deepcopy(document)

    def frame(self, resource, identifier, version, command):
        document, prefix = super().frame(resource, identifier, version, command)
        self.remember_configuration(document)
        return document, prefix


def login_text(current, remainder):
    return current.login_greeting.decode("ascii", "strict") + remainder


class CommandSuite(harness.Suite):
    """Disposable source-bound cases own every session they create.

    The profile's initial shared sessions predate case entry. New sessions are
    retired even after an assertion failure, so the first defect cannot exhaust
    admission and hide the remaining evidence. Cleanup errors remain failures.
    """

    @contextlib.contextmanager
    def retirement_deadline(self):
        original = self.deadline
        if time.monotonic() >= original:
            self.deadline = time.monotonic() + 30
        try:
            yield
        finally:
            self.deadline = original

    def case(self, name, operation, required=None):
        started = time.monotonic()
        existing = {id(current) for current in self.sessions}

        def owned_operation():
            try:
                operation()
            finally:
                with self.retirement_deadline():
                    for current in self.sessions:
                        if id(current) not in existing:
                            try:
                                current.close()
                            except Exception as error:
                                self.cleanup_errors.append(f"{name}: {error}")
        super().case(name, owned_operation, required)
        self.results[-1]["elapsed_seconds"] = round(time.monotonic() - started, 3)


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


def archive_rows(session, label, selection=""):
    # These zero/one-row queries do not exercise the ten-row automatic pause trailer.
    query = f"SHOW DX W6ABC 10{(' ' + selection) if selection else ''} COMMENT {label}"
    result = []
    for rows, more, _, _ in history_pages(session, query, "SHOW DX NEXT"):
        require(not any(row in result for row in rows) and len(rows) == len(set(rows)),
                "archive selection repeated a row")
        result.extend(rows)
        if not more:
            return result


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


def active_fixture_bank(document):
    return document["configuration"]["filters"][document["status"]["active_dialect"]]


def require_fixture_persisted(document, record):
    saved = copy.deepcopy(stored_configuration(record.read_bytes()))
    # The native disk field is optional when false; schema seven writes it
    # explicitly. Compare the complete preference value, including both banks.
    saved["settings"].setdefault("cw_speed", False)
    require(saved == document["configuration"], "native preferences were not saved before disconnect")
    require_cc_typed_value(saved, document["configuration"])


def require_fixture_unchanged(before, after, record, saved):
    require(after["configuration"] == before["configuration"] and
            after["revision"] == before["revision"] and
            after["status"]["effective"] == before["status"]["effective"] and
            after["status"]["presets"] == before["status"]["presets"] and
            record.read_bytes() == saved,
            "refused/read-only operation changed preferences, either baseline, runtime or stored bytes")
    require_cc_typed_value(after["configuration"], before["configuration"])
    require_cc_typed_value(after["status"]["presets"], before["status"]["presets"])


def require_fixture_refusal(session, command, record, marker="No changes applied."):
    before, saved = fixture_before(session), record.read_bytes()
    text, after = session.human(command, marker)
    require("No changes applied." in text, "retired or malformed filter command lacks an explicit refusal")
    require_fixture_unchanged(before, after, record, saved)
    return after


def fixture_edit(session, command, record):
    before = fixture_before(session)
    references = stored_document(record.read_bytes())["presets"]
    text, after = session.human(command)
    require(text.strip() and "No changes applied." not in text and "warning: failed to persist" not in text and
            "Unknown command" not in text and "Usage:" not in text,
            "positive native command was refused or not saved: " + command)
    inactive = "cc" if before["status"]["active_dialect"] == "go" else "go"
    require(after["configuration"]["filters"][inactive] == before["configuration"]["filters"][inactive],
            "ordinary command changed the inactive filter bank")
    require_fixture_persisted(after, record)
    require_cc_typed_value(stored_document(record.read_bytes())["presets"], references)
    return after


def fixture_before(session):
    before = getattr(session, "last_configuration", None)
    return before if isinstance(before, dict) else session.get()


def reconnect_fixture_session(session, record):
    # Semantic groups use the same durable owner on a fresh connection. Never
    # retry a failed command, change command pacing, or hide unsaved outcomes.
    before = session.get()
    require_fixture_persisted(before, record)
    references = stored_document(record.read_bytes())["presets"]
    session.close()
    restored = fixture_session(session.suite, session.call)
    _, after = restored.human("PAUSE 300")
    require_cc_typed_value(after["configuration"], before["configuration"])
    require_cc_typed_value(after["status"]["presets"], before["status"]["presets"])
    require_fixture_persisted(after, record)
    require_cc_typed_value(stored_document(record.read_bytes())["presets"], references)
    return restored


def require_mode_rules(document, expected, record=None):
    require(active_fixture_bank(document) == expected, "MODE delta lost its exact native bank")
    require_cc_typed_value(active_fixture_bank(document), expected)
    if record is not None:
        require_fixture_persisted(document, record)


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
            stored_configuration(record.read_bytes())["settings"]["grid"] == grid,
            "GRID canonical selection, effective location or saved bytes disagree")
    require_fixture_persisted(document, record)


def require_callsign_rules(document, expected, record):
    require(active_fixture_bank(document) == expected, "callsign rules or native CC bank changed")
    require_cc_typed_value(active_fixture_bank(document), expected)
    require_fixture_persisted(document, record)


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
    return reply, complete_reply_config(session, received, "human refusal")


def callsign_pattern_cases(suite, root, cw_label, saved_callsigns):
    for dialect, call in (("go", "VE3ABC-44"), ("cc", "VE3ABC-45")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        record = root / "users" / (call + ".yaml")
        if dialect == "cc":
            fixture_edit(current, "SET/NOFILTER", record)
            for domain in ("DXCALL", "DECALL"):
                for verb in ("SET/FILTER", "UNSET/FILTER", "PASS", "REJECT"):
                    for values in ("w6*,*abc,k1abc,ea8/w1xyz,w2abc-1", "N0XYZ,W*ABC"):
                        require_fixture_refusal(current, verb + " " + domain + " " + values, record)
                current = reconnect_fixture_session(current, record)
            # Native CC has no arbitrary callsign condition. Refusal must leave
            # the real matching row visible and the inactive GO bank untouched.
            require_callsign_archive(current, cw_label, True)
        else:
            fixture_edit(current, "RESET FILTER RULE ALL", record)
            for domain, matching, prefix in (("DXCALL", "W6ABC", "w6*"), ("DECALL", "K1ABC", "k1*")):
                fixture_edit(current, "RESET FILTER RULE ALL", record)
                fixture_edit(current, "PASS RULE 0 " + domain + "=w6*,*abc,k1abc,ea8/w1xyz,w2abc-1", record)
                require_callsign_archive(current, cw_label, True)
                for action in ("PASS", "REJECT"):
                    for bad_list in ("N0XYZ,W*ABC", "*ABC*,N0XYZ", "N0XYZ,W1?", "W1.ABC,N0XYZ"):
                        require_fixture_refusal(current, action + " RULE 9 " + domain + "=" + bad_list,
                                                record, "Invalid rule:")
                        require_callsign_archive(current, cw_label, True)
                    current = reconnect_fixture_session(current, record)
                fixture_edit(current, "REJECT RULE 1 " + domain + "=" + matching, record)
                require_callsign_archive(current, cw_label, False)
                fixture_edit(current, "PASS RULE 0 " + domain + "=*", record)
                require_callsign_archive(current, cw_label, False)  # REJECT vetoes PASS.
                fixture_edit(current, "RESET FILTER RULE 1", record)
                require_callsign_archive(current, cw_label, True)
                fixture_edit(current, "PASS RULE 0 " + domain + "=" + prefix, record)
                require_callsign_archive(current, cw_label, True)
                current = reconnect_fixture_session(current, record)
            fixture_edit(current, "RESET FILTER RULE ALL", record)
            fixture_edit(current, "PASS RULE 0 DXCALL=w6* AND DECALL=k1*", record)
            document = fixture_edit(current, "REJECT RULE 1 DXCALL=N0XYZ OR DECALL=N0XYZ", record)
            require(active_fixture_bank(document)["rules"] == [
                dict(slot=0, action="pass", expr="DXCALL=W6* AND DECALL=K1*"),
                dict(slot=1, action="reject", expr="DXCALL=N0XYZ OR DECALL=N0XYZ")],
                "callsign lists lost exact canonical definitions")
            for old in ("PASS DXCALL W6*", "REJECT DECALL N0XYZ"):
                require_fixture_refusal(current, old, record, "Use numbered rules:")
        document = current.get()
        expected = copy.deepcopy(active_fixture_bank(document))
        require_callsign_rules(document, expected, record)
        require_callsign_archive(current, cw_label, True)
        result = current.upload("VALIDATE", "CONFIG", document["configuration"])
        require("error" not in result and result["result"]["operation"] == "VALIDATE" and
                result["result"]["valid"] is True and result["result"]["applied"] is False and
                result["result"]["persisted"] is False, "human callsign configuration is machine-invalid")
        saved_callsigns[call] = (document["configuration"], expected)


def require_filter_argument_refusal(session, command, record, exact_feedback=None):
    if exact_feedback is None:
        fields = command.upper().split()
        if fields[0].startswith("SET/") and fields[0] != "SET/FILTER":
            exact_feedback = "Usage: " + fields[0] + "\nNo changes applied."
        elif fields[0] == "SET/FILTER" and "/" in fields[1]:
            exact_feedback = "Usage: SET/FILTER " + fields[1] + "\nNo changes applied."
        else:
            domain = "ANNOUNCE" if fields[1] == "PC93" else fields[1]
            exact_feedback = domain + " accepts no value or ALL. No changes applied."
    require_command_refusal(session, command, record, exact_feedback)


def require_command_refusal(session, command, record, exact_feedback):
    before, saved = fixture_before(session), record.read_bytes()
    text, after = exact_human_reply(session, command, exact_feedback)
    require(text.strip() == exact_feedback, "malformed filter arguments lack a refusal")
    require_fixture_unchanged(before, after, record, saved)
    return after


require_remote_logout = harness.remote_logout


def require_setting_argument_refusal(session, command, record, feedback):
    mode = fixture_before(session)["status"]["session"]["diagnostic_comments"]
    after = require_command_refusal(session, command, record, feedback)
    require(after["status"]["session"]["diagnostic_comments"] == mode,
            "malformed setting arguments changed the session diagnostic mode")


def require_setting_control(session, record, command, feedback, field, value):
    before, saved = fixture_before(session), record.read_bytes()
    text, after = exact_human_reply(session, command, feedback)
    require(text.strip() == feedback, "valid setting lacks exact feedback")
    if field == "diagnostic_comments":
        require(after["status"]["session"][field] == value and
                after["configuration"] == before["configuration"] and after["revision"] == before["revision"] and
                after["status"]["effective"] == before["status"]["effective"] and
                after["status"]["presets"] == before["status"]["presets"] and record.read_bytes() == saved,
                "diagnostic control lost session mode or changed persistent state")
    else:
        stored = stored_configuration(record.read_bytes())["settings"]
        stored_value = stored.get(field)
        configured_value = after["configuration"]["settings"][field]
        require(type(configured_value) is type(value) and configured_value == value and
                type(stored_value) is type(value) and stored_value == value,
                "valid setting lacks its exact configured or immediately saved value")
        if field not in ("path_min_observation_count", "solar_summary_minutes"):
            require(after["status"]["effective"][field] == value, "valid setting did not reach runtime state")
        require_fixture_persisted(after, record)
    return after


def fixed_setting_cases(suite, root, saved_settings):
    for dialect, call in (("go", "VE3ABC-56"), ("cc", "VE3ABC-57")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        current.human("SET NOISE RURAL")
        current.human("SET GRID FN31")
        current.human("SET PATHSAMPLES 30", "Path sample minimum set to 30 (cluster default 21)")
        current.human("SET SOLAR 30")
        current.human("SET DEDUPE FAST")
        current.human("SET DIAG SOURCE")
        record = root / "users" / (call + ".yaml")
        for prefix, tails, feedback in (
                ("DIALECT", (" GO EXTRA", " CC EXTRA", " LIST EXTRA", " UNKNOWN EXTRA"), "Usage: DIALECT [LIST|go|cc]"),
                ("SET GRID", ("", " FN31PR EXTRA"), "Usage: SET GRID <4 or 6 character Maidenhead locator>"),
                ("SET NOISE", ("", " URBAN EXTRA"), "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>"),
                ("SET PATHSAMPLES", ("", " 40 EXTRA", " DEFAULT EXTRA"), "Usage: SET PATHSAMPLES <count|DEFAULT>"),
                ("SET SOLAR", ("", " 15 EXTRA", " 30 EXTRA", " OFF EXTRA"), "Usage: SET SOLAR <15|30|60|OFF>"),
                ("SET DEDUPE", ("", " SLOW EXTRA"), "Usage: SET DEDUPE <FAST|MED|SLOW>"),
                ("SHOW DEDUPE", (" EXTRA",), "Usage: SHOW DEDUPE"),
                ("SET DIAG", ("", " OFF EXTRA", " CONFIDENCE EXTRA"), "Usage: SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>")):
            for tail in tails:
                require_setting_argument_refusal(current, prefix.lower() + tail, record, feedback)
            current = reconnect_fixture_session(current, record)
        require(current.get()["status"]["server"]["path_min_observation_count"] == 21,
                "loaded fixture path floor differs from literal positive control")
        for alias in ("go", "gocluster", "classic", "default", "cc"):
            value = "cc" if alias == "cc" else "go"
            unchanged = current.get()["configuration"]["settings"]["dialect"] == value
            feedback = ("Dialect already set to " if unchanged else "Dialect set to ") + value.upper()
            require_setting_control(current, record, "DIALECT " + alias, feedback, "dialect", value)
        exact_human_reply(current, "DIALECT", "Current dialect: CC")
        exact_human_reply(current, "DIALECT LIST", "Available dialects: CC, GO")
        current = reconnect_fixture_session(current, record)
        for grid in ("FN31", "FN31PR"):
            require_setting_control(current, record, "SET GRID " + grid, "Grid set to " + grid, "grid", grid)
        for noise in ("QUIET", "RURAL", "SUBURBAN", "URBAN", "INDUSTRIAL"):
            require_setting_control(current, record, "SET NOISE " + noise, "Noise class set to " + noise, "noise_class", noise)
        current = reconnect_fixture_session(current, record)
        for token, value, feedback in (("30", 30, "Path sample minimum set to 30 (cluster default 21)"),
                                        ("DEFAULT", 0, "Path sample minimum reset to cluster default 21"),
                                        ("30", 30, "Path sample minimum set to 30 (cluster default 21)")):
            after = require_setting_control(current, record, "SET PATHSAMPLES " + token, feedback, "path_min_observation_count", value)
            require(after["status"]["effective"]["path_min_observation_count"] == (30 if value else 21),
                    "path sample control did not reach the runtime floor")
        current = reconnect_fixture_session(current, record)
        for policy, key in (("FAST", "grid2"), ("MED", "grid2"), ("SLOW", "cqzone")):
            require_setting_control(current, record, "SET DEDUPE " + policy, "Dedupe policy set to " + policy, "dedupe_policy", policy)
            exact_human_reply(current, "SHOW DEDUPE", f"Dedupe: {policy} ({key}) (fast=on med=on slow=on)")
        current = reconnect_fixture_session(current, record)
        for token, value in (("15", 15), ("30", 30), ("60", 60), ("OFF", 0), ("30", 30)):
            feedback = "Solar summaries: OFF" if not value else f"Solar summaries: every {value} minutes when conditions are active"
            require_setting_control(current, record, "SET SOLAR " + token, feedback, "solar_summary_minutes", value)
        current = reconnect_fixture_session(current, record)
        for token, canonical in (("OFF", "OFF"), ("DEDUPE", "DEDUPE"), ("SOURCE", "SOURCE"),
                                  ("CONF", "CONF"), ("CONFIDENCE", "CONF"), ("PATH", "PATH"), ("MODE", "MODE")):
            require_setting_argument_refusal(current, "SET DIAG " + token + " EXTRA", record,
                                             "Usage: SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>")
            require_setting_control(current, record, "SET DIAG " + token, "Diagnostic comments: " + canonical,
                                    "diagnostic_comments", canonical)
            # DIAG is intentionally session-only; exercise the next token on
            # its own connection rather than expecting diagnostics to persist.
            current = reconnect_fixture_session(current, record)
        unchanged = current.get()["configuration"]["settings"]["dialect"] == dialect
        require_setting_control(current, record, "DIALECT " + dialect,
                                ("Dialect already set to " if unchanged else "Dialect set to ") + dialect.upper(), "dialect", dialect)
        saved_settings[call] = current.get()["configuration"]


def command_arity_cases(suite, root, saved_arity):
    for index, (dialect, call) in enumerate((("go", "VE3ABC-48"), ("cc", "VE3ABC-49"))):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        current.human("SET DEDUPE FAST")
        record = root / "users" / (call + ".yaml")
        for prefix in ("SHOW DXCC", "SH DXCC"):
            for tail in ("", " VE EXTRA", " W6/LZ5VV/M EXTRA MORE"):
                require_command_refusal(current, prefix + tail, record, "Usage: SHOW DXCC <prefix|callsign>")
            current.human(prefix + " VE", "Canada")
            current = reconnect_fixture_session(current, record)
        for verb in ("BYE", "QUIT", "EXIT"):
            for tail in (" NOW", " ALL MORE"):
                require_command_refusal(current, verb.lower() + tail, record, "Usage: " + verb)
                # Keep using the original connection after each invalid logout.
                text, _ = current.human("SHOW OWN", "Login call: " + call)
                require("Own call: VE3ABC" in text, "baseline own-call identity changed after refused logout")
            current = reconnect_fixture_session(current, record)
        saved_arity[call] = current.get()["configuration"]
        for offset, verb in enumerate(("BYE", "QUIT", "EXIT")):
            terminal = fixture_session(suite, "K1ABC-" + str(50 + index * 3 + offset))
            terminal.human("PAUSE 300")
            terminal.human("DIALECT " + dialect)
            require_remote_logout(terminal, verb)


def filter_argument_cases(suite, root, cw_label, saved_arguments):
    switches = dict(BEACON="include_beacons", WWV="allow_wwv", WCY="allow_wcy",
                    ANNOUNCE="allow_announce", SELF="allow_self", TOXIC="allow_toxic")
    for dialect, call in (("go", "VE3ABC-46"), ("cc", "VE3ABC-47")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        allow, block = ("PASS", "REJECT") if dialect == "go" else ("SET/FILTER", "UNSET/FILTER")
        record = root / "users" / (call + ".yaml")
        for domain, key in switches.items():
            for verb, opposite, enabled in ((allow, block, True), (block, allow, False)):
                fixture_edit(current, opposite + " " + domain, record)
                for tail in ("OFF", "ALL OFF", "ALL ALL"):
                    require_filter_argument_refusal(current, verb + " " + domain + " " + tail, record)
                document = fixture_edit(current, verb + " " + domain + " ALL", record)
                require(active_fixture_bank(document)[key] is enabled,
                        "valid switch operation lost its live or saved direction")
                current = reconnect_fixture_session(current, record)
        if dialect == "go":
            for old in ("PASS NOFILTER OFF", "PASS NOFILTER ALL", "REJECT BAND ALL",
                        "PASS MODE CW", "SET/FILTER BAND/ON", "SET/FILTER DXCALL/OFF"):
                require_fixture_refusal(current, old, record, "Use numbered rules:")
            current = reconnect_fixture_session(current, record)
            fixture_edit(current, "RESET FILTER RULE ALL", record)
            fixture_edit(current, "PASS RULE 0 MODE=CW", record)
            for malformed in ("PASS RULE 0 MODE=CW EXTRA", "REJECT RULE 1 BAND=20M ALL",
                              "PASS RULE 01 MODE=CW", "RESET FILTER RULE ALL EXTRA"):
                require_fixture_refusal(current, malformed, record)
            require_callsign_archive(current, cw_label, True)
            document = fixture_edit(current, "REJECT RULE 1 MODE=FT8", record)
            require(active_fixture_bank(document)["rules"] == [
                dict(slot=0, action="pass", expr="MODE=CW"), dict(slot=1, action="reject", expr="MODE=FT8")],
                "GO replacement rule arguments were not retained exactly")
        else:
            for yes, no, key in (("SET/ANN", "SET/NOANN", "allow_announce"),
                                 ("SET/BEACON", "SET/NOBEACON", "include_beacons"),
                                 ("SET/WWV", "SET/NOWWV", "allow_wwv"),
                                 ("SET/WCY", "SET/NOWCY", "allow_wcy"),
                                 ("SET/SELF", "SET/NOSELF", "allow_self")):
                for command, opposite, enabled in ((yes, no, True), (no, yes, False)):
                    fixture_edit(current, opposite, record)
                    for tail in ("ON", "ALL"):
                        require_fixture_refusal(current, command + " " + tail, record, "Usage:")
                    document = fixture_edit(current, command, record)
                    require(active_fixture_bank(document)[key] is enabled, "CC shortcut changed the wrong control")
                    current = reconnect_fixture_session(current, record)
            for command, opposite, enabled in (("SET/SKIMMER", "SET/NOSKIMMER", True),
                                               ("SET/NOSKIMMER", "SET/SKIMMER", False)):
                fixture_edit(current, opposite, record)
                require_fixture_refusal(current, command + " ALL", record, "Usage:")
                document = fixture_edit(current, command, record)
                require(("SKIMMER" in active_fixture_bank(document)["sources"]) is enabled,
                        "CC skimmer shortcut changed the wrong source")
                current = reconnect_fixture_session(current, record)
            fixture_edit(current, "SET/NOFILTER", record)
            for mode in ("CW", "FT8"):
                for command, opposite in (("SET/" + mode, "SET/NO" + mode),
                                          ("SET/NO" + mode, "SET/" + mode)):
                    fixture_edit(current, opposite, record)
                    require_fixture_refusal(current, command + " CW", record, "Usage:")
                    document = fixture_edit(current, command, record)
                    require((mode in active_fixture_bank(document)["modes"]) is (command == "SET/" + mode),
                            "CC mode shortcut changed the wrong selection")
                current = reconnect_fixture_session(current, record)
            fixture_edit(current, "SET/CW", record)
            require_callsign_archive(current, cw_label, True)
            fixture_edit(current, "SET/NOCW", record)
            require_callsign_archive(current, cw_label, False)
            fixture_edit(current, "SET/CW", record)
            for old in ("SET/FILTER BAND/ON", "SET/FILTER MODE/OFF", "SET/FILTER DXCALL/ON",
                        "SET/FILTER DECALL/OFF", "SET/FILTER NOFILTER/ON", "SET/FILTER NOFILTER/OFF",
                        "PASS NOFILTER ALL", "PASS RULE 0 MODE=CW"):
                require_fixture_refusal(current, old, record)
            current = reconnect_fixture_session(current, record)
            for domain in ("BEACON", "TOXIC"):
                for flag, opposite in (("OFF", "ON"), ("ON", "OFF")):
                    fixture_edit(current, "SET/FILTER " + domain + "/" + opposite, record)
                    for tail in ("ALL", "20 CW"):
                        require_filter_argument_refusal(current, "SET/FILTER " + domain + "/" + flag + " " + tail, record)
                    document = fixture_edit(current, "SET/FILTER " + domain + "/" + flag, record)
                    require(active_fixture_bank(document)[switches[domain]] is (flag == "ON"),
                            "CC inline feature flag lost its direction")
                    current = reconnect_fixture_session(current, record)
            for malformed in ("SET/NOFILTER ALL", "SET/FILTER DXBM/OFF ALL",
                              "SET/FILTER DXBM/PASS 20,", "SET/FILTER DOC/OFF K"):
                require_fixture_refusal(current, malformed, record)
            fixture_edit(current, "SET/FILTER DXBM/OFF", record)
            document = fixture_edit(current, "SET/FILTER DXBM/PASS 20", record)
            require(active_fixture_bank(document)["dxbm"]["active"] is True and
                    active_fixture_bank(document)["dxbm"]["base_allow"] is False,
                    "native DXBM PASS did not establish its restrictive selection")
            require_callsign_archive(current, cw_label, True)
            fixture_edit(current, "SET/FILTER DXBM/REJECT 20", record)
            require_callsign_archive(current, cw_label, False)
            fixture_edit(current, "SET/NOFILTER", record)
            fixture_edit(current, "SET/NOFT8", record)
        document = fixture_edit(current, "REJECT BEACON", record)
        require(active_fixture_bank(document)["include_beacons"] is False,
                "final supported BEACON choice was lost")
        require_callsign_archive(current, cw_label, True)
        saved_arguments[call] = document["configuration"]


def grid_exact_cases(suite, root, saved_grids):
    for dialect, call in (("go", "VE3ABC-42"), ("cc", "VE3ABC-43")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT GO")
        record = root / "users" / (call + ".yaml")
        fixture_edit(current, "RESET FILTER RULE ALL", record)
        fixture_edit(current, "PASS RULE 0 NEARBY", record)
        current.human("DIALECT " + dialect)
        fixture_edit(current, "SET GRID IO91", record)
        for command in ("SET GRID FN31PRJUNK", "SET GRID FN31P", "SET GRID FN31YZ",
                        "SET GRID SN31", "SET GRID FN31 EXTRA", "SET GRID FN31 PR"):
            before, saved = current.get(), record.read_bytes()
            text, after = current.human(command)
            require("Invalid grid." in text or "Usage: SET GRID" in text,
                    "malformed GRID lacks a specific refusal")
            require_fixture_unchanged(before, after, record, saved)
        for token in ("fn31", "Fn31pR"):
            grid = token.upper()
            document = fixture_edit(current, "SET GRID " + token, record)
            require_grid_configuration(document, grid, record)
            result = current.upload("VALIDATE", "CONFIG", document["configuration"])
            require("error" not in result and result["result"]["operation"] == "VALIDATE" and
                    result["result"]["valid"] is True and
                    result["result"]["applied"] is False and result["result"]["persisted"] is False,
                    "accepted human GRID failed machine validation")
        require_fixture_refusal(current, "PASS NEARBY ON", record)
        document = fixture_edit(current, "SET GRID IO91", record)
        require_grid_configuration(document, "IO91", record)
        require(document["configuration"]["filters"]["go"]["rules"] == [dict(slot=0, action="pass", expr="NEARBY")],
                "GRID publication lost the independent GO proximity rule")
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
    # Prove each seeded row exists before selecting the GO predicate. CC has
    # no NEARBY override: its retired commands must refuse without hiding rows.
    seeds = (("DX20", 14021, True), ("DE20", 14022, True),
             ("DXBAD20", 14023, True), ("DEBAD20", 14024, True),
             ("NONE20", 14025, False), ("FAR20", 14026, False),
             ("DX80", 3521, True), ("DE80", 3522, True),
             ("DX60", 5355, True), ("DE160", 1821, True))
    for dialect, call in (("go", "VE3ABC-40"), ("cc", "VE3ABC-41")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        record = child.root / "users" / (call + ".yaml")
        fixture_edit(current, "RESET FILTER RULE ALL" if dialect == "go" else "SET/NOFILTER", record)
        fixture_edit(current, "SET GRID FN31", record)
        for name, frequency, _ in seeds:
            label = "N08" + child.nonce[:8].upper() + name
            deadline = min(suite.deadline, time.monotonic() + 5)
            while True:
                rows = archive_rows(current, label)
                if rows:
                    require_nearby_archive(rows, label, frequency, True)
                    break
                require(time.monotonic() < deadline, "NEARBY pre-filter archive barrier absent")
                time.sleep(.025)
        require_fixture_refusal(current, "PASS NEARBY ON", record)
        if dialect == "go":
            before = current.get()
            document = fixture_edit(current, "PASS RULE 0 NEARBY", record)
            require(active_fixture_bank(document)["rules"] == [dict(slot=0, action="pass", expr="NEARBY")] and
                    document["status"]["effective"]["grid"] == "FN31" and
                    document["status"]["effective"]["dedupe_policy"] == before["status"]["effective"]["dedupe_policy"],
                    "GO proximity rule lacks its saved definition/location or changed dedupe")
        else:
            for command in ("SET/FILTER NEARBY ON", "PASS RULE 0 NEARBY"):
                require_fixture_refusal(current, command, record)
        for name, frequency, visible in seeds:
            label = "N08" + child.nonce[:8].upper() + name
            require_nearby_archive(archive_rows(current, label), label, frequency, visible if dialect == "go" else True)


def mode_delta_cases(suite, root, cw_label, saved_modes):
    poster = fixture_session(suite, "K1ABC-34")
    poster.human("PAUSE 300")
    poster.human("DIALECT GO")
    poster.human("RESET FILTER RULE ALL")
    ft8_label = literal_archive_label(suite, "F8")
    poster.human("DX 7074 W6ABC FT8 " + ft8_label, "Spot queued.")
    deadline = min(suite.deadline, time.monotonic() + 5)
    while not archive_rows(poster, ft8_label):
        require(time.monotonic() < deadline, "MODE positive archive barrier absent")
        time.sleep(.025)
    require_mode_archive(poster, ft8_label, "FT8", True)
    for dialect, call in (("go", "VE3ABC-33"), ("cc", "VE3ABC-34")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        current.human("DIALECT " + dialect)
        record = root / "users" / (call + ".yaml")
        if dialect == "go":
            fixture_edit(current, "RESET FILTER RULE ALL", record)
            operations = (("RESET FILTER RULE 1", True), ("REJECT RULE 1 MODE=FT8", False),
                          ("RESET FILTER RULE 1", True))
        else:
            initial = current.get()
            result = current.upload("PATCH", "FILTER", dict(dialect="cc", filters=dict(modes=["CW", "FT8"])))
            require("error" not in result and result["result"]["persisted"] is True,
                    "native MODE list setup failed")
            require(current.get()["configuration"]["filters"]["go"] == initial["configuration"]["filters"]["go"],
                    "native MODE setup changed GO rules")
            operations = (("SET/FT8", True), ("SET/NOFT8", False), ("SET/FT8", True))
        for command, ft8_visible in operations:
            fixture_edit(current, command, record)
            require_mode_archive(current, cw_label, "CW", True)
            require_mode_archive(current, ft8_label, "FT8", ft8_visible)
        document = current.get()
        expected = copy.deepcopy(active_fixture_bank(document))
        require(expected["rules"] == [] if dialect == "go" else expected["modes"] == ["CW", "FT8"],
                "MODE re-enable did not restore the explicit native selection")
        require_mode_rules(document, expected, record)
        saved_modes[call] = (document["configuration"], expected, cw_label, ft8_label, True)
    current = fixture_session(suite, "VE3ABC-35")
    current.human("PAUSE 300")
    current.human("DIALECT GO")
    record = root / "users" / (current.call + ".yaml")
    fixture_edit(current, "RESET FILTER RULE ALL", record)
    fixture_edit(current, "PASS RULE 0 MODE=CW", record)
    document = fixture_edit(current, "REJECT RULE 1 MODE=CW", record)
    expected = copy.deepcopy(active_fixture_bank(document))
    require(expected["rules"] == [dict(slot=0, action="pass", expr="MODE=CW"),
                                  dict(slot=1, action="reject", expr="MODE=CW")],
            "restrictive MODE rule/veto definitions differ")
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
        saved = stored_configuration((root / "users" / (current.call + ".yaml")).read_bytes())["settings"]
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
                sessions[1] = fixture_session(suite, sessions[1].call)
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
        sessions[:] = [fixture_session(suite, current.call) for current in sessions]
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
    require(saved.get("configuration_version") == 7, "NOFILTER owner is not native seven")
    live = document["configuration"]
    stored = copy.deepcopy(saved["configuration"])
    stored["settings"].setdefault("cw_speed", False)
    require(stored == live, "NOFILTER live and stored banks disagree")
    dialect = document["status"]["active_dialect"]
    bank = live["filters"][dialect]
    if dialect == "go":
        require(bank["rules"] == [], "GO reset retained personal or NEARBY rules")
    else:
        require(set(bank["geography"]) == {"DOC", "DOS", "DXCTY", "DXSTATE"} and
                all(row == {"global": {"action": "off"}, "bands": {}} for row in bank["geography"].values()) and
                bank["dxbm"] == dict(active=False, base_allow=True, slots={}),
                "CC NOFILTER retained geography or native-band restrictions")


def require_save_warning(text):
    require(text.count("warning: failed to persist") == 1 and
            "\nwarning: failed to persist;" in text and
            "changes are active but were not saved" in text,
            f"missing or misleading persistence warning: {text[:800]}")


def require_setting_save_warning(text):
    require(text.count("warning: failed to persist:") == 1 and
            re.fullmatch(r"[^\r\n]+ \(warning: failed to persist: [^\r\n]+\)", text.strip()),
            f"missing setting persistence warning: {text[:800]}")


RECORD_BYTES = 1 << 20


def refused_resource_record(kind, base, canonical=None):
    """Native shapes isolate physical bytes, Go encoding bytes and alias work."""
    if kind == "RAW":
        require(len(base) < RECORD_BYTES, "resource baseline must fit")
        return base + b"\n#" + b"X" * (RECORD_BYTES - len(base) - 1)
    if kind == "CANONICAL":
        require(canonical is not None and len(canonical) < RECORD_BYTES,
                "canonical refusal needs measured compact native bytes")
        return canonical
    require(kind == "WORK", "unknown resource fixture")
    owner = stored_document(base)
    owner.pop("recent_ips", None)
    # Seventy physical list entries are below 128. The anchored scalar belongs
    # to a known field, so unknown-field rejection cannot mask logical work.
    return native_storage_bytes(owner) + (
        "recent_ips:\n  - &text " + "K" * 32768 + "\n" + "  - *text\n" * 69).encode("ascii")


def resource_vectors(suite, root):
    if hasattr(suite, "resource_vectors"):
        return suite.resource_vectors
    base = suite.resource_seed
    admitted = resource_fixture(suite.binary, root, suite.source, "profile_admission", input_bytes=base)
    require(not admitted["measurements"]["input.yaml"]["error"], "resource baseline refused")
    sizing = resource_fixture(suite.binary, root, suite.source, "profile_at_size",
                              input_bytes=base, target_bytes=RECORD_BYTES + 1)
    require(len(sizing["artifacts"]["canonical.yaml"]) == RECORD_BYTES + 1,
            "canonical overflow target was not exact")
    vectors = {kind: refused_resource_record(kind, base, sizing["artifacts"]["compact.yaml"])
               for kind in ("RAW", "CANONICAL", "WORK")}
    for kind, raw in vectors.items():
        receipt = resource_fixture(suite.binary, root, suite.source, "profile_admission", input_bytes=raw)
        measure = receipt["measurements"]["input.yaml"]
        expected = ("stored configuration decoding work exceeds 2097152 units" if kind == "WORK"
                    else "v7 document exceeds 1048576 bytes")
        require(measure["admission"] and expected in measure["error"],
                f"{kind} fixture refused at wrong boundary: {measure}")
    frames = resource_fixture(suite.binary, root, suite.source, "config_frames", input_bytes=base)
    fit, over = frames["measurements"]["fit.yaml"], frames["measurements"]["over.yaml"]
    require(not fit["error"] and 0 < fit["frame_bytes"] <= 262144 and
            fit["canonical_bytes"] < 196608 and over["canonical_bytes"] == fit["canonical_bytes"] + 1 and
            over["admission"] and over["reference_bytes"] > 262144 and over["error"] ==
            "response exceeds 65,536 bytes; request a smaller filter category or reduce the configuration",
            "native frame owner did not separate fitting compact output from refusal")
    suite.resource_frame_measurements = frames["measurements"]
    dedupe = resource_fixture(suite.binary, root, suite.source, "dedupe_frames")
    require_dedupe_owner_receipt(dedupe)
    suite.resource_frame_measurements["human_dedupe"] = dedupe["measurements"]
    suite.resource_vectors = vectors
    return vectors


def require_dedupe_owner_receipt(receipt):
    for name, count in (("fit.txt", 49200), ("over.txt", 49366)):
        wire = receipt["artifacts"][name]
        measure = receipt["measurements"][name]
        require(0 < len(wire) <= 65536 and wire.endswith(b"\r\n") and
                all(row.endswith(b"\r") and b"\r" not in row[:-1] for row in wire.split(b"\n")[:-1]) and
                all(len(row) <= 78 and all(32 <= byte <= 126 for byte in row)
                    for row in wire.split(b"\r\n")[:-1]) and
                measure["raw_bytes"] == count and measure["frame_bytes"] == len(wire),
                "human DEDUPE owner did not prove exact bounded final wire")
        text = wire.decode("ascii", "strict").replace("\r\n", "\n").rstrip("\n")
        if name == "fit.txt":
            require(not measure["admission"] and measure["canonical_bytes"] < 65536,
                    "fitting human DEDUPE owner was refused")
            require_large_dedupe_output(text, "X" * count)
        else:
            require(measure["admission"] and measure["canonical_bytes"] == 65536 and
                    text == "Readback failed: response exceeds 65,536 bytes.",
                    "oversized human DEDUPE owner partially succeeded or failed at another boundary")


def stored_document(raw):
    document = harness.yaml.load(raw.decode("ascii"), Loader=harness.StrictLoader)
    require(isinstance(document, dict), "stored fixture is not a mapping")
    return document


def stored_configuration(raw):
    document = stored_document(raw)
    require(document.get("configuration_version") == 7 and
            set(document["configuration"]["filters"]) == {"go", "cc"},
            "stored owner is not a complete native-seven profile")
    configuration = document["configuration"]
    # False is omitted by the native disk DTO and explicit on the wire.
    configuration["settings"].setdefault("cw_speed", False)
    return configuration


def native_storage_bytes(document):
    owner = copy.deepcopy(document)
    timestamp = owner.get("last_login_utc")
    if hasattr(timestamp, "isoformat"):
        owner["last_login_utc"] = timestamp.isoformat().replace("+00:00", "Z")
    return yaml.safe_dump(owner, sort_keys=False).encode("ascii")


def native_record(configuration, *, presets=None):
    return dict(configuration_version=7, configuration=copy.deepcopy(configuration),
                presets=copy.deepcopy(presets or {}),
                notice=dict(profile_reset_generation=0, profile_acknowledged_generation=0,
                            library_acknowledged_generation=0))


def tagged_preset(configuration, dialect=None):
    dialect = dialect or configuration["settings"]["dialect"]
    return dict(configuration_version=7, dialect=dialect, settings=copy.deepcopy(configuration["settings"]),
                **{dialect: copy.deepcopy(configuration["filters"][dialect])})


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



def preset_library(root):
    return root / "users" / "presets" / ("K1ABC".encode().hex() + ".yaml")


def require_transaction_unchanged(before, after, record, saved, library, library_saved):
    require_cc_typed_value(after["configuration"], before["configuration"])
    require_cc_typed_value(after["status"]["presets"], before["status"]["presets"])
    require(after["configuration"] == before["configuration"] and
            after["revision"] == before["revision"] and
            after["status"]["presets"] == before["status"]["presets"] and
            after["status"]["effective"] == before["status"]["effective"] and
            after["status"]["session"]["temporary_defaults"] ==
            before["status"]["session"]["temporary_defaults"] and
            record.read_bytes() == saved and library.read_bytes() == library_saved,
            "resource refusal published, protected or durably changed the transaction")


def resource_login_cases(suite, root, refused):
    library = preset_library(root)
    for index, kind in enumerate(("RAW", "CANONICAL", "WORK"), start=21):
        call = f"K1ABC-{index:02d}"
        record, raw = root / "users" / (call + ".yaml"), resource_vectors(suite, root)[kind]
        record.write_bytes(raw)
        refused[call] = raw
        current = fixture_session(suite, call)
        startup, document = current.human("PAUSE 300")
        require("temporary defaults" in login_text(current, startup) and
                document["status"]["session"]["temporary_defaults"] is True and
                record.read_bytes() == raw, f"{kind} resource-refused login was not protected")
        for dialect in ("cc", "go"):
            text, document = current.human("DIALECT " + dialect)
            line = "SET/FILTER BEACON" if dialect == "cc" else "PASS RULE 0 BAND=40M"
            text, document = current.human(line)
            require_save_warning(text)
            bank = document["configuration"]["filters"][dialect]
            require((bank["include_beacons"] is True if dialect == "cc" else
                     bank["rules"] == [dict(slot=0, action="pass", expr="BAND=40M")]) and record.read_bytes() == raw,
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
        call = f"K1ABC-{index:02d}"
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        name = "WARM" + kind
        current.human("SAVE PRESET " + name, "Saved preset " + name)
        record = root / "users" / (call + ".yaml")
        repair_source, raw = record.read_bytes(), resource_vectors(suite, root)[kind]
        record.write_bytes(raw)
        text, temporary = current.human("DIALECT cc")
        current.human("DIALECT go")
        text, temporary = current.human("PASS RULE 0 COMMENT=TEMP" + kind)
        require_save_warning(text)
        require(temporary["configuration"]["filters"]["go"]["rules"] ==
                [dict(slot=0, action="pass", expr='COMMENT="TEMP' + kind + '"')] and
                temporary["status"]["session"]["temporary_defaults"] is False and
                record.read_bytes() == raw, "warm refusal became sticky or lost its temporary edit")
        library_saved = library.read_bytes()
        for target in (name, "DENY" + kind):
            text, after = current.human("SAVE PRESET " + target, "SAVE PRESET failed:")
            reason = {"RAW": "user record exceeds 1048576 bytes",
                      "CANONICAL": "v7 document exceeds 1048576 bytes",
                      "WORK": "stored configuration decoding work exceeds 2097152 units"}[kind]
            require("current user record could not be loaded: " + reason in text,
                    "warm SAVE omitted the intended storage resource reason")
            require("but could not persist its association" not in text and
                    "protected" not in text,
                    "warm refusal was misreported as protected or as a successful library write")
            require_transaction_unchanged(temporary, after, record, raw, library, library_saved)
        timestamp, ip = f"2026-10-08T12:34:{index:02d}Z", f"198.51.100.{index}"
        record.write_bytes(record_with_metadata(repair_source, timestamp, ip))
        text, after = current.human("PASS RULE 1 COMMENT=RETRY" + kind)
        require("warning: failed to persist" not in text and
                after["configuration"]["filters"]["go"]["rules"] == [
                    dict(slot=0, action="pass", expr='COMMENT="TEMP' + kind + '"'),
                    dict(slot=1, action="pass", expr='COMMENT="RETRY' + kind + '"')] and
                after["status"]["session"]["temporary_defaults"] is False,
                "warm preparation did not retry after authoritative repair")
        require(stored_configuration(record.read_bytes()) == after["configuration"],
                "warm repair did not immediately persist complete native preferences")
        require_metadata(record.read_bytes(), timestamp, ip)
        repaired[call] = (after["configuration"], timestamp, ip)

    # A still-refused ordinary logout must preserve bytes; a repaired logout
    # must independently retry without a second mutating command masking it.
    for call, repair in (("K1ABC-28", False), ("K1ABC-29", True)):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        record = root / "users" / (call + ".yaml")
        repair_source, raw = record.read_bytes(), resource_vectors(suite, root)["RAW"]
        record.write_bytes(raw)
        current.human("DIALECT go")
        text, temporary = current.human("PASS RULE 0 COMMENT=LOGOUTRETRY")
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
        while stored_configuration(record.read_bytes())["filters"]["go"]["rules"] != [dict(slot=0, action="pass", expr='COMMENT="LOGOUTRETRY"')]:
            require(time.monotonic() < deadline, "repaired logout did not save the temporary live edit")
            time.sleep(.025)
        require_metadata(record.read_bytes(), timestamp, ip)
        repaired[call] = (temporary["configuration"], timestamp, ip)


def resource_prospective_case(suite, root, prospective):
    call = "K1ABC-27"
    current = fixture_session(suite, call)
    current.human("PAUSE 300")
    current.human("DIALECT go")
    current.human("SET NOISE QUIET")
    current.human("RESET FILTER RULE ALL")
    current.human("SAVE PRESET QUOTAOLD", "Saved preset QUOTAOLD")
    _, before = current.human("PASS RULE 0 COMMENT=CURRENTQUOTA")
    require(before["status"]["presets"]["go"] == dict(associated=True, name="QUOTAOLD", modified=True),
            "prospective control lacks smaller applied baseline")
    library, record = preset_library(root), root / "users" / (call + ".yaml")
    collection = stored_document(library.read_bytes())
    target = copy.deepcopy(collection["presets"]["QUOTAOLD"])
    target["go"]["rules"] = [dict(slot=0, action="pass", expr='COMMENT="LOADTARGETGROWSBOTHCONFIGURATIONANDBASELINE"')]
    collection["presets"]["QUOTALOAD"] = target
    library.write_text(yaml.safe_dump(collection, sort_keys=False), encoding="ascii")
    library_saved = library.read_bytes()
    sizing = resource_fixture(suite.binary, root, suite.source, "profile_at_size",
                              input_bytes=record.read_bytes(), target_bytes=RECORD_BYTES - 8)
    saved = sizing["artifacts"]["canonical.yaml"]
    require(len(saved) == RECORD_BYTES - 8 and not sizing["measurements"]["canonical.yaml"]["error"],
            "prospective destination is not an admitted fitting control")
    record.write_bytes(saved)
    text, after = current.human("PASS RULE 1 COMMENT=HUMANPROSPECTIVEQUOTA", "Configuration command failed:")
    require("exceeds 1048576 bytes" in text and "load current user record:" not in text and
            "warning: failed to persist" not in text, "human quota failed at wrong layer")
    require_transaction_unchanged(before, after, record, saved, library, library_saved)
    for name in ("QUOTAOLD", "QUOTANEW"):
        text, after = current.human("SAVE PRESET " + name, "SAVE PRESET failed:")
        require("exceeds 1048576 bytes" in text and "load current user record:" not in text and
                "but could not persist its association" not in text, "SAVE published before prospective admission")
        require_transaction_unchanged(before, after, record, saved, library, library_saved)
    candidate = copy.deepcopy(before["configuration"])
    candidate["filters"]["go"]["rules"].append(dict(slot=1, action="pass", expr="COMMENT=EXTRAPROSPECTIVEQUOTA"))
    for verb, resource, body in (("VALIDATE", "CONFIG", candidate), ("PUT", "CONFIG", candidate),
                                  ("PATCH", "CONFIG", {"filters": {"go": candidate["filters"]["go"]}})):
        response = current.upload(verb, resource, body, revision=before["revision"])
        require(response.get("error", {}).get("code") == "invalid_configuration" and
                "exceeds 1048576 bytes" in response["error"]["message"],
                f"{verb} did not refuse prospective destination before publication: {response}")
        require_transaction_unchanged(before, current.get(), record, saved, library, library_saved)
    text, after = current.human("LOAD PRESET QUOTALOAD", "LOAD PRESET failed:")
    require("exceeds 1048576 bytes" in text and "load current user record:" not in text,
            "LOAD failed before prospective destination admission")
    require_transaction_unchanged(before, after, record, saved, library, library_saved)
    prospective[call] = (saved, before["configuration"], before["status"]["presets"])


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
                f"{transport}: DXCC and logout arity preserve the original session",
                f"{transport}: fixed settings reject tails and preserve valid values",
                f"{transport}/0: large DEDUPE final send fits or refuses without effects",
                f"{transport}/0: enabled SOLAR feedback preserves cadence and read owners",
                f"{transport}/0: configured taxonomy underscores reach human commands and matching",
                f"{transport}/0: COMMENT aliases preserve literal history and continuation",
                f"{transport}/0: CC HELP grammar agrees with saved command outcomes",
                f"{transport}/0: HELP routes existing aliases without preference effects",
                f"{transport}/0: filter discovery and HELP wording preserve all owners",
                f"{transport}/0: DX comment alphabet and whole-command limit reach archive",
                f"{transport}/0: HOLD controls preserve countdown counters and delivery",
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
                f"{transport}: supported filter arguments survive fresh child",
                f"{transport}: command arity preferences survive fresh child",
                f"{transport}: fixed setting values survive fresh child with session reset",
                f"{transport}/1: large DEDUPE final send survives fresh child",
                f"{transport}/1: enabled SOLAR cadence survives fresh child",
                f"{transport}/1: configured taxonomy underscore rules survive fresh child",
                f"{transport}/1: COMMENT history aliases work on fresh child",
                f"{transport}/1: CC HELP grammar and preferences survive fresh child",
                f"{transport}/1: HELP routing and full preferences survive fresh child",
                f"{transport}/1: discovery wording and full preferences survive fresh child",
                f"{transport}/1: HOLD preferences and delivery survive fresh child"]
    call = "K1ABC-01"
    observer_call, observer_configuration = "VE3ABC-03", None
    failed_records, refused_records, compatible_records = {}, {}, {}
    prepared_records = {}
    staged_records = {}
    resource_refused, resource_repaired, resource_prospective = {}, {}, {}
    saved_modes = {}
    saved_grids = {}
    saved_callsigns = {}
    saved_arguments = {}
    saved_arity = {}
    saved_settings = {}
    saved_solar = {}
    saved_taxonomy = {}
    saved_history_aliases = {}
    saved_cc_grammar = {}
    saved_help_routes = {}
    saved_discovery = {}
    saved_hold = {}
    label = None
    suites = []
    for generation in range(2):
        child, suite = None, None
        replacement_faults = contextlib.ExitStack()
        try:
            child = Child(binary, root, source)
            children.append(child.identity)
            suite = CommandSuite(SimpleNamespace(host=child.host, port=child.port, call="K1ABC",
                                                  timeout=8, run_seconds=240,
                                                  output=root / f"wire-{generation}"))
            if generation == 0:
                label = literal_archive_label(suite, "CW")
            suites.append(suite)
            session = fixture_session(suite, call)
            suite.binary, suite.source = binary, source
            suite.resource_seed = (root / "users" / (call + ".yaml")).read_bytes()

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
                            sync = f"GET YAML CONFIG SCHEMA 7 ID {identifier}"
                            session.send(bad + "\r\nSHOW OWN\r\n" + sync + "\r\n")
                            after, prefix = session.frame("CONFIG", identifier, 7, "rejected physical line")
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
                    terminal = fixture_session(suite, "K1ABC-02")
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
                        require(saved["configuration"]["settings"]["dialect"] == dialect, "dialect not saved before next command")
                    _, doc = session.human("SET GRID FN31", "Grid set")
                    require(doc["configuration"]["settings"]["grid"] == "FN31", "loaded grid unavailable")
                    _, doc = session.human("SET NOISE URBAN", "Noise class set")
                    require(doc["configuration"]["settings"]["noise_class"] == "URBAN", "live setting absent")
                    # Inspect while still connected: final unregister autosave
                    # must never repair a false-green command-save assertion.
                    saved = harness.yaml.load((root / "users" / (call + ".yaml")).read_text(),
                                              Loader=harness.StrictLoader)
                    require(stored_configuration((root / "users" / (call + ".yaml")).read_bytes()) == doc["configuration"] and
                            saved["configuration"]["settings"]["dialect"] == "cc",
                            "complete command persistence absent before disconnect")

                case(f"{transport}: live change saved before disconnect", persistence)

                def posting():
                    session.human("DX NOTAFREQ W6ABC " + label, "Invalid frequency")
                    session.human("DX 14074 W6ABC CW " + label, "Spot queued.")
                    require_archive(session, label)

                case(f"{transport}: invalid control and actual archive write", posting)

                def nofilter():
                    nonlocal observer_configuration
                    observer = fixture_session(suite, observer_call)
                    observer.human("PAUSE 300")
                    observer.human("SET GRID FN31", "Grid set")
                    for dialect in ("go", "cc"):
                        observer.human("DIALECT " + dialect)
                        observer.human("RESET FILTER" if dialect == "go" else "SET/NOFILTER")
                        require_archive(observer, label)
                        observer.human("PASS RULE 0 DXCONT=EU AND NEARBY" if dialect == "go"
                                       else "SET/FILTER DOC/PASS VE")
                        require(not archive_rows(observer, label), "restriction did not hide control row")
                        _, document = observer.human("RESET FILTER RULE ALL" if dialect == "go" else "SET/NOFILTER")
                        saved = stored_document((root / "users" / (observer_call + ".yaml")).read_bytes())
                        require_unrestricted_geography(document, saved)
                        require(saved["configuration"]["settings"]["grid"] == "FN31",
                                "bank reset cleared shared grid")
                        require_archive(observer, label)
                        observer_configuration = document["configuration"]

                case(f"{transport}: NOFILTER retires NEARBY geography and saves", nofilter)

                def failed_saves():
                    for dialect, failed_call in (("go", "K1ABC-04"), ("cc", "K1ABC-07")):
                        failed = fixture_session(suite, failed_call)
                        failed.human("PAUSE 300")
                        failed.human("DIALECT " + dialect)
                        failed.human("PASS RULE 0 BAND=40M" if dialect == "go" else "SET/FILTER BEACON")
                        before = failed.get()
                        record = root / "users" / (failed_call + ".yaml")
                        saved = record.read_bytes()
                        failed_records[failed_call] = (saved, before["configuration"])
                        replacement_faults.enter_context(deny_record_replacement(record))

                        def changed(line, check):
                            text, document = failed.human(line)
                            require_save_warning(text)
                            require(check(document["configuration"]), f"live change absent: {line}")
                            require(record.read_bytes() == saved, f"failed save changed disk: {line}")
                            return document

                        # Edit both banks before switching back: a later complete save must own
                        # all pending state, while every failed replacement preserves disk.
                        other = "cc" if dialect == "go" else "go"
                        changed("DIALECT " + other, lambda cfg: cfg["settings"]["dialect"] == other)
                        changed("PASS RULE 2 COMMENT=NEW" if other == "go" else "SET/FT8",
                                lambda cfg: bool(cfg["filters"]["go"]["rules"]) if other == "go"
                                else "FT8" in cfg["filters"]["cc"]["modes"])
                        changed("DIALECT " + dialect, lambda cfg: cfg["settings"]["dialect"] == dialect)
                        before_noop = failed.get()
                        text, after_noop = failed.human("DIALECT " + dialect)
                        require("warning: failed to persist" not in text and
                                after_noop["configuration"] == before_noop["configuration"] and
                                record.read_bytes() == saved, "no-op dialect attempted persistence")
                        if dialect == "go":
                            changed("PASS RULE 0 BAND=20M", lambda cfg: cfg["filters"]["go"]["rules"] ==
                                    [dict(slot=0, action="pass", expr="BAND=20M")])
                            changed("REJECT RULE 1 MODE=FT8", lambda cfg: len(cfg["filters"]["go"]["rules"]) == 2)
                            changed("RESET FILTER RULE 1", lambda cfg: len(cfg["filters"]["go"]["rules"]) == 1)
                            changed("RESET FILTER RULE ALL", lambda cfg: cfg["filters"]["go"]["rules"] == [])
                        else:
                            changed("SET/FT8", lambda cfg: "FT8" in cfg["filters"]["cc"]["modes"])
                            changed("SET/FILTER DXBM/PASS 20", lambda cfg: cfg["filters"]["cc"]["dxbm"]["active"] is True)
                            changed("SET/NOFILTER", lambda cfg: cfg["filters"]["cc"]["dxbm"]["active"] is False)

                case(f"{transport}: actual replacement failure warns and preserves saved bytes", failed_saves)

                def refused_storage():
                    for refused_call, raw in (("K1ABC-05", b"[broken"),
                                              ("K1ABC-06", b"configuration_version: 99\nnoise_class: URBAN\n")):
                        existing = fixture_session(suite, refused_call)
                        existing.human("PAUSE 300")
                        record = root / "users" / (refused_call + ".yaml")
                        record.write_bytes(raw)
                        refused_records[refused_call] = raw
                        for dialect, line in (("cc", "SET/FILTER BEACON"), ("go", "PASS RULE 0 BAND=40M")):
                            text, document = existing.human("DIALECT " + dialect)
                            require(document["configuration"]["settings"]["dialect"] == dialect, "dialect lost")
                            text, document = existing.human(line)
                            require_save_warning(text)
                            bank = document["configuration"]["filters"][dialect]
                            require((bank["include_beacons"] is True if dialect == "cc" else
                                     bank["rules"] == [dict(slot=0, action="pass", expr="BAND=40M")]) and
                                    record.read_bytes() == raw, "warm refusal changed disk or lost live bank")
                        existing.close()
                        protected = fixture_session(suite, refused_call)
                        startup, document = protected.human("PAUSE 300")
                        require("temporary defaults" in login_text(protected, startup) and
                                document["status"]["session"]["temporary_defaults"] is True,
                                "protected login omitted temporary defaults")
                        for line in ("DIALECT cc", "SET/FILTER BEACON", "DIALECT go", "PASS RULE 0 COMMENT=TEMP"):
                            text, document = protected.human(line)
                            if not line.startswith("DIALECT "):
                                require("temporary" in text or "not saved" in text, "protected edit warning absent")
                            require(record.read_bytes() == raw, "protected edit overwrote refused file")
                        require(document["configuration"]["filters"]["go"]["rules"] ==
                                [dict(slot=0, action="pass", expr='COMMENT="TEMP"')], "protected live edit absent")

                case(f"{transport}: preparation failure and protected login preserve refused records", refused_storage)

                def compatible_storage():
                    users = root / "users"
                    legacy_call, exact_call, trap_call = "K1ABC-08", "K1ABC-09", "K1ABC-10"
                    legacy = b"bands: {20m: true, 40m: false}\nallbands: false\ndialect: cc\ndedupe_policy: MED\nnoise_class: URBAN\n"
                    library = preset_library(root)
                    library.parent.mkdir(exist_ok=True)
                    require(not library.exists(), "legacy reset requires an owned unused library")
                    old_library = b"presets:\n  LEGACY04A:\n    dialect: go\n    dedupe_policy: MED\n    bands: {20m: true}\n"
                    library.write_bytes(old_library)
                    legacy_path = users / (legacy_call + ".yaml")
                    legacy_path.write_bytes(legacy)
                    restored = fixture_session(suite, legacy_call)
                    startup, document = restored.human("PAUSE 300")
                    require(document["status"]["session"]["temporary_defaults"] is False and
                            document["configuration"]["settings"]["dialect"] == "cc" and
                            document["configuration"]["settings"]["noise_class"] == "URBAN" and
                            document["configuration"]["filters"] == stored_configuration(suite.resource_seed)["filters"] and
                            Path(str(legacy_path) + ".pre-v7.bak").read_bytes() == legacy,
                            "legacy reset lost shared preferences, backup, or GO fresh start")
                    collection = stored_document(library.read_bytes())
                    require(collection["configuration_version"] == 7 and collection["reset_generation"] == 7 and
                            collection["presets"] == {} and
                            Path(str(library) + ".pre-v7.bak").read_bytes() == old_library,
                            "legacy library reset lost backup or completion marker")
                    compatible_records[legacy_call] = document["configuration"]
                    restored.human("SAVE PRESET NATIVE04A", "Saved preset NATIVE04A")
                    library_saved = library.read_bytes()
                    # A second eligible SSID must not repeat an already completed library reset.
                    second = users / (exact_call + ".yaml")
                    second.write_bytes(legacy)
                    upgrading = fixture_session(suite, exact_call)
                    upgrading.human("PAUSE 300")
                    require(library.read_bytes() == library_saved and
                            Path(str(second) + ".pre-v7.bak").read_bytes() == legacy,
                            "second SSID repeated library reset or lost original backup")
                    exact = native_record(document["configuration"])
                    exact["configuration"]["settings"]["dialect"] = "go"
                    exact["configuration"]["filters"]["go"]["rules"] = [dict(slot=3, action="pass", expr='COMMENT="CQ TEST"')]
                    exact["configuration"]["filters"]["go"]["allow_wwv"] = False
                    suite.acknowledge(upgrading.upload("PUT", "CONFIG", exact["configuration"]))
                    upgrading.human("SAVE PRESET BASE04A", "Saved preset BASE04A")
                    upgrading.close()
                    library_saved = library.read_bytes()
                    current = fixture_session(suite, exact_call)
                    _, native = current.human("PAUSE 300")
                    require(native["configuration"] == exact["configuration"] and
                            native["status"]["presets"]["go"] == dict(associated=True, name="BASE04A", modified=False),
                            "native exact rules, false switch or baseline changed at login")
                    inactive = copy.deepcopy(native["configuration"]["filters"]["go"])
                    _, loaded = current.human("LOAD PRESET NATIVE04A", "Loaded preset")
                    require(loaded["configuration"]["settings"]["dialect"] == "cc" and
                            loaded["configuration"]["filters"]["go"] == inactive and library.read_bytes() == library_saved,
                            "tagged LOAD changed inactive bank or library")
                    compatible_records[exact_call] = loaded["configuration"]
                    unused = "a: &a {x: 0}\nb: &b {<<: [*a,*a,*a,*a,*a,*a,*a]}\nc: &c {<<: [*b,*b,*b,*b,*b,*b,*b]}\nd: &d {<<: [*c,*c,*c,*c,*c,*c,*c]}\n"
                    trap = (unused + "<<: *d\n").encode("ascii")
                    require(len(trap) == 125, "legacy alias refusal fixture changed")
                    record = users / (trap_call + ".yaml")
                    record.write_bytes(trap)
                    refused_records[trap_call] = trap
                    protected = fixture_session(suite, trap_call)
                    startup, state = protected.human("PAUSE 300")
                    require("temporary defaults" in login_text(protected, startup) and state["status"]["session"]["temporary_defaults"],
                            "legacy alias refusal was lost at login")
                    protected.human("DIALECT go")
                    text, _ = protected.human("PASS RULE 0 BAND=20M")
                    require_save_warning(text)
                    require(record.read_bytes() == trap, "alias-refused original was overwritten")

                case(f"{transport}: compatible storage and old alias refusal reach login and LOAD", compatible_storage)

                def prepared_commit_faults():
                    prepared_call = "K1ABC-11"
                    current = fixture_session(suite, prepared_call)
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
                                document["status"]["presets"] == before["status"]["presets"] and
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
                        require(collection["presets"]["NEW04B"]["settings"]["noise_class"] == "URBAN",
                                "later association I/O failure lost the successful library write")

                    # The record is writable for this control. A library failure
                    # must stop before any attempted association can alter it.
                    library_before = library.read_bytes()
                    with deny_record_replacement(library):
                        _, document = current.human("SAVE PRESET FAIL04B", "SAVE PRESET failed:")
                        preserved(document)
                        require(library.read_bytes() == library_before,
                                "failed library replacement altered the saved presets")
                    prepared_records[prepared_call] = (saved, before["configuration"], before["status"]["presets"])

                case(f"{transport}: real machine LOAD and SAVE commit faults preserve publication", prepared_commit_faults)

                def detached_human_edits():
                    for dialect, staged_call in (("go", "K1ABC-12"), ("cc", "K1ABC-13")):
                        current = fixture_session(suite, staged_call)
                        current.human("PAUSE 300")
                        current.human("DIALECT go")
                        current.human("SET DIAG SOURCE")
                        current.human("SET GRID FN31", "Grid set to FN31")
                        current.human("PASS RULE 0 DXCONT=EU")
                        _, nearby = current.human("PASS RULE 1 NEARBY")
                        record = root / "users" / (staged_call + ".yaml")
                        saved = record.read_bytes()
                        for line in ("PASS RULE 2 BAND=20M,NOTABAND", "REJECT RULE 2 MODE=CW,NOTAMODE",
                                     "PASS RULE 2 COMMENT=", "SET NOISE NOTACLASS", "PASS NEARBY MAYBE"):
                            text, refused = current.human(line)
                            require(refused["configuration"] == nearby["configuration"] and
                                    refused["revision"] == nearby["revision"] and record.read_bytes() == saved,
                                    "refused edit changed state")
                            require(any(word in text for word in ("Invalid", "Unknown", "Usage", "unsupported", "Use ")),
                                    "refused edit lacks feedback")
                        with deny_record_replacement(record):
                            text, temporary = current.human("SET NOISE URBAN")
                            require_setting_save_warning(text)
                            require(temporary["configuration"]["settings"]["noise_class"] == "URBAN" and
                                    temporary["configuration"]["filters"] == nearby["configuration"]["filters"] and
                                    record.read_bytes() == saved, "late failure lost live edit or predicate")
                        _, changed_grid = current.human("SET GRID IO91", "Grid set to IO91")
                        require(changed_grid["configuration"]["filters"] == nearby["configuration"]["filters"] and
                                changed_grid["status"]["effective"]["grid"] == "IO91", "GRID lost predicate")
                        _, restored = current.human("RESET FILTER RULE 1")
                        require(restored["configuration"]["filters"]["go"]["rules"] ==
                                [dict(slot=0, action="pass", expr="DXCONT=EU")], "removing NEARBY lost unrelated rule")
                        current.human("DIALECT " + dialect)
                        _, configured = current.human("SET SOLAR 15", "Solar summaries: every 15 minutes")
                        require(configured["status"]["session"]["diagnostic_comments"] == "SOURCE" and
                                configured["status"]["session"]["pause_active"], "publication changed session state")
                        persisted = record.read_bytes()
                        require(stored_configuration(persisted) == configured["configuration"], "later save lost banks")
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
                case(f"{transport}: DXCC and logout arity preserve the original session",
                     lambda: command_arity_cases(suite, root, saved_arity))
                case(f"{transport}: fixed settings reject tails and preserve valid values",
                     lambda: fixed_setting_cases(suite, root, saved_settings))
                case(f"{transport}/0: large DEDUPE final send fits or refuses without effects",
                     lambda: large_dedupe_cases(suite, root, generation))
                case(f"{transport}/0: enabled SOLAR feedback preserves cadence and read owners",
                     lambda: solar_cases(suite, root, True, generation, saved_solar))
                case(f"{transport}/0: configured taxonomy underscores reach human commands and matching",
                     lambda: taxonomy_cases(suite, root, child.nonce, generation, saved_taxonomy, label))
                case(f"{transport}/0: COMMENT aliases preserve literal history and continuation",
                     lambda: history_alias_cases(suite, root, child.nonce, generation, saved_history_aliases))
                case(f"{transport}/0: CC HELP grammar agrees with saved command outcomes",
                     lambda: cc_help_grammar_cases(suite, root, generation, saved_cc_grammar))
                case(f"{transport}/0: HELP routes existing aliases without preference effects",
                     lambda: help_routing_cases(suite, root, generation, saved_help_routes))
                case(f"{transport}/0: filter discovery and HELP wording preserve all owners",
                     lambda: filter_discovery_cases(suite, root, generation, saved_discovery))
                case(f"{transport}/0: DX comment alphabet and whole-command limit reach archive",
                     lambda: dx_input_contract_cases(suite, root))
                case(f"{transport}/0: HOLD controls preserve countdown counters and delivery",
                     lambda: hold_command_cases(suite, root, generation, saved_hold, session))
            else:
                def restored():
                    settings = session.get()["configuration"]["settings"]
                    require(settings["noise_class"] == "URBAN" and settings["grid"] == "FN31" and
                            settings["dialect"] == "cc", "preferences lost across process replacement")
                    require_archive(session, label)

                case(f"{transport}: preferences and archive survive fresh child", restored)

                def restored_nofilter():
                    observer = fixture_session(suite, observer_call)
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
                        restored = fixture_session(suite, failed_call)
                        restored.human("PAUSE 300")
                        require(restored.get()["configuration"] == configuration,
                                "fresh child restored a previously unsaved change")
                        text, document = restored.human("SET GRID FN31")
                        require("warning: failed to persist" not in text and
                                document["configuration"]["settings"]["grid"] == "FN31",
                                "save did not recover after fault release")
                        saved_now = harness.yaml.load(record.read_text(), Loader=harness.StrictLoader)
                        require(saved_now["configuration"]["settings"]["grid"] == "FN31", "later successful save not immediately durable")
                    for refused_call, raw in refused_records.items():
                        record = root / "users" / (refused_call + ".yaml")
                        require(record.read_bytes() == raw, "teardown overwrote refused record")
                        protected = fixture_session(suite, refused_call)
                        startup, document = protected.human("PAUSE 300")
                        require("temporary defaults" in login_text(protected, startup) and
                                document["status"]["session"]["temporary_defaults"] is True,
                                "fresh child did not protect refused record")
                        require(record.read_bytes() == raw, "fresh login overwrote refused record")

                case(f"{transport}: failed saves and refused records survive fresh child", restored_failures)

                def restored_compatible_storage():
                    for stored_call, configuration in compatible_records.items():
                        restored = fixture_session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" not in login_text(restored, startup) and
                                document["configuration"] == configuration,
                                "accepted legacy/exact storage did not survive a fresh process")
                    listing, _ = session.human("LIST PRESET", "NATIVE04A")
                    require("Failed" not in listing, "merged preset library was lost on restart")

                case(f"{transport}: compatible storage survives fresh child", restored_compatible_storage)

                def restored_prepared_commits():
                    for stored_call, (saved, configuration, preset_status) in prepared_records.items():
                        require((root / "users" / (stored_call + ".yaml")).read_bytes() == saved,
                                "teardown changed a failed transaction's stored record")
                        restored = fixture_session(suite, stored_call)
                        _, document = restored.human("PAUSE 300")
                        require(document["configuration"] == configuration and
                                document["status"]["presets"] == preset_status,
                                "fresh child restored a failed staged change or association")
                        listing, _ = restored.human("LIST PRESET", "NEW04B")
                        require("OLD04B" in listing and "FAIL04B" not in listing,
                                "library I/O outcomes did not survive replacement")

                case(f"{transport}: prepared commit outcomes survive fresh child", restored_prepared_commits)

                def restored_detached_humans():
                    for stored_call, configuration in staged_records.items():
                        restored = fixture_session(suite, stored_call)
                        _, document = restored.human("PAUSE 300")
                        require(document["configuration"] == configuration and
                                document["status"]["effective"]["grid"] == "IO91" and
                                document["status"]["effective"]["noise_class"] == "URBAN" and
                                document["configuration"]["filters"]["go"]["rules"] == [dict(slot=0, action="pass", expr="DXCONT=EU")],
                                "fresh child lost accepted detached human outcomes")

                case(f"{transport}: detached human settings survive fresh child", restored_detached_humans)

                def restored_resource_refusals():
                    require(len(resource_refused) == 4, "required resource refusal controls did not execute")
                    for stored_call, raw in resource_refused.items():
                        record = root / "users" / (stored_call + ".yaml")
                        require(record.read_bytes() == raw, "teardown changed resource-refused bytes")
                        restored = fixture_session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" in login_text(restored, startup) and
                                document["status"]["session"]["temporary_defaults"] is True and
                                record.read_bytes() == raw, "fresh child did not protect resource refusal")

                case(f"{transport}: resource-refused records survive fresh child", restored_resource_refusals)

                def restored_resource_repairs():
                    require(len(resource_repaired) == 4, "required authoritative retry controls did not execute")
                    for stored_call, (configuration, timestamp, ip) in resource_repaired.items():
                        record = root / "users" / (stored_call + ".yaml")
                        require_metadata(record.read_bytes(), timestamp, ip) # Before login changes timestamp.
                        restored = fixture_session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" not in login_text(restored, startup) and
                                document["status"]["session"]["temporary_defaults"] is False and
                                document["configuration"] == configuration,
                                "fresh child lost the admitted temporary edit or repair")

                case(f"{transport}: repaired resource metadata survives fresh child", restored_resource_repairs)

                def restored_prospective_admission():
                    require(len(resource_prospective) == 1, "prospective admission control did not execute")
                    library = stored_document(preset_library(root).read_bytes())["presets"]
                    require("QUOTANEW" not in library and library["QUOTAOLD"]["go"]["rules"] == [] and
                            library["QUOTALOAD"]["go"]["rules"] ==
                            [dict(slot=0, action="pass", expr='COMMENT="LOADTARGETGROWSBOTHCONFIGURATIONANDBASELINE"')],
                            "failed quota SAVE altered the library before restart")
                    for stored_call, (saved, configuration, preset_status) in resource_prospective.items():
                        record = root / "users" / (stored_call + ".yaml")
                        require(record.read_bytes() == saved, "teardown changed refused prospective state")
                        restored = fixture_session(suite, stored_call)
                        startup, document = restored.human("PAUSE 300")
                        require("temporary defaults" not in login_text(restored, startup) and
                                document["status"]["session"]["temporary_defaults"] is False and
                                document["configuration"] == configuration and
                                document["status"]["presets"] == preset_status,
                                "fresh child restored a refused candidate or changed its baseline")

                case(f"{transport}: prospective admission outcomes survive fresh child", restored_prospective_admission)

                def restored_modes():
                    require(len(saved_modes) == 3, "required MODE controls did not execute")
                    for saved_call, (configuration, expected, cw_label, ft8_label, visible) in saved_modes.items():
                        current = fixture_session(suite, saved_call)
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
                        current = fixture_session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration and
                                any("NEARBY" in row["expr"] for row in document["configuration"]["filters"]["go"]["rules"]),
                                "fresh child lost canonical GRID or active NEARBY")
                        require_grid_configuration(document, "IO91", root / "users" / (stored_call + ".yaml"))

                case(f"{transport}: canonical GRID and active cells survive fresh child", restored_grids)

                def restored_callsigns():
                    require(len(saved_callsigns) == 2, "required callsign controls did not execute")
                    for stored_call, (configuration, expected) in saved_callsigns.items():
                        current = fixture_session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration,
                                "fresh child lost callsign configuration")
                        require_callsign_rules(document, expected, root / "users" / (stored_call + ".yaml"))
                        require_callsign_archive(current, label, True)

                case(f"{transport}: callsign patterns and matching survive fresh child", restored_callsigns)

                def restored_arguments():
                    require(len(saved_arguments) == 2, "required original-argument controls did not execute")
                    for stored_call, configuration in saved_arguments.items():
                        current = fixture_session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration,
                                "fresh child lost supported filter argument selections")
                        require_callsign_archive(current, label, True)

                case(f"{transport}: supported filter arguments survive fresh child", restored_arguments)

                def restored_arity():
                    require(len(saved_arity) == 2, "required command arity controls did not execute")
                    for stored_call, configuration in saved_arity.items():
                        current = fixture_session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration,
                                "fresh child lost preferences after refused command arity")
                        current.human("SH DXCC VE", "Canada")

                case(f"{transport}: command arity preferences survive fresh child", restored_arity)

                def restored_settings():
                    require(len(saved_settings) == 2, "required fixed setting controls did not execute")
                    for stored_call, configuration in saved_settings.items():
                        current = fixture_session(suite, stored_call)
                        _, document = current.human("PAUSE 300")
                        require(document["configuration"] == configuration and
                                document["status"]["effective"]["path_min_observation_count"] == 30 and
                                document["status"]["session"]["diagnostic_comments"] == "OFF",
                                "fresh child lost fixed settings or persisted session diagnostics")

                case(f"{transport}: fixed setting values survive fresh child with session reset", restored_settings)
                case(f"{transport}/1: large DEDUPE final send survives fresh child",
                     lambda: large_dedupe_cases(suite, root, generation))
                case(f"{transport}/1: enabled SOLAR cadence survives fresh child",
                     lambda: solar_cases(suite, root, True, generation, saved_solar))
                case(f"{transport}/1: configured taxonomy underscore rules survive fresh child",
                     lambda: taxonomy_cases(suite, root, child.nonce, generation, saved_taxonomy, label))
                case(f"{transport}/1: COMMENT history aliases work on fresh child",
                     lambda: history_alias_cases(suite, root, child.nonce, generation, saved_history_aliases))
                case(f"{transport}/1: CC HELP grammar and preferences survive fresh child",
                     lambda: cc_help_grammar_cases(suite, root, generation, saved_cc_grammar))
                case(f"{transport}/1: HELP routing and full preferences survive fresh child",
                     lambda: help_routing_cases(suite, root, generation, saved_help_routes))
                case(f"{transport}/1: discovery wording and full preferences survive fresh child",
                     lambda: filter_discovery_cases(suite, root, generation, saved_discovery))
                case(f"{transport}/1: HOLD preferences and delivery survive fresh child",
                     lambda: hold_command_cases(suite, root, generation, saved_hold, session))
        finally:
            try:
                if suite:
                    with suite.retirement_deadline():
                        for session in suite.sessions:
                            try:
                                session.close()
                            except Exception as error:
                                suite.cleanup_errors.append(str(error))
                    suite.log.close()
                    results.extend(suite.results)
                    wire.extend(suite.wire_failures)
                    cleanup.extend(suite.cleanup_errors)
                if child:
                    try:
                        child.stop()
                    except Exception as error:
                        cleanup.append(str(error))
            finally:
                try:
                    replacement_faults.close()
                except (OSError, ValueError) as error:
                    cleanup.append(str(error))
    report = {"transport": transport, "required": required, "results": results,
              "wire_failures": wire, "cleanup_errors": cleanup, "children": children,
              "resource_frames": [current.resource_frame_measurements for current in suites
                                  if hasattr(current, "resource_frame_measurements")]}
    (root / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    qualified(results, required, wire, cleanup)
    return report


def require_dedupe_readback(before, after, record, saved, text=None):
    require(before["configuration"]["settings"]["dedupe_policy"] == "SLOW" and
            after["status"]["effective"]["dedupe_policy"] == "FAST" and
            after["configuration"] == before["configuration"] and after["revision"] == before["revision"] and
            after["status"]["effective"] == before["status"]["effective"] and
            after["status"]["presets"] == before["status"]["presets"] and record.read_bytes() == saved,
            "DEDUPE readback changed or hid its configured/effective owner")
    require_cc_unchanged(before, after, record, saved)
    if text is not None:
        expected = "Dedupe        SLOW; effective FAST\n              Configured choice is unavailable on this server"
        require(expected in text and text.splitlines().count("Dedupe        SLOW; effective FAST") == 1,
                "SHOW SETTINGS does not distinguish saved SLOW from effective FAST")


def large_dedupe_reply(current, final):
    received, raw, lines = 0, bytearray(), []
    try:
        current.send("SHOW DEDUPE\r\n")
        while not lines or lines[-1] != final:
            line = current.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                    "large DEDUPE response exceeds final wire bound or breaks CRLF")
            text = line[:-2].decode("ascii", "strict")
            if not lines and text in ("", "SHOW DEDUPE"):
                continue
            require(len(text) <= 78, "large DEDUPE response exceeds line width")
            lines.append(text)
    except Exception:
        received += len(current.reader.buffer)
        raw.extend(current.reader.buffer[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{current.call}] SHOW DEDUPE\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[human receipt truncated at 65536 bytes]\n"
        current.suite.log.write(receipt)
        current.suite.log.flush()
    return "\n".join(lines), dedupe_read_barrier(current, harness.MAX_FRAME - received)


def dedupe_read_barrier(current, remaining):
    # The correlated frame closes this response. Session.get discards its
    # prefix, which could hide leaked content after a refusal or pause footer.
    identifier = current.suite.identifier()
    command = f"GET YAML CONFIG SCHEMA 7 ID {identifier}"
    current.send(command + "\r\n")
    document, prefix = current.frame("CONFIG", identifier, 7, command)
    echoed, trailing = False, 0
    for line in prefix.splitlines(keepends=True):
        require(line.endswith(b"\r\n") and b"\r" not in line[:-2],
                "large DEDUPE barrier prefix breaks CRLF")
        if current.reader.remote_echo and not echoed and line == command.encode("ascii") + b"\r\n":
            echoed = True
            continue
        require(line == b"\r\n", "large DEDUPE response has trailing content")
        trailing += len(line)
    require(trailing <= remaining, "large DEDUPE response exceeds final wire bound")
    require("error" not in document and isinstance(document.get("configuration"), dict) and
            isinstance(document.get("status"), dict) and isinstance(document.get("revision"), str) and
            bool(document["revision"]), "large DEDUPE barrier lacks a valid readback")
    require(document.get("resource") == "CONFIG" and type(document.get("schema_version")) is int and
            document["schema_version"] == 7 and document.get("request_id") == identifier,
            "large DEDUPE barrier has the wrong resource, schema or request identity")
    return document


def require_large_dedupe_output(text, value):
    _, paused = harness.split_auto_pause(text.splitlines())
    require(text.startswith('Dedupe: "') and "".join(re.findall(r'"(X+)"', text)) == value and
            text.count("; effective MED (grid2)") == 1 and "(fast=on med=on slow=on)" in text and
            "Configured choice is unrecognized" in text and paused,
            "large DEDUPE output lost exact value, runtime reason or automatic footer")


def large_dedupe_cases(suite, root, generation):
    users = root / "users"
    # The full native login now admits every promised view. Its 64KiB budget
    # rejects the old final-send boundary before a TCP session can own it.
    # Direct dedupe_frames owner probes retain the exact 49200/49366 send
    # boundary; these sessions prove fitting login and protected refusal.
    for count, call in ((40000, "VE3ABC-63"), (49366, "VE3ABC-64")):
        record = users / (call + ".yaml")
        if generation == 0:
            require(not record.exists(), "large DEDUPE requires an unused fixture owner")
            native = stored_document(suite.resource_seed)
            require(native["configuration_version"] == 7, "large DEDUPE seed is not native seven")
            native["configuration"]["settings"]["dedupe_policy"] = "X" * count
            record.write_bytes(native_storage_bytes(native))
        original = record.read_bytes()
        require(stored_configuration(original)["settings"]["dedupe_policy"] == "X" * count,
                "large DEDUPE restart prerequisite lost its exact saved choice")
        current = fixture_session(suite, call)
        before, saved = current.get(), record.read_bytes()
        if count == 49366:
            require("Could not load or upgrade your saved settings." in login_text(current, "") and
                    before["status"]["session"]["temporary_defaults"] is True and saved == original,
                    "oversized native login did not protect its original record")
            effective = before["status"]["effective"]["dedupe_policy"]
            require(effective == before["status"]["server"]["default_dedupe_policy"] and
                    before["configuration"]["settings"]["dedupe_policy"] == effective and
                    effective in ("FAST", "MED", "SLOW"), "protected login misstates its bounded fallback")
            key = "cqzone" if effective == "SLOW" else "grid2"
            expected = "Dedupe: " + effective + " (" + key + ") (fast=on med=on slow=on)"
            _, after = exact_human_reply(current, "SHOW DEDUPE", expected)
            require_cc_unchanged(before, after, record, original)
            continue
        require(before["status"]["configured"]["dedupe_policy"] == "X" * count and
                before["status"]["effective"]["dedupe_policy"] == "MED" and
                before["status"]["session"]["temporary_defaults"] is False,
                "large native record did not restore exact choice and existing MED interpretation")
        text, after = large_dedupe_reply(current, harness.PAUSE_FOOTER_END)
        require_large_dedupe_output(text, "X" * count)
        require_cc_unchanged(before, after, record, saved)


def decode_history_alias_page(raw, query, older):
    require(len(raw) <= harness.MAX_FRAME and raw.endswith(b"\r\n"), "history alias page exceeds its wire bound")
    rows, echoed, heading, partial = [], False, False, False
    lines = raw.splitlines(keepends=True)
    for i, line in enumerate(lines):
        require(line.endswith(b"\r\n") and b"\r" not in line[:-2], "history alias page breaks CRLF")
        text = line[:-2].decode("ascii", "strict")
        require(len(text) <= 78, "history alias page exceeds line width")
        if text == query.upper():
            require(not echoed and not rows and not heading and not partial, "history alias page repeats or interleaves echo")
            echoed = True
        elif text == "Older retained history page:":
            require(older and not heading and not rows and not partial, "history alias page has a wrong or duplicate heading")
            heading = True
        elif text.startswith("DX de"):
            require(not partial, "history alias page has rows after its early cutoff")
            require_history_row(text)
            rows.append(text)
        elif text == harness.HISTORY_PARTIAL:
            require(not partial, "history alias page repeats its early cutoff")
            partial = True
        elif text == "" and i < len(lines) - 1:
            continue
        else:
            require(i == len(lines) - 1, "history alias page has trailing content")
            more = text == harness.HISTORY_NEXT
            require(not partial or more, "history alias early cutoff lacks continuation")
            if more:
                require(bool(rows) or partial, "count-reached history alias page has no matching row")
            elif text == harness.HISTORY_END:
                require(bool(rows) or older, "history footer disagrees with returned rows")
            else:
                require(text == harness.HISTORY_EMPTY and not rows and not older,
                        "history footer disagrees with returned rows or search position")
            require(heading == older, "history alias page lost its continuation heading")
            return rows, more, partial
    raise ValueError("history alias page has no footer")


def history_alias_page(current, query, older=False):
    # Pipelining GET can interleave its echo with rows. Consume and log each
    # bounded page before sending the correlated readback.
    received, raw = 0, bytearray()
    try:
        current.send(query + "\r\n")
        while True:
            line = current.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                    "history alias page exceeds its bound or breaks CRLF")
            text = line[:-2].decode("ascii", "strict")
            if text in (harness.HISTORY_END, harness.HISTORY_EMPTY, harness.HISTORY_NEXT):
                break
            if text.startswith("DX de"):
                require_history_row(text)
            else:
                require(text in ("", query.upper(), harness.HISTORY_OLDER, harness.HISTORY_PARTIAL),
                        "unexpected or interleaved history output")
        rows, more, partial = decode_history_alias_page(bytes(raw), query, older)
    except Exception:
        received += len(current.reader.buffer)
        raw.extend(current.reader.buffer[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{current.call}] {query}\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[history receipt truncated at 65536 bytes]\n"
        current.suite.log.write(receipt)
        current.suite.log.flush()
    return rows, more, partial, complete_reply_config(current, received, "history")


def history_pages(current, query, next_query):
    """Yield bounded, checked pages, preserving the caller's plain NEXT alias."""
    deadline = time.monotonic() + harness.HISTORY_MAX_SECONDS
    older = query.endswith(" NEXT")
    for _ in range(harness.HISTORY_MAX_PAGES):
        require(time.monotonic() < deadline, "history continuation time budget exhausted")
        page = history_alias_page(current, query, older)
        yield page
        if not page[1]:
            return
        query, older = next_query, True
    raise ValueError("history continuation page budget exhausted")


def require_history_alias_row(rows, frequency, phrase):
    require(len(rows) == 1 and rows[0][39:63].rstrip() == "CW " + phrase and
            re.search(r"^DX de N0XYZ:\s+" + str(frequency) + r"\.00\s+W6ABC\s+", rows[0]),
            "history alias row has the wrong mode, spacing, identity, frequency or count")
    require_history_row(rows[0])


def history_alias_cases(suite, root, nonce, generation, saved_history):
    phrase = "C16" + nonce[:8].upper() + ":  UP5!"
    for dialect, call, prefixes in (("go", "VE3ABC-69", ("SHOW DX", "SH MYDX")),
                                    ("cc", "VE3ABC-70", ("SHOW/DX", "SH/DX", "SHOW MYDX", "SH DX"))):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        if generation == 0:
            current.human("DIALECT " + dialect)
        before = current.get()
        record = root / "users" / (call + ".yaml")
        saved = record.read_bytes()
        if generation:
            require((before["configuration"], before["status"]["presets"]) == saved_history[call],
                    "fresh child lost read-only history preferences")

        def unchanged(after):
            require(after["configuration"] == before["configuration"] and after["revision"] == before["revision"] and
                    after["status"]["effective"] == before["status"]["effective"] and
                    after["status"]["presets"] == before["status"]["presets"] and record.read_bytes() == saved,
                    "history alias request changed a preference owner")

        for index, prefix in enumerate(prefixes):
            values = "CW, COMMENT" if index % 2 else "COMMENT"
            query = prefix + " W6ABC 1 MODE " + values + " COMMENT " + phrase
            next_query = prefix + " NEXT"
            for rows, more, partial, after in history_pages(current, query, next_query):
                unchanged(after)
                if partial:
                    require(not rows, "one-row alias page returned a row before reaching its count")
                    continue
                require_history_alias_row(rows, 14064, phrase)
                require(more, "two matching alias stimuli did not produce continuation")
                break
            for invalid, expected in ((prefix + " W6ABC 1 MODE COMMENT up:5!", "Command input"),
                                      (prefix + " W6ABC 1 MODE CW, COMMENT FT8", "separate values with commas"),
                                      (prefix + " W6ABC 1 MODE COMMENT COMMENT", "Invalid COMMENT phrase")):
                _, after = current.human(invalid, expected)
                unchanged(after)
            found = []
            for rows, _, _, after in history_pages(current, next_query, next_query):
                unchanged(after)
                if rows:
                    require(not found, "alias continuation repeated a row or retained decoys")
                    require_history_alias_row(rows, 14063, phrase)
                    found.extend(rows)
            require(bool(found), "alias continuation failed to advance to the older row")
            _, after = current.human("SHOW DX NEXT", harness.HISTORY_NO_PENDING)
            unchanged(after)
        saved_history[call] = before["configuration"], before["status"]["presets"]


CC_HELP_LISTS = tuple("BAND MODE SOURCE EVENT CONFIDENCE PATH DXCONT DECONT DXZONE DEZONE "
                      "DXGRID2 DEGRID2 DXDXCC DEDXCC DXSTATE DESTATE DXCALL DECALL".split())
CC_HELP_SWITCHES = dict(BEACON="include_beacons", WWV="allow_wwv", WCY="allow_wcy",
                        ANNOUNCE="allow_announce", SELF="allow_self", TOXIC="allow_toxic")
CC_HELP_RULE_FIELDS = dict(bands="bands", modes="modes", sources="sources", events="events",
                           confidence="confidence", path_classes="pathclasses", dx_continents="dxcontinents",
                           de_continents="decontinents", dx_zones="dxzones", de_zones="dezones",
                           dx_grid2="dxgrid2", de_grid2="degrid2", dx_dxcc="dxdxcc", de_dxcc="dedxcc",
                           dx_states="dxstates", de_states="destates")


def cc_task_terminal(topic):
    if topic == "FILTER EXAMPLES":
        return "  HELP PRESETS"
    if topic in ("FILTERS", "PRESETS"):
        return harness.help_task_contract(topic, "cc")[1]
    return None


def decode_cc_help(raw, query, minimum_rows, minimum_seconds):
    require(0 < len(raw) <= harness.MAX_FRAME and raw.endswith(b"\r\n"), "CC HELP exceeds its complete wire bound")
    lines, echoed = [], False
    for line in raw.splitlines(keepends=True):
        require(line.endswith(b"\r\n") and b"\r" not in line[:-2], "CC HELP breaks CRLF")
        value = line[:-2].decode("ascii", "strict")
        require(len(value) <= 78 and all(32 <= ord(byte) <= 126 for byte in value), "CC HELP breaks width/ASCII")
        if value == query.upper():
            require(not echoed and not lines, "CC HELP duplicates or interleaves command echo")
            echoed = True
        else:
            lines.append(value)
    body, paused = harness.split_auto_pause(lines, minimum_rows, minimum_seconds)
    while body and body[0] == "":
        body.pop(0)
    topic = query.removeprefix("HELP ")
    task = topic in ("FILTERS", "FILTER EXAMPLES", "PRESETS")
    terminal = cc_task_terminal(topic)
    require((terminal is None and paused) or (terminal and body and body[-1] == terminal),
            "CC HELP lacks its complete ending")
    header = harness.help_task_contract(topic, "cc")[0] if task else topic + " - "
    matches = bool(body) and (body[0] == header if task else body[0].startswith(header))
    require(matches, "CC HELP has the wrong topic header")
    return body


def bounded_filter_refusal(message, indent=""):
    import textwrap
    return "\n".join("\n".join(textwrap.wrap(line, width=78, subsequent_indent=indent,
                                           break_long_words=False, break_on_hyphens=False))
                     for line in message.split("\n")) + "\nNo changes applied."


def native_retired_reply(command):
    if command in ("PASS", "REJECT") or command.startswith("PASS ") or command.startswith("REJECT "):
        message = "Use HELP SET/FILTER for native CC filters. Boolean rule edits require DIALECT GO."
    elif command.startswith("UNSET/FILTER"):
        message = "UNSET/FILTER changes the six switches. For geography or DXBM use SET/FILTER with /PASS, /REJECT or /OFF."
    else:
        message = "Use SET/FILTER <category>/PASS <list>, /REJECT <list> or /OFF. Per-band geography terminal syntax is unverified; use schema-seven YAML."
    return bounded_filter_refusal(message)


def require_cc_help_grammar(lines, topic):
    if topic in ("FILTERS", "FILTER EXAMPLES", "PRESETS"):
        harness.require_help_task(lines, topic, "cc")
        return
    require(topic in ("SET/FILTER", "UNSET/FILTER"), "unsupported detailed CC HELP oracle")
    require(lines[0] == topic + " - Change native CC selections or switches.", "wrong native CC HELP header")
    usages = [line[7:] for line in lines if line.startswith("Usage: ") or line.startswith("       " + topic)]
    expected = [topic + " <switch> [ALL]"]
    if topic == "SET/FILTER":
        expected += ["SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/<PASS|REJECT> <list>",
                     "SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/OFF",
                     "SET/FILTER <country-list>/PASS or /REJECT",
                     "SET/FILTER DXBM/PASS <band-or-slot-list>",
                     "SET/FILTER DXBM/REJECT <band-or-slot-list>", "SET/FILTER DXBM/OFF"]
    require(usages == expected, "native CC HELP broadens or omits supported grammar")
    flat = " ".join(" ".join(lines).split())
    for phrase in ("Switches accept no value or ALL.", "PC93 aliases ANNOUNCE.",
                   "SELF controls spots of your own call", "OWN, BOB, DX/NODX, TALK, WX"):
        require(phrase in flat, "native CC HELP loses switch/unsupported-feature boundaries")
    if topic == "SET/FILTER":
        for phrase in ("Each geography edit replaces", "Other categories remain unchanged.",
                       "First PASS after OFF starts a whitelist", "Later edits keep other slots.",
                       "Per-band geography is a GoCluster schema-seven YAML extension.",
                       "do not split them across replacing commands.", "Native bands:", "Native slots:", "State/province codes:"):
            require(phrase in flat, "native CC HELP loses category ownership: " + phrase)
    else:
        require("UNSET/FILTER does not accept inline /ON or /OFF or geography lists." in flat,
                "native CC HELP invents UNSET geography/inline syntax")
    require("NEARBY ON|OFF" not in flat and "CC list types:" not in flat and "MINSNR <" not in flat,
            "native CC HELP advertises retired category syntax")


def cc_help_readback(current, topic, minimum_rows, minimum_seconds):
    query, raw, received = "HELP " + topic, bytearray(), 0
    terminal = cc_task_terminal(topic)
    try:
        current.send(query + "\r\n")
        while True:
            line = current.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME, "CC HELP exceeds its wire bound")
            if line == ((terminal or harness.PAUSE_FOOTER_END) + "\r\n").encode("ascii"):
                break
        if terminal and len(help_routing_lines(bytes(raw), query, current.reader.remote_echo)) >= minimum_rows:
            for _ in range(2):
                line = current.reader.line()
                received += len(line)
                raw.extend(line[:harness.MAX_FRAME - len(raw)])
                require(received <= harness.MAX_FRAME, "CC HELP footer exceeds its wire bound")
        body = decode_cc_help(bytes(raw), query, minimum_rows, minimum_seconds)
    except Exception:
        received += len(current.reader.buffer)
        raw.extend(current.reader.buffer[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{current.call}] {query}\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[CC HELP receipt truncated at 65536 bytes]\n"
        current.suite.log.write(receipt)
        current.suite.log.flush()
    identifier = current.suite.identifier()
    command = f"GET YAML CONFIG SCHEMA 7 ID {identifier}"
    current.send(command + "\r\n")
    document, prefix = current.frame("CONFIG", identifier, 7, command)
    echo, trailing = False, 0
    for line in prefix.splitlines(keepends=True):
        require(line.endswith(b"\r\n") and b"\r" not in line[:-2], "CC HELP barrier breaks CRLF")
        if current.reader.remote_echo and not echo and line == command.encode("ascii") + b"\r\n":
            echo = True
            continue
        require(line == b"\r\n", "CC HELP hides trailing response content")
        trailing += len(line)
    require(received + trailing <= harness.MAX_FRAME, "CC HELP final response exceeds its wire bound")
    require("error" not in document and type(document.get("configuration")) is dict and
            type(document.get("status")) is dict and type(document.get("revision")) is str and document["revision"],
            "CC HELP barrier lacks a valid configuration")
    require(document.get("resource") == "CONFIG" and type(document.get("schema_version")) is int and
            document["schema_version"] == 7 and document.get("request_id") == identifier,
            "CC HELP barrier has the wrong resource, schema or request identity")
    require_cc_help_grammar(body, topic)
    return document


def require_cc_typed_value(actual, expected):
    require(type(actual) is type(expected), "CC command state has the wrong value type")
    if type(expected) is dict:
        require(actual.keys() == expected.keys(), "CC command state has missing or extra fields")
        require({(type(key), key) for key in actual} == {(type(key), key) for key in expected}, "CC command state has the wrong key type")
        for key in expected:
            require_cc_typed_value(actual[key], expected[key])
    elif type(expected) is list:
        require(len(actual) == len(expected), "CC command state has the wrong list length")
        for value, wanted in zip(actual, expected):
            require_cc_typed_value(value, wanted)
    else:
        require(actual == expected, "CC command state differs from the intended outcome")


def require_cc_saved_state(document, expected, record):
    require_cc_typed_value(document["configuration"], expected)
    saved = copy.deepcopy(stored_configuration(record.read_bytes()))
    # cw_speed is intentionally optional false on disk, explicit on schema-seven wire.
    saved["settings"].setdefault("cw_speed", False)
    require_cc_typed_value(saved, expected)
    require(set(expected["filters"]) == {"go", "cc"} and
            document["status"]["active_dialect"] == expected["settings"]["dialect"] and
            set(document["status"]["presets"]) == {"go", "cc"}, "missing active/inactive bank owner")


def scoped_bank_fixture(configuration, dialect, label):
    expected = copy.deepcopy(configuration)
    expected["settings"].update(dialect=dialect, grid="FN31PR", noise_class="URBAN", dedupe_policy="SLOW",
                                path_min_observation_count=30, solar_summary_minutes=30)
    expected["filters"]["go"]["rules"] = [dict(slot=0, action="pass", expr='MODE=CW AND COMMENT="' + label + '" AND NOT COMMENT="QRT" AND (SOURCE=HUMAN OR SNR>=7)')]
    cc = expected["filters"]["cc"]
    cc.update(modes=["CW"], sources=["HUMAN"], dxbm=dict(active=False, base_allow=True, slots={}))
    cc["geography"] = {key: {"global": {"action": "off"}, "bands": {}} for key in ("DOC", "DOS", "DXCTY", "DXSTATE")}
    for bank in expected["filters"].values():
        bank.update({field: True for field in CC_HELP_SWITCHES.values()})
    return expected


def require_cc_unchanged(before, after, record, saved):
    for field in ("configuration", "revision"):
        require_cc_typed_value(after[field], before[field])
    require(after["status"].keys() == before["status"].keys(), "readback changed status shape or owners")
    for field in before["status"]:
        if field != "session":
            require_cc_typed_value(after["status"][field], before["status"][field])
    require(record.read_bytes() == saved, "refusal/readback changed durable bytes or preset references")


def require_cc_success(text, before, after, expected, record):
    require(text.strip() and not re.search(r"(?:^|\n)(?:Unknown\b|Invalid\b|Usage:|Unable\b|Warning:|No changes applied|Filter unavailable)", text),
            "CC advertised command was refused or unknown")
    require(expected != before["configuration"] and after["revision"] != before["revision"],
            "CC advertised command has no proven state transition")
    require_cc_saved_state(after, expected, record)


def resume_help_fixture(current, record, before, *, diagnostic=None, pause=False):
    # Independent successful groups reconnect the same durable identity. This
    # never retries a failed command or changes the server's pacing policy.
    references = copy.deepcopy(stored_document(record.read_bytes())["presets"])
    current.close()
    restored = fixture_session(current.suite, current.call)
    after = restored.get()
    require_cc_saved_state(after, before["configuration"], record)
    require_cc_typed_value(after["status"]["presets"], before["status"]["presets"])
    require_cc_typed_value(stored_document(record.read_bytes())["presets"], references)
    session = after["status"]["session"]
    require(session["diagnostic_comments"] == "OFF" and session["pause_active"] is False and
            session["pause_pending_delivery"] is False and session["pause_remaining_seconds"] == 0 and
            session["suppressed_spots"] == 0, "HELP reconnect retained session-only owners")
    commands = []
    if diagnostic is not None:
        commands.append(("SET DIAG " + diagnostic, "Diagnostic comments: " + diagnostic))
    if pause:
        commands.append(("PAUSE 300", harness.pause_footer(300)))
    for command, feedback in commands:
        prior, saved = after, record.read_bytes()
        _, after = restored.human(command, feedback)
        require_cc_unchanged(prior, after, record, saved)
    require(after["status"]["session"]["diagnostic_comments"] == (diagnostic or "OFF") and
            after["status"]["session"]["pause_active"] is pause,
            "HELP reconnect did not establish requested session-only owners")
    return restored, after


def cc_help_grammar_cases(suite, root, generation, saved_cc_grammar):
    call, record = "VE3ABC-71", root / "users" / "VE3ABC-71.yaml"
    if not generation:
        require(not record.exists(), "CC grammar control requires a fresh unused SSID")
    current = fixture_session(suite, call)
    current.human("PAUSE 300")
    current.human("DIALECT CC")
    before = current.get()
    defaults = copy.deepcopy(before["configuration"]["filters"]["cc"]) if not generation else saved_cc_grammar[call][1]
    if generation:
        require_cc_saved_state(before, saved_cc_grammar[call][0], record)
    private = stored_document((root / "config/runtime.yaml").read_bytes())["telnet"]
    minimum_rows, minimum_seconds = private["auto_read_pause_min_rows"], private["auto_read_pause_seconds"]
    require(minimum_rows > 0 and minimum_seconds > 0 and before["status"]["server"]["auto_read_pause_min_rows"] == minimum_rows and
            before["status"]["server"]["auto_read_pause_seconds"] == minimum_seconds, "CC HELP lacks loaded pause owners")
    for topic in ("SET/FILTER", "UNSET/FILTER", "FILTERS", "FILTER EXAMPLES", "PRESETS"):
        saved = record.read_bytes()
        after = cc_help_readback(current, topic, minimum_rows, minimum_seconds)
        require_cc_unchanged(before, after, record, saved)
        before = after
    current, state = resume_help_fixture(current, record, before, pause=True)

    def apply(command, changes):
        nonlocal state
        prior = state
        expected = copy.deepcopy(prior["configuration"])
        expected["filters"]["cc"].update(copy.deepcopy(changes))
        text, after = current.human(command)
        require_cc_success(text, prior, after, expected, record)
        require(after["status"]["presets"]["go"] == prior["status"]["presets"]["go"], "CC edit changed inactive GO reference")
        state = after
        return after

    def refuse(command, feedback):
        nonlocal state
        prior, saved = state, record.read_bytes()
        _, after = exact_human_reply(current, command, feedback)
        require_cc_unchanged(prior, after, record, saved)
        state = after

    # Native replacing/incremental selections and the six surviving switches.
    for index, (domain, field) in enumerate(CC_HELP_SWITCHES.items()):
        if index > 0:
            current, state = resume_help_fixture(current, record, state, pause=True)
        if state["configuration"]["filters"]["cc"][field] is not True:
            apply("SET/FILTER " + domain, {field: True})
        apply("UNSET/FILTER " + domain + " ALL", {field: False})
        apply("SET/FILTER " + domain + "/ON", {field: True})
    current, state = resume_help_fixture(current, record, state, pause=True)
    for index, (command, field, enabled) in enumerate((("SET/NOFT8", "modes", False), ("SET/FT8", "modes", True),
                                     ("UNSET/SKIMMER", "sources", False), ("SET/SKIMMER", "sources", True))):
        if index == 2:
            current, state = resume_help_fixture(current, record, state, pause=True)
        prior = state["configuration"]["filters"]["cc"]
        value = "FT8" if field == "modes" else "SKIMMER"
        if (value in prior[field]) is enabled:
            opposite = "SET/FT8" if field == "modes" and not enabled else "SET/NOFT8" if field == "modes" else "SET/SKIMMER" if not enabled else "UNSET/SKIMMER"
            values = sorted(set(prior[field]) | {value}) if not enabled else [item for item in prior[field] if item != value]
            apply(opposite, {field: values})
        prior = state["configuration"]["filters"]["cc"]
        values = sorted(set(prior[field]) | {value}) if enabled else [item for item in prior[field] if item != value]
        apply(command, {field: values})
    current, state = resume_help_fixture(current, record, state, pause=True)
    for category, action, values in (("DOC", "pass", {"countries": [291]}), ("DOC", "reject", {"countries": [1]}),
                                      ("DXSTATE", "pass", {"states": ["CA"]}), ("DOC", "off", {}), ("DXSTATE", "off", {})):
        geo = copy.deepcopy(state["configuration"]["filters"]["cc"]["geography"])
        geo[category] = {"global": {"action": action, **values}, "bands": {}}
        suffix = "" if action == "off" else " " + ("CA" if category == "DXSTATE" else "291" if action == "pass" else "1")
        apply("SET/FILTER " + category + "/" + action.upper() + suffix, {"geography": geo})
    current, state = resume_help_fixture(current, record, state, pause=True)
    if state["configuration"]["filters"]["cc"]["dxbm"]["active"]:
        apply("SET/FILTER DXBM/OFF", {"dxbm": dict(active=False, base_allow=True, slots={})})
    apply("SET/FILTER DXBM/PASS 20-CW", {"dxbm": dict(active=True, base_allow=False, slots={"20-CW": True})})
    apply("SET/FILTER DXBM/REJECT 20-CW", {"dxbm": dict(active=True, base_allow=False, slots={})})
    apply("SET/FILTER DXBM/OFF", {"dxbm": dict(active=False, base_allow=True, slots={})})
    for command in ("PASS COMMENT CQ", "PASS MINSNR CW 7", "SET/FILTER NEARBY ON", "SET/FILTER BAND 20",
                    "UNSET/FILTER MODE CW", "SET/FILTER DXBM 20 CW", "SET/FILTER NOFILTER"):
        refuse(command, native_retired_reply(command))
    before = state
    text, after = current.human("SET/NOFILTER")
    require(text.strip() and "No changes applied" not in text, "native reset refused")
    expected = copy.deepcopy(before["configuration"])
    expected["filters"]["cc"] = copy.deepcopy(defaults)
    expected["filters"]["cc"].update({field: True for field in CC_HELP_SWITCHES.values()})
    require_cc_saved_state(after, expected, record)
    state = after
    after = apply("UNSET/FILTER BEACON", dict(include_beacons=False))
    saved_cc_grammar[call] = copy.deepcopy(after["configuration"]), copy.deepcopy(defaults)


HELP_SHORTCUT_MODES = ("CW", "FT2", "FT4", "FT8", "PSK", "RTTY")


def help_route_contract(key, dialect):
    # Source-literal heads, complete usages/aliases and terminal lines. The
    # output itself never supplies an expected contract.
    contracts = {
        "DX": ("DX - Post a spot (human entry).",
               ("DX <freq_khz> <callsign> [comment]", "DX <callsign> <freq_khz> [comment]"), None, ()),
        **{topic: (harness.help_task_contract(topic, dialect)[0], (),
                   "  HELP PRESETS" if topic == "FILTER EXAMPLES" else harness.help_task_contract(topic, dialect)[1], ())
           for topic in ("FILTERS", "FILTER EXAMPLES", "PRESETS")},
        "OVERVIEW": ("Getting help", (), "  HELP PRESETS     Learn how to manage saved setups.", ()),
        "HELP": ("HELP - Show command list or command-specific help.", ("HELP [command]",),
                 "  With a command, shows detailed usage.", ("H",)),
        "SHOW BUILD": ("SHOW BUILD - Show binary build metadata.", ("SHOW BUILD",),
                       "  Shows the build version, release tag, and Go version when available.", ("SH BUILD",)),
        "SHOW OWN": ("SHOW OWN - Show your login and baseline own call.", ("SHOW OWN",),
                     "  Numeric SSIDs are ignored for own-call features.", ("SH OWN",)),
        "SHOW DXCC": ("SHOW DXCC - Look up DXCC/ADIF and zones.", ("SHOW DXCC <prefix|callsign>",),
                      "  Returns other prefixes for the same country when available.", ("SH DXCC",)),
        "SHOW PROP": ("SHOW PROP - Show point-to-point propagation outlook.",
                      ("SHOW PROP <call|prefix|grid> [band] [mode]",),
                      "  Only rows with REL HIGH, MEDIUM, or LOW are shown.", ("SH PROP", "SHOW/PROP", "SH/PROP")),
        "SHOW HOLD": ("SHOW HOLD - Show read-pause status.", ("SHOW HOLD",),
                      "  Shows remaining pause time and suppressed spot count.", ()),
        "SHOW SETTINGS": ("SHOW SETTINGS - Display preferences and session behavior.", ("SHOW SETTINGS",),
                          "  65,536 bytes.", ("SHOW/SETTINGS", "SH/SETTINGS")),
        "SHOW DEDUPE": ("SHOW DEDUPE - Show your broadcast dedupe policy.", ("SHOW DEDUPE",),
                        "  Shows if a policy is disabled server-side.", ()),
        "SET SOLAR": ("SET SOLAR - Opt into solar summaries.", ("SET SOLAR <15|30|60|OFF>",),
                      "  OFF disables summaries.", ()),
        "SET GRID": ("SET GRID - Set your grid for path reliability glyphs.", ("SET GRID <locator>",),
                     "  Unavailable grid mapping is reported separately from invalid syntax.", ()),
        "SET NOISE": ("SET NOISE - Set your noise class for glyphs.",
                      ("SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>",), "  Default is QUIET when unset.", ()),
        "SET PATHSAMPLES": ("SET PATHSAMPLES - Set your path sample floor.", ("SET PATHSAMPLES <count|DEFAULT>",),
                            "  DEFAULT clears your personal override.", ()),
        "SET DIAG": ("SET DIAG - Select diagnostic comments.", ("SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>",),
                     "  MODE shows <mode>|<provenance>.", ()),
        "SET DEDUPE": ("SET DEDUPE - Select broadcast dedupe policy.", ("SET DEDUPE <FAST|MED|SLOW>",),
                       "  If a policy is disabled, the nearest available is chosen.", ()),
        "DIALECT": ("DIALECT - Show or switch filter command dialect.",
                    ("DIALECT", "DIALECT LIST", "DIALECT <go|cc>"), "  Dialect selection does not change session-only VE7CC spot output.", ()),
        "BYE": ("BYE - Disconnect from the cluster.", ("BYE",), "Aliases: QUIT, EXIT", ("QUIT", "EXIT")),
        "VALIDATE YAML CONFIG": ("VALIDATE YAML CONFIG - Check a complete client proposal.",
                                 ("VALIDATE YAML CONFIG", "VALIDATE YAML CONFIG SCHEMA 7"),
                                 "  Does not change revisions, pause, diagnostics or scheduling.", ()),
    }

    def last_note(note):
        import textwrap
        return textwrap.wrap(note, width=78, initial_indent="  ", subsequent_indent="  ",
                             break_long_words=False, break_on_hyphens=False)[-1]

    contracts["SHOW SETTINGS"] = ("SHOW SETTINGS - Display preferences and session behavior.", ("SHOW SETTINGS",),
        last_note("The complete response, including its pause footer, is limited to 65,536 bytes."), ("SHOW/SETTINGS", "SH/SETTINGS"))
    parent_usages = (("SHOW DX [count|selector]",) if dialect == "go" else ()) + (
        "SHOW MYDX [count|selector]", "SHOW DXCC <prefix|callsign>", "SHOW WWV [FULL]", "SHOW WCY [FULL]",
        "SHOW PROP <call|prefix|grid> [band] [mode]", "SHOW BUILD", "SHOW OWN")
    contracts["SHOW"] = ("SHOW - See SHOW subcommands.", parent_usages,
                         "  Use HELP " + ("SHOW DX" if dialect == "go" else "SHOW/DX") + " for the history alias.", ("SH",))
    for topic in ("SHOW MYDX", "SHOW DX" if dialect == "go" else "SHOW/DX"):
        summary = "Show filtered spot history." if topic == "SHOW MYDX" else "Alias of SHOW MYDX (stored history)."
        usage = (topic + " [count]", topic + " <prefix|callsign> [count]", topic + " [count] <prefix|callsign>",
                 topic + " NEXT [cursor]", topic + " [selector] [count] COMMENT <phrase>",
                 topic + " [selector] [count] BAND <list> MODE <list>")
        contracts[topic] = (topic + " - " + summary, usage, None,
                           ("SH MYDX",) if topic == "SHOW MYDX" else ("SH DX",) if dialect == "go" else ("SH/DX", "SHOW DX", "SH DX"))
    filter_topic = "SHOW FILTER" if dialect == "go" else "SHOW/FILTER"
    contracts[filter_topic] = ((filter_topic + " - Display current filter state.",
        (filter_topic, filter_topic + " RULE", filter_topic + " FULL"), None, ("SHOW/FILTER", "SH/FILTER")) if dialect == "go" else
        (filter_topic + " - Inspect native CC state.",
         (filter_topic + " [DOC|DOS|DXCTY|DXSTATE|DXBM|MODE|SOURCE|FULL]",),
         last_note("GET YAML CONFIG SCHEMA 7 provides a complete both-bank backup. Schema 6 does not."), ("SHOW FILTER", "SH/FILTER")))
    for verb in ("PASS", "REJECT"):
        if dialect == "go":
            summary = "Allow matching spots or enable separate controls." if verb == "PASS" else "Veto matching spots or disable separate controls."
            contracts[verb] = (verb + " - " + summary,
                (verb + " RULE <0..9> <expression>", verb + " <BEACON|WWV|WCY|ANNOUNCE|SELF> [ALL]"),
                "  Old category mutation syntax is refused, not translated; use HELP RULE.", ())
        else:
            contracts[verb] = (verb + " - Change separate delivery switches.",
                (verb + " <BEACON|WWV|WCY|ANNOUNCE|SELF> [ALL]",),
                last_note("Numbered PASS and REJECT rules are for GO. In CC, see HELP SET/FILTER."), ())
    for topic in ("SET/FILTER", "UNSET/FILTER"):
        usages = [topic + " <switch> [ALL]"]
        if topic == "SET/FILTER":
            usages += ["SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/<PASS|REJECT> <list>", "SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/OFF",
                       "SET/FILTER <country-list>/PASS or /REJECT", "SET/FILTER DXBM/PASS <band-or-slot-list>",
                       "SET/FILTER DXBM/REJECT <band-or-slot-list>", "SET/FILTER DXBM/OFF"]
        contracts[topic] = (topic + " - Change native CC selections or switches.", tuple(usages), None, ())
    for topic in ("RESET FILTER COMMENT", "SHOW FILTER COMMENT", "PASS COMMENT", "REJECT COMMENT", "PASS MINSNR", "REJECT MINSNR"):
        if dialect == "cc":
            contracts[topic] = (topic + " - GO dialect only.", ("DIALECT GO",),
                last_note("Switching banks preserves the other bank. Use HELP SET/FILTER or HELP FILTERS for native CC commands."), ())
        elif topic.endswith("MINSNR"):
            contracts[topic] = (topic + " - Replaced by Boolean SNR expressions.", ("HELP RULE",),
                "  Missing SNR makes comparisons false; NOT reverses that result.", ())
        else:
            contracts[topic] = (topic + " - Replaced by numbered Boolean rules.", ("HELP RULE",),
                "  Use SHOW FILTER RULE to read saved and effective expressions.", ())
    contracts["RESET FILTER"] = (("RESET FILTER - Reset filters without restoring defaults.",
        ("RESET FILTER", "RESET FILTER RULE <0..9|ALL>"), "  No rules means no personal restriction. See HELP RULE.", ()) if dialect == "go" else
        ("RESET FILTER - GO dialect only.", ("DIALECT GO",),
         last_note("Switching banks preserves the other bank. Use HELP SET/FILTER or HELP FILTERS for native CC commands."), ()))
    contracts["PASS NEARBY"] = (("NEARBY - Boolean proximity predicate.",
        ("PASS RULE 0 NEARBY", "PASS RULE 0 MODE=CW AND (DXCONT=EU OR NEARBY)"),
        last_note("The old PASS/REJECT NEARBY ON/OFF commands are refused; use numbered rules."), ("PASS NEARBY", "REJECT NEARBY")) if dialect == "go" else
        ("NEARBY - GO dialect only.", ("DIALECT GO",),
         last_note("Switching banks preserves the other bank. Use HELP SET/FILTER or HELP FILTERS for native CC commands."), ()))
    for topic in ("SET/NOFILTER", "SET/<MODE>", "SET/NO<MODE>"):
        note = ("Restores current operator mode/source defaults and clears geography, band exceptions and DXBM. Keeps six switches, inactive GO bank and shared preferences." if topic == "SET/NOFILTER" else
                "SET/<MODE> enables; SET/NO<MODE> or UNSET/<MODE> disables. Empty mode/source selections admit no reports.")
        contracts[topic] = (topic + " - Change a native CC selection.", (topic,), last_note(note), ())
    contracts["SET/ANN"] = ("SET/ANN - Enable announcements.", ("SET/ANN",), "  Alias of PASS ANNOUNCE.", ())
    contracts["SET/NOANN"] = ("SET/NOANN - Disable announcements.", ("SET/NOANN",), "  Alias of REJECT ANNOUNCE.", ())
    for verb, summary in (("SAVE", "Save filters and preferences to a preset."), ("LIST", "List your saved presets."),
                          ("LOAD", "Load a preset and save this SSID's defaults."), ("DELETE", "Delete a saved preset.")):
        topic = verb + " PRESET"
        contracts[topic] = (topic + " - " + summary, (topic + ("" if verb == "LIST" else " <name>"),), None, ())
    for resource in ("FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"):
        topic = "GET YAML " + resource
        contracts[topic] = (topic + " - Read " + resource.lower() + " for clients.",
                           (topic + " SCHEMA 6 [ID <id>]", topic + " SCHEMA 7 [ID <id>]"), None, ())
    for verb in ("PUT", "PATCH"):
        for resource in ("FILTER", "SETTINGS", "CONFIG"):
            topic = verb + " YAML " + resource
            contracts[topic] = (topic + " - Write " + resource.lower() + " for clients.", (topic, topic + " SCHEMA 7"), None, ())
    require(key in contracts, "unknown independent HELP contract")
    return contracts[key]


def help_routing_topics(dialect):
    topics = [("H", "HELP"), ("SH", "SHOW"), ("SH BUILD EXTRA", "SHOW BUILD"), ("SH OWN EXTRA", "SHOW OWN"),
              ("SH DXCC DL EXTRA", "SHOW DXCC"), ("SH MYDX 1 MODE COMMENT", "SHOW MYDX"),
              ("SH DX W6ABC 1", "SHOW DX" if dialect == "go" else "SHOW/DX"),
              ("SH PROP DL 20 CW", "SHOW PROP"), ("SHOW/PROP DL 20 CW", "SHOW PROP"),
              ("SH/PROP DL 20 CW", "SHOW PROP"), ("SHOW HOLD EXTRA", "SHOW HOLD"),
              ("SHOW SETTINGS EXTRA", "SHOW SETTINGS"), ("SHOW DEDUPE EXTRA", "SHOW DEDUPE"),
              ("SET SOLAR OFF EXTRA", "SET SOLAR"), ("SET GRID IO91 EXTRA", "SET GRID"),
              ("SET NOISE QUIET EXTRA", "SET NOISE"), ("SET PATHSAMPLES DEFAULT EXTRA", "SET PATHSAMPLES"),
              ("SET DIAG OFF EXTRA", "SET DIAG"), ("SET DEDUPE FAST EXTRA", "SET DEDUPE"),
              ("DIALECT LIST EXTRA", "DIALECT"), ("QUIT EXTRA", "BYE"), ("EXIT EXTRA", "BYE"),
              ("RESET FILTER EXTRA", "RESET FILTER"), ("RESET FILTER COMMENT PASS EXTRA", "RESET FILTER COMMENT"),
              ("PASS BAND 40 EXTRA", "PASS"), ("REJECT MODE CW EXTRA", "REJECT"),
              ("PASS COMMENT ROUTE EXTRA", "PASS COMMENT"), ("REJECT COMMENT ROUTE EXTRA", "REJECT COMMENT"),
              ("PASS MINSNR CW 3 EXTRA", "PASS MINSNR"), ("REJECT MINSNR CW NONE EXTRA", "REJECT MINSNR"),
              ("PASS NEARBY OFF EXTRA", "PASS NEARBY"), ("REJECT NEARBY OFF EXTRA", "PASS NEARBY")]
    filter_topic = "SHOW FILTER" if dialect == "go" else "SHOW/FILTER"
    topics += [(head + " FULL EXTRA", filter_topic) for head in ("SHOW FILTER", "SHOW/FILTER", "SH/FILTER")]
    topics += [(head + " COMMENT EXTRA", "SHOW FILTER COMMENT") for head in ("SHOW FILTER", "SHOW/FILTER", "SH/FILTER")]
    topics += [(verb + " PRESET ROUTE18 EXTRA", verb + " PRESET") for verb in ("SAVE", "LIST", "LOAD", "DELETE")]
    topics += [("GET YAML " + resource + " SCHEMA 7 ID ROUTE18", "GET YAML " + resource)
               for resource in ("FILTER", "SETTINGS", "CONFIG", "CAPABILITIES")]
    topics += [(verb + " YAML " + resource + " EXTRA", verb + " YAML " + resource)
               for verb in ("PUT", "PATCH") for resource in ("FILTER", "SETTINGS", "CONFIG")]
    topics += [("VALIDATE YAML CONFIG EXTRA", "VALIDATE YAML CONFIG")]
    unknown = ["SH FILTER", "SH SETTINGS", "SH DEDUPE", "SH HOLD", "SHOW/MYDX 1", "SHOW BUILDX", "SH OWNX",
               "SHOW DXCCX DL", "GET YAML CONFIGX ID ROUTE18", "SET SOLARX 30"]
    if dialect == "cc":
        topics += [(head + " 1", "SHOW/DX") for head in ("SHOW/DX", "SH/DX")]
        topics += [("SET/FILTER BAND 40", "SET/FILTER"), ("UNSET/FILTER MODE CW", "UNSET/FILTER"),
                   ("SET/NOFILTER EXTRA", "SET/NOFILTER"), ("SET/ANN EXTRA", "SET/ANN"), ("SET/NOANN EXTRA", "SET/NOANN")]
        topics += [("SET/" + token + " EXTRA", "SET/<MODE>") for token in ("CW_ALIAS", "COMMENT", "BAND", "MODE")]
        topics += [("SET/NO" + token + " EXTRA", "SET/NO<MODE>") for token in ("CW_ALIAS", "COMMENT")]
        unknown += ["SET/TEST_MODE EXTRA", "SET/NOTEST_MODE EXTRA", "SET/UNLISTED EXTRA"]
    else:
        unknown += ["SHOW/DX 1", "SH/DX 1", "SET/CW_ALIAS EXTRA", "SET/COMMENT EXTRA"]
    return topics + [(topic, None) for topic in unknown]


def help_routing_lines(raw, query, remote_echo):
    require(0 < len(raw) <= harness.MAX_FRAME and raw.endswith(b"\r\n"), "HELP routing exceeds its complete wire bound")
    lines, echoed = [], False
    for line in raw.splitlines(keepends=True):
        require(line.endswith(b"\r\n") and b"\r" not in line[:-2], "HELP routing breaks CRLF")
        value = line[:-2].decode("ascii", "strict")
        require(len(value) <= 78 and all(32 <= ord(ch) <= 126 for ch in value), "HELP routing breaks width/ASCII")
        if value == query.upper():
            require(remote_echo and not echoed and not any(lines), "HELP routing duplicates or interleaves command echo")
            echoed = True
        else:
            lines.append(value)
    return lines


def decode_help_routing(raw, query, contract, minimum_rows, minimum_seconds, remote_echo=False):
    lines = help_routing_lines(raw, query, remote_echo)
    lines, paused = harness.split_auto_pause(lines, minimum_rows, minimum_seconds)
    while lines and lines[0] == "":
        lines.pop(0)
    header, usage, terminal, allowed_aliases = contract
    intro = []
    if header.startswith(("SHOW DX - Alias of", "SHOW/DX - Alias of")):
        show = header.split(" - ", 1)[0]
        intro = [f"Example: {show} 10 - Show up to 10 recent spots that match your filters.",
                 f"  {show + ' K1ABC 10':<29}Search for a callsign.",
                 f"  {show + ' 10 BAND 20 MODE CW':<29}Search for CW spots on 20m.",
                 f"  {show + ' 10 COMMENT CQ':<29}Find spots with CQ in the comment.",
                 "These searches don't change your saved filters.", ""]
    elif header.startswith(("PASS - ", "REJECT - ")):
        intro = ["Spot rules (GO only)", "  PASS RULE 0 MODE=CW       Allow CW under rule 0.",
                 "  REJECT RULE 9 MODE=FT8    Block FT8 even if a PASS rule allows it.",
                 "  See HELP RULE for more examples.", "", "Delivery switches (GO and CC)",
                 "  Switch     Turn on          Turn off", "  WWV        PASS WWV         REJECT WWV",
                 "  WCY        PASS WCY         REJECT WCY", "  ANNOUNCE   PASS ANNOUNCE    REJECT ANNOUNCE",
                 "  BEACON     PASS BEACON      REJECT BEACON", "",
                 "WWV, WCY, and announcements are live messages, not spots.",
                 "Numbered rules do not filter those messages.",
                 "Beacon spots still go through your spot filters when BEACON is on.",
                 "Changing a switch does not change any numbered rule.", ""]
    require(lines[:len(intro)] == intro and len(lines) > len(intro) and lines[len(intro)] == header,
            "HELP routing selects the wrong intended header or introduction")
    body = lines[len(intro):]
    if header.startswith("Unknown help topic: "):
        require(lines == [header, "Type HELP for available commands."], "unsupported HELP route was accepted or altered")
    else:
        require(not any(line.startswith(("Unknown ", "Usage: Invalid", "Invalid ")) for line in lines),
                "supported HELP route contains rejection feedback")
        expected = [("Usage: " if index == 0 else "       ") + value for index, value in enumerate(usage)]
        require(body[1:1 + len(expected)] == expected and
                (len(body) == len(expected) + 1 or not body[1 + len(expected)].startswith("       ")),
                "HELP routing loses or broadens the intended usage")
        aliases = []
        for index, line in enumerate(lines):
            if line.startswith("Aliases: "):
                aliases.extend(value.strip() for value in line[9:].split(","))
                index += 1
                while index < len(lines) and lines[index].startswith("         "):
                    aliases.extend(value.strip() for value in lines[index].split(","))
                    index += 1
        require(len(aliases) == len(set(aliases)) and set(aliases) == set(allowed_aliases),
                "HELP routing loses a supported alias or advertises an unsupported alias")
    require((terminal is None and paused) or (terminal is not None and lines[-1] == terminal),
            "HELP routing does not contain its complete intended ending")
    return lines, paused


def help_routing_readback(current, topic, contract, minimum_rows, minimum_seconds, body_oracle=None):
    query, raw, received = ("HELP " + topic).rstrip(), bytearray(), 0
    terminal = contract[2] or harness.PAUSE_FOOTER_END
    try:
        current.send(query + "\r\n")
        while True:
            line = current.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME, "HELP routing exceeds its wire bound")
            if line == terminal.encode("ascii") + b"\r\n":
                break
        if contract[2] is not None and len(help_routing_lines(bytes(raw), query, current.reader.remote_echo)) >= minimum_rows:
            for _ in range(2):
                line = current.reader.line()
                received += len(line)
                raw.extend(line[:harness.MAX_FRAME - len(raw)])
                require(received <= harness.MAX_FRAME, "HELP routing footer exceeds its wire bound")
        body, paused = decode_help_routing(bytes(raw), query, contract, minimum_rows, minimum_seconds, current.reader.remote_echo)
        if body_oracle is not None:
            body_oracle(body)
    except Exception:
        received += len(current.reader.buffer)
        raw.extend(current.reader.buffer[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{current.call}] {query}\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[HELP routing receipt truncated at 65536 bytes]\n"
        current.suite.log.write(receipt)
        current.suite.log.flush()
    return complete_reply_config(current, received, "HELP routing"), paused


def complete_reply_config(current, received, label):
    # Called after a complete response receipt is logged/flushed, or with no
    # human response pending. The frame collector retains its own receipt.
    # Unlike Session.get, the barrier rejects any hidden trailing human output.
    identifier = current.suite.identifier()
    command = f"GET YAML CONFIG SCHEMA 7 ID {identifier}"
    current.send(command + "\r\n")
    document, prefix = current.frame("CONFIG", identifier, 7, command)
    echoed, trailing = False, 0
    for line in prefix.splitlines(keepends=True):
        require(line.endswith(b"\r\n") and b"\r" not in line[:-2], label + " barrier breaks CRLF")
        if current.reader.remote_echo and not echoed and line == command.encode("ascii") + b"\r\n":
            echoed = True
            continue
        require(line == b"\r\n", label + " hides trailing response content")
        trailing += len(line)
    require(received + trailing <= harness.MAX_FRAME, label + " final response exceeds its wire bound")
    require("error" not in document and type(document.get("configuration")) is dict and
            type(document.get("status")) is dict and type(document.get("revision")) is str and document["revision"],
            label + " barrier lacks a valid configuration")
    require(document.get("resource") == "CONFIG" and type(document.get("schema_version")) is int and
            document["schema_version"] == 7 and document.get("request_id") == identifier,
            label + " barrier has the wrong resource, schema or request identity")
    return document


def require_help_routing_state(before, after, record, saved, library, library_saved, paused):
    require_cc_unchanged(before, after, record, saved)
    require(library.read_bytes() == library_saved, "HELP routing changed the shared preset library")
    for field in ("callsign", "diagnostic_comments", "temporary_defaults", "suppressed_spots"):
        require_cc_typed_value(after["status"]["session"][field], before["status"]["session"][field])
    session = after["status"]["session"]
    require(session["pause_pending_delivery"] is False and type(session["pause_active"]) is bool and
            type(session["pause_remaining_seconds"]) is int and session["pause_remaining_seconds"] >= 0,
            "HELP routing leaves delivery pause pending or has invalid pause state")
    if paused:
        require(session["pause_active"] is True and type(session["pause_remaining_seconds"]) is int and
                session["pause_remaining_seconds"] > 0, "long HELP routing lacks its deliberate session pause")
    elif before["status"]["session"]["pause_active"] is False:
        require(session["pause_active"] is False, "short HELP routing unexpectedly changes the session pause")


def require_help_routing_saved(document, expected, record):
    require_cc_saved_state(document, expected, record)


def help_routing_cases(suite, root, generation, saved_help_routes):
    private = stored_document((root / "config" / "runtime.yaml").read_bytes())["telnet"]
    minimum_rows, minimum_seconds = private["auto_read_pause_min_rows"], private["auto_read_pause_seconds"]
    require(type(minimum_rows) is int and minimum_rows == 10 and type(minimum_seconds) is int and minimum_seconds > 0,
            "HELP routing requires the fixture's existing loaded automatic pause")
    taxonomy = stored_document((root / "config" / "spot_taxonomy.yaml").read_bytes())
    cw = next(mode for mode in taxonomy["modes"] if mode["name"] == "CW")
    shortcuts = [mode["name"] for mode in taxonomy["modes"] if mode.get("cc_shortcut") is True]
    require(len(shortcuts) == len(HELP_SHORTCUT_MODES) and set(shortcuts) == set(HELP_SHORTCUT_MODES) and
            all(alias in cw["variants"] for alias in ("CW_ALIAS", "COMMENT", "BAND", "MODE")) and
            any(mode["name"] == "TEST_MODE" and mode.get("filter_visible") is True and not mode.get("cc_shortcut")
                for mode in taxonomy["modes"]), "HELP routing lost its configured shortcut/alias prerequisites")
    library = root / "users" / "presets" / ("VE3ABC".encode().hex() + ".yaml")
    for dialect, call in (("go", "VE3ABC-73"), ("cc", "VE3ABC-74")):
        record = root / "users" / (call + ".yaml")
        if not generation:
            require(not record.exists(), "HELP routing requires fresh unused SSIDs")
        current = fixture_session(suite, call)
        before = current.get()
        if generation:
            require_help_routing_saved(before, saved_help_routes[call], record)
        else:
            expected = scoped_bank_fixture(before["configuration"], dialect, "ROUTE18")
            response = current.upload("PUT", "CONFIG", expected, revision=before["revision"])
            require("error" not in response, "HELP routing preference setup was refused")
            before = current.get()
            require_help_routing_saved(before, expected, record)
            name = "HELP18" + dialect.upper()
            exact_human_reply(current, "SAVE PRESET " + name, "Saved preset " + name + " (" + dialect.upper() + ") for VE3ABC.")
            before = current.get()
            require_help_routing_saved(before, expected, record)
            require(before["status"]["presets"][dialect] == dict(associated=True, name=name, modified=False),
                    "HELP routing setup lacks its saved preset baseline")
            saved_help_routes[call] = copy.deepcopy(expected)
        require(before["configuration"]["settings"]["dialect"] == dialect and
                before["status"]["server"]["auto_read_pause_min_rows"] == minimum_rows and
                before["status"]["server"]["auto_read_pause_seconds"] == minimum_seconds and
                before["status"]["presets"][dialect] == dict(associated=True, name="HELP18" + dialect.upper(), modified=False),
                "HELP routing fresh preferences or loaded pause owner disagree")
        before = require_setting_control(current, record, "SET DIAG MODE", "Diagnostic comments: MODE", "diagnostic_comments", "MODE")
        for index, (topic, key) in enumerate(help_routing_topics(dialect)):
            if index % 8 == 0:
                current, before = resume_help_fixture(current, record, before, diagnostic="MODE")
            saved, library_saved = record.read_bytes(), library.read_bytes()
            contract = help_route_contract(key, dialect) if key else (
                "Unknown help topic: " + topic, (), "Type HELP for available commands.", ())
            after, paused = help_routing_readback(current, topic, contract, minimum_rows, minimum_seconds)
            require_help_routing_state(before, after, record, saved, library, library_saved, paused)
            before = after


HELP_MAILING_CODES = tuple("AA AB AE AK AL AP AR AS AZ BC CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA "
                           "MA MB MD ME MI MN MO MP MS MT NB NC ND NE NH NJ NL NM NS NT NU NV NY OH OK ON "
                           "OR PA PE PR QC RI SC SD SK TN TX UM UT VA VI VT WA WI WV WY YT".split())


def filter_discovery_reply(verb, unknown=False, dialect="go"):
    require(verb in ("PASS", "REJECT"), "invalid discovery action oracle")
    if dialect == "cc":
        return native_retired_reply(verb)
    if unknown:
        message = "Use numbered rules: PASS RULE <0..9> <expression> or REJECT RULE <0..9> <expression>. Type HELP RULE for fields and examples."
    else:
        message = "Usage: PASS RULE <0..9> <expression>\n       REJECT RULE <0..9> <expression>\nType HELP RULE for fields and examples."
    return bounded_filter_refusal(message, "  ")


def require_discovery_inventory(reply, verb, unknown=False, dialect="go"):
    require(reply == filter_discovery_reply(verb, unknown, dialect), "discovery loses exact dialect-specific feedback")
    if dialect == "go":
        flat = " ".join(reply.split())
        require("PASS RULE <0..9> <expression>" in flat and "REJECT RULE <0..9> <expression>" in flat and
                "HELP RULE" in flat, "GO discovery omits numbered-rule recovery")
    else:
        require("HELP SET/FILTER" in reply and "DIALECT GO" in " ".join(reply.split()) and "HELP RULE" not in reply,
                "CC discovery loses bank-specific recovery")
    require(reply.endswith("No changes applied."), "discovery does not prove a refusal")


def decode_discovery_reply(raw, query, expected, remote_echo=False):
    lines = help_routing_lines(raw, query, remote_echo)
    require(len(lines) <= 9, "filter discovery exceeds the nine-row feedback bound")
    while lines and lines[0] == "":
        lines.pop(0)
    require(lines == expected.splitlines(), "filter discovery differs from its exact action/refusal feedback")
    return lines


def discovery_readback(current, query, expected):
    # These fixed replies stay below the loaded ten-row automatic-pause floor.
    # Receive and log every known row before sending the correlated barrier.
    wanted, raw, received, index = expected.splitlines(), bytearray(), 0, 0
    require(0 < len(wanted) <= 9, "invalid bounded discovery reply oracle")
    try:
        current.send(query + "\r\n")
        while index < len(wanted):
            line = current.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                    "filter discovery exceeds its bound or breaks CRLF")
            value = line[:-2].decode("ascii", "strict")
            if index == 0 and value in ("", query.upper()):
                continue
            require(value == wanted[index], "filter discovery differs from its exact action/refusal feedback")
            index += 1
        decode_discovery_reply(bytes(raw), query, expected, current.reader.remote_echo)
    except Exception:
        received += len(current.reader.buffer)
        raw.extend(current.reader.buffer[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{current.call}] {query}\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[filter discovery receipt truncated at 65536 bytes]\n"
        current.suite.log.write(receipt)
        current.suite.log.flush()
    return complete_reply_config(current, received, "filter discovery")


def discovery_feedback_cases(dialect):
    cases = []
    for verb in ("PASS", "REJECT"):
        cases.append((verb, filter_discovery_reply(verb, False, dialect), verb, False))
        cases.append((verb + " INVENTED", filter_discovery_reply(verb, True, dialect), verb, True))
        for family in ("BAND", "MODE", "SOURCE", "EVENT", "COMMENT", "DXCALL", "DECALL", "CONFIDENCE", "PATH",
                       "DXGRID2", "DEGRID2", "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE",
                       "DESTATE", "MINSNR", "NEARBY", "DXBM", "NOFILTER"):
            cases.append((verb + " " + family + " ALL", filter_discovery_reply(verb, True, dialect), verb, True))
    if dialect == "cc":
        missing = bounded_filter_refusal("Usage: SET/FILTER <DOC|DOS|DXCTY|DXSTATE|DXBM>/<PASS|REJECT> <list>, or <category>/OFF.")
        cases.append(("SET/FILTER", missing, None, False))
        for command in ("SET/FILTER COMMENT CQ", "UNSET/FILTER COMMENT CQ", "SET/FILTER NEARBY ON",
                        "SET/FILTER BAND 20", "UNSET/FILTER MODE CW", "SET/FILTER DXBM ALL"):
            cases.append((command, native_retired_reply(command), None, False))
    return cases


def require_discovery_help(lines, key, dialect):
    # Literal observable contracts, independent of catalog construction/wrapping.
    flat = " ".join(" ".join(lines).split())
    if key == "DX":
        required = ("Frequency is in kHz (e.g., 7001.0).",
                    "Use plain decimal numbers; exponent notation is not accepted.",
                    "Values in gaps between bands remain accepted.",
                    "Comment: ASCII letters, digits, spaces, and / # @ - . , * ? _.",
                    "Other punctuation and non-ASCII characters are rejected.",
                    "The whole command has a node-configured byte limit (default 128).",
                    "Mode/report may be parsed from the comment.",
                    "Rejects invalid callsigns and CTY-unknown DX when CTY is enabled.",
                    "Requires a valid logged-in callsign.",
                    "Logins ending TEST require a CTY-known baseline callsign.",
                    "When enabled, station reputation admission is shared across numeric SSIDs.",
                    "Posting admission may respond Spot rejected.",
                    "Spot queued confirms intake; filtering and processing determine delivery.",
                    "A missing or full input queue refuses the spot with an explicit response.")
        forbidden = ("free text", "guarantees delivery", "bypasses reputation", "SSID has its own allowance", "FCC license required to post")
    elif key == "OVERVIEW":
        show, readback = ("SHOW DX", "SHOW FILTER") if dialect == "go" else ("SHOW/DX", "SHOW/FILTER")
        required = ("Getting help", "HELP COMMANDS", "HELP <command>", "HELP SYMBOLS", "HELP TROUBLE",
                    "Showing archive spots", show + " 10", "HELP " + show,
                    "Managing filters", readback, "HELP FILTERS", "SHOW SETTINGS",
                    "Managing presets", "LIST PRESET", "SAVE PRESET DAY", "LOAD PRESET DAY", "HELP PRESETS")
        forbidden = ("GoCluster help -", "Call confidence", "Path reliability", "RESET MODE", "RESET FILTER RULE ALL", "HELP WEB")
    elif key in ("PASS", "REJECT"):
        required = (("Spot rules (GO only)", "Delivery switches (GO and CC)", "Old category mutation syntax is refused, not translated;")
                    if dialect == "go" else ("Spot rules (GO only)", "PC93 aliases ANNOUNCE.", "Numbered PASS and REJECT rules are for GO."))
        forbidden = ("MODE preserves visibility for every mode you do not name.", "MINSNR takes modes", "NEARBY takes ON|OFF", "RESET MODE")
    elif key in ("FILTERS", "FILTER EXAMPLES", "PRESETS"):
        harness.require_help_task(lines, key, dialect)
        required, forbidden = (), ("RESET MODE", "show FCC mailing-address codes")
    else:
        require(key in ("SHOW FILTER", "SHOW/FILTER"), "invalid HELP wording oracle")
        required = (("Shows canonical numbered rules and separate controls.", "GET YAML CONFIG SCHEMA 7") if dialect == "go" else
                    ("Shows exact native selections and six controls.", "Band INHERIT differs from explicit OFF", "GET YAML CONFIG SCHEMA 7"))
        forbidden = ("show FCC mailing-address codes",)
    for phrase in required:
        require(phrase in flat, "HELP omits intended current contract: " + phrase)
    for phrase in forbidden:
        require(phrase.lower() not in flat.lower(), "HELP advertises retired behavior: " + phrase)


def discovery_file_receipt(path):
    data, info = path.read_bytes(), path.stat()
    return data, (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def require_discovery_owners(before, after, record, user_receipt, library, library_receipt, paused=None):
    # Observed bytes/stat receipts complement the composed Go zero-save-call
    # controls; they do not expose an actual server commit-call counter.
    require_help_routing_state(before, after, record, user_receipt[0], library, library_receipt[0], bool(paused))
    require(discovery_file_receipt(record) == user_receipt and discovery_file_receipt(library) == library_receipt,
            "discovery changed durable file bytes, identity, size or modification time")
    require(after["status"].keys() == before["status"].keys() and
            after["status"]["session"].keys() == before["status"]["session"].keys(), "discovery changed runtime/session status shape")
    if paused is None:
        # Fixed feedback never enters the automatic HELP/readback pause path.
        require_cc_typed_value(after["status"]["session"], before["status"]["session"])


def require_discovery_saved(document, expected, record, library, name):
    require_help_routing_saved(document, expected, record)
    dialect = expected["settings"]["dialect"]
    require_cc_typed_value(document["status"]["presets"][dialect], dict(associated=True, name=name, modified=False))
    references = stored_document(record.read_bytes()).get("presets")
    require(type(references) is dict, "missing native preset references")
    reference = references.get(dialect)
    require(type(reference) is dict and reference.get("name") == name and type(reference.get("baseline")) is dict,
            "discovery lost its tagged preset association/baseline")
    baseline = reference["baseline"]
    require_cc_typed_value(baseline, stored_document(library.read_bytes())["presets"][name])
    wanted = tagged_preset(expected, dialect)
    # Optional false is the sole stored/wire representation distinction.
    actual, wanted = copy.deepcopy(baseline), copy.deepcopy(wanted)
    actual["settings"].setdefault("cw_speed", False)
    wanted["settings"].setdefault("cw_speed", False)
    require_cc_typed_value(actual, wanted)


def filter_discovery_cases(suite, root, generation, saved_discovery):
    private = stored_document((root / "config" / "runtime.yaml").read_bytes())["telnet"]
    minimum_rows, minimum_seconds = private["auto_read_pause_min_rows"], private["auto_read_pause_seconds"]
    require(type(minimum_rows) is int and minimum_rows == 10 and type(minimum_seconds) is int and minimum_seconds > 0,
            "discovery requires the unchanged fixture automatic-pause floor")
    library = root / "users" / "presets" / ("VE3ABC".encode().hex() + ".yaml")
    for dialect, call in (("go", "VE3ABC-75"), ("cc", "VE3ABC-76")):
        record, name = root / "users" / (call + ".yaml"), "DISC18" + dialect.upper()
        if not generation:
            require(not record.exists(), "discovery requires a fresh unused SSID")
        current = fixture_session(suite, call)
        before = current.get()
        if generation:
            expected = saved_discovery[call]
            require_discovery_saved(before, expected, record, library, name)
            require(before["status"]["session"]["diagnostic_comments"] == "OFF", "fresh discovery session retained diagnostics")
        else:
            expected = scoped_bank_fixture(before["configuration"], dialect, "DISCOVERY18")
            response = current.upload("PUT", "CONFIG", expected, revision=before["revision"])
            require("error" not in response, "discovery preference setup was refused")
            before = current.get()
            require_help_routing_saved(before, expected, record)
            exact_human_reply(current, "SAVE PRESET " + name, "Saved preset " + name + " (" + dialect.upper() + ") for VE3ABC.")
            before = current.get()
            require_discovery_saved(before, expected, record, library, name)
            saved_discovery[call] = copy.deepcopy(expected)
        require(before["configuration"]["settings"]["dialect"] == dialect and
                before["status"]["server"]["auto_read_pause_min_rows"] == minimum_rows and
                before["status"]["server"]["auto_read_pause_seconds"] == minimum_seconds and
                before["status"]["session"]["pause_active"] is False, "discovery lacks its saved dialect/loaded pause prerequisites")
        before = require_setting_control(current, record, "SET DIAG MODE", "Diagnostic comments: MODE", "diagnostic_comments", "MODE")
        for index, (query, feedback, verb, unknown) in enumerate(discovery_feedback_cases(dialect)):
            if index % 6 == 0:
                current, before = resume_help_fixture(current, record, before, diagnostic="MODE")
            if verb is not None:
                require_discovery_inventory(feedback, verb, unknown, dialect)
            user_receipt, library_receipt = discovery_file_receipt(record), discovery_file_receipt(library)
            after = discovery_readback(current, query, feedback)
            require_discovery_owners(before, after, record, user_receipt, library, library_receipt)
            before = after
        current, before = resume_help_fixture(current, record, before, diagnostic="MODE")
        filter_topic = "SHOW FILTER" if dialect == "go" else "SHOW/FILTER"
        for topic, key in (("DX", "DX"), ("", "OVERVIEW"), ("FILTERS", "FILTERS"),
                           ("FILTER EXAMPLES", "FILTER EXAMPLES"), ("PRESETS", "PRESETS"),
                           ("PASS", "PASS"), ("REJECT", "REJECT"), (filter_topic, filter_topic)):
            user_receipt, library_receipt = discovery_file_receipt(record), discovery_file_receipt(library)
            after, paused = help_routing_readback(current, topic, help_route_contract(key, dialect), minimum_rows, minimum_seconds,
                                                lambda lines: require_discovery_help(lines, key, dialect))
            require_discovery_owners(before, after, record, user_receipt, library, library_receipt, paused)
            before = after


HOLD_VALID_QUERIES = ("SHOW HOLD", "show hold", "  ShOw   HoLd  ", "SHOW    HOLD")
HOLD_REFUSALS = (("SHOW HOLD EXTRA", "Usage: SHOW HOLD"), ("show  hold  10", "Usage: SHOW HOLD"),
                 (" SHOW HOLD EXTRA TAIL ", "Usage: SHOW HOLD"), ("SHOW HOLD ALL", "Usage: SHOW HOLD"),
                 ("SH HOLD", "Unknown SHOW subcommand: HOLD"), ("SHOW HOLDS", "Unknown SHOW subcommand: HOLDS"),
                 ("SHOW HOLDX", "Unknown SHOW subcommand: HOLDX"), ("SHOW HOLD/EXTRA", "Unknown SHOW subcommand: HOLD/EXTRA"),
                 ("SHOW/HOLD", "Unknown command: SHOW/HOLD\nType HELP for available commands."),
                 ("SH/HOLD", "Unknown command: SH/HOLD\nType HELP for available commands."),
                 ("SHOWHOLD", "Unknown command: SHOWHOLD\nType HELP for available commands."))


def decode_hold_reply(raw, query, remote_echo=False):
    lines = help_routing_lines(raw, query, remote_echo)
    while lines and lines[0] == "":
        lines.pop(0)
    require(len(lines) == 1, "HOLD status has missing or extra reply rows")
    text = lines[0]
    if text == "Live spots are not paused.":
        return dict(pause_active=False, pause_pending_delivery=False, pause_remaining_seconds=0, suppressed_spots=0)
    match = re.fullmatch(r"Live spots paused for ([1-9][0-9]*)s more\. Suppressed spots: (0|[1-9][0-9]*)\.", text)
    if match:
        require(int(match[1]) <= 300, "HOLD status invents an excessive remaining interval")
        return dict(pause_active=True, pause_pending_delivery=False, pause_remaining_seconds=int(match[1]), suppressed_spots=int(match[2]))
    match = re.fullmatch(r"Live spots are not paused\. Suppressed spots in last pause: ([1-9][0-9]*)\.", text)
    if match:
        return dict(pause_active=False, pause_pending_delivery=False, pause_remaining_seconds=0, suppressed_spots=int(match[1]))
    match = re.fullmatch(r"Live spots paused while a response is being delivered\. Suppressed spots: (0|[1-9][0-9]*)\.", text)
    require(match is not None, "HOLD status differs from its complete literal grammar")
    return dict(pause_active=True, pause_pending_delivery=True, pause_remaining_seconds=0, suppressed_spots=int(match[1]))


def hold_readback(current, query, expected=None):
    started = time.monotonic()
    if expected is not None:
        document = discovery_readback(current, query, expected)
        return None, document, (started, time.monotonic())
    raw, received = bytearray(), 0
    try:
        current.send(query + "\r\n")
        while True:
            line = current.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                    "HOLD status exceeds its wire bound or breaks CRLF")
            value = line[:-2].decode("ascii", "strict")
            if value not in ("", query.upper()):
                break
        status = decode_hold_reply(bytes(raw), query, current.reader.remote_echo)
    except Exception:
        received += len(current.reader.buffer)
        raw.extend(current.reader.buffer[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{current.call}] {query}\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[HOLD receipt truncated at 65536 bytes]\n"
        current.suite.log.write(receipt)
        current.suite.log.flush()
    document = complete_reply_config(current, received, "HOLD status")
    return status, document, (started, time.monotonic())


def hold_snapshot(current):
    started = time.monotonic()
    document = complete_reply_config(current, 0, "HOLD snapshot")
    return document, (started, time.monotonic())


def hold_deadline_window(window, session, observation):
    # Remaining time is ceil(seconds), not an exposed absolute deadline. Each
    # read therefore bounds the deadline in a one-second quantization interval
    # widened only by measured send/receive time. Exact nanos/epoch are Go proof.
    started, finished = observation
    require(all(type(value) in (int, float) and math.isfinite(value) for value in observation) and
            0 <= started <= finished, "HOLD has invalid observation clock bounds")
    active, pending, seconds, suppressed = (session[field] for field in
                                           ("pause_active", "pause_pending_delivery", "pause_remaining_seconds", "suppressed_spots"))
    require(type(active) is bool and type(pending) is bool and type(seconds) is int and 0 <= seconds <= 300 and
            type(suppressed) is int and suppressed >= 0 and pending is False and (active is (seconds > 0)),
            "HOLD readback has invalid finite pause/counter types or state")
    if window is not None:
        require(len(window) == 2 and all(type(value) in (int, float) and math.isfinite(value) for value in window) and
                window[0] <= window[1], "HOLD has invalid existing deadline bounds")
    if not active:
        require(window is None or window[0] <= finished, "HOLD ended before its observed deadline interval")
        return window
    lower, upper = started + seconds - 1, finished + seconds
    if window is not None:
        lower, upper = max(lower, window[0]), min(upper, window[1])
    require(lower <= upper, "HOLD extended or shortened its observed deadline")
    return lower, upper


def require_hold_state(before, after, record, user_receipt, library, library_receipt, observation, window, rendered=None):
    require_cc_unchanged(before, after, record, user_receipt[0])
    require(discovery_file_receipt(record) == user_receipt and discovery_file_receipt(library) == library_receipt,
            "HOLD changed durable file bytes, identity, size or modification time")
    require(before["status"].keys() == after["status"].keys(), "HOLD changed status owners")
    for field in before["status"]:
        if field != "session":
            require_cc_typed_value(after["status"][field], before["status"][field])
    previous, current = before["status"]["session"], after["status"]["session"]
    require(previous.keys() == current.keys(), "HOLD changed session status shape")
    for field in previous:
        if field not in ("pause_active", "pause_remaining_seconds"):
            require_cc_typed_value(current[field], previous[field])
    require(type(current["pause_remaining_seconds"]) is int and
            current["pause_remaining_seconds"] <= previous["pause_remaining_seconds"], "HOLD reset or increased its remaining interval")
    if previous["pause_active"] is False:
        require(current["pause_active"] is False and current["pause_remaining_seconds"] == 0, "inactive HOLD created a pause")
    window = hold_deadline_window(window, current, observation)
    if rendered is not None:
        require(rendered["suppressed_spots"] == current["suppressed_spots"] and rendered["pause_pending_delivery"] is False,
                "HOLD human status misstates the established suppression count or pending state")
        require(current["pause_remaining_seconds"] <= rendered["pause_remaining_seconds"] <= previous["pause_remaining_seconds"],
                "HOLD human status misstates the observed countdown")
        window = hold_deadline_window(window, rendered, observation)
    return window


def require_hold_spot(row, label, frequency, live=False):
    require_history_row(row)
    comment = "CW CW|CMT" if live else "CW " + label
    require(re.fullmatch(r"DX de K1ABC:\s+" + re.escape(str(frequency)) + r"\.00", row[:25]) and
            row[27:39].rstrip() == "W6ABC" and row[39:63].rstrip() == comment,
            "HOLD stimulus has the wrong admission identity, frequency or exact comment")


def hold_live_readback(current, frequency):
    # No command is pending. The first nonblank live row must be the fresh post,
    # so a replayed paused row cannot be hidden in a later machine frame prefix.
    current.reader.deadline = min(current.suite.deadline, time.monotonic() + 5)
    raw, received = bytearray(), 0
    try:
        while True:
            line = current.reader.line()
            received += len(line)
            raw.extend(line[:harness.MAX_FRAME - len(raw)])
            require(received <= harness.MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                    "HOLD fresh delivery exceeds its wire bound or breaks CRLF")
            value = line[:-2].decode("ascii", "strict")
            if value:
                require_hold_spot(value, "", frequency, True)
                break
    except Exception:
        received += len(current.reader.buffer)
        raw.extend(current.reader.buffer[:harness.MAX_FRAME - len(raw)])
        raise
    finally:
        receipt = f"\n[{current.call}] HOLD fresh delivery\n".encode() + bytes(raw)
        if received > len(raw):
            receipt += b"\n[HOLD live receipt truncated at 65536 bytes]\n"
        current.suite.log.write(receipt)
        current.suite.log.flush()
    return complete_reply_config(current, received, "HOLD fresh delivery")


def require_dx_input_archive(rows, label, frequency):
    require(len(rows) == 1 and label in rows[0] and
            re.search(r"\b" + re.escape(str(frequency)) + r"\.00\s+W6ABC\s+CW\b", rows[0]),
            "DX input control lacks its exact archive identity")


def dx_input_contract_cases(suite, root):
    limit = stored_document((root / "config/runtime.yaml").read_bytes())["telnet"]["command_line_limit"]
    require(type(limit) is int and limit == 128, "DX input fixture lacks its loaded 128-byte command bound")
    for index, dialect in enumerate(("go", "cc")):
        current = fixture_session(suite, "VE3ABC-" + str(79 + index))
        current.human("DIALECT " + dialect)
        current.human("RESET FILTER" if dialect == "go" else "SET/NOFILTER")
        current.human("PAUSE 300", harness.pause_footer(300))
        before = current.get()
        require(before["configuration"]["settings"]["dialect"] == dialect and
                before["status"]["session"]["pause_active"] is True,
                "DX input fixture lost its selected dialect")
        record = root / "users" / (current.call + ".yaml")
        saved = record.read_bytes()
        base = 14321 + index * 10
        accepted = literal_archive_label(suite, dialect.upper() + "D")
        discovery_readback(current, f"DX {base} W6ABC CW {accepted} /#@-.,*?_", "Spot queued.")

        def archived(label, frequency):
            deadline = min(suite.deadline, time.monotonic() + 5)
            while True:
                rows = archive_rows(current, label)
                if rows:
                    require_dx_input_archive(rows, label, frequency)
                    return
                require(time.monotonic() < deadline, "accepted DX input absent from archive")
                time.sleep(.025)

        archived(accepted, base)
        refused = []
        for offset, suffix in enumerate((":POTA", "!CQ", "LONG"), 1):
            label = literal_archive_label(suite, dialect.upper() + str(offset))
            refused.append(label)
            query = f"DX {base + offset} W6ABC CW {label} "
            query += ("X" * (limit + 1 - len(query))) if suffix == "LONG" else suffix
            identifier = suite.identifier()
            sync = f"GET YAML CONFIG SCHEMA 7 ID {identifier}"
            current.send(query + "\r\nSHOW OWN\r\n" + sync + "\r\n")
            after, prefix = current.frame("CONFIG", identifier, 7, "DX input refusal and following command")
            diagnostic = b"Command input is too long" if suffix == "LONG" else b"Command input may only contain"
            require(diagnostic in prefix and prefix.count(b"Own call: VE3ABC") == 1 and b"Spot queued" not in prefix,
                    "DX refusal missing, dispatched a spot, or lost the following valid command")
            require(after["configuration"] == before["configuration"] and after["revision"] == before["revision"] and
                    record.read_bytes() == saved, "DX input changed preferences or saved state")
        barrier = literal_archive_label(suite, dialect.upper() + "B")
        discovery_readback(current, f"DX W6ABC {base + 4} CW {barrier}", "Spot queued.")
        archived(barrier, base + 4)  # FIFO fixture archive witness after every refused command.
        for label in refused:
            require(not archive_rows(current, label), "refused DX input reached the archive")


def hold_command_cases(suite, root, generation, saved_hold, poster):
    private = stored_document((root / "config" / "runtime.yaml").read_bytes())["telnet"]
    require(type(private["auto_read_pause_min_rows"]) is int and private["auto_read_pause_min_rows"] == 10,
            "HOLD checkout keeps the loaded threshold ten; threshold-one bypass proof belongs to Go sessions")
    library = root / "users" / "presets" / ("VE3ABC".encode().hex() + ".yaml")
    for index, (dialect, call) in enumerate((("go", "VE3ABC-77"), ("cc", "VE3ABC-78"))):
        record, name = root / "users" / (call + ".yaml"), "HOLD19" + dialect.upper()
        if not generation:
            require(not record.exists(), "HOLD requires a fresh unused SSID")
        current = fixture_session(suite, call)
        before, observation = hold_snapshot(current)
        if generation:
            expected = saved_hold[call]
            require_discovery_saved(before, expected, record, library, name)
        else:
            expected = scoped_bank_fixture(before["configuration"], dialect, "HOLD19")
            response = current.upload("PUT", "CONFIG", expected, revision=before["revision"])
            require("error" not in response, "HOLD preference/consumer-predicate setup was refused")
            before, observation = hold_snapshot(current)
            require_help_routing_saved(before, expected, record)
            discovery_readback(current, "SAVE PRESET " + name, "Saved preset " + name + " (" + dialect.upper() + ") for VE3ABC.")
            before, observation = hold_snapshot(current)
            require_discovery_saved(before, expected, record, library, name)
            saved_hold[call] = copy.deepcopy(expected)
        require(before["status"]["server"]["auto_read_pause_min_rows"] == 10 and
                before["status"]["session"]["pause_active"] is False and
                before["status"]["session"]["suppressed_spots"] == 0 and
                before["status"]["session"]["diagnostic_comments"] == "OFF", "HOLD fresh session has stale runtime owners")
        require_setting_control(current, record, "SET DIAG MODE", "Diagnostic comments: MODE", "diagnostic_comments", "MODE")
        before, observation = hold_snapshot(current)
        user_receipt, library_receipt = discovery_file_receipt(record), discovery_file_receipt(library)

        def check(query, feedback=None, window=None):
            nonlocal before
            rendered, after, observed = hold_readback(current, query, feedback)
            window = require_hold_state(before, after, record, user_receipt, library, library_receipt, observed, window, rendered)
            before = after
            return window

        for query in HOLD_VALID_QUERIES:
            check(query)
        for query, feedback in HOLD_REFUSALS:
            check(query, feedback)
        _, paused, observation = hold_readback(current, "PAUSE 15",
                                              harness.pause_footer(15))
        require_cc_unchanged(before, paused, record, user_receipt[0])
        require(paused["status"]["session"]["pause_active"] is True and paused["status"]["session"]["suppressed_spots"] == 0,
                "HOLD suppression requires an active finite pause with no invented counter")
        window = hold_deadline_window(None, paused["status"]["session"], observation)
        frequency = 14177 + generation * 4 + index * 2
        label = "HOLD19 " + literal_archive_label(suite, dialect.upper() + "S")
        discovery_readback(poster, f"DX {frequency} W6ABC CW {label}", "Spot queued.")
        deadline = min(suite.deadline, time.monotonic() + 4)
        while True:
            rows = archive_rows(current, label)
            actual, observation = hold_snapshot(current)
            count = actual["status"]["session"]["suppressed_spots"]
            require(type(count) is int and count in (0, 1), "HOLD suppression counted an uncontrolled or duplicate spot")
            window = hold_deadline_window(window, actual["status"]["session"], observation)
            if rows and count == 1:
                require(len(rows) == 1, "HOLD suppression archive stimulus was duplicated")
                require_hold_spot(rows[0], label, frequency)
                before = actual
                break
            require(time.monotonic() < deadline, "HOLD admitted archive/consumer suppression predicate was not established")
            time.sleep(.025)  # Predicate polling only; time does not establish success.
        initial_seconds = before["status"]["session"]["pause_remaining_seconds"]
        deadline = min(suite.deadline, time.monotonic() + 3)
        while before["status"]["session"]["pause_remaining_seconds"] == initial_seconds:
            require(time.monotonic() < deadline, "HOLD did not observe a real countdown before its preservation checks")
            window = check("SHOW HOLD", window=window)
            time.sleep(.025)
        require(before["status"]["session"]["pause_active"] is True, "HOLD positive countdown expired before active controls")
        for query in HOLD_VALID_QUERIES:
            window = check(query, window=window)
        for query, feedback in HOLD_REFUSALS:
            window = check(query, feedback, window)
        _, resumed, observation = hold_readback(current, "RESUME", "Live spots resumed. Suppressed spots: 1.")
        require_cc_unchanged(before, resumed, record, user_receipt[0])
        require(resumed["status"]["session"]["pause_active"] is False and
                resumed["status"]["session"]["pause_pending_delivery"] is False and
                resumed["status"]["session"]["pause_remaining_seconds"] == 0 and
                resumed["status"]["session"]["suppressed_spots"] == 0, "HOLD RESUME did not retire the pause/counter")
        before = resumed
        check("show  hold")
        fresh_label = "HOLD19 " + literal_archive_label(suite, dialect.upper() + "F")
        discovery_readback(poster, f"DX {frequency + 1} W6ABC CW {fresh_label}", "Spot queued.")
        after = hold_live_readback(current, frequency + 1)
        require_discovery_owners(before, after, record, user_receipt, library, library_receipt)
        rows = archive_rows(current, fresh_label)
        deadline = min(suite.deadline, time.monotonic() + 4)
        while not rows:
            require(time.monotonic() < deadline, "HOLD fresh delivered stimulus lacks its archive identity")
            time.sleep(.025)
            rows = archive_rows(current, fresh_label)
        require(len(rows) == 1, "HOLD fresh archive stimulus was duplicated")
        require_hold_spot(rows[0], fresh_label, frequency + 1)
        final, _ = hold_snapshot(current)
        require_discovery_saved(final, expected, record, library, name)


def require_taxonomy_rules(document, expected, record):
    require(active_fixture_bank(document) == expected,
            "taxonomy selection lost exact canonical rules or native modes")
    require_cc_typed_value(active_fixture_bank(document), expected)
    require_fixture_persisted(document, record)


def require_taxonomy_archive(rows, label, frequency, visible):
    if not visible:
        require(not rows, "blocked taxonomy control remained visible")
        return
    require(len(rows) == 1 and rows[0][39:63].rstrip() == "TEST_MODE " + label and
            re.search(r"^DX de N0XYZ:\s+" + str(frequency) + r"\.00\s+W6ABC\s+", rows[0]),
            "taxonomy control is absent, stale or has the wrong identity, frequency or mode")
    require_history_row(rows[0])


def taxonomy_cases(suite, root, nonce, generation, saved_taxonomy, cw_label):
    labels = ["T15" + nonce[:8].upper() + str(i) for i in range(2)]
    go_expression = "MODE=TEST_MODE,CW AND (EVENT=TEST_EVENT OR MODE=CW)"
    for dialect, call in (("go", "VE3ABC-67"), ("cc", "VE3ABC-68")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        choices = current.get("CAPABILITIES")["configuration"]["go_legacy_schema6_capabilities"]["vocabulary"]
        require("TEST_MODE" in choices["modes"] and "TEST_EVENT" in choices["events"],
                "loaded taxonomy does not advertise literal underscore names")
        record = root / "users" / (call + ".yaml")
        if generation == 0:
            current.human("DIALECT " + dialect)
            if dialect == "go":
                fixture_edit(current, "RESET FILTER RULE ALL", record)
                fixture_edit(current, "PASS RULE 0 MODE=test_mode,cw_alias AND (EVENT=test_event OR MODE=cw)", record)
            else:
                fixture_edit(current, "SET/NOFILTER", record)
                result = current.upload("PATCH", "FILTER", dict(dialect="cc", filters=dict(modes=["test_mode", "cw_alias"])))
                require("error" not in result and result["result"]["persisted"] is True,
                        "native YAML taxonomy mode list was refused")
            current.human("SAVE PRESET TAXONOMY15", "Saved preset TAXONOMY15")
        before, saved = current.get(), record.read_bytes()
        expected = copy.deepcopy(active_fixture_bank(before))
        require(expected["rules"] == [dict(slot=0, action="pass", expr=go_expression)] if dialect == "go" else
                expected["modes"] == ["CW", "TEST_MODE"], "taxonomy canonical names differ from literal expectations")
        require_taxonomy_rules(before, expected, record)
        if generation:
            require((before["configuration"], before["status"]["presets"]) == saved_taxonomy[call],
                    "fresh child lost underscore rules or preset association")
        if dialect == "go":
            for domain, known, unknown in (("MODE", "TEST_MODE", "MISSING_MODE"), ("EVENT", "TEST_EVENT", "MISSING_EVENT")):
                for action in ("PASS", "REJECT"):
                    require_fixture_refusal(current, action + " RULE 9 " + domain + "=" + known + "," + unknown,
                                            record, "Invalid rule:")
            text, after = current.human("SHOW FILTER RULE")
            require(go_expression in text, "GO inspection omitted its complete underscore expression")
        else:
            for command in ("PASS RULE 0 EVENT=test_event", "SET/FILTER EVENT test_event",
                            "SET/TEST_MODE", "SET/MISSING_MODE"):
                require_fixture_refusal(current, command, record)
            result = current.upload("PATCH", "FILTER", dict(dialect="cc", filters=dict(modes=["TEST_MODE", "MISSING_MODE"])))
            require("error" in result, "native YAML accepted an unknown taxonomy mode")
            after = current.get()
            require_fixture_unchanged(before, after, record, saved)
            text, after = current.human("SHOW/FILTER MODE")
            require("TEST_MODE" in text and "CW" in text, "CC inspection omitted its explicit taxonomy modes")
        require_fixture_unchanged(before, after, record, saved)
        current = reconnect_fixture_session(current, record)
        require_mode_archive(current, cw_label, "CW", True)
        cw_rows = archive_rows(current, cw_label)
        require(archive_rows(current, cw_label, "MODE CW_ALIAS") == cw_rows and len(cw_rows) == 1,
                "configured underscore alias cannot select its canonical history mode")
        for i in range(2):
            require_taxonomy_archive(archive_rows(current, labels[i]), labels[i], 14061 + i,
                                     i == 0 or dialect == "cc")
        if dialect == "go":
            fixture_edit(current, "PASS RULE 0 MODE=TEST_MODE,CW", record)
            require_taxonomy_archive(archive_rows(current, labels[1]), labels[1], 14062, True)
            fixture_edit(current, "PASS RULE 0 " + go_expression, record)
            fixture_edit(current, "REJECT RULE 1 MODE=TEST_MODE", record)
            require_taxonomy_archive(archive_rows(current, labels[0]), labels[0], 14061, False)
            fixture_edit(current, "RESET FILTER RULE 1", record)
        else:
            result = current.upload("PATCH", "FILTER", dict(dialect="cc", filters=dict(modes=["CW"])))
            require("error" not in result and result["result"]["persisted"] is True, "native mode removal failed")
            require_taxonomy_archive(archive_rows(current, labels[0]), labels[0], 14061, False)
            result = current.upload("PATCH", "FILTER", dict(dialect="cc", filters=dict(modes=["CW", "TEST_MODE"])))
            require("error" not in result and result["result"]["persisted"] is True, "native mode restoration failed")
        require_taxonomy_archive(archive_rows(current, labels[0]), labels[0], 14061, True)
        final = current.get()
        require_taxonomy_rules(final, expected, record)
        final_saved = record.read_bytes()
        result = current.upload("VALIDATE", "CONFIG", final["configuration"])
        require("error" not in result and result["result"]["valid"] is True and result["result"]["applied"] is False and
                result["result"]["persisted"] is False, "native underscore configuration is machine-invalid")
        require_fixture_unchanged(final, current.get(), record, final_saved)
        saved_taxonomy[call] = final["configuration"], final["status"]["presets"]


def require_solar_value(document, value, available):
    configured = document["configuration"]["settings"].get("solar_summary_minutes", 0)
    effective = document["status"]["effective"]["solar_summary_minutes"]
    require(type(configured) is int and configured == value and type(effective) is int and
            effective == (value if available else 0), "SOLAR configured/effective cadence mismatch")


def require_solar_readback(before, after, record, saved, available, text=None):
    require_solar_value(before, 30, available)
    require_solar_value(after, 30, available)
    require(after["configuration"] == before["configuration"] and after["revision"] == before["revision"] and
            after["status"]["effective"] == before["status"]["effective"] and
            after["status"]["presets"] == before["status"]["presets"] and record.read_bytes() == saved,
            "SOLAR read changed a preference owner")
    if text is not None:
        wording = "Every 30 minutes when conditions are active" if available else "30 minutes; unavailable on this server"
        rows = [line for line in text.splitlines() if line.startswith("Solar         ")]
        require(rows == ["Solar         " + wording], "SOLAR readback misrepresents conditional availability")


def solar_cases(suite, root, available, generation, saved_solar):
    # This manager has loaded availability and unobserved feeds. Fresh calm and
    # active generation/dispatch are qualified by permanent owner/queue tests.
    for dialect, call in (("go", "VE3ABC-65"), ("cc", "VE3ABC-66")):
        current = fixture_session(suite, call)
        current.human("PAUSE 300")
        record = root / "users" / (call + ".yaml")
        if generation == 0:
            current.human("DIALECT " + dialect)
            feedback = "Solar summaries: every 30 minutes when conditions are active" if available else \
                       "Solar cadence: 30 minutes; unavailable on this server"
            require_setting_control(current, record, "SET SOLAR 30", feedback, "solar_summary_minutes", 30)
            current.human("SAVE PRESET SOLAR14", "Saved preset SOLAR14")
            before, saved = current.get(), record.read_bytes()
            with deny_record_replacement(record):
                warning = "Solar summaries: every 15 minutes when conditions are active" if available else \
                          "Solar cadence: 15 minutes; unavailable on this server"
                text, failed = current.human("SET SOLAR 15", warning)
                require_solar_value(failed, 15, available)
                require_setting_save_warning(text)
                require(record.read_bytes() == saved,
                        "SOLAR commit failure hid failure or replaced saved cadence")
            require_setting_control(current, record, "SET SOLAR 30", feedback, "solar_summary_minutes", 30)
            before, saved = current.get(), record.read_bytes()
            candidate = copy.deepcopy(before["configuration"])
            candidate["settings"]["solar_summary_minutes"] = 60
            response = current.upload("VALIDATE", "CONFIG", candidate)
            require("error" not in response and response["result"]["valid"] is True and
                    response["result"]["applied"] is False and response["result"]["persisted"] is False,
                    "SOLAR preference validation changed availability admission")
            require_solar_readback(before, current.get(), record, saved, available)
            response = current.upload("PUT", "CONFIG", before["configuration"], revision=before["revision"])
            require("error" not in response, "unchanged SOLAR machine publication failed")
            current.human("LOAD PRESET SOLAR14", "Loaded preset SOLAR14")
        before, saved = current.get(), record.read_bytes()
        require_solar_value(before, 30, available)
        if generation:
            require((before["configuration"], before["status"]["presets"]) == saved_solar[call],
                    "fresh child lost saved SOLAR cadence or preset association")
        require_solar_value(current.get("CONFIG", 7), 30, available)
        legacy = current.get("SETTINGS", 6)
        require_solar_value(dict(configuration={"settings": legacy["configuration"]}, status=legacy["status"]),
                            30, available)
        for _ in range(2):
            text, after = current.human("SHOW SETTINGS")
            require_solar_readback(before, after, record, saved, available, text)
        saved_solar[call] = before["configuration"], before["status"]["presets"]


def dedupe_profile(binary, root, source, transport):
    """Real marked-record login and read-only fallback on a loaded FAST-only node."""
    prefix = transport + "/dedupe"
    required = [f"{prefix}/0: held endpoint exposes loaded FAST-only availability",
                f"{prefix}/0: saved SLOW readbacks preserve every preference owner",
                f"{prefix}: applied SET fallback and machine refusal remain compatible",
                f"{prefix}/0: disabled SOLAR keeps saved cadence and reports effective zero",
                f"{prefix}/1: held endpoint exposes loaded FAST-only availability",
                f"{prefix}/1: saved SLOW survives fresh child without read effects",
                f"{prefix}/1: disabled SOLAR cadence survives fresh child"]
    available = dict(FAST=True, MED=False, SLOW=False)
    calls = (("go", "K1ABC-58"), ("cc", "K1ABC-59"))
    users = root / "users"
    users.mkdir()
    saved_configuration = {}
    saved_solar = {}
    results, wire, cleanup, children = [], [], [], []
    for generation in range(2):
        child, suite = None, None
        try:
            child = Child(binary, root, source, dedupe_available=available, solar_enabled=False)
            children.append(child.identity)
            suite = CommandSuite(SimpleNamespace(host=child.host, port=child.port, call="K1ABC",
                                                  timeout=8, run_seconds=120, output=root / f"wire-{generation}"))
            if generation == 0:
                seed = fixture_session(suite, "VE3ABC-62")
                seed.human("PAUSE 300")
                seed_owner = stored_document((users / "VE3ABC-62.yaml").read_bytes())
                seed.close()
                for dialect, call in calls:
                    record = users / (call + ".yaml")
                    require(not record.exists(), "dedupe native seed requires an unused owner")
                    owner = copy.deepcopy(seed_owner)
                    cfg = owner["configuration"]
                    cfg["settings"].update(dialect=dialect, dedupe_policy="SLOW")
                    cfg["filters"]["go"]["rules"] = [dict(slot=0, action="pass", expr="BAND=20M")]
                    cfg["filters"]["cc"]["modes"] = ["CW"]
                    owner["presets"] = {dialect: dict(name="DEDUPE13", baseline=tagged_preset(cfg))}
                    record.write_bytes(native_storage_bytes(owner))

            def identity():
                current = fixture_session(suite, "VE3ABC-62")
                current.human("PAUSE 300")
                current.human("SHOW BUILD", "Build version: fixture-" + child.nonce)
                require(current.get("CAPABILITIES")["schema_version"] == 7 and
                        child.identity["dedupe_available"] == available,
                        "actual server does not expose the literal FAST-only profile")

            suite.case(required[0 if generation == 0 else 4], identity)
            child.check_running()

            def reads():
                for dialect, call in calls:
                    current = fixture_session(suite, call)
                    _, before = current.human("PAUSE 300")
                    record = users / (call + ".yaml")
                    saved = record.read_bytes()
                    require(before["configuration"]["settings"]["dialect"] == dialect and
                            before["status"]["session"]["temporary_defaults"] is False and
                            before["status"]["preset"] == dict(associated=True, name="DEDUPE13", modified=False),
                            "literal marked record did not restore normally")
                    if generation:
                        require(before["configuration"] == saved_configuration[call], "fresh child lost configured SLOW")
                    for _ in range(2):
                        _, after = exact_human_reply(current, "SHOW DEDUPE", "Dedupe: SLOW; effective FAST (grid2)\n"
                                                    "        (fast=on med=off slow=off)\n"
                                                    "        Configured choice is unavailable on this server")
                        require_dedupe_readback(before, after, record, saved)
                        text, after = current.human("SHOW SETTINGS")
                        require_dedupe_readback(before, after, record, saved, text)
                    saved_configuration[call] = before["configuration"]

            suite.case(required[1 if generation == 0 else 5], reads)
            child.check_running()
            if generation == 0:
                def positive():
                    for dialect, call in (("go", "VE3ABC-60"), ("cc", "VE3ABC-61")):
                        current = fixture_session(suite, call)
                        current.human("PAUSE 300")
                        current.human("DIALECT " + dialect)
                        record = users / (call + ".yaml")
                        require_setting_control(current, record, "SET DEDUPE SLOW",
                                                "Dedupe policy set to FAST (requested SLOW disabled)", "dedupe_policy", "FAST")
                        before, saved = current.get(), record.read_bytes()
                        candidate = copy.deepcopy(before["configuration"])
                        candidate["settings"]["dedupe_policy"] = "SLOW"
                        response = current.upload("VALIDATE", "CONFIG", candidate)
                        require("error" in response and "dedupe_policy is unavailable on this server" in response["error"]["message"],
                                "unavailable machine selection was admitted")
                        after = current.get()
                        require(after["configuration"] == before["configuration"] and after["revision"] == before["revision"] and
                                after["status"]["effective"] == before["status"]["effective"] and record.read_bytes() == saved,
                                "machine refusal changed applied human fallback")

                suite.case(required[2], positive)
                child.check_running()
            suite.case(required[3 if generation == 0 else 6],
                       lambda: solar_cases(suite, root, False, generation, saved_solar))
            child.check_running()
        finally:
            try:
                if suite:
                    for current in suite.sessions:
                        try:
                            current.close()
                        except Exception as error:
                            suite.cleanup_errors.append(str(error))
                    suite.log.close()
                    results.extend(suite.results)
                    wire.extend(suite.wire_failures)
                    cleanup.extend(suite.cleanup_errors)
            finally:
                if child:
                    try:
                        child.stop()
                    except Exception as error:
                        cleanup.append(str(error))
    report = {"transport": transport, "profile": "dedupe", "required": required, "results": results,
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
    calls = ("K1ABC-81", "K1ABC-82")
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
            suite = CommandSuite(SimpleNamespace(host=child.host, port=child.port, call="K1ABC",
                                                  timeout=8, run_seconds=120,
                                                  output=root / f"wire-{generation}"))
            sessions = [fixture_session(suite, call) for call in calls]
            observer = fixture_session(suite, "VE3ABC-83")

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
                        except Exception as error:
                            suite.cleanup_errors.append(str(error))
                    suite.log.close()
                    results.extend(suite.results)
                    wire.extend(suite.wire_failures)
                    cleanup.extend(suite.cleanup_errors)
            finally:
                if child:
                    try:
                        child.stop()
                    except Exception as error:
                        cleanup.append(str(error))
    report = {"transport": transport, "profile": "reputation", "required": required,
              "results": results, "wire_failures": wire, "cleanup_errors": cleanup,
              "children": children, "runtime_counts": "reset on fresh child; not persisted"}
    (root / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    qualified(results, required, wire, cleanup)
    return report


def main():
    validate_fixture_calls(FIXTURE_LOGINS)
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
                for suffix, operation, options in (
                        ("", profile, dict(underscore_taxonomy=True)),
                        ("-reputation", reputation_profile, dict(reputation_enabled=True)),
                        ("-dedupe", dedupe_profile, dict(dedupe_fast_only=True, solar_enabled=False))):
                    root = run / (transport + suffix)
                    try:
                        stage(root, transport, **options)
                        row = operation(binary, root, source, transport)
                        row["qualification"] = "PASS"
                    except Exception as error:
                        # Keep independent profiles visible after a failure.
                        # Missing or interrupted evidence remains a failure.
                        receipt = root / "results.json"
                        row = json.loads(receipt.read_text()) if receipt.is_file() else {}
                        row.update(qualification="FAIL", error=f"{type(error).__name__}: {error}")
                    row["profile_name"] = transport + suffix
                    report["profiles"].append(row)
            require(len(report["profiles"]) == 6 and
                    all(row["qualification"] == "PASS" for row in report["profiles"]),
                    "one or more required integration profiles failed")
        report["result"] = "PASS"
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        (run / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"result": report["result"], "output": str(run), "error": report.get("error")}, indent=2))
    return int(report["result"] != "PASS")


if __name__ == "__main__":
    raise SystemExit(main())
