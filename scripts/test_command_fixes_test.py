"""Offline failure controls for the checkout runner; these do not qualify telnet."""

import ast
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from unittest import mock

import command_fixture_support as support
import command_fixture_process as process_support

spec = importlib.util.spec_from_file_location("command_fix_driver", Path(__file__).with_name("test-command-fixes.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)

# Literal 78-column archive row; expected layout is separate from the checker.
HISTORY_ROW = "DX de K1ABC:     14074.00  W6ABC       CW LABEL                          2152Z"


def cc_help_literal(verb):
    # Independent grammar fixture; wording and wrapping do not copy the Go catalog.
    clear = "ALL" if verb == "SET/FILTER" else "NONE"
    lines = [verb + " - Contract fixture (CC dialect).",
             "Usage: " + verb + " <list-type> <values>",
             "       " + verb + " MINSNR <modes|ALL> <integer dB|" + clear + ">",
             "       " + verb + " NEARBY ON|OFF", "       " + verb + " <switch> [ALL]",
             "       " + verb + " DXBM <band-codes>"]
    if verb == "SET/FILTER":
        lines += ["       SET/FILTER NOFILTER [ALL]", "       SET/FILTER DXBM PASS|REJECT <band-codes>",
                  "       SET/FILTER DXBM/PASS <band-codes>", "       SET/FILTER DXBM/REJECT <band-codes>"]
    lines += ["Notes:"]
    notes = ["SOURCE takes one of HUMAN, SKIMMER or ALL.",
             "MINSNR numeric thresholds use the same inclusive minimum with either verb.",
             "NEARBY uses the explicit ON/OFF choice with either verb.",
             "Switches allow no value or ALL.", "COMMENT uses PASS/REJECT COMMENT <phrase>.",
             "SET/FILTER COMMENT and UNSET/FILTER COMMENT remain unsupported.",
             "UNSET/FILTER does not accept /ON, /OFF or NOFILTER.",
             "DXBM requires numeric band codes, not ALL."]
    for note in notes:
        lines.extend(textwrap.wrap(note, width=78, initial_indent="  ", subsequent_indent="  "))
    lines += ["CC list types:", "  BAND, MODE, SOURCE, EVENT, CONFIDENCE, PATH, DXCONT, DECONT, DXZONE, DEZONE",
              "  DXGRID2, DEGRID2, DXDXCC, DEDXCC, DXSTATE, DESTATE, DXCALL, DECALL",
              "Switches:", "  BEACON, WWV, WCY, ANNOUNCE, SELF, TOXIC"]
    if verb == "SET/FILTER":
        lines += ["Notes:", "  SET/FILTER <list-type>/ON or /OFF changes an ordinary list.",
                  "  SET/FILTER <switch>/ON or /OFF changes a feature switch.",
                  "  SET/FILTER NOFILTER/ON clears restrictions; NOFILTER/OFF is unsupported.",
                  "  No inline flags for COMMENT, MINSNR, NEARBY or DXBM.", "  Flags allow no extra arguments."]
    return lines


def cc_help_wire(lines, query="HELP SET/FILTER", seconds=30, count=None):
    footer = f"Live spots paused for {seconds}s after {len(lines) if count is None else count} output rows. Type RESUME to resume now."
    return (query + "\r\n" + "\r\n".join(lines + [footer, "Missed spots are not replayed."]) + "\r\n").encode("ascii")


def routing_help_wire(lines, query="HELP SH BUILD EXTRA", pause=False, echo=True, seconds=30, count=None):
    body = list(lines)
    if pause:
        body += [f"Live spots paused for {seconds}s after {len(lines) if count is None else count} output rows. Type RESUME to resume now.",
                 "Missed spots are not replayed."]
    return ((query + "\r\n" if echo else "") + "\r\n".join(body) + "\r\n").encode("ascii")


def discovery_literal_reply(verb, unknown=False):
    types = ("Types: BAND, MODE, MINSNR, SOURCE, EVENT, COMMENT, DXCALL, DECALL,\n"
             "       CONFIDENCE, PATH, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE,\n"
             "       DXDXCC, DEDXCC, DXSTATE, DESTATE, DXBM, NEARBY,\n"
             "       BEACON, WWV, WCY, ANNOUNCE, SELF, TOXIC\n")
    if unknown:
        extra = "PASS also supports NOFILTER; PC93 aliases ANNOUNCE.\n" if verb == "PASS" else "PC93 aliases ANNOUNCE.\n"
        return "Unknown filter type.\n" + types + "COMMENT uses PASS/REJECT COMMENT in either dialect.\n" + extra + "Type HELP " + verb + " for usage."
    ending = ("PASS supports NOFILTER. Type HELP PASS for complete syntax and examples." if verb == "PASS" else
              "Type HELP REJECT for complete syntax and examples.")
    clear = "ALL" if verb == "PASS" else "NONE"
    return ("Usage: " + verb + " <type> ...\n" + types + "COMMENT uses PASS/REJECT COMMENT <literal phrase>.\n"
            "MINSNR takes modes and <integer dB|" + clear + ">.\n"
            "NEARBY takes ON|OFF with either verb; PC93 aliases ANNOUNCE.\n" + ending)


def discovery_help_literal(key, dialect):
    # Deliberately distinct layout from generated HELP; literal semantic facts.
    if key == "DX":
        lines = ["DX - Post a spot (human entry).", "Usage: DX <freq_khz> <callsign> [comment]",
                 "       DX <callsign> <freq_khz> [comment]", "Notes:"]
        notes = ["Frequency is in kHz (e.g., 7001.0).", "Use plain decimal numbers; exponent notation is not accepted.",
                 "Comment: ASCII letters, digits, spaces, and / # @ - . , * ? _.",
                 "Other punctuation and non-ASCII characters are rejected.",
                 "The whole command has a node-configured byte limit (default 128).",
                 "Mode/report may be parsed from the comment.",
                 "Values in gaps between bands remain accepted.", "Rejects invalid callsigns and CTY-unknown DX when CTY is enabled.",
                 "Requires a valid logged-in callsign.", "Logins ending TEST require a CTY-known baseline callsign.",
                 "When enabled, station reputation admission is shared across numeric SSIDs.", "Posting admission may respond Spot rejected.",
                 "Spot queued confirms intake; filtering and processing determine delivery.",
                 "A missing or full input queue refuses the spot with an explicit response."]
    elif key in ("SHOW FILTER", "SHOW/FILTER"):
        lines = [key + " - Display current filter state.", "Usage: " + key, "       " + key + " FULL", "       " + key + " <category>"]
        aliases = "SHOW/FILTER, SH/FILTER" if dialect == "go" else "SH/FILTER, SHOW FILTER"
        lines += ["Aliases: " + aliases, "Notes:"]
        notes = ["DESTATE/DXSTATE show US/Canadian registered mailing codes; empty means unknown."]
    else:
        if key == "OVERVIEW":
            lines = ["GoCluster help - " + dialect.upper() + " dialect"]
        elif key == "FILTERS":
            lines = ["Filter reference:"]
        elif dialect == "cc":
            lines = cc_help_literal("SET/FILTER" if key == "PASS" else "UNSET/FILTER")
            lines[0] = ("SET/FILTER - Allow" if key == "PASS" else "UNSET/FILTER - Block") + " filter matches (CC dialect)."
        else:
            lines = [key + " - " + ("Allow" if key == "PASS" else "Block") + " filter matches.", "Usage: " + key + " <type> <list>"]
        notes = ["DXSTATE Spotted station's US/Canadian mailing code.", "DESTATE Spotter's US/Canadian mailing code."]
        if key == "FILTERS":
            notes += ["PASS/REJECT MODE <list> are deltas; modes not listed are unchanged.",
                      "DXSTATE/DESTATE accept US/Canadian registered mailing codes."]
        else:
            notes += ["Filter categories have their own selection rules; see HELP PASS/REJECT.",
                      "MODE preserves visibility for every mode you do not name.", "MODE changes only the modes you name."]
            lines += (["  REJECT MODE ALL", "  PASS MODE CW,FT8"] if dialect == "go" else
                      ["  UNSET/FILTER MODE ALL", "  SET/FILTER MODE CW,FT8"])
    for note in notes:
        lines += textwrap.wrap(note, width=78, initial_indent="  ", subsequent_indent="  ")
    if key in ("FILTERS", "PASS", "REJECT"):
        lines += ["", "US/Canada mailing codes (states, provinces, territories):"]
        codes = ("AA AB AE AK AL AP AR AS AZ BC CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA "
                 "MA MB MD ME MI MN MO MP MS MT NB NC ND NE NH NJ NL NM NS NT NU NV NY OH OK ON "
                 "OR PA PE PR QC RI SC SD SK TN TX UM UT VA VI VT WA WI WV WY YT").split()
        lines += ["  " + ", ".join(codes[index:index + 15]) for index in range(0, len(codes), 15)]
    while len(lines) < 10:
        lines.append("  Independent receipt filler.")
    return lines


class DriverControls(unittest.TestCase):
    def test_dx_input_archive_requires_positive_exact_identity(self):
        driver.require_dx_input_archive([HISTORY_ROW], "LABEL", 14074)
        for rows, label, frequency in (([], "LABEL", 14074), ([HISTORY_ROW] * 2, "LABEL", 14074),
                                       ([HISTORY_ROW], "OTHER", 14074), ([HISTORY_ROW], "LABEL", 14075),
                                       ([HISTORY_ROW.replace("CW", "FT8")], "LABEL", 14074)):
            with self.assertRaises(ValueError):
                driver.require_dx_input_archive(rows, label, frequency)

    def test_hold_matrix_and_registrations_keep_case_space_arity_and_alias_boundaries(self):
        self.assertEqual(driver.HOLD_VALID_QUERIES, ("SHOW HOLD", "show hold", "  ShOw   HoLd  ", "SHOW    HOLD"))
        expected = {"SHOW HOLD EXTRA": "Usage: SHOW HOLD", "show  hold  10": "Usage: SHOW HOLD",
                    " SHOW HOLD EXTRA TAIL ": "Usage: SHOW HOLD", "SHOW HOLD ALL": "Usage: SHOW HOLD",
                    "SH HOLD": "Unknown SHOW subcommand: HOLD", "SHOW HOLDS": "Unknown SHOW subcommand: HOLDS",
                    "SHOW HOLDX": "Unknown SHOW subcommand: HOLDX", "SHOW HOLD/EXTRA": "Unknown SHOW subcommand: HOLD/EXTRA",
                    "SHOW/HOLD": "Unknown command: SHOW/HOLD\nType HELP for available commands.",
                    "SH/HOLD": "Unknown command: SH/HOLD\nType HELP for available commands.",
                    "SHOWHOLD": "Unknown command: SHOWHOLD\nType HELP for available commands."}
        self.assertEqual(dict(driver.HOLD_REFUSALS), expected)
        self.assertEqual(len(driver.HOLD_REFUSALS), len(expected))
        self.assertFalse(any("\t" in query for query in driver.HOLD_VALID_QUERIES))
        tree = ast.parse(Path(driver.__file__).read_text(encoding="utf-8"))
        functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
        counts = []
        for name in ("profile", "dedupe_profile", "reputation_profile"):
            required = next(node.value for node in functions[name].body if isinstance(node, ast.Assign) and
                            any(isinstance(target, ast.Name) and target.id == "required" for target in node.targets))
            self.assertIsInstance(required, ast.List)
            counts.append(len(required.elts))
            if name == "profile":
                titles = ["".join(part.value for part in item.values if isinstance(part, ast.Constant))
                          for item in required.elts if isinstance(item, ast.JoinedStr)]
                self.assertEqual([value for value in titles if "HOLD" in value],
                                 ["/0: HOLD controls preserve countdown counters and delivery",
                                  "/1: HOLD preferences and delivery survive fresh child"])
        self.assertEqual(counts, [55, 7, 7])
        self.assertEqual(sum(counts) * 2, 138)
        calls = [node for node in ast.walk(functions["profile"]) if isinstance(node, ast.Call) and
                 isinstance(node.func, ast.Name) and node.func.id == "hold_command_cases"]
        self.assertEqual(len(calls), 2)
        for call in calls:
            self.assertEqual([node.id for node in call.args], ["suite", "root", "generation", "saved_hold", "session"])

    def test_hold_decoder_requires_complete_literal_status_counter_and_finite_seconds(self):
        query = "  ShOw   HoLd  "
        vectors = (("Live spots are not paused.", (False, False, 0, 0)),
                   ("Live spots are not paused. Suppressed spots in last pause: 1.", (False, False, 0, 1)),
                   ("Live spots paused for 15s more. Suppressed spots: 1.", (True, False, 15, 1)),
                   ("Live spots paused while a response is being delivered. Suppressed spots: 1.", (True, True, 0, 1)))
        for text, expected in vectors:
            for echo in (False, True):
                self.assertEqual(driver.decode_hold_reply(routing_help_wire([text], query.upper(), echo=echo), query, echo),
                                 dict(zip(("pause_active", "pause_pending_delivery", "pause_remaining_seconds", "suppressed_spots"), expected)))
        for text in ("Live spots paused for 0s more. Suppressed spots: 1.",
                     "Live spots paused for 301s more. Suppressed spots: 1.",
                     "Live spots paused for 1.5s more. Suppressed spots: 1.",
                     "Live spots paused for 015s more. Suppressed spots: 1.",
                     "Live spots paused for 15s more. Suppressed spots: -1.",
                     "Live spots paused for 15s more. Suppressed spots: 01.",
                     "Live spots are not paused. Suppressed spots in last pause: 0.",
                     "Live spots were not paused.", "Usage: SHOW HOLD", "Unknown SHOW subcommand: HOLD"):
            with self.assertRaises(ValueError):
                driver.decode_hold_reply(routing_help_wire([text], query.upper()), query, True)
        raw = routing_help_wire([vectors[2][0]], query.upper())
        for bad in (raw + b"LEAK\r\n", raw + b"Missed spots are not replayed.\r\n",
                    query.upper().encode() + b"\r\n" + raw, raw.replace(b"\r\n", b"\n"),
                    raw.replace(b"paused", b"paus\xffd"), raw.replace(b"paused", b"paus\x00d"), b"X" * 65537 + b"\r\n"):
            with self.assertRaises((ValueError, UnicodeDecodeError)):
                driver.decode_hold_reply(bad, query, True)

    def test_hold_complete_receipt_and_usage_precede_strict_barrier_with_failure_receipts(self):
        class Receipt(io.BytesIO):
            flushed = False

            def flush(self):
                self.flushed = True

        query = "show  hold"
        raw = routing_help_wire(["Live spots paused for 14s more. Suppressed spots: 1."], query.upper())
        command = b"GET YAML CONFIG SCHEMA 4 ID HOLD19\r\n"
        document = dict(resource="CONFIG", schema_version=4, request_id="HOLD19", configuration={}, status={}, revision="r19")

        def session(prefix=command, reply=raw):
            current = mock.Mock()
            current.call = "VE3ABC-70077"
            current.reader.remote_echo, current.reader.buffer = True, b""
            current.reader.line.side_effect = reply.splitlines(keepends=True)
            current.suite.log, current.suite.identifier.return_value = Receipt(), "HOLD19"
            current.frame.return_value = copy.deepcopy(document), prefix

            def sent(text):
                if text.startswith("GET "):
                    self.assertTrue(current.suite.log.flushed)
                    self.assertTrue(current.suite.log.getvalue().endswith(reply))
                    self.assertEqual(current.reader.line.call_count, len(reply.splitlines()))

            current.send.side_effect = sent
            return current

        for prefix in (command, b"\r\n" + command, command + b"LEAK\r\n", b"LEAK\r\n" + command,
                       command + command, b"\n" + command, command.replace(b"HOLD19", b"OTHER"), b"\r\n" * 32769 + command):
            current = session(prefix)
            if prefix in (command, b"\r\n" + command):
                rendered, actual, observed = driver.hold_readback(current, query)
                self.assertEqual(actual, document)
                self.assertEqual(rendered["pause_remaining_seconds"], 14)
                self.assertLessEqual(observed[0], observed[1])
            else:
                with self.assertRaises(ValueError):
                    driver.hold_readback(current, query)
        for field, value in (("resource", "FILTER"), ("schema_version", True), ("request_id", "OTHER"),
                             ("configuration", []), ("status", []), ("revision", ""), ("error", {})):
            current = session()
            current.frame.return_value[0][field] = value
            with self.assertRaises(ValueError):
                driver.hold_readback(current, query)
        refusal = routing_help_wire(["Usage: SHOW HOLD"], "SHOW HOLD EXTRA")
        current = session(reply=refusal)
        rendered, actual, _ = driver.hold_readback(current, "SHOW HOLD EXTRA", "Usage: SHOW HOLD")
        self.assertIsNone(rendered)
        self.assertEqual(actual, document)
        for error in (EOFError("incomplete"), TimeoutError("stalled"), ValueError("receive limit")):
            current = session()
            echo = raw.splitlines(keepends=True)[0]
            current.reader.line.side_effect, current.reader.buffer = [echo, error], b"PENDING"
            with self.assertRaises(type(error)):
                driver.hold_readback(current, query)
            self.assertEqual(current.send.call_count, 1)
            current.frame.assert_not_called()
            self.assertTrue(current.suite.log.getvalue().endswith(echo + b"PENDING"))
        for fault in (b"Usage: SHOW HOLD\r\n", b"X" * 65537):
            current = session()
            current.reader.line.side_effect = [fault]
            with self.assertRaises(ValueError):
                driver.hold_readback(current, query)
            self.assertEqual(current.send.call_count, 1)
            current.frame.assert_not_called()
            if len(fault) > 65536:
                self.assertIn(b"X" * 65536 + b"\n[HOLD receipt truncated at 65536 bytes]\n", current.suite.log.getvalue())

    def test_hold_deadline_intersection_rejects_extension_shortening_early_expiry_and_bad_types(self):
        state = dict(pause_active=True, pause_pending_delivery=False, pause_remaining_seconds=15, suppressed_spots=1)
        window = driver.hold_deadline_window(None, state, (100, 100.05))
        self.assertEqual(window, (114, 115.05))
        window = driver.hold_deadline_window(window, state, (100.4, 100.45))
        self.assertEqual(window, (114.4, 115.05))
        state["pause_remaining_seconds"] = 14
        self.assertEqual(driver.hold_deadline_window(window, state, (101.4, 101.45)), window)
        for seconds in (15, 12):
            with self.assertRaises(ValueError):
                driver.hold_deadline_window(window, dict(state, pause_remaining_seconds=seconds), (101.4, 101.45))
        inactive = dict(state, pause_active=False, pause_remaining_seconds=0)
        with self.assertRaises(ValueError):
            driver.hold_deadline_window(window, inactive, (103, 103.05))
        self.assertEqual(driver.hold_deadline_window(window, inactive, (115.1, 115.2)), window)
        for field, value in (("pause_active", 1), ("pause_pending_delivery", 0), ("pause_pending_delivery", True),
                             ("pause_remaining_seconds", True), ("pause_remaining_seconds", 14.0),
                             ("pause_remaining_seconds", -1), ("pause_remaining_seconds", 301),
                             ("suppressed_spots", True), ("suppressed_spots", -1), ("suppressed_spots", "1")):
            with self.assertRaises(ValueError):
                driver.hold_deadline_window(window, dict(state, **{field: value}), (101.4, 101.45))
        for observed in ((True, 101.45), (101.4, float("inf")), (float("nan"), 101.45), (101.45, 101.4), (-1, 1)):
            with self.assertRaises(ValueError):
                driver.hold_deadline_window(window, state, observed)
        for invalid in ((115, 114), (float("nan"), 115), (True, 115)):
            with self.assertRaises(ValueError):
                driver.hold_deadline_window(invalid, inactive, (115.1, 115.2))

    def test_hold_state_preserves_all_owners_counters_and_observed_deadline(self):
        session = dict(callsign="VE3ABC-70077", diagnostic_comments="MODE", temporary_defaults=False, suppressed_spots=1,
                       pause_active=True, pause_pending_delivery=False, pause_remaining_seconds=15)
        before = dict(configuration=dict(filters=dict(comments=["HOLD19"])), revision="r19",
                      status=dict(configured=dict(grid="FN31PR"), effective=dict(dedupe_policy="SLOW"),
                                  preset=dict(associated=True, name="HOLD19GO", modified=False), session=session,
                                  server=dict(auto_read_pause_min_rows=10), min_snr=dict(configured_count=1), comments=dict(pass_count=1)))
        after = copy.deepcopy(before)
        after["status"]["session"]["pause_remaining_seconds"] = 14
        record, library = mock.Mock(), mock.Mock()
        record.read_bytes.return_value, library.read_bytes.return_value = b"USER BYTES", b"LIBRARY BYTES"
        record.stat.return_value = mock.Mock(st_dev=1, st_ino=101, st_size=10, st_mtime_ns=1001)
        library.stat.return_value = mock.Mock(st_dev=1, st_ino=102, st_size=13, st_mtime_ns=1002)
        saved, library_saved = driver.discovery_file_receipt(record), driver.discovery_file_receipt(library)
        rendered = {field: after["status"]["session"][field] for field in
                    ("pause_active", "pause_pending_delivery", "pause_remaining_seconds", "suppressed_spots")}
        window, observed = (114.4, 115.05), (101.4, 101.45)

        def check(actual=after, human=rendered):
            return driver.require_hold_state(before, actual, record, saved, library, library_saved, observed, window, human)

        self.assertEqual(check(), window)
        for owner, field, value in (("document", "configuration", {}), ("document", "revision", "changed"),
                                    ("status", "configured", {}), ("status", "effective", {}), ("status", "preset", {}),
                                    ("status", "server", {}), ("status", "min_snr", {}), ("status", "comments", {}),
                                    ("session", "diagnostic_comments", "OFF"), ("session", "temporary_defaults", True),
                                    ("session", "suppressed_spots", 0), ("session", "suppressed_spots", 2),
                                    ("session", "suppressed_spots", True), ("session", "pause_pending_delivery", True),
                                    ("session", "pause_remaining_seconds", 15), ("session", "pause_remaining_seconds", 12),
                                    ("session", "pause_remaining_seconds", True)):
            changed = copy.deepcopy(after)
            target = changed if owner == "document" else changed["status"] if owner == "status" else changed["status"]["session"]
            target[field] = value
            with self.assertRaises(ValueError):
                check(changed)
        for field, value in (("suppressed_spots", 2), ("pause_pending_delivery", True),
                             ("pause_remaining_seconds", 15), ("pause_remaining_seconds", 13)):
            with self.assertRaises(ValueError):
                check(human=dict(rendered, **{field: value}))
        for handle in (record, library):
            data = handle.read_bytes.return_value
            handle.read_bytes.return_value = b"CHANGED"
            with self.assertRaises(ValueError):
                check()
            handle.read_bytes.return_value = data
            value = handle.stat.return_value.st_mtime_ns
            handle.stat.return_value.st_mtime_ns = value + 1
            with self.assertRaises(ValueError):
                check()
            handle.stat.return_value.st_mtime_ns = value
        inactive = copy.deepcopy(before)
        inactive["status"]["session"].update(pause_active=False, pause_remaining_seconds=0)
        self.assertIsNone(driver.require_hold_state(inactive, copy.deepcopy(inactive), record, saved, library,
                                                   library_saved, observed, None))
        with self.assertRaises(ValueError):
            driver.require_hold_state(inactive, after, record, saved, library, library_saved, observed, None)

    def test_hold_archive_and_live_spots_require_exact_identity_comment_and_fresh_frequency(self):
        label = "HOLD19 TABCDEFGHGS"
        base = HISTORY_ROW.replace("14074.00", "14177.00")
        archived = base[:39] + ("CW " + label).ljust(24) + base[63:]
        live = base[:39] + "CW CW|CMT".ljust(24) + base[63:]
        driver.require_hold_spot(archived, label, 14177)
        driver.require_hold_spot(live, "", 14177, True)
        for row in (archived.replace("K1ABC", "K1ABX"), archived.replace("W6ABC", "W6XYZ"),
                    archived.replace("14177.00", "14178.00"),
                    archived[:39] + ("CW " + label + "X").ljust(24) + archived[63:]):
            with self.assertRaises(ValueError):
                driver.require_hold_spot(row, label, 14177)
        for row in (live.replace("CW|CMT", "CW|SRC"), live.replace("14177.00", "14176.00"),
                    live.replace("W6ABC", "W6XYZ"), archived, live + "X"):
            with self.assertRaises(ValueError):
                driver.require_hold_spot(row, "", 14177, True)

    def test_hold_live_receipt_precedes_barrier_and_refuses_stale_or_unbounded_delivery(self):
        class Receipt(io.BytesIO):
            flushed = False

            def flush(self):
                self.flushed = True

        base = HISTORY_ROW.replace("14074.00", "14178.00")
        raw = (base[:39] + "CW CW|CMT".ljust(24) + base[63:] + "\r\n").encode("ascii")
        command = b"GET YAML CONFIG SCHEMA 4 ID HOLD19\r\n"
        document = dict(resource="CONFIG", schema_version=4, request_id="HOLD19", configuration={}, status={}, revision="r19")

        def session(prefix=command):
            current = mock.Mock()
            current.call = "VE3ABC-70077"
            current.reader.remote_echo, current.reader.buffer = True, b""
            current.reader.line.side_effect = [raw]
            current.suite.log, current.suite.identifier.return_value, current.suite.deadline = Receipt(), "HOLD19", float("inf")
            current.frame.return_value = copy.deepcopy(document), prefix

            def sent(text):
                self.assertTrue(text.startswith("GET "))
                self.assertTrue(current.suite.log.flushed)
                self.assertTrue(current.suite.log.getvalue().endswith(raw))
                self.assertEqual(current.reader.line.call_count, 1)

            current.send.side_effect = sent
            return current

        self.assertEqual(driver.hold_live_readback(session(), 14178), document)
        for prefix in (b"STALE\r\n" + command, command + command, b"\n" + command):
            with self.assertRaises(ValueError):
                driver.hold_live_readback(session(prefix), 14178)
        for bad in (raw.replace(b"14178.00", b"14177.00"), b"Unknown command: DX\r\n", b"X" * 65537):
            current = session()
            current.reader.line.side_effect = [bad]
            with self.assertRaises(ValueError):
                driver.hold_live_readback(current, 14178)
            current.send.assert_not_called()
            current.frame.assert_not_called()
            if len(bad) > 65536:
                self.assertIn(b"X" * 65536 + b"\n[HOLD live receipt truncated at 65536 bytes]\n", current.suite.log.getvalue())
        for error in (EOFError("incomplete"), TimeoutError("stalled"), ValueError("receive limit")):
            current = session()
            current.reader.line.side_effect, current.reader.buffer = [error], b"PENDING"
            with self.assertRaises(type(error)):
                driver.hold_live_readback(current, 14178)
            current.send.assert_not_called()
            current.frame.assert_not_called()
            self.assertTrue(current.suite.log.flushed)
            self.assertTrue(current.suite.log.getvalue().endswith(b"PENDING"))

    def test_help_reference_unknown_value_explanations_are_not_rejection_replies(self):
        query = "HELP FILTERS"
        contract = driver.help_route_contract("FILTERS", "go")
        lines = discovery_help_literal("FILTERS", "go")
        explanations = ["Unknown state fails an explicit PASS list and passes a REJECT-only list.",
                        "Unknown prefixes reject the whole list; canonical input requires CTY."]
        body = [*lines, *explanations]
        self.assertEqual(driver.decode_help_routing(routing_help_wire(body, query, pause=True), query,
                                                   contract, 10, 30, True), (body, True))
        for rejection in ("Unknown help topic: FILTERS", "Unknown state input", "Unknown prefixes", "Invalid configuration", "Usage: Invalid"):
            with self.assertRaisesRegex(ValueError, "rejection feedback"):
                driver.decode_help_routing(routing_help_wire([*body, rejection], query, pause=True), query,
                                           contract, 10, 30, True)
        other = discovery_help_literal("DX", "go")
        with self.assertRaisesRegex(ValueError, "rejection feedback"):
            driver.decode_help_routing(routing_help_wire([*other, explanations[0]], "HELP DX", pause=True), "HELP DX",
                                       driver.help_route_contract("DX", "go"), 10, 30, True)

    def test_discovery_inventory_is_literal_complete_action_specific_and_alias_qualified(self):
        families = ("BAND MODE SOURCE EVENT COMMENT DXCALL DECALL CONFIDENCE PATH DXGRID2 DEGRID2 "
                    "DXCONT DECONT DXZONE DEZONE DXDXCC DEDXCC DXSTATE DESTATE MINSNR BEACON WWV WCY "
                    "ANNOUNCE SELF TOXIC NEARBY DXBM NOFILTER").split()
        self.assertEqual(len(families), 29)
        self.assertEqual(set(driver.DISCOVERY_FAMILIES), set(families))
        for verb in ("PASS", "REJECT"):
            for unknown in (False, True):
                expected = discovery_literal_reply(verb, unknown)
                self.assertEqual(driver.filter_discovery_reply(verb, unknown), expected)
                driver.require_discovery_inventory(expected, verb, unknown)
                self.assertEqual(len(expected.splitlines()), 8 if unknown else 9)
                self.assertTrue(all(len(line) <= 78 for line in expected.splitlines()))
                opposite = "REJECT" if verb == "PASS" else "PASS"
                faults = [discovery_literal_reply(opposite, unknown), expected.replace("COMMENT, ", ""),
                          expected.replace("DXBM, NEARBY", "DXBM, DXBM"), expected.replace("SELF, TOXIC", "SELF, INVENTED"),
                          expected.replace("PC93 aliases ANNOUNCE.", "PC93 is a separate family."),
                          expected.replace("COMMENT uses PASS/REJECT COMMENT", "COMMENT uses SET/FILTER COMMENT")]
                for fault in faults:
                    with self.assertRaises(ValueError):
                        driver.require_discovery_inventory(fault, verb, unknown)
        # Altering both the expected wording and emitted reply must still fail
        # the separately enumerated family/alias/action boundary oracle.
        with mock.patch.object(driver, "DISCOVERY_TYPES", driver.DISCOVERY_TYPES.replace("COMMENT, ", "")):
            with self.assertRaisesRegex(ValueError, "filter family"):
                driver.require_discovery_inventory(driver.filter_discovery_reply("PASS"), "PASS")
        bad = discovery_literal_reply("REJECT", True).replace("PC93 aliases ANNOUNCE.", "NOFILTER; PC93 aliases ANNOUNCE.")
        with mock.patch.object(driver, "filter_discovery_reply", return_value=bad):
            with self.assertRaisesRegex(ValueError, "PASS-only NOFILTER"):
                driver.require_discovery_inventory(bad, "REJECT", True)

    def test_discovery_case_matrix_covers_both_actions_domains_and_existing_cc_forms(self):
        for dialect in ("go", "cc"):
            cases = driver.discovery_feedback_cases(dialect)
            commands = {query: (reply, verb, unknown) for query, reply, verb, unknown in cases}
            self.assertEqual(len(commands), len(cases))
            heads = [("PASS", "PASS"), ("REJECT", "REJECT")]
            if dialect == "cc":
                heads += [("SET/FILTER", "PASS"), ("UNSET/FILTER", "REJECT")]
            for head, verb in heads:
                self.assertEqual(commands[head], (discovery_literal_reply(verb), verb, False))
                self.assertEqual(commands[head + " INVENTED"], (discovery_literal_reply(verb, True), verb, True))
                self.assertEqual(commands[head + " DXBM"][0], "Usage: PASS DXBM <band-codes> or REJECT DXBM <band-codes>\nType HELP PASS or HELP REJECT for usage.")
                self.assertEqual(commands[head + " DXBM ALL"][0], "Unknown DXBM band: ALL\nSupported: 160, 80, 40, 30, 20, 17, 15, 12, 10, 6, 2 (and 1 if enabled).\nModes ignored; use REJECT MODE <mode>.")
                for family in ("DXSTATE", "DESTATE"):
                    self.assertEqual(commands[head + " " + family][0], "Usage: " + verb + " " + family + " <state>[,<state>...] or ALL\nUS/Canadian mailing codes include provinces and territories.\nType HELP " + verb + " for usage.")
                    self.assertEqual(commands[head + " " + family + " CT,ZZ"][0], "Invalid state selection: ZZ")
            self.assertEqual(commands["RESET MODE"][0], "Usage: RESET FILTER\nType HELP for usage.")
            self.assertEqual(commands["REJECT NOFILTER"][0], "Invalid filter command. Type HELP for usage.")
            self.assertEqual(any(query.startswith("SET/FILTER") for query in commands), dialect == "cc")
            if dialect == "cc":
                self.assertEqual(commands["SET/FILTER COMMENT CQ"][1:], ("PASS", True))
                self.assertEqual(commands["UNSET/FILTER COMMENT CQ"][1:], ("REJECT", True))

    def test_discovery_decoder_requires_complete_exact_printable_bounded_feedback(self):
        query, expected = "PASS", discovery_literal_reply("PASS")
        lines = expected.splitlines()
        for echo in (True, False):
            raw = routing_help_wire(lines, query, echo=echo)
            self.assertEqual(driver.decode_discovery_reply(raw, query, expected, echo), lines)
        faults = [routing_help_wire(discovery_literal_reply("REJECT").splitlines(), query),
                  routing_help_wire(lines[:-1], query), routing_help_wire(lines + ["EXTRA"], query),
                  routing_help_wire([lines[0], *lines[2:]], query), routing_help_wire([lines[0], "PASS", *lines[1:]], query),
                  routing_help_wire(["PASS", *lines], query), routing_help_wire(lines, query, pause=True),
                  routing_help_wire([lines[0], "X" * 79, *lines[2:]], query),
                  routing_help_wire(lines, query).replace(b"\r\n", b"\n", 1),
                  routing_help_wire(lines, query).replace(b"NEARBY", b"NEAR\x01Y"), b"X" * 65537]
        for raw in faults:
            with self.assertRaises(ValueError):
                driver.decode_discovery_reply(raw, query, expected, True)
        with self.assertRaises(ValueError):
            driver.decode_discovery_reply(routing_help_wire(lines, query), query, expected, False)

    def test_discovery_receipt_precedes_strict_barrier_and_preserves_bounded_failures(self):
        class Receipt(io.BytesIO):
            flushed = False

            def flush(self):
                self.flushed = True

        query, expected = "PASS", discovery_literal_reply("PASS")
        raw = routing_help_wire(expected.splitlines(), query)
        command = b"GET YAML CONFIG SCHEMA 4 ID DISC18\r\n"
        document = dict(resource="CONFIG", schema_version=4, request_id="DISC18", configuration={}, status={}, revision="r18")

        def session(prefix=command):
            current = mock.Mock()
            current.call = "VE3ABC-70075"
            current.reader.remote_echo, current.reader.buffer = True, b""
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            current.suite.log, current.suite.identifier.return_value = Receipt(), "DISC18"
            current.frame.return_value = copy.deepcopy(document), prefix

            def sent(text):
                if text.startswith("GET "):
                    self.assertTrue(current.suite.log.flushed)
                    self.assertTrue(current.suite.log.getvalue().endswith(raw))
                    self.assertEqual(current.reader.line.call_count, len(raw.splitlines()))

            current.send.side_effect = sent
            return current

        for prefix in (command, b"\r\n" + command, command + b"LEAK\r\n", b"LEAK\r\n" + command,
                       command + command, b"\n" + command, command.replace(b"DISC18", b"OTHER"), b"\r\n" * 32769 + command):
            current = session(prefix)
            if prefix in (command, b"\r\n" + command):
                self.assertEqual(driver.discovery_readback(current, query, expected), document)
                current.frame.assert_called_once_with("CONFIG", "DISC18", 4, command.decode().rstrip("\r\n"))
            else:
                with self.assertRaises(ValueError):
                    driver.discovery_readback(current, query, expected)
        for field, value in (("resource", "FILTER"), ("schema_version", True), ("schema_version", "4"),
                             ("request_id", "OTHER"), ("configuration", []), ("status", []), ("revision", ""), ("error", {})):
            current = session()
            current.frame.return_value[0][field] = value
            with self.assertRaises(ValueError):
                driver.discovery_readback(current, query, expected)
        for error in (EOFError("incomplete"), TimeoutError("stalled"), ValueError("receive limit")):
            current = session()
            current.reader.line.side_effect = [raw.splitlines(keepends=True)[0], error]
            current.reader.buffer = b"PENDING"
            with self.assertRaises(type(error)):
                driver.discovery_readback(current, query, expected)
            self.assertEqual(current.send.call_count, 1)
            current.frame.assert_not_called()
            self.assertTrue(current.suite.log.getvalue().endswith(raw.splitlines(keepends=True)[0] + b"PENDING"))
        for fault in (b"Unknown filter type.\r\n", b"X" * 65537):
            current = session()
            current.reader.line.side_effect = [fault]
            with self.assertRaises(ValueError):
                driver.discovery_readback(current, query, expected)
            self.assertEqual(current.send.call_count, 1)
            current.frame.assert_not_called()
            if len(fault) > 65536:
                self.assertIn(b"X" * 65536 + b"\n[filter discovery receipt truncated at 65536 bytes]\n", current.suite.log.getvalue())

    def test_discovery_help_wording_requires_literals_mailing_population_and_executable_mode_recipe(self):
        for dialect in ("go", "cc"):
            for key in ("DX", "OVERVIEW", "FILTERS", "PASS", "REJECT", "SHOW FILTER" if dialect == "go" else "SHOW/FILTER"):
                lines = discovery_help_literal(key, dialect)
                topic = "" if key == "OVERVIEW" else key
                query = ("HELP " + topic).rstrip()
                contract = driver.help_route_contract(key, dialect)
                decoded, paused = driver.decode_help_routing(routing_help_wire(lines, query, pause=True), query, contract, 10, 30, True)
                self.assertTrue(paused)
                driver.require_discovery_help(decoded, key, dialect)
                claim = ("Spot queued guarantees delivery." if key == "DX" else
                         "show FCC mailing-address codes" if key.startswith("SHOW") else "RESET MODE")
                bad = [*lines, "  " + claim]
                # Fault remains a valid complete, counted, correctly routed HELP
                # response; only the independent semantic oracle rejects it.
                decoded, _ = driver.decode_help_routing(routing_help_wire(bad, query, pause=True), query, contract, 10, 30, True)
                with self.assertRaises(ValueError):
                    driver.require_discovery_help(decoded, key, dialect)
                if key in ("FILTERS", "PASS", "REJECT"):
                    for fault in ("NT", "NU", "YT", "AA", "AE", "AP", "UM"):
                        bad = [line.replace(fault + ",", "ZZ,").replace(", " + fault, ", ZZ") for line in lines]
                        decoded, _ = driver.decode_help_routing(routing_help_wire(bad, query, pause=True), query, contract, 10, 30, True)
                        with self.assertRaisesRegex(ValueError, "mailing codes"):
                            driver.require_discovery_help(decoded, key, dialect)
                    heading = "US/Canada mailing codes (states, provinces, territories):"
                    for bad in ([*lines, heading], [*lines, "  YT"], [line for line in lines if line != heading]):
                        with self.assertRaises(ValueError):
                            driver.require_discovery_help(bad, key, dialect)
                if key in ("OVERVIEW", "PASS", "REJECT"):
                    for note in ("MODE preserves visibility", "Filter categories have their own", "DXSTATE Spotted", "DESTATE Spotter"):
                        bad = [line for line in lines if note not in line]
                        with self.assertRaises(ValueError):
                            driver.require_discovery_help(bad, key, dialect)
                    bad = [line.replace("REJECT MODE ALL", "PASS MODE ALL").replace("UNSET/FILTER MODE ALL", "SET/FILTER MODE ALL") for line in lines]
                    with self.assertRaisesRegex(ValueError, "block-before-pass"):
                        driver.require_discovery_help(bad, key, dialect)
                if key == "DX":
                    for note in ("station reputation admission", "Spot queued confirms", "Posting admission may", "Requires a valid logged", "Logins ending TEST",
                                 "Comment: ASCII", "Other punctuation", "whole command", "Mode/report may"):
                        with self.assertRaises(ValueError):
                            driver.require_discovery_help([line for line in lines if note not in line], key, dialect)
                    with self.assertRaises(ValueError):
                        driver.require_discovery_help([*lines, "Comment is free text."], key, dialect)
                if key.startswith("SHOW"):
                    with self.assertRaises(ValueError):
                        driver.require_discovery_help([line.replace("US/Canadian registered mailing", "FCC mailing-address") for line in lines], key, dialect)

    def test_discovery_help_semantic_callback_logs_receipt_before_barrier_and_refuses_false_green(self):
        query, lines = "HELP DX", discovery_help_literal("DX", "go")
        raw = routing_help_wire(lines, query, pause=True)
        document = dict(resource="CONFIG", schema_version=4, request_id="DISC18", configuration={}, status={}, revision="r18")
        for corrupt in (False, True):
            current = mock.Mock()
            current.call, current.reader.remote_echo, current.reader.buffer = "VE3ABC-70075", True, b""
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            current.suite.log, current.suite.identifier.return_value = io.BytesIO(), "DISC18"
            current.frame.return_value = document, b"GET YAML CONFIG SCHEMA 4 ID DISC18\r\n"

            def oracle(body):
                self.assertEqual(body, lines)
                self.assertEqual(current.send.call_count, 1)
                current.frame.assert_not_called()
                if corrupt:
                    raise ValueError("semantic fault")
                driver.require_discovery_help(body, "DX", "go")

            if corrupt:
                with self.assertRaisesRegex(ValueError, "semantic fault"):
                    driver.help_routing_readback(current, "DX", driver.help_route_contract("DX", "go"), 10, 30, oracle)
                current.frame.assert_not_called()
                self.assertEqual(current.send.call_count, 1)
            else:
                self.assertEqual(driver.help_routing_readback(current, "DX", driver.help_route_contract("DX", "go"), 10, 30, oracle), (document, True))
            self.assertTrue(current.suite.log.getvalue().endswith(raw))

    def test_discovery_owners_refuse_session_runtime_revision_bytes_and_same_bytes_rewrites(self):
        session = dict(callsign="VE3ABC-70075", diagnostic_comments="MODE", temporary_defaults=False, suppressed_spots=0,
                       pause_active=False, pause_pending_delivery=False, pause_remaining_seconds=0)
        before = dict(configuration=dict(filters=dict(comments=["DISCOVERY18 GO"])), revision="r18",
                      status=dict(configured=dict(grid="FN31PR"), effective=dict(dedupe_policy="SLOW"),
                                  preset=dict(associated=True, name="DISC18GO", modified=False), session=session,
                                  server=dict(auto_read_pause_min_rows=10), min_snr=dict(configured_count=1), comments=dict(pass_count=1)))
        record, library = mock.Mock(), mock.Mock()
        record.read_bytes.return_value, library.read_bytes.return_value = b"USER BYTES", b"LIBRARY BYTES"
        record.stat.return_value = mock.Mock(st_dev=1, st_ino=101, st_size=10, st_mtime_ns=1001)
        library.stat.return_value = mock.Mock(st_dev=1, st_ino=102, st_size=13, st_mtime_ns=1002)
        saved, library_saved = driver.discovery_file_receipt(record), driver.discovery_file_receipt(library)
        driver.require_discovery_owners(before, copy.deepcopy(before), record, saved, library, library_saved)
        long = copy.deepcopy(before)
        long["status"]["session"].update(pause_active=True, pause_remaining_seconds=30)
        driver.require_discovery_owners(before, long, record, saved, library, library_saved, True)
        with self.assertRaises(ValueError):
            driver.require_discovery_owners(before, long, record, saved, library, library_saved)
        for owner, field, value in (("document", "configuration", {}), ("document", "revision", "changed"),
                                    ("status", "configured", {}), ("status", "effective", {}), ("status", "preset", {}),
                                    ("status", "server", {}), ("status", "min_snr", {}), ("status", "comments", {}),
                                    ("session", "diagnostic_comments", "OFF"), ("session", "temporary_defaults", True),
                                    ("session", "suppressed_spots", 1), ("session", "pause_pending_delivery", True),
                                    ("session", "pause_active", True), ("session", "pause_remaining_seconds", 1)):
            changed = copy.deepcopy(before)
            target = changed if owner == "document" else changed["status"] if owner == "status" else changed["status"]["session"]
            target[field] = value
            with self.assertRaises(ValueError):
                driver.require_discovery_owners(before, changed, record, saved, library, library_saved)
        for handle in (record, library):
            data = handle.read_bytes.return_value
            handle.read_bytes.return_value = b"CHANGED"
            with self.assertRaises(ValueError):
                driver.require_discovery_owners(before, copy.deepcopy(before), record, saved, library, library_saved)
            handle.read_bytes.return_value = data
            for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns"):
                value = getattr(handle.stat.return_value, field)
                setattr(handle.stat.return_value, field, value + 1)
                with self.assertRaisesRegex(ValueError, "modification time"):
                    driver.require_discovery_owners(before, copy.deepcopy(before), record, saved, library, library_saved)
                setattr(handle.stat.return_value, field, value)
        for target in ("status", "session"):
            changed = copy.deepcopy(before)
            fields = changed["status"] if target == "status" else changed["status"]["session"]
            fields["invented"] = True
            with self.assertRaisesRegex(ValueError, "status shape"):
                driver.require_discovery_owners(before, changed, record, saved, library, library_saved)

    def test_discovery_fresh_saved_baseline_must_match_full_preferences_and_named_library(self):
        rule = dict(allow_all=True, block_all=False, allow={}, block={})
        filters = {field: copy.deepcopy(rule) for field in ("bands", "modes", "sources", "events", "confidence", "path_classes",
                                                           "dx_continents", "de_continents", "dx_zones", "de_zones", "dx_grid2",
                                                           "de_grid2", "dx_dxcc", "de_dxcc", "dx_states", "de_states")}
        filters.update(dx_callsigns=[], block_dx_callsigns=[], de_callsigns=[], block_de_callsigns=[], comments=["DISCOVERY18 CC"],
                       block_comments=[], min_snr={"CW": 7}, nearby_enabled=False, include_beacons=False,
                       allow_wwv=True, allow_wcy=True, allow_announce=False, allow_self=True, allow_toxic=True)
        settings = dict(dialect="cc", grid="FN31PR", noise_class="URBAN", dedupe_policy="SLOW",
                        path_min_observation_count=30, solar_summary_minutes=30)
        expected = dict(settings=settings, filters=filters)
        baseline = dict(settings, configuration_version=4, comments=["DISCOVERY18 CC"], min_snr={"CW": 7},
                        include_beacons=False, allow_wwv=True, allow_wcy=True, allow_announce=False, allow_self=True, allow_toxic=True)
        for stem in "bands modes sources events confidence pathclasses dxcontinents decontinents dxzones dezones dxgrid2 degrid2 dxdxcc dedxcc dxstates destates".split():
            baseline[stem + "prefixes" if stem in ("dxgrid2", "degrid2") else stem] = {}
            baseline["block" + stem], baseline["all" + stem], baseline["blockall" + stem] = {}, True, False
        stored = {**baseline, "preset": dict(name="DISC18CC", baseline=copy.deepcopy(baseline))}
        collection = dict(presets=dict(DISC18CC=copy.deepcopy(baseline)))
        record, library = mock.Mock(), mock.Mock()
        document = dict(configuration=expected, status=dict(effective=dict(nearby_active=False),
                                                            preset=dict(associated=True, name="DISC18CC", modified=False)))

        def assign(user=stored, presets=collection):
            record.read_bytes.return_value = driver.yaml.safe_dump(user).encode("ascii")
            library.read_bytes.return_value = driver.yaml.safe_dump(presets).encode("ascii")

        assign()
        driver.require_discovery_saved(document, expected, record, library, "DISC18CC")
        for field, value in (("grid", "IO91"), ("noise_class", "QUIET"), ("dedupe_policy", "FAST"),
                             ("path_min_observation_count", True), ("solar_summary_minutes", 0), ("comments", []), ("min_snr", {})):
            bad = copy.deepcopy(stored)
            bad["preset"]["baseline"][field] = value
            changed_library = dict(presets=dict(DISC18CC=copy.deepcopy(bad["preset"]["baseline"])))
            assign(bad, changed_library)
            with self.assertRaises(ValueError):
                driver.require_discovery_saved(document, expected, record, library, "DISC18CC")
        for bad in ({**stored, "preset": None}, {**stored, "preset": dict(name="OTHER", baseline=baseline)},
                    {**stored, "preset": dict(name="DISC18CC")}, {**stored, "grid": "IO91"}):
            assign(bad)
            with self.assertRaises(ValueError):
                driver.require_discovery_saved(document, expected, record, library, "DISC18CC")
        assign()
        changed = copy.deepcopy(document)
        changed["status"]["preset"]["modified"] = True
        with self.assertRaises(ValueError):
            driver.require_discovery_saved(changed, expected, record, library, "DISC18CC")
        changed_library = copy.deepcopy(collection)
        changed_library["presets"]["DISC18CC"]["comments"] = ["OTHER"]
        assign(presets=changed_library)
        with self.assertRaises(ValueError):
            driver.require_discovery_saved(document, expected, record, library, "DISC18CC")

    def test_help_routing_short_response_requires_intended_header_usage_terminal_and_real_aliases(self):
        contract = driver.help_route_contract("SHOW BUILD", "go")
        lines = ["SHOW BUILD - Show binary build metadata.", "Usage: SHOW BUILD", "Aliases: SH BUILD", "Notes:",
                 "  Shows the build version, release tag, and Go version when available."]
        query = "HELP SH BUILD EXTRA"
        for echo in (False, True):
            raw = routing_help_wire(lines, query, echo=echo)
            self.assertEqual(driver.decode_help_routing(raw, query, contract, 10, 30, echo), (lines, False))
        faults = [["SHOW - See SHOW subcommands."] + lines[1:],
                  [lines[0], "Usage: SHOW", *lines[2:]],
                  [lines[0], lines[1], "       SHOW BUILDX", *lines[2:]],
                  [line for line in lines if not line.startswith("Aliases:")],
                  [*lines[:-1], "Aliases: SH BUILDX", lines[-1]],
                  [*lines[:-1], "Unknown help topic: SH BUILD EXTRA", lines[-1]],
                  [*lines[:-1], query, lines[-1]], [query, *lines], lines + ["TRAILING CONTENT"],
                  [*lines[:-1], "  " + "X" * 77, lines[-1]], [*lines[:-1], "  invalid\x01note", lines[-1]],
                  lines[:-1]]
        for fault in faults:
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                driver.decode_help_routing(routing_help_wire(fault, query), query, contract, 10, 30, True)
        for raw in (routing_help_wire(lines, query).replace(b"\r\n", b"\n"),
                    routing_help_wire(lines, query)[:-2], b"\r\n" * 32769 + routing_help_wire(lines, query)):
            with self.assertRaises(ValueError):
                driver.decode_help_routing(raw, query, contract, 10, 30, True)
        with self.assertRaises(ValueError):
            driver.decode_help_routing(routing_help_wire(lines, query), query, contract, 10, 30, False)
        bye = ["BYE - Disconnect from the cluster.", "Usage: BYE", "Aliases: QUIT, EXIT"]
        self.assertEqual(driver.decode_help_routing(routing_help_wire(bye, "HELP EXIT EXTRA", echo=False),
                         "HELP EXIT EXTRA", driver.help_route_contract("BYE", "cc"), 10, 30), (bye, False))
        with self.assertRaises(ValueError):
            driver.decode_help_routing(routing_help_wire([*bye[:-1], "Aliases: QUIT, EXIT, SH BYE"], "HELP EXIT EXTRA", echo=False),
                                       "HELP EXIT EXTRA", driver.help_route_contract("BYE", "cc"), 10, 30)

    def test_help_routing_long_response_requires_conditional_complete_counted_pause_footer(self):
        query = "HELP PASS BAND 40 EXTRA"
        lines = ["PASS - Allow filter matches.", "Usage: PASS <type> <list>", "Notes:"] + ["  Literal contract note."] * 7
        contract = driver.help_route_contract("PASS", "go")
        raw = routing_help_wire(lines, query, pause=True)
        self.assertEqual(driver.decode_help_routing(raw, query, contract, 10, 30, True), (lines, True))
        for fault in (routing_help_wire(lines, query), raw.replace(b"Missed spots are not replayed.\r\n", b""),
                      routing_help_wire(lines, query, pause=True, count=11), routing_help_wire(lines, query, pause=True, seconds=29),
                      routing_help_wire(lines[:3], query, pause=True), raw + b"TRAILING\r\n",
                      routing_help_wire(lines + ["Missed spots are not replayed."], query, pause=True),
                      routing_help_wire(["REJECT - Block filter matches.", *lines[1:]], query, pause=True)):
            with self.assertRaises(ValueError):
                driver.decode_help_routing(fault, query, contract, 10, 30, True)
        # Leading body blanks count as rendered rows; they do not satisfy the header.
        blank_raw = routing_help_wire(["", *lines], query, pause=True)
        self.assertEqual(driver.decode_help_routing(blank_raw, query, contract, 10, 30, True), (lines, True))

    def test_help_routing_independent_topics_cover_source_heads_and_preserve_explicit_unknowns(self):
        shared = {"SH", "SH BUILD EXTRA", "SH OWN EXTRA", "SH DXCC DL EXTRA", "SH MYDX 1 MODE COMMENT",
                  "SH DX W6ABC 1", "SHOW/PROP DL 20 CW", "SH/PROP DL 20 CW", "SET SOLAR OFF EXTRA",
                  "SAVE PRESET ROUTE18 EXTRA", "LOAD PRESET ROUTE18 EXTRA", "LIST PRESET ROUTE18 EXTRA",
                  "DELETE PRESET ROUTE18 EXTRA", "SHOW/FILTER FULL EXTRA", "SH/FILTER COMMENT EXTRA",
                  "VALIDATE YAML CONFIG EXTRA"}
        negatives = {"SH FILTER", "SH SETTINGS", "SH DEDUPE", "SH HOLD", "SHOW/MYDX 1", "SHOW BUILDX",
                     "SH OWNX", "SHOW DXCCX DL", "GET YAML CONFIGX ID ROUTE18", "SET SOLARX 30"}
        for dialect in ("go", "cc"):
            rows = driver.help_routing_topics(dialect)
            topics = dict(rows)
            self.assertEqual(len(rows), len(topics))
            self.assertTrue(shared <= topics.keys())
            self.assertTrue(negatives <= {topic for topic, key in rows if key is None})
            self.assertEqual(topics["SH DXCC DL EXTRA"], "SHOW DXCC")
            for resource in ("FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"):
                self.assertEqual(topics["GET YAML " + resource + " SCHEMA 4 ID ROUTE18"], "GET YAML " + resource)
            for verb in ("PUT", "PATCH"):
                for resource in ("FILTER", "SETTINGS", "CONFIG"):
                    self.assertEqual(topics[verb + " YAML " + resource + " EXTRA"], verb + " YAML " + resource)
            for topic in negatives:
                query = "HELP " + topic
                contract = ("Unknown help topic: " + topic, (), "Type HELP for available commands.", ())
                lines = [contract[0], contract[2]]
                self.assertEqual(driver.decode_help_routing(routing_help_wire(lines, query, echo=False), query,
                                                           contract, 10, 30), (lines, False))
                with self.assertRaises(ValueError):
                    driver.decode_help_routing(routing_help_wire(["SHOW - See SHOW subcommands.", "Usage: SHOW"], query, echo=False),
                                               query, contract, 10, 30)
        cc = dict(driver.help_routing_topics("cc"))
        for topic in ("SET/CW_ALIAS EXTRA", "SET/COMMENT EXTRA", "SET/BAND EXTRA", "SET/MODE EXTRA"):
            self.assertEqual(cc[topic], "SET/<MODE>")
        for topic in ("SET/NOCW_ALIAS EXTRA", "SET/NOCOMMENT EXTRA"):
            self.assertEqual(cc[topic], "SET/NO<MODE>")
        for topic in ("SET/TEST_MODE EXTRA", "SET/NOTEST_MODE EXTRA", "SET/UNLISTED EXTRA"):
            self.assertIsNone(cc[topic])
        for topic in ("SET/NOFILTER EXTRA", "SET/ANN EXTRA", "SET/NOANN EXTRA"):
            self.assertEqual(cc[topic], topic.split()[0])
        go = dict(driver.help_routing_topics("go"))
        self.assertIsNone(go["SHOW/DX 1"])
        self.assertIsNone(go["SH/DX 1"])
        self.assertEqual(driver.help_route_contract("PASS", "cc")[0], "SET/FILTER - Allow filter matches (CC dialect).")
        self.assertEqual(driver.help_route_contract("REJECT", "cc")[0], "UNSET/FILTER - Block filter matches (CC dialect).")

    def test_help_routing_receipt_completes_and_flushes_before_strict_correlated_barrier(self):
        class Receipt(io.BytesIO):
            flushed = False

            def flush(self):
                self.flushed = True

        query = "HELP SH BUILD EXTRA"
        lines = ["SHOW BUILD - Show binary build metadata.", "Usage: SHOW BUILD", "Aliases: SH BUILD", "Notes:",
                 "  Shows the build version, release tag, and Go version when available."]
        raw = routing_help_wire(lines, query)
        command = b"GET YAML CONFIG SCHEMA 4 ID HELP18\r\n"
        document = dict(resource="CONFIG", schema_version=4, request_id="HELP18", configuration={}, status={}, revision="r18")
        for prefix in (command, b"\r\n" + command, command + b"LEAK\r\n", b"LEAK\r\n" + command,
                       command + command, b"\n" + command, command.replace(b"HELP18", b"OTHER"), b"\r\n" * 32769 + command):
            current = mock.Mock()
            current.call = "VE3ABC-70073"
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            current.reader.buffer = b""
            current.reader.remote_echo = True
            current.suite.identifier.return_value = "HELP18"
            current.suite.log = Receipt()
            current.frame.return_value = copy.deepcopy(document), prefix

            def sent(text):
                if text.startswith("GET "):
                    self.assertTrue(current.suite.log.flushed)
                    self.assertTrue(current.suite.log.getvalue().endswith(raw))
                    self.assertEqual(current.reader.line.call_count, len(raw.splitlines()))

            current.send.side_effect = sent
            if prefix in (command, b"\r\n" + command):
                self.assertEqual(driver.help_routing_readback(current, "SH BUILD EXTRA", driver.help_route_contract("SHOW BUILD", "go"),
                                                           10, 30), (document, False))
                current.frame.assert_called_once_with("CONFIG", "HELP18", 4, command.decode().rstrip("\r\n"))
            else:
                with self.assertRaises(ValueError):
                    driver.help_routing_readback(current, "SH BUILD EXTRA", driver.help_route_contract("SHOW BUILD", "go"), 10, 30)
        for field, value in (("resource", "FILTER"), ("schema_version", True), ("schema_version", "4"),
                             ("request_id", "OTHER"), ("configuration", []), ("status", []), ("revision", ""), ("error", {})):
            current.reader.line.reset_mock()
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            changed = copy.deepcopy(document)
            changed[field] = value
            current.frame.return_value = changed, command
            with self.assertRaises(ValueError):
                driver.help_routing_readback(current, "SH BUILD EXTRA", driver.help_route_contract("SHOW BUILD", "go"), 10, 30)

    def test_help_routing_sequential_short_terminal_requires_footer_when_row_threshold_is_met(self):
        query = "HELP SH BUILD EXTRA"
        lines = ["SHOW BUILD - Show binary build metadata.", "Usage: SHOW BUILD", "Aliases: SH BUILD", "Notes:",
                 *["  Independent filler note."] * 5,
                 "  Shows the build version, release tag, and Go version when available."]
        raw = routing_help_wire(lines, query, pause=True)
        command = b"GET YAML CONFIG SCHEMA 4 ID HELP18\r\n"
        document = dict(resource="CONFIG", schema_version=4, request_id="HELP18", configuration={}, status={}, revision="r18")
        current = mock.Mock()
        current.call = "VE3ABC-70074"
        current.reader.line.side_effect = raw.splitlines(keepends=True)
        current.reader.buffer = b""
        current.reader.remote_echo = True
        current.suite.identifier.return_value = "HELP18"
        current.suite.log = io.BytesIO()
        current.frame.return_value = document, command

        def sent(text):
            if text.startswith("GET "):
                self.assertIn(raw, current.suite.log.getvalue())
                self.assertEqual(current.reader.line.call_count, len(raw.splitlines()))

        current.send.side_effect = sent
        self.assertEqual(driver.help_routing_readback(current, "SH BUILD EXTRA", driver.help_route_contract("SHOW BUILD", "cc"), 10, 30),
                         (document, True))
        for error in (EOFError("incomplete"), TimeoutError("stalled"), ValueError("receive buffer limit")):
            current.reader.line.side_effect = [raw.splitlines(keepends=True)[0], error]
            current.reader.buffer = b"PENDING"
            current.suite.log = io.BytesIO()
            current.send.reset_mock()
            current.frame.reset_mock()
            with self.assertRaises(type(error)):
                driver.help_routing_readback(current, "SH BUILD EXTRA", driver.help_route_contract("SHOW BUILD", "cc"), 10, 30)
            self.assertTrue(current.suite.log.getvalue().endswith(raw.splitlines(keepends=True)[0] + b"PENDING"))
            current.frame.assert_not_called()
            self.assertEqual(current.send.call_count, 1)
        current.reader.line.side_effect = [b"X" * 65537]
        current.reader.buffer = b"UNRETURNED"
        current.suite.log = io.BytesIO()
        current.send.reset_mock()
        with self.assertRaises(ValueError):
            driver.help_routing_readback(current, "SH BUILD EXTRA", driver.help_route_contract("SHOW BUILD", "cc"), 10, 30)
        receipt = current.suite.log.getvalue().split(b"\n", 2)[2]
        self.assertTrue(receipt.startswith(b"X" * 65536))
        self.assertIn(b"[HELP routing receipt truncated at 65536 bytes]", receipt)
        self.assertNotIn(b"UNRETURNED", receipt)
        self.assertEqual(current.send.call_count, 1)

    def test_help_routing_preserves_every_preference_owner_and_only_allows_deliberate_pause(self):
        session = dict(callsign="VE3ABC-70073", diagnostic_comments="MODE", temporary_defaults=False, suppressed_spots=0,
                       pause_active=False, pause_pending_delivery=False, pause_remaining_seconds=0)
        before = dict(configuration=dict(filters=dict(comments=["ROUTE18 GO"])), revision="r18",
                      status=dict(configured=dict(grid="FN31PR"), effective=dict(dedupe_policy="SLOW"),
                                  preset=dict(associated=True, name="HELP18GO", modified=False), session=session,
                                  server=dict(auto_read_pause_seconds=30), min_snr=dict(configured_count=1), comments=dict(pass_count=1)))
        record, library = mock.Mock(), mock.Mock()
        record.read_bytes.return_value, library.read_bytes.return_value = b"USER BYTES", b"PRESET BYTES"
        driver.require_help_routing_state(before, copy.deepcopy(before), record, b"USER BYTES", library, b"PRESET BYTES", False)
        long = copy.deepcopy(before)
        long["status"]["session"].update(pause_active=True, pause_remaining_seconds=30)
        driver.require_help_routing_state(before, long, record, b"USER BYTES", library, b"PRESET BYTES", True)
        for owner, field, value in (("document", "configuration", {}), ("document", "revision", "changed"),
                                    ("status", "configured", {}), ("status", "effective", {}), ("status", "preset", {}),
                                    ("status", "server", {}), ("status", "min_snr", {}), ("status", "comments", {}),
                                    ("session", "diagnostic_comments", "OFF"), ("session", "callsign", "VE3ABC"),
                                    ("session", "temporary_defaults", True), ("session", "suppressed_spots", 1),
                                    ("session", "pause_pending_delivery", True), ("session", "pause_active", True),
                                    ("session", "pause_active", 0), ("session", "pause_remaining_seconds", False)):
            after = copy.deepcopy(before)
            target = after if owner == "document" else after["status"] if owner == "status" else after["status"]["session"]
            target[field] = value
            with self.assertRaises(ValueError):
                driver.require_help_routing_state(before, after, record, b"USER BYTES", library, b"PRESET BYTES", False)
        with self.assertRaises(ValueError):
            driver.require_help_routing_state(before, copy.deepcopy(before), record, b"USER BYTES", library, b"PRESET BYTES", True)
        for handle in (record, library):
            original = handle.read_bytes.return_value
            handle.read_bytes.return_value = b"CHANGED"
            with self.assertRaises(ValueError):
                driver.require_help_routing_state(before, copy.deepcopy(before), record, b"USER BYTES", library, b"PRESET BYTES", False)
            handle.read_bytes.return_value = original

    def test_help_routing_fresh_preferences_require_every_immediately_saved_setting(self):
        expected = dict(settings=dict(dialect="cc", grid="FN31PR", noise_class="URBAN", dedupe_policy="SLOW",
                                      path_min_observation_count=30, solar_summary_minutes=30))
        record = mock.Mock()
        record.read_bytes.return_value = driver.yaml.safe_dump(expected["settings"]).encode()
        document = dict(configuration=copy.deepcopy(expected))
        with mock.patch.object(driver, "require_cc_saved_state") as previous:
            driver.require_help_routing_saved(document, expected, record)
            previous.assert_called_once_with(document, expected, record)
            for field in expected["settings"]:
                changed = copy.deepcopy(expected["settings"])
                del changed[field]
                record.read_bytes.return_value = driver.yaml.safe_dump(changed).encode()
                with self.assertRaises(ValueError):
                    driver.require_help_routing_saved(document, expected, record)
            for field, value in (("path_min_observation_count", True), ("solar_summary_minutes", "30"),
                                 ("dedupe_policy", "FAST"), ("noise_class", "QUIET")):
                changed = copy.deepcopy(expected["settings"])
                changed[field] = value
                record.read_bytes.return_value = driver.yaml.safe_dump(changed).encode()
                with self.assertRaises(ValueError):
                    driver.require_help_routing_saved(document, expected, record)

    def test_cc_help_decoder_requires_complete_bounded_printable_response_and_count(self):
        lines = cc_help_literal("SET/FILTER")
        raw = cc_help_wire(lines)
        self.assertEqual(driver.decode_cc_help(raw, "HELP SET/FILTER", 10, 30), lines)
        for changed in (raw.replace(b"\r\n", b"\n"), raw + b"LEAK\r\n", raw[:-2],
                        raw.replace(b"CC dialect", b"CC\x01dialect"), cc_help_wire(lines, seconds=29),
                        cc_help_wire(lines, count=len(lines) + 1), cc_help_wire(lines, count=0),
                        cc_help_wire(["X" * 79] + lines), cc_help_wire(["HELP SET/FILTER"] + lines),
                        cc_help_wire(["WRONG - header"] + lines[1:]),
                        raw.replace(b"Missed spots are not replayed.\r\n", b""), b"\r\n" * 32769 + raw):
            with self.assertRaises(ValueError):
                driver.decode_cc_help(changed, "HELP SET/FILTER", 10, 30)

    def test_cc_help_grammar_requires_independent_categories_usage_and_flag_exceptions(self):
        for verb in ("SET/FILTER", "UNSET/FILTER"):
            lines = cc_help_literal(verb)
            driver.require_cc_help_grammar(lines, verb)
            faults = [lines + ["Usage: " + verb + " COMMENT <phrase>"],
                      [line.replace("DXCALL, DECALL", "COMMENT, DECALL") for line in lines],
                      [line.replace("SELF, TOXIC", "SELF, MINSNR") for line in lines],
                      [line for line in lines if "NEARBY ON|OFF" not in line],
                      [line.replace("<integer dB|" + ("ALL" if verb == "SET/FILTER" else "NONE") + ">", "<integer dB|BAD>") for line in lines],
                      [line.replace("CC list types:", "Lists:") for line in lines],
                      [line.replace("remain unsupported", "are supported") for line in lines],
                      lines + ["  SET/FILTER <type>/ON or /OFF"]]
            if verb == "SET/FILTER":
                faults += [[line for line in lines if "<list-type>/ON" not in line],
                           [line.replace("NOFILTER/OFF is unsupported", "NOFILTER/OFF is supported") for line in lines],
                           [line for line in lines if "DXBM/REJECT <band-codes>" not in line]]
            for fault in faults:
                with self.assertRaises(ValueError):
                    decoded = driver.decode_cc_help(cc_help_wire(fault, "HELP " + verb), "HELP " + verb, 10, 30)
                    driver.require_cc_help_grammar(decoded, verb)
        combined = ["Filter reference:"] + cc_help_literal("SET/FILTER") + cc_help_literal("UNSET/FILTER")
        driver.require_cc_help_grammar(combined, "FILTERS")
        raw = cc_help_wire(combined, "HELP FILTERS")
        self.assertEqual(driver.decode_cc_help(raw, "HELP FILTERS", 10, 30), combined)

    def test_cc_help_receipt_precedes_barrier_and_rejects_hidden_suffix(self):
        query = "HELP SET/FILTER"
        raw = cc_help_wire(cc_help_literal("SET/FILTER"), query)
        command = b"GET YAML CONFIG SCHEMA 4 ID CC17\r\n"
        document = dict(configuration={}, status={}, revision="r17")
        for prefix in (command, command + b"LEAK\r\n", b"LEAK\r\n" + command,
                       command + command, b"\n" + command, b"\r\n" * 32769 + command):
            current = mock.Mock()
            current.call = "VE3ABC-70071"
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            current.reader.buffer = b""
            current.reader.remote_echo = True
            current.suite.identifier.return_value = "CC17"
            current.suite.log = io.BytesIO()
            current.frame.return_value = copy.deepcopy(document), prefix

            def sent(text):
                if text.startswith("GET "):
                    self.assertIn(raw, current.suite.log.getvalue())
                    self.assertEqual(current.reader.line.call_count, len(raw.splitlines()))

            current.send.side_effect = sent
            if prefix == command:
                self.assertEqual(driver.cc_help_readback(current, "SET/FILTER", 10, 30), document)
            else:
                with self.assertRaises(ValueError):
                    driver.cc_help_readback(current, "SET/FILTER", 10, 30)
        for error in (EOFError("incomplete"), TimeoutError("stalled"), ValueError("receive buffer limit")):
            current.reader.line.side_effect = [raw.splitlines(keepends=True)[0], error]
            current.reader.buffer = b"PENDING"
            current.suite.log = io.BytesIO()
            current.send.reset_mock()
            current.send.side_effect = None
            current.frame.reset_mock()
            with self.assertRaises(type(error)):
                driver.cc_help_readback(current, "SET/FILTER", 10, 30)
            self.assertIn(b"PENDING", current.suite.log.getvalue())
            current.frame.assert_not_called()
            self.assertEqual(current.send.call_count, 1)

    def test_cc_command_success_requires_state_change_and_rejects_unknown(self):
        before = dict(configuration=dict(filters={}), revision="before")
        expected = dict(filters=dict(include_beacons=False))
        after = dict(configuration=expected, revision="after")
        record = mock.Mock()
        with mock.patch.object(driver, "require_cc_saved_state") as saved:
            driver.require_cc_success("Beacons disabled", before, after, expected, record)
            saved.assert_called_once_with(after, expected, record)
            for text, value, wanted in (("Unknown filter type.\nBeacons disabled", after, expected),
                                        ("Usage: SET/FILTER BEACON", after, expected),
                                        ("Invalid filter command", after, expected),
                                        ("Warning: changes could not be saved", after, expected),
                                        ("", after, expected), ("Beacons disabled", before, before["configuration"]),
                                        ("Beacons disabled", dict(configuration=expected, revision="before"), expected)):
                with self.assertRaises(ValueError):
                    driver.require_cc_success(text, before, value, wanted, record)
        for actual, wanted in ((False, 0), ({"CW": False}, {"CW": 0}), ({True: True}, {1: True}),
                               ({"allow": {}}, {"allow": {}, "block": {}}), ([False], [0])):
            with self.assertRaises(ValueError):
                driver.require_cc_typed_value(actual, wanted)

    def test_cc_saved_state_requires_live_and_immediate_disk_rules(self):
        rule = dict(allow_all=True, block_all=False, allow={}, block={})
        filters = {field: copy.deepcopy(rule) for field in ("bands", "modes", "sources", "events", "confidence", "path_classes",
                                                           "dx_continents", "de_continents", "dx_zones", "de_zones", "dx_grid2",
                                                           "de_grid2", "dx_dxcc", "de_dxcc", "dx_states", "de_states")}
        filters.update(dx_callsigns=[], block_dx_callsigns=[], de_callsigns=[], block_de_callsigns=[], comments=["CQ"],
                       block_comments=[], min_snr={"CW": -3}, nearby_enabled=False, include_beacons=False,
                       allow_wwv=True, allow_wcy=True, allow_announce=True, allow_self=True, allow_toxic=True)
        expected = dict(filters=filters, settings=dict(dialect="cc", grid="FN31"))
        stored = dict(dialect="cc", grid="FN31", comments=["CQ"], min_snr={"CW": -3}, include_beacons=False,
                      allow_wwv=True, allow_wcy=True, allow_announce=True, allow_self=True, allow_toxic=True)
        for stem in "bands modes sources events confidence pathclasses dxcontinents decontinents dxzones dezones dxgrid2 degrid2 dxdxcc dedxcc dxstates destates".split():
            stored[stem + "prefixes" if stem in ("dxgrid2", "degrid2") else stem] = {}
            stored["block" + stem], stored["all" + stem], stored["blockall" + stem] = {}, True, False
        record = mock.Mock()
        record.read_bytes.return_value = driver.yaml.safe_dump(stored).encode()
        document = dict(configuration=expected, status=dict(effective=dict(nearby_active=False)))
        driver.require_cc_saved_state(document, expected, record)
        for field, value in (("min_snr", {"CW": True}), ("include_beacons", 0), ("comments", []), ("allbands", 1), ("grid", "IO91")):
            changed = copy.deepcopy(stored)
            changed[field] = value
            record.read_bytes.return_value = driver.yaml.safe_dump(changed).encode()
            with self.assertRaises(ValueError):
                driver.require_cc_saved_state(document, expected, record)
        record.read_bytes.return_value = driver.yaml.safe_dump(stored).encode()
        changed = copy.deepcopy(expected)
        changed["filters"]["include_beacons"] = 0
        with self.assertRaises(ValueError):
            driver.require_cc_saved_state(dict(configuration=changed, status=document["status"]), expected, record)
        for active in (True, 0):
            with self.assertRaises(ValueError):
                driver.require_cc_saved_state(dict(configuration=expected, status=dict(effective=dict(nearby_active=active))), expected, record)

    def test_cc_refusal_preserves_all_preference_owners_and_exact_disk_bytes(self):
        before = dict(configuration=dict(filters=dict(nearby_enabled=False)), revision="r17",
                      status=dict(configured=dict(grid="FN31"), effective=dict(nearby_active=False), preset=dict(associated=False)))
        record = mock.Mock()
        record.read_bytes.return_value = b"EXACT STORED BYTES"
        driver.require_cc_unchanged(before, copy.deepcopy(before), record, b"EXACT STORED BYTES")
        for field in ("configuration", "revision", "configured", "effective", "preset"):
            after = copy.deepcopy(before)
            target = after if field in ("configuration", "revision") else after["status"]
            target[field] = "changed" if field == "revision" else {"changed": True}
            with self.assertRaises(ValueError):
                driver.require_cc_unchanged(before, after, record, b"EXACT STORED BYTES")
        record.read_bytes.return_value = b"REWRITTEN STORED BYTES"
        with self.assertRaises(ValueError):
            driver.require_cc_unchanged(before, copy.deepcopy(before), record, b"EXACT STORED BYTES")

    def test_history_alias_pages_require_complete_framing_and_continuation(self):
        query = "SHOW DX 1 MODE COMMENT COMMENT LABEL"
        token = "H1" + "A" * 32
        footer = "Continue older history: SHOW DX NEXT " + token + "\r\n"
        raw = (query + "\r\n" + HISTORY_ROW + "\r\n" + footer).encode()
        self.assertEqual(driver.decode_history_alias_page(raw, query, False), ([HISTORY_ROW], token))
        older = ("Older retained history page:\r\n" + HISTORY_ROW + "\r\n" + footer).encode()
        self.assertEqual(driver.decode_history_alias_page(older, query, True), ([HISTORY_ROW], token))
        self.assertEqual(driver.decode_history_alias_page(b"Older retained history page:\r\nEnd of retained history search.\r\n",
                                                        query, True), ([], None))
        faults = ((raw.replace(b"\r\n", b"\n"), False),
                  (raw + b"LEAK\r\n", False), (raw + footer.encode(), False),
                  ((query + "\r\n").encode() + raw, False),
                  (raw.replace(token.encode(), b"H1BAD"), False),
                  (footer.encode(), False), (b"End of retained history search.\r\n", False),
                  (older, False), (raw, True),
                  (b"Older retained history page:\r\nNo matching retained spots.\r\n", True),
                  (raw.replace(footer.encode(), b"Search work limit reached; this page is incomplete.\r\n"), False),
                  (raw[:-len(footer)], False))
        for data, continuation in faults:
            with self.assertRaises(ValueError):
                driver.decode_history_alias_page(data, query, continuation)

    def test_history_alias_rows_require_exact_literal_fields_and_count(self):
        phrase = "C16LABEL:  UP5!"
        row = HISTORY_ROW.replace("K1ABC", "N0XYZ").replace("14074.00", "14064.00")
        row = row[:39] + ("CW " + phrase).ljust(24) + row[63:]
        driver.require_history_alias_row([row], 14064, phrase)
        wrong_mode = row[:39] + ("CW-X " + phrase).ljust(24) + row[63:]
        driver.require_history_row(wrong_mode)
        wrong_spaces = row[:39] + ("CW " + phrase.replace(":  ", ": ")).ljust(24) + row[63:]
        driver.require_history_row(wrong_spaces)
        for rows, frequency, value in (([], 14064, phrase), ([row, row], 14064, phrase),
                                       ([wrong_mode], 14064, phrase), ([row], 14063, phrase),
                                       ([row.replace("N0XYZ", "W1XYZ")], 14064, phrase),
                                       ([wrong_spaces], 14064, phrase),
                                       ([row], 14064, phrase + "X")):
            with self.assertRaises(ValueError):
                driver.require_history_alias_row(rows, frequency, value)

    def test_history_alias_barrier_rejects_hidden_suffix_and_missing_readback(self):
        query = "SHOW DX 1 MODE COMMENT COMMENT LABEL"
        raw = (HISTORY_ROW + "\r\nEnd of retained history search.\r\n").encode()
        command = b"GET YAML CONFIG SCHEMA 4 ID BARRIER16\r\n"
        document = dict(configuration={}, status={}, revision="r16")

        def session(prefix, readback=document):
            current = mock.Mock()
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            current.reader.buffer = b""
            current.reader.remote_echo = True
            current.suite.identifier.return_value = "BARRIER16"
            current.suite.log = io.BytesIO()
            current.frame.return_value = (copy.deepcopy(readback), prefix)
            return current

        self.assertEqual(driver.history_alias_page(session(command), query), ([HISTORY_ROW], None, document))
        for prefix in (b"LEAK\r\n" + command, command + b"LEAK\r\n", command + command,
                       b"\n" + command, b"\r\n" * 32769 + command):
            with self.assertRaises(ValueError):
                driver.history_alias_page(session(prefix), query)
        for readback in (dict(error="refused"), dict(configuration={}, status={}, revision="")):
            with self.assertRaises(ValueError):
                driver.history_alias_page(session(command, readback), query)

    def test_taxonomy_rules_require_canonical_live_and_saved_names_and_booleans(self):
        expected = dict(modes=dict(allow_all=False, block_all=False, allow={"TEST_MODE": True, "CW": True}, block={}),
                        events=dict(allow_all=False, block_all=False, allow={"TEST_EVENT": True}, block={}))
        stored = dict(modes={"TEST_MODE": True, "CW": True}, blockmodes={}, allmodes=False, blockallmodes=False,
                      events={"TEST_EVENT": True}, blockevents={}, allevents=False, blockallevents=False)
        record = mock.Mock()
        record.read_bytes.return_value = driver.yaml.safe_dump(stored).encode()
        document = dict(configuration=dict(filters=copy.deepcopy(expected)))
        driver.require_taxonomy_rules(document, expected, record)
        for kind in ("modes", "events"):
            for fault in ("wrong_name", "integer_flag", "integer_entry", "missing_name"):
                changed = copy.deepcopy(document)
                rules = changed["configuration"]["filters"][kind]
                if fault == "wrong_name":
                    rules["allow"] = {"UNDERSCORE_REMOVED": True}
                elif fault == "integer_flag":
                    rules["allow_all"] = 0
                elif fault == "integer_entry":
                    rules["allow"][next(iter(rules["allow"]))] = 1
                else:
                    rules["allow"] = {}
                with self.assertRaises(ValueError):
                    driver.require_taxonomy_rules(changed, expected, record)
            changed = copy.deepcopy(stored)
            changed[kind] = {}
            record.read_bytes.return_value = driver.yaml.safe_dump(changed).encode()
            with self.assertRaises(ValueError):
                driver.require_taxonomy_rules(document, expected, record)
            record.read_bytes.return_value = driver.yaml.safe_dump(stored).encode()

    def test_taxonomy_archive_requires_literal_identity_mode_frequency_and_label(self):
        # Reuse the literal fixture's padding/tail, not the production formatter.
        row = HISTORY_ROW.replace("K1ABC", "N0XYZ").replace("14074.00", "14061.00")
        row = row[:39] + "TEST_MODE T15LABEL".ljust(24) + row[63:]
        suffix_mode = row[:39] + "TEST_MODE-X T15LABEL1234" + row[63:]
        suffix_label = row[:39] + "TEST_MODE T15LABELX".ljust(24) + row[63:]
        driver.require_taxonomy_archive([row], "T15LABEL", 14061, True)
        driver.require_taxonomy_archive([], "T15LABEL", 14061, False)
        driver.require_history_row(suffix_mode)
        driver.require_history_row(suffix_label)
        for rows, label, frequency, visible in (([], "T15LABEL", 14061, True),
                                              ([row], "T15LABEL", 14061, False),
                                              ([row], "STALELABEL", 14061, True),
                                              ([row], "T15LABEL", 14062, True),
                                              ([row.replace("TEST_MODE", "WRONGMODE")], "T15LABEL", 14061, True),
                                              ([suffix_mode], "T15LABEL1234", 14061, True),
                                              ([suffix_label], "T15LABEL", 14061, True),
                                              ([row.replace("N0XYZ", "W1XYZ")], "T15LABEL", 14061, True),
                                              ([row, row], "T15LABEL", 14061, True)):
            with self.assertRaises(ValueError):
                driver.require_taxonomy_archive(rows, label, frequency, visible)

    def test_solar_receipt_requires_exact_loaded_availability(self):
        for available in (False, True):
            support.require_solar_identity({"solar_enabled": available}, available)
            for value in (None, not available, int(available), str(available)):
                with self.assertRaisesRegex(ValueError, "loaded solar availability mismatch"):
                    support.require_solar_identity({"solar_enabled": value}, available)

    def test_solar_oracle_requires_exact_cadence_availability_and_read_owners(self):
        record = mock.Mock()
        record.read_bytes.return_value = b"solar_summary_minutes: 30\n"
        saved = record.read_bytes()
        for available in (False, True):
            before = dict(configuration={"settings": {"solar_summary_minutes": 30}}, revision="r14",
                          status=dict(effective={"solar_summary_minutes": 30 if available else 0},
                                      preset={"name": "SOLAR14"}))
            wording = "Every 30 minutes when conditions are active" if available else "30 minutes; unavailable on this server"
            text = "Solar         " + wording
            driver.require_solar_readback(before, copy.deepcopy(before), record, saved, available, text)
            for fault in ("configuration", "effective", "revision", "preset", "wording", "duplicate", "disk"):
                after, output = copy.deepcopy(before), text
                record.read_bytes.return_value = saved
                if fault == "configuration":
                    after["configuration"]["settings"]["solar_summary_minutes"] = 0
                elif fault == "effective":
                    after["status"]["effective"]["solar_summary_minutes"] = 0 if available else 30
                elif fault == "revision":
                    after["revision"] = "changed"
                elif fault == "preset":
                    after["status"]["preset"] = {}
                elif fault == "wording":
                    output = "Solar         Every 30 minutes"
                elif fault == "duplicate":
                    output += "\n" + text
                else:
                    record.read_bytes.return_value = b"changed"
                with self.assertRaises(ValueError):
                    driver.require_solar_readback(before, after, record, saved, available, output)
            record.read_bytes.return_value = saved
        for configured, effective in ((False, 0), (0.0, 0), (0, False), (0, 0.0)):
            value = dict(configuration={"settings": {"solar_summary_minutes": configured}},
                         status=dict(effective={"solar_summary_minutes": effective}))
            with self.assertRaisesRegex(ValueError, "cadence mismatch"):
                driver.require_solar_value(value, 0, False)

    def test_large_dedupe_oracle_requires_lossless_value_reason_and_footer(self):
        text = ('Dedupe: "XX"\n        + "XX"; effective MED (grid2)\n'
                '        (fast=on med=on slow=on)\n        Configured choice is unrecognized\n'
                'Live spots paused for 30s after 4 output rows. Type RESUME to resume now.\nMissed spots are not replayed.')
        driver.require_large_dedupe_output(text, "XXXX")
        for old, new in (("; effective MED", "; effective FAST"), ('+ "XX"', '+ "X"'),
                         ("Configured choice is unrecognized", "Configured choice is unavailable"),
                         ("Missed spots are not replayed.", "")):
            with self.assertRaisesRegex(ValueError, "large DEDUPE output lost"):
                driver.require_large_dedupe_output(text.replace(old, new), "XXXX")

    def test_large_dedupe_reader_checks_wire_before_correlated_get(self):
        current = mock.Mock()
        current.call, current.suite.log = "N0CALL-1", io.BytesIO()
        current.suite.identifier.return_value = "BARRIER13"
        current.reader.remote_echo = True
        expected = dict(configuration={}, status={}, revision="r13")
        current.frame.return_value = (expected, b"\r\nGET YAML FILTER SCHEMA 4 ID BARRIER13\r\n\r\n")
        current.reader.buffer = b"pending"
        current.reader.line.side_effect = [b"Dedupe: control\r\n", b"Done\r\n"]
        text, document = driver.large_dedupe_reply(current, "Done")
        self.assertEqual(text, "Dedupe: control\nDone")
        current.frame.assert_called_once_with("FILTER", "BARRIER13", 4, "GET YAML FILTER SCHEMA 4 ID BARRIER13")
        current.get.assert_not_called()
        self.assertIs(document, expected)
        for line in (b"bad\n", b"bad\r\r\n", b"X" * 79 + b"\r\n", b"X" * 65536 + b"\r\n"):
            current.frame.reset_mock()
            current.suite.log = io.BytesIO()
            current.reader.line.side_effect = [line]
            with self.assertRaises(ValueError):
                driver.large_dedupe_reply(current, "Done")
            current.frame.assert_not_called()
            self.assertIn(line[:65536], current.suite.log.getvalue())

    def test_large_dedupe_barrier_refuses_content_after_both_terminators(self):
        current = mock.Mock()
        current.call, current.suite.log = "N0CALL-1", io.BytesIO()
        current.suite.identifier.return_value = "BARRIER13"
        current.reader.remote_echo, current.reader.buffer = True, b""
        document = dict(configuration={}, status={}, revision="r13")
        echo = b"GET YAML FILTER SCHEMA 4 ID BARRIER13\r\n"
        for final in ("Readback failed: response exceeds 65,536 bytes.", "Missed spots are not replayed."):
            for prefix in (b'        + "LEAKED"\r\n', b"\r\n" * 32768, b"bad\n", b"\r\r\n", echo + echo):
                with self.subTest(final=final, prefix=prefix[:80]):
                    current.reader.line.side_effect = [final.encode("ascii") + b"\r\n"]
                    current.frame.return_value = document, prefix
                    with self.assertRaisesRegex(ValueError, "large DEDUPE"):
                        driver.large_dedupe_reply(current, final)

    def test_large_dedupe_barrier_requires_valid_document_and_expected_echo(self):
        current = mock.Mock()
        current.suite.identifier.return_value = "BARRIER13"
        current.reader.remote_echo = False
        document = dict(configuration={}, status={}, revision="r13")
        current.frame.return_value = document, b"GET YAML FILTER SCHEMA 4 ID BARRIER13\r\n"
        with self.assertRaisesRegex(ValueError, "trailing content"):
            driver.dedupe_read_barrier(current, 100)
        for bad in ({}, {**document, "revision": ""}, {**document, "error": {"message": "failed"}}):
            current.frame.return_value = bad, b"\r\n"
            with self.assertRaisesRegex(ValueError, "valid readback"):
                driver.dedupe_read_barrier(current, 100)

    def test_dedupe_readback_oracle_requires_saved_effective_and_unchanged_owners(self):
        before = dict(configuration={"settings": {"dedupe_policy": "SLOW"}}, revision="r1",
                      status=dict(effective={"dedupe_policy": "FAST"}, preset={"name": "DEDUPE13"}))
        text = "Dedupe        SLOW; effective FAST\n              Configured choice is unavailable on this server"
        record = mock.Mock()
        record.read_bytes.return_value = b"dedupe_policy: SLOW\n"
        driver.require_dedupe_readback(before, copy.deepcopy(before), record, record.read_bytes(), text)
        for field in ("configured", "effective", "revision", "preset", "disk", "wording"):
            value = copy.deepcopy(before)
            record.read_bytes.return_value = b"changed" if field == "disk" else b"dedupe_policy: SLOW\n"
            if field == "configured":
                value["configuration"]["settings"]["dedupe_policy"] = "FAST"
            elif field == "effective":
                value["status"]["effective"]["dedupe_policy"] = "SLOW"
            elif field == "revision":
                value["revision"] = "r2"
            elif field == "preset":
                value["status"]["preset"] = {}
            with self.assertRaises(ValueError):
                driver.require_dedupe_readback(before, value, record, b"dedupe_policy: SLOW\n",
                                              "Dedupe        FAST" if field == "wording" else text)

    def test_dedupe_receipt_requires_exact_loaded_availability(self):
        expected = dict(FAST=True, MED=False, SLOW=False)
        support.require_dedupe_identity({"dedupe_available": expected}, expected)
        for value in (None, {}, dict(FAST=True, MED=True, SLOW=True), dict(FAST=1, MED=0, SLOW=0),
                      dict(FAST=True, MED=False, SLOW=False, EXTRA=False)):
            with self.assertRaisesRegex(ValueError, "loaded dedupe availability mismatch"):
                support.require_dedupe_identity({"dedupe_available": value}, expected)

    def test_setting_zero_oracle_requires_integer_representation(self):
        for field in ("path_min_observation_count", "solar_summary_minutes"):
            session, record = mock.Mock(), mock.Mock()
            session.get.return_value = {"configuration": {"settings": {field: 30}}}
            # Go's canonical omitempty representation may omit zero; both an
            # omitted key and a literal integer zero retain the numeric contract.
            for wire_value, saved in ((0, b"{}\n"), (0, (field + ": 0\n").encode()),
                                       (False, b"{}\n"), (0.0, b"{}\n"),
                                       (0, (field + ": false\n").encode()),
                                       (0, (field + ": 0.0\n").encode()),
                                       (0, (field + ": '0'\n").encode()),
                                       (0, (field + ": null\n").encode())):
                record.read_bytes.return_value = saved
                document = {"configuration": {"settings": {field: wire_value}}}
                with mock.patch.object(driver, "exact_human_reply", return_value=("Zero control", document)):
                    if type(wire_value) is int and saved in (b"{}\n", (field + ": 0\n").encode()):
                        driver.require_setting_control(session, record, "Zero", "Zero control", field, 0)
                    else:
                        with self.assertRaisesRegex(ValueError, "immediately saved value"):
                            driver.require_setting_control(session, record, "Zero", "Zero control", field, 0)

    def test_setting_refusal_oracle_includes_session_only_diagnostics(self):
        session = mock.Mock()
        session.get.side_effect = [{"status": {"session": {"diagnostic_comments": "SOURCE"}}},
                                   {"status": {"session": {"diagnostic_comments": "OFF"}}}]
        with mock.patch.object(driver, "require_command_refusal") as common:
            with self.assertRaisesRegex(ValueError, "session diagnostic mode"):
                driver.require_setting_argument_refusal(session, "SET DIAG OFF EXTRA", mock.Mock(), "Usage")
            common.assert_called_once()
        session.get.side_effect = [{"status": {"session": {"diagnostic_comments": "SOURCE"}}},
                                   {"status": {"session": {"diagnostic_comments": "SOURCE"}}}]
        with mock.patch.object(driver, "require_command_refusal"):
            driver.require_setting_argument_refusal(session, "SET DIAG OFF EXTRA", mock.Mock(), "Usage")

    def test_setting_control_requires_live_value_and_immediate_disk(self):
        before = dict(configuration={"settings": {"noise_class": "RURAL"}}, revision="revision-1",
                      status=dict(effective={"noise_class": "RURAL"}, preset={"name": "control"},
                                  session={"diagnostic_comments": "SOURCE"}))
        after = dict(configuration={"settings": {"noise_class": "URBAN"}}, revision="revision-2",
                     status=dict(effective={"noise_class": "URBAN"}, preset={"name": "control"},
                                 session={"diagnostic_comments": "SOURCE"}))
        session, record = mock.Mock(), mock.Mock()
        for live, disk in (("URBAN", b"noise_class: URBAN\n"), ("RURAL", b"noise_class: URBAN\n"),
                           ("URBAN", b"noise_class: RURAL\n")):
            value = copy.deepcopy(after)
            value["configuration"]["settings"]["noise_class"] = live
            session.get.return_value = before
            record.read_bytes.return_value = disk
            with mock.patch.object(driver, "exact_human_reply", return_value=("Noise class set to URBAN", value)):
                if live == "URBAN" and b"URBAN" in disk:
                    driver.require_setting_control(session, record, "SET NOISE URBAN", "Noise class set to URBAN", "noise_class", "URBAN")
                else:
                    with self.assertRaisesRegex(ValueError, "immediately saved value"):
                        driver.require_setting_control(session, record, "SET NOISE URBAN", "Noise class set to URBAN", "noise_class", "URBAN")
        value = copy.deepcopy(after)
        value["status"]["effective"]["noise_class"] = "RURAL"
        record.read_bytes.return_value = b"noise_class: URBAN\n"
        with mock.patch.object(driver, "exact_human_reply", return_value=("Noise class set to URBAN", value)):
            with self.assertRaisesRegex(ValueError, "runtime state"):
                driver.require_setting_control(session, record, "SET NOISE URBAN", "Noise class set to URBAN", "noise_class", "URBAN")
        for channel in ("configuration", "revision", "effective", "preset", "disk", "diagnostics"):
            value = copy.deepcopy(before)
            value["status"]["session"]["diagnostic_comments"] = "CONF"
            record.read_bytes.side_effect = [b"unchanged", b"changed" if channel == "disk" else b"unchanged"]
            if channel in ("configuration", "revision"):
                value[channel] = "changed"
            elif channel in ("effective", "preset"):
                value["status"][channel] = "changed"
            elif channel == "diagnostics":
                value["status"]["session"]["diagnostic_comments"] = "OFF"
            with mock.patch.object(driver, "exact_human_reply", return_value=("Diagnostic comments: CONF", value)):
                with self.assertRaisesRegex(ValueError, "changed persistent state"):
                    driver.require_setting_control(session, record, "SET DIAG CONFIDENCE", "Diagnostic comments: CONF", "diagnostic_comments", "CONF")

    def test_logout_oracle_requires_remote_eof_before_local_close(self):
        session = mock.Mock()
        session.call, session.suite.log = "N0CALL-1", io.BytesIO()
        session.reader.buffer, session.reader.state = bytearray(), "data"
        events = []

        def read_line():
            events.append("read")
            if len(events) == 1:
                return b"BYE\r\n"
            if len(events) == 2:
                return b"73!\r\n"
            events.append("remote EOF")
            raise EOFError("closed")

        session.reader.line.side_effect = read_line
        session.sock.close.side_effect = lambda: events.append("local close")
        driver.require_remote_logout(session, "BYE")
        self.assertEqual(events[-2:], ["remote EOF", "local close"])
        self.assertTrue(session.closed)
        self.assertTrue(session.suite.log.getvalue().endswith(b"BYE\r\n73!\r\n"))
        for lines, pending, state in (([EOFError("no farewell")], b"", "data"),
                                      ([b"73!\r\n", TimeoutError("still open")], b"", "data"),
                                      ([b"73!\r\n", EOFError("partial")], b"PARTIAL", "data"),
                                      ([b"73!\r\n", EOFError("partial IAC")], b"", "iac"),
                                      ([b"73!\r\n", b"73!\r\n"], b"", "data"),
                                      ([b"73!\n"], b"", "data"),
                                      ([b"X" * (driver.harness.MAX_FRAME + 1) + b"\r\n"], b"", "data")):
            session = mock.Mock()
            session.call, session.suite.log = "N0CALL-1", io.BytesIO()
            session.reader.buffer, session.reader.state = bytearray(pending), state
            session.reader.line.side_effect = lines
            with self.assertRaises((ValueError, TimeoutError)):
                driver.require_remote_logout(session, "BYE")
            session.sock.close.assert_called_once()
            self.assertTrue(session.closed)
            if pending:
                self.assertTrue(session.suite.log.getvalue().endswith(pending))
        session = mock.Mock()
        session.reader.buffer, session.reader.state = bytearray(), "data"
        session.reader.line.side_effect = [b"73!\r\n", EOFError("closed")]
        session.suite.log.write.side_effect = OSError("receipt failure")
        with self.assertRaisesRegex(OSError, "receipt failure"):
            driver.require_remote_logout(session, "BYE")
        session.sock.close.assert_called_once()
        self.assertTrue(session.closed)

    def test_filter_argument_oracle_requires_refusal_and_exact_owners(self):
        before = dict(configuration={"selection": "before"}, revision="revision-1",
                      status=dict(effective={"selection": "before"}, preset={"name": "before"}))
        for fault in (None, "response", "configuration", "revision", "effective", "preset", "disk"):
            with self.subTest(fault=fault):
                session, record = mock.Mock(), mock.Mock()
                session.get.return_value = copy.deepcopy(before)
                after = copy.deepcopy(before)
                response = "Usage: SET/BEACON\nNo changes applied."
                if fault == "response":
                    response = "enabled"
                elif fault in ("configuration", "revision"):
                    after[fault] = "changed"
                elif fault in ("effective", "preset"):
                    after["status"][fault] = "changed"
                record.read_bytes.side_effect = [b"before", b"changed" if fault == "disk" else b"before"]
                with mock.patch.object(driver, "exact_human_reply", return_value=(response, after)):
                    if fault is None:
                        driver.require_filter_argument_refusal(session, "SET/BEACON OFF", record)
                    else:
                        with self.assertRaises(ValueError):
                            driver.require_filter_argument_refusal(session, "SET/BEACON OFF", record)
        session, record = mock.Mock(), mock.Mock()
        session.get.return_value = before
        record.read_bytes.return_value = b"before"
        expected = "Invalid filter command. Type HELP for usage."
        with mock.patch.object(driver, "exact_human_reply", return_value=(expected + "\n", before)):
            driver.require_filter_argument_refusal(session, "SET/FILTER NOFILTER/OFF", record, expected)
        with mock.patch.object(driver, "exact_human_reply", return_value=(expected + " enabled\n", before)):
            with self.assertRaises(ValueError):
                driver.require_filter_argument_refusal(session, "SET/FILTER NOFILTER/OFF", record, expected)

    def test_exact_human_reply_precedes_get_and_preserves_failures(self):
        command, reply = "PASS DXCALL BAD", "First refusal line\nLast refusal line"
        session = mock.Mock()
        session.call, session.suite.log = "N0CALL-1", io.BytesIO()
        raw = (command + "\r\nFirst refusal line\r\nLast refusal line\r\n").encode()
        session.reader.line.side_effect = raw.splitlines(keepends=True)

        document = dict(resource="CONFIG", request_id="End-1", schema_version=4, revision="r", configuration={}, status={})
        session.suite.identifier.return_value = "End-1"
        session.reader.remote_echo = True
        for prefix in (b"", b"hidden row\r\n", b"GET YAML CONFIG SCHEMA 4 ID Other\r\n"):
            session.reader.line.side_effect = raw.splitlines(keepends=True)
            session.suite.log = io.BytesIO()
            def framed(*args):
                self.assertTrue(session.suite.log.getvalue().endswith(raw))
                return document, prefix
            session.frame.side_effect = framed
            if not prefix:
                self.assertEqual(driver.exact_human_reply(session, command, reply), (reply, document))
            else:
                with self.assertRaisesRegex(ValueError, "trailing response content"):
                    driver.exact_human_reply(session, command, reply)
        session.get.assert_not_called()
        session.human.assert_not_called()
        for bad in (b"GET YAML First refusal line\r\n", b"First refusal line EXTRA\r\n",
                    b"First refusal line\n", b"X" * (driver.harness.MAX_FRAME + 1) + b"\r\n"):
            session = mock.Mock()
            session.call, session.suite.log = "N0CALL-1", io.BytesIO()
            session.reader.buffer = bytearray()
            session.reader.line.return_value = bad
            with self.assertRaises(ValueError):
                driver.exact_human_reply(session, command, reply)
            session.get.assert_not_called()
            self.assertIn(bad[:driver.harness.MAX_FRAME], session.suite.log.getvalue())
        for error in (EOFError("partial"), TimeoutError("partial")):
            session = mock.Mock()
            session.call, session.suite.log = "N0CALL-1", io.BytesIO()
            session.reader.buffer = bytearray(b"Last refusal")
            session.reader.line.side_effect = [b"First refusal line\r\n", error]
            with self.assertRaises(type(error)):
                driver.exact_human_reply(session, command, reply)
            session.get.assert_not_called()
            self.assertTrue(session.suite.log.getvalue().endswith(b"First refusal line\r\nLast refusal"))

    def test_archive_get_follows_complete_history_and_preserves_rows(self):
        events = []
        session = mock.Mock()
        session.call = "N0CALL-1"
        raw_log = io.BytesIO()
        session.suite.log = mock.Mock(wraps=raw_log)
        query = "SHOW DX W6ABC 10 COMMENT LABEL"
        row = HISTORY_ROW
        lines = iter([(query + "\r\n").encode(), (row + "\r\n").encode(),
                      b"End of retained history search.\r\n"])
        session.send.side_effect = lambda text: events.append(("send", text))

        def read_line():
            value = next(lines)
            events.append(("line", value))
            return value

        session.reader.line.side_effect = read_line
        expected_receipt = ("\n[N0CALL-1] " + query + "\n" + query + "\r\n" + row +
                            "\r\nEnd of retained history search.\r\n").encode()

        session.suite.identifier.return_value = "End-1"
        session.reader.remote_echo = True
        document = dict(resource="CONFIG", request_id="End-1", schema_version=4, revision="r", configuration={}, status={})
        def framed(*args):
            self.assertEqual(raw_log.getvalue(), expected_receipt)
            session.suite.log.flush.assert_called_once()
            events.append(("frame",))
            return document, b""
        session.frame.side_effect = framed
        self.assertEqual(driver.archive_rows(session, "LABEL"), [row])
        self.assertEqual(events, [("send", query + "\r\n"),
                                  ("line", (query + "\r\n").encode()),
                                  ("line", (row + "\r\n").encode()),
                                  ("line", b"End of retained history search.\r\n"),
                                  ("send", "GET YAML CONFIG SCHEMA 4 ID End-1\r\n"), ("frame",)])
        session.human.assert_not_called()

        for prefix in (b"DX de EXTRA: additional row\r\n", b"\n"):
            session.reader.line.side_effect = [b"No matching retained spots.\r\n"]
            session.frame.side_effect = None
            session.frame.return_value = document, prefix
            with self.assertRaises(ValueError):
                driver.archive_rows(session, "LABEL")

    def test_history_row_oracle_rejects_valid_prefix_corruption(self):
        self.assertEqual(len(HISTORY_ROW), 78)
        driver.require_history_row(HISTORY_ROW)
        bad_rows = (HISTORY_ROW + "GET YAML CONFIG SCHEMA 4 ID Test-99",
                    HISTORY_ROW[:-5] + "GET Y", HISTORY_ROW[:-5] + "2560Z",
                    HISTORY_ROW[:27] + "BAD?  " + HISTORY_ROW[33:],
                    HISTORY_ROW[:63] + "X" + HISTORY_ROW[64:])
        for row in bad_rows:
            with self.assertRaisesRegex(ValueError, "column or UTC-tail"):
                driver.require_history_row(row)
            session = mock.Mock()
            session.reader.buffer = bytearray()
            session.reader.line.return_value = (row + "\r\n").encode()
            with self.assertRaises(ValueError):
                driver.archive_rows(session, "LABEL")
            session.get.assert_not_called()

    def test_archive_refuses_incomplete_interleaved_and_unbounded_responses(self):
        for line in (b"GET YAML CONFIG SCHEMADX de K1ABC: 14074 W6ABC CW LABEL\r\n",
                     b"End of retained history search. EXTRA\r\n",
                     b"Search work limit reached.\r\n", b"unreadable archive\r\n",
                     b"No matching retained spots.\n", b"No matching retained spots.\rX\r\n",
                     b"DX de " + b"A" * driver.harness.MAX_FRAME + b"\r\n"):
            session = mock.Mock()
            session.reader.buffer = bytearray()
            session.call, session.suite.log = "N0CALL-1", io.BytesIO()
            session.reader.line.return_value = line
            with self.assertRaises(ValueError):
                driver.archive_rows(session, "LABEL")
            session.get.assert_not_called()
            receipt = session.suite.log.getvalue()
            self.assertTrue(receipt.startswith(b"\n[N0CALL-1] SHOW DX W6ABC 10 COMMENT LABEL\n"))
            self.assertIn(line[:driver.harness.MAX_FRAME], receipt)
            if len(line) > driver.harness.MAX_FRAME:
                self.assertIn(b"receipt truncated at 65536 bytes", receipt)
        session = mock.Mock()
        session.reader.buffer = bytearray()
        session.call, session.suite.log = "N0CALL-1", io.BytesIO()
        session.reader.line.side_effect = [(HISTORY_ROW + "\r\n").encode(), EOFError("missing footer")]
        with self.assertRaises(EOFError):
            driver.archive_rows(session, "LABEL")
        session.get.assert_not_called()
        self.assertTrue(session.suite.log.getvalue().endswith((HISTORY_ROW + "\r\n").encode()))
        for lines in ([b"End of retained history search.\r\n"],
                      [(HISTORY_ROW + "\r\n").encode(), b"No matching retained spots.\r\n"]):
            session = mock.Mock()
            session.reader.buffer = bytearray()
            session.reader.line.side_effect = lines
            with self.assertRaisesRegex(ValueError, "footer disagrees"):
                driver.archive_rows(session, "LABEL")
            session.get.assert_not_called()

    def test_archive_failure_receipt_preserves_bounded_pending_decoded_bytes(self):
        for error, pending in ((EOFError("partial footer"), b"End of retained hist"),
                               (TimeoutError("partial row"), b"DX de K1ABC: 14074"),
                               (ValueError("receive buffer limit exceeded"),
                                b"X" * (driver.harness.MAX_FRAME + 1))):
            with self.subTest(error=type(error).__name__):
                session = mock.Mock()
                session.call, session.suite.log = "N0CALL-1", io.BytesIO()
                session.reader.buffer = bytearray(pending)
                session.reader.line.side_effect = error
                with self.assertRaises(type(error)):
                    driver.archive_rows(session, "LABEL")
                session.get.assert_not_called()
                self.assertEqual(session.reader.buffer, pending)
                prefix = b"\n[N0CALL-1] SHOW DX W6ABC 10 COMMENT LABEL\n"
                expected = prefix + pending[:driver.harness.MAX_FRAME]
                if len(pending) > driver.harness.MAX_FRAME:
                    expected += b"\n[history receipt truncated at 65536 bytes]\n"
                self.assertEqual(session.suite.log.getvalue(), expected)

    def test_callsign_oracle_requires_all_four_canonical_and_saved_lists(self):
        expected = dict(dx_callsigns=["W6*"], block_dx_callsigns=["N0XYZ"],
                        de_callsigns=["K1*"], block_de_callsigns=["N0XYZ"])
        document = {"configuration": {"filters": copy.deepcopy(expected)}}
        record = mock.Mock()
        record.read_bytes.return_value = b"callsigns: [W6*]\nblock_callsigns: [N0XYZ]\ndecallsigns: [K1*]\nblock_decallsigns: [N0XYZ]\n"
        driver.require_callsign_rules(document, expected, record)
        for key in expected:
            document["configuration"]["filters"][key] = ["W*ABC"]
            with self.assertRaisesRegex(ValueError, "callsign canonical selection"):
                driver.require_callsign_rules(document, expected, record)
            document["configuration"]["filters"][key] = expected[key]
        record.read_bytes.return_value = b"callsigns: [W6*]\nblock_callsigns: [N0XYZ]\ndecallsigns: [K1*]\n"
        with self.assertRaisesRegex(ValueError, "callsign canonical selection"):
            driver.require_callsign_rules(document, expected, record)

    def test_callsign_archive_oracle_requires_identity_visibility_and_exact_row(self):
        session = mock.Mock()
        row = HISTORY_ROW
        with mock.patch.object(driver, "archive_rows", return_value=[row]):
            driver.require_callsign_archive(session, "LABEL", True)
            with self.assertRaisesRegex(ValueError, "blocked callsign"):
                driver.require_callsign_archive(session, "LABEL", False)
        for rows in ([], [row, row], [row.replace("K1ABC", "N0XYZ")], [row.replace("W6ABC", "W6XYZ")]):
            with mock.patch.object(driver, "archive_rows", return_value=rows):
                with self.assertRaisesRegex(ValueError, "visible callsign"):
                    driver.require_callsign_archive(session, "LABEL", True)
        corrupted = row[:39] + "CW LABEL GET YAML".ljust(24) + row[63:]
        driver.require_history_row(corrupted)  # Legal layout cannot replace exact content.
        with mock.patch.object(driver, "archive_rows", return_value=[corrupted]):
            with self.assertRaisesRegex(ValueError, "visible callsign"):
                driver.require_callsign_archive(session, "LABEL", True)

    def test_grid_oracle_requires_configured_effective_and_saved_canonical_values(self):
        document = {"configuration": {"settings": {"grid": "FN31"}},
                    "status": {"effective": {"grid": "FN31"}}}
        record = mock.Mock()
        record.read_bytes.return_value = b"grid: FN31\n"
        driver.require_grid_configuration(document, "FN31", record)
        for owner in (document["configuration"]["settings"], document["status"]["effective"]):
            owner["grid"] = "FN31PRJUNK"
            with self.assertRaisesRegex(ValueError, "GRID canonical selection"):
                driver.require_grid_configuration(document, "FN31", record)
            owner["grid"] = "FN31"
        record.read_bytes.return_value = b"grid: FN31PRJUNK\n"
        with self.assertRaisesRegex(ValueError, "GRID canonical selection"):
            driver.require_grid_configuration(document, "FN31", record)

    def test_literal_archive_labels_avoid_signal_tags_and_clipping(self):
        suite = mock.Mock(token="341C7DBB")
        for suffix in ("F8", "G0P14074", "G0R0F14074", "G1BAND", "G1BARRIER"):
            label = driver.literal_archive_label(suite, suffix)
            self.assertEqual(label, "TDEBMHNLL" + suffix)
            self.assertLessEqual(len(label), 20)
            self.assertNotRegex(label, r"(?i)[-+]?\d{1,3}\s*dB")
        cw = driver.literal_archive_label(suite, "CW")
        ft8 = driver.literal_archive_label(suite, "F8")
        self.assertNotIn(cw, ft8)
        self.assertNotIn(ft8, cw)

    def test_nearby_oracle_requires_seeded_rows_and_unmatched_absence(self):
        row = "DX de N0XYZ:       14021.00  W6ABC       CW NEARBYCONTROL             1234Z"
        driver.require_nearby_archive([row], "NEARBYCONTROL", 14021, True)
        driver.require_nearby_archive([], "NEARBYCONTROL", 14021, False)
        for rows in ([], [row, row], [row.replace("N0XYZ:", "K1ABC:")],
                     [row.replace("14021.00", "3521.00")], [row.replace("NEARBYCONTROL", "OTHER")]):
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, "seeded archive row"):
                driver.require_nearby_archive(rows, "NEARBYCONTROL", 14021, True)
        with self.assertRaisesRegex(ValueError, "unmatched NEARBY control"):
            driver.require_nearby_archive([row], "NEARBYCONTROL", 14021, False)

    def test_mode_oracles_reject_whitelist_and_missing_or_misclassified_controls(self):
        expected = dict(allow_all=True, block_all=False, allow={}, block={})
        document = {"configuration": {"filters": {"modes": copy.deepcopy(expected)}}}
        driver.require_mode_rules(document, expected)
        document["configuration"]["filters"]["modes"]["allow_all"] = False
        with self.assertRaisesRegex(ValueError, "exact open/restrictive state"):
            driver.require_mode_rules(document, expected)
        row = "DX de K1ABC:       14074.00  W6ABC       CW MODECONTROL             1234Z"
        with mock.patch.object(driver, "archive_rows", return_value=[row]):
            driver.require_mode_archive(None, "MODECONTROL", "CW", True)
            with self.assertRaisesRegex(ValueError, "blocked MODE control"):
                driver.require_mode_archive(None, "MODECONTROL", "CW", False)
            with self.assertRaisesRegex(ValueError, "absent or misclassified"):
                driver.require_mode_archive(None, "MODECONTROL", "FT8", True)
        with mock.patch.object(driver, "archive_rows", return_value=[]), \
                self.assertRaisesRegex(ValueError, "absent or misclassified"):
            driver.require_mode_archive(None, "MODECONTROL", "CW", True)

    def test_frequency_refusal_requires_its_specific_feedback(self):
        driver.require_frequency_refusal("Invalid frequency. Use a kHz value like 7001.0.\r\n")
        for text in ("", "Spot queued.", "Spot rejected.", "Invalid input."):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "frequency refusal is missing"):
                driver.require_frequency_refusal(text)

    def test_reputation_refusal_requires_only_generic_feedback(self):
        driver.require_reputation_refusal("\r\nSpot rejected.\r\n")
        for text in ("", "Spot queued.", "Spot queue full.",
                     "Spot rejected. Probation", "Spot rejected.\nASN: AS64500"):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "feedback is not generic"):
                driver.require_reputation_refusal(text)

    def test_reputation_receipt_requires_the_requested_gate_identity(self):
        for requested in (False, True):
            support.require_reputation_identity({"reputation_enabled": requested}, requested)
            for receipt in ({}, {"reputation_enabled": not requested},
                            {"reputation_enabled": int(requested)}, {"reputation_enabled": "true"}):
                with self.subTest(requested=requested, receipt=receipt), \
                        self.assertRaisesRegex(ValueError, "reputation gate identity mismatch"):
                    support.require_reputation_identity(receipt, requested)

    def test_reputation_archive_oracles_require_positive_identity_and_refused_absence(self):
        row = "DX de K1ABC:       14074.00  W6ABC       CW LABEL123             1234Z"
        driver.require_reputation_archive_row([row], "LABEL123", 14074)
        driver.require_archive_absent([])
        for rows in ([], [row, row], [row.replace("K1ABC:", "K1ABC-7:")],
                     [row.replace("14074.00", "7074.00")], [row.replace("LABEL123", "OTHER")]):
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, "baseline-station archive row"):
                driver.require_reputation_archive_row(rows, "LABEL123", 14074)
        with self.assertRaisesRegex(ValueError, "refused spot reached the archive"):
            driver.require_archive_absent([row])

    def test_resource_fixtures_separate_raw_canonical_and_work_refusals(self):
        raw = driver.refused_resource_record("RAW")
        self.assertEqual(len(raw), driver.RECORD_BYTES + 1)
        self.assertEqual(driver.stored_document(raw), {})
        canonical = driver.refused_resource_record("CANONICAL")
        decoded = driver.stored_document(canonical)
        self.assertLess(len(canonical), driver.RECORD_BYTES)
        self.assertEqual(len(decoded["recent_ips"]), 5)
        self.assertTrue(all(len(ip) == 210000 for ip in decoded["recent_ips"]))
        self.assertGreater(sum(map(len, decoded["recent_ips"])), driver.RECORD_BYTES)
        # Scalar work, including physical anchor plus all logical occurrences,
        # is below the separate two-million-unit guard for this byte fixture.
        self.assertLess(6 * 210000 + 1000, 2 << 20)
        work = driver.refused_resource_record("WORK")
        decoded = driver.stored_document(work)
        self.assertEqual(work.count(b"*text"), 70)
        self.assertEqual(len(decoded["recent_ips"]), 70)
        self.assertTrue(all(len(ip) == 32768 for ip in decoded["recent_ips"]))
        self.assertGreater(sum(map(len, decoded["recent_ips"])), 2 << 20)
        self.assertLess(sum(map(len, decoded["recent_ips"][:5])) + 4096, driver.RECORD_BYTES)
        with self.assertRaisesRegex(ValueError, "unknown resource fixture"):
            driver.refused_resource_record("OTHER")

    def test_canonical_padding_changes_only_retained_loopback_metadata(self):
        raw = (b"configuration_version: 4\nrecent_ips:\n    - 127.0.0.1\n"
               b"last_login_utc: 2026-10-08T12:34:56Z\nnoise_class: QUIET\n")
        result = driver.padded_canonical_record(raw)
        self.assertEqual(len(result), driver.RECORD_BYTES - 8)
        before, after = driver.stored_document(raw), driver.stored_document(result)
        self.assertEqual(after["recent_ips"][0], "127.0.0.1")
        self.assertEqual(len(after["recent_ips"]), 2)
        self.assertTrue(after["recent_ips"][1].startswith("METADATA"))
        self.assertEqual(after["recent_ips"][1][len("METADATA"):],
                         "A" * (len(after["recent_ips"][1]) - len("METADATA")))
        after["recent_ips"] = before["recent_ips"]
        self.assertEqual(after, before)
        for invalid in (raw.replace(b"127.0.0.1", b"198.51.100.1"),
                        raw.replace(b"recent_ips:\n    - 127.0.0.1", b"recent_ips: [127.0.0.1]")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                driver.padded_canonical_record(invalid)

    def test_resource_transaction_checker_refuses_each_missing_effect_channel(self):
        before = dict(configuration={"filters": {}}, revision="epoch-1",
                      status=dict(preset=dict(associated=False), effective=dict(grid="FN31"),
                                  session=dict(temporary_defaults=False)))
        record, library = mock.Mock(), mock.Mock()
        record.read_bytes.return_value, library.read_bytes.return_value = b"record", b"library"
        driver.require_transaction_unchanged(before, before, record, b"record", library, b"library")
        for channel in ("configuration", "revision", "preset", "effective", "protection", "record", "library"):
            after = copy.deepcopy(before)
            record.read_bytes.return_value, library.read_bytes.return_value = b"record", b"library"
            if channel == "configuration":
                after["configuration"]["filters"]["comments"] = ["CHANGED"]
            elif channel == "revision":
                after["revision"] = "epoch-2"
            elif channel == "preset":
                after["status"]["preset"]["associated"] = True
            elif channel == "effective":
                after["status"]["effective"]["grid"] = "IO91"
            elif channel == "protection":
                after["status"]["session"]["temporary_defaults"] = True
            elif channel == "record":
                record.read_bytes.return_value = b"changed record"
            else:
                library.read_bytes.return_value = b"changed library"
            with self.subTest(channel=channel), self.assertRaises(ValueError):
                driver.require_transaction_unchanged(before, after, record, b"record", library, b"library")

    @unittest.skipUnless(os.name == "nt", "native Windows replacement behavior")
    def test_record_fault_permits_reads_refuses_replace_and_releases(self):
        with tempfile.TemporaryDirectory(prefix="record-fault-", dir=support.ROOT / ".tmp") as directory:
            target, source = Path(directory) / "record.yaml", Path(directory) / "new.yaml"
            target.write_bytes(b"old record")
            source.write_bytes(b"new record")
            with support.deny_record_replacement(target):
                self.assertEqual(target.read_bytes(), b"old record")
                with self.assertRaises(OSError):
                    os.replace(source, target)
                self.assertEqual(target.read_bytes(), b"old record")
                self.assertEqual(source.read_bytes(), b"new record")
            os.replace(source, target)
            self.assertEqual(target.read_bytes(), b"new record")
            source.write_bytes(b"after failure")
            with self.assertRaisesRegex(ValueError, "test failure"):
                with support.deny_record_replacement(target):
                    raise ValueError("test failure")
            os.replace(source, target)
            self.assertEqual(target.read_bytes(), b"after failure")

    def test_optimized_launch_refuses_before_any_tools(self):
        result = subprocess.run([sys.executable, "-O", str(Path(driver.__file__)), "--race"],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("require assertions", result.stderr)

    def test_pythonoptimize_refuses_before_any_tools(self):
        result = subprocess.run([sys.executable, str(Path(driver.__file__))],
                                env={**os.environ, "PYTHONOPTIMIZE": "1"},
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("require assertions", result.stderr)

    def test_no_tests_missing_duplicate_skip_and_fail_are_not_green(self):
        for results, required in (([], []), ([], ["a"]),
                                  ([{"case": "a", "result": "SKIP"}], ["a"]),
                                  ([{"case": "a", "result": "FAIL"}], ["a"]),
                                  ([{"case": "a", "result": "PASS"}] * 2, ["a"])):
            with self.subTest(results=results), self.assertRaises(ValueError):
                driver.qualified(results, required, [], [])

    def test_wire_and_cleanup_failures_override_semantic_pass(self):
        for wire, cleanup in ((["bad CRLF"], []), ([], ["forced termination"])):
            with self.subTest(wire=wire), self.assertRaises(ValueError):
                driver.qualified([{"case": "a", "result": "PASS"}], ["a"], wire, cleanup)

    def test_success_requires_every_evidence_channel(self):
        driver.qualified([{"case": "a", "result": "PASS"}], ["a"], [], [])

    def test_output_outside_checkout_refused(self):
        with self.assertRaises(ValueError):
            support.plain_output(Path(tempfile.gettempdir()) / "command-fixture-unowned")

    def test_cross_compilation_refused_before_build(self):
        value = dict(GOOS="other", GOHOSTOS="windows", GOARCH="amd64", GOHOSTARCH="amd64", GOROOT="unused")
        with mock.patch.object(support, "command", return_value=json.dumps(value)) as command:
            with self.assertRaisesRegex(ValueError, "native Go target"):
                support.build(False)
            self.assertEqual(command.call_count, 1)

    def test_build_failure_never_runs_stale_binary(self):
        env = dict(GOOS="windows", GOHOSTOS="windows", GOARCH="amd64", GOHOSTARCH="amd64", GOROOT="unused")
        with mock.patch.object(support, "command", side_effect=[json.dumps(env), "dxcluster/telnet", ValueError("build failed")]), \
                mock.patch.object(support, "source_manifest", return_value={"digest": "before"}), \
                mock.patch.dict(os.environ):
            with self.assertRaisesRegex(ValueError, "build failed"):
                support.build(False)

    def test_mutated_sources_fail_after_build(self):
        env = dict(GOOS="windows", GOHOSTOS="windows", GOARCH="amd64", GOHOSTARCH="amd64", GOROOT="unused")
        with mock.patch.object(support, "command", side_effect=[json.dumps(env), "dxcluster/telnet", ""]), \
                mock.patch.object(support, "source_manifest", side_effect=[{"digest": "before"}, {"digest": "after"}]), \
                mock.patch.dict(os.environ):
            with self.assertRaisesRegex(ValueError, "source changed"):
                support.build(False)

    def test_child_launch_failure_closes_log(self):
        log = io.BytesIO()
        with tempfile.TemporaryDirectory(dir=support.ROOT / ".tmp") as directory:
            root = Path(directory)
            (root / "h3").mkdir()
            for asset in ("cty.plist", "h3/res1.bin", "h3/res2.bin"):
                (root / asset).write_bytes(b"fixture")
            # Keep real spec admission, then substitute only the process log.
            original_open = Path.open

            def opened(path, *args, **kwargs):
                if path.name.startswith("child-"):
                    return log
                return original_open(path, *args, **kwargs)

            with mock.patch.object(Path, "open", opened), \
                    mock.patch.object(support.subprocess, "Popen", side_effect=OSError("launch failed")):
                with self.assertRaisesRegex(OSError, "launch failed"):
                    support.Child(Path("missing.exe"), root, {"digest": "source"})
        self.assertTrue(log.closed)

    def test_failed_child_readiness_retires_process(self):
        with tempfile.TemporaryDirectory(dir=support.ROOT / ".tmp") as directory:
            root = Path(directory)
            (root / "h3").mkdir()
            for asset in ("cty.plist", "h3/res1.bin", "h3/res2.bin"):
                (root / asset).write_bytes(b"readiness-fixture")
            # Python is deliberately not a Go test binary: these flags fail.
            with self.assertRaisesRegex(ValueError, "fixture exited early"):
                support.Child(Path(sys.executable), root, {"digest": "source"})

    def test_readiness_sharing_retry_is_bounded(self):
        for fault in (None, "deadline", "assets"):
            expires = fault == "deadline"
            with self.subTest(fault=fault), tempfile.TemporaryDirectory(dir=support.ROOT / ".tmp") as directory:
                root = Path(directory)
                (root / "config").mkdir()
                process = mock.Mock(pid=123)
                process.poll.return_value = None
                receipt = json.dumps(dict(nonce="fixed", pid=123, started=True,
                                          binary_hash="binary", source_digest="source",
                                          config_hashes={}, address="127.0.0.1:12345",
                                          reputation_enabled=False, dedupe_available=dict(FAST=True, MED=True, SLOW=True),
                                          solar_enabled=True))
                reads = [PermissionError("sharing publication"), receipt]
                with mock.patch.object(support.uuid, "uuid4", return_value=mock.Mock(hex="fixed")), \
                        mock.patch.object(support.subprocess, "Popen", return_value=process), \
                        mock.patch.object(support, "digest", return_value="binary", side_effect=["binary"] * 4 + ["changed", "binary", "binary"] if fault == "assets" else None), \
                        mock.patch.object(Path, "read_text", side_effect=reads) as reading, \
                        mock.patch.object(support.time, "monotonic", side_effect=([0, 0, 46] if expires else [0, 0, 1])), \
                        mock.patch.object(support.time, "sleep"):
                    if expires:
                        with self.assertRaisesRegex(ValueError, "readiness deadline expired"):
                            support.Child(Path("fixture.exe"), root, {"digest": "source"})
                        process.kill.assert_called_once()
                        self.assertEqual(reading.call_count, 1)
                    elif fault == "assets":
                        with self.assertRaisesRegex(ValueError, "assets changed during startup"):
                            support.Child(Path("fixture.exe"), root, {"digest": "source"})
                        process.kill.assert_called_once()
                    else:
                        child = support.Child(Path("fixture.exe"), root, {"digest": "source"})
                        self.assertEqual(child.port, 12345)
                        self.assertEqual(reading.call_count, 2)
                        child.abort()

    def test_direct_cleanup_exception_still_retires_and_closes_job(self):
        for failing_method in ("kill", "wait"):
            process = mock.Mock()
            process.poll.return_value = None
            getattr(process, failing_method).side_effect = OSError("direct cleanup failed")
            job = mock.Mock(name="owned job")
            with self.subTest(method=failing_method), \
                    mock.patch.object(process_support, "ToolJob", return_value=job), \
                    mock.patch.object(support.subprocess, "Popen", return_value=process):
                with self.assertRaisesRegex(OSError, "direct cleanup failed"):
                    support.command(["unused"], timeout=0)
            job.join.assert_called_once_with(terminate=True)
            job.close.assert_called_once_with()

    def test_unsupported_host_refused_before_tool_launch(self):
        with mock.patch.object(support.os, "name", "posix"), \
                mock.patch.object(support.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "requires native Windows"):
                support.command(["unused"])
            launch.assert_not_called()

    def test_timeout_retires_descendant_before_lock_reacquisition(self):
        self.retire_descendant(overflow=False)

    def test_output_overflow_retires_descendant_before_lock_reacquisition(self):
        self.retire_descendant(overflow=True)

    def retire_descendant(self, overflow):
        with tempfile.TemporaryDirectory(dir=support.ROOT / ".tmp") as directory:
            root = Path(directory)
            started, delayed = root / "started", root / "delayed"
            child = ("import pathlib,time; " +
                     f"pathlib.Path({str(started)!r}).write_text('ready'); time.sleep(4); " +
                     f"pathlib.Path({str(delayed)!r}).write_text('orphan survived')")
            parent = ("import pathlib,subprocess,sys,time; " +
                      f"subprocess.Popen([sys.executable,'-c',{child!r}]); " +
                      f"\nwhile not pathlib.Path({str(started)!r}).exists(): time.sleep(.01)\n")
            parent += ("for i in range(1000):\n print('x'*4096,flush=True); time.sleep(.02)\n" if overflow
                       else "time.sleep(30)\n")
            with support.runner_lock(), mock.patch.object(support, "MAX_LOG", 8192):
                with self.assertRaisesRegex(ValueError, "output exceeds" if overflow else "timed out"):
                    support.command([sys.executable, "-c", parent], timeout=2)
            self.assertTrue(started.exists(), "cancellation control never started descendant")
            # The entire process tree must already be retired when this succeeds.
            with support.runner_lock():
                time.sleep(4.1)
                self.assertFalse(delayed.exists(), "descendant survived cancellation and lock release")

    @unittest.skipUnless(os.name == "nt", "native Windows lock interoperability")
    def test_python_lock_excludes_powershell_runner_before_build(self):
        with support.runner_lock():
            result = subprocess.run(["pwsh", "-NoProfile", "-File", str(support.ROOT / "scripts" / "test-windows.ps1"),
                                     "-Packages", "./telnet", "-PrepareOnly"], capture_output=True, text=True, timeout=15)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Cannot acquire Windows test runner lock", result.stderr)
        self.assertNotIn("BUILD dxcluster", result.stdout)

    @unittest.skipUnless(os.name == "nt", "native Windows lock interoperability")
    def test_powershell_exclusive_handle_excludes_python(self):
        lock = support.OUTPUT / "runner.lock"
        ready = support.OUTPUT / ("lock-ready-" + str(os.getpid()))
        # All interpolated paths are repository-owned and quoted as PS literals.
        quote = lambda path: "'" + str(path).replace("'", "''") + "'"
        script = (f"$lock=[IO.File]::Open({quote(lock)},'OpenOrCreate','ReadWrite','None'); "
                  f"try {{[IO.File]::WriteAllText({quote(ready)},'READY'); [Console]::ReadLine() | Out-Null}} "
                  "finally {$lock.Dispose()}")
        child = subprocess.Popen(["pwsh", "-NoProfile", "-Command", script], stdin=subprocess.PIPE,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 10
            while not ready.exists():
                self.assertIsNone(child.poll())
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.025)
            with self.assertRaises(OSError):
                with support.runner_lock():
                    self.fail("overlapping lock admitted")
        finally:
            child.communicate(b"STOP\n", timeout=10)
            ready.unlink(missing_ok=True)
        self.assertEqual(child.returncode, 0)


if __name__ == "__main__":
    unittest.main()
