"""Offline behavioral fixtures for the live telnet command harness.

No fixture opens a network connection. The fake socket retains stream boundaries
only as recv chunks, allowing the reader to prove its framing and IAC handling.
"""

import copy
import importlib.util
import io
import re
from pathlib import Path
import socket
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch


HARNESS_PATH = Path(__file__).with_name("test-telnet-commands.py")
SPEC = importlib.util.spec_from_file_location("telnet_command_harness", HARNESS_PATH)
HARNESS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = HARNESS
SPEC.loader.exec_module(HARNESS)


class FakeSocket:
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.sent = []
        self.timeouts = []
        self.events = []

    def recv(self, size):
        if not self.chunks:
            self.events.append("remote EOF")
            return b""
        item = self.chunks.pop(0)
        if isinstance(item, BaseException):
            raise item
        if len(item) > size:
            self.chunks.insert(0, item[size:])
            return item[:size]
        return item

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def sendall(self, data):
        self.sent.append(data)

    def close(self):
        self.events.append("local close")


def reader(chunks, deadline=None):
    sock = FakeSocket(chunks)
    value = HARNESS.TelnetReader(sock, deadline or time.monotonic() + 30, timeout=1)
    return value, sock


def frame(body=None, newline=b"\r\n"):
    if body is None:
        body = (
            'schema_version: 3\nrequest_id: "Case-1"\nresource: CONFIG\n'
            'revision: "revision-a"\nconfiguration: {}\nstatus: {}\n'
        )
    return newline.join([b"---", *body.encode("ascii").rstrip(b"\n").split(b"\n"), b"...", b""])


class ReaderFixtures(unittest.TestCase):
    def test_split_lines_keep_raw_line_endings_and_buffered_tail(self):
        value, sock = reader([b"fir", b"st\r", b"\nsecond\r\r\nthird\n"])
        self.assertEqual(value.line(), b"first\r\n")
        self.assertEqual(value.line(), b"second\r\r\n")
        self.assertEqual(value.line(), b"third\n")
        self.assertTrue(sock.timeouts)
        self.assertTrue(all(0 < timeout <= 1 for timeout in sock.timeouts))

    def test_split_login_marker_leaves_next_line_available(self):
        value, _ = reader([b"Welcome\r\nLog", b"in:", b" ready\r\n"])
        self.assertEqual(value.until(b"Login:"), b"Welcome\r\nLogin:")
        self.assertEqual(value.line(), b" ready\r\n")

    def test_iac_negotiation_arbitrary_byte_splits(self):
        raw = bytes([255, 251, 1, 255, 251, 3, 255, 251, 42, 255, 253, 1]) + b"ok\r\n"
        value, sock = reader([bytes([byte]) for byte in raw])
        self.assertEqual(value.line(), b"ok\r\n")
        self.assertEqual(b"".join(sock.sent), bytes([255, 253, 1, 255, 253, 3, 255, 254, 42, 255, 252, 1]))

    def test_escaped_iac_preserved_subnegotiation_discarded(self):
        raw = b"a" + bytes([255, 255, 255, 250, 24, 0, 255, 255, 1, 255, 240]) + b"b\r\n"
        value, _ = reader([bytes([byte]) for byte in raw])
        self.assertEqual(value.line(), b"a\xffb\r\n")

    def test_interleaved_spot_prefix_and_split_yaml_markers(self):
        payload = b"DX de K1ABC: 14030 W1XYZ CW\r\n" + frame() + b"late spot\r\n"
        value, _ = reader([payload[index:index + 2] for index in range(0, len(payload), 2)])
        self.assertTrue(value.line().startswith(b"DX de "))
        lines = []
        while True:
            line = value.line()
            lines.append(line)
            if line == b"...\r\n":
                break
        document, errors = HARNESS.decode_frame(b"".join(lines), "CONFIG", "Case-1", 3)
        self.assertEqual(document["revision"], "revision-a")
        self.assertEqual(errors, [])
        self.assertEqual(value.line(), b"late spot\r\n")

    def test_eof_truncated_line_and_iac_are_errors(self):
        for chunks in ([b""], [b"partial"], [bytes([255])], [bytes([255, 251])], [bytes([255, 250, 1])]):
            with self.subTest(chunks=chunks):
                value, _ = reader(chunks)
                with self.assertRaises((EOFError, ConnectionError, RuntimeError, ValueError)):
                    value.line()

    def test_socket_timeout_and_expired_run_deadline(self):
        value, _ = reader([socket.timeout("fixture timeout")], deadline=11)
        with patch.object(HARNESS.time, "monotonic", side_effect=[10, 12]):
            with self.assertRaises((TimeoutError, RuntimeError)):
                value.line()
        value, sock = reader([b"unreachable\n"], time.monotonic() - 1)
        with self.assertRaises((TimeoutError, RuntimeError)):
            value.line()
        self.assertEqual(sock.chunks, [b"unreachable\n"])

    def test_connection_buffer_has_hard_limit(self):
        value, _ = reader([b"x" * 262145])
        with self.assertRaises((ValueError, RuntimeError, BufferError)):
            value.line()


class FrameFixtures(unittest.TestCase):
    def test_schema_seven_keeps_both_independent_banks_and_identity(self):
        body = ('schema_version: 7\nrequest_id: Case-1\nresource: CONFIG\n'
                'revision: rev-7\nconfiguration:\n  filters:\n'
                '    go: {rules: [{slot: 1, action: pass, expr: "MODE=CW"}]}\n'
                '    cc: {modes: [FT8], sources: [HUMAN]}\n'
                '  settings: {dialect: cc}\nstatus: {}\n')
        raw = frame(body)
        doc, errors = HARNESS.decode_frame(raw, "CONFIG", "Case-1", 7)
        self.assertEqual(errors, [])
        self.assertEqual(doc["configuration"]["filters"]["go"]["rules"][0]["expr"], "MODE=CW")
        self.assertEqual(doc["configuration"]["filters"]["cc"]["modes"], ["FT8"])
        for malformed in (raw[:-5], raw.replace(b"request_id: Case-1", b"request_id: Case-2"),
                          raw.replace(b"schema_version: 7", b"schema_version: 6")):
            with self.subTest(raw=malformed[:80]), self.assertRaises(ValueError):
                HARNESS.decode_frame(malformed, "CONFIG", "Case-1", 7)

    def test_schema_seven_large_frame_limit_is_distinct_from_schema_six(self):
        def body(version, size):
            return (f'schema_version: {version}\nrequest_id: Case-1\nresource: CONFIG\n'
                    'configuration:\n  payload: "' + "x" * size + '"\nstatus: {}\n')
        _, errors = HARNESS.decode_frame(frame(body(7, 70000)), "CONFIG", "Case-1", 7)
        self.assertEqual(errors, [], "schema 7 permits configuration frames above 64 KiB")
        _, errors = HARNESS.decode_frame(frame(body(6, 70000)), "CONFIG", "Case-1", 6)
        self.assertTrue(errors, "schema 6 retains its 64 KiB bound")
        _, errors = HARNESS.decode_frame(frame(body(7, 262144)), "CONFIG", "Case-1", 7)
        self.assertTrue(errors, "schema 7 frames above 256 KiB must fail")

    def test_schema_four_preserves_literal_comment_collections(self):
        body = ('schema_version: 4\nrequest_id: Case-1\nresource: CONFIG\n'
                'configuration:\n  filters:\n'
                '    comments: ["up  5: please!", "POTA", "pota"]\n'
                '    block_comments: ["POTA"]\nstatus: {}\n')
        document, errors = HARNESS.decode_frame(frame(body), "CONFIG", "Case-1", 4)
        self.assertEqual(errors, [])
        self.assertEqual(document["configuration"]["filters"]["comments"],
                         ["up  5: please!", "POTA", "pota"])
        self.assertEqual(document["configuration"]["filters"]["block_comments"], ["POTA"])

    def test_valid_success_and_error_common_envelopes(self):
        document, errors = HARNESS.decode_frame(frame(), "CONFIG", "Case-1", 3)
        self.assertEqual(document["configuration"], {})
        self.assertEqual(errors, [])
        body = 'schema_version: 3\nrequest_id: Case-1\nresource: CONFIG\nerror:\n  code: conflict\n  message: retry\n'
        document, errors = HARNESS.decode_frame(frame(body), "CONFIG", "Case-1", 3)
        self.assertEqual(document["error"]["code"], "conflict")
        self.assertEqual(errors, [])

    def test_mismatched_envelope_is_rejected(self):
        for resource, request_id, version in (("FILTER", "Case-1", 3), ("CONFIG", "case-1", 3), ("CONFIG", "Case-1", 2)):
            with self.subTest(resource=resource, request_id=request_id, version=version):
                with self.assertRaises((ValueError, RuntimeError, AssertionError)):
                    HARNESS.decode_frame(frame(), resource, request_id, version)
        for scalar in ("true", '"1"'):
            with self.subTest(schema=scalar), self.assertRaisesRegex(ValueError, "schema_version"):
                HARNESS.decode_frame(frame(f"schema_version: {scalar}\nrequest_id: Case-1\nresource: CONFIG\n"), "CONFIG", "Case-1", 1)

    def test_duplicate_top_level_and_nested_yaml_keys_rejected(self):
        for body in (
            'schema_version: 3\nrequest_id: Case-1\nresource: CONFIG\nresource: CONFIG\n',
            'schema_version: 3\nrequest_id: Case-1\nresource: CONFIG\nconfiguration:\n  grid: FN20\n  grid: FN21\n',
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValueError):
                    HARNESS.decode_frame(frame(body), "CONFIG", "Case-1", 3)

    def test_incomplete_nonstandalone_extra_documents_and_prefix_rejected(self):
        valid = frame()
        for invalid in (valid[:-5], valid.replace(b"---\r\n", b"--- bad\r\n", 1), valid.replace(b"...\r\n", b"... bad\r\n"), valid + valid, b"spot\r\n" + valid, frame("- not\n- envelope\n")):
            with self.subTest(invalid=invalid[:30]):
                with self.assertRaises((ValueError, RuntimeError, AssertionError)):
                    HARNESS.decode_frame(invalid, "CONFIG", "Case-1", 3)

    def test_wire_defects_are_recorded_even_if_semantics_continue(self):
        for newline in (b"\n", b"\r\r\n"):
            with self.subTest(newline=newline):
                document, errors = HARNESS.decode_frame(frame(newline=newline), "CONFIG", "Case-1", 3)
                self.assertEqual(document["resource"], "CONFIG")
                self.assertTrue(errors, "wire anomaly must not become a green semantic result")

    def test_oversize_frame_never_has_clean_wire_result(self):
        body = 'schema_version: 3\nrequest_id: Case-1\nresource: CONFIG\nvalue: "' + "x" * 65536 + '"\n'
        try:
            _, errors = HARNESS.decode_frame(frame(body), "CONFIG", "Case-1", 3)
        except (ValueError, RuntimeError):
            return
        self.assertTrue(errors)


class HumanEchoFixtures(unittest.TestCase):
    def test_cleanup_pause_drains_queued_rows_before_strict_logout_and_closes_on_failure(self):
        for fault in (None, "pause", "log"):
            with self.subTest(fault=fault):
                value = object.__new__(HARNESS.Session)
                value.call, value.closed = "FIXTURE", False
                value.suite = SimpleNamespace(identifier=lambda: "Close-1", commands=0, log=io.BytesIO(),
                                              wire_failures=[], deadline=time.monotonic() + 10, args=SimpleNamespace(timeout=1))
                body = "schema_version: 4\nrequest_id: Close-1\nresource: CAPABILITIES\nrevision: r\nconfiguration: {}\nstatus:\n  session:\n    pause_active: " + ("false" if fault == "pause" else "true") + "\n"
                value.reader, value.sock = reader([b"DX de K1ABC: 14179.00 W6ABC CW|CMT\r\n" + frame(body), b"73!\r\n"])
                if fault == "log":
                    def failed_receipt(raw):
                        raise OSError("receipt failed")
                    value.suite.log = SimpleNamespace(write=failed_receipt, flush=lambda: None)
                if fault:
                    with self.assertRaises((ValueError, OSError)):
                        value.close()
                    self.assertEqual(len(value.sock.sent), 1, "failed pause must not start logout")
                else:
                    value.close()
                    self.assertEqual(value.sock.sent, [b"PAUSE 300\r\nGET YAML CAPABILITIES SCHEMA 4 ID Close-1\r\n", b"BYE\r\n"])
                    self.assertEqual(value.sock.events, ["remote EOF", "local close"])
                    self.assertIn(b"CW|CMT\r\n", value.suite.log.getvalue())
                self.assertTrue(value.closed)
                self.assertEqual(value.sock.events[-1], "local close")

    def session(self, prefix, remote_echo=True):
        value = object.__new__(HARNESS.Session)
        value.suite = SimpleNamespace(identifier=lambda: "Echo-1", commands=0)
        value.reader = SimpleNamespace(remote_echo=remote_echo)
        value.send = lambda data: None
        value.frame = lambda *args: ({"configuration": {}, "status": {}, "revision": "fixed"}, prefix)
        return value

    def test_exact_response_equal_to_command_survives_single_echo_removal(self):
        command = "PASS DXSTATE ALL"
        prefix = (command + "\r\n" + command + "\r\nGET YAML CONFIG SCHEMA 4 ID Echo-1\r\n").encode()
        text, _ = self.session(prefix).human(command, command)
        self.assertEqual(text, command)
        for field, bad in (("configuration", []), ("status", []), ("revision", True), ("revision", "")):
            value = self.session(prefix)
            document = {"configuration": {}, "status": {}, "revision": "fixed", field: bad}
            value.frame = lambda *args: (document, prefix)
            with self.subTest(field=field, value=bad), self.assertRaises(AssertionError):
                value.human(command, command)

    def test_echo_only_never_satisfies_expected_response(self):
        command = "PASS DXSTATE ALL"
        prefix = (command + "\r\nGET YAML CONFIG SCHEMA 4 ID Echo-1\r\n").encode()
        with self.assertRaises(AssertionError):
            self.session(prefix).human(command, command)

    def test_no_echo_negotiation_preserves_response_equal_to_command(self):
        command = "PASS DXSTATE ALL"
        text, _ = self.session((command + "\r\n").encode(), remote_echo=False).human(command, command)
        self.assertEqual(text, command)

    def test_human_wire_anomaly_remains_failure_before_normalization(self):
        command = "HELP"
        response = b"User          FIXTURE\r\r\nBands\r\r\n  PASS: ALL\r\r\nMissed spots are not replayed.\r\r\n"
        value = self.session(response, remote_echo=False)
        value.call = "FIXTURE"
        value.suite.wire_failures = []
        value.human(command, "PASS: ALL")
        self.assertTrue(value.suite.wire_failures)


class QualificationAccountingFixtures(unittest.TestCase):
    def test_cc_shortcut_cli_normalizes_and_rejects_bad_inputs_before_suite(self):
        base = ["test-telnet-commands.py", "--host", "127.0.0.1", "--call", "LZ1BB",
                "--ssid-start", "1", "--output", "unused-fixture-output"]
        for raw in ("CW,CW", "cw,CW", "CW,,FT8", "NOFT8", "SKIMMER", "bad-name", ""):
            with self.subTest(raw=raw), patch.object(sys, "argv", base + ["--cc-shortcuts", raw]), \
                    patch.object(HARNESS, "V7Suite") as suite, patch("sys.stderr", new_callable=io.StringIO):
                with self.assertRaises(SystemExit):
                    HARNESS.main()
                suite.assert_not_called()
        with patch.object(sys, "argv", base + ["--cc-shortcuts", "cw, ft8"]), patch.object(HARNESS, "V7Suite") as suite:
            HARNESS.main()
            self.assertEqual(suite.call_args.args[0].cc_shortcuts, ["CW", "FT8"])
        required = HARNESS.v7_required_cases(["CW", "FT8"])
        self.assertEqual(required.count("CC configured shortcut CW"), 1)
        self.assertEqual(required.count("CC configured shortcut FT8"), 1)
        self.assertEqual(len(required), len(set(required)))

    def test_cc_shortcuts_detect_noop_and_unrelated_mutation(self):
        for defect in (None, "noop", "unrelated"):
            with self.subTest(defect=defect):
                value = object.__new__(HARNESS.V7Suite)
                value.args = SimpleNamespace(cc_shortcuts=["CW", "FT8"])
                value.results = []
                state = {"filters": {"cc": {"modes": []}}, "settings": {"grid": "FN31"}}
                commands = []

                def human(command):
                    if command == "DIALECT CC":
                        return "", {"configuration": copy.deepcopy(state)}
                    commands.append(command)
                    mode = command.split("/", 1)[1]
                    enabled = command.startswith("SET/") and not mode.startswith("NO")
                    if mode.startswith("NO"):
                        mode = mode[2:]
                    target_operation = len(commands) % 2 == 0
                    if not (defect == "noop" and target_operation):
                        modes = set(state["filters"]["cc"]["modes"])
                        modes.add(mode) if enabled else modes.discard(mode)
                        state["filters"]["cc"]["modes"] = sorted(modes)
                    if defect == "unrelated" and target_operation:
                        state["settings"]["grid"] = "FN32" if state["settings"]["grid"] == "FN31" else "FN31"
                    return "", {"configuration": copy.deepcopy(state)}

                with patch("builtins.print"):
                    value._configured_shortcuts(SimpleNamespace(human=human))
                self.assertEqual([r["case"] for r in value.results],
                                 ["CC configured shortcut CW", "CC configured shortcut FT8"])
                self.assertEqual([r["result"] for r in value.results],
                                 ["PASS", "PASS"] if defect is None else ["FAIL", "FAIL"])

    def test_family_aggregates_failed_and_unexpected_children_and_requires_every_leaf(self):
        for fault in (None, "failure", "exception", "missing", "duplicate", "blocked"):
            value = object.__new__(HARNESS.Suite)
            value.results = []
            def family():
                def operation():
                    if fault == "failure":
                        raise AssertionError("wrong output")
                    if fault == "exception":
                        raise KeyError("unexpected")
                    if fault == "blocked":
                        raise HARNESS.BlockedPrerequisite("not admitted")
                if fault != "missing":
                    value.case("leaf", operation)
                if fault == "duplicate":
                    value.case("leaf", lambda: None)
            value.case("family", family, ["leaf"])
            value.account_required(["leaf"])
            self.assertEqual(value.results[-1]["result"] == "PASS", fault is None)
            if fault == "missing":
                self.assertEqual(value.results[-1]["result"], "UNEXECUTED")

    def test_independent_inventory_matches_actual_family_leaf_selection(self):
        value = object.__new__(HARNESS.Suite)
        value.token, value.args, value.deadline = "ABCDEF01", SimpleNamespace(call="K1ABC", timeout=1), time.monotonic() + 10
        selected = []
        value.case = lambda name, operation: selected.append(name) is None
        text = "DX de K1ABC: 28201 VA3UXA TABCDEF01-0\nDX de K1ABC: 21201 VA3UXA TABCDEF01-1\nH1" + "A" * 32 + " Older retained history page:"
        configuration = {"configuration": {"settings": {"dialect": "go"}}, "revision": "fixed"}
        def human(command, expected=None):
            if command.startswith("DIALECT "):
                configuration["configuration"]["settings"]["dialect"] = command.split()[1].lower()
            return text, configuration
        session = SimpleNamespace(human=human, get=lambda: configuration)
        inventory = HARNESS.required_live_cases(value.token, value.args.call)
        for family, execute in (("human commands and filters", lambda: value.reads_and_filters(session)),
                                ("pause and machine commands", lambda: value.pause_and_machine(session)),
                                ("history and read commands", lambda: value.history(session)),
                                ("terminal inputs", lambda: value.terminal_inputs("unused"))):
            selected.clear()
            with patch.object(value, "history_result", return_value=text), patch.object(value, "unchanged"):
                execute()
            self.assertEqual(set(selected), set(inventory[family]), family)
            self.assertEqual(len(selected), len(inventory[family]), family)

    def test_frame_failure_retains_bounded_raw_partial_receipt(self):
        for fault in ("eof", "deadline", "buffer"):
            value = object.__new__(HARNESS.Session)
            value.call = "FIXTURE"
            value.suite = SimpleNamespace(log=io.BytesIO(), wire_failures=[])
            raw = b"---\r\npartial" if fault != "buffer" else b"X" * (HARNESS.MAX_BUFFER + 1)
            value.reader, _ = reader([raw], deadline=10)
            with patch.object(HARNESS.time, "monotonic", side_effect=[1, 11] if fault == "deadline" else lambda: 1):
                with self.assertRaises((EOFError, TimeoutError, ValueError)):
                    value.frame("CONFIG", "fixed", 4, "GET")
            receipt = value.suite.log.getvalue()
            self.assertIn(raw[:HARNESS.MAX_BUFFER], receipt)
            self.assertLessEqual(len(receipt), HARNESS.MAX_BUFFER + 100)


class SyntheticHistorySession:
    """Literal fixture answers challenge the collector without a second parser.

    The two distinct identities are 10m/CW and 15m/FT8. Defect variants change
    one observable answer so a permissive checker cannot silently stay green.
    This fixture validates harness assertions, not server history behavior.
    """
    selections = {
        "BAND 10m": [0], "BAND 15": [1], "MODE cw": [0], "MODE FT8": [1],
        "BAND 10m,15m": [0, 1], "BAND 10, 15": [0, 1],
        "MODE CW,FT8": [0, 1], "MODE CW, FT8": [0, 1],
        "BAND 10m,10 MODE CW,CW": [0], "BAND 10,15 MODE CW": [0],
        "MODE CW, FT8 BAND 15m": [1], "BAND 10 MODE FT8": [],
        "MODE UNKNOWN": [], "BAND 10 MODE CW": [0],
        "BAND 10,15 MODE CW, FT8": [0, 1],
        "MODE CW, FT8 BAND 10,15": [0, 1], "BAND 10M": [], "MODE CW": [],
    }
    invalid = {
        "BAND": "BAND", "MODE": "MODE", "BAND MODE CW": "BAND", "MODE BAND 10": "MODE",
        "BAND ,": "BAND", "MODE ,": "MODE", "BAND 10 15": "BAND", "MODE CW FT8": "MODE",
        "BAND 10,15 MODE CW FT8": "MODE", "MODE CW,FT8 BAND 10 15": "BAND",
        "BAND 10 INVALID": "BAND", "MODE CW INVALID": "MODE",
        "BAND 10,INVALID": "BAND", "MODE CW,INVALID": "MODE",
        "BAND ALL": "BAND", "BAND NONE": "BAND", "BAND UNKNOWN": "BAND",
        "MODE ALL": "MODE", "MODE NONE": "MODE", "BAND 10 BAND 15": "BAND",
        "MODE CW MODE FT8": "MODE", "BAND 10 MODE CW BAND 15": "BAND",
    }

    def __init__(self, defect=None):
        self.defect, self.dialect, self.cursor = defect, "go", False
        self.configuration = {"blocked_band": False, "blocked_mode": False, "marker": 0}
        self.revision, self.commands = 0, []

    def get(self):
        return {"configuration": dict(self.configuration), "revision": str(self.revision)}

    def rows(self, indices):
        lines = [f"DX de W1AW: {28200 if index == 0 else 21200}.0 K1ABC TABCDEF01-{index} up-5?" for index in indices]
        return "\n".join(lines) if lines else "No matching retained spots."

    def human(self, command, expected=None):
        self.commands.append(command)
        if command == "PASS NOFILTER":
            self.configuration.update(blocked_band=False, blocked_mode=False)
            self.revision += 1
            text = "Filters reset"
        elif command in ("REJECT BAND 10M", "REJECT MODE CW"):
            self.configuration["blocked_band" if "BAND" in command else "blocked_mode"] = True
            self.revision += 1
            text = command
        elif command.startswith(("SHOW/DX", "SH/DX")) and self.dialect == "go":
            text = self.rows([0, 1]) if self.defect == "dialect_bypass" else "Use SHOW DX or SH DX for DX history."
        elif " NEXT " in command:
            if not self.cursor or self.defect == "invalid_clears_cursor":
                text = "Invalid history continuation."
            else:
                self.cursor = False
                text = "Older retained history page:\n" + self.rows([1] if self.defect == "next_forgets_selection" else [0])
        else:
            _, args = command.split(" K1ABC ", 1)
            count, suffix = args.split(" ", 1)
            if suffix in self.invalid:
                text = "No matching retained spots." if self.defect == "accept_invalid" else "Invalid " + self.invalid[suffix] + " selection."
            else:
                selection, phrase = suffix.split(" COMMENT ", 1)
                indices = list(self.selections[selection])
                if phrase == "TABCDEF01-0 up-5?":
                    indices = [0]
                elif phrase == "TABCDEF01-0 up-5!":
                    indices = [0] if self.defect == "trim_punctuation" else []
                elif phrase not in ("tabcdef01", "TABCDEF01"):
                    raise AssertionError("unexpected synthetic phrase: " + phrase)
                if (selection, self.defect) in (("BAND 10m", "ignore_band"), ("MODE cw", "ignore_mode"), ("BAND 10 MODE FT8", "categories_union")):
                    indices = [0, 1]
                if self.defect != "ignore_saved" and (self.configuration["blocked_band"] or self.configuration["blocked_mode"]):
                    indices = [index for index in indices if index != 0]
                if count == "1":
                    assert indices == [0, 1]
                    self.cursor = True
                    indices = [1]
                text = self.rows(indices)
                if count == "1":
                    text += "\nContinue older history: SHOW DX NEXT H1" + "A" * 32
                if self.defect == "mutate_preferences":
                    self.configuration["marker"] += 1
                if self.defect == "wrong_frequency":
                    text = text.replace("28200.0", "14030.0")
        if expected is not None:
            assert expected in text, f"{command}: missing {expected!r}: {text}"
        return text, self.get()


class HistoryOracleFixtures(unittest.TestCase):
    def suite(self):
        value = object.__new__(HARNESS.Suite)
        value.token, value.results = "ABCDEF01", []
        return value

    def test_v7_history_waits_past_command_timeout_and_requires_exact_rows(self):
        for defect in (None, "wrong_frequency", "never_arrives", "unrelated_row", "inactive_pause", "pause_expires"):
            with self.subTest(defect=defect):
                clock = [0.0]
                value = object.__new__(HARNESS.V7Suite)
                value.token = "ABCDEF01"
                value.args = SimpleNamespace(timeout=1, history_wait_seconds=0, history_timeout_seconds=4)
                value.started_at, value.deadline = 0, 10
                document = {"configuration": {}, "revision": "fixture", "status": {"session": {"pause_active": True}}}
                queries = []

                def human(command, expected=None):
                    if command == "PAUSE 300" and defect == "inactive_pause":
                        return "Live spots paused.", {"status": {"session": {"pause_active": False}}}
                    if command.startswith("DX "):
                        return "Spot queued.", document
                    if command.startswith("SHOW DX"):
                        queries.append(clock[0])
                        rows = f"DX de W1AW: 21011.0 VA3UXA {value.history_label()}-1\n"
                        if clock[0] >= 2 and defect != "never_arrives":
                            frequency = 14011 if defect == "wrong_frequency" else 28011
                            rows += f"DX de W1AW: {frequency}.0 VA3UXA {value.history_label()}-0\n"
                        if defect == "unrelated_row" and clock[0] >= 2:
                            rows += "DX de K1ABC: 14074.0 W1AW unexpected\n"
                        rows += "End of retained history search.\n"
                        if defect == "pause_expires":
                            return rows, {"status": {"session": {"pause_active": False}}}
                        return rows, document
                    return "", document

                session = SimpleNamespace(human=human, get=lambda: document)
                def sleep(seconds):
                    clock[0] += seconds
                with patch.object(HARNESS.time, "monotonic", side_effect=lambda: clock[0]), patch.object(HARNESS.time, "sleep", side_effect=sleep):
                    if defect in ("wrong_frequency", "unrelated_row"):
                        with self.assertRaises(AssertionError):
                            value._history(session)
                    elif defect in ("inactive_pause", "pause_expires"):
                        with self.assertRaisesRegex(ValueError, "history delivery hold"):
                            value._history(session)
                    elif defect == "never_arrives":
                        with self.assertRaises(HARNESS.BlockedPrerequisite):
                            value._history(session)
                        self.assertLessEqual(clock[0], 4)
                    else:
                        value._history(session)
                        self.assertEqual(value.history_frequencies, [28011, 21011])
                        self.assertGreaterEqual(clock[0], 2)
                        self.assertTrue(any(query > value.args.timeout for query in queries))

    def test_live_stimuli_keep_two_explicit_modes_and_a_nonself_target(self):
        for base, target in (("VA3UXA", "K1ABC"), ("K1ABC", "VA3UXA")):
            with self.subTest(base=base):
                value, commands = self.suite(), []
                value.args = SimpleNamespace(call=base, timeout=1)
                value.deadline = time.monotonic() + 10
                document = {"configuration": {}, "revision": "fixture"}
                def human(command, expected=None):
                    commands.append(command)
                    text = expected or (f"DX de W1AW: 28201.0 {target} TABCDEF01-0\nDX de W1AW: 21201.0 {target} TABCDEF01-1" if command.startswith("SHOW DX") else "")
                    return text, document
                session = SimpleNamespace(human=human, get=lambda: document)
                with patch.object(HARNESS.time, "sleep"), patch.object(value, "history_result", side_effect=RuntimeError("fixture boundary")):
                    with self.assertRaisesRegex(RuntimeError, "fixture boundary"):
                        value.history(session)
                stimuli = [command for command in commands if command.startswith("DX ") and "NOTAFREQ" not in command]
                self.assertEqual(stimuli, [f"DX 28201 {target} CW TABCDEF01-0 up-5?", f"DX {target} 21201 FT8 TABCDEF01-1 up-5?"])

    def test_v7_history_query_refuses_pause_expiring_during_response(self):
        value = object.__new__(HARNESS.V7Suite)
        value.token = "ABCDEF01"
        document = {"configuration": {}, "revision": "fixture", "status": {"session": {"pause_active": True}}}
        ended = copy.deepcopy(document)
        ended["status"]["session"]["pause_active"] = False
        row = f"DX de W1AW: 28011.0 VA3UXA {value.history_label()}-0\nEnd of retained history search."
        session = SimpleNamespace(get=lambda: document, human=lambda command: (row, ended))
        with self.assertRaisesRegex(ValueError, "history delivery hold expired during query"):
            value.history_result(session, "SHOW DX", "VA3UXA", [28011, 21011], [0])

    def test_v7_history_label_avoids_embedded_snr_and_keeps_distinct_rows(self):
        value = object.__new__(HARNESS.V7Suite)
        value.token = "8111DBD1"
        label = value.history_label()
        self.assertEqual(label, "T8111HGH1")
        self.assertIsNone(re.search(r"(?i)([-+]?\d{1,3})\s*dB", label + "-1"))
        self.assertEqual(int(value.token[-2:], 16), 209)
        document = {"configuration": {}, "revision": "fixture", "status": {"session": {"pause_active": True}}}
        rows = (f"DX de W1AW: 28039.0 VA3UXA {label}-0\n"
                f"DX de W1AW: 21039.0 VA3UXA {label}-1\nEnd of retained history search.")
        session = SimpleNamespace(get=lambda: document, human=lambda command: (rows, document))
        value.history_result(session, "SHOW DX", "VA3UXA", [28039, 21039], [0, 1])
        session.human = lambda command: (rows.replace(label + "-1", label), document)
        with self.assertRaises(AssertionError):
            value.history_result(session, "SHOW DX", "VA3UXA", [28039, 21039], [0, 1])

    def test_v7_history_work_limit_requires_bounded_exact_continuation(self):
        for defect in (None, "missing_token", "repeated_token", "duplicate", "unexpected", "never_finishes"):
            with self.subTest(defect=defect):
                value = object.__new__(HARNESS.V7Suite)
                value.token = "ABCDEF01"
                document = {"configuration": {}, "revision": "fixture", "status": {"session": {"pause_active": True}}}
                first = f"DX de W1AW: 28011.0 VA3UXA {value.history_label()}-0\n"
                cursor = "H1" + "A" * 32
                commands = []

                def human(command):
                    commands.append(command)
                    if len(commands) == 1:
                        text = first + "Search work limit reached; this page is incomplete.\n"
                        if defect != "missing_token":
                            text += f"Continue older history: SHOW DX NEXT {cursor}\n"
                    elif defect in ("repeated_token", "never_finishes"):
                        token = cursor if defect == "repeated_token" else "H1" + f"{len(commands):032X}"
                        text = f"Search work limit reached; this page is incomplete.\nContinue older history: SHOW DX NEXT {token}\n"
                    elif defect == "duplicate":
                        text = first + "End of retained history search.\n"
                    elif defect == "unexpected":
                        text = f"DX de W1AW: 21011.0 VA3UXA {value.history_label()}-1\nEnd of retained history search.\n"
                    else:
                        text = "End of retained history search.\n"
                    return text, document

                session = SimpleNamespace(get=lambda: document, human=human)
                if defect is None:
                    value.history_result(session, "SHOW DX VA3UXA 2", "VA3UXA", [28011, 21011], [0])
                    self.assertEqual(commands, ["SHOW DX VA3UXA 2", "SHOW DX NEXT " + cursor])
                elif defect == "never_finishes":
                    with self.assertRaises(HARNESS.BlockedPrerequisite):
                        value.history_result(session, "SHOW DX VA3UXA 2", "VA3UXA", [28011, 21011], [0])
                    self.assertEqual(len(commands), 64)
                else:
                    with self.assertRaises(AssertionError):
                        value.history_result(session, "SHOW DX VA3UXA 2", "VA3UXA", [28011, 21011], [0])

    def test_v7_history_empty_selection_requires_later_exhaustion(self):
        value = object.__new__(HARNESS.V7Suite)
        value.token = "ABCDEF01"
        document = {"configuration": {}, "revision": "fixture", "status": {"session": {"pause_active": True}}}
        cursor = "H1" + "B" * 32
        pages = iter([f"Search work limit reached; this page is incomplete.\nContinue older history: SHOW DX NEXT {cursor}\n",
                      "Older retained history page:\nEnd of retained history search.\n"])
        commands = []
        def human(command):
            commands.append(command)
            return next(pages), document
        value.history_result(SimpleNamespace(get=lambda: document, human=human), "SHOW DX VA3UXA 2", "VA3UXA", [28011, 21011], [])
        self.assertEqual(commands, ["SHOW DX VA3UXA 2", "SHOW DX NEXT " + cursor])

    def test_selection_matrix_and_saved_filters_have_exact_positive_results(self):
        value, session = self.suite(), SyntheticHistorySession()
        value.history_selections(session, "K1ABC", [28200, 21200])
        self.assertEqual(len(value.results), 18)
        self.assertTrue(all(result["result"] == "PASS" for result in value.results), value.results)
        self.assertFalse(session.configuration["blocked_band"])
        self.assertFalse(session.configuration["blocked_mode"])

    def test_selection_oracle_rejects_plausible_false_green_answers(self):
        for defect in ("ignore_band", "ignore_mode", "categories_union", "ignore_saved", "trim_punctuation", "mutate_preferences", "wrong_frequency"):
            with self.subTest(defect=defect):
                value = self.suite()
                with patch("builtins.print"):
                    value.history_selections(SyntheticHistorySession(defect), "K1ABC", [28200, 21200])
                self.assertTrue(any(result["result"] == "FAIL" for result in value.results), defect)

    def test_selection_next_retains_identity_after_all_invalid_requests(self):
        value, session = self.suite(), SyntheticHistorySession()
        value.history_selection_next(session, "K1ABC", [28200, 21200])
        rejected = [command for command in session.commands if " COMMENT " not in command and " NEXT " not in command]
        self.assertEqual(len(rejected), len(SyntheticHistorySession.invalid))
        self.assertTrue(any(command.startswith("SHOW/DX") for command in session.commands))
        self.assertTrue(any(command.startswith("SH/DX") for command in session.commands))

    def test_invalid_and_next_oracle_rejects_false_green_answers(self):
        for defect in ("accept_invalid", "invalid_clears_cursor", "next_forgets_selection", "dialect_bypass"):
            with self.subTest(defect=defect):
                with self.assertRaises(AssertionError):
                    self.suite().history_selection_next(SyntheticHistorySession(defect), "K1ABC", [28200, 21200])

    def test_selection_aliases_require_the_same_exact_rows_in_both_dialects(self):
        value, session = self.suite(), SyntheticHistorySession()
        for dialect in ("go", "cc"):
            session.dialect = dialect
            for alias in ("SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX") + (("SHOW/DX", "SH/DX") if dialect == "cc" else ()):
                value.history_result(session, alias + " K1ABC 2 MODE CW, FT8 BAND 10,15 COMMENT TABCDEF01", "K1ABC", [28200, 21200], [0, 1])

    def test_exact_row_oracle_rejects_missing_duplicate_or_unrelated_rows(self):
        value = self.suite()
        for response in ("Spot queued.", "No matching retained spots.",
                         "\n".join([SyntheticHistorySession().rows([0])] * 2),
                         "DX de W1AW: 28200.0 K1ABC unrelated", "DX de malformed",
                         SyntheticHistorySession().rows([0, 1]) + "\nSearch work limit reached; this page is incomplete.",
                         SyntheticHistorySession().rows([0, 1]) + "\nWarning: unreadable archive records were skipped;"):
            with self.subTest(response=response):
                session = SyntheticHistorySession()
                session.human = lambda *args: (response, session.get())
                with self.assertRaises(AssertionError):
                    value.history_result(session, "query", "K1ABC", [28200, 21200], [0, 1])


class SafetyFixtures(unittest.TestCase):
    def test_optimized_python_refused_before_any_connection(self):
        with patch.object(HARNESS.sys, "flags", SimpleNamespace(optimize=1)):
            with patch.object(HARNESS, "Suite") as suite:
                with self.assertRaises(RuntimeError):
                    HARNESS.main()
                suite.assert_not_called()

    def cleanup_suite(self):
        value = object.__new__(HARNESS.Suite)
        value.sessions = []
        value.baselines = {"FIXTURE-1": {"configuration": {"marker": 7}, "status": {"session": {"diagnostic_comments": "OFF"}}}}
        value.owned = {"OWNED-TEST-NAME"}
        value.cleanup_errors = []
        return value

    def test_cleanup_restores_exact_snapshot_and_deletes_only_owned_name(self):
        value = self.cleanup_suite()
        client = SimpleNamespace()
        client.upload = lambda verb, resource, configuration: {
            "result": {"valid": configuration == {"marker": 7}, "applied": True, "persisted": True}}
        commands = []
        def human(command):
            commands.append(command)
            return "Deleted preset OWNED-TEST-NAME", {}
        client.human = human
        client.close = lambda: None
        client.get = lambda: {"configuration": {"marker": 7}}
        with patch.object(HARNESS, "Session", return_value=client) as sessions:
            value.cleanup()
        self.assertEqual(sessions.call_count, 2, "cleanup must reconnect to verify disk continuity")
        self.assertEqual(commands, ["SET DIAG OFF", "RESUME", "DELETE PRESET OWNED-TEST-NAME"])
        self.assertEqual(value.owned, set())
        self.assertEqual(value.cleanup_errors, [])

    def test_cleanup_failure_keeps_ownership_and_reports_residual_state(self):
        value = self.cleanup_suite()
        with patch.object(HARNESS, "Session", side_effect=TimeoutError("unreachable")):
            value.cleanup()
        self.assertEqual(value.owned, {"OWNED-TEST-NAME"})
        self.assertEqual(value.cleanup_errors[0]["call"], "FIXTURE-1")


class V7SafetyFixtures(unittest.TestCase):
    def test_preset_collision_capacity_and_lost_save_ack_never_delete_unowned_name(self):
        for fault in ("collision", "capacity", "lost_ack"):
            with self.subTest(fault=fault):
                suite = self.suite()
                suite.token, suite.results = "ABCDEF01", []
                commands = []
                def human(command, expected=None):
                    commands.append(command)
                    if command == "LIST PRESET":
                        listing = {"collision": "LIVE-ABCDEF01 (1/20)",
                                   "capacity": "Presets (20/20)",
                                   "lost_ack": "Presets (0/20)"}[fault]
                        return listing, {}
                    if command.startswith("SAVE PRESET"):
                        raise TimeoutError("save acknowledgement lost")
                    return "", {}
                a = SimpleNamespace(human=human)
                b = SimpleNamespace(human=lambda *args: self.fail("dependent preset operation ran"),
                                    get=lambda: self.fail("dependent preset read ran"))
                with patch("builtins.print"):
                    suite._presets(a, b)
                self.assertFalse(any(command.startswith("DELETE PRESET") for command in commands), commands)
                self.assertEqual(suite.owned, {"LIVE-ABCDEF01"} if fault == "lost_ack" else set())
                results = {row["case"]: row["result"] for row in suite.results}
                self.assertEqual(results["preset sharing"], "FAIL")
                for case in ("preset cross dialect", "preset round trip after reconnect", "preset cleanup"):
                    self.assertEqual(results[case], "BLOCKED", (fault, results))

    def test_preflight_refuses_partial_or_associated_backups(self):
        controls = {name: True for name in ("include_beacons", "allow_wwv", "allow_wcy",
                    "allow_announce", "allow_self", "allow_toxic")}
        document = {"schema_version": 7, "resource": "CONFIG", "revision": "fresh",
                    "configuration": {"filters": {"go": {**controls, "rules": []},
                                      "cc": {**controls, "modes": [], "sources": [],
                                             "geography": {}, "dxbm": {}}},
                                      "settings": {name: "" for name in ("dialect", "grid", "noise_class",
                                          "dedupe_policy", "path_min_observation_count", "solar_summary_minutes")}},
                    "status": {"presets": {"go": {"associated": False}, "cc": {"associated": False}},
                               "session": {"temporary_defaults": False}}}
        HARNESS.validate_v7_baseline(document)
        for defect in ("version", "resource", "revision", "go", "cc", "control", "settings", "presets", "go association", "cc association", "temporary"):
            bad = copy.deepcopy(document)
            if defect == "version": bad["schema_version"] = 6
            elif defect == "resource": bad["resource"] = "FILTER"
            elif defect == "revision": bad["revision"] = ""
            elif defect in ("go", "cc"): del bad["configuration"]["filters"][defect]
            elif defect == "control": del bad["configuration"]["filters"]["cc"]["allow_self"]
            elif defect == "settings": del bad["configuration"]["settings"]["grid"]
            elif defect == "presets": del bad["status"]["presets"]
            elif defect.endswith("association"): bad["status"]["presets"][defect.split()[0]]["associated"] = True
            else: bad["status"]["session"]["temporary_defaults"] = True
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                HARNESS.validate_v7_baseline(bad)

    def suite(self):
        value = object.__new__(HARNESS.V7Suite)
        value.sessions, value.owned, value.cleanup_errors = [], set(), []
        value.calls = ["FIXTURE-1"]
        configuration = {"filters": {"go": {"rules": [{"slot": 2, "expr": "MODE=CW"}]},
                                      "cc": {"modes": ["FT8"]}},
                         "settings": {"dialect": "go"}}
        value.baselines = {"FIXTURE-1": {"configuration": configuration,
                           "status": {"session": {"diagnostic_comments": "OFF"}}}}
        return value

    def test_cleanup_uploads_both_banks_and_requires_exact_reconnected_state(self):
        for corrupt in (None, "go", "cc", "settings"):
            with self.subTest(corrupt=corrupt):
                value = self.suite()
                original = copy.deepcopy(value.baselines["FIXTURE-1"]["configuration"])
                seen = []
                first = SimpleNamespace(upload=lambda verb, resource, cfg: (
                    seen.append((verb, resource, copy.deepcopy(cfg))) or
                    {"result": {"valid": True, "applied": True, "persisted": True}}),
                    human=lambda *args: ("", {}), close=lambda: None,
                    closed=True, sock=SimpleNamespace(close=lambda: None))
                returned = copy.deepcopy(original)
                if corrupt in ("go", "cc"):
                    returned["filters"][corrupt] = {}
                elif corrupt == "settings":
                    returned["settings"]["dialect"] = "cc"
                presets = {"go": {"associated": False}, "cc": {"associated": False}}
                verify = SimpleNamespace(get=lambda: {"configuration": returned, "status": {"presets": presets}}, close=lambda: None)
                with patch.object(HARNESS, "V7Session", side_effect=[first, verify]) as factory:
                    value.cleanup()
                self.assertEqual(seen, [("PUT", "CONFIG", original)])
                self.assertEqual(factory.call_count, 2)
                self.assertEqual(bool(value.cleanup_errors), corrupt is not None)
                self.assertEqual(value.residual_associations["FIXTURE-1"], presets)

    def test_cleanup_lost_restore_ack_is_reported_and_owned_preset_retained(self):
        value = self.suite()
        value.owned = {"OWNED"}
        first = SimpleNamespace(upload=lambda *args: (_ for _ in ()).throw(TimeoutError("lost ACK")),
                                closed=True, sock=SimpleNamespace(close=lambda: None))
        with patch.object(HARNESS, "V7Session", side_effect=[first, TimeoutError("delete unavailable")]):
            value.cleanup()
        self.assertEqual(value.owned, {"OWNED"})
        self.assertIn("lost ACK", value.cleanup_errors[0]["detail"])

    def test_cleanup_deletes_owned_preset_after_lost_restore_ack(self):
        value = self.suite()
        value.owned = {"OWNED"}
        first = SimpleNamespace(upload=lambda *args: (_ for _ in ()).throw(TimeoutError("lost ACK")),
                                closed=True, sock=SimpleNamespace(close=lambda: None))
        commands = []
        second = SimpleNamespace(human=lambda command: (commands.append(command) or "Deleted preset", {}),
                                 close=lambda: None)
        with patch.object(HARNESS, "V7Session", side_effect=[first, second]):
            value.cleanup()
        self.assertEqual(commands, ["DELETE PRESET OWNED"])
        self.assertFalse(value.owned)
        self.assertIn("lost ACK", value.cleanup_errors[0]["detail"])

    def test_rule_success_ack_without_expected_state_fails(self):
        value = self.suite()
        doc = {"configuration": copy.deepcopy(value.baselines["FIXTURE-1"]["configuration"])}
        session = SimpleNamespace(human=lambda command: ("Rule saved", doc))
        with self.assertRaises(ValueError):
            value._rule(session, "PASS RULE 9 MODE=CW", 9, "pass", "MODE=CW")

    def test_invalid_refusal_must_preserve_both_banks_and_revision(self):
        value = self.suite()
        before = {"configuration": copy.deepcopy(value.baselines["FIXTURE-1"]["configuration"]), "revision": "one"}
        for mutation in (None, "go", "cc", "revision"):
            after = copy.deepcopy(before)
            if mutation in ("go", "cc"):
                after["configuration"]["filters"][mutation] = {}
            elif mutation == "revision":
                after["revision"] = "two"
            session = SimpleNamespace(get=lambda: before, human=lambda command: ("Invalid input", after))
            if mutation is None:
                value._unchanged_refusal(session, "bad")
            else:
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    value._unchanged_refusal(session, "bad")

    def test_v7_upload_explicit_schema_and_current_revision(self):
        value = object.__new__(HARNESS.V7Session)
        value.suite = SimpleNamespace(identifier=lambda: "Req-1")
        value.get = lambda: {"revision": "fresh"}
        sent = []
        value.send = sent.append
        value.frame = lambda resource, identifier, version, command: (
            {"result": {"operation": "PUT"}}, b"")
        config = {"filters": {"go": {}, "cc": {}}, "settings": {}}
        value.upload("PUT", "CONFIG", config)
        self.assertTrue(sent[0].startswith("PUT YAML CONFIG SCHEMA 7\r\n---\r\n"))
        body = HARNESS.yaml.safe_load(sent[0].split("---\r\n", 1)[1].rsplit("...\r\n", 1)[0])
        self.assertEqual(body["if_revision"], "fresh")
        self.assertEqual(body["configuration"], config)

    def test_upload_schema_six_uses_bare_header_but_seven_is_explicit(self):
        for version, header in ((6, "PUT YAML CONFIG"), (7, "PUT YAML CONFIG SCHEMA 7")):
            with self.subTest(version=version):
                value = object.__new__(HARNESS.V7Session)
                value.suite = SimpleNamespace(identifier=lambda: "Req-2")
                value.get = lambda: {"revision": "current-revision"}
                sent, frames = [], []
                value.send = sent.append
                def frame(resource, identifier, frame_version, command):
                    frames.append((resource, identifier, frame_version, command))
                    return {"result": {"operation": "PUT"}}, b""
                value.frame = frame
                config = {"settings": {"grid": "FN31"}}
                value.upload("PUT", "CONFIG", config, version=version)
                self.assertTrue(sent[0].startswith(header + "\r\n---\r\n"), sent[0])
                body = HARNESS.yaml.safe_load(sent[0].split("---\r\n", 1)[1].rsplit("...\r\n", 1)[0])
                self.assertEqual(body, {"schema_version": version, "request_id": "Req-2",
                                        "configuration": config, "if_revision": "current-revision"})
                self.assertEqual(frames, [("CONFIG", "Req-2", version, header)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
