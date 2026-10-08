"""Offline behavioral fixtures for the live telnet command harness.

No fixture opens a network connection. The fake socket retains stream boundaries
only as recv chunks, allowing the reader to prove its framing and IAC handling.
"""

import importlib.util
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

    def recv(self, size):
        if not self.chunks:
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
    def session(self, prefix, remote_echo=True):
        value = object.__new__(HARNESS.Session)
        value.suite = SimpleNamespace(identifier=lambda: "Echo-1", commands=0)
        value.reader = SimpleNamespace(remote_echo=remote_echo)
        value.send = lambda data: None
        value.frame = lambda *args: ({"configuration": {}, "status": {}}, prefix)
        return value

    def test_exact_response_equal_to_command_survives_single_echo_removal(self):
        command = "PASS DXSTATE ALL"
        prefix = (command + "\r\n" + command + "\r\nGET YAML CONFIG SCHEMA 4 ID Echo-1\r\n").encode()
        text, _ = self.session(prefix).human(command, command)
        self.assertEqual(text, command)

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
        command = "SHOW FILTER BAND"
        response = b"User          FIXTURE\r\r\nBands\r\r\n  PASS: ALL\r\r\nMissed spots are not replayed.\r\r\n"
        value = self.session(response, remote_echo=False)
        value.call = "FIXTURE"
        value.suite.wire_failures = []
        value.human(command, "PASS: ALL")
        self.assertTrue(value.suite.wire_failures)


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

    def test_live_stimuli_keep_two_explicit_modes_and_a_nonself_target(self):
        for base, target in (("VA3UXA", "K1ABC"), ("K1ABC", "VA3UXA")):
            with self.subTest(base=base):
                value, commands = self.suite(), []
                value.args = SimpleNamespace(call=base)
                document = {"configuration": {}, "revision": "fixture"}
                def human(command, expected=None):
                    commands.append(command)
                    text = expected or (f"DX de W1AW: 28201.0 {target} fixture" if command.startswith("SHOW DX") else "")
                    return text, document
                session = SimpleNamespace(human=human, get=lambda: document)
                with patch.object(HARNESS.time, "sleep"), patch.object(value, "history_result", side_effect=RuntimeError("fixture boundary")):
                    with self.assertRaisesRegex(RuntimeError, "fixture boundary"):
                        value.history(session)
                stimuli = [command for command in commands if command.startswith("DX ") and "NOTAFREQ" not in command]
                self.assertEqual(stimuli, [f"DX 28201 {target} CW TABCDEF01-0 up-5?", f"DX {target} 21201 FT8 TABCDEF01-1 up-5?"])

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
