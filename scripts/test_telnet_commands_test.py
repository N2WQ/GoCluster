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
        prefix = (command + "\r\n" + command + "\r\nGET YAML CONFIG SCHEMA 3 ID Echo-1\r\n").encode()
        text, _ = self.session(prefix).human(command, command)
        self.assertEqual(text, command)

    def test_echo_only_never_satisfies_expected_response(self):
        command = "PASS DXSTATE ALL"
        prefix = (command + "\r\nGET YAML CONFIG SCHEMA 3 ID Echo-1\r\n").encode()
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
