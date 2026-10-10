"""Offline failure controls for the checkout runner; these do not qualify telnet."""

import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
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


class DriverControls(unittest.TestCase):
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

        def get_after_receipt():
            self.assertTrue(session.suite.log.getvalue().endswith(raw))
            return {"checked": True}

        session.get.side_effect = get_after_receipt
        self.assertEqual(driver.exact_human_reply(session, command, reply), (reply, {"checked": True}))
        session.send.assert_called_once_with(command + "\r\n")
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

        def get_after_receipt():
            self.assertEqual(raw_log.getvalue(), expected_receipt)
            session.suite.log.flush.assert_called_once()
            events.append(("get",))

        session.get.side_effect = get_after_receipt
        self.assertEqual(driver.archive_rows(session, "LABEL"), [row])
        self.assertEqual(events, [("send", query + "\r\n"),
                                  ("line", (query + "\r\n").encode()),
                                  ("line", (row + "\r\n").encode()),
                                  ("line", b"End of retained history search.\r\n"), ("get",)])
        session.human.assert_not_called()

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
            # Python is deliberately not a Go test binary: these flags fail.
            with self.assertRaisesRegex(ValueError, "fixture exited early"):
                support.Child(Path(sys.executable), Path(directory), {"digest": "source"})

    def test_readiness_sharing_retry_is_bounded(self):
        for expires in (False, True):
            with self.subTest(expires=expires), tempfile.TemporaryDirectory(dir=support.ROOT / ".tmp") as directory:
                root = Path(directory)
                (root / "config").mkdir()
                process = mock.Mock(pid=123)
                process.poll.return_value = None
                receipt = json.dumps(dict(nonce="fixed", pid=123, started=True,
                                          binary_hash="binary", source_digest="source",
                                          config_hashes={}, address="127.0.0.1:12345",
                                          reputation_enabled=False))
                reads = [PermissionError("sharing publication"), receipt]
                with mock.patch.object(support.uuid, "uuid4", return_value=mock.Mock(hex="fixed")), \
                        mock.patch.object(support.subprocess, "Popen", return_value=process), \
                        mock.patch.object(support, "digest", return_value="binary"), \
                        mock.patch.object(Path, "read_text", side_effect=reads) as reading, \
                        mock.patch.object(support.time, "monotonic", side_effect=([0, 0, 46] if expires else [0, 0, 1])), \
                        mock.patch.object(support.time, "sleep"):
                    if expires:
                        with self.assertRaisesRegex(ValueError, "readiness deadline expired"):
                            support.Child(Path("fixture.exe"), root, {"digest": "source"})
                        process.kill.assert_called_once()
                        self.assertEqual(reading.call_count, 1)
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
