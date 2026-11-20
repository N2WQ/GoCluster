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
    lines = [verb + " - Change native CC selections or switches.", "Usage: " + verb + " <switch> [ALL]"]
    if verb == "SET/FILTER":
        lines += ["       SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/<PASS|REJECT> <list>",
                  "       SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/OFF",
                  "       SET/FILTER <country-list>/PASS or /REJECT",
                  "       SET/FILTER DXBM/PASS <band-or-slot-list>",
                  "       SET/FILTER DXBM/REJECT <band-or-slot-list>", "       SET/FILTER DXBM/OFF"]
    notes = ["Switches accept no value or ALL. PC93 aliases ANNOUNCE.",
             "SELF controls spots of your own call in GoCluster.", "OWN, BOB, DX/NODX, TALK, WX are unavailable."]
    if verb == "SET/FILTER":
        notes += ["Each geography edit replaces its category. Other categories remain unchanged.",
                  "First PASS after OFF starts a whitelist; Later edits keep other slots.",
                  "Per-band geography is a GoCluster schema-seven YAML extension.",
                  "Use YAML for long lists; do not split them across replacing commands.",
                  "Native bands:", "Native slots:", "State/province codes:"]
    else:
        notes += ["UNSET/FILTER does not accept inline /ON or /OFF or geography lists."]
    for note in notes:
        lines += textwrap.wrap(note, width=78, initial_indent="  ", subsequent_indent="  ")
    while len(lines) < 10:
        lines.append("  Independent receipt filler.")
    return lines


PAUSE_FOOTER_END = "Type RESUME to restart now, or SHOW HOLD to check the pause."


def pause_lines(seconds=30):
    unit = "second" if seconds == 1 else "seconds"
    return [f"Live spots paused for {seconds} {unit}. Missed spots aren't replayed.", PAUSE_FOOTER_END]


def cc_help_wire(lines, query="HELP SET/FILTER", seconds=30, pause=True):
    return routing_help_wire(lines, query, pause=pause, seconds=seconds)


def routing_help_wire(lines, query="HELP SH BUILD EXTRA", pause=False, echo=True, seconds=30):
    body = list(lines)
    if pause:
        body += pause_lines(seconds)
    return ((query + "\r\n" if echo else "") + "\r\n".join(body) + "\r\n").encode("ascii")


def discovery_literal_reply(verb, unknown=False, dialect="go"):
    if dialect == "cc":
        return ("Use HELP SET/FILTER for native CC filters. Boolean rule edits require DIALECT\nGO.\nNo changes applied.")
    if unknown:
        return ("Use numbered rules: PASS RULE <0..9> <expression> or REJECT RULE <0..9>\n"
                "  <expression>. Type HELP RULE for fields and examples.\nNo changes applied.")
    return ("Usage: PASS RULE <0..9> <expression>\n       REJECT RULE <0..9> <expression>\n"
            "Type HELP RULE for fields and examples.\nNo changes applied.")


def current_configuration(dialect="cc"):
    controls = dict(include_beacons=False, allow_wwv=True, allow_wcy=True, allow_announce=True, allow_self=True, allow_toxic=True)
    return dict(settings=dict(dialect=dialect, grid="FN31", noise_class="URBAN", dedupe_policy="SLOW",
                              path_min_observation_count=30, solar_summary_minutes=30, cw_speed=False),
                filters={"go": dict(rules=[dict(slot=0, action="pass", expr='COMMENT="CQ"')], **controls),
                         "cc": dict(modes=["CW"], sources=["HUMAN"],
                                    geography={key: {"global": {"action": "off"}, "bands": {}} for key in ("DOC", "DOS", "DXCTY", "DXSTATE")},
                                    dxbm=dict(active=False, base_allow=True, slots={}), **controls)})


def current_document(dialect="cc"):
    configuration = current_configuration(dialect)
    refs = {key: dict(associated=False, name="", modified=False) for key in ("go", "cc")}
    return dict(configuration=configuration, revision="r7", schema_version=7, resource="CONFIG",
                status=dict(configured=copy.deepcopy(configuration["settings"]), effective=dict(dedupe_policy="FAST"),
                            active_dialect=dialect, preset=copy.deepcopy(refs[dialect]), presets=refs))


def task_help_literal(topic, dialect):
    # Short, literal pages deliberately differ from the rendered layout.
    # Their facts and commands are independent of the harness's task oracle.
    if topic == "FILTERS":
        if dialect == "go":
            return ["SHOW FILTER shows your active rules.",
                    "A matching REJECT rule blocks a spot, even if a PASS rule matches.",
                    "HELP RULE explains the syntax. HELP FILTER EXAMPLES shows complete setups."]
        return ["SHOW/FILTER shows your active filters.",
                "Band, mode, source, country, and state filters all apply together.",
                "HELP SET/FILTER lists the options. HELP FILTER EXAMPLES shows complete setups."]
    elif topic == "FILTER EXAMPLES":
        if dialect == "go":
            lines = ["Human-reported CW and FT8 spots on 20m and 40m:",
                     "  Removes all current GO rules. Other settings stay unchanged.",
                     "  RESET FILTER RULE ALL",
                     "  PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8 AND SOURCE=HUMAN",
                     "  PASS RULE 0 BAND=20M,40M AND MODE=CW AND SOURCE=SKIMMER",
                     "  REJECT RULE 0 MODE=FT8", "  RESET FILTER", "  SHOW FILTER", "  RESUME"]
        else:
            lines = ["Select the 20m and 40m frequency ranges:",
                     "  Replaces all band choices. Other filters still apply.",
                     "  SET/FILTER DXBM/OFF", "  SET/FILTER DXBM/PASS 20,40",
                     "  SET/FILTER DOC/PASS K,VE", "  SET/NOFT8", "  SET/NOFILTER", "  SHOW/FILTER", "  RESUME"]
        terminal = "  HELP PRESETS"
    else:
        assert topic == "PRESETS"
        return ["A preset saves your active GO or CC filters and shared settings.",
                "  SAVE PRESET DAY", "  LIST PRESET", "  LOAD PRESET DAY", "  DELETE PRESET DAY",
                "Your callsign's numeric SSIDs share the same presets."]
    while len(lines) < 10:
        lines.append("  Independent receipt filler.")
    return [*lines, terminal]


def discovery_help_literal(key, dialect):
    if key in ("FILTERS", "FILTER EXAMPLES", "PRESETS"):
        return task_help_literal(key, dialect)
    if key == "OVERVIEW":
        show, readback = ("SHOW DX", "SHOW FILTER") if dialect == "go" else ("SHOW/DX", "SHOW/FILTER")
        return ["Getting help", "  HELP COMMANDS", "  HELP <command>", "  HELP SYMBOLS", "  HELP TROUBLE",
                "Showing archive spots", "  " + show + " 10", "  HELP " + show,
                "Managing filters", "  " + readback, "  HELP FILTERS", "  SHOW SETTINGS",
                "Managing presets", "  LIST PRESET", "  SAVE PRESET DAY", "  LOAD PRESET DAY",
                "  HELP PRESETS     Learn how to manage saved setups."]
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
        if dialect == "go":
            lines = ["SHOW FILTER - Display current filter state.", "Usage: SHOW FILTER", "       SHOW FILTER RULE", "       SHOW FILTER FULL",
                     "Aliases: SHOW/FILTER, SH/FILTER"]
            notes = ["Shows canonical numbered rules and separate controls.", "GET YAML CONFIG SCHEMA 7 is complete."]
        else:
            lines = ["SHOW/FILTER - Inspect native CC state.", "Usage: SHOW/FILTER [DOC|DOS|DXCTY|DXSTATE|DXBM|MODE|SOURCE|FULL]",
                     "Aliases: SHOW FILTER, SH/FILTER"]
            notes = ["Shows exact native selections and six controls.", "Band INHERIT differs from explicit OFF; DXBM also has independent state.",
                     "GET YAML CONFIG SCHEMA 7 provides a complete both-bank backup. Schema 6 does not."]
    else:
        assert key in ("PASS", "REJECT")
        summary = ("Allow matching spots or enable separate controls." if key == "PASS" else "Veto matching spots or disable separate controls.") if dialect == "go" else "Change separate delivery switches."
        lines = ["Spot rules (GO only)", "  PASS RULE 0 MODE=CW       Allow CW under rule 0.",
                 "  REJECT RULE 9 MODE=FT8    Block FT8 even if a PASS rule allows it.",
                 "  See HELP RULE for more examples.", "", "Delivery switches (GO and CC)",
                 "  Switch     Turn on          Turn off", "  WWV        PASS WWV         REJECT WWV",
                 "  WCY        PASS WCY         REJECT WCY", "  ANNOUNCE   PASS ANNOUNCE    REJECT ANNOUNCE",
                 "  BEACON     PASS BEACON      REJECT BEACON", "",
                 "WWV, WCY, and announcements are live messages, not spots.",
                 "Numbered rules do not filter those messages.",
                 "Beacon spots still go through your spot filters when BEACON is on.",
                 "Changing a switch does not change any numbered rule.", "", key + " - " + summary]
        if dialect == "go":
            lines += ["Usage: " + key + " RULE <0..9> <expression>", "       " + key + " <BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC> [ALL]"]
            notes = ["SELF and TOXIC are also separate switches, not rule fields.", "PC93 aliases ANNOUNCE. Switches accept no value or ALL.",
                     "Old category mutation syntax is refused, not translated; use HELP RULE."]
        else:
            lines += ["Usage: " + key + " <BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC> [ALL]"]
            notes = ["PC93 aliases ANNOUNCE. Switches accept no value or ALL.",
                     "Numbered PASS and REJECT rules are for GO. In CC, see HELP SET/FILTER."]
    for note in notes[:-1]:
        lines += textwrap.wrap(note, width=78, initial_indent="  ", subsequent_indent="  ")
    # Filler precedes the independently specified terminal, so the strict
    # decoder still detects trailing text after a complete response.
    while len(lines) < 10:
        lines.append("  Independent receipt filler.")
    lines += textwrap.wrap(notes[-1], width=78, initial_indent="  ", subsequent_indent="  ")
    return lines


def native_oracle_document(dialect="go"):
    """Literal two-bank values, independent of the driver and Go encoders."""
    controls = dict(include_beacons="DEFAULT", allow_wwv="DEFAULT", allow_wcy="DEFAULT",
                    allow_announce="DEFAULT", allow_self="DEFAULT", allow_toxic="DEFAULT")
    configuration = dict(filters={
        "go": dict(rules=[], **controls),
        "cc": dict(modes=["CW", "FT8"], sources=["HUMAN", "SKIMMER"],
                   geography={name: {"global": {"action": "off"}, "bands": {}}
                              for name in ("DOC", "DOS", "DXCTY", "DXSTATE")},
                   dxbm=dict(active=False, base_allow=True, slots={}), **controls)},
        settings=dict(dialect=dialect, grid="FN31", noise_class="RURAL", dedupe_policy="FAST",
                      path_min_observation_count=0, solar_summary_minutes=0, cw_speed=False))
    return dict(configuration=configuration, revision="revision-1",
                status=dict(active_dialect=dialect, effective=copy.deepcopy(configuration["settings"]),
                            presets={name: dict(associated=False, name="", modified=False) for name in ("go", "cc")},
                            session=dict(diagnostic_comments="SOURCE")))


def native_oracle_bytes(document):
    return driver.yaml.safe_dump(dict(configuration_version=7, configuration=copy.deepcopy(document["configuration"]),
                                      presets={}, notice=dict(profile_reset_generation=0,
                                                              profile_acknowledged_generation=0,
                                                              library_acknowledged_generation=0))).encode("ascii")


class DriverControls(unittest.TestCase):
    def test_semantic_reconnect_requires_saved_owner_and_never_retries_failed_close(self):
        before = native_oracle_document()
        for fault in (None, "configuration", "go", "cc", "stored_reference", "unsaved", "close"):
            session, restored, record = mock.Mock(), mock.Mock(), mock.Mock()
            session.call = "VE3ABC-46"
            session.get.return_value = before
            after = copy.deepcopy(before)
            if fault == "configuration":
                after["configuration"]["filters"]["cc"]["modes"] = []
            elif fault in ("go", "cc"):
                after["status"]["presets"][fault]["name"] = "CHANGED"
            initial = native_oracle_bytes(before)
            later = driver.yaml.safe_load(initial)
            if fault == "stored_reference":
                later["presets"] = {"go": {"name": "CHANGED"}}
            if fault == "unsaved":
                later["configuration"]["settings"]["grid"] = "IO91"
                initial = driver.yaml.safe_dump(later).encode()
            record.read_bytes.side_effect = [initial, initial, driver.yaml.safe_dump(later).encode(),
                                              driver.yaml.safe_dump(later).encode()]
            restored.human.return_value = "Paused", after
            if fault == "close":
                session.close.side_effect = ValueError("close failed")
            with self.subTest(fault=fault), mock.patch.object(driver, "fixture_session", return_value=restored) as opened:
                if fault is None:
                    self.assertIs(driver.reconnect_fixture_session(session, record), restored)
                    opened.assert_called_once_with(session.suite, "VE3ABC-46")
                    session.close.assert_called_once()
                else:
                    with self.assertRaises(ValueError):
                        driver.reconnect_fixture_session(session, record)
                    if fault in ("close", "unsaved"):
                        opened.assert_not_called()
                    if fault == "unsaved":
                        session.close.assert_not_called()

    def test_native_edit_preserves_inactive_bank_and_both_stored_baselines(self):
        before = native_oracle_document()
        changed = copy.deepcopy(before)
        changed["configuration"]["filters"]["go"]["rules"] = [dict(slot=0, action="pass", expr="MODE=CW")]
        references = {name: dict(name=name.upper(), baseline=dict(configuration_version=7, dialect=name,
                                                               settings=copy.deepcopy(before["configuration"]["settings"]),
                                                               **{name: copy.deepcopy(before["configuration"]["filters"][name])}))
                      for name in ("go", "cc")}
        for fault in (None, "inactive", "go", "cc"):
            session, record = mock.Mock(), mock.Mock()
            after = copy.deepcopy(changed)
            if fault == "inactive":
                after["configuration"]["filters"]["cc"]["modes"] = []
            original = driver.yaml.safe_load(native_oracle_bytes(before))
            stored = driver.yaml.safe_load(native_oracle_bytes(after))
            original["presets"], stored["presets"] = copy.deepcopy(references), copy.deepcopy(references)
            if fault in ("go", "cc"):
                stored["presets"][fault]["name"] = "CHANGED"
            first, last = driver.yaml.safe_dump(original).encode(), driver.yaml.safe_dump(stored).encode()
            record.read_bytes.side_effect = [first, last, last]
            session.get.return_value = before
            session.human.return_value = "Rule 0 set.", after
            with self.subTest(fault=fault):
                if fault is None:
                    driver.fixture_edit(session, "PASS RULE 0 MODE=CW", record)
                else:
                    with self.assertRaises(ValueError):
                        driver.fixture_edit(session, "PASS RULE 0 MODE=CW", record)

    def test_native_unchanged_requires_boolean_types_and_both_reference_owners(self):
        before = native_oracle_document()
        before["configuration"]["filters"]["go"]["include_beacons"] = False
        record = mock.Mock()
        record.read_bytes.return_value = b"unchanged"
        driver.require_fixture_unchanged(before, before, record, b"unchanged")
        for fault in ("integer_control", "go", "cc"):
            after = copy.deepcopy(before)
            if fault == "integer_control":
                after["configuration"]["filters"]["go"]["include_beacons"] = 0
            else:
                after["status"]["presets"][fault]["modified"] = True
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                driver.require_fixture_unchanged(before, after, record, b"unchanged")

    def test_fixture_login_allocation_has_exact_stable_spelling(self):
        expected_main = """K1ABC-01 K1ABC-02 K1ABC-04 K1ABC-05 K1ABC-06 K1ABC-07
            K1ABC-08 K1ABC-09 K1ABC-10 K1ABC-11 K1ABC-12 K1ABC-13
            K1ABC-21 K1ABC-22 K1ABC-23 K1ABC-24 K1ABC-25 K1ABC-26 K1ABC-27 K1ABC-28 K1ABC-29
            K1ABC-34 K1ABC-50 K1ABC-51 K1ABC-52 K1ABC-53 K1ABC-54 K1ABC-55
            VE3ABC-03 VE3ABC-33 VE3ABC-34 VE3ABC-35 VE3ABC-40 VE3ABC-41 VE3ABC-42 VE3ABC-43
            VE3ABC-44 VE3ABC-45 VE3ABC-46 VE3ABC-47 VE3ABC-48 VE3ABC-49 VE3ABC-56 VE3ABC-57
            VE3ABC-63 VE3ABC-64 VE3ABC-65 VE3ABC-66 VE3ABC-67 VE3ABC-68 VE3ABC-69 VE3ABC-70
            VE3ABC-71 VE3ABC-73 VE3ABC-74 VE3ABC-75 VE3ABC-76 VE3ABC-77 VE3ABC-78 VE3ABC-79 VE3ABC-80""".split()
        self.assertEqual(set(driver.FIXTURE_LOGINS["main"]), set(expected_main))
        self.assertEqual(driver.FIXTURE_LOGINS["dedupe"],
                         ("K1ABC-58", "K1ABC-59", "VE3ABC-60", "VE3ABC-61", "VE3ABC-62"))
        self.assertEqual(driver.FIXTURE_LOGINS["reputation"], ("K1ABC-81", "K1ABC-82", "VE3ABC-83"))
        driver.validate_fixture_calls(driver.FIXTURE_LOGINS)

    def test_fixture_login_validation_preserves_leading_zero_and_base(self):
        inventory = {"main": ("K1ABC-1", "K1ABC-01", "VE3ABC-1", "K1ABC-0", "K1ABC-99")}
        before = copy.deepcopy(inventory)
        driver.validate_fixture_calls(inventory)
        self.assertEqual(inventory, before)
        for suffix in ("100", "70001", "001", "", "A", "1A", "-1", "\u0661", "\uff11"):
            with self.subTest(suffix=suffix), self.assertRaisesRegex(ValueError, "invalid fixture login"):
                driver.validate_fixture_calls({"main": ("K1ABC-" + suffix,)})

    def test_fixture_login_preflight_rejects_collisions_and_unbounded_inventory(self):
        for inventory in ({"main": ("K1ABC-01", "K1ABC-01")},
                          {"main": ("K1ABC-01",) * 201}, {}, {"other": ("K1ABC-01",)}):
            with self.subTest(inventory=inventory), self.assertRaises(ValueError):
                driver.validate_fixture_calls(inventory)

    def test_fixture_session_refuses_unallocated_call_and_keeps_reconnect_spelling(self):
        suite = mock.Mock()
        with mock.patch.object(driver, "FixtureSession") as session:
            with self.assertRaisesRegex(ValueError, "unallocated fixture login"):
                driver.fixture_session(suite, "K1ABC-70001")
            session.assert_not_called()
            driver.fixture_session(suite, "K1ABC-01")
            driver.fixture_session(suite, "K1ABC-01")
            self.assertEqual(session.call_args_list, [mock.call(suite, "K1ABC-01")] * 2)

    def test_fixture_preflight_fails_before_build_staging_or_output(self):
        with mock.patch.object(driver, "validate_fixture_calls", side_effect=ValueError("bad allocation")), \
                mock.patch.object(driver, "build") as build, mock.patch.object(driver, "stage") as stage, \
                mock.patch.object(driver, "plain_output") as output:
            with self.assertRaisesRegex(ValueError, "bad allocation"):
                driver.main()
            build.assert_not_called()
            stage.assert_not_called()
            output.assert_not_called()

    def test_positive_logins_cannot_bypass_allocation_guard(self):
        tree = ast.parse(Path(driver.__file__).read_text(encoding="utf-8"))
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and
                 isinstance(node.func, ast.Name) and node.func.id == "FixtureSession"]
        factory = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "fixture_session")
        self.assertEqual(len(calls), 1)
        self.assertIn(calls[0], list(ast.walk(factory)))

    def test_case_retirement_preserves_shared_sessions_and_original_failure(self):
        suite = driver.CommandSuite.__new__(driver.CommandSuite)
        shared, first, second = mock.Mock(), mock.Mock(), mock.Mock()
        suite.sessions, suite.results, suite.cleanup_errors, suite.deadline = [shared], [], [], float("inf")
        first.close.side_effect = ValueError("cleanup refusal")

        def failed_case():
            suite.sessions.extend((first, second))
            raise ValueError("original semantic failure")

        suite.case("owned", failed_case)
        shared.close.assert_not_called()
        first.close.assert_called_once()
        second.close.assert_called_once()
        self.assertEqual(suite.results[0]["result"], "FAIL")
        self.assertIn("original semantic failure", suite.results[0]["detail"])
        self.assertEqual(suite.cleanup_errors, ["owned: cleanup refusal"])

    def test_expired_case_retires_owned_sessions_with_one_bounded_budget(self):
        suite = driver.CommandSuite.__new__(driver.CommandSuite)
        shared, first, second = mock.Mock(), mock.Mock(), mock.Mock()
        suite.sessions, suite.results, suite.cleanup_errors, suite.deadline = [shared], [], [], 90.0
        observed = []

        def close_first():
            observed.append(suite.deadline)
            raise TimeoutError("retirement timed out")

        first.close.side_effect = close_first
        second.close.side_effect = lambda: observed.append(suite.deadline)

        def failed_case():
            suite.sessions.extend((first, second))
            raise ValueError("original semantic failure")

        with mock.patch.object(driver.time, "monotonic", return_value=100.0):
            suite.case("owned", failed_case)
        self.assertEqual(observed, [130.0, 130.0])
        self.assertEqual(suite.deadline, 90.0)
        shared.close.assert_not_called()
        self.assertIn("original semantic failure", suite.results[0]["detail"])
        self.assertEqual(suite.cleanup_errors, ["owned: retirement timed out"])

    def test_retirement_does_not_extend_unexpired_deadline(self):
        suite = driver.CommandSuite.__new__(driver.CommandSuite)
        suite.deadline = 101.0
        with mock.patch.object(driver.time, "monotonic", return_value=100.0):
            with suite.retirement_deadline():
                self.assertEqual(suite.deadline, 101.0)
        self.assertEqual(suite.deadline, 101.0)

    def test_login_receipt_uses_existing_bounded_read_without_replay(self):
        from types import SimpleNamespace
        session = driver.FixtureSession.__new__(driver.FixtureSession)
        greeting = b"Hello K1ABC-01; using temporary defaults. UTC>"
        until = mock.Mock(return_value=greeting)
        session.call, session.sock = "K1ABC-01", mock.Mock()
        session.reader = SimpleNamespace(until=until)
        session.suite = SimpleNamespace(deadline=time.monotonic() + 20,
                                        args=SimpleNamespace(timeout=8), log=io.BytesIO())
        session.send("K1ABC-01\r\n", count=False)
        until.assert_not_called()
        self.assertEqual(session.reader.until(b"UTC>"), greeting)
        until.assert_called_once_with(b"UTC>")
        self.assertIs(session.reader.until, until)
        session.sock.sendall.assert_called_once_with(b"K1ABC-01\r\n")
        self.assertIn(greeting, session.suite.log.getvalue())
        self.assertIn("temporary defaults", driver.login_text(session, "later reply"))

    def test_adjacent_edit_snapshot_is_detached_complete_and_only_from_current_config(self):
        session = driver.FixtureSession.__new__(driver.FixtureSession)
        complete = dict(resource="CONFIG", schema_version=7, configuration={"filters": {"go": {}, "cc": {}}, "settings": {}},
                        status={"presets": {"go": {}, "cc": {}}})
        session.remember_configuration(complete)
        original = copy.deepcopy(session.last_configuration)
        complete["configuration"]["filters"]["go"]["mutated_by_caller"] = True
        self.assertEqual(session.last_configuration, original)
        for changed in (dict(complete, resource="SETTINGS"), dict(complete, schema_version=6),
                        dict(complete, error={"code": "refused"}), dict(resource="CONFIG", schema_version=7),
                        dict(complete, status={"presets": {"go": {}}}),
                        dict(complete, configuration={"filters": {"go": {}}, "settings": {}})):
            session.remember_configuration(changed)
            self.assertEqual(session.last_configuration, original)
        with mock.patch.object(session, "get") as get:
            self.assertIs(driver.fixture_before(session), session.last_configuration)
            get.assert_not_called()
        del session.last_configuration
        with mock.patch.object(session, "get", return_value=original) as get:
            self.assertIs(driver.fixture_before(session), original)
            get.assert_called_once_with()
        with mock.patch.object(driver.harness.V7Session, "frame", return_value=(original, b"prefix")) as frame:
            actual, prefix = session.frame("CONFIG", "request", 7, "manual barrier")
            self.assertEqual((actual, prefix), (original, b"prefix"))
            self.assertEqual(session.last_configuration, original)
            self.assertIsNot(session.last_configuration, actual)
            frame.assert_called_once_with("CONFIG", "request", 7, "manual barrier")

    def test_setting_warning_requires_complete_single_line_failure(self):
        reply = "Noise class set to URBAN (warning: failed to persist: Access is denied.)"
        driver.require_setting_save_warning(reply)
        for text in ("Noise class set to URBAN", reply + "\n" + reply,
                     reply.replace(" (warning:", "\nwarning:"), "warning: failed to persist:"):
            with self.assertRaises(ValueError):
                driver.require_setting_save_warning(text)

    def test_help_group_reconnect_preserves_both_banks_refs_and_resets_only_session(self):
        before = native_oracle_document("cc")
        fresh = copy.deepcopy(before)
        fresh["status"]["session"] = dict(diagnostic_comments="OFF", pause_active=False,
            pause_pending_delivery=False, pause_remaining_seconds=0, suppressed_spots=0)
        for fault in (None, "go", "cc", "reference", "disk_reference", "diagnostic", "pause", "suppressed"):
            with self.subTest(fault=fault):
                current, restored, record = mock.Mock(), mock.Mock(), mock.Mock()
                current.call, current.suite = "VE3ABC-73", mock.Mock()
                raw = native_oracle_bytes(before)
                record.read_bytes.return_value = raw
                after = copy.deepcopy(fresh)
                if fault == "go":
                    after["configuration"]["filters"]["go"]["include_beacons"] = False
                elif fault == "cc":
                    after["configuration"]["filters"]["cc"]["modes"] = []
                elif fault == "reference":
                    after["status"]["presets"]["go"]["name"] = "OTHER"
                elif fault == "disk_reference":
                    bad = driver.yaml.safe_load(raw)
                    bad["presets"]["go"] = dict(name="OTHER", baseline={})
                    record.read_bytes.side_effect = [raw, driver.yaml.safe_dump(bad).encode(), driver.yaml.safe_dump(bad).encode()]
                elif fault == "diagnostic":
                    after["status"]["session"]["diagnostic_comments"] = "MODE"
                elif fault == "pause":
                    after["status"]["session"].update(pause_active=True, pause_remaining_seconds=30)
                elif fault == "suppressed":
                    after["status"]["session"]["suppressed_spots"] = 1
                restored.get.return_value = after

                def human(command, feedback):
                    result = copy.deepcopy(after)
                    self.assertEqual((command, feedback), ("SET DIAG MODE", "Diagnostic comments: MODE"))
                    result["status"]["session"]["diagnostic_comments"] = "MODE"
                    return feedback, result

                restored.human.side_effect = human
                with mock.patch.object(driver, "fixture_session", return_value=restored) as factory:
                    if fault is None:
                        result, document = driver.resume_help_fixture(current, record, before, diagnostic="MODE")
                        self.assertIs(result, restored)
                        self.assertEqual(document["status"]["session"]["diagnostic_comments"], "MODE")
                        restored.human.assert_called_once()
                    else:
                        with self.assertRaises(ValueError):
                            driver.resume_help_fixture(current, record, before, diagnostic="MODE")
                        restored.human.assert_not_called()
                    factory.assert_called_once_with(current.suite, current.call)
                    current.close.assert_called_once()
                    restored.get.assert_called_once()

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
        command = b"GET YAML CONFIG SCHEMA 7 ID HOLD19\r\n"
        document = dict(resource="CONFIG", schema_version=7, request_id="HOLD19", configuration={}, status={}, revision="r19")

        def session(prefix=command, reply=raw):
            current = mock.Mock()
            current.call = "VE3ABC-77"
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
        session = dict(callsign="VE3ABC-77", diagnostic_comments="MODE", temporary_defaults=False, suppressed_spots=1,
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
        command = b"GET YAML CONFIG SCHEMA 7 ID HOLD19\r\n"
        document = dict(resource="CONFIG", schema_version=7, request_id="HOLD19", configuration={}, status={}, revision="r19")

        def session(prefix=command):
            current = mock.Mock()
            current.call = "VE3ABC-77"
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

    def test_help_tasks_reject_error_feedback(self):
        query = "HELP FILTERS"
        contract = driver.help_route_contract("FILTERS", "go")
        lines = discovery_help_literal("FILTERS", "go")
        body = lines
        self.assertEqual(driver.decode_help_routing(routing_help_wire(body, query), query,
                                                   contract, 10, 30, True), (body, False))
        with self.assertRaisesRegex(ValueError, "row threshold"):
            driver.decode_help_routing(routing_help_wire(body, query, pause=True), query, contract, 10, 30, True)
        for rejection in ("Unknown help topic: FILTERS", "Unknown state input", "Unknown prefixes", "Invalid configuration", "Usage: Invalid"):
            with self.assertRaisesRegex(ValueError, "rejection feedback"):
                driver.decode_help_routing(routing_help_wire([*body[:-1], rejection, body[-1]], query), query,
                                           contract, 10, 30, True)
        other = discovery_help_literal("DX", "go")
        with self.assertRaisesRegex(ValueError, "rejection feedback"):
            driver.decode_help_routing(routing_help_wire([*other, "Unknown state input"], "HELP DX", pause=True), "HELP DX",
                                       driver.help_route_contract("DX", "go"), 10, 30, True)

    def test_discovery_inventory_is_literal_complete_action_specific_and_alias_qualified(self):
        for dialect in ("go", "cc"):
            for verb in ("PASS", "REJECT"):
                for unknown in (False, True):
                    reply = discovery_literal_reply(verb, unknown, dialect)
                    self.assertEqual(driver.filter_discovery_reply(verb, unknown, dialect), reply)
                    driver.require_discovery_inventory(reply, verb, unknown, dialect)
                    for bad in (reply.replace("No changes applied.", "Applied."), reply.replace("RULE", "BROKEN"), reply + "\nEXTRA"):
                        if bad != reply:
                            with self.assertRaises(ValueError):
                                driver.require_discovery_inventory(bad, verb, unknown, dialect)

    def test_discovery_case_matrix_covers_both_actions_domains_and_existing_cc_forms(self):
        for dialect in ("go", "cc"):
            cases = driver.discovery_feedback_cases(dialect)
            commands = {query: (reply, verb, unknown) for query, reply, verb, unknown in cases}
            self.assertEqual(len(commands), len(cases))
            for verb in ("PASS", "REJECT"):
                self.assertEqual(commands[verb], (discovery_literal_reply(verb, False, dialect), verb, False))
                self.assertEqual(commands[verb + " INVENTED"], (discovery_literal_reply(verb, True, dialect), verb, True))
                for family in ("BAND", "MODE", "SOURCE", "EVENT", "COMMENT", "DXCALL", "DECALL", "CONFIDENCE", "PATH",
                               "DXGRID2", "DEGRID2", "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE",
                               "DESTATE", "MINSNR", "NEARBY", "DXBM", "NOFILTER"):
                    self.assertEqual(commands[verb + " " + family + " ALL"],
                                     (discovery_literal_reply(verb, True, dialect), verb, True))
            self.assertEqual(any(query.startswith("SET/FILTER") for query in commands), dialect == "cc")
            if dialect == "cc":
                for command in ("SET/FILTER COMMENT CQ", "UNSET/FILTER COMMENT CQ", "SET/FILTER NEARBY ON", "SET/FILTER BAND 20"):
                    self.assertIn("No changes applied.", commands[command][0])
                    self.assertNotIn("HELP RULE", commands[command][0])

    def test_discovery_decoder_requires_complete_exact_printable_bounded_feedback(self):
        query, expected = "PASS", discovery_literal_reply("PASS")
        lines = expected.splitlines()
        for echo in (True, False):
            raw = routing_help_wire(lines, query, echo=echo)
            self.assertEqual(driver.decode_discovery_reply(raw, query, expected, echo), lines)
        faults = [routing_help_wire(["Usage: WRONG", *lines[1:]], query),
                  routing_help_wire(lines[:-1], query), routing_help_wire(lines + ["EXTRA"], query),
                  routing_help_wire([lines[0], *lines[2:]], query), routing_help_wire([lines[0], "PASS", *lines[1:]], query),
                  routing_help_wire(["PASS", *lines], query), routing_help_wire(lines, query, pause=True),
                  routing_help_wire([lines[0], "X" * 79, *lines[2:]], query),
                  routing_help_wire(lines, query).replace(b"\r\n", b"\n", 1),
                  routing_help_wire(lines, query).replace(b"RULE", b"RU\x01E", 1), b"X" * 65537]
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
        command = b"GET YAML CONFIG SCHEMA 7 ID DISC18\r\n"
        document = dict(resource="CONFIG", schema_version=7, request_id="DISC18", configuration={}, status={}, revision="r18")

        def session(prefix=command):
            current = mock.Mock()
            current.call = "VE3ABC-75"
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
                current.frame.assert_called_once_with("CONFIG", "DISC18", 7, command.decode().rstrip("\r\n"))
            else:
                with self.assertRaises(ValueError):
                    driver.discovery_readback(current, query, expected)
        for field, value in (("resource", "FILTER"), ("schema_version", True), ("schema_version", "7"),
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
            for key in ("DX", "OVERVIEW", "FILTERS", "FILTER EXAMPLES", "PRESETS", "PASS", "REJECT",
                        "SHOW FILTER" if dialect == "go" else "SHOW/FILTER"):
                lines = discovery_help_literal(key, dialect)
                topic, query = ("" if key == "OVERVIEW" else key), ("HELP " + ("" if key == "OVERVIEW" else key)).rstrip()
                contract = driver.help_route_contract(key, dialect)
                expected_paused = len(lines) >= 10
                decoded, paused = driver.decode_help_routing(routing_help_wire(lines, query, pause=expected_paused),
                                                            query, contract, 10, 30, True)
                self.assertEqual(paused, expected_paused)
                driver.require_discovery_help(decoded, key, dialect)
                claim = "Spot queued guarantees delivery." if key == "DX" else "show FCC mailing-address codes" if key.startswith("SHOW") else "RESET MODE"
                with self.assertRaises(ValueError):
                    driver.require_discovery_help([*decoded, claim], key, dialect)
                required = (("Spot rules (GO only)", "Delivery switches (GO and CC)", "Old category mutation syntax is refused") if key in ("PASS", "REJECT") and dialect == "go" else
                            ("Spot rules (GO only)", "Numbered PASS and REJECT rules are for GO.") if key in ("PASS", "REJECT") else
                            ("Shows canonical numbered rules",) if key == "SHOW FILTER" else
                            ("Shows exact native selections", "Band INHERIT") if key == "SHOW/FILTER" else
                            ("HELP SYMBOLS", "Showing archive spots") if key == "OVERVIEW" else
                            ("station reputation admission", "Spot queued confirms", "Comment: ASCII") if key == "DX" else ())
                for phrase in required:
                    with self.assertRaises(ValueError):
                        driver.require_discovery_help([line for line in lines if phrase not in line], key, dialect)

    def test_discovery_help_semantic_callback_logs_receipt_before_barrier_and_refuses_false_green(self):
        query, lines = "HELP DX", discovery_help_literal("DX", "go")
        raw = routing_help_wire(lines, query, pause=True)
        document = dict(resource="CONFIG", schema_version=7, request_id="DISC18", configuration={}, status={}, revision="r18")
        for corrupt in (False, True):
            current = mock.Mock()
            current.call, current.reader.remote_echo, current.reader.buffer = "VE3ABC-75", True, b""
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            current.suite.log, current.suite.identifier.return_value = io.BytesIO(), "DISC18"
            current.frame.return_value = document, b"GET YAML CONFIG SCHEMA 7 ID DISC18\r\n"

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
        session = dict(callsign="VE3ABC-75", diagnostic_comments="MODE", temporary_defaults=False, suppressed_spots=0,
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
        for dialect in ("go", "cc"):
            expected, document = current_configuration(dialect), current_document(dialect)
            baseline = dict(configuration_version=7, dialect=dialect, settings=copy.deepcopy(expected["settings"]))
            baseline[dialect] = copy.deepcopy(expected["filters"][dialect])
            native = dict(configuration_version=7, configuration=copy.deepcopy(expected),
                          presets={dialect: dict(name="DISC18", baseline=baseline)}, notice={})
            document["status"]["presets"][dialect] = dict(associated=True, name="DISC18", modified=False)
            record, library = mock.Mock(), mock.Mock()
            record.read_bytes.return_value = driver.yaml.safe_dump(native).encode()
            library.read_bytes.return_value = driver.yaml.safe_dump(dict(configuration_version=7, reset_generation=7, presets={"DISC18": baseline})).encode()
            driver.require_discovery_saved(document, expected, record, library, "DISC18")
            for fault in ("name", "tag", "settings", "bank", "library", "inactive"):
                bad_native, bad_doc, bad_library = copy.deepcopy(native), copy.deepcopy(document), {"DISC18": copy.deepcopy(baseline)}
                ref = bad_native["presets"][dialect]
                if fault == "name": ref["name"] = "OTHER"
                elif fault == "tag": ref["baseline"]["dialect"] = "go" if dialect == "cc" else "cc"
                elif fault == "settings": ref["baseline"]["settings"]["grid"] = "IO91"
                elif fault == "bank": ref["baseline"][dialect]["include_beacons"] = True
                elif fault == "library": bad_library["DISC18"]["settings"]["grid"] = "IO91"
                else: bad_native["configuration"]["filters"]["go" if dialect == "cc" else "cc"]["allow_self"] = False
                record.read_bytes.return_value = driver.yaml.safe_dump(bad_native).encode()
                library.read_bytes.return_value = driver.yaml.safe_dump(dict(presets=bad_library)).encode()
                with self.assertRaises(ValueError):
                    driver.require_discovery_saved(bad_doc, expected, record, library, "DISC18")

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

    def test_help_routing_long_response_requires_complete_thresholded_pause_footer(self):
        query, lines = "HELP PASS BAND 40 EXTRA", discovery_help_literal("PASS", "go")
        contract = driver.help_route_contract("PASS", "go")
        raw = routing_help_wire(lines, query, pause=True)
        self.assertEqual(driver.decode_help_routing(raw, query, contract, 10, 30, True), (lines, True))
        for fault in (routing_help_wire(lines, query), raw.replace((PAUSE_FOOTER_END + "\r\n").encode(), b""),
                      routing_help_wire(lines, query, pause=True, seconds=29),
                      raw + b"TRAILING\r\n", routing_help_wire(lines + pause_lines(), query, pause=True),
                      routing_help_wire(["REJECT - Wrong header.", *lines[1:]], query, pause=True)):
            with self.assertRaises(ValueError):
                driver.decode_help_routing(fault, query, contract, 10, 30, True)
        self.assertEqual(driver.decode_help_routing(routing_help_wire(["", *lines], query, pause=True), query, contract, 10, 30, True), (lines, True))

    def test_archive_help_requires_exact_introduction_before_matching_details(self):
        for key in ("SHOW DX", "SHOW/DX"):
            with self.subTest(key=key):
                intro = [f"Example: {key} 10 - Show up to 10 recent spots that match your filters.",
                         f"  {key + ' K1ABC 10':<29}Search for a callsign.",
                         f"  {key + ' 10 BAND 20 MODE CW':<29}Search for CW spots on 20m.",
                         f"  {key + ' 10 COMMENT CQ':<29}Find spots with CQ in the comment.",
                         "These searches don't change your saved filters.", ""]
                contract = driver.help_route_contract(key, "go" if key == "SHOW DX" else "cc")
                body = [contract[0], "Usage: " + contract[1][0],
                        *("       " + usage for usage in contract[1][1:]),
                        "Aliases: " + ", ".join(contract[3]), "Notes:", "  More details."]
                query = "HELP " + key
                lines = intro + body
                self.assertEqual(driver.decode_help_routing(routing_help_wire(lines, query, pause=True),
                                 query, contract, 10, 30, True), (lines, True))
                for fault in (lines[1:], ["Unexpected text", *lines], [*intro[:-1], *body],
                              [*intro, body[0].replace("Alias", "Wrong"), *body[1:]],
                              [*intro, body[0], *body[2:]]):
                    with self.assertRaises(ValueError):
                        driver.decode_help_routing(routing_help_wire(fault, query, pause=True),
                                                   query, contract, 10, 30, True)

    def test_help_routing_independent_topics_cover_source_heads_and_preserve_explicit_unknowns(self):
        self.assertEqual(driver.HELP_SHORTCUT_MODES, ("CW", "FT2", "FT4", "FT8", "PSK", "RTTY"))
        for dialect in ("go", "cc"):
            self.assertEqual(driver.help_route_contract("VALIDATE YAML CONFIG", dialect)[1],
                             ("VALIDATE YAML CONFIG", "VALIDATE YAML CONFIG SCHEMA 7"))
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
                self.assertEqual(topics["GET YAML " + resource + " SCHEMA 7 ID ROUTE18"], "GET YAML " + resource)
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
        self.assertEqual(driver.help_route_contract("PASS", "cc")[0], "PASS - Change separate delivery switches.")
        self.assertEqual(driver.help_route_contract("REJECT", "cc")[0], "REJECT - Change separate delivery switches.")

    def test_help_routing_receipt_completes_and_flushes_before_strict_correlated_barrier(self):
        class Receipt(io.BytesIO):
            flushed = False

            def flush(self):
                self.flushed = True

        query = "HELP SH BUILD EXTRA"
        lines = ["SHOW BUILD - Show binary build metadata.", "Usage: SHOW BUILD", "Aliases: SH BUILD", "Notes:",
                 "  Shows the build version, release tag, and Go version when available."]
        raw = routing_help_wire(lines, query)
        command = b"GET YAML CONFIG SCHEMA 7 ID HELP18\r\n"
        document = dict(resource="CONFIG", schema_version=7, request_id="HELP18", configuration={}, status={}, revision="r18")
        for prefix in (command, b"\r\n" + command, command + b"LEAK\r\n", b"LEAK\r\n" + command,
                       command + command, b"\n" + command, command.replace(b"HELP18", b"OTHER"), b"\r\n" * 32769 + command):
            current = mock.Mock()
            current.call = "VE3ABC-73"
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
                current.frame.assert_called_once_with("CONFIG", "HELP18", 7, command.decode().rstrip("\r\n"))
            else:
                with self.assertRaises(ValueError):
                    driver.help_routing_readback(current, "SH BUILD EXTRA", driver.help_route_contract("SHOW BUILD", "go"), 10, 30)
        for field, value in (("resource", "FILTER"), ("schema_version", True), ("schema_version", "7"),
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
        command = b"GET YAML CONFIG SCHEMA 7 ID HELP18\r\n"
        document = dict(resource="CONFIG", schema_version=7, request_id="HELP18", configuration={}, status={}, revision="r18")
        current = mock.Mock()
        current.call = "VE3ABC-74"
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
        session = dict(callsign="VE3ABC-73", diagnostic_comments="MODE", temporary_defaults=False, suppressed_spots=0,
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
        expected, document = current_configuration(), current_document()
        native = dict(configuration_version=7, configuration=copy.deepcopy(expected), presets={}, notice={})
        record = mock.Mock()
        record.read_bytes.return_value = driver.yaml.safe_dump(native).encode()
        driver.require_help_routing_saved(document, expected, record)
        for field in expected["settings"]:
            bad = copy.deepcopy(native)
            bad["configuration"]["settings"][field] = "CHANGED"
            record.read_bytes.return_value = driver.yaml.safe_dump(bad).encode()
            with self.assertRaises(ValueError):
                driver.require_help_routing_saved(document, expected, record)

    def test_cc_help_decoder_requires_complete_bounded_printable_response_and_count(self):
        lines = cc_help_literal("SET/FILTER")
        raw = cc_help_wire(lines)
        self.assertEqual(driver.decode_cc_help(raw, "HELP SET/FILTER", 10, 30), lines)
        for changed in (raw.replace(b"\r\n", b"\n"), raw + b"LEAK\r\n", raw[:-2],
                        raw.replace(b"native CC", b"native\x01CC"), cc_help_wire(lines, seconds=29),
                        cc_help_wire(lines, pause=False), cc_help_wire(lines, seconds=0),
                        cc_help_wire(["X" * 79] + lines), cc_help_wire(["HELP SET/FILTER"] + lines),
                        cc_help_wire(["WRONG - header"] + lines[1:]),
                        raw.replace((PAUSE_FOOTER_END + "\r\n").encode(), b""), b"\r\n" * 32769 + raw):
            with self.assertRaises(ValueError):
                driver.decode_cc_help(changed, "HELP SET/FILTER", 10, 30)

    def test_cc_help_grammar_requires_independent_categories_usage_and_flag_exceptions(self):
        for verb in ("SET/FILTER", "UNSET/FILTER"):
            lines = cc_help_literal(verb)
            driver.require_cc_help_grammar(lines, verb)
            faults = [lines + ["Usage: " + verb + " COMMENT <phrase>"],
                      ["WRONG - header", *lines[1:]], [line for line in lines if "<switch> [ALL]" not in line],
                      [*lines, "Usage: " + verb + " NEARBY ON|OFF"],
                      [line.replace("PC93 aliases ANNOUNCE.", "PC93 is independent.") for line in lines]]
            if verb == "SET/FILTER":
                faults += [[line for line in lines if "DXBM/REJECT <band-or-slot-list>" not in line],
                           [line.replace("Each geography edit replaces", "Each geography edit appends") for line in lines]]
            for fault in faults:
                with self.assertRaises(ValueError):
                    driver.require_cc_help_grammar(fault, verb)

    def test_cc_task_guides_have_their_own_oracle_not_detailed_grammar(self):
        for topic in ("FILTERS", "FILTER EXAMPLES", "PRESETS"):
            lines = task_help_literal(topic, "cc")
            query = "HELP " + topic
            driver.require_cc_help_grammar(lines, topic)
            self.assertEqual(driver.decode_cc_help(cc_help_wire(lines, query, pause=len(lines) >= 10), query, 10, 30), lines)
            for changed in (["Filter reference:", *lines[1:]], lines[:-1],
                            [line for line in lines if "SHOW/FILTER" not in line] if topic != "PRESETS" else
                            [line for line in lines if "LOAD PRESET DAY" not in line]):
                with self.assertRaises(ValueError):
                    decoded = driver.decode_cc_help(cc_help_wire(changed, query, pause=len(changed) >= 10), query, 10, 30)
                    driver.require_cc_help_grammar(decoded, topic)

    def test_cc_help_receipt_precedes_barrier_and_rejects_hidden_suffix(self):
        query = "HELP SET/FILTER"
        raw = cc_help_wire(cc_help_literal("SET/FILTER"), query)
        command = b"GET YAML CONFIG SCHEMA 7 ID CC17\r\n"
        document = dict(resource="CONFIG", schema_version=7, request_id="CC17", configuration={}, status={}, revision="r17")
        for prefix in (command, command + b"LEAK\r\n", b"LEAK\r\n" + command,
                       command + command, b"\n" + command, b"\r\n" * 32769 + command):
            current = mock.Mock()
            current.call = "VE3ABC-71"
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
        expected, document = current_configuration(), current_document()
        record = mock.Mock()
        native = dict(configuration_version=7, configuration=copy.deepcopy(expected), presets={}, notice={})
        record.read_bytes.return_value = driver.yaml.safe_dump(native).encode()
        driver.require_cc_saved_state(document, expected, record)
        for bank, field, value in (("go", "rules", []), ("go", "include_beacons", 0),
                                   ("cc", "modes", []), ("cc", "sources", []), ("cc", "include_beacons", 0)):
            bad = copy.deepcopy(native)
            bad["configuration"]["filters"][bank][field] = value
            record.read_bytes.return_value = driver.yaml.safe_dump(bad).encode()
            with self.assertRaises(ValueError):
                driver.require_cc_saved_state(document, expected, record)
        record.read_bytes.return_value = driver.yaml.safe_dump(native).encode()
        for field, value in (("active_dialect", "go"), ("presets", {"cc": {}})):
            bad = copy.deepcopy(document)
            bad["status"][field] = value
            with self.assertRaises(ValueError):
                driver.require_cc_saved_state(bad, expected, record)

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
        footer = "Type SHOW DX NEXT to search older spots.\r\n"
        cutoff = "Older archive spots haven't been checked yet.\r\n"
        raw = (query + "\r\n" + HISTORY_ROW + "\r\n" + footer).encode()
        self.assertEqual(driver.decode_history_alias_page(raw, query, False), ([HISTORY_ROW], True, False))
        older = ("Older retained history page:\r\n" + HISTORY_ROW + "\r\n" + footer).encode()
        self.assertEqual(driver.decode_history_alias_page(older, query, True), ([HISTORY_ROW], True, False))
        self.assertEqual(driver.decode_history_alias_page((cutoff + footer).encode(), query, False), ([], True, True))
        self.assertEqual(driver.decode_history_alias_page(b"Older retained history page:\r\nEnd of retained history search.\r\n",
                                                        query, True), ([], False, False))
        faults = ((raw.replace(b"\r\n", b"\n"), False),
                  (raw + b"LEAK\r\n", False), (raw + footer.encode(), False),
                  ((query + "\r\n").encode() + raw, False),
                  (raw.replace(footer.encode(), b"Type SHOW DX NEXT to search older spots. EXTRA\r\n"), False),
                  (footer.encode(), False), (b"End of retained history search.\r\n", False),
                  (older, False), (raw, True),
                  (b"Older retained history page:\r\nNo matching retained spots.\r\n", True),
                  (raw.replace(footer.encode(), cutoff.encode()), False),
                  ((cutoff + "No matching retained spots.\r\n").encode(), False),
                  (raw[:-len(footer)], False))
        for data, continuation in faults:
            with self.assertRaises(ValueError):
                driver.decode_history_alias_page(data, query, continuation)

    def test_archive_pages_send_plain_next_until_exhausted_and_refuse_no_progress(self):
        query = "SHOW DX W6ABC 10 COMMENT LABEL"
        first = (b"Older archive spots haven't been checked yet.\r\n",
                 b"Type SHOW DX NEXT to search older spots.\r\n")
        heading = b"Older retained history page:\r\n"
        row = (HISTORY_ROW + "\r\n").encode()
        for defect in (None, "duplicate", "page_budget", "time_budget"):
            with self.subTest(defect=defect):
                session = mock.Mock()
                session.call, session.suite.log = "N0CALL-1", io.BytesIO()
                session.reader.buffer = bytearray()
                if defect == "duplicate":
                    lines = (row, *first, heading, row, b"End of retained history search.\r\n")
                elif defect == "page_budget":
                    lines = (*first, *((heading, *first) * (driver.harness.HISTORY_MAX_PAGES - 1)))
                elif defect == "time_budget":
                    lines = first
                else:
                    lines = (*first, heading, row, b"End of retained history search.\r\n")
                session.reader.line.side_effect = lines
                with mock.patch.object(driver, "complete_reply_config"):
                    if defect == "time_budget":
                        with mock.patch.object(driver.time, "monotonic", side_effect=[0, 0, 120]):
                            with self.assertRaisesRegex(ValueError, "time budget"):
                                driver.archive_rows(session, "LABEL")
                        self.assertEqual(session.send.call_args_list, [mock.call(query + "\r\n")])
                        continue
                    if defect:
                        with self.assertRaisesRegex(ValueError, "repeated a row|page budget"):
                            driver.archive_rows(session, "LABEL")
                    else:
                        self.assertEqual(driver.archive_rows(session, "LABEL"), [HISTORY_ROW])
                continuations = driver.harness.HISTORY_MAX_PAGES - 1 if defect == "page_budget" else 1
                self.assertEqual(session.send.call_args_list, [mock.call(query + "\r\n")] +
                                 [mock.call("SHOW DX NEXT\r\n")] * continuations)

    def test_history_alias_paging_sends_plain_show_mydx_next(self):
        query = "SHOW MYDX NEXT"
        session = mock.Mock()
        session.call, session.suite.log = "N0CALL-1", io.BytesIO()
        session.reader.buffer = bytearray()
        session.reader.line.side_effect = [
            b"Older retained history page:\r\n",
            b"Older archive spots haven't been checked yet.\r\n",
            b"Type SHOW DX NEXT to search older spots.\r\n",
            b"Older retained history page:\r\n",
            (HISTORY_ROW + "\r\n").encode(),
            b"End of retained history search.\r\n",
        ]
        with mock.patch.object(driver, "complete_reply_config", return_value={}):
            self.assertEqual(list(driver.history_pages(session, query, query)),
                             [([], True, True, {}), ([HISTORY_ROW], False, False, {})])
        self.assertEqual(session.send.call_args_list, [mock.call("SHOW MYDX NEXT\r\n")] * 2)

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
        command = b"GET YAML CONFIG SCHEMA 7 ID BARRIER16\r\n"
        document = dict(resource="CONFIG", schema_version=7, request_id="BARRIER16", configuration={}, status={}, revision="r16")

        def session(prefix, readback=document):
            current = mock.Mock()
            current.reader.line.side_effect = raw.splitlines(keepends=True)
            current.reader.buffer = b""
            current.reader.remote_echo = True
            current.suite.identifier.return_value = "BARRIER16"
            current.suite.log = io.BytesIO()
            current.frame.return_value = (copy.deepcopy(readback), prefix)
            return current

        self.assertEqual(driver.history_alias_page(session(command), query), ([HISTORY_ROW], False, False, document))
        for prefix in (b"LEAK\r\n" + command, command + b"LEAK\r\n", command + command,
                       b"\n" + command, b"\r\n" * 32769 + command):
            with self.assertRaises(ValueError):
                driver.history_alias_page(session(prefix), query)
        for readback in (dict(error="refused"), dict(configuration={}, status={}, revision="")):
            with self.assertRaises(ValueError):
                driver.history_alias_page(session(command, readback), query)

    def test_taxonomy_rules_require_canonical_live_and_saved_names_and_booleans(self):
        for dialect in ("go", "cc"):
            document = native_oracle_document(dialect)
            if dialect == "go":
                document["configuration"]["filters"]["go"]["rules"] = [
                    dict(slot=0, action="pass", expr="MODE=TEST_MODE,CW AND EVENT=TEST_EVENT")]
            else:
                document["configuration"]["filters"]["cc"]["modes"] = ["CW", "TEST_MODE"]
            document["configuration"]["filters"][dialect]["include_beacons"] = False
            expected = copy.deepcopy(document["configuration"]["filters"][dialect])
            record = mock.Mock()
            record.read_bytes.return_value = native_oracle_bytes(document)
            driver.require_taxonomy_rules(document, expected, record)
            for fault in ("wrong_name", "missing_name", "integer_control", "wrong_owner"):
                changed = copy.deepcopy(document)
                bank = changed["configuration"]["filters"][dialect]
                if fault == "wrong_owner":
                    changed["status"]["active_dialect"] = "cc" if dialect == "go" else "go"
                elif fault == "integer_control":
                    bank["include_beacons"] = 0
                elif dialect == "go":
                    bank["rules"][0]["expr"] = "MODE=CW" if fault == "missing_name" else "MODE=UNDERSCORE_REMOVED"
                else:
                    bank["modes"] = ["CW"] if fault == "missing_name" else ["CW", "UNDERSCORE_REMOVED"]
                with self.subTest(dialect=dialect, fault=fault), self.assertRaises(ValueError):
                    driver.require_taxonomy_rules(changed, expected, record)
            for bank_name in ("go", "cc"):
                changed = copy.deepcopy(document)
                changed["configuration"]["filters"][bank_name]["allow_toxic"] = True
                record.read_bytes.return_value = native_oracle_bytes(changed)
                with self.subTest(dialect=dialect, disk_bank=bank_name), self.assertRaises(ValueError):
                    driver.require_taxonomy_rules(document, expected, record)
            record.read_bytes.return_value = native_oracle_bytes(document)


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
                                      presets={"go": {"name": "SOLAR14"}, "cc": {"name": "INACTIVE"}}))
            wording = "Every 30 minutes when conditions are active" if available else "30 minutes; unavailable on this server"
            text = "Solar         " + wording
            driver.require_solar_readback(before, copy.deepcopy(before), record, saved, available, text)
            for fault in ("configuration", "effective", "revision", "preset", "inactive_preset", "wording", "duplicate", "disk"):
                after, output = copy.deepcopy(before), text
                record.read_bytes.return_value = saved
                if fault == "configuration":
                    after["configuration"]["settings"]["solar_summary_minutes"] = 0
                elif fault == "effective":
                    after["status"]["effective"]["solar_summary_minutes"] = 0 if available else 30
                elif fault == "revision":
                    after["revision"] = "changed"
                elif fault == "preset":
                    after["status"]["presets"]["go"] = {}
                elif fault == "inactive_preset":
                    after["status"]["presets"]["cc"] = {}
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
                + "\n".join(pause_lines()))
        driver.require_large_dedupe_output(text, "XXXX")
        for old, new in (("; effective MED", "; effective FAST"), ('+ "XX"', '+ "X"'),
                         ("Configured choice is unrecognized", "Configured choice is unavailable"),
                         (PAUSE_FOOTER_END, "")):
            with self.assertRaises(ValueError):
                driver.require_large_dedupe_output(text.replace(old, new), "XXXX")

    def test_large_dedupe_live_restart_distinguishes_admitted_value_from_protected_login(self):
        for fault in (None, "positive_effective", "negative_admitted", "negative_fallback", "negative_record"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "users").mkdir()
                documents, sessions = [], []
                for count, call in ((40000, "VE3ABC-63"), (49366, "VE3ABC-64")):
                    owner = current_document("go")
                    owner["configuration"]["settings"]["dedupe_policy"] = "X" * count
                    raw = driver.yaml.safe_dump(dict(configuration_version=7,
                        configuration=owner["configuration"], presets={}, notice={})).encode()
                    record = root / "users" / (call + ".yaml")
                    record.write_bytes(raw)
                    document = copy.deepcopy(owner)
                    document["status"].update(configured=copy.deepcopy(owner["configuration"]["settings"]),
                        session=dict(temporary_defaults=False), effective=dict(dedupe_policy="MED"),
                        server=dict(default_dedupe_policy="SLOW"))
                    if count == 49366:
                        document["configuration"]["settings"]["dedupe_policy"] = "SLOW"
                        document["status"]["configured"]["dedupe_policy"] = "SLOW"
                        document["status"]["session"]["temporary_defaults"] = True
                        document["status"]["effective"]["dedupe_policy"] = "SLOW"
                        if fault == "negative_admitted":
                            document["status"]["session"]["temporary_defaults"] = False
                        elif fault == "negative_fallback":
                            document["status"]["effective"]["dedupe_policy"] = "FAST"
                    elif fault == "positive_effective":
                        document["status"]["effective"]["dedupe_policy"] = "SLOW"
                    session = mock.Mock()
                    session.get.return_value = document
                    session.login_greeting = b"Could not load or upgrade your saved settings. Using temporary defaults."
                    if count == 49366 and fault == "negative_record":
                        session.get.side_effect = lambda record=record, raw=raw, document=document: (record.write_bytes(raw + b"#changed\n"), document)[1]
                    documents.append(document)
                    sessions.append(session)
                text = ('Dedupe: "' + "X" * 40000 + '"; effective MED (grid2)\n'
                        '(fast=on med=on slow=on)\nConfigured choice is unrecognized\n'
                        + "\n".join(pause_lines()))
                with mock.patch.object(driver, "fixture_session", side_effect=sessions), \
                     mock.patch.object(driver, "large_dedupe_reply", return_value=(text, documents[0])) as positive, \
                     mock.patch.object(driver, "exact_human_reply", return_value=("bounded fallback", documents[1])) as fallback:
                    if fault is None:
                        driver.large_dedupe_cases(mock.Mock(), root, 1)
                        positive.assert_called_once_with(sessions[0], PAUSE_FOOTER_END)
                        fallback.assert_called_once_with(sessions[1], "SHOW DEDUPE", "Dedupe: SLOW (cqzone) (fast=on med=on slow=on)")
                    else:
                        with self.assertRaises(ValueError):
                            driver.large_dedupe_cases(mock.Mock(), root, 1)

    def test_large_dedupe_reader_checks_wire_before_correlated_get(self):
        current = mock.Mock()
        current.call, current.suite.log = "N0CALL-1", io.BytesIO()
        current.suite.identifier.return_value = "BARRIER13"
        current.reader.remote_echo = True
        expected = dict(resource="CONFIG", schema_version=7, request_id="BARRIER13", configuration={}, status={}, revision="r13")
        current.frame.return_value = (expected, b"\r\nGET YAML CONFIG SCHEMA 7 ID BARRIER13\r\n\r\n")
        current.reader.buffer = b"pending"
        current.reader.line.side_effect = [b"Dedupe: control\r\n", b"Done\r\n"]
        text, document = driver.large_dedupe_reply(current, "Done")
        self.assertEqual(text, "Dedupe: control\nDone")
        current.frame.assert_called_once_with("CONFIG", "BARRIER13", 7, "GET YAML CONFIG SCHEMA 7 ID BARRIER13")
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
        document = dict(resource="CONFIG", schema_version=7, request_id="BARRIER13", configuration={}, status={}, revision="r13")
        echo = b"GET YAML CONFIG SCHEMA 7 ID BARRIER13\r\n"
        for final in ("Readback failed: response exceeds 65,536 bytes.", PAUSE_FOOTER_END):
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
        document = dict(resource="CONFIG", schema_version=7, request_id="BARRIER13", configuration={}, status={}, revision="r13")
        current.frame.return_value = document, b"GET YAML CONFIG SCHEMA 7 ID BARRIER13\r\n"
        with self.assertRaisesRegex(ValueError, "trailing content"):
            driver.dedupe_read_barrier(current, 100)
        for bad in ({}, {**document, "revision": ""}, {**document, "error": {"message": "failed"}}):
            current.frame.return_value = bad, b"\r\n"
            with self.assertRaisesRegex(ValueError, "valid readback"):
                driver.dedupe_read_barrier(current, 100)

    def test_dedupe_readback_oracle_requires_saved_effective_and_unchanged_owners(self):
        before = dict(configuration={"settings": {"dedupe_policy": "SLOW"}}, revision="r1",
                      status=dict(configured={"dedupe_policy": "SLOW"}, active_dialect="go",
                                  effective={"dedupe_policy": "FAST"}, presets={"go": {"name": "DEDUPE13"}, "cc": {"associated": False}}))
        text = "Dedupe        SLOW; effective FAST\n              Configured choice is unavailable on this server"
        record = mock.Mock()
        record.read_bytes.return_value = b"dedupe_policy: SLOW\n"
        driver.require_dedupe_readback(before, copy.deepcopy(before), record, record.read_bytes(), text)
        for field in ("configured", "status_configured", "active_dialect", "effective", "revision", "preset", "disk", "wording"):
            value = copy.deepcopy(before)
            record.read_bytes.return_value = b"changed" if field == "disk" else b"dedupe_policy: SLOW\n"
            if field == "configured":
                value["configuration"]["settings"]["dedupe_policy"] = "FAST"
            elif field == "status_configured":
                value["status"]["configured"]["dedupe_policy"] = "FAST"
            elif field == "active_dialect":
                value["status"]["active_dialect"] = "cc"
            elif field == "effective":
                value["status"]["effective"]["dedupe_policy"] = "SLOW"
            elif field == "revision":
                value["revision"] = "r2"
            elif field == "preset":
                value["status"]["presets"] = {}
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
            session.get.return_value = native_oracle_document()
            for wire_value, stored_value in ((0, 0), (False, 0), (0.0, 0), (0, False),
                                             (0, 0.0), (0, "0"), (0, None)):
                document = native_oracle_document()
                document["configuration"]["settings"][field] = wire_value
                stored = native_oracle_document()
                stored["configuration"]["settings"][field] = stored_value
                record.read_bytes.return_value = native_oracle_bytes(stored)
                with mock.patch.object(driver, "exact_human_reply", return_value=("Zero control", document)):
                    if type(wire_value) is int and type(stored_value) is int:
                        driver.require_setting_control(session, record, "Zero", "Zero control", field, 0)
                    else:
                        with self.assertRaisesRegex(ValueError, "immediately saved value"):
                            driver.require_setting_control(session, record, "Zero", "Zero control", field, 0)
            stored = native_oracle_document()
            del stored["configuration"]["settings"][field]
            record.read_bytes.return_value = native_oracle_bytes(stored)
            document = native_oracle_document()
            with mock.patch.object(driver, "exact_human_reply", return_value=("Zero control", document)), \
                    self.assertRaisesRegex(ValueError, "immediately saved value"):
                driver.require_setting_control(session, record, "Zero", "Zero control", field, 0)


    def test_setting_refusal_oracle_includes_session_only_diagnostics(self):
        session = mock.Mock()
        session.last_configuration = {"status": {"session": {"diagnostic_comments": "SOURCE"}}}
        with mock.patch.object(driver, "require_command_refusal", return_value=
                               {"status": {"session": {"diagnostic_comments": "OFF"}}}) as common:
            with self.assertRaisesRegex(ValueError, "session diagnostic mode"):
                driver.require_setting_argument_refusal(session, "SET DIAG OFF EXTRA", mock.Mock(), "Usage")
            common.assert_called_once()
        with mock.patch.object(driver, "require_command_refusal", return_value=
                               {"status": {"session": {"diagnostic_comments": "SOURCE"}}}):
            driver.require_setting_argument_refusal(session, "SET DIAG OFF EXTRA", mock.Mock(), "Usage")
        session.get.assert_not_called()

    def test_setting_control_requires_live_value_and_immediate_disk(self):
        before = native_oracle_document()
        after = copy.deepcopy(before)
        after["configuration"]["settings"]["noise_class"] = "URBAN"
        after["status"]["effective"]["noise_class"] = "URBAN"
        after["revision"] = "revision-2"
        session, record = mock.Mock(), mock.Mock()
        for live, disk in (("URBAN", "URBAN"), ("RURAL", "URBAN"), ("URBAN", "RURAL")):
            value, stored = copy.deepcopy(after), copy.deepcopy(after)
            value["configuration"]["settings"]["noise_class"] = live
            stored["configuration"]["settings"]["noise_class"] = disk
            session.get.return_value = before
            record.read_bytes.return_value = native_oracle_bytes(stored)
            with mock.patch.object(driver, "exact_human_reply", return_value=("Noise class set to URBAN", value)):
                if live == disk == "URBAN":
                    driver.require_setting_control(session, record, "SET NOISE URBAN", "Noise class set to URBAN", "noise_class", "URBAN")
                else:
                    with self.assertRaisesRegex(ValueError, "immediately saved value"):
                        driver.require_setting_control(session, record, "SET NOISE URBAN", "Noise class set to URBAN", "noise_class", "URBAN")
        value = copy.deepcopy(after)
        value["status"]["effective"]["noise_class"] = "RURAL"
        record.read_bytes.return_value = native_oracle_bytes(after)
        with mock.patch.object(driver, "exact_human_reply", return_value=("Noise class set to URBAN", value)):
            with self.assertRaisesRegex(ValueError, "runtime state"):
                driver.require_setting_control(session, record, "SET NOISE URBAN", "Noise class set to URBAN", "noise_class", "URBAN")
        for channel in ("configuration", "revision", "effective", "go_preset", "cc_preset", "disk", "diagnostics"):
            value = copy.deepcopy(before)
            value["status"]["session"]["diagnostic_comments"] = "CONF"
            record.read_bytes.side_effect = [b"unchanged", b"changed" if channel == "disk" else b"unchanged"]
            if channel in ("configuration", "revision"):
                value[channel] = "changed"
            elif channel == "effective":
                value["status"][channel] = "changed"
            elif channel.endswith("_preset"):
                value["status"]["presets"][channel.split("_")[0]]["name"] = "changed"
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
        before = native_oracle_document()
        for fault in (None, "response", "configuration", "revision", "effective", "go_preset", "cc_preset", "disk"):
            with self.subTest(fault=fault):
                session, record = mock.Mock(), mock.Mock()
                session.get.return_value = copy.deepcopy(before)
                after = copy.deepcopy(before)
                response = "Usage: SET/BEACON\nNo changes applied."
                if fault == "response":
                    response = "enabled"
                elif fault in ("configuration", "revision"):
                    after[fault] = "changed"
                elif fault == "effective":
                    after["status"][fault] = "changed"
                elif fault in ("go_preset", "cc_preset"):
                    after["status"]["presets"][fault.split("_")[0]]["name"] = "changed"
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

        document = dict(resource="CONFIG", request_id="End-1", schema_version=7, revision="r", configuration={}, status={})
        session.suite.identifier.return_value = "End-1"
        session.reader.remote_echo = True
        for prefix in (b"", b"hidden row\r\n", b"GET YAML CONFIG SCHEMA 7 ID Other\r\n"):
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
        document = dict(resource="CONFIG", request_id="End-1", schema_version=7, revision="r", configuration={}, status={})
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
                                  ("send", "GET YAML CONFIG SCHEMA 7 ID End-1\r\n"), ("frame",)])
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
        document = native_oracle_document()
        expected = copy.deepcopy(document["configuration"]["filters"]["go"])
        expected["rules"] = [dict(slot=0, action="pass", expr="DXCALL=W6* AND DECALL=K1*"),
                             dict(slot=1, action="reject", expr="DXCALL=N0XYZ OR DECALL=N0XYZ")]
        document["configuration"]["filters"]["go"] = copy.deepcopy(expected)
        record = mock.Mock()
        record.read_bytes.return_value = native_oracle_bytes(document)
        driver.require_callsign_rules(document, expected, record)
        for index, field in ((0, "DXCALL"), (0, "DECALL"), (1, "DXCALL"), (1, "DECALL")):
            changed = copy.deepcopy(document)
            rule = changed["configuration"]["filters"]["go"]["rules"][index]
            rule["expr"] = rule["expr"].replace(field + "=", field + "<>by_error,")
            with self.assertRaisesRegex(ValueError, "callsign rules"):
                driver.require_callsign_rules(changed, expected, record)
        stored = copy.deepcopy(document)
        stored["configuration"]["filters"]["go"]["rules"].pop()
        record.read_bytes.return_value = native_oracle_bytes(stored)
        with self.assertRaisesRegex(ValueError, "not saved before disconnect"):
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
        document = native_oracle_document()
        record = mock.Mock()
        record.read_bytes.return_value = native_oracle_bytes(document)
        driver.require_grid_configuration(document, "FN31", record)
        for owner in (document["configuration"]["settings"], document["status"]["effective"]):
            owner["grid"] = "FN31PRJUNK"
            with self.assertRaisesRegex(ValueError, "GRID canonical selection"):
                driver.require_grid_configuration(document, "FN31", record)
            owner["grid"] = "FN31"
        stored = copy.deepcopy(document)
        stored["configuration"]["settings"]["grid"] = "FN31PRJUNK"
        record.read_bytes.return_value = native_oracle_bytes(stored)
        with self.assertRaisesRegex(ValueError, "GRID canonical selection"):
            driver.require_grid_configuration(document, "FN31", record)
        stored = copy.deepcopy(document)
        stored["configuration"]["filters"]["cc"]["modes"] = []
        record.read_bytes.return_value = native_oracle_bytes(stored)
        with self.assertRaisesRegex(ValueError, "not saved before disconnect"):
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
        for dialect in ("go", "cc"):
            document = native_oracle_document(dialect)
            expected = copy.deepcopy(document["configuration"]["filters"][dialect])
            driver.require_mode_rules(document, expected)
            bank = document["configuration"]["filters"][dialect]
            if dialect == "go":
                bank["rules"] = [dict(slot=0, action="pass", expr="MODE=CW")]
            else:
                bank["modes"] = ["CW"]
            with self.assertRaisesRegex(ValueError, "exact native bank"):
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
        owner = driver.native_record(dict(filters=dict(go=dict(rules=[]), cc=dict(modes=["CW"])),
                                          settings=dict(dialect="go")))
        base = driver.yaml.safe_dump(owner).encode("ascii")
        raw = driver.refused_resource_record("RAW", base)
        self.assertEqual(len(raw), 1_048_577)
        self.assertEqual(driver.stored_document(raw), owner)
        compact = base + b"# measured compact canonical candidate\n"
        self.assertEqual(driver.refused_resource_record("CANONICAL", base, compact), compact)
        with self.assertRaises(ValueError):
            driver.refused_resource_record("CANONICAL", base)
        work = driver.refused_resource_record("WORK", base)
        decoded = driver.stored_document(work)
        self.assertEqual(work.count(b"*text"), 69)
        self.assertNotIn("legacy", decoded)
        self.assertEqual(decoded["configuration"], owner["configuration"])
        self.assertEqual(len(decoded["recent_ips"]), 70)
        self.assertTrue(all(len(ip) == 32768 for ip in decoded["recent_ips"]))
        self.assertGreater(71 * 32768, 2_097_152)
        self.assertLess(5 * 32768 + len(base), 1_048_576)
        with self.assertRaisesRegex(ValueError, "unknown resource fixture"):
            driver.refused_resource_record("OTHER", base)

    def test_resource_cause_requires_exact_reason_and_fitting_control(self):
        from types import SimpleNamespace
        base = driver.yaml.safe_dump(driver.native_record(dict(filters=dict(go=dict(rules=[]), cc={}),
                                                               settings=dict(dialect="go")))).encode()
        def receipt(error="", admission=False):
            return dict(measurements={"input.yaml": dict(error=error, admission=admission)})
        sizing = dict(artifacts={"canonical.yaml": b"X" * 1_048_577, "compact.yaml": base})
        expected = [receipt(), sizing,
                    receipt("v7 document exceeds 1048576 bytes", True),
                    receipt("v7 document exceeds 1048576 bytes", True),
                    receipt("stored configuration decoding work exceeds 2097152 units", True),
                    dict(measurements={
                        "fit.yaml": dict(error="", frame_bytes=262100, canonical_bytes=196400),
                        "over.yaml": dict(error="response exceeds 65,536 bytes; request a smaller filter category or reduce the configuration",
                                          admission=True, reference_bytes=262200, canonical_bytes=196401)}),
                    dict(measurements={})]
        for index in range(6):
            replies = copy.deepcopy(expected)
            if index == 1:
                replies[1]["artifacts"]["canonical.yaml"] = b"X" * 1_048_576
            elif index == 5:
                replies[5]["measurements"]["over.yaml"]["reference_bytes"] = 262144
            else:
                replies[index] = receipt("wrong layer", True)
            suite = SimpleNamespace(binary="binary", source={}, resource_seed=base)
            with mock.patch.object(driver, "resource_fixture", side_effect=replies), \
                    mock.patch.object(driver, "require_dedupe_owner_receipt"), self.assertRaises(ValueError):
                driver.resource_vectors(suite, Path("unused"))
        suite = SimpleNamespace(binary="binary", source={}, resource_seed=base)
        with mock.patch.object(driver, "resource_fixture", side_effect=expected) as measure, \
                mock.patch.object(driver, "require_dedupe_owner_receipt") as dedupe:
            vectors = driver.resource_vectors(suite, Path("unused"))
            self.assertEqual(set(vectors), {"RAW", "CANONICAL", "WORK"})
            self.assertIs(driver.resource_vectors(suite, Path("unused")), vectors)
            self.assertEqual(measure.call_count, 7)
            dedupe.assert_called_once_with(expected[-1])

    def test_dedupe_owner_receipt_requires_lossless_fit_and_exact_final_refusal(self):
        chunks = ["X" * 60] * 819 + ["X" * 30, "X" * 30]
        lines = ['Dedupe: "' + chunks[0] + '"'] + ['        + "' + value + '"' for value in chunks[1:]]
        lines[-1] += "; effective MED (grid2)"
        lines += ["        (fast=on med=on slow=on)", "        Configured choice is unrecognized"]
        lines += pause_lines()
        fit = ("\r\n".join(lines) + "\r\n").encode()
        over = b"Readback failed: response exceeds 65,536 bytes.\r\n"
        receipt = dict(artifacts={"fit.txt": fit, "over.txt": over}, measurements={
            "fit.txt": dict(raw_bytes=49200, frame_bytes=len(fit), canonical_bytes=len(fit)-200, admission=False),
            "over.txt": dict(raw_bytes=49366, frame_bytes=len(over), canonical_bytes=65536, admission=True)})
        driver.require_dedupe_owner_receipt(receipt)
        for fault in ("count", "wire_size", "classification", "wrong_boundary", "partial", "crlf", "value", "footer", "width"):
            changed = copy.deepcopy(receipt)
            if fault == "count":
                changed["measurements"]["fit.txt"]["raw_bytes"] = 49199
            elif fault == "wire_size":
                changed["measurements"]["fit.txt"]["frame_bytes"] += 1
            elif fault == "classification":
                changed["measurements"]["over.txt"]["admission"] = False
            elif fault == "wrong_boundary":
                changed["measurements"]["over.txt"]["canonical_bytes"] = 65535
            else:
                key = "over.txt" if fault == "partial" else "fit.txt"
                wire = changed["artifacts"][key]
                if fault == "partial":
                    wire = b"Partial output\r\n" + wire
                elif fault == "crlf":
                    wire = wire.replace(b"\r\n", b"\n", 1)
                elif fault == "value":
                    wire = wire.replace(b"XXX", b"XX", 1)
                elif fault == "width":
                    wire = b"A" * 79 + b"\r\n" + wire
                else:
                    wire = wire.replace((PAUSE_FOOTER_END + "\r\n").encode(), b"")
                changed["artifacts"][key] = wire
                changed["measurements"][key]["frame_bytes"] = len(wire)
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                driver.require_dedupe_owner_receipt(changed)

    def test_resource_transaction_checker_refuses_each_missing_effect_channel(self):
        before = dict(configuration={"filters": {}}, revision="epoch-1",
                      status=dict(presets=dict(go=dict(associated=False), cc=dict(associated=False)), effective=dict(grid="FN31"),
                                  session=dict(temporary_defaults=False)))
        record, library = mock.Mock(), mock.Mock()
        record.read_bytes.return_value, library.read_bytes.return_value = b"record", b"library"
        driver.require_transaction_unchanged(before, before, record, b"record", library, b"library")
        for channel in ("configuration", "revision", "preset", "inactive_preset", "effective", "protection", "record", "library"):
            after = copy.deepcopy(before)
            record.read_bytes.return_value, library.read_bytes.return_value = b"record", b"library"
            if channel == "configuration":
                after["configuration"]["filters"]["comments"] = ["CHANGED"]
            elif channel == "revision":
                after["revision"] = "epoch-2"
            elif channel == "preset":
                after["status"]["presets"]["go"]["associated"] = True
            elif channel == "inactive_preset":
                after["status"]["presets"]["cc"]["associated"] = True
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


class CommandResourceReceipts(unittest.TestCase):
    """Receipt integrity only; native boundary behavior is qualified separately."""

    def setUp(self):
        directory = support.ROOT / ".tmp" / "resource-receipt-tests"
        directory.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=directory)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.binary = self.root / "fixture.test.exe"
        self.binary.write_bytes(b"offline receipt fixture executable identity")
        self.source = {"digest": "1" * 64}
        self.private = self.root / "direct"
        self.private.mkdir()
        (self.private / "input.yaml").write_bytes(b"offline source owner")
        self.spec = {"nonce": "f" * 32, "root": str(self.private),
                     "source_digest": self.source["digest"],
                     "input_hash": support.digest(self.private / "input.yaml"),
                     "operation": "profile_at_size", "target_bytes": 1048568}
        self.binary_hash = support.digest(self.binary)

    def receipt(self, spec=None, private=None):
        spec = self.spec if spec is None else spec
        private = self.private if private is None else private
        names = {"profile_admission": ("input.yaml",),
                 "profile_at_size": ("canonical.yaml", "compact.yaml"),
                 "config_frames": ("fit.yaml", "over.yaml"),
                 "dedupe_frames": ("fit.txt", "over.txt")}[spec["operation"]]
        measurements = {}
        artifacts = {}
        for name in names:
            value = b"offline measured owner: " + name.encode("ascii")
            if name != "input.yaml":
                (private / name).write_bytes(value)
                artifacts[name] = {"bytes": len(value), "sha256": support.digest(private / name)}
            measurements[name] = {"raw_bytes": len(value), "canonical_bytes": len(value),
                                  "frame_bytes": 0, "reference_bytes": 0,
                                  "admission": False, "error": ""}
        return {**{key: spec[key] for key in
                   ("nonce", "source_digest", "input_hash", "operation", "target_bytes")},
                "binary_hash": self.binary_hash, "pid": 123,
                "measurements": measurements, "artifacts": artifacts}

    def verify(self, receipt):
        return support.verify_resource_receipt(receipt, self.spec, self.private, self.binary_hash)

    def command_stub(self, *, change=None, output=None, receipt_mode="normal"):
        def execute(args, *, timeout, cwd, env):
            self.assertEqual(args[0], str(self.binary))
            self.assertIn("-test.run=^TestCheckoutCommandResourceFixture$", args)
            self.assertEqual(timeout, 60)
            self.assertEqual(cwd, support.ROOT / "telnet")
            path = Path(env["GOCLUSTER_COMMAND_RESOURCE_FIXTURE"])
            spec = json.loads(path.read_text(encoding="utf-8"))
            private = path.parent
            receipt = self.receipt(spec, private)
            if change is not None:
                change(receipt)
            if receipt_mode == "oversized":
                (private / "receipt.json").write_text(" " * 16385, encoding="ascii")
            elif receipt_mode == "normal":
                (private / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")
            if output is not None:
                return output(spec)
            return ("CHECKOUT_RESOURCE_FIXTURE_EXECUTED " + spec["nonce"] +
                    "\n--- PASS: TestCheckoutCommandResourceFixture")
        return execute

    def run_wrapper(self, operation="profile_at_size"):
        return support.resource_fixture(self.binary, self.root, self.source, operation,
                                        input_bytes=b"" if operation == "dedupe_frames" else b"offline source owner",
                                        target_bytes=1048568 if operation == "profile_at_size" else 0)

    def test_verified_wrapper_success_for_each_fixed_operation(self):
        for operation in ("profile_admission", "profile_at_size", "config_frames", "dedupe_frames"):
            with self.subTest(operation=operation), mock.patch.object(
                    support, "command", side_effect=self.command_stub()) as command:
                result = self.run_wrapper(operation)
                self.assertEqual(result["operation"], operation)
                self.assertEqual(result["source_digest"], "1" * 64)
                self.assertEqual(result["binary_hash"], self.binary_hash)
                self.assertTrue(all(type(value) is bytes for value in result["artifacts"].values()))
                command.assert_called_once()

    def test_each_provenance_mismatch_is_rejected(self):
        for key in ("nonce", "source_digest", "binary_hash", "input_hash", "operation", "target_bytes", "pid"):
            with self.subTest(field=key):
                receipt = self.receipt()
                receipt[key] = 0 if key == "pid" else "substituted"
                with self.assertRaisesRegex(ValueError, "provenance mismatch"):
                    self.verify(receipt)

    def test_input_replacement_is_rejected(self):
        receipt = self.receipt()
        (self.private / "input.yaml").write_bytes(b"substituted source owner")
        with self.assertRaisesRegex(ValueError, "changed input bytes"):
            self.verify(receipt)

    def test_output_digest_and_size_mismatches_are_rejected(self):
        for mutation in ("content", "digest", "size"):
            with self.subTest(mutation=mutation):
                receipt = self.receipt()
                descriptor = receipt["artifacts"]["canonical.yaml"]
                if mutation == "content":
                    original = (self.private / "canonical.yaml").read_bytes()
                    (self.private / "canonical.yaml").write_bytes(b"X" * len(original))
                elif mutation == "digest":
                    descriptor["sha256"] = "0" * 64
                else:
                    descriptor["bytes"] += 1
                with self.assertRaisesRegex(ValueError, "artifact (digest|size) mismatch"):
                    self.verify(receipt)

    def test_missing_and_oversized_receipts_are_rejected(self):
        for mode, error in (("missing", FileNotFoundError), ("oversized", ValueError)):
            with self.subTest(mode=mode), mock.patch.object(
                    support, "command", side_effect=self.command_stub(receipt_mode=mode)):
                if mode == "oversized":
                    with self.assertRaisesRegex(error, "receipt exceeds bound"):
                        self.run_wrapper()
                else:
                    with self.assertRaises(error):
                        self.run_wrapper()

    def test_wrong_measurement_sets_are_rejected(self):
        for mutation in ("missing", "unexpected", "wrong-type"):
            with self.subTest(mutation=mutation):
                receipt = self.receipt()
                if mutation == "missing":
                    del receipt["measurements"]["compact.yaml"]
                elif mutation == "unexpected":
                    receipt["measurements"]["other.yaml"] = receipt["measurements"]["compact.yaml"]
                else:
                    receipt["measurements"] = []
                with self.assertRaisesRegex(ValueError, "measurements"):
                    self.verify(receipt)

    def test_measurement_types_and_bounds_are_strict(self):
        invalid = [(key, value) for key in
                   ("raw_bytes", "canonical_bytes", "frame_bytes", "reference_bytes")
                   for value in (True, -1, 2097155, "1")]
        invalid += [("admission", 1), ("error", None), ("error", "X" * 2049)]
        for key, value in invalid:
            with self.subTest(field=key, value=repr(value)[:50]):
                receipt = self.receipt()
                receipt["measurements"]["canonical.yaml"][key] = value
                with self.assertRaisesRegex(ValueError, "measurement is malformed"):
                    self.verify(receipt)
        for value in (None, [], 1):
            with self.subTest(measurement=value):
                receipt = self.receipt()
                receipt["measurements"]["canonical.yaml"] = value
                with self.assertRaisesRegex(ValueError, "measurement is malformed"):
                    self.verify(receipt)

    def test_artifact_names_and_sets_are_fixed(self):
        for name in ("../outside.yaml", "input.yaml", "unexpected.yaml"):
            with self.subTest(name=name):
                receipt = self.receipt()
                receipt["artifacts"][name] = receipt["artifacts"].pop("canonical.yaml")
                with self.assertRaisesRegex(ValueError, "unexpected artifacts"):
                    self.verify(receipt)
        receipt = self.receipt()
        del receipt["artifacts"]["compact.yaml"]
        with self.assertRaisesRegex(ValueError, "unexpected artifacts"):
            self.verify(receipt)

    def test_artifact_descriptor_types_and_bounds_are_strict(self):
        for key, value in (("bytes", 0), ("bytes", -1), ("bytes", True),
                           ("bytes", 1048578), ("bytes", "1"), ("sha256", None)):
            with self.subTest(field=key, value=value):
                receipt = self.receipt()
                receipt["artifacts"]["canonical.yaml"][key] = value
                with self.assertRaisesRegex(ValueError, "descriptor is malformed"):
                    self.verify(receipt)

    def test_gated_test_must_execute_and_pass(self):
        outputs = (lambda spec: "--- PASS: TestCheckoutCommandResourceFixture",
                   lambda spec: "CHECKOUT_RESOURCE_FIXTURE_EXECUTED " + spec["nonce"],
                   lambda spec: "CHECKOUT_RESOURCE_FIXTURE_EXECUTED " + "0" * 32 +
                                "\n--- PASS: TestCheckoutCommandResourceFixture")
        for index, output in enumerate(outputs):
            with self.subTest(output=index), mock.patch.object(
                    support, "command", side_effect=self.command_stub(output=output)):
                with self.assertRaisesRegex(ValueError, "did not execute"):
                    self.run_wrapper()

    def test_invalid_operation_input_and_target_stop_before_command(self):
        bad = (("unknown", b"", 0), ("profile_admission", b"X" * 1048578, 0),
               ("profile_admission", b"", 1), ("profile_at_size", b"", True),
               ("profile_at_size", b"", 1048578), ("dedupe_frames", b"unexpected input", 0))
        for operation, raw, target in bad:
            with self.subTest(operation=operation, target=target), mock.patch.object(support, "command") as command:
                with self.assertRaises(ValueError):
                    support.resource_fixture(self.binary, self.root, self.source, operation,
                                             input_bytes=raw, target_bytes=target)
                command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
