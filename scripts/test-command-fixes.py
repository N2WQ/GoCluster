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


def archive_rows(session, label):
    text, _ = session.human(f"SHOW DX W6ABC 10 COMMENT {label}")
    require("Search work limit reached" not in text and "unreadable archive" not in text,
            "archive selection incomplete")
    return [line for line in text.splitlines() if line.startswith("DX de")]


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
                f"{transport}/1: held endpoint and loaded consumers",
                f"{transport}: preferences and archive survive fresh child",
                f"{transport}: NOFILTER geography survives fresh child",
                f"{transport}: failed saves and refused records survive fresh child"]
    call = "K1ABC-70001"
    observer_call, observer_configuration = "VE3ABC-70003", None
    failed_records, refused_records = {}, {}
    label = "T" + uuid.uuid4().hex[:8].upper()
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
        report["result"] = "PASS"
    except (OSError, ValueError, EOFError, TimeoutError, subprocess.TimeoutExpired, harness.yaml.YAMLError) as error:
        report["error"] = str(error)
    finally:
        (run / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"result": report["result"], "output": str(run), "error": report.get("error")}, indent=2))
    return int(report["result"] != "PASS")


if __name__ == "__main__":
    raise SystemExit(main())
