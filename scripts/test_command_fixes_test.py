"""Offline failure controls for the checkout runner; these do not qualify telnet."""

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


class DriverControls(unittest.TestCase):
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
