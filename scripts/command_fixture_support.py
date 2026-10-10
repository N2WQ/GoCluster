"""Private process/provenance support for the checkout command-fix driver."""

import contextlib
import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

import yaml

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / ".tmp" / "windows-tests"
MAX_LOG = 8 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def plain_output(path):
    """Refuse writable paths outside this checkout or through reparse points."""
    path = Path(os.path.abspath(path))
    require(path != ROOT and path.is_relative_to(ROOT), "output must be inside checkout")
    current = path
    while current != ROOT:
        require(not current.is_symlink() and not current.is_junction(),
                f"output traverses symlink/junction: {current}")
        current = current.parent
    return path


@contextlib.contextmanager
def runner_lock():
    path = plain_output(OUTPUT / "runner.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        # FileShare.None in test-windows.ps1 excludes this same native handle in
        # both directions. A Python advisory lock alone would not interoperate.
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        create = kernel.CreateFileW
        create.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                           ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
        create.restype = ctypes.c_void_p
        close = kernel.CloseHandle
        close.argtypes = [ctypes.c_void_p]
        close.restype = ctypes.c_int
        handle = create(str(path), 0xC0000000, 0, None, 4, 0x80, None)
        if handle == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_last_error(), "cannot acquire Windows test runner lock")
        try:
            yield
        finally:
            require(bool(close(handle)), "cannot release Windows test runner lock")
    else:
        raise ValueError("checkout command-fix runner requires native Windows")


@contextlib.contextmanager
def deny_record_replacement(path):
    """Keep an owned record readable while actual atomic replacement fails."""
    path = plain_output(path)
    require(os.name == "nt" and path.is_file(), "replacement fault requires an existing Windows record")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                       ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
    create.restype = ctypes.c_void_p
    close = kernel.CloseHandle
    close.argtypes = [ctypes.c_void_p]
    close.restype = ctypes.c_int
    # GENERIC_READ, FILE_SHARE_READ|FILE_SHARE_WRITE, OPEN_EXISTING. Withhold
    # FILE_SHARE_DELETE: LoadUserRecord may read, but MoveFileEx cannot commit.
    handle = create(str(path), 0x80000000, 3, None, 3, 0x80, None)
    if handle == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), "cannot install record replacement fault")
    try:
        yield
    finally:
        require(bool(close(handle)), "cannot release record replacement fault")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(args, *, timeout=180, cwd=ROOT):
    """Bound diagnostic output during execution, including failed builds."""
    require(os.name == "nt", "checkout command-fix runner requires native Windows")
    log_dir = plain_output(ROOT / ".tmp" / "command-fixes" / "tool-logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / (uuid.uuid4().hex + ".log")
    from command_fixture_process import ToolJob
    job = ToolJob()
    process = None
    try:
        with log_path.open("wb") as log:
            launch = [sys.executable, str(ROOT / "scripts" / "command_fixture_process.py"), *args]
            process = subprocess.Popen(launch, cwd=cwd, stdout=log, stderr=subprocess.STDOUT,
                                       env={**os.environ, "GOCLUSTER_TEST_TOOL_JOB": job.name})
            deadline = time.monotonic() + timeout
            while process.poll() is None:
                require(time.monotonic() < deadline, f"command timed out: {args[0]}")
                require(log_path.stat().st_size <= MAX_LOG, "command output exceeds log limit")
                time.sleep(.05)
            require(log_path.stat().st_size <= MAX_LOG, "command output exceeds log limit")
            job.join()
    finally:
        try:
            if process:
                if process.poll() is None:
                    process.kill()  # Join the wrapper before retiring its job.
                process.wait()
        finally:
            try:
                job.join(terminate=True)
            finally:
                job.close()
    text = log_path.read_text(encoding="utf-8", errors="replace")
    require(process.returncode == 0, f"command failed ({process.returncode}): {args!r}\n{text[-4000:]}")
    return text.strip()


def source_manifest():
    names = command(["git", "ls-files", "--cached", "--others", "--exclude-standard",
                     "--", "*.go", "go.mod", "go.sum"]).splitlines()
    hashes = {name: digest(ROOT / name) for name in sorted(set(names))}
    return {"commit": command(["git", "rev-parse", "HEAD"]), "files": hashes,
            "digest": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()}


def build(race):
    environment = json.loads(command(["go", "env", "-json", "GOOS", "GOHOSTOS", "GOARCH", "GOHOSTARCH", "GOROOT"]))
    require(environment["GOOS"] == environment["GOHOSTOS"] and
            environment["GOARCH"] == environment["GOHOSTARCH"], "native Go target required")
    os.environ["PATH"] = str(Path(environment["GOROOT"]) / "bin") + os.pathsep + os.environ["PATH"]
    package = command(["go", "list", "-f", "{{.ImportPath}}", "./telnet"])
    require(package == "dxcluster/telnet", "unexpected checkout module identity")
    binary = plain_output(OUTPUT / package / "package.test.exe")
    binary.parent.mkdir(parents=True, exist_ok=True)
    before = source_manifest()
    args = ["go", "test", "-c", "-o", str(binary)]
    if race:
        args.append("-race")
    command(args + [package], timeout=300)
    require(source_manifest() == before, "source changed during fixture build")
    return binary, before


def stage(root, transport, *, reputation_enabled=False, dedupe_fast_only=False, solar_enabled=None):
    """Copy tracked config only; disable network destinations in private copies."""
    root = plain_output(root)
    root.mkdir(parents=True, exist_ok=False)
    (root / "config").mkdir()
    required = ("app archive data dedupe floodcontrol ingest mode_seeds peering pipeline "
                "prop_report reputation runtime path_reliability solarweather iaru_regions "
                "iaru_mode_inference spot_taxonomy").split()
    tracked = set(command(["git", "ls-files", "data/config"]).splitlines())

    def local_urls(value):
        if isinstance(value, dict):
            return {key: local_urls(child) for key, child in value.items()}
        if isinstance(value, list):
            return [local_urls(child) for child in value]
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return "http://127.0.0.1:1/fixture-disabled"
        return value

    for name in required:
        rel = f"data/config/{name}.yaml"
        require(rel in tracked, f"required config is not tracked: {rel}")
        content = local_urls(yaml.safe_load((ROOT / rel).read_text(encoding="utf-8")))
        if name == "runtime":
            content["telnet"].update(transport=transport, welcome_message="Checkout command fixture\r\n",
                                     broadcast_workers=2, broadcast_queue_size=64, worker_queue_size=32,
                                     client_buffer_size=64, control_queue_size=64, keepalive_seconds=0,
                                     broadcast_batch_interval_ms=0, accept_rate_per_ip=100,
                                     accept_burst_per_ip=100, accept_rate_per_subnet=100,
                                     accept_burst_per_subnet=100, prelogin_concurrency_per_ip=8)
        if name == "archive":
            content["archive"].update(db_path=str(root / "archive"), queue_size=64, batch_size=1,
                                      batch_interval_ms=25, auto_delete_corrupt_db=False)
        if name == "dedupe" and dedupe_fast_only:
            content["dedup"].update(secondary_med_window_seconds=0, secondary_slow_window_seconds=0)
        if name == "solarweather" and solar_enabled is not None:
            content["enabled"] = solar_enabled
        if name == "data":
            content["cty"]["file"] = str(root / "cty.plist")
            for key in ("fcc_uls", "ised"):
                content[key].update(enabled=False, db_path=str(root / (key + ".sqlite")),
                                    archive_path=str(root / (key + ".zip")), temp_dir=str(root))
            content["fcc_uls"]["allowlist_path"] = str(root / "allowlist.txt")
            content["h3_table_path"] = str(root / "h3")
            content["grid_db"] = str(root / "grid")
            content["skew"]["file"] = str(root / "skew.json")
        if name == "reputation":
            # No enrichment workers or deployment paths belong to a checkout
            # fixture. The optional profile exercises the real loaded gate;
            # its seeded baseline history makes initial positive posts eligible
            # while retaining the shipped, strictly valid initial wait.
            content["reputation"].update(
                enabled=reputation_enabled, reputation_dir=str(root / "reputation"),
                ipinfo_snapshot_path=str(root / "ipinfo" / "location.csv"),
                ipinfo_download_path=str(root / "ipinfo" / "location.csv.gz"),
                ipinfo_pebble_path=str(root / "ipinfo" / "pebble"),
                ipinfo_download_enabled=False, ipinfo_api_enabled=False,
                fallback_team_cymru=False, initial_wait_seconds=60,
                ramp_window_seconds=3600, per_band_start=1, per_band_cap=1,
                total_cap_start=2, total_cap_post_ramp=2,
                ipv4_bucket_size=10000, ipv4_bucket_refill_per_sec=10000,
                ipv6_bucket_size=10000, ipv6_bucket_refill_per_sec=10000)
        (root / "config" / f"{name}.yaml").write_text(yaml.safe_dump(content, sort_keys=False), encoding="utf-8")
    (root / "h3").mkdir()
    for name in ("res1.bin", "res2.bin"):
        shutil.copyfile(ROOT / "data" / "h3" / name, root / "h3" / name)
    # Real CTY plist parsing, without borrowing deployment data or credentials.
    entries = []
    for prefix, country, adif, cq, itu in (("K1", "United States", 291, 5, 8),
                                         ("W6", "United States", 291, 3, 6), ("VE", "Canada", 1, 4, 9)):
        entries.append(f"""<key>{prefix}</key><dict><key>Country</key><string>{country}</string>
<key>Prefix</key><string>{prefix}</string><key>ADIF</key><integer>{adif}</integer>
<key>CQZone</key><integer>{cq}</integer><key>ITUZone</key><integer>{itu}</integer>
<key>Continent</key><string>NA</string><key>ExactCallsign</key><false/></dict>""")
    (root / "cty.plist").write_text('<?xml version="1.0"?><plist version="1.0"><dict>' +
                                    "".join(entries) + "</dict></plist>", encoding="utf-8")
    return root


def require_reputation_identity(receipt, requested):
    enabled = receipt.get("reputation_enabled")
    require(type(enabled) is bool and enabled == requested,
            "fixture reputation gate identity mismatch")


def require_dedupe_identity(receipt, requested):
    available = receipt.get("dedupe_available")
    require(isinstance(available, dict) and available.keys() == requested.keys() and
            all(type(value) is bool for value in available.values()) and available == requested,
            "fixture loaded dedupe availability mismatch")


def require_solar_identity(receipt, requested):
    require(type(receipt.get("solar_enabled")) is bool and receipt["solar_enabled"] is requested,
            "fixture loaded solar availability mismatch")


class Child:
    def __init__(self, binary, root, source, *, reputation_enabled=False, dedupe_available=None, solar_enabled=True):
        self.root, self.nonce = root, uuid.uuid4().hex
        self.reputation_enabled = reputation_enabled
        self.solar_enabled = solar_enabled
        self.dedupe_available = {"FAST": True, "MED": True, "SLOW": True} if dedupe_available is None else dedupe_available
        self.ready = root / ("ready-" + self.nonce + ".json")
        self.receipt = root / ("stopped-" + self.nonce + ".json")
        self.log_path = root / ("child-" + self.nonce + ".log")
        spec = root / ("spec-" + self.nonce + ".json")
        spec.write_text(json.dumps({"root": str(root), "nonce": self.nonce,
                                    "source_digest": source["digest"],
                                    "reputation_enabled": reputation_enabled}), encoding="utf-8")
        self.log = self.log_path.open("wb")
        try:
            self.process = subprocess.Popen([str(binary), "-test.run=^TestCheckoutCommandFixture$",
                                         "-test.count=1", "-test.paniconexit0", "-test.timeout=180s", "-test.v"],
                                        cwd=ROOT / "telnet", stdin=subprocess.PIPE, stdout=self.log,
                                        stderr=subprocess.STDOUT,
                                            env={**os.environ, "GOCLUSTER_COMMAND_FIXTURE": str(spec)})
        except BaseException:
            self.log.close()
            raise
        try:
            deadline = time.monotonic() + 45
            while True:
                self.check_running()
                require(time.monotonic() < deadline, "fixture readiness deadline expired")
                try:
                    ready_text = self.ready.read_text()
                except (FileNotFoundError, PermissionError):
                    # Windows can briefly refuse an open during atomic receipt
                    # publication. Keep the existing live-child/time bounds.
                    time.sleep(.025)
                    continue
                value = json.loads(ready_text)
                break
            require(value["nonce"] == self.nonce and value["pid"] == self.process.pid and value["started"],
                    "fixture child identity mismatch")
            require(value["binary_hash"] == digest(binary) and value["source_digest"] == source["digest"],
                    "fixture provenance mismatch")
            require(value["config_hashes"] == {p.name: digest(p) for p in (root / "config").iterdir()},
                    "fixture loaded configuration identity mismatch")
            require_reputation_identity(value, reputation_enabled)
            require_dedupe_identity(value, self.dedupe_available)
            require_solar_identity(value, self.solar_enabled)
            host, port = value["address"].rsplit(":", 1)
            require(host == "127.0.0.1" and 0 < int(port) <= 65535, "non-loopback fixture endpoint refused")
            self.host, self.port = host, int(port)
            self.identity = value
        except BaseException:
            self.abort()
            raise

    def check_running(self):
        require(self.process.poll() is None, f"fixture exited early; see {self.log_path}")
        require(self.log_path.stat().st_size <= MAX_LOG, "fixture log limit exceeded")

    def abort(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.process.stdin.close()
        self.log.close()

    def stop(self):
        try:
            self.check_running()
            self.process.stdin.write(b"STOP\n")
            self.process.stdin.flush()
            self.process.stdin.close()
            self.process.wait(timeout=15)
            require(self.process.returncode == 0, f"fixture shutdown failed; see {self.log_path}")
            value = json.loads(self.receipt.read_text())
            require(value["nonce"] == self.nonce and all(value[k] for k in
                    ("started", "clients_retired", "consumer_joined", "archive_stopped")),
                    "fixture lacks graceful retirement receipt")
            require_reputation_identity(value, self.reputation_enabled)
            require_dedupe_identity(value, self.dedupe_available)
            require_solar_identity(value, self.solar_enabled)
            text = self.log_path.read_text(encoding="utf-8", errors="replace")
            require("CHECKOUT_FIXTURE_EXECUTED " + self.nonce in text and
                    "--- PASS: TestCheckoutCommandFixture" in text, "required fixture did not execute")
            require(self.log_path.stat().st_size <= MAX_LOG, "fixture log limit exceeded")
        except BaseException:
            self.abort()
            raise
        finally:
            self.log.close()
