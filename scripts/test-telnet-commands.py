"""Bounded live command regression derived from the October 3 preset transcript.

Requires PyYAML 6.0.3. Only numeric test SSIDs are mutated. Raw wire defects
remain failures even when tolerant decoding permits the remaining checks to run.
See docs/telnet-command-validation.md for authorization and residual state.
"""
import argparse
import copy
import json
import re
import socket
import sys
import time
import uuid
from pathlib import Path

import yaml

MAX_FRAME = 65536
MAX_BUFFER = 262144
CATEGORIES = "BAND MODE MINSNR COMMENT SOURCE EVENT CONFIDENCE PATH DXCONT DECONT DXZONE DEZONE DXGRID2 DEGRID2 DXDXCC DEDXCC DXSTATE DESTATE DXCALL DECALL BEACON WWV WCY ANNOUNCE SELF TOXIC NEARBY".split()
HEADINGS = dict(zip(CATEGORIES, ["Bands", "Modes", "Minimum SNR", "Comments", "Sources", "Events", "Confidence", "Path", "DX continents", "DE continents", "DX zones", "DE zones", "DX grids", "DE grids", "DX DXCC", "DE DXCC", "DX states", "DE states", "DX calls", "DE calls", "Beacons", "WWV", "WCY", "Announce", "Self", "Toxic", "Nearby"]))


class StrictLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in result:
                raise ValueError(f"duplicate YAML key: {key!r}")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def decode_frame(raw, resource, request_id, version):
    """Separate strict transport evidence from tolerant semantic diagnostics."""
    # splitlines treats CR CR LF as multiple lines: use LF boundaries instead.
    lines = [line + b"\n" for line in raw.split(b"\n")[:-1]]
    if not raw.endswith(b"\n") or not lines or lines[0].rstrip(b"\r\n") != b"---" or lines[-1].rstrip(b"\r\n") != b"...":
        raise ValueError("incomplete or malformed YAML frame")
    errors = []
    limit = MAX_BUFFER if version == 7 else MAX_FRAME
    if len(raw) > limit:
        errors.append(f"frame exceeds {limit} raw bytes")
    if any(not line.endswith(b"\r\n") or b"\r" in line[:-2] for line in lines):
        errors.append("line endings violate CRLF contract")
    text = b"\n".join(line.rstrip(b"\r\n") for line in lines).decode("utf-8", "strict")
    try:
        document = yaml.load(text, Loader=StrictLoader)
    except yaml.YAMLError as error:
        raise ValueError("invalid YAML frame") from error
    if not isinstance(document, dict):
        raise ValueError("YAML envelope is not a mapping")
    for key, expected in (("resource", resource), ("request_id", request_id), ("schema_version", version)):
        if type(document.get(key)) is not type(expected) or document.get(key) != expected:
            raise ValueError(f"{key}: expected {expected!r}, got {document.get(key)!r}")
    return document, errors


class TelnetReader:
    """One bounded connection-local decoder, including split IAC sequences."""
    def __init__(self, sock, deadline, timeout=12):
        self.sock, self.deadline, self.timeout = sock, deadline, timeout
        self.buffer = bytearray()
        self.state, self.verb = "data", 0
        self.remote_echo = False

    def _receive(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("response deadline exceeded")
        self.sock.settimeout(min(remaining, 1))
        try:
            data = self.sock.recv(8192)
        except socket.timeout:
            if time.monotonic() >= self.deadline:
                raise TimeoutError("response deadline exceeded")
            return
        if not data:
            raise EOFError("connection closed before response completed")
        for b in data:
            if self.state == "data":
                if b == 255:
                    self.state = "iac"
                else:
                    self.buffer.append(b)
            elif self.state == "iac":
                if b in (251, 252, 253, 254):
                    self.verb, self.state = b, "option"
                elif b == 250:
                    self.state = "sub"
                else:
                    if b == 255:
                        self.buffer.append(b)
                    self.state = "data"
            elif self.state == "option":
                if b == 1 and self.verb in (251, 252):
                    self.remote_echo = self.verb == 251
                if self.verb == 251:
                    self.sock.sendall(bytes((255, 253 if b in (1, 3) else 254, b)))
                elif self.verb == 253:
                    self.sock.sendall(bytes((255, 252, b)))
                self.state = "data"
            elif self.state == "sub":
                if b == 255:
                    self.state = "subiac"
            elif self.state == "subiac":
                self.state = "data" if b == 240 else "sub"
        if len(self.buffer) > MAX_BUFFER:
            raise ValueError("receive buffer limit exceeded")

    def until(self, marker):
        while marker not in self.buffer:
            self._receive()
        end = self.buffer.index(marker) + len(marker)
        data = bytes(self.buffer[:end])
        del self.buffer[:end]
        return data

    def line(self):
        return self.until(b"\n")


class Session:
    def __init__(self, suite, call):
        self.suite, self.call, self.closed = suite, call, False
        self.sock = socket.create_connection((suite.args.host, suite.args.port), suite.args.timeout)
        self.reader = TelnetReader(self.sock, min(suite.deadline, time.monotonic() + suite.args.timeout))
        try:
            self.reader.until(b"login: ")
            self.send(call + "\r\n", count=False)
            greeting = self.reader.until(b"UTC>")
            if ("Hello " + call).encode() not in greeting:
                raise ValueError("successful login greeting absent")
        except BaseException:
            self.sock.close()
            raise
        suite.sessions.append(self)

    def send(self, text, count=True):
        self.reader.deadline = min(self.suite.deadline, time.monotonic() + self.suite.args.timeout)
        self.sock.settimeout(max(.001, self.reader.deadline - time.monotonic()))
        self.sock.sendall(text.encode("ascii").replace(b"\xff", b"\xff\xff"))
        if count:
            self.suite.commands += 1

    def frame(self, resource, identifier, version, command):
        prefix, frame = bytearray(), bytearray()
        complete = False
        try:
            while not frame:
                line = self.reader.line()
                if line.rstrip(b"\r\n") == b"---":
                    frame.extend(line)
                else:
                    prefix.extend(line)
                if len(prefix) > MAX_BUFFER:
                    raise ValueError("no frame within prefix limit")
            while frame.split(b"\n")[-2].rstrip(b"\r") != b"...":
                frame.extend(self.reader.line())
                if len(frame) > MAX_BUFFER:
                    raise ValueError("frame recovery limit exceeded")
            document, errors = decode_frame(bytes(frame), resource, identifier, version)
            if errors:
                self.suite.wire_failures.append({"call": self.call, "command": command, "errors": errors})
            complete = True
            return document, bytes(prefix)
        finally:
            # On refusal/EOF/deadline retain the decoded partial suffix as well.
            raw = bytes(prefix) + bytes(frame) + (b"" if complete else bytes(self.reader.buffer))
            self.suite.log.write(f"\n[{self.call}] {command}\n".encode() + raw[:MAX_BUFFER])
            if len(raw) > MAX_BUFFER:
                self.suite.log.write(b"\n[frame receipt truncated at 262144 bytes]\n")
            self.suite.log.flush()

    def get(self, resource="CONFIG", version=4):
        identifier = self.suite.identifier()
        command = f"GET YAML {resource}" + (f" SCHEMA {version}" if version != 1 else "") + f" ID {identifier}"
        self.send(command + "\r\n")
        document, _ = self.frame(resource, identifier, version, command)
        assert "error" not in document, document.get("error")
        assert isinstance(document.get("configuration"), dict) and isinstance(document.get("status"), dict)
        assert isinstance(document.get("revision"), str) and document["revision"]
        return document

    def human(self, command, expected=None, *, resource="CONFIG"):
        identifier = self.suite.identifier()
        sync = f"GET YAML {resource} SCHEMA 4 ID {identifier}"
        self.send(command + "\r\n" + sync + "\r\n")
        self.suite.commands += 1
        document, prefix = self.frame(resource, identifier, 4, command)
        if len(prefix) > MAX_FRAME or (prefix and not prefix.endswith(b"\r\n")) or any(
                not line.endswith(b"\r") or b"\r" in line[:-1] for line in prefix.split(b"\n")[:-1]):
            self.suite.wire_failures.append({"call": self.call, "command": command, "errors": ["human reply violates CRLF/size contract"]})
        text = prefix.replace(b"\r\r\n", b"\r\n").decode("ascii", "strict")
        # Remove only the exact input echo. Never let echoed input satisfy an oracle.
        lines = text.splitlines()
        if self.reader.remote_echo:
            for echoed in (command.upper(), sync):
                if echoed in lines:
                    lines.remove(echoed)
        text = "\n".join(lines)
        if expected is not None:
            assert expected in text, f"{command}: missing {expected!r}: {text[:1000]}"
        assert "error" not in document, document.get("error")
        assert type(document.get("configuration")) is dict and type(document.get("status")) is dict
        assert type(document.get("revision")) is str and document["revision"]
        return text, document

    def upload(self, verb, resource, configuration, version=4, revision=None, raw_body=None, error_id=False):
        identifier = self.suite.identifier()
        body = {"schema_version": version, "request_id": identifier, "configuration": configuration}
        if verb != "VALIDATE":
            body["if_revision"] = revision or self.get()["revision"]
        text = raw_body(identifier, body) if raw_body else yaml.safe_dump(body, sort_keys=False)
        command = f"{verb} YAML {resource}"
        self.send(command + "\r\n---\r\n" + text.replace("\n", "\r\n") + "...\r\n")
        result = self.frame(resource, "" if error_id else identifier, version, command)[0]
        if "error" not in result:
            assert result.get("result", {}).get("operation") == verb
        return result

    def close(self, verb="BYE"):
        if self.closed:
            return
        try:
            # Drain queued live rows through a correlated paused-state fence.
            # The strict logout collector then owns a quiet stream and EOF.
            # CAPABILITIES avoids full filters and a second copy of oversized
            # saved settings; the same correlated status carries the pause.
            _, document = self.human("PAUSE 300", resource="CAPABILITIES")
            check(document["status"]["session"]["pause_active"] is True, "logout did not establish a pause")
            remote_logout(self, verb)
        finally:
            if not self.closed:
                try:
                    self.sock.close()
                finally:
                    self.closed = True


def check(condition, message):
    if not condition:
        raise ValueError(message)


def remote_logout(session, verb):
    received, raw, farewell = 0, bytearray(), False
    try:
        session.send(verb + "\r\n")
        while True:
            try:
                line = session.reader.line()
            except EOFError:
                check(farewell and not session.reader.buffer and session.reader.state == "data",
                      "logout lacks farewell or has partial trailing bytes")
                return  # Remote EOF is observed before any local socket close.
            received += len(line)
            raw.extend(line[:MAX_FRAME - len(raw)])
            check(received <= MAX_FRAME and line.endswith(b"\r\n") and b"\r" not in line[:-2],
                  "logout reply exceeds its bound or has invalid CRLF")
            text = line[:-2].decode("ascii", "strict")
            if not farewell and text == "73!":
                farewell = True
            else:
                check(text == "" or (not farewell and text == verb.upper()), "unexpected logout output")
    finally:
        try:
            pending = session.reader.buffer
            received += len(pending)
            raw.extend(pending[:MAX_FRAME - len(raw)])
            receipt = f"\n[{session.call}] {verb} [remote logout]\n".encode() + bytes(raw)
            if received > len(raw):
                receipt += b"\n[logout receipt truncated at 65536 bytes]\n"
            session.suite.log.write(receipt)
            session.suite.log.flush()
        finally:
            session.sock.close()
            session.closed = True

class BlockedPrerequisite(RuntimeError):
    """Required evidence is unavailable; this never qualifies as a pass."""


def required_live_cases(token, call):
    """Independent leaf inventory, deliberately separate from execution loops."""
    reads = []
    categories = "BAND MODE MINSNR COMMENT SOURCE EVENT CONFIDENCE PATH DXCONT DECONT DXZONE DEZONE DXGRID2 DEGRID2 DXDXCC DEDXCC DXSTATE DESTATE DXCALL DECALL BEACON WWV WCY ANNOUNCE SELF TOXIC NEARBY".split()
    for dialect, allow, block in (("go", "PASS", "REJECT"), ("cc", "SET/FILTER", "UNSET/FILTER")):
        prefix = dialect + " "
        reads += [prefix + f"COMMENT step {i}" for i in range(9)] + [prefix + "COMMENT overlong"]
        reads += [prefix + c for c in ("HELP", "H", "DIALECT LIST", "SHOW BUILD", "SHOW OWN", "SHOW DEDUPE", "SHOW DXCC VE", "WHOSPOTSME", "WHOSPOTSME 20M")]
        head = "SHOW FILTER" if dialect == "go" else "SHOW/FILTER"
        reads += [prefix + head + (" " + c if c else "") for c in ["", "FULL"] + categories + ["CONF", "PC93"]]
        reads += [prefix + "SHOW SETTINGS"]
        reads += [prefix + v + " " + d for d in ("BAND", "MODE", "SOURCE", "EVENT", "DXZONE", "DXGRID2", "DXDXCC", "DXCALL", "DECALL", "ANNOUNCE") for v in (allow, block)]
        reads += [prefix + "atomic " + c for c in (allow + " DXSTATE ON,ZZ", block + " DESTATE NY,ZZ", allow + " DXGRID2 FN,SS", block + " DXDXCC VE,ZZZZ", allow + " MINSNR CW,BOGUS 10")]
        reads += [prefix + c for c in (allow + " MINSNR CW,RTTY 0", block + " MINSNR FT8,FT4 -10", allow + " MINSNR ALL 7", allow + " MINSNR CW ALL", block + " MINSNR ALL NONE")]
        reads += [prefix + v + " " + d + " ALL" for d in ("DXSTATE", "DESTATE") for v in (block, allow)]
        reads += [prefix + "configured detail " + c for c in ("MINSNR", "DXSTATE", "DXGRID2")]
    reads += ["SET GRID FN31", "SET NOISE URBAN", "SET PATHSAMPLES 1000", "SET SOLAR 30", "SET DEDUPE FAST"]
    reads += ["SET DIAG " + mode for mode in ("OFF", "DEDUPE", "SOURCE", "CONF", "PATH", "MODE")]
    reads += ["SET/" + alias for alias in ("ANN", "NOANN", "BEACON", "NOBEACON", "WWV", "NOWWV", "WCY", "NOWCY", "SELF", "NOSELF", "SKIMMER", "NOSKIMMER", "FT8", "NOFT8", "FILTER DXBM/PASS 20", "FILTER DXBM/REJECT 20", "NOFILTER")]
    machine = ["PAUSE", "PAUSE 300", "PAUSE 1", "PAUSE 0", "PAUSE 301", "PAUSE BAD", "PAUSE 3 EXTRA", "SHOW HOLD"]
    machine += [f"GET FILTER schema {v}" for v in (1, 2, 3, 4)] + [f"GET {r} schema 4" for r in ("SETTINGS", "CONFIG", "CAPABILITIES")]
    machine += [f"PUT {action} {r}" for r in ("FILTER", "SETTINGS", "CONFIG") for action in ("unchanged", "changed")]
    machine += ["PATCH " + r for r in ("SETTINGS", "FILTER", "CONFIG")]
    machine += ["VALIDATE read-only", "stale revision atomic rejection", "invalid YAML {'noise_class': None}", "invalid YAML {'unexpected': True}", "old schema writes preserve newer fields", "RESUME immediate state"]
    history = ["DX valid syntax 0", "DX valid syntax 1", "DX invalid input"]
    history += ["history selection " + q for q in ("BAND 10m", "BAND 15", "MODE cw", "MODE FT8", "BAND 10m,15m", "BAND 10, 15", "MODE CW,FT8", "MODE CW, FT8", "BAND 10m,10 MODE CW,CW", "BAND 10,15 MODE CW", "MODE CW, FT8 BAND 15m", "BAND 10 MODE FT8", "MODE UNKNOWN", "BAND 10 MODE CW")]
    history += [f"history selection literal T{token}-0 up-5{ending}" for ending in ("?", "!")]
    history += ["history narrows saved BAND", "history narrows saved MODE", "history selection NEXT and invalid preservation"]
    target = "K1ABC" if call.upper() == "VA3UXA" else "VA3UXA"
    target_prefix = "K" if target == "K1ABC" else "VE"
    for dialect in ("go", "cc"):
        history += [dialect + " " + c for c in ("SHOW DX 1", "SH DX 1", f"SHOW MYDX {target} 2", f"SH MYDX 2 {target}", f"SHOW DX {target_prefix} 1")]
        if dialect == "cc":
            history += ["cc SHOW/DX 1", "cc SH/DX 1"]
        history += [dialect + " selection " + c for c in ("SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX")]
        if dialect == "cc":
            history += ["cc selection SHOW/DX", "cc selection SH/DX"]
        history += [dialect + " invalid " + c for c in ("SHOW DX 0", "SHOW MYDX 251")]
    history += ["SHOW PROP syntax", "SHOW PROP forecast"]
    return {"human commands and filters": reads, "pause and machine commands": machine,
            "original preset regression plus state/SNR": ["original preset regression plus state/SNR"],
            "history and read commands": history,
            "terminal inputs": ["terminal upload " + k for k in ("header", "oversize", "deadline", "incomplete")] + ["disconnect " + v for v in ("BYE", "QUIT", "EXIT")]}


class Suite:
    def __init__(self, args):
        self.args, self.deadline = args, time.monotonic() + args.run_seconds
        self.commands, self.sequence = 0, 0
        self.sessions, self.baselines, self.owned = [], {}, set()
        self.results, self.wire_failures, self.cleanup_errors = [], [], []
        self.token = uuid.uuid4().hex[:8].upper()
        # Random numeric SSIDs avoid the main account's persistent association.
        self.calls = [args.call.upper() + "-" + str(10000 + int(self.token, 16) % 80000 + i) for i in range(2)]
        args.output.mkdir(parents=True, exist_ok=False)
        self.log = (args.output / "transcript.bin").open("wb")

    def identifier(self):
        self.sequence += 1
        return f"Test-{self.sequence}"

    def case(self, name, operation, required=None):
        start, result, detail = len(self.results), "PASS", None
        try:
            operation()
            if required is not None:
                children = self.results[start:]
                check(len(children) == len(required) and {r["case"] for r in children} == set(required),
                      "required family leaves missing, duplicated or unexpected")
                check(all(r["result"] == "PASS" for r in children), "required child failed or blocked")
        except Exception as error:
            result = "BLOCKED" if isinstance(error, BlockedPrerequisite) else "FAIL"
            detail = f"{type(error).__name__}: {error}"
            print(f"{result} {name}: {detail[:220]}", flush=True)
        row = {"case": name, "result": result}
        if required is not None:
            row["kind"] = "family"
        if detail:
            row["detail"] = detail
        self.results.append(row)
        return result == "PASS"

    def account_required(self, required):
        leaves = [row for row in self.results if row.get("kind") != "family"]
        names = [row["case"] for row in leaves]
        for name in required:
            if name not in names:
                self.results.append({"case": name, "result": "UNEXECUTED", "detail": "required leaf was not reached"})
        if len(names) != len(set(names)) or set(names) - set(required):
            self.results.append({"case": "required inventory", "result": "FAIL", "detail": "duplicate or unexpected leaf identities"})

    def unchanged(self, session, command, expected):
        before = session.get()
        _, after = session.human(command, expected)
        assert before["configuration"] == after["configuration"] and before["revision"] == after["revision"]

    def acknowledge(self, document, applied=True):
        assert "error" not in document, document.get("error")
        result = document.get("result", {})
        assert result.get("valid") is True and result.get("applied") is applied and result.get("persisted") is applied, document

    def reads_and_filters(self, a):
        for dialect in ("go", "cc"):
            _, selected = a.human("DIALECT " + dialect)
            assert selected["configuration"]["settings"]["dialect"] == dialect
            a.human("PAUSE 300")
            for step, (command, allow, block) in enumerate((
                ("RESET FILTER COMMENT", [], []),
                ("PASS COMMENT up  5: please!", ["UP  5: PLEASE!"], []),
                ("PASS COMMENT up  5: please!", ["UP  5: PLEASE!"], []),
                ("REJECT COMMENT QRT", ["UP  5: PLEASE!"], ["QRT"]),
                ("REJECT COMMENT up  5: please!", [], ["QRT", "UP  5: PLEASE!"]),
                ("REMOVE REJECT COMMENT UP  5: PLEASE!", [], ["QRT"]),
                ("RESET FILTER COMMENT REJECT", [], []),
                ("PASS COMMENT " + "A" * 64, ["A" * 64], []),
                ("RESET FILTER COMMENT PASS", [], []),
            )):
                def comment_rule(c=command, pa=allow, re=block):
                    _, doc = a.human(c)
                    fields = doc["configuration"]["filters"]
                    assert fields["comments"] == pa and fields["block_comments"] == re
                self.case(f"{dialect} COMMENT step {step}", comment_rule)
            self.case(dialect + " COMMENT overlong", lambda: self.unchanged(a, "PASS COMMENT " + "A" * 65, "Usage:"))
            for command, expected in (("HELP", "GoCluster help - "), ("H", "GoCluster help - "), ("DIALECT LIST", "GO"), ("SHOW BUILD", "Build version:"), ("SHOW OWN", "Own call: " + self.args.call.upper()), ("SHOW DEDUPE", "Dedupe"), ("SHOW DXCC VE", "Canada"), ("WHOSPOTSME", "WHOSPOTSME"), ("WHOSPOTSME 20M", "WHOSPOTSME")):
                self.case(dialect + " " + command, lambda c=command, e=expected: a.human(c, e))
            for category in ["", "FULL"] + CATEGORIES + ["CONF", "PC93"]:
                command = ("SHOW FILTER" if dialect == "go" else "SHOW/FILTER") + (" " + category if category else "")
                def human_view(c=command, cat=category):
                    text, status = a.human(c, "Type RESUME when ready.")
                    assert status["status"]["session"]["pause_active"] is True
                    assert all(len(line) <= 78 and all(32 <= ord(ch) <= 126 for ch in line) for line in text.splitlines())
                    if cat == "FULL":
                        assert all(heading in text for heading in HEADINGS.values())
                    elif cat:
                        assert HEADINGS[{"CONF": "CONFIDENCE", "PC93": "ANNOUNCE"}.get(cat, cat)] in text
                    else:
                        assert "Bands" in text and "Sources" in text and "DX geography" in text
                self.case(dialect + " " + command, human_view)
            self.case(dialect + " SHOW SETTINGS", lambda: a.human("SHOW SETTINGS", "Session only"))
            pass_verb, reject_verb = ("PASS", "REJECT") if dialect == "go" else ("SET/FILTER", "UNSET/FILTER")
            # Exhaustive categories remain in the owner tests. These witnesses
            # cover named, scalar, grid-canonicalization and prefix-lookup paths.
            domains = {"BAND": ("20M", "bands", "20m"), "MODE": ("CW", "modes", "CW"), "SOURCE": ("HUMAN", "sources", "HUMAN"), "EVENT": ("POTA", "events", "POTA"), "DXZONE": ("5", "dx_zones", 5), "DXGRID2": ("FN20", "dx_grid2", "FN"), "DXDXCC": ("VE", "dx_dxcc", 1)}
            a.human("RESET FILTER", "Filters reset")
            for domain, (value, field, key) in domains.items():
                for verb, collection in ((pass_verb, "allow"), (reject_verb, "block")):
                    def rule(v=verb, d=domain, val=value, f=field, k=key, coll=collection):
                        _, doc = a.human(f"{v} {d} {val}")
                        assert doc["configuration"]["filters"][f][coll].get(k) is True, doc["configuration"]["filters"][f]
                    self.case(f"{dialect} {verb} {domain}", rule)
            for domain, field in (("DXCALL", "dx_callsigns"), ("DECALL", "de_callsigns")):
                for verb, prefix in ((pass_verb, ""), (reject_verb, "block_")):
                    def pattern(v=verb, d=domain, f=prefix + field):
                        _, doc = a.human(f"{v} {d} VA3*")
                        assert "VA3*" in doc["configuration"]["filters"][f]
                    self.case(f"{dialect} {verb} {domain}", pattern)
            for domain, field in (("ANNOUNCE", "allow_announce"),):
                for verb, value in ((pass_verb, True), (reject_verb, False)):
                    def toggle(v=verb, d=domain, f=field, expected=value):
                        _, doc = a.human(f"{v} {d}")
                        assert doc["configuration"]["filters"][f] is expected
                    self.case(f"{dialect} {verb} {domain}", toggle)
            for command, expected in ((f"{pass_verb} DXSTATE ON,ZZ", "Invalid state"), (f"{reject_verb} DESTATE NY,ZZ", "Invalid state"), (f"{pass_verb} DXGRID2 FN,SS", "Unknown 2-character grid"), (f"{reject_verb} DXDXCC VE,ZZZZ", "Invalid DXCC"), (f"{pass_verb} MINSNR CW,BOGUS 10", "Invalid MINSNR")):
                self.case(dialect + " atomic " + command, lambda c=command, e=expected: self.unchanged(a, c, e))
            for command in (f"{pass_verb} MINSNR CW,RTTY 0", f"{reject_verb} MINSNR FT8,FT4 -10", f"{pass_verb} MINSNR ALL 7", f"{pass_verb} MINSNR CW ALL", f"{reject_verb} MINSNR ALL NONE"):
                def minimum(c=command):
                    _, doc = a.human(c, "Minimum SNR")
                    minima = doc["configuration"]["filters"]["min_snr"]
                    if "ALL NONE" in c:
                        assert minima == {}
                    elif "CW,RTTY 0" in c:
                        assert minima.get("CW") == 0 and minima.get("RTTY") == 0
                    elif "FT8,FT4 -10" in c:
                        assert minima.get("FT8") == -10 and minima.get("FT4") == -10
                    elif "ALL 7" in c:
                        assert minima and all(value == 7 for value in minima.values())
                    else:
                        assert "CW" not in minima
                self.case(dialect + " " + command, minimum)
            for command in (f"{reject_verb} DXSTATE ALL", f"{pass_verb} DXSTATE ALL", f"{reject_verb} DESTATE ALL", f"{pass_verb} DESTATE ALL"):
                self.case(dialect + " " + command, lambda c=command: a.human(c, c.replace("SET/FILTER", "PASS").replace("UNPASS", "REJECT")))
            for category, expected in (("MINSNR", "Minimum SNR"), ("DXSTATE", "PASS: ALL"), ("DXGRID2", "REJECT: FN")):
                self.case(dialect + " configured detail " + category, lambda cat=category, e=expected: a.human("SHOW FILTER " + cat, e))
        for command, field, value, expected in (("SET GRID FN31", "grid", "FN31", "Grid set"), ("SET NOISE URBAN", "noise_class", "URBAN", "Noise class set"), ("SET PATHSAMPLES 1000", "path_min_observation_count", 1000, "Path sample minimum set"), ("SET SOLAR 30", "solar_summary_minutes", 30, "Solar summaries"), ("SET DEDUPE FAST", "dedupe_policy", "FAST", "Dedupe policy")):
            def setting(c=command, f=field, val=value, e=expected):
                _, doc = a.human(c, e)
                assert doc["configuration"]["settings"][f] == val
            self.case(command, setting)
        for mode in ("OFF", "DEDUPE", "SOURCE", "CONF", "PATH", "MODE"):
            def diagnostic(m=mode):
                _, doc = a.human("SET DIAG " + m)
                assert doc["status"]["session"]["diagnostic_comments"] == m
            self.case("SET DIAG " + mode, diagnostic)
        for alias in ("SET/ANN", "SET/NOANN", "SET/BEACON", "SET/NOBEACON", "SET/WWV", "SET/NOWWV", "SET/WCY", "SET/NOWCY", "SET/SELF", "SET/NOSELF", "SET/SKIMMER", "SET/NOSKIMMER", "SET/FT8", "SET/NOFT8", "SET/FILTER DXBM/PASS 20", "SET/FILTER DXBM/REJECT 20", "SET/NOFILTER"):
            def cc_alias(c=alias):
                text, document = a.human(c)
                assert text.strip() and not any(error in text for error in ("Usage:", "Unknown", "Invalid")), text
                f = document["configuration"]["filters"]
                if c == "SET/NOFILTER":
                    assert f["min_snr"] == {} and f["bands"]["allow_all"] and not f["bands"]["block_all"]
                elif "DXBM" in c:
                    assert f["bands"]["block" if "REJECT" in c else "allow"].get("20m") is True
                elif "SKIMMER" in c:
                    assert f["sources"]["block" if "NOSKIMMER" in c else "allow"].get("SKIMMER") is True
                elif "FT8" in c:
                    assert f["modes"]["allow"].get("FT8", False) is (c == "SET/FT8")
                else:
                    name = c.removeprefix("SET/")
                    enabled = not name.startswith("NO")
                    name = name.removeprefix("NO")
                    field = {"ANN": "allow_announce", "BEACON": "include_beacons", "WWV": "allow_wwv", "WCY": "allow_wcy", "SELF": "allow_self"}[name]
                    assert f[field] is enabled
            self.case(alias, cc_alias)

    def pause_and_machine(self, a):
        for command, seconds in (("PAUSE", 30), ("PAUSE 300", 300), ("PAUSE 1", 1)):
            def pause(c=command, n=seconds):
                _, doc = a.human(c, f"paused for {n}s.")
                assert doc["status"]["session"]["pause_active"] is True
                assert 0 < doc["status"]["session"]["pause_remaining_seconds"] <= n
            self.case(command, pause)
        a.human("PAUSE 120", "paused for 120s.")
        for command in ("PAUSE 0", "PAUSE 301", "PAUSE BAD", "PAUSE 3 EXTRA"):
            def bad_pause(c=command):
                _, doc = a.human(c, "Usage: PAUSE")
                assert doc["status"]["session"]["pause_remaining_seconds"] > 100
            self.case(command, bad_pause)
        self.case("SHOW HOLD", lambda: a.human("SHOW HOLD", "Live spots paused"))
        for version in (1, 2, 3, 4):
            for resource in (("FILTER", "SETTINGS", "CONFIG", "CAPABILITIES") if version == 4 else ("FILTER",)):
                def projection(r=resource, v=version):
                    doc = a.get(r, v)
                    data = doc["configuration"]
                    if r in ("FILTER", "CONFIG"):
                        fields = data if r == "FILTER" else data["filters"]
                        assert ("min_snr" in fields) == (v >= 3)
                        assert ("dx_states" in fields) == (v >= 2)
                        assert ("comments" in fields) == (v >= 4)
                        assert ("block_comments" in fields) == (v >= 4)
                    assert doc["status"]["session"]["pause_remaining_seconds"] > 80
                self.case(f"GET {resource} schema {version}", projection)
        for resource in ("FILTER", "SETTINGS", "CONFIG"):
            def put(r=resource):
                before = a.get(r)
                self.acknowledge(a.upload("PUT", r, before["configuration"], revision=before["revision"]))
                after = a.get(r)
                assert before["configuration"] == after["configuration"] and before["revision"] == after["revision"]
            self.case("PUT unchanged " + resource, put)
            def changed_put(r=resource):
                candidate = copy.deepcopy(a.get(r)["configuration"])
                if r == "SETTINGS":
                    candidate["noise_class"] = "INDUSTRIAL"
                elif r == "FILTER":
                    candidate["min_snr"] = {"RTTY": -3}
                else:
                    candidate["settings"]["noise_class"] = "SUBURBAN"
                    candidate["filters"]["min_snr"] = {"CW": 11}
                self.acknowledge(a.upload("PUT", r, candidate))
                assert a.get(r)["configuration"] == candidate
            self.case("PUT changed " + resource, changed_put)
        for resource, patch in (("SETTINGS", {"noise_class": "QUIET"}), ("FILTER", {"min_snr": {"CW": 6}}), ("CONFIG", {"settings": {"noise_class": "RURAL"}, "filters": {"min_snr": {"FT8": -12}}})):
            def patch_case(r=resource, p=patch):
                expected = copy.deepcopy(a.get()["configuration"])
                for section, fields in (p.items() if r == "CONFIG" else [("filters" if r == "FILTER" else "settings", p)]):
                    expected[section].update(fields)
                self.acknowledge(a.upload("PATCH", r, p))
                doc = a.get()["configuration"]
                assert doc == expected, "PATCH lost an omitted field"
                for section, fields in (p.items() if r == "CONFIG" else [("filters" if r == "FILTER" else "settings", p)]):
                    for key, value in fields.items():
                        assert doc[section][key] == value
            self.case("PATCH " + resource, patch_case)
        def validate():
            before = a.get()
            candidate = copy.deepcopy(before["configuration"])
            candidate["settings"]["noise_class"] = "INDUSTRIAL"
            self.acknowledge(a.upload("VALIDATE", "CONFIG", candidate), False)
            after = a.get()
            assert before["configuration"] == after["configuration"] and before["revision"] == after["revision"]
        self.case("VALIDATE read-only", validate)
        def conflict():
            before = a.get()
            doc = a.upload("PATCH", "SETTINGS", {"noise_class": "URBAN"}, revision="stale-revision")
            assert doc.get("error", {}).get("code") == "revision_conflict", doc
            assert a.get()["configuration"] == before["configuration"]
        self.case("stale revision atomic rejection", conflict)
        for patch in ({"noise_class": None}, {"unexpected": True}):
            def invalid(p=patch):
                before = a.get()
                result = a.upload("PATCH", "SETTINGS", p, error_id=any(value is None for value in p.values()))
                assert result.get("error", {}).get("code") == "invalid_document", result
                assert a.get()["configuration"] == before["configuration"]
            self.case("invalid YAML " + repr(patch), invalid)
        def old_write():
            a.human("SET/FILTER DXSTATE ON,NY")
            a.human("PASS COMMENT POTA")
            a.human("REJECT COMMENT QRT")
            before = a.get()["configuration"]
            for version in (1, 2, 3):
                projected = a.get("FILTER", version)
                self.acknowledge(a.upload("PUT", "FILTER", projected["configuration"], version, projected["revision"]))
                assert a.get()["configuration"] == before
            a.human("RESET FILTER COMMENT")
        self.case("old schema writes preserve newer fields", old_write)
        def resume():
            _, doc = a.human("RESUME")
            assert doc["status"]["session"]["pause_active"] is False
        self.case("RESUME immediate state", resume)

    def presets(self, a, b):
        names = [f"LIVE-{self.token}-ALPHA", f"LIVE-{self.token}-ZULU"]
        listing, _ = a.human("LIST PRESET", "Saved presets")
        assert all(name not in listing for name in names), "test preset collision"
        count = re.search(r"\((\d+)/20\)", listing)
        assert count and int(count.group(1)) <= 18, "need two unused preset slots"
        a.human("DIALECT GO")
        a.human("RESET FILTER", "Filters reset")
        for command in ("PASS BAND 20M", "PASS SOURCE HUMAN", "REJECT WWV", "SET GRID FN31", "SET NOISE URBAN", "SET PATHSAMPLES 1000", "SET SOLAR 30", "SET DEDUPE SLOW", "PASS DXSTATE ON,NY", "PASS MINSNR CW 4"):
            a.human(command)
        gold = a.get()["configuration"]
        for name in reversed(names):
            self.owned.add(name)  # Reserve before send: a lost ACK may still commit.
            a.human("SAVE PRESET " + name.lower(), "Saved preset " + name)
        listing, _ = b.human("LIST PRESET", "Saved presets")
        assert listing.index(names[0]) < listing.index(names[1])
        a.human("SET GRID IO91")
        assert a.get()["status"]["preset"]["modified"] is True
        other = a.get()["configuration"]
        b.human("LOAD PRESET " + names[0].lower(), "Loaded preset")
        loaded = b.get()
        assert loaded["configuration"] == gold and loaded["status"]["preset"]["modified"] is False and loaded["status"]["preset"]["name"] == names[0]
        assert a.get()["configuration"] == other
        b.close()
        b = Session(self, self.calls[1])
        assert b.get()["configuration"] == gold
        for command, expected in (("LOAD PRESET LIVE-MISSING-" + self.token, "not found"), ("SAVE PRESET", "Usage:"), ("LIST PRESET EXTRA", "Usage:"), ("LOAD PRESET " + names[0] + " EXTRA", "Usage:"), ("DELETE PRESET", "Usage:"), ("SAVE PRESET -BAD", "must start"), ("SAVE PRESET " + "A" * 33, "must contain")):
            self.unchanged(b, command, expected)
        b.human("PASS NEARBY ON", "Nearby filter enabled")
        b.human("DIALECT CC", "Dialect set")
        cc = b.get()["configuration"]
        b.human("SAVE PRESET " + names[1], "Saved preset")
        a.human("LOAD PRESET " + names[1], "Loaded preset")
        loaded = a.get()
        assert loaded["configuration"] == cc and loaded["status"]["preset"]["modified"] is False
        a.human("SHOW/FILTER NEARBY", "ON")
        a.human("HELP LOAD PRESET", "hyphens")
        a.human("SET/FILTER NEARBY OFF", "Nearby filter disabled")
        a.human("LOAD PRESET " + names[1], "Loaded preset")
        a.close()
        a = Session(self, self.calls[0])
        assert a.get()["configuration"] == cc
        for name in names:
            a.human("DELETE PRESET " + name, "Deleted preset")
            self.owned.remove(name)
        doc = a.get()
        assert doc["configuration"] == cc and doc["status"]["preset"]["associated"] is True
        self.unchanged(a, "LOAD PRESET " + names[0], "not found")

    def history_label(self):
        return "T" + self.token

    def history_result(self, a, command, target, frequencies, expected):
        """Require exact labeled archive rows, never a queue ACK or empty PASS."""
        before = a.get()
        text, after = a.human(command)
        assert before["configuration"] == after["configuration"] and before["revision"] == after["revision"], "history search changed saved preferences"
        assert "Warning: unreadable archive records" not in text and "Search work limit reached" not in text, "history coverage is incomplete: " + text[:1000]
        actual = []
        for line in text.splitlines():
            if not line.startswith("DX de"):
                continue
            row = re.match(r"^DX de\s+\S+:\s+([0-9.]+)\s+(\S+)\s+", line)
            labels = re.findall(r"\b" + re.escape(self.history_label()) + r"-([01])\b", line)
            assert row and len(labels) == 1, "history returned malformed or unrelated row: " + line
            actual.append((row.group(2), float(row.group(1)), int(labels[0])))
        wanted = [(target, float(frequencies[index]), index) for index in expected]
        assert sorted(actual) == sorted(wanted), f"{command}: expected {wanted!r}, got {actual!r}"
        if not expected:
            assert "No matching retained spots." in text, "empty selection lacks exhausted-history status"
        return text

    def history_selections(self, a, target, frequencies):
        # Explicit mode tokens in the two DX stimuli establish CW/FT8 independently
        # of frequency inference; labels also isolate this run from ambient spots.
        label = "T" + self.token
        queries = (
            ("BAND 10m", [0]), ("BAND 15", [1]),
            ("MODE cw", [0]), ("MODE FT8", [1]),
            ("BAND 10m,15m", [0, 1]), ("BAND 10, 15", [0, 1]),
            ("MODE CW,FT8", [0, 1]), ("MODE CW, FT8", [0, 1]),
            ("BAND 10m,10 MODE CW,CW", [0]),
            ("BAND 10,15 MODE CW", [0]), ("MODE CW, FT8 BAND 15m", [1]),
            ("BAND 10 MODE FT8", []),
            ("MODE UNKNOWN", []),
            ("BAND 10 MODE CW", [0]),
        )
        for suffix, expected in queries:
            command = f"SHOW DX {target} 2 {suffix} COMMENT {label.lower()}"
            self.case("history selection " + suffix, lambda c=command, e=expected: self.history_result(a, c, target, frequencies, e))
        for phrase, expected in ((label + "-0 up-5?", [0]), (label + "-0 up-5!", [])):
            command = f"SHOW DX {target} 2 BAND 10,15 MODE CW, FT8 COMMENT {phrase}"
            self.case("history selection literal " + phrase, lambda c=command, e=expected: self.history_result(a, c, target, frequencies, e))
        for category, value, suffix, expected in (
            ("BAND", "10M", "BAND 10,15 MODE CW, FT8", [1]),
            ("MODE", "CW", "BAND 10,15 MODE CW, FT8", [1]),
        ):
            def saved_block(cat=category, val=value, search=suffix, remaining=expected):
                a.human(f"REJECT {cat} {val}")
                try:
                    self.history_result(a, f"SHOW DX {target} 2 {search} COMMENT {label}", target, frequencies, remaining)
                    self.history_result(a, f"SHOW DX {target} 2 {cat} {val} COMMENT {label}", target, frequencies, [])
                finally:
                    a.human("PASS NOFILTER")
            self.case("history narrows saved " + category, saved_block)

    def history_selection_next(self, a, target, frequencies):
        label = "T" + self.token
        command = f"SHOW DX {target} 1 BAND 10,15 MODE CW, FT8 COMMENT {label}"
        text = self.history_result(a, command, target, frequencies, [1])
        cursor = re.search(r"H1[A-F0-9]{32}", text)
        assert cursor, "selection NEXT unavailable: both labeled stimuli must survive admission"
        # The failing category may follow a valid clause. Assert its exact
        # diagnostic rather than assuming the first word owns every error.
        invalid = (
            ("BAND", "BAND"), ("MODE", "MODE"),
            ("BAND MODE CW", "BAND"), ("MODE BAND 10", "MODE"),
            ("BAND ,", "BAND"), ("MODE ,", "MODE"),
            ("BAND 10 15", "BAND"), ("MODE CW FT8", "MODE"),
            ("BAND 10,15 MODE CW FT8", "MODE"), ("MODE CW,FT8 BAND 10 15", "BAND"),
            ("BAND 10 INVALID", "BAND"), ("MODE CW INVALID", "MODE"),
            ("BAND 10,INVALID", "BAND"), ("MODE CW,INVALID", "MODE"),
            ("BAND ALL", "BAND"), ("BAND NONE", "BAND"), ("BAND UNKNOWN", "BAND"),
            ("MODE ALL", "MODE"), ("MODE NONE", "MODE"),
            ("BAND 10 BAND 15", "BAND"), ("MODE CW MODE FT8", "MODE"),
            ("BAND 10 MODE CW BAND 15", "BAND"),
        )
        for suffix, category in invalid:
            before = a.get()
            response, after = a.human(f"SHOW DX {target} 2 {suffix}")
            assert "Invalid " + category + " selection" in response, "invalid selection was not rejected: " + response
            assert before["configuration"] == after["configuration"] and before["revision"] == after["revision"]
        for alias in ("SHOW/DX", "SH/DX"):
            self.unchanged(a, f"{alias} {target} 2 BAND 10,15 MODE CW, FT8 COMMENT {label}", "Use SHOW DX or SH DX")
        older = self.history_result(a, "SHOW MYDX NEXT " + cursor.group(), target, frequencies, [0])
        assert "Older retained history page:" in older
        self.unchanged(a, "SHOW DX NEXT " + cursor.group(), "Invalid history continuation")

    def history(self, a):
        a.human("DIALECT GO")
        a.human("PASS NOFILTER")
        a.human("PAUSE 300")
        # Owner-authorized, labeled human spots create exact-call history evidence.
        offset = int(self.token[-2:], 16)
        # A non-self target is essential: history intentionally exempts self DX
        # from saved band/mode filters, which would conceal a narrowing defect.
        target = "K1ABC" if self.args.call.upper() == "VA3UXA" else "VA3UXA"
        target_prefix = "K" if target == "K1ABC" else "VE"
        frequencies = [28200 + offset, 21200 + offset]
        admitted = []
        for index in range(2):
            # DX retains the ordinary reader safe list. Only explicit COMMENT
            # commands admit printable punctuation such as ':' and '!'.
            command = f"DX {frequencies[0]} {target} CW T{self.token}-0 up-5?" if index == 0 else f"DX {target} {frequencies[1]} FT8 T{self.token}-1 up-5?"
            admitted.append(self.case(f"DX valid syntax {index}", lambda c=command: a.human(c, "Spot queued.")))
        self.case("DX invalid input", lambda: self.unchanged(a, "DX NOTAFREQ VA3UXA", "Invalid frequency"))
        # Both fixtures must survive admission/dedupe for selection coverage.
        # The separate unrestricted paging check also uses older retained data.
        if not all(admitted):
            raise BlockedPrerequisite("history stimuli were not both admitted")
        deadline = min(self.deadline, time.monotonic() + self.args.timeout)
        while True:
            exact, _ = a.human(f"SHOW DX {target} 2 COMMENT T{self.token}")
            if len(re.findall(r"\bT" + self.token + r"-[01]\b", exact)) == 2:
                self.history_result(a, f"SHOW DX {target} 2 COMMENT T{self.token}", target, frequencies, [0, 1])
                break
            if time.monotonic() >= deadline:
                raise BlockedPrerequisite("admitted labeled rows did not both reach retained history")
            time.sleep(.025)
        rows = re.findall(r"^DX de\s+\S+:\s+[0-9.]+\s+(\S+)", exact, re.MULTILINE)
        assert rows and all(call == target for call in rows), "exact-call response lacks exclusively matching rows"
        self.history_result(a, f"SHOW DX {target} 2 COMMENT T" + self.token, target, frequencies, [0, 1])
        self.history_selections(a, target, frequencies)
        self.case("history selection NEXT and invalid preservation", lambda: self.history_selection_next(a, target, frequencies))
        text, _ = a.human("SHOW DX 1", "DX de")
        token = re.search(r"H1[A-F0-9]{32}", text)
        assert token, "positive NEXT unavailable: insufficient retained matching rows"
        first = token.group()
        self.unchanged(a, "SHOW DX NEXT H1BAD", "Invalid history continuation")
        older, _ = a.human("SHOW MYDX NEXT " + first, "Older retained history page:")
        assert "DX de" in older
        self.unchanged(a, "SHOW DX NEXT " + first, "Invalid history continuation")
        for dialect in ("go", "cc"):
            a.human("DIALECT " + dialect)
            for command in ("SHOW DX 1", "SH DX 1", f"SHOW MYDX {target} 2", f"SH MYDX 2 {target}", f"SHOW DX {target_prefix} 1") + (("SHOW/DX 1", "SH/DX 1") if dialect == "cc" else ()):
                self.case(dialect + " " + command, lambda c=command: a.human(c, "DX de"))
            for alias in ("SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX") + (("SHOW/DX", "SH/DX") if dialect == "cc" else ()):
                command = f"{alias} {target} 2 MODE CW, FT8 BAND 10,15 COMMENT T{self.token}"
                self.case(dialect + " selection " + alias, lambda c=command: self.history_result(a, c, target, frequencies, [0, 1]))
            for command in ("SHOW DX 0", "SHOW MYDX 251"):
                self.case(dialect + " invalid " + command, lambda c=command: self.unchanged(a, c, "1-250"))
        self.case("SHOW PROP syntax", lambda: a.human("SHOW PROP", "Usage: SHOW PROP"))
        self.case("SHOW PROP forecast", lambda: a.human("SHOW PROP FN31 20M CW", "20m"))

    def terminal_inputs(self, call):
        for kind in ("header", "oversize", "deadline", "incomplete"):
            def terminal(k=kind):
                c = Session(self, call)
                before = c.get()["configuration"]
                if k == "header":
                    payload = "PUT YAML CONFIG EXTRA\r\n---\r\nRESUME\r\n...\r\n"
                elif k == "oversize":
                    payload = "PUT YAML CONFIG\r\n---\r\n#" + "X" * 65535 + "\r\n...\r\n"
                else:
                    payload = "PUT YAML CONFIG\r\n---\r\n"
                try:
                    # TCP RST is an observable terminal close, just like FIN.
                    try:
                        c.send(payload)
                    except ConnectionResetError:
                        pass
                    if k == "incomplete":
                        c.sock.shutdown(socket.SHUT_WR)
                    c.reader.deadline = min(self.deadline, time.monotonic() + (36 if k == "deadline" else 12))
                    received = bytearray()
                    try:
                        while len(received) <= MAX_BUFFER:
                            received.extend(c.reader.line())
                        raise AssertionError("terminal response exceeded bound")
                    except (EOFError, ConnectionResetError):
                        pass
                    self.log.write(f"\nTERMINAL {k}\n".encode() + received)
                finally:
                    c.sock.close()
                    c.closed = True
                after = Session(self, call)
                assert after.get()["configuration"] == before
                after.close()
            self.case("terminal upload " + kind, terminal)
        for verb in ("BYE", "QUIT", "EXIT"):
            self.case("disconnect " + verb, lambda v=verb: Session(self, call).close(v))

    def cleanup(self):
        # Separate time budget and fresh sockets; retired test streams are never reused.
        for session in self.sessions:
            if not session.closed:
                session.sock.close()
                session.closed = True
        self.deadline = time.monotonic() + 120
        for call, baseline in self.baselines.items():
            try:
                c = Session(self, call)
                self.acknowledge(c.upload("PUT", "CONFIG", baseline["configuration"]))
                c.human("SET DIAG " + baseline["status"]["session"]["diagnostic_comments"])
                c.human("RESUME")
                for name in list(self.owned):
                    text, _ = c.human("DELETE PRESET " + name)
                    if "Deleted preset" in text or "not found" in text:
                        self.owned.remove(name)
                    else:
                        raise AssertionError(text)
                c.close()
                verify = Session(self, call)
                assert verify.get()["configuration"] == baseline["configuration"], "baseline did not survive reconnect"
                verify.close()
            except Exception as error:
                self.cleanup_errors.append({"call": call, "detail": str(error)})
        for session in self.sessions:
            session.sock.close()
            session.closed = True

    def run(self):
        inventory = required_live_cases(self.token, self.args.call)
        required = [name for family in inventory.values() for name in family]
        try:
            main = Session(self, self.args.call.upper())
            original = main.get()
            self.remote_build, _ = main.human("SHOW BUILD", "Build version:")
            assert main.get()["configuration"] == original["configuration"]
            main.close()
            a, b = [Session(self, call) for call in self.calls]
            for c in (a, b):
                baseline = c.get()
                assert baseline["status"]["preset"]["associated"] is False, "refusing an associated test profile"
                assert baseline["status"]["session"]["temporary_defaults"] is False
                self.baselines[c.call] = baseline
            (self.args.output / "baselines.json").write_text(json.dumps(self.baselines, indent=2), encoding="utf-8")
            self.case("human commands and filters", lambda: self.reads_and_filters(a), inventory["human commands and filters"])
            self.case("pause and machine commands", lambda: self.pause_and_machine(a), inventory["pause and machine commands"])
            self.case("original preset regression plus state/SNR", lambda: self.presets(a, b))
            a = Session(self, self.calls[0])
            self.case("history and read commands", lambda: self.history(a), inventory["history and read commands"])
            a.close()
            self.terminal_inputs(self.calls[0])
        except Exception as error:
            self.results.append({"case": "setup/run", "result": "FAIL", "detail": str(error)})
        finally:
            try:
                self.cleanup()
            except Exception as error:
                self.cleanup_errors.append({"detail": f"{type(error).__name__}: {error}"})
            self.account_required(required)
            counts = {status: sum(row["result"] == status and row.get("kind") != "family" and row["case"] in required for row in self.results) for status in ("PASS", "FAIL", "BLOCKED", "UNEXECUTED")}
            report = {"host": self.args.host, "port": self.args.port, "remote_build": getattr(self, "remote_build", "unknown"), "commands_including_GET_barriers": self.commands, "required": required, "required_leaf_results": counts, "results": self.results, "wire_failures": self.wire_failures, "cleanup_errors": self.cleanup_errors, "remaining_owned_presets": sorted(self.owned), "residual_remote_test_profiles": list(self.baselines), "test_spot_comment": "T" + self.token}
            (self.args.output / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
            self.log.close()
        failures = sum(row["result"] != "PASS" for row in self.results)
        print(json.dumps({"commands": self.commands, "semantic_cases": len(self.results), "semantic_failures": failures, "wire_failures": len(self.wire_failures), "cleanup_errors": self.cleanup_errors, "output": str(self.args.output)}, indent=2))
        return int(bool(failures or self.wire_failures or self.cleanup_errors or self.owned))


class V7Session(Session):
    """Schema-seven live session; legacy Session remains available to fixtures."""

    def __init__(self, suite, call):
        self.suite, self.call, self.closed = suite, call, False
        # Avoid exhausting per-IP admission during reconnect tests; never retry
        # a refused connection as though it were a successful test.
        now = time.monotonic()
        delay = getattr(suite, "next_connection_at", 0) - now
        if delay > 0:
            check(now + delay < suite.deadline, "connection pacing exceeds run deadline")
            time.sleep(delay)
        suite.next_connection_at = time.monotonic() + .4
        self.sock = socket.create_connection((suite.args.host, suite.args.port), suite.args.timeout)
        self.reader = TelnetReader(self.sock, min(suite.deadline, time.monotonic() + suite.args.timeout))
        try:
            self.reader.until(b"login: ")
            self.send(call + "\r\n", count=False)
            while b"UTC>" not in self.reader.buffer:
                if b"Invalid call" in self.reader.buffer:
                    raise ValueError(f"server refused test login {call}")
                self.reader._receive()
            greeting = self.reader.until(b"UTC>")
            check(("Hello " + call).encode() in greeting, "successful login greeting absent")
        except BaseException:
            self.sock.close()
            raise
        suite.sessions.append(self)

    def get(self, resource="CONFIG", version=7):
        return super().get(resource, version)

    def human(self, command, expected=None, *, resource="CONFIG"):
        identifier = self.suite.identifier()
        sync = f"GET YAML {resource} SCHEMA 7 ID {identifier}"
        self.send(command + "\r\n" + sync + "\r\n")
        self.suite.commands += 1
        document, prefix = self.frame(resource, identifier, 7, command)
        if len(prefix) > MAX_BUFFER or (prefix and not prefix.endswith(b"\r\n")) or any(
                not line.endswith(b"\r") or b"\r" in line[:-1] for line in prefix.split(b"\n")[:-1]):
            self.suite.wire_failures.append({"call": self.call, "command": command, "errors": ["human reply violates CRLF/size contract"]})
        text = prefix.replace(b"\r\r\n", b"\r\n").decode("ascii", "strict")
        lines = text.splitlines()
        if self.reader.remote_echo:
            for echoed in (command.upper(), sync):
                if echoed in lines:
                    lines.remove(echoed)
        text = "\n".join(lines)
        if expected is not None:
            check(expected in text, f"{command}: missing {expected!r}: {text[:1000]}")
        # Large human output may still hold the configuration-output lease when
        # the immediate GET arrives. Retry only the read-only barrier, never the
        # human command; retain every refusal in the transcript and report.
        retry_deadline = min(self.suite.deadline, time.monotonic() + 3)
        while document.get("error", {}).get("code") == "configuration_busy" and time.monotonic() < retry_deadline:
            self.suite.busy_barriers = getattr(self.suite, "busy_barriers", [])
            self.suite.busy_barriers.append({"call": self.call, "command": command, "request_id": identifier})
            time.sleep(.1)
            identifier = self.suite.identifier()
            retry = f"GET YAML {resource} SCHEMA 7 ID {identifier}"
            self.send(retry + "\r\n")
            document, retry_prefix = self.frame(resource, identifier, 7, retry)
            if len(retry_prefix) > MAX_BUFFER or (retry_prefix and not retry_prefix.endswith(b"\r\n")) or any(
                    not line.endswith(b"\r") or b"\r" in line[:-1] for line in retry_prefix.split(b"\n")[:-1]):
                self.suite.wire_failures.append({"call": self.call, "command": retry, "errors": ["retry prefix violates CRLF/size contract"]})
        check("error" not in document and isinstance(document.get("configuration"), dict)
              and isinstance(document.get("status"), dict) and bool(document.get("revision")),
              f"invalid barrier response: {document}")
        return text, document

    def upload(self, verb, resource, configuration, version=7, revision=None, raw_body=None, error_id=False):
        identifier = self.suite.identifier()
        body = {"schema_version": version, "request_id": identifier, "configuration": configuration}
        if verb != "VALIDATE":
            body["if_revision"] = revision or self.get()["revision"]
        body_text = raw_body(identifier, body) if raw_body else yaml.safe_dump(body, sort_keys=False)
        # Schema 6 uploads select their version in the document, while schema
        # 7 requires an explicit header for the larger transport limit.
        command = f"{verb} YAML {resource}" + (f" SCHEMA {version}" if version == 7 else "")
        self.send(command + "\r\n---\r\n" + body_text.replace("\n", "\r\n") + "...\r\n")
        result = self.frame(resource, "" if error_id else identifier, version, command)[0]
        if "error" not in result:
            check(result.get("result", {}).get("operation") == verb, "wrong operation receipt")
        return result


def v7_required_cases(cc_shortcuts=()):
    """Explicit inventory: every omitted or interrupted case is a failure."""
    return ["GO rule add", "GO rule replace", "GO invalid atomic", "GO retired NEARBY",
            "GO rule remove", "GO rule reset", "CC DOC pass", "CC DOC replace",
            "CC DOC reject", "CC DOC off", "CC DXSTATE pass", "CC DXBM pass",
            "CC DXBM off", "CC DXBM reject", "CC invalid atomic", "bank switching", "GET resources",
            "PUT CONFIG unchanged", "PATCH SETTINGS", "VALIDATE CONFIG", "stale write",
            "preset sharing", "preset cross dialect", "preset cleanup",
            "history labeled spot", "GO HELP", "CC HELP", "GO settings", "CC settings",
            "GO pause resume", "CC pause resume", "GO retired category", "CC GO rule refusal",
            "settings controls", "GO delivery controls", "CC modes and sources", "CC delivery controls", "CC geographic families",
            "HELP topics and aliases", "general reads", "WWV and WCY", "history selection and paging",
            "schema 6 compatibility", "schema 7 filter writes", "schema 7 config PATCH", "invalid machine input", "GO rule readbacks", "CC filter readbacks",
            "terminal uploads", "logout aliases", "preset round trip after reconnect",
            "CC schema 7 FILTER PUT and PATCH", "schema 7 CONFIG changed both banks",
            "schema 6 GO writes preserve CC", "CC implied DOC", "dialect GO aliases",
            "preset invalid names", "preset overwrite"] + [
                f"{dialect} {name}" for dialect in ("GO", "CC") for name in (
                    "pause default", "pause 1", "pause 300", "invalid pauses", "show hold",
                    "dialect discovery", "invalid settings", "settings aliases", "solar off",
                    "WHOSPOTSME band", "PROP forecast", "PC93 alias", "HELP discovery",
                    "DXCC alias", "PROP aliases")
            ] + ["CC UNSET SKIMMER", "CC SET/FILTER controls", "CC UNSET/FILTER controls",
                 "CC inline controls", "CC control shortcuts", "CC unsupported control forms"] + [
                f"CC configured shortcut {mode}" for mode in cc_shortcuts
            ]


def validate_v7_baseline(document):
    """Reject incomplete backups and either pre-existing preset association."""
    check(document.get("schema_version") == 7 and document.get("resource") == "CONFIG", "backup is not schema 7 CONFIG")
    cfg = document.get("configuration")
    check(isinstance(cfg, dict) and set(cfg) == {"filters", "settings"}, "incomplete configuration")
    banks, settings = cfg["filters"], cfg["settings"]
    check(isinstance(banks, dict) and set(banks) == {"go", "cc"}, "incomplete filter banks")
    check(isinstance(settings, dict) and {"dialect", "grid", "noise_class", "dedupe_policy", "path_min_observation_count", "solar_summary_minutes"} <= set(settings), "incomplete settings")
    controls = {"include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic"}
    check(isinstance(banks["go"], dict) and controls | {"rules"} <= set(banks["go"]), "incomplete GO bank")
    check(isinstance(banks["cc"], dict) and controls | {"modes", "sources", "geography", "dxbm"} <= set(banks["cc"]), "incomplete CC bank")
    status = document.get("status", {})
    presets = status.get("presets")
    check(isinstance(presets, dict) and set(presets) == {"go", "cc"} and
          all(isinstance(p, dict) and p.get("associated") is False for p in presets.values()),
          "refusing associated or unreadable preset bank")
    check(isinstance(status.get("session"), dict) and status["session"].get("temporary_defaults") is False,
          "refusing temporary or unreadable defaults")
    check(isinstance(document.get("revision"), str) and bool(document["revision"]), "missing revision")


class V7Suite(Suite):
    """Live v7 suite keeps both banks and temporary preset ownership explicit."""

    def history_label(self):
        # Avoid the comment parser's unanchored digit+DB SNR pattern while
        # retaining a bijective mapping of the original random hex token.
        return "T" + self.token.translate(str.maketrans({"B": "G", "D": "H"}))

    def history_result(self, session, command, target, frequencies, expected):
        # A work-limited page is partial evidence. Follow its cursor with a
        # fixed page/time bound and keep the exact row oracle across all pages.
        deadline = min(getattr(self, "deadline", float("inf")), time.monotonic() + 120)
        wanted = [(target, float(frequencies[index]), index) for index in expected]
        initial_cursor = re.search(r"\bNEXT\s+(H1[A-F0-9]{32})\b", command)
        actual, seen_tokens = [], ({initial_cursor.group(1)} if initial_cursor else set())
        for _ in range(64):
            if time.monotonic() >= deadline:
                raise BlockedPrerequisite("history continuation time budget exhausted")
            before = session.get()
            check(before["status"]["session"]["pause_active"] is True,
                  "history delivery hold expired before query")
            text, after = session.human(command)
            check(after["status"]["session"]["pause_active"] is True,
                  "history delivery hold expired during query")
            assert before["configuration"] == after["configuration"] and before["revision"] == after["revision"], "history search changed saved preferences"
            assert "Warning: unreadable archive records" not in text, "history contains unreadable records"
            for line in text.splitlines():
                if not line.startswith("DX de"):
                    continue
                row = re.match(r"^DX de\s+\S+:\s+([0-9.]+)\s+(\S+)\s+", line)
                labels = re.findall(r"\b" + re.escape(self.history_label()) + r"-([01])\b", line)
                assert row and len(labels) == 1, "history returned malformed or unrelated row: " + line
                identity = (row.group(2), float(row.group(1)), int(labels[0]))
                assert identity in wanted and identity not in actual, "unexpected or duplicate history row: " + line
                actual.append(identity)
            partial = "Search work limit reached; this page is incomplete." in text
            exhausted = "End of retained history search." in text or "No matching retained spots." in text
            cursors = re.findall(r"Continue older history: SHOW DX NEXT (H1[A-F0-9]{32})", text)
            assert len(cursors) <= 1, "ambiguous history continuation"
            assert not (exhausted and (partial or cursors)), "contradictory history completion status"
            assert not ("No matching retained spots." in text and actual), "empty history status with rows"
            if not partial:
                assert exhausted or cursors, "history page lacks completion or continuation status"
                assert wanted or exhausted, "empty result is not an exhausted search"
                assert sorted(actual) == sorted(wanted), f"{command}: expected {wanted!r}, got {actual!r}"
                return text
            assert len(cursors) == 1, "work-limited page lacks continuation"
            token = cursors[0]
            assert token not in seen_tokens, "repeated history continuation"
            seen_tokens.add(token)
            command = "SHOW DX NEXT " + token
        raise BlockedPrerequisite("history continuation page budget exhausted")

    def __init__(self, args):
        super().__init__(args)
        # Login accepts one- or two-digit SSIDs, so use two distinct values.
        start = getattr(args, "ssid_start", None) or int(self.token, 16) % 98 + 1
        self.calls = [args.call.upper() + "-" + str(start + i) for i in range(2)]

    def cleanup(self):
        if not hasattr(self, "residual_associations"):
            self.residual_associations = {}
        for session in self.sessions:
            if not session.closed:
                session.sock.close()
                session.closed = True
        self.deadline = time.monotonic() + 120
        for call, baseline in self.baselines.items():
            try:
                c = V7Session(self, call)
                try:
                    self.acknowledge(c.upload("PUT", "CONFIG", baseline["configuration"]))
                    c.human("SET DIAG " + baseline["status"]["session"]["diagnostic_comments"])
                    c.human("RESUME")
                    c.close()
                finally:
                    if not c.closed:
                        c.sock.close()
                        c.closed = True
                verify = V7Session(self, call)
                try:
                    restored = verify.get()
                    self.residual_associations[call] = restored["status"]["presets"]
                    check(restored["configuration"] == baseline["configuration"],
                          "full two-bank baseline did not survive reconnect")
                finally:
                    verify.close()
            except Exception as error:
                self.cleanup_errors.append({"call": call, "detail": str(error)})
        # A lost restore ACK must not prevent deletion of a possibly committed
        # test-owned preset. Use a fresh connection and retain ownership on error.
        for name in list(self.owned):
            try:
                c = V7Session(self, self.calls[0])
                try:
                    response, _ = c.human("DELETE PRESET " + name)
                    check("Deleted preset" in response or "not found" in response, response)
                    self.owned.remove(name)
                finally:
                    c.close()
            except Exception as error:
                self.cleanup_errors.append({"preset": name, "detail": str(error)})
        for session in self.sessions:
            if not session.closed:
                session.sock.close()
                session.closed = True

    def _unchanged_refusal(self, a, command):
        before = a.get()
        text, after = a.human(command)
        check(any(word in text for word in ("No changes applied", "Usage:", "Invalid", "Use numbered", "requires GO", "only available")),
              f"refusal absent: {command}: {text}")
        check(before["configuration"] == after["configuration"] and before["revision"] == after["revision"],
              f"refusal changed configuration: {command}")

    def _rule(self, a, command, slot, action=None, expr=None):
        _, doc = a.human(command)
        rules = doc["configuration"]["filters"]["go"]["rules"] or []
        matches = [r for r in rules if r["slot"] == slot]
        if action is None:
            check(not matches, f"rule {slot} still present: {rules}")
        else:
            check(len(matches) == 1 and matches[0]["action"] == action and matches[0]["expr"] == expr,
                  f"wrong rule: {rules}")

    def _cc(self, a, command, category, expected):
        _, doc = a.human(command)
        actual = doc["configuration"]["filters"]["cc"]["geography"][category]["global"]
        check(actual == expected, f"{command}: expected {expected}, got {actual}")

    def exercise(self, a, b):
        a.human("DIALECT GO")
        self.case("GO rule add", lambda: self._rule(a, "PASS RULE 9 MODE=CW AND NEARBY", 9, "pass", "MODE=CW AND NEARBY"))
        self.case("GO rule replace", lambda: self._rule(a, "REJECT RULE 9 BAND=20", 9, "reject", "BAND=20M"))
        self.case("GO invalid atomic", lambda: self._unchanged_refusal(a, "PASS RULE 9 MODE=NOT_A_MODE"))
        self.case("GO retired NEARBY", lambda: self._unchanged_refusal(a, "PASS NEARBY ON"))
        self.case("GO rule remove", lambda: self._rule(a, "RESET FILTER RULE 9", 9))
        a.human("PASS RULE 9 MODE=CW")
        def reset_rules():
            _, doc = a.human("RESET FILTER RULE ALL")
            check(not doc["configuration"]["filters"]["go"]["rules"], "rule reset left rules")
        self.case("GO rule reset", reset_rules)
        self.case("GO retired category", lambda: self._unchanged_refusal(a, "PASS BAND 20M"))
        a.human("PASS RULE 9 MODE=CW AND NEARBY")
        before_switch = a.get()["configuration"]["filters"]["go"]
        a.human("DIALECT CC")
        self.case("CC DOC pass", lambda: self._cc(a, "SET/FILTER DOC/PASS K", "DOC", {"action": "pass", "countries": [291]}))
        self.case("CC DOC replace", lambda: self._cc(a, "SET/FILTER DOC/PASS VE", "DOC", {"action": "pass", "countries": [1]}))
        self.case("CC DOC reject", lambda: self._cc(a, "SET/FILTER DOC/REJECT K", "DOC", {"action": "reject", "countries": [291]}))
        self.case("CC DOC off", lambda: self._cc(a, "SET/FILTER DOC/OFF", "DOC", {"action": "off"}))
        self.case("CC DXSTATE pass", lambda: self._cc(a, "SET/FILTER DXSTATE/PASS NY", "DXSTATE", {"action": "pass", "states": ["NY"]}))
        def dxbm(active):
            _, doc = a.human("SET/FILTER DXBM/" + ("PASS 20" if active else "OFF"))
            actual = doc["configuration"]["filters"]["cc"]["dxbm"]
            check(actual["active"] is active, "DXBM active state mismatch")
            if active:
                check(actual["base_allow"] is False and actual["slots"] == {"20-CW": True, "20-RTTY": True, "20-SSB": True}, "DXBM PASS selected wrong slots")
        self.case("CC DXBM pass", lambda: dxbm(True))
        self.case("CC DXBM off", lambda: dxbm(False))
        def dxbm_reject():
            _, doc = a.human("SET/FILTER DXBM/REJECT 20")
            actual = doc["configuration"]["filters"]["cc"]["dxbm"]
            check(actual["active"] is True and actual["base_allow"] is True and
                  actual["slots"] == {"20-CW": False, "20-RTTY": False, "20-SSB": False}, "DXBM REJECT selected wrong slots")
            a.human("SET/FILTER DXBM/OFF")
        self.case("CC DXBM reject", dxbm_reject)
        self.case("CC invalid atomic", lambda: self._unchanged_refusal(a, "SET/FILTER DXSTATE/PASS NY,ZZ"))
        self.case("CC GO rule refusal", lambda: self._unchanged_refusal(a, "PASS RULE 0 MODE=CW"))
        cc = copy.deepcopy(a.get()["configuration"]["filters"]["cc"])
        def switch():
            _, doc = a.human("DIALECT GO")
            check(doc["configuration"]["filters"]["go"] == before_switch, "GO bank changed while inactive")
            _, doc = a.human("DIALECT CC")
            check(doc["configuration"]["filters"]["cc"] == cc, "CC bank changed while inactive")
        self.case("bank switching", switch)
        def gets():
            for resource in ("CONFIG", "FILTER", "SETTINGS", "CAPABILITIES"):
                doc = a.get(resource)
                if resource == "FILTER":
                    check(doc["configuration"]["dialect"] == "cc", "FILTER is not tagged CC")
                if resource == "CONFIG":
                    check(set(doc["configuration"]["filters"]) == {"go", "cc"}, "missing bank")
        self.case("GET resources", gets)
        def put():
            before = a.get()
            self.acknowledge(a.upload("PUT", "CONFIG", before["configuration"], revision=before["revision"]))
            check(a.get()["configuration"] == before["configuration"], "PUT changed configuration")
        self.case("PUT CONFIG unchanged", put)
        def patch():
            before = a.get()["configuration"]
            self.acknowledge(a.upload("PATCH", "SETTINGS", {"noise_class": "URBAN"}))
            after = a.get()["configuration"]
            check(after["settings"]["noise_class"] == "URBAN" and after["filters"] == before["filters"], "PATCH lost bank")
        self.case("PATCH SETTINGS", patch)
        def validate():
            before = a.get()
            self.acknowledge(a.upload("VALIDATE", "CONFIG", before["configuration"]), False)
            after = a.get()
            check(before["configuration"] == after["configuration"] and before["revision"] == after["revision"], "VALIDATE changed state")
        self.case("VALIDATE CONFIG", validate)
        def stale():
            before = a.get()
            result = a.upload("PATCH", "SETTINGS", {"noise_class": "RURAL"}, revision="stale-revision")
            check("error" in result, "stale write accepted")
            after = a.get()
            check(before["configuration"] == after["configuration"] and before["revision"] == after["revision"], "stale write changed state")
        self.case("stale write", stale)
        self._more_cases(a)
        self._end_user_cases(a)
        self._configured_shortcuts(a)
        self._presets(a, b)
        if not b.closed:
            b.close()
        self.case("terminal uploads", self._v7_terminal_uploads)
        self.case("logout aliases", self._v7_logouts)
        for dialect in ("GO", "CC"):
            a.human("DIALECT " + dialect)
            self.case(dialect + " HELP", lambda: a.human("HELP", "GoCluster help"))
            self.case(dialect + " settings", lambda: a.human("SHOW SETTINGS", "Session"))
            def pause():
                _, doc = a.human("PAUSE 30")
                check(doc["status"]["session"]["pause_active"] is True, "pause absent")
                _, doc = a.human("RESUME")
                check(doc["status"]["session"]["pause_active"] is False, "resume failed")
            self.case(dialect + " pause resume", pause)
        history_ok = self.case("history labeled spot", lambda: self._history(a))
        if history_ok:
            self.case("history selection and paging", lambda: self._history_selection_v7(a))

    def _more_cases(self, a):
        def go_controls():
            a.human("DIALECT GO")
            for word, field in (("BEACON", "include_beacons"), ("WWV", "allow_wwv"), ("WCY", "allow_wcy"),
                                ("ANNOUNCE", "allow_announce"), ("SELF", "allow_self"), ("TOXIC", "allow_toxic")):
                for enabled in (True, False):
                    _, doc = a.human(("PASS " if enabled else "REJECT ") + word)
                    check(doc["configuration"]["filters"]["go"][field] is enabled, f"GO {word} not {enabled}")
        self.case("GO delivery controls", go_controls)
        def rule_views():
            self._rule(a, "PASS RULE 9 MODE=CW", 9, "pass", "MODE=CW")
            for command in ("SHOW FILTER", "SHOW FILTER RULE", "SHOW FILTER FULL", "HELP RULE"):
                response, _ = a.human(command)
                check("RULE" in response.upper() and not any(word in response for word in ("Unknown command", "Unknown help topic")), f"{command}: wrong rule readback")
                if command.startswith("SHOW"):
                    check("PASS RULE 9 MODE=CW" in response, f"{command}: missing seeded rule")
                if command == "SHOW FILTER FULL":
                    check("Program" in response and "occupied slots" in response, "GO FULL lacks preparation details")
        self.case("GO rule readbacks", rule_views)
        def settings():
            for command, field, value in (("SET GRID FN31", "grid", "FN31"),
                                           ("SET NOISE RURAL", "noise_class", "RURAL"),
                                           ("SET PATHSAMPLES 1000", "path_min_observation_count", 1000),
                                           ("SET SOLAR 30", "solar_summary_minutes", 30),
                                           ("SET DEDUPE FAST", "dedupe_policy", "FAST")):
                _, doc = a.human(command)
                check(doc["configuration"]["settings"][field] == value, f"{command}: setting absent")
            for mode in ("OFF", "DEDUPE", "SOURCE", "CONF", "PATH", "MODE"):
                _, doc = a.human("SET DIAG " + mode)
                check(doc["status"]["session"]["diagnostic_comments"].upper() == mode, f"diagnostic mode {mode} absent")
        self.case("settings controls", settings)
        def modes_sources():
            a.human("DIALECT CC")
            for command, field, value, present in (("SET/FT8", "modes", "FT8", True), ("SET/NOFT8", "modes", "FT8", False),
                                                    ("SET/FT8", "modes", "FT8", True), ("UNSET/FT8", "modes", "FT8", False),
                                                    ("SET/SKIMMER", "sources", "SKIMMER", True), ("SET/NOSKIMMER", "sources", "SKIMMER", False)):
                _, doc = a.human(command)
                check((value in doc["configuration"]["filters"]["cc"][field]) is present, f"{command}: wrong {field}")
        self.case("CC modes and sources", modes_sources)
        def controls():
            for word, field in (("BEACON", "include_beacons"), ("WWV", "allow_wwv"), ("WCY", "allow_wcy"),
                                ("ANN", "allow_announce"), ("SELF", "allow_self"), ("TOXIC", "allow_toxic")):
                for enabled in (True, False):
                    command = ("PASS" if enabled else "REJECT") + " TOXIC" if word == "TOXIC" else "SET/" + ("" if enabled else "NO") + word
                    _, doc = a.human(command)
                    check(doc["configuration"]["filters"]["cc"][field] is enabled, f"CC {word} not {enabled}")
        self.case("CC delivery controls", controls)
        def geography():
            for category, argument, field, val in (("DOS", "NY", "states", "NY"), ("DXCTY", "VE", "countries", 1), ("DXSTATE", "NY", "states", "NY")):
                for action in ("PASS", "REJECT"):
                    self._cc(a, f"SET/FILTER {category}/{action} {argument}", category, {"action": action.lower(), field: [val]})
                self._cc(a, f"SET/FILTER {category}/OFF", category, {"action": "off"})
        self.case("CC geographic families", geography)
        def cc_views():
            self._cc(a, "SET/FILTER DOC/PASS K", "DOC", {"action": "pass", "countries": [291]})
            for head in ("SHOW/FILTER", "SH/FILTER"):
                for category, expected in (("", "Active filter bank: CC"), ("FULL", "Active filter bank: CC"),
                                           ("DOC", "DOC - spotter country"), ("DOS", "DOS - spotter"),
                                           ("DXCTY", "DXCTY"), ("DXSTATE", "DXSTATE"), ("DXBM", "DXBM:"),
                                           ("MODE", "Modes:"), ("SOURCE", "Sources:")):
                    command = head + (" " + category if category else "")
                    response, _ = a.human(command)
                    check(expected.lower() in response.lower() and not any(word in response for word in ("Unknown command", "Usage:")), f"{command}: wrong filter view")
                    flat = " ".join(response.split())
                    if category in ("", "FULL", "DOC"):
                        check("Global: PASS" in flat and "ADIF values: 291" in flat, f"{command}: missing seeded DOC selection")
                    if category == "FULL":
                        check(all(label in response for label in ("DOC -", "DOS -", "DXCTY -", "DXSTATE -", "DXBM:", "Modes:", "Sources:")), f"{command}: incomplete FULL view")
        self.case("CC filter readbacks", cc_views)
        def helps():
            for dialect in ("GO", "CC"):
                a.human("DIALECT " + dialect)
                for command in ("HELP", "H", "HELP LOAD PRESET", "HELP SHOW SETTINGS"):
                    text, _ = a.human(command)
                    check(text.strip() and "Unknown command" not in text and "Unknown help topic" not in text, f"{command}: missing help")
                a.human("HELP MADEUP", "Unknown help topic")
        self.case("HELP topics and aliases", helps)
        def reads():
            for dialect in ("GO", "CC"):
                a.human("DIALECT " + dialect)
                for command, expected in (("SHOW BUILD", "Build version:"), ("SHOW OWN", "Own call:"),
                                          ("SHOW DEDUPE", "Dedupe"), ("SHOW DXCC VE", "Canada"),
                                          ("WHOSPOTSME", "WHOSPOTSME"), ("SHOW PROP", "Usage: SHOW PROP")):
                    a.human(command, expected)
        self.case("general reads", reads)
        def bulletin():
            for dialect in ("GO", "CC"):
                a.human("DIALECT " + dialect)
                for command in ("SHOW WWV", "SHOW WCY", "SH WWV", "SH WCY", "SHOW/WWV", "SHOW/WCY", "SH/WWV", "SH/WCY", "SHOW WWV FULL", "SHOW WCY FULL"):
                    response, _ = a.human(command)
                    check(("WWV" in response if "WWV" in command else "WCY" in response) and
                          not any(word in response for word in ("Unknown command", "Unknown help topic", "Usage:")), f"{command}: missing bulletin response")
        self.case("WWV and WCY", bulletin)
        def filter_writes():
            a.human("DIALECT GO")
            before = a.get("FILTER")
            self.acknowledge(a.upload("PUT", "FILTER", before["configuration"], revision=before["revision"]))
            check(a.get("FILTER")["configuration"] == before["configuration"], "FILTER PUT changed state")
            self.acknowledge(a.upload("PATCH", "FILTER", {"dialect": "go", "filters": {"allow_wwv": True}}))
            check(a.get("FILTER")["configuration"]["filters"]["allow_wwv"] is True, "FILTER PATCH failed")
            settings = a.get("SETTINGS")
            self.acknowledge(a.upload("PUT", "SETTINGS", settings["configuration"], revision=settings["revision"]))
            check(a.get("SETTINGS")["configuration"] == settings["configuration"], "SETTINGS PUT changed state")
        self.case("schema 7 filter writes", filter_writes)
        def config_patch():
            before = a.get()["configuration"]
            self.acknowledge(a.upload("PATCH", "CONFIG", {"settings": {"noise_class": "QUIET"}}))
            after = a.get()["configuration"]
            check(after["settings"]["noise_class"] == "QUIET" and after["filters"] == before["filters"], "CONFIG PATCH damaged banks")
        self.case("schema 7 config PATCH", config_patch)
        def invalid():
            before = a.get()
            result = a.upload("PATCH", "SETTINGS", {"unexpected": True})
            check("error" in result, "invalid settings accepted")
            after = a.get()
            check(after["configuration"] == before["configuration"] and after["revision"] == before["revision"], "invalid write changed state")
        self.case("invalid machine input", invalid)
        def schema_six():
            a.human("DIALECT GO")
            for resource in ("FILTER", "CONFIG", "SETTINGS", "CAPABILITIES"):
                a.get(resource, 6)
            a.human("DIALECT CC")
            for resource in ("SETTINGS", "CAPABILITIES"):
                a.get(resource, 6)
            for resource in ("FILTER", "CONFIG"):
                identifier = self.identifier()
                command = f"GET YAML {resource} SCHEMA 6 ID {identifier}"
                a.send(command + "\r\n")
                doc, _ = a.frame(resource, identifier, 6, command)
                check("error" in doc, f"schema 6 {resource} unexpectedly available in CC")
        self.case("schema 6 compatibility", schema_six)

        def cc_filter_writes():
            a.human("DIALECT CC")
            original = a.get()
            filtered = a.get("FILTER")
            check(filtered["configuration"]["dialect"] == "cc", "CC FILTER missing tag")
            self.acknowledge(a.upload("PUT", "FILTER", filtered["configuration"], revision=filtered["revision"]))
            check(a.get()["configuration"] == original["configuration"], "unchanged CC FILTER PUT changed a bank")
            before = a.get()["configuration"]
            expected = copy.deepcopy(before)
            expected["filters"]["cc"]["allow_wwv"] = not before["filters"]["cc"]["allow_wwv"]
            self.acknowledge(a.upload("PATCH", "FILTER", {"dialect": "cc", "filters": {"allow_wwv": expected["filters"]["cc"]["allow_wwv"]}}))
            check(a.get()["configuration"] == expected, "CC FILTER PATCH damaged or failed to update configuration")
        self.case("CC schema 7 FILTER PUT and PATCH", cc_filter_writes)

        def changed_config():
            before = a.get()["configuration"]
            candidate = copy.deepcopy(before)
            for bank in ("go", "cc"):
                candidate["filters"][bank]["allow_wwv"] = not before["filters"][bank]["allow_wwv"]
            candidate["settings"]["noise_class"] = "RURAL" if before["settings"]["noise_class"] != "RURAL" else "URBAN"
            self.acknowledge(a.upload("PUT", "CONFIG", candidate))
            check(a.get()["configuration"] == candidate, "CONFIG PUT did not update both banks and settings")
            self.acknowledge(a.upload("PUT", "CONFIG", before))
            check(a.get()["configuration"] == before, "CONFIG PUT did not restore the snapshot")
        self.case("schema 7 CONFIG changed both banks", changed_config)

        def legacy_writes():
            a.human("DIALECT GO")
            before = a.get()["configuration"]
            for resource in ("FILTER", "CONFIG", "SETTINGS"):
                projected = a.get(resource, 6)
                self.acknowledge(a.upload("PUT", resource, projected["configuration"], version=6, revision=projected["revision"]))
                check(a.get()["configuration"] == before, f"schema 6 {resource} PUT changed either bank")
        self.case("schema 6 GO writes preserve CC", legacy_writes)

        def implied_doc():
            a.human("DIALECT CC")
            self._cc(a, "SET/FILTER K,VE/PASS", "DOC", {"action": "pass", "countries": [1, 291]})
            self._cc(a, "SET/FILTER K/REJECT", "DOC", {"action": "reject", "countries": [291]})
            self._cc(a, "SET/FILTER DOC/OFF", "DOC", {"action": "off"})
        self.case("CC implied DOC", implied_doc)

        def dialect_aliases():
            for alias in ("CLASSIC", "DEFAULT", "GOCLUSTER"):
                a.human("DIALECT CC")
                before = a.get()["configuration"]
                _, doc = a.human("DIALECT " + alias, "Dialect set to GO")
                expected = copy.deepcopy(before)
                expected["settings"]["dialect"] = "go"
                check(doc["configuration"] == expected, f"DIALECT {alias}: wrong selection or damaged settings")
        self.case("dialect GO aliases", dialect_aliases)

    def _end_user_cases(self, a):
        """Exercise user-facing commands and refusal atomicity in both dialects."""
        for dialect in ("GO", "CC"):
            a.human("DIALECT " + dialect)

            def pause(command, seconds):
                before = a.get()
                text, after = a.human(command, f"paused for {seconds}s.")
                check("Missed spots are not replayed." in text, "pause did not describe missed spots")
                check(before["configuration"] == after["configuration"] and before["revision"] == after["revision"],
                      "pause changed saved preferences")
                if seconds > 1:
                    status = after["status"]["session"]
                    check(status["pause_active"] is True and 0 < status["pause_remaining_seconds"] <= seconds,
                          "pause status did not match requested duration")

            for label, command, duration in (("pause default", "PAUSE", 30),
                                             ("pause 1", "PAUSE 1", 1),
                                             ("pause 300", "PAUSE 300", 300)):
                self.case(dialect + " " + label, lambda c=command, n=duration: pause(c, n))

            def invalid_pauses():
                a.human("PAUSE 120", "paused for 120s.")
                before = a.get()
                for command in ("PAUSE 0", "PAUSE 301", "PAUSE BAD", "PAUSE 3 EXTRA"):
                    _, after = a.human(command, "Usage: PAUSE")
                    status = after["status"]["session"]
                    check(status["pause_active"] is True and status["pause_remaining_seconds"] > 90,
                          f"{command}: invalid duration changed the existing pause")
                    check(after["configuration"] == before["configuration"] and after["revision"] == before["revision"],
                          f"{command}: invalid duration changed preferences")
            self.case(dialect + " invalid pauses", invalid_pauses)

            def hold():
                response, doc = a.human("SHOW HOLD", "Live spots paused")
                check("Suppressed spots:" in response and doc["status"]["session"]["pause_active"] is True,
                      "SHOW HOLD did not report the active pause")
                a.human("SHOW HOLD EXTRA", "Usage: SHOW HOLD")
                _, doc = a.human("RESUME")
                check(doc["status"]["session"]["pause_active"] is False, "RESUME did not clear the pause")
                a.human("SHOW HOLD", "Live spots are not paused")
            self.case(dialect + " show hold", hold)

            def dialect_discovery():
                before = a.get()
                a.human("DIALECT", "Current dialect: " + dialect)
                a.human("DIALECT LIST", "Available dialects: CC, GO")
                _, after = a.human("DIALECT LIST EXTRA", "Usage: DIALECT")
                check(after["configuration"] == before["configuration"] and after["revision"] == before["revision"],
                      "dialect query or invalid argument changed preferences")
            self.case(dialect + " dialect discovery", dialect_discovery)

            def invalid_settings():
                for command in ("SET GRID FN31 EXTRA", "SET NOISE URBAN EXTRA",
                                "SET PATHSAMPLES 40 EXTRA", "SET SOLAR OFF EXTRA",
                                "SET DEDUPE FAST EXTRA", "SET DIAG OFF EXTRA"):
                    before = a.get()
                    text, after = a.human(command, "Usage:")
                    check(text and before["configuration"] == after["configuration"] and before["revision"] == after["revision"]
                          and before["status"]["session"]["diagnostic_comments"] == after["status"]["session"]["diagnostic_comments"],
                          f"{command}: refusal changed state")
            self.case(dialect + " invalid settings", invalid_settings)

            def settings_aliases():
                before = a.get()
                for command in ("SHOW SETTINGS", "SHOW/SETTINGS", "SH/SETTINGS"):
                    response, after = a.human(command, "Session")
                    check("Grid" in response and "Noise" in response, f"{command}: incomplete settings view")
                    check(after["configuration"] == before["configuration"] and after["revision"] == before["revision"],
                          f"{command}: settings view changed preferences")
            self.case(dialect + " settings aliases", settings_aliases)

            def solar_off():
                _, doc = a.human("SET SOLAR OFF", "Solar summaries: OFF")
                check(doc["configuration"]["settings"]["solar_summary_minutes"] == 0, "solar off was not saved")
            self.case(dialect + " solar off", solar_off)
            self.case(dialect + " WHOSPOTSME band", lambda: a.human("WHOSPOTSME 20M", "WHOSPOTSME 20M"))
            self.case(dialect + " PROP forecast", lambda: a.human("SHOW PROP FN31 20M CW", "20m"))

            def pc93_alias():
                bank = dialect.lower()
                for verb, enabled in (("PASS", True), ("REJECT", False)):
                    _, doc = a.human(verb + " PC93")
                    check(doc["configuration"]["filters"][bank]["allow_announce"] is enabled,
                          f"{verb} PC93 did not change ANNOUNCE")
            self.case(dialect + " PC93 alias", pc93_alias)

            def help_discovery():
                reference = "Native CC filter reference:" if dialect == "CC" else "Filter reference:"
                a.human("HELP FILTERS", reference)
                a.human("HELP SYMBOLS", "Confidence glyphs:")
                for topic, expected in (("SHOW HOLD", "Usage: SHOW HOLD"),
                                        ("SET SOLAR", "SET SOLAR <15|30|60|OFF>"),
                                        ("SAVE PRESET", "SAVE PRESET <name>"),
                                        ("SHOW/SETTINGS", "Usage: SHOW SETTINGS")):
                    a.human("HELP " + topic, expected)
                if dialect == "CC":
                    for verb in ("SET/FILTER", "UNSET/FILTER"):
                        response, _ = a.human("HELP " + verb, verb + " -")
                        check("Switches:" in response, f"HELP {verb}: missing switch list")
            self.case(dialect + " HELP discovery", help_discovery)
            self.case(dialect + " DXCC alias", lambda: a.human("SH DXCC VE", "Canada"))

            def prop_aliases():
                for head in ("SH PROP", "SHOW/PROP", "SH/PROP"):
                    response, _ = a.human(head + " FN31 20M CW", "20m")
                    check("Unknown command" not in response and "Usage:" not in response,
                          f"{head}: propagation alias refused")
            self.case(dialect + " PROP aliases", prop_aliases)

        a.human("DIALECT CC")
        def unset_skimmer():
            a.human("SET/SKIMMER")
            _, doc = a.human("UNSET/SKIMMER")
            check("SKIMMER" not in doc["configuration"]["filters"]["cc"]["sources"],
                  "UNSET/SKIMMER did not disable skimmer reports")
        self.case("CC UNSET SKIMMER", unset_skimmer)

        controls = (("BEACON", "include_beacons"), ("WWV", "allow_wwv"),
                    ("WCY", "allow_wcy"), ("ANNOUNCE", "allow_announce"),
                    ("PC93", "allow_announce"), ("SELF", "allow_self"),
                    ("TOXIC", "allow_toxic"))
        def cc_controls(verb, enabled):
            for word, field in controls:
                # Seed the opposite value so an acknowledged no-op cannot pass.
                a.human(("REJECT " if enabled else "PASS ") + word)
                before = a.get()["configuration"]
                _, doc = a.human(verb + " " + word + " ALL")
                expected = copy.deepcopy(before)
                expected["filters"]["cc"][field] = enabled
                check(doc["configuration"] == expected, f"{verb} {word}: wrong state or unrelated changes")
        self.case("CC SET/FILTER controls", lambda: cc_controls("SET/FILTER", True))
        self.case("CC UNSET/FILTER controls", lambda: cc_controls("UNSET/FILTER", False))

        def inline_controls():
            for word, field in controls:
                for suffix, enabled in (("ON", True), ("OFF", False)):
                    a.human(("REJECT " if enabled else "PASS ") + word)
                    _, doc = a.human(f"SET/FILTER {word}/{suffix}")
                    check(doc["configuration"]["filters"]["cc"][field] is enabled,
                          f"SET/FILTER {word}/{suffix}: wrong control")
        self.case("CC inline controls", inline_controls)

        def control_shortcuts():
            aliases = (("ANN", "allow_announce"), ("ANNOUNCE", "allow_announce"),
                       ("BEACON", "include_beacons"), ("WWV", "allow_wwv"),
                       ("WCY", "allow_wcy"), ("SELF", "allow_self"), ("TOXIC", "allow_toxic"))
            for word, field in aliases:
                for command, enabled in (("SET/" + word, True), ("SET/NO" + word, False), ("UNSET/" + word, False)):
                    # Establish opposite state even when the same control has aliases.
                    a.human(("REJECT " if enabled else "PASS ") + ("ANNOUNCE" if word == "ANN" else word))
                    _, doc = a.human(command)
                    check(doc["configuration"]["filters"]["cc"][field] is enabled,
                          f"{command}: wrong control")
        self.case("CC control shortcuts", control_shortcuts)

        def unsupported_control_forms():
            for command in ("UNSET/FILTER WWV/OFF", "SET/FILTER BEACON/OFF EXTRA", "SET/FILTER DOC/PASS"):
                before = a.get()
                text, after = a.human(command)
                check(any(word in text.lower() for word in ("usage:", "no changes", "require", "does not accept", "use set/filter")),
                      f"{command}: refusal absent")
                check(after["configuration"] == before["configuration"] and after["revision"] == before["revision"],
                      f"{command}: invalid control changed preferences")
        self.case("CC unsupported control forms", unsupported_control_forms)

    def _configured_shortcuts(self, a):
        """Check each caller-supplied canonical mode against the CC bank."""
        a.human("DIALECT CC")
        for mode in getattr(self.args, "cc_shortcuts", ()):
            def shortcut(name=mode):
                for command, enabled in (("SET/" + name, True), ("SET/NO" + name, False),
                                         ("SET/" + name, True), ("UNSET/" + name, False)):
                    seed = ("UNSET/" if enabled else "SET/") + name
                    _, seeded = a.human(seed)
                    before = seeded["configuration"]
                    check((name in before["filters"]["cc"]["modes"]) is (not enabled),
                          f"{seed}: could not establish opposite mode state")
                    _, doc = a.human(command)
                    expected = copy.deepcopy(before)
                    modes = set(expected["filters"]["cc"]["modes"])
                    if enabled:
                        modes.add(name)
                    else:
                        modes.discard(name)
                    actual = doc["configuration"]
                    actual_modes = actual["filters"]["cc"]["modes"]
                    check(len(actual_modes) == len(set(actual_modes)) and set(actual_modes) == modes,
                          f"{command}: incorrect mode selection {actual_modes}")
                    expected["filters"]["cc"]["modes"] = actual_modes
                    check(actual == expected, f"{command}: changed unrelated configuration")
            self.case(f"CC configured shortcut {mode}", shortcut)

    def _presets(self, a, b):
        name = "LIVE-" + self.token
        def invalid_names():
            for command in ("SAVE PRESET", "SAVE PRESET GOOD_BAD", "LOAD PRESET ONE TWO", "LIST PRESET EXTRA"):
                before = a.get()
                _, after = a.human(command, "Usage:")
                check(before["configuration"] == after["configuration"] and before["revision"] == after["revision"]
                      and before["status"]["presets"] == after["status"]["presets"],
                      f"{command}: invalid preset request changed settings or associations")
        self.case("preset invalid names", invalid_names)
        def sharing():
            a.human("DIALECT CC")
            listing, _ = a.human("LIST PRESET")
            check(name not in listing, "preset collision")
            count = re.search(r"\((\d+)/20\)", listing)
            check(count and int(count.group(1)) < 20, "no available preset slot")
            self.owned.add(name)
            text, saved = a.human("SAVE PRESET " + name, "Saved preset")
            status = saved["status"]["presets"]["cc"]
            check("not saved" not in text.lower() and "could not" not in text.lower() and
                  status["associated"] is True and status["name"] == name and status["modified"] is False,
                  "SAVE did not establish the preset association")
            self.saved_preset = copy.deepcopy(saved["configuration"])
            listing, _ = b.human("LIST PRESET")
            check(name in listing, "shared preset absent")
        if not self.case("preset sharing", sharing):
            def blocked():
                raise BlockedPrerequisite("preset was not safely created and verified")
            for case in ("preset cross dialect", "preset overwrite", "preset round trip after reconnect", "preset cleanup"):
                self.case(case, blocked)
            return
        def cross():
            before = b.get()["configuration"]
            b.human("DIALECT GO")
            text, loaded = b.human("LOAD PRESET " + name, "Loaded preset")
            status = loaded["status"]["presets"]["cc"]
            check("not saved" not in text.lower() and "could not" not in text.lower() and
                  status["associated"] is True and status["name"] == name and status["modified"] is False,
                  "LOAD did not establish the preset association")
            after = loaded["configuration"]
            check(after["settings"] == self.saved_preset["settings"] and after["filters"]["cc"] == self.saved_preset["filters"]["cc"] and
                  after["filters"]["go"] == before["filters"]["go"], "preset did not restore CC or damaged inactive GO bank")
        self.case("preset cross dialect", cross)
        def overwrite():
            a.human("DIALECT CC")
            before = a.get()["configuration"]
            noise = "URBAN" if before["settings"]["noise_class"] != "URBAN" else "RURAL"
            a.human("SET NOISE " + noise)
            _, saved = a.human("SAVE PRESET " + name, "Saved preset")
            check(saved["status"]["presets"]["cc"]["associated"] is True and
                  saved["status"]["presets"]["cc"]["name"] == name and
                  saved["status"]["presets"]["cc"]["modified"] is False, "overwrite did not save association")
            expected = saved["configuration"]
            prior_b = b.get()["configuration"]
            _, loaded = b.human("LOAD PRESET " + name, "Loaded preset")
            actual = loaded["configuration"]
            check(actual["settings"] == expected["settings"] and actual["filters"]["cc"] == expected["filters"]["cc"]
                  and actual["filters"]["go"] == prior_b["filters"]["go"], "overwrite did not replace the shared preset")
            check(loaded["status"]["presets"]["cc"]["name"] == name and
                  loaded["status"]["presets"]["cc"]["modified"] is False, "overwritten preset association mismatch")
        self.case("preset overwrite", overwrite)
        def reconnect():
            expected = b.get()["configuration"]
            b.close()
            reconnected = V7Session(self, self.calls[1])
            try:
                check(reconnected.get()["configuration"] == expected, "preset did not survive reconnect")
            finally:
                reconnected.close()
        self.case("preset round trip after reconnect", reconnect)
        def delete():
            check(name in self.owned, "refusing to delete a preset that this run does not own")
            a.human("DELETE PRESET " + name, "Deleted preset")
            self.owned.remove(name)
        self.case("preset cleanup", delete)

    def _v7_logouts(self):
        for verb in ("BYE", "QUIT", "EXIT"):
            V7Session(self, self.calls[1]).close(verb)

    def _v7_terminal_uploads(self):
        for kind in ("header", "oversize", "incomplete", "deadline"):
            session = V7Session(self, self.calls[1])
            before = session.get()["configuration"]
            if kind == "header":
                payload = "PUT YAML CONFIG SCHEMA 7 EXTRA\r\n---\r\nRESUME\r\n...\r\n"
            elif kind == "oversize":
                payload = "PUT YAML CONFIG SCHEMA 7\r\n---\r\n#" + "X" * MAX_BUFFER + "\r\n...\r\n"
            else:
                payload = "PUT YAML CONFIG SCHEMA 7\r\n---\r\n"
            try:
                try:
                    session.send(payload)
                except ConnectionResetError:
                    pass
                if kind == "incomplete":
                    session.sock.shutdown(socket.SHUT_WR)
                session.reader.deadline = min(self.deadline, time.monotonic() + (36 if kind == "deadline" else 12))
                received = bytearray()
                try:
                    while len(received) <= MAX_BUFFER:
                        received.extend(session.reader.line())
                    raise ValueError("terminal response exceeded bound")
                except (EOFError, ConnectionResetError):
                    pass
                self.log.write(f"\nTERMINAL {kind}\n".encode() + received)
            finally:
                session.sock.close()
                session.closed = True
            verify = V7Session(self, self.calls[1])
            try:
                check(verify.get()["configuration"] == before, f"{kind} upload changed configuration")
            finally:
                verify.close()

    def _history(self, a):
        a.human("DIALECT GO")
        a.human("RESET FILTER")
        # Drain already queued live spots at the correlated barrier, then hold
        # live delivery while exact history rows are collected.
        _, paused = a.human("PAUSE 300")
        check(paused["status"]["session"]["pause_active"] is True, "history delivery hold not active")
        # Earlier control tests deliberately disable these switches. Resetting
        # rules does not reset controls; history must request an unrestricted view.
        for control in ("BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"):
            a.human("PASS " + control)
        ready = self.started_at + self.args.history_wait_seconds
        remaining = ready - time.monotonic()
        if remaining > 0:
            check(time.monotonic() + remaining < self.deadline, "history probation wait exceeds run deadline")
            time.sleep(remaining)
        target = "VA3UXA"
        offset = int(self.token[-2:], 16)
        frequencies = [28010 + offset % 90, 21010 + offset % 90]
        label = self.history_label()
        for command in (f"DX {frequencies[0]} {target} CW {label}-0 up-5?", f"DX {target} {frequencies[1]} FT8 {label}-1 up-5?"):
            response, _ = a.human(command)
            if "Spot rejected." in response:
                raise BlockedPrerequisite("server rejected the labeled spot; retained history cannot be qualified")
            check("Spot queued." in response, f"DX intake was not confirmed: {response}")
        deadline = min(self.deadline, time.monotonic() + self.args.history_timeout_seconds)
        while time.monotonic() < deadline:
            text, state = a.human(f"SHOW DX {target} 2 COMMENT {label}")
            check(state["status"]["session"]["pause_active"] is True, "history delivery hold expired while polling")
            if all(re.search(r"\b" + label + "-" + str(index) + r"\b", text) for index in (0, 1)):
                self.history_result(a, f"SHOW DX {target} 2 COMMENT {label}", target, frequencies, [0, 1])
                self.history_target, self.history_frequencies = target, frequencies
                return
            time.sleep(.5)
        raise BlockedPrerequisite("both labeled spots not found in retained history")

    def _history_selection_v7(self, a):
        target, frequencies = self.history_target, self.history_frequencies
        label = self.history_label()
        for dialect in ("GO", "CC"):
            a.human("DIALECT " + dialect)
            if dialect == "CC":
                a.human("SET/NOFILTER")
                # NOFILTER restores operator defaults, which need not include
                # the two modes in our labeled history fixtures.
                a.human("SET/CW")
                a.human("SET/FT8")
                for control in ("BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"):
                    a.human("PASS " + control)
            for selector, expected in (("BAND 10", [0]), ("BAND 15", [1]), ("MODE CW", [0]),
                                       ("MODE FT8", [1]), ("BAND 10 MODE FT8", []),
                                       ("BAND 10,15 MODE CW,FT8", [0, 1])):
                self.history_result(a, f"SHOW DX {target} 2 {selector} COMMENT {label}", target, frequencies, expected)
            for alias in (("SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX") if dialect == "GO" else
                          ("SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX", "SHOW/DX", "SH/DX")):
                self.history_result(a, f"{alias} {target} 2 COMMENT {label}", target, frequencies, [0, 1])
            self.unchanged(a, "SHOW DX " + target + " 2 BAND INVALID", "Invalid BAND selection")
        a.human("DIALECT GO")
        first = self.history_result(a, f"SHOW DX {target} 1 COMMENT {label}", target, frequencies, [1])
        cursor = re.search(r"H1[A-F0-9]{32}", first)
        check(cursor is not None, "history continuation token absent")
        self.unchanged(a, "SHOW DX " + target + " 1 BAND INVALID", "Invalid BAND selection")
        self.history_result(a, "SHOW DX NEXT " + cursor.group(), target, frequencies, [0])

    def run(self):
        required = v7_required_cases(getattr(self.args, "cc_shortcuts", ()))
        self.residual_associations = {}
        self.started_at = time.monotonic()
        print("Test profiles: " + ", ".join(self.calls), flush=True)
        try:
            a, b = [V7Session(self, call) for call in self.calls]
            for session in (a, b):
                baseline = session.get()
                validate_v7_baseline(baseline)
                self.baselines[session.call] = baseline
            (self.args.output / "baselines.json").write_text(json.dumps(self.baselines, indent=2), encoding="utf-8")
            self.remote_build, _ = a.human("SHOW BUILD", "Build version:")
            self.exercise(a, b)
        except Exception as error:
            self.results.append({"case": "setup/run", "result": "FAIL", "detail": f"{type(error).__name__}: {error}"})
        finally:
            self.cleanup()
            self.account_required(required)
            counts = {status: sum(row["result"] == status and row["case"] in required for row in self.results) for status in ("PASS", "FAIL", "BLOCKED", "UNEXECUTED")}
            report = {"host": self.args.host, "port": self.args.port, "remote_build": getattr(self, "remote_build", "unknown"),
                      "commands_including_GET_barriers": self.commands, "required": required, "required_leaf_results": counts,
                      "results": self.results, "wire_failures": self.wire_failures, "cleanup_errors": self.cleanup_errors,
                      "busy_barriers": getattr(self, "busy_barriers", []),
                      "remaining_owned_presets": sorted(self.owned), "residual_remote_test_profiles": list(self.baselines),
                      "residual_profile_preset_associations": self.residual_associations,
                      "test_spot_comment": self.history_label(),
                      "retired_coverage": "schema 1-4 filters, legacy category commands and NEARBY toggles were replaced by schema 7 and GO rules / native CC"}
            (self.args.output / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
            self.log.close()
        failures = sum(row["result"] != "PASS" for row in self.results)
        print(json.dumps({"commands": self.commands, "required_leaf_results": counts, "failures": failures,
                          "wire_failures": len(self.wire_failures), "cleanup_errors": self.cleanup_errors,
                          "output": str(self.args.output)}, indent=2))
        return int(bool(failures or self.wire_failures or self.cleanup_errors or self.owned))


def main():
    if sys.flags.optimize:
        raise RuntimeError("optimized Python disables safety guards; rerun without -O/PYTHONOPTIMIZE")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=8300)
    parser.add_argument("--call", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=12)
    parser.add_argument("--run-seconds", type=float, default=900)
    parser.add_argument("--ssid-start", type=int, required=True, help="first of two verified unused adjacent numeric SSIDs (1..98)")
    parser.add_argument("--history-wait-seconds", type=float, default=0, help="minimum elapsed time after start before posting history stimuli")
    parser.add_argument("--history-timeout-seconds", type=float, default=120, help="maximum time to await both labeled history rows")
    parser.add_argument("--cc-shortcuts", default=None, metavar="CW,FT8,...",
                        help="canonical enabled CC shortcut modes, comma-separated (optional)")
    args = parser.parse_args()
    if args.cc_shortcuts is not None:
        parts = [part.strip().upper() for part in args.cc_shortcuts.split(",")]
        reserved = {"NOFILTER", "SKIMMER", "NOSKIMMER", "ANN", "ANNOUNCE", "BEACON", "WWV", "WCY", "SELF", "TOXIC"}
        if (any(not re.fullmatch(r"[A-Z0-9_]+", part) or part.startswith("NO") or part in reserved for part in parts)
                or len(parts) != len(set(parts)) or len(parts) > 128):
            parser.error("--cc-shortcuts requires distinct canonical mode names without fixed-command collisions")
        args.cc_shortcuts = parts
    else:
        args.cc_shortcuts = []
    if not re.fullmatch(r"[A-Za-z0-9]+", args.call) or not 1 <= args.port <= 65535 or args.timeout <= 0 or args.run_seconds <= 0 or args.history_wait_seconds < 0 or args.history_timeout_seconds <= 0 or (args.ssid_start is not None and not 1 <= args.ssid_start <= 98):
        parser.error("use a base callsign, valid port and positive deadlines")
    return V7Suite(args).run()


if __name__ == "__main__":
    sys.exit(main())
