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
    if len(raw) > MAX_FRAME:
        errors.append("frame exceeds 65536 raw bytes")
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
        if document.get(key) != expected:
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
        while not frame:
            line = self.reader.line()
            if line.rstrip(b"\r\n") == b"---":
                frame.extend(line)
            else:
                prefix.extend(line)
            if len(prefix) > MAX_BUFFER:
                raise ValueError("no frame within prefix limit")
        while frame[-1:] != b"\n" or frame.split(b"\n")[-2].rstrip(b"\r") != b"...":
            frame.extend(self.reader.line())
            if len(frame) > MAX_BUFFER:
                raise ValueError("frame recovery limit exceeded")
        self.suite.log.write(f"\n[{self.call}] {command}\n".encode() + bytes(prefix) + bytes(frame))
        self.suite.log.flush()
        document, errors = decode_frame(bytes(frame), resource, identifier, version)
        if errors:
            self.suite.wire_failures.append({"call": self.call, "command": command, "errors": errors})
        return document, bytes(prefix)

    def get(self, resource="CONFIG", version=4):
        identifier = self.suite.identifier()
        command = f"GET YAML {resource}" + (f" SCHEMA {version}" if version != 1 else "") + f" ID {identifier}"
        self.send(command + "\r\n")
        document, _ = self.frame(resource, identifier, version, command)
        assert "error" not in document, document.get("error")
        assert isinstance(document.get("configuration"), dict) and isinstance(document.get("status"), dict)
        assert isinstance(document.get("revision"), str) and document["revision"]
        return document

    def human(self, command, expected=None):
        identifier = self.suite.identifier()
        sync = f"GET YAML CONFIG SCHEMA 4 ID {identifier}"
        self.send(command + "\r\n" + sync + "\r\n")
        self.suite.commands += 1
        document, prefix = self.frame("CONFIG", identifier, 4, command)
        if command.startswith(("SHOW FILTER", "SHOW/FILTER", "SH/FILTER", "SHOW SETTINGS")):
            start = prefix.find(b"User          ")
            end = prefix.find(b"Missed spots are not replayed.")
            if start >= 0 and end > start:
                raw = prefix[start:prefix.find(b"\n", end) + 1]
                lines = raw.split(b"\n")[:-1]
                if len(raw) > MAX_FRAME or any(not line.endswith(b"\r") or b"\r" in line[:-1] for line in lines):
                    self.suite.wire_failures.append({"call": self.call, "command": command, "errors": ["human readback violates CRLF/size contract"]})
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
            self.send(verb + "\r\n")
            self.reader.until(b"73!")
        finally:
            self.sock.close()
            self.closed = True


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

    def case(self, name, operation):
        try:
            operation()
            self.results.append({"case": name, "result": "PASS"})
        except (AssertionError, ValueError, EOFError, OSError, yaml.YAMLError) as error:
            self.results.append({"case": name, "result": "FAIL", "detail": str(error)})
            print(f"FAIL {name}: {str(error)[:220]}", flush=True)

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
            for command, allow, block in (
                ("RESET FILTER COMMENT", [], []),
                ("PASS COMMENT up  5: please!", ["UP  5: PLEASE!"], []),
                ("PASS COMMENT up  5: please!", ["UP  5: PLEASE!"], []),
                ("REJECT COMMENT QRT", ["UP  5: PLEASE!"], ["QRT"]),
                ("REJECT COMMENT up  5: please!", [], ["QRT", "UP  5: PLEASE!"]),
                ("REMOVE REJECT COMMENT UP  5: PLEASE!", [], ["QRT"]),
                ("RESET FILTER COMMENT REJECT", [], []),
                ("PASS COMMENT " + "A" * 64, ["A" * 64], []),
                ("RESET FILTER COMMENT PASS", [], []),
            ):
                def comment_rule(c=command, pa=allow, re=block):
                    _, doc = a.human(c)
                    fields = doc["configuration"]["filters"]
                    assert fields["comments"] == pa and fields["block_comments"] == re
                self.case(dialect + " " + command, comment_rule)
            self.case(dialect + " COMMENT overlong", lambda: self.unchanged(a, "PASS COMMENT " + "A" * 65, "Usage:"))
            for command, expected in (("HELP", "Available commands:"), ("H", "Available commands:"), ("DIALECT LIST", "GO"), ("SHOW BUILD", "Build version:"), ("SHOW OWN", "Own call: " + self.args.call.upper()), ("SHOW DEDUPE", "Dedupe"), ("SHOW DXCC VE", "Canada"), ("WHOSPOTSME", "WHOSPOTSME"), ("WHOSPOTSME 20M", "WHOSPOTSME")):
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
            domains = {"BAND": ("20M", "bands", "20m"), "MODE": ("CW", "modes", "CW"), "SOURCE": ("HUMAN", "sources", "HUMAN"), "EVENT": ("POTA", "events", "POTA"), "CONFIDENCE": ("C", "confidence", "C"), "PATH": ("HIGH", "path_classes", "HIGH"), "DXCONT": ("NA", "dx_continents", "NA"), "DECONT": ("EU", "de_continents", "EU"), "DXZONE": ("5", "dx_zones", 5), "DEZONE": ("14", "de_zones", 14), "DXGRID2": ("FN20", "dx_grid2", "FN"), "DEGRID2": ("IO", "de_grid2", "IO"), "DXDXCC": ("VE", "dx_dxcc", 1), "DEDXCC": ("291", "de_dxcc", 291), "DXSTATE": ("ON,NY", "dx_states", "ON"), "DESTATE": ("QC,CA", "de_states", "QC")}
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
            for domain, field in (("BEACON", "include_beacons"), ("WWV", "allow_wwv"), ("WCY", "allow_wcy"), ("ANNOUNCE", "allow_announce"), ("SELF", "allow_self"), ("TOXIC", "allow_toxic")):
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
            for resource in ("FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"):
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
            labels = re.findall(r"\bT" + self.token + r"-([01])\b", line)
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
        for index in range(2):
            # DX retains the ordinary reader safe list. Only explicit COMMENT
            # commands admit printable punctuation such as ':' and '!'.
            command = f"DX {frequencies[0]} {target} CW T{self.token}-0 up-5?" if index == 0 else f"DX {target} {frequencies[1]} FT8 T{self.token}-1 up-5?"
            self.case(f"DX valid syntax {index}", lambda c=command: a.human(c, "Spot queued."))
            time.sleep(1)
        self.case("DX invalid input", lambda: self.unchanged(a, "DX NOTAFREQ VA3UXA", "Invalid frequency"))
        # Both fixtures must survive admission/dedupe for selection coverage.
        # The separate unrestricted paging check also uses older retained data.
        time.sleep(.5)
        exact, _ = a.human(f"SHOW DX {target} 2")
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
            except (AssertionError, ValueError, EOFError, OSError, yaml.YAMLError) as error:
                self.cleanup_errors.append({"call": call, "detail": str(error)})
        for session in self.sessions:
            session.sock.close()
            session.closed = True

    def run(self):
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
            self.case("human commands and filters", lambda: self.reads_and_filters(a))
            self.case("pause and machine commands", lambda: self.pause_and_machine(a))
            self.case("original preset regression plus state/SNR", lambda: self.presets(a, b))
            a = Session(self, self.calls[0])
            self.case("history and read commands", lambda: self.history(a))
            a.close()
            self.terminal_inputs(self.calls[0])
        except (AssertionError, ValueError, EOFError, OSError, yaml.YAMLError) as error:
            self.results.append({"case": "setup/run", "result": "FAIL", "detail": str(error)})
        finally:
            self.cleanup()
            report = {"host": self.args.host, "port": self.args.port, "remote_build": getattr(self, "remote_build", "unknown"), "commands_including_GET_barriers": self.commands, "results": self.results, "wire_failures": self.wire_failures, "cleanup_errors": self.cleanup_errors, "remaining_owned_presets": sorted(self.owned), "residual_remote_test_profiles": list(self.baselines), "test_spot_comment": "T" + self.token}
            (self.args.output / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
            self.log.close()
        failures = sum(row["result"] == "FAIL" for row in self.results)
        print(json.dumps({"commands": self.commands, "semantic_cases": len(self.results), "semantic_failures": failures, "wire_failures": len(self.wire_failures), "cleanup_errors": self.cleanup_errors, "output": str(self.args.output)}, indent=2))
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
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9]+", args.call) or not 1 <= args.port <= 65535 or args.timeout <= 0 or args.run_seconds <= 0:
        parser.error("use a base callsign, valid port and positive deadlines")
    return Suite(args).run()


if __name__ == "__main__":
    sys.exit(main())
