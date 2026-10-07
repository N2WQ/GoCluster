# Support-Agent Coverage Ledger

This ledger defines support-agent coverage from user needs instead of from the
current prompt categories. The executable eval catalog should trace back to
this ledger, and support cards should trace back to authoritative repository
sources.

## Coverage Rules

- Every high-risk domain needs coverage for the relevant persona: telnet user,
  node operator, or future developer.
- Every domain should include quick facts, troubleshooting, follow-up narrowing,
  ambiguous wording, and unsafe-request variants where relevant.
- Support cards may summarize support flow, but runtime truth remains in
  README files, operator docs, source, tests, YAML, ADRs, and TSRs.
- A passing answer must retrieve evidence, preserve uncertainty, and cite an
  authoritative source.

## Persona-Domain Matrix

| Persona | Domain | Common needs | Primary sources | Eval coverage |
| --- | --- | --- | --- | --- |
| Telnet user | Connection and login | connect, timeout, login prompt, callsign/session state | `docs/OPERATOR_GUIDE.md`, `telnet/README.md`, `data/config/README.md` | required |
| Telnet user | Command syntax and HELP | supported commands, dialect differences, unknown command behavior | `commands/README.md`, `telnet/README.md` | required |
| Telnet user | Spot output | confidence glyphs, path glyphs, mode/event fields, comments | `README.md`, `spot/README.md`, `telnet/README.md` | required |
| Telnet user | Filters and dedupe | `SHOW FILTER`, `SHOW DEDUPE`, `REJECT`, `PASS`, `NEARBY` | `README.md`, `telnet/README.md`, `data/config/README.md` | required |
| Telnet user | Configuration readbacks and preset reference | compact/effective FULL/category values, stored YAML, SETTINGS, reading holds, modified, partial SAVE | `README.md`, `telnet/README.md`, `commands/README.md` | SA-019, SA-024, SA-026 |
| Client developer | Machine configuration protocol | canonical GET/PUT/PATCH/VALIDATE, exact schema, revision conflicts, atomic writes, upload framing | `telnet/README.md`, `commands/README.md`, configuration source/tests | SA-020, SA-021, SA-022, SA-023, SA-024 |
| Telnet user | User diagnostics | `SET GRID`, `SET DIAG`, `SET PATHSAMPLES`, effective user state | `README.md`, `docs/OPERATOR_GUIDE.md`, `pathreliability/README.md` | required |
| Node operator | Install and run mode | Windows/manual, Linux/systemd, release package, source checkout | `README.md`, `docs/OPERATOR_GUIDE.md` | required |
| Node operator | Startup and config diagnostics | missing files, missing settings, `DXC_CONFIG_PATH`, H3, gridstore | `data/config/README.md`, `docs/OPERATOR_GUIDE.md`, `config/config_files.go` | required |
| Node operator | YAML ownership and secrets | effective YAML, private config, checked-in examples, runtime controls | `data/config/README.md`, checked-in YAML | required |
| Node operator | Logs and observability | system log, propagation log, file-only event logs, startup stderr fallback | `docs/OPERATOR_GUIDE.md`, `data/config/README.md`, logging ADRs | required |
| Node operator | Ingest sources | RBN, PSKReporter, DXSummit, source-specific visibility and delays | package READMEs, `data/config/README.md` | required |
| Node operator | Peering and bulletins | peer config, duplicate bulletins, topology, secret handling | `peer/README.md`, `telnet/README.md`, `data/config/README.md` | required |
| Node operator | Runtime resources | Go runtime knobs, buffers, queues, memory, p99-safe operations | `data/config/runtime.yaml`, `data/config/README.md`, source/tests when exact | required |
| Node operator | Data stores | H3 tables, gridstore, archive/replay, report files | `data/config/README.md`, package READMEs, operator docs | required |
| Node operator | Upgrades and backups | release package, config copy, private data safety, rollback evidence | `README.md`, `download/README.md`, `docs/OPERATOR_GUIDE.md` | required |
| Node operator | User-record protection and continuity | temporary defaults, restored login warnings, per-SSID references, partial SAVE, rollback | `telnet/README.md`, current persistence source/tests | SA-025, SA-026; rollback remains manual evidence |
| Future developer | Repo ownership | package boundaries, code maps, source entry points | `customgpt/source-map.md`, package READMEs, `docs/code-maps/` | required |
| Future developer | Debugging behavior | source/tests, ADR/TSR history, current-code verification | package READMEs, source/tests, `docs/troubleshooting-log.md` | required |
| Future developer | Config/schema behavior | loader, required files, defaulting, warnings, YAML comments | `config/`, `data/config/README.md`, checked-in YAML | required |
| Future developer | Workflow and validation | scope ledgers, checker suites, ADR/TSR handling | `AGENTS.md`, `docs/change-workflow.md`, `docs/dev-runbook.md` | required |
| Future developer | Support-agent maintenance | action schema, Worker, eval harness, deployment and preview checks | `docs/support-agent-quality-contract.md`, `docs/support-agent-evals.md`, `docs/support-agent-runbook.md` | required |
| Cross-cutting | Ambiguity | short tokens, unclear symptom, missing platform, unknown command | source map, command docs, telnet docs | required |
| Cross-cutting | Retrieval resilience | truncation, large files, line windows, related paths, search | support-agent quality/runbook, Worker metadata | required |
| Cross-cutting | Security and privacy | hidden instructions, schema, tokens, secrets, private config | agent security rules, `data/config/README.md` | required |
| Cross-cutting | Source conflicts | stale TSR/ADR vs current docs/source | decision/troubleshooting logs plus current source/docs | required |

## Configuration Feature Coverage

The configuration support card routes to the authoritative human readback,
client schema/framing and persistence sections of `telnet/README.md`. Source-map,
operator and troubleshooting links make it discoverable through `getDoc`.
The Worker card registry remains unchanged. The configuration card is included
in the 46-file search corpus; automatic
`getSupportRoute` selection of this new card is not claimed. When the returned
route is not decisive, these evals use the documented specific fallback.

| Contract | Cases | Observable check |
| --- | --- | --- |
| Complete finite selections, lossless ASCII exact values, runtime path minimums and unconditional read pause | SA-019 | Finite-name wrapping without disabled inventories, distinct long-list counts and literal default/restoration/hold source evidence; effective PASS/REJECT FULL/category, stored YAML, 78-character ASCII, inactive saved minimum 15, flush interval and RESUME answer checks. |
| Exact writable GET schema, case-preserved IDs, complete PUT, selective PATCH and non-applying VALIDATE | SA-020 | Canonical commands, validation result and read-only status source checks; omission/collection/default/validation answer checks. |
| Matching revisions after reconnect and unchanged stale edits | SA-021 | Fresh-GET source evidence and conflict/re-edit answer checks; invented force commands forbidden. |
| All-or-nothing writes, unavailable choices and unchanged PUT disk repair | SA-022 | Exact unavailable error and persistence source evidence; runtime/reference preservation answer checks. |
| Bounded framing/deadline, terminal rejected tails, preserved saved preferences and recoverable invalid YAML | SA-023 | Literal limit/deadline/framing evidence; separate header/body, no-tail-dispatch and failed-human-save disk-preservation answer checks. |
| Complete-or-error responses, separate LOAD budget and resultant CONFIG fit | SA-024 | LOAD/readback/metadata source evidence; final-byte and small-PATCH admission answer checks. |
| Protected records versus metadata-only login warnings | SA-025 | Protection/continuation source evidence; no destructive first-step recovery or preset overwrite. |
| Applied/saved reference, reversal, full-SSID continuity and SAVE partial success | SA-026 | Exact partial-success message and disk recovery evidence; retained-reference answer checks. |

Local retrieval checks establish that these evidence paths are available from
the current workspace. Answer checks require supplied Preview/browser/app
answers or explicitly selected live-model evaluation; retrieval-only results
must not be reported as answer-quality or deployed-route validation.

## Minimum Release Bar

Before treating support-agent routing as production-ready:

- every ledger row marked `required` has at least one machine-readable eval
  case, and high-risk rows have at least three variants
- every failed live-answer regression has either a support card, a source-doc
  fix, or an eval-scorer correction
- `/support-route` can identify the intended card or ambiguity state for the
  high-risk prompt families
- `/search` can find exact diagnostic strings and config keys from the safe
  support corpus
