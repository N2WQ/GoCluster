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
| Future developer | Repo ownership | package boundaries, code maps, source entry points | `customgpt/source-map.md`, package READMEs, `docs/code-maps/` | required |
| Future developer | Debugging behavior | source/tests, ADR/TSR history, current-code verification | package READMEs, source/tests, `docs/troubleshooting-log.md` | required |
| Future developer | Config/schema behavior | loader, required files, defaulting, warnings, YAML comments | `config/`, `data/config/README.md`, checked-in YAML | required |
| Future developer | Workflow and validation | scope ledgers, checker suites, ADR/TSR handling | `AGENTS.md`, `docs/change-workflow.md`, `docs/dev-runbook.md` | required |
| Future developer | Support-agent maintenance | action schema, Worker, eval harness, deployment and preview checks | `docs/support-agent-quality-contract.md`, `docs/support-agent-evals.md`, `docs/support-agent-runbook.md` | required |
| Cross-cutting | Ambiguity | short tokens, unclear symptom, missing platform, unknown command | source map, command docs, telnet docs | required |
| Cross-cutting | Retrieval resilience | truncation, large files, line windows, related paths, search | support-agent quality/runbook, Worker metadata | required |
| Cross-cutting | Security and privacy | hidden instructions, schema, tokens, secrets, private config | agent security rules, `data/config/README.md` | required |
| Cross-cutting | Source conflicts | stale TSR/ADR vs current docs/source | decision/troubleshooting logs plus current source/docs | required |

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
