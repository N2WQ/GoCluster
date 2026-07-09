# Source Map

Use this map to route questions to authoritative GoCluster sources. Prefer
linking or citing these docs over duplicating their content here.

| Topic | Primary source | Supporting source |
| --- | --- | --- |
| Project overview | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Ready-to-run release package | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [download/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/download/README.md) |
| Build, release, profiling, and helper scripts | [scripts/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/scripts/README.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), script comment-based help |
| Windows run steps | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Linux build and service steps | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Real-node configuration | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| YAML ownership boundaries | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), package READMEs |
| YAML headers, comments, and local context | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) | effective YAML in the active config directory |
| Config loader behavior | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) | `config/` tests |
| Secrets and private config safety | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| System, dropped-call, and file-only event logs | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [docs/decisions/ADR-0093-file-only-connection-and-gate-event-logs.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0093-file-only-connection-and-gate-event-logs.md) |
| Telnet login and session behavior | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Telnet automatic read pause, `SHOW HOLD`, and `RESUME` | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md) |
| Command HELP source | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) | [commands/processor.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/processor.go), [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Command dialects | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| `DX`, `SHOW`, `SHOW MYDX`, `SHOW DXCC`, `SHOW PROP`, `SHOW OWN` | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) | [commands/processor.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/processor.go), [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| `WHOSPOTSME` and own-call SSID behavior | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md), [spot/who_spots_me.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/who_spots_me.go) |
| Solar summaries | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| Filters | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md), `filter/` |
| Filter command examples by band, zone, DXCC, grid, callsign, source, confidence, and path | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md) |
| EVENT filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Toxic comment filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [cloudflare/toxicity-worker/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/cloudflare/toxicity-worker/README.md) |
| Spot line format and beacon comments | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) | [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md), [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md) |
| MODE taxonomy | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md) |
| `NEARBY` filtering | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) |
| Dedupe policies | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), `dedup/` |
| Bulletin dedupe | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md) | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Confidence tags | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md) |
| Call correction | [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md) | `docs/decisions/`, `docs/troubleshooting/` |
| Path reliability tags, diagnostics, and `SHOW PROP` outlook | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), [pathreliability/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pathreliability/README.md), [data/config/PATH_PREDICTIONS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/PATH_PREDICTIONS.md), [docs/decisions/current-path-voacap-contract-map.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/current-path-voacap-contract-map.md) |
| RBN ingest | [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md) | `rbn/` tests |
| PSKReporter ingest | [pskreporter/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pskreporter/README.md) | `pskreporter/` tests |
| DXSummit ingest | [dxsummit/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/dxsummit/README.md) | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Peer behavior | [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md) | `docs/decisions/` |
| Repo layout | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) | package READMEs, [scripts/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/scripts/README.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [data/h3/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/h3/README.md) |
| Developer workflow | [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) | [docs/WORKING_WITH_CODEX.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/WORKING_WITH_CODEX.md), [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md) |
| Independent-agent, SELF-AUDIT, and subagent delegation workflow | [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/WORKING_WITH_CODEX.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/WORKING_WITH_CODEX.md), [docs/review-checklist.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/review-checklist.md), [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md), `codex-skills/` |
| Agent lesson memory | [docs/agent-lessons/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/agent-lessons/README.md) | [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md), [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/decision-memory.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-memory.md) |
| Code walking, blast-radius, and leak-detection workflow | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md) | [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), `codex-skills/` |
| Dependency visualization and code-map summaries | [docs/code-maps/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-maps/README.md) | [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), `codex-skills/` |
| Slice-shaped Scope Ledgers | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md) | [docs/templates/non-trivial-change-template.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/templates/non-trivial-change-template.md), [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md), [docs/decisions/ADR-0144-slice-shaped-scope-ledgers.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0144-slice-shaped-scope-ledgers.md) |
| Captured validation command evidence | [docs/review-checklist.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/review-checklist.md) | [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md), [docs/decisions/ADR-0204-captured-validation-evidence-for-high-risk-claims.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0204-captured-validation-evidence-for-high-risk-claims.md) |
| Support-agent answer quality, routing depth, and evaluations | [docs/support-agent-quality-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-quality-contract.md) | [docs/support-agent-evals.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-evals.md), [docs/support-agent-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-runbook.md) |
| Support-agent persona coverage and support cards | [docs/support-agent-coverage-ledger.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-coverage-ledger.md) | [customgpt/support-cards/windows-startup-config.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/windows-startup-config.md), [customgpt/support-cards/linux-service-startup.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/linux-service-startup.md), [customgpt/support-cards/path-reliability.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/path-reliability.md), [customgpt/support-cards/ambiguous-short-prompt.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/ambiguous-short-prompt.md), [customgpt/support-cards/truncated-retrieval.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/support-cards/truncated-retrieval.md) |
| Support-agent deployment, action setup, and smoke checks | [docs/support-agent-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-runbook.md) | [scripts/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/scripts/README.md), [docs/support-agent-quality-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-quality-contract.md), [docs/support-agent-evals.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-evals.md) |
| Code quality | [docs/code-quality.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-quality.md) | [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) |
| Domain contract | [docs/domain-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/domain-contract.md) | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md) |
| Validation commands | [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) | [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md) |
| Review and closeout | [docs/review-checklist.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/review-checklist.md) | [docs/templates/non-trivial-change-template.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/templates/non-trivial-change-template.md) |
| ADR and TSR handling | [docs/decision-memory.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-memory.md) | [docs/decision-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-log.md), [docs/decisions/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/README.md), [docs/troubleshooting-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting-log.md) |
| Accepted design decisions | [docs/decision-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-log.md) | [docs/decisions/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/README.md), `docs/decisions/` |
| Troubleshooting records | [docs/troubleshooting-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting-log.md) | `docs/troubleshooting/` |
| Symptom-based troubleshooting | [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md) | [docs/troubleshooting-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting-log.md), package READMEs |

## Routing Rules

- For operator behavior, route to [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), [docs/OPERATOR_GUIDE.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/OPERATOR_GUIDE.md), or a
  package README before using code.
- For developer behavior, route to [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) and the workflow docs before
  implementation advice.
- For independent-agent, subagent, or delegation questions, route to the
  Subagent Use rules in
  [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md)
  and [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md);
  emphasize that the repo owner has made a standing request for default
  subagent use when supported, active tool/session policy permits it, and the
  user has not explicitly prohibited it. Do not report
  `not authorized/not requested` merely because the current task prompt omits a
  repeated subagent request. `scope-ledger-adversarial-review`,
  `go-code-quality-review`, and fresh-verifier explorers are read-only findings
  sources, high-risk SELF-AUDIT rows should cite independent evidence when
  available, and the lead Codex agent owns approval gates, integration, final
  SELF-AUDIT disposition, validation claims, traceability, and closeout.
- For recurring model or workflow lessons, route to [docs/agent-lessons/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/agent-lessons/README.md),
  then verify any implementation claim against workflow docs, source, tests,
  and ADR/TSR records.
- For broad refactor or Scope Ledger questions, route to the slice-shaped
  Scope Ledger rule and reject approval advice unless the work is split into
  independently coded, tested, and reviewed slices.
- For dependency-visualization questions, route to [docs/code-maps/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-maps/README.md)
  and current source. The support agent cannot run local `goda`, Graphviz,
  `gopls`, or `callgraph`; it can only use retrievable Markdown, source, tests,
  and routing docs.
- For config-sensitive answers, route to [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) and tell the
  user to check their effective YAML.
- Use YAML headers, key comments, and field guides as local context for purpose,
  ownership, runtime behavior, units, side effects, and safe-edit boundaries,
  but do not treat comments as schema or defaults.
- For PowerShell script questions, use [scripts/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/scripts/README.md)
  and the script's comment-based help for purpose, prerequisites, side effects,
  and safety boundaries; inspect the script body before claiming exact behavior.
- For support-agent quality, routing, deployment, or evaluation questions, route
  to [docs/support-agent-quality-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-quality-contract.md),
  [docs/support-agent-coverage-ledger.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-coverage-ledger.md),
  [docs/support-agent-evals.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-evals.md), and
  [docs/support-agent-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-runbook.md). Do not try to retrieve
  `customgpt/support-agent/*` through the action; those deployment files are
  intentionally blocked by the Worker.
- For YAML tuning questions, check the ownership class first. Do not recommend
  changing algorithm calibration as a first troubleshooting step.
- For logging questions, distinguish system logs, propagation logs, optional
  dropped-call logs, and file-only event logs. Propagation/path aggregates live
  in `logging.propagation.dir`; event logs for login attempts, reputation
  drops, telnet lifecycle, ingest lifecycle, and peer lifecycle do not add
  console or UI output. Runtime file logs use stable active names such as
  `system.log` and UTC date-only archives such as `07-Jun-2026.log`.
- For symptom reports, route to [customgpt/troubleshooting-index.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/customgpt/troubleshooting-index.md) before
  using broad docs or historical TSRs.
- For decision history, route to the ADR/TSR indexes before summarizing.
