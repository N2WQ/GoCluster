# Codex Skills Bundle

This directory is the authoritative gocluster Codex skills bundle. A fresh
checkout should use these skill files directly instead of copying them into a
user-level Codex skills directory.

Repo-authoritative skills:
- `design-challenger` - independently compares viable designs from a neutral,
  semantically resolved evidence packet before the Scope Ledger
- `decision-memory-audit` - ADR/TSR pre-read, record choice, indexes, decision
  refs, and Scope-to-Code Traceability for Non-trivial work
- `explain-code` - grounded explanation of existing code without changes
- `gh-address-comments` - inspect and address GitHub PR review comments through
  `gh` CLI when requested
- `gh-fix-ci`
- `go-blast-radius-audit` - maps blast radius, including optional
  `goda`/Graphviz dependency visualization and support-agent-readable code maps
  when useful
- `go-code-quality-review` - independently reviews newly written Go
  implementation diffs and applicable SELF-AUDIT evidence against scope,
  code-quality, validation, and operational standards before closeout
- `go-code-walk` - walks unfamiliar code paths with source, semantic tools, and
  optional dependency visualization
- `go-connection-lifecycle-audit` - audits reconnect, retry/backoff,
  keepalive, silent-stall, liveness, shutdown, and operator diagnostics for
  long-lived Go connection paths
- `go-config-contract-audit`
- `go-hotpath-design`
- `go-leak-detection`
- `go-retained-state-audit`
- `initial-review` - concise code-understanding review without implementation
- `pprof-impact-review`
- `requirements-ambiguity-review` - independently searches for unresolved
  product/operator semantics before scope hardens
- `security-best-practices` - explicit security best-practice review support
- `security-threat-model` - repository-grounded threat modeling support
- `scientific-model-oracle` - establishes normative model contracts, golden
  vectors, uncertainty, and supportable scientific claims before design
- `sentry`
- `scope-ledger-adversarial-review` - independently challenges Non-trivial
  Scope Ledgers before approval
- `test-strategy-adversary` - independently checks whether the planned evidence
  can falsify a broken design before implementation
- `workflow-contract-audit` - checks Codex workflow, validation, runbook,
  template, skill, and workflow-script edits for contract drift

Intentionally not vendored as project skills:
- Codex runtime/system skills; those are provided by the Codex environment.
- `openai-docs`; current OpenAI product guidance should come from live official
  docs tooling.
- `pdf` and `screenshot`; those are task-specific utility skills, not part of
  the gocluster workflow contract.
- Plugin-only GitHub skills such as `github` and `yeet`; connector tools and
  authentication remain external even when repo skills use `gh` CLI fallbacks.

Verify the repo bundle:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```

To target a subset:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1 -Skills gh-fix-ci
```
