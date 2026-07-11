# Codex Skills Bundle

This directory is the authoritative repo-managed Codex skill source. Load a
skill only when its positive trigger applies; Non-trivial work alone is not a
trigger. Retained skills preserve their unique engineering methods even when
their orchestration or reporting is simplified.

| Skill | Positive trigger |
| --- | --- |
| `decision-memory-audit` | Durable decision, troubleshooting record, or ADR/TSR index may change |
| `workflow-contract-audit` | Codex workflow authority, routing, skills, or enforcement changes |
| `requirements-ambiguity-review` | Current evidence leaves material semantics genuinely multi-valued |
| `scientific-model-oracle` | Scientific/model semantics, boundaries, classifications, or claims change |
| `design-challenger` | A genuine consequential design fork remains |
| `scope-ledger-adversarial-review` | Scope is High-risk, uncertain, disputed, difficult to reverse, or materially incomplete |
| `test-strategy-adversary` | A concrete false-green or unclear-oracle risk remains |
| `go-code-quality-review` | Go implementation is High-risk or substantial |
| `go-code-walk` | Unfamiliar or cross-package Go behavior needs an execution-path walk |
| `go-blast-radius-audit` | Shared, semantic, cross-package, test, config, docs, or support impact is uncertain |
| `go-config-contract-audit` | YAML/loaders/defaults/schema/operator config semantics change |
| `go-connection-lifecycle-audit` | Long-lived connection liveness, recovery, or shutdown changes |
| `go-leak-detection` | Goroutine, timer, socket, handle, retained-heap, or lifecycle leak risk changes |
| `go-retained-state-audit` | Server-lifetime state, bounds, eviction, or secondary indexes change |
| `go-hotpath-design` | Allocation-sensitive parsing, fan-out, queues, or optimization design changes |
| `pprof-impact-review` | Multiple local profile bundles need comparable impact analysis |
| `explain-code` | Existing code behavior needs grounded explanation without changes |
| `initial-review` | A concise code-understanding review is explicitly requested |
| `gh-address-comments` | GitHub review comments need inspection or approved fixes |
| `gh-fix-ci` | GitHub Actions failures need diagnosis or an approved fix |
| `security-best-practices` | An explicit supported-language security review is requested |
| `security-threat-model` | An explicit repository threat model is requested |
| `sentry` | Configured Sentry issues or events need read-only inspection |

Generic skill relocation, deletion, and consolidation are outside the current
workflow change.

Verify the bundle with:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```
