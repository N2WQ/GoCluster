# Codex Skills Bundle

This directory is the authoritative repo-managed Codex skill source. Load a
skill only when its positive trigger applies; Non-trivial work alone is not a
trigger. Retained skills preserve their unique engineering methods even when
their orchestration or reporting is simplified.

| Skill | Positive trigger |
| --- | --- |
| `workflow-contract-audit` | Codex workflow authority, routing, skills, or enforcement changes |
| `requirements-ambiguity-review` | Current evidence leaves material semantics genuinely multi-valued |
| `scientific-model-oracle` | Scientific/model semantics, boundaries, classifications, or claims change |
| `design-challenger` | A genuine consequential design fork remains |
| `test-strategy-adversary` | A concrete false-green or unclear-oracle risk remains |
| `go-code-quality-review` | Go implementation is High-risk or substantial; routes to the canonical checklist method |
| `go-code-walk` | Unfamiliar or cross-package Go behavior needs an execution-path walk |
| `go-blast-radius-audit` | Shared, semantic, cross-package, test, config, docs, or support impact is uncertain |
| `go-config-contract-audit` | YAML/loaders/defaults/schema/operator config semantics change |
| `go-connection-lifecycle-audit` | Long-lived connection liveness, recovery, or shutdown changes |
| `go-leak-detection` | Goroutine, timer, socket, handle, retained-heap, or lifecycle leak risk changes |
| `go-retained-state-audit` | Server-lifetime state, bounds, eviction, or secondary indexes change |
| `go-hotpath-design` | Allocation-sensitive parsing, fan-out, queues, or optimization design changes |
| `pprof-impact-review` | Multiple local profile bundles need comparable impact analysis |
| `explain-code` | Existing code needs grounded orientation, explanation, review or diagnosis at the requested depth |
| `gh-address-comments` | GitHub review comments need inspection or approved fixes |
| `gh-fix-ci` | GitHub Actions failures need diagnosis or an approved fix |
| `security-best-practices` | An explicit supported-language security review is requested |
| `security-threat-model` | An explicit repository threat model is requested |
| `sentry` | Configured Sentry issues or events need read-only inspection |

Codex discovers these real directories under `.agents/skills` from the
checkout. No symlink privilege or user-level copy is required. A metadata
verifier pass alone does not prove discovery: reload skills or start a new
session and inspect the live skill inventory for this repository. Same-named
user-level skills remain outside the project authority defined by ADR-0155.

Codex reaches consolidated methods through the existing `AGENTS.md` routes:

- [decision discovery and record handling](../../docs/decision-memory.md#codex-discovery-and-record-handling)
  during discovery and closeout via `docs/change-workflow.md`;
- [scope challenge](../../docs/change-workflow.md#scope-challenge-method)
  before approval when the deeper risk trigger applies; and
- [substantial Go review](../../docs/review-checklist.md#substantial-go-review-method)
  through the retained `go-code-quality-review` entry point and workflow route.

These methods remain required when triggered even though they do not each need
a separate skill identity. A static check does not establish live Codex routing
or execution quality; report actual discovery separately from source review.

Verify the bundle with:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```
