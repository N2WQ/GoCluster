# TSR-0010 - Stabilizer Delaying V Glyph Spots Against Intended Delay Eligibility

Status: Resolved
Date Opened: 2026-03-01
Date Resolved: 2026-03-01
Owner: Cluster maintainers
Technical Area: internal/correctionflow stabilizer policy, main output pipeline, replay parity, operator docs
Trigger Source: Chat request
Led To ADR(s): ADR-0046
Tags: stabilizer, confidence-glyphs, main-replay-parity

## Triggering Request
- Request date: 2026-03-01
- Request summary: verify whether `V` spots were being delayed after seeing high average `V` stabilizer turns in runtime stats; enforce intended behavior where only `?`, `S`, and `P` are delay-eligible.
- Request reference (chat/issue/link): repository chat thread.

## Symptoms and Impact
- What failed or looked wrong?
  - Runtime stats showed substantial delayed-turn counts for glyph `V`, which was inconsistent with intended stabilizer semantics.
- User/operator impact:
  - Created operator confusion about whether high-confidence spots were being unnecessarily delayed.
  - Increased risk of avoidable telnet latency and queue occupancy.
- Scope and affected components:
  - Shared stabilizer policy helper (`internal/correctionflow/stabilizer.go`) used by both main and replay.
  - Main stabilizer release/retry path and replay stabilizer-delay proxy interpretation.
  - Operator-facing documentation for stabilizer semantics.

## Timeline
1. 2026-03-01 - Operator reported `V` delay-turn observations and requested deep verification.
2. 2026-03-01 - Code inspection confirmed shared stabilizer policy could delay `V` for ambiguity/edit-neighbor/non-recent branches.
3. 2026-03-01 - Implemented glyph eligibility guard (`?`/`S`/`P`), updated tests and docs, and recorded decision in ADR-0046.

## Hypotheses and Tests
1. Hypothesis A - `V` delays were stats-only artifacts and policy never delays `V`.
   - Evidence/commands: inspected `internal/correctionflow/stabilizer.go` and `main.go` wrappers/retry path.
   - Outcome: Rejected.
2. Hypothesis B - shared stabilizer policy lacked explicit glyph eligibility guard, allowing `V` delays.
   - Evidence/commands: traced `EvaluateStabilizerDelay(...)` branches; added tests in `internal/correctionflow/stabilizer_test.go` and `main_test.go`.
   - Outcome: Supported.
3. Hypothesis C - scoping delay eligibility to `?`/`S`/`P` in shared helper preserves main/replay parity with minimal risk.
   - Evidence/commands: policy implemented once in `internal/correctionflow`; verified via targeted tests.
   - Outcome: Supported.

## Findings
- Root cause (or best current explanation):
  - Shared stabilizer decision logic did not gate delay eligibility by confidence glyph before applying ambiguity/edit-neighbor/non-recent rails.
- Contributing factors:
  - Evolving policy additions (ambiguity and contested-neighbor rails) were applied broadly without an explicit allowlist.
- Why this did or did not require a durable decision:
  - Required a durable decision because it changes shared runtime/replay stabilizer semantics and operator-facing behavior.

## Decision Linkage
- ADR created/updated:
  - `docs/decisions/ADR-0046-stabilizer-delay-eligibility-unknown-s-p-only.md`
- Decision delta summary:
  - Stabilizer delay eligibility is now glyph-scoped to `?`, `S`, and `P`.
  - `V` and `C` always pass through stabilizer delay and retry checks.
- Contract/behavior changes (or `No contract changes`):
  - User-visible telnet behavior changed for `V`/`C`: no stabilizer delay.
  - Main/replay policy parity preserved because the shared helper changed centrally.

## Verification and Monitoring
- Validation steps run:
  - `go test ./internal/correctionflow -run Stabilizer -count=1`
  - `go test . -run TestEvaluateTelnetStabilizerDelay -count=1`
- Signals to monitor (metrics/logs):
  - Stabilizer glyph-turn metrics for `V` should trend toward immediate/no-delay behavior.
  - Stabilizer held/delayed reason counters should shift away from `V`-origin cases.
  - Queue pressure (`held`, `overflow-release`) should not regress.
- Rollback triggers:
  - Material correction-quality regression attributable to removing `V` delays.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0029-stabilizer-targeted-ambiguity-and-lowp-delay.md`
  - `docs/decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md`
  - `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md`
  - `docs/decisions/ADR-0046-stabilizer-delay-eligibility-unknown-s-p-only.md`
- Related docs:
  - `docs/OPERATOR_GUIDE.md`
  - `README.md`
  - `data/config/pipeline.yaml`
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`
