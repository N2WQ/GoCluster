# ADR-0204: Captured Validation Evidence For High-Risk Claims

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0194 requires material progress, implementation, validation, performance,
and science/model claims to be grounded in current-session evidence. ADR-0203
then moved high-risk SELF-AUDIT evidence toward independent reviewers when
supported.

The remaining gap was proof quality for the highest-risk command-backed claims.
A response could still report `go test -race ./... - pass` or a
`Leak-detection evidence: PASS` row without showing the actual current-session
output that made the claim checkable. That leaves too much room for stale,
cached, skipped, timed-out, partial, or self-assessed validation to look like a
fresh PASS.

## Decision

Require captured evidence for command-backed high-risk validation claims:

1. Command-backed concurrency, lifecycle, queue, timer, shutdown,
   shared-state, or leak-detection validation claims must include a short
   captured transcript excerpt.
2. The canonical location is `docs/review-checklist.md` `Verification command
   reporting`, surfaced in the `REVIEW` marker. `SELF-AUDIT` and `CLOSEOUT`
   reference that evidence instead of repasting it.
3. The final `VALIDATION` marker remains the exact three-line block required by
   `VALIDATION.md`; do not add transcript excerpts inside that block.
4. The excerpt must identify the command or evidence source, target scope,
   result status, key pass/fail/profile/trace/runtime line, and whether the
   result was incremental or final.
5. Static reasoning remains allowed when it is the evidence level reached, but
   it must be labeled as static reasoning and name the inspected files. Static
   reasoning cannot be presented as local test, race, profile, trace, or
   runtime confirmation.
6. Missing, failed, timed-out, stale, skipped, cached-without-usable-output,
   partial, or waived excerpts are validation status, gaps, or waivers. They
   cannot be silently converted into `PASS`.
7. Captured excerpts must be minimal and must not include full logs, secrets,
   tokens, credentials, environment dumps, private hostnames, unnecessary user
   data, or large runtime traces. Redactions must be stated.

## Alternatives considered

1. Keep bare command PASS/FAIL summaries.
   - Rejected because they do not prove the command was current, scoped,
     non-skipped, non-stale, and relevant to the claim.
2. Require full transcript capture for all validation commands.
   - Rejected because it would create noisy closeouts and increase accidental
     disclosure risk. The requirement is limited to high-risk command-backed
     concurrency and leak-detection claims.
3. Put excerpts in `SELF-AUDIT`, `CLOSEOUT`, or the final `VALIDATION` block.
   - Rejected because duplicated excerpts make closeout noisy and the final
     `VALIDATION` block must remain exactly three lines.
4. Require command evidence for static-only leak audits.
   - Rejected because the existing leak-detection workflow has legitimate
     static-reasoning evidence levels. The requirement applies when the claim
     is command-backed.

## Consequences

### Benefits

- High-risk validation claims become directly auditable from current-session
  output.
- SELF-AUDIT rows for concurrency and leak-detection can cite concrete
  evidence instead of a lead-agent self-grade.
- The workflow avoids duplicate evidence by keeping one canonical excerpt
  location.
- Safety limits reduce the chance of leaking credentials or irrelevant runtime
  data.

### Risks

- Closeouts for high-risk Go work require slightly more evidence capture.
- Agents must be careful not to paste excessive output or sensitive material.
- A cached or partial command can still appear in output; the rule requires
  reporting that status rather than treating it as a clean PASS.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Future workflow closeouts for command-backed concurrency or leak-detection
  claims must include captured excerpts in `REVIEW` verification command
  evidence and reference them from `SELF-AUDIT` and `CLOSEOUT`.

## Links

- Related issues/PRs/commits:
- Related tests:
  - `scripts/verify-codex-skills.ps1`
  - targeted workflow text checks
  - targeted support-agent routing text checks
  - `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/review-checklist.md`, `docs/dev-runbook.md`, `VALIDATION.md`,
  `docs/templates/non-trivial-change-template.md`,
  `docs/WORKING_WITH_CODEX.md`,
  `codex-skills/go-code-quality-review/SKILL.md`,
  `codex-skills/go-leak-detection/SKILL.md`,
  `customgpt/source-map.md`, `customgpt/developer-guide-index.md`,
  `customgpt/common-questions.md`
- Related ADRs: ADR-0194, ADR-0199, ADR-0202, ADR-0203
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0194 claim-evidence requirements
  and ADR-0203 SELF-AUDIT evidence requirements
