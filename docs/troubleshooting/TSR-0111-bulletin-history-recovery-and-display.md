# TSR-0111 - Bulletin History Recovery and Display

Status: Monitoring
Date Opened: 2026-10-28
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: bulletin history and telnet login
Trigger Source: Operator review
Led To ADR(s): ADR-0324
Tags: WWV, WCY, snapshot recovery, terminal controls, greeting

## RCA Summary
- What happened: Review of commit `8f4d82e` identified that a failed snapshot load could be followed by replacement of the original, archived control characters could reach clients, and bulletin login output followed the shipped prompt.
- Why: The initial write was signaled even after load failure; display text retained controls; startup messages were sent after the greeting as a whole.
- What fixed it: Preserve a verified recovery copy before replacement, sanitize archived display, and provide an explicit startup insertion point in the shipped greeting.
- How we know: The source paths and recovery, output, and transcript regression tests cover these boundaries. A test does not prove every power-loss behavior of a filesystem.
- Operator/support answer: Keep recovery files for investigation, correct the storage problem before relying on persistence, and use the active greeting's insertion marker for welcome-first ordering.

## Triggering Request
- Request date: 2026-10-28
- Request summary: Address the review findings before release.
- Request reference (chat/issue/link): Review of commit `8f4d82e24e05d43a6208acdbd930f55570987368`.

## Symptoms and Impact
- What failed or looked wrong? A potentially recoverable snapshot could be destroyed; terminal controls could replay; the prompt was not the end of startup output.
- User/operator impact: Lost history, client-dependent terminal effects, and confusing login ordering.
- Scope and affected components: Dedicated bulletin snapshot worker and archived display, telnet startup, and active greeting configuration.

## Timeline
1. 2026-10-28 - Review identified the three failure mechanisms in the committed implementation.
2. 2026-10-28 - The owner selected recovery-copy, archive-only sanitization, and greeting-order behavior.
3. 2026-10-28 - Approved v6 authorized the corrective scope.

## Hypotheses and Tests
1. Failed load still schedules a write.
   - Evidence/commands: Inspect the prior `bulletinhistory.Open`, worker, and snapshot replacement paths.
   - Outcome: Supported.
2. Controls can survive into archived output.
   - Evidence/commands: Inspect the feed, archive record, and login/SHOW rendering paths; add archived wire regression tests.
   - Outcome: Supported by source inspection; client effects depend on the terminal.
3. The shipped prompt precedes history.
   - Evidence/commands: Inspect `data/config/runtime.yaml` and post-login sends; exercise the shipped greeting transcript.
   - Outcome: Supported by source inspection.

## Findings
- Root cause (or best current explanation): Storage failure and display boundaries were not enforced at snapshot replacement and archive output, while startup output was queued after the prompt-bearing greeting.
- Contributing factors: A warning-only storage test and a shortened login test greeting did not detect the failures.
- Why this did or did not require a durable decision: Recovery, compatibility, and operator behavior are recorded in ADR-0324.

## Decision Linkage
- ADR created/updated: ADR-0324, refining ADR-0323.
- Decision delta summary: Preserve recoverable bytes before replacement, make archived display safe, and position startup output explicitly.
- Contract/behavior changes: Live bulletin text and filtering are unchanged; the greeting marker changes deployment and rollback guidance.

## Verification and Monitoring
- Validation steps run: See the change closeout for the executed test and validation results.
- Signals to monitor (metrics/logs): Bulletin storage errors, recovery-copy paths, and history gap warnings.
- Rollback triggers: Preserve the snapshot and recovery files before rollback; follow the greeting marker and older snapshot compatibility guidance.

## References
- Commit(s): `8f4d82e24e05d43a6208acdbd930f55570987368`.
- Related ADR(s): ADR-0323, ADR-0324.
- Related docs: [bulletin history](../../telnet/README.md#wwv-and-wcy-bulletin-history), [configuration](../../data/config/README.md).
