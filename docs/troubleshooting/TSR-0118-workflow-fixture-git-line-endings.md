# TSR-0118 - Workflow Fixture Git Line Endings

Status: Monitoring
Date Opened: 2026-11-10
Date Resolved: n/a
Owner: Codex / repository owner
Technical Area: workflow-contract test fixture, Windows PowerShell, Git
Trigger Source: Chat request
Led To ADR(s): none
Tags: workflow, fixture, Git, line endings, Windows

## RCA Summary

- What happened: the local workflow-contract fixture suite stopped before its final protected-path checks when run with Windows PowerShell.
- Why: the temporary Git repository inherited system `core.autocrlf=true`. Git warned about the LF fixture while the checker inspected the worktree; the parent PowerShell process treated child stderr as a terminating native-command error.
- What fixed it: the fixture configures its private temporary Git repository with `core.autocrlf=false` and checks Git setup failures. It retains the existing positive and negative protected-path checks.
- How we know: the original run failed with the Git warning. A process-only `core.autocrlf=false` retry passed the complete fixture suite before the repair. Afterward, the suite passed under both Windows PowerShell 5.1 and PowerShell 7 with ordinary system true and isolated global false settings; all three protected-path checks ran.
- Operator/support answer: this affects development workflow validation, not cluster operation. It changes neither the user's Git configuration nor Fable authority.

## Triggering Request

- Request date: 2026-11-10.
- Request summary: diagnose and fix the observed CI test and local workflow-fixture failures.
- Request reference: owner chat, exact `Approved v1` for the bounded follow-on repair.

## Symptoms and Impact

Windows PowerShell 5.1 stopped `scripts/test-workflow-contract.ps1` with a `NativeCommandError` on Git's LF-to-CRLF warning. The original workflow fixture predated the reviewed workflow and CI-repair commits. The checker itself had passed, but the interrupted suite could not be reported as passed.

## Timeline

1. 2026-11-10 - The ordinary fixture run failed at the temporary-repository relocation checks under the machine's `core.autocrlf=true` setting.
2. 2026-11-10 - A process-only false setting allowed the complete existing suite to pass; the owner subsequently approved a fixture-local correction.
3. 2026-11-10 - Both PowerShell engines passed under the system true setting and an isolated global false setting. A temporary failing Git executable caused initialization to fail explicitly rather than allowing the suite to report success.

## Hypotheses and Tests

1. Hypothesis: a protected Fable path really changed in the user's checkout.
   - Evidence: the warning named the synthetic `.claude/agents/relocation-fixture.md` in the temporary repository; the real checkout was clean.
   - Outcome: Rejected for the observed failure.
2. Hypothesis: inherited Git line-ending conversion caused an otherwise harmless warning to terminate the PowerShell fixture.
   - Evidence: the machine's system Git config set `core.autocrlf=true`; an otherwise identical process-only false retry passed.
   - Outcome: Supported for the observed invocation.

## Findings

- Root cause: a Git line-ending warning crossed the fixture's native-process stderr boundary into terminating Windows PowerShell error handling.
- The fixture owns its temporary repository and may select its line-ending behavior without changing the user's configuration.
- A Git command-level override has higher precedence than repository-local config and is outside this repair's claim.
- Do not suppress native errors or remove the three invariant-specific relocation checks to obtain a green result.

## Decision Linkage

- ADR created/updated: none.
- Decision delta summary: no durable architecture or workflow-policy decision.
- Contract/behavior changes: fixture setup only; protected-path and checker semantics remain unchanged.

## Verification and Monitoring

- Validation steps: four complete fixture runs passed: Windows PowerShell 5.1 and PowerShell 7, each using inherited system `core.autocrlf=true` and isolated temporary global `core.autocrlf=false`. Each reported the three expected protected-path outcomes and the final pass. In a separate Windows PowerShell run, a temporary `git.cmd` exiting 42 caused the explicit initialization failure and no final pass; other setup error paths were source-reviewed, not individually fault-injected. The PowerShell parser passed. Git and PowerShell output logs are retained in the user's temporary directory as `gocluster-fixture-*.log`.
- Signals to monitor: workflow fixture failures under supported PowerShell and ordinary Git system/user settings.
- Rollback trigger: the fixture no longer detects forbidden protected-path content or masks Git setup failures.

## References

- Related docs: [decision memory](../decision-memory.md), [workflow checks](../runbooks/codex-workflow-checks.md), and [CI repair evidence](TSR-0117-ci-fixtures-and-shadow-run-identity.md).
- Support-agent documentation impact: not required; this is an internal test-fixture correction with no operator-facing behavior change.
