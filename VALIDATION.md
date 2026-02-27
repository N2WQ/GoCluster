# VALIDATION.md - Non-trivial Task Compliance Rubric

Use this scorecard after any non-trivial Codex task to check whether Codex actually followed `AGENTS.md`.

## How to use
- Score each item `0` or `1`.
- Total the score out of `6`.
- Apply the automatic fail rules even if the numeric score looks acceptable.

## Scorecard

### 1) Scope gate
Score `1` if:
- Codex showed `Proposed Scope Ledger vN` or explicitly referenced the current approved ledger, and
- Codex did not start code, diffs, file edits, or full validation before you replied `Approved vN`.

Score `0` if:
- Codex assumed approval,
- skipped the ledger,
- or started implementation before `Approved vN`.

### 2) Skill discipline
Score `1` if:
- Codex explicitly said which skill it used, or
- Codex explicitly stated `Skill check: none applicable`.

Score `0` if:
- no skill check was shown, or
- an obviously relevant installed skill was skipped.

### 3) Dependency rigor
Score `1` if:
- Codex stated `Light` or `Full` dependency rigor, and
- listed touched files/packages,
- upstream callers or sources reviewed,
- downstream consumers reviewed,
- and any shared components or interfaces reviewed.

Score `0` if:
- Codex coded without a concrete dependency sweep.

### 4) Pre-code design discipline
Score `1` if, before coding, Codex stated:
- contract changes or `No contract changes`,
- user-visible changes or `No user-visible behavior changes`, and
- a short architecture note.

Score `0` if:
- Codex jumped into code without that pre-code framing.

### 5) Verification discipline
Score `1` if:
- Codex updated relevant tests, and
- showed the required verification commands,
- including race and static checks when applicable.

Score `0` if:
- tests or checks were missing,
- vague,
- or only implied.

### 6) Documentation and traceability
Score `1` if:
- Codex explicitly stated `README impact: Required` or `README impact: Not required`,
- updated docs when needed, and
- mapped scope items to code, tests, and docs in the summary.

Score `0` if:
- README or doc review was omitted, or
- traceability was missing.

## Scoring interpretation
- `6/6` = Fully followed the file
- `5/6` = Strong compliance, minor gap
- `4/6` = Partial compliance, review carefully
- `3/6 or below` = Did not reliably follow the file

## Automatic fail conditions
Mark the task non-compliant regardless of numeric score if any of the following happened:
- Codex implemented before `Approved vN`
- Codex skipped repo-wide or shared-component dependency review for a change that clearly required it
- Codex omitted README review status on a non-trivial task

## Quick-use checklist
- Scope approved
- Skill checked
- Dependencies scanned
- Pre-code design stated
- Tests and checks run
- README, docs, and traceability done

## Copy-paste review template

```text
Non-trivial task validation

1) Scope gate: 0/1
2) Skill discipline: 0/1
3) Dependency rigor: 0/1
4) Pre-code design discipline: 0/1
5) Verification discipline: 0/1
6) Documentation and traceability: 0/1

Total: __ / 6

Automatic fail triggered: Yes / No
Reason:

Notes:
- 
- 
- 
```
