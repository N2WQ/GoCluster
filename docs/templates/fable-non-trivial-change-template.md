# docs/templates/fable-non-trivial-change-template.md

Use this exact evidence shape for Fable Non-trivial tasks unless the user
explicitly requests a different reporting shape. It is the Fable-native
counterpart to `docs/templates/non-trivial-change-template.md` (Codex) — the
same marker names and 15-row SELF-AUDIT taxonomy, so a human operator
running both contracts in this repository can read either output the same
way. What differs is *where* each marker's evidence lives: Phase A lives
inside the Plan Mode plan file, not a chat block; Phase B is the closing
response after `ExitPlanMode` approval and implementation.

Token efficiency changes reporting shape only. Later markers may reference
earlier evidence by name instead of restating unchanged facts. Omit
untriggered optional details instead of filling them with placeholder text.

## Phase A: The Plan Mode Plan

Write this content into the plan before calling `ExitPlanMode`. Do not
request approval until every required piece below is present.

### GATE

- Classification: Non-trivial
- Plan status: not yet approved
- Independent-agent status: supported, authorized, and not prohibited |
  unsupported | `not authorized/not requested` | explicitly prohibited |
  failed/timed out - <details>
- Pre-approval independent agent: `fable-scope-adversary` used - <read-only
  confirmation, no edits/diffs/formatters/full validation> | unsupported/
  `not authorized/not requested`/failed/timed out - <evidence status>

### DISCOVERY

- entrypoints/surfaces:
- caller/callee flow:
- persisted/config/archive/schema:
- user-visible/help/docs:
- existing tests:
- decision-memory pre-read: <ADR/TSR refs inspected | No relevant ADR found;
  No relevant TSR found>
- independent-agent evidence: <none | role/purpose/findings used as evidence
  | unsupported/`not authorized/not requested`/prohibited/failed/timed out -
  status>
- unknowns:

### SCOPE

- Objective:
- In scope:
  - [Agreed|Pending|Rejected|Deferred] item
- Out of scope:
- Slice plan:
  - slice:
  - objective:
  - bounded files/packages/docs:
  - blast-radius boundary:
  - production-safe stopping point:
  - targeted checks before next slice:
- Risks requiring attention:
- Reasoning budget: `<low|high|xhigh|medium>` (lowest sufficient). Rationale:
  <one sentence>; escalation trigger: <one phrase or "none expected">.

### SCOPE ADVERSARIAL REVIEW

- question: What edge case would make this scope unsafe or incomplete?
- `fable-scope-adversary`: used - <findings and lead disposition> |
  unsupported/`not authorized/not requested`/prohibited/failed/timed out -
  <evidence status and lead disposition>
- applicable edge areas:
- gaps found: none | <items>
- disposition: nothing material found | revise plan

If material gaps are found, do not call `ExitPlanMode` for the current plan.
Revise the plan and repeat this review.

Call `ExitPlanMode` only once disposition is exactly `nothing material
found`. No code, diffs, file writes, formatters, or full validation commands
before that approval.

---

## Phase B: Closing Response

Written after implementation, once the approved plan's slices are complete.

### GATE

- Classification: Non-trivial
- Plan status: approved via `ExitPlanMode`
- Independent-agent status: supported, authorized, and not prohibited |
  unsupported | `not authorized/not requested` | explicitly prohibited |
  failed/timed out - <details>
- Independent-agent phase/use: none | pre-approval explorer | post-approval
  worker | `fable-code-reviewer` | `fable-fresh-verifier` - <lead-owned
  disposition>

### PREFLIGHT

- Git preflight: branch=<name>; worktree=<clean|dirty acknowledged>;
  rollback=<hash/tag/branch>
- Dirty files not owned by this task (including another executor's
  in-progress work):

### DESIGN

- current flow:
- implementation plan:
- independent-agent plan: none | <worker/`fable-code-reviewer`/`fable-
  fresh-verifier` roles; approved plan version; slice name/objective; base
  revision; allowed paths; forbidden paths; production-safe stopping point;
  targeted checks; expected output; stop conditions; lead verification>
- contracts: changed | unchanged
- user-visible behavior: changed | unchanged
- dependency rigor: Light | Full
- dependency scan evidence: <required for Full rigor>
- triggered skills: <`.claude/skills/*` used | none>
- README impact: Required | Not required - <one sentence>
- Support-agent docs impact: Required | Not required - <one sentence>
- ADR/TSR pre-read: <relevant refs | No relevant ADR found; No relevant TSR
  found>
- validation lane: documentation-only Markdown | workflow-contract |
  code/mixed/runtime-contract | other - <reason>
- checker plan:

### IMPLEMENTATION

For each slice:

- slice:
- objective:
- files:
- blast-radius boundary:
- subagent use: none | <worker role; approved plan version; slice; base
  revision; allowed paths; forbidden paths; production-safe stopping point;
  targeted checks; expected output; stop conditions>
- checks:
- result:
- next-slice gate: passed | blocked - reason
- remaining risk:

### REVIEW

- findings by severity:
- confirmed fixes:
- rerun checks:
- `fable-code-reviewer`: used - <findings and lead disposition> | N/A - no
  Go implementation | unsupported/`not authorized/not requested`/
  prohibited/failed/timed out - <evidence status and lead disposition>
- fresh verifier pass: <`fable-fresh-verifier` | fresh self-verification |
  N/A - reason>

If no material findings: `Review Pass findings: none material`.

### SELF-AUDIT

Use the canonical SA1-SA15 mapping in `docs/fable-review-checklist.md`.

- Applicability manifest:
  - applicable: <SA IDs>
  - not applicable: <SA IDs> - <shared reason; repeat for different reasons>
- Results:
  - <applicable SA ID>: PASS|FAIL - <evidence note or earlier-marker
    reference>

Classify every SA1-SA15 ID exactly once. The manifest replaces individual
`N/A` rows; missing, unknown, or duplicate IDs fail the Self-Audit.
Applicable IDs require results. Required independent evidence that is
unsupported, `not authorized/not requested`, prohibited, failed, timed out,
missing, or stale is a `FAIL` or explicit gap/waiver, not an omission. SA7
and SA8 reference `REVIEW` verification command evidence when command-backed.
The lead owns every final disposition.

### CLOSEOUT

- summary:
- tradeoffs:
- risks and mitigations:
- contracts and compatibility:
- user impact:
- README impact:
- Support-agent docs impact:
- fresh verifier outcome:
- independent-agent use and lead-owned disposition:
- claim evidence:
- verification commands and results:
- validation lane:
- ADR handling outcome:
- Decision refs:

### TRACEABILITY

For every plan item that was `Agreed` or `Pending` at the start of
implementation:

- item:
- locations:
- tests/checks:
- docs/comments:
- support-agent docs:
- independent-agent outputs:
- decision refs:

### VALIDATION

Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)
