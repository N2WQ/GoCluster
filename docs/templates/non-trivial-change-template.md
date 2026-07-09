# docs/templates/non-trivial-change-template.md

Use this exact Codex evidence-ledger shape for Non-trivial tasks unless the
user explicitly requests a different reporting shape.

The ledger is strict but compact. Token efficiency changes reporting shape only;
it does not reduce required discovery, approval, dependency rigor, validation,
review, ADR handling, or traceability.

If a required marker cannot be completed from inspected workspace evidence,
Codex must stop and report the missing evidence instead of continuing. Omit
untriggered optional details instead of filling them with placeholder text.

Later markers may reference earlier evidence by marker name instead of
restating unchanged facts.

## Phase A: Approval Packet

### GATE
- Skill check: selected <skill> | none applicable
- Classification: Non-trivial
- Ledger status: Approved vN found: no
- Independent-agent status: supported, authorized, and not prohibited |
  unsupported | not authorized/not requested | explicitly prohibited |
  failed/timed out - <details>
- Pre-approval independent agents: scope-ledger-adversarial-review used |
  unsupported/not authorized/not requested/prohibited/failed/timed out -
  <evidence status>; other read-only explorers - <purpose and allowed actions>

### DISCOVERY
- entrypoints/surfaces:
- caller/callee flow:
- persisted/config/archive/schema:
- user-visible/help/docs:
- existing tests:
- independent-agent evidence: <none | agent role/purpose/findings used as
  evidence | unsupported/not authorized/not requested/prohibited/failed/timed
  out - status>
- unknowns:

### SCOPE
- Proposed Scope Ledger vN
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
- Reasoning budget: <low|medium|high|xhigh> (lowest sufficient). Rationale: <one sentence>; escalation trigger: <one phrase or "none expected">.

### SCOPE ADVERSARIAL REVIEW
- question: What edge case would make this scope unsafe or incomplete?
- scope-ledger-adversarial-review: used - <findings and lead disposition> |
  unsupported/not authorized/not requested/prohibited/failed/timed out -
  <evidence status and lead disposition>
- applicable edge areas:
- gaps found: none | <items>
- disposition: nothing material found | revise ledger to v<N+1>

If material gaps are found, do not present the approval token for the current
version. Produce the revised Scope Ledger and repeat this review.

Stop here and wait for the exact approval token:
`Approved vN`

No code, diffs, file writes, formatters, or full validation commands before
that approval.

---

## Phase B: Execution Ledger

### GATE
- Skill check: selected <skill> | none applicable
- Classification: Non-trivial
- Ledger status: Approved vN found: yes
- Approved scope version:
- Independent-agent status: supported, authorized, and not prohibited |
  unsupported | not authorized/not requested | explicitly prohibited |
  failed/timed out - <details>
- Independent-agent phase/use: none | pre-approval explorer | post-approval
  worker | go-code-quality-review explorer | fresh-verifier explorer -
  <lead-owned disposition>

### PREFLIGHT
- Git preflight: branch=<name>; worktree=<clean|dirty acknowledged>; rollback=<hash/tag/branch>
- Dirty files not owned by this task:

### DESIGN
- current flow:
- code-walk evidence: <commands/files/ADRs inspected | N/A - reason>
- implementation plan:
- independent-agent plan: none | <worker/go-code-quality-review/fresh-verifier
  roles; approved scope version; slice name/objective; base revision or
  integration point; allowed paths; forbidden paths; production-safe stopping
  point; targeted checks; expected output/changed paths or findings-only
  output; stop conditions; lead verification>
- contracts: changed | unchanged
- user-visible behavior: changed | unchanged
- operator-visible behavior: changed | unchanged | N/A
- dependency rigor: Light | Full
- dependency scan evidence: <required for Full rigor>
- blast-radius audit: <result | N/A - reason>
- triggered audits: Config Contract Audit | Retained-State Audit | Performance evidence | Decision-memory audit | Workflow-drift audit | none
- connection lifecycle audit: <result | N/A - reason>
- leak-detection audit: <result | N/A - reason>
- YAML comment/header audit: PASS|FAIL|N/A - note
- Go comment intent audit: PASS|FAIL|N/A - note
- Go crawler-entry audit: PASS|FAIL|N/A - note
- README impact: Required | Not required - <one sentence>
- Support-agent docs impact: Required | Not required - <one sentence>
- ADR/TSR pre-read: <relevant refs | No relevant ADR found; No relevant TSR found>
- claim evidence plan: <how progress/validation/performance/science claims will be grounded | N/A - reason>
- validation lane: documentation-only Markdown | workflow/skill-doc |
  code/mixed/runtime-contract | other - <reason>
- checker plan:

### IMPLEMENTATION
For each slice:
- slice:
- objective:
- files:
- blast-radius boundary:
- subagent use: none | <worker role; approved scope version; slice; base
  revision or integration point; allowed paths; forbidden paths; production-safe
  stopping point; targeted checks; expected output/changed paths; stop
  conditions>
- checks:
- result:
- next-slice gate: passed | blocked - reason
- remaining risk:

### REVIEW
- findings by severity:
- confirmed fixes:
- rerun checks:
- go-code-quality-review: used - <findings and lead disposition> |
  N/A - no Go implementation |
  unsupported/not authorized/not requested/prohibited/failed/timed out -
  <evidence status and lead disposition>
- fresh verifier pass: <fresh-verifier explorer | fresh self-verification | N/A - reason>
- subagent lead verification: PASS|FAIL|N/A - note
- verification command evidence: <captured excerpts required by
  `docs/review-checklist.md` Verification command reporting | N/A - reason>

If no material findings:
- `Review Pass findings: none material`

### SELF-AUDIT
Use the canonical SA1-SA15 mapping in `docs/review-checklist.md`.

- Applicability manifest:
  - applicable: <SA IDs>
  - not applicable: <SA IDs> - <shared reason; repeat for different reasons>
- Results:
  - <applicable SA ID>: PASS|FAIL - <evidence note or earlier-marker reference>

Classify every SA1-SA15 ID exactly once. The manifest replaces individual
`N/A` rows; missing, unknown, or duplicate IDs fail the Self-Audit. Applicable
IDs require results. Required independent evidence that is unsupported, not
authorized/not requested, prohibited, failed, timed out, missing, or stale is a
`FAIL` or explicit gap/waiver, not an omission. SA7 and SA8 reference `REVIEW`
verification command evidence when command-backed. The lead owns every final
disposition.

### CLOSEOUT
- summary:
- tradeoffs:
- risks and mitigations:
- contracts and compatibility:
- user impact and determinism:
- README impact:
- Support-agent docs impact:
- fresh verifier outcome:
- independent-agent use and lead-owned disposition:
- claim evidence:
- verification commands and results: <reference `REVIEW` verification command
  evidence; do not repeat captured excerpts>
- validation lane:
- ADR handling outcome:
- Decision refs:

### TRACEABILITY
For every Scope Ledger item that was `Agreed` or `Pending` at the start of implementation:
- ledger item:
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
