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

### DISCOVERY
- entrypoints/surfaces:
- caller/callee flow:
- persisted/config/archive/schema:
- user-visible/help/docs:
- existing tests:
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

### PREFLIGHT
- Git preflight: branch=<name>; worktree=<clean|dirty acknowledged>; rollback=<hash/tag/branch>
- Dirty files not owned by this task:

### DESIGN
- current flow:
- code-walk evidence: <commands/files/ADRs inspected | N/A - reason>
- implementation plan:
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
- validation lane: documentation-only Markdown | code/mixed/runtime-contract | other - <reason>
- checker plan:

### IMPLEMENTATION
For each slice:
- slice:
- objective:
- files:
- blast-radius boundary:
- checks:
- result:
- next-slice gate: passed | blocked - reason
- remaining risk:

### REVIEW
- findings by severity:
- confirmed fixes:
- rerun checks:

If no material findings:
- `Review Pass findings: none material`

### SELF-AUDIT
- Scope and dependency coverage: PASS|FAIL|N/A - note
- Code-walk and blast-radius evidence: PASS|FAIL|N/A - note
- Contract, config, and protocol correctness: PASS|FAIL|N/A - note
- YAML comment/header audit: PASS|FAIL|N/A - note
- Go comment intent audit: PASS|FAIL|N/A - note
- Go crawler-entry audit: PASS|FAIL|N/A - note
- Concurrency, backpressure, and resource bounds: PASS|FAIL|N/A - note
- Leak-detection evidence: PASS|FAIL|N/A - note
- Verification and checker discipline: PASS|FAIL|N/A - note
- Documentation, decision memory, and traceability: PASS|FAIL|N/A - note
- Workflow-drift audit: PASS|FAIL|N/A - note
- Validation block completeness: PASS|FAIL|N/A - note

### CLOSEOUT
- summary:
- tradeoffs:
- risks and mitigations:
- contracts and compatibility:
- user impact and determinism:
- README impact:
- Support-agent docs impact:
- verification commands and results:
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
- decision refs:

### VALIDATION
Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)
