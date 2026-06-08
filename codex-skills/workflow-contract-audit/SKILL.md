---
name: workflow-contract-audit
description: "Use when editing or reviewing gocluster Codex workflow contracts, AGENTS.md, VALIDATION.md, docs/change-workflow.md, review checklists, dev runbooks, code-quality rules, non-trivial templates, repo-managed skills under codex-skills/, workflow scripts, skill verification, approval gates, evidence markers, validation scoring, or closeout rules."
---

# Workflow Contract Audit

## Overview

Use this skill to prevent drift between the always-loaded Codex contract,
detailed workflow docs, validation rubric, templates, repo-managed skills, and
scripts that enforce the workflow.

## Workflow

1. Identify every contract surface touched.
   - Always-loaded contract: `AGENTS.md`
   - Detailed workflow: `docs/change-workflow.md`
   - Evidence shape: `docs/templates/non-trivial-change-template.md`
   - Validation rubric: `VALIDATION.md`
   - Review and checker sources: `docs/review-checklist.md`,
     `docs/dev-runbook.md`, `docs/code-quality.md`
   - Repo skills: `codex-skills/**/SKILL.md` and `agents/openai.yaml`
   - Workflow scripts and script docs under `scripts/`

2. Preserve exact operational strings.
   - Approval token shape: `Approved vN`
   - Skill marker shape: `Skill check: selected <skill>` or
     `Skill check: none applicable`
   - Required evidence marker names
   - Final validation block labels and score format
   - Scope Ledger, Scope Adversarial Review, Decision refs, and
     Scope-to-Code Traceability wording when other docs rely on it

3. Check trigger coherence.
   - Skill frontmatter descriptions must contain trigger conditions because
     Codex sees them before loading the skill body.
   - `AGENTS.md` should route to triggered skills without duplicating long
     command recipes.
   - `docs/change-workflow.md` should own detailed workflow behavior.
   - `docs/templates/non-trivial-change-template.md` should contain reportable
     evidence fields for newly required audits.
   - `codex-skills/README.md` should list repo-authoritative skills.

4. Check contradiction and reachability.
   - Verify moved or shortened rules remain reachable from `AGENTS.md`.
   - Verify validation rules, runbook commands, review expectations, skill
     output expectations, and closeout requirements do not contradict each
     other.
   - Verify read-only, Small, and Non-trivial paths remain distinct.
   - Verify optional tools remain optional unless the workflow explicitly makes
     them required.

5. Check skill bundle hygiene.
   - No generated template placeholder text remains.
   - Frontmatter names match directory names.
   - `agents/openai.yaml` metadata matches the skill body when present.
   - Repo skill docs do not tell agents to copy or install skills into
     user-level skill storage.
   - Run `scripts/verify-codex-skills.ps1` after repo skill edits.

6. Check documentation impact.
   - README impact is required when onboarding, user-facing workflow, or public
     repo behavior changes.
   - Support-agent docs impact is required when operator-support topics,
     routing docs, or support answers change. Pure Codex workflow routing does
     not automatically require support-agent docs.
   - Use `decision-memory-audit` when workflow changes require ADR/TSR
     handling.

## Output Expectations

- Include a `Workflow-drift audit` section when this skill triggers.
- Name the contract surfaces inspected, exact strings preserved, trigger
  coherence checks, targeted text checks, verifier/checker results, and
  remaining drift risks.
- Treat a missing route from `AGENTS.md` to a detailed rule as a material
  workflow gap.
