---
name: go-code-quality-review
description: "Use for read-only review of newly written gocluster Go implementation when the work is High-risk or substantial: shared/exported interfaces, material algorithms or state machines, substantial rewrites, meaningful residual uncertainty, or multiple production packages with shared impact or cross-package uncertainty. Do not trigger for every Non-trivial Go edit."
---

# Go Code Quality Review

After implementation and before final closeout, read
[Substantial Go Review Method](../../../docs/review-checklist.md#substantial-go-review-method)
and apply it completely. That checklist owns the method and its trigger;
`docs/dev-runbook.md` owns validation commands.

This is the Codex skill-discovery entry point. A separate reviewer remains
read-only and findings-only; the lead retains fixes, validation and final claims.
