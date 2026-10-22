---
title: "ADR-0318: Old Profile Reset Upgrade"
description: One-time default-modes-only upgrade that preserves settings without translating legacy filters.
---

- Status: Accepted
- Date: 2026-10-21
- Decision Origin: Troubleshooting chat

## Context

[ADR-0319](ADR-0319-boolean-onboarding-and-source-default.md) supersedes the
modes-only selection below: old-profile resets now receive the same configured
mode/source defaults as new users. Other upgrade safety and preservation
decisions remain in force.

[ADR-0317](ADR-0317-boolean-filter-replacement-mvp.md) deliberately qualified the
Boolean MVP against fresh v5 storage. Its strict reader refuses older records.
That boundary is not sufficient for existing users: login would use temporary
defaults, omit their saved settings and refuse ordinary saving.

The owner requires a normal, writable upgrade and selected configured default
modes only as the starting rule. This supersedes the fresh-store-only handling
of recognized old profiles, not the Boolean engine or machine schema.

## Decision

Use an explicit, login-owned reset-only upgrade for recognized unversioned and
version 1–4 profiles. Keep pure v5 codecs strict and read-only; do not turn
ordinary decoding into an implicit write operation.

Discard old category filters, turn NEARBY OFF and clear the applied-preset
association. Install an ordinary editable PASS rule in slot 0 containing
configured default modes, with no SOURCE restriction. Preserve exact settings,
separate controls and full-SSID login metadata. Updating the current login's
timestamp/IP remains the existing login operation.

This is a one-time reset. Existing v5 profiles and rules are not defaulted again.
New-user mode/source defaults remain unchanged, and RESET FILTER continues to
clear personal rules rather than reinstalling defaults.

Coordinate old preset cleanup with the existing base-call library stripe.
Remove recognized legacy entries without deleting v5 presets already created
by another SSID. The user record and shared library have different owners;
fixing only the record would leave SAVE PRESET blocked by old storage.

Admit replacement payloads before writing and preserve verified original-file
backups. Never overwrite a different existing backup. Keep per-file atomic
replacement, explicit errors and safe retries; do not claim one atomic
transaction across the library and user record. Future, malformed and
unsafe/over-budget inputs remain protected rather than silently reset.
An existing v5 profile with only a library-cleanup failure retains ordinary
writable settings and a separate warning. Distinguish a genuinely absent profile
from an upgrade-stage filesystem error: only authoritative initial absence grants
new-user creation. A later wrapped not-found error must never replace old
settings with new-user defaults.

Do not restore the old filtering engine, translate old rules, accept older
machine protocols or introduce an unbounded historical registry.

## Alternatives considered

1. Continue using protected temporary defaults. Rejected because recognized old
   users lose their active preferences and cannot save Boolean rules.
2. Require an offline conversion/reset utility. It can assist deployment, but
   cannot be the only mechanism: omitting it would leave login broken.
3. Translate all category filters. Rejected by the selected full-replacement
   and reset policy; it preserves unwanted compatibility complexity.
4. Install empty rules or full new-user defaults. The owner instead selected
   modes-only defaults, avoiding an all-mode feed without adding HUMAN-only
   source filtering.

## Consequences

### Benefits

- Existing users retain settings and can save after upgrading.
- Default rules are visible and editable, not hidden category restrictions.
- New Boolean settings and presets survive subsequent logins and other SSIDs.

### Risks

- Upgrade discards old personal filters and presets deliberately.
- Backup, decoding or disk failures can block an upgrade and must be visible.
- Per-file atomicity does not remove cross-file partial-failure cases.

### Operational impact

Explain the reset at login. Keep original backups and compatible binary/data
backups for rollback. Implementation and private tests do not authorize an
agent to alter production data or restart production services.

## Links

- Authority: owner-selected configured-modes-only policy and exact Scope Ledger
  v4 authorization in the implementation conversation.
- Root cause: [TSR-0105](../troubleshooting/TSR-0105-old-profile-login-upgrade.md).
- Integration: [login coordinator](../../telnet/configuration_session.go).
- Regression: [actual TCP upgrade tests](../../telnet/configuration_upgrade_test.go).
- Supersedes: recognized old-profile/preset refusal-only clauses of
  [ADR-0317](ADR-0317-boolean-filter-replacement-mvp.md).
- Superseded by: [ADR-0319](ADR-0319-boolean-onboarding-and-source-default.md)
  for the modes-only starting selection.
