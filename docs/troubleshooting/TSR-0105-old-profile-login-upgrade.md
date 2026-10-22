---
title: "TSR-0105: Old Profile Login Upgrade"
description: Existing profiles fell into protected temporary defaults instead of a writable Boolean reset.
---

Status: Resolved
Date Opened: 2026-10-21
Date Resolved: 2026-10-22
Owner: GoCluster engineering
Technical Area: filter persistence and telnet login
Trigger Source: Operator report
Led To ADR(s): ADR-0318
Tags: boolean, upgrade, settings, presets, login

## RCA Summary

The repair and validation below record Scope Ledger v4. The later owner-selected
[ADR-0319](../decisions/ADR-0319-boolean-onboarding-and-source-default.md)
replaces its modes-only starting selection with the same configured mode/source
defaults as new users. Settings preservation, backup and retry safety remain
unchanged; the old HUMAN-only temporary fallback was defective because it lost
preferences and could not save, not because HUMAN is inherently invalid.

- What happened: a recognized old-format user logged in with temporary defaults,
  lost active saved preferences and could not persist new Boolean rules.
- Why: login called the strict v5 loader without a reset-only upgrade boundary.
  Separate storage-refusal and protected-login tests did not prove this scenario.
- What fixes it: an explicit, bounded reset preserving settings and metadata,
  installing configured default modes only, and cleaning old shared presets.
- How we know: an actual TCP version-4 login regression reproduced lost
  preferences and the unwanted HUMAN-only temporary default before integration.
  Corrected old-profile TCP login/save/reconnect and fault/race checks pass.
- Operator/support answer: fresh-store MVP success does not establish that
  old profiles can upgrade. Do not delete profiles or edit version markers to
  work around the missing transition.

## Triggering Request

- Request date: 2026-10-21
- Request summary: the owner asked whether old-profile login had been tested,
  identified the missing behavior as a defect, and authorized Scope Ledger v4.
- Request reference: Boolean implementation and existing-user upgrade session.

## Symptoms and Impact

The real TCP test seeded an old flat version-4 record for LZ3NY-11. Its readback
returned GO, empty GRID, default dedupe and inherited controls instead of saved
CC, FN31, URBAN, MED and explicit control choices. The session used a
mode-and-HUMAN temporary default and logout could not overwrite the protected
old file. Preserving old bytes was safe, but normal upgrade behavior was absent.

## Timeline

1. 2026-10-21 - Existing tests confirmed storage refusal and protected fallback,
   but not representative old-profile login.
2. 2026-10-21 - The owner selected default modes without a source restriction.
3. 2026-10-21 - Scope Ledger v4 authorized reset-only upgrade and failure coverage.
4. 2026-10-21 - The new TCP regression reproduced the defect before integration.
5. 2026-10-22 - Review caught and corrected upgrade-stage not-found errors being
   mistaken for initial profile absence. Current-v5/library-only and new-SSID
   shared-library cases were also verified.

## Hypotheses and Tests

1. Old-format refusal alone qualified upgrade behavior.
   - Evidence: the storage tests passed while the actual old-record TCP login
     returned different preferences and could not save.
   - Outcome: Rejected.
2. Login lacks the explicit reset/settings-preservation operation.
   - Evidence: the inspected login called LoadUserRecord directly and substituted
     NewFilter on all load errors.
   - Outcome: Supported.

## Findings

- Root cause: the fresh-store-only MVP boundary was not an existing-user upgrade.
- Contributing factor: tests proved safe refusal separately from login fallback,
  leaving the composed old-profile/settings/save/reconnect path unqualified.
- Related failure: an old shared preset library can still block SAVE PRESET
  after the user record alone is fixed.
- Decision impact: [ADR-0318](../decisions/ADR-0318-old-profile-reset-upgrade.md)
  selects an explicit reset-only upgrade without restoring old rule semantics.

## Decision Linkage

- ADR created/updated: ADR-0318, refining ADR-0317.
- Decision delta summary: recognized old profiles upgrade once; malformed and
  future formats remain protected.
- Contract/behavior changes: preserve settings/controls/metadata and install
  configured default modes with all sources eligible.

## Verification and Monitoring

- Before-fix regression: TestOldProfileUpgradeActualTCPLoginAndReconnect,
  native/version-4, failed with the wrong settings and temporary defaults.
- Required correction checks: supported old versions, both transports, actual
  edit/save/reconnect, old shared presets, backup/write failures and concurrent
  SSIDs retaining new Boolean presets.
- Corrected checks: 194 focused race cases passed, including both transports
  and old versions; the new decoder passed a short coverage-guided fuzz run.
- The complete normal repository suite passed 11,014 cases, with 99 documented
  opt-in/helper skips. Broad race validation encountered the unchanged
  TestDailyFileSinkArchivesStaleActiveOnStartup failure; this separate logging
  failure is not reported as a successful race lane.
- A dedicated initial-profile-missing marker prevents later backup/commit
  not-found errors from creating new-user defaults over an existing profile.
- A genuinely new SSID with old shared presets retains its normal new-user
  defaults while clearing the old library after its first profile commit.
- Monitor explicit upgrade notices, backup errors and retries; never label
  protected fallback as a successful upgrade.

## References

- [Login coordinator](../../telnet/configuration_session.go).
- [TCP upgrade regression](../../telnet/configuration_upgrade_test.go).
- [ADR-0318](../decisions/ADR-0318-old-profile-reset-upgrade.md).
- Issues/PRs/commits: none created for this local implementation.
