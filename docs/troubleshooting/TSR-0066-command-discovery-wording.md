# TSR-0066 - Command Discovery Wording

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: filter usage, HELP, DX admission and command recipes
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

Unknown-type suggestions omitted working filter families, and generic usage
mixed list grammars in a single very long line. Shared DXBM errors recommended
CC-only syntax. HELP/README generalized MODE's visibility contract to other
lists, described only part of the US/Canadian mailing-code vocabulary and
omitted DX admission qualifications. Copied recipes advertised RESET MODE,
which neither dialect executes.

## Root cause or best current explanation

Hand-maintained wording duplicated only part of dispatch and applied ordinary
list rules to special categories. Synchronized HELP fixtures could reproduce
the same inaccurate claim. Source-owner inventories and executable recipes
are needed in addition to renderer synchronization.

## Fix or mitigation

Use finite action-aware suggestions and short bounded usage. The 29-family
inventory includes COMMENT, MINSNR, NEARBY, DXBM and PASS-only NOFILTER; PC93
is an ANNOUNCE alias. COMMENT uses shared PASS/REJECT rather than a CC alias.
Full syntax and loaded vocabularies remain in HELP PASS/REJECT.

Shared DXBM errors recommend shared PASS/REJECT and REJECT MODE. State wording
covers the registered US/Canadian mailing vocabulary, including provinces and
territories. MODE alone promises to preserve unnamed visibility. Exclusive
recipes block MODE ALL first and then pass selected modes; PASS MODE ALL opens
the category. Correct the unsupported opening recipe in ADR-0052, ADR-0272 and
TSR-0053, retaining dated correction notes and the accepted decision history.

DX help describes plain decimal frequencies, valid login and TEST prerequisites,
optional shared station reputation and bounded queue outcomes. Spot queued
confirms intake; processing and filtering still determine delivery.

## Checker Boundaries

Independent literal inventories check all 29 families and 73 mailing codes,
action-only support, aliases and intended wording. Composed real owner checks
preserve configuration, revision, runtime owners, preset and exact stored bytes
with no commit call on refusals. Actual checkout cases inspect complete feedback
before a strict correlated CONFIG barrier, then compare preferences, exact
bytes and file identity/time/size. They cannot observe internal commit calls.

HELP receipts include their deliberate pause footer. Ordinary short feedback
stays below the fixture's ten-row threshold and preserves pause state. These
are local command/discovery checks, not production load, deployed configuration,
scientific inference, location accuracy or spot-delivery guarantees.

## Verification And Support

Observed results are retained in the
[execution record](../command-fix-execution.md#checkpoint-18b-accurate-suggestions-categories-and-wording).
Use HELP PASS/REJECT for the category-specific grammar. Registered mailing codes
can differ from a station's operating location. Use the actual command response
to distinguish a refused post from queue intake.
