# TSR-0053 - MODE Delta Became A Whitelist Transition

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: filter MODE mutation, feedback and persistence
Trigger Source: Chat request
Led To ADR(s): ADR-0272

## RCA Summary

After allowing every mode and rejecting FT8, passing FT8 again could hide CW.
The named command added a true entry to an empty allow map, converting the
previous open filter into a whitelist. The common finite-family toggle also
opened an empty whitelist after its last allowance was removed. Neither
transition satisfies the selected MODE visibility-delta contract.

## Root cause or best current explanation

The shared toggle sets AllModes from map length. The existing matcher gives
BlockAll first precedence, then named denies, then any nonempty allow map;
AllModes is the fallback only when the allow map is empty. A false-only map is
therefore restrictive. Flags alone, a list of current supported modes, and
displaying only true entries cannot identify that fallback reliably.

UnknownModeVisible used flags instead of the complete predicate. MODE feedback
could promote coverage of today's taxonomy to ALL or list false custom entries,
even though neither represents effective acceptance of unseen modes.

## Fix or mitigation

The MODE owner preserves its actual open/restrictive fallback and changes the
named allowance/deny. A named PASS from BlockAll clears latent allows before
opening, so hidden selections do not revive. Named REJECT keeps BlockAll and
leaves an empty restrictive whitelist closed. Other finite-family transitions
and the matcher retain their existing behavior.

Unknown feedback uses the matcher; MODE-only snapshots use raw map length for
fallback and true entries for display. Marked records, presets and baselines
already preserve the resulting exact state without legacy migration.

## Checker Boundaries

Pointwise tests compare actual Matches before and after each named command,
including absent/false/true map entries and custom/unseen tokens. Later-taxonomy
tests distinguish an open fallback from an explicit list covering every current
supported mode. Persistence checks compare exact rules and actual matching.

Old tests constructing an exclusive whitelist with ResetModes then SetMode
must explicitly start restrictive. Preserve their rejection assertions; merely
changing expected flags would weaken the COMMENT and unknown-mode oracles.

## Decision Disposition And Verification

[ADR-0272](../decisions/ADR-0272-mode-visibility-deltas.md) records the selected
MODE-specific refinement of ADR-0018. The
[execution record](../command-fix-execution.md#checkpoint-07-mode-visibility-preserving-deltas)
contains the state matrix, observed qualification and its limits. The checkout
runner checks history outcomes, immediate saved rules and a fresh server process;
an acknowledgement alone cannot demonstrate filtering or durability.

Support answer: PASS/REJECT MODE changes only named modes. To build a whitelist,
use REJECT MODE ALL first, then PASS MODE with the desired names. PASS MODE ALL
explicitly opens the MODE filter. Other categories still apply. The unsupported
RESET MODE recipe was removed during checkpoint 18b's wording reconciliation.
