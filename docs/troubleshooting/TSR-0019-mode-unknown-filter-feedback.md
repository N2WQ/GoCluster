# TSR-0019: MODE UNKNOWN Filter Feedback

- Status: Resolved
- Date opened: 2026-04-22
- Status date: 2026-04-22

## RCA Summary

- What happened: `PASS MODE UNKNOWN` looked like it replaced the MODE filter,
  but `SHOW FILTER` still displayed the default full allowlist.
- Why: MODE PASS/REJECT commands are additive, and default sessions already
  allow `UNKNOWN`; the command response sounded like exact replacement even
  though matching was working.
- What fixed it: MODE and EVENT feedback now reports enabled/rejected modes,
  effective lists, and warnings when `UNKNOWN` or no-event spots are hidden.
- How we know: Unit tests showed `UNKNOWN` matching worked, direct telnet
  testing reproduced the confusing display, and source inspection confirmed
  additive `filter.SetMode(mode, true)` semantics.
- Operator/support answer: Treat this as an operator-feedback issue, not a
  filter-matching bug; use effective MODE display to explain what is actually
  enabled.

## Trigger

An operator tried `PASS MODE UNKNOWN` while investigating blank-mode spots. The command response implied a narrow MODE setting, but live `SHOW FILTER` still showed the full default mode allowlist.

## Symptoms and impact

`PASS MODE UNKNOWN` returned `Filter set: Modes UNKNOWN`, which implied the MODE filter had been replaced. In reality, `PASS MODE <list>` is additive: it enables listed modes and leaves modes not listed unchanged. Because default filters already include `UNKNOWN`, the command was effectively a no-op for a default session and looked broken.

## Hypotheses tested

1. `UNKNOWN` was not recognized by the taxonomy-backed MODE filter.
2. Blank-mode spots did not map to the `UNKNOWN` filter token.
3. Telnet command feedback was misleading even though filter mutation and matching were working as designed.

## Evidence

- Unit-level filter tests showed default filters include blank-mode spots and `REJECT MODE UNKNOWN` suppresses them.
- A direct telnet session against `127.0.0.1:8300` showed `PASS MODE UNKNOWN` left the effective MODE list as `CW, LSB, USB, RTTY, MSK144, PSK, SSTV, UNKNOWN`.
- The follow-up display patch changed `REJECT MODE ALL` followed by `PASS MODE UNKNOWN` to show `MODE: enabled=UNKNOWN` instead of exposing the raw allow/block rule.
- Code inspection showed `PASS MODE <list>` routes through `filter.SetMode(mode, true)`, which is intentionally additive.

## Root cause or best current explanation

The root cause was operator feedback, not filter matching. MODE commands use additive allow/block semantics, but the response `Filter set: Modes <list>` read like an exact replacement operation. The default explicit MODE allowlist made that ambiguity visible for `UNKNOWN`.

## Fix or mitigation

MODE command responses now say which modes were enabled or rejected, include an explicit effective MODE list, and warn when the effective MODE filter hides `UNKNOWN`. `SHOW FILTER` also warns when `UNKNOWN` is hidden. EVENT feedback follows the same display rule and reports that no-event spots pass the EVENT domain.

## Why an ADR was or was not required

- No decision change. The existing additive PASS/REJECT filter contract remains intact.
- No ADR was required because the patch changes operator feedback and documentation, not the durable filter semantics.

## Links

- Related ADRs: ADR-0018, ADR-0052, ADR-0069
- Related issues/PRs/commits: pending
- Related tests: `telnet/server_filter_test.go`, `filter/filter_test.go`
- Related docs: `README.md`, `telnet/README.md`
