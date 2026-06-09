# ADR-0166: Closed Path Filter Token

- Status: Accepted
- Date: 2026-06-09
- Decision Origin: Scope Ledger v1

## Context

ADR-0161 introduced the VOACAP closed fallback and mapped closed fallback spots
to the existing `UNLIKELY` PATH filter class. That was conservative for the
first VOACAP integration, but it left users without a way to reject or isolate
closed fallback spots separately from ordinary observed-bucket `UNLIKELY`
predictions.

The display already distinguishes closed fallback spots through the configured
closed glyph and `vcap` diagnostics. The filter surface should provide the same
operator control without breaking stored or scripted filters that already use
`UNLIKELY`.

## Decision

Add `CLOSED` as a supported `PASS/REJECT PATH` token. VOACAP closed fallback
results are filter-visible as `CLOSED`; normal bucket and VOACAP-aligned sparse
p50 predictions continue to use `HIGH`, `MEDIUM`, `LOW`, or `UNLIKELY`, and
missing or gated evidence remains `INSUFFICIENT`.

Treat `CLOSED` as a subtype of `UNLIKELY` for compatibility:

1. Existing `PASS PATH UNLIKELY` filters continue to pass closed fallback spots.
2. Existing `REJECT PATH UNLIKELY` filters continue to reject closed fallback
   spots.
3. Direct `PASS PATH CLOSED` and `REJECT PATH CLOSED` rules target only closed
   fallback spots.
4. Direct `CLOSED` rules are more specific than inherited `UNLIKELY`
   compatibility, so users can express both `PASS PATH UNLIKELY` plus
   `REJECT PATH CLOSED`, and `REJECT PATH UNLIKELY` plus `PASS PATH CLOSED`.

This changes only PATH filter matching semantics. It does not change glyph
selection, VOACAP fallback eligibility, p50 thresholds, propagation counters,
or `SET DIAG PATH` output.

## Alternatives considered

1. Keep mapping closed fallback spots only to `UNLIKELY`.
   - Rejected because operators could not reject closed fallback spots without
     also rejecting ordinary poor-path predictions.
2. Replace `UNLIKELY` behavior entirely with `CLOSED` for closed fallback
   spots.
   - Rejected because existing `PASS/REJECT PATH UNLIKELY` filters and user
     records would silently change behavior.
3. Add a separate `PATHSOURCE` or `PATHCLOSED` command family.
   - Rejected because closed is a path result category from the user's point of
     view, and extending the existing PATH class list keeps command syntax
     compact.

## Consequences

### Benefits

- Users can pass or reject VOACAP closed fallback spots directly.
- Existing `UNLIKELY` filters remain compatible with the ADR-0161 behavior.
- `SHOW FILTER`, command help, README, and support docs can name the same
  `CLOSED` token that operators see through the closed glyph and `vcap`
  diagnostics.

### Risks

- `CLOSED` is only available after the existing VOACAP fallback has a cached
  closed result. Before that, an otherwise eligible sparse path still filters as
  `INSUFFICIENT`.
- Older binaries will not understand persisted user records containing the new
  `CLOSED` token.
- The subtype rule is more nuanced than the other string filter domains, so it
  needs explicit tests and documentation.

### Operational impact

- `PASS PATH CLOSED` allows closed fallback spots.
- `REJECT PATH CLOSED` blocks closed fallback spots.
- `PASS/REJECT PATH UNLIKELY` keeps including closed fallback spots for
  compatibility.
- No YAML setting or migration is required.

## Links

- Related code: `filter/filter.go`, `telnet/server.go`,
  `telnet/filter_commands.go`, `commands/processor.go`
- Related docs: `README.md`, `telnet/README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Related tests: `filter/filter_test.go`, `telnet/server_filter_test.go`,
  `telnet/path_settings_test.go`, `commands/processor_test.go`
- Related ADRs: ADR-0161, ADR-0163
- Related TSRs: none
- Supersedes / superseded by: Supersedes ADR-0161's closed-fallback
  `UNLIKELY`-only PATH filter semantics; preserves ADR-0163's sparse p50
  alignment behavior
