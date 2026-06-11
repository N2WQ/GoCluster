# ADR-0177: Distance-Selected VOACAP Method

- Status: Accepted
- Date: 2026-06-11
- Decision Origin: Scope Ledger v3

## Context

Runtime VOACAP fallback decks previously used Method 30 for every path. VOACAP
guidance recommends Method 20 for paths under 7000 km and Method 30 for paths
at or above 7000 km, where short-path/long-path smoothing becomes relevant.

GoCluster already caches VOACAP results by fine path cells rather than exact
Maidenhead grid pairs. The generated VOACAP deck itself uses the Maidenhead
grid-center endpoints from the request, not H3 centroids. Method selection
therefore needs to follow the deck endpoints while preserving the existing
cache granularity and bounded worker behavior.

## Decision

Runtime VOACAP path decks select the METHOD card from the same Maidenhead
grid-center coordinates used for the `CIRCUIT` card:

```text
distance < 7000 km   -> METHOD 20
distance >= 7000 km  -> METHOD 30
```

The 7000 km boundary is inclusive for Method 30. Distance is spherical
great-circle distance over the deck endpoints.

Experiment decks and direct `BuildPathDeck` callers keep the historical Method
30 default unless they explicitly request another supported method. This keeps
the SSN forecast experiment baseline stable while the runtime fallback follows
VOACAP's distance recommendation.

The fallback cache and delay keys continue to use the existing fine H3
path-cell identity. Near the 7000 km boundary, a cache hit may reuse a method
selected for another grid pair in the same res-2 path-cell pair. This is an
accepted part of the current cache granularity, not a new exact-grid cache
contract.

Method 20 and Method 30 output both use the `FREQ`, `SNR`, and optional `REL`
rows parsed by the shared VOACAP prediction parser. The old
`ParseMethod30Predictions` function remains as a compatibility wrapper around
the generalized parser.

## Alternatives considered

1. Keep Method 30 for every runtime fallback path.
   - Rejected because it does not follow VOACAP's documented method guidance
     for shorter paths.
2. Select the method from H3 cell centroids.
   - Rejected because the VOACAP deck uses Maidenhead grid-center endpoints.
     Selecting from different coordinates could make method choice disagree
     with the generated `CIRCUIT` card.
3. Add the selected method or exact grids to the cache key.
   - Rejected for this slice because current VOACAP cache reuse is already
     intentionally fine-cell based. Res-2 granularity is close enough for the
     7000 km boundary uncertainty, and exact-grid cache keys would increase
     retained entries without evidence that the added precision changes useful
     operator outcomes.
4. Expose method selection as YAML.
   - Rejected because method choice is model contract, not routine operator
     policy.

## Consequences

### Benefits

- Runtime fallback follows VOACAP's published Method 20/30 recommendation.
- The method decision is deterministic and tied to the exact deck endpoints.
- Existing worker, queue, delay, cache cap, SSN cadence, p50 authority, REL
  gates, and `SHOW PROP` command behavior stay unchanged.
- Parser tests now prove Method 20 and Method 30 fixture compatibility.

### Risks

- VOACAP-derived glyphs and `SHOW PROP` rows can change for sub-7000 km paths.
- Near 7000 km, fine-cell cache reuse can cross the exact method boundary.
  This is consistent with the existing cache granularity but should be kept in
  mind when investigating marginal paths.
- Method 20/30 parser compatibility depends on the retained `FREQ`, `SNR`, and
  `REL` row shape. Future VOACAP output drift should fail parser tests rather
  than silently change records.

### Operational impact

- No new YAML keys are introduced.
- No new worker lane, queue priority, synchronous VOACAP execution, or cache
  sizing change is introduced.
- Operators may see different fallback classifications or `SHOW PROP` outlooks
  for shorter paths after a fresh VOACAP run.

## Links

- Related issues/PRs/commits: none
- Related code: `internal/voacap/deck.go`, `internal/voacap/output.go`,
  `pathreliability/voacap_fallback.go`
- Related tests: `internal/voacap/forecast_state_test.go`,
  `internal/voacap/output_test.go`,
  `pathreliability/voacap_fallback_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: updates ADR-0165 and ADR-0169 Method-30-only
  runtime wording; continues ADR-0162, ADR-0164, ADR-0172, ADR-0173, and
  ADR-0174 cache, rolling-window, command, REL, and beacon semantics
