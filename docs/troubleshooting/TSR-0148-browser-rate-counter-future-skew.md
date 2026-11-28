# TSR-0148 - Browser Rate Counter Drops and Row Flash Waves on RBN Minute Rollovers

Status: Monitoring
Date Opened: 2026-11-27
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Browser console rate counter and row highlight
Trigger Source: Operator report
Led To ADR(s): none
Tags: browser-console, rate-counter, timestamps, rbn

## RCA Summary
- What happened: On a busy CW contest feed (~1,500–2,000 spots/min), the live `/min` counter climbed throughout each minute and abruptly dropped ("crashed") at `:00`, and right at `:00` the visible spot rows flashed dark teal/blue-green all at once for ~2.5 seconds. Future-stamped spots could also pin `S.rate` across multiple minutes and corrupt the former top-bar sparkline with `NaN`.
- Why: RBN spots carry minute-only timestamps (`HHMMZ`), so the server sets seconds to `:00.000` (1,825/1,825 spots in live `VE3EID-15` observation). Both `spotsPerMinute()` (`S.rate` FIFO over remote `s.t`) and `.row.fresh` (`Date.now() - r.s.t < 2500`) used remote spot timestamps instead of browser arrival time. Between `:00` and `:59`, zero `:00.000` spots from the current minute expired; at `:00.001` the entire minute's backlog expired in one tick (crashing `/min`), while the new minute's `:00.000` spots all satisfied `Date.now() - r.s.t < 2500` simultaneously (flashing `.row.fresh` across the table for ~2.5 seconds after 57.5 seconds of zero flashes). Separately, future-stamped spots could plug `S.rate[0]` and write out-of-bounds `NaN` in the former `perMinute()` sparkline helper.
- What fixed it: Replaced the uncapped `S.rate` timestamp list with bounded 1-second arrival-time buckets keyed by browser monotonic time (`performance.now()`), retaining at most 60 buckets and clearing on session reset. Removed the `.row.fresh` animation and class from JS and CSS. Subsequently (`Approved v3`) removed the top-bar `NodeStatus` pill and `Sparkline`/`perMinute()` helper and moved the live `<rate>/m` readout into the `All` band-strip tile (`SubBar`), keeping the monotonic `spotsPerMinute()` engine intact.
- How we know: Live Playwright Edge observation on `https://ui.n2wq.com/` (`VE3EID-15`, `FN05GK`, `BAND=160M,80M,40M,20M,15M,10M AND MODE=CW AND CONF<>B,?`) confirmed `1,825/1,825` spots had `:00.000` timestamps, verified the new monotonic `/min` counter stayed smooth across `02:41:00Z` (`1407 → 1437 → 1453/min`, `60` buckets, zero disconnects/errors), and caught the `.row.fresh` burst firing exclusively at `02:40:01–02:40:02` (18 rows) and `02:41:00–02:41:02` (28 rows). Local Edge Playwright regressions in `cloudflare/gocluster-pilot/test/filters.cjs` verify arrival-time expiry, bounded buckets, 4-digit contest rate fit in the `All` tile, paused/offline states, reset, and zero `.row.fresh` animations.
- Operator/support answer: The live spots-per-minute counter (shown in the **All** tile of the band strip) now measures monotonic 60-second browser arrival rate independent of remote spot timestamps, and the `.row.fresh` flash highlight has been removed so RBN minute rollovers no longer flash the spot table. Confirm the browser loaded the updated hashed `app.*.js` and `app.*.css` assets (`asset-manifest.json`) before attributing either symptom to this fix.

## Triggering Request
- Request date: 2026-11-27
- Request summary: Owner reported that the web UI spots-per-minute counter kept climbing and crashed around ~2,000 spots/min followed by spot rows quickly flashing dark green, approved scoped fixes for `spotsPerMinute()`/`perMinute()` (`Approved v1`) and removal of `.row.fresh` (`Approved v2`).
- Request reference (chat/issue/link): Owner chat, Approved v1, and Approved v2.

## Symptoms and Impact
- What failed or looked wrong?: On a high-rate CW feed, the top-bar `/min` counter climbed throughout the minute and plunged at `:00`, accompanied by a ~2.5-second dark teal/blue-green flash wave across the visible spot rows right at `:00`. Future-stamped spots could also pin `S.rate` and break the sparkline SVG with `NaN`.
- User/operator impact: Misleading `/min` counter drops, distracting once-per-minute full-table flash waves during busy CW feeds, and unbounded `S.rate` growth when pinned by future-stamped spots.
- Scope and affected components: Browser console only (`cloudflare/gocluster-pilot/site/assets/app.*.js` and `app.*.css`). Server delivery, filtering, and backend timestamp rules were unaffected.

## Timeline
1. 2026-11-27 21:09 - Owner reported the climbing/crashing `/min` counter around ~2,000/min and quick dark-green row flash.
2. 2026-11-27 21:21 - Implemented `Approved v1` (monotonic 1-second arrival buckets in `spotsPerMinute()` and timestamp guards in `perMinute()`), deployed `app.cf5365d88d.js` to production for live review.
3. 2026-11-27 21:39 - Live 75-second Edge Playwright observation on `https://ui.n2wq.com/` (`VE3EID-15`) proved `1,825/1,825` spots had `:00.000` timestamps, confirmed the `/min` counter no longer dropped at `:00`, and captured `.row.fresh` firing exclusively at `02:40:01–02:40:02` (18 rows) and `02:41:00–02:41:02` (28 rows).
4. 2026-11-27 21:44 - Owner selected Option A and approved `Approved v2` to remove `.row.fresh` from JS and CSS.

## Hypotheses and Tests
1. Hypothesis A - Head-of-line blocking in `S.rate` from future-stamped spots (PSKReporter/RBN/peer) or `:00.000` RBN minute quantization caused abrupt drops in `spotsPerMinute()`.
   - Evidence/commands: Live capture on `ui.n2wq.com` (`VE3EID-15`) showed `1,825/1,825` spots had `s.t % 60000 === 0` (`future: 0`), confirming `:00.000` RBN minute quantization drained an entire minute's spots at `:00.001` under the old `S.rate` logic while future skew remained an additional pinning hazard; `app.cf5365d88d.js` kept `/min` steady across `02:41:00Z`.
   - Outcome: Supported
2. Hypothesis B - `perMinute()` writes out-of-bounds `NaN` into the sparkline array on future timestamps and stops early on out-of-order stale spots.
   - Evidence/commands: Inspected `app.6e03cd4d32.js:3762-3770` and verified Edge Playwright regression renders finite SVG sparkline coordinates under future, `NaN`/`null`, and out-of-order stale timestamps.
   - Outcome: Supported
3. Hypothesis C - The quick dark-green/teal row flash across the table was `.row.fresh` firing in a synchronized `:00–:02.5` wave on RBN `:00.000` timestamps.
   - Evidence/commands: Live capture on `ui.n2wq.com` (`VE3EID-15`) recorded `46` `fresh` animations exclusively during `02:40:01–02:40:02` and `02:41:00–02:41:02`, `0` during the other 57.5 seconds of each minute, and `0` other animations; screenshot confirmed dark teal (`oklab(0.788859 -0.0841335 -0.102396 / ~0.15)`) on `0240Z / 1s` rows.
   - Outcome: Supported

## Findings
- Root cause (or best current explanation): Both `spotsPerMinute()` and `.row.fresh` compared `Date.now()` against remote spot timestamps (`s.t`), which for RBN CW spots are minute-quantized to `:00.000` (`rbn/client.go:711-713`). Every minute rollover at `:00` simultaneously expired the previous minute in `S.rate` (crashing `/min`) and made all incoming spots satisfy `Date.now() - r.s.t < 2500` for 2.5 seconds (flashing `.row.fresh` across the visible table).
- Contributing factors: Upstream feeds also admit future timestamp skew (`pskreporter/client.go`, `rbn/client.go`, `peer/spot_relay.go`), which could pin `S.rate[0]` and break `perMinute()`.
- Why this did or did not require a durable decision: No protocol, backend, config, or durable architecture contract changed; this is a localized browser console fix.

## Decision Linkage
- ADR created/updated: none
- Decision delta summary: No ADR changes required.
- Contract/behavior changes (or `No contract changes`): Live spots-per-minute counter (in the `All` band-strip tile) measures monotonic 60-second arrival rate in ≤ 60 1-second buckets; the top-bar `NodeStatus` pill and `Sparkline`/`perMinute()` helper are removed; `.row.fresh` row highlight animation is removed.

## Verification and Monitoring
- Validation steps run: `node --check` on the hashed app JS bundle, Edge Playwright browser tests (`test/filters.cjs`, sequential `test/responsive.cjs`), Worker tests (`test/worker.test.mjs`, `test/workerd.cjs`), `scripts/check-troubleshooting-records.ps1` (0 issues on `TSR-0148`), `scripts/check-support-agent.ps1`, and `git diff --check`.
- Signals to monitor (metrics/logs): Live `/m` counter in the `All` tile staying smooth across UTC `:00` rollovers with no table-wide row flashes after Wrangler deployment.
- Rollback triggers: Use the [console rollback procedure](../../cloudflare/gocluster-pilot/PRODUCTION.md#rollback) if the deployed bundle displays an inaccurate `/m` rate or layout/rendering errors.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s): none
- Related docs: [Production browser console runbook](../../cloudflare/gocluster-pilot/PRODUCTION.md), [Browser regression tests](../../cloudflare/gocluster-pilot/test/filters.cjs)
