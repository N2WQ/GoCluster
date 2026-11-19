# Call Correction v2: v9 Integration Validation

Authority: exact `Approved v9`; [scope](call-correction-v2-scope-ledger-v9.md).
Integration starts at main `6a072b7b` and incorporates correction `9523dbbc`.
The original thirty-commit correction history is retained on its source branch;
main receives a single linear-history squash integration.

Status: integration and all selected local validation passed. Publication is
authorized by v9; final local/remote commit IDs and runtime-file preservation
are verified separately in the publication receipt and owner closeout.

## Approved Work And Evidence

| Work | Implementation and distinguishing evidence |
| --- | --- |
| Preserve both source histories | Separate read-only comparison verified the parent source contributions at reconciliation. Shared Go changes retain main's fsutil path identity, CustomSCP eviction statistics and reputation shutdown alongside correction routing. The only later Go edit is the replay entry comment described below. Frozen model/evidence fixtures remain byte-identical and LF-only. |
| Reconcile records | Main ADR-0358/0359 and TSR-0137 retained; correction records become ADR-0360/0361 and TSR-0139. Both indexes retain their records. Troubleshooting checker and numeric filename uniqueness checks pass. No source algorithm changes. |
| Preserve inactive methods | Shipped C=false, legacy Bayes=false with empty cell mask; B remains replay-only. Source and configuration inspected. |
| Verify combined state | Native ordinary and race each pass 15,367 cases with 100 skips across 76 packages (62 executed, 14 build-only). Connected CustomSCP ordinary/race, vet, Staticcheck and lint pass. |
| Preserve primary/publish safely | Primary checkout remains separate through validation. Baseline hashes recorded for its two dirty runtime files. Publication requires expected refs, fast-forward-only main, ordinary non-force pushes and verified local/remote tips. |

The scope challenge and source review used a separate read-only agent. The lead
verified material findings and owns the final checks and claims. This is not new
independent RF evidence. The principal false-green risks were dropping main's
unrelated behavior and missing connected persisted-store consumption; explicit
source comparison and the connected runner address those respective risks.

## Checks And Dispositions

- Go 1.27.1 Windows/amd64, Staticcheck 2026.2.1 and golangci-lint 2.14.0.
- `go vet ./...`, `staticcheck ./...`, and configured golangci-lint: passed.
- Native Windows full preparation: passed, 76 packages.
- Full native ordinary lane: passed without result caching. The additional skip
  compared with v8 is main's Unix-only symlink traversal test; all prior skip
  identities are unchanged. It is not a missing-data skip.
- Connected ordinary CustomSCP: passed; producer persistence reached the
  independent consumer, which verified exact TCP delivery and held control.
- Full native race lane and connected CustomSCP race: passed. Both connected
  runs independently exercise persisted-store handoff and actual TCP delivery.
- Full race uses a bounded 15-minute package timeout for the integrated suite;
  individual behavioral deadlines and performance acceptance rules are unchanged.
- YAML headers: passed, 20 files, no new Boolean-comment warning.
- Crawler-entry check initially found a missing entry in replay `history.go`.
  A role/ownership comment was added; all 66 selected files then passed. This
  changes no runtime behavior or model semantics. Replay-package vet,
  Staticcheck and lint were rerun after the comment and passed.
- Troubleshooting-record index/status/RCA/ADR check: passed.
- Both code maps were generated and checked fresh; refreshed again after final
  documentation as the last repository write in validation. Final diff checked
  against approved scope and both parent histories. The fresh lead-owned pass
  found no remaining material integration defect; it is not independent review.
- Logs and before/after publication receipts belong to ignored `.tmp/v9/`.

Support-agent documentation impact: Required; merged source-map, confidence,
configuration, domain and replay guidance retain the approved method boundaries.
No new durable algorithm decision or troubleshooting incident is introduced by
the mechanical integration. Record identifiers were normalized for main.

Source publication does not deploy or restart a service. C/B development
objectives/reference guards failed; the earlier CPU qualification remains failed,
deferred and unwaived. No model was fitted/installed, no production activation
was performed, and no November 17+ archive contents/names/hashes were opened.
