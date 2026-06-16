# RBN Ingest

This directory owns telnet ingest from the Reverse Beacon Network feeds and other DX-cluster-style line feeds that reuse the same parser shape.

## Feed Types

- RBN CW/RTTY feed
- RBN digital feed
- optional minimal-parser telnet feeds for human or upstream cluster input

## Connection Lifecycle

Enabled production RBN feeds keep retrying after a startup dial or DNS failure.
The first error is still logged and recorded as an ingest connection failure,
but the client remains owned by the runtime, its health row stays visible, and
bounded background reconnect attempts continue until shutdown.

The console's top-level RBN source-family label is green when either the
CW/RTTY RBN feed or the digital RBN feed is connected. The detailed ingest
source rows remain per-feed: an operator can see one RBN feed green and the
other red during a partial outage.

## Parsing Shape

The parser is intentionally split into two stages.

### Stage 1: structural token walk

[`client.go`](client.go) does the structural pass:

- tokenizes the incoming line on whitespace
- supports `CALL:freq` glued forms
- finds the first plausible dial frequency
- finds the first valid DX call after that frequency
- passes the remaining unconsumed text to the shared comment parser

This stage is responsible for the left-to-right structure of a `DX de ...` line.

### Stage 2: shared comment parsing

The remainder goes to [`../spot/comment_parser.go`](../spot/comment_parser.go), which:

- finds explicit mode tokens
- parses reports such as `+5 dB` and `-13dB`
- extracts `HHMMZ`
- returns a cleaned comment string

The shared comment parser uses an Aho-Corasick keyword scanner so the runtime can recognize mode/report/time markers consistently across inputs.

## Ingest Rules

Important operator-visible ingest behavior:

- RBN and RBN-digital are explicit-mode skimmer feeds
- RBN's spot-class field is separate from RF mode:
  - `mode` / live class token: `CQ`, `DX`, `BEACON`, `NCDXF B`
  - `tx_mode` / explicit mode token: RF transmission mode such as `CW` or `RTTY`
- only spot classes `CQ`, `BEACON`, and `NCDXF B` are ingested
- spot class `DX`, blank class, and unknown class values are dropped before ingest
- `BEACON` and `NCDXF B` are ingested with source-class beacon state and
  `IsBeacon=true`
- blank comments on generic `BEACON` spots display and archive as `BEACON`
- blank comments on `NCDXF B` spots display and archive as `NCDXF BEACON`
- peer forwarding keeps the original blank comment
- missing mode tokens on those feeds are rejected before ingest
- zero-SNR skimmer spots are dropped before ingest
- per-spotter skew corrections are applied before later normalization stages

The parser does not own final mode policy, dedupe, confidence, or peer handling.
It produces canonical spots for the downstream pipeline, with `Spot.Mode`
reserved for RF/transmission mode rather than RBN spot class.

For the operator-facing overview, see [`../README.md`](../README.md).
