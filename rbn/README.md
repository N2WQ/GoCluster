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

Each client has one supervisor that serializes dialing, login, reading, and
retry. A successful dial creates a connection generation that exclusively owns
its socket, buffered reader/writer, write lock, and keepalive worker. Retiring
an older generation cannot close a newer socket or clear its connected state.
Both initial and mid-stream retry use capped 5-to-60-second backoff with stable
endpoint/attempt jitter, which prevents a failed upstream from causing a tight
retry loop or synchronized reconnect storm.

The public startup methods intentionally have different first-result behavior:

- `Connect` waits for the first dial and returns its error without retrying an
  initial failure; after a successful first dial, mid-stream failures retry.
- `ConnectWithInitialRetry` waits for and returns the first dial result while
  leaving supervision active after an initial failure.
- `Start(ctx)` returns immediately and retries initial and mid-stream failures
  until its context is canceled or `Stop` is called.

`Connected` means that the current TCP generation is connected; recent lines
and accepted spots are reported separately. Login waits and default/injected
dials are cancellation-aware. `Stop` is idempotent: it cancels dialing or
backoff, closes the active socket, joins the supervisor and generation workers,
then closes the spot output channel exactly once. The optional raw passthrough
channel is caller-owned and is never closed by the client.

The console's top-level RBN source-family label is green when either the
CW/RTTY RBN feed or the digital RBN feed is connected. The detailed ingest
source rows remain per-feed: an operator can see one RBN feed green and the
other red during a partial outage.

Human/upstream telnet ingest reuses the same client, minimal parser, bounded
spot channel, and generation-safe lifecycle. The runtime creates one client
per enabled `human_telnet` entry and starts them independently with
`Start(ctx)`. A failed dial or later disconnect on one upstream therefore does
not stall any other configured upstream. These clients share one runtime-owned
raw announcement channel; shutdown stops and joins every producer before the
runtime closes that shared channel and joins its consumer.

Each enabled human entry has a distinct `HUMAN/<name>` health/dashboard label,
keeps the configured name as `SourceNode`, and otherwise enters the existing
human `UPSTREAM` parse, dedupe, and flood path unchanged.

## Parsing Shape

The parser is intentionally split into two stages.

### Stage 1: structural token walk

[`client.go`](client.go) does the structural pass:

- tokenizes the incoming line on whitespace
- supports `CALL:freq` glued forms
- finds the first plausible dial frequency
- finds the first valid DX call after that frequency
- for human telnet input, requires and removes the terminal wire timestamp
- passes the remaining unconsumed text to the shared comment parser

This stage is responsible for the left-to-right structure of a `DX de ...` line.

### Stage 2: shared comment parsing

RBN/skimmer input uses the legacy entry points in
[`../spot/comment_parser.go`](../spot/comment_parser.go), which:

- finds explicit mode tokens
- parses reports such as `+5 dB` and `-13dB`
- extracts `HHMMZ`
- returns a cleaned comment string

The shared comment parser uses an Aho-Corasick keyword scanner so the runtime can recognize mode/report/time markers consistently across inputs.

Human telnet input uses `ParseHumanSpotComment` with the same recognition
machinery. It preserves comment times, `CWT`, unrelated numbers and speed-like
text. Bare reports require an adjacent signed integer after FT8 or FT4, within
-30 through +30 and permitted by taxonomy. Explicit `dB` handling remains
available. See the [human comment contract](../spot/README.md#human-spot-comments).

### Human telnet wire time

Every human telnet spot must end with a standalone uppercase `HHMMZ`, with
hours 00 through 23 and minutes 00 through 59. Flexible spacing remains valid.
The reader removes only that final raw token before parsing the comment.
Missing or malformed times reject the spot before queue admission; it never
uses an earlier time mentioned in the comment as a fallback. Wrapped times,
fused forms such as `2100ZIO87`, lowercase `z`, and fields after the time do not
satisfy this wire convention.

For example, `QRT 1800Z 2100Z` retains `QRT 1800Z` as comment text and uses
the terminal `2100Z` for timestamp conversion. If an upstream omits its actual
timestamp and its prose ends in `1800Z`, the identical bytes cannot be
distinguished from a wire field; that terminal-time ambiguity is accepted.

Existing UTC-date assignment and future-skew conversion remain unchanged.
In particular, a lexically valid clock more than two minutes in the future
still converts to arrival time. This change does not add midnight rollover
inference or alter RBN/skimmer time parsing. See
[ADR-0348](../docs/decisions/ADR-0348-human-spot-comment-integrity.md).

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
