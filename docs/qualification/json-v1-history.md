# JSON v1 archive history qualification

JSON history admits two concurrent collecting archive scans per process. A conservative 64-bit candidate-work allowance of 128 MiB per admitted scan gives a combined allowance of 256 MiB. Independently, all connections share 160 MiB for retained row projections. A slow socket holds its selected rows and releases its archive iterator before the writer sends `hb`. A saved continuation holds a query, a position and a token. [Archive reader](../../archive/history.go) · [Connection history](../../telnet/history.go)

## Archive and candidate bounds

The accepted native layouts bound each ordinary variable-length field with an unsigned 16-bit length. The two optional state fields each take at most two bytes. Each of the two optional CW fields takes at most 1,024 bytes. A valid CW vector can contain up to 170 labels. V7 therefore admits at most 78 header bytes, 17 ordinary fields of up to 65,535 bytes, 4 state bytes and 2,048 CW bytes. The reader checks the Pebble lazy value's advertised length before requesting its value. Invalid lengths or malformed payloads follow the existing unreadable-record path. [Record layouts and decoding](../../archive/archive.go) · [Boundary test](../../archive/history_collect_test.go)

| Native version | Maximum valid value in bytes |
| --- | ---: |
| 2 | 852,009 |
| 3 | 852,017 |
| 4 | 917,554 |
| 5 | 1,114,165 |
| 6 | 1,114,173 |
| 7 | 1,116,225 |

Each admitted scan decodes and matches one candidate at a time. These allowances cover simultaneously reachable GoCluster-owned candidate work on Go 1.27.1 amd64.

| Candidate storage | Conservative allowance and derivation |
| --- | --- |
| Value and decoded strings | Under 3 MiB for one admitted raw value and the 21 copied fields, including allocation rounding. |
| Comment, event, toxicity and CW processing | Under 4 MiB for split-field and compacted string-header vectors. A 65,535-byte legacy comment can produce at most 32,768 one-byte tokens separated by whitespace. At 80 bytes per token the token vector takes 2.5 MiB. Uppercased token backings add under 3 MiB in aggregate. |
| Normalization and matching | Ten normalized spot strings and fewer than 24 text slots in the filter view. An allowance of 64 full-field derived backings covers those slots plus concurrent builder generations and temporary lookups. Each native field starts at 65,535 bytes or less. Four output bytes per input byte, builder growth and allocator rounding give a conservative bound of `16 × input bytes + 32` per backing, below 1,049,000 bytes per full field. Canonical taxonomy values share their immutable backing. |
| CTY and projection | Under 5 MiB for slash-segment lookups. CTY supplemental results copy fixed headers. Optional country and node text are length-checked before copying. One candidate awaiting reservation uses under 1 MiB of projection storage. |
| Remaining candidate work | Boolean programs admit up to 128 nodes, the literal-comment matcher has a fixed 1 KiB table, and path observations use fixed receiver slots. The 128 MiB allowance includes slack above the listed working sets. |

The preexisting detached filter snapshot belongs to the command and is captured once per request. The handler drops it before writer publication. Shared CTY, taxonomy, normalization and path-service caches, Pebble blocks, decompression and iterator-internal storage, per-connection command state, goroutine stacks, allocator arenas and memory awaiting garbage collection have separate lifetimes. The allowances above describe GoCluster candidate backing. Process RSS also includes those other resources. [Matcher snapshot](../../telnet/history_filter.go) · [Comment parser](../../spot/comment_parser.go) · [Archive admission](../../archive/history.go)

An offline linux/amd64 benchmark used a 1,116,225-byte valid V7 value with 32,768 legacy tokens, comma-heavy fields, case-normalization expansion and 170 CW labels. Two six-iteration runs on an Intel Core i9-10900 with Go 1.27.1 allocated 12,209,446 and 12,092,472 bytes per decode. These figures measure cumulative allocation per operation. The simultaneous-memory allowance comes from the structural bounds above. [Benchmark and independent native fixtures](../../archive/history_collect_test.go)

## Retained pages and socket writers

Each accepted row copies all public strings and CW labels into its own backing and charges the capacity the Go byte allocator supplied. Its row header and CW string-header array each carry a conservative small-allocation charge rounded up to the next power of two including a pointer-sized allocator header. The first row additionally charges the delivery descriptor, prospective cursor header, and 512 bytes for its completion channel, copied position, token and allocator rounding. A declined row owns no reservation. Its uncommitted allocation belongs to the admitted scan. [Row accounting](../../telnet/json_history_rows.go)

Empty pages still need a small response descriptor and channel to report a full budget. The per-connection history lock allows one outstanding JSON history command per session and the existing control queue bounds its entry. The fixed socket batch and one 6,146-byte quoting buffer belong to the connection writer. History adds a 160-byte local control-record buffer. Long rows stream through the same writer buffers. The selected-row budget is independent of these fixed per-connection costs and the separate YAML budget. [History writer](../../telnet/json_history_stream.go) · [Socket writer qualification](json-v1-writer.md)

| Offline check | Observed result on linux/amd64 |
| --- | --- |
| Maximum page | 250 rows at independently derived projection upper bounds consumed 150,656,896 of 167,772,160 charged bytes. The test verified each row and returned the reservation. |
| Shared capacity and reconnects | Sixteen socket writers simultaneously held two-row pages for a total of 31,488 charged bytes. Three waves alternated completed pages and disconnects during delivery. Each close released its expected share and every wave returned to baseline. |
| Admission and retry | Two blocked collecting scans on one archive made a third collecting request on another return busy. Ordinary text history still completed. A partially reserved page that exhausted the row budget produced zero rows and a retryable continuation. |
| Ordering and failure | Independent binary V2–V7 fixtures reopened through Pebble reached the socket in text display order. Tests exercised a requested count of 50 with 7 matches, 250 rows with live delivery before the last history row, early NEXT, failed footer flush, stale generation, queue drain, and duplicate-login terminal delivery. |
| NEARBY | The same spots reached live fan-out, text history and JSON history with matching membership. Cases cover DX, DE, both or neither endpoint, missing user and station grids, coarse-only matches on each side at 160, 80 and 60 metres, fine resolution on 20 metres, and the own-spot exception. Equal timestamps checked sequence order after reopen. |

The fixture, resource and lifecycle checks use temporary offline archives and test connections. [Archive tests](../../archive/history_collect_test.go) · [Maximum page and partial refusal](../../telnet/json_history_rows_test.go) · [Streaming, filtering and reconnect checks](../../telnet/json_history_stream_test.go)
