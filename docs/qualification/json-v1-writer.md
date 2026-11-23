# JSON v1 live writer qualification

Measured November 23, 2026 on an Intel Core i9-10900 using Go 1.27.1, Linux amd64 on Ubuntu 24.04 under WSL2, kernel 6.6.87.2. Both binaries used `GOMAXPROCS=2` and CPU affinity 4,5. The baseline is commit `73bdaabd`. The candidate contains the JSON v1 writer.

## Fixture and measurement

The fixture cycles through seven already-normalized spots with equal weight. It models the public field combinations each source can produce. The Go fixture sets the source, station metadata, observed frequency, event bits, confidence and CW-speed data explicitly. Each timed admission creates a new envelope, stamped immediately before enqueue. The fixture enables the production latency trackers and checks that the writer observed exactly one first byte per received spot. The tests use the repository's built-in taxonomy, CW-speed display on, and path display off. Separate path tests exercise two recipients, a queued grid change and parity with the text glyph.

| Source | Fields exercised | Record bytes including LF |
| --- | --- | ---: |
| Manual | USB, POTA, accented comment | 288 |
| RBN | CW, two speeds, cleaned comment, confidence | 271 |
| FT8 | Negative report and distinct observed frequency | 270 |
| FT4 | Positive report | 263 |
| PSKReporter | PSK31, two eight-character source grids | 270 |
| Human upstream | LSB, quotes and backslash | 298 |
| Peer | CW, two events, human and node origin | 274 |

The seven exact wire records occupy 1,934 bytes, averaging 276.29 bytes per record. The socket benchmark uses the real connection writer, the session's ON switch and a loopback TCP reader. It configures a 256-entry spot queue, eight control slots, a 16,384-byte batch and a 5 ms maximum batch wait. Measurement starts after the client reads the acknowledgement, hello and both schemas. The timed loop ends when the peer has matched every record, in order, against an independently specified byte fixture. The benchmark separately checks the total socket bytes. Socket write sizes measure application calls to `net.Conn.Write`. TCP segmentation and receiving-side read sizes can differ.

The workload covers one continuously supplied client. Production source proportions, arrival rates, slow WAN clients, filtering, path prediction and concurrent history scans require separate measurements. The timed loop shares the seven normalized spots across independently timestamped admissions. The retained-heap test also clones the underlying spots.

See the [source cases and TCP fixture](../../telnet/json_spot_writer_test.go), [wire fixtures](../../telnet/testdata/json-v1-writer.ndjson) and [projection, path and archive tests](../../telnet/json_spot_test.go).

## JSON throughput, writes and CPU

The measured Linux amd64 binary includes the 64-bit pause generation in each broadcast envelope. Its DWARF type records a 48-byte structure. The socket benchmark measured a single 48-byte allocation per admitted spot in every run, matching Go's 48-byte small-object size class.

While concurrent Go compilation, linking and tests were active on the host, five 70,000-row runs took a median of 4,760 ns/spot from admission through verified TCP drain (3,537–8,298). All 350,000 rows, first-byte observations and expected bytes matched. This run also observed 48 B/op and one allocation per admission.

A second series ran after compilation pressure eased. Other Go tests and a linker were still active. Five 70,000-row runs delivered 350,000 verified spots and 96,700,000 bytes. The isolated warm encoder took a median of 1,200 ns/spot (1,055–1,247) and allocated zero bytes per spot. Admission through verified TCP drain took a median of 1,843 ns/spot (1,753–1,928), about 543,000 spots/s in this workload. The allocation profile attributed 65.50 MiB of sampled allocations to the timestamped-envelope producer line in the 1.4-million-row run. The encoder also has a zero-allocation regression test.

| Bytes in one socket write | Calls across five runs |
| --- | ---: |
| 1–511 | 1 |
| 512–2,047 | 0 |
| 2,048–8,191 | 217 |
| 8,192–16,383 | 5,855 |
| 16,384 and above | 0 |

Of 6,073 socket writes, 96.4% contained at least 8 KiB. The smallest write was 270 bytes and the largest was 16,332. The writer starts a fresh batch when an ordinary record fits into an empty batch but exceeds the remaining space.

A separate CPU-profiled run delivered 1,400,000 verified records and 386,800,000 bytes in 2.61 seconds of profile wall time. It sampled 3.57 CPU-seconds across the sending and receiving sides. The writer loop accounted for 71.7% cumulative CPU, of which the live spot encoder accounted for 58.0 percentage points. System calls contributed 12.3% flat, standard-library JSON quoting 7.6% flat and runtime channel selection 5.3% flat. The measured benchmark timer averaged 1,853 ns/spot. Cumulative profiles include their child frames.

## Retained Go heap

The heap fixture samples process-wide `HeapAlloc` after forced GC at each checkpoint. It negotiates JSON, gates the first socket write, fills all 256 remaining queue slots with distinct cloned spot objects, verifies all 257 records after release, and closes both sockets. Five cycles gave these deltas.

| Checkpoint difference | Median bytes | Range in bytes |
| --- | ---: | ---: |
| Connected idle minus preconnection | 120,184 | 120,072–121,400 |
| Full queue minus connected idle | 279,424 | 279,424–285,736 |
| Drained minus connected idle | 248 | 248–6,560 |
| Closed minus preconnection | 1,152 | 904–8,792 |

The largest sampled process heap was 16,679,096 bytes. These process snapshots include the reader, writer, fixture server's three latency rings and Go runtime. The full-queue delta reflects the clone-per-slot workload, while normal broadcast can share spots across clients. Sampling after GC does not measure transient peak allocations, kernel socket memory, total RSS or a per-client production ceiling.

## Warm plain text and CC11

The existing writer-drain benchmarks validated each output byte and each count. Each cell below summarizes ten 250,000-row samples. The two binaries ran one at a time, alternating which went first each round. Values are medians. The final column gives the Mann–Whitney p-value reported by benchstat.

| Writer | Baseline ns/spot | JSON-capable build ns/spot | Allocations and bytes per spot in both | p |
| --- | ---: | ---: | --- | ---: |
| Plain text | 765.9 | 747.6 | 1 allocation, 80 bytes | .853 |
| Text with source diagnostic | 1,238 | 1,324 | 5 allocations, 256 bytes | .315 |
| Text fixture with nil server | 718.1 | 690.5 | 1 allocation, 80 bytes | .912 |
| Native CC11 | 1,711 | 1,748 | 0 allocations, 0 bytes | .481 |

All 20 million expected rows drained across the two binaries and four cases. Allocation counts matched in all samples. Timing differences fell within run-to-run variability. The 95% median intervals reported by benchstat ranged from ±13% to ±48%, limiting the size of timing regressions this comparison can rule out.

## Reproduction

From a Linux checkout with Go 1.27.1, run the following commands. CPU IDs 4 and 5 were available on the measured host. Set the pair to available cores when reproducing elsewhere. Run the pre-existing text and CC11 cases in both checkouts, alternating processes in the order described above. Compare their complete Go benchmark outputs with `benchstat baseline.txt candidate.txt`.

```sh
out=$(mktemp -d)
go test -c -o "$out/telnet.test" ./telnet
cd telnet
export GOMAXPROCS=2
taskset -c 4,5 "$out/telnet.test" -test.run '^TestJSONSpotSocketRetained$' -test.v -test.count=5
taskset -c 4,5 "$out/telnet.test" -test.run '^$' -test.bench '^BenchmarkJSONSpot(MixedSources|SocketMixed)$' -test.benchtime=70000x -test.count=5
taskset -c 4,5 "$out/telnet.test" -test.run '^$' -test.bench '^BenchmarkJSONSpotSocketMixed$' -test.benchtime=1400000x -test.cpuprofile "$out/writer.cpu" -test.memprofile "$out/writer.end"
go tool pprof -top -cum "$out/telnet.test" "$out/writer.cpu"
taskset -c 4,5 "$out/telnet.test" -test.run '^$' -test.bench '^(BenchmarkVE7CCWriterSpotDrain|BenchmarkWriterV15SpotDrain)$' -test.benchtime=250000x
```
