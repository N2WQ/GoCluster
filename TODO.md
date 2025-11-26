## TODO

- Telnet: enforce `MaxConnections` in `acceptConnections` and close/deny beyond limit; close any existing session for the same callsign before registering a new one to avoid leaked goroutines/sockets.
- Telnet broadcast: avoid per-spot shard rebuilding/allocations; maintain per-worker client lists or reuse buffers to cut GC/CPU cost with hundreds of clients.
- Dedup: current single worker with 1k in/out queues will drop under bursts; consider multiple workers, larger queues, and pooling to sustain tens of thousands of spots per minute.
- Queues: increase/configure PSKReporter processing, broadcast queues, worker queues, and dedup buffers to reduce drops at high ingest rates; expose drop metrics clearly.
- Testing: add synthetic load tests/stress harness for telnet fan-out and spot pipeline to verify throughput at target volumes.
- Long-running schedulers never stop: `startKnownCallScheduler` and `startSkewScheduler` spin forever without cancelation/shutdown, so embedding/restarts or tests will leak goroutines; add context-driven stop hooks.
- PSKReporter worker pool lacks a `Close`: `startWorkerPool` spawns workers that wait on `shutdown` but the client never exposes a close path, so reused clients can leak goroutines.
- Recorder inserts spawn a goroutine per spot: `Record` launches `insert` in its own goroutine until limits are hit; with higher limits/many modes, bursts will create many goroutines contending on one SQLite handle-consider a bounded worker pool or buffered channel.
- Grid writer silently drops updates: when the `updates` channel is full, new grid updates are dropped without metrics/logs, and pending batches are cleared even if `UpsertBatch` failed; add backpressure or accounting to avoid losing grids quietly.
- Make dedup input/output channel sizes configurable (currently hard-coded to 1000) and optionally the PSKReporter spot channel (also 1000).
