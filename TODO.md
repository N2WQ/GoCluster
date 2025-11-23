## TODO

- Telnet: enforce `MaxConnections` in `acceptConnections` and close/deny beyond limit; close any existing session for the same callsign before registering a new one to avoid leaked goroutines/sockets.
- Telnet broadcast: avoid per-spot shard rebuilding/allocations; maintain per-worker client lists or reuse buffers to cut GC/CPU cost with hundreds of clients.
- Dedup: current single worker with 1k in/out queues will drop under bursts; consider multiple workers, larger queues, and pooling to sustain tens of thousands of spots per minute.
- Queues: increase/configure PSKReporter processing, broadcast queues, worker queues, and dedup buffers to reduce drops at high ingest rates; expose drop metrics clearly.
- Testing: add synthetic load tests/stress harness for telnet fan-out and spot pipeline to verify throughput at target volumes.
