# Changelog

All notable changes to alias-cache are documented here. The project is
pre-1.0; formats and guarantees may change between minor versions.

## [0.2.0] - 2026-10-20

### Added

- RESP3: `HELLO 2/3` negotiation and full wire types (Map, Boolean, Double,
  VerbatimString, Push, BigNumber), RESP2 fallback per command.
- Security: `AUTH`/`HELLO AUTH` password authentication (`--password` /
  `ALIAS_CACHE_PASSWORD`), TLS for client connections, mutual TLS for raft
  replication.
- Data structures: full SET option matrix (`NX`/`XX`/`KEEPTTL`/`GET`/
  `EX`/`EXAT`/`PX`/`PXAT`), `INCRBY` with i64 saturation, `HINCRBY`,
  `ZINCRBY` (f64 scores stored as deterministic bit patterns), Hash, List,
  Set, Sorted Set families.
- Iteration: `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` with deterministic decimal
  cursors over the ordered store.
- Transactions: `MULTI`/`EXEC`/`DISCARD` as one raft log entry, plus
  `WATCH`/`UNWATCH` optimistic locking with in-log CAS: per-key versions
  live in the replicated state machine and the check commits atomically
  with the writes, so there is no check-vs-commit race on any replica.
- Observability: `INFO health|server|replication|cluster` including
  four-level health (alive / raft_recovered / readable / writable) and
  per-follower replication lag; read-only Sentinel-style discovery.
- Resource limits: `--max-memory` OOM gate at prepare time (Redis error
  text), `--max-connections` accept-loop cap with the Redis rejection
  reply.
- Performance: group commit for raft appends (one fsync per batch),
  pipeline writes submitted concurrently with in-order replies —
  three-node SET 461→2352 ops/s, linearizable GET 4.4k→17.7k ops/s on
  the reference machine.
- Bounded client batch RPC with per-command deduplication and apply-time
  evaluation, ordered RESP pipeline batching, and a streaming `pwrite`
  benchmark mode with per-request latency samples.
- Read-only `CONFIG GET save|appendonly|*` compatibility for Redis benchmark
  clients; configuration mutations remain unsupported.
- Storage format version 4 for the replicated batch operation. Existing
  version 3 data is rejected and requires the documented 0.x rebuild procedure.
- Configurability: CLI flags for cluster topology (`--peer`,
  `--bootstrap`), resource limits, and `--snapshot-policy`.
- Packaging: Dockerfile (pinned toolchain, non-root), docker-compose
  three-node cluster, CI build matrix for linux/macos/windows with
  release artifacts.
- Validation: repeatable local and Docker Compose three-node smoke tests cover
  leader failover, post-failover writes, and recovery of the restarted node.
- Storage recovery: partial Raft log tails are truncated on reopen; log
  truncate/purge uses fsync plus atomic replacement, with injected sync-failure
  tests covering both pre-publication rollback and post-rename directory-sync
  failure state consistency.
- Benchmark tool (`alias-cache-benchmark`) and a same-machine comparison
  against Redis 6.2 with an honest interpretation of the strong
  consistency cost.

### Performance

Same-machine reference parity (a Rust/Raft store built on the same
TiKV raft-engine log store), measured on the 3-node 512-connection ×
pipeline-16 harness, all zero-error:

| Workload | Throughput | vs reference |
| --- | ---: | ---: |
| 95% read / 5% write | ~402k ops/s | ~204% |
| 5% read / 95% write | ~184k ops/s | ~120% |
| 50% read / 50% write | ~171-177k ops/s | ~112-116% |
| pure write | ~192-207k ops/s | ~104-112% |

The changes behind it, each validated with same-machine same-window
A/Bs (cross-window baselines on a shared laptop swing ±35% and only
prove direction):

- `RaftEngineLogStore`: TiKV raft-engine replaces the hand-rolled
  append-only log store. FileLogStore's fsync cost grew linearly with
  the WAL file (2.3 ms average at 700 MB, decaying write throughput
  over a run's life); raft-engine's segmented log holds ~0.3 ms flat at
  multi-GB WAL sizes, removing time-dependent throughput decay.
- Pipelined raft replication: `TcpNetwork` overrides openraft's
  sequential send-one/wait-one replication stream with one persistent
  connection per replication stream, bounded in-flight AppendEntries
  (depth 4), and in-order response streaming — per-RPC latency 1.67 ms
  → 0.67 ms, ~3.6× the RPC rate, pure write +14.5%.
- Idle-flush adaptive commit window: the leader's coalescing group
  commits when the queue is quiet for 4 ms, in addition to the
  entry-count target and the 20 ms deadline. Dense arrivals keep the
  old batching; sparse write bursts (a mixed pipeline alternates write
  bursts with reads that must wait for them) stop paying the full
  window — 50/50 throughput 74k → 171-177k ops/s, p50 latency
  326-441 ms → ~138 ms.
- State-machine apply path: per-entry versions merged into the value
  entry and the idempotency cache moved from an ordered map to a hash
  map with an insertion-order eviction deque (apply micro-benchmark
  0.67 → 0.55 µs/entry); client-batch assembly computes wire sizes in
  closed form instead of full serde walks.
- Observability for the above: `INFO stats` gained commit-round phase
  timers (queue wait / prepare / raft commit / respond), replication
  RPC round-trip and over-budget counters, follower append-handler
  latency, and the raft apply lag; the benchmark reports GET/SET
  latency percentiles separately for mixed-ratio runs.

### Fixed

- The decoder applied its pipeline limit across a connection lifetime,
  rejecting any session past 1024 total commands; the limit now bounds a
  single read segment.
- Non-seed cluster voters waited for self-leadership at startup and never
  enabled election, hanging bootstrap and failover in the server path.
- Follower reads surfaced raw openraft forwarding errors instead of the
  transparent leader redirect that writes already used.
- CLI startup errors from the server task were not observed by the process,
  which could leave a failed listener looking like a live server.
- Docker Compose used hostnames where the CLI requires socket addresses, and
  the image's non-root user could not initialize its mounted data directory.

## [0.1.0] - 2026-10-08

Initial public state: RESP2, deterministic state machine, WAL/recovery,
three-node raft cluster with linearizable reads, chunked snapshot
transfer with resume, replicated logical-clock TTL, CLI configuration.
