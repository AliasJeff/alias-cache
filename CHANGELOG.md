# Changelog

All notable changes to alias-cache are documented here. The project is
pre-1.0; formats and guarantees may change between minor versions (see
[storage format migration](docs/storage-format-migration.md)).

## [Unreleased]

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
- Configurability: CLI flags for cluster topology (`--peer`,
  `--bootstrap`), resource limits, and `--snapshot-policy`.
- Packaging: Dockerfile (pinned toolchain, non-root), docker-compose
  three-node cluster, CI build matrix for linux/macos/windows with
  release artifacts.
- Benchmark tool (`alias-cache-benchmark`) and a same-machine comparison
  against Redis 6.2 with an honest interpretation of the strong
  consistency cost.

### Fixed

- The decoder applied its pipeline limit across a connection lifetime,
  rejecting any session past 1024 total commands; the limit now bounds a
  single read segment.
- Non-seed cluster voters waited for self-leadership at startup and never
  enabled election, hanging bootstrap and failover in the server path.
- Follower reads surfaced raw openraft forwarding errors instead of the
  transparent leader redirect that writes already used.

## [0.1.0] - 2026-10-08

Initial public state: RESP2, deterministic state machine, WAL/recovery,
three-node raft cluster with linearizable reads, chunked snapshot
transfer with resume, replicated logical-clock TTL, CLI configuration.
