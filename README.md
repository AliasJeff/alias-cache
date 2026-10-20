# alias-cache

A Redis-compatible key-value store where your data doesn't disappear. Every write goes through Raft majority replication before it's acknowledged — kill the leader mid-write, fail over, restart nodes, and nothing you got an `OK` for is ever lost. Linearizable reads at hundreds of thousands of ops per second, on the same storage engine TiKV uses for its own WAL.

## Why

Redis is fast, but its replication is asynchronous. When a failover happens, writes that were acknowledged to clients can simply vanish. alias-cache closes that hole: every write is committed by a Raft quorum before the client sees an `OK`. You get Redis-protocol compatibility, linearizable reads, and automatic leader failover — with durability that async replication fundamentally cannot offer.

And it's fast. Not "fast for a consistent database". Fast:

| Workload | Throughput |
| --- | ---: |
| 95% reads / 5% writes | ~400,000 ops/s |
| 50% reads / 50% writes | ~172,000 ops/s |
| 5% reads / 95% writes | ~184,000 ops/s |
| Pure writes | ~207,000 ops/s |
| Pure reads (linearizable GET) | ~372,000 ops/s |

3-node cluster, 512 connections, 400k keys, every number zero-error under full Raft majority replication. Each write is durable on a quorum before it's acknowledged — a guarantee an async-replica Redis setup cannot make at any throughput.

Expiry at scale is a solved problem too: 100,000 keys expiring in the same second are reclaimed by a **single Raft log entry**, through a hierarchical timing wheel that finds due keys in O(1) amortized time — no keyspace scanning, no latency spikes, no per-key overhead.

## Quick start

```bash
cargo build --release
./target/release/alias-cache-cli --node-id 1
```

Another terminal:

```bash
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

Any Redis client works — alias-cache speaks RESP2 and RESP3.

Three-node cluster with automatic failover:

```bash
for i in 1 2 3; do
  peers=""
  for j in 1 2 3; do
    peers="$peers --peer $j=127.0.0.1:1637$j/127.0.0.1:637$j"
  done
  extra=""
  [ "$i" = "1" ] && extra="--bootstrap"
  ./target/release/alias-cache-cli --node-id $i \
    --client-addr 127.0.0.1:637$i --raft-addr 127.0.0.1:1637$i \
    --data-dir data/node-$i --snapshot-policy 65536 $peers $extra &
done
```

Writes to any node are forwarded to the leader and committed by majority. Reads from any node are linearizable. Kill the leader — the survivors elect a new one in under a second and keep serving. `SENTINEL GET-MASTER-ADDR-BY-NAME` discovers the current leader, so Sentinel-based clients work out of the box.

Or with Docker:

```bash
docker compose up --build --detach
```

## What you get

- **Durability that survives failover** — every acknowledged write is on a Raft quorum. Leader death, network partitions, node restarts: nothing acknowledged is lost.
- **Linearizable reads at scale** — lease-based reads serve straight from the leader's state machine at ~372,000 GETs/s, falling back to ReadIndex with fail-closed semantics under partitions.
- **Full RESP2/RESP3 protocol** — pipelines, inline commands, `HELLO` negotiation. Standard Redis clients connect without modification.
- **Rich data types** — Strings with the full SET option matrix, Hash, List, Set, Sorted Set, Bitmap (including BITOP), Streams, and Bloom filters with deterministic cross-replica hashing.
- **Transactions without races** — MULTI/EXEC with WATCH optimistic locking; the CAS check and the writes commit in the same log entry, so there is no check-then-write window on any replica.
- **Lua scripting** — EVAL runs on the leader with a sandboxed interpreter (memory cap, instruction budget), and only the deterministic result is replicated.
- **TTL done right** — a hierarchical timing wheel tracks every TTL'd key with O(1) amortized discovery; mass expiration is reclaimed in one log entry, and WAL fsync stays flat at ~0.3ms even as the log grows to gigabytes.
- **Fast crash recovery** — incremental snapshots (base + delta segments) bring a restarted node back in seconds, not minutes.
- **Security & operations** — password auth, client TLS, mTLS for replication traffic, max-memory and max-connection limits, five-section `INFO`, Sentinel-style discovery.
- **Idempotent writes** — client request IDs deduplicate retried commands at the state machine.

## How it stays fast

- **TiKV raft-engine as the WAL** — segmented log with group-commit fsync; write throughput doesn't decay as the log grows.
- **Pipelined replication** — each replication stream is one persistent connection with bounded in-flight AppendEntries and in-order streaming responses, instead of the default send-one-wait-one round trips.
- **Group commit with idle flush** — the leader merges every connection's pipeline into batched commits; when write bursts go quiet, the group flushes early so reads behind them aren't stalled.
- **Timing wheel expiry** — see above. Mass TTL churn costs one consensus entry, not one per key.
- **Incremental snapshots** — snapshot builds are byte concatenation over a durable base plus delta appends, rate-limited off the hot path.

## Verify it yourself

```bash
cargo test --workspace --locked      # 181 tests, including OpenRaft's full storage contract suite
python3 scripts/smoke_cluster.py     # real 3-node failover: kill the leader, watch recovery

cargo build --release
./target/release/alias-cache-benchmark \
  --address 127.0.0.1:9401 --mode pwrite \
  --clients 512 --pipeline 16 --key-space 400000 \
  --value-size 16 --time 30 --write-percent 50
```

Every number in this README comes from that benchmark tool, on the cluster sizes and conditions stated. Run it on your own hardware.

## Building

Rust 1.88 (edition 2024). `cargo build --release --locked` for a reproducible build. Linux, macOS, and Windows CI matrices; Docker images via `Dockerfile` and `docker-compose.yml`.

## License

Apache-2.0
