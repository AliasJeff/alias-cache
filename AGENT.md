# alias-cache Development Guide

## Project Intent

alias-cache is a Rust project for a Redis-compatible, strongly consistent, Raft-based in-memory key-value store.

The primary engineering goals are:

- Deterministic replicated state-machine execution.
- Correct WAL, snapshot, restart, and failover behavior.
- Clear protocol semantics for RESP2 and RESP3.
- Small, composable, testable modules.
- Measurable performance without compromising correctness.

## Language and Tooling

- Use stable Rust unless a documented feature requires a newer toolchain.
- Use `rustfmt` and keep formatting checks clean.
- Treat `clippy` warnings as actionable.
- Prefer established crates over custom implementations of consensus, TLS, parsing, or serialization.
- Pin or carefully review dependencies that affect storage and wire compatibility.

## Code Quality

- Keep modules focused and APIs narrow.
- Avoid speculative abstractions and duplicated helper logic.
- Prefer explicit state transitions over hidden side effects.
- Return structured errors at subsystem boundaries.
- Do not use `unwrap`, `expect`, or `panic!` on network, disk, protocol, or user-controlled input paths.
- Use bounded buffers, bounded queues, and explicit request limits.
- Make ownership, locking, and task lifetimes easy to inspect.
- Avoid holding synchronous locks across `.await`.
- Do not optimize a hot path without a benchmark or profile that explains the change.

## Comments and Documentation

- All source-code comments must be written in English.
- Comments should explain invariants, protocol constraints, recovery ordering, or non-obvious trade-offs.
- Do not add comments that merely restate the code.
- Public Rust APIs require concise rustdoc when their behavior or safety contract is not obvious.
- User-facing project documentation is written in Chinese unless a plan explicitly calls for an English version.

## Protocol Rules

- Preserve Redis argument parsing, error behavior, response shape, and binary safety.
- Keep protocol parsing separate from command execution.
- Every command must declare whether it is a read, write, multi-key operation, blocking operation, or transaction operation.
- Replicated commands must be deterministic on every node.
- Never use local wall-clock time as a replicated result without converting it into an explicit replicated value.
- A command must not mutate local state before the Raft log entry is committed.

## Consensus and State Machine Rules

- Raft is the source of truth for committed writes.
- Reads must declare and test their consistency policy.
- State-machine application must be replayable from the log.
- Snapshot installation must be atomic from the perspective of readers.
- WAL and snapshot replacement must define crash behavior for every intermediate step.
- Membership changes must be tested with odd-sized clusters and failure cases.

## Testing Requirements

Every feature should include the smallest useful test at the same time as the implementation.

Required layers:

- Unit tests for parsers, data structures, and state transitions.
- Protocol tests for exact RESP2/RESP3 replies.
- State-machine replay tests.
- Integration tests for real TCP connections.
- Multi-node tests for election, replication, recovery, and failover.
- Fault-injection tests for process crashes, delayed messages, dropped messages, and disk failures where practical.

Do not mark a distributed feature complete based only on a happy-path single-node test.

## Git and Change Discipline

- Keep commits focused and explain the invariant or behavior changed.
- Do not mix formatting-only changes with functional changes.
- Update relevant documentation and plans with behavior changes.
- Do not silently change wire format, on-disk format, or compatibility guarantees.
- Record known limitations instead of hiding them behind feature flags or broad error messages.

## Definition of Done

A feature is complete only when:

1. Its behavior and scope are documented.
2. Its implementation has focused tests.
3. Failure behavior is defined.
4. `cargo fmt --check` passes.
5. `cargo test --workspace` passes for the relevant scope.
6. `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes, or documented warnings are intentionally addressed.
7. The corresponding plan document has an evidence-based completion note.
