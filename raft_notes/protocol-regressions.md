# Protocol review and tested decisions

The implementation was reviewed around persistence ordering, quorum reasoning, stale traffic, conflict handling, restart, and independent safety evidence rather than only a happy-path leader election.

## Concrete correctness decisions

- State changes are copied, synchronously saved, then published; failed saves halt and emit no success. Tests assert durable vote/log contents before responses, and failed append neither acknowledges nor applies.
- A new term persists its reset vote before handling an RPC. One-vote-per-term and immutable committed prefixes are enforced by both protocol code and stable-store transition validation.
- Commit counts only current-term entries; an elected leader's no-op provides the necessary current-term anchor. A 40-entry fixture demonstrates that acknowledging an old-term 32-entry batch cannot advance commit, whereas acknowledging the current-term no-op can.
- Partial append commit is bounded by the actually verified prefix. A follower with a 40-entry local tail must not commit 40 entries after an empty-prefix heartbeat; a validated 32-entry append may commit only 32.
- Stale sequence failures do not undo later replication progress; duplicate votes cannot create a five-member majority.
- Conflict repair preserves committed entries and survives restart. An incoming higher term can be persisted even when a subsequent committed-prefix conflict is rejected; the committed log remains untouched.
- Higher-term rejected vote requests preserve elapsed election time. Initial exploration exposed this timer-reset liveness issue; its direct regression now prevents reintroduction.
- Invalid member/index/term/command/batch shapes are rejected before mutation. Resource exhaustion reports an error rather than silently truncating durable state.
- Real file recovery verifies checksum and schema, refuses corrupt files, and does not treat corruption as an empty new node. Storage has one owner; checksums are not authentication.

## Verification

Tests cover node protocol, actual temporary-directory FileStore restart/corruption, failure injection, concurrent memory-store readers/copy isolation, static membership refusal, deterministic simulator replay, minority safety, crash recovery, CLI reports, and eight adversarial checker histories. Run `go test -race ./...` and `go vet ./...`; exact executed counts and measured seeded results are recorded in [partition-histories.md](partition-histories.md).

The race detector checks executed code, not every possible concurrent use. Nodes intentionally have a single serialized owner; using one Node concurrently from arbitrary goroutines violates the API contract. The FileStore uses an in-process mutex but not a cross-process lock.

## Remaining limits

The scheduler explores bounded seeded executions, not all message interleavings or a formal TLA+/state-space proof. No real RPC sockets, power-loss faults, filesystem fault emulator, snapshot/read-index/membership extension, external state-machine transaction, or client linearizability history was tested. The simulator's durable memory model is explicitly distinct from the actual file backend. No production-ready, Byzantine-safe, or exactly-once claim is made.
