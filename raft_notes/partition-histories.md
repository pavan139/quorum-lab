# Executed experiments and practical limits

## Toolchain and commands actually executed

The implementation declares Go 1.23.0 and uses only the standard library. Verification ran on Linux with the actual Go 1.25.1 SDK at `/workspace/ai-adjacent-workbench/tools/go/bin/go`. Compiler concurrency was bounded with `GOFLAGS=-p=2 GOMAXPROCS=2`; no dependency/model download, provider call, or training was performed.

```bash
GOFLAGS=-p=2 GOMAXPROCS=2 go test ./...
GOFLAGS=-p=2 GOMAXPROCS=2 go test -race -count=1 ./...
GOFLAGS=-p=2 GOMAXPROCS=2 go vet ./...
gofmt -l raft simulator cmd
go run ./cmd/quorumlab demo --output artifacts/demo.json
go run ./cmd/quorumlab evaluate --output artifacts/evaluation.json
go build -o artifacts/quorumlab ./cmd/quorumlab
./artifacts/quorumlab evaluate --output artifacts/binary-evaluation.json
```

The SDK path above was used for execution; the portable commands assume that actual SDK is on PATH. The built binary and `go run` produced exactly equal evaluation JSON. `gofmt -l` returned no files. Standard tests, race tests, and vet passed.

## Tests

There are **29 top-level tests**, plus **eight adversarial safety-checker subcases**. They exercise durable voting/election, before-response persistence, current-term commit through partial batches, suffix repair, committed-entry protection, follower commit bounds, stale responses, duplicate votes, higher-term step-down, timer rejection semantics, malformed input, log/term capacity, actual-file restart/corruption, storage rollback refusal, concurrent store readers/copy isolation, immutable membership, simulator bounds, CLI failures, minority safety, and deterministic replay.

The temporary-directory FileStore tests actually execute file sync, rename, directory sync, recovery through a new Store/Node instance, permission checks, and corrupt/truncated-file rejection. They do not simulate machine power loss or prove a filesystem's physical durability. The failure-injection store verifies that a failed vote/log write produces no success message or applied entry.

## Frozen seeded exploration

Dataset: `raft-partition-restart-v1`.

Dataset SHA-256: `bf7768d671f751a2fc34c6c95204acd49d13f9a2f3a2cfb9e247f44566cde8cb`. The simulator hashes its compact JSON manifest encoding; whitespace in the human-readable manifest file is not part of that digest.

The exact input manifest is preserved in [../schedules/exploration.json](../schedules/exploration.json) and the [measured summary](../histories/evaluation-summary.json). Seeds 1–12 are used for both static cluster sizes. Each 360-tick schedule contains 40 stable ticks, 15 pre-write ticks, 120 partition ticks, 25 majority-write ticks, 10 crashed-leader ticks, 120 healing ticks, and 30 final-write ticks. Reordering and the stated phase-specific loss percentages are seeded. Randomized election timers are also seed-derived.

| Executed measure | Result |
| --- | ---: |
| Three-member scenarios passed | 12/12 |
| Five-member scenarios passed | 12/12 |
| Total complete scenarios passed | 24/24 |
| Independent full-state safety observations | 52,780 |
| Messages delivered | 18,244 |
| Messages dropped | 8,745 |
| Delivered messages selected out of FIFO order | 9,273 |
| Node crash/restart pairs | 24 |
| Expected committed application commands per scenario | 3 |
| Minority-only commands committed | 0/24 |

A safety observation runs multiple invariant checks; the number is not a count of unique reachable states or an exhaustive state-space proof. Every case retains its failure status; no failed seed is discarded. All expected initial/majority/final commands committed, and all nodes converged on the same committed prefix after healing. The minority's accepted uncommitted suffix was excluded from the final committed history.

Reports preserve final states, full committed entry histories including internal no-ops, leaders by term, up to 256 initial transport events, and separate partition/crash/restart/heal/proposal/leader milestones. Omitted counts are explicit. The [checked-in demo](../histories/demo.json) is a real three-node seed-7 execution. The complete 24-case artifact is reproducible and git-ignored under `artifacts/`.

## Review finding discovered during execution

Early exploration exposed repeated election disruption because a higher-term **rejected** vote request reset the receiver's timeout. The fix preserves elapsed time when rejecting a stale candidate and resets only on legitimate append/heartbeat or granted vote. A direct regression now demonstrates that the up-to-date receiver still starts its own election on time. Election timeouts were randomized as required for practical Raft election progress. The final schedule remains finite; results do not establish that every lossy asynchronous schedule must elect a leader.

## What was not established

No real network/RPC deployment, process supervisor, OS/power-crash model, authenticated peers, Byzantine safety, snapshot/compaction, membership changes, read-index/linearizable reads, client exactly-once semantics, or external state-machine transaction was executed or claimed. The checker starts at genesis and follows full in-process states; truncated report text is not a standalone safety proof. FileStore is a simple whole-state persistence backend, not a production WAL. The protocol needs a serialized owner and one owner per stable file. These are bounded implementation/simulation results, not a formal verification or production-readiness claim.
