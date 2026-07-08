# Protocol design and failure analysis

## Raft core and invariants

For a static cluster of n=3 or n=5, quorum size is `floor(n/2)+1`. Every two majorities intersect. Persistent term and one-vote-per-term rules prevent two legitimately elected leaders in the same term. Voters compare `(lastLogTerm, lastLogIndex)` lexicographically; a longer old-term log does not beat a shorter newer-term log.

The leader maintains `match[p]` and `next[p]`. After a matching-prefix append succeeds, `match` can only increase. Reordered responses with an already-acknowledged per-peer RPC number are ignored; failures cannot backtrack below the known matched prefix. Term changes reset volatile replication state, and stale-term responses never authorize work in the new term.

The commit rule is explicit:

```text
advance commitIndex to N only if:
  N > commitIndex
  log[N].term == currentTerm
  count(p such that match[p] >= N) >= floor(n/2)+1
```

A new leader appends a durable current-term no-op. Committing that entry can safely commit preceding old-term entries. Counting a majority of replicas of an old-term entry alone is forbidden and directly tested using a 40-entry old log and actual 32-entry replication batches.

Followers first match `prevLogIndex/prevLogTerm`. A conflict truncates only an uncommitted suffix and appends the leader's entries. Same-index/same-term commands must agree. Committed prefixes can never be changed. A successful append acknowledges exactly `prevLogIndex + len(entries)`, not an unrelated longer local suffix. Follower commit is capped at that verified prefix, preventing partial batches or empty heartbeats from committing unverified local entries.

## Persistence barriers and identities

The Store contract is synchronous and must return only after the new state is durable:

1. Increment term and self-vote before election requests.
2. Save a granted vote before the vote response.
3. Save appended/truncated log state before append success.
4. Save commit advancement before exposing the applied prefix or advertising the new commit.

The implementation persists commit index as an additional recovery guarantee. It validates monotonic term/commit, vote consistency within a term, preserved committed entries, and immutable identity/membership on every stable transition. `persist` assigns new node state only after successful storage. Failure halts; it never produces a canned successful reply.

`FileStore` stores a SHA-256-checked JSON envelope using a synced 0600 temporary file, atomic rename, then synced containing directory. It preserves the complete bounded log. Checksums detect corruption, not malicious modification or rollback of a previously valid file. There is no shared-path multiprocess lock, protocol snapshot, or compaction.

## Elections and timer semantics

Election deadlines are randomized in `[base, 2*base)`, with `base=10+2*nodeID` in the simulator and a seeded generator. A heartbeat or granted vote resets the deadline. Receiving a higher term updates durable term and role, but a **rejected** vote request does not postpone the receiver's election. Otherwise a stale-log node with a short timer can repeatedly disrupt more up-to-date candidates without ever being eligible itself. The regression test preserves elapsed time across this exact rejection.

Basic Raft here has no pre-vote or check-quorum extension. An isolated leader can remain locally in leader role, accept an uncommitted command, and later step down when it sees a newer term. Acceptance is not an acknowledgement of commitment. Liveness depends on eventual communication/timing; a bounded passing schedule is not a liveness proof.

## Failure matrix

| Failure/event | Required behavior | Implemented evidence |
| --- | --- | --- |
| Crash after durable vote | Restart cannot grant a different vote in that term | Memory and actual-file restart tests |
| Storage fails before vote/log/commit publication | Halt, no successful reply or invented application | Failure-injection tests and unchanged durable state |
| Minority partition accepts writes | No majority commit; majority can elect and overwrite suffix later | Static3/5 partition schedules and direct test |
| Old-term entry reaches a majority | Wait for current-term quorum entry | Real partial-batch regression |
| Follower has a longer unverified suffix | Commit only through the prefix proven by this append | Empty/32-entry append regression |
| Duplicate vote/stale append response | Do not count twice or regress matched replication | Vote and sequence tests |
| Conflicting uncommitted suffix | Truncate and repair | Conflict + restart test |
| Attempt to change committed entry | Fail closed | Node and stable-store tests |
| Truncated/checksum-invalid stable file | Reject recovery; do not reset to empty | Real file corruption tests |
| Unknown member/invalid index/term/oversized batch | Reject before state mutation | Input and budget tests |
| Crashed replacement leader; network heals | Preserve stable state, re-elect, converge within measured schedule | Seeded 24-case experiment |

## Independent checker

The checker does not call the node's commit or election algorithms. It reads stable states and observes leader transitions from genesis, retaining its own vote, term, commit, elected-leader, and committed-entry histories. It asserts election safety, log matching, leader completeness when a term's leader first appears, majority durable-prefix evidence/current-term restriction on leader commit advancement, and agreement of every committed entry across time and nodes.

Eight intentionally corrupted histories test the checker itself: two leaders, divergent commit, same-term prefix mismatch, old-term commit-by-counting, missing quorum, incomplete new leader, double vote, and term rollback. This tests detector behavior; it is not a formal proof that all possible protocol defects will be detected.

## Complexity and bounds

Let L be bounded log length, B append batch size, and n static member count. Elections use O(n) request/response messages. Heartbeats use O(n); a batch transfers O(nB) entries. Log validation/copy and whole-file persistence cost O(L) per saved transition, and the leader's simple commit scan costs O(nL) per acknowledgement in the worst case. Memory is O(nL + queued messages). The deliberately exhaustive checker may cost O(n²L²) for repeated prefix comparisons; n<=5 and L<=512 bound this educational implementation.

Hard limits: 512 log entries, B=32, 256 command bytes, bounded term, a 4,096-message simulator queue, 3,000 total simulated ticks, and 64 delivery attempts per tick. Reports retain a 256-event prefix and 128 milestones, with omitted counts. Capacity errors are explicit; no hidden compaction, timer loop, background worker, provider call, or external integration exists.

## Unsupported features

Membership/joint consensus, learners, dynamic cluster sizing, snapshots/InstallSnapshot, read-index and linearizable reads, lease reads, real transport/authentication, Byzantine nodes, application transactions, exactly-once clients, and hard process/power-failure testing are unsupported. The single-owner Node contract must be serialized by any future real transport adapter. Adding these features requires new invariants and independent histories; their absence is not covered by the current passing experiment.
