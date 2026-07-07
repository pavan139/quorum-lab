# Architecture and execution boundaries

The protocol is independent of the simulator and CLI. `raft.Node` has a single serialized owner. It consumes one logical clock tick or typed RPC, synchronously saves necessary state, and returns outbound messages. No goroutine, network, filesystem permission, or model action is derived from command text.

```mermaid
sequenceDiagram
    participant Client
    participant Leader
    participant Stable as Stable store
    participant Follower
    participant PeerStore as Follower stable store
    participant Check as Independent checker
    Client->>Leader: Propose opaque bounded command
    Leader->>Stable: Save appended log; wait for durable completion
    Leader-->>Client: Accepted index (not commit acknowledgement)
    Leader->>Follower: AppendEntries with matching prefix
    Follower->>PeerStore: Save log before success response
    Follower-->>Leader: Matching prefix acknowledgement
    Leader->>Leader: Count majority; require current-term entry
    Leader->>Stable: Save advanced committed prefix
    Leader->>Follower: Advertise leader commit
    Check->>Stable: Inspect durable logs/commit
    Check->>PeerStore: Inspect durable logs/commit
    Check->>Check: Check safety and history independently
```

## Files and responsibilities

- `raft/types.go`: bounded entries, messages, static configuration, stable-state/transition validation.
- `raft/node.go`: elections, timers, append conflict repair, stale-response handling, quorum commit, and applied-prefix reconstruction.
- `raft/store.go`: cloning crash-surviving memory model and actual atomic synced/checksummed file backend.
- `simulator/simulator.go`: logical ticks, seeded reordering/drop, partitions, crash/restart, bounded queues/traces.
- `simulator/check.go`: independent safety assertions over stable states and elected-leader/history observations.
- `simulator/evaluation.go`: frozen scenario schedule, manifest/hash, convergence expectations, per-case reports.
- `cmd/quorumlab`: identical source-run/binary CLI and report export.

## Persistence and crash boundary

Persisted state includes identity/membership, current term, vote, the complete bounded log, and commit index. Node construction restores these bytes, validates them, and starts as a follower; leader role, timer, candidate votes, per-peer replication offsets, and RPC sequence counters are volatile. The committed ledger is reconstructed from the stored committed prefix. No external state-machine callback is claimed.

A storage failure halts the node and emits no success messages. New state is assigned in memory only after the save succeeds. A sync failure after rename may leave additional durable state; halting is conservative, and restart validates the file. Missing storage can bootstrap; corrupt/truncated/checksum-invalid storage never silently bootstraps.

The file backend writes the whole bounded state rather than a write-ahead log. This is not Raft snapshotting or log compaction. It assumes one process owns a stable path and a local filesystem honors the sync/rename contract. Simulation storage survives node-object destruction but does not simulate OS-cache/power behavior.

## Network and observation boundary

All nodes have the same fixed members. The simulator validates a complete partition assignment before mutating connectivity, preserves in-flight messages, and may deliver messages from a subsequently crashed sender. A crashed destination drops delivery. Reordering selects any current queue index; losses and election timers are seeded. All actual protocol replies still pass the same durable-state rules.

The safety checker begins at genesis and follows the full in-process execution. It can inspect all stable stores, including crashed-node stores; this is an oracle available for testing, not an operational distributed read. It retains committed history independently of any current leader, checks candidate leader completeness at first observation, and checks replication evidence whenever a leader advances commit. It does not claim linearizable client histories, Byzantine authentication, complete model checking, or inference from truncated report traces.
