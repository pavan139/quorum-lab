# Protocol changes

Start a Raft change with the shortest message sequence that exposes it. State which term, vote, log prefix, or commit index may change, and which state must be durable before a reply leaves the node. The [persistence rules](raft_notes/persistence-and-commit.md) and [protocol regressions](raft_notes/protocol-regressions.md) are the relevant starting points.

Keep a node single-owner and membership static. FileStore tests need a real temporary directory and a new store/node instance for recovery. A checksum failure must remain an error; treating it as an empty node would erase the evidence needed for safe recovery. Use injected store failures to check that persistence failures produce no success response.

With an actual Go SDK on PATH:

```bash
GOFLAGS=-p=2 GOMAXPROCS=2 go test ./...
GOFLAGS=-p=2 GOMAXPROCS=2 go test -race ./...
GOFLAGS=-p=2 GOMAXPROCS=2 go vet ./...
gofmt -l raft simulator cmd
```

When changing exploration, version the manifest in `schedules/` and retain every seed outcome. Check the manifest hash and counters in the saved history summary. A bounded seed run and the race detector answer different questions; neither replaces a direct regression for the protocol rule being changed. [Publishing replays](raft_notes/publishing-replays.md) describes the evidence to ship.
