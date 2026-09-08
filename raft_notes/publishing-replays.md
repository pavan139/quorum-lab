# Publishing a replayable revision

Keep the versioned schedule manifest, its SHA-256, the committed histories, and failure outcomes together. The saved event prefix has an explicit truncation limit; the separate partition/crash/heal milestones and final durable states remain necessary when interpreting a run.

Use a real Go SDK compatible with the module. The recorded compiler is Go 1.25.1; the module declares Go 1.23.0. Bound compiler/test concurrency when reproducing the checks:

```bash
GOFLAGS=-p=2 GOMAXPROCS=2 go test -race ./...
GOFLAGS=-p=2 GOMAXPROCS=2 go vet ./...
go build -o artifacts/quorumlab ./cmd/quorumlab
./artifacts/quorumlab evaluate --output artifacts/binary-history.json
python scripts/check_protocol.py --require-lint --wheel --go /absolute/path/to/go
```

The last command uses Python for the combined check report; the compiled program itself has no Python dependency. Race checks require the normal C/race toolchain. Verify the binary report against `go run` and inspect `reports/verification.json` before distributing the source or binary.

Include the [persistence contract](persistence-and-commit.md) and [partition record](partition-histories.md) with the artifact. State which transport and store were used. File sync/rename tests, simulated node restart, and machine power loss are different fault models. Preserve the license and actual revision history when uploading to a repository you own.
