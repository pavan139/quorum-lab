# Toolchain and protocol checks

The Go suite contains 29 top-level tests across `raft`, `simulator`, and `cmd/quorumlab`, including actual temporary-file recovery/corruption and eight adversarial checker histories. The recorded SDK is Go 1.25.1. Run tests with `GOFLAGS=-p=2 GOMAXPROCS=2 go test ./...`; race and vet runs are listed in the [partition record](partition-histories.md).

The [verification report](../reports/verification.json) records current source/configuration digests, top-level tests/skips, race/vet/build outcomes, and execution of the compiled CLI outside the source tree. `scripts/check_protocol.py` accepts an explicit real SDK path when producing that report.

A passing race run applies to executed code. A Node still requires one serialized owner, and FileStore recovery tests do not model power loss or cross-process ownership.
