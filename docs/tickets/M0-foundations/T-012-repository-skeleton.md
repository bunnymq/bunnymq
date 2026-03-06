# T-012: Repository skeleton

**Milestone:** M0 — Foundations
**Effort:** M
**Status:** TODO

## Goal

Create the Go module tree matching `01-modules.md` with stub packages, stub types, and stub interface implementations so `go build ./...` succeeds on an otherwise empty codebase.

## Context

`01-modules.md §1` defines the full package layout. All subsequent milestones depend on this skeleton: M1 fills in `internal/storage`, M2 fills in `internal/raft` and FSMs, and so on. Creating the skeleton first enforces the dependency graph from the type system and ensures no circular imports are introduced later.

References:
- [01-modules.md §1 — module tree](../../design/01-modules.md#1-module-tree)
- [01-modules.md §2 — per-module descriptions](../../design/01-modules.md#2-per-module-descriptions)
- [02-storage.md §2 — Storage interface](../../design/02-storage.md#2-public-interface)
- [03-raft-fsm.md §1.3 — Raft Host public API](../../design/03-raft-fsm.md#13-public-api-of-the-wrapper)

## Scope

- Initialize `go.mod` for module `github.com/bunnymq/bunnymq` with Go 1.23 (current stable).
- Create all directories: `api/`, `cmd/bunnymq/`, `cmd/bunnymq-cli/`, `internal/config/`, `internal/raft/`, `internal/metadata/`, `internal/partition/`, `internal/storage/`, `internal/coordinator/cluster/`, `internal/coordinator/data/`, `internal/coordinator/group/`, `internal/api/data/`, `internal/api/management/`, `internal/auth/`, `internal/metrics/`, `internal/log/`, `pkg/client/`, `pkg/proto/`.
- Each package: one `.go` file declaring the package. Stub types that match the design's public interfaces exactly (empty struct bodies, method stubs returning zero/nil/`errors.New("not implemented")`).
- `internal/storage`: declare the full `Storage` interface from `02-storage.md §2` (all 10 methods, exact signatures). A `FileStorage` stub struct implementing it.
- `internal/raft`: declare `Host` struct with all typed helper methods from `03-raft-fsm.md §1.3` (`SyncProposeMetadata`, `ProposeMetadata`, `LookupMetadata`, `SyncProposePartition`, `ProposePartition`, `LookupPartition`, `StartPartitionShard`, `StopPartitionShard`). Define `MetadataCommand`, `MetadataQuery`, `PartitionCommand`, `PartitionQuery` stub types.
- `internal/metadata`: declare `MetadataFSM` struct with stub `IStateMachine` methods (`Update`, `Lookup`, `SaveSnapshot`, `RecoverFromSnapshot`, `Close`).
- `internal/partition`: declare `PartitionFSM` struct with stub `IOnDiskStateMachine` methods (`Open`, `Update`, `Lookup`, `PrepareSnapshot`, `SaveSnapshot`, `RecoverFromSnapshot`, `Sync`, `Close`).
- `internal/coordinator/cluster`: declare `ClusterCoordinator` struct with all methods from `04-cluster-coordinator.md §2`.
- `internal/coordinator/data`: declare `DataCoordinator` struct with all methods from `05-data-coordinator.md §2`.
- `internal/coordinator/group`: declare `GroupCoordinator` stub.
- `internal/api/data`, `internal/api/management`: declare gRPC server stub structs.
- `internal/auth`, `internal/metrics`, `internal/log`: declare package-level function stubs.
- `pkg/client`: declare `Producer`, `Consumer`, `AdminClient` stub structs.
- `go.mod` and `go.sum`: add all required dependencies: `github.com/lni/dragonboat/v4`, `google.golang.org/grpc`, `google.golang.org/protobuf`, `github.com/prometheus/client_golang`, `go.uber.org/zap`, `github.com/spf13/viper`, `github.com/spf13/cobra`.

## Out of scope

- Any implementation logic — M1 through M5.
- Proto codegen — T-013.
- Makefile / CI — T-014.
- `pkg/proto`: leave as empty package; T-013 generates its content.

## Definition of done

- [ ] `go build ./...` passes with zero errors on the full skeleton.
- [ ] `go test ./...` exits 0 (no test files; no failures).
- [ ] All packages from `01-modules.md §1` exist as Go packages.
- [ ] `internal/storage.Storage` interface matches `02-storage.md §2` exactly (all 10 methods, correct signatures).
- [ ] `internal/raft.Host` exposes all 8 typed helpers from `03-raft-fsm.md §1.3`.
- [ ] `ClusterCoordinator` and `DataCoordinator` stubs have all methods from `04-CC.md §2` and `05-DC.md §2`.
- [ ] Module name is `github.com/bunnymq/bunnymq`.
- [ ] `go.mod` lists all 7 external dependencies.

## Tests required

`TestSkeletonCompiles` — satisfied by `go build ./...` in CI. No additional unit tests for stub code.

## Dependencies

T-001 (informs dragonboat config constants for `internal/raft` RaftConfig defaults).
T-007 (informs whether `Storage.Sync()` is in the interface — it is already in `02-storage.md §2`, so this is informational only).

## Notes

Keep stub method bodies minimal — `return nil` or `return nil, nil` or `panic("not implemented")`. The goal is a compilable skeleton. Do not implement any logic. The `IStateMachine` and `IOnDiskStateMachine` interfaces must be satisfied exactly — pull the correct interface definition from the dragonboat v4 package. The `dragonboat.Entry` and `sm.Result` types must be imported from dragonboat; stub FSM methods use these types in their signatures.
