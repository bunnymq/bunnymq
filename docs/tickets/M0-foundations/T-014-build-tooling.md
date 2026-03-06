# T-014: Build tooling — Makefile, golangci-lint, CI

**Milestone:** M0 — Foundations
**Effort:** S
**Status:** TODO

## Goal

Establish the build, lint, and CI configuration so that `make build`, `make test`, `make lint`, and `make proto` work consistently in the repository.

## Context

The M0 milestone DoD requires a Makefile (or task script) with `build`, `test`, `lint`, `proto` targets, golangci-lint configured, and a basic CI script. All subsequent milestones depend on these tools: implementation tickets run `go test ./<package>/...` and rely on linting to catch errors early. The CI script serves as a green-gate check before code is merged.

References:
- [CLAUDE.md — M0 Milestone DoD](../../CLAUDE.md) — lists required Makefile targets and lint requirements
- [09-metrics-logging.md §2.1](../../design/09-metrics-logging.md#21-library-choice) — indirectly requires `go test ./...` to work correctly

## Scope

- Write `Makefile` at repo root with targets:
  - `build`: `go build ./...`
  - `test`: `go test -race ./...`
  - `lint`: `golangci-lint run ./...`
  - `proto`: delegates to T-013's `buf generate` command
  - `clean`: removes build artifacts
  - `all`: runs `lint`, `build`, `test` in sequence
- Configure `.golangci.yml` at repo root with at minimum: `errcheck`, `staticcheck`, `govet`, `gofmt`, `ineffassign`, `unused` linters enabled; `gocyclo` with threshold 20; `misspell`.
- Write GitHub Actions workflow at `.github/workflows/ci.yml` that triggers on push and pull request to `main`, running: `make proto`, `make build`, `make lint`, `make test`.
- Pin Go version in the CI workflow to match the version in `go.mod`.
- Add `tools.go` (blank build tag `//go:build tools`) importing `golangci-lint` via `go:generate` or equivalent to pin the version in `go.mod`.

## Out of scope

- Integration test runner — M5 ticket.
- Docker/docker-compose — M5 ticket.
- Coverage thresholds — not enforced in v1.

## Definition of done

- [ ] `make build` exits 0 on the skeleton codebase.
- [ ] `make test` exits 0 (no test files; zero failures; `-race` flag present).
- [ ] `make lint` exits 0 on the skeleton codebase.
- [ ] `make proto` runs buf generate and exits 0.
- [ ] `make all` runs lint + build + test and exits 0.
- [ ] `.golangci.yml` committed to root.
- [ ] `.github/workflows/ci.yml` committed and syntactically valid.
- [ ] golangci-lint version pinned (v1.60 or later, supporting Go 1.23).

## Tests required

`TestBuildToolingCI` — exercised by CI running the workflow. No additional unit tests for this ticket.

`TestLintClean` — running `make lint` on the M0 skeleton must exit 0; no lint errors permitted in stub code.

## Dependencies

T-012 (skeleton must exist for `make build` to work).
T-013 (proto target depends on codegen being configured).

## Notes

Pin golangci-lint version in both `.golangci.yml` (field: `linters-settings.golangci-lint-version` if supported, or CI download URL) and the CI workflow's setup step. Use `golangci-lint/golangci-lint-action@v6` in GitHub Actions. The `errcheck` linter may flag stub function bodies that return `errors.New("not implemented")` — add a blanket `// nolint:errcheck` or configure the linter to exclude `_test.go` and stub files if needed. Keep the CI matrix simple: single OS (ubuntu-latest), single Go version.
