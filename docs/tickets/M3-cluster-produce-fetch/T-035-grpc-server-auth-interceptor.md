# T-035: gRPC server infrastructure and auth interceptor

**Milestone:** M3 — 3-node cluster with produce/fetch
**Effort:** S
**Status:** TODO

## Goal

Implement the two `grpc.Server` instances (Management API on `:9091`, Data API on `:9092`), wire the unary and stream interceptor chains (`Auth → Logging → Metrics → Handler`), and implement the auth interceptor that validates `bunnymq-auth-token` from gRPC incoming metadata.

## Context

Both gRPC services share the same interceptor chain. The auth interceptor is the outermost — it rejects unauthenticated requests before logging or metrics are recorded. PLAINTEXT mode (empty token list) bypasses validation entirely, which is the default for demo deployments per REQUIREMENTS.md §8.1.

References:
- [06-api-protocol.md §8 — Authentication](../../design/06-api-protocol.md#8-authentication)
- [06-api-protocol.md §9 — Interceptor chain](../../design/06-api-protocol.md#9-server-side-interceptor-chain)

## Scope

- Create `internal/api/server.go`:
  - `NewManagementServer(config ServerConfig, cc *cluster.ClusterCoordinator) *grpc.Server` — registers `ManagementServiceServer`, applies interceptor chain.
  - `NewDataServer(config ServerConfig, dc *data.DataCoordinator) *grpc.Server` — registers `DataServiceServer`, applies interceptor chain.
  - `ServerConfig` struct: `Addr string`, `AuthTokens []string`, `TLSConfig *tls.Config`.
- Create `internal/api/auth/interceptor.go`:
  - `UnaryInterceptor(validTokens []string) grpc.UnaryServerInterceptor`
  - `StreamInterceptor(validTokens []string) grpc.StreamServerInterceptor`
  - Both call the shared `validateToken(ctx, validTokens) error` which reads `bunnymq-auth-token` from incoming metadata; returns `codes.Unauthenticated` on failure.
  - PLAINTEXT mode: if `len(validTokens) == 0`, return nil immediately.
- Create `internal/api/logging/interceptor.go`:
  - Logs RPC entry at `debug`, result (status code + latency) at `info` using `zap.Logger`.
  - Stub implementation — full structured logging polish is M5.
- Wire interceptors with `grpc.ChainUnaryInterceptor` / `grpc.ChainStreamInterceptor`.
- TLS: if `config.TLSConfig != nil`, append `grpc.Creds(credentials.NewTLS(config.TLSConfig))`.

## Out of scope

- Metrics interceptor — M5 (T-MET in M5 tickets).
- ManagementService and DataService handler implementations — T-036, T-037, T-038.

## Definition of done

- [ ] `go build ./internal/api/...` passes.
- [ ] `go test ./internal/api/auth/...` passes.
- [ ] Auth interceptor: valid token → `nil`; wrong token → `codes.Unauthenticated`; PLAINTEXT (empty list) → `nil`.
- [ ] Interceptor chain order is Auth → Logging → Handler (verified by test).

## Tests required

- `TestAuthInterceptor_ValidToken` — valid token in metadata; handler called.
- `TestAuthInterceptor_InvalidToken` — wrong token; returns `codes.Unauthenticated`; handler NOT called.
- `TestAuthInterceptor_MissingToken` — no metadata key; returns `codes.Unauthenticated`.
- `TestAuthInterceptor_Plaintext` — empty `validTokens`; any request passes without token.
- `TestInterceptorChain_Order` — wrap a test handler that records which interceptors fired and in what order.

## Dependencies

T-034 (generated proto stubs, for gRPC server registration type signatures).

## Notes

The logging interceptor is a stub here (just logs at debug/info level). The full structured logging audit (request IDs, field names, log levels) is a polish task in M5. For this ticket, the logging interceptor's only requirement is that it does not break the chain and does not panic. Auth failures should be logged at `warn` by the auth interceptor itself, not propagated to the logging interceptor, to avoid double-logging unauthenticated noise.
