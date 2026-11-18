# Sub2API v0.2.23 fork: scheduling disable and account disconnect

This fork's `v0.2.23-drainfix` branch is based exactly on the official `v0.2.23` tag at commit `5af9afb588c4b014b3df9db7d4073af1f8ae01b6`. The two production patches and regression tests were cherry-picked from `v0.2.22-drainfix`; the production patch IDs are identical and there were no text conflicts. Later upstream main commits are not included. The `v0.2.21-drainfix` and `v0.2.22-drainfix` branches remain separate and unchanged.

## Behavior

When an operator disables scheduling for an account, new OpenAI gateway requests and subsequent Responses WebSocket turns must not be admitted to that account. Requests already admitted before the disable continue until they finish.

- HTTP admission checks the database after acquiring an account slot, including slots obtained after waiting. If the account is disabled, paused or deleted, release the newly acquired slot and select another account.
- Check the first Responses WebSocket turn and every subsequent turn. An existing connection does not authorize another request after scheduling is disabled. Refused subsequent turns close with code 1013 and request reconnection.
- Read only the account's active/deleted/schedulable gate directly from the database, independently of cached scheduler snapshots. Fail closed if that read fails.
- Scheduling disable and gate lookup errors do not count as upstream account health failures.
- Preserve the existing release of per-turn WebSocket concurrency slots; an idle connection does not need to hold a request slot.

## Scope and limits

The original graceful-drain patch covers the shared OpenAI gateway admission paths and their callers, including Responses, Messages, Chat Completions, Embeddings, Images, Alpha Search and the related Grok paths. It does not introduce account-wide force-disconnect or cancellation APIs, change administrative account tests/credential refresh semantics, or change database schemas, billing rules, or quota rules.

The admission boundary is the authoritative gate check after slot acquisition. A request that passed that check before disable is already in flight. Idle WebSockets may stay connected, but a new turn is refused while the account remains disabled. Client reconnection behavior belongs to the client.

Each request/turn admission adds one lightweight database lookup by account primary key. This patch does not establish a permanent retirement generation across later account re-enablement.

## Optional force disconnect API

The follow-up adds `POST /api/v1/admin/accounts/:id/disconnect`. It explicitly disables scheduling and cancels this instance's OpenAI gateway work, including idle Responses WebSockets. The original scheduling toggle remains graceful. See [ACCOUNT_DISCONNECT_API.md](ACCOUNT_DISCONNECT_API.md) for the contract, completion checks and instance scope.

## Current v0.2.23 validation (2026-11-18)

See [V0.2.23_PORT_VALIDATION.md](V0.2.23_PORT_VALIDATION.md) for the upstream comparison, test results and deployment limits. The API contract is unchanged. All build hashes and test results below are historical; they do not describe the current release.

## Historical v0.2.22 validation (2026-11-17)

See [V0.2.22_PORT_VALIDATION.md](V0.2.22_PORT_VALIDATION.md) for the Go 1.27.4 test results, the added pre-output keepalive cancellation regressions, ARM64 compilation scope, and upstream migration considerations. The API contract is unchanged.

The remaining build records below are historical v0.2.21 records. Their version strings, binary hashes and test totals do not describe a v0.2.22 release artifact.

## Historical v0.2.21 force-disconnect build (2026-11-14)

The following results were recorded before publication using Linux amd64 (WSL Ubuntu 22.04), Go 1.27.3 and `GOMAXPROCS=4`:

- Targeted disconnect and admission regressions: 4 packages, 36 top-level tests and 115 passes including subtests; no failures.
- Race detection: 4 packages, 104 top-level tests and 202 passes including subtests; no failures.
- Full backend `go test -tags unit ./... -count=1`: all 61 packages with tests passed; 12,820 top-level tests and 26,989 passes including subtests, with 41 existing environment-dependent skips. No new targeted tests were skipped and no existing assertions were relaxed.
- Coverage includes real local HTTP/SSE and Responses WS cancellation, all four WS modes, idle WS sessions, queued requests, pool prewarming/dial cancellation, HTTP/2 stream isolation, asynchronous image task isolation, accurate concurrency accounting, retries selecting another eligible account, and normal graceful drain. Local upstreams were used; these results are not a production traffic replay or a guarantee that the upstream provider stops remote computation.
- Cross-compiled Linux ARM64 with `CGO_ENABLED=0`, `GOARM64=v8.0`, `-tags embed` and `-trimpath`. The artifact is `0.2.21-drainfix.3-local`, 117,440,672 bytes. All 28 changed Go files and `go.mod`/`go.sum` match the validated build's source SHA-256 manifest. Only these publication notes were updated afterwards.

The force-disconnect ARM64 binary SHA-256 is:

```text
b02df89937a3ee5b18c0933750e5270ce72842ff0c14e9dd3e6c0d4585ec253b
```

This binary was built before the follow-up commit, so its embedded commit metadata is `1a1bdf5-dirty`; the `-local` version suffix does not indicate a different code branch. The older `.2-local` artifact lacks the final cancellation and connection-pool fixes and must not be substituted. Tests ran on amd64; this record does not establish an ARM64 test-suite pass or production load testing.

The validation below is retained for the earlier graceful-drain build.

## Historical v0.2.21 graceful-drain ARM64 build (2026-11-13)

- Built natively for Linux ARM64 with Go 1.27.3, `CGO_ENABLED=0`, `GOARM64=v8.0`, and embedded frontend assets (`-tags embed`).
- Targeted service, handler and repository regressions passed. Cases include selected/fast/queued HTTP admission, lookup failures, already-admitted work, account re-enablement, and both WebSocket modes completing turn one while refusing turn two after disable.
- Complete handler unit tests passed (63.713s), and repository unit tests passed (10.990s).
- The complete service suite was not fully green: the patched suite hit the pre-existing `TestFilterThinkingBlocksSignedHistorySkipsDecode` allocation threshold (40 allocations, expected below 16). The unmodified official baseline hit a different pre-existing performance threshold, `TestInflightEstimate_AccountMappingNoDBAndBoundedMemory` (8,716,192 bytes, expected below 8,388,608). Both tests passed individually on both versions. No assertions were removed or relaxed; these results do not establish a fully passing service suite.
- Frontend frozen-lockfile installation, i18n checks, type checking and production build passed.
- Before publishing this branch, all 15 modified Go files were verified against the source SHA-256 manifest from that ARM64 build. Source code was not changed during publication.

To run the targeted regressions from the backend directory:

```sh
go test -tags unit ./internal/service ./internal/handler ./internal/repository \
  -run 'TestCheckAccountScheduling|TestAccountSchedulingAdmissionReadsDatabaseGate|TestOpenAIAccountAdmission_|TestOpenAIResponsesWebSocket_DisableDrainsCurrentTurnAndRejectsNext' \
  -count=1
```

The originally built ARM64 binary SHA-256 is:

```text
8b33a8bf0ec5fbb763d20b2d668f1dd36aa70a3e205b9bb2dd707762df715032
```

This repository commit contains source, regression tests and these notes. Local deployment configuration, credentials, server inspection reports, build logs and binaries are not included.
