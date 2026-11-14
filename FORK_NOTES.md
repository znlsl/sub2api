# Sub2API v0.2.21 fork: scheduling disable and graceful drain

This fork's `v0.2.21-drainfix` branch is based on the official `v0.2.21` tag at commit `10ba8f9b4dc2e6023d4a9879ab77b21841babd10`. It does not merge later upstream main changes. The first patch corresponds to the Linux ARM64 build identified as `0.2.21-drainfix.1`.

## Behavior

When an operator disables scheduling for an account, new OpenAI gateway requests and subsequent Responses WebSocket turns must not be admitted to that account. Requests already admitted before the disable continue until they finish.

- HTTP admission checks the database after acquiring an account slot, including slots obtained after waiting. If the account is disabled, paused or deleted, release the newly acquired slot and select another account.
- Check the first Responses WebSocket turn and every subsequent turn. An existing connection does not authorize another request after scheduling is disabled. Refused subsequent turns close with code 1013 and request reconnection.
- Read only the account's active/deleted/schedulable gate directly from the database, independently of cached scheduler snapshots. Fail closed if that read fails.
- Scheduling disable and gate lookup errors do not count as upstream account health failures.
- Preserve the existing release of per-turn WebSocket concurrency slots; an idle connection does not need to hold a request slot.

## Scope and limits

The patch covers the shared OpenAI gateway admission paths and their callers, including Responses, Messages, Chat Completions, Embeddings, Images, Alpha Search and the related Grok paths. It does not introduce account-wide force-disconnect or cancellation APIs, change administrative account tests/credential refresh semantics, or change database schemas, billing rules, or quota rules.

The admission boundary is the authoritative gate check after slot acquisition. A request that passed that check before disable is already in flight. Idle WebSockets may stay connected, but a new turn is refused while the account remains disabled. Client reconnection behavior belongs to the client.

Each request/turn admission adds one lightweight database lookup by account primary key. This patch does not establish a permanent retirement generation across later account re-enablement.

## Validation of the ARM64 build (2026-11-13)

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
