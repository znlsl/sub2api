# Chained Codex service-tier billing fix (v0.2.25)

## Scope and invariant

Official base: `v0.2.25` (`348997267d756eab5729bb6b78a275a15d18796a`).
Previous fork head: `d73becbfad13e4f8a1f7c19b0ff0ecb55f15b5bc`.
Branch: `v0.2.25-drainfix`; release target: `0.2.25-drainfix.2`.

An OAuth gateway's client-facing processing-tier declaration must agree with the outbound processing tier used by its existing billing policy when Codex reports a non-authoritative placeholder. An outer API-key relay must not silently lower its billed tier solely because the inner credential is OAuth. Each group's own rates and discounts still apply independently; equal tiers do not imply equal final charges across differently priced groups.

Before: client priority -> inner OAuth sends priority -> Codex echoes default -> inner bills priority but forwards default -> outer API-key relay interprets default as a genuine downgrade and bills Standard.

After: the same raw response is observed first for diagnostics/billing -> OAuth response adapter emits priority -> outer relay observes priority -> both usage records retain the processing tier and apply their own existing prices.

A real loopback HTTP/SSE regression, running `Forward` and then `RecordUsage` on both hops, fails on the previous fork head because the outer usage record is default rather than priority. The exact same fixture passes after this patch. Repository stubs capture the real usage persistence and balance-deduction arguments; no production database or paid provider request is involved.

## Implementation and boundaries

- `openai_service_tier_response.go` owns the client-response transformation. The existing `ResolveOpenAIServiceTierBilling` remains the authority for the OAuth default echo. Codex auto is treated as a placeholder only at the client boundary; the existing observer and billing resolver are unchanged.
- Use the **final outbound** tier after group, Fast policy and account transformations, never the initial unfiltered client field. HTTP attempts overwrite the context value even when the tier is omitted. WS uses the current turn's payload/atomic metadata, not a connection-wide remembered Fast flag.
- Observe raw provider payloads before response transformation. Preserve `UpstreamResponseServiceTier`, model observation, token counts, errors and request IDs. Only the tier token is replaced, or its missing declaration inserted. Large JSON numbers, tool arguments and Unicode are not round-tripped through a generic JSON encoder.
- Cover Responses HTTP JSON, SSE and buffered SSE; both normal and passthrough forwarding; Chat Completions conversion and all generated chunks; HTTP-to-WS; WS context pool/shared/dedicated; WS passthrough; and WS-to-HTTP bridge. Created/in-progress and terminal response declarations use the same rule. JSON inside unrelated deltas/tools is untouched; opaque binary WS frames retain their existing behavior.
- Public API-key responses are not rewritten. Genuine Flex downgrades and unknown/non-string declarations are preserved. Standard, omitted/filtered and non-processing request tiers are not promoted. Non-OpenAI providers are untouched.
- `free_openai_fast`, custom Fast/Flex/Ultrafast pricing, per-group rates and balance deduction remain the original implementations. This fixes future requests; it does not retrospectively alter usage records or balances.
- No scheduler, force-disconnect contract, schema, dependency, frontend production code or rotation code is changed by this patch.

## Provenance

Ported with attribution from `Wei-Shaw/sub2api` PR #8143, which includes the original znlsl PR #7772 fix for issue #7770 and two lifecycle/auto follow-ups:

- `32cb688d1d525be1d1a6b8b673972e077c970cb8` (znlsl): original response/billing alignment.
- `a2a765488762fe9a21ee3e4b5ef37547642690d6` (lcy): missing/lifecycle declarations and all Chat chunks.
- `4508b7b91111560fa757877c4a3b9b46bf3538e8` (lcy): client-only auto placeholder handling.

PR #7772 was closed without merge; #8143 was still open at inspection on 2026-11-25. This is an explicitly reviewed fork port, not an upstream release claim. The append-only conflict with the v0.2.25 WS HTTP bridge fingerprint test retained both tests. Additional fork coverage adds two real HTTP hops with actual billing/discount/balance effects and shared/dedicated/auto multi-turn WS cases.

## Validation performed 2026-11-25

Linux amd64 (WSL), Go 1.27.4, GOMAXPROCS=4. Backend tests were compiled separately and executed in batches with both a 60-second Go timeout and a 60-second external process-group deadline.

- New/related targeted race selection: 25 top-level tests x 3 repetitions = **75** top-level passes, **1,269** including subtests; no failures or skips.
- Full backend `unit` selection: **61 packages**, **113 batches**, **13,441** top-level passes, **31,283** including subtests, **40 existing conditional skips**; no failures, omitted listed tests or unexpected test omissions. Longest batch: 21.772 seconds. The previous fork's recorded passing cases are retained (apart from normalizing one runtime-timestamp test name for comparison).
- Existing admission/disconnect/pool core race selection: **120** top-level passes, **254** including subtests across five packages; no failures or skips.
- Real local PostgreSQL 18 / Redis integration: **5** scheduler/probe synchronization tests passed, no skips.
- Frontend: **3,205** tests passed, no failures/skips. No additional frontend assertions were changed.
- Full `go vet -tags unit ./...` passed. Go formatting and diff whitespace checks passed.
- Regression reproduction on exact old head plus the new loopback test fixture: the priority/SSE/non-passthrough case fails as expected on the outer usage tier. This is a fresh reproduction; the old full-unit results are reused from the previous dated validation, not represented as newly rerun.
- The two protected production patches remain in history. Migration trees are identical between official v0.2.24, v0.2.25 and this fork.

The loopback test matrix covers JSON/SSE, passthrough on/off, Fast alias, priority, missing/auto echoes, Ultra Fast, actual Flex downgrade, explicitly requested Flex, Standard, omitted tier, public-API downgrade, outer-group Free Fast and Ultra Fast's exclusion from Free Fast. It checks cached/noncached token separation, processing tier, TotalCost, ActualCost, group rate and exactly one balance deduction per hop. WS tests switch priority -> omitted -> ultrafast within one connection and check the raw observer independently.

This is not a claim that every possible production flow or the entire project's race suite is flawless. The two upstream race categories previously reproduced on official v0.2.25 (parallel Gin test-mode writes and shared Account model-mapping cache access) remain documented in `V0.2.25_PORT_VALIDATION.md`; no unrelated production changes or assertion suppression were added. Build hashes, embedded-UI checks and upload verification belong to the release artifact's `BUILD_INFO.json`/`SHA256SUMS`, not this pre-build source note. Production replacement remains manual.
