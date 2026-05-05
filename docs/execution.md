# Verified execution

Verified execution covers two read paths: explicit record resolution by name, and primary-name verification by `(address, coin_type)`. Both consume declared topology snapshots, manifest versions, and the requested chain positions. Neither reads adapter-specific internals directly.

Mixed routes return per-result `ResultStatus`: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`. Verified-only outcomes are `mismatch` and `execution_failed`.

Companion docs: [`architecture.md`](architecture.md), [`api-v1.md`](api-v1.md), [`storage.md`](storage.md).

## Resolution flow

```
   1. load declared topology for the surface + ChainPositions
   2. select the namespace's execution entrypoint
   3. resolve resolver, alias rewrites, wildcard traversal
   4. execute onchain calls
   5. follow CCIP-Read where the manifest and resolver family allow
   6. hand admitted block-anchored snapshots to intake; persist trace + answer
```

Every step is attributable in provenance. One request may cover multiple explicit selectors under one trace, returning one `verified_queries` entry per selector. Wildcard traversal and alias rewriting appear explicitly in the trace. Entrypoint selection is attributable to a manifest-declared `source_family` and `role` — registry-family presence alone doesn't imply it.

Before persisting a selector-local result as a supported, cache-eligible outcome, execution reloads from storage the manifest versions, the same declared topology snapshot the mixed route would serve, and any resolver-profile admission state required by participating resolver-local fact families. The namespace support class derives from those stored inputs, not from transient trace shape. If revalidation can't re-establish a frozen supported class, audit material may persist but supported-outcome persistence fails closed.

Unsupported record families surface explicit `status=unsupported` — never silent declared-cache fallback. Supported requests that produce no trustworthy answer return `status=execution_failed` with a typed `failure_reason`.

### Entrypoints

| Namespace | Entrypoint | Address |
| --- | --- | --- |
| ENS (Ethereum mainnet) | `ens_execution`, `universal_resolver` role | `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` (proxy)[^ens-univ] |
| Basenames (mainnet) | active `basenames_execution` v2, `l1_resolver` role | `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31`[^bn-readme] |

The pinned ENSv1 deployment artifact is the implementation/ABI anchor behind `ens_execution` rather than the route-facing proxy.

For Basenames, `basenames_l1_compat` and `basenames_execution` share the same L1 Resolver address. Transport ownership stays with `basenames_l1_compat`; verified-resolution entrypoint selection stays with `basenames_execution`. Declared exact-name, address-name, and children reads remain on the Base registry/registrar/resolver families. `basenames_base_primary` is claim intake only.[^bn-revreg]

### On-demand execution

`GET /v1/resolutions/{namespace}/{name}` and `GET /v1/resolve/{name}` with `mode=verified` or `mode=both` are cache-or-live-execute reads for supported Universal Resolver selectors.[^v1-iur] The route first looks for matching persisted execution output at the selected exact-name snapshot. On miss, the API performs Universal Resolver execution against that selected chain position, persists the trace and outcome, and returns the persisted outcome in the same response.

Live-execution rules:

- The execution target is the exact `ChainPositions` selected by the route before any verified-support check. Absent `at` and `chain_positions`: `consistency=head` at the latest stored checkpoint for the required chain.
- Full resolution and explain/audit execution never retarget to provider latest, a newer checkpoint, or a different snapshot mid-request.
- The API Ethereum RPC provider must be configured (`BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<url>`) and able to serve the selected block. Missing config or provider unavailability fails closed with `409 stale` and a configuration message — never declared cache fallback.
- Unsupported selector families and unsupported verified path classes stay selector-local `status=unsupported`.
- `GET /v1/explain/resolutions/{namespace}/{name}/execution` is persisted-trace readback only.

The compact records routes — `GET /v1/names/{namespace}/{name}/records` and `GET /v1/resolve/{name}/records` — use the same supported-selector boundary but are current UI reads. When they need on-demand ENS verified values, they call the Universal Resolver with the provider `latest` block tag, return the result inline, and don't persist exact-snapshot execution cache rows or `raw_call_snapshots`.

### Namespace inference

`GET /v1/resolve/{name}` infers namespace before step 1:

| Pattern | Namespace |
| --- | --- |
| exact `base.eth` | `ens` |
| `*.base.eth` | `basenames` |
| other supported ENS names | `ens` |

The inferred namespace selects topology, entrypoint, trace namespace, request key, provenance, and cache identity exactly as if the caller had used the canonical route. Inference and verified support are separate gates — an inferred `namespace=basenames` request never retries as `namespace=ens`.

## Primary-name verification

```
   1. load the claimed name from the admitted declared claim surface
   2. normalize using the recorded normalizer version
   3. resolve the claimed name for the requested coin_type
   4. compare resolved target to the requested address
   5. persist both claim state and verification result
```

The route keeps claim state separate from the execution-derived verification result.

| Object | Statuses |
| --- | --- |
| `claimed_primary_name` | `success`, `not_found`, `unsupported`, `invalid_name` |
| `verified_primary_name` | the above plus `mismatch`, `execution_failed` |

`mismatch` means the claim normalized, resolved for the requested `coin_type`, and produced a concrete target address that didn't equal the requested one. A nonblank raw claim that can't be normalized surfaces `invalid_name`; blank or whitespace-only is `not_found`. `raw_claim_name` is claim-local — it may be preserved to explain `claimed_primary_name.status=invalid_name` but doesn't migrate into `verified_primary_name`. When verification establishes a concrete normalized target, `verified_primary_name` may carry that name identity for `success` or `mismatch`; otherwise it's omitted.

### Claim sources

For ENS on Ethereum mainnet, declared claim intake is reverse-only through `ens_v1_reverse_l1` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy] Verification reuses `ens_execution` and the Universal Resolver proxy. Declared claim ownership and verified execution ownership stay separate.[^v1-aur]

For Basenames, declared claim intake is `basenames_base_primary` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`.[^bn-revreg] Claim intake only — exact-name, address-name, and children declared truth remain on the Base registry/registrar/resolver families because upstream exposes reverse-name writes through the dedicated `ReverseRegistrar` rather than the Base authority stack. Verification runs through `basenames_execution` against the Mainnet `L1Resolver`.

`claimed_primary_name.name`, when present, comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source for that same tuple. Never synthesized from manifest presence, resolver-backed identity, verified execution identity, tuple presence alone, a different tuple, or any fallback claim source. Missing or unsupported reverse claims don't trigger fallback to registry-, resolver-, or other claim-setting surfaces.[^v1-revreg-claim]

### Coverage and provenance

The exact-tuple verified-primary support class is persisted readback only. Both ENS and Basenames slices use it. Stable execution identity is `request_type=verified_primary_name`; the request key is the normalized tuple `{namespace}:{normalized_address}:{coin_type}` (lowercase address). Claimed text, normalized identity, verified target, status, and section-local provenance are not part of the key.

Supported tuples may publish `coverage.status=partial` with `exhaustiveness=non_enumerable`. Tuples outside the frozen class remain explicit `unsupported` — they don't inherit coverage from manifest rollout, tuple presence, or verified-resolution support.

`primary_names_current(address, coin_type, namespace)` is the claim-side lookup and invalidation anchor for that tuple. Projection-owned claim state may explain tuple admission or claim invalidation but doesn't persist `execution_trace_id` or `verified_primary_name`.

Section-local provenance:

- `claimed_primary_name.provenance` is exact-tuple declared-only provenance from the requested row. Strips `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material; omits `execution_trace_id`.
- `verified_primary_name.provenance` (when present) is `{execution_trace_id, manifest_versions}`. Its `execution_trace_id` must equal top-level `provenance.execution_trace_id`; its `manifest_versions` must narrow that same persisted trace.
- Top-level route provenance joins claim-side and verification-side context.

The shipped ENS and Basenames primary-name paths don't require dedicated manifest capability flags. Reverse claim admission stays under `ens_v1_reverse_l1` / `basenames_base_primary`; verified-primary readback stays execution-derived under `ens_execution` / `basenames_execution`.

## Trace schema

Each verified answer persists into `execution_traces`:

- `execution_trace_id`
- request type, request key
- namespace, chain positions, manifest versions
- step list
- contracts called, gateway digests
- final value, failure reason, finished timestamp

For resolution, one persisted answer may carry multiple selector-scoped outputs under one `execution_trace_id`. For exact-tuple verified primary, one answer covers exactly one `{address, namespace, coin_type}` tuple under `request_type=verified_primary_name`.

Each `execution_steps` row records:

- step index, step kind
- input digest, output digest
- latency
- canonicality dependency

Admitted exact block-anchored `raw_call_snapshots` aren't part of this schema. They remain intake-owned raw facts keyed by exact block identity even when verified-resolution persistence hands them off alongside the trace.

Execution traces and steps are durable audit artifacts. Reorg-driven cache invalidation doesn't delete them — it only changes whether a persisted outcome is reusable as a cache hit.

### Worker inspection

`bigname-worker inspect execution-trace --execution-trace-id <id> --json` is read-only over one persisted trace. Output includes `command`, `execution_trace_id`, request metadata, request type and key, namespace, chain positions, manifest versions, status, final value digest, failure reason, finished timestamp, and ordered `steps` with index, kind, digests, latency, canonicality dependency, attachment digest metadata.

Reads `execution_traces`, `execution_steps`, and trace attachment metadata only. Doesn't execute or re-execute, expose a public `v1` route, dump raw gateway transcripts, synthesize topology from non-trace storage, or mutate any state.

## Cache identity and invalidation

Persisted outcomes live in `execution_cache_outcomes`, keyed by:

- request key
- requested chain positions
- manifest versions
- topology version boundary
- record version boundary

For resolution, the request key includes the normalized explicit selector set so the cache boundary matches `verified_queries`. For `GET /v1/resolve/{name}`, the resolution request key is built from the inferred namespace, normalized name, and normalized selector set — a namespace-inferred request and the equivalent canonical request share cache identity after inference.

For verified primary, the request key is the normalized tuple `{namespace}:{normalized_address}:{coin_type}`. The matching `primary_names_current(address, coin_type, namespace)` row is the only admitted claim-side anchor; projection updates may invalidate request-matching answers but the projection doesn't persist verified payloads or trace IDs.

Invalidate on:

- reorg
- manifest change
- resolver change
- alias or wildcard topology change
- relevant record change
- primary claim change

**Reorg invalidation:** reorg repair invalidates any `execution_cache_outcomes` whose dependency set contains an orphaned block identity. Cache dependencies must tie to explicit block-hash-bearing chain positions or boundaries; block numbers, `latest`/`head` tags, manifest versions, topology versions, and record versions aren't sufficient unless they resolve to one or more block hashes or to source rows that carry block hashes. Verified resolution and verified primary-name rows without explicit block-hash-bearing dependencies fail closed and are ineligible for cache reuse after a reorg check. Invalidation affects cache eligibility only; traces, steps, and attachments stay durable.

## Explain

Every verified answer must be explainable through the selected entrypoint, resolver discovery path, wildcard traversal, alias rewriting, CCIP steps, and the final comparison or returned record value.

`GET /v1/explain/resolutions/{namespace}/{name}/execution` is keyed by the same current exact surface and explicit selector set as the mixed route. It reads the persisted trace and selector-scoped results — never re-executing or synthesizing from declared topology alone. Top-level provenance and any selector-local provenance anchor to the same persisted `execution_trace_id`.

The route surfaces the selected entrypoint, resolver discovery path, wildcard traversal, alias rewriting, and the ordered persisted step summary. CCIP-Read participation appears through persisted step kinds, not a raw gateway transcript.

Public explain support stays coupled to the same verified-resolution support boundary as the mixed route. Deferred unsupported path classes don't gain a synthetic trace-shaped public contract. For Basenames, the public execution-explain boundary applies only to execution explain; the separate declared exact-name explain routes stay on the Base-side declared read plane.

## Support boundary

ENS verified resolution on Ethereum mainnet uses `ens_execution` at the Universal Resolver proxy.[^ens-univ][^v1-aur-entrypoint] Public verified support covers three exact-surface path classes against the same declared topology snapshot used by the mixed route:

| Class | Conditions |
| --- | --- |
| **Direct** | `resolver_path[0].logical_name_id == data.logical_name_id`; `wildcard.source=null` with `matched_labels=[]`; `alias.final_target=null` with `hops=[]`; all `transport=null`. |
| **Alias-only non-direct** | Same shape, except `alias.final_target` non-`null` with non-empty `hops`. |
| **Wildcard-derived** | `wildcard.source` non-`null` with non-empty `matched_labels`; `resolver_path[0].logical_name_id == wildcard.source.logical_name_id`; `alias.final_target=null` with `hops=[]`; `subregistry_path=[]`; all `transport=null`. |

All three flow through the same persisted execution trace and explain contract.

ENS requests outside these classes — non-alias ancestor-selected paths, linked-subregistry ancestor-selected paths, transport-assisted paths, requests whose persisted execution used CCIP-Read — return selector-local `status=unsupported`. The explain route doesn't synthesize public traces for them.

Basenames verified resolution uses active `basenames_execution` v2 at the L1 Resolver for the exact-surface transport-assisted direct-path class:

- `resolver_path[0].logical_name_id == data.logical_name_id`
- `wildcard.source=null`, `matched_labels=[]`
- `alias.final_target=null`, `hops=[]`
- `subregistry_path=[]`
- `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`

CCIP-participating traces are eligible for that class — not `unsupported` — because upstream `L1Resolver` initiates `OffchainLookup` for non-`base.eth` requests and completes them through `resolveWithProof`.[^bn-l1resolver-flow] Explain surfaces the resulting persisted CCIP steps without inventing a second trace family. Other Basenames paths return `unsupported`.

`GET /v1/resolve/{name}` doesn't widen this boundary. Inferred Basenames verified selectors return `unsupported` unless the requested snapshot satisfies the same Basenames class.

ENS and Basenames primary-name coverage is graduated only for the exact-tuple persisted-readback class. Out-of-class tuples, fallback claim sources, richer claimed payloads, fresh verified-primary execution, and namespace-wide claims remain `unsupported` or out of scope.

Declared resolver-profile gaps stay requestable on the declared read plane. They don't by themselves make a supported verified-resolution path unsupported. Supported Universal Resolver selectors read matching persisted output or execute on demand at the selected snapshot, then persist and return the outcome.

---

[^ens-univ]: <https://docs.ens.domains/resolvers/universal/>
[^bn-readme]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-revreg]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^v1-iur]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-aur]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f)
[^v1-revreg-claim]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-aur-entrypoint]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L90 @ ens_v1@91c966f)
[^bn-l1resolver-flow]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
